use super::block_allocation::BlockAllocation;
use super::line::*;
use super::{block::*, chunk::ChunkMap, defrag::Defrag};
use crate::plan::immix::{Immix, ImmixCopyContext, Pause};
use crate::plan::EdgeIterator;
use crate::plan::PlanConstraints;
use crate::policy::immix::chunk::Chunk;
use crate::policy::largeobjectspace::{RCReleaseMatureLOS, RCSweepMatureLOS};
use crate::policy::space::SpaceOptions;
use crate::policy::space::{CommonSpace, Space, SFT};
use crate::util::heap::layout::heap_layout::{Mmapper, VMMap};
use crate::util::heap::HeapMeta;
use crate::util::heap::PageResource;
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::*;
use crate::util::metadata::{self, compare_exchange_metadata, load_metadata, MetadataSpec};
use crate::util::rc::SweepBlocksAfterDecs;
use crate::util::{object_forwarding as ForwardingWord, rc};
use crate::util::{Address, ObjectReference};
use crate::{
    plan::TransitiveClosure,
    scheduler::{gc_work::ProcessEdgesWork, GCWork, GCWorkScheduler, GCWorker, WorkBucketStage},
    util::{heap::blockpageresource::BlockPageResource, opaque_pointer::VMThread},
    AllocationSemantics, CopyContext, MMTK,
};
use crate::{vm::*, LazySweepingJobsCounter};
use atomic::Ordering;
use crossbeam_queue::SegQueue;
use spin::Mutex;
use std::sync::atomic::AtomicUsize;
use std::{
    iter::Step,
    ops::Range,
    sync::{atomic::AtomicU8, Arc},
};
use std::{mem, ptr};

pub static RELEASED_NURSERY_BLOCKS: AtomicUsize = AtomicUsize::new(0);
pub static RELEASED_BLOCKS: AtomicUsize = AtomicUsize::new(0);

pub struct ImmixSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pub pr: BlockPageResource<VM>,
    /// Allocation status for all chunks in immix space
    pub chunk_map: ChunkMap,
    /// Current line mark state
    pub line_mark_state: AtomicU8,
    /// Line mark state in previous GC
    line_unavail_state: AtomicU8,
    /// A list of all reusable blocks
    pub reusable_blocks: BlockList,
    /// Defrag utilities
    pub(super) defrag: Defrag,
    /// Object mark state
    mark_state: u8,
    /// Work packet scheduler
    scheduler: Arc<GCWorkScheduler<VM>>,
    pub block_allocation: BlockAllocation<VM>,
    possibly_dead_mature_blocks: SegQueue<Block>,
    initial_mark_pause: bool,
    pub last_mutator_recycled_blocks: SegQueue<Block>,
    pub mutator_recycled_blocks: SegQueue<Block>,
    pub mature_evac_remsets: Mutex<Vec<Box<dyn GCWork<VM>>>>,
    last_defrag_blocks: Vec<Block>,
    defrag_blocks: Vec<Block>,
    num_defrag_blocks: AtomicUsize,
    defrag_chunk_cursor: AtomicUsize,
}

unsafe impl<VM: VMBinding> Sync for ImmixSpace<VM> {}

impl<VM: VMBinding> SFT for ImmixSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }
    fn is_live(&self, object: ObjectReference) -> bool {
        if super::REF_COUNT {
            return crate::util::rc::count(object) > 0;
        }
        if self.initial_mark_pause {
            return true;
        }
        if crate::args::CONCURRENT_MARKING {
            let block_state = Block::containing::<VM>(object).get_state();
            if block_state == BlockState::Nursery {
                return true;
            }
        }
        self.is_marked(object) || ForwardingWord::is_forwarded::<VM>(object)
    }
    fn is_movable(&self) -> bool {
        super::DEFRAG
    }
    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        true
    }
    fn initialize_object_metadata(&self, _object: ObjectReference, _bytes: usize, _alloc: bool) {
        #[cfg(feature = "global_alloc_bit")]
        crate::util::alloc_bit::set_alloc_bit(_object);
    }
}

impl<VM: VMBinding> Space<VM> for ImmixSpace<VM> {
    fn as_space(&self) -> &dyn Space<VM> {
        self
    }
    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }
    #[inline(always)]
    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        &self.pr
    }
    #[inline(always)]
    fn common(&self) -> &CommonSpace<VM> {
        &self.common
    }
    fn init(&mut self, _vm_map: &'static VMMap) {
        super::validate_features();
        self.common().init(self.as_space());
        self.block_allocation
            .init(unsafe { &*(self as *const Self) })
    }
    fn release_multiple_pages(&mut self, _start: Address) {
        panic!("immixspace only releases pages enmasse")
    }
}

impl<VM: VMBinding> ImmixSpace<VM> {
    const UNMARKED_STATE: u8 = 0;
    const MARKED_STATE: u8 = 1;

    /// Get side metadata specs
    fn side_metadata_specs() -> Vec<SideMetadataSpec> {
        if crate::plan::immix::REF_COUNT {
            return metadata::extract_side_metadata(&vec![
                MetadataSpec::OnSide(Block::DEFRAG_STATE_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                MetadataSpec::OnSide(crate::util::rc::RC_STRADDLE_LINES),
                MetadataSpec::OnSide(Block::LOG_TABLE),
            ]);
        }
        metadata::extract_side_metadata(&if super::BLOCK_ONLY {
            vec![
                MetadataSpec::OnSide(Block::DEFRAG_STATE_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
            ]
        } else {
            vec![
                MetadataSpec::OnSide(Line::MARK_TABLE),
                MetadataSpec::OnSide(Block::DEFRAG_STATE_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
            ]
        })
    }

    pub fn new(
        name: &'static str,
        vm_map: &'static VMMap,
        mmapper: &'static Mmapper,
        heap: &mut HeapMeta,
        scheduler: Arc<GCWorkScheduler<VM>>,
        global_side_metadata_specs: Vec<SideMetadataSpec>,
        constraints: &'static PlanConstraints,
    ) -> Self {
        let common = CommonSpace::new(
            SpaceOptions {
                name,
                movable: true,
                immortal: false,
                zeroed: true,
                vmrequest: VMRequest::discontiguous(),
                side_metadata_specs: SideMetadataContext {
                    global: global_side_metadata_specs,
                    local: Self::side_metadata_specs(),
                },
                needs_log_bit: constraints.needs_log_bit,
                needs_field_log_bit: constraints.needs_field_log_bit,
            },
            vm_map,
            mmapper,
            heap,
        );
        ImmixSpace {
            pr: if common.vmrequest.is_discontiguous() {
                unreachable!()
                // BlockPageResource::new_discontiguous(Block::LOG_PAGES, vm_map)
            } else {
                BlockPageResource::new_contiguous(
                    Block::LOG_PAGES,
                    common.start,
                    common.extent,
                    vm_map,
                )
            },
            common,
            chunk_map: ChunkMap::new(),
            line_mark_state: AtomicU8::new(Line::RESET_MARK_STATE),
            line_unavail_state: AtomicU8::new(Line::RESET_MARK_STATE),
            reusable_blocks: BlockList::default(),
            defrag: Defrag::default(),
            mark_state: Self::UNMARKED_STATE,
            scheduler,
            block_allocation: BlockAllocation::new(),
            possibly_dead_mature_blocks: Default::default(),
            initial_mark_pause: false,
            last_mutator_recycled_blocks: Default::default(),
            mutator_recycled_blocks: Default::default(),
            mature_evac_remsets: Default::default(),
            num_defrag_blocks: AtomicUsize::new(0),
            defrag_chunk_cursor: AtomicUsize::new(0),
            defrag_blocks: Default::default(),
            last_defrag_blocks: Default::default(),
        }
    }

    /// Get the number of defrag headroom pages.
    pub fn defrag_headroom_pages(&self) -> usize {
        self.defrag.defrag_headroom_pages(self)
    }

    /// Check if current GC is a defrag GC.
    #[inline(always)]
    pub fn in_defrag(&self) -> bool {
        self.defrag.in_defrag()
    }

    /// check if the current GC should do defragmentation.
    pub fn decide_whether_to_defrag(
        &self,
        emergency_collection: bool,
        collect_whole_heap: bool,
        collection_attempts: usize,
        user_triggered_collection: bool,
        full_heap_system_gc: bool,
    ) -> bool {
        self.defrag.decide_whether_to_defrag(
            emergency_collection,
            collect_whole_heap,
            collection_attempts,
            user_triggered_collection,
            self.reusable_blocks.len() == 0,
            full_heap_system_gc,
        );
        self.defrag.in_defrag()
    }

    /// Get work packet scheduler
    #[inline(always)]
    pub fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    pub fn select_mature_evacuation_candidates(&self) {
        let me = unsafe { &mut *(self as *const Self as *mut Self) };
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        // Select mature defrag blocks
        let defrag_blocks = 1024;
        let mut count = 0;
        let chunks = self.chunk_map.all_chunks();
        let num_chunks = Chunk::steps_between(&chunks.start, &chunks.end).unwrap();
        let mut search_defrag_blocks =
            |chunks: Range<Chunk>, chunk_cursor: &mut usize, limit: usize, count: &mut usize| {
                'out: for c in chunks.skip(*chunk_cursor).filter(|c| c.is_committed()) {
                    for b in c.committed_blocks().filter(|c| {
                        c.get_state() != BlockState::Nursery && c.get_state() != BlockState::Reusing
                    }) {
                        // println!(" - defrag {:?} {:?}", b, b.get_state());
                        b.set_as_defrag_source(true);
                        me.defrag_blocks.push(b);
                        *count += 1;
                        if *count >= defrag_blocks {
                            break 'out;
                        }
                    }
                    *chunk_cursor += 1;
                    if *chunk_cursor >= limit {
                        break 'out;
                    }
                }
            };
        let mut chunk_cursor = self.defrag_chunk_cursor.load(Ordering::SeqCst);
        let original_cursor = chunk_cursor;
        search_defrag_blocks(chunks.clone(), &mut chunk_cursor, num_chunks, &mut count);
        if count < defrag_blocks && chunk_cursor >= num_chunks {
            chunk_cursor = 0;
            search_defrag_blocks(
                chunks.clone(),
                &mut chunk_cursor,
                original_cursor,
                &mut count,
            );
        }
        if chunk_cursor >= num_chunks {
            chunk_cursor = 0;
        }
        self.defrag_chunk_cursor
            .store(chunk_cursor, Ordering::SeqCst);
        if crate::args::LOG_PER_GC_STATE {
            println!(
                " - Defrag {} mature blocks (cursor @ {}/{})",
                count, chunk_cursor, num_chunks
            );
        }
        self.num_defrag_blocks.store(count, Ordering::SeqCst);
    }

    pub fn rc_eager_prepare(&mut self, pause: Pause) {
        let num_workers = self.scheduler().worker_group().worker_count();
        // let (stw_packets, delayed_packets, nursery_blocks) =
        //     if crate::args::LOCK_FREE_BLOCK_ALLOCATION {
        //         self.block_allocation
        //             .reset_and_generate_nursery_sweep_tasks(num_workers)
        //     } else {
        //         unreachable!();
        //     };
        let (stw_packets, nursery_blocks) = self
            .block_allocation
            .reset_and_generate_nursery_sweep_tasks2(num_workers);
        // If there are not too much nursery blocks for release, we
        // reclain mature blocks as well.
        let mature_blocks = if pause == Pause::FinalMark || pause == Pause::FullTraceFast {
            self.num_defrag_blocks.load(Ordering::SeqCst)
        } else {
            0
        };
        if crate::args::LAZY_DECREMENTS
            && (nursery_blocks + mature_blocks) < crate::args::NO_LAZY_DEC_THRESHOLD
        {
            if crate::args::LOG_PER_GC_STATE {
                println!(
                    "disable lazy dec: nursery_blocks={} mature_blocks={} threshold={}",
                    nursery_blocks,
                    mature_blocks,
                    crate::args::NO_LAZY_DEC_THRESHOLD
                );
            }
            crate::DISABLE_LASY_DEC_FOR_CURRENT_GC.store(true, Ordering::SeqCst);
        }
        self.scheduler().work_buckets[WorkBucketStage::RCReleaseNursery].bulk_add(stw_packets);
        if pause == Pause::FullTraceFast || pause == Pause::InitialMark {
            // Update mark_state
            // if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_on_side() {
            //     self.mark_state = Self::MARKED_STATE;
            // } else {
            //     // For header metadata, we use cyclic mark bits.
            //     unimplemented!("cyclic mark bits is not supported at the moment");
            // }
            // Reset block mark and object mark table.
            let space = unsafe { &mut *(self as *mut Self) };
            let work_packets = self.chunk_map.generate_prepare_tasks::<VM>(space, None);
            self.scheduler().work_buckets[WorkBucketStage::Initial].bulk_add(work_packets);
        }
    }

    pub fn prepare_rc(&mut self, pause: Pause) {
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            debug_assert!(self.last_defrag_blocks.is_empty());
            std::mem::swap(&mut self.defrag_blocks, &mut self.last_defrag_blocks);
        }
        // Mutator reused blocks cannot be released until reaching a RC pause.
        // Remaing the block state as "reusing" and reset them here.
        debug_assert!(self.last_mutator_recycled_blocks.is_empty());
        std::mem::swap(
            &mut self.last_mutator_recycled_blocks,
            &mut self.mutator_recycled_blocks,
        );
        debug_assert_ne!(pause, Pause::FullTraceDefrag);
        // Tracing GC preparation work
        if pause == Pause::FullTraceFast || pause == Pause::InitialMark {
            // Update mark_state
            if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_on_side() {
                self.mark_state = Self::MARKED_STATE;
            } else {
                // For header metadata, we use cyclic mark bits.
                unimplemented!("cyclic mark bits is not supported at the moment");
            }
            // Reset block mark and object mark table.
            // let space = unsafe { &mut *(self as *mut Self) };
            // let work_packets = self.chunk_map.generate_prepare_tasks::<VM>(space, None);
            // self.scheduler().work_buckets[WorkBucketStage::Prepare].bulk_add(work_packets);
        }
    }

    pub fn release_rc(&mut self, pause: Pause) {
        debug_assert_ne!(pause, Pause::FullTraceDefrag);
        self.block_allocation.reset();
        let disable_lasy_dec_for_current_gc = crate::disable_lasy_dec_for_current_gc();
        if disable_lasy_dec_for_current_gc {
            self.scheduler().process_lazy_decrement_packets();
        }
    }

    pub fn schedule_mature_sweeping(&mut self, pause: Pause) {
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            let disable_lasy_dec_for_current_gc = crate::disable_lasy_dec_for_current_gc();
            let dead_cycle_sweep_packets = self.chunk_map.generate_dead_cycle_sweep_tasks();
            let sweep_los = RCSweepMatureLOS::new(LazySweepingJobsCounter::new_desc());
            if crate::args::LAZY_DECREMENTS && !disable_lasy_dec_for_current_gc {
                self.scheduler().postpone_all(dead_cycle_sweep_packets);
                self.scheduler().postpone(sweep_los);
            } else {
                self.scheduler().work_buckets[WorkBucketStage::RCFullHeapRelease]
                    .bulk_add(dead_cycle_sweep_packets);
                self.scheduler().work_buckets[WorkBucketStage::RCFullHeapRelease].add(sweep_los);
            }
            while let Some(block) = self.last_defrag_blocks.pop() {
                block.set_as_defrag_source(false);
                block.clear_rc_table::<VM>();
                block.clear_striddle_table::<VM>();
                self.add_to_possibly_dead_mature_blocks(block);
            }
        }
    }

    pub fn prepare(&mut self, major_gc: bool, initial_mark_pause: bool) {
        self.initial_mark_pause = initial_mark_pause;
        debug_assert!(!crate::args::REF_COUNT);
        self.block_allocation.reset();
        if major_gc {
            // Update mark_state
            if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_on_side() {
                self.mark_state = Self::MARKED_STATE;
            } else {
                // For header metadata, we use cyclic mark bits.
                unimplemented!("cyclic mark bits is not supported at the moment");
            }
        }

        // Prepare defrag info
        if super::DEFRAG {
            self.defrag.prepare(self);
        }
        // Prepare each block for GC
        let threshold = self.defrag.defrag_spill_threshold.load(Ordering::Acquire);
        // # Safety: ImmixSpace reference is always valid within this collection cycle.
        let space = unsafe { &*(self as *const Self) };
        let work_packets = self.chunk_map.generate_prepare_tasks::<VM>(
            space,
            if space.in_defrag() {
                Some(threshold)
            } else {
                None
            },
        );
        self.scheduler().work_buckets[WorkBucketStage::Prepare].bulk_add(work_packets);
        // Update line mark state
        if !super::BLOCK_ONLY {
            self.line_mark_state.fetch_add(1, Ordering::AcqRel);
            if self.line_mark_state.load(Ordering::Acquire) > Line::MAX_MARK_STATE {
                self.line_mark_state
                    .store(Line::RESET_MARK_STATE, Ordering::Release);
            }
        }
    }

    /// Release for the immix space. This is called when a GC finished.
    /// Return whether this GC was a defrag GC, as a plan may want to know this.
    pub fn release(&mut self, major_gc: bool) -> bool {
        debug_assert!(!crate::args::REF_COUNT);
        self.block_allocation.reset();
        let did_defrag = self.defrag.in_defrag();
        if major_gc {
            // Update line_unavail_state for hole searching afte this GC.
            if !super::BLOCK_ONLY {
                self.line_unavail_state.store(
                    self.line_mark_state.load(Ordering::Acquire),
                    Ordering::Release,
                );
            }
        }
        // Clear reusable blocks list
        if !super::BLOCK_ONLY {
            self.reusable_blocks.reset();
        }
        // Sweep chunks and blocks
        // # Safety: ImmixSpace reference is always valid within this collection cycle.
        let space = unsafe { &*(self as *const Self) };
        let work_packets = self.chunk_map.generate_sweep_tasks(space, false);
        self.scheduler().work_buckets[WorkBucketStage::Release].bulk_add(work_packets);
        if super::DEFRAG {
            self.defrag.release(self);
        }
        self.initial_mark_pause = false;
        did_defrag
    }

    /// Release a block.
    pub fn release_block(&self, block: Block, nursery: bool) {
        self.deinit_block(block, nursery);
        self.pr.release_pages(block.start());
    }

    pub fn deinit_block(&self, block: Block, nursery: bool) {
        // println!("Release {:?} {} defrag={}", block, nursery, block.is_defrag_source());
        if crate::args::LOG_PER_GC_STATE {
            if nursery {
                RELEASED_NURSERY_BLOCKS.fetch_add(1, Ordering::SeqCst);
            }
            RELEASED_BLOCKS.fetch_add(1, Ordering::SeqCst);
        }
        if crate::args::BARRIER_MEASUREMENT
            || (!(crate::args::RC_NURSERY_EVACUATION && nursery) && self.common().needs_log_bit)
        {
            block.clear_log_table::<VM>();
        }
        block.deinit();
    }

    /// Allocate a clean block.
    #[inline(always)]
    pub fn get_clean_block(&self, tls: VMThread, copy: bool) -> Option<Block> {
        self.block_allocation.get_clean_block(tls, copy)
    }

    /// Pop a reusable block from the reusable block list.
    #[inline(always)]
    pub fn get_reusable_block(&self, copy: bool) -> Option<Block> {
        self.block_allocation.get_reusable_block(copy)
    }

    /// Trace and mark objects without evacuation.
    #[inline(always)]
    pub fn process_mature_evacuation_remset(&self) {
        let mut remsets = vec![];
        mem::swap(&mut remsets, &mut self.mature_evac_remsets.lock());
        self.scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].bulk_add(remsets);
    }

    /// Trace and mark objects without evacuation.
    #[inline(always)]
    pub fn fast_trace_object(
        &self,
        trace: &mut impl TransitiveClosure,
        object: ObjectReference,
    ) -> ObjectReference {
        self.trace_object_without_moving(trace, object)
    }

    /// Trace and mark objects. If the current object is in defrag block, then do evacuation as well.
    #[inline(always)]
    pub fn trace_object(
        &self,
        trace: &mut impl TransitiveClosure,
        object: ObjectReference,
        semantics: AllocationSemantics,
        copy_context: &mut impl CopyContext,
    ) -> ObjectReference {
        #[cfg(feature = "global_alloc_bit")]
        debug_assert!(
            crate::util::alloc_bit::is_alloced(object),
            "{:x}: alloc bit not set",
            object
        );
        if Block::containing::<VM>(object).is_defrag_source() {
            self.trace_object_with_opportunistic_copy(trace, object, semantics, copy_context)
        } else {
            self.trace_object_without_moving(trace, object)
        }
    }

    /// Trace and mark objects without evacuation.
    #[inline(always)]
    pub fn trace_object_without_moving(
        &self,
        trace: &mut impl TransitiveClosure,
        object: ObjectReference,
    ) -> ObjectReference {
        if self.attempt_mark(object) {
            // println!("Mark {:?}", object.range::<VM>());
            if !crate::args::REF_COUNT {
                // Mark block and lines
                if !super::BLOCK_ONLY {
                    if !super::MARK_LINE_AT_SCAN_TIME {
                        self.mark_lines(object);
                    }
                } else {
                    let block = Block::containing::<VM>(object);
                    let state = block.get_state();
                    if state != BlockState::Nursery && state != BlockState::Marked {
                        block.set_state(BlockState::Marked);
                    }
                }
            }
            // Visit node
            trace.process_node(object);
        }
        object
    }

    /// Trace object and do evacuation if required.
    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    pub fn trace_object_with_opportunistic_copy(
        &self,
        trace: &mut impl TransitiveClosure,
        object: ObjectReference,
        semantics: AllocationSemantics,
        copy_context: &mut impl CopyContext,
    ) -> ObjectReference {
        debug_assert!(!super::BLOCK_ONLY);
        let forwarding_status = ForwardingWord::attempt_to_forward::<VM>(object);
        if ForwardingWord::state_is_forwarded_or_being_forwarded(forwarding_status) {
            ForwardingWord::spin_and_get_forwarded_object::<VM>(object, forwarding_status)
        } else if self.is_marked(object) {
            ForwardingWord::clear_forwarding_bits::<VM>(object);
            object
        } else {
            let new_object = if Self::is_pinned(object) || self.defrag.space_exhausted() {
                self.attempt_mark(object);
                ForwardingWord::clear_forwarding_bits::<VM>(object);
                Block::containing::<VM>(object).set_state(BlockState::Marked);
                object
            } else {
                #[cfg(feature = "global_alloc_bit")]
                crate::util::alloc_bit::unset_alloc_bit(object);
                ForwardingWord::forward_object::<VM, _>(object, semantics, copy_context)
            };
            if !super::MARK_LINE_AT_SCAN_TIME {
                self.mark_lines(new_object);
            }
            debug_assert!({
                let state = Block::containing::<VM>(new_object).get_state();
                state == BlockState::Marked || state == BlockState::Nursery
            });
            trace.process_node(new_object);
            new_object
        }
    }

    #[inline(always)]
    pub fn rc_trace_object(
        &self,
        trace: &mut impl TransitiveClosure,
        object: ObjectReference,
        copy_context: &mut impl CopyContext,
        pause: Pause,
    ) -> ObjectReference {
        debug_assert!(crate::args::REF_COUNT);
        if crate::args::RC_MATURE_EVACUATION && Block::containing::<VM>(object).is_defrag_source() {
            self.trace_forward_rc_mature_object(trace, object, copy_context, pause)
        } else {
            if crate::args::RC_MATURE_EVACUATION {
                self.trace_mark_rc_mature_object(trace, object, pause)
            } else {
                self.trace_object_without_moving(trace, object)
            }
        }
    }

    #[inline(always)]
    pub fn trace_mark_rc_mature_object(
        &self,
        trace: &mut impl TransitiveClosure,
        mut object: ObjectReference,
        pause: Pause,
    ) -> ObjectReference {
        if ForwardingWord::is_forwarded::<VM>(object) {
            object = ForwardingWord::read_forwarding_pointer::<VM>(object);
        }
        if self.attempt_mark(object) {
            trace.process_node(object);
        }
        object
    }

    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    pub fn trace_forward_rc_mature_object(
        &self,
        trace: &mut impl TransitiveClosure,
        object: ObjectReference,
        copy_context: &mut impl CopyContext,
        pause: Pause,
    ) -> ObjectReference {
        let forwarding_status = ForwardingWord::attempt_to_forward::<VM>(object);
        if ForwardingWord::state_is_forwarded_or_being_forwarded(forwarding_status) {
            let new =
                ForwardingWord::spin_and_get_forwarded_object::<VM>(object, forwarding_status);
            if self.attempt_mark(new) {
                trace.process_node(new)
            }
            new
        } else {
            // Evacuate the mature object
            let new = ForwardingWord::forward_object::<VM, _>(
                object,
                AllocationSemantics::Default,
                copy_context,
            );
            // Transfer RC count
            new.log_start_address::<VM>();
            if !crate::args::BLOCK_ONLY {
                if new.get_size::<VM>() > Line::BYTES {
                    rc::mark_straddle_object::<VM>(new);
                }
            }
            rc::set(new, rc::count(object));
            self.attempt_mark(new);
            self.unmark(object);
            trace.process_node(new);
            new
        }
    }

    /// Mark all the lines that the given object spans.
    #[allow(clippy::assertions_on_constants)]
    #[inline]
    pub fn mark_lines(&self, object: ObjectReference) {
        debug_assert!(!super::BLOCK_ONLY);
        if crate::args::REF_COUNT {
            return;
        }
        Line::mark_lines_for_object::<VM>(object, self.line_mark_state.load(Ordering::Acquire));
    }

    /// Atomically mark an object.
    #[inline(always)]
    pub fn attempt_mark(&self, object: ObjectReference) -> bool {
        loop {
            let old_value = load_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                object,
                None,
                Some(Ordering::SeqCst),
            ) as u8;
            if old_value == self.mark_state {
                return false;
            }

            if compare_exchange_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                object,
                old_value as usize,
                self.mark_state as usize,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                break;
            }
        }
        true
    }

    /// Atomically mark an object.
    #[inline(always)]
    pub fn unmark(&self, object: ObjectReference) -> bool {
        loop {
            let old_value = load_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                object,
                None,
                Some(Ordering::SeqCst),
            ) as u8;
            if old_value == Self::UNMARKED_STATE {
                return false;
            }

            if compare_exchange_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                object,
                Self::MARKED_STATE as _,
                Self::UNMARKED_STATE as _,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                break;
            }
        }
        true
    }

    /// Check if an object is marked.
    #[inline(always)]
    pub fn is_marked(&self, object: ObjectReference) -> bool {
        let old_value = load_metadata::<VM>(
            &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
            object,
            None,
            Some(Ordering::SeqCst),
        ) as u8;
        old_value == self.mark_state
    }

    #[inline(always)]
    pub fn mark_bit(&self, object: ObjectReference) -> bool {
        let old_value = load_metadata::<VM>(
            &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
            object,
            None,
            Some(Ordering::SeqCst),
        ) as u8;
        old_value == self.mark_state
    }

    /// Check if an object is pinned.
    #[inline(always)]
    fn is_pinned(_object: ObjectReference) -> bool {
        // TODO(wenyuzhao): Object pinning not supported yet.
        false
    }

    /// Hole searching.
    ///
    /// Linearly scan lines in a block to search for the next
    /// hole, starting from the given line.
    ///
    /// Returns None if the search could not find any more holes.
    #[allow(clippy::assertions_on_constants)]
    pub fn get_next_available_lines(&self, copy: bool, search_start: Line) -> Option<Range<Line>> {
        debug_assert!(!super::BLOCK_ONLY);
        if super::REF_COUNT {
            self.rc_get_next_available_lines(copy, search_start)
        } else {
            self.normal_get_next_available_lines(search_start)
        }
    }

    /// Search holes by ref-counts instead of line marks
    #[allow(clippy::assertions_on_constants)]
    #[inline]
    pub fn rc_get_next_available_lines(
        &self,
        copy: bool,
        search_start: Line,
    ) -> Option<Range<Line>> {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(super::REF_COUNT);
        let block = search_start.block();
        let rc_array = RCArray::of(block);
        let mut cursor = search_start.get_index_within_block();
        let limit = Block::LINES;
        // Find start
        while cursor < limit {
            if rc_array.is_dead(cursor) {
                break;
            }
            cursor += 1;
        }
        // Conservatively skip the next line
        cursor += 1;
        if cursor >= limit {
            return None;
        }
        let start = cursor;
        // Find limit
        while cursor < limit {
            if !rc_array.is_dead(cursor) {
                break;
            }
            cursor += 1;
        }
        if cursor == start {
            return None;
        }
        let end = cursor;
        let start = Line::from(block.start() + (start << Line::LOG_BYTES));
        let end = Line::from(block.start() + (end << Line::LOG_BYTES));
        if self.common.needs_log_bit {
            if !copy {
                Line::clear_log_table::<VM>(start..end);
            } else {
                Line::initialize_log_table_as_unlogged::<VM>(start..end);
            }
        }
        // Line::clear_mark_table::<VM>(start..end);
        // if !_copy {
        //     println!("reuse {:?} copy={}", start..end, _copy);
        // }
        Some(start..end)
    }

    #[allow(clippy::assertions_on_constants)]
    #[inline]
    pub fn normal_get_next_available_lines(&self, search_start: Line) -> Option<Range<Line>> {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(!super::REF_COUNT);
        let unavail_state = self.line_unavail_state.load(Ordering::Acquire);
        let current_state = self.line_mark_state.load(Ordering::Acquire);
        let block = search_start.block();
        let mut mark_data = block.line_mark_table();
        let start_cursor = search_start.get_index_within_block();
        let mut cursor = start_cursor;
        // Find start
        while cursor < Block::LINES {
            let mark = mark_data.get(cursor);
            if mark != unavail_state && mark != current_state {
                break;
            }
            cursor += 1;
        }
        if cursor == Block::LINES {
            return None;
        }
        let start = Line::forward(search_start, cursor - start_cursor);
        // Find limit
        while cursor < Block::LINES {
            let mark = mark_data.get(cursor);
            if mark == unavail_state || mark == current_state {
                break;
            }
            if crate::plan::immix::CONCURRENT_MARKING {
                mark_data.set(cursor, current_state);
            }
            cursor += 1;
        }
        let end = Line::forward(search_start, cursor - start_cursor);
        if self.common.needs_log_bit {
            Line::clear_log_table::<VM>(start..end);
        }
        Some(start..end)
    }

    #[inline(always)]
    pub fn add_to_possibly_dead_mature_blocks(&self, block: Block) {
        if block.log() {
            self.possibly_dead_mature_blocks.push(block);
        }
    }

    pub fn schedule_rc_block_sweeping_tasks(&self, counter: LazySweepingJobsCounter) {
        while let Some(x) = self.last_mutator_recycled_blocks.pop() {
            x.set_state(BlockState::Marked);
        }
        // This may happen either within a pause, or in concurrent.
        let size = self.possibly_dead_mature_blocks.len();
        let num_bins = self.scheduler().num_workers() << 1;
        let bin_cap = size / num_bins + if size % num_bins == 0 { 0 } else { 1 };
        let mut bins = (0..num_bins)
            .map(|_| Vec::with_capacity(bin_cap))
            .collect::<Vec<Vec<Block>>>();
        'out: for i in 0..num_bins {
            for _ in 0..bin_cap {
                if let Some(block) = self.possibly_dead_mature_blocks.pop() {
                    bins[i].push(block);
                } else {
                    break 'out;
                }
            }
        }
        let packets = bins
            .into_iter()
            .map::<Box<dyn GCWork<VM>>, _>(|blocks| {
                box SweepBlocksAfterDecs::new(blocks, counter.clone())
            })
            .collect();
        self.scheduler().work_buckets[WorkBucketStage::Unconstrained].bulk_add(packets);
        self.scheduler().work_buckets[WorkBucketStage::Unconstrained]
            .add(RCReleaseMatureLOS::new(counter.clone()));
    }
}

/// A work packet to scan the fields of each objects and mark lines.
pub struct ScanObjectsAndMarkLines<Edges: ProcessEdgesWork> {
    buffer: Vec<ObjectReference>,
    concurrent: bool,
    immix: Option<&'static Immix<Edges::VM>>,
    immix_space: &'static ImmixSpace<Edges::VM>,
    edges: Vec<Address>,
    worker: *mut GCWorker<Edges::VM>,
    mmtk: *const MMTK<Edges::VM>,
    check_mature_evac_remset: bool,
}

unsafe impl<E: ProcessEdgesWork> Send for ScanObjectsAndMarkLines<E> {}

impl<E: ProcessEdgesWork> ScanObjectsAndMarkLines<E> {
    pub fn new(
        buffer: Vec<ObjectReference>,
        concurrent: bool,
        immix: Option<&'static Immix<E::VM>>,
        immix_space: &'static ImmixSpace<E::VM>,
    ) -> Self {
        debug_assert!(!concurrent);
        if concurrent {
            crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        }
        Self {
            buffer,
            concurrent,
            immix,
            immix_space,
            edges: vec![],
            worker: ptr::null_mut(),
            mmtk: ptr::null_mut(),
            check_mature_evac_remset: crate::args::REF_COUNT
                && crate::args::RC_MATURE_EVACUATION
                && immix
                    .map(|ix| {
                        let pause = ix.current_pause();
                        pause == Some(Pause::FinalMark) || pause == Some(Pause::FullTraceFast)
                    })
                    .unwrap_or(false),
        }
    }

    const fn worker(&self) -> &mut GCWorker<E::VM> {
        unsafe { &mut *self.worker }
    }

    #[inline(always)]
    fn process_node(&mut self, o: ObjectReference) {
        let check_mature_evac_remset = self.check_mature_evac_remset;
        let mut should_add_to_mature_evac_remset = false;
        EdgeIterator::<E::VM>::iterate(o, |e| {
            if check_mature_evac_remset && !should_add_to_mature_evac_remset {
                let immix = unsafe { self.immix.unwrap_unchecked() };
                if !immix.in_defrag(o) && immix.in_defrag(unsafe { e.load() }) {
                    should_add_to_mature_evac_remset = true;
                }
            }
            let t = unsafe { e.load::<ObjectReference>() };
            if !t.is_null() {
                self.edges.push(e);
            }
        });
        if self.edges.len() >= E::CAPACITY {
            self.flush();
        }
        if should_add_to_mature_evac_remset {
            unsafe {
                self.worker()
                    .local::<ImmixCopyContext<E::VM>>()
                    .add_mature_evac_remset(o)
            }
        }
    }

    fn flush(&mut self) {
        if !self.edges.is_empty() {
            let mut new_edges = Vec::new();
            mem::swap(&mut new_edges, &mut self.edges);
            self.worker().add_work(
                WorkBucketStage::Closure,
                E::new(new_edges, false, unsafe { &*self.mmtk }),
            );
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ScanObjectsAndMarkLines<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("ScanObjectsAndMarkLines");
        self.mmtk = mmtk;
        self.worker = worker;
        let mut buffer = vec![];
        mem::swap(&mut buffer, &mut self.buffer);
        for object in buffer {
            self.process_node(object);
            if super::MARK_LINE_AT_SCAN_TIME
                && !super::BLOCK_ONLY
                && self.immix_space.in_space(object)
            {
                self.immix_space.mark_lines(object);
            }
        }
        self.flush();
    }
}

impl<E: ProcessEdgesWork> Drop for ScanObjectsAndMarkLines<E> {
    fn drop(&mut self) {
        if self.concurrent {
            crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

pub struct MatureSweeping;

impl<VM: VMBinding> GCWork<VM> for MatureSweeping {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        let immix_mut = unsafe { &mut *(immix as *const _ as *mut Immix<VM>) };
        immix_mut
            .immix_space
            .schedule_mature_sweeping(immix.current_pause().unwrap())
    }
}
