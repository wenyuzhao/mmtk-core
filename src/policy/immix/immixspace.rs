use super::block_allocation::BlockAllocation;
use super::line::*;
use super::{
    block::*,
    chunk::{Chunk, ChunkMap},
    defrag::Defrag,
};
use crate::plan::immix::Pause;
use crate::plan::lxr::{RemSet, LXR};
use crate::plan::ObjectsClosure;
use crate::plan::PlanConstraints;
use crate::policy::gc_work::TraceKind;
use crate::policy::immix::block_allocation::RCSweepNurseryBlocks;
use crate::policy::largeobjectspace::{RCReleaseMatureLOS, RCSweepMatureLOS};
use crate::policy::space::SpaceOptions;
use crate::policy::space::*;
use crate::policy::space::{CommonSpace, Space, SFT};
use crate::scheduler::ProcessEdgesWork;
use crate::util::copy::*;
use crate::util::heap::layout::heap_layout::{Mmapper, VMMap};
use crate::util::heap::HeapMeta;
use crate::util::heap::PageResource;
use crate::util::heap::VMRequest;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::*;
use crate::util::metadata::{
    self, compare_exchange_metadata, load_metadata, store_metadata, MetadataSpec,
};
use crate::util::object_forwarding as ForwardingWord;
use crate::util::rc;
use crate::util::{Address, ObjectReference};
use crate::{
    plan::ObjectQueue,
    scheduler::{GCWork, GCWorkScheduler, GCWorker, WorkBucketStage},
    util::opaque_pointer::{VMThread, VMWorkerThread},
    MMTK,
};
use crate::{vm::*, LazySweepingJobsCounter};
use atomic::Ordering;
use crossbeam::queue::SegQueue;
use spin::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic::AtomicU8, Arc};
use std::{mem, ptr};

pub static RELEASED_NURSERY_BLOCKS: AtomicUsize = AtomicUsize::new(0);
pub static RELEASED_BLOCKS: AtomicUsize = AtomicUsize::new(0);

// For 32-bit platforms, we still use FreeListPageResource
#[cfg(target_pointer_width = "32")]
use crate::util::heap::FreeListPageResource as ImmixPageResource;

// BlockPageResource is for 64-bit platforms only
#[cfg(target_pointer_width = "64")]
use crate::util::heap::BlockPageResource as ImmixPageResource;

pub(crate) const TRACE_KIND_FAST: TraceKind = 0;
pub(crate) const TRACE_KIND_DEFRAG: TraceKind = 1;

pub struct ImmixSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pub pr: ImmixPageResource<VM>,
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
    possibly_dead_mature_blocks: SegQueue<(Block, bool)>,
    initial_mark_pause: bool,
    pub mutator_recycled_blocks: SegQueue<Vec<Block>>,
    pub mature_evac_remsets: Mutex<Vec<Box<dyn GCWork<VM>>>>,
    pub last_defrag_blocks: Vec<Block>,
    defrag_blocks: Vec<Block>,
    num_defrag_blocks: AtomicUsize,
    #[allow(dead_code)]
    defrag_chunk_cursor: AtomicUsize,
    fragmented_blocks: SegQueue<Vec<(Block, usize)>>,
    fragmented_blocks_size: AtomicUsize,
    pub num_clean_blocks_released: AtomicUsize,
    pub num_clean_blocks_released_lazy: AtomicUsize,
    pub remset: RemSet,
    pub cm_enabled: bool,
    pub rc_enabled: bool,
}

unsafe impl<VM: VMBinding> Sync for ImmixSpace<VM> {}

impl<VM: VMBinding> SFT for ImmixSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }
    fn is_live(&self, object: ObjectReference) -> bool {
        if self.rc_enabled {
            return crate::util::rc::count(object) > 0
                || ForwardingWord::is_forwarded::<VM>(object);
        }
        if self.initial_mark_pause {
            return true;
        }
        if self.cm_enabled {
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
    #[inline(always)]
    fn get_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        debug_assert!(!object.is_null());
        if ForwardingWord::is_forwarded::<VM>(object) {
            Some(ForwardingWord::read_forwarding_pointer::<VM>(object))
        } else {
            None
        }
    }
    fn sft_trace_object(
        &self,
        _queue: &mut VectorObjectQueue,
        _object: ObjectReference,
        _worker: GCWorkerMutRef,
    ) -> ObjectReference {
        panic!("We do not use SFT to trace objects for Immix. sft_trace_object() cannot be used.")
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
    fn initialize_sft(&self) {
        self.common().initialize_sft(self.as_sft());
        // Initialize the block queues in `reusable_blocks` and `pr`.
        let me = unsafe { &mut *(self as *const Self as *mut Self) };
        me.block_allocation.init(unsafe { &*(self as *const Self) })
    }
    fn release_multiple_pages(&mut self, _start: Address) {
        panic!("immixspace only releases pages enmasse")
    }
    fn set_copy_for_sft_trace(&mut self, _semantics: Option<CopySemantics>) {
        panic!("We do not use SFT to trace objects for Immix. set_copy_context() cannot be used.")
    }
}

impl<VM: VMBinding> crate::policy::gc_work::PolicyTraceObject<VM> for ImmixSpace<VM> {
    #[inline(always)]
    fn trace_object<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        copy: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        if KIND == TRACE_KIND_DEFRAG {
            self.trace_object(queue, object, copy.unwrap(), worker)
        } else if KIND == TRACE_KIND_FAST {
            self.fast_trace_object(queue, object)
        } else {
            unreachable!()
        }
    }

    #[inline(always)]
    fn post_scan_object(&self, object: ObjectReference) {
        if super::MARK_LINE_AT_SCAN_TIME && !super::BLOCK_ONLY {
            debug_assert!(self.in_space(object));
            self.mark_lines(object);
        }
    }

    #[inline(always)]
    fn may_move_objects<const KIND: TraceKind>() -> bool {
        if KIND == TRACE_KIND_DEFRAG {
            true
        } else if KIND == TRACE_KIND_FAST {
            false
        } else {
            unreachable!()
        }
    }
}

impl<VM: VMBinding> ImmixSpace<VM> {
    const UNMARKED_STATE: u8 = 0;
    const MARKED_STATE: u8 = 1;

    /// Get side metadata specs
    fn side_metadata_specs(rc_enabled: bool) -> Vec<SideMetadataSpec> {
        if rc_enabled {
            return metadata::extract_side_metadata(&vec![
                MetadataSpec::OnSide(Block::DEFRAG_STATE_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                MetadataSpec::OnSide(crate::util::rc::RC_STRADDLE_LINES),
                MetadataSpec::OnSide(Block::LOG_TABLE),
                MetadataSpec::OnSide(Block::DEAD_WORDS),
                MetadataSpec::OnSide(Line::VALIDITY_STATE),
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
        rc_enabled: bool,
    ) -> Self {
        super::validate_features();
        let common = CommonSpace::new(
            SpaceOptions {
                name,
                movable: true,
                immortal: false,
                zeroed: true,
                vmrequest: VMRequest::discontiguous(),
                side_metadata_specs: SideMetadataContext {
                    global: global_side_metadata_specs,
                    local: Self::side_metadata_specs(rc_enabled),
                },
                needs_log_bit: constraints.needs_log_bit,
                needs_field_log_bit: constraints.needs_field_log_bit,
            },
            vm_map,
            mmapper,
            heap,
        );
        println!("Workers: {}", scheduler.num_workers());
        ImmixSpace {
            #[cfg(target_pointer_width = "32")]
            pr: if common.vmrequest.is_discontiguous() {
                ImmixPageResource::new_discontiguous(0, vm_map)
            } else {
                ImmixPageResource::new_contiguous(common.start, common.extent, 0, vm_map)
            },
            #[cfg(target_pointer_width = "64")]
            pr: ImmixPageResource::new_contiguous(
                Block::LOG_PAGES,
                common.start,
                common.extent,
                vm_map,
                scheduler.num_workers(),
            ),
            common,
            chunk_map: ChunkMap::new(),
            line_mark_state: AtomicU8::new(Line::RESET_MARK_STATE),
            line_unavail_state: AtomicU8::new(Line::RESET_MARK_STATE),
            reusable_blocks: BlockList::new(scheduler.num_workers()),
            defrag: Defrag::default(),
            mark_state: Self::UNMARKED_STATE,
            scheduler,
            block_allocation: BlockAllocation::new(),
            possibly_dead_mature_blocks: Default::default(),
            initial_mark_pause: false,
            mutator_recycled_blocks: Default::default(),
            mature_evac_remsets: Default::default(),
            num_defrag_blocks: AtomicUsize::new(0),
            defrag_chunk_cursor: AtomicUsize::new(0),
            defrag_blocks: Default::default(),
            last_defrag_blocks: Default::default(),
            fragmented_blocks: Default::default(),
            fragmented_blocks_size: Default::default(),
            num_clean_blocks_released: Default::default(),
            num_clean_blocks_released_lazy: Default::default(),
            remset: RemSet::new(),
            cm_enabled: false,
            rc_enabled,
        }
    }

    /// Flush the thread-local queues in BlockPageResource
    pub fn flush_page_resource(&self) {
        self.reusable_blocks.flush_all();
        #[cfg(target_pointer_width = "64")]
        self.pr.flush_all()
    }

    /// Get the number of defrag headroom pages.
    pub fn defrag_headroom_pages(&self) -> usize {
        self.defrag.defrag_headroom_pages(self)
    }

    pub fn num_defrag_blocks(&self) -> usize {
        self.num_defrag_blocks.load(Ordering::SeqCst)
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
            self.cm_enabled,
            self.rc_enabled,
        );
        self.defrag.in_defrag()
    }

    /// Get work packet scheduler
    #[inline(always)]
    pub fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    fn select_mature_evacuation_candidates(&self, _pause: Pause, _total_pages: usize) {
        let me = unsafe { &mut *(self as *const Self as *mut Self) };
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        // Select mature defrag blocks
        // let available_clean_pages_for_defrag = VM::VMActivePlan::global().get_total_pages()
        //     + self.defrag_headroom_pages()
        //     - VM::VMActivePlan::global().get_pages_reserved();
        // let defrag_bytes = available_clean_pages_for_defrag << 12;
        let defrag_bytes = self.defrag_headroom_pages() << 12;
        let mut blocks = Vec::with_capacity(self.fragmented_blocks_size.load(Ordering::SeqCst));
        while let Some(mut x) = self.fragmented_blocks.pop() {
            blocks.append(&mut x);
        }
        let mut live_bytes = 0usize;
        let mut num_blocks = 0usize;
        blocks.sort_by_key(|x| x.1);
        while let Some((block, dead_bytes)) = blocks.pop() {
            if block.is_defrag_source()
                || block.get_state() == BlockState::Unallocated
                || block.get_state() == BlockState::Nursery
            {
                // println!(" - skip defrag {:?} {:?}", block, block.get_state());
                continue;
            }
            block.set_as_defrag_source(true);
            // println!(
            //     " - defrag {:?} {:?} {}",
            //     block,
            //     block.get_state(),
            //     block.dead_bytes()
            // );
            me.defrag_blocks.push(block);
            live_bytes += (Block::BYTES - dead_bytes) >> 1;
            num_blocks += 1;
            if live_bytes >= defrag_bytes {
                break;
            }
        }
        if crate::args::LOG_PER_GC_STATE {
            println!(
                " - Defrag {} mature bytes ({} blocks)",
                live_bytes, num_blocks
            );
        }
        self.num_defrag_blocks.store(num_blocks, Ordering::SeqCst);
    }

    fn schedule_defrag_selection_packets(&self, _pause: Pause) {
        let tasks = self.chunk_map.generate_tasks(|chunk| {
            Box::new(SelectDefragBlocksInChunk {
                chunk,
                defrag_threshold: 1,
            })
        });
        self.fragmented_blocks_size.store(0, Ordering::SeqCst);
        SELECT_DEFRAG_BLOCK_JOB_COUNTER.store(tasks.len(), Ordering::SeqCst);
        self.scheduler().work_buckets[WorkBucketStage::FinishConcurrentWork].bulk_add(tasks);
    }

    pub fn rc_eager_prepare(&mut self, pause: Pause) {
        if pause == Pause::FullTraceFast || pause == Pause::InitialMark {
            self.schedule_defrag_selection_packets(pause);
        }
        let num_workers = self.scheduler().worker_group.worker_count();
        // let (stw_packets, delayed_packets, nursery_blocks) =
        //     if crate::args::LOCK_FREE_BLOCK_ALLOCATION {
        //         self.block_allocation
        //             .reset_and_generate_nursery_sweep_tasks(num_workers)
        //     } else {
        //         unreachable!();
        //     };
        let (stw_packets, nursery_blocks) = self
            .block_allocation
            .reset_and_generate_nursery_sweep_tasks(num_workers);
        // If there are not too much nursery blocks for release, we
        // reclain mature blocks as well.
        if crate::args::NO_LAZY_SWEEP_WHEN_STW_CANNOT_RELEASE_ENOUGH_MEMORY {
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
        }
        if pause == Pause::FinalMark {
            self.scheduler().work_buckets[WorkBucketStage::RCEvacuateMature].bulk_add(stw_packets);
        } else {
            self.scheduler().work_buckets[WorkBucketStage::RCReleaseNursery].bulk_add(stw_packets);
        }
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

    pub fn schedule_mark_table_zeroing_tasks(&self, stage: WorkBucketStage) {
        assert!(crate::args::HEAP_HEALTH_GUIDED_GC);
        let space = unsafe { &mut *(self as *const Self as *mut Self) };
        let work_packets = self
            .chunk_map
            .generate_concurrent_mark_table_zeroing_tasks::<VM>(space);
        self.scheduler().work_buckets[stage].bulk_add(work_packets);
    }

    pub fn prepare_rc(&mut self, pause: Pause) {
        self.num_clean_blocks_released.store(0, Ordering::SeqCst);
        self.num_clean_blocks_released_lazy
            .store(0, Ordering::SeqCst);
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            debug_assert!(self.last_defrag_blocks.is_empty());
            std::mem::swap(&mut self.defrag_blocks, &mut self.last_defrag_blocks);
        }
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
        // SATB sweep has problem scanning mutator recycled blocks.
        // Remaing the block state as "reusing" and reset them here.
        let space = unsafe { &mut *(self as *mut Self) };
        if pause == Pause::FullTraceFast {
            while let Some(blocks) = self.mutator_recycled_blocks.pop() {
                for b in blocks {
                    b.set_state(BlockState::Marked);
                }
            }
        } else {
            let mut packets: Vec<Box<dyn GCWork<VM>>> = vec![];
            packets.reserve(self.mutator_recycled_blocks.len());
            while let Some(blocks) = self.mutator_recycled_blocks.pop() {
                if !blocks.is_empty() {
                    for chunk in blocks.chunks(256) {
                        packets.push(Box::new(RCSweepNurseryBlocks {
                            space,
                            blocks: chunk.to_vec(),
                            mutator_reused_blocks: true,
                        }));
                    }
                }
            }
            if crate::args::LAZY_MU_REUSE_BLOCK_SWEEPING {
                self.scheduler().postpone_all_prioritized(packets);
            } else {
                if pause == Pause::FinalMark {
                    self.scheduler().work_buckets[WorkBucketStage::RCEvacuateMature]
                        .bulk_add(packets);
                } else {
                    self.scheduler().work_buckets[WorkBucketStage::RCReleaseNursery]
                        .bulk_add(packets);
                }
            }
        }
        if pause == Pause::FinalMark {
            crate::REMSET_RECORDING.store(false, Ordering::SeqCst);
        }
    }

    pub fn release_rc(&mut self, pause: Pause) {
        debug_assert_ne!(pause, Pause::FullTraceDefrag);
        self.block_allocation.reset();
        let disable_lasy_dec_for_current_gc = crate::disable_lasy_dec_for_current_gc();
        if disable_lasy_dec_for_current_gc {
            self.scheduler().process_lazy_decrement_packets();
        }
        rc::reset_inc_buffer_size();
    }

    pub fn schedule_mature_sweeping(&mut self, pause: Pause) {
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            if self.last_defrag_blocks.len() > 0 {
                while let Some(block) = self.last_defrag_blocks.pop() {
                    if !block.is_defrag_source() || block.get_state() == BlockState::Unallocated {
                        continue;
                    }
                    block.clear_rc_table::<VM>();
                    block.clear_striddle_table::<VM>();
                    block.rc_sweep_mature::<VM>(self, true);
                    assert!(!block.is_defrag_source());
                }
            }
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
        }
    }

    pub fn prepare(&mut self, major_gc: bool, initial_mark_pause: bool) {
        self.initial_mark_pause = initial_mark_pause;
        debug_assert!(!self.rc_enabled);
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
        debug_assert!(!self.rc_enabled);
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
    pub fn release_block(&self, block: Block, nursery: bool, zero_unlog_table: bool) {
        // println!(
        //     "Release {:?} nursery={} defrag={}",
        //     block,
        //     nursery,
        //     block.is_defrag_source()
        // );
        if crate::args::LOG_PER_GC_STATE {
            if nursery {
                RELEASED_NURSERY_BLOCKS.fetch_add(1, Ordering::SeqCst);
            }
            RELEASED_BLOCKS.fetch_add(1, Ordering::SeqCst);
        }
        if crate::args::BARRIER_MEASUREMENT || zero_unlog_table {
            block.clear_log_table::<VM>();
        }
        self.num_clean_blocks_released
            .fetch_add(1, Ordering::Relaxed);
        block.deinit(self);
        crate::stat(|s| {
            if nursery {
                s.reclaimed_blocks_nursery += 1;
            } else {
                s.reclaimed_blocks_mature += 1;
            }
        });
        self.pr.release_pages(block.start());
    }

    /// Allocate a clean block.
    #[inline(always)]
    pub fn get_clean_block(&self, tls: VMThread, copy: bool) -> Option<Block> {
        self.block_allocation
            .get_clean_block(tls, copy, self.rc_enabled)
    }

    /// Pop a reusable block from the reusable block list.
    #[inline(always)]
    pub fn get_reusable_block(&self, copy: bool) -> Option<Block> {
        self.block_allocation.get_reusable_block(copy)
    }

    #[inline(always)]
    pub fn reusable_blocks_drained(&self) -> bool {
        self.reusable_blocks.len() == 0
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
        trace: &mut impl ObjectQueue,
        object: ObjectReference,
    ) -> ObjectReference {
        self.trace_object_without_moving(trace, object)
    }

    /// Trace and mark objects. If the current object is in defrag block, then do evacuation as well.
    #[inline(always)]
    pub fn trace_object(
        &self,
        trace: &mut impl ObjectQueue,
        object: ObjectReference,
        semantics: CopySemantics,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        #[cfg(feature = "global_alloc_bit")]
        debug_assert!(
            crate::util::alloc_bit::is_alloced(object),
            "{:x}: alloc bit not set",
            object
        );
        if Block::containing::<VM>(object).is_defrag_source() {
            debug_assert!(self.in_defrag());
            self.trace_object_with_opportunistic_copy(trace, object, semantics, worker)
        } else {
            self.trace_object_without_moving(trace, object)
        }
    }

    /// Trace and mark objects without evacuation.
    #[inline(always)]
    pub fn trace_object_without_moving(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
    ) -> ObjectReference {
        if self.attempt_mark(object) {
            if self.rc_enabled {
                let straddle = rc::is_straddle_line(Line::from(Line::align(object.to_address())));
                if straddle {
                    return object;
                }
            }
            // println!("Mark {:?}", object.range::<VM>());
            if !self.rc_enabled {
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
            queue.enqueue(object);
        }
        object
    }

    /// Trace object and do evacuation if required.
    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    pub fn trace_object_with_opportunistic_copy(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
        semantics: CopySemantics,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        let copy_context = worker.get_copy_context_mut();
        debug_assert!(!super::BLOCK_ONLY);
        let forwarding_status = ForwardingWord::attempt_to_forward::<VM>(object);
        if ForwardingWord::state_is_forwarded_or_being_forwarded(forwarding_status) {
            // We lost the forwarding race as some other thread has set the forwarding word; wait
            // until the object has been forwarded by the winner. Note that the object may not
            // necessarily get forwarded since Immix opportunistically moves objects.
            #[allow(clippy::let_and_return)]
            let new_object =
                ForwardingWord::spin_and_get_forwarded_object::<VM>(object, forwarding_status);
            #[cfg(debug_assertions)]
            {
                if new_object == object {
                    debug_assert!(
                        self.is_marked(object) || self.defrag.space_exhausted() || Self::is_pinned(object),
                        "Forwarded object is the same as original object {} even though it should have been copied",
                        object,
                    );
                } else {
                    // new_object != object
                    debug_assert!(
                        !Block::containing::<VM>(new_object).is_defrag_source(),
                        "Block {:?} containing forwarded object {} should not be a defragmentation source",
                        Block::containing::<VM>(new_object),
                        new_object,
                    );
                }
            }
            new_object
        } else if self.is_marked(object) {
            // We won the forwarding race but the object is already marked so we clear the
            // forwarding status and return the unmoved object
            debug_assert!(
                self.defrag.space_exhausted() || Self::is_pinned(object),
                "Forwarded object is the same as original object {} even though it should have been copied",
                object,
            );
            ForwardingWord::clear_forwarding_bits::<VM>(object);
            object
        } else {
            // We won the forwarding race; actually forward and copy the object if it is not pinned
            // and we have sufficient space in our copy allocator
            let new_object = if Self::is_pinned(object) || self.defrag.space_exhausted() {
                self.attempt_mark(object);
                ForwardingWord::clear_forwarding_bits::<VM>(object);
                Block::containing::<VM>(object).set_state(BlockState::Marked);
                object
            } else {
                #[cfg(feature = "global_alloc_bit")]
                crate::util::alloc_bit::unset_alloc_bit(object);
                ForwardingWord::forward_object::<VM>(object, semantics, copy_context)
            };
            debug_assert!({
                let state = Block::containing::<VM>(new_object).get_state();
                state == BlockState::Marked || state == BlockState::Nursery
            });
            queue.enqueue(new_object);
            debug_assert!(new_object.is_live());
            new_object
        }
    }

    #[inline(always)]
    pub fn rc_trace_object(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
        semantics: CopySemantics,
        pause: Pause,
        mark: bool,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        debug_assert!(self.rc_enabled);
        if crate::args::RC_MATURE_EVACUATION && Block::containing::<VM>(object).is_defrag_source() {
            self.trace_forward_rc_mature_object(queue, object, semantics, pause, worker)
        } else if crate::args::RC_MATURE_EVACUATION {
            self.trace_mark_rc_mature_object(queue, object, pause, mark)
        } else {
            self.trace_object_without_moving(queue, object)
        }
    }

    #[inline(always)]
    pub fn trace_mark_rc_mature_object(
        &self,
        queue: &mut impl ObjectQueue,
        mut object: ObjectReference,
        _pause: Pause,
        mark: bool,
    ) -> ObjectReference {
        if ForwardingWord::is_forwarded::<VM>(object) {
            object = ForwardingWord::read_forwarding_pointer::<VM>(object);
        }
        if mark && self.attempt_mark(object) {
            queue.enqueue(object);
        }
        object
    }

    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    pub fn trace_forward_rc_mature_object(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
        _semantics: CopySemantics,
        _pause: Pause,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        let copy_context = worker.get_copy_context_mut();
        let forwarding_status = ForwardingWord::attempt_to_forward::<VM>(object);
        if ForwardingWord::state_is_forwarded_or_being_forwarded(forwarding_status) {
            let new =
                ForwardingWord::spin_and_get_forwarded_object::<VM>(object, forwarding_status);
            new
        } else {
            // Evacuate the mature object
            let new = ForwardingWord::forward_object::<VM>(
                object,
                CopySemantics::DefaultCopy,
                copy_context,
            );
            crate::stat(|s| {
                s.mature_copy_objects += 1usize;
                s.mature_copy_volume += new.get_size::<VM>();
            });
            if crate::should_record_copy_bytes() {
                unsafe { crate::SLOPPY_COPY_BYTES += new.get_size::<VM>() }
            }
            // Transfer RC count
            new.log_start_address::<VM>();
            if !crate::args::BLOCK_ONLY && new.get_size::<VM>() > Line::BYTES {
                rc::mark_straddle_object::<VM>(new);
            }
            rc::set(new, rc::count(object));
            self.attempt_mark(new);
            self.unmark(object);
            queue.enqueue(new);
            new
        }
    }

    /// Mark all the lines that the given object spans.
    #[allow(clippy::assertions_on_constants)]
    #[inline]
    pub fn mark_lines(&self, object: ObjectReference) {
        debug_assert!(!super::BLOCK_ONLY);
        if self.rc_enabled {
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
    /// hole, starting from the given line. If we find available lines,
    /// return a tuple of the start line and the end line (non-inclusive).
    ///
    /// Returns None if the search could not find any more holes.
    #[allow(clippy::assertions_on_constants)]
    pub fn get_next_available_lines(&self, copy: bool, search_start: Line) -> Option<(Line, Line)> {
        debug_assert!(!super::BLOCK_ONLY);
        if self.rc_enabled {
            self.rc_get_next_available_lines(copy, search_start)
        } else {
            self.normal_get_next_available_lines(search_start)
        }
    }

    /// Search holes by ref-counts instead of line marks
    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    pub fn rc_get_next_available_lines(
        &self,
        copy: bool,
        search_start: Line,
    ) -> Option<(Line, Line)> {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(self.rc_enabled);
        let block = search_start.block();
        let rc_array = RCArray::of(block);
        let limit = Block::LINES;
        // Find start
        let first_free_cursor = {
            let start_cursor = search_start.get_index_within_block();
            let mut first_free_cursor = None;
            let mut find_free_line = false;
            for i in start_cursor..limit {
                if rc_array.is_dead(i) {
                    if i == 0 {
                        first_free_cursor = Some(i);
                        break;
                    } else if !find_free_line {
                        find_free_line = true;
                    } else {
                        first_free_cursor = Some(i);
                        break;
                    }
                } else {
                    find_free_line = false;
                }
            }
            first_free_cursor
        };
        let start = match first_free_cursor {
            Some(c) => c,
            _ => return None,
        };
        // Find limit
        let end = {
            let mut cursor = start + 1;
            while cursor < limit {
                if !rc_array.is_dead(cursor) {
                    break;
                }
                cursor += 1;
            }
            cursor
        };
        let start = Line::from(block.start() + (start << Line::LOG_BYTES));
        let end = Line::from(block.start() + (end << Line::LOG_BYTES));
        if self.common.needs_log_bit {
            if !copy {
                Line::clear_log_table::<VM>(start..end);
            } else {
                Line::initialize_log_table_as_unlogged::<VM>(start..end);
            }
            Line::update_validity(RegionIterator::<Line>::new(start, end));
        }
        block.dec_dead_bytes_sloppy(Line::steps_between(&start, &end).unwrap() << Line::LOG_BYTES);
        // Line::clear_mark_table::<VM>(start..end);
        // if !_copy {
        //     println!("reuse {:?} copy={}", start..end, copy);
        // }
        Some((start, end))
    }

    #[allow(clippy::assertions_on_constants)]
    #[inline]
    pub fn normal_get_next_available_lines(&self, search_start: Line) -> Option<(Line, Line)> {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(!self.rc_enabled);
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
        let start = search_start.next_nth(cursor - start_cursor);
        // Find limit
        while cursor < Block::LINES {
            let mark = mark_data.get(cursor);
            if mark == unavail_state || mark == current_state {
                break;
            }
            if self.cm_enabled {
                mark_data.set(cursor, current_state);
            }
            cursor += 1;
        }
        let end = search_start.next_nth(cursor - start_cursor);
        if self.common.needs_log_bit {
            Line::clear_log_table::<VM>(start..end);
        }
        Some((start, end))
    }

    pub fn is_last_gc_exhaustive(did_defrag_for_last_gc: bool) -> bool {
        if super::DEFRAG {
            did_defrag_for_last_gc
        } else {
            // If defrag is disabled, every GC is exhaustive.
            true
        }
    }

    #[inline(always)]
    pub fn add_to_possibly_dead_mature_blocks(&self, block: Block, is_defrag_source: bool) {
        if block.log() {
            self.possibly_dead_mature_blocks
                .push((block, is_defrag_source));
        }
    }

    pub fn schedule_rc_block_sweeping_tasks(&self, counter: LazySweepingJobsCounter) {
        // while let Some(x) = self.last_mutator_recycled_blocks.pop() {
        //     x.set_state(BlockState::Marked);
        // }
        // This may happen either within a pause, or in concurrent.
        let size = self.possibly_dead_mature_blocks.len();
        let num_bins = self.scheduler().num_workers() << 1;
        let bin_cap = size / num_bins + if size % num_bins == 0 { 0 } else { 1 };
        let mut bins = (0..num_bins)
            .map(|_| Vec::with_capacity(bin_cap))
            .collect::<Vec<Vec<(Block, bool)>>>();
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
                Box::new(SweepBlocksAfterDecs::new(blocks, counter.clone()))
            })
            .collect();
        self.scheduler().work_buckets[WorkBucketStage::Unconstrained].bulk_add_prioritized(packets);
        self.scheduler().work_buckets[WorkBucketStage::Unconstrained]
            .add_prioritized(Box::new(RCReleaseMatureLOS::new(counter.clone())));
    }
}

/// A work packet to scan the fields of each objects and mark lines.
pub struct ScanObjectsAndMarkLines<Edges: ProcessEdgesWork> {
    buffer: Vec<ObjectReference>,
    concurrent: bool,
    immix_space: &'static ImmixSpace<Edges::VM>,
    edges: Vec<Address>,
    worker: *mut GCWorker<Edges::VM>,
    mmtk: *const MMTK<Edges::VM>,
}

unsafe impl<E: ProcessEdgesWork> Send for ScanObjectsAndMarkLines<E> {}

impl<E: ProcessEdgesWork> ScanObjectsAndMarkLines<E> {
    pub fn new(
        buffer: Vec<ObjectReference>,
        concurrent: bool,
        immix_space: &'static ImmixSpace<E::VM>,
    ) -> Self {
        debug_assert!(!concurrent);
        if concurrent {
            crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        }
        Self {
            buffer,
            concurrent,
            immix_space,
            edges: vec![],
            worker: ptr::null_mut(),
            mmtk: ptr::null_mut(),
        }
    }

    fn worker(&self) -> &mut GCWorker<E::VM> {
        unsafe { &mut *self.worker }
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
        let tls = worker.tls;
        let mut closure = ObjectsClosure::<E>::new(worker);
        for object in buffer {
            <E::VM as VMBinding>::VMScanning::scan_object(tls, object, &mut closure);
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
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        let lxr_mut = unsafe { &mut *(lxr as *const _ as *mut LXR<VM>) };
        lxr_mut
            .immix_space
            .schedule_mature_sweeping(lxr.current_pause().unwrap())
    }
}

static SELECT_DEFRAG_BLOCK_JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct SelectDefragBlocksInChunk {
    chunk: Chunk,
    #[allow(unused)]
    defrag_threshold: usize,
}

impl<VM: VMBinding> GCWork<VM> for SelectDefragBlocksInChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut blocks = vec![];
        // Iterate over all blocks in this chunk
        for block in self.chunk.committed_blocks() {
            let state = block.get_state();
            // Skip unallocated blocks.
            if state == BlockState::Unallocated
                || state == BlockState::Nursery
                || block.is_defrag_source()
            {
                continue;
            }
            let score = if crate::args::HOLE_COUNTING {
                unreachable!();
                // match state {
                //     BlockState::Reusable { unavailable_lines } => unavailable_lines as _,
                //     _ => block.calc_holes(),
                // }
            } else {
                // block.dead_bytes()
                // block.calc_dead_bytes::<VM>()
                block.calc_dead_lines() << Line::LOG_BYTES
            };
            if score >= (Block::BYTES >> 1) {
                blocks.push((block, score));
            }
        }
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        lxr.immix_space
            .fragmented_blocks_size
            .fetch_add(blocks.len(), Ordering::SeqCst);
        lxr.immix_space.fragmented_blocks.push(blocks);
        if SELECT_DEFRAG_BLOCK_JOB_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
            lxr.immix_space.select_mature_evacuation_candidates(
                lxr.current_pause().unwrap(),
                mmtk.plan.get_total_pages(),
            )
        }
    }
}

pub struct UpdateWeakProcessor;

impl<VM: VMBinding> GCWork<VM> for UpdateWeakProcessor {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        VM::VMCollection::update_weak_processor();
    }
}

use crate::plan::{Plan, VectorObjectQueue};
use crate::policy::copy_context::PolicyCopyContext;
use crate::util::alloc::Allocator;
use crate::util::alloc::ImmixAllocator;

/// Immix copy allocator
pub struct ImmixCopyContext<VM: VMBinding> {
    copy_allocator: ImmixAllocator<VM>,
    defrag_allocator: ImmixAllocator<VM>,
}

impl<VM: VMBinding> PolicyCopyContext for ImmixCopyContext<VM> {
    type VM = VM;

    fn prepare(&mut self) {
        self.copy_allocator.reset();
        self.defrag_allocator.reset();
    }
    fn release(&mut self) {
        self.copy_allocator.reset();
        self.defrag_allocator.reset();
    }
    #[inline(always)]
    fn alloc_copy(
        &mut self,
        _original: ObjectReference,
        bytes: usize,
        align: usize,
        offset: isize,
    ) -> Address {
        if self.get_space().in_defrag() {
            self.defrag_allocator.alloc(bytes, align, offset)
        } else {
            self.copy_allocator.alloc(bytes, align, offset)
        }
    }
    #[inline(always)]
    fn post_copy(&mut self, obj: ObjectReference, _bytes: usize) {
        let space = self.get_space();
        if space.rc_enabled {
            return;
        }
        // Mark the object
        store_metadata::<VM>(
            &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
            obj,
            space.mark_state as usize,
            None,
            Some(Ordering::SeqCst),
        );
        // Mark the line
        if !super::MARK_LINE_AT_SCAN_TIME {
            space.mark_lines(obj);
        }
    }
}

impl<VM: VMBinding> ImmixCopyContext<VM> {
    pub fn new(
        tls: VMWorkerThread,
        plan: &'static dyn Plan<VM = VM>,
        space: &'static ImmixSpace<VM>,
    ) -> Self {
        ImmixCopyContext {
            copy_allocator: ImmixAllocator::new(tls.0, Some(space), plan, true),
            defrag_allocator: ImmixAllocator::new(tls.0, Some(space), plan, true),
        }
    }

    #[inline(always)]
    fn get_space(&self) -> &ImmixSpace<VM> {
        // Both copy allocators should point to the same space.
        debug_assert_eq!(
            self.defrag_allocator.immix_space().common().descriptor,
            self.copy_allocator.immix_space().common().descriptor
        );
        // Just get the space from either allocator
        self.defrag_allocator.immix_space()
    }
}

pub struct SweepBlocksAfterDecs {
    blocks: Vec<(Block, bool)>,
    _counter: LazySweepingJobsCounter,
}

impl SweepBlocksAfterDecs {
    pub fn new(blocks: Vec<(Block, bool)>, counter: LazySweepingJobsCounter) -> Self {
        Self {
            blocks,
            _counter: counter,
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for SweepBlocksAfterDecs {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        if self.blocks.is_empty() {
            return;
        }
        let mut count = 0;
        for (block, defrag) in &self.blocks {
            block.unlog();
            if block.rc_sweep_mature::<VM>(&lxr.immix_space, *defrag) {
                count += 1;
            } else {
                assert!(
                    !*defrag,
                    "defrag block is freed? {:?} {:?} {}",
                    block,
                    block.get_state(),
                    block.is_defrag_source()
                );
            }
        }
        if count != 0 && lxr.current_pause().is_none() {
            lxr.immix_space
                .num_clean_blocks_released_lazy
                .fetch_add(count, Ordering::Relaxed);
        }
    }
}
