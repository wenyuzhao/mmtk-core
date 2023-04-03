use super::block_allocation::BlockAllocation;
use super::line::*;
use super::rc_work::*;
use super::{block::*, defrag::Defrag};
use crate::plan::immix::Pause;
use crate::plan::lxr::RemSet;
use crate::policy::gc_work::TraceKind;
use crate::policy::largeobjectspace::{RCReleaseMatureLOS, RCSweepMatureLOS};
use crate::policy::sft::GCWorkerMutRef;
use crate::policy::sft::SFT;
use crate::policy::space::{CommonSpace, Space};
use crate::util::copy::*;
use crate::util::heap::chunk_map::*;
use crate::util::heap::BlockPageResource;
use crate::util::heap::PageResource;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::*;
use crate::util::metadata::{self, MetadataSpec};
use crate::util::object_forwarding as ForwardingWord;
use crate::util::options::Options;
use crate::util::rc::RefCountHelper;
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
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic::AtomicU8, Arc};

pub static RELEASED_NURSERY_BLOCKS: AtomicUsize = AtomicUsize::new(0);
pub static RELEASED_BLOCKS: AtomicUsize = AtomicUsize::new(0);

pub(crate) const TRACE_KIND_FAST: TraceKind = 0;
pub(crate) const TRACE_KIND_DEFRAG: TraceKind = 1;

pub struct ImmixSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pub pr: BlockPageResource<VM, Block>,
    /// Allocation status for all chunks in immix space
    pub chunk_map: ChunkMap,
    /// Current line mark state
    pub line_mark_state: AtomicU8,
    /// Line mark state in previous GC
    line_unavail_state: AtomicU8,
    /// A list of all reusable blocks
    pub reusable_blocks: ReusableBlockPool,
    /// Defrag utilities
    pub(super) defrag: Defrag,
    /// Object mark state
    mark_state: u8,
    /// Work packet scheduler
    scheduler: Arc<GCWorkScheduler<VM>>,
    pub block_allocation: BlockAllocation<VM>,
    possibly_dead_mature_blocks: SegQueue<(Block, bool)>,
    initial_mark_pause: bool,
    pub mutator_recycled_blocks: Mutex<Vec<Block>>,
    pub mature_evac_remsets: Mutex<Vec<Box<dyn GCWork<VM>>>>,
    pub num_clean_blocks_released: AtomicUsize,
    pub num_clean_blocks_released_lazy: AtomicUsize,
    pub remset: RemSet<VM>,
    pub cm_enabled: bool,
    pub rc_enabled: bool,
    pub is_end_of_satb_or_full_gc: bool,
    pub rc: RefCountHelper<VM>,
    pub(super) options: Arc<Options>,
    pub(super) evac_set: MatureEvacuationSet,
}

unsafe impl<VM: VMBinding> Sync for ImmixSpace<VM> {}

impl<VM: VMBinding> SFT for ImmixSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }

    fn get_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        debug_assert!(!object.is_null());
        if ForwardingWord::is_forwarded::<VM>(object) {
            Some(ForwardingWord::read_forwarding_pointer::<VM>(object))
        } else {
            None
        }
    }

    fn is_live(&self, object: ObjectReference) -> bool {
        if self.rc_enabled {
            if self.is_end_of_satb_or_full_gc {
                if self.is_marked(object) {
                    let block = Block::containing::<VM>(object);
                    if block.is_defrag_source() {
                        if ForwardingWord::is_forwarded::<VM>(object) {
                            let forwarded = ForwardingWord::read_forwarding_pointer::<VM>(object);
                            return self.is_marked(forwarded) && self.rc.count(forwarded) > 0;
                        } else {
                            return false;
                        }
                    }
                    return self.rc.count(object) > 0;
                } else if ForwardingWord::is_forwarded::<VM>(object) {
                    let forwarded = ForwardingWord::read_forwarding_pointer::<VM>(object);
                    return self.is_marked(forwarded) && self.rc.count(forwarded) > 0;
                } else {
                    return false;
                }
            }
            return self.rc.count(object) > 0 || ForwardingWord::is_forwarded::<VM>(object);
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
    fn is_reachable(&self, object: ObjectReference) -> bool {
        if self.rc_enabled {
            if ForwardingWord::is_forwarded::<VM>(object) {
                let forwarded = ForwardingWord::read_forwarding_pointer::<VM>(object);
                return self.is_marked(forwarded) && self.rc.count(forwarded) > 0;
            }
            return self.is_marked(object) && self.rc.count(object) > 0;
        } else {
            self.is_live(object)
        }
    }
    #[cfg(feature = "object_pinning")]
    fn pin_object(&self, object: ObjectReference) -> bool {
        VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC.pin_object::<VM>(object)
    }
    #[cfg(feature = "object_pinning")]
    fn unpin_object(&self, object: ObjectReference) -> bool {
        VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC.unpin_object::<VM>(object)
    }
    #[cfg(feature = "object_pinning")]
    fn is_object_pinned(&self, object: ObjectReference) -> bool {
        VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC.is_object_pinned::<VM>(object)
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
        crate::util::alloc_bit::set_alloc_bit::<VM>(_object);
    }
    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> bool {
        crate::util::alloc_bit::is_alloced_object::<VM>(addr).is_some()
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
    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        &self.pr
    }
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

    fn post_scan_object(&self, object: ObjectReference) {
        if super::MARK_LINE_AT_SCAN_TIME && !super::BLOCK_ONLY {
            debug_assert!(self.in_space(object));
            self.mark_lines(object);
        }
    }

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
                MetadataSpec::OnSide(Block::NURSERY_PROMOTION_STATE_TABLE),
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
                *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC,
                *VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC,
                #[cfg(feature = "object_pinning")]
                *VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC,
            ]
        } else {
            vec![
                MetadataSpec::OnSide(Line::MARK_TABLE),
                MetadataSpec::OnSide(Block::DEFRAG_STATE_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC,
                *VM::VMObjectModel::LOCAL_FORWARDING_POINTER_SPEC,
                #[cfg(feature = "object_pinning")]
                *VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC,
            ]
        })
    }

    pub fn new(args: crate::policy::space::PlanCreateSpaceArgs<VM>) -> Self {
        #[cfg(feature = "immix_no_defrag")]
        info!(
            "Creating non-moving ImmixSpace: {}. Block size: 2^{}",
            args.name,
            Block::LOG_BYTES
        );

        super::validate_features();
        let vm_map = args.vm_map;
        let scheduler = args.scheduler.clone();
        let options = args.options.clone();
        let rc_enabled = args.constraints.rc_enabled;
        let common = CommonSpace::new(args.into_policy_args(
            true,
            false,
            Self::side_metadata_specs(rc_enabled),
        ));
        ImmixSpace {
            pr: if common.vmrequest.is_discontiguous() {
                BlockPageResource::new_discontiguous(
                    Block::LOG_PAGES,
                    vm_map,
                    scheduler.num_workers(),
                )
            } else {
                BlockPageResource::new_contiguous(
                    Block::LOG_PAGES,
                    common.start,
                    common.extent,
                    vm_map,
                    scheduler.num_workers(),
                )
            },
            common,
            chunk_map: ChunkMap::new(),
            line_mark_state: AtomicU8::new(Line::RESET_MARK_STATE),
            line_unavail_state: AtomicU8::new(Line::RESET_MARK_STATE),
            reusable_blocks: ReusableBlockPool::new(scheduler.num_workers()),
            defrag: Defrag::default(),
            mark_state: Self::UNMARKED_STATE,
            remset: RemSet::new(scheduler.num_workers()),
            scheduler,
            block_allocation: BlockAllocation::new(),
            possibly_dead_mature_blocks: Default::default(),
            initial_mark_pause: false,
            mutator_recycled_blocks: Default::default(),
            mature_evac_remsets: Default::default(),
            num_clean_blocks_released: Default::default(),
            num_clean_blocks_released_lazy: Default::default(),
            cm_enabled: false,
            rc_enabled,
            is_end_of_satb_or_full_gc: false,
            rc: RefCountHelper::NEW,
            options,
            evac_set: MatureEvacuationSet::default(),
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

    /// Check if current GC is a defrag GC.
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
    pub fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    fn schedule_defrag_selection_packets(&self, _pause: Pause) {
        self.evac_set.schedule_defrag_selection_packets(self)
    }

    pub fn rc_eager_prepare(&self, pause: Pause) {
        if pause == Pause::FullTraceFast || pause == Pause::InitialMark {
            self.schedule_defrag_selection_packets(pause);
        }
        self.block_allocation.notify_mutator_phase_end();
        if pause == Pause::FullTraceFast || pause == Pause::InitialMark {
            // Update mark_state
            // if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_on_side() {
            //     self.mark_state = Self::MARKED_STATE;
            // } else {
            //     // For header metadata, we use cyclic mark bits.
            //     unimplemented!("cyclic mark bits is not supported at the moment");
            // }
            // Reset block mark and object mark table.
            let work_packets = self.generate_prepare_tasks(None);
            self.scheduler().work_buckets[WorkBucketStage::Initial].bulk_add(work_packets);
        }
    }

    pub fn schedule_mark_table_zeroing_tasks(&self, stage: WorkBucketStage) {
        let work_packets = self.generate_concurrent_mark_table_zeroing_tasks();
        self.scheduler().work_buckets[stage].bulk_add(work_packets);
    }

    pub fn prepare_rc(&mut self, pause: Pause) {
        self.num_clean_blocks_released.store(0, Ordering::SeqCst);
        self.num_clean_blocks_released_lazy
            .store(0, Ordering::SeqCst);
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            self.evac_set.update_last_defrag_blocks();
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
        }
        // SATB sweep has problem scanning mutator recycled blocks.
        // Remaing the block state as "reusing" and reset them here.
        let mut blocks = self.mutator_recycled_blocks.lock();
        for b in &*blocks {
            b.set_state(BlockState::Marked);
        }
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            // Release young blocks to reduce to-space overflow
            let scheduler = self.scheduler.clone();
            self.block_allocation.sweep_and_reset(&scheduler);
            self.flush_page_resource();
        } else {
            for b in &*blocks {
                self.add_to_possibly_dead_mature_blocks(*b, false);
            }
        }
        blocks.clear();
        if pause == Pause::FinalMark {
            crate::REMSET_RECORDING.store(false, Ordering::SeqCst);
            self.is_end_of_satb_or_full_gc = true;
        } else if pause == Pause::FullTraceFast {
            self.is_end_of_satb_or_full_gc = true;
        }
    }

    pub fn release_rc(&mut self, pause: Pause) {
        debug_assert_ne!(pause, Pause::FullTraceDefrag);
        let scheduler = self.scheduler.clone();
        self.block_allocation.sweep_and_reset(&scheduler);
        self.flush_page_resource();
        let disable_lasy_dec_for_current_gc = crate::disable_lasy_dec_for_current_gc();
        if disable_lasy_dec_for_current_gc {
            self.scheduler().process_lazy_decrement_packets();
        } else {
            debug_assert_ne!(pause, Pause::FullTraceFast);
        }
        self.rc.reset_inc_buffer_size();
        self.is_end_of_satb_or_full_gc = false;
        // This cannot be done in parallel in a separate thread
        self.schedule_mature_sweeping(pause);
    }

    pub fn schedule_mature_sweeping(&self, pause: Pause) {
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            self.evac_set.sweep_mature_evac_candidates(self);
            let disable_lasy_dec_for_current_gc = crate::disable_lasy_dec_for_current_gc();
            let dead_cycle_sweep_packets = self.generate_dead_cycle_sweep_tasks();
            let sweep_los = RCSweepMatureLOS::new(LazySweepingJobsCounter::new_decs());
            if crate::args::LAZY_DECREMENTS && !disable_lasy_dec_for_current_gc {
                debug_assert_ne!(pause, Pause::FullTraceFast);
                self.scheduler().postpone_all(dead_cycle_sweep_packets);
                self.scheduler().postpone(sweep_los);
            } else {
                self.scheduler().work_buckets[WorkBucketStage::STWRCDecsAndSweep]
                    .bulk_add(dead_cycle_sweep_packets);
                self.scheduler().work_buckets[WorkBucketStage::STWRCDecsAndSweep].add(sweep_los);
            }
        }
    }

    pub fn prepare(&mut self, major_gc: bool, initial_mark_pause: bool) {
        self.initial_mark_pause = initial_mark_pause;
        debug_assert!(!self.rc_enabled);
        // self.block_allocation.reset();
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
        let work_packets = self.chunk_map.generate_tasks(|chunk| {
            Box::new(PrepareBlockState {
                space,
                chunk,
                defrag_threshold: if space.in_defrag() {
                    Some(threshold)
                } else {
                    None
                },
            })
        });
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
        // self.block_allocation.reset();
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
        let work_packets = self.generate_sweep_tasks();
        self.scheduler().work_buckets[WorkBucketStage::Release].bulk_add(work_packets);
        if super::DEFRAG {
            self.defrag.release(self);
        }
        self.initial_mark_pause = false;
        did_defrag
    }

    /// Generate chunk sweep tasks
    fn generate_sweep_tasks(&self) -> Vec<Box<dyn GCWork<VM>>> {
        self.defrag.mark_histograms.lock().clear();
        // # Safety: ImmixSpace reference is always valid within this collection cycle.
        let space = unsafe { &*(self as *const Self) };
        let epilogue = Arc::new(FlushPageResource {
            space,
            counter: AtomicUsize::new(0),
        });
        let tasks = self.chunk_map.generate_tasks(|chunk| {
            Box::new(SweepChunk {
                space,
                chunk,
                epilogue: epilogue.clone(),
            })
        });
        epilogue.counter.store(tasks.len(), Ordering::SeqCst);
        tasks
    }

    /// Generate chunk sweep work packets.
    pub fn generate_dead_cycle_sweep_tasks(&self) -> Vec<Box<dyn GCWork<VM>>> {
        self.chunk_map.generate_tasks(|chunk| {
            Box::new(SweepDeadCyclesChunk::new(
                chunk,
                LazySweepingJobsCounter::new_decs(),
            ))
        })
    }

    /// Generate chunk sweep work packets.
    pub fn generate_prepare_tasks(
        &self,
        defrag_threshold: Option<usize>,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        let rc_enabled = self.rc_enabled;
        let cm_enabled = self.cm_enabled;
        self.chunk_map.generate_tasks(|chunk| {
            Box::new(PrepareChunk {
                chunk,
                defrag_threshold,
                rc_enabled,
                cm_enabled,
            })
        })
    }

    pub fn generate_concurrent_mark_table_zeroing_tasks(&self) -> Vec<Box<dyn GCWork<VM>>> {
        self.chunk_map
            .generate_tasks(|chunk| Box::new(ConcurrentChunkMetadataZeroing { chunk }))
    }

    /// Release a block.
    pub fn release_block(
        &self,
        block: Block,
        nursery: bool,
        zero_unlog_table: bool,
        single_thread: bool,
    ) {
        // println!(
        //     "Release {:?} nursery={} defrag={}",
        //     block,
        //     nursery,
        //     block.is_defrag_source()
        // );
        if *self.options.verbose >= 2 {
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
        self.pr.release_block(block, single_thread);
    }

    /// Allocate a clean block.
    pub fn get_clean_block(&self, tls: VMThread, copy: bool) -> Option<Block> {
        self.block_allocation
            .get_clean_block(tls, copy, self.rc_enabled)
    }

    /// Pop a reusable block from the reusable block list.
    pub fn get_reusable_block(&self, copy: bool) -> Option<Block> {
        self.block_allocation.get_reusable_block(copy)
    }

    pub fn reusable_blocks_drained(&self) -> bool {
        self.reusable_blocks.len() == 0
    }

    /// Trace and mark objects without evacuation.
    pub fn process_mature_evacuation_remset(&self) {
        let mut remsets = vec![];
        mem::swap(&mut remsets, &mut self.mature_evac_remsets.lock());
        self.scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].bulk_add(remsets);
    }

    /// Trace and mark objects without evacuation.
    pub fn fast_trace_object(
        &self,
        trace: &mut impl ObjectQueue,
        object: ObjectReference,
    ) -> ObjectReference {
        self.trace_object_without_moving(trace, object)
    }

    /// Trace and mark objects. If the current object is in defrag block, then do evacuation as well.
    pub fn trace_object(
        &self,
        trace: &mut impl ObjectQueue,
        object: ObjectReference,
        semantics: CopySemantics,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        #[cfg(feature = "global_alloc_bit")]
        debug_assert!(
            crate::util::alloc_bit::is_alloced::<VM>(object),
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
    pub fn trace_object_without_moving(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
    ) -> ObjectReference {
        if self.attempt_mark(object) {
            if self.rc_enabled {
                let straddle = self
                    .rc
                    .is_straddle_line(Line::from(Line::align(object.to_address::<VM>())));
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
                        self.is_marked(object) || self.defrag.space_exhausted() || self.is_pinned(object),
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
                self.defrag.space_exhausted() || self.is_pinned(object),
                "Forwarded object is the same as original object {} even though it should have been copied",
                object,
            );
            ForwardingWord::clear_forwarding_bits::<VM>(object);
            object
        } else {
            // We won the forwarding race; actually forward and copy the object if it is not pinned
            // and we have sufficient space in our copy allocator
            let new_object = if self.is_pinned(object) || self.defrag.space_exhausted() {
                self.attempt_mark(object);
                ForwardingWord::clear_forwarding_bits::<VM>(object);
                Block::containing::<VM>(object).set_state(BlockState::Marked);
                object
            } else {
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

    pub fn rc_trace_object<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
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

    pub fn trace_mark_rc_mature_object(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
        _pause: Pause,
        mark: bool,
    ) -> ObjectReference {
        debug_assert!(
            !ForwardingWord::is_forwarded::<VM>(object),
            "object {:?} is forwarded",
            object
        );
        if mark && self.attempt_mark(object) {
            queue.enqueue(object);
        }
        object
    }

    #[allow(clippy::assertions_on_constants)]
    pub fn trace_forward_rc_mature_object<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
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
                crate::SLOPPY_COPY_BYTES.store(
                    crate::SLOPPY_COPY_BYTES.load(Ordering::Relaxed) + new.get_size::<VM>(),
                    Ordering::Relaxed,
                );
            }
            // Transfer RC count
            new.log_start_address::<VM>();
            if !crate::args::BLOCK_ONLY && new.get_size::<VM>() > Line::BYTES {
                self.rc.mark_straddle_object(new);
            }
            self.rc.set(new, self.rc.count(object));
            self.attempt_mark(new);
            self.unmark(object);
            queue.enqueue(new);
            new
        }
    }

    /// Mark all the lines that the given object spans.
    #[allow(clippy::assertions_on_constants)]
    pub fn mark_lines(&self, object: ObjectReference) {
        debug_assert!(!super::BLOCK_ONLY);
        if self.rc_enabled {
            return;
        }
        Line::mark_lines_for_object::<VM>(object, self.line_mark_state.load(Ordering::Acquire));
    }

    /// Atomically mark an object.
    pub fn attempt_mark(&self, object: ObjectReference) -> bool {
        loop {
            let old_value = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.load_atomic::<VM, u8>(
                object,
                None,
                Ordering::SeqCst,
            );
            if old_value == self.mark_state {
                return false;
            }

            if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
                .compare_exchange_metadata::<VM, u8>(
                    object,
                    old_value,
                    self.mark_state,
                    None,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break;
            }
        }
        true
    }

    /// Atomically mark an object.
    pub fn unmark(&self, object: ObjectReference) -> bool {
        let mark_bit = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec();
        let obj_addr = VM::VMObjectModel::ref_to_address(object);
        loop {
            let old_value: u8 = mark_bit.load_atomic(obj_addr, Ordering::SeqCst);
            if old_value == Self::UNMARKED_STATE {
                return false;
            }

            if mark_bit
                .compare_exchange_atomic(
                    obj_addr,
                    Self::MARKED_STATE,
                    Self::UNMARKED_STATE,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break;
            }
        }
        true
    }

    /// Check if an object is marked.
    pub fn is_marked(&self, object: ObjectReference) -> bool {
        let old_value = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.load_atomic::<VM, u8>(
            object,
            None,
            Ordering::SeqCst,
        );
        old_value == self.mark_state
    }

    /// Check if an object is pinned.
    fn is_pinned(&self, _object: ObjectReference) -> bool {
        #[cfg(feature = "object_pinning")]
        return self.is_object_pinned(_object);

        #[cfg(not(feature = "object_pinning"))]
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
        if Line::steps_between(&start, &end).unwrap() < crate::args().min_reuse_lines {
            if end == block.end_line() {
                return None;
            } else {
                return self.rc_get_next_available_lines(copy, end);
            };
        }
        if self.common.needs_log_bit {
            if !copy {
                Line::clear_log_table::<VM>(start..end);
            } else {
                Line::initialize_log_table_as_unlogged::<VM>(start..end);
            }
            Line::update_validity::<VM>(RegionIterator::<Line>::new(start, end));
        }
        block.dec_dead_bytes_sloppy(
            (Line::steps_between(&start, &end).unwrap() as u32) << Line::LOG_BYTES,
        );
        if self
            .block_allocation
            .concurrent_marking_in_progress_or_final_mark()
        {
            Line::initialize_mark_table_as_marked::<VM>(start..end);
        } else {
            // Line::clear_mark_table::<VM>(start..end);
        }
        // if !_copy {
        //     println!("reuse {:?} copy={}", start..end, copy);
        // }
        Some((start, end))
    }

    #[allow(clippy::assertions_on_constants)]
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
        if Line::steps_between(&start, &end).unwrap() < crate::args().min_reuse_lines {
            if end == block.end_line() {
                return None;
            } else {
                return self.normal_get_next_available_lines(end);
            };
        }
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
        let num_bins = self.scheduler().num_workers();
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

pub struct PrepareBlockState<VM: VMBinding> {
    pub space: &'static ImmixSpace<VM>,
    pub chunk: Chunk,
    pub defrag_threshold: Option<usize>,
}
impl<VM: VMBinding> PrepareBlockState<VM> {
    /// Clear object mark table
    fn reset_object_mark(chunk: Chunk) {
        // NOTE: We reset the mark bits because cyclic mark bit is currently not supported, yet.
        // See `ImmixSpace::prepare`.
        if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC {
            side.bzero_metadata(chunk.start(), Chunk::BYTES);
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for PrepareBlockState<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let defrag_threshold = self.defrag_threshold.unwrap_or(0);
        // Clear object mark table for this chunk
        Self::reset_object_mark(self.chunk);
        // Iterate over all blocks in this chunk
        for block in self.chunk.iter_region::<Block>() {
            let state = block.get_state();
            // Skip unallocated blocks.
            if state == BlockState::Unallocated {
                continue;
            }
            // Check if this block needs to be defragmented.
            if super::DEFRAG && defrag_threshold != 0 && block.get_holes() > defrag_threshold {
                block.set_as_defrag_source(true);
            } else {
                block.set_as_defrag_source(false);
            }
            // Clear block mark data.
            block.set_state(BlockState::Unmarked);
            debug_assert!(!block.get_state().is_reusable());
            debug_assert_ne!(block.get_state(), BlockState::Marked);
            // Clear forwarding bits if necessary.
            // if is_defrag_source {
            //     if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC {
            //         // Clear on-the-side forwarding bits.
            //         // NOTE: In theory, we only need to clear the forwarding bits of occupied lines of
            //         // blocks that are defrag sources.
            //         side.bzero_metadata(block.start(), Block::BYTES);
            //     }
            // }
            // NOTE: We don't need to reset the forwarding pointer metadata because it is meaningless
            // until the forwarding bits are also set, at which time we also write the forwarding
            // pointer.
        }
    }
}

/// Chunk sweeping work packet.
struct SweepChunk<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    chunk: Chunk,
    /// A destructor invoked when all `SweepChunk` packets are finished.
    epilogue: Arc<FlushPageResource<VM>>,
}

impl<VM: VMBinding> GCWork<VM> for SweepChunk<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let mut histogram = self.space.defrag.new_histogram();
        if self.space.chunk_map.get(self.chunk) == ChunkState::Allocated {
            let line_mark_state = if super::BLOCK_ONLY {
                None
            } else {
                Some(self.space.line_mark_state.load(Ordering::Acquire))
            };
            // number of allocated blocks.
            let mut allocated_blocks = 0;
            // Iterate over all allocated blocks in this chunk.
            for block in self
                .chunk
                .iter_region::<Block>()
                .filter(|block| block.get_state() != BlockState::Unallocated)
            {
                if !block.sweep(self.space, &mut histogram, line_mark_state) {
                    // Block is live. Increment the allocated block count.
                    allocated_blocks += 1;
                }
            }
            // Set this chunk as free if there is not live blocks.
            if allocated_blocks == 0 {
                self.space.chunk_map.set(self.chunk, ChunkState::Free)
            }
        }
        self.space.defrag.add_completed_mark_histogram(histogram);
        self.epilogue.finish_one_work_packet();
    }
}

/// Count number of remaining work pacets, and flush page resource if all packets are finished.
struct FlushPageResource<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    counter: AtomicUsize,
}

impl<VM: VMBinding> FlushPageResource<VM> {
    /// Called after a related work packet is finished.
    fn finish_one_work_packet(&self) {
        if 1 == self.counter.fetch_sub(1, Ordering::SeqCst) {
            // We've finished releasing all the dead blocks to the BlockPageResource's thread-local queues.
            // Now flush the BlockPageResource.
            self.space.flush_page_resource()
        }
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
    fn post_copy(&mut self, obj: ObjectReference, _bytes: usize) {
        let space = self.get_space();
        if space.rc_enabled {
            return;
        }
        // Mark the object
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.store_atomic::<VM, u8>(
            obj,
            space.mark_state,
            None,
            Ordering::SeqCst,
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
