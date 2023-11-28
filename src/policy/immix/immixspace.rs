use super::block_allocation::BlockAllocation;
use super::line::*;
use super::rc_work::*;
use super::{block::*, defrag::Defrag};
use crate::gc_log;
use crate::plan::immix::Pause;
use crate::plan::lxr::{MatureEvecRemSet, YoungRemSet};
use crate::plan::VectorObjectQueue;
use crate::policy::gc_work::{TraceKind, TRACE_KIND_TRANSITIVE_PIN};
use crate::policy::largeobjectspace::{RCReleaseMatureLOS, RCSweepMatureAfterSATBLOS};
use crate::policy::sft::GCWorkerMutRef;
use crate::policy::sft::SFT;
use crate::policy::sft_map::SFTMap;
use crate::policy::space::{CommonSpace, Space};
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::util::copy::*;
use crate::util::heap::chunk_map::*;
use crate::util::heap::BlockPageResource;
use crate::util::heap::PageResource;
use crate::util::linear_scan::Region;
use crate::util::metadata::side_metadata::*;
use crate::util::metadata::{self, MetadataSpec};
use crate::util::object_forwarding as ForwardingWord;
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
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;
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
    /// How many lines have been consumed since last GC?
    lines_consumed: AtomicUsize,
    reused_lines_consumed: AtomicUsize,
    /// Object mark state
    mark_state: u8,
    /// Work packet scheduler
    scheduler: Arc<GCWorkScheduler<VM>>,
    /// Some settings for this space
    space_args: ImmixSpaceArgs,
    pub block_allocation: BlockAllocation<VM>,
    possibly_dead_mature_blocks: SegQueue<(Block, bool)>,
    initial_mark_pause: bool,
    pub mature_evac_remsets: Mutex<Vec<Box<dyn GCWork<VM>>>>,
    pub num_clean_blocks_released_young: AtomicUsize,
    pub num_clean_blocks_released_mature: AtomicUsize,
    pub num_clean_blocks_released_lazy: AtomicUsize,
    pub copy_alloc_bytes: AtomicUsize,
    pub rc_killed_bytes: AtomicUsize,
    pub mature_evac_remset: MatureEvecRemSet<VM>,
    pub young_remset: YoungRemSet<VM>,
    pub cm_enabled: bool,
    pub rc_enabled: bool,
    pub is_end_of_satb_or_full_gc: bool,
    pub rc: RefCountHelper<VM>,
    pub(super) evac_set: MatureEvacuationSet,
    pub do_promotion: AtomicBool,
    pub one_epoch_blocks: Mutex<Vec<Block>>,
}

/// Some arguments for Immix Space.
pub struct ImmixSpaceArgs {
    /// Mark an object as unlogged when we trace an object.
    /// Normally we set the log bit when we copy an object with [`crate::util::copy::CopySemantics::PromoteToMature`].
    /// In sticky immix, we 'promote' an object to mature when we trace the object
    /// (no matter we copy an object or not). So we have to use `PromoteToMature`, and instead
    /// just set the log bit in the space when an object is traced.
    pub unlog_object_when_traced: bool,
    /// Reset log bit at the start of a major GC.
    /// Normally we do not need to do this. When immix is used as the mature space,
    /// any object should be set as unlogged, and that bit does not need to be cleared
    /// even if the object is dead. But in sticky Immix, the mature object and
    /// the nursery object are in the same space, we will have to use the
    /// bit to differentiate them. So we reset all the log bits in major GCs,
    /// and unlogged the objects when they are traced (alive).
    pub reset_log_bit_in_major_gc: bool,
    /// Whether this ImmixSpace instance contains both young and old objects.
    /// This affects the updating of valid-object bits.  If some lines or blocks of this ImmixSpace
    /// instance contain young objects, their VO bits need to be updated during this GC.  Currently
    /// only StickyImmix is affected.  GenImmix allocates young objects in a separete CopySpace
    /// nursery and its VO bits can be cleared in bulk.
    pub mixed_age: bool,
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
                    debug_assert!(
                        forwarded.to_raw_address().is_mapped(),
                        "Invalid forwarded object: {:?} -> {:?}",
                        object,
                        forwarded
                    );
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
            if Block::containing::<VM>(object).is_nursery() {
                return true;
            }
        }

        // If the mark bit is set, it is live.
        if self.is_marked(object) {
            return true;
        }

        // If we never move objects, look no further.
        if super::NEVER_MOVE_OBJECTS {
            return false;
        }

        // If the forwarding bits are on the side,
        if VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC.is_on_side() {
            // we need to ensure `object` is in a defrag source
            // because `PrepareBlockState` does not clear forwarding bits
            // for non-defrag-source blocks.
            if !Block::containing::<VM>(object).is_defrag_source() {
                // Objects not in defrag sources cannot be forwarded.
                return false;
            }
        }

        // If the object is forwarded, it is live, too.
        ForwardingWord::is_forwarded::<VM>(object)
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
        !super::NEVER_MOVE_OBJECTS
    }

    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        true
    }
    fn initialize_object_metadata(&self, _object: ObjectReference, _bytes: usize, _alloc: bool) {
        self.copy_alloc_bytes.store(0, Ordering::SeqCst);
        #[cfg(feature = "vo_bit")]
        crate::util::metadata::vo_bit::set_vo_bit::<VM>(_object);
    }
    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> bool {
        crate::util::metadata::vo_bit::is_vo_bit_set_for_addr::<VM>(addr).is_some()
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
    fn initialize_sft(&self, sft_map: &mut dyn SFTMap) {
        self.common().initialize_sft(
            self.as_sft(),
            sft_map,
            &self.get_page_resource().common().metadata,
        );
        // Initialize the block queues in `reusable_blocks` and `pr`.
        self.block_allocation.init(self);
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
        if KIND == TRACE_KIND_TRANSITIVE_PIN {
            self.trace_object_without_moving(queue, object)
        } else if KIND == TRACE_KIND_DEFRAG {
            if Block::containing::<VM>(object).is_defrag_source() {
                debug_assert!(self.in_defrag());
                debug_assert!(
                    !crate::plan::is_nursery_gc(worker.mmtk.get_plan()),
                    "Calling PolicyTraceObject on Immix in nursery GC"
                );
                self.trace_object_with_opportunistic_copy(
                    queue,
                    object,
                    copy.unwrap(),
                    worker,
                    // This should not be nursery collection. Nursery collection does not use PolicyTraceObject.
                    false,
                )
            } else {
                self.trace_object_without_moving(queue, object)
            }
        } else if KIND == TRACE_KIND_FAST {
            self.trace_object_without_moving(queue, object)
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
        } else if KIND == TRACE_KIND_FAST || KIND == TRACE_KIND_TRANSITIVE_PIN {
            false
        } else {
            unreachable!()
        }
    }
}

impl<VM: VMBinding> ImmixSpace<VM> {
    #[allow(unused)]
    const UNMARKED_STATE: u8 = 0;
    const MARKED_STATE: u8 = 1;

    /// Get side metadata specs
    fn side_metadata_specs(rc_enabled: bool) -> Vec<SideMetadataSpec> {
        if rc_enabled {
            let meta = vec![
                MetadataSpec::OnSide(Block::DEFRAG_STATE_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                MetadataSpec::OnSide(crate::util::rc::RC_STRADDLE_LINES),
                MetadataSpec::OnSide(Block::LOG_TABLE),
                MetadataSpec::OnSide(Block::NURSERY_PROMOTION_STATE_TABLE),
                MetadataSpec::OnSide(Block::PHASE_EPOCH),
                MetadataSpec::OnSide(Block::DEAD_WORDS),
            ];
            return metadata::extract_side_metadata(&meta);
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

    pub fn new(
        args: crate::policy::space::PlanCreateSpaceArgs<VM>,
        space_args: ImmixSpaceArgs,
    ) -> Self {
        #[cfg(feature = "immix_non_moving")]
        info!(
            "Creating non-moving ImmixSpace: {}. Block size: 2^{}",
            args.name,
            Block::LOG_BYTES
        );

        if space_args.unlog_object_when_traced || space_args.reset_log_bit_in_major_gc {
            assert!(
                args.constraints.needs_log_bit,
                "Invalid args when the plan does not use log bit"
            );
        }

        super::validate_features();
        #[cfg(feature = "vo_bit")]
        vo_bit::helper::validate_config::<VM>();
        let vm_map = args.vm_map;
        let scheduler = args.scheduler.clone();
        let rc_enabled = args.constraints.rc_enabled;
        let policy_args = args.into_policy_args(true, false, Self::side_metadata_specs(rc_enabled));
        let metadata = policy_args.metadata();
        let common = CommonSpace::new(policy_args);
        ImmixSpace {
            pr: if common.vmrequest.is_discontiguous() {
                BlockPageResource::new_discontiguous(
                    Block::LOG_PAGES,
                    vm_map,
                    scheduler.num_workers(),
                    metadata,
                )
            } else {
                BlockPageResource::new_contiguous(
                    Block::LOG_PAGES,
                    common.start,
                    common.extent,
                    vm_map,
                    scheduler.num_workers(),
                    metadata,
                )
            },
            common,
            chunk_map: ChunkMap::new(),
            line_mark_state: AtomicU8::new(Line::RESET_MARK_STATE),
            line_unavail_state: AtomicU8::new(Line::RESET_MARK_STATE),
            lines_consumed: AtomicUsize::new(0),
            reused_lines_consumed: AtomicUsize::new(0),
            reusable_blocks: ReusableBlockPool::new(scheduler.num_workers()),
            defrag: Defrag::default(),
            // Set to the correct mark state when inititialized. We cannot rely on prepare to set it (prepare may get skipped in nursery GCs).
            mark_state: Self::MARKED_STATE,
            mature_evac_remset: MatureEvecRemSet::new(scheduler.num_workers()),
            young_remset: YoungRemSet::new(scheduler.num_workers()),
            scheduler,
            space_args,
            block_allocation: BlockAllocation::new(),
            possibly_dead_mature_blocks: Default::default(),
            initial_mark_pause: false,
            mature_evac_remsets: Default::default(),
            num_clean_blocks_released_young: Default::default(),
            num_clean_blocks_released_mature: Default::default(),
            num_clean_blocks_released_lazy: Default::default(),
            copy_alloc_bytes: Default::default(),
            rc_killed_bytes: Default::default(),
            cm_enabled: false,
            rc_enabled,
            is_end_of_satb_or_full_gc: false,
            rc: RefCountHelper::NEW,
            evac_set: MatureEvacuationSet::default(),
            do_promotion: Default::default(),
            one_epoch_blocks: Default::default(),
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

    pub fn do_promotion(&self) -> bool {
        self.do_promotion.load(Ordering::SeqCst)
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
        self.block_allocation.notify_mutator_phase_end();
        self.pr.prepare_gc();
        if pause == Pause::Full || pause == Pause::InitialMark {
            // Update mark_state
            // if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_on_side() {
            //     self.mark_state = Self::MARKED_STATE;
            // } else {
            //     // For header metadata, we use cyclic mark bits.
            //     unimplemented!("cyclic mark bits is not supported at the moment");
            // }
            // Reset block mark and object mark table.
            let work_packets = self.generate_lxr_full_trace_prepare_tasks();
            self.scheduler().work_buckets[WorkBucketStage::Initial].bulk_add(work_packets);
        }
    }

    pub fn schedule_mark_table_zeroing_tasks(&self, stage: WorkBucketStage) {
        let work_packets = self.generate_concurrent_mark_table_zeroing_tasks();
        self.scheduler().work_buckets[stage].bulk_add(work_packets);
    }

    pub fn prepare_rc(&mut self, pause: Pause) {
        self.num_clean_blocks_released_young
            .store(0, Ordering::SeqCst);
        self.num_clean_blocks_released_mature
            .store(0, Ordering::SeqCst);
        self.num_clean_blocks_released_lazy
            .store(0, Ordering::SeqCst);
        self.copy_alloc_bytes.store(0, Ordering::SeqCst);
        self.rc_killed_bytes.store(0, Ordering::SeqCst);
        debug_assert_ne!(pause, Pause::FullDefrag);
        if pause == Pause::InitialMark || pause == Pause::Full {
            // Select mature evacuation set
            self.schedule_defrag_selection_packets(pause);
        }
        // Initialize mark state for tracing
        if pause == Pause::Full || pause == Pause::InitialMark {
            // Update mark_state
            if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_on_side() {
                self.mark_state = Self::MARKED_STATE;
            } else {
                // For header metadata, we use cyclic mark bits.
                unimplemented!("cyclic mark bits is not supported at the moment");
            }
        }
        // Release nursery blocks
        if pause != Pause::RefCount {
            self.pr.reset_before_mature_evac();
            if pause == Pause::Full || pause == Pause::FinalMark {
                // Reset worker TLABs.
                // The block of the current worker TLAB may be selected as part of the mature evacuation set.
                // So the copied mature objects might be copied into defrag blocks, and get copied out again.
                crate::scheduler::worker::reset_workers::<VM>();
            }
            // Release young blocks to reduce to-space overflow
            // self.block_allocation
            //     .sweep_nursery_blocks(&self.scheduler, pause);
            self.flush_page_resource();
        }
        if pause == Pause::FinalMark {
            crate::REMSET_RECORDING.store(false, Ordering::SeqCst);
            self.is_end_of_satb_or_full_gc = true;
        } else if pause == Pause::Full {
            self.is_end_of_satb_or_full_gc = true;
        }
    }

    pub fn release_rc(&mut self, pause: Pause) {
        #[cfg(feature = "lxr_release_stage_timer")]
        gc_log!([3]
            "    - ({:.3}ms) sweep_nursery_blocks start",
            crate::gc_start_time_ms(),
        );
        debug_assert_ne!(pause, Pause::FullDefrag);
        self.block_allocation.sweep_nursery_blocks(self, pause);
        #[cfg(feature = "lxr_release_stage_timer")]
        gc_log!([3]
            "    - ({:.3}ms) sweep_mutator_reused_blocks start",
            crate::gc_start_time_ms(),
        );
        #[cfg(feature = "lxr_release_stage_timer")]
        gc_log!([3]
            "    - ({:.3}ms) sweep_mutator_reused_blocks finish",
            crate::gc_start_time_ms(),
        );
        if cfg!(feature = "lxr_log_reclaim") {
            gc_log!([3]
                " - copy alloc size {}({}M)",
                self.copy_alloc_bytes.load( Ordering::Relaxed),
                self.copy_alloc_bytes.load( Ordering::Relaxed) >> 20,
            );
        }
        self.flush_page_resource();
        let disable_lasy_dec_for_current_gc = crate::disable_lasy_dec_for_current_gc();
        if disable_lasy_dec_for_current_gc {
            self.scheduler().process_lazy_decrement_packets();
        } else {
            debug_assert_ne!(pause, Pause::Full);
        }
        self.rc.reset_inc_buffer_size();
        self.is_end_of_satb_or_full_gc = false;
        // This cannot be done in parallel in a separate thread
        self.schedule_post_satb_mature_sweeping(pause);
        self.reused_lines_consumed.store(0, Ordering::Relaxed);
    }

    fn schedule_post_satb_mature_sweeping(&self, pause: Pause) {
        if pause == Pause::Full || pause == Pause::FinalMark {
            self.evac_set.sweep_mature_evac_candidates(self);
            let disable_lasy_dec_for_current_gc = crate::disable_lasy_dec_for_current_gc();
            let dead_cycle_sweep_packets = self.generate_dead_cycle_sweep_tasks();
            let sweep_los = RCSweepMatureAfterSATBLOS::new(LazySweepingJobsCounter::new_decs());
            if crate::args::LAZY_DECREMENTS && !disable_lasy_dec_for_current_gc {
                debug_assert_ne!(pause, Pause::Full);
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

            if !super::BLOCK_ONLY {
                self.line_mark_state.fetch_add(1, Ordering::AcqRel);
                if self.line_mark_state.load(Ordering::Acquire) > Line::MAX_MARK_STATE {
                    self.line_mark_state
                        .store(Line::RESET_MARK_STATE, Ordering::Release);
                }
            }
        }

        #[cfg(feature = "vo_bit")]
        if vo_bit::helper::need_to_clear_vo_bits_before_tracing::<VM>() {
            let maybe_scope = if major_gc {
                // If it is major GC, we always clear all VO bits because we are doing full-heap
                // tracing.
                Some(VOBitsClearingScope::FullGC)
            } else if self.space_args.mixed_age {
                // StickyImmix nursery GC.
                // Some lines (or blocks) contain only young objects,
                // while other lines (or blocks) contain only old objects.
                if super::BLOCK_ONLY {
                    // Block only.  Young objects are only allocated into fully empty blocks.
                    // Only clear unmarked blocks.
                    Some(VOBitsClearingScope::BlockOnly)
                } else {
                    // Young objects are allocated into empty lines.
                    // Only clear unmarked lines.
                    let line_mark_state = self.line_mark_state.load(Ordering::SeqCst);
                    Some(VOBitsClearingScope::Line {
                        state: line_mark_state,
                    })
                }
            } else {
                // GenImmix nursery GC.  We do nothing to the ImmixSpace because the nursery is a
                // separate CopySpace.  It'll clear its own VO bits.
                None
            };

            if let Some(scope) = maybe_scope {
                let work_packets = self
                    .chunk_map
                    .generate_tasks(|chunk| Box::new(ClearVOBitsAfterPrepare { chunk, scope }));
                self.scheduler.work_buckets[WorkBucketStage::ClearVOBits].bulk_add(work_packets);
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

        self.lines_consumed.store(0, Ordering::Relaxed);

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
    fn generate_lxr_full_trace_prepare_tasks(&self) -> Vec<Box<dyn GCWork<VM>>> {
        let rc_enabled = self.rc_enabled;
        let cm_enabled = self.cm_enabled;
        self.chunk_map.generate_tasks(|chunk| {
            Box::new(PrepareChunk {
                chunk,
                rc_enabled,
                cm_enabled,
            })
        })
    }

    pub fn generate_concurrent_mark_table_zeroing_tasks(&self) -> Vec<Box<dyn GCWork<VM>>> {
        self.chunk_map
            .generate_tasks(|chunk| Box::new(ConcurrentChunkMetadataZeroing { chunk }))
    }

    /// Dump fragmentation distribution and heap usage
    ///
    /// Collect 5 distrubutions:
    /// 1. `avail-pages-in-block` -  Number of available pages in block (0-8)
    /// 2. `avail-lines-in-block` -  Number of available lines in block (0-128)
    /// 3. `contig-avail-lines` -  Number of **contiguous** available lines in block (0-64)
    /// 4. `avail-blocks-in-chunk` -  Number of available blocks in chunk (0-128)
    /// 5. `rc-live-words-in-block` -  RC Live size in block (0-4096)
    pub fn dump_memory(&self) {
        #[derive(Default)]
        struct Dist {
            avail_blocks_in_chunk: Vec<u8>,
            avail_pages_in_block: Vec<u8>,
            avail_lines_in_block: Vec<u8>,
            contig_avail_lines: Vec<u8>,
            rc_live_words_in_block: Vec<u16>,
            live_chunks: usize,
            live_blocks: usize,
            live_pages: usize,
            live_lines: usize,
            rc_live_bytes: usize,
        }
        let mut dist = Dist::default();
        for chunk in self.chunk_map.all_chunks() {
            if !self.address_in_space(chunk.start()) {
                continue;
            }
            dist.live_chunks += 1;
            let mut avail_blocks_in_chunk = 0u8;
            for block in chunk
                .iter_region::<Block>()
                .filter(|b| b.get_state() != BlockState::Unallocated)
            {
                dist.live_blocks += 1;
                avail_blocks_in_chunk += 1;
                // Get avail_pages_in_block and avail_lines_in_block
                let rc_array = RCArray::of(block);
                let mut avail_lines_in_block = 0u8;
                let mut avail_pages_in_block = 0u8;
                for page in block.iter_region::<Page>() {
                    let mut avail_lines_in_page = 0;
                    for line in page.iter_region::<Line>() {
                        let i = line.get_index_within_block();
                        if rc_array.is_dead(i) {
                            avail_lines_in_page += 1;
                        } else {
                            dist.live_lines += 1;
                        }
                    }
                    avail_lines_in_block += avail_lines_in_page;
                    if avail_lines_in_page == (Page::BYTES / Line::BYTES) as u8 {
                        avail_pages_in_block += 1;
                    } else {
                        dist.live_pages += 1;
                    }
                }
                dist.avail_pages_in_block.push(avail_pages_in_block);
                dist.avail_lines_in_block.push(avail_lines_in_block);
                // Get contig_avail_lines
                block.iter_holes(|lines| dist.contig_avail_lines.push(lines as u8));
                // Get rc_live_bytes_in_block
                let mut rc_live_size: usize = 0;
                let mut cursor = block.start();
                let limit = block.end();
                while cursor < limit {
                    let o: ObjectReference = cursor.to_object_reference::<VM>();
                    cursor = cursor + crate::util::rc::MIN_OBJECT_SIZE;
                    let c = self.rc.count(o);
                    if c != 0 {
                        if Line::is_aligned(o.to_address::<VM>())
                            && self.rc.is_straddle_line(Line::from(o.to_address::<VM>()))
                        {
                            continue;
                        }
                        rc_live_size += o.get_size::<VM>();
                    }
                }
                dist.rc_live_words_in_block.push((rc_live_size >> 3) as u16);
                dist.rc_live_bytes += rc_live_size;
            }
            dist.avail_blocks_in_chunk.push(avail_blocks_in_chunk);
        }

        fn dump_bins<T: Copy + Ord + std::ops::Add<Output = T> + Into<usize>>(
            name: &str,
            v: &mut Vec<T>,
            max: usize,
        ) {
            assert!(max <= u16::MAX as usize);
            let max = max as u16;
            v.sort();
            let mut bins = vec![0usize; max as usize + 1];
            for x in v.iter() {
                let x: usize = (*x).into();
                bins[x] += 1;
            }
            print!("  {}: ", name);
            // let mut sum = 0usize;
            for i in 0..=max {
                if i == 0 {
                    print!("[");
                } else {
                    print!(",");
                }
                print!("{}", bins[i as usize]);
                // sum += cdf[i as usize] as usize;
                // print!("{}", sum);
            }
            println!("]");
        }
        // owned chunks
        let mut owned_chunks = 0usize;
        let mut a = self.pr.common().get_head_discontiguous_region();
        while !a.is_zero() {
            owned_chunks += self.common.vm_map().get_contiguous_region_chunks(a);
            a = self.common.vm_map().get_next_contiguous_region(a);
        }
        println!("immix:");
        println!("  owned-chunks: {}", owned_chunks);
        println!("  live-chunks: {}", dist.live_chunks);
        println!("  live-blocks: {}", dist.live_blocks);
        println!("  live-pages: {}", dist.live_pages);
        println!("  live-lines: {}", dist.live_lines);
        println!("  rc-live-bytes: {}", dist.rc_live_bytes);
        println!(
            "  reachable-live-bytes: {}",
            crate::SANITY_LIVE_SIZE_IX.load(Ordering::SeqCst)
        );
        dump_bins(
            "avail-blocks-in-chunk",
            &mut dist.avail_blocks_in_chunk,
            Chunk::BYTES / Block::BYTES,
        );
        dump_bins(
            "avail-pages-in-block",
            &mut dist.avail_pages_in_block,
            Block::BYTES / Page::BYTES,
        );
        dump_bins(
            "avail-lines-in-block",
            &mut dist.avail_lines_in_block,
            Block::BYTES / Line::BYTES,
        );
        dump_bins(
            "contig-avail-lines",
            &mut dist.contig_avail_lines,
            Block::BYTES / Line::BYTES,
        );
        dump_bins(
            "rc-live-words-in-block",
            &mut dist.rc_live_words_in_block,
            Block::BYTES >> 3,
        );
    }

    /// Allocate a clean block.
    pub fn get_clean_block(&self, tls: VMThread, copy: bool) -> Option<Block> {
        let block_address = self.acquire(tls, Block::PAGES);
        if block_address.is_zero() {
            return None;
        }
        if !self.rc_enabled {
            self.defrag.notify_new_clean_block(copy);
        }
        let block = Block::from_aligned_address(block_address);
        self.block_allocation
            .initialize_new_clean_block(block, copy, self.cm_enabled);
        self.chunk_map.set(block.chunk(), ChunkState::Allocated);
        if !self.rc_enabled {
            self.lines_consumed
                .fetch_add(Block::LINES, Ordering::SeqCst);
        }

        #[cfg(feature = "lxr_srv_ratio_counter")]
        if !copy {
            crate::plan::lxr::SURVIVAL_RATIO_PREDICTOR
                .ix_clean_alloc_vol
                .fetch_add(Block::BYTES, Ordering::SeqCst);
        }
        Some(block)
    }

    /// Get a list of clean or reusable blocks.
    /// For blocks in a new chunk, they should be mapped before returning.
    /// No heap accounting should be updated. They are updated when the mutator starts to allocating into them.
    pub fn acquire_blocks(
        &self,
        alloc_count: usize,
        steal_count: usize,
        clean: bool,
        buf: &mut Vec<Block>,
        copy: bool,
        owner: VMThread,
    ) -> bool {
        debug_assert!(!owner.0.to_address().is_zero());
        let mature_evac = copy
            && self.rc_enabled
            && self.scheduler().work_buckets[WorkBucketStage::Closure].is_activated();
        self.pr.acquire_blocks(
            alloc_count,
            steal_count,
            clean,
            buf,
            self,
            copy,
            mature_evac,
            owner,
        )
    }

    /// Logically acquire a clean block and poll for GC.
    /// This does not actually allocate a block, but only updates the heap counter and do GC when necessary.
    pub fn get_clean_block_logically(&self, tls: VMThread, _copy: bool) -> Result<(), ()> {
        let success = self.acquire_logically(tls, Block::PAGES);
        if !success {
            return Err(());
        }
        Ok(())
    }

    pub fn initialize_new_block(&self, block: Block, clean: bool, copy: bool) {
        // gc_log!("new-block: {:?} clean={} copy={}", block, clean, copy);
        if clean {
            if !self.rc_enabled {
                self.defrag.notify_new_clean_block(copy);
            }
            self.block_allocation
                .initialize_new_clean_block(block, copy, self.cm_enabled);
            self.chunk_map.set(block.chunk(), ChunkState::Allocated);
            if !self.rc_enabled {
                self.lines_consumed
                    .fetch_add(Block::LINES, Ordering::SeqCst);
            }
            #[cfg(feature = "lxr_srv_ratio_counter")]
            if !copy {
                crate::plan::lxr::SURVIVAL_RATIO_PREDICTOR
                    .ix_clean_alloc_vol
                    .fetch_add(Block::BYTES, Ordering::SeqCst);
            }
        } else {
            if self.rc_enabled {
                // pass
            } else {
                // Get available lines. Do this before block.init which will reset block state.
                let lines_delta = match block.get_state() {
                    BlockState::Reusable { unavailable_lines } => {
                        Block::LINES - unavailable_lines as usize
                    }
                    BlockState::Unmarked => Block::LINES,
                    _ => unreachable!("{:?} {:?}", block, block.get_state()),
                };
                self.lines_consumed.fetch_add(lines_delta, Ordering::SeqCst);
            }
            block.init(copy, true, self);
        }
    }

    pub fn reusable_blocks_drained(&self) -> bool {
        self.reusable_blocks.len() == 0
    }

    /// Trace and mark objects without evacuation.
    pub fn process_mature_evacuation_remset(&self) {
        let mut remsets = vec![];
        mem::swap(&mut remsets, &mut self.mature_evac_remsets.lock().unwrap());
        self.scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].bulk_add(remsets);
    }

    /// Trace and mark objects without evacuation.
    pub fn trace_object_without_moving(
        &self,
        queue: &mut impl ObjectQueue,
        object: ObjectReference,
    ) -> ObjectReference {
        #[cfg(feature = "vo_bit")]
        vo_bit::helper::on_trace_object::<VM>(object);

        if self.attempt_mark(object) {
            if self.rc_enabled {
                let straddle = self
                    .rc
                    .is_straddle_line(Line::from(Line::align(object.to_address::<VM>())));
                if straddle {
                    return object;
                }
            } else {
                // Mark block and lines
                if !super::BLOCK_ONLY {
                    if !super::MARK_LINE_AT_SCAN_TIME {
                        self.mark_lines(object);
                    }
                } else {
                    let block = Block::containing::<VM>(object);
                    let state = block.get_state();
                    if state != BlockState::Marked {
                        debug_assert_ne!(state, BlockState::Unallocated);
                        block.set_state(BlockState::Marked);
                    }
                }
            }

            #[cfg(feature = "vo_bit")]
            vo_bit::helper::on_object_marked::<VM>(object);

            // Visit node
            queue.enqueue(object);
            if !self.rc_enabled {
                self.unlog_object_if_needed(object);
            }
            return object;
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
        nursery_collection: bool,
    ) -> ObjectReference {
        let copy_context = worker.get_copy_context_mut();
        debug_assert!(!super::BLOCK_ONLY);

        #[cfg(feature = "vo_bit")]
        vo_bit::helper::on_trace_object::<VM>(object);

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
            ForwardingWord::clear_forwarding_bits::<VM>(object);
            object
        } else {
            // We won the forwarding race; actually forward and copy the object if it is not pinned
            // and we have sufficient space in our copy allocator
            debug_assert!(!nursery_collection || !self.rc_enabled);
            let new_object = if self.is_pinned(object)
                || (!nursery_collection && self.defrag.space_exhausted())
            {
                self.attempt_mark(object);
                ForwardingWord::clear_forwarding_bits::<VM>(object);
                Block::containing::<VM>(object).set_state(BlockState::Marked);

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_object_marked::<VM>(object);

                object
            } else {
                // We are forwarding objects. When the copy allocator allocates the block, it should
                // mark the block. So we do not need to explicitly mark it here.
                // Clippy complains if the "vo_bit" feature is not enabled.
                #[allow(clippy::let_and_return)]
                let new_object =
                    ForwardingWord::try_forward_object::<VM>(object, semantics, copy_context)
                        .expect("to-space overflow");

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_object_forwarded::<VM>(new_object);

                new_object
            };
            debug_assert!({
                let state = Block::containing::<VM>(new_object).get_state();
                state == BlockState::Marked
            });
            queue.enqueue(new_object);
            debug_assert!(new_object.is_live());
            self.unlog_object_if_needed(new_object);
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
            let new = ForwardingWord::try_forward_object::<VM>(
                object,
                CopySemantics::DefaultCopy,
                copy_context,
            )
            .expect("to-space overflow");
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

    fn unlog_object_if_needed(&self, object: ObjectReference) {
        debug_assert!(!self.rc_enabled);
        if self.space_args.unlog_object_when_traced {
            // Make sure the side metadata for the line can fit into one byte. For smaller line size, we should
            // use `mark_as_unlogged` instead to mark the bit.
            // const_assert!(
            //     Line::BYTES
            //         >= (1
            //             << (crate::util::constants::LOG_BITS_IN_BYTE
            //                 + crate::util::constants::LOG_MIN_OBJECT_SIZE))
            // );
            // const_assert_eq!(
            //     crate::vm::object_model::specs::VMGlobalLogBitSpec::LOG_NUM_BITS,
            //     0
            // ); // We should put this to the addition, but type casting is not allowed in constant assertions.

            // Every immix line is 256 bytes, which is mapped to 4 bytes in the side metadata.
            // If we have one object in the line that is mature, we can assume all the objects in the line are mature objects.
            // So we can just mark the byte.
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                .mark_byte_as_unlogged::<VM>(object, Ordering::Relaxed);
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
    fn is_marked_with(&self, object: ObjectReference, mark_state: u8) -> bool {
        let old_value = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.load_atomic::<VM, u8>(
            object,
            None,
            Ordering::SeqCst,
        );
        old_value == mark_state
    }

    pub fn is_marked(&self, object: ObjectReference) -> bool {
        self.is_marked_with(object, self.mark_state)
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
            self.normal_get_next_available_lines(copy, search_start)
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
            if !copy || !self.do_promotion() {
                Line::clear_field_unlog_table::<VM>(start..end);
            } else {
                Line::initialize_field_unlog_table_as_unlogged::<VM>(start..end);
            }
        }
        let num_lines = Line::steps_between(&start, &end).unwrap();
        if !copy {
            self.reused_lines_consumed
                .fetch_add(num_lines, Ordering::Relaxed);
        }
        block.dec_dead_bytes_sloppy((num_lines as u32) << Line::LOG_BYTES);
        #[cfg(feature = "lxr_srv_ratio_counter")]
        crate::plan::lxr::SURVIVAL_RATIO_PREDICTOR
            .reused_alloc_vol
            .fetch_add(num_lines << Line::LOG_BYTES, Ordering::SeqCst);
        if self
            .block_allocation
            .concurrent_marking_in_progress_or_final_mark()
            && self.do_promotion()
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
    pub fn normal_get_next_available_lines(
        &self,
        copy: bool,
        search_start: Line,
    ) -> Option<(Line, Line)> {
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
                return self.normal_get_next_available_lines(copy, end);
            };
        }
        if self.common.needs_log_bit && !crate::args::BARRIER_MEASUREMENT_NO_SLOW {
            if !copy {
                Line::clear_field_unlog_table::<VM>(start..end);
            } else {
                Line::initialize_field_unlog_table_as_unlogged::<VM>(start..end);
            }
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

    pub(crate) fn get_mutator_recycled_lines_in_pages(&self) -> usize {
        debug_assert!(self.rc_enabled);
        self.reused_lines_consumed.load(Ordering::Relaxed)
            >> (LOG_BYTES_IN_PAGE - Line::LOG_BYTES as u8)
    }

    pub(crate) fn get_pages_allocated(&self) -> usize {
        debug_assert!(!self.rc_enabled);
        self.lines_consumed.load(Ordering::Relaxed) >> (LOG_BYTES_IN_PAGE - Line::LOG_BYTES as u8)
    }

    /// Post copy routine for Immix copy contexts
    fn post_copy(&self, object: ObjectReference, _bytes: usize) {
        if self.rc_enabled {
            if cfg!(feature = "lxr_log_reclaim") {
                self.copy_alloc_bytes.fetch_add(_bytes, Ordering::Relaxed);
            }
            return;
        }
        // Mark the object
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.store_atomic::<VM, u8>(
            object,
            self.mark_state,
            None,
            Ordering::SeqCst,
        );
        // Mark the line
        if !super::MARK_LINE_AT_SCAN_TIME {
            self.mark_lines(object);
        }
    }
}

/// A work packet to prepare each block for a major GC.
/// Performs the action on a range of chunks.
pub struct PrepareBlockState<VM: VMBinding> {
    pub space: &'static ImmixSpace<VM>,
    pub chunk: Chunk,
    pub defrag_threshold: Option<usize>,
}
impl<VM: VMBinding> PrepareBlockState<VM> {
    /// Clear object mark table
    fn reset_object_mark(&self) {
        // NOTE: We reset the mark bits because cyclic mark bit is currently not supported, yet.
        // See `ImmixSpace::prepare`.
        if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC {
            side.bzero_metadata(self.chunk.start(), Chunk::BYTES);
        }
        if self.space.space_args.reset_log_bit_in_major_gc {
            if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC {
                // We zero all the log bits in major GC, and for every object we trace, we will mark the log bit again.
                side.bzero_metadata(self.chunk.start(), Chunk::BYTES);
            } else {
                // If the log bit is not in side metadata, we cannot bulk zero. We can either
                // clear the bit for dead objects in major GC, or clear the log bit for new
                // objects. In either cases, we do not need to set log bit at tracing.
                unimplemented!("We cannot bulk zero unlogged bit.")
            }
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for PrepareBlockState<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        // Clear object mark table for this chunk
        self.reset_object_mark();
        // Iterate over all blocks in this chunk
        for block in self.chunk.iter_region::<Block>() {
            let state = block.get_state();
            // Skip unallocated blocks.
            if state == BlockState::Unallocated {
                continue;
            }
            // Check if this block needs to be defragmented.
            let is_defrag_source = if !super::DEFRAG {
                // Do not set any block as defrag source if defrag is disabled.
                false
            } else if super::DEFRAG_EVERY_BLOCK {
                // Set every block as defrag source if so desired.
                true
            } else if let Some(defrag_threshold) = self.defrag_threshold {
                // This GC is a defrag GC.
                block.get_holes() > defrag_threshold
            } else {
                // Not a defrag GC.
                false
            };
            block.set_as_defrag_source(is_defrag_source);
            // Clear block mark data.
            block.set_state(BlockState::Unmarked);
            debug_assert!(!block.get_state().is_reusable());
            debug_assert_ne!(block.get_state(), BlockState::Marked);
            // Clear forwarding bits if necessary.
            if is_defrag_source {
                // Note that `ImmixSpace::is_live` depends on the fact that we only clear side
                // forwarding bits for defrag sources.  If we change the code here, we need to
                // make sure `ImmixSpace::is_live` is fixed, too.
                if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC {
                    // Clear on-the-side forwarding bits.
                    side.bzero_metadata(block.start(), Block::BYTES);
                }
            }
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

use crate::plan::Plan;
use crate::policy::copy_context::PolicyCopyContext;
use crate::util::alloc::Allocator;
use crate::util::alloc::ImmixAllocator;

/// Normal immix copy context. It has one copying Immix allocator.
/// Most immix plans use this copy context.
pub struct ImmixCopyContext<VM: VMBinding> {
    allocator: ImmixAllocator<VM>,
}

impl<VM: VMBinding> PolicyCopyContext for ImmixCopyContext<VM> {
    type VM = VM;

    fn prepare(&mut self) {
        self.allocator.reset();
    }
    fn release(&mut self) {
        self.allocator.reset();
    }
    fn alloc_copy(
        &mut self,
        _original: ObjectReference,
        bytes: usize,
        align: usize,
        offset: usize,
    ) -> Address {
        self.allocator.alloc(bytes, align, offset)
    }
    fn post_copy(&mut self, obj: ObjectReference, bytes: usize) {
        self.get_space().post_copy(obj, bytes)
    }
}

impl<VM: VMBinding> ImmixCopyContext<VM> {
    pub fn new(
        tls: VMWorkerThread,
        plan: &'static dyn Plan<VM = VM>,
        space: &'static ImmixSpace<VM>,
    ) -> Self {
        ImmixCopyContext {
            allocator: ImmixAllocator::new(tls.0, Some(space), plan, true),
        }
    }

    fn get_space(&self) -> &ImmixSpace<VM> {
        self.allocator.immix_space()
    }
}

/// Hybrid Immix copy context. It includes two different immix allocators. One with `copy = true`
/// is used for defrag GCs, and the other is used for other purposes (such as promoting objects from
/// nursery to Immix mature space). This is used by generational immix.
pub struct ImmixHybridCopyContext<VM: VMBinding> {
    copy_allocator: ImmixAllocator<VM>,
    defrag_allocator: ImmixAllocator<VM>,
}

impl<VM: VMBinding> PolicyCopyContext for ImmixHybridCopyContext<VM> {
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
        offset: usize,
    ) -> Address {
        if self.get_space().in_defrag() {
            self.defrag_allocator.alloc(bytes, align, offset)
        } else {
            self.copy_allocator.alloc(bytes, align, offset)
        }
    }
    fn post_copy(&mut self, obj: ObjectReference, bytes: usize) {
        self.get_space().post_copy(obj, bytes)
    }
}

impl<VM: VMBinding> ImmixHybridCopyContext<VM> {
    pub fn new(
        tls: VMWorkerThread,
        plan: &'static dyn Plan<VM = VM>,
        space: &'static ImmixSpace<VM>,
    ) -> Self {
        ImmixHybridCopyContext {
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

#[cfg(feature = "vo_bit")]
#[derive(Clone, Copy)]
enum VOBitsClearingScope {
    /// Clear all VO bits in all blocks.
    FullGC,
    /// Clear unmarked blocks, only.
    BlockOnly,
    /// Clear unmarked lines, only.  (i.e. lines with line mark state **not** equal to `state`).
    Line { state: u8 },
}

/// A work packet to clear VO bit metadata after Prepare.
#[cfg(feature = "vo_bit")]
struct ClearVOBitsAfterPrepare {
    chunk: Chunk,
    scope: VOBitsClearingScope,
}

#[cfg(feature = "vo_bit")]
impl<VM: VMBinding> GCWork<VM> for ClearVOBitsAfterPrepare {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        match self.scope {
            VOBitsClearingScope::FullGC => {
                vo_bit::bzero_vo_bit(self.chunk.start(), Chunk::BYTES);
            }
            VOBitsClearingScope::BlockOnly => {
                self.clear_blocks(None);
            }
            VOBitsClearingScope::Line { state } => {
                self.clear_blocks(Some(state));
            }
        }
    }
}

#[cfg(feature = "vo_bit")]
impl ClearVOBitsAfterPrepare {
    fn clear_blocks(&mut self, line_mark_state: Option<u8>) {
        for block in self
            .chunk
            .iter_region::<Block>()
            .filter(|block| block.get_state() != BlockState::Unallocated)
        {
            block.clear_vo_bits_for_unmarked_regions(line_mark_state);
        }
    }
}
