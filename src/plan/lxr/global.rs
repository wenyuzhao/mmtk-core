use super::gc_work::{LXREmergencyWeakRefWorkContext, LXRGCWorkContext, LXRWeakRefWorkContext};
use super::mutator::ALLOCATOR_MAPPING;
use super::rc::{ProcessDecs, RCImmixCollectRootEdges};
use super::remset::FlushMatureEvacRemsets;
use crate::mmtk::VM_MAP;
use crate::plan::global::CommonPlan;
use crate::plan::global::GcStatus;
use crate::plan::global::{BasePlan, CreateGeneralPlanArgs, CreateSpecificPlanArgs};
use crate::plan::immix::Pause;
use crate::plan::lxr::gc_work::FastRCPrepare;
use crate::plan::AllocationSemantics;
use crate::plan::MutatorContext;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::immix::block::Block;
use crate::policy::immix::rc_work::UpdateWeakProcessor;
use crate::policy::largeobjectspace::LargeObjectSpace;
use crate::policy::space::Space;
use crate::scheduler::gc_work::*;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
#[cfg(feature = "analysis")]
use crate::util::analysis::GcHookWork;
use crate::util::constants::*;
use crate::util::copy::*;
use crate::util::heap::layout::vm_layout_constants::*;
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSanity;
use crate::util::metadata::MetadataSpec;
use crate::util::options::{GCTriggerSelector, Options};
use crate::util::rc::{RefCountHelper, RC_LOCK_BIT_SPEC, RC_TABLE};
#[cfg(feature = "sanity")]
use crate::util::sanity::sanity_checker::*;
use crate::util::{metadata, Address, ObjectReference};
use crate::vm::{ActivePlan, Collection, ObjectModel, VMBinding};
use crate::{policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use crate::{BarrierSelector, LazySweepingJobsCounter};
use atomic::{Atomic, Ordering};
use crossbeam::queue::SegQueue;
use enum_map::EnumMap;
use spin::Lazy;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Condvar, Mutex, RwLock};
use std::time::SystemTime;

const LOG_CONSERVATIVE_SURVIVAL_RATIO_MULTIPLER: usize = 1;

static INITIAL_GC_TRIGGERED: AtomicBool = AtomicBool::new(false);
static INCS_TRIGGERED: AtomicBool = AtomicBool::new(false);
static ALLOC_TRIGGERED: AtomicBool = AtomicBool::new(false);
static SURVIVAL_TRIGGERED: AtomicBool = AtomicBool::new(false);
static HEAP_AFTER_GC: AtomicUsize = AtomicUsize::new(0);

use mmtk_macros::PlanTraceObject;

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum GCCause {
    Unknown,
    FullHeap,
    Emergency,
    UserTriggered,
    FixedNursery,
    Survival,
    Increments,
    ImmixSpaceFull,
}

#[derive(PlanTraceObject)]
pub struct LXR<VM: VMBinding> {
    #[post_scan]
    #[trace(CopySemantics::DefaultCopy)]
    pub immix_space: ImmixSpace<VM>,
    #[fallback_trace]
    pub common: CommonPlan<VM>,
    /// Always true for non-rc immix.
    /// For RC immix, this is used for enable backup tracing.
    perform_cycle_collection: AtomicBool,
    current_pause: Atomic<Option<Pause>>,
    previous_pause: Atomic<Option<Pause>>,
    next_gc_may_perform_cycle_collection: AtomicBool,
    next_gc_may_perform_emergency_collection: AtomicBool,
    last_gc_was_defrag: AtomicBool,
    nursery_blocks: Option<usize>,
    avail_pages_at_end_of_last_gc: AtomicUsize,
    zeroing_packets_scheduled: AtomicBool,
    next_gc_selected: (Mutex<bool>, Condvar),
    in_concurrent_marking: AtomicBool,
    pub prev_roots: RwLock<SegQueue<Vec<ObjectReference>>>,
    pub curr_roots: RwLock<SegQueue<Vec<ObjectReference>>>,
    pub rc: RefCountHelper<VM>,
    gc_cause: Atomic<GCCause>,
}

pub static LXR_CONSTRAINTS: Lazy<PlanConstraints> = Lazy::new(|| PlanConstraints {
    moves_objects: true,
    gc_header_bits: 2,
    gc_header_words: 0,
    num_specialized_scans: 1,
    /// Max immix object size is half of a block.
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
    barrier: BarrierSelector::FieldBarrier,
    needs_log_bit: true,
    needs_field_log_bit: true,
    rc_enabled: true,
    ..PlanConstraints::default()
});

impl<VM: VMBinding> Plan for LXR<VM> {
    type VM = VM;

    fn get_spaces(&self) -> Vec<&dyn Space<Self::VM>> {
        let mut ret = self.common.get_spaces();
        ret.push(&self.immix_space);
        ret
    }

    fn collection_required(&self, space_full: bool, _space: Option<&dyn Space<Self::VM>>) -> bool {
        // Spaces or heap full
        if self.base().collection_required(self, space_full) {
            self.gc_cause.store(GCCause::FullHeap, Ordering::Relaxed);
            return true;
        }
        // Survival limits
        let predicted_survival = ((self.immix_space.block_allocation.nursery_mb() as f64
            * super::SURVIVAL_RATIO_PREDICTOR.ratio()) as usize)
            << LOG_CONSERVATIVE_SURVIVAL_RATIO_MULTIPLER;
        if !cfg!(feature = "lxr_no_survival_trigger") {
            if predicted_survival >= crate::args().max_survival_mb {
                SURVIVAL_TRIGGERED.store(true, Ordering::Relaxed);
                self.gc_cause.store(GCCause::Survival, Ordering::Relaxed);
                return true;
            }
        }
        if !self.immix_space.common().contiguous {
            let available_to_space = (self.immix_space.pr.available_pages()
                + (VM_MAP.available_chunks() << (LOG_BYTES_IN_CHUNK - LOG_BYTES_IN_PAGE as usize)))
                / 256;
            if predicted_survival >= available_to_space {
                self.gc_cause
                    .store(GCCause::ImmixSpaceFull, Ordering::Relaxed);
                return true;
            }
        }
        if crate::args::LXR_RC_ONLY {
            let inc_overflow = crate::args()
                .incs_limit
                .map(|x| self.rc.inc_buffer_size() >= x)
                .unwrap_or(false);
            let alloc_overflow = self
                .nursery_blocks
                .map(|x| self.immix_space.block_allocation.nursery_blocks() >= x)
                .unwrap_or(false);
            if alloc_overflow || inc_overflow {
                if inc_overflow {
                    INCS_TRIGGERED.store(true, Ordering::SeqCst);
                }
                if alloc_overflow {
                    ALLOC_TRIGGERED.store(true, Ordering::SeqCst);
                }
                return true;
            }

            if crate::args::ENABLE_INITIAL_ALLOC_LIMIT
                && !INITIAL_GC_TRIGGERED.load(Ordering::SeqCst)
            {
                if self.immix_space.block_allocation.nursery_blocks() >= 1024 {
                    return true;
                }
            }
        }
        // inc limits
        if !crate::args::LXR_RC_ONLY
            && crate::args()
                .incs_limit
                .map(|x| self.rc.inc_buffer_size() >= x)
                .unwrap_or(false)
        {
            self.gc_cause.store(GCCause::Increments, Ordering::Relaxed);
            return true;
        }
        // fresh young blocks limits
        if !crate::args::LXR_RC_ONLY
            && self
                .nursery_blocks
                .map(|x| self.immix_space.block_allocation.nursery_blocks() >= x)
                .unwrap_or(false)
        {
            self.gc_cause
                .store(GCCause::FixedNursery, Ordering::Relaxed);
            return true;
        }
        // Concurrent tracing finished
        // if !crate::args::LXR_RC_ONLY
        //     && self.concurrent_marking_in_progress()
        //     && crate::concurrent_marking_packets_drained()
        // {
        //     return true;
        // }
        false
    }

    fn concurrent_collection_required(&self) -> bool {
        return false;
        // Don't do a GC until we finished the lazy reclaimation.
        // if !LazySweepingJobs::all_finished() {
        //     return false;
        // }
        // self.concurrent_marking_enabled()
        //     && !crate::plan::barriers::BARRIER_MEASUREMENT
        //     && !self.concurrent_marking_in_progress()
        //     && self.base().gc_status() == GcStatus::NotInGC
        //     && self.get_reserved_pages()
        //         >= self.get_total_pages() * crate::args().concurrent_marking_threshold / 100
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        if crate::args::LXR_RC_ONLY {
            return true;
        }
        let x = self.previous_pause.load(Ordering::SeqCst);
        x == Some(Pause::FullTraceFast) || x == Some(Pause::FullTraceDefrag)
    }

    fn constraints(&self) -> &'static PlanConstraints {
        &LXR_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        use enum_map::enum_map;
        CopyConfig {
            copy_mapping: enum_map! {
                CopySemantics::DefaultCopy => CopySelector::Immix(0),
                _ => CopySelector::Unused,
            },
            space_mapping: vec![(CopySelector::Immix(0), &self.immix_space)],
            constraints: &LXR_CONSTRAINTS,
        }
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        #[cfg(feature = "nogc_no_zeroing")]
        if true {
            unreachable!();
        }
        if !crate::LazySweepingJobs::all_finished() {
            crate::COUNTERS
                .gc_with_unfinished_lazy_jobs
                .fetch_add(1, Ordering::Relaxed);
        }
        let pause =
            self.select_collection_kind(self.base().gc_requester.is_concurrent_collection());
        if self.concurrent_marking_in_progress() && pause == Pause::RefCount {
            crate::COUNTERS
                .rc_during_satb
                .fetch_add(1, Ordering::SeqCst);
        }
        let gc_cause = if self.is_emergency_collection() {
            GCCause::Emergency
        } else if self.base().is_user_triggered_collection() {
            GCCause::UserTriggered
        } else {
            self.gc_cause.load(Ordering::SeqCst)
        };
        gc_log!([3] "GC({}) GC Cause {:?}", crate::GC_EPOCH.load(Ordering::SeqCst), gc_cause);
        gc_log!([2]
            "GC({}) {:?} start. incs={} young-blocks={}({}M)",
            crate::GC_EPOCH.load(Ordering::SeqCst),
            pause,
            self.rc.inc_buffer_size(),
            self.immix_space.block_allocation.nursery_blocks(),
            self.immix_space.block_allocation.nursery_blocks() / 32,
        );
        self.dump_heap_usage();
        match pause {
            Pause::FullTraceFast => self
                .schedule_emergency_full_heap_collection::<RCImmixCollectRootEdges<VM>>(scheduler),
            Pause::FullTraceDefrag => unreachable!(),
            Pause::RefCount => self.schedule_rc_collection(scheduler),
            Pause::InitialMark => self.schedule_concurrent_marking_initial_pause(scheduler),
            Pause::FinalMark => self.schedule_concurrent_marking_final_pause(scheduler),
        }

        // Analysis routine that is ran. It is generally recommended to take advantage
        // of the scheduling system we have in place for more performance
        #[cfg(feature = "analysis")]
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(GcHookWork);
        // Resume mutators
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            #[cfg(feature = "sanity")]
            scheduler.work_buckets[WorkBucketStage::Final].add(ScheduleSanityGC::<Self>::new(self));
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &*ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        crate::stat(|s| {
            if pause == Pause::RefCount {
                s.rc_pauses += 1
            }
        });
        if pause == Pause::FinalMark || pause == Pause::FullTraceFast {
            self.common.los.is_end_of_satb_or_full_gc = true;
        }
        self.common.prepare(
            tls,
            pause == Pause::FullTraceFast || pause == Pause::InitialMark,
        );
        if crate::args::RC_MATURE_EVACUATION
            && (pause == Pause::FinalMark || pause == Pause::FullTraceFast)
        {
            self.immix_space.process_mature_evacuation_remset();
            self.immix_space.scheduler().work_buckets[WorkBucketStage::RCEvacuateMature]
                .add(FlushMatureEvacRemsets);
        }
        self.immix_space.prepare_rc(pause);
    }

    fn release(&mut self, tls: VMWorkerThread) {
        let new_ratio = super::SURVIVAL_RATIO_PREDICTOR.update_ratio();
        gc_log!([3] " - updated survival ratio: {}", new_ratio);
        let pause = self.current_pause().unwrap();
        VM::VMCollection::update_weak_processor(
            pause == Pause::RefCount || pause == Pause::InitialMark,
        );
        self.common.los.is_end_of_satb_or_full_gc = false;
        self.common.release(
            tls,
            pause == Pause::FullTraceFast || pause == Pause::FinalMark,
        );
        self.immix_space.release_rc(pause);
        // swap roots
        let mut prev_roots = self.prev_roots.write().unwrap();
        let mut curr_roots = self.curr_roots.write().unwrap();
        std::mem::swap::<SegQueue<_>>(&mut prev_roots, &mut curr_roots);
        debug_assert!(curr_roots.is_empty());
        // release the collected region
        self.last_gc_was_defrag.store(
            self.current_pause().unwrap() == Pause::FullTraceDefrag,
            Ordering::Relaxed,
        );
    }

    fn get_collection_reserved_pages(&self) -> usize {
        let predicated_survival = (self.immix_space.block_allocation.nursery_mb() as f64
            * super::SURVIVAL_RATIO_PREDICTOR.ratio()) as usize;
        let survival = predicated_survival << LOG_CONSERVATIVE_SURVIVAL_RATIO_MULTIPLER;
        return survival + self.immix_space.defrag_headroom_pages();
    }

    fn get_used_pages(&self) -> usize {
        self.immix_space.reserved_pages() + self.common.get_used_pages()
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.common.base
    }

    fn common(&self) -> &CommonPlan<VM> {
        &self.common
    }

    fn gc_pause_start(&self, _scheduler: &GCWorkScheduler<VM>) {
        // self.immix_space.flush_page_resource();
        crate::NO_EVAC.store(false, Ordering::SeqCst);
        let pause = self.current_pause().unwrap();

        super::SURVIVAL_RATIO_PREDICTOR
            .set_alloc_size(self.immix_space.block_allocation.nursery_blocks() << Block::LOG_BYTES);
        super::SURVIVAL_RATIO_PREDICTOR
            .pause_start
            .store(SystemTime::now(), Ordering::SeqCst);
        self.immix_space.rc_eager_prepare(pause);

        for mutator in <VM as VMBinding>::VMActivePlan::mutators() {
            mutator.flush();
        }

        if pause == Pause::FinalMark {
            self.set_concurrent_marking_state(false);
            if cfg!(feature = "satb_timer") {
                let t = crate::SATB_START
                    .load(Ordering::SeqCst)
                    .elapsed()
                    .unwrap()
                    .as_nanos();
                crate::COUNTERS.satb_nanos.fetch_add(t, Ordering::SeqCst);
            }
        } else if cfg!(feature = "satb_timer")
            && pause == Pause::RefCount
            && self.concurrent_marking_in_progress()
        {
            let t = crate::SATB_START
                .load(Ordering::SeqCst)
                .elapsed()
                .unwrap()
                .as_nanos();
            crate::COUNTERS.satb_nanos.fetch_add(t, Ordering::SeqCst);
        }
    }

    fn gc_pause_end(&self) {
        crate::DISABLE_LASY_DEC_FOR_CURRENT_GC.store(false, Ordering::SeqCst);
        // self.immix_space.flush_page_resource();
        let pause = self.current_pause().unwrap();
        if pause == Pause::InitialMark {
            self.set_concurrent_marking_state(true);
            crate::REMSET_RECORDING.store(true, Ordering::SeqCst);
            if cfg!(feature = "satb_timer") {
                crate::SATB_START.store(SystemTime::now(), Ordering::SeqCst)
            }
        } else if cfg!(feature = "satb_timer")
            && pause == Pause::RefCount
            && self.concurrent_marking_in_progress()
        {
            crate::SATB_START.store(SystemTime::now(), Ordering::SeqCst)
        }
        // if pause == Pause::RefCount || pause == Pause::InitialMark {
        //     self.resize_nursery();
        // }
        self.previous_pause.store(Some(pause), Ordering::SeqCst);
        self.current_pause.store(None, Ordering::SeqCst);
        crate::LAZY_SWEEPING_JOBS.write().swap();
        if crate::args::LAZY_DECREMENTS {
            let perform_cycle_collection =
                self.get_available_pages() < super::CYCLE_TRIGGER_THRESHOLD;
            self.next_gc_may_perform_cycle_collection
                .store(perform_cycle_collection, Ordering::SeqCst);
            self.next_gc_may_perform_emergency_collection
                .store(false, Ordering::SeqCst);
            self.perform_cycle_collection.store(false, Ordering::SeqCst);
        }
        self.avail_pages_at_end_of_last_gc
            .store(self.get_available_pages(), Ordering::SeqCst);
        HEAP_AFTER_GC.store(self.get_reserved_pages(), Ordering::SeqCst);
        self.dump_heap_usage();
        if cfg!(feature = "object_size_distribution") {
            if pause == Pause::FinalMark || pause == Pause::FullTraceFast {
                crate::dump_and_reset_obj_dist("Static", &mut crate::OBJ_COUNT.lock().unwrap());
            }
        }
    }

    #[cfg(feature = "nogc_no_zeroing")]
    fn handle_user_collection_request(&self, _tls: crate::util::VMMutatorThread, _force: bool) {
        println!("Warning: User attempted a collection request. The request is ignored.");
    }

    fn no_mutator_prepare_release(&self) -> bool {
        true
    }

    fn no_worker_prepare(&self) -> bool {
        true
    }

    fn fast_worker_release(&self) -> bool {
        true
    }

    fn should_process_reference(
        &self,
        reference: ObjectReference,
        referent: ObjectReference,
    ) -> bool {
        if self.rc.count(reference) == 0 || self.rc.count(referent) == 0 {
            return false;
        }
        true
    }

    fn discover_reference(&self, reference: ObjectReference, referent: ObjectReference) {
        // Keep weak references and referents alive during SATB.
        // They can only be swept by mature sweeping.
        let _ = self.rc.inc(reference);
        let _ = self.rc.inc(referent);
    }

    /// Collect all the CLD roots only at initial mark pause or full gc pause, as marking roots.
    /// For RC and final mark pauses, only collect modified CLD roots for increments.
    ///
    /// Note:
    ///  - Full/FinalMark pause: Use mark bit as liveness test when updating WeakProcessor
    ///  - Do not apply decs to all CLD roots
    ///
    /// TODO:
    ///  - Remset for CLDs. So we don't need to scan them all during RC pauses
    fn current_gc_should_scan_all_classloader_strong_roots(&self) -> bool {
        let pause = self.current_pause().unwrap();
        pause == Pause::InitialMark || pause == Pause::FullTraceFast
    }

    fn current_gc_should_prepare_for_class_unloading(&self) -> bool {
        let pause = self.current_pause().unwrap();
        pause == Pause::InitialMark || pause == Pause::FullTraceFast
    }

    fn current_gc_should_perform_class_unloading(&self) -> bool {
        let pause = self.current_pause().unwrap();
        pause == Pause::FinalMark || pause == Pause::FullTraceFast
    }
}

impl<VM: VMBinding> LXR<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Box<Self> {
        let immix_specs = metadata::extract_side_metadata(&[
            RC_LOCK_BIT_SPEC,
            MetadataSpec::OnSide(RC_TABLE),
            MetadataSpec::OnSide(
                *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                    .as_spec()
                    .extract_side_spec(),
            ),
        ]);
        let global_side_metadata_specs = SideMetadataContext::new_global_specs(&immix_specs);
        let options = args.options.clone();
        let mut plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &LXR_CONSTRAINTS,
            global_side_metadata_specs,
        };
        let mut immix_space =
            ImmixSpace::new(plan_args.get_space_args("immix", true, VMRequest::discontiguous()));
        immix_space.cm_enabled = true;
        let mut lxr = Box::new(LXR {
            immix_space,
            common: CommonPlan::new(plan_args),
            perform_cycle_collection: AtomicBool::new(false),
            next_gc_may_perform_cycle_collection: AtomicBool::new(false),
            next_gc_may_perform_emergency_collection: AtomicBool::new(false),
            current_pause: Atomic::new(None),
            previous_pause: Atomic::new(None),
            last_gc_was_defrag: AtomicBool::new(false),
            nursery_blocks: crate::args().nursery_blocks,
            avail_pages_at_end_of_last_gc: AtomicUsize::new(0),
            zeroing_packets_scheduled: AtomicBool::new(false),
            next_gc_selected: (Mutex::new(true), Condvar::new()),
            in_concurrent_marking: AtomicBool::new(false),
            prev_roots: Default::default(),
            curr_roots: Default::default(),
            rc: RefCountHelper::NEW,
            gc_cause: Atomic::new(GCCause::Unknown),
        });

        lxr.gc_init(&options);

        {
            let mut side_metadata_sanity_checker = SideMetadataSanity::new();
            lxr.common
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
            lxr.immix_space
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
        }

        lxr
    }

    pub fn concurrent_marking_enabled(&self) -> bool {
        self.immix_space.cm_enabled
    }

    pub fn concurrent_marking_in_progress(&self) -> bool {
        self.concurrent_marking_enabled() && self.in_concurrent_marking.load(Ordering::Relaxed)
    }

    fn decide_next_gc_may_perform_cycle_collection(&self, pause: Pause) {
        let (lock, cvar) = &self.next_gc_selected;
        let notify = || {
            let mut gc_selection_done = lock.lock().unwrap();
            *gc_selection_done = true;
            cvar.notify_one();
        };
        let pages_after_gc = HEAP_AFTER_GC.load(Ordering::SeqCst).saturating_sub(
            self.immix_space
                .num_clean_blocks_released_lazy
                .load(Ordering::SeqCst)
                << Block::LOG_PAGES,
        );
        if pause == Pause::FinalMark || pause == Pause::FullTraceFast {
            let live_mature_pages = super::MATURE_LIVE_PREDICTOR.update(pages_after_gc);
            gc_log!([3] " - predicted live mature pages: {}", live_mature_pages)
        }
        let live_mature_pages = super::MATURE_LIVE_PREDICTOR.live_pages() as usize;
        let garbage = if pages_after_gc > live_mature_pages {
            pages_after_gc - live_mature_pages
        } else {
            0
        };
        let total_pages = self.get_total_pages();
        let available_blocks = (total_pages - pages_after_gc) >> Block::LOG_PAGES;
        self.next_gc_may_perform_emergency_collection
            .store(false, Ordering::SeqCst);
        if !self.concurrent_marking_in_progress()
            && garbage * 100 >= crate::args().trace_threshold as usize * total_pages
            || available_blocks < crate::args().concurrent_marking_stop_blocks
        {
            self.next_gc_may_perform_cycle_collection
                .store(true, Ordering::SeqCst);
            if available_blocks < crate::args().concurrent_marking_stop_blocks {
                self.next_gc_may_perform_emergency_collection
                    .store(true, Ordering::SeqCst);
            }
            if self.concurrent_marking_enabled()
                && !self.concurrent_marking_in_progress()
                && !cfg!(feature = "sanity")
            {
                self.zeroing_packets_scheduled.store(true, Ordering::SeqCst);
                self.immix_space
                    .schedule_mark_table_zeroing_tasks(WorkBucketStage::Unconstrained);
            }
        } else {
            self.next_gc_may_perform_cycle_collection
                .store(false, Ordering::SeqCst);
        }
        notify();
    }

    fn select_lxr_collection_kind(&self, emergency: bool) -> Pause {
        {
            let (lock, cvar) = &self.next_gc_selected;
            let mut gc_selection_done = lock.lock().unwrap();
            while !*gc_selection_done {
                gc_selection_done = cvar.wait(gc_selection_done).unwrap();
            }
            *gc_selection_done = false;
        }
        let concurrent_marking_in_progress = self.concurrent_marking_in_progress();
        let concurrent_marking_packets_drained = crate::concurrent_marking_packets_drained();
        gc_log!([2]
            " - next gc mey perform: cycle_collection = {}, emergency_collection = {}",
            self.next_gc_may_perform_cycle_collection.load(Ordering::Relaxed),
            self.next_gc_may_perform_emergency_collection.load(Ordering::Relaxed),
        );

        // If CM is finished, do a final mark pause
        if self.concurrent_marking_enabled()
            && concurrent_marking_in_progress
            && concurrent_marking_packets_drained
        {
            return Pause::FinalMark;
        }
        // Either final mark pause or full pause for emergency GC
        if emergency || self.base().is_user_triggered_collection() {
            return if self.concurrent_marking_enabled() && concurrent_marking_in_progress {
                Pause::FinalMark
            } else {
                Pause::FullTraceFast
            };
        }
        if self
            .next_gc_may_perform_cycle_collection
            .load(Ordering::Relaxed)
            && concurrent_marking_in_progress
        {
            return Pause::FinalMark;
        }
        // Should trigger CM?
        if self
            .next_gc_may_perform_cycle_collection
            .load(Ordering::Relaxed)
            && !concurrent_marking_in_progress
        {
            return if self.concurrent_marking_enabled()
                && !self
                    .next_gc_may_perform_emergency_collection
                    .load(Ordering::Relaxed)
            {
                Pause::InitialMark
            } else {
                Pause::FullTraceFast
            };
        } else {
            return Pause::RefCount;
        }
    }

    #[allow(clippy::collapsible_else_if)]
    fn select_collection_kind(&self, _concurrent: bool) -> Pause {
        if crate::args::ENABLE_INITIAL_ALLOC_LIMIT {
            INITIAL_GC_TRIGGERED.store(true, Ordering::SeqCst);
        }
        {
            let o = Ordering::SeqCst;
            if SURVIVAL_TRIGGERED.load(o) {
                crate::COUNTERS.survival_triggerd.fetch_add(1, o);
            } else if INCS_TRIGGERED.load(o) {
                crate::COUNTERS.incs_triggerd.fetch_add(1, o);
            } else if ALLOC_TRIGGERED.load(o) {
                crate::COUNTERS.alloc_triggerd.fetch_add(1, o);
            } else {
                crate::COUNTERS.overflow_triggerd.fetch_add(1, o);
            }
        }
        self.base().set_collection_kind::<Self>(self);
        self.base().set_gc_status(GcStatus::GcPrepare);
        let emergency_collection = (self.base().cur_collection_attempts.load(Ordering::SeqCst) > 1)
            || self.is_emergency_collection()
            || *self.base().options.full_heap_system_gc;

        let concurrent_marking_packets_drained = crate::concurrent_marking_packets_drained();
        let pause = if crate::args::LXR_RC_ONLY {
            Pause::RefCount
        } else {
            let pause = self.select_lxr_collection_kind(emergency_collection);
            if (pause == Pause::InitialMark || pause == Pause::FullTraceFast)
                && !self.zeroing_packets_scheduled.load(Ordering::SeqCst)
            {
                self.immix_space
                    .schedule_mark_table_zeroing_tasks(WorkBucketStage::RCProcessIncs);
            }
            self.zeroing_packets_scheduled
                .store(false, Ordering::SeqCst);
            if emergency_collection {
                crate::COUNTERS.emergency.fetch_add(1, Ordering::Relaxed);
            }
            pause
        };
        if pause == Pause::FinalMark && !concurrent_marking_packets_drained {
            crate::COUNTERS
                .cm_early_quit
                .fetch_add(1, Ordering::Relaxed);
        }
        match pause {
            Pause::RefCount => crate::COUNTERS.rc.fetch_add(1, Ordering::Relaxed),
            Pause::InitialMark => crate::COUNTERS.initial_mark.fetch_add(1, Ordering::Relaxed),
            Pause::FinalMark => crate::COUNTERS.final_mark.fetch_add(1, Ordering::Relaxed),
            _ => crate::COUNTERS.full.fetch_add(1, Ordering::Relaxed),
        };
        self.current_pause.store(Some(pause), Ordering::SeqCst);
        self.perform_cycle_collection
            .store(pause != Pause::RefCount, Ordering::SeqCst);
        pause
    }

    fn disable_unnecessary_buckets(&'static self, scheduler: &GCWorkScheduler<VM>, pause: Pause) {
        if pause == Pause::RefCount {
            scheduler.work_buckets[WorkBucketStage::Prepare].set_as_disabled();
        }
        if pause != Pause::FullTraceFast {
            scheduler.work_buckets[WorkBucketStage::ResetRC].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::PrepareForSecondClosure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::ForwardClosure].set_as_disabled();
        }
        if pause == Pause::RefCount || pause == Pause::InitialMark {
            scheduler.work_buckets[WorkBucketStage::Closure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::WeakRefClosure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::FinalRefClosure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::PhantomRefClosure].set_as_disabled();
        }
        scheduler.work_buckets[WorkBucketStage::VMRefClosure].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::VMRefForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::SoftRefClosure].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::CalculateForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::SecondRoots].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::RefForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::FinalizableForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::Compact].set_as_disabled();
        if crate::args::LAZY_DECREMENTS && pause != Pause::FullTraceFast {
            scheduler.work_buckets[WorkBucketStage::STWRCDecsAndSweep].set_as_disabled();
        }
    }

    fn schedule_rc_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        self.disable_unnecessary_buckets(scheduler, Pause::RefCount);
        #[allow(clippy::collapsible_if)]
        if self.concurrent_marking_enabled() && !crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING
        {
            if self.concurrent_marking_in_progress() {
                scheduler.pause_concurrent_marking_work_packets_during_gc();
            }
        }
        type E<VM> = RCImmixCollectRootEdges<VM>;
        // Before start yielding, wrap all the roots from the previous GC with work-packets.
        self.process_prev_roots(scheduler);
        // Stop & scan mutators (mutator scanning can happen before STW)
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E<VM>>::new());
        // Prepare global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::RCProcessIncs].add(FastRCPrepare);
        // Release global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<LXRGCWorkContext<VM>>::new(self));
    }

    fn schedule_concurrent_marking_initial_pause(&'static self, scheduler: &GCWorkScheduler<VM>) {
        self.disable_unnecessary_buckets(scheduler, Pause::InitialMark);
        self.process_prev_roots(scheduler);
        scheduler.work_buckets[WorkBucketStage::Unconstrained]
            .add(StopMutators::<RCImmixCollectRootEdges<VM>>::new());
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<LXRGCWorkContext<VM>>::new(self));
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<LXRGCWorkContext<VM>>::new(self));
    }

    fn schedule_concurrent_marking_final_pause(&'static self, scheduler: &GCWorkScheduler<VM>) {
        self.disable_unnecessary_buckets(scheduler, Pause::FinalMark);
        if self.concurrent_marking_in_progress() {
            crate::MOVE_CONCURRENT_MARKING_TO_STW.store(true, Ordering::SeqCst);
        }
        self.process_prev_roots(scheduler);
        scheduler.work_buckets[WorkBucketStage::Unconstrained]
            .add(StopMutators::<RCImmixCollectRootEdges<VM>>::new());

        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<LXRGCWorkContext<VM>>::new(self));
        scheduler.work_buckets[WorkBucketStage::Prepare].add(UpdateWeakProcessor);
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<LXRGCWorkContext<VM>>::new(self));
        scheduler.schedule_ref_proc_work::<LXRWeakRefWorkContext<VM>>(self);
    }

    fn schedule_emergency_full_heap_collection<E: ProcessEdgesWork<VM = VM>>(
        &'static self,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        crate::DISABLE_LASY_DEC_FOR_CURRENT_GC.store(true, Ordering::SeqCst);
        self.disable_unnecessary_buckets(scheduler, Pause::FullTraceFast);
        // Before start yielding, drop all the dec buffers
        self.drop_prev_roots();
        scheduler.work_buckets[WorkBucketStage::Prepare].add(UpdateWeakProcessor);
        // Stop & scan mutators (mutator scanning can happen before STW)
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E>::new());
        // Reset RC table
        super::emergency::schedule_first_pass_preparation_tasks(scheduler);
        // Prepare global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<LXRGCWorkContext<VM>>::new(self));
        // reset mark for second pass
        super::emergency::schedule_second_pass_preparation_tasks(scheduler);
        // Release global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<LXRGCWorkContext<VM>>::new(self));
        scheduler.schedule_ref_proc_work::<LXREmergencyWeakRefWorkContext<VM>>(self);
    }

    fn drop_prev_roots(&self) {
        let mut prev_roots = self.prev_roots.write().unwrap();
        *prev_roots = Default::default();
    }

    fn process_prev_roots(&self, scheduler: &GCWorkScheduler<VM>) {
        let prev_roots = self.prev_roots.write().unwrap();
        let mut work_packets: Vec<Box<dyn GCWork<VM>>> = Vec::with_capacity(prev_roots.len());
        while let Some(decs) = prev_roots.pop() {
            work_packets.push(Box::new(ProcessDecs::new(
                decs,
                LazySweepingJobsCounter::new_decs(),
            )))
        }
        if work_packets.is_empty() {
            work_packets.push(Box::new(ProcessDecs::new(
                vec![],
                LazySweepingJobsCounter::new_decs(),
            )));
        }
        if crate::args::LAZY_DECREMENTS {
            debug_assert!(!crate::args::BARRIER_MEASUREMENT);
            scheduler.postpone_all_prioritized(work_packets);
        } else {
            scheduler.work_buckets[WorkBucketStage::STWRCDecsAndSweep].bulk_add(work_packets);
        }
    }

    pub fn perform_cycle_collection(&self) -> bool {
        self.perform_cycle_collection.load(Ordering::SeqCst)
    }

    pub fn current_pause(&self) -> Option<Pause> {
        self.current_pause.load(Ordering::SeqCst)
    }

    pub fn previous_pause(&self) -> Option<Pause> {
        self.previous_pause.load(Ordering::SeqCst)
    }

    pub fn in_defrag(&self, o: ObjectReference) -> bool {
        self.immix_space.in_space(o) && Block::in_defrag_block::<VM>(o)
    }

    pub fn address_in_defrag(&self, a: Address) -> bool {
        self.immix_space.address_in_space(a) && Block::address_in_defrag_block(a)
    }

    pub fn mark(&self, o: ObjectReference) -> bool {
        debug_assert!(!o.is_null());
        if self.immix_space.in_space(o) {
            self.immix_space.attempt_mark(o)
        } else {
            self.common.los.attempt_mark(o)
        }
    }

    pub fn mark2(&self, o: ObjectReference, los: bool) -> bool {
        debug_assert!(!o.is_null());
        if !los {
            self.immix_space.attempt_mark(o)
        } else {
            self.common.los.attempt_mark(o)
        }
    }

    pub fn is_marked(&self, o: ObjectReference) -> bool {
        debug_assert!(!o.is_null());
        if self.immix_space.in_space(o) {
            self.immix_space.is_marked(o)
        } else {
            self.common.los.is_marked(o)
        }
    }

    pub fn los(&self) -> &LargeObjectSpace<VM> {
        &self.common.los
    }

    fn gc_init(&mut self, options: &Options) {
        crate::args::validate_features(BarrierSelector::FieldBarrier, options);
        self.immix_space.cm_enabled = !cfg!(feature = "lxr_no_cm");
        self.immix_space.rc_enabled = true;
        self.common.los.rc_enabled = true;
        unsafe {
            let me = &*(self as *const Self);
            self.immix_space.block_allocation.lxr = Some(me);
        }
        let mut lazy_sweeping_jobs = crate::LAZY_SWEEPING_JOBS.write();
        lazy_sweeping_jobs.swap();
        lazy_sweeping_jobs.end_of_decs = Some(Box::new(move |c| {
            let lxr = GCWorker::<VM>::current()
                .mmtk
                .get_plan()
                .downcast_ref::<Self>()
                .unwrap();
            lxr.immix_space.schedule_rc_block_sweeping_tasks(c);
        }));
        lazy_sweeping_jobs.end_of_lazy = Some(Box::new(move || {
            let lxr = GCWorker::<VM>::current()
                .mmtk
                .get_plan()
                .downcast_ref::<Self>()
                .unwrap();
            lxr.immix_space.flush_page_resource();
            let released_blocks = lxr
                .immix_space
                .num_clean_blocks_released_lazy
                .load(Ordering::SeqCst);
            let released_los_pages = lxr.los().num_pages_released_lazy.load(Ordering::SeqCst);
            let total_released_bytes =
                (released_blocks << Block::LOG_BYTES) + (released_los_pages << LOG_BYTES_IN_PAGE);
            gc_log!([2]
                " - lazy jobs finished since-gc-start={:.3}ms, current-reserved-heap={}M({}M), released-blocks={}, released-los-pages={}, total-released={}",
                crate::gc_start_time_ms(),
                lxr.get_reserved_pages() / 256,
                lxr.get_total_pages() / 256,
                released_blocks,
                released_los_pages,
                if total_released_bytes < BYTES_IN_KBYTE {
                    format!("{}B", total_released_bytes)
                } else if total_released_bytes < BYTES_IN_MBYTE {
                    format!("{}K", total_released_bytes >> LOG_BYTES_IN_KBYTE)
                } else if total_released_bytes < (1 << 30) {
                    format!("{}M", total_released_bytes >> LOG_BYTES_IN_MBYTE)
                } else {
                    format!("{}G", total_released_bytes >> 30)
                }
            );
            gc_log!([2]
                " - num_clean_blocks_released_lazy = {}",
                lxr.immix_space
                .num_clean_blocks_released_lazy
                .load(Ordering::SeqCst)
            );
            // Update counters
            if !crate::args::LAZY_DECREMENTS {
                HEAP_AFTER_GC.store(lxr.get_used_pages(), Ordering::SeqCst);
            }
            {
                let o = Ordering::Relaxed;
                let used_pages_after_gc = HEAP_AFTER_GC.load(Ordering::SeqCst);
                let lazy_released_pages = lxr
                    .immix_space
                    .num_clean_blocks_released_lazy
                    .load(Ordering::SeqCst)
                    << Block::LOG_PAGES;
                let x = if used_pages_after_gc >= lazy_released_pages {
                    used_pages_after_gc - lazy_released_pages
                } else {
                    0
                };
                crate::COUNTERS.total_used_pages.store(
                    crate::COUNTERS.total_used_pages.load(o) + x,
                    Ordering::Relaxed,
                );
                let min = crate::COUNTERS.min_used_pages.load(o);
                crate::COUNTERS.min_used_pages.store(usize::min(x, min), o);
                let max = crate::COUNTERS.max_used_pages.load(o);
                crate::COUNTERS.max_used_pages.store(usize::max(x, max), o);
            }
            let pause = if lxr.current_pause().is_none() {
                lxr.previous_pause().unwrap()
            } else {
                lxr.current_pause().unwrap()
            };
            lxr.decide_next_gc_may_perform_cycle_collection(pause);
        }));

        if let Some(nursery_ratio) = crate::args().nursery_ratio {
            let heap_size = match *options.gc_trigger {
                GCTriggerSelector::FixedHeapSize(x) => x,
                _ => unimplemented!(),
            };
            let total_blocks = heap_size >> Block::LOG_BYTES;
            let nursery_blocks = total_blocks / (nursery_ratio + 1);
            self.nursery_blocks = Some(nursery_blocks);
        }
    }

    fn set_concurrent_marking_state(&self, active: bool) {
        <VM as VMBinding>::VMCollection::set_concurrent_marking_state(active);
        self.in_concurrent_marking.store(active, Ordering::SeqCst);
    }

    pub fn options(&self) -> &Options {
        &self.common.base.options
    }

    pub fn dump_heap_usage(&self) {
        gc_log!([3]
            " - reserved={}M (ix-{}M, los-{}M) collection_reserve={}M defrag_headroom={}M ix_avail={}M vmmap_avail={}M",
            self.get_reserved_pages() / 256,
            self.immix_space.reserved_pages() / 256,
            self.los().reserved_pages() / 256,
            self.get_collection_reserved_pages() / 256,
            self.immix_space.defrag_headroom_pages() / 256,
            self.immix_space.pr.available_pages() / 256,
            if self.immix_space.common().contiguous {
                0
            } else {
                (VM_MAP.available_chunks() << (LOG_BYTES_IN_CHUNK - LOG_BYTES_IN_PAGE as usize)) / 256
            },
        );
    }
}
