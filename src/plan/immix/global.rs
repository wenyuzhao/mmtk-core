use super::gc_work::{ImmixCopyContext, ImmixGCWorkContext, ImmixProcessEdges, TraceKind};
use super::mutator::ALLOCATOR_MAPPING;
use super::Pause;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::GcStatus;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::immix::block::Block;
use crate::policy::immix::cset::{CollectionSet, PerRegionRemSet};
use crate::policy::immix::MatureSweeping;
use crate::policy::largeobjectspace::LargeObjectSpace;
use crate::policy::space::Space;
use crate::scheduler::gc_work::*;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
#[cfg(feature = "analysis")]
use crate::util::analysis::GcHookWork;
use crate::util::cm::CMImmixCollectRootEdges;
use crate::util::heap::layout::heap_layout::Mmapper;
use crate::util::heap::layout::heap_layout::VMMap;
use crate::util::heap::layout::vm_layout_constants::{HEAP_END, HEAP_START};
use crate::util::heap::HeapMeta;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSanity;
use crate::util::metadata::MetadataSpec;
use crate::util::options::UnsafeOptionsWrapper;
use crate::util::rc::{self, ProcessDecs, RCImmixCollectRootEdges};
use crate::util::rc::{RC_LOCK_BIT_SPEC, RC_TABLE};
#[cfg(feature = "sanity")]
use crate::util::sanity::sanity_checker::*;
use crate::util::{metadata, ObjectReference};
use crate::vm::{ObjectModel, VMBinding};
use crate::{mmtk::MMTK, policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use crate::{BarrierSelector, LazySweepingJobs, LazySweepingJobsCounter};
use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Condvar, Mutex};
use std::time::SystemTime;

use atomic::{Atomic, Ordering};
use enum_map::EnumMap;
use spin::Lazy;

pub const ALLOC_IMMIX: AllocationSemantics = AllocationSemantics::Default;

static INITIAL_GC_TRIGGERED: AtomicBool = AtomicBool::new(false);
static INCS_TRIGGERED: AtomicBool = AtomicBool::new(false);
static ALLOC_TRIGGERED: AtomicBool = AtomicBool::new(false);
static SIMPLE_INCREMENTAL_DEFRAG_COUNTER: AtomicUsize = AtomicUsize::new(0);
static HEAP_AFTER_GC: AtomicUsize = AtomicUsize::new(0);

pub struct Immix<VM: VMBinding> {
    pub immix_space: ImmixSpace<VM>,
    pub common: CommonPlan<VM>,
    /// Always true for non-rc immix.
    /// For RC immix, this is used for enable backup tracing.
    perform_cycle_collection: AtomicBool,
    current_pause: Atomic<Option<Pause>>,
    previous_pause: Atomic<Option<Pause>>,
    next_gc_may_perform_cycle_collection: AtomicBool,
    last_gc_was_defrag: AtomicBool,
    nursery_blocks: Option<usize>,
    inc_buffer_limit: Option<usize>,
    avail_pages_at_end_of_last_gc: AtomicUsize,
    cm_threshold: usize,
    zeroing_packets_scheduled: AtomicBool,
    next_gc_selected: (Mutex<bool>, Condvar),
}

pub static ACTIVE_BARRIER: Lazy<BarrierSelector> = Lazy::new(|| {
    if crate::plan::barriers::BARRIER_MEASUREMENT {
        match env::var("BARRIER") {
            Ok(s) if s == "ObjectBarrier" => BarrierSelector::ObjectBarrier,
            Ok(s) if s == "NoBarrier" => BarrierSelector::NoBarrier,
            Ok(s) if s == "FieldBarrier" => BarrierSelector::FieldLoggingBarrier,
            _ => unreachable!("Please explicitly specify barrier"),
        }
    } else if super::CONCURRENT_MARKING || super::REF_COUNT {
        BarrierSelector::FieldLoggingBarrier
    } else {
        BarrierSelector::NoBarrier
    }
});

pub static IMMIX_CONSTRAINTS: Lazy<PlanConstraints> = Lazy::new(|| PlanConstraints {
    moves_objects: true,
    gc_header_bits: 2,
    gc_header_words: 0,
    num_specialized_scans: 1,
    /// Max immix object size is half of a block.
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
    barrier: *ACTIVE_BARRIER,
    needs_log_bit: crate::args::BARRIER_MEASUREMENT
        || *ACTIVE_BARRIER != BarrierSelector::NoBarrier,
    needs_field_log_bit: *ACTIVE_BARRIER == BarrierSelector::FieldLoggingBarrier
        || (crate::args::BARRIER_MEASUREMENT && *ACTIVE_BARRIER == BarrierSelector::NoBarrier),
    ..PlanConstraints::default()
});

impl<VM: VMBinding> Plan for Immix<VM> {
    type VM = VM;

    fn collection_required(&self, space_full: bool, space: &dyn Space<Self::VM>) -> bool {
        // Don't do a GC until we finished the lazy reclaimation.
        // if crate::args::HEAP_HEALTH_GUIDED_GC && !LazySweepingJobs::all_finished() {
        //     return false;
        // }
        // Spaces or heap full
        if self.base().collection_required(self, space_full, space) {
            if !crate::args::HEAP_HEALTH_GUIDED_GC {
                self.next_gc_may_perform_cycle_collection
                    .store(true, Ordering::SeqCst);
            }
            return true;
        }
        // Alloc limits
        if crate::args::REF_COUNT
            && self
                .nursery_blocks
                .map(|x| self.immix_space.block_allocation.nursery_blocks() >= x)
                .unwrap_or(false)
        {
            ALLOC_TRIGGERED.store(true, Ordering::SeqCst);
            return true;
        }
        // Inc limits
        if crate::args::REF_COUNT
            && self
                .inc_buffer_limit
                .map(|x| rc::inc_buffer_size() >= x)
                .unwrap_or(false)
        {
            INCS_TRIGGERED.store(true, Ordering::SeqCst);
            return true;
        }
        // RC-only LXR can only have alloc/incs/fullheap trigger
        if crate::args::LXR_RC_ONLY {
            assert!(!crate::args::ENABLE_INITIAL_ALLOC_LIMIT);
            return false;
        }
        // Concurrent tracing finished
        if !crate::args::HEAP_HEALTH_GUIDED_GC
            && crate::args::CONCURRENT_MARKING
            && crate::concurrent_marking_in_progress()
            && crate::concurrent_marking_packets_drained()
        {
            return true;
        }
        false
    }

    fn concurrent_collection_required(&self) -> bool {
        if crate::args::HEAP_HEALTH_GUIDED_GC || crate::args::LXR_RC_ONLY {
            return false;
        }
        // Don't do a GC until we finished the lazy reclaimation.
        if !LazySweepingJobs::all_finished() {
            return false;
        }
        super::CONCURRENT_MARKING
            && !crate::plan::barriers::BARRIER_MEASUREMENT
            && !crate::concurrent_marking_in_progress()
            && self.base().gc_status() == GcStatus::NotInGC
            && self.get_pages_reserved()
                >= self.get_total_pages() * *crate::args::CONCURRENT_MARKING_THRESHOLD / 100
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        if crate::args::LXR_RC_ONLY {
            return true;
        }
        let x = self.previous_pause.load(Ordering::SeqCst);
        x == Some(Pause::FullTraceFast) || x == Some(Pause::FullTraceDefrag)
    }

    fn constraints(&self) -> &'static PlanConstraints {
        &IMMIX_CONSTRAINTS
    }

    fn create_worker_local(
        &self,
        tls: VMWorkerThread,
        mmtk: &'static MMTK<Self::VM>,
    ) -> GCWorkerLocalPtr {
        let mut c = ImmixCopyContext::new(mmtk);
        c.init(tls);
        GCWorkerLocalPtr::new(c)
    }

    fn gc_init(
        &mut self,
        heap_size: usize,
        vm_map: &'static VMMap,
        scheduler: &Arc<GCWorkScheduler<VM>>,
    ) {
        crate::args::validate_features(*ACTIVE_BARRIER);
        self.common.gc_init(heap_size, vm_map, scheduler);
        self.immix_space.init(vm_map);
        unsafe {
            crate::LAZY_SWEEPING_JOBS.init();
            crate::LAZY_SWEEPING_JOBS.swap();
            let me = &*(self as *const Self);
            crate::LAZY_SWEEPING_JOBS.end_of_decs = Some(box move |c| {
                me.immix_space.schedule_rc_block_sweeping_tasks(c);
            });
            crate::LAZY_SWEEPING_JOBS.end_of_lazy = Some(box move || {
                if crate::args::LOG_PER_GC_STATE {
                    println!(
                        " - lazy jobs done, heap {:?}M",
                        me.get_pages_reserved() / 256
                    );
                }
                // Update counters
                {
                    let o = Ordering::Relaxed;
                    let used_pages_after_gc = HEAP_AFTER_GC.load(Ordering::SeqCst);
                    let lazy_released_pages = me
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
                me.decide_next_gc_may_perform_cycle_collection();
            });
        }
        if let Some(nursery_ratio) = *crate::args::NURSERY_RATIO {
            let total_blocks = heap_size >> Block::LOG_BYTES;
            let nursery_blocks = total_blocks / (nursery_ratio + 1);
            self.nursery_blocks = Some(nursery_blocks);
        }
        if let Some(inc_buffer_limit) = *crate::args::INC_BUFFER_LIMIT {
            self.inc_buffer_limit = Some(inc_buffer_limit);
        }
        self.cm_threshold = *crate::args::CONCURRENT_MARKING_THRESHOLD;
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
        let pause = self.select_collection_kind(
            self.base()
                .control_collector_context
                .is_concurrent_collection(),
        );
        if crate::concurrent_marking_in_progress() && pause == Pause::RefCount {
            crate::COUNTERS
                .rc_during_satb
                .fetch_add(1, Ordering::SeqCst);
        }
        if crate::args::LOG_PER_GC_STATE {
            let boot_time = crate::BOOT_TIME.elapsed().unwrap().as_millis() as f64 / 1000f64;
            println!(
                "[{:.3}s][pause] {:?} {}",
                boot_time,
                pause,
                crate::util::rc::inc_buffer_size()
            );
        }
        match pause {
            Pause::FullTraceFast => {
                if crate::args::REF_COUNT {
                    self.schedule_immix_collection::<RCImmixCollectRootEdges<VM>, { TraceKind::Fast }>(scheduler)
                } else {
                    self.schedule_immix_collection::<ImmixProcessEdges<VM, { TraceKind::Fast }>, { TraceKind::Fast }>(
                        scheduler,
                    )
                }
            }
            Pause::FullTraceDefrag => self
                .schedule_immix_collection::<ImmixProcessEdges<VM, { TraceKind::Defrag }>, { TraceKind::Defrag }>(
                    scheduler,
                ),
            Pause::RefCount => self.schedule_rc_collection(scheduler),
            Pause::InitialMark => self.schedule_concurrent_marking_initial_pause(scheduler),
            Pause::FinalMark => self.schedule_concurrent_marking_final_pause(scheduler),
        }

        // Analysis routine that is ran. It is generally recommended to take advantage
        // of the scheduling system we have in place for more performance
        #[cfg(feature = "analysis")]
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(GcHookWork);
        // Resume mutators
        #[cfg(feature = "sanity")]
        scheduler.work_buckets[WorkBucketStage::Final]
            .add(ScheduleSanityGC::<Self, ImmixCopyContext<VM>>::new(self));

        scheduler.set_finalizer(Some(EndOfGC));
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
        if !super::REF_COUNT {
            if pause != Pause::FinalMark {
                self.common.prepare(tls, true);
                self.immix_space.prepare(true, true);
            }
        } else {
            self.common.prepare(
                tls,
                pause == Pause::FullTraceFast || pause == Pause::InitialMark,
            );
            self.immix_space.prepare_rc(pause);
        }
    }

    fn release(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        if !super::REF_COUNT {
            if pause != Pause::InitialMark {
                self.common.release(tls, true);
                self.immix_space.release(true);
            }
        } else {
            self.common.release(
                tls,
                pause == Pause::FullTraceFast || pause == Pause::FinalMark,
            );
            self.immix_space.release_rc(pause);
        }
        if super::REF_COUNT {
            unsafe {
                std::mem::swap(&mut super::CURR_ROOTS, &mut super::PREV_ROOTS);
                debug_assert!(super::CURR_ROOTS.is_empty());
            }
        }
        // release the collected region
        self.last_gc_was_defrag.store(
            self.current_pause().unwrap() == Pause::FullTraceDefrag,
            Ordering::Relaxed,
        );
    }

    fn get_collection_reserve(&self) -> usize {
        self.immix_space.defrag_headroom_pages()
    }

    fn get_pages_used(&self) -> usize {
        self.immix_space.reserved_pages() + self.common.get_pages_used()
    }

    #[inline(always)]
    fn base(&self) -> &BasePlan<VM> {
        &self.common.base
    }

    #[inline(always)]
    fn common(&self) -> &CommonPlan<VM> {
        &self.common
    }

    fn gc_pause_start(&self) {
        self.immix_space.pr.flush_all();
        crate::NO_EVAC.store(false, Ordering::SeqCst);
        let pause = self.current_pause().unwrap();
        if crate::args::REF_COUNT {
            let me = unsafe { &mut *(self as *const _ as *mut Self) };
            me.immix_space.rc_eager_prepare(pause);
        }
        if crate::args::REF_COUNT {
            let scheduler = self.base().control_collector_context.scheduler();
            scheduler.work_buckets[WorkBucketStage::FinishConcurrentWork].activate();
            if pause == Pause::RefCount {
                // scheduler.work_buckets[WorkBucketStage::Initial].activate();
            }
            scheduler.work_buckets[WorkBucketStage::FinishConcurrentWork].notify_all_workers();
        }
        if pause == Pause::FinalMark {
            crate::IN_CONCURRENT_GC.store(false, Ordering::SeqCst);
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
            && crate::concurrent_marking_in_progress()
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
        self.immix_space.pr.flush_all();
        let pause = self.current_pause().unwrap();
        if pause == Pause::InitialMark {
            PerRegionRemSet::enable_recording();
            crate::IN_CONCURRENT_GC.store(true, Ordering::SeqCst);
            if cfg!(feature = "satb_timer") {
                crate::SATB_START.store(SystemTime::now(), Ordering::SeqCst)
            }
        } else if cfg!(feature = "satb_timer")
            && pause == Pause::RefCount
            && crate::concurrent_marking_in_progress()
        {
            crate::SATB_START.store(SystemTime::now(), Ordering::SeqCst)
        }
        // if pause == Pause::RefCount || pause == Pause::InitialMark {
        //     self.resize_nursery();
        // }
        self.previous_pause.store(Some(pause), Ordering::SeqCst);
        self.current_pause.store(None, Ordering::SeqCst);
        unsafe {
            crate::LAZY_SWEEPING_JOBS.swap();
        };
        let perform_cycle_collection = self.get_pages_avail() < super::CYCLE_TRIGGER_THRESHOLD;
        self.next_gc_may_perform_cycle_collection
            .store(perform_cycle_collection, Ordering::SeqCst);
        self.perform_cycle_collection.store(false, Ordering::SeqCst);
        self.avail_pages_at_end_of_last_gc
            .store(self.get_pages_avail(), Ordering::SeqCst);
        HEAP_AFTER_GC.store(self.get_pages_used(), Ordering::SeqCst);
    }

    #[cfg(feature = "nogc_no_zeroing")]
    fn handle_user_collection_request(&self, _tls: crate::util::VMMutatorThread, _force: bool) {
        println!("Warning: User attempted a collection request. The request is ignored.");
    }
}

impl<VM: VMBinding> Immix<VM> {
    pub fn new(
        vm_map: &'static VMMap,
        mmapper: &'static Mmapper,
        options: Arc<UnsafeOptionsWrapper>,
        scheduler: Arc<GCWorkScheduler<VM>>,
    ) -> Self {
        let mut heap = HeapMeta::new(HEAP_START, HEAP_END);
        let immix_specs =
            if crate::args::BARRIER_MEASUREMENT || *ACTIVE_BARRIER != BarrierSelector::NoBarrier {
                metadata::extract_side_metadata(&[
                    RC_LOCK_BIT_SPEC,
                    MetadataSpec::OnSide(RC_TABLE),
                    *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
                ])
            } else {
                vec![]
            };
        let global_metadata_specs = SideMetadataContext::new_global_specs(&immix_specs);
        let immix = Immix {
            immix_space: ImmixSpace::new(
                "immix",
                vm_map,
                mmapper,
                &mut heap,
                scheduler,
                global_metadata_specs.clone(),
                &IMMIX_CONSTRAINTS,
            ),
            common: CommonPlan::new(
                vm_map,
                mmapper,
                options,
                heap,
                &IMMIX_CONSTRAINTS,
                global_metadata_specs,
            ),
            perform_cycle_collection: AtomicBool::new(false),
            next_gc_may_perform_cycle_collection: AtomicBool::new(false),
            current_pause: Atomic::new(None),
            previous_pause: Atomic::new(None),
            last_gc_was_defrag: AtomicBool::new(false),
            nursery_blocks: *crate::args::NURSERY_BLOCKS,
            inc_buffer_limit: None,
            avail_pages_at_end_of_last_gc: AtomicUsize::new(0),
            cm_threshold: 0,
            zeroing_packets_scheduled: AtomicBool::new(false),
            next_gc_selected: (Mutex::new(true), Condvar::new()),
        };

        {
            let mut side_metadata_sanity_checker = SideMetadataSanity::new();
            immix
                .common
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
            immix
                .immix_space
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
        }

        immix
    }

    fn decide_next_gc_may_perform_cycle_collection(&self) {
        let (lock, cvar) = &self.next_gc_selected;
        let notify = || {
            let mut gc_selection_done = lock.lock().unwrap();
            *gc_selection_done = true;
            cvar.notify_one();
        };
        if !crate::args::HEAP_HEALTH_GUIDED_GC {
            notify();
            return;
        }
        let total_blocks = self.get_total_pages() >> Block::LOG_PAGES;
        let threshold = *crate::args::TRACE_THRESHOLD;
        let last_freed_blocks = self
            .immix_space
            .num_clean_blocks_released
            .load(Ordering::SeqCst);
        if !CollectionSet::defrag_in_progress()
            && last_freed_blocks * 100 < (threshold * total_blocks as f32) as usize
        {
            if crate::args::LOG_PER_GC_STATE {
                println!("next trace ({} / {}) 1", last_freed_blocks, total_blocks);
            }
            self.next_gc_may_perform_cycle_collection
                .store(true, Ordering::SeqCst);
            if crate::args::CONCURRENT_MARKING
                && crate::args::LXR_DEFRAG_N.is_none()
                && !crate::concurrent_marking_in_progress()
            {
                self.zeroing_packets_scheduled.store(true, Ordering::SeqCst);
                self.immix_space.schedule_mark_table_zeroing_tasks();
            }
        } else {
            if crate::args::LOG_PER_GC_STATE {
                println!("next rc ({} / {})", last_freed_blocks, total_blocks);
            }
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
        let concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        let concurrent_marking_packets_drained = crate::concurrent_marking_packets_drained();
        if let Some(n) = *crate::args::LXR_DEFRAG_N {
            // If CM is finished, do a final mark pause and reset counter
            if crate::args::CONCURRENT_MARKING
                && concurrent_marking_in_progress
                && concurrent_marking_packets_drained
            {
                SIMPLE_INCREMENTAL_DEFRAG_COUNTER.store(0, Ordering::SeqCst);
                // println!("=== {} ===", 0);
                return Pause::FinalMark;
            }
            let old = SIMPLE_INCREMENTAL_DEFRAG_COUNTER.fetch_add(1, Ordering::SeqCst);
            // println!("=== {} ===", old + 1);
            // Force RC if still evacuating
            if CollectionSet::defrag_in_progress() {
                return Pause::RefCount;
            }
            // Do N RCs
            if crate::args::CONCURRENT_MARKING && !concurrent_marking_in_progress && old + 1 >= n {
                return Pause::InitialMark;
            }
            return Pause::RefCount;
        }
        if crate::args::LOG_PER_GC_STATE {
            println!(
                "next_gc_may_perform_cycle_collection: {:?}",
                self.next_gc_may_perform_cycle_collection
                    .load(Ordering::Relaxed)
            );
        }
        // If CM is finished, do a final mark pause
        if crate::args::CONCURRENT_MARKING
            && concurrent_marking_in_progress
            && concurrent_marking_packets_drained
        {
            return Pause::FinalMark;
        }
        // Only do RC pauses if we're doing incremental evacuation
        if CollectionSet::defrag_in_progress() {
            return Pause::RefCount;
        }
        // Either final mark pause or full pause for emergency GC
        if emergency {
            return if crate::args::CONCURRENT_MARKING && concurrent_marking_in_progress {
                Pause::FinalMark
            } else if CollectionSet::defrag_in_progress() {
                CollectionSet::force_evacuate_all();
                Pause::RefCount
            } else {
                // FIXME: Trigger STW Full GC
                assert!(crate::args::CONCURRENT_MARKING);
                CollectionSet::force_evacuate_all();
                Pause::FullTraceFast
            };
        }
        // Should trigger CM?
        if self
            .next_gc_may_perform_cycle_collection
            .load(Ordering::Relaxed)
            && !concurrent_marking_in_progress
        {
            return if crate::args::CONCURRENT_MARKING {
                Pause::InitialMark
            } else {
                Pause::FullTraceFast
            };
        } else {
            return Pause::RefCount;
        }
    }

    #[allow(clippy::collapsible_else_if)]
    fn select_collection_kind(&self, concurrent: bool) -> Pause {
        if crate::args::ENABLE_INITIAL_ALLOC_LIMIT {
            INITIAL_GC_TRIGGERED.store(true, Ordering::SeqCst);
        }
        {
            let o = Ordering::SeqCst;
            if INCS_TRIGGERED.load(o) {
                crate::COUNTERS.incs_triggerd.fetch_add(1, o);
            } else if ALLOC_TRIGGERED.load(o) {
                crate::COUNTERS.alloc_triggerd.fetch_add(1, o);
            } else {
                crate::COUNTERS.overflow_triggerd.fetch_add(1, o);
            }
        }
        self.base().set_collection_kind::<Self>(self);
        self.base().set_gc_status(GcStatus::GcPrepare);
        let in_defrag = self.immix_space.decide_whether_to_defrag(
            self.is_emergency_collection(),
            true,
            self.base().cur_collection_attempts.load(Ordering::SeqCst),
            self.base().is_user_triggered_collection(),
            self.base().options.full_heap_system_gc,
        );
        let full_trace = || {
            if in_defrag {
                Pause::FullTraceDefrag
            } else {
                Pause::FullTraceFast
            }
        };
        let emergency_collection = (self.base().cur_collection_attempts.load(Ordering::SeqCst) > 1)
            || self.is_emergency_collection()
            || self.base().options.full_heap_system_gc;

        let concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        let concurrent_marking_packets_drained = crate::concurrent_marking_packets_drained();
        let force_no_rc = self
            .next_gc_may_perform_cycle_collection
            .load(Ordering::SeqCst);
        let pause = if crate::args::LXR_RC_ONLY {
            Pause::RefCount
        } else if crate::args::HEAP_HEALTH_GUIDED_GC && crate::args::REF_COUNT {
            let pause = self.select_lxr_collection_kind(emergency_collection);
            if (pause == Pause::InitialMark || pause == Pause::FullTraceFast)
                && !self.zeroing_packets_scheduled.load(Ordering::SeqCst)
            {
                self.immix_space.schedule_mark_table_zeroing_tasks();
            }
            self.zeroing_packets_scheduled
                .store(false, Ordering::SeqCst);
            pause
        } else if emergency_collection {
            crate::COUNTERS.emergency.fetch_add(1, Ordering::Relaxed);
            if concurrent_marking_in_progress {
                Pause::FinalMark
            } else {
                full_trace()
            }
        } else if concurrent_marking_in_progress && concurrent_marking_packets_drained {
            Pause::FinalMark
        } else if concurrent {
            Pause::InitialMark
        } else {
            if (!super::REF_COUNT || crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING)
                && concurrent_marking_in_progress
            {
                Pause::FinalMark
            } else if super::REF_COUNT && (!force_no_rc || concurrent_marking_in_progress) {
                Pause::RefCount
            } else {
                full_trace()
            }
        };
        if pause == Pause::FinalMark && !concurrent_marking_packets_drained {
            crate::COUNTERS
                .cm_early_quit
                .fetch_add(1, Ordering::Relaxed);
        }
        if CollectionSet::defrag_in_progress() {
            crate::COUNTERS.defrag.fetch_add(1, Ordering::Relaxed);
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

    fn schedule_immix_collection<E: ProcessEdgesWork<VM = VM>, const KIND: TraceKind>(
        &'static self,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        // Before start yielding, wrap all the roots from the previous GC with work-packets.
        if super::REF_COUNT {
            Self::process_prev_roots(scheduler);
        }
        // Stop & scan mutators (mutator scanning can happen before STW)
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E>::new());
        // Prepare global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<ImmixGCWorkContext<VM, KIND>>::new(self));
        // Release global/collectors/mutators
        if super::REF_COUNT {
            scheduler.work_buckets[WorkBucketStage::RCFullHeapRelease].add(MatureSweeping);
        }
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<ImmixGCWorkContext<VM, KIND>>::new(self));
    }

    fn schedule_rc_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        #[allow(clippy::collapsible_if)]
        if crate::args::CONCURRENT_MARKING && !crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING {
            if crate::concurrent_marking_in_progress() {
                scheduler.pause_concurrent_work_packets_during_gc();
            }
        }
        debug_assert!(super::REF_COUNT);
        type E<VM> = RCImmixCollectRootEdges<VM>;
        // Before start yielding, wrap all the roots from the previous GC with work-packets.
        Self::process_prev_roots(scheduler);
        // Stop & scan mutators (mutator scanning can happen before STW)
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E<VM>>::new());
        // Prepare global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Prepare].add(Prepare::<
            ImmixGCWorkContext<VM, { TraceKind::Fast }>,
        >::new(self));
        // Release global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Release].add(Release::<
            ImmixGCWorkContext<VM, { TraceKind::Fast }>,
        >::new(self));
    }

    fn schedule_concurrent_marking_initial_pause(&'static self, scheduler: &GCWorkScheduler<VM>) {
        if super::REF_COUNT {
            Self::process_prev_roots(scheduler);
        }
        if crate::args::REF_COUNT {
            scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(StopMutators::<RCImmixCollectRootEdges<VM>>::new())
        } else {
            scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(StopMutators::<CMImmixCollectRootEdges<VM>>::new())
        };
        scheduler.work_buckets[WorkBucketStage::Prepare].add(Prepare::<
            ImmixGCWorkContext<VM, { TraceKind::Fast }>,
        >::new(self));
        scheduler.work_buckets[WorkBucketStage::Release].add(Release::<
            ImmixGCWorkContext<VM, { TraceKind::Fast }>,
        >::new(self));
    }

    fn schedule_concurrent_marking_final_pause(&'static self, scheduler: &GCWorkScheduler<VM>) {
        if super::REF_COUNT {
            Self::process_prev_roots(scheduler);
            scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(StopMutators::<RCImmixCollectRootEdges<VM>>::new());
        } else {
            scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(StopMutators::<ImmixProcessEdges<VM, { TraceKind::Fast }>>::new());
        }

        scheduler.work_buckets[WorkBucketStage::Prepare].add(Prepare::<
            ImmixGCWorkContext<VM, { TraceKind::Fast }>,
        >::new(self));
        if super::REF_COUNT {
            scheduler.work_buckets[WorkBucketStage::RCFullHeapRelease].add(MatureSweeping);
        }
        scheduler.work_buckets[WorkBucketStage::Release].add(Release::<
            ImmixGCWorkContext<VM, { TraceKind::Fast }>,
        >::new(self));
    }

    fn process_prev_roots(scheduler: &GCWorkScheduler<VM>) {
        debug_assert!(super::REF_COUNT);
        let prev_roots = unsafe { &super::PREV_ROOTS };
        let mut work_packets: Vec<Box<dyn GCWork<VM>>> = Vec::with_capacity(prev_roots.len());
        while let Some(decs) = prev_roots.pop() {
            let w = ProcessDecs::new(decs, LazySweepingJobsCounter::new_desc());
            work_packets.push(box w);
        }
        if work_packets.is_empty() {
            work_packets.push(box ProcessDecs::new(
                vec![],
                LazySweepingJobsCounter::new_desc(),
            ));
        }
        if crate::args::LAZY_DECREMENTS {
            debug_assert!(!crate::args::BARRIER_MEASUREMENT);
            scheduler.postpone_all(work_packets);
        } else {
            scheduler.work_buckets[WorkBucketStage::RCProcessDecs].bulk_add(work_packets);
        }
    }

    #[inline(always)]
    pub fn perform_cycle_collection(&self) -> bool {
        self.perform_cycle_collection.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn current_pause(&self) -> Option<Pause> {
        self.current_pause.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn previous_pause(&self) -> Option<Pause> {
        self.previous_pause.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn mark(&self, o: ObjectReference) -> bool {
        debug_assert!(!o.is_null());
        if self.immix_space.in_space(o) {
            self.immix_space.attempt_mark(o)
        } else {
            self.common.los.attempt_mark(o)
        }
    }

    #[inline(always)]
    pub fn mark2(&self, o: ObjectReference, los: bool) -> bool {
        debug_assert!(!o.is_null());
        if !los {
            self.immix_space.attempt_mark(o)
        } else {
            self.common.los.attempt_mark(o)
        }
    }

    #[inline(always)]
    pub fn is_marked(&self, o: ObjectReference) -> bool {
        debug_assert!(!o.is_null());
        if self.immix_space.in_space(o) {
            self.immix_space.mark_bit(o)
        } else {
            self.common.los.is_marked(o)
        }
    }

    #[inline(always)]
    pub const fn los(&self) -> &LargeObjectSpace<VM> {
        &self.common.los
    }

    // fn resize_nursery(&self) {
    //     // Don't resize if nursery is fixed.
    //     if crate::args::NURSERY_BLOCKS.is_some() {
    //         return;
    //     }
    //     // Resize based on throughput goal
    //     let min_nursery = *crate::args::MIN_NURSERY_BLOCKS;
    //     let max_nursery = crate::args::MAX_NURSERY_BLOCKS.unwrap_or_else(|| {
    //         usize::max(
    //             min_nursery,
    //             (self.get_total_pages() >> Block::LOG_PAGES) / 3,
    //         )
    //     });
    //     let pause_time = crate::GC_START_TIME
    //         .load(Ordering::SeqCst)
    //         .elapsed()
    //         .unwrap()
    //         .as_micros() as f64
    //         / 1000f64;
    //     static PREV_AVG_PAUSE: Atomic<f64> = Atomic::new(0f64);
    //     let prev_avg_pause = PREV_AVG_PAUSE.load(Ordering::Relaxed);
    //     let avg_pause = if prev_avg_pause == 0f64 {
    //         pause_time
    //     } else {
    //         (prev_avg_pause + pause_time) / 2f64
    //     };
    //     PREV_AVG_PAUSE.store(avg_pause, Ordering::Relaxed);
    //     let scale = avg_pause / 5f64;
    //     let _ = crate::args::ADAPTIVE_NURSERY_BLOCKS.fetch_update(
    //         Ordering::SeqCst,
    //         Ordering::SeqCst,
    //         |x| Some((x as f64 * (1f64 / scale)) as _),
    //     );
    //     let mut n = crate::args::ADAPTIVE_NURSERY_BLOCKS.load(Ordering::SeqCst);
    //     if n < min_nursery {
    //         n = min_nursery;
    //     }
    //     if n > max_nursery {
    //         n = max_nursery;
    //     }
    //     crate::args::ADAPTIVE_NURSERY_BLOCKS.store(n, Ordering::SeqCst);
    // }
}
