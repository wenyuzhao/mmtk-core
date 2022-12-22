use super::gc_work::{LXRGCWorkContext, LXRWeakRefWorkContext};
use super::mutator::ALLOCATOR_MAPPING;
use super::rc::{ProcessDecs, RCImmixCollectRootEdges};
use super::remset::FlushMatureEvacRemsets;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::GcStatus;
use crate::plan::immix::Pause;
use crate::plan::lxr::gc_work::FastRCPrepare;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::immix::block::Block;
use crate::policy::immix::rc_work::{MatureSweeping, UpdateWeakProcessor};
use crate::policy::largeobjectspace::LargeObjectSpace;
use crate::policy::space::Space;
use crate::scheduler::gc_work::*;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
#[cfg(feature = "analysis")]
use crate::util::analysis::GcHookWork;
use crate::util::copy::*;
use crate::util::heap::layout::heap_layout::Map;
use crate::util::heap::layout::heap_layout::Mmapper;
use crate::util::heap::HeapMeta;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSanity;
use crate::util::metadata::MetadataSpec;
use crate::util::options::Options;
use crate::util::rc::{RefCountHelper, RC_LOCK_BIT_SPEC, RC_TABLE};
#[cfg(feature = "sanity")]
use crate::util::sanity::sanity_checker::*;
use crate::util::{metadata, Address, ObjectReference};
use crate::vm::{Collection, ObjectModel, VMBinding};
use crate::{policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use crate::{BarrierSelector, LazySweepingJobsCounter};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Condvar, Mutex};
use std::time::SystemTime;

use atomic::{Atomic, Ordering};
use enum_map::EnumMap;
use spin::Lazy;

static INITIAL_GC_TRIGGERED: AtomicBool = AtomicBool::new(false);
static INCS_TRIGGERED: AtomicBool = AtomicBool::new(false);
static ALLOC_TRIGGERED: AtomicBool = AtomicBool::new(false);
static SURVIVAL_TRIGGERED: AtomicBool = AtomicBool::new(false);
static HEAP_AFTER_GC: AtomicUsize = AtomicUsize::new(0);

use mmtk_macros::PlanTraceObject;

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
    last_gc_was_defrag: AtomicBool,
    nursery_blocks: Option<usize>,
    avail_pages_at_end_of_last_gc: AtomicUsize,
    zeroing_packets_scheduled: AtomicBool,
    next_gc_selected: (Mutex<bool>, Condvar),
    in_concurrent_marking: AtomicBool,
    pub rc: RefCountHelper<VM>,
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
            return true;
        }
        // Survival limits
        if crate::args()
            .max_survival_mb
            .map(|x| {
                self.immix_space.block_allocation.nursery_mb() as f64
                    * super::SURVIVAL_RATIO_PREDICTOR.ratio()
                    >= x as f64
            })
            .unwrap_or(false)
        {
            // println!(
            //     "Survival limits {} * {} > {} blocks={}",
            //     self.immix_space.block_allocation.nursery_mb(),
            //     super::SURVIVAL_RATIO_PREDICTOR.ratio(),
            //     self.max_survival_mb.unwrap(),
            //     self.immix_space.block_allocation.nursery_blocks()
            // );
            SURVIVAL_TRIGGERED.store(true, Ordering::SeqCst);
            return true;
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
            return true;
        }
        // fresh young blocks limits
        if !crate::args::LXR_RC_ONLY
            && crate::args()
                .blocks_limit
                .map(|x| self.immix_space.block_allocation.nursery_blocks() >= x)
                .unwrap_or(false)
        {
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
        if crate::args::LOG_PER_GC_STATE {
            let boot_time = crate::BOOT_TIME.elapsed().unwrap().as_millis() as f64 / 1000f64;
            println!(
                "[{:.3}s][pause] {:?} {}",
                boot_time,
                pause,
                self.rc.inc_buffer_size()
            );
        }
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
        #[cfg(feature = "sanity")]
        scheduler.work_buckets[WorkBucketStage::Final].add(ScheduleSanityGC::<Self>::new(self));
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
        super::SURVIVAL_RATIO_PREDICTOR.update_ratio();
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
        VM::VMCollection::update_weak_processor(true);
        let pause = self.current_pause().unwrap();
        self.common.release(
            tls,
            pause == Pause::FullTraceFast || pause == Pause::FinalMark,
        );
        self.immix_space.release_rc(pause);
        unsafe {
            std::mem::swap(&mut super::CURR_ROOTS, &mut super::PREV_ROOTS);
            debug_assert!(super::CURR_ROOTS.is_empty());
        }
        // release the collected region
        self.last_gc_was_defrag.store(
            self.current_pause().unwrap() == Pause::FullTraceDefrag,
            Ordering::Relaxed,
        );
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.immix_space.defrag_headroom_pages()
    }

    fn get_used_pages(&self) -> usize {
        self.immix_space.reserved_pages() + self.common.get_used_pages()
    }

    #[inline(always)]
    fn base(&self) -> &BasePlan<VM> {
        &self.common.base
    }

    #[inline(always)]
    fn common(&self) -> &CommonPlan<VM> {
        &self.common
    }

    fn gc_pause_start(&self, _scheduler: &GCWorkScheduler<VM>) {
        self.immix_space.flush_page_resource();
        crate::NO_EVAC.store(false, Ordering::SeqCst);
        let pause = self.current_pause().unwrap();

        super::SURVIVAL_RATIO_PREDICTOR
            .set_alloc_size(self.immix_space.block_allocation.nursery_blocks() << Block::LOG_BYTES);
        super::SURVIVAL_RATIO_PREDICTOR
            .pause_start
            .store(SystemTime::now(), Ordering::SeqCst);
        let me = unsafe { &mut *(self as *const _ as *mut Self) };
        me.immix_space.rc_eager_prepare(pause);

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
        self.immix_space.flush_page_resource();
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
            self.perform_cycle_collection.store(false, Ordering::SeqCst);
        }
        self.avail_pages_at_end_of_last_gc
            .store(self.get_available_pages(), Ordering::SeqCst);
        HEAP_AFTER_GC.store(self.get_used_pages(), Ordering::SeqCst);
    }

    #[cfg(feature = "nogc_no_zeroing")]
    fn handle_user_collection_request(&self, _tls: crate::util::VMMutatorThread, _force: bool) {
        println!("Warning: User attempted a collection request. The request is ignored.");
    }

    #[inline(always)]
    fn no_mutator_prepare_release(&self) -> bool {
        true
    }

    #[inline(always)]
    fn no_worker_prepare(&self) -> bool {
        true
    }

    #[inline(always)]
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

    fn current_gc_should_scan_weak_classloader_roots(&self) -> bool {
        let pause = self.current_pause().unwrap();
        pause != Pause::FullTraceFast
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
    pub fn new(
        vm_map: &'static dyn Map,
        mmapper: &'static dyn Mmapper,
        options: Arc<Options>,
        scheduler: Arc<GCWorkScheduler<VM>>,
    ) -> Box<Self> {
        let mut heap = HeapMeta::new(&options);
        let immix_specs = metadata::extract_side_metadata(&[
            RC_LOCK_BIT_SPEC,
            MetadataSpec::OnSide(RC_TABLE),
            MetadataSpec::OnSide(crate::policy::immix::get_unlog_bit_slow::<VM>()),
        ]);
        let global_metadata_specs = SideMetadataContext::new_global_specs(&immix_specs);
        let mut immix_space = ImmixSpace::new(
            "immix",
            vm_map,
            mmapper,
            &mut heap,
            scheduler,
            global_metadata_specs.clone(),
            &LXR_CONSTRAINTS,
            true,
        );
        immix_space.cm_enabled = true;
        let mut lxr = Box::new(LXR {
            immix_space,
            common: CommonPlan::new(
                vm_map,
                mmapper,
                options.clone(),
                heap,
                &LXR_CONSTRAINTS,
                global_metadata_specs,
            ),
            perform_cycle_collection: AtomicBool::new(false),
            next_gc_may_perform_cycle_collection: AtomicBool::new(false),
            current_pause: Atomic::new(None),
            previous_pause: Atomic::new(None),
            last_gc_was_defrag: AtomicBool::new(false),
            nursery_blocks: crate::args().nursery_blocks,
            avail_pages_at_end_of_last_gc: AtomicUsize::new(0),
            zeroing_packets_scheduled: AtomicBool::new(false),
            next_gc_selected: (Mutex::new(true), Condvar::new()),
            in_concurrent_marking: AtomicBool::new(false),
            rc: RefCountHelper::NEW,
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

    #[inline(always)]
    pub fn concurrent_marking_enabled(&self) -> bool {
        self.immix_space.cm_enabled
    }

    #[inline(always)]
    pub fn concurrent_marking_in_progress(&self) -> bool {
        self.concurrent_marking_enabled() && self.in_concurrent_marking.load(Ordering::Relaxed)
    }

    fn decide_next_gc_may_perform_cycle_collection(&self) {
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
        if self.previous_pause() == Some(Pause::FinalMark)
            || self.previous_pause() == Some(Pause::FullTraceFast)
        {
            super::MATURE_LIVE_PREDICTOR.update(pages_after_gc)
        }
        let live_mature_pages = super::MATURE_LIVE_PREDICTOR.live_pages() as usize;
        let garbage = if pages_after_gc > live_mature_pages {
            pages_after_gc - live_mature_pages
        } else {
            0
        };
        let total_pages = self.get_total_pages();
        let available_blocks = (total_pages - pages_after_gc) >> Block::LOG_PAGES;
        // println!(
        //     "garbage {} / {} livemature={} ratio={:.4}",
        //     garbage,
        //     total_pages,
        //     live_mature_pages,
        //     (garbage as f64) / (total_pages as f64)
        // );
        if !self.concurrent_marking_in_progress()
            && garbage * 100 >= crate::args().trace_threshold as usize * total_pages
            || available_blocks < crate::args().concurrent_marking_stop_blocks
        {
            if crate::args::LOG_PER_GC_STATE {
                // println!(
                //     "next trace ({} / {}) {} {}",
                //     garbage,
                //     total_pages,
                //     pages_after_gc,
                //     HEAP_AFTER_GC.load(Ordering::SeqCst)
                // );
            }
            self.next_gc_may_perform_cycle_collection
                .store(true, Ordering::SeqCst);
            if self.concurrent_marking_enabled() && !self.concurrent_marking_in_progress() {
                self.zeroing_packets_scheduled.store(true, Ordering::SeqCst);
                self.immix_space
                    .schedule_mark_table_zeroing_tasks(WorkBucketStage::Unconstrained);
            }
        } else {
            if crate::args::LOG_PER_GC_STATE {
                // println!("next rc ({} / {}) {}", garbage, total_pages, pages_after_gc);
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
        let concurrent_marking_in_progress = self.concurrent_marking_in_progress();
        let concurrent_marking_packets_drained = crate::concurrent_marking_packets_drained();
        if crate::args::LOG_PER_GC_STATE {
            println!(
                " - next_gc_may_perform_cycle_collection: {:?}",
                self.next_gc_may_perform_cycle_collection
                    .load(Ordering::Relaxed)
            );
        }
        // If CM is finished, do a final mark pause
        if self.concurrent_marking_enabled()
            && concurrent_marking_in_progress
            && concurrent_marking_packets_drained
        {
            return Pause::FinalMark;
        }
        // Either final mark pause or full pause for emergency GC
        if emergency {
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
            return if self.concurrent_marking_enabled() {
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
        if pause == Pause::RefCount || pause == Pause::InitialMark {
            scheduler.work_buckets[WorkBucketStage::Closure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::WeakRefClosure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::FinalRefClosure].set_as_disabled();
            scheduler.work_buckets[WorkBucketStage::PhantomRefClosure].set_as_disabled();
        }
        scheduler.work_buckets[WorkBucketStage::SoftRefClosure].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::CalculateForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::SecondRoots].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::RefForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::FinalizableForwarding].set_as_disabled();
        scheduler.work_buckets[WorkBucketStage::Compact].set_as_disabled();
        if crate::args::LAZY_DECREMENTS {
            scheduler.work_buckets[WorkBucketStage::STWRCDecsAndSweep].set_as_disabled();
        }
    }

    fn schedule_rc_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        self.disable_unnecessary_buckets(scheduler, Pause::RefCount);
        #[allow(clippy::collapsible_if)]
        if self.concurrent_marking_enabled() && !crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING
        {
            if self.concurrent_marking_in_progress() {
                scheduler.pause_concurrent_work_packets_during_gc();
            }
        }
        type E<VM> = RCImmixCollectRootEdges<VM>;
        // Before start yielding, wrap all the roots from the previous GC with work-packets.
        Self::process_prev_roots(scheduler);
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
        Self::process_prev_roots(scheduler);
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
        Self::process_prev_roots(scheduler);
        scheduler.work_buckets[WorkBucketStage::Unconstrained]
            .add(StopMutators::<RCImmixCollectRootEdges<VM>>::new());

        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<LXRGCWorkContext<VM>>::new(self));
        scheduler.work_buckets[WorkBucketStage::Prepare].add(UpdateWeakProcessor);
        scheduler.work_buckets[WorkBucketStage::Release].add(MatureSweeping);
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<LXRGCWorkContext<VM>>::new(self));
        if VM::VMObjectModel::compressed_pointers_enabled() {
            scheduler.schedule_ref_proc_work::<LXRWeakRefWorkContext<VM, true>>(self);
        } else {
            scheduler.schedule_ref_proc_work::<LXRWeakRefWorkContext<VM, false>>(self);
        }
    }

    fn schedule_emergency_full_heap_collection<E: ProcessEdgesWork<VM = VM>>(
        &'static self,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        self.disable_unnecessary_buckets(scheduler, Pause::FullTraceFast);
        // Before start yielding, wrap all the roots from the previous GC with work-packets.
        Self::process_prev_roots(scheduler);
        scheduler.work_buckets[WorkBucketStage::Prepare].add(UpdateWeakProcessor);
        // Stop & scan mutators (mutator scanning can happen before STW)
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E>::new());
        // Prepare global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<LXRGCWorkContext<VM>>::new(self));
        // Release global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Release].add(MatureSweeping);
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<LXRGCWorkContext<VM>>::new(self));
        if VM::VMObjectModel::compressed_pointers_enabled() {
            scheduler.schedule_ref_proc_work::<LXRWeakRefWorkContext<VM, true>>(self);
        } else {
            scheduler.schedule_ref_proc_work::<LXRWeakRefWorkContext<VM, false>>(self);
        }
    }

    fn process_prev_roots(scheduler: &GCWorkScheduler<VM>) {
        let prev_roots = unsafe { &super::PREV_ROOTS };
        let mut work_packets: Vec<Box<dyn GCWork<VM>>> = Vec::with_capacity(prev_roots.len());
        while let Some(decs) = prev_roots.pop() {
            work_packets.push(if VM::VMObjectModel::compressed_pointers_enabled() {
                Box::new(ProcessDecs::<_, true>::new(
                    decs,
                    LazySweepingJobsCounter::new_decs(),
                ))
            } else {
                Box::new(ProcessDecs::<_, false>::new(
                    decs,
                    LazySweepingJobsCounter::new_decs(),
                ))
            })
        }
        if work_packets.is_empty() {
            work_packets.push(if VM::VMObjectModel::compressed_pointers_enabled() {
                Box::new(ProcessDecs::<_, true>::new(
                    vec![],
                    LazySweepingJobsCounter::new_decs(),
                ))
            } else {
                Box::new(ProcessDecs::<_, false>::new(
                    vec![],
                    LazySweepingJobsCounter::new_decs(),
                ))
            });
        }
        if crate::args::LAZY_DECREMENTS {
            debug_assert!(!crate::args::BARRIER_MEASUREMENT);
            scheduler.postpone_all_prioritized(work_packets);
        } else {
            scheduler.work_buckets[WorkBucketStage::STWRCDecsAndSweep].bulk_add(work_packets);
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
    pub fn in_defrag(&self, o: ObjectReference) -> bool {
        self.immix_space.in_space(o) && Block::in_defrag_block::<VM>(o)
    }

    #[inline(always)]
    pub fn address_in_defrag(&self, a: Address) -> bool {
        self.immix_space.address_in_space(a) && Block::address_in_defrag_block(a)
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
            self.immix_space.is_marked(o)
        } else {
            self.common.los.is_marked(o)
        }
    }

    #[inline(always)]
    pub fn los(&self) -> &LargeObjectSpace<VM> {
        &self.common.los
    }

    fn gc_init(&mut self, options: &Options) {
        crate::args::validate_features(BarrierSelector::FieldBarrier, options);
        self.immix_space.cm_enabled = !cfg!(feature = "lxr_no_cm");
        self.common.los.rc_enabled = true;
        unsafe {
            let mut lazy_sweeping_jobs = crate::LAZY_SWEEPING_JOBS.write();
            lazy_sweeping_jobs.swap();
            let me = &*(self as *const Self);
            lazy_sweeping_jobs.end_of_decs = Some(Box::new(move |c| {
                me.immix_space.schedule_rc_block_sweeping_tasks(c);
            }));
            lazy_sweeping_jobs.end_of_lazy = Some(Box::new(move || {
                // me.immix_space.reusable_blocks.flush_all();
                me.immix_space.flush_page_resource();
                if crate::args::LOG_PER_GC_STATE {
                    let pause_time = crate::GC_START_TIME
                        .load(Ordering::SeqCst)
                        .elapsed()
                        .unwrap();
                    let pause_time = pause_time.as_micros() as f64 / 1000f64;
                    println!(
                        " - lazy jobs finished {}M->{}M({}M) {:.3}ms",
                        crate::RESERVED_PAGES_AT_GC_END.load(Ordering::SeqCst) / 256,
                        me.get_reserved_pages() / 256,
                        me.get_total_pages() / 256,
                        pause_time
                    );
                }
                // Update counters
                if !crate::args::LAZY_DECREMENTS {
                    HEAP_AFTER_GC.store(me.get_used_pages(), Ordering::SeqCst);
                }
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
            }));
        }
        if let Some(nursery_ratio) = crate::args().nursery_ratio {
            let total_blocks = *options.heap_size >> Block::LOG_BYTES;
            let nursery_blocks = total_blocks / (nursery_ratio + 1);
            self.nursery_blocks = Some(nursery_blocks);
        }
    }

    fn set_concurrent_marking_state(&self, active: bool) {
        <VM as VMBinding>::VMCollection::set_concurrent_marking_state(active);
        self.in_concurrent_marking.store(active, Ordering::SeqCst);
    }
}
