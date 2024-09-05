use self::worker::PollResult;

use super::gc_work::ScheduleCollection;
use super::stat::SchedulerStat;
use super::work_bucket::*;
use super::worker::{GCWorker, ThreadId, WorkerGroup};
use super::worker_goals::{WorkerGoal, WorkerGoals};
use super::worker_monitor::{LastParkedResult, WorkerMonitor};
use super::*;
use crate::global_state::GcStatus;
use crate::mmtk::MMTK;
use crate::plan::lxr::LXR;
use crate::util::memory::result_is_mapped;
use crate::util::opaque_pointer::*;
use crate::util::options::AffinityKind;
use crate::util::reference_processor::PhantomRefProcessing;
use crate::util::rust_util::array_from_fn;
use crate::vm::Collection;
use crate::vm::VMBinding;
use crate::Pause;
use crate::Plan;
use crossbeam::deque::{Injector, Steal};
use enum_map::{Enum, EnumMap};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use worker::GCWorkerShared;

pub struct GCWorkScheduler<VM: VMBinding> {
    /// Work buckets
    pub work_buckets: EnumMap<WorkBucketStage, WorkBucket<VM>>,
    /// Workers
    pub(crate) worker_group: Arc<WorkerGroup<VM>>,
    /// For synchronized communication between workers and with mutators.
    pub(crate) worker_monitor: Arc<WorkerMonitor>,
    /// How to assign the affinity of each GC thread. Specified by the user.
    affinity: AffinityKind,
    pub(super) postponed_concurrent_work: spin::RwLock<Injector<Box<dyn GCWork<VM>>>>,
    pub(super) postponed_concurrent_work_prioritized: spin::RwLock<Injector<Box<dyn GCWork<VM>>>>,
    in_gc_pause: AtomicBool,
    bucket_update_progress: AtomicUsize,
}

// FIXME: GCWorkScheduler should be naturally Sync, but we cannot remove this `impl` yet.
// Some subtle interaction between ObjectRememberingBarrier, Mutator and some GCWork instances
// makes the compiler think WorkBucket is not Sync.
unsafe impl<VM: VMBinding> Sync for GCWorkScheduler<VM> {}

impl<VM: VMBinding> GCWorkScheduler<VM> {
    pub fn new(num_workers: usize, num_conc_workers: usize, affinity: AffinityKind) -> Arc<Self> {
        assert!(num_conc_workers > 0 && num_conc_workers <= num_workers);
        assert!(num_workers > 0);
        let worker_monitor: Arc<WorkerMonitor> = Arc::new(WorkerMonitor::new(num_workers));
        let worker_group = WorkerGroup::new(num_workers);

        // Create work buckets for workers.
        // TODO: Replace `array_from_fn` with `std::array::from_fn` after bumping MSRV.
        let mut work_buckets = EnumMap::from_array(array_from_fn(|stage_num| {
            let stage = WorkBucketStage::from_usize(stage_num);
            let active = stage == WorkBucketStage::Unconstrained;
            WorkBucket::new(active, worker_monitor.clone())
        }));

        work_buckets[WorkBucketStage::Unconstrained].enable_prioritized_queue();

        // Set the open condition of each bucket.
        {
            let first_stw_stage = WorkBucketStage::first_stw_stage();
            let mut open_stages: Vec<WorkBucketStage> = vec![first_stw_stage];
            let stages = (0..WorkBucketStage::LENGTH).map(WorkBucketStage::from_usize);
            for stage in stages {
                // Unconstrained is always open.
                // The first STW stage (Prepare) will be opened when the world stopped
                // (i.e. when all mutators are suspended).
                if stage != WorkBucketStage::Unconstrained && stage != first_stw_stage {
                    // Other work packets will be opened after previous stages are done
                    // (i.e their buckets are drained and all workers parked).
                    let cur_stages = open_stages.clone();
                    work_buckets[stage].set_open_condition(
                        move |scheduler: &GCWorkScheduler<VM>| {
                            scheduler.are_buckets_drained(&cur_stages)
                        },
                    );
                    open_stages.push(stage);
                }
            }
        }

        Arc::new(Self {
            work_buckets,
            worker_group,
            worker_monitor,
            affinity,
            postponed_concurrent_work: spin::RwLock::new(Injector::new()),
            postponed_concurrent_work_prioritized: spin::RwLock::new(Injector::new()),
            in_gc_pause: AtomicBool::new(false),
            bucket_update_progress: AtomicUsize::new(0),
        })
    }

    pub fn pause_concurrent_marking_work_packets_during_gc(&self) {
        let mut unconstrained_queue = Injector::new();
        unconstrained_queue =
            self.work_buckets[WorkBucketStage::Unconstrained].swap_queue(unconstrained_queue);
        let postponed_queue = self.postponed_concurrent_work.read();
        if !unconstrained_queue.is_empty() {
            loop {
                match unconstrained_queue.steal() {
                    Steal::Empty => break,
                    Steal::Success(x) => postponed_queue.push(x),
                    Steal::Retry => continue,
                }
            }
        }
        crate::PAUSE_CONCURRENT_MARKING.store(true, Ordering::SeqCst);
    }

    pub fn process_lazy_decrement_packets(&self) {
        let mut no_postpone = vec![];
        let mut cm_packets = vec![];
        // Buggy
        let postponed_concurrent_work = self.postponed_concurrent_work_prioritized.read();
        loop {
            if postponed_concurrent_work.is_empty() {
                break;
            }
            match postponed_concurrent_work.steal() {
                Steal::Success(w) => {
                    if !w.is_concurrent_marking_work() {
                        no_postpone.push(w)
                    } else {
                        cm_packets.push(w)
                    }
                }
                Steal::Empty => break,
                Steal::Retry => {}
            }
        }
        for w in cm_packets {
            postponed_concurrent_work.push(w)
        }
        // self.work_buckets[WorkBucketStage::STWRCDecsAndSweep].bulk_add(no_postpone);

        unimplemented!()
    }

    pub fn postpone(&self, w: impl GCWork<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work.read().push(Box::new(w))
    }

    pub fn postpone_prioritized(&self, w: impl GCWork<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work_prioritized
            .read()
            .push(Box::new(w))
    }

    pub fn postpone_dyn(&self, w: Box<dyn GCWork<VM>>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work.read().push(w)
    }

    pub fn postpone_dyn_prioritized(&self, w: Box<dyn GCWork<VM>>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work_prioritized.read().push(w)
    }

    pub fn postpone_all(&self, ws: Vec<Box<dyn GCWork<VM>>>) {
        let postponed_concurrent_work = self.postponed_concurrent_work.read();
        ws.into_iter()
            .for_each(|w| postponed_concurrent_work.push(w));
    }

    pub fn postpone_all_prioritized(&self, ws: Vec<Box<dyn GCWork<VM>>>) {
        let postponed_concurrent_work = self.postponed_concurrent_work_prioritized.read();
        ws.into_iter()
            .for_each(|w| postponed_concurrent_work.push(w));
    }

    pub fn num_workers(&self) -> usize {
        self.worker_group.as_ref().worker_count()
    }

    /// Create GC threads for the first time.  It will also create the `GCWorker` instances.
    ///
    /// Currently GC threads only include worker threads, and we currently have only one worker
    /// group.  We may add more worker groups in the future.
    pub fn spawn_gc_threads(self: &Arc<Self>, mmtk: &'static MMTK<VM>, tls: VMThread) {
        self.worker_group.initial_spawn(tls, mmtk);
    }

    /// Ask all GC workers to exit for forking.
    pub fn stop_gc_threads_for_forking(self: &Arc<Self>) {
        self.worker_group.prepare_surrender_buffer();

        debug!("A mutator is requesting GC threads to stop for forking...");
        self.worker_monitor.make_request(WorkerGoal::StopForFork);
    }

    /// Surrender the `GCWorker` struct of a GC worker when it exits.
    pub fn surrender_gc_worker(&self, worker: Box<GCWorker<VM>>) {
        let all_surrendered = self.worker_group.surrender_gc_worker(worker);

        if all_surrendered {
            debug!(
                "All {} workers surrendered.",
                self.worker_group.worker_count()
            );
            self.worker_monitor.on_all_workers_exited();
        }
    }

    /// Respawn GC threads after forking.  This will reuse the `GCWorker` instances of stopped
    /// workers.  `tls` is the VM thread that requests GC threads to be re-spawn, and will be
    /// passed down to [`crate::vm::Collection::spawn_gc_thread`].
    pub fn respawn_gc_threads_after_forking(self: &Arc<Self>, tls: VMThread) {
        self.worker_group.respawn(tls)
    }

    /// Resolve the affinity of a thread.
    pub fn resolve_affinity(&self, thread: ThreadId) {
        self.affinity.resolve_affinity(thread);
    }

    /// Request a GC to be scheduled.  Called by mutator via `GCRequester`.
    pub(crate) fn request_schedule_collection(&self) {
        debug!("A mutator is sending GC-scheduling request to workers...");
        self.worker_monitor.make_request(WorkerGoal::Gc);
    }

    /// Add the `ScheduleCollection` packet.  Called by the last parked worker.
    fn add_schedule_collection_packet(&self) {
        // We are still holding the mutex `WorkerMonitor::sync`.  Do not notify now.
        self.work_buckets[WorkBucketStage::Unconstrained].add_no_notify(ScheduleCollection);
    }

    pub fn schedule_common_work_no_refs<C: GCWorkContext<VM = VM>>(
        &self,
        plan: &'static C::PlanType,
    ) {
        use crate::scheduler::gc_work::*;
        // Stop & scan mutators (mutator scanning can happen before STW)
        self.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<C>::new());

        // Prepare global/collectors/mutators
        self.work_buckets[WorkBucketStage::Prepare].add(Prepare::<C>::new(plan));

        // Release global/collectors/mutators
        self.work_buckets[WorkBucketStage::Release].add(Release::<C>::new(plan));

        // Analysis GC work
        #[cfg(feature = "analysis")]
        {
            use crate::util::analysis::GcHookWork;
            self.work_buckets[WorkBucketStage::Unconstrained].add(GcHookWork);
        }

        // Sanity
        #[cfg(feature = "sanity")]
        {
            use crate::util::sanity::sanity_checker::ScheduleSanityGC;
            self.work_buckets[WorkBucketStage::Final]
                .add(ScheduleSanityGC::<C::PlanType>::new(plan));
        }
    }

    /// Schedule all the common work packets
    pub fn schedule_common_work<C: GCWorkContext<VM = VM> + 'static>(
        &self,
        plan: &'static C::PlanType,
    ) {
        self.schedule_common_work_no_refs::<C>(plan);

        use crate::scheduler::gc_work::*;
        // Reference processing
        if !*plan.base().options.no_reference_types || !*plan.base().options.no_finalizer {
            // use crate::util::reference_processor::{
            //     PhantomRefProcessing, SoftRefProcessing, WeakRefProcessing,
            // };
            // self.work_buckets[WorkBucketStage::WeakRefClosure]
            //     .add(SoftRefProcessing::<C::DefaultProcessEdges>::new());
            // self.work_buckets[WorkBucketStage::WeakRefClosure]
            //     .add(WeakRefProcessing::<C::DefaultProcessEdges>::new());
            self.work_buckets[WorkBucketStage::PhantomRefClosure]
                .add(PhantomRefProcessing::<C::DefaultProcessEdges>::new());

            // VM-specific weak ref processing
            self.work_buckets[WorkBucketStage::WeakRefClosure]
                .add(VMProcessWeakRefs::<C::DefaultProcessEdges>::new());

            // use crate::util::reference_processor::RefForwarding;
            // if plan.constraints().needs_forward_after_liveness {
            //     self.work_buckets[WorkBucketStage::RefForwarding]
            //         .add(RefForwarding::<C::DefaultProcessEdges>::new());
            // }

            // use crate::util::reference_processor::RefEnqueue;
            // self.work_buckets[WorkBucketStage::Release].add(RefEnqueue::<VM>::new());
        }

        // Finalization
        if !*plan.base().options.no_finalizer {
            use crate::util::finalizable_processor::{Finalization, ForwardFinalization};
            // finalization
            self.work_buckets[WorkBucketStage::FinalRefClosure]
                .add(Finalization::<C::DefaultProcessEdges>::new());
            // forward refs
            if plan.constraints().needs_forward_after_liveness {
                // self.work_buckets[WorkBucketStage::FinalizableForwarding]
                //     .add(ForwardFinalization::<C::DefaultProcessEdges>::new());
                unimplemented!()
            }
        }
    }

    /// Schedule all the common work packets
    pub fn schedule_ref_proc_work<C: GCWorkContext<VM = VM> + 'static>(
        &self,
        plan: &'static C::PlanType,
    ) {
        use crate::scheduler::gc_work::*;

        // Reference processing
        if !*plan.base().options.no_reference_types || !*plan.base().options.no_finalizer {
            // use crate::util::reference_processor::{
            //     PhantomRefProcessing, SoftRefProcessing, WeakRefProcessing,
            // };
            // self.work_buckets[WorkBucketStage::WeakRefClosure]
            //     .add(SoftRefProcessing::<C::DefaultProcessEdges>::new());
            // self.work_buckets[WorkBucketStage::WeakRefClosure]
            //     .add(WeakRefProcessing::<C::DefaultProcessEdges>::new());

            // VM-specific weak ref processing
            self.work_buckets[WorkBucketStage::WeakRefClosure]
                .add(VMProcessWeakRefs::<C::DefaultProcessEdges>::new());
            self.work_buckets[WorkBucketStage::PhantomRefClosure]
                .add(PhantomRefProcessing::<C::DefaultProcessEdges>::new());

            // use crate::util::reference_processor::RefForwarding;
            // if plan.constraints().needs_forward_after_liveness {
            //     self.work_buckets[WorkBucketStage::RefForwarding]
            //         .add(RefForwarding::<C::DefaultProcessEdges>::new());
            // }

            // use crate::util::reference_processor::RefEnqueue;
            // self.work_buckets[WorkBucketStage::Release].add(RefEnqueue::<VM>::new());
        }

        // Finalization
        if !*plan.base().options.no_finalizer {
            use crate::util::finalizable_processor::{Finalization, ForwardFinalization};
            // finalization
            self.work_buckets[WorkBucketStage::FinalRefClosure]
                .add(Finalization::<C::DefaultProcessEdges>::new());
            // forward refs
            if plan.constraints().needs_forward_after_liveness {
                // self.work_buckets[WorkBucketStage::FinalizableForwarding]
                //     .add(ForwardFinalization::<C::DefaultProcessEdges>::new());
                unimplemented!()
            }
        }

        // We add the VM-specific weak ref processing work regardless of MMTK-side options,
        // including Options::no_finalizer and Options::no_reference_types.
        //
        // VMs need weak reference handling to function properly.  The VM may treat weak references
        // as strong references, but it is not appropriate to simply disable weak reference
        // handling from MMTk's side.  The VM, however, may choose to do nothing in
        // `Collection::process_weak_refs` if appropriate.
        //
        // It is also not sound for MMTk core to turn off weak
        // reference processing or finalization alone, because (1) not all VMs have the notion of
        // weak references or finalizers, so it may not make sence, and (2) the VM may
        // processing them together.

        // VM-specific weak ref processing
        // The `VMProcessWeakRefs` work packet is set as the sentinel so that it is executed when
        // the `VMRefClosure` bucket is drained.  The VM binding may spawn new work packets into
        // the `VMRefClosure` bucket, and request another `VMProcessWeakRefs` work packet to be
        // executed again after this bucket is drained again.  Strictly speaking, the first
        // `VMProcessWeakRefs` packet can be an ordinary packet (doesn't have to be a sentinel)
        // because there are no other packets in the bucket.  We set it as sentinel for
        // consistency.
        // self.work_buckets[WorkBucketStage::VMRefClosure]
        //     .set_sentinel(Box::new(VMProcessWeakRefs::<C::DefaultProcessEdges>::new()));

        // if plan.constraints().needs_forward_after_liveness {
        //     // VM-specific weak ref forwarding
        //     self.work_buckets[WorkBucketStage::VMRefForwarding]
        //         .add(VMForwardWeakRefs::<C::DefaultProcessEdges>::new());
        // }

        // self.work_buckets[WorkBucketStage::Release].add(VMPostForwarding::<VM>::default());
    }

    fn are_buckets_drained(&self, buckets: &[WorkBucketStage]) -> bool {
        // debug_assert!(
        //     self.pending_coordinator_packets.load(Ordering::SeqCst) == 0,
        //     "GCWorker attempted to open buckets when there are pending coordinator work packets"
        // );
        buckets
            .iter()
            .all(|&b| self.work_buckets[b].is_drained() || self.work_buckets[b].disabled())
    }

    pub fn all_buckets_empty(&self) -> bool {
        self.work_buckets.values().all(|bucket| bucket.is_empty())
    }

    pub(super) fn schedule_concurrent_packets(
        &self,
        queue: Injector<Box<dyn GCWork<VM>>>,
        pqueue: Injector<Box<dyn GCWork<VM>>>,
    ) {
        crate::MOVE_CONCURRENT_MARKING_TO_STW.store(false, Ordering::SeqCst);
        crate::PAUSE_CONCURRENT_MARKING.store(false, Ordering::SeqCst);
        let mut notify = false;
        if !queue.is_empty() {
            let old_queue = self.work_buckets[WorkBucketStage::Unconstrained].swap_queue(queue);
            debug_assert!(old_queue.is_empty());
            notify = true;
        }
        if !pqueue.is_empty() {
            let old_queue =
                self.work_buckets[WorkBucketStage::Unconstrained].swap_queue_prioritized(pqueue);
            debug_assert!(old_queue.is_empty());
            notify = true;
        }
        if notify {
            self.wakeup_all_conc_workers();
        }
    }

    /// Schedule "sentinel" work packets for all activated buckets.
    pub(crate) fn schedule_sentinels(&self) -> bool {
        let mut new_packets = false;
        for (id, work_bucket) in self.work_buckets.iter() {
            if work_bucket.is_activated() && work_bucket.maybe_schedule_sentinel() {
                trace!("Scheduled sentinel packet into {:?}", id);
                new_packets = true;
            }
        }
        new_packets
    }

    /// Open buckets if their conditions are met.
    ///
    /// This function should only be called after all the workers are parked.
    /// No workers will be waked up by this function. The caller is responsible for that.
    ///
    /// Return true if there're any non-empty buckets updated.
    pub(crate) fn update_buckets(&self) -> bool {
        let mut buckets_updated = false;
        let mut new_packets = false;
        let start_index = self.bucket_update_progress.load(Ordering::SeqCst) + 1;
        let mut new_progress = WorkBucketStage::LENGTH;
        for i in start_index..WorkBucketStage::LENGTH {
            let id = WorkBucketStage::from_usize(i);
            if id == WorkBucketStage::Unconstrained {
                continue;
            }

            let bucket = &self.work_buckets[id];
            if bucket.disabled() || bucket.is_activated() {
                continue;
            }
            let bucket_opened = bucket.update(self);
            #[cfg(feature = "tracing")]
            if bucket_opened {
                probe!(mmtk, bucket_opened, id);
            }
            let verbose = crate::verbose(3);
            if (verbose || cfg!(feature = "pause_time")) && bucket_opened {
                if verbose {
                    gc_log!([3]
                        " - ({:.3}ms) Start GC Stage: {:?}",
                        crate::GC_START_TIME
                            .elapsed()
                            .as_nanos() as f64
                            / 1000000f64,
                        id
                    );
                }
            }
            if cfg!(feature = "yield_and_roots_timer")
                && bucket_opened
                && id == WorkBucketStage::Prepare
            {
                let t = crate::GC_START_TIME.elapsed().as_nanos();
                crate::counters().roots_nanos.fetch_add(t, Ordering::SeqCst);
            }
            buckets_updated = buckets_updated || bucket_opened;
            if bucket_opened {
                #[cfg(feature = "tracing")]
                probe!(mmtk, bucket_opened, id);
                new_packets = new_packets || !bucket.is_drained();
                new_packets = new_packets || bucket.maybe_schedule_sentinel();
                if new_packets {
                    new_progress = i;
                    // Quit the loop. A sentinel packet is added to the newly opened buckets.
                    trace!("Sentinel is scheduled at stage {:?}.  Break.", id);
                    break;
                }
            }
        }
        if buckets_updated && new_packets {
            self.bucket_update_progress
                .store(new_progress, Ordering::SeqCst);
        } else {
            self.bucket_update_progress.store(0, Ordering::SeqCst);
        }
        buckets_updated && new_packets
    }

    pub fn deactivate_all(&self) {
        self.work_buckets.iter().for_each(|(id, bkt)| {
            if id != WorkBucketStage::Unconstrained {
                bkt.deactivate();
                bkt.set_as_enabled();
            }
        });
        self.bucket_update_progress.store(0, Ordering::SeqCst);
    }

    pub fn reset_state(&self) {
        let first_stw_stage = WorkBucketStage::first_stw_stage();
        self.work_buckets.iter().for_each(|(id, bkt)| {
            if id != WorkBucketStage::Unconstrained && id != first_stw_stage {
                bkt.deactivate();
                bkt.set_as_enabled();
            }
        });
        self.bucket_update_progress.store(0, Ordering::SeqCst);
    }

    pub fn debug_assert_all_buckets_deactivated(&self) {
        if cfg!(debug_assertions) {
            self.work_buckets.iter().for_each(|(id, bkt)| {
                if id != WorkBucketStage::Unconstrained {
                    assert!(!bkt.is_activated());
                }
            });
        }
    }

    /// Check if all the work buckets are empty
    #[allow(unused)]
    pub(crate) fn assert_all_activated_buckets_are_empty(&self) {
        let mut error_example = None;
        for (id, bucket) in self.work_buckets.iter() {
            if bucket.is_activated() && !bucket.is_empty() {
                error!("Work bucket {:?} is active but not empty!", id);
                // This error can be hard to reproduce.
                // If an error happens in the release build where logs are turned off,
                // we should show at least one abnormal bucket in the panic message
                // so that we still have some information for debugging.
                error_example = Some(id);
            }
        }
        if let Some(id) = error_example {
            panic!("Some active buckets (such as {:?}) are not empty.", id);
        }
    }

    pub(super) fn set_in_gc_pause(&self, in_gc_pause: bool) {
        self.in_gc_pause.store(in_gc_pause, Ordering::SeqCst);
        for wb in self.work_buckets.values() {
            wb.set_in_concurrent(!in_gc_pause);
        }
    }

    pub fn in_concurrent(&self) -> bool {
        !self.in_gc_pause.load(Ordering::SeqCst)
    }

    /// Get a schedulable work packet without retry.
    fn poll_schedulable_work_once(&self, worker: &GCWorker<VM>) -> Steal<Box<dyn GCWork<VM>>> {
        let mut should_retry = false;
        // Try find a packet that can be processed only by this worker.
        if let Some(w) = worker.shared.designated_work.pop() {
            return Steal::Success(w);
        }
        // Try get a packet from a work bucket.
        for work_bucket in self.work_buckets.values() {
            match work_bucket.poll(&worker.local_work_buffer) {
                Steal::Success(w) => return Steal::Success(w),
                Steal::Retry => should_retry = true,
                _ => {}
            }
        }
        // Try steal some packets from any worker
        for (id, worker_shared) in self.worker_group.workers_shared.iter().enumerate() {
            if id == worker.ordinal {
                continue;
            }
            match worker_shared.stealer.as_ref().unwrap().steal() {
                Steal::Success(w) => return Steal::Success(w),
                Steal::Retry => should_retry = true,
                _ => {}
            }
        }
        if should_retry {
            Steal::Retry
        } else {
            Steal::Empty
        }
    }

    fn random_park_and_miller(seed0: &mut usize) -> usize {
        let a = 16807;
        let m = 2147483647;
        let q = 127773;
        let r = 2836;
        let seed = *seed0;
        let hi = seed / q;
        let lo = seed % q;
        let test = a * lo - r * hi;
        *seed0 = if test > 0 { test } else { test + m };
        *seed0
    }

    pub(super) fn get_random_steal_index(
        worker_id: usize,
        hash_seed: &mut usize,
        n: usize,
    ) -> (usize, usize) {
        let mut k1 = worker_id;
        while k1 == worker_id {
            k1 = Self::random_park_and_miller(hash_seed) % n;
        }
        let mut k2 = worker_id;
        while k2 == worker_id || k2 == k1 {
            k2 = Self::random_park_and_miller(hash_seed) % n;
        }
        (k1, k2)
    }

    fn steal_best_of_2(
        worker_id: usize,
        hash_seed: &mut usize,
        workers: &[Arc<GCWorkerShared<VM>>],
    ) -> Steal<Box<dyn GCWork<VM>>> {
        let n = workers.len();
        if n > 2 {
            let (k1, k2) = Self::get_random_steal_index(worker_id, hash_seed, n);
            let sz1 = workers[k1].stealer.as_ref().unwrap().len();
            let sz2 = workers[k2].stealer.as_ref().unwrap().len();
            if sz1 < sz2 {
                return workers[k2].stealer.as_ref().unwrap().steal();
            } else {
                return workers[k1].stealer.as_ref().unwrap().steal();
            }
        } else if n == 2 {
            let k = (worker_id + 1) % 2;
            return workers[k].stealer.as_ref().unwrap().steal();
        } else {
            return Steal::Empty;
        }
    }

    /// Get a schedulable work packet without retry.
    pub(super) fn try_steal(&self, worker: &mut GCWorker<VM>) -> Steal<Box<dyn GCWork<VM>>> {
        for _ in 0..self.worker_group.workers_shared.len() * 2 {
            if let Steal::Success(slot) = Self::steal_best_of_2(
                worker.ordinal,
                &mut worker.hash_seed,
                &self.worker_group.workers_shared,
            ) {
                return Steal::Success(slot);
            }
        }
        Steal::Empty
    }

    /// Get a schedulable work packet.
    fn poll_schedulable_work(&self, worker: &GCWorker<VM>) -> Option<Box<dyn GCWork<VM>>> {
        // Loop until we successfully get a packet.
        loop {
            match self.poll_schedulable_work_once(worker) {
                Steal::Success(w) => {
                    return Some(w);
                }
                Steal::Retry => {
                    std::thread::yield_now();
                    continue;
                }
                Steal::Empty => {
                    return None;
                }
            }
        }
    }

    /// Called by workers to get a schedulable work packet.
    /// Park the worker if there're no available packets.
    pub(crate) fn poll(&self, worker: &GCWorker<VM>) -> PollResult<VM> {
        if let Some(work) = self.poll_schedulable_work(worker) {
            return Ok(work);
        }
        self.poll_slow(worker)
    }

    fn poll_slow(&self, worker: &GCWorker<VM>) -> PollResult<VM> {
        loop {
            flush_logs!();
            // Retry polling
            if let Some(work) = self.poll_schedulable_work(worker) {
                return Ok(work);
            }
            flush_logs!();

            let ordinal = worker.ordinal;
            self.worker_monitor
                .park_and_wait(ordinal, |goals| self.on_last_parked(worker, goals))?;
        }
    }

    /// Called when the last worker parked.  `goal` allows this function to inspect and change the
    /// current goal.
    fn on_last_parked(&self, worker: &GCWorker<VM>, goals: &mut WorkerGoals) -> LastParkedResult {
        let Some(ref current_goal) = goals.current() else {
            // There is no goal.  Find a request to respond to.
            return self.respond_to_requests(worker, goals);
        };

        match current_goal {
            WorkerGoal::Gc => {
                // We are in the progress of GC.

                // In stop-the-world GC, mutators cannot request for GC while GC is in progress.
                // When we support concurrent GC, we should remove this assertion.
                assert!(
                    !goals.debug_is_requested(WorkerGoal::Gc),
                    "GC request sent to WorkerMonitor while GC is still in progress."
                );

                // We are in the middle of GC, and the last GC worker parked.
                trace!("The last worker parked during GC.  Try to find more work to do...");

                // During GC, if all workers parked, all open buckets must have been drained.
                // self.assert_all_activated_buckets_are_empty();

                // Find more work for workers to do.
                let found_more_work = self.find_more_work_for_workers();

                if found_more_work {
                    LastParkedResult::WakeAll
                } else {
                    // GC finished.
                    self.on_gc_finished(worker);

                    // Clear the current goal
                    goals.on_current_goal_completed();
                    self.respond_to_requests(worker, goals)
                }
            }
            WorkerGoal::StopForFork => {
                panic!(
                    "Worker {} parked again when it is asked to exit.",
                    worker.ordinal
                )
            }
        }
    }

    /// Respond to a worker reqeust.
    fn respond_to_requests(
        &self,
        worker: &GCWorker<VM>,
        goals: &mut WorkerGoals,
    ) -> LastParkedResult {
        assert!(goals.current().is_none());

        let Some(goal) = goals.poll_next_goal() else {
            // No requests.  Park this worker, too.
            return LastParkedResult::ParkSelf;
        };

        match goal {
            WorkerGoal::Gc => {
                trace!("A mutator requested a GC to be scheduled.");

                // We set the eBPF trace point here so that bpftrace scripts can start recording
                // work packet events before the `ScheduleCollection` work packet starts.
                #[cfg(feature = "tracing")]
                probe!(mmtk, gc_start);

                {
                    let mut gc_start_time = worker.mmtk.state.gc_start_time.borrow_mut();
                    assert!(gc_start_time.is_none(), "GC already started?");
                    *gc_start_time = Some(Instant::now());
                }

                self.add_schedule_collection_packet();
                LastParkedResult::WakeSelf
            }
            WorkerGoal::StopForFork => {
                trace!("A mutator wanted to fork.");
                LastParkedResult::WakeAll
            }
        }
    }

    /// Find more work for workers to do.  Return true if more work is available.
    fn find_more_work_for_workers(&self) -> bool {
        if self.worker_group.has_designated_work() {
            trace!("Some workers have designated work.");
            return true;
        }

        // See if any bucket has a sentinel.
        if self.schedule_sentinels() {
            trace!("Some sentinels are scheduled.");
            return true;
        }

        // Try to open new buckets.
        let closure_bucket_opened = self.work_buckets[WorkBucketStage::Closure].is_activated()
            && self.bucket_update_progress.load(Ordering::SeqCst)
                <= WorkBucketStage::Closure.into_usize();
        if self.update_buckets() {
            trace!("Some buckets are opened.");
            if crate::inside_harness() {
                if !closure_bucket_opened
                    && self.bucket_update_progress.load(Ordering::SeqCst)
                        == WorkBucketStage::Closure.into_usize()
                {
                    super::TRACE_START.start();
                }
                if closure_bucket_opened
                    && self.bucket_update_progress.load(Ordering::SeqCst)
                        > WorkBucketStage::Closure.into_usize()
                {
                    let trace_us = super::TRACE_START.elapsed().as_micros();
                    super::TOTAL_TRACE_TIME_US.fetch_add(trace_us as usize, Ordering::SeqCst);
                    let total_trace_us = trace_us * self.num_workers() as u128;
                    let busy_us = super::TOTAL_TRACE_BUSY_TIME_US.load(Ordering::SeqCst);
                    let utilization: f32 = busy_us as f32 / total_trace_us as f32;
                    assert!(utilization <= 1.0, "{busy_us:.3} {total_trace_us:.3}");
                    super::TRACE_UTILIZATIONS.push(utilization);
                }
            }
            return true;
        }

        // If all of the above failed, it means GC has finished.
        false
    }

    fn do_class_unloading(&self, mmtk: &MMTK<VM>) {
        let perform_class_unloading = mmtk.get_plan().current_gc_should_perform_class_unloading();
        if mmtk.get_plan().downcast_ref::<LXR<VM>>().is_none() {
            if perform_class_unloading {
                gc_log!([3] "    - class unloading");
            }
            <VM as VMBinding>::VMCollection::vm_release(perform_class_unloading);
        }
    }

    fn dump_gc_stats(&self, mmtk: &MMTK<VM>) {
        let pause_time = crate::GC_START_TIME.elapsed();
        let pause = mmtk
            .get_plan()
            .downcast_ref::<LXR<VM>>()
            .map(|ix| ix.current_pause().unwrap())
            .unwrap_or(Pause::Full);
        crate::add_pause_time(pause, pause_time.as_nanos());
        if crate::verbose(2) {
            let _released_n =
                crate::policy::immix::immixspace::RELEASED_NURSERY_BLOCKS.load(Ordering::SeqCst);
            let _released =
                crate::policy::immix::immixspace::RELEASED_BLOCKS.load(Ordering::SeqCst);
            crate::policy::immix::immixspace::RELEASED_NURSERY_BLOCKS.store(0, Ordering::SeqCst);
            crate::policy::immix::immixspace::RELEASED_BLOCKS.store(0, Ordering::SeqCst);

            let pause_time = pause_time.as_micros() as f64 / 1000f64;
            let pause_s = match pause {
                Pause::RefCount => "RefCount",
                Pause::InitialMark => "InitialMark",
                Pause::FinalMark => "FinalMark",
                _ => "Full",
            };
            gc_log!([2]
                "GC({}) {} finished. {}M->{}M({}M) used={}M pause-time={:.3}ms",
                crate::GC_EPOCH.load(Ordering::SeqCst),
                pause_s,
                crate::RESERVED_PAGES_AT_GC_START.load(Ordering::SeqCst) / 256,
                mmtk.get_plan().get_reserved_pages() / 256,
                mmtk.get_plan().get_total_pages() / 256,
                mmtk.get_plan().get_used_pages() / 256,
                pause_time
            );
            if cfg!(feature = "lxr_precise_incs_counter") {
                crate::RC_STAT.dump(pause, pause_time);
            }
            crate::RESERVED_PAGES_AT_GC_END
                .store(mmtk.get_plan().get_reserved_pages(), Ordering::SeqCst);
        }
    }

    fn schedule_postponed_concurrent_packets(
        &self,
    ) -> (Injector<Box<dyn GCWork<VM>>>, Injector<Box<dyn GCWork<VM>>>) {
        let mut queue = Injector::new();
        type Q<VM> = Injector<Box<dyn GCWork<VM>>>;
        std::mem::swap::<Q<VM>>(&mut queue, &mut self.postponed_concurrent_work.write());

        let mut pqueue = Injector::new();
        std::mem::swap::<Q<VM>>(
            &mut pqueue,
            &mut self.postponed_concurrent_work_prioritized.write(),
        );
        (queue, pqueue)
    }

    /// Called when GC has finished, i.e. when all work packets have been executed.
    fn on_gc_finished(&self, worker: &GCWorker<VM>) {
        let stw_us = crate::GC_START_TIME.elapsed().as_micros() * self.num_workers() as u128;
        let busy_us = super::TOTAL_BUSY_TIME_US.load(Ordering::SeqCst);
        super::TOTAL_BUSY_TIME_US.store(0, Ordering::SeqCst);
        super::TOTAL_TRACE_BUSY_TIME_US.store(0, Ordering::SeqCst);
        let utilization: f32 = busy_us as f32 / stw_us as f32;
        if crate::inside_harness() {
            // println!("Utilization: {stw_us} / {busy_us} = {utilization:.2}");
            super::UTILIZATIONS.push(utilization);
        }
        // println!("Utilization: {:.2}%", utilization * 100.0);
        // All GC workers must have parked by now.
        debug_assert!(!self.worker_group.has_designated_work());
        debug_assert!(self.all_buckets_empty());

        // Deactivate all work buckets to prepare for the next GC.
        self.deactivate_all();
        self.debug_assert_all_buckets_deactivated();

        self.do_class_unloading(worker.mmtk);
        self.dump_gc_stats(worker.mmtk);

        let mmtk = worker.mmtk;

        let (queue, pqueue) = self.schedule_postponed_concurrent_packets();

        // Tell GC trigger that GC ended - this happens before we resume mutators.
        mmtk.gc_trigger.policy.on_gc_end(mmtk);

        // All other workers are parked, so it is safe to access the Plan instance mutably.
        #[cfg(feature = "tracing")]
        probe!(mmtk, plan_end_of_gc_begin);
        let plan_mut: &mut dyn Plan<VM = VM> = unsafe { mmtk.get_plan_mut() };
        plan_mut.end_of_gc(worker.tls);
        #[cfg(feature = "tracing")]
        probe!(mmtk, plan_end_of_gc_end);

        // Compute the elapsed time of the GC.
        let start_time = {
            let mut gc_start_time = worker.mmtk.state.gc_start_time.borrow_mut();
            gc_start_time.take().expect("GC not started yet?")
        };
        let elapsed = start_time.elapsed();

        info!(
            "End of GC ({}/{} pages, took {} ms)",
            mmtk.get_plan().get_reserved_pages(),
            mmtk.get_plan().get_total_pages(),
            elapsed.as_millis()
        );

        // USDT tracepoint for the end of GC.
        #[cfg(feature = "tracing")]
        probe!(mmtk, gc_end);

        #[cfg(feature = "count_live_bytes_in_gc")]
        {
            let live_bytes = mmtk.state.get_live_bytes_in_last_gc();
            let used_bytes =
                mmtk.get_plan().get_used_pages() << crate::util::constants::LOG_BYTES_IN_PAGE;
            debug_assert!(
                live_bytes <= used_bytes,
                "Live bytes of all live objects ({} bytes) is larger than used pages ({} bytes), something is wrong.",
                live_bytes, used_bytes
            );
            info!(
                "Live objects = {} bytes ({:04.1}% of {} used pages)",
                live_bytes,
                live_bytes as f64 * 100.0 / used_bytes as f64,
                mmtk.get_plan().get_used_pages()
            );
        }

        #[cfg(feature = "extreme_assertions")]
        if crate::util::slot_logger::should_check_duplicate_slots(mmtk.get_plan()) {
            // reset the logging info at the end of each GC
            mmtk.slot_logger.reset();
        }

        mmtk.get_plan().gc_pause_end();

        // Reset the triggering information.
        mmtk.state.reset_collection_trigger();

        // Set to NotInGC after everything, and right before resuming mutators.
        mmtk.set_gc_status(GcStatus::NotInGC);
        <VM as VMBinding>::VMCollection::resume_mutators(worker.tls);

        self.set_in_gc_pause(false);
        self.schedule_concurrent_packets(queue, pqueue);
        self.debug_assert_all_buckets_deactivated();
    }

    pub fn enable_stat(&self) {
        for worker in &self.worker_group.workers_shared {
            let worker_stat = worker.borrow_stat();
            worker_stat.enable();
        }
    }

    pub fn statistics(&self) -> HashMap<String, String> {
        let mut summary = SchedulerStat::default();
        for worker in &self.worker_group.workers_shared {
            let worker_stat = worker.borrow_stat();
            summary.merge(&worker_stat);
        }
        let mut stat = summary.harness_stat();
        if crate::plan::barriers::TAKERATE_MEASUREMENT {
            let fast = crate::plan::barriers::FAST_COUNT.load(Ordering::SeqCst);
            let slow = crate::plan::barriers::SLOW_COUNT.load(Ordering::SeqCst);
            stat.insert("barrier.fast".to_owned(), format!("{:?}", fast));
            stat.insert("barrier.slow".to_owned(), format!("{:?}", slow));
            stat.insert(
                "barrier.takerate".to_owned(),
                format!("{}", slow as f64 / fast as f64),
            );
            if crate::args::HARNESS_PRETTY_PRINT {
                println!(
                    "barrier: fast={} slow={} takerate={}",
                    fast,
                    slow,
                    slow as f64 / fast as f64
                );
            }
        }
        let mut utilizations = vec![];
        while let Some(x) = super::UTILIZATIONS.pop() {
            utilizations.push(x);
        }
        let mean = utilizations.iter().sum::<f32>() / utilizations.len() as f32;
        let min = utilizations
            .iter()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max = utilizations
            .iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let geomean = utilizations
            .iter()
            .product::<f32>()
            .powf(1.0 / utilizations.len() as f32);
        println!("Utilization: {:?}", utilizations);
        stat.insert("utilization.mean".to_owned(), format!("{:.2}", mean));
        stat.insert("utilization.min".to_owned(), format!("{:.2}", min));
        stat.insert("utilization.max".to_owned(), format!("{:.2}", max));
        stat.insert("utilization.geomean".to_owned(), format!("{:.2}", geomean));
        // Trace utilization
        let mut utilizations = vec![];
        while let Some(x) = super::TRACE_UTILIZATIONS.pop() {
            utilizations.push(x);
        }
        println!("TRACE Utilization: {:?}", utilizations);
        let mean = utilizations.iter().sum::<f32>() / utilizations.len() as f32;
        let min = utilizations
            .iter()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max = utilizations
            .iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let geomean = utilizations
            .iter()
            .product::<f32>()
            .powf(1.0 / utilizations.len() as f32);
        stat.insert("trace.utilization.mean".to_owned(), format!("{:.2}", mean));
        stat.insert("trace.utilization.min".to_owned(), format!("{:.2}", min));
        stat.insert("trace.utilization.max".to_owned(), format!("{:.2}", max));
        stat.insert(
            "trace.utilization.geomean".to_owned(),
            format!("{:.2}", geomean),
        );

        let trace_time = super::TOTAL_TRACE_TIME_US.load(Ordering::SeqCst);
        stat.insert(
            "time.trace".to_owned(),
            format!("{:.2}", trace_time as f64 / 1000.0),
        );

        stat
    }

    pub fn notify_mutators_paused(&self, mmtk: &'static MMTK<VM>) {
        mmtk.gc_requester.clear_request();
        let first_stw_bucket = &self.work_buckets[WorkBucketStage::first_stw_stage()];
        // debug_assert!(!first_stw_bucket.is_activated());
        // Note: This is the only place where a bucket is opened without having all workers parked.
        // We usually require all workers to park before opening new buckets because otherwise
        // packets will be executed out of order.  However, since `Prepare` is the first STW
        // bucket, and all subsequent buckets require all workers to park before opening, workers
        // cannot execute work packets out of order.  This is not generally true if we are not
        // opening the first STW bucket.  In the future, we should redesign the opening condition
        // of work buckets to make the synchronization more robust,
        first_stw_bucket.activate();
        gc_log!([3]
            " - ({:.3}ms) Start GC Stage: {:?}",
            crate::gc_start_time_ms(),
            WorkBucketStage::from_usize(1)
        );
        if first_stw_bucket.is_empty()
            && self.worker_monitor.parked.load(Ordering::SeqCst) + 1 == self.num_workers()
            && crate::concurrent_marking_packets_drained()
            && crate::LazySweepingJobs::all_finished()
        {
            let second_stw_bucket = &self.work_buckets[WorkBucketStage::from_usize(2)];
            second_stw_bucket.activate();
            gc_log!([3]
                " - ({:.3}ms) Start GC Stage: {:?}",
                crate::gc_start_time_ms(),
                WorkBucketStage::from_usize(2)
            );
        }
        self.worker_monitor.notify_work_available(true);
    }
    pub fn wakeup_all_conc_workers(&self) {
        self.worker_monitor.notify_work_available(true);
    }
}
