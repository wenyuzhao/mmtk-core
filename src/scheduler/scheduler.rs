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
use crate::util::opaque_pointer::*;
use crate::util::options::AffinityKind;
use crate::util::reference_processor::PhantomRefProcessing;
use crate::vm::Collection;
use crate::vm::VMBinding;
use crate::Pause;
use crate::Plan;
use crossbeam::deque::Steal;
use enum_map::EnumMap;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Instant;

pub struct GCWorkScheduler<VM: VMBinding> {
    active_bucket: ActiveWorkBucket,
    /// Workers
    pub(crate) worker_group: Arc<WorkerGroup<VM>>,
    /// For synchronized communication between workers and with mutators.
    pub(crate) worker_monitor: Arc<WorkerMonitor>,
    /// How to assign the affinity of each GC thread. Specified by the user.
    affinity: AffinityKind,
    pub(super) postponed_concurrent_work: spin::RwLock<SegQueue<Box<dyn GCWork>>>,
    pub(super) postponed_concurrent_work_prioritized: spin::RwLock<SegQueue<Box<dyn GCWork>>>,
    in_gc_pause: AtomicBool,
    current_schedule: RwLock<Cow<'static, BucketGraph>>,
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

        Arc::new(Self {
            active_bucket: ActiveWorkBucket::new(worker_monitor.clone()),
            worker_group,
            worker_monitor,
            affinity,
            postponed_concurrent_work: spin::RwLock::new(SegQueue::new()),
            postponed_concurrent_work_prioritized: spin::RwLock::new(SegQueue::new()),
            in_gc_pause: AtomicBool::new(false),
            current_schedule: RwLock::new(Cow::Borrowed(&*EMPTY_SCHEDULE)),
        })
    }

    pub fn spawn_boxed(&self, bucket: BucketId, w: Box<dyn GCWork>) {
        // Increment counter
        bucket.get_bucket().inc();
        // Add to the corresponding bucket/queue
        if bucket.get_bucket().is_open() {
            // The bucket is open. Either add to the global pool, or the thread local queue
            if let Err(w) = GCWorker::<VM>::current().add_local_packet(bucket, w) {
                self.active_bucket.add_boxed(bucket, w);
            }
        } else {
            // The bucket is closed. Add to the bucket's queue
            bucket.get_bucket().add(w);
        }
    }

    pub fn spawn(&self, bucket: BucketId, w: impl GCWork) {
        self.spawn_boxed(bucket, Box::new(w))
    }

    pub fn spawn_bulk(&self, bucket: BucketId, ws: Vec<Box<dyn GCWork>>) {
        for w in ws {
            self.spawn_boxed(bucket, w);
        }
    }

    pub fn execute(&self, graph: &'static BucketGraph) {
        println!("current_schedule = {:?}", graph as *const BucketGraph);
        // reset all buckets
        let mut schedule = self.current_schedule.write().unwrap();
        *schedule = Cow::Borrowed(graph);
    }

    pub fn merge_schedule(&self, graph: &'static BucketGraph) {
        let mut schedule = self.current_schedule.write().unwrap();
        schedule.to_mut().merge(graph);
    }

    pub fn pause_concurrent_marking_work_packets_during_gc(&self) {
        // let mut unconstrained_queue = Injector::new();
        // unconstrained_queue =
        //     self.work_buckets[WorkBucketStage::Unconstrained].swap_queue(unconstrained_queue);
        // let postponed_queue = self.postponed_concurrent_work.read();
        // if !unconstrained_queue.is_empty() {
        //     loop {
        //         match unconstrained_queue.steal() {
        //             Steal::Empty => break,
        //             Steal::Success(x) => postponed_queue.push(x),
        //             Steal::Retry => continue,
        //         }
        //     }
        // }
        // crate::PAUSE_CONCURRENT_MARKING.store(true, Ordering::SeqCst);
        unimplemented!()
    }

    pub fn process_lazy_decrement_packets(&self) {
        let mut postponed_concurrent_work = self.postponed_concurrent_work_prioritized.write();
        let mut new_queue = SegQueue::new();
        std::mem::swap(&mut *postponed_concurrent_work, &mut new_queue);
        BucketId::Decs.get_bucket().set_queue(new_queue);
    }

    pub fn postpone(&self, w: impl GCWork) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work.read().push(Box::new(w))
    }

    pub fn postpone_prioritized(&self, w: impl GCWork) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work_prioritized
            .read()
            .push(Box::new(w))
    }

    pub fn postpone_dyn(&self, w: Box<dyn GCWork>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work.read().push(w)
    }

    pub fn postpone_dyn_prioritized(&self, w: Box<dyn GCWork>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work_prioritized.read().push(w)
    }

    pub fn postpone_all(&self, ws: Vec<Box<dyn GCWork>>) {
        let postponed_concurrent_work = self.postponed_concurrent_work.read();
        ws.into_iter()
            .for_each(|w| postponed_concurrent_work.push(w));
    }

    pub fn postpone_all_prioritized(&self, ws: Vec<Box<dyn GCWork>>) {
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
        self.spawn(BucketId::Start, ScheduleCollection::<VM>::default());
    }

    pub fn schedule_common_work_no_refs<C: GCWorkContext<VM = VM>>(
        &self,
        plan: &'static C::PlanType,
    ) {
        use crate::scheduler::gc_work::*;
        // Stop & scan mutators (mutator scanning can happen before STW)
        self.spawn(BucketId::Start, StopMutators::<C>::new());

        // Prepare global/collectors/mutators
        self.spawn(BucketId::Prepare, Prepare::<C>::new(plan));

        // Release global/collectors/mutators
        self.spawn(BucketId::Release, Release::<C>::new(plan));

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
            self.spawn(
                BucketId::PhantomRefClosure,
                PhantomRefProcessing::<C::DefaultProcessEdges>::new(),
            );

            // VM-specific weak ref processing
            self.spawn(
                BucketId::WeakRefClosure,
                VMProcessWeakRefs::<C::DefaultProcessEdges>::new(),
            );

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
            use crate::util::finalizable_processor::Finalization;
            // finalization
            self.spawn(
                BucketId::FinalRefClosure,
                Finalization::<C::DefaultProcessEdges>::new(),
            );
            // forward refs
            if plan.constraints().needs_forward_after_liveness {
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
            self.spawn(
                BucketId::PhantomRefClosure,
                PhantomRefProcessing::<C::DefaultProcessEdges>::new(),
            );
            // VM-specific weak ref processing
            self.spawn(
                BucketId::WeakRefClosure,
                VMProcessWeakRefs::<C::DefaultProcessEdges>::new(),
            );
        }

        // Finalization
        if !*plan.base().options.no_finalizer {
            use crate::util::finalizable_processor::Finalization;
            self.spawn(
                BucketId::FinalRefClosure,
                Finalization::<C::DefaultProcessEdges>::new(),
            );
        }
    }

    // fn are_buckets_drained(&self, buckets: &[WorkBucketStage]) -> bool {
    //     // debug_assert!(
    //     //     self.pending_coordinator_packets.load(Ordering::SeqCst) == 0,
    //     //     "GCWorker attempted to open buckets when there are pending coordinator work packets"
    //     // );
    //     buckets
    //         .iter()
    //         .all(|&b| self.work_buckets[b].is_drained() || self.work_buckets[b].disabled())
    // }

    pub fn all_buckets_empty(&self) -> bool {
        self.schedule()
            .all_buckets
            .iter()
            .all(|&b| b.get_bucket().is_empty())
    }

    #[allow(unused)]
    pub(super) fn schedule_concurrent_packets(
        &self,
        queue: SegQueue<Box<dyn GCWork>>,
        pqueue: SegQueue<Box<dyn GCWork>>,
    ) {
        crate::MOVE_CONCURRENT_MARKING_TO_STW.store(false, Ordering::SeqCst);
        crate::PAUSE_CONCURRENT_MARKING.store(false, Ordering::SeqCst);
        let mut notify = !queue.is_empty() || !pqueue.is_empty();
        println!(
            "schedule_concurrent_packets: count={},{}",
            queue.len(),
            pqueue.len()
        );
        assert!(queue.is_empty());
        BucketId::Decs.get_bucket().set_queue(pqueue);
        if notify {
            self.notify_bucket_empty(None);
        }
    }

    /// Schedule "sentinel" work packets for all activated buckets.
    pub(crate) fn schedule_sentinels(&self) -> bool {
        // let mut new_packets = false;
        // for (id, work_bucket) in self.work_buckets.iter() {
        //     if work_bucket.is_activated() && work_bucket.maybe_schedule_sentinel() {
        //         trace!("Scheduled sentinel packet into {:?}", id);
        //         new_packets = true;
        //     }
        // }
        // new_packets
        // unimplemented!()
        false
    }

    pub(crate) fn schedule(&self) -> RwLockReadGuard<Cow<BucketGraph>> {
        let guard = self.current_schedule.read().unwrap();
        assert!(!guard.is_empty());
        guard
    }

    /// Open buckets if their conditions are met.
    ///
    /// This function should only be called after all the workers are parked.
    /// No workers will be waked up by this function. The caller is responsible for that.
    ///
    /// Return true if there're any non-empty buckets updated.
    pub(crate) fn notify_bucket_empty(&self, bucket: Option<BucketId>) -> bool {
        println!("notify_bucket_empty, bucket: {:?}", bucket);
        let mut new_buckets = false;
        let mut new_work = false;
        self.schedule().update(bucket, |b| {
            // dump everything to the active bucket
            let q = b.get_bucket().take_queue();
            while let Some(q) = q.pop() {
                self.active_bucket.add_boxed_no_notify(b, q);
                new_work = true;
            }
            new_buckets = true;
        });
        if new_buckets && new_work {
            println!("notify_work_available");
            self.worker_monitor.notify_work_available(true)
        }
        println!("notify_bucket_empty END");
        new_buckets && new_work

        // let mut buckets_updated = false;
        // let mut new_packets = false;
        // let start_index = self.bucket_update_progress.load(Ordering::SeqCst) + 1;
        // let mut new_progress = WorkBucketStage::LENGTH;
        // for i in start_index..WorkBucketStage::LENGTH {
        //     let id = WorkBucketStage::from_usize(i);
        //     if id == WorkBucketStage::Unconstrained {
        //         continue;
        //     }

        //     let bucket = &self.work_buckets[id];
        //     if bucket.disabled() || bucket.is_activated() {
        //         continue;
        //     }
        //     let bucket_opened = bucket.update(self);
        //     #[cfg(feature = "tracing")]
        //     if bucket_opened {
        //         probe!(mmtk, bucket_opened, id);
        //     }
        //     let verbose = crate::verbose(3);
        //     if (verbose || cfg!(feature = "pause_time")) && bucket_opened {
        //         if verbose {
        //             gc_log!([3]
        //                 " - ({:.3}ms) Start GC Stage: {:?}",
        //                 crate::GC_START_TIME
        //                     .elapsed()
        //                     .as_nanos() as f64
        //                     / 1000000f64,
        //                 id
        //             );
        //         }
        //     }
        //     if cfg!(feature = "yield_and_roots_timer")
        //         && bucket_opened
        //         && id == WorkBucketStage::Prepare
        //     {
        //         let t = crate::GC_START_TIME.elapsed().as_nanos();
        //         crate::counters().roots_nanos.fetch_add(t, Ordering::SeqCst);
        //     }
        //     buckets_updated = buckets_updated || bucket_opened;
        //     if bucket_opened {
        //         #[cfg(feature = "tracing")]
        //         probe!(mmtk, bucket_opened, id);
        //         new_packets = new_packets || !bucket.is_drained();
        //         new_packets = new_packets || bucket.maybe_schedule_sentinel();
        //         if new_packets {
        //             new_progress = i;
        //             // Quit the loop. A sentinel packet is added to the newly opened buckets.
        //             trace!("Sentinel is scheduled at stage {:?}.  Break.", id);
        //             break;
        //         }
        //     }
        // }
        // if buckets_updated && new_packets {
        //     self.bucket_update_progress
        //         .store(new_progress, Ordering::SeqCst);
        // } else {
        //     self.bucket_update_progress.store(0, Ordering::SeqCst);
        // }
        // buckets_updated && new_packets
    }

    pub fn deactivate_all(&self) {
        let mut schedule = self.current_schedule.write().unwrap();
        schedule.reset();
        *schedule = Cow::Borrowed(&*EMPTY_SCHEDULE);
    }

    pub fn reset_state(&self) {
        unreachable!()
    }

    pub fn debug_assert_all_buckets_deactivated(&self) {
        if cfg!(debug_assertions) {
            for b in self.schedule().all_buckets.iter() {
                assert!(!self.schedule().bucket_is_open(*b));
            }
        }
    }

    /// Check if all the work buckets are empty
    #[allow(unused)]
    pub(crate) fn assert_all_activated_buckets_are_empty(&self) {
        // let mut error_example = None;
        for b in self.schedule().all_buckets.iter() {
            assert!(b.get_bucket().is_empty());
        }
        assert!(self.active_bucket.is_empty());
        // if let Some(id) = error_example {
        //     panic!("Some active buckets (such as {:?}) are not empty.", id);
        // }
    }

    pub(super) fn set_in_gc_pause(&self, in_gc_pause: bool) {
        self.in_gc_pause.store(in_gc_pause, Ordering::SeqCst);
        // for wb in self.work_buckets.values() {
        //     wb.set_in_concurrent(!in_gc_pause);
        // }
    }

    pub fn in_concurrent(&self) -> bool {
        !self.in_gc_pause.load(Ordering::SeqCst)
    }

    /// Get a schedulable work packet without retry.
    fn poll_schedulable_work_once(
        &self,
        worker: &GCWorker<VM>,
    ) -> Steal<(BucketId, Box<dyn GCWork>)> {
        let mut should_retry = false;
        // Try find a packet that can be processed only by this worker.
        if let Some(w) = worker.shared.designated_work.pop() {
            return Steal::Success(w);
        }
        // Try get a packet from the active work bucket.
        match self.active_bucket.poll(&worker.local_work_buffer) {
            Steal::Success(w) => return Steal::Success(w),
            Steal::Retry => should_retry = true,
            _ => {}
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

    /// Get a schedulable work packet.
    fn poll_schedulable_work(&self, worker: &GCWorker<VM>) -> Option<(BucketId, Box<dyn GCWork>)> {
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
    pub(crate) fn poll(&self, worker: &GCWorker<VM>) -> PollResult {
        if let Some(work) = self.poll_schedulable_work(worker) {
            return Ok(work);
        }
        self.poll_slow(worker)
    }

    fn poll_slow(&self, worker: &GCWorker<VM>) -> PollResult {
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
            self.deactivate_all();
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
                    unreachable!()
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

                crate::GC_TRIGGER_TIME.start();
                GCWorker::<VM>::mmtk().get_plan().schedule_collection(self);
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

    fn take_postponed_concurrent_packets(
        &self,
    ) -> (SegQueue<Box<dyn GCWork>>, SegQueue<Box<dyn GCWork>>) {
        let mut queue = SegQueue::new();
        type Q = SegQueue<Box<dyn GCWork>>;
        std::mem::swap::<Q>(&mut queue, &mut self.postponed_concurrent_work.write());

        let mut pqueue = SegQueue::new();
        std::mem::swap::<Q>(
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
        // self.debug_assert_all_buckets_deactivated();

        self.do_class_unloading(worker.mmtk);
        self.dump_gc_stats(worker.mmtk);

        let mmtk = worker.mmtk;

        let (queue, pqueue) = self.take_postponed_concurrent_packets();

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
        // self.debug_assert_all_buckets_deactivated();
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
        stat
    }

    pub fn notify_mutators_paused(&self, mmtk: &'static MMTK<VM>) {
        mmtk.gc_requester.clear_request();
        // let first_stw_bucket = &self.work_buckets[WorkBucketStage::first_stw_stage()];
        // // debug_assert!(!first_stw_bucket.is_activated());
        // // Note: This is the only place where a bucket is opened without having all workers parked.
        // // We usually require all workers to park before opening new buckets because otherwise
        // // packets will be executed out of order.  However, since `Prepare` is the first STW
        // // bucket, and all subsequent buckets require all workers to park before opening, workers
        // // cannot execute work packets out of order.  This is not generally true if we are not
        // // opening the first STW bucket.  In the future, we should redesign the opening condition
        // // of work buckets to make the synchronization more robust,
        // first_stw_bucket.activate();
        // gc_log!([3]
        //     " - ({:.3}ms) Start GC Stage: {:?}",
        //     crate::gc_start_time_ms(),
        //     WorkBucketStage::from_usize(1)
        // );
        // if first_stw_bucket.is_empty()
        //     && self.worker_monitor.parked.load(Ordering::SeqCst) + 1 == self.num_workers()
        //     && crate::concurrent_marking_packets_drained()
        //     && crate::LazySweepingJobs::all_finished()
        // {
        //     let second_stw_bucket = &self.work_buckets[WorkBucketStage::from_usize(2)];
        //     second_stw_bucket.activate();
        //     gc_log!([3]
        //         " - ({:.3}ms) Start GC Stage: {:?}",
        //         crate::gc_start_time_ms(),
        //         WorkBucketStage::from_usize(2)
        //     );
        // }
        // self.worker_monitor.notify_work_available(true);
    }

    pub fn wakeup_all_conc_workers(&self) {
        self.worker_monitor.notify_work_available(true);
    }
}

pub struct BucketGraph {
    pub(crate) preds: EnumMap<BucketId, Vec<BucketId>>,
    pub(crate) succs: EnumMap<BucketId, Vec<BucketId>>,
    pub(crate) all_buckets: HashSet<BucketId>,
    lock: RwLock<()>,
}

pub static EMPTY_SCHEDULE: spin::Lazy<BucketGraph> = spin::Lazy::new(|| BucketGraph::new());

impl Clone for BucketGraph {
    fn clone(&self) -> Self {
        Self {
            preds: self.preds.clone(),
            succs: self.succs.clone(),
            all_buckets: self.all_buckets.clone(),
            lock: RwLock::new(()),
        }
    }
}

impl BucketGraph {
    pub fn new() -> Self {
        Self {
            preds: EnumMap::default(),
            succs: EnumMap::default(),
            all_buckets: HashSet::new(),
            lock: RwLock::new(()),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        for (b, preds) in &other.preds {
            self.preds[b].extend(preds.iter().cloned());
        }
        for (b, succs) in &other.succs {
            self.succs[b].extend(succs.iter().cloned());
        }
        // dedup
        for (_, preds) in &mut self.preds {
            preds.dedup();
        }
        self.all_buckets.extend(&other.all_buckets);
    }

    pub fn is_empty(&self) -> bool {
        self.all_buckets.is_empty()
    }

    pub fn bucket_is_open(&self, b: BucketId) -> bool {
        b.get_bucket().is_open()
    }

    pub fn dep(&mut self, parent: BucketId, children: Vec<BucketId>) {
        self.all_buckets.insert(parent);
        for c in &children {
            self.all_buckets.insert(*c);
        }
        // self.deps[parent].extend(children);
        for c in children {
            self.preds[c].push(parent);
            self.succs[parent].push(c);
        }
    }

    fn reset(&self) {
        for b in &self.all_buckets {
            println!("RESET BUCKET {:?}", b);
            b.get_bucket().reset()
        }
    }

    fn bucket_can_open(&self, b: BucketId) -> bool {
        if self.bucket_is_open(b) {
            return false;
        }
        for pred in &self.preds[b] {
            if !self.bucket_is_open(*pred) {
                return false;
            }
            if !pred.get_bucket().is_empty() {
                return false;
            }
        }
        true
    }

    fn update(&self, bucket: Option<BucketId>, mut on_bucket_open: impl FnMut(BucketId)) {
        println!("UPDATE BUCKET {:?}", bucket);
        let _lock = self.lock.write().unwrap();
        let mut open_buckets = vec![];
        if let Some(b) = bucket {
            if crate::GC_START_TIME.ready() {
                gc_log!([3]
                    " - ({:.3}ms) Bucket {:?} finished",
                    crate::GC_START_TIME.elapsed_ms(),
                    b
                );
            }
            // Check successors
            for s in &self.succs[b] {
                if self.bucket_can_open(*s) {
                    open_buckets.push(*s);
                }
            }
        } else {
            // Check all buckets
            for b in &self.all_buckets {
                if self.bucket_can_open(*b) {
                    open_buckets.push(*b);
                }
            }
        }
        // If the newly opened bucket is empty, we can open its successors.
        for b in open_buckets {
            b.get_bucket().open();
            on_bucket_open(b);
            if crate::GC_START_TIME.ready() {
                gc_log!([3]
                    " - ({:.3}ms) Bucket {:?} opened",
                    crate::GC_START_TIME.elapsed_ms(),
                    b
                );
            }
            println!("Bucket {:?} opened", b);
        }
    }
}
