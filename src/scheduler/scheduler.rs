use super::stat::SchedulerStat;
use super::work_bucket::*;
use super::worker::{GCWorker, GCWorkerShared, ThreadId, WorkerGroup, WorkerMonitor};
use super::*;
use crate::mmtk::MMTK;
use crate::util::opaque_pointer::*;
use crate::util::options::AffinityKind;
use crate::util::reference_processor::PhantomRefProcessing;
use crate::vm::Collection;
use crate::vm::{GCThreadContext, VMBinding};
use crossbeam::deque::{self, Injector, Steal};
use enum_map::Enum;
use enum_map::{enum_map, EnumMap};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

pub struct GCWorkScheduler<VM: VMBinding> {
    /// Work buckets
    pub work_buckets: EnumMap<WorkBucketStage, WorkBucket<VM>>,
    /// Workers
    pub(crate) worker_group: Arc<WorkerGroup<VM>>,
    /// The shared part of the GC worker object of the controller thread
    coordinator_worker_shared: Arc<GCWorkerShared<VM>>,
    /// Condition Variable for worker synchronization
    pub(crate) worker_monitor: Arc<WorkerMonitor>,
    pub(super) postponed_concurrent_work: spin::RwLock<Injector<Box<dyn GCWork<VM>>>>,
    pub(super) postponed_concurrent_work_prioritized: spin::RwLock<Injector<Box<dyn GCWork<VM>>>>,
    pub(super) in_gc_pause: AtomicBool,
    /// How to assign the affinity of each GC thread. Specified by the user.
    affinity: AffinityKind,
    bucket_update_progress: AtomicUsize,
    pub(super) controller_channel: (mpsc::Sender<()>, mpsc::Receiver<()>),
}

// The 'channel' inside Scheduler disallows Sync for Scheduler. We have to make sure we use channel properly:
// 1. We should never directly use Sender. We clone the sender and let each worker have their own copy.
// 2. Only the coordinator can use Receiver.
// TODO: We should remove channel from Scheduler, and directly send Sender/Receiver when creating the coordinator and
// the workers.
unsafe impl<VM: VMBinding> Sync for GCWorkScheduler<VM> {}

impl<VM: VMBinding> GCWorkScheduler<VM> {
    pub fn new(num_workers: usize, affinity: AffinityKind) -> Arc<Self> {
        let worker_monitor: Arc<WorkerMonitor> = Arc::new(WorkerMonitor::new(num_workers));
        let worker_group = WorkerGroup::new(num_workers);

        // Create work buckets for workers.
        let mut work_buckets = enum_map! {
            WorkBucketStage::Unconstrained => WorkBucket::new(true, worker_monitor.clone()),
            WorkBucketStage::FinishConcurrentWork => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::Initial => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::Prepare => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::Closure => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::SoftRefClosure => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::WeakRefClosure => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::FinalRefClosure => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::PhantomRefClosure => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::VMRefClosure => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::CalculateForwarding => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::SecondRoots => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::RefForwarding => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::FinalizableForwarding => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::VMRefForwarding => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::Compact => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::Release => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::STWRCDecsAndSweep => WorkBucket::new(false, worker_monitor.clone()),
            WorkBucketStage::Final => WorkBucket::new(false, worker_monitor.clone()),
        };

        work_buckets[WorkBucketStage::Unconstrained].enable_prioritized_queue();

        // Set the open condition of each bucket.
        {
            // Unconstrained is always open. Prepare will be opened at the beginning of a GC.
            // This vec will grow for each stage we call with open_next()
            let first_stw_stage = WorkBucketStage::first_stw_stage();
            let mut open_stages: Vec<WorkBucketStage> = vec![first_stw_stage];
            // The rest will open after the previous stage is done.
            let stages = (0..WorkBucketStage::LENGTH).map(WorkBucketStage::from_usize);
            for stage in stages {
                if stage != WorkBucketStage::Unconstrained && stage != first_stw_stage {
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

        let coordinator_worker_shared = Arc::new(GCWorkerShared::<VM>::new(None));

        Arc::new(Self {
            work_buckets,
            worker_group,
            coordinator_worker_shared,
            worker_monitor,
            postponed_concurrent_work: spin::RwLock::new(Injector::new()),
            postponed_concurrent_work_prioritized: spin::RwLock::new(Injector::new()),
            in_gc_pause: AtomicBool::new(false),
            affinity,
            bucket_update_progress: AtomicUsize::new(0),
            controller_channel: mpsc::channel(),
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
        self.work_buckets[WorkBucketStage::STWRCDecsAndSweep].bulk_add(no_postpone);
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

    /// Create GC threads, including the controller thread and all workers.
    pub fn spawn_gc_threads(self: &Arc<Self>, mmtk: &'static MMTK<VM>, tls: VMThread) {
        // Spawn the controller thread.
        let coordinator_worker = GCWorker::new(
            mmtk,
            usize::MAX,
            self.clone(),
            true,
            self.coordinator_worker_shared.clone(),
            deque::Worker::new_fifo(),
        );
        let gc_controller = GCController::new(
            mmtk,
            mmtk.plan.base().gc_requester.clone(),
            self.clone(),
            coordinator_worker,
        );
        VM::VMCollection::spawn_gc_thread(tls, GCThreadContext::<VM>::Controller(gc_controller));

        self.worker_group.spawn(mmtk, tls)
    }

    /// Resolve the affinity of a thread.
    pub fn resolve_affinity(&self, thread: ThreadId) {
        self.affinity.resolve_affinity(thread);
    }

    pub fn schedule_common_work_no_refs<C: GCWorkContext<VM = VM> + 'static>(
        &self,
        plan: &'static C::PlanType,
    ) {
        use crate::scheduler::gc_work::*;
        // Stop & scan mutators (mutator scanning can happen before STW)
        self.work_buckets[WorkBucketStage::Unconstrained]
            .add(StopMutators::<C::ProcessEdgesWorkType>::new());

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

        use crate::plan::Plan;
        use crate::scheduler::gc_work::*;
        // Reference processing
        if !*plan.base().options.no_reference_types || !*plan.base().options.no_finalizer {
            // use crate::util::reference_processor::{
            //     PhantomRefProcessing, SoftRefProcessing, WeakRefProcessing,
            // };
            // self.work_buckets[WorkBucketStage::WeakRefClosure]
            //     .add(SoftRefProcessing::<C::ProcessEdgesWorkType>::new());
            // self.work_buckets[WorkBucketStage::WeakRefClosure]
            //     .add(WeakRefProcessing::<C::ProcessEdgesWorkType>::new());
            self.work_buckets[WorkBucketStage::PhantomRefClosure]
                .add(PhantomRefProcessing::<C::ProcessEdgesWorkType>::new());

            // VM-specific weak ref processing
            self.work_buckets[WorkBucketStage::WeakRefClosure]
                .add(VMProcessWeakRefs::<C::ProcessEdgesWorkType>::new());

            // use crate::util::reference_processor::RefForwarding;
            // if plan.constraints().needs_forward_after_liveness {
            //     self.work_buckets[WorkBucketStage::RefForwarding]
            //         .add(RefForwarding::<C::ProcessEdgesWorkType>::new());
            // }

            // use crate::util::reference_processor::RefEnqueue;
            // self.work_buckets[WorkBucketStage::Release].add(RefEnqueue::<VM>::new());
        }

        // Finalization
        if !*plan.base().options.no_finalizer {
            use crate::util::finalizable_processor::{Finalization, ForwardFinalization};
            // finalization
            self.work_buckets[WorkBucketStage::FinalRefClosure]
                .add(Finalization::<C::ProcessEdgesWorkType>::new());
            // forward refs
            if plan.constraints().needs_forward_after_liveness {
                self.work_buckets[WorkBucketStage::FinalizableForwarding]
                    .add(ForwardFinalization::<C::ProcessEdgesWorkType>::new());
                unimplemented!()
            }
        }
    }

    /// Schedule all the common work packets
    pub fn schedule_ref_proc_work<C: GCWorkContext<VM = VM> + 'static>(
        &self,
        plan: &'static C::PlanType,
    ) {
        use crate::plan::Plan;
        use crate::scheduler::gc_work::*;

        // Reference processing
        if !*plan.base().options.no_reference_types || !*plan.base().options.no_finalizer {
            // use crate::util::reference_processor::{
            //     PhantomRefProcessing, SoftRefProcessing, WeakRefProcessing,
            // };
            // self.work_buckets[WorkBucketStage::WeakRefClosure]
            //     .add(SoftRefProcessing::<C::ProcessEdgesWorkType>::new());
            // self.work_buckets[WorkBucketStage::WeakRefClosure]
            //     .add(WeakRefProcessing::<C::ProcessEdgesWorkType>::new());

            // VM-specific weak ref processing
            self.work_buckets[WorkBucketStage::WeakRefClosure]
                .add(VMProcessWeakRefs::<C::ProcessEdgesWorkType>::new());
            self.work_buckets[WorkBucketStage::PhantomRefClosure]
                .add(PhantomRefProcessing::<C::ProcessEdgesWorkType>::new());

            // use crate::util::reference_processor::RefForwarding;
            // if plan.constraints().needs_forward_after_liveness {
            //     self.work_buckets[WorkBucketStage::RefForwarding]
            //         .add(RefForwarding::<C::ProcessEdgesWorkType>::new());
            // }

            // use crate::util::reference_processor::RefEnqueue;
            // self.work_buckets[WorkBucketStage::Release].add(RefEnqueue::<VM>::new());
        }

        // Finalization
        if !*plan.base().options.no_finalizer {
            use crate::util::finalizable_processor::{Finalization, ForwardFinalization};
            // finalization
            self.work_buckets[WorkBucketStage::FinalRefClosure]
                .add(Finalization::<C::ProcessEdgesWorkType>::new());
            // forward refs
            if plan.constraints().needs_forward_after_liveness {
                self.work_buckets[WorkBucketStage::FinalizableForwarding]
                    .add(ForwardFinalization::<C::ProcessEdgesWorkType>::new());
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
        //     .set_sentinel(Box::new(VMProcessWeakRefs::<C::ProcessEdgesWorkType>::new()));

        // if plan.constraints().needs_forward_after_liveness {
        //     // VM-specific weak ref forwarding
        //     self.work_buckets[WorkBucketStage::VMRefForwarding]
        //         .add(VMForwardWeakRefs::<C::ProcessEdgesWorkType>::new());
        // }

        // self.work_buckets[WorkBucketStage::Release].add(VMPostForwarding::<VM>::default());
    }

    fn are_buckets_drained(&self, buckets: &[WorkBucketStage]) -> bool {
        buckets
            .iter()
            .all(|&b| self.work_buckets[b].is_drained() || self.work_buckets[b].disabled())
    }

    pub fn all_buckets_empty(&self) -> bool {
        self.work_buckets
            .values()
            .all(|bucket| bucket.is_empty() || bucket.disabled())
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
            self.work_buckets[WorkBucketStage::Unconstrained].notify_all_workers();
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
                            .load(Ordering::SeqCst)
                            .elapsed()
                            .unwrap()
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
                let t = crate::GC_START_TIME
                    .load(Ordering::SeqCst)
                    .elapsed()
                    .unwrap()
                    .as_nanos();
                crate::counters().roots_nanos.fetch_add(t, Ordering::SeqCst);
            }
            buckets_updated = buckets_updated || bucket_opened;
            if bucket_opened {
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

    pub fn in_concurrent(&self) -> bool {
        !self.in_gc_pause.load(Ordering::SeqCst)
    }

    /// Check if all the work buckets are empty
    fn all_activated_buckets_are_empty(&self) -> bool {
        for bucket in self.work_buckets.values() {
            if bucket.is_activated() && !bucket.is_drained() {
                return false;
            }
        }
        true
    }

    /// Check if all the work buckets are empty
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

    /// Get a schedulable work packet without retry.
    fn poll_schedulable_work_once(&self, worker: &GCWorker<VM>) -> Steal<Box<dyn GCWork<VM>>> {
        let mut should_retry = false;
        // Try find a packet that can be processed only by this worker.
        if let Some(w) = worker.shared.designated_work.pop() {
            return Steal::Success(w);
        }
        if self.in_concurrent() && !worker.is_concurrent_worker() {
            return Steal::Empty;
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
    pub fn poll(&self, worker: &GCWorker<VM>) -> Box<dyn GCWork<VM>> {
        self.poll_schedulable_work(worker)
            .unwrap_or_else(|| self.poll_slow(worker))
    }

    fn find_more_work_for_workers(&self) -> bool {
        if self.worker_group.has_designated_work() {
            return true;
        }

        // See if any bucket has a sentinel.
        if self.schedule_sentinels() {
            return true;
        }

        // Try to open new buckets.
        if self.update_buckets() {
            return true;
        }

        // If all of the above failed, it means GC has finished.
        false
    }

    fn poll_slow(&self, worker: &GCWorker<VM>) -> Box<dyn GCWork<VM>> {
        let mut sync = self.worker_monitor.sync.lock().unwrap();
        loop {
            flush_logs!();
            // Retry polling
            if let Some(work) = self.poll_schedulable_work(worker) {
                return work;
            }

            // Park this worker
            self.worker_monitor.parked.fetch_add(1, Ordering::Relaxed);
            let all_parked = sync.inc_parked_workers();
            // gc_log!("park #{}", worker.ordinal);

            if all_parked {
                // gc_log!("all parked");
                if self.worker_group.has_designated_work() {
                    self.worker_monitor.all_workers_parked.notify_all();
                } else {
                    if self.schedule_sentinels() {
                        self.worker_monitor.parked.fetch_sub(1, Ordering::Relaxed);
                        sync.dec_parked_workers();
                        break;
                    }
                    if self.update_buckets() {
                        self.worker_monitor.parked.fetch_sub(1, Ordering::Relaxed);
                        sync.dec_parked_workers();
                        break;
                    }
                    // gc_log!("no packets");
                    debug_assert!(!self.worker_group.has_designated_work());
                    if self.in_gc_pause.load(Ordering::SeqCst) {
                        worker.sender.send(()).unwrap();
                    }
                }
            }
            sync = self.worker_monitor.all_workers_parked.wait(sync).unwrap();
            // Unpark this worker.
            self.worker_monitor.parked.fetch_sub(1, Ordering::Relaxed);
            sync.dec_parked_workers();
            // gc_log!("unpark #{}", worker.ordinal);
            flush_logs!();
        }
        let work = self.poll_schedulable_work(worker).unwrap();
        if !self.all_activated_buckets_are_empty() {
            // Have more jobs in this buckets. Notify other workers.
            self.worker_monitor.all_workers_parked.notify_all();
        }
        return work;
    }

    pub fn enable_stat(&self) {
        for worker in &self.worker_group.workers_shared {
            let worker_stat = worker.borrow_stat();
            worker_stat.enable();
        }
        let coordinator_worker_stat = self.coordinator_worker_shared.borrow_stat();
        coordinator_worker_stat.enable();
    }

    pub fn statistics(&self) -> HashMap<String, String> {
        let mut summary = SchedulerStat::default();
        for worker in &self.worker_group.workers_shared {
            let worker_stat = worker.borrow_stat();
            summary.merge(&worker_stat);
        }
        let coordinator_worker_stat = self.coordinator_worker_shared.borrow_stat();
        summary.merge(&coordinator_worker_stat);
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
        stat
    }

    pub fn notify_mutators_paused(&self, mmtk: &'static MMTK<VM>) {
        mmtk.plan.base().gc_requester.clear_request();
        let first_stw_bucket = &self.work_buckets[WorkBucketStage::first_stw_stage()];
        debug_assert!(!first_stw_bucket.is_activated());
        // Note: This is the only place where a non-coordinator thread opens a bucket.
        // If the `StopMutators` is executed by the coordinator thread, it will open
        // the `Prepare` bucket and let workers start executing packets while the coordinator
        // can still add more work packets to `Prepare`.  However, since `Prepare` is the first STW
        // bucket and only the coordinator can open any subsequent buckets, workers cannot execute
        // work packets out of order.  This is not generally true if we are not opening the first
        // STW bucket.  In the future, we should redesign the opening condition of work buckets to
        // make the synchronization more robust,
        first_stw_bucket.activate();
        if first_stw_bucket.is_empty()
            && self.worker_monitor.parked.load(Ordering::Relaxed) + 1
                == self.worker_group.worker_count()
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
}
