use super::stat::SchedulerStat;
use super::work_bucket::*;
use super::worker::{GCWorker, GCWorkerShared, ThreadId, WorkerGroup};
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
use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::time::SystemTime;

pub enum CoordinatorMessage<VM: VMBinding> {
    /// Send a work-packet to the coordinator thread/
    Work(Box<dyn CoordinatorWork<VM>>),
    /// Notify the coordinator thread that all GC tasks are finished.
    /// When sending this message, all the work buckets should be
    /// empty, and all the workers should be parked.
    Finish,
}

pub struct GCWorkScheduler<VM: VMBinding> {
    /// Work buckets
    pub work_buckets: EnumMap<WorkBucketStage, WorkBucket<VM>>,
    /// Workers
    pub worker_group: Arc<WorkerGroup<VM>>,
    /// The shared part of the GC worker object of the controller thread
    coordinator_worker_shared: Arc<GCWorkerShared<VM>>,
    /// Condition Variable for worker synchronization
    pub worker_monitor: Arc<(Mutex<()>, Condvar)>,
    /// A callback to be fired after the `Closure` bucket is drained.
    /// This callback should return `true` if it adds more work packets to the
    /// `Closure` bucket. `WorkBucket::can_open` then consult this return value
    /// to prevent the GC from proceeding to the next stage, if we still have
    /// `Closure` work to do.
    ///
    /// We use this callback to process ephemeron objects. `closure_end` can re-enable
    /// the `Closure` bucket multiple times to iteratively discover and process
    /// more ephemeron objects.
    closure_end: Mutex<Option<Box<dyn Send + Fn() -> bool>>>,
    pub(super) postponed_concurrent_work: spin::RwLock<Injector<Box<dyn GCWork<VM>>>>,
    pub(super) postponed_concurrent_work_prioritized: spin::RwLock<Injector<Box<dyn GCWork<VM>>>>,
    pub(super) in_gc_pause: AtomicBool,
    /// Counter for pending coordinator messages.
    pub(super) pending_coordinator_packets: AtomicUsize,
    /// How to assign the affinity of each GC thread. Specified by the user.
    affinity: AffinityKind,
    bucket_update_progress: AtomicUsize,
}

// The 'channel' inside Scheduler disallows Sync for Scheduler. We have to make sure we use channel properly:
// 1. We should never directly use Sender. We clone the sender and let each worker have their own copy.
// 2. Only the coordinator can use Receiver.
// TODO: We should remove channel from Scheduler, and directly send Sender/Receiver when creating the coordinator and
// the workers.
unsafe impl<VM: VMBinding> Sync for GCWorkScheduler<VM> {}

impl<VM: VMBinding> GCWorkScheduler<VM> {
    pub fn new(num_workers: usize, affinity: AffinityKind) -> Arc<Self> {
        let worker_monitor: Arc<(Mutex<()>, Condvar)> = Default::default();
        let worker_group = WorkerGroup::new(num_workers);

        // Create work buckets for workers.
        let mut work_buckets = enum_map! {
            WorkBucketStage::Unconstrained => WorkBucket::new(true, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::FinishConcurrentWork => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::Initial => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::Prepare => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::Closure => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::SoftRefClosure => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::WeakRefClosure => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::FinalRefClosure => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::PhantomRefClosure => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::CalculateForwarding => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::SecondRoots => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::RefForwarding => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::FinalizableForwarding => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::Compact => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::Release => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
            WorkBucketStage::Final => WorkBucket::new(false, worker_monitor.clone(), worker_group.clone()),
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
                            let should_open = scheduler.are_buckets_drained(&cur_stages);
                            // Additional check before the `RefClosure` bucket opens.
                            if should_open && stage == LAST_CLOSURE_BUCKET {
                                if let Some(closure_end) =
                                    scheduler.closure_end.lock().unwrap().as_ref()
                                {
                                    if closure_end() {
                                        // Don't open `LAST_CLOSURE_BUCKET` if `closure_end` added more works to `Closure`.
                                        return false;
                                    }
                                }
                            }
                            should_open
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
            closure_end: Mutex::new(None),
            postponed_concurrent_work: spin::RwLock::new(Injector::new()),
            postponed_concurrent_work_prioritized: spin::RwLock::new(Injector::new()),
            in_gc_pause: AtomicBool::new(false),
            pending_coordinator_packets: AtomicUsize::new(0),
            affinity,
            bucket_update_progress: AtomicUsize::new(0),
        })
    }

    #[inline]
    pub fn pause_concurrent_work_packets_during_gc(&self) {
        let mut old_queue = Injector::new();
        old_queue = self.work_buckets[WorkBucketStage::Unconstrained].swap_queue(old_queue);
        let mut queue = self.postponed_concurrent_work.write();
        assert!(queue.is_empty());
        *queue = old_queue;
        crate::PAUSE_CONCURRENT_MARKING.store(true, Ordering::SeqCst);
    }

    #[inline]
    pub fn process_lazy_decrement_packets(&self) {
        crate::DISABLE_LASY_DEC_FOR_CURRENT_GC.store(false, Ordering::SeqCst);
        let mut no_postpone = vec![];
        let mut cm_packets = vec![];
        // Buggy
        let postponed_concurrent_work = self.postponed_concurrent_work.read();
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
        self.work_buckets[WorkBucketStage::RCProcessDecs].bulk_add(no_postpone);
    }

    #[inline]
    pub fn postpone(&self, w: impl GCWork<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work.read().push(Box::new(w))
    }

    #[inline]
    pub fn postpone_prioritized(&self, w: impl GCWork<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work_prioritized
            .read()
            .push(Box::new(w))
    }

    #[inline]
    pub fn postpone_dyn(&self, w: Box<dyn GCWork<VM>>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work.read().push(w)
    }

    #[inline]
    pub fn postpone_dyn_prioritized(&self, w: Box<dyn GCWork<VM>>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work_prioritized.read().push(w)
    }

    #[inline]
    pub fn postpone_all(&self, ws: Vec<Box<dyn GCWork<VM>>>) {
        let postponed_concurrent_work = self.postponed_concurrent_work.read();
        ws.into_iter()
            .for_each(|w| postponed_concurrent_work.push(w));
    }

    #[inline]
    pub fn postpone_all_prioritized(&self, ws: Vec<Box<dyn GCWork<VM>>>) {
        let postponed_concurrent_work = self.postponed_concurrent_work_prioritized.read();
        ws.into_iter()
            .for_each(|w| postponed_concurrent_work.push(w));
    }

    #[inline]
    pub fn num_workers(&self) -> usize {
        self.worker_group.as_ref().worker_count()
    }

    /// Create GC threads, including the controller thread and all workers.
    pub fn spawn_gc_threads(self: &Arc<Self>, mmtk: &'static MMTK<VM>, tls: VMThread) {
        // Create the communication channel.
        let (sender, receiver) = channel::<CoordinatorMessage<VM>>();

        // Spawn the controller thread.
        let coordinator_worker = GCWorker::new(
            mmtk,
            usize::MAX,
            self.clone(),
            true,
            sender.clone(),
            self.coordinator_worker_shared.clone(),
            deque::Worker::new_fifo(),
        );
        let gc_controller = GCController::new(
            mmtk,
            mmtk.plan.base().gc_requester.clone(),
            self.clone(),
            receiver,
            coordinator_worker,
        );
        VM::VMCollection::spawn_gc_thread(tls, GCThreadContext::<VM>::Controller(gc_controller));

        self.worker_group.spawn(mmtk, sender, tls)
    }

    /// Resolve the affinity of a thread.
    pub fn resolve_affinity(&self, thread: ThreadId) {
        self.affinity.resolve_affinity(thread);
    }

    /// Schedule all the common work packets
    pub fn schedule_common_work<C: GCWorkContext<VM = VM> + 'static>(
        &self,
        plan: &'static C::PlanType,
    ) {
        use crate::plan::Plan;
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
    }

    fn are_buckets_drained(&self, buckets: &[WorkBucketStage]) -> bool {
        debug_assert!(
            self.pending_coordinator_packets.load(Ordering::SeqCst) == 0,
            "GCWorker attempted to open buckets when there are pending coordinator work packets"
        );
        buckets
            .iter()
            .all(|&b| self.work_buckets[b].is_drained() || self.work_buckets[b].disabled())
    }

    pub fn on_closure_end(&self, f: Box<dyn Send + Fn() -> bool>) {
        *self.closure_end.lock().unwrap() = Some(f);
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
            self.work_buckets[WorkBucketStage::Unconstrained].notify_all_workers();
        }
    }

    /// Open buckets if their conditions are met.
    ///
    /// This function should only be called after all the workers are parked.
    /// No workers will be waked up by this function. The caller is responsible for that.
    ///
    /// Return true if there're any non-empty buckets updated.
    fn update_buckets(&self) -> bool {
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
            if (crate::args::LOG_STAGES || cfg!(feature = "pause_time")) && bucket_opened {
                unsafe {
                    let since_prev_stage = LAST_ACTIVATE_TIME
                        .unwrap_or_else(|| crate::GC_START_TIME.load(Ordering::SeqCst))
                        .elapsed()
                        .unwrap()
                        .as_nanos();
                    crate::add_bucket_time(id, since_prev_stage);
                    if crate::args::LOG_STAGES {
                        println!(" - [{:.6}ms] Activate {:?} (since prev stage: {} ns,    since gc trigger = {} ns,    since gc = {} ns)",
                            crate::gc_trigger_time() as f64 / 1000000f64,
                            id, since_prev_stage,
                            crate::GC_TRIGGER_TIME.load(Ordering::SeqCst).elapsed().unwrap().as_nanos(),
                            crate::GC_START_TIME.load(Ordering::SeqCst).elapsed().unwrap().as_nanos(),
                        );
                    }
                    LAST_ACTIVATE_TIME = Some(SystemTime::now());
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
                crate::COUNTERS.roots_nanos.fetch_add(t, Ordering::SeqCst);
            }
            buckets_updated = buckets_updated || bucket_opened;
            if bucket_opened {
                new_packets = new_packets || !bucket.is_drained();
                // Quit the loop. There'are already new packets in the newly opened buckets.
                if new_packets {
                    new_progress = i;
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

    pub fn add_coordinator_work(&self, work: impl CoordinatorWork<VM>, worker: &GCWorker<VM>) {
        self.pending_coordinator_packets
            .fetch_add(1, Ordering::SeqCst);
        worker
            .sender
            .send(CoordinatorMessage::Work(Box::new(work)))
            .unwrap();
    }

    #[inline(always)]
    pub fn in_concurrent(&self) -> bool {
        !self.in_gc_pause.load(Ordering::SeqCst)
    }

    /// Check if all the work buckets are empty
    #[inline(always)]
    fn all_activated_buckets_are_empty(&self) -> bool {
        for bucket in self.work_buckets.values() {
            if bucket.is_activated() && !bucket.is_drained() {
                return false;
            }
        }
        true
    }

    /// Get a schedulable work packet without retry.
    #[inline(always)]
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
            if work_bucket.disabled() {
                continue;
            }
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
    #[inline]
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
    #[inline]
    pub fn poll(&self, worker: &GCWorker<VM>) -> Box<dyn GCWork<VM>> {
        self.poll_schedulable_work(worker)
            .unwrap_or_else(|| self.poll_slow(worker))
    }

    #[cold]
    fn poll_slow(&self, worker: &GCWorker<VM>) -> Box<dyn GCWork<VM>> {
        // Note: The lock is released during `wait` in the loop.
        let mut guard = self.worker_monitor.0.lock().unwrap();
        loop {
            // Retry polling
            if let Some(work) = self.poll_schedulable_work(worker) {
                return work;
            }
            // Prepare to park this worker
            let all_parked = self.worker_group.inc_parked_workers();
            // If all workers are parked, try activate new buckets
            if all_parked {
                // If there're any designated work, resume the workers and process them
                if self.worker_group.has_designated_work() {
                    assert!(
                        worker.shared.designated_work.is_empty(),
                        "The last parked worker has designated work."
                    );
                    self.worker_monitor.1.notify_all();
                    // The current worker is going to wait, because the designated work is not for it.
                } else if self.pending_coordinator_packets.load(Ordering::SeqCst) == 0 {
                    if self.update_buckets() {
                        // We're not going to sleep since a new bucket is just open.
                        self.worker_group.dec_parked_workers();
                        // We guarantee that we can at least fetch one packet.
                        let work = self.poll_schedulable_work(worker).unwrap();
                        // Optimize for the case that a newly opened bucket only has one packet.
                        // We only notify_all if there're more than one packets available.
                        if !self.all_activated_buckets_are_empty() {
                            // Have more jobs in this buckets. Notify other workers.
                            self.worker_monitor.1.notify_all();
                        }
                        // Return this packet and execute it.
                        return work;
                    }
                    debug_assert!(!self.worker_group.has_designated_work());
                    // The current pause is finished if we can't open more buckets.
                    worker.sender.send(CoordinatorMessage::Finish).unwrap();
                }
                // Otherwise, if there is still pending coordinator work, the last parked
                // worker will wait on the monitor, too.  The coordinator will notify a
                // worker (maybe not the current one) once it finishes executing all
                // coordinator work packets.
            }
            // Wait
            guard = self.worker_monitor.1.wait(guard).unwrap();
            // Unpark this worker
            self.worker_group.dec_parked_workers();
        }
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
        first_stw_bucket.activate();
        if first_stw_bucket.is_empty() {
            let second_stw_bucket = &self.work_buckets[WorkBucketStage::from_usize(2)];
            second_stw_bucket.activate();
            unsafe {
                if crate::args::LOG_STAGES {
                    let since_prev_stage = LAST_ACTIVATE_TIME
                        .unwrap_or_else(|| crate::GC_START_TIME.load(Ordering::SeqCst))
                        .elapsed()
                        .unwrap()
                        .as_nanos();
                    println!(" - [{:.6}ms] Activate {:?} (since prev stage: {} ns,    since gc trigger = {} ns,    since gc = {} ns)",
                    crate::gc_trigger_time() as f64 / 1000000f64,
                    WorkBucketStage::from_usize(2), since_prev_stage,
                    crate::GC_TRIGGER_TIME.load(Ordering::SeqCst).elapsed().unwrap().as_nanos(),
                    crate::GC_START_TIME.load(Ordering::SeqCst).elapsed().unwrap().as_nanos(),
                );
                }
            }
        }
        let _guard = self.worker_monitor.0.lock().unwrap();
        // println!("Notify ALL 3");
        self.worker_monitor.1.notify_all();
    }
}

pub static mut LAST_ACTIVATE_TIME: Option<SystemTime> = None;
