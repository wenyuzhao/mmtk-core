use super::stat::SchedulerStat;
use super::work_bucket::*;
use super::worker::{GCWorker, GCWorkerShared, ParkingGuard, ThreadId, WorkerGroup};
use super::*;
use crate::mmtk::MMTK;
use crate::util::opaque_pointer::*;
use crate::util::options::AffinityKind;
use crate::util::reference_processor::PhantomRefProcessing;
use crate::util::rust_util::array_from_fn;
use crate::vm::Collection;
use crate::vm::{GCThreadContext, VMBinding};
use crossbeam::deque::{self, Injector, Steal};
use enum_map::{Enum, EnumMap};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};

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
    /// Workers that can run both concurrently and STW
    pub(crate) generic_worker_group: Arc<WorkerGroup<VM>>,
    /// STW-only Workers
    pub(crate) stw_worker_group: Arc<WorkerGroup<VM>>,
    /// The shared part of the GC worker object of the controller thread
    coordinator_worker_shared: Arc<GCWorkerShared<VM>>,
    pub(super) postponed_concurrent_work: spin::RwLock<Injector<Box<dyn GCWork<VM>>>>,
    pub(super) postponed_concurrent_work_prioritized: spin::RwLock<Injector<Box<dyn GCWork<VM>>>>,
    in_gc_pause: AtomicBool,
    /// Counter for pending coordinator messages.
    pub(super) pending_coordinator_packets: AtomicUsize,
    /// How to assign the affinity of each GC thread. Specified by the user.
    affinity: AffinityKind,
    bucket_update_progress: AtomicUsize,
    parked_workers: Arc<AtomicUsize>,
}

// The 'channel' inside Scheduler disallows Sync for Scheduler. We have to make sure we use channel properly:
// 1. We should never directly use Sender. We clone the sender and let each worker have their own copy.
// 2. Only the coordinator can use Receiver.
// TODO: We should remove channel from Scheduler, and directly send Sender/Receiver when creating the coordinator and
// the workers.
unsafe impl<VM: VMBinding> Sync for GCWorkScheduler<VM> {}

impl<VM: VMBinding> GCWorkScheduler<VM> {
    pub fn new(num_workers: usize, num_conc_workers: usize, affinity: AffinityKind) -> Arc<Self> {
        assert!(num_conc_workers > 0 && num_conc_workers <= num_workers);
        assert!(num_workers > 0);
        let parked_workers = Arc::new(AtomicUsize::new(0));
        let lock = Arc::new(Mutex::new(()));
        let generic_worker_group = WorkerGroup::new(
            parked_workers.clone(),
            false,
            0,
            num_conc_workers,
            num_workers,
            lock.clone(),
        );
        let stw_worker_group = WorkerGroup::new(
            parked_workers.clone(),
            true,
            num_conc_workers,
            num_workers - num_conc_workers,
            num_workers,
            lock.clone(),
        );

        // Create work buckets for workers.
        // TODO: Replace `array_from_fn` with `std::array::from_fn` after bumping MSRV.
        let mut work_buckets = EnumMap::from_array(array_from_fn(|stage_num| {
            let stage = WorkBucketStage::from_usize(stage_num);
            let active = stage == WorkBucketStage::Unconstrained;
            WorkBucket::new(
                active,
                generic_worker_group.clone(),
                stw_worker_group.clone(),
            )
        }));

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

        let coordinator_worker_shared = Arc::new(GCWorkerShared::<VM>::new(None, false));

        Arc::new(Self {
            work_buckets,
            stw_worker_group,
            generic_worker_group,
            coordinator_worker_shared,
            postponed_concurrent_work: spin::RwLock::new(Injector::new()),
            postponed_concurrent_work_prioritized: spin::RwLock::new(Injector::new()),
            in_gc_pause: AtomicBool::new(false),
            pending_coordinator_packets: AtomicUsize::new(0),
            affinity,
            bucket_update_progress: AtomicUsize::new(0),
            parked_workers,
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
        self.generic_worker_group.as_ref().worker_count()
            + self.stw_worker_group.as_ref().worker_count()
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
            mmtk.get_plan().base().gc_requester.clone(),
            self.clone(),
            receiver,
            coordinator_worker,
        );
        VM::VMCollection::spawn_gc_thread(tls, GCThreadContext::<VM>::Controller(gc_controller));

        self.generic_worker_group.spawn(mmtk, sender.clone(), tls);
        self.stw_worker_group.spawn(mmtk, sender, tls);
    }

    /// Resolve the affinity of a thread.
    pub fn resolve_affinity(&self, thread: ThreadId) {
        self.affinity.resolve_affinity(thread);
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
    fn schedule_sentinels(&self) -> bool {
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
        let _guard = self.stw_worker_group.monitor.0.lock().unwrap();
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

    pub(super) fn set_in_gc_pause(&self, in_gc_pause: bool) {
        self.in_gc_pause.store(in_gc_pause, Ordering::SeqCst);
        for wb in self.work_buckets.values() {
            wb.set_in_concurrent(!in_gc_pause);
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
        for (id, worker_shared) in self.generic_worker_group.workers_shared.iter().enumerate() {
            if id == worker.ordinal {
                continue;
            }
            match worker_shared.stealer.as_ref().unwrap().steal() {
                Steal::Success(w) => return Steal::Success(w),
                Steal::Retry => should_retry = true,
                _ => {}
            }
        }
        if !self.in_concurrent() {
            for (id, worker_shared) in self.stw_worker_group.workers_shared.iter().enumerate() {
                if id == worker.ordinal {
                    continue;
                }
                match worker_shared.stealer.as_ref().unwrap().steal() {
                    Steal::Success(w) => return Steal::Success(w),
                    Steal::Retry => should_retry = true,
                    _ => {}
                }
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
        if self.in_concurrent() && worker.is_stw() {
            return None;
        }
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

    fn poll_slow(&self, worker: &GCWorker<VM>) -> Box<dyn GCWork<VM>> {
        // Note: The lock is released during `wait` in the loop.
        let stw_worker = worker.is_stw();
        let group = if !stw_worker {
            &self.generic_worker_group
        } else {
            &self.stw_worker_group
        };
        let mut guard = group.monitor.0.lock().unwrap();
        'polling_loop: loop {
            flush_logs!();
            // Retry polling
            if let Some(work) = self.poll_schedulable_work(worker) {
                return work;
            }
            // Prepare to park this worker
            let parking_guard = ParkingGuard::new(group);
            // If all workers are parked, try activate new buckets
            if parking_guard.all_parked() {
                // If there're any designated work, resume the workers and process them
                if group.has_designated_work() {
                    assert!(
                        worker.shared.designated_work.is_empty(),
                        "The last parked worker has designated work."
                    );
                    group.monitor.1.notify_all();
                    // The current worker is going to wait, because the designated work is not for it.
                } else if self.pending_coordinator_packets.load(Ordering::SeqCst) == 0 {
                    // See if any bucket has a sentinel.
                    if self.schedule_sentinels() {
                        // We're not going to sleep since new work packets are just scheduled.
                        break 'polling_loop;
                    }
                    // Try to open new buckets.
                    if self.update_buckets() {
                        // We're not going to sleep since a new bucket is just open.
                        break 'polling_loop;
                    }
                    debug_assert!(!group.has_designated_work());
                    // The current pause is finished if we can't open more buckets.
                    worker.sender.send(CoordinatorMessage::Finish).unwrap();
                }
                // Otherwise, if there is still pending coordinator work, the last parked
                // worker will wait on the monitor, too.  The coordinator will notify a
                // worker (maybe not the current one) once it finishes executing all
                // coordinator work packets.
            }
            // Wait
            if cfg!(feature = "report_worker_sleep_events")
                && self.in_gc_pause.load(Ordering::Relaxed)
                && self.work_buckets[WorkBucketStage::FinishConcurrentWork].is_activated()
            {
                gc_log!([3]
                    "    - ({:.3}ms) worker#{} sleep",
                    crate::gc_start_time_ms(),
                    worker.ordinal,
                );
            }
            flush_logs!();
            loop {
                guard = group.monitor.1.wait(guard).unwrap();
                if !(self.in_concurrent() && stw_worker) {
                    break;
                }
            }
            // gc_log!(
            //     "    - ({:.3}ms) worker#{} wakeup stw={}",
            //     crate::gc_start_time_ms(),
            //     worker.ordinal,
            //     worker.is_stw(),
            // );
            flush_logs!();
            // The worker is unparked here where `parking_guard` goes out of scope.
        }
        flush_logs!();

        // We guarantee that we can at least fetch one packet when we reach here.
        let work = self.poll_schedulable_work(worker).unwrap();
        // Optimize for the case that a newly opened bucket only has one packet.
        // We only notify_all if there're more than one packets available.
        if !self.all_activated_buckets_are_empty() {
            // Have more jobs in this buckets. Notify other workers.
            self.generic_worker_group.monitor.1.notify_all();
            self.stw_worker_group.monitor.1.notify_all();
        }
        // Return this packet and execute it.
        work
    }

    pub fn enable_stat(&self) {
        for worker in &self.generic_worker_group.workers_shared {
            let worker_stat = worker.borrow_stat();
            worker_stat.enable();
        }
        for worker in &self.stw_worker_group.workers_shared {
            let worker_stat = worker.borrow_stat();
            worker_stat.enable();
        }
        let coordinator_worker_stat = self.coordinator_worker_shared.borrow_stat();
        coordinator_worker_stat.enable();
    }

    pub fn statistics(&self) -> HashMap<String, String> {
        let mut summary = SchedulerStat::default();
        for worker in &self.generic_worker_group.workers_shared {
            let worker_stat = worker.borrow_stat();
            summary.merge(&worker_stat);
        }
        for worker in &self.stw_worker_group.workers_shared {
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
        #[cfg(feature = "measure_trace_rate")]
        crate::plan::lxr::STW_CM_COUNTERS_TOTAL.report(&mut stat);
        stat
    }

    pub fn notify_mutators_paused(&self, mmtk: &'static MMTK<VM>) {
        mmtk.get_plan().base().gc_requester.clear_request();
        let first_stw_bucket = &self.work_buckets[WorkBucketStage::first_stw_stage()];
        first_stw_bucket.activate();
        gc_log!([3]
            " - ({:.3}ms) Start GC Stage: {:?}",
            crate::gc_start_time_ms(),
            WorkBucketStage::from_usize(1)
        );
        if first_stw_bucket.is_empty()
            && self.parked_workers() + 1 == self.num_workers()
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
        let _guard = self.generic_worker_group.monitor.0.lock().unwrap();
        self.generic_worker_group.monitor.1.notify_all();
        self.stw_worker_group.monitor.1.notify_all();
    }

    pub fn all_parked(&self) -> bool {
        self.num_workers() == self.parked_workers()
    }

    fn parked_workers(&self) -> usize {
        self.parked_workers.load(Ordering::SeqCst)
    }

    pub fn current_epoch_finished(&self) -> bool {
        let _guard = self.stw_worker_group.monitor.0.lock().unwrap();
        self.all_parked() && self.all_buckets_empty()
    }

    pub fn has_designated_work(&self) -> bool {
        self.generic_worker_group.has_designated_work()
            || self.stw_worker_group.has_designated_work()
    }

    pub(super) fn push_designated_work(&self, create: impl Fn() -> Box<dyn GCWork<VM>>) {
        for w in &self.stw_worker_group.workers_shared {
            let result = w.designated_work.push(create());
            debug_assert!(result.is_ok());
        }
        for w in &self.generic_worker_group.workers_shared {
            let result = w.designated_work.push(create());
            debug_assert!(result.is_ok());
        }
    }

    pub fn wakeup_one_stw_worker(&self) {
        let _guard = self.stw_worker_group.monitor.0.lock().unwrap();
        self.stw_worker_group.monitor.1.notify_one();
    }

    pub fn wakeup_all_conc_workers(&self) {
        let _guard: std::sync::MutexGuard<'_, ()> =
            self.generic_worker_group.monitor.0.lock().unwrap();
        self.generic_worker_group.monitor.1.notify_all();
    }
}
