use super::stat::SchedulerStat;
use super::work_bucket::*;
use super::worker::{GCWorker, GCWorkerShared, WorkerGroup};
use super::*;
use crate::mmtk::MMTK;
use crate::util::cm::ImmixConcurrentTraceObjects;
use crate::util::opaque_pointer::*;
use crate::vm::Collection;
use crate::vm::{GCThreadContext, VMBinding};
use crossbeam_deque::{Injector, Steal};
use enum_map::{enum_map, EnumMap};
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Condvar, Mutex};
use std::time::SystemTime;

pub enum CoordinatorMessage<VM: VMBinding> {
    Work(Box<dyn CoordinatorWork<VM>>),
    AllWorkerParked,
    BucketDrained,
    Finish,
}

pub struct GCWorkScheduler<VM: VMBinding> {
    /// Work buckets
    pub work_buckets: EnumMap<WorkBucketStage, WorkBucket<VM>>,
    /// workers
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
}

// The 'channel' inside Scheduler disallows Sync for Scheduler. We have to make sure we use channel properly:
// 1. We should never directly use Sender. We clone the sender and let each worker have their own copy.
// 2. Only the coordinator can use Receiver.
// TODO: We should remove channel from Scheduler, and directly send Sender/Receiver when creating the coordinator and
// the workers.
unsafe impl<VM: VMBinding> Sync for GCWorkScheduler<VM> {}

impl<VM: VMBinding> GCWorkScheduler<VM> {
    pub fn new(num_workers: usize) -> Arc<Self> {
        let worker_monitor: Arc<(Mutex<()>, Condvar)> = Default::default();

        // Create work buckets for workers.
        let mut work_buckets = enum_map! {
            WorkBucketStage::Unconstrained => WorkBucket::new(true, worker_monitor.clone(), true),
            WorkBucketStage::FinishConcurrentWork => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::Initial => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::Prepare => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::Closure => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::RefClosure => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::CalculateForwarding => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::SecondRoots => WorkBucket::new(false, worker_monitor.clone(),false),
            WorkBucketStage::RefForwarding => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::Compact => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::Release => WorkBucket::new(false, worker_monitor.clone(), false),
            WorkBucketStage::Final => WorkBucket::new(false, worker_monitor.clone(), false),
        };

        // Set the open condition of each bucket.
        {
            // Unconstrained is always open. Prepare will be opened at the beginning of a GC.
            // This vec will grow for each stage we call with open_next()
            let first_stw_stage = work_buckets
                .iter()
                .skip(1)
                .next()
                .map(|(id, _)| id)
                .unwrap();
            let mut open_stages: Vec<WorkBucketStage> = vec![first_stw_stage];
            // The rest will open after the previous stage is done.
            let stages = work_buckets
                .iter()
                .map(|(stage, _)| stage)
                .collect::<Vec<_>>();
            let mut open_next = |s: WorkBucketStage| {
                let cur_stages = open_stages.clone();
                work_buckets[s].set_open_condition(move |scheduler: &GCWorkScheduler<VM>| {
                    let should_open = scheduler.are_buckets_drained(&cur_stages);
                    // && self.worker_group.all_parked();
                    // Additional check before the `RefClosure` bucket opens.
                    // if should_open && s == WorkBucketStage::RefClosure {
                    //     if let Some(closure_end) = scheduler.closure_end.lock().unwrap().as_ref() {
                    //         if closure_end() {
                    //             // Don't open `RefClosure` if `closure_end` added more works to `Closure`.
                    //             return false;
                    //         }
                    //     }
                    // }
                    should_open
                });
                open_stages.push(s);
            };

            for stages in stages {
                if stages != WorkBucketStage::Unconstrained && stages != first_stw_stage {
                    open_next(stages);
                }
            }
        }

        let coordinator_worker_shared = Arc::new(GCWorkerShared::<VM>::new(worker_monitor.clone()));

        let worker_group = WorkerGroup::new(num_workers, worker_monitor.clone());
        work_buckets.values_mut().for_each(|bucket| {
            bucket.set_group(worker_group.clone());
        });

        Arc::new(Self {
            work_buckets,
            worker_group,
            coordinator_worker_shared,
            worker_monitor,
            closure_end: Mutex::new(None),
            postponed_concurrent_work: spin::RwLock::new(Injector::new()),
            postponed_concurrent_work_prioritized: spin::RwLock::new(Injector::new()),
            in_gc_pause: AtomicBool::new(false),
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
                    if w.type_id() != TypeId::of::<ImmixConcurrentTraceObjects<VM>>() {
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
            0,
            self.clone(),
            true,
            sender.clone(),
            self.coordinator_worker_shared.clone(),
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

        // VM-specific weak ref processing
        self.work_buckets[WorkBucketStage::RefClosure]
            .add(ProcessWeakRefs::<C::ProcessEdgesWorkType>::new());

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

        // Finalization
        if !*plan.base().options.no_finalizer {
            use crate::util::finalizable_processor::{Finalization, ForwardFinalization};
            // finalization
            self.work_buckets[WorkBucketStage::RefClosure]
                .add(Finalization::<C::ProcessEdgesWorkType>::new());
            // forward refs
            if plan.constraints().needs_forward_after_liveness {
                self.work_buckets[WorkBucketStage::RefForwarding]
                    .add(ForwardFinalization::<C::ProcessEdgesWorkType>::new());
            }
        }
    }

    fn are_buckets_drained(&self, buckets: &[WorkBucketStage]) -> bool {
        buckets.iter().all(|&b| self.work_buckets[b].is_drained())
    }

    pub fn on_closure_end(&self, f: Box<dyn Send + Fn() -> bool>) {
        *self.closure_end.lock().unwrap() = Some(f);
    }

    pub fn all_buckets_empty(&self) -> bool {
        self.work_buckets.values().all(|bucket| bucket.is_empty())
    }

    /// Open buckets if their conditions are met
    fn update_buckets(&self) -> bool {
        let mut buckets_updated = false;
        let mut new_packets = false;
        for (id, bucket) in self.work_buckets.iter() {
            if id == WorkBucketStage::Unconstrained {
                continue;
            }
            let x = bucket.update(self);
            if (crate::args::LOG_STAGES || cfg!(feature = "pause_time")) && x {
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
            if cfg!(feature = "yield_and_roots_timer") && x && id == WorkBucketStage::Prepare {
                let t = crate::GC_START_TIME
                    .load(Ordering::SeqCst)
                    .elapsed()
                    .unwrap()
                    .as_nanos();
                crate::COUNTERS.roots_nanos.fetch_add(t, Ordering::SeqCst);
            }
            buckets_updated |= x;
            if x {
                new_packets |= !bucket.is_drained();
            }
        }
        buckets_updated && new_packets
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

    pub fn deactivate_all(&self) {
        self.work_buckets.iter().for_each(|(id, bkt)| {
            if id != WorkBucketStage::Unconstrained {
                bkt.deactivate();
            }
        });
    }

    pub fn reset_state(&self) {
        let first_stw_stage = self
            .work_buckets
            .iter()
            .skip(1)
            .next()
            .map(|(id, _)| id)
            .unwrap();
        self.work_buckets.iter().for_each(|(id, bkt)| {
            if id != WorkBucketStage::Unconstrained && id != first_stw_stage {
                bkt.deactivate();
            }
        });
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
        worker
            .sender
            .send(CoordinatorMessage::Work(Box::new(work)))
            .unwrap();
    }

    #[inline(always)]
    pub fn in_concurrent(&self) -> bool {
        !self.in_gc_pause.load(Ordering::SeqCst)
    }

    #[inline(always)]
    fn all_activated_buckets_are_empty(&self) -> bool {
        for bucket in self.work_buckets.values() {
            if bucket.is_activated() && !bucket.is_drained() {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    fn pop_schedulable_work_once(&self, worker: &GCWorker<VM>) -> Steal<Box<dyn GCWork<VM>>> {
        let mut retry = false;
        match worker.shared.local_work_bucket.poll_no_batch() {
            Steal::Success(w) => return Steal::Success(w),
            Steal::Retry => retry = true,
            _ => {}
        }
        if self.in_concurrent() && !worker.is_concurrent_worker() {
            return Steal::Empty;
        }
        for work_bucket in self.work_buckets.values() {
            match work_bucket.poll(&worker.shared.local_work_buffer) {
                Steal::Success(w) => return Steal::Success(w),
                Steal::Retry => retry = true,
                _ => {}
            }
        }
        for (id, stealer) in &self.worker_group.stealers {
            if *id == worker.ordinal {
                continue;
            }
            match stealer.steal() {
                Steal::Success(w) => return Steal::Success(w),
                Steal::Retry => retry = true,
                _ => {}
            }
        }
        if retry {
            Steal::Retry
        } else {
            Steal::Empty
        }
    }

    #[inline]
    fn pop_schedulable_work(&self, worker: &GCWorker<VM>) -> Option<Box<dyn GCWork<VM>>> {
        loop {
            std::hint::spin_loop();
            match self.pop_schedulable_work_once(worker) {
                Steal::Success(w) => {
                    return Some(w);
                }
                Steal::Retry => {
                    // std::thread::yield_now();
                    continue;
                }
                Steal::Empty => {
                    return None;
                }
            }
        }
    }

    /// Get a schedulable work. Called by workers
    #[inline]
    pub fn poll(&self, worker: &GCWorker<VM>) -> Box<dyn GCWork<VM>> {
        self.pop_schedulable_work(worker)
            .unwrap_or_else(|| self.poll_slow(worker))
    }

    #[cold]
    fn poll_slow(&self, worker: &GCWorker<VM>) -> Box<dyn GCWork<VM>> {
        debug_assert!(!worker.shared.is_parked());
        let mut guard = self.worker_monitor.0.lock().unwrap();
        loop {
            debug_assert!(!worker.shared.is_parked());
            if let Some(work) = self.pop_schedulable_work(worker) {
                return work;
            }
            // Park this worker
            let all_parked = self.worker_group.inc_parked_workers();
            if all_parked {
                if self.update_buckets() {
                    self.worker_group.dec_parked_workers();
                    // We guarantee that we can at least fetch one packet.
                    let work = self.pop_schedulable_work(worker).unwrap();
                    // Optimize for the case that a newly opened bucket only has one packet.
                    if !self.all_activated_buckets_are_empty() {
                        // Have more jobs in this buckets. Notify other workers.
                        self.worker_monitor.1.notify_all();
                    }
                    return work;
                }
                worker.sender.send(CoordinatorMessage::Finish).unwrap();
            }
            // Wait
            // println!("[{}] sleep", worker.ordinal);
            guard = self.worker_monitor.1.wait(guard).unwrap();
            // println!("[{}] wake", worker.ordinal);
            // Unpark this worker
            self.worker_group.dec_parked_workers();
            worker.shared.parked.store(false, Ordering::SeqCst);
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
        let first_stw_bucket = self.work_buckets.values().skip(1).next().unwrap();
        // debug_assert!(!first_stw_bucket.is_activated());
        first_stw_bucket.activate();
        let _guard = self.worker_monitor.0.lock().unwrap();
        self.worker_monitor.1.notify_all();
    }
}

pub static mut LAST_ACTIVATE_TIME: Option<SystemTime> = None;
