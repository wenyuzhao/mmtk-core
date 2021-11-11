use super::stat::SchedulerStat;
use super::work_bucket::*;
use super::worker::{GCWorker, WorkerGroup};
use super::*;
use crate::mmtk::MMTK;
use crate::util::cm::ImmixConcurrentTraceObjects;
use crate::util::opaque_pointer::*;
use crate::vm::VMBinding;
use crossbeam_deque::{Injector, Steal};
use enum_map::{enum_map, EnumMap};
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::SystemTime;

pub enum CoordinatorMessage<VM: VMBinding> {
    Work(Box<dyn CoordinatorWork<VM>>),
    AllWorkerParked,
    BucketDrained,
    Finish,
}

pub struct GCWorkScheduler<VM: VMBinding> {
    pub work_buckets: EnumMap<WorkBucketStage, WorkBucket<VM>>,
    /// workers
    worker_group: Option<Arc<WorkerGroup<VM>>>,
    /// Condition Variable for worker synchronization
    pub worker_monitor: Arc<(Mutex<()>, Condvar)>,
    mmtk: Option<&'static MMTK<VM>>,
    coordinator_worker: Option<RwLock<GCWorker<VM>>>,
    /// A message channel to send new coordinator work and other actions to the coordinator thread
    channel: (
        Sender<CoordinatorMessage<VM>>,
        Receiver<CoordinatorMessage<VM>>,
    ),
    startup: Mutex<Option<Box<dyn CoordinatorWork<VM>>>>,
    finalizer: Mutex<Option<Box<dyn CoordinatorWork<VM>>>>,
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
    postponed_concurrent_work: Injector<Box<dyn GCWork<VM>>>,
    in_gc_pause: AtomicBool,
}

// The 'channel' inside Scheduler disallows Sync for Scheduler. We have to make sure we use channel properly:
// 1. We should never directly use Sender. We clone the sender and let each worker have their own copy.
// 2. Only the coordinator can use Receiver.
// TODO: We should remove channel from Scheduler, and directly send Sender/Receiver when creating the coordinator and
// the workers.
unsafe impl<VM: VMBinding> Sync for GCWorkScheduler<VM> {}

impl<VM: VMBinding> GCWorkScheduler<VM> {
    pub fn new() -> Arc<Self> {
        let worker_monitor: Arc<(Mutex<()>, Condvar)> = Default::default();
        Arc::new(Self {
            work_buckets: enum_map! {
                WorkBucketStage::Unconstrained => WorkBucket::new(true, worker_monitor.clone(), true),
                WorkBucketStage::FinishConcurrentWork => WorkBucket::new(false, worker_monitor.clone(), false),
                WorkBucketStage::Initial => WorkBucket::new(false, worker_monitor.clone(), false),
                WorkBucketStage::Prepare => WorkBucket::new(false, worker_monitor.clone(), false),
                WorkBucketStage::Closure => WorkBucket::new(false, worker_monitor.clone(), false),
                WorkBucketStage::RefClosure => WorkBucket::new(false, worker_monitor.clone(), false),
                WorkBucketStage::RefForwarding => WorkBucket::new(false, worker_monitor.clone(), false),
                WorkBucketStage::Release => WorkBucket::new(false, worker_monitor.clone(), false),
                WorkBucketStage::Final => WorkBucket::new(false, worker_monitor.clone(), false),
            },
            worker_group: None,
            worker_monitor: worker_monitor.clone(),
            mmtk: None,
            coordinator_worker: None,
            channel: channel(),
            startup: Mutex::new(None),
            finalizer: Mutex::new(None),
            closure_end: Mutex::new(None),
            postponed_concurrent_work: Injector::new(),
            in_gc_pause: AtomicBool::new(false),
        })
    }

    #[inline]
    pub fn pause_concurrent_work_packets_during_gc(&self) {
        let mut old_queue = Injector::new();
        old_queue = self.work_buckets[WorkBucketStage::Unconstrained].swap_queue(old_queue);
        let mut postponed = 0usize;
        loop {
            match old_queue.steal() {
                Steal::Success(w) => {
                    debug_assert_eq!(w.type_id(), TypeId::of::<ImmixConcurrentTraceObjects<VM>>());
                    postponed += 1;
                    self.postponed_concurrent_work.push(w);
                }
                Steal::Empty => break,
                Steal::Retry => {}
            }
        }
        if crate::args::LOG_PER_GC_STATE {
            println!("Pause {} concurrent packets", postponed);
        }
        crate::PAUSE_CONCURRENT_MARKING.store(true, Ordering::SeqCst);
    }

    #[inline]
    pub fn process_lazy_decrement_packets(&self) {
        crate::DISABLE_LASY_DEC_FOR_CURRENT_GC.store(false, Ordering::SeqCst);
        let mut no_postpone = vec![];
        let mut cm_packets = vec![];
        loop {
            if self.postponed_concurrent_work.is_empty() {
                break;
            }
            match self.postponed_concurrent_work.steal() {
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
            self.postponed_concurrent_work.push(w)
        }
        self.work_buckets[WorkBucketStage::RCProcessDecs].bulk_add(no_postpone);
    }

    #[inline]
    pub fn postpone(&self, w: impl GCWork<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work.push(box w)
    }

    #[inline]
    pub fn postpone_dyn(&self, w: Box<dyn GCWork<VM>>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        self.postponed_concurrent_work.push(w)
    }

    #[inline]
    pub fn postpone_all(&self, ws: Vec<Box<dyn GCWork<VM>>>) {
        ws.into_iter()
            .for_each(|w| self.postponed_concurrent_work.push(w));
    }

    #[inline]
    pub fn num_workers(&self) -> usize {
        self.worker_group.as_ref().unwrap().worker_count()
    }

    pub fn initialize(
        self: &'static Arc<Self>,
        num_workers: usize,
        mmtk: &'static MMTK<VM>,
        tls: VMThread,
    ) {
        let num_workers = if cfg!(feature = "single_worker") {
            1
        } else {
            num_workers
        };

        let mut self_mut = self.clone();
        let self_mut = unsafe { Arc::get_mut_unchecked(&mut self_mut) };

        self_mut.mmtk = Some(mmtk);
        self_mut.coordinator_worker = Some(RwLock::new(GCWorker::new(
            233,
            Arc::downgrade(self),
            true,
            self.channel.0.clone(),
        )));
        self_mut.worker_group = Some(WorkerGroup::new(
            num_workers,
            Arc::downgrade(self),
            self.channel.0.clone(),
        ));
        let group = self_mut.worker_group.as_ref().unwrap().clone();
        self_mut.work_buckets.values_mut().for_each(|bucket| {
            bucket.set_group(group.clone());
        });
        self.worker_group.as_ref().unwrap().spawn_workers(tls, mmtk);

        {
            // Unconstrained is always open. Prepare will be opened at the beginning of a GC.
            // This vec will grow for each stage we call with open_next()
            let first_stw_stage = self
                .work_buckets
                .iter()
                .skip(1)
                .next()
                .map(|(id, _)| id)
                .unwrap();
            let mut open_stages: Vec<WorkBucketStage> = vec![first_stw_stage];
            // The rest will open after the previous stage is done.
            let mut open_next = |s: WorkBucketStage| {
                let cur_stages = open_stages.clone();
                self_mut.work_buckets[s].set_open_condition(move || {
                    let should_open = self.are_buckets_drained(&cur_stages);
                    // && self.worker_group().all_parked();
                    // Additional check before the `RefClosure` bucket opens.
                    // if should_open && s == WorkBucketStage::RefClosure {
                    //     if let Some(closure_end) = self.closure_end.lock().unwrap().as_ref() {
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

            for (id, _) in self.work_buckets.iter() {
                if id != WorkBucketStage::Unconstrained && id != first_stw_stage {
                    open_next(id);
                }
            }
        }
    }

    fn are_buckets_drained(&self, buckets: &[WorkBucketStage]) -> bool {
        buckets.iter().all(|&b| self.work_buckets[b].is_drained())
    }

    pub fn initialize_worker(self: &Arc<Self>, tls: VMWorkerThread) {
        let mut coordinator_worker = self.coordinator_worker.as_ref().unwrap().write().unwrap();
        coordinator_worker.init(tls);
    }

    pub fn set_initializer<W: CoordinatorWork<VM>>(&self, w: Option<W>) {
        *self.startup.lock().unwrap() = w.map(|w| box w as Box<dyn CoordinatorWork<VM>>);
    }

    pub fn set_finalizer<W: CoordinatorWork<VM>>(&self, w: Option<W>) {
        *self.finalizer.lock().unwrap() = w.map(|w| box w as Box<dyn CoordinatorWork<VM>>);
    }

    pub fn on_closure_end(&self, f: Box<dyn Send + Fn() -> bool>) {
        *self.closure_end.lock().unwrap() = Some(f);
    }

    pub fn worker_group(&self) -> Arc<WorkerGroup<VM>> {
        self.worker_group.as_ref().unwrap().clone()
    }

    fn all_buckets_empty(&self) -> bool {
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
            let x = bucket.update();
            if crate::args::LOG_STAGES && x {
                unsafe {
                    let since_prev_stage = LAST_ACTIVATE_TIME
                        .unwrap_or_else(|| crate::GC_START_TIME.load(Ordering::SeqCst))
                        .elapsed()
                        .unwrap()
                        .as_nanos();
                    println!(" - [{:.6}ms] Activate {:?} (since prev stage: {} ns,    since gc trigger = {} ns,    since gc = {} ns)",
                        crate::gc_trigger_time() as f64 / 1000000f64,
                        id, since_prev_stage,
                        crate::GC_TRIGGER_TIME.load(Ordering::SeqCst).elapsed().unwrap().as_nanos(),
                        crate::GC_START_TIME.load(Ordering::SeqCst).elapsed().unwrap().as_nanos(),
                    );
                    LAST_ACTIVATE_TIME = Some(SystemTime::now())
                }
            }
            if cfg!(feature = "yield_and_roots_timer")
                && x
                && id == WorkBucketStage::Prepare
                && crate::inside_harness()
            {
                let t = crate::GC_START_TIME
                    .load(Ordering::SeqCst)
                    .elapsed()
                    .unwrap()
                    .as_nanos();
                crate::PAUSES.roots_nanos.fetch_add(t, Ordering::SeqCst);
            }
            buckets_updated |= x;
            if x {
                new_packets |= !bucket.is_drained();
            }
        }
        buckets_updated && new_packets
    }

    /// Execute coordinator work, in the controller thread
    fn process_coordinator_work(&self, mut work: Box<dyn CoordinatorWork<VM>>) {
        let mut coordinator_worker = self.coordinator_worker.as_ref().unwrap().write().unwrap();
        let mmtk = self.mmtk.unwrap();
        work.do_work_with_stat(&mut coordinator_worker, mmtk);
    }

    /// Drain the message queue and execute coordinator work. Only the coordinator should call this.
    pub fn wait_for_completion(&self) {
        self.in_gc_pause.store(true, Ordering::SeqCst);
        // At the start of a GC, we probably already have received a `ScheduleCollection` work. Run it now.
        if let Some(initializer) = self.startup.lock().unwrap().take() {
            self.process_coordinator_work(initializer);
        }
        loop {
            let message = self.channel.1.recv().unwrap();
            match message {
                CoordinatorMessage::Work(work) => {
                    self.process_coordinator_work(work);
                }
                CoordinatorMessage::AllWorkerParked | CoordinatorMessage::BucketDrained => {
                    unreachable!()
                }
                CoordinatorMessage::Finish => {}
            }
            let _guard = self.worker_monitor.0.lock().unwrap();
            if self.worker_group().all_parked() && self.all_buckets_empty() {
                break;
            }
        }
        for message in self.channel.1.try_iter() {
            if let CoordinatorMessage::Work(work) = message {
                self.process_coordinator_work(work);
            }
        }
        self.deactivate_all();
        // Finalization: Resume mutators, reset gc states
        // Note: Resume-mutators must happen after all work buckets are closed.
        //       Otherwise, for generational GCs, workers will receive and process
        //       newly generated remembered-sets from those open buckets.
        //       But these remsets should be preserved until next GC.
        if let Some(finalizer) = self.finalizer.lock().unwrap().take() {
            self.process_coordinator_work(finalizer);
        }
        self.in_gc_pause.store(false, Ordering::SeqCst);
        self.schedule_concurrent_packets();
        self.assert_all_deactivated();
    }

    fn schedule_concurrent_packets(&self) {
        crate::PAUSE_CONCURRENT_MARKING.store(false, Ordering::SeqCst);
        if !self.postponed_concurrent_work.is_empty() {
            let mut queue = Injector::new();
            type Q<VM> = Injector<Box<dyn GCWork<VM>>>;
            unsafe {
                let postponed_concurrent_work =
                    &mut *(&self.postponed_concurrent_work as *const Q<VM> as *mut Q<VM>);
                std::mem::swap::<Q<VM>>(&mut queue, postponed_concurrent_work);
            }
            let old_queue = self.work_buckets[WorkBucketStage::Unconstrained].swap_queue(queue);
            debug_assert!(old_queue.is_empty());
            self.work_buckets[WorkBucketStage::Unconstrained].notify_all_workers();
        }
    }

    pub fn assert_all_deactivated(&self) {
        if cfg!(debug_assertions) {
            self.work_buckets.iter().for_each(|(id, bkt)| {
                if id != WorkBucketStage::Unconstrained {
                    assert!(!bkt.is_activated());
                }
            });
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

    pub fn add_coordinator_work(&self, work: impl CoordinatorWork<VM>, worker: &GCWorker<VM>) {
        worker
            .sender
            .send(CoordinatorMessage::Work(box work))
            .unwrap();
    }

    #[inline]
    fn in_concurrent(&self) -> bool {
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
        match worker.local_work_bucket.poll_no_batch() {
            Steal::Success(w) => return Steal::Success(w),
            Steal::Retry => retry = true,
            _ => {}
        }
        if self.in_concurrent() && !worker.is_concurrent_worker() {
            return Steal::Empty;
        }
        for work_bucket in self.work_buckets.values() {
            match work_bucket.poll(&worker.local_work_buffer) {
                Steal::Success(w) => return Steal::Success(w),
                Steal::Retry => retry = true,
                _ => {}
            }
        }
        for (id, stealer) in &self.worker_group().stealers {
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
        debug_assert!(!worker.is_parked());
        let mut guard = self.worker_monitor.0.lock().unwrap();
        loop {
            debug_assert!(!worker.is_parked());
            if !self.in_concurrent() || worker.is_concurrent_worker() {
                if let Some(work) = self.pop_schedulable_work(worker) {
                    return work;
                }
            }
            // Park this worker
            let all_parked = self.worker_group().inc_parked_workers();
            if all_parked {
                if self.update_buckets() {
                    self.worker_group().dec_parked_workers();
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
            self.worker_group().dec_parked_workers();
        }
    }

    pub fn enable_stat(&self) {
        for worker in &self.worker_group().workers {
            worker.stat.enable();
        }
        let coordinator_worker = self.coordinator_worker.as_ref().unwrap().read().unwrap();
        coordinator_worker.stat.enable();
    }

    pub fn statistics(&self) -> HashMap<String, String> {
        let mut summary = SchedulerStat::default();
        for worker in &self.worker_group().workers {
            summary.merge(&worker.stat);
        }
        let coordinator_worker = self.coordinator_worker.as_ref().unwrap().read().unwrap();
        summary.merge(&coordinator_worker.stat);
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
        mmtk.plan.base().control_collector_context.clear_request();
        let first_stw_bucket = self.work_buckets.values().skip(1).next().unwrap();
        // debug_assert!(!first_stw_bucket.is_activated());
        first_stw_bucket.activate();
        let _guard = self.worker_monitor.0.lock().unwrap();
        self.worker_monitor.1.notify_all();
    }
}

pub static mut LAST_ACTIVATE_TIME: Option<SystemTime> = None;
