use super::worker::WorkerGroup;
use super::*;
use crate::vm::VMBinding;
use crossbeam_deque::{Injector, Steal, Worker};
use enum_map::Enum;
use spin::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

enum BucketQueue<VM: VMBinding> {
    LockFree(Injector<Box<dyn GCWork<VM>>>),
    LwLock(RwLock<Injector<Box<dyn GCWork<VM>>>>),
}

impl<VM: VMBinding> BucketQueue<VM> {
    #[inline(always)]
    fn is_empty(&self) -> bool {
        match self {
            Self::LockFree(x) => x.is_empty(),
            Self::LwLock(x) => x.read().is_empty(),
        }
    }
    #[inline(always)]
    fn steal(&self) -> Steal<Box<dyn GCWork<VM>>> {
        match self {
            Self::LockFree(x) => x.steal(),
            Self::LwLock(x) => x.read().steal(),
        }
    }
    #[inline(always)]
    fn steal_batch_and_pop(
        &self,
        dest: &Worker<Box<dyn GCWork<VM>>>,
    ) -> Steal<Box<dyn GCWork<VM>>> {
        match self {
            Self::LockFree(x) => x.steal_batch_and_pop(dest),
            Self::LwLock(x) => x.read().steal_batch_and_pop(dest),
        }
    }
    #[inline(always)]
    fn push(&self, w: Box<dyn GCWork<VM>>) {
        match self {
            Self::LockFree(x) => x.push(w),
            Self::LwLock(x) => x.read().push(w),
        }
    }
    #[inline(always)]
    fn push_all(&self, ws: Vec<Box<dyn GCWork<VM>>>) {
        match self {
            Self::LockFree(x) => {
                for w in ws {
                    x.push(w)
                }
            }
            Self::LwLock(x) => {
                let x = x.read();
                for w in ws {
                    x.push(w)
                }
            }
        }
    }
}

pub struct WorkBucket<VM: VMBinding> {
    active: AtomicBool,
    /// A priority queue
    queue: BucketQueue<VM>,
    prioritized_queue: BucketQueue<VM>,
    monitor: Arc<(Mutex<()>, Condvar)>,
    can_open: Option<Box<dyn (Fn(&GCWorkScheduler<VM>) -> bool) + Send>>,
    group: Option<Arc<WorkerGroup<VM>>>,
}

impl<VM: VMBinding> WorkBucket<VM> {
    pub const DEFAULT_PRIORITY: usize = 1000;
    pub fn new(active: bool, monitor: Arc<(Mutex<()>, Condvar)>, unconstrained: bool) -> Self {
        Self {
            active: AtomicBool::new(active),
            queue: if unconstrained {
                BucketQueue::LwLock(RwLock::new(Injector::new()))
            } else {
                BucketQueue::LockFree(Injector::new())
            },
            prioritized_queue: if unconstrained {
                BucketQueue::LwLock(RwLock::new(Injector::new()))
            } else {
                BucketQueue::LockFree(Injector::new())
            },
            monitor,
            can_open: None,
            group: None,
        }
    }
    pub fn swap_queue(
        &self,
        mut queue: Injector<Box<dyn GCWork<VM>>>,
    ) -> Injector<Box<dyn GCWork<VM>>> {
        match &self.queue {
            BucketQueue::LockFree(_) => unreachable!(),
            BucketQueue::LwLock(x) => {
                std::mem::swap::<Injector<Box<dyn GCWork<VM>>>>(&mut x.write(), &mut queue);
                queue
            }
        }
    }
    pub fn swap_queue_prioritized(
        &self,
        mut queue: Injector<Box<dyn GCWork<VM>>>,
    ) -> Injector<Box<dyn GCWork<VM>>> {
        match &self.prioritized_queue {
            BucketQueue::LockFree(_) => unreachable!(),
            BucketQueue::LwLock(x) => {
                std::mem::swap::<Injector<Box<dyn GCWork<VM>>>>(&mut x.write(), &mut queue);
                queue
            }
        }
    }
    pub fn set_group(&mut self, group: Arc<WorkerGroup<VM>>) {
        self.group = Some(group)
    }
    #[inline(always)]
    fn parked_workers(&self) -> Option<usize> {
        Some(self.group.as_ref()?.parked_workers())
    }
    #[inline(always)]
    fn notify_one_worker(&self) {
        if !self.is_activated() {
            return;
        }
        if let Some(parked) = self.parked_workers() {
            if parked > 0 {
                let _guard = self.monitor.0.lock().unwrap();
                self.monitor.1.notify_one()
            }
        }
    }
    #[inline(always)]
    pub fn force_notify_all_workers(&self) {
        if let Some(parked) = self.parked_workers() {
            if parked > 0 {
                let _guard = self.monitor.0.lock().unwrap();
                self.monitor.1.notify_all()
            }
        }
    }
    #[inline(always)]
    pub fn notify_all_workers(&self) {
        if !self.is_activated() {
            return;
        }
        if let Some(parked) = self.parked_workers() {
            if parked > 0 {
                let _guard = self.monitor.0.lock().unwrap();
                self.monitor.1.notify_all()
            }
        }
    }
    #[inline(always)]
    pub fn is_activated(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
    /// Enable the bucket
    pub fn activate(&self) {
        self.active.store(true, Ordering::SeqCst);
    }
    /// Test if the bucket is drained
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty() && self.prioritized_queue.is_empty()
    }
    #[inline(always)]
    pub fn is_drained(&self) -> bool {
        self.is_activated() && self.is_empty()
    }
    /// Disable the bucket
    pub fn deactivate(&self) {
        debug_assert!(self.queue.is_empty(), "Bucket not drained before close");
        self.active.store(false, Ordering::SeqCst);
    }
    #[inline(always)]
    pub fn add_prioritized(&self, work: Box<dyn GCWork<VM>>) {
        self.prioritized_queue.push(work);
        if self.is_activated() && self.parked_workers().map(|c| c > 0).unwrap_or(true) {
            self.notify_one_worker();
        }
    }
    /// Add a work packet to this bucket, with a given priority
    #[inline(always)]
    pub fn add_with_priority(&self, _priority: usize, work: Box<dyn GCWork<VM>>) {
        self.queue.push(work);
        if self.is_activated() && self.parked_workers().map(|c| c > 0).unwrap_or(true) {
            self.notify_one_worker();
        }
    }
    /// Add a work packet to this bucket, with a default priority (1000)
    #[inline(always)]
    pub fn add<W: GCWork<VM>>(&self, work: W) {
        self.add_with_priority(Self::DEFAULT_PRIORITY, box work);
    }

    #[inline(always)]
    pub fn add_dyn(&self, work: Box<dyn GCWork<VM>>) {
        self.add_with_priority(Self::DEFAULT_PRIORITY, work);
    }

    #[inline(always)]
    pub fn bulk_add_with_priority(&self, _priority: usize, work_vec: Vec<Box<dyn GCWork<VM>>>) {
        self.queue.push_all(work_vec);
        if self.is_activated() {
            self.notify_all_workers();
        }
    }
    #[inline(always)]
    pub fn bulk_add_prioritized(&self, work_vec: Vec<Box<dyn GCWork<VM>>>) {
        self.prioritized_queue.push_all(work_vec);
        if self.is_activated() {
            self.notify_all_workers();
        }
    }
    #[inline(always)]
    pub fn bulk_add(&self, work_vec: Vec<Box<dyn GCWork<VM>>>) {
        if work_vec.is_empty() {
            return;
        }
        self.bulk_add_with_priority(1000, work_vec)
    }
    /// Get a work packet (with the greatest priority) from this bucket
    #[inline(always)]
    pub fn poll(&self, worker: &Worker<Box<dyn GCWork<VM>>>) -> Steal<Box<dyn GCWork<VM>>> {
        if !self.active.load(Ordering::SeqCst)
            || (self.queue.is_empty() && self.prioritized_queue.is_empty())
        {
            return Steal::Empty;
        }
        self.prioritized_queue
            .steal_batch_and_pop(worker)
            .or_else(|| self.queue.steal_batch_and_pop(worker))
    }
    #[inline(always)]
    pub fn poll_no_batch(&self) -> Steal<Box<dyn GCWork<VM>>> {
        if !self.active.load(Ordering::SeqCst) {
            return Steal::Empty;
        }
        if self.queue.is_empty() && self.prioritized_queue.is_empty() {
            return Steal::Empty;
        }
        self.prioritized_queue
            .steal()
            .or_else(|| self.queue.steal())
    }
    pub fn set_open_condition(
        &mut self,
        pred: impl Fn(&GCWorkScheduler<VM>) -> bool + Send + 'static,
    ) {
        self.can_open = Some(box pred);
    }
    #[inline(always)]
    pub fn update(&self, scheduler: &GCWorkScheduler<VM>) -> bool {
        if let Some(can_open) = self.can_open.as_ref() {
            if !self.is_activated() && can_open(scheduler) {
                self.activate();
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Enum, Copy, Clone, Eq, PartialEq, Hash)]
pub enum WorkBucketStage {
    Unconstrained,
    FinishConcurrentWork,
    Initial,
    Prepare,
    Closure,
    RefClosure,
    CalculateForwarding,
    SecondRoots,
    RefForwarding,
    Compact,
    Release,
    Final,
}

// Alias
#[allow(non_upper_case_globals)]
impl WorkBucketStage {
    pub const RCProcessIncs: Self = Self::Initial;
    #[cfg(feature = "ix_delayed_nursery_evacuation")]
    pub const RCEvacuateNursery: Self = Self::RefClosure;
    #[cfg(not(feature = "ix_delayed_nursery_evacuation"))]
    pub const RCEvacuateMature: Self = Self::Closure;
    pub const RCReleaseNursery: Self = Self::Release;
    #[cfg(not(feature = "instrumentation"))]
    pub const RCFullHeapRelease: Self = Self::RefForwarding;
    #[cfg(not(feature = "instrumentation"))]
    pub const RCProcessDecs: Self = Self::Release;

    #[cfg(feature = "instrumentation")]
    pub const RCFullHeapRelease: Self = Self::Compact;
    #[cfg(feature = "instrumentation")]
    pub const RCProcessDecs: Self = Self::RefForwarding;

    pub const fn rc_process_incs_stage() -> Self {
        if crate::args::EAGER_INCREMENTS && !crate::args::BARRIER_MEASUREMENT {
            WorkBucketStage::Unconstrained
        } else {
            WorkBucketStage::RCProcessIncs
        }
    }
}
