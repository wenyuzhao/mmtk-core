use super::worker_monitor::WorkerMonitor;
use super::*;
use crossbeam::deque::{Injector, Steal, Worker};
use crossbeam::queue::SegQueue;
use enum_map::Enum;
use portable_atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

struct BucketQueue {
    // FIXME: Performance!
    queue: RwLock<Injector<(BucketId, Box<dyn GCWork>)>>,
}

impl BucketQueue {
    fn new() -> Self {
        Self {
            queue: RwLock::new(Injector::new()),
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.read().unwrap().is_empty()
    }

    fn steal_batch_and_pop(
        &self,
        dest: &Worker<(BucketId, Box<dyn GCWork>)>,
    ) -> Steal<(BucketId, Box<dyn GCWork>)> {
        self.queue.read().unwrap().steal_batch_and_pop(dest)
    }

    fn push(&self, b: BucketId, w: Box<dyn GCWork>) {
        self.queue.read().unwrap().push((b, w));
    }
}

pub struct WorkBucket {
    #[allow(unused)]
    name: &'static str,
    count: AtomicUsize,
    queue: RwLock<SegQueue<Box<dyn GCWork>>>,
}

impl WorkBucket {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            count: AtomicUsize::new(0),
            queue: RwLock::new(SegQueue::new()),
        }
    }

    pub fn reset(&self) {
        assert!(
            self.count.load(Ordering::SeqCst) == 0,
            "{:?} is not empty",
            self.name
        );
        assert!(self.queue.read().unwrap().is_empty());
    }

    pub fn take_queue(&self) -> SegQueue<Box<dyn GCWork>> {
        std::mem::replace(&mut self.queue.write().unwrap(), SegQueue::new())
    }

    pub fn set_queue(&self, new_queue: SegQueue<Box<dyn GCWork>>) {
        let count = new_queue.len();
        let mut queue = self.queue.write().unwrap();
        assert!(queue.is_empty());
        *queue = new_queue;
        assert!(self.count.load(Ordering::SeqCst) == 0);
        self.count.store(count, Ordering::SeqCst);
    }

    /// Test if the bucket is drained
    pub fn is_empty(&self) -> bool {
        self.queue.read().unwrap().is_empty() && self.count.load(Ordering::Relaxed) == 0
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub(super) fn inc(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns true if the count is zero
    pub(super) fn dec(&self, _name: &str) -> bool {
        let x = self.count.fetch_sub(1, Ordering::Relaxed);
        x == 1
    }

    /// Add a work packet to this bucket
    pub(super) fn add(&self, work: Box<dyn GCWork>) {
        let queue = self.queue.read().unwrap();
        queue.push(work);
    }
}

pub struct ActiveWorkBucket {
    queue: BucketQueue,
    prioritized_queue: BucketQueue,
    monitor: Arc<WorkerMonitor>,
}

impl ActiveWorkBucket {
    pub(crate) fn new(monitor: Arc<WorkerMonitor>) -> Self {
        Self {
            queue: BucketQueue::new(),
            prioritized_queue: BucketQueue::new(),
            monitor,
        }
    }

    // pub fn swap_queue(
    //     &self,
    //     mut new_queue: Injector<Box<dyn GCWork>>,
    // ) -> Injector<Box<dyn GCWork>> {
    //     let mut queue = self.queue.queue.write().unwrap();
    //     std::mem::swap::<Injector<Box<dyn GCWork>>>(&mut queue, &mut new_queue);
    //     new_queue
    // }

    // pub fn swap_queue_prioritized(
    //     &self,
    //     mut new_queue: Injector<Box<dyn GCWork>>,
    // ) -> Injector<Box<dyn GCWork>> {
    //     let mut queue = self.prioritized_queue.queue.write().unwrap();
    //     std::mem::swap::<Injector<Box<dyn GCWork>>>(&mut queue, &mut new_queue);
    //     new_queue
    // }

    fn notify_one_worker(&self) {
        // Notify one if there're any parked workers.
        self.monitor.notify_work_available(false);
    }

    /// Test if the bucket is drained
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty() && self.prioritized_queue.is_empty()
    }

    /// Add a work packet to this bucket
    /// Panic if this bucket cannot receive prioritized packets.
    // pub fn add_prioritized(&self, work: Box<dyn GCWork>) {
    //     self.prioritized_queue.push(work);
    //     self.notify_one_worker();
    // }

    /// Add a work packet to this bucket
    // pub fn add<W: GCWork>(&self, work: W) {
    //     self.queue.push(Box::new(work));
    //     self.notify_one_worker();
    // }

    /// Add a work packet to this bucket
    pub fn add_boxed(&self, b: BucketId, work: Box<dyn GCWork>) {
        self.queue.push(b, work);
        self.notify_one_worker();
    }

    /// Add a work packet to this bucket, but do not notify any workers.
    /// This is useful when the current thread is holding the mutex of `WorkerMonitor` which is
    /// used for notifying workers.  This usually happens if the current thread is the last worker
    /// parked.
    // pub(crate) fn add_no_notify<W: GCWork>(&self, work: W) {
    //     self.queue.push(Box::new(work));
    // }

    /// Like [`WorkBucket::add_no_notify`], but the work is boxed.
    pub(crate) fn add_boxed_no_notify(&self, b: BucketId, work: Box<dyn GCWork>) {
        self.queue.push(b, work);
    }

    /// Add multiple packets with a higher priority.
    /// Panic if this bucket cannot receive prioritized packets.
    // pub fn bulk_add_prioritized(&self, work_vec: Vec<Box<dyn GCWork>>) {
    //     self.prioritized_queue.push_all(work_vec);
    //     self.notify_all_workers();
    // }

    /// Add multiple packets
    // pub fn bulk_add(&self, work_vec: Vec<Box<dyn GCWork>>) {
    //     if work_vec.is_empty() {
    //         return;
    //     }
    //     let len = work_vec.len();
    //     self.queue.push_all(work_vec);
    //     if len == 1 {
    //         self.notify_one_worker();
    //     } else {
    //         self.notify_all_workers();
    //     }
    // }

    /// Get a work packet from this bucket
    pub fn poll(
        &self,
        worker: &Worker<(BucketId, Box<dyn GCWork>)>,
    ) -> Steal<(BucketId, Box<dyn GCWork>)> {
        if self.is_empty() {
            return Steal::Empty;
        }
        self.prioritized_queue
            .steal_batch_and_pop(worker)
            .or_else(|| self.queue.steal_batch_and_pop(worker))
    }
}

/// This enum defines all the work bucket types. The scheduler
/// will instantiate a work bucket for each stage defined here.
#[derive(Debug, Enum, Copy, Clone, Eq, PartialEq, Hash)]
pub enum BucketId {
    Start,
    Incs,
    Prepare,
    Roots,
    Closure,
    WeakRefClosure,
    FinalRefClosure,
    PhantomRefClosure,
    Release,
    Finish,
    Decs,
}

static START: WorkBucket = WorkBucket::new("start");
static ROOTS: WorkBucket = WorkBucket::new("roots");
static INCS: WorkBucket = WorkBucket::new("incs");
static PREPARE: WorkBucket = WorkBucket::new("prepare");
static CLOSURE: WorkBucket = WorkBucket::new("closure");
static RELEASE: WorkBucket = WorkBucket::new("release");
static REF_WEAK: WorkBucket = WorkBucket::new("ref.weak");
static REF_FINAL: WorkBucket = WorkBucket::new("ref.final");
static REF_PHANTOM: WorkBucket = WorkBucket::new("ref.phantom");
static FINISH: WorkBucket = WorkBucket::new("finish");
static DECS: WorkBucket = WorkBucket::new("decs");

impl BucketId {
    #[inline(always)]
    pub fn get_bucket(&self) -> &'static WorkBucket {
        match self {
            BucketId::Start => &START,
            BucketId::Prepare => &PREPARE,
            BucketId::Roots => &ROOTS,
            BucketId::Incs => &INCS,
            BucketId::Closure => &CLOSURE,
            BucketId::WeakRefClosure => &REF_WEAK,
            BucketId::FinalRefClosure => &REF_FINAL,
            BucketId::PhantomRefClosure => &REF_PHANTOM,
            BucketId::Release => &RELEASE,
            BucketId::Finish => &FINISH,
            BucketId::Decs => &DECS,
        }
    }
}
