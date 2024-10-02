use super::worker_monitor::WorkerMonitor;
use super::*;
use crossbeam::deque::{Injector, Steal, Worker};
use crossbeam::queue::SegQueue;
use enum_map::Enum;
use portable_atomic::{AtomicBool, AtomicUsize};
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
    id: BucketId,
    count: AtomicUsize,
    queue: RwLock<SegQueue<Box<dyn GCWork>>>,
    is_open: AtomicBool,
}

impl WorkBucket {
    pub const fn new(id: BucketId) -> Self {
        Self {
            id,
            count: AtomicUsize::new(0),
            queue: RwLock::new(SegQueue::new()),
            is_open: AtomicBool::new(false),
        }
    }

    pub fn is_open(&self) -> bool {
        self.is_open.load(Ordering::Relaxed)
    }

    pub(super) fn open(&self) {
        self.is_open.store(true, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        if self.count.load(Ordering::SeqCst) != 0 {
            println!(
                "Error: {:?} is not empty: {}",
                self.id,
                self.count.load(Ordering::SeqCst)
            );
        }
        assert!(
            self.count.load(Ordering::SeqCst) == 0,
            "{:?} is not empty",
            self.id
        );
        assert!(self.queue.read().unwrap().is_empty());
        self.is_open.store(false, Ordering::SeqCst);
    }

    pub fn take_queue(&self) -> SegQueue<Box<dyn GCWork>> {
        std::mem::replace(&mut self.queue.write().unwrap(), SegQueue::new())
    }

    pub fn set_queue(&self, new_queue: SegQueue<Box<dyn GCWork>>) {
        let count = new_queue.len();
        let mut queue = self.queue.write().unwrap();
        assert!(queue.is_empty(), "Queue is not empty: {}", queue.len());
        *queue = new_queue;
        assert!(self.count.load(Ordering::SeqCst) == 0);
        self.count.store(count, Ordering::SeqCst);
    }

    pub fn merge_queue(&self, new_queue: SegQueue<Box<dyn GCWork>>) {
        let count = new_queue.len();
        let queue = self.queue.write().unwrap();
        while let Some(work) = new_queue.pop() {
            queue.push(work);
        }
        self.count.fetch_add(count, Ordering::SeqCst);
    }

    /// Test if the bucket is drained
    pub fn is_empty(&self) -> bool {
        self.queue.read().unwrap().is_empty() && self.count.load(Ordering::SeqCst) == 0
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn queue_count(&self) -> usize {
        self.queue.read().unwrap().len()
    }

    pub(super) fn inc(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Returns true if the count is zero
    pub(super) fn dec(&self) -> bool {
        self.count.fetch_sub(1, Ordering::SeqCst) == 1
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

    pub(super) fn merge(&self, bucket: BucketId, queue: SegQueue<Box<dyn GCWork>>) {
        let global = if bucket.is_prioritized() {
            self.prioritized_queue.queue.read().unwrap()
        } else {
            self.queue.queue.read().unwrap()
        };
        while let Some(work) = queue.pop() {
            global.push((bucket, work));
        }
    }

    fn notify_one_worker(&self) {
        // Notify one if there're any parked workers.
        self.monitor.notify_work_available(false);
    }

    /// Test if the bucket is drained
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty() && self.prioritized_queue.is_empty()
    }

    /// Add a work packet to this bucket
    pub fn add_boxed(&self, b: BucketId, work: Box<dyn GCWork>) {
        self.queue.push(b, work);
        self.notify_one_worker();
    }

    /// Like [`WorkBucket::add_no_notify`], but the work is boxed.
    pub(crate) fn add_boxed_no_notify(&self, b: BucketId, work: Box<dyn GCWork>) {
        self.queue.push(b, work);
    }

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
#[repr(u8)]
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
    ConcClosure,
    FinishMark,
    PostSATBSWeep,
    LazySweep,
}

static START: WorkBucket = WorkBucket::new(BucketId::Start);
static ROOTS: WorkBucket = WorkBucket::new(BucketId::Roots);
static INCS: WorkBucket = WorkBucket::new(BucketId::Incs);
static PREPARE: WorkBucket = WorkBucket::new(BucketId::Prepare);
static CLOSURE: WorkBucket = WorkBucket::new(BucketId::Closure);
static RELEASE: WorkBucket = WorkBucket::new(BucketId::Release);
static REF_WEAK: WorkBucket = WorkBucket::new(BucketId::WeakRefClosure);
static REF_FINAL: WorkBucket = WorkBucket::new(BucketId::FinalRefClosure);
static REF_PHANTOM: WorkBucket = WorkBucket::new(BucketId::PhantomRefClosure);
static FINISH: WorkBucket = WorkBucket::new(BucketId::Finish);
static DECS: WorkBucket = WorkBucket::new(BucketId::Decs);
static CONC_CLOSURE: WorkBucket = WorkBucket::new(BucketId::ConcClosure);
static FINISH_MARK: WorkBucket = WorkBucket::new(BucketId::FinishMark);
static POST_SATB_SWEEP: WorkBucket = WorkBucket::new(BucketId::PostSATBSWeep);
static LAZY_SWEEP: WorkBucket = WorkBucket::new(BucketId::LazySweep);

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
            BucketId::ConcClosure => &CONC_CLOSURE,
            BucketId::FinishMark => &FINISH_MARK,
            BucketId::PostSATBSWeep => &POST_SATB_SWEEP,
            BucketId::LazySweep => &LAZY_SWEEP,
        }
    }

    pub const fn is_prioritized(&self) -> bool {
        match self {
            BucketId::Decs | BucketId::PostSATBSWeep | BucketId::LazySweep | BucketId::Roots => {
                true
            }
            _ => false,
        }
    }
}
