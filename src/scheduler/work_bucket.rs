use super::worker::WorkerGroup;
use super::*;
use crate::vm::VMBinding;
use crossbeam_queue::SegQueue;
use enum_map::Enum;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub struct WorkBucket<VM: VMBinding> {
    active: AtomicBool,
    /// A priority queue
    queue: SegQueue<Box<dyn GCWork<VM>>>,
    monitor: Arc<(Mutex<()>, Condvar)>,
    can_open: Option<Box<dyn (Fn() -> bool) + Send>>,
    group: Option<Arc<WorkerGroup<VM>>>,
}

impl<VM: VMBinding> WorkBucket<VM> {
    pub const DEFAULT_PRIORITY: usize = 1000;
    pub fn new(active: bool, monitor: Arc<(Mutex<()>, Condvar)>) -> Self {
        Self {
            active: AtomicBool::new(active),
            queue: Default::default(),
            monitor,
            can_open: None,
            group: None,
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
        if let Some(parked) = self.parked_workers() {
            if parked > 0 {
                let _guard = self.monitor.0.lock().unwrap();
                self.monitor.1.notify_one()
            }
        }
    }
    #[inline(always)]
    fn notify_all_workers(&self) {
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
        self.queue.is_empty()
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
    /// Add a work packet to this bucket, with a given priority
    pub fn add_with_priority(&self, _priority: usize, work: Box<dyn GCWork<VM>>) {
        self.queue.push(work);
        self.notify_one_worker(); // FIXME: Performance
    }
    /// Add a work packet to this bucket, with a default priority (1000)
    #[inline(always)]
    pub fn add<W: GCWork<VM>>(&self, work: W) {
        self.add_with_priority(Self::DEFAULT_PRIORITY, box work);
    }
    #[inline(always)]
    pub fn bulk_add_with_priority(&self, _priority: usize, work_vec: Vec<Box<dyn GCWork<VM>>>) {
        for w in work_vec {
            self.queue.push(w)
        }
        self.notify_all_workers(); // FIXME: Performance
    }
    #[inline(always)]
    pub fn bulk_add(&self, work_vec: Vec<Box<dyn GCWork<VM>>>) {
        self.bulk_add_with_priority(1000, work_vec)
    }
    /// Get a work packet (with the greatest priority) from this bucket
    #[inline(always)]
    pub fn poll(&self) -> Option<Box<dyn GCWork<VM>>> {
        if !self.active.load(Ordering::SeqCst) {
            return None;
        }
        self.queue.pop()
    }
    pub fn set_open_condition(&mut self, pred: impl Fn() -> bool + Send + 'static) {
        self.can_open = Some(box pred);
    }
    #[inline(always)]
    pub fn update(&self) -> bool {
        if let Some(can_open) = self.can_open.as_ref() {
            if !self.is_activated() && can_open() {
                self.activate();
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Enum, Copy, Clone, Eq, PartialEq)]
pub enum WorkBucketStage {
    Unconstrained,
    Prepare,
    ProcessRoots,
    PreClosure,
    Closure,
    // TODO: We only support final reference at the moment. If we have references of multiple strengths,
    // we may need more than one buckets for each reference strength.
    PostClosure,
    RefClosure,
    RefForwarding,
    Release,
    Final,
}
