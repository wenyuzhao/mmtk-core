use super::stat::WorkerLocalStat;
use super::work_bucket::*;
use super::*;
use crate::mmtk::MMTK;
use crate::util::copy::GCWorkerCopyContext;
use crate::util::opaque_pointer::*;
use crate::vm::{Collection, GCThreadContext, VMBinding};
use atomic::Atomic;
use atomic_refcell::{AtomicRef, AtomicRefCell, AtomicRefMut};
use crossbeam::deque::{self, Stealer};
use crossbeam::queue::ArrayQueue;
#[cfg(feature = "count_live_bytes_in_gc")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Condvar, Mutex};

/// Represents the ID of a GC worker thread.
pub type ThreadId = usize;

thread_local! {
    /// Current worker's ordinal
    static WORKER_ORDINAL: Atomic<ThreadId> = const { Atomic::new(ThreadId::MAX) };
    static _WORKER: Atomic<usize> = Atomic::new(0);
}

lazy_static! {
    static ref _WORKERS: Mutex<Vec<OpaquePointer>> = Mutex::new(Vec::new());
}

pub fn reset_workers<VM: VMBinding>() {
    let workers = _WORKERS.lock().unwrap();
    for w in workers.iter() {
        let w = w.as_mut_ptr::<GCWorker<VM>>();
        unsafe {
            (*w).get_copy_context_mut().release();
        }
    }
}

/// Get current worker ordinal. Return `None` if the current thread is not a worker.
pub fn current_worker_ordinal() -> Option<ThreadId> {
    let ordinal = WORKER_ORDINAL.with(|x| x.load(Ordering::Relaxed));
    if ordinal == ThreadId::MAX {
        None
    } else {
        Some(ordinal)
    }
}

/// The part shared between a GCWorker and the scheduler.
/// This structure is used for communication, e.g. adding new work packets.
pub struct GCWorkerShared<VM: VMBinding> {
    /// Worker-local statistics data.
    stat: AtomicRefCell<WorkerLocalStat<VM>>,
    /// Accumulated bytes for live objects in this GC. When each worker scans
    /// objects, we increase the live bytes. We get this value from each worker
    /// at the end of a GC, and reset this counter.
    #[cfg(feature = "count_live_bytes_in_gc")]
    live_bytes: AtomicUsize,
    /// A queue of GCWork that can only be processed by the owned thread.
    ///
    /// Note: Currently, designated work cannot be added from the GC controller thread, or
    /// there will be synchronization problems.  If it is necessary to do so, we need to
    /// update the code in `GCWorkScheduler::poll_slow` for proper synchornization.
    pub designated_work: ArrayQueue<Box<dyn GCWork<VM>>>,
    /// Handle for stealing packets from the current worker
    pub stealer: Option<Stealer<Box<dyn GCWork<VM>>>>,
    is_stw_worker: bool,
}

impl<VM: VMBinding> GCWorkerShared<VM> {
    pub fn new(stealer: Option<Stealer<Box<dyn GCWork<VM>>>>, is_stw_worker: bool) -> Self {
        Self {
            stat: Default::default(),
            #[cfg(feature = "count_live_bytes_in_gc")]
            live_bytes: AtomicUsize::new(0),
            designated_work: ArrayQueue::new(16),
            stealer,
            is_stw_worker,
        }
    }

    #[cfg(feature = "count_live_bytes_in_gc")]
    pub(crate) fn increase_live_bytes(&self, bytes: usize) {
        self.live_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    #[cfg(feature = "count_live_bytes_in_gc")]
    pub(crate) fn get_and_clear_live_bytes(&self) -> usize {
        self.live_bytes.swap(0, Ordering::SeqCst)
    }
}

/// A GC worker.  This part is privately owned by a worker thread.
/// The GC controller also has an embedded `GCWorker` because it may also execute work packets.
pub struct GCWorker<VM: VMBinding> {
    /// The VM-specific thread-local state of the GC thread.
    pub tls: VMWorkerThread,
    /// The ordinal of the worker, numbered from 0 to the number of workers minus one. The ordinal
    /// is usize::MAX if it is the embedded worker of the GC controller thread.
    pub ordinal: ThreadId,
    /// The reference to the scheduler.
    scheduler: Arc<GCWorkScheduler<VM>>,
    /// The copy context, used to implement copying GC.
    copy: GCWorkerCopyContext<VM>,
    /// The sending end of the channel to send message to the controller thread.
    pub sender: Sender<CoordinatorMessage<VM>>,
    /// The reference to the MMTk instance.
    pub mmtk: &'static MMTK<VM>,
    /// True if this struct is the embedded GCWorker of the controller thread.
    /// False if this struct belongs to a standalone GCWorker thread.
    is_coordinator: bool,
    /// Reference to the shared part of the GC worker.  It is used for synchronization.
    pub shared: Arc<GCWorkerShared<VM>>,
    /// Local work packet queue.
    pub local_work_buffer: deque::Worker<Box<dyn GCWork<VM>>>,
}

unsafe impl<VM: VMBinding> Sync for GCWorkerShared<VM> {}
unsafe impl<VM: VMBinding> Send for GCWorkerShared<VM> {}

// Error message for borrowing `GCWorkerShared::stat`.
const STAT_BORROWED_MSG: &str = "GCWorkerShared.stat is already borrowed.  This may happen if \
    the mutator calls harness_begin or harness_end while the GC is running.";

impl<VM: VMBinding> GCWorkerShared<VM> {
    pub fn borrow_stat(&self) -> AtomicRef<WorkerLocalStat<VM>> {
        self.stat.try_borrow().expect(STAT_BORROWED_MSG)
    }

    pub fn borrow_stat_mut(&self) -> AtomicRefMut<WorkerLocalStat<VM>> {
        self.stat.try_borrow_mut().expect(STAT_BORROWED_MSG)
    }
}

impl<VM: VMBinding> GCWorker<VM> {
    pub fn new(
        mmtk: &'static MMTK<VM>,
        ordinal: ThreadId,
        scheduler: Arc<GCWorkScheduler<VM>>,
        is_coordinator: bool,
        sender: Sender<CoordinatorMessage<VM>>,
        shared: Arc<GCWorkerShared<VM>>,
        local_work_buffer: deque::Worker<Box<dyn GCWork<VM>>>,
    ) -> Self {
        Self {
            tls: VMWorkerThread(VMThread::UNINITIALIZED),
            ordinal,
            // We will set this later
            copy: GCWorkerCopyContext::new_non_copy(),
            sender,
            scheduler,
            mmtk,
            is_coordinator,
            shared,
            local_work_buffer,
        }
    }

    pub fn is_stw(&self) -> bool {
        self.shared.is_stw_worker
    }

    /// Get current worker.
    pub fn current() -> &'static mut Self {
        let ptr = _WORKER.with(|x| x.load(Ordering::Relaxed)) as *mut Self;
        unsafe { &mut *ptr }
    }

    const LOCALLY_CACHED_WORK_PACKETS: usize = 16;

    /// Add a work packet to the work queue and mark it with a higher priority.
    /// If the bucket is activated, the packet will be pushed to the local queue, otherwise it will be
    /// pushed to the global bucket with a higher priority.
    pub fn add_work_prioritized(&mut self, bucket: WorkBucketStage, work: impl GCWork<VM>) {
        if !self.scheduler().work_buckets[bucket].is_activated()
            || self.local_work_buffer.len() >= Self::LOCALLY_CACHED_WORK_PACKETS
        {
            self.scheduler.work_buckets[bucket].add_prioritized(Box::new(work));
            return;
        }
        self.local_work_buffer.push(Box::new(work));
    }

    pub fn add_boxed_work(&mut self, bucket: WorkBucketStage, work: Box<dyn GCWork<VM>>) {
        if !self.scheduler().work_buckets[bucket].is_activated()
            || self.local_work_buffer.len() >= Self::LOCALLY_CACHED_WORK_PACKETS
        {
            self.scheduler.work_buckets[bucket].add_boxed(work);
            return;
        }
        self.local_work_buffer.push(work);
    }

    /// Add a work packet to the work queue.
    /// If the bucket is activated, the packet will be pushed to the local queue, otherwise it will be
    /// pushed to the global bucket.
    pub fn add_work(&mut self, bucket: WorkBucketStage, work: impl GCWork<VM>) {
        if !self.scheduler().work_buckets[bucket].is_activated()
            || self.local_work_buffer.len() >= Self::LOCALLY_CACHED_WORK_PACKETS
        {
            self.scheduler.work_buckets[bucket].add(work);
            return;
        }
        self.local_work_buffer.push(Box::new(work));
    }

    /// Is this worker a coordinator or a normal GC worker?
    pub fn is_coordinator(&self) -> bool {
        self.is_coordinator
    }

    /// Get the scheduler. There is only one scheduler per MMTk instance.
    pub fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    /// Get a mutable reference of the copy context for this worker.
    pub fn get_copy_context_mut(&mut self) -> &mut GCWorkerCopyContext<VM> {
        &mut self.copy
    }

    /// Poll a ready-to-execute work packet in the following order:
    ///
    /// 1. Any packet that should be processed only by this worker.
    /// 2. Poll from the local work queue.
    /// 3. Poll from activated global work-buckets
    /// 4. Steal from other workers
    fn poll(&self) -> Box<dyn GCWork<VM>> {
        self.shared
            .designated_work
            .pop()
            .or_else(|| self.local_work_buffer.pop())
            .unwrap_or_else(|| self.scheduler().poll(self))
    }

    /// Entry of the worker thread. Resolve thread affinity, if it has been specified by the user.
    /// Each worker will keep polling and executing work packets in a loop.
    pub fn run(&mut self, tls: VMWorkerThread, mmtk: &'static MMTK<VM>) {
        #[cfg(feature = "tracing")]
        probe!(mmtk, gcworker_run);
        WORKER_ORDINAL.with(|x| x.store(self.ordinal, Ordering::SeqCst));
        let worker = self as *mut Self;
        _WORKER.with(|x| x.store(self as *mut Self as usize, Ordering::SeqCst));
        _WORKERS
            .lock()
            .unwrap()
            .push(OpaquePointer::from_mut_ptr(worker));
        self.scheduler.resolve_affinity(self.ordinal);
        self.tls = tls;
        self.copy = crate::plan::create_gc_worker_context(tls, mmtk);
        let lower_priority_for_concurrent_work = crate::args().lower_concurrent_worker_priority;
        assert!(!lower_priority_for_concurrent_work);
        loop {
            // Instead of having work_start and work_end tracepoints, we have
            // one tracepoint before polling for more work and one tracepoint
            // before executing the work.
            // This allows measuring the distribution of both the time needed
            // poll work (between work_poll and work), and the time needed to
            // execute work (between work and next work_poll).
            // If we have work_start and work_end, we cannot measure the first
            // poll.
            #[cfg(feature = "tracing")]
            probe!(mmtk, work_poll);
            let mut work = self.poll();
            // probe! expands to an empty block on unsupported platforms
            #[allow(unused_variables)]
            #[cfg(feature = "tracing")]
            let typename = work.get_type_name();

            #[cfg(feature = "bpftrace_workaround")]
            // Workaround a problem where bpftrace script cannot see the work packet names,
            // by force loading from the packet name.
            // See the "Known issues" section in `tools/tracing/timeline/README.md`
            std::hint::black_box(unsafe { *(typename.as_ptr()) });

            #[cfg(feature = "tracing")]
            probe!(mmtk, work, typename.as_ptr(), typename.len());
            work.do_work_with_stat(self, mmtk);
            std::mem::drop(work);
            flush_logs!();
        }
    }
}

/// A worker group to manage all the GC workers (except the coordinator worker).
pub struct WorkerGroup<VM: VMBinding> {
    /// Shared worker data
    pub workers_shared: Vec<Arc<GCWorkerShared<VM>>>,
    parked_workers: AtomicUsize,
    total_parked_workers: Arc<AtomicUsize>,
    unspawned_local_work_queues: Mutex<Vec<deque::Worker<Box<dyn GCWork<VM>>>>>,
    ordinal_base: usize,
    pub monitor: Arc<(Arc<Mutex<()>>, Condvar)>,
    total_workers: usize,
}

impl<VM: VMBinding> WorkerGroup<VM> {
    /// Create a WorkerGroup
    pub fn new(
        total_parked_workers: Arc<AtomicUsize>,
        is_stw: bool,
        ordinal_base: usize,
        num_workers: usize,
        total_workers: usize,
        lock: Arc<Mutex<()>>,
    ) -> Arc<Self> {
        let unspawned_local_work_queues = (0..num_workers)
            .map(|_| deque::Worker::new_fifo())
            .collect::<Vec<_>>();

        let workers_shared = (0..num_workers)
            .map(|i| {
                Arc::new(GCWorkerShared::<VM>::new(
                    Some(unspawned_local_work_queues[i].stealer()),
                    is_stw,
                ))
            })
            .collect::<Vec<_>>();

        Arc::new(Self {
            workers_shared,
            parked_workers: Default::default(),
            total_parked_workers,
            unspawned_local_work_queues: Mutex::new(unspawned_local_work_queues),
            ordinal_base,
            monitor: Arc::new((lock, Default::default())),
            total_workers,
        })
    }

    /// Spawn all the worker threads
    pub fn spawn(
        &self,
        mmtk: &'static MMTK<VM>,
        sender: Sender<CoordinatorMessage<VM>>,
        tls: VMThread,
    ) {
        let mut unspawned_local_work_queues = self.unspawned_local_work_queues.lock().unwrap();
        // Spawn each worker thread.
        for (ordinal, shared) in self.workers_shared.iter().enumerate() {
            let worker = Box::new(GCWorker::new(
                mmtk,
                self.ordinal_base + ordinal,
                mmtk.scheduler.clone(),
                false,
                sender.clone(),
                shared.clone(),
                unspawned_local_work_queues.pop().unwrap(),
            ));
            VM::VMCollection::spawn_gc_thread(tls, GCThreadContext::<VM>::Worker(worker));
        }
        debug_assert!(unspawned_local_work_queues.is_empty());
    }

    /// Get the number of workers in the group
    pub fn worker_count(&self) -> usize {
        self.workers_shared.len()
    }

    /// Increase the packed-workers counter.
    /// Called before a worker is parked.
    ///
    /// Return true if all the workers are parked.
    pub fn inc_parked_workers(&self) -> bool {
        self.parked_workers.fetch_add(1, Ordering::SeqCst);
        let old = self.total_parked_workers.fetch_add(1, Ordering::SeqCst);
        debug_assert!(old < self.total_workers);
        old + 1 == self.total_workers
    }

    /// Decrease the packed-workers counter.
    /// Called after a worker is resumed from the parked state.
    pub fn dec_parked_workers(&self) {
        self.parked_workers.fetch_sub(1, Ordering::SeqCst);
        let old = self.total_parked_workers.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(old <= self.total_workers);
    }

    /// Get the number of parked workers in the group
    pub fn parked_workers_in_group(&self) -> usize {
        self.parked_workers.load(Ordering::SeqCst)
    }

    /// Return true if there're any pending designated work
    pub fn has_designated_work(&self) -> bool {
        self.workers_shared
            .iter()
            .any(|w| !w.designated_work.is_empty())
    }

    #[cfg(feature = "count_live_bytes_in_gc")]
    pub fn get_and_clear_worker_live_bytes(&self) -> usize {
        self.workers_shared
            .iter()
            .map(|w| w.get_and_clear_live_bytes())
            .sum()
    }
}

/// This ensures the worker always decrements the parked worker count on all control flow paths.
pub(crate) struct ParkingGuard<'a, VM: VMBinding> {
    worker_group: &'a WorkerGroup<VM>,
    all_parked: bool,
}

impl<'a, VM: VMBinding> ParkingGuard<'a, VM> {
    pub fn new(worker_group: &'a WorkerGroup<VM>) -> Self {
        let all_parked = worker_group.inc_parked_workers();
        ParkingGuard {
            worker_group,
            all_parked,
        }
    }

    pub fn all_parked(&self) -> bool {
        self.all_parked
    }
}

impl<'a, VM: VMBinding> Drop for ParkingGuard<'a, VM> {
    fn drop(&mut self) {
        self.worker_group.dec_parked_workers();
    }
}
