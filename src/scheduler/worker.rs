use super::stat::WorkerLocalStat;
use super::work_bucket::*;
use super::*;
use crate::mmtk::MMTK;
use crate::util::copy::GCWorkerCopyContext;
use crate::util::opaque_pointer::*;
use crate::vm::{Collection, GCThreadContext, VMBinding};
use atomic_refcell::{AtomicRef, AtomicRefCell, AtomicRefMut};
use crossbeam_deque::{Stealer, Worker};
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Weak};
use thread_priority::ThreadPriority;

/// Thread-local data for each worker thread.
///
/// For mmtk, each gc can define their own worker-local data, to contain their required copy allocators and other stuffs.
pub trait GCWorkerLocal {
    fn init(&mut self, _tls: VMWorkerThread) {}
}

/// This struct will be accessed during trace_object(), which is performance critical.
/// However, we do not know its concrete type as the plan and its copy context is dynamically selected.
/// Instead use a void* type to store it, and during trace_object() we cast it to the correct copy context type.
#[derive(Copy, Clone)]
pub struct GCWorkerLocalPtr {
    data: *mut c_void,
    // Save the type name for debug builds, so we can later do type check
    #[cfg(debug_assertions)]
    ty: &'static str,
}

impl GCWorkerLocalPtr {
    pub const UNINITIALIZED: Self = GCWorkerLocalPtr {
        data: std::ptr::null_mut(),
        #[cfg(debug_assertions)]
        ty: "uninitialized",
    };

    pub fn new<W: GCWorkerLocal>(worker_local: W) -> Self {
        GCWorkerLocalPtr {
            data: Box::into_raw(Box::new(worker_local)) as *mut c_void,
            #[cfg(debug_assertions)]
            ty: std::any::type_name::<W>(),
        }
    }

    /// # Safety
    /// The user needs to guarantee that the type supplied here is the same type used to create this pointer.
    pub unsafe fn as_type<W: GCWorkerLocal>(&mut self) -> &mut W {
        #[cfg(debug_assertions)]
        debug_assert_eq!(self.ty, std::any::type_name::<W>());
        &mut *(self.data as *mut W)
    }
}

/// The part shared between a GCWorker and the scheduler.
/// This structure is used for communication, e.g. adding new work packets.
pub struct GCWorkerShared<VM: VMBinding> {
    /// True if the GC worker is parked.
    pub parked: AtomicBool,
    /// Worker-local statistics data.
    stat: AtomicRefCell<WorkerLocalStat<VM>>,
    /// Incoming work packets to be executed by the current worker.
    pub local_work_bucket: WorkBucket<VM>,
    /// Cache of work packets created by the current worker.
    /// May be flushed to the global pool or executed locally.
    pub local_work_buffer: Worker<Box<dyn GCWork<VM>>>,
}

/// A GC worker.  This part is privately owned by a worker thread.
/// The GC controller also has an embedded `GCWorker` because it may also execute work packets.
pub struct GCWorker<VM: VMBinding> {
    /// The VM-specific thread-local state of the GC thread.
    pub tls: VMWorkerThread,
    /// The ordinal of the worker, numbered from 0 to the number of workers minus one.
    /// 0 if it is the embedded worker of the GC controller thread.
    pub ordinal: usize,
    /// The reference to the scheduler.
    scheduler: Arc<GCWorkScheduler<VM>>,
    /// The copy context, used to implement copying GC.
    copy: GCWorkerCopyContext<VM>,
    /// The sending end of the channel to send message to the controller thread.
    pub sender: Sender<CoordinatorMessage<VM>>,
    /// The reference to the MMTk instance.
    mmtk: &'static MMTK<VM>,
    /// True if this struct is the embedded GCWorker of the controller thread.
    /// False if this struct belongs to a standalone GCWorker thread.
    is_coordinator: bool,
    /// Reference to the shared part of the GC worker.  It is used for synchronization.
    pub shared: Arc<GCWorkerShared<VM>>,
}

unsafe impl<VM: VMBinding> Sync for GCWorkerShared<VM> {}
unsafe impl<VM: VMBinding> Send for GCWorkerShared<VM> {}

// Error message for borrowing `GCWorkerShared::stat`.
const STAT_BORROWED_MSG: &str = "GCWorkerShared.stat is already borrowed.  This may happen if \
    the mutator calls harness_begin or harness_end while the GC is running.";

impl<VM: VMBinding> GCWorkerShared<VM> {
    pub fn is_parked(&self) -> bool {
        self.parked.load(Ordering::SeqCst)
    }

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
        ordinal: usize,
        scheduler: Arc<GCWorkScheduler<VM>>,
        is_coordinator: bool,
        sender: Sender<CoordinatorMessage<VM>>,
    ) -> Self {
        let worker_monitor = scheduler.worker_monitor.clone();
        Self {
            tls: VMWorkerThread(VMThread::UNINITIALIZED),
            ordinal,
            // We will set this later
            copy: GCWorkerCopyContext::new_non_copy(),
            sender,
            scheduler,
            mmtk,
            is_coordinator,
            shared: Arc::new(GCWorkerShared {
                parked: AtomicBool::new(true),
                stat: Default::default(),
                local_work_buffer: Worker::new_fifo(),
                local_work_bucket: WorkBucket::new(true, worker_monitor, false),
            }),
        }
    }

    #[inline]
    pub fn add_work_prioritized(&mut self, bucket: WorkBucketStage, work: impl GCWork<VM>) {
        if !self.scheduler().work_buckets[bucket].is_activated()
            || !self.shared.local_work_buffer.is_empty()
        {
            self.scheduler.work_buckets[bucket].add_prioritized(box work);
            return;
        }
        self.shared.local_work_buffer.push(box work);
    }

    #[inline]
    pub fn add_work(&mut self, bucket: WorkBucketStage, work: impl GCWork<VM>) {
        if !self.scheduler().work_buckets[bucket].is_activated()
            || !self.shared.local_work_buffer.is_empty()
        {
            self.scheduler.work_buckets[bucket].add(work);
            return;
        }
        self.shared.local_work_buffer.push(box work);
    }

    pub fn is_coordinator(&self) -> bool {
        self.is_coordinator
    }

    pub fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    pub fn get_copy_context_mut(&mut self) -> &mut GCWorkerCopyContext<VM> {
        &mut self.copy
    }

    pub fn do_work(&'static mut self, mut work: impl GCWork<VM>) {
        work.do_work(self, self.mmtk);
    }

    fn poll(&self) -> Box<dyn GCWork<VM>> {
        self.shared
            .local_work_buffer
            .pop()
            .or_else(|| Some(self.scheduler().poll(self)))
            .unwrap()
    }

    pub fn run(&mut self, tls: VMWorkerThread, mmtk: &'static MMTK<VM>) {
        self.tls = tls;
        self.copy = crate::plan::create_gc_worker_context(tls, mmtk);
        self.shared.parked.store(false, Ordering::SeqCst);
        IS_WORKER.store(true, Ordering::SeqCst);
        WORKER_ID.store(self.ordinal, Ordering::SeqCst);
        let lower_priority_for_concurrent_work = *crate::args::LOWER_CONCURRENT_GC_THREAD_PRIORITY;
        let mut low_priority = false;
        loop {
            let mut work = self.poll();

            if lower_priority_for_concurrent_work {
                let in_concurrent = self.scheduler().in_concurrent();
                if in_concurrent && !low_priority {
                    let _ = thread_priority::set_current_thread_priority(ThreadPriority::Min);
                    low_priority = true;
                }
                if !in_concurrent && low_priority {
                    let _ = thread_priority::set_current_thread_priority(ThreadPriority::Max);
                    low_priority = false;
                }
            }

            debug_assert!(!self.shared.is_parked());
            if work.should_defer() {
                mmtk.scheduler.postpone_dyn(work);
                continue;
            }
            if let Some(stage) = work.should_move_to_stw() {
                if !self.scheduler.work_buckets[stage].is_activated() {
                    self.scheduler.work_buckets[stage].add_dyn(work);
                    continue;
                }
            }
            work.do_work_with_stat(self, mmtk);
        }
    }

    #[inline]
    pub fn is_concurrent_worker(&self) -> bool {
        self.ordinal
            < usize::max(
                1,
                (self.scheduler().num_workers() * *crate::args::CONCURRENT_GC_THREADS_RATIO / 100)
                    as usize,
            )
    }
}

#[thread_local]
pub static IS_WORKER: AtomicBool = AtomicBool::new(false);

#[thread_local]
pub static WORKER_ID: AtomicUsize = AtomicUsize::new(0);

pub struct WorkerGroup<VM: VMBinding> {
    pub workers_shared: Vec<Arc<GCWorkerShared<VM>>>,
    pub stealers: Vec<(usize, Stealer<Box<dyn GCWork<VM>>>)>,
    parked_workers: AtomicUsize,
}

impl<VM: VMBinding> WorkerGroup<VM> {
    pub fn new(
        mmtk: &'static MMTK<VM>,
        workers: usize,
        scheduler: Arc<GCWorkScheduler<VM>>,
        sender: Sender<CoordinatorMessage<VM>>,
    ) -> (Arc<Self>, Box<dyn FnOnce(VMThread)>) {
        let mut workers_shared = Vec::new();
        let mut workers_to_spawn = Vec::new();

        for ordinal in 0..workers {
            let worker = Box::new(GCWorker::new(
                mmtk,
                ordinal,
                scheduler.clone(),
                false,
                sender.clone(),
            ));
            let worker_shared = worker.shared.clone();
            workers_shared.push(worker_shared);
            workers_to_spawn.push(worker);
        }

        // NOTE: We cannot call spawn_gc_thread here,
        // because the worker will access `Scheduler::worker_group` immediately after started,
        // but that field will not be assigned to before this function returns.
        // Therefore we defer the spawning operation later.
        let deferred_spawn = Box::new(move |tls| {
            for worker in workers_to_spawn.drain(..) {
                VM::VMCollection::spawn_gc_thread(tls, GCThreadContext::<VM>::Worker(worker));
            }
        });

        let stealers = workers_shared
            .iter()
            .zip(0..workers)
            .map(|(w, ordinal)| (ordinal, w.local_work_buffer.stealer()))
            .collect();

        (
            Arc::new(Self {
                workers_shared,
                stealers,
                parked_workers: Default::default(),
            }),
            deferred_spawn,
        )
    }

    #[inline(always)]
    pub fn worker_count(&self) -> usize {
        self.workers_shared.len()
    }

    #[inline(always)]
    pub fn inc_parked_workers(&self) -> bool {
        let old = self.parked_workers.fetch_add(1, Ordering::SeqCst);
        old + 1 == self.worker_count()
    }

    #[inline(always)]
    pub fn dec_parked_workers(&self) {
        self.parked_workers.fetch_sub(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn parked_workers(&self) -> usize {
        self.parked_workers.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn all_parked(&self) -> bool {
        self.parked_workers() == self.worker_count()
    }
}
