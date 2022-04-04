use super::stat::WorkerLocalStat;
use super::work_bucket::*;
use super::*;
use crate::mmtk::MMTK;
use crate::util::opaque_pointer::*;
use crate::vm::{Collection, VMBinding};
use crossbeam_deque::{Stealer, Worker};
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Weak};

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

pub struct GCWorker<VM: VMBinding> {
    pub tls: VMWorkerThread,
    pub ordinal: usize,
    pub parked: AtomicBool,
    scheduler: Arc<GCWorkScheduler<VM>>,
    local: GCWorkerLocalPtr,
    pub local_work_bucket: WorkBucket<VM>,
    pub sender: Sender<CoordinatorMessage<VM>>,
    pub stat: WorkerLocalStat<VM>,
    mmtk: Option<&'static MMTK<VM>>,
    is_coordinator: bool,
    pub local_work_buffer: Worker<Box<dyn GCWork<VM>>>,
}

unsafe impl<VM: VMBinding> Sync for GCWorker<VM> {}
unsafe impl<VM: VMBinding> Send for GCWorker<VM> {}

impl<VM: VMBinding> GCWorker<VM> {
    pub fn new(
        ordinal: usize,
        scheduler: Weak<GCWorkScheduler<VM>>,
        is_coordinator: bool,
        sender: Sender<CoordinatorMessage<VM>>,
    ) -> Self {
        let scheduler = scheduler.upgrade().unwrap();
        Self {
            tls: VMWorkerThread(VMThread::UNINITIALIZED),
            ordinal,
            parked: AtomicBool::new(true),
            local: GCWorkerLocalPtr::UNINITIALIZED,
            local_work_bucket: WorkBucket::new(true, scheduler.worker_monitor.clone(), false),
            sender,
            scheduler,
            stat: Default::default(),
            mmtk: None,
            is_coordinator,
            local_work_buffer: Worker::new_fifo(),
        }
    }

    #[inline]
    pub fn add_work_prioritized(&mut self, bucket: WorkBucketStage, work: impl GCWork<VM>) {
        if !self.scheduler().work_buckets[bucket].is_activated()
            || !self.local_work_buffer.is_empty()
        {
            self.scheduler.work_buckets[bucket].add_prioritized(box work);
            return;
        }
        self.local_work_buffer.push(box work);
    }

    #[inline]
    pub fn add_work(&mut self, bucket: WorkBucketStage, work: impl GCWork<VM>) {
        if !self.scheduler().work_buckets[bucket].is_activated()
            || !self.local_work_buffer.is_empty()
        {
            self.scheduler.work_buckets[bucket].add(work);
            return;
        }
        self.local_work_buffer.push(box work);
    }

    pub fn is_parked(&self) -> bool {
        self.parked.load(Ordering::SeqCst)
    }

    pub fn is_coordinator(&self) -> bool {
        self.is_coordinator
    }

    pub fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    /// # Safety
    /// The user needs to guarantee that the type supplied here is the same type used to create this pointer.
    #[inline]
    pub unsafe fn local<W: 'static + GCWorkerLocal>(&mut self) -> &mut W {
        self.local.as_type::<W>()
    }

    pub fn set_local(&mut self, local: GCWorkerLocalPtr) {
        self.local = local;
    }

    pub fn init(&mut self, tls: VMWorkerThread) {
        self.tls = tls;
    }

    pub fn do_work(&'static mut self, mut work: impl GCWork<VM>) {
        work.do_work(self, self.mmtk.unwrap());
    }

    fn poll(&self) -> Box<dyn GCWork<VM>> {
        self.local_work_buffer
            .pop()
            .or_else(|| Some(self.scheduler().poll(self)))
            .unwrap()
    }

    pub fn run(&mut self, mmtk: &'static MMTK<VM>) {
        self.mmtk = Some(mmtk);
        self.parked.store(false, Ordering::SeqCst);
        IS_WORKER.store(true, Ordering::SeqCst);
        WORKER_ID.store(self.ordinal, Ordering::SeqCst);
        loop {
            let mut work = self.poll();
            debug_assert!(!self.is_parked());
            if work.should_defer() {
                mmtk.scheduler.postpone_dyn(work);
                continue;
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
    pub workers: Vec<GCWorker<VM>>,
    pub stealers: Vec<(usize, Stealer<Box<dyn GCWork<VM>>>)>,
    parked_workers: AtomicUsize,
}

impl<VM: VMBinding> WorkerGroup<VM> {
    pub fn new(
        workers: usize,
        scheduler: Weak<GCWorkScheduler<VM>>,
        sender: Sender<CoordinatorMessage<VM>>,
    ) -> Arc<Self> {
        let workers = (0..workers)
            .map(|i| GCWorker::new(i, scheduler.clone(), false, sender.clone()))
            .collect::<Vec<_>>();
        let stealers = workers
            .iter()
            .map(|w| (w.ordinal, w.local_work_buffer.stealer()))
            .collect();
        Arc::new(Self {
            workers,
            stealers,
            parked_workers: Default::default(),
        })
    }

    #[inline(always)]
    pub fn worker_count(&self) -> usize {
        self.workers.len()
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

    pub fn spawn_workers(&'static self, tls: VMThread, _mmtk: &'static MMTK<VM>) {
        for i in 0..self.worker_count() {
            let worker = &self.workers[i];
            VM::VMCollection::spawn_worker_thread(tls, Some(worker));
        }
    }
}
