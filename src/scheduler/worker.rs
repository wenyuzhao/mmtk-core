use super::stat::WorkerLocalStat;
use super::work_bucket::*;
use super::*;
use crate::mmtk::MMTK;
#[cfg(feature = "utilization")]
use crate::plan::immix::Pause;
#[cfg(feature = "utilization")]
use crate::plan::lxr::LXR;
use crate::util::copy::GCWorkerCopyContext;
use crate::util::{opaque_pointer::*, ObjectReference};
use crate::vm::{Collection, GCThreadContext, VMBinding};
use atomic::Atomic;
use atomic_refcell::{AtomicRef, AtomicRefCell, AtomicRefMut};
use crossbeam::deque::{self, Stealer};
use crossbeam::queue::ArrayQueue;
#[cfg(feature = "count_live_bytes_in_gc")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
#[cfg(feature = "utilization")]
use std::time::Instant;

#[cfg(feature = "fifo")]
pub type ItemWorker<T> = crate::util::deque::fifo::Worker<T>;
#[cfg(feature = "fifo")]
pub type ItemStealer<T> = crate::util::deque::fifo::Stealer<T>;

#[cfg(not(feature = "fifo"))]
pub type ItemWorker<T> = crate::util::deque::lifo::Worker<T>;
#[cfg(not(feature = "fifo"))]
pub type ItemStealer<T> = crate::util::deque::lifo::Stealer<T>;

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

/// The struct has one instance per worker, but is shared between workers via the scheduler
/// instance.  This structure is used for communication between workers, e.g. adding designated
/// work packets, stealing work packets from other workers, and collecting per-worker statistics.
pub struct GCWorkerShared<VM: VMBinding> {
    /// Worker-local statistics data.
    stat: AtomicRefCell<WorkerLocalStat<VM>>,
    /// Accumulated bytes for live objects in this GC. When each worker scans
    /// objects, we increase the live bytes. We get this value from each worker
    /// at the end of a GC, and reset this counter.
    #[cfg(feature = "count_live_bytes_in_gc")]
    live_bytes: AtomicUsize,
    /// A queue of GCWork that can only be processed by the owned thread.
    pub designated_work: ArrayQueue<(BucketId, Box<dyn GCWork>)>,
    /// Handle for stealing packets from the current worker
    pub stealer: Option<Stealer<(BucketId, Box<dyn GCWork>)>>,
    pub deque_stealer: ItemStealer<VM::VMSlot>,
    pub obj_deque_stealer: ItemStealer<ObjectReference>,
    pub satb_deque_stealer: ItemStealer<ObjectReference>,
    pub ordinal: ThreadId,
}

impl<VM: VMBinding> GCWorkerShared<VM> {
    pub fn new(
        ordinal: ThreadId,
        stealer: Option<Stealer<(BucketId, Box<dyn GCWork>)>>,
        ds: ItemStealer<VM::VMSlot>,
        ds2: ItemStealer<ObjectReference>,
        ds3: ItemStealer<ObjectReference>,
    ) -> Self {
        Self {
            ordinal,
            stat: Default::default(),
            #[cfg(feature = "count_live_bytes_in_gc")]
            live_bytes: AtomicUsize::new(0),
            designated_work: ArrayQueue::new(16),
            stealer,
            deque_stealer: ds,
            obj_deque_stealer: ds2,
            satb_deque_stealer: ds3,
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
pub struct GCWorker<VM: VMBinding> {
    /// The VM-specific thread-local state of the GC thread.
    pub tls: VMWorkerThread,
    /// The ordinal of the worker, numbered from 0 to the number of workers minus one.
    pub ordinal: ThreadId,
    /// The reference to the scheduler.
    scheduler: Arc<GCWorkScheduler<VM>>,
    /// The copy context, used to implement copying GC.
    copy: GCWorkerCopyContext<VM>,
    /// The reference to the MMTk instance.
    pub mmtk: &'static MMTK<VM>,
    /// Reference to the shared part of the GC worker.  It is used for synchronization.
    pub shared: Arc<GCWorkerShared<VM>>,
    /// Local work packet queue.
    pub local_work_buffer: deque::Worker<(BucketId, Box<dyn GCWork>)>,
    pub deque: ItemWorker<VM::VMSlot>,
    pub obj_deque: ItemWorker<ObjectReference>,
    pub satb_deque: ItemWorker<ObjectReference>,
    pub hash_seed: AtomicUsize,
    pub cache: Option<(BucketId, Box<dyn GCWork>)>,
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

/// A special error type that indicate a worker should exit.
/// This may happen if the VM needs to fork and asks workers to exit.
#[derive(Debug)]
pub(crate) struct WorkerShouldExit;

/// The result type of `GCWorker::pool`.
/// Too many functions return `Option<Box<dyn GCWork<VM>>>`.  In most cases, when `None` is
/// returned, the caller should try getting work packets from another place.  To avoid confusion,
/// we use `Err(WorkerShouldExit)` to clearly indicate that the worker should exit immediately.
pub(crate) type PollResult = Result<(BucketId, Box<dyn GCWork>), WorkerShouldExit>;

impl<VM: VMBinding> GCWorker<VM> {
    pub(crate) fn new(
        mmtk: &'static MMTK<VM>,
        ordinal: ThreadId,
        scheduler: Arc<GCWorkScheduler<VM>>,
        shared: Arc<GCWorkerShared<VM>>,
        local_work_buffer: deque::Worker<(BucketId, Box<dyn GCWork>)>,
        deque: ItemWorker<VM::VMSlot>,
        obj_deque: ItemWorker<ObjectReference>,
        satb_deque: ItemWorker<ObjectReference>,
    ) -> Self {
        // use rand::prelude::*;
        // let mut rng = thread_rng();

        Self {
            tls: VMWorkerThread(VMThread::UNINITIALIZED),
            ordinal,
            // We will set this later
            copy: GCWorkerCopyContext::new_non_copy(),
            scheduler,
            mmtk,
            shared,
            local_work_buffer,
            deque,
            obj_deque,
            satb_deque,
            hash_seed: AtomicUsize::new(17),
            cache: None,
            // hash_seed: rng.gen_range(0..102400),
        }
    }

    pub fn mmtk() -> &'static MMTK<VM> {
        Self::current().mmtk
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
    // pub fn add_work_prioritized(&mut self, bucket: WorkBucketStage, work: impl GCWork) {
    //     if !self.scheduler().work_buckets[bucket].is_activated()
    //         || self.local_work_buffer.len() >= Self::LOCALLY_CACHED_WORK_PACKETS
    //     {
    //         self.scheduler.work_buckets[bucket].add_prioritized(Box::new(work));
    //         return;
    //     }
    //     self.local_work_buffer.push(Box::new(work));
    // }

    // pub fn add_boxed_work(&mut self, bucket: WorkBucketStage, work: Box<dyn GCWork>) {
    //     if !self.scheduler().work_buckets[bucket].is_activated()
    //         || self.local_work_buffer.len() >= Self::LOCALLY_CACHED_WORK_PACKETS
    //     {
    //         self.scheduler.work_buckets[bucket].add_boxed(work);
    //         return;
    //     }
    //     self.local_work_buffer.push(work);
    // }

    // /// Add a work packet to the work queue.
    // /// If the bucket is activated, the packet will be pushed to the local queue, otherwise it will be
    // /// pushed to the global bucket.
    // pub fn add_work(&mut self, bucket: WorkBucketStage, work: impl GCWork) {
    //     if !self.scheduler().work_buckets[bucket].is_activated()
    //         || self.local_work_buffer.len() >= Self::LOCALLY_CACHED_WORK_PACKETS
    //     {
    //         self.scheduler.work_buckets[bucket].add(work);
    //         return;
    //     }
    //     self.local_work_buffer.push(Box::new(work));
    // }

    pub fn add_local_packet(
        &mut self,
        bucket: BucketId,
        work: Box<dyn GCWork>,
    ) -> Result<(), Box<dyn GCWork>> {
        if self.local_work_buffer.len() >= Self::LOCALLY_CACHED_WORK_PACKETS {
            return Err(work);
        }
        self.local_work_buffer.push((bucket, work));
        Ok(())
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
    fn poll(&mut self) -> PollResult {
        if let Some(work) = self.cache.take() {
            return Ok(work);
        }
        if let Some(work) = self.shared.designated_work.pop() {
            return Ok(work);
        }

        if let Some(work) = self.local_work_buffer.pop() {
            return Ok(work);
        }

        self.scheduler().poll(self)
    }

    /// Entry point of the worker thread.
    ///
    /// This function will resolve thread affinity, if it has been specified by the user.
    ///
    /// Each worker will keep polling and executing work packets in a loop.  It runs until the
    /// worker is requested to exit.  Currently a worker may exit after
    /// [`crate::mmtk::MMTK::prepare_to_fork`] is called.
    ///
    /// Arguments:
    /// * `tls`: The VM-specific thread-local storage for this GC worker thread.
    /// * `mmtk`: A reference to an MMTk instance.
    pub fn run(mut self: Box<Self>, tls: VMWorkerThread, mmtk: &'static MMTK<VM>) {
        #[cfg(feature = "tracing")]
        probe!(mmtk, gcworker_run);
        debug!(
            "Worker started. ordinal: {}, {}",
            self.ordinal,
            crate::util::rust_util::debug_process_thread_id(),
        );
        WORKER_ORDINAL.with(|x| x.store(self.ordinal, Ordering::SeqCst));
        let worker = (&mut *self as &mut Self) as *mut Self;
        _WORKER.with(|x| {
            x.store(
                (&mut *self as &mut Self) as *mut Self as usize,
                Ordering::SeqCst,
            )
        });
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
            let Ok((bucket, mut work)) = self.poll() else {
                // The worker is asked to exit.  Break from the loop.
                break;
            };
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
            #[cfg(feature = "utilization")]
            let t = Instant::now();
            #[cfg(feature = "utilization")]
            let record = crate::inside_harness() && !self.scheduler.in_concurrent();
            work.do_work_with_stat();
            std::mem::drop(work);
            #[cfg(feature = "utilization")]
            if record && !self.scheduler.in_concurrent() {
                let us = t.elapsed().as_micros();
                let lxr = self.mmtk.get_plan().downcast_ref::<LXR<VM>>();
                let pause = lxr.map(|x| x.current_pause()).flatten();
                use BucketId::*;
                // Transitive closure
                if BucketId::Closure.is_open() && !BucketId::WeakRefClosure.is_open() {
                    if let Some(start) = super::TRACE_START.elapsed_us_opt() {
                        let us = us.min(start);
                        super::TOTAL_TRACE_BUSY_TIME_US.fetch_add(us as usize, Ordering::SeqCst);
                    }
                }
                // RC increments
                if pause == Some(Pause::RefCount) && Incs.is_open() && !Release.is_open() {
                    if let Some(start) = super::INCS_START.elapsed_us_opt() {
                        let us = us.min(start);
                        super::TOTAL_INC_BUSY_TIME_US.fetch_add(us as usize, Ordering::SeqCst);
                    }
                }
                if pause != Some(Pause::RefCount) && Incs.is_open() && !Prepare.is_open() {
                    if let Some(start) = super::INCS_START.elapsed_us_opt() {
                        let us = us.min(start);
                        super::TOTAL_INC_BUSY_TIME_US.fetch_add(us as usize, Ordering::SeqCst);
                    }
                }
                // Whole GC except Release bucket
                if !Release.is_open() && (lxr.is_none() || (!Decs.is_open() && !Finish.is_open())) {
                    super::TOTAL_BUSY_TIME_US.fetch_add(us as usize, Ordering::SeqCst);
                }
            }
            if bucket.get_bucket().dec() {
                self.mmtk.scheduler.notify_bucket_empty(Some(bucket));
            }
            flush_logs!();
        }
        debug!(
            "Worker exiting. ordinal: {}, {}",
            self.ordinal,
            crate::util::rust_util::debug_process_thread_id(),
        );
        #[cfg(feature = "tracing")]
        probe!(mmtk, gcworker_exit);

        mmtk.scheduler.surrender_gc_worker(self);
    }

    pub fn steal_best_of_2<T>(
        &self,
        deque: &ItemWorker<T>,
        workers: &[Arc<GCWorkerShared<VM>>],
        stealer: impl Fn(&GCWorkerShared<VM>) -> &ItemStealer<T>,
    ) -> Option<T> {
        const BULK: bool = cfg!(feature = "steal_bulk");
        let n = workers.len();
        if n > 2 {
            let (k1, k2) =
                GCWorkScheduler::<VM>::get_random_steal_index(self.ordinal, &self.hash_seed, n);
            let stealer1 = stealer(&workers[k1]);
            let stealer2 = stealer(&workers[k2]);
            let sz1 = stealer1.size();
            let sz2 = stealer2.size();
            if sz1 < sz2 {
                return stealer2
                    .steal_and_pop(&deque, |n| if BULK { n / 2 } else { 1 })
                    .ok()
                    .map(|x| x.0);
            } else {
                return stealer1
                    .steal_and_pop(&deque, |n| if BULK { n / 2 } else { 1 })
                    .ok()
                    .map(|x| x.0);
            }
        } else if n == 2 {
            let k = (self.ordinal + 1) % 2;
            let stealer = stealer(&workers[k]);
            return stealer
                .steal_and_pop(&deque, |n| if BULK { n / 2 } else { 1 })
                .ok()
                .map(|x| x.0);
        } else {
            return None;
        }
    }
}

/// Stateful part of [`WorkerGroup`].
enum WorkerCreationState<VM: VMBinding> {
    /// The initial state.  `GCWorker` structs have not been created and GC worker threads have not
    /// been spawn.
    Initial {
        /// The local work queues for to-be-created workers.
        local_work_queues: Vec<deque::Worker<(BucketId, Box<dyn GCWork>)>>,
        local_item_queues: Vec<ItemWorker<VM::VMSlot>>,
        local_obj_queues: Vec<ItemWorker<ObjectReference>>,
        local_satb_queues: Vec<ItemWorker<ObjectReference>>,
    },
    /// All worker threads are spawn and running.  `GCWorker` structs have been transferred to
    /// worker threads.
    Spawned,
    /// Worker threads are stopping, or have already stopped, for forking. Instances of `GCWorker`
    /// structs are collected here to be reused when GC workers are respawn.
    Surrendered {
        /// `GCWorker` instances not currently owned by active GC worker threads.  Once GC workers
        /// are respawn, they will take ownership of these `GCWorker` instances.
        // Note: Clippy warns about `Vec<Box<T>>` because `Vec<T>` is already in the heap.
        // However, the purpose of this `Vec` is allowing GC worker threads to give their
        // `Box<GCWorker<VM>>` instances back to this pool.  Therefore, the `Box` is necessary.
        #[allow(clippy::vec_box)]
        workers: Vec<Box<GCWorker<VM>>>,
    },
}

/// A worker group to manage all the GC workers.
pub(crate) struct WorkerGroup<VM: VMBinding> {
    /// Shared worker data
    pub workers_shared: Vec<Arc<GCWorkerShared<VM>>>,
    /// The stateful part.  `None` means state transition is underway.
    state: Mutex<Option<WorkerCreationState<VM>>>,
}

/// We have to persuade Rust that `WorkerGroup` is safe to share because the compiler thinks one
/// worker can refer to another worker via the path "worker -> scheduler -> worker_group ->
/// `Surrendered::workers` -> worker" which is cyclic reference and unsafe.
unsafe impl<VM: VMBinding> Sync for WorkerGroup<VM> {}

impl<VM: VMBinding> WorkerGroup<VM> {
    /// Create a WorkerGroup
    pub fn new(num_workers: usize) -> Arc<Self> {
        let local_work_queues = (0..num_workers)
            .map(|_| deque::Worker::new_fifo())
            .collect::<Vec<_>>();

        let local_item_queues = (0..num_workers)
            .map(|_| ItemWorker::new(64))
            .collect::<Vec<_>>();

        let local_obj_queues = (0..num_workers)
            .map(|_| ItemWorker::new(64))
            .collect::<Vec<_>>();

        let local_satb_queues = (0..num_workers)
            .map(|_| ItemWorker::new(64))
            .collect::<Vec<_>>();

        let workers_shared = (0..num_workers)
            .map(|i| {
                Arc::new(GCWorkerShared::<VM>::new(
                    i,
                    Some(local_work_queues[i].stealer()),
                    local_item_queues[i].stealer(),
                    local_obj_queues[i].stealer(),
                    local_satb_queues[i].stealer(),
                ))
            })
            .collect::<Vec<_>>();

        Arc::new(Self {
            workers_shared,
            state: Mutex::new(Some(WorkerCreationState::Initial {
                local_work_queues,
                local_item_queues,
                local_obj_queues,
                local_satb_queues,
            })),
        })
    }

    /// Spawn GC worker threads for the first time.
    pub fn initial_spawn(&self, tls: VMThread, mmtk: &'static MMTK<VM>) {
        let mut state = self.state.lock().unwrap();

        let WorkerCreationState::Initial {
            local_work_queues,
            local_item_queues,
            local_obj_queues,
            local_satb_queues,
        } = state.take().unwrap()
        else {
            panic!("GCWorker structs have already been created");
        };

        let workers = self.create_workers(
            local_work_queues,
            local_item_queues,
            local_obj_queues,
            local_satb_queues,
            mmtk,
        );
        self.spawn(workers, tls);

        *state = Some(WorkerCreationState::Spawned);
    }

    /// Respawn GC threads after stopping for forking.
    pub fn respawn(&self, tls: VMThread) {
        let mut state = self.state.lock().unwrap();

        let WorkerCreationState::Surrendered { workers } = state.take().unwrap() else {
            panic!("GCWorker structs have not been created, yet.");
        };

        self.spawn(workers, tls);

        *state = Some(WorkerCreationState::Spawned)
    }

    /// Create `GCWorker` instances.
    #[allow(clippy::vec_box)] // See `WorkerCreationState::Surrendered`.
    fn create_workers(
        &self,
        local_work_queues: Vec<deque::Worker<(BucketId, Box<dyn GCWork>)>>,
        local_item_queues: Vec<ItemWorker<VM::VMSlot>>,
        local_obj_queues: Vec<ItemWorker<ObjectReference>>,
        local_satb_queues: Vec<ItemWorker<ObjectReference>>,
        mmtk: &'static MMTK<VM>,
    ) -> Vec<Box<GCWorker<VM>>> {
        debug!("Creating GCWorker instances...");

        assert_eq!(self.workers_shared.len(), local_work_queues.len());

        // Each `GCWorker` instance corresponds to a `GCWorkerShared` at the same index.
        let workers = (local_work_queues.into_iter())
            .zip(self.workers_shared.iter())
            .zip(local_item_queues.into_iter())
            .zip(local_obj_queues.into_iter())
            .zip(local_satb_queues.into_iter())
            .enumerate()
            .map(
                |(ordinal, ((((queue, shared), item_queue), obj_queue), satb_queue))| {
                    Box::new(GCWorker::new(
                        mmtk,
                        ordinal,
                        mmtk.scheduler.clone(),
                        shared.clone(),
                        queue,
                        item_queue,
                        obj_queue,
                        satb_queue,
                    ))
                },
            )
            .collect::<Vec<_>>();

        debug!("Created {} GCWorker instances.", workers.len());
        workers
    }

    /// Spawn all the worker threads
    #[allow(clippy::vec_box)] // See `WorkerCreationState::Surrendered`.
    fn spawn(&self, workers: Vec<Box<GCWorker<VM>>>, tls: VMThread) {
        debug!(
            "Spawning GC workers.  {}",
            crate::util::rust_util::debug_process_thread_id(),
        );

        // We transfer the ownership of each `GCWorker` instance to a GC thread.
        for worker in workers {
            VM::VMCollection::spawn_gc_thread(tls, GCThreadContext::<VM>::Worker(worker));
        }

        debug!(
            "Spawned {} worker threads.  {}",
            self.worker_count(),
            crate::util::rust_util::debug_process_thread_id(),
        );
    }

    /// Prepare the buffer for workers to surrender their `GCWorker` structs.
    pub fn prepare_surrender_buffer(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(matches!(*state, Some(WorkerCreationState::Spawned)));

        *state = Some(WorkerCreationState::Surrendered {
            workers: Vec::with_capacity(self.worker_count()),
        })
    }

    /// Return the `GCWorker` struct to the worker group.
    /// This function returns `true` if all workers returned their `GCWorker` structs.
    pub fn surrender_gc_worker(&self, worker: Box<GCWorker<VM>>) -> bool {
        let mut state = self.state.lock().unwrap();
        let WorkerCreationState::Surrendered { ref mut workers } = state.as_mut().unwrap() else {
            panic!("GCWorker structs have not been created, yet.");
        };
        let ordinal = worker.ordinal;
        workers.push(worker);
        trace!(
            "Worker {} surrendered. ({}/{})",
            ordinal,
            workers.len(),
            self.worker_count()
        );
        workers.len() == self.worker_count()
    }

    /// Get the number of workers in the group
    pub fn worker_count(&self) -> usize {
        self.workers_shared.len()
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
