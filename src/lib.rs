// TODO: We should fix missing docs for public items and turn this on (Issue #309).
// #![deny(missing_docs)]

// Allow this for now. Clippy suggests we should use Sft, Mmtk, rather than SFT and MMTK.
// According to its documentation (https://rust-lang.github.io/rust-clippy/master/index.html#upper_case_acronyms),
// with upper-case-acronyms-aggressive turned on, it should also warn us about SFTMap, VMBinding, GCWorker.
// However, it seems clippy does not catch all these patterns at the moment. So it would be hard for us to
// find all the patterns and consistently change all of them. I think it would be a better idea to just allow this.
// We may reconsider this in the future. Plus, using upper case letters for acronyms does not sound a big issue
// to me - considering it will break our API and all the efforts for all the developers to make the change, it may
// not worth it.
#![allow(clippy::upper_case_acronyms)]

//! Memory Management ToolKit (MMTk) is a portable and high performance memory manager
//! that includes various garbage collection algorithms and provides clean and efficient
//! interfaces to cooperate with language implementations. MMTk features highly modular
//! and highly reusable designs. It includes components such as allocators, spaces and
//! work packets that GC implementers can choose from to compose their own GC plan easily.
//!
//! Logically, this crate includes these major parts:
//! * GC components:
//!   * [Allocators](util/alloc/allocator/trait.Allocator.html): handlers of allocation requests which allocate objects to the bound space.
//!   * [Policies](policy/space/trait.Space.html): definitions of semantics and behaviors for memory regions.
//!      Each space is an instance of a policy, and takes up a unique proportion of the heap.
//!   * [Work packets](scheduler/work/trait.GCWork.html): units of GC work scheduled by the MMTk's scheduler.
//! * [GC plans](plan/global/trait.Plan.html): GC algorithms composed from components.
//! * [Heap implementations](util/heap/index.html): the underlying implementations of memory resources that support spaces.
//! * [Scheduler](scheduler/scheduler/struct.GCWorkScheduler.html): the MMTk scheduler to allow flexible and parallel execution of GC work.
//! * Interfaces: bi-directional interfaces between MMTk and language implementations
//!   i.e. [the memory manager API](memory_manager/index.html) that allows a language's memory manager to use MMTk
//!   and [the VMBinding trait](vm/trait.VMBinding.html) that allows MMTk to call the language implementation.

extern crate libc;
extern crate strum_macros;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[cfg(target = "x86_64-unknown-linux-gnu")]
extern crate atomic;
extern crate atomic_traits;
extern crate crossbeam;
extern crate num_cpus;
#[macro_use]
extern crate downcast_rs;

mod mmtk;
pub use mmtk::MMTKBuilder;
use std::{
    fs::File,
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    time::SystemTime,
};

use atomic::{Atomic, Ordering};
use crossbeam::queue::SegQueue;
pub(crate) use mmtk::MMAPPER;
pub use mmtk::MMTK;
use plan::immix::Pause;
use scheduler::WorkBucketStage;
use spin::{Lazy, Mutex};
type RwLock<T> = spin::rwlock::RwLock<T>;

#[macro_use]
mod policy;

pub mod args;
pub mod build_info;
pub mod memory_manager;
pub mod plan;
pub mod scheduler;
pub mod util;
pub mod vm;

pub use crate::plan::{
    AllocationSemantics, BarrierSelector, Mutator, MutatorContext, ObjectQueue, Plan,
};
pub use crate::policy::copy_context::PolicyCopyContext;

static NUM_CONCURRENT_TRACING_PACKETS: AtomicUsize = AtomicUsize::new(0);

pub struct LazySweepingJobsCounter {
    decs_counter: Option<Arc<AtomicUsize>>,
    counter: Arc<AtomicUsize>,
}
impl LazySweepingJobsCounter {
    #[inline(always)]
    pub fn new() -> Self {
        let lazy_sweeping_jobs = LAZY_SWEEPING_JOBS.read();
        let counter = lazy_sweeping_jobs.curr_counter.as_ref().unwrap();
        counter.fetch_add(1, Ordering::SeqCst);
        Self {
            decs_counter: None,
            counter: counter.clone(),
        }
    }

    #[inline(always)]
    pub fn new_decs() -> Self {
        let lazy_sweeping_jobs = LAZY_SWEEPING_JOBS.read();
        let decs_counter = lazy_sweeping_jobs.curr_decs_counter.as_ref().unwrap();
        decs_counter.fetch_add(1, Ordering::SeqCst);
        let counter = lazy_sweeping_jobs.curr_counter.as_ref().unwrap();
        counter.fetch_add(1, Ordering::SeqCst);
        Self {
            decs_counter: Some(decs_counter.clone()),
            counter: counter.clone(),
        }
    }

    #[inline(always)]
    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Self {
            decs_counter: None,
            counter: self.counter.clone(),
        }
    }

    #[inline(always)]
    pub fn clone_with_decs(&self) -> Self {
        self.decs_counter
            .as_ref()
            .unwrap()
            .fetch_add(1, Ordering::SeqCst);
        self.counter.fetch_add(1, Ordering::SeqCst);
        Self {
            decs_counter: self.decs_counter.clone(),
            counter: self.counter.clone(),
        }
    }
}

impl Drop for LazySweepingJobsCounter {
    #[inline(always)]
    fn drop(&mut self) {
        let lazy_sweeping_jobs = LAZY_SWEEPING_JOBS.read();
        if let Some(decs) = self.decs_counter.as_ref() {
            if decs.fetch_sub(1, Ordering::SeqCst) == 1 {
                let f = lazy_sweeping_jobs.end_of_decs.as_ref().unwrap();
                f(self.clone())
            }
        }
        if self.counter.fetch_sub(1, Ordering::SeqCst) == 1 {
            if let Some(f) = lazy_sweeping_jobs.end_of_lazy.as_ref() {
                f()
            }
        }
    }
}

pub struct LazySweepingJobs {
    prev_decs_counter: Option<Arc<AtomicUsize>>,
    curr_decs_counter: Option<Arc<AtomicUsize>>,
    prev_counter: Option<Arc<AtomicUsize>>,
    curr_counter: Option<Arc<AtomicUsize>>,
    pub end_of_decs: Option<Box<dyn Send + Sync + Fn(LazySweepingJobsCounter)>>,
    pub end_of_lazy: Option<Box<dyn Send + Sync + Fn()>>,
}

impl LazySweepingJobs {
    fn new() -> Self {
        Self {
            prev_decs_counter: None,
            curr_decs_counter: None,
            prev_counter: None,
            curr_counter: None,
            end_of_decs: None,
            end_of_lazy: None,
        }
    }

    #[inline(always)]
    pub fn all_finished() -> bool {
        LAZY_SWEEPING_JOBS
            .read()
            .prev_counter
            .as_ref()
            .map(|c| c.load(Ordering::SeqCst))
            .unwrap_or(0)
            == 0
    }

    pub fn swap(&mut self) {
        self.prev_decs_counter = self.curr_decs_counter.take();
        self.curr_decs_counter = Some(Arc::new(AtomicUsize::new(0)));
        self.prev_counter = self.curr_counter.take();
        self.curr_counter = Some(Arc::new(AtomicUsize::new(0)));
    }
}

static LAZY_SWEEPING_JOBS: Lazy<RwLock<LazySweepingJobs>> =
    Lazy::new(|| RwLock::new(LazySweepingJobs::new()));

#[inline(always)]
fn concurrent_marking_packets_drained() -> bool {
    crate::NUM_CONCURRENT_TRACING_PACKETS.load(Ordering::SeqCst) == 0
}

static DISABLE_LASY_DEC_FOR_CURRENT_GC: AtomicBool = AtomicBool::new(false);

#[inline(always)]
fn disable_lasy_dec_for_current_gc() -> bool {
    crate::DISABLE_LASY_DEC_FOR_CURRENT_GC.load(Ordering::SeqCst)
}

static GC_TRIGGER_TIME: Atomic<SystemTime> = Atomic::new(SystemTime::UNIX_EPOCH);
static GC_START_TIME: Atomic<SystemTime> = Atomic::new(SystemTime::UNIX_EPOCH);
static BOOT_TIME: spin::Lazy<SystemTime> = spin::Lazy::new(SystemTime::now);
static GC_EPOCH: AtomicUsize = AtomicUsize::new(0);
static RESERVED_PAGES_AT_GC_START: AtomicUsize = AtomicUsize::new(0);
static RESERVED_PAGES_AT_GC_END: AtomicUsize = AtomicUsize::new(0);
static INSIDE_HARNESS: AtomicBool = AtomicBool::new(false);
static SATB_START: Atomic<SystemTime> = Atomic::new(SystemTime::UNIX_EPOCH);
static PAUSE_CONCURRENT_MARKING: AtomicBool = AtomicBool::new(false);
static MOVE_CONCURRENT_MARKING_TO_STW: AtomicBool = AtomicBool::new(false);

#[inline(always)]
fn boot_time_secs() -> f64 {
    crate::BOOT_TIME.elapsed().unwrap().as_millis() as f64 / 1000f64
}

#[inline(always)]
fn gc_trigger_time() -> u128 {
    crate::GC_TRIGGER_TIME
        .load(Ordering::SeqCst)
        .elapsed()
        .unwrap()
        .as_nanos()
}

#[inline(always)]
#[allow(unused)]
fn inside_harness() -> bool {
    crate::INSIDE_HARNESS.load(Ordering::Relaxed)
}

struct Counters {
    pub rc: AtomicUsize,
    pub initial_mark: AtomicUsize,
    pub final_mark: AtomicUsize,
    pub cm_early_quit: AtomicUsize,
    pub full: AtomicUsize,
    pub emergency: AtomicUsize,
    pub yield_nanos: Atomic<u128>,
    pub roots_nanos: Atomic<u128>,
    pub satb_nanos: Atomic<u128>,
    pub total_used_pages: AtomicUsize,
    pub min_used_pages: AtomicUsize,
    pub max_used_pages: AtomicUsize,
    pub gc_with_unfinished_lazy_jobs: AtomicUsize,
    pub incs_triggerd: AtomicUsize,
    pub alloc_triggerd: AtomicUsize,
    pub survival_triggerd: AtomicUsize,
    pub overflow_triggerd: AtomicUsize,
    pub rc_during_satb: AtomicUsize,
}

macro_rules! counter_print_keys_and_values {
    ([$this: ident] $($k: literal: $v: expr,)*) => {
        pub fn print_keys(&$this) { $(print!("{}\t", $k);)* }
        pub fn print_values(&$this) { $(print!("{}\t", $v);)* }
    };
}

impl Counters {
    counter_print_keys_and_values! { [self]
        "gc.rc": self.rc.load(Ordering::SeqCst),
        "gc.initial_satb": self.initial_mark.load(Ordering::SeqCst),
        "gc.final_satb": self.final_mark.load(Ordering::SeqCst),
        "gc.full": self.full.load(Ordering::SeqCst),
        "gc.emergency": self.emergency.load(Ordering::SeqCst),
        "cm_early_quit": self.cm_early_quit.load(Ordering::SeqCst),
        "gc_with_unfinished_lazy_jobs": self.gc_with_unfinished_lazy_jobs.load(Ordering::SeqCst),
        "time.yield": self.yield_nanos.load(Ordering::SeqCst) as f64 / 1000000.0,
        "time.roots": self.roots_nanos.load(Ordering::SeqCst) as f64 / 1000000.0,
        "time.satb": self.satb_nanos.load(Ordering::SeqCst) as f64 / 1000000.0,
        "total_used_pages": self.total_used_pages.load(Ordering::SeqCst),
        "min_used_pages": self.min_used_pages.load(Ordering::SeqCst),
        "max_used_pages": self.max_used_pages.load(Ordering::SeqCst),
        "incs_triggerd": self.incs_triggerd.load(Ordering::SeqCst),
        "alloc_triggerd": self.alloc_triggerd.load(Ordering::SeqCst),
        "survival_triggerd": self.survival_triggerd.load(Ordering::SeqCst),
        "overflow_triggerd": self.overflow_triggerd.load(Ordering::SeqCst),
        "rc_during_satb": self.rc_during_satb.load(Ordering::SeqCst),
    }
}

const fn create_counters() -> Counters {
    let mut counters: Counters =
        unsafe { std::mem::transmute([0u8; std::mem::size_of::<Counters>()]) };
    counters.min_used_pages = AtomicUsize::new(usize::MAX);
    counters
}

fn reset_counters() {
    let mut new_counters = create_counters();
    let global = unsafe { &mut *(&COUNTERS as *const Counters as *mut Counters) };
    std::mem::swap(global, &mut new_counters);
}

fn stop_counters() {
    let retired_counters = unsafe { &mut RETIRED_COUNTERS };
    let global = unsafe { &mut *(&COUNTERS as *const Counters as *mut Counters) };
    std::mem::swap(global, retired_counters);
}

static mut RETIRED_COUNTERS: Counters = create_counters();
static COUNTERS: Counters = create_counters();

#[derive(Default)]
struct GCStat {
    pub rc_pauses: usize,
    pub alloc_objects: usize,
    pub alloc_volume: usize,
    pub alloc_los_objects: usize,
    pub alloc_los_volume: usize,
    pub promoted_objects: usize,
    pub promoted_volume: usize,
    pub promoted_copy_objects: usize,
    pub promoted_copy_volume: usize,
    pub promoted_los_objects: usize,
    pub promoted_los_volume: usize,
    pub mature_copy_objects: usize,
    pub mature_copy_volume: usize,
    // Dead mature objects
    pub dead_mature_objects: usize,
    pub dead_mature_volume: usize,
    pub dead_mature_los_objects: usize,
    pub dead_mature_los_volume: usize,
    // Dead mature objects (killed by RC)
    pub dead_mature_rc_objects: usize,
    pub dead_mature_rc_volume: usize,
    pub dead_mature_rc_los_objects: usize,
    pub dead_mature_rc_los_volume: usize,
    // Dead mature objects (killed by SATB)
    pub dead_mature_tracing_objects: usize,
    pub dead_mature_tracing_volume: usize,
    pub dead_mature_tracing_los_objects: usize,
    pub dead_mature_tracing_los_volume: usize,
    // Dead mature objects (with stuck RC)
    pub dead_mature_tracing_stuck_objects: usize,
    pub dead_mature_tracing_stuck_volume: usize,
    pub dead_mature_tracing_stuck_los_objects: usize,
    pub dead_mature_tracing_stuck_los_volume: usize,
    // Reclaimed blocks
    pub reclaimed_blocks_nursery: usize,
    pub reclaimed_blocks_mature: usize,
    // Inc counters
    pub inc_objects: usize,
    pub inc_volume: usize,
}

macro_rules! print_keys_and_values {
    ($($n: ident,)*) => {
        #[allow(unused)]
        pub fn print_keys(&self) {
            $(print!("{}\t", stringify!($n));)*
        }
        #[allow(unused)]
        pub fn print_values(&self) {
            $(print!("{}\t", self.$n);)*
        }
        #[allow(unused)]
        pub fn pretty_print(&self) {
            $(println!(" - {} {}", stringify!($n), self.$n);)*
        }
    };
}

impl GCStat {
    print_keys_and_values![
        rc_pauses,
        alloc_objects,
        alloc_volume,
        alloc_los_objects,
        alloc_los_volume,
        promoted_objects,
        promoted_volume,
        promoted_copy_objects,
        promoted_copy_volume,
        promoted_los_objects,
        promoted_los_volume,
        mature_copy_objects,
        mature_copy_volume,
        dead_mature_objects,
        dead_mature_volume,
        dead_mature_los_objects,
        dead_mature_los_volume,
        dead_mature_rc_objects,
        dead_mature_rc_volume,
        dead_mature_rc_los_objects,
        dead_mature_rc_los_volume,
        dead_mature_tracing_objects,
        dead_mature_tracing_volume,
        dead_mature_tracing_los_objects,
        dead_mature_tracing_los_volume,
        dead_mature_tracing_stuck_objects,
        dead_mature_tracing_stuck_volume,
        dead_mature_tracing_stuck_los_objects,
        dead_mature_tracing_stuck_los_volume,
        reclaimed_blocks_nursery,
        reclaimed_blocks_mature,
        inc_objects,
        inc_volume,
    ];
}

#[allow(unused)]
static STAT: Mutex<GCStat> = Mutex::new(GCStat {
    rc_pauses: 0,
    alloc_objects: 0,
    alloc_volume: 0,
    alloc_los_objects: 0,
    alloc_los_volume: 0,
    promoted_objects: 0,
    promoted_volume: 0,
    promoted_copy_objects: 0,
    promoted_copy_volume: 0,
    promoted_los_objects: 0,
    promoted_los_volume: 0,
    mature_copy_objects: 0,
    mature_copy_volume: 0,
    dead_mature_objects: 0,
    dead_mature_volume: 0,
    dead_mature_los_objects: 0,
    dead_mature_los_volume: 0,
    dead_mature_rc_objects: 0,
    dead_mature_rc_volume: 0,
    dead_mature_rc_los_objects: 0,
    dead_mature_rc_los_volume: 0,
    dead_mature_tracing_objects: 0,
    dead_mature_tracing_volume: 0,
    dead_mature_tracing_los_objects: 0,
    dead_mature_tracing_los_volume: 0,
    dead_mature_tracing_stuck_objects: 0,
    dead_mature_tracing_stuck_volume: 0,
    dead_mature_tracing_stuck_los_objects: 0,
    dead_mature_tracing_stuck_los_volume: 0,
    reclaimed_blocks_nursery: 0,
    reclaimed_blocks_mature: 0,
    inc_objects: 0,
    inc_volume: 0,
});

#[inline(always)]
fn stat(f: impl Fn(&mut GCStat)) {
    if !cfg!(feature = "instrumentation") {
        return;
    }
    if !INSIDE_HARNESS.load(Ordering::SeqCst) {
        return;
    }
    f(&mut STAT.lock())
}

#[inline(always)]
fn should_record_copy_bytes() -> bool {
    false
}

static mut SLOPPY_COPY_BYTES: usize = 0;

#[inline(always)]
fn add_copy_bytes(bytes: usize) {
    if should_record_copy_bytes() {
        COPY_BYTES.push(bytes);
        unsafe {
            SLOPPY_COPY_BYTES = 0;
        }
    }
}

#[inline(always)]
fn add_incs(incs: usize) {
    if should_record_copy_bytes() {
        INCS.push(incs);
    }
}

static COPY_BYTES: SegQueue<usize> = SegQueue::new();
static INCS: SegQueue<usize> = SegQueue::new();

#[inline(always)]
fn should_record_pause_time() -> bool {
    cfg!(feature = "pause_time") && INSIDE_HARNESS.load(Ordering::SeqCst)
}

#[inline(always)]
fn add_bucket_time(_stage: WorkBucketStage, _nanos: u128) {}

static SRV: SegQueue<(f64, f64)> = SegQueue::new();

#[inline(always)]
fn add_survival_ratio(srv: f64, predict: f64) {
    if cfg!(feature = "survival_ratio") && INSIDE_HARNESS.load(Ordering::SeqCst) {
        SRV.push((srv, predict));
    }
}

fn output_survival_ratios() {
    let headers = ["srv", "predict"];
    let mut s = headers.join(",") + "\n";
    while let Some((a, b)) = SRV.pop() {
        s += &[format!("{:.3}", a), format!("{:.3}", b)].join(",");
        s += "\n";
    }
    let mut file = File::create("scratch/srv.csv").unwrap();
    file.write_all(s.as_bytes()).unwrap();
}

static PAUSE_TIMES: SegQueue<u128> = SegQueue::new();

#[inline(always)]
fn add_pause_time(_pause: Pause, nanos: u128) {
    if should_record_pause_time() {
        PAUSE_TIMES.push(nanos);
    }
}

fn output_pause_time() {
    let mut s = "".to_owned();
    while let Some(record) = PAUSE_TIMES.pop() {
        s += &format!("{}\n", record);
    }
    let mut file = File::create("scratch/pauses.csv").unwrap();
    file.write_all(s.as_bytes()).unwrap();
}

static NO_EVAC: AtomicBool = AtomicBool::new(false);
static REMSET_RECORDING: AtomicBool = AtomicBool::new(false);

#[inline(always)]
pub fn gc_worker_id() -> Option<usize> {
    crate::scheduler::current_worker_ordinal()
}

#[inline(always)]
pub(crate) fn args() -> &'static crate::args::RuntimeArgs {
    crate::args::RuntimeArgs::get()
}
