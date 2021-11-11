#![allow(incomplete_features)]
#![feature(asm)]
#![feature(integer_atomics)]
#![feature(is_sorted)]
#![feature(drain_filter)]
#![feature(nll)]
#![feature(box_syntax)]
#![feature(maybe_uninit_extra)]
#![feature(get_mut_unchecked)]
#![feature(arbitrary_self_types)]
#![feature(associated_type_defaults)]
#![feature(specialization)]
#![feature(trait_alias)]
#![feature(const_panic)]
#![feature(step_trait)]
#![feature(once_cell)]
#![feature(const_generics_defaults)]
#![feature(const_trait_impl)]
#![feature(const_option)]
#![feature(const_fn_trait_bound)]
#![feature(core_intrinsics)]
#![feature(adt_const_params)]
#![feature(generic_const_exprs)]
#![feature(const_raw_ptr_deref)]
#![feature(const_mut_refs)]
#![feature(option_result_unwrap_unchecked)]
#![feature(hash_drain_filter)]
#![feature(const_for)]
#![feature(const_ptr_offset)]
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
//! * [Scheduler](scheduler/scheduler/struct.Scheduler.html): the MMTk scheduler to allow flexible and parallel execution of GC work.
//! * Interfaces: bi-directional interfaces between MMTk and language implementations
//!   i.e. [the memory manager API](memory_manager/index.html) that allows a language's memory manager to use MMTk
//!   and [the VMBinding trait](vm/trait.VMBinding.html) that allows MMTk to call the language implementation.

#[macro_use]
extern crate custom_derive;
#[macro_use]
extern crate enum_derive;

extern crate libc;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[cfg(target = "x86_64-unknown-linux-gnu")]
extern crate atomic;
extern crate atomic_traits;
extern crate crossbeam_deque;
extern crate num_cpus;
#[macro_use]
extern crate downcast_rs;

mod mmtk;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    time::SystemTime,
};

use crate::util::constants;
use atomic::{Atomic, Ordering};
pub(crate) use mmtk::MMAPPER;
pub use mmtk::MMTK;
pub(crate) use mmtk::VM_MAP;
use spin::Mutex;

#[macro_use]
mod policy;

pub mod args;
pub mod memory_manager;
pub mod plan;
pub mod scheduler;
pub mod util;
pub mod vm;

pub use crate::plan::{
    AllocationSemantics, BarrierSelector, CopyContext, Mutator, MutatorContext, Plan, TraceLocal,
    TransitiveClosure,
};

static IN_CONCURRENT_GC: AtomicBool = AtomicBool::new(false);
static NUM_CONCURRENT_TRACING_PACKETS: AtomicUsize = AtomicUsize::new(0);

pub struct LazySweepingJobsCounter {
    decs_counter: Option<Arc<AtomicUsize>>,
    counter: Arc<AtomicUsize>,
}
impl LazySweepingJobsCounter {
    #[inline(always)]
    pub fn new() -> Self {
        unsafe {
            let counter = LAZY_SWEEPING_JOBS.curr_counter.as_ref().unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            Self {
                decs_counter: None,
                counter: counter.clone(),
            }
        }
    }

    #[inline(always)]
    pub fn new_desc() -> Self {
        unsafe {
            let decs_counter = LAZY_SWEEPING_JOBS.curr_decs_counter.as_ref().unwrap();
            decs_counter.fetch_add(1, Ordering::SeqCst);
            let counter = LAZY_SWEEPING_JOBS.curr_counter.as_ref().unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            Self {
                decs_counter: Some(decs_counter.clone()),
                counter: counter.clone(),
            }
        }
    }

    #[inline(always)]
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
        if let Some(decs) = self.decs_counter.as_ref() {
            if decs.fetch_sub(1, Ordering::SeqCst) == 1 {
                unsafe {
                    let f = LAZY_SWEEPING_JOBS.end_of_decs.as_ref().unwrap();
                    f(self.clone())
                }
            }
        }
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct LazySweepingJobs {
    prev_decs_counter: Option<Arc<AtomicUsize>>,
    curr_decs_counter: Option<Arc<AtomicUsize>>,
    prev_counter: Option<Arc<AtomicUsize>>,
    curr_counter: Option<Arc<AtomicUsize>>,
    pub end_of_decs: Option<Box<dyn Fn(LazySweepingJobsCounter)>>,
}

impl LazySweepingJobs {
    const fn new() -> Self {
        Self {
            prev_decs_counter: None,
            curr_decs_counter: None,
            prev_counter: None,
            curr_counter: None,
            end_of_decs: None,
        }
    }

    #[inline(always)]
    pub fn all_finished() -> bool {
        unsafe {
            LAZY_SWEEPING_JOBS
                .prev_counter
                .as_ref()
                .map(|c| c.load(Ordering::SeqCst))
                .unwrap_or(0)
                == 0
        }
    }

    pub fn init(&mut self) {}

    pub fn swap(&mut self) {
        self.prev_decs_counter = self.curr_decs_counter.take();
        self.curr_decs_counter = Some(Arc::new(AtomicUsize::new(0)));
        self.prev_counter = self.curr_counter.take();
        self.curr_counter = Some(Arc::new(AtomicUsize::new(0)));
    }
}

static mut LAZY_SWEEPING_JOBS: LazySweepingJobs = LazySweepingJobs::new();

#[inline(always)]
fn concurrent_marking_in_progress() -> bool {
    cfg!(feature = "ix_concurrent_marking") && crate::IN_CONCURRENT_GC.load(Ordering::SeqCst)
}

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
static BOOT_TIME: spin::Lazy<SystemTime> = spin::Lazy::new(|| SystemTime::now());
static GC_EPOCH: AtomicUsize = AtomicUsize::new(0);
static RESERVED_PAGES_AT_GC_START: AtomicUsize = AtomicUsize::new(0);
static INSIDE_HARNESS: AtomicBool = AtomicBool::new(false);
static SATB_START: Atomic<SystemTime> = Atomic::new(SystemTime::UNIX_EPOCH);

#[inline(always)]
fn gc_trigger_time() -> u128 {
    crate::GC_TRIGGER_TIME
        .load(Ordering::SeqCst)
        .elapsed()
        .unwrap()
        .as_nanos()
}

#[inline(always)]
fn inside_harness() -> bool {
    crate::INSIDE_HARNESS.load(Ordering::Relaxed)
}

struct Pauses {
    pub rc: AtomicUsize,
    pub initial_mark: AtomicUsize,
    pub final_mark: AtomicUsize,
    pub full: AtomicUsize,
    pub emergency: AtomicUsize,
    pub yield_nanos: Atomic<u128>,
    pub roots_nanos: Atomic<u128>,
    pub satb_nanos: Atomic<u128>,
}

impl Pauses {
    pub fn print_keys(&self) {
        print!("gc.rc\t");
        print!("gc.initial_satb\t");
        print!("gc.final_satb\t");
        print!("gc.full\t");
        print!("gc.emergency\t");
        if cfg!(feature = "yield_and_roots_timer") {
            print!("time.yield\t");
            print!("time.roots\t");
        }
        if cfg!(feature = "satb_timer") {
            print!("time.satb\t");
        }
    }
    pub fn print_values(&self) {
        print!("{}\t", self.rc.load(Ordering::SeqCst));
        print!("{}\t", self.initial_mark.load(Ordering::SeqCst));
        print!("{}\t", self.final_mark.load(Ordering::SeqCst));
        print!("{}\t", self.full.load(Ordering::SeqCst));
        print!("{}\t", self.emergency.load(Ordering::SeqCst));
        if cfg!(feature = "yield_and_roots_timer") {
            print!(
                "{}\t",
                self.yield_nanos.load(Ordering::SeqCst) as f64 / 1000000.0
            );
            print!(
                "{}\t",
                self.roots_nanos.load(Ordering::SeqCst) as f64 / 1000000.0
            );
        }
        if cfg!(feature = "satb_timer") {
            print!(
                "{}\t",
                self.satb_nanos.load(Ordering::SeqCst) as f64 / 1000000.0
            );
        }
    }
}

static PAUSES: Pauses = Pauses {
    rc: AtomicUsize::new(0),
    initial_mark: AtomicUsize::new(0),
    final_mark: AtomicUsize::new(0),
    full: AtomicUsize::new(0),
    emergency: AtomicUsize::new(0),
    yield_nanos: Atomic::new(0),
    roots_nanos: Atomic::new(0),
    satb_nanos: Atomic::new(0),
};

#[derive(Default)]
struct GCStat {
    pub rc_pauses: usize,
    pub alloc_objects: usize,
    pub alloc_volume: usize,
    pub alloc_los_objects: usize,
    pub alloc_los_volume: usize,
    pub promoted_objects: usize,
    pub promoted_volume: usize,
    pub promoted_los_objects: usize,
    pub promoted_los_volume: usize,
    pub dead_objects: usize,
    pub dead_volume: usize,
    pub dead_los_objects: usize,
    pub dead_los_volume: usize,
    pub dead_mature_objects: usize,
    pub dead_mature_volume: usize,
    pub dead_mature_los_objects: usize,
    pub dead_mature_los_volume: usize,
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
        promoted_los_objects,
        promoted_los_volume,
        dead_objects,
        dead_volume,
        dead_los_objects,
        dead_los_volume,
        dead_mature_objects,
        dead_mature_volume,
        dead_mature_los_objects,
        dead_mature_los_volume,
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
    promoted_los_objects: 0,
    promoted_los_volume: 0,
    dead_objects: 0,
    dead_volume: 0,
    dead_los_objects: 0,
    dead_los_volume: 0,
    dead_mature_objects: 0,
    dead_mature_volume: 0,
    dead_mature_los_objects: 0,
    dead_mature_los_volume: 0,
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
