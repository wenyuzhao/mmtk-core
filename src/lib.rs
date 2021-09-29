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
#![feature(const_generics)]
#![feature(once_cell)]
#![feature(const_generics_defaults)]
#![feature(const_trait_impl)]
#![feature(const_option)]
#![feature(const_fn_trait_bound)]
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
use std::{sync::atomic::AtomicBool, time::SystemTime};

pub(crate) use mmtk::MMAPPER;
pub use mmtk::MMTK;
pub(crate) use mmtk::VM_MAP;
use spin::Mutex;
use crate::util::constants;

#[macro_use]
mod policy;

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

static GC_TRIGGER_TIME: Mutex<Option<SystemTime>> = Mutex::new(None);
static GC_START_TIME: Mutex<Option<SystemTime>> = Mutex::new(None);

/// Immix or barrier related flags
pub mod flags {
    // ---------- Immix flags ---------- //
    pub const CONCURRENT_MARKING: bool = false;
    pub const REF_COUNT: bool = true;
    pub const CYCLE_TRIGGER_THRESHOLD: usize = 1024;
    /// Mark/sweep memory for block-level only
    pub const BLOCK_ONLY: bool = true;
    /// Opportunistic copying
    pub const DEFRAG: bool = false;
    /// Mark lines when scanning objects. Otherwise, do it at mark time.
    pub const MARK_LINE_AT_SCAN_TIME: bool = true;
    pub const EAGER_INCREMENTS: bool = true;
    pub const LAZY_DECREMENTS: bool = true;
    pub const LOCK_FREE_BLOCK_ALLOCATION: bool = true;
    pub const NURSERY_BLOCKS_THRESHOLD_FOR_RC: usize = 1000;
    pub const RC_EVACUATE_NURSERY: bool = false;
    pub const LOG_BYTES_PER_RC_LOCK_BIT: usize = super::constants::LOG_BYTES_IN_PAGE as _;

    // ---------- Barrier flags ---------- //
    pub const BARRIER_MEASUREMENT: bool = false;
    pub const TAKERATE_MEASUREMENT: bool = false;

    // ---------- Debugging flags ---------- //
    pub const HARNESS_PRETTY_PRINT: bool = false;
    pub const LOG_PER_GC_STATE: bool = false;
    pub const LOG_STAGES: bool = false;

    pub fn validate_features() {
        validate!(DEFRAG => !BLOCK_ONLY);
        validate!(DEFRAG => !CONCURRENT_MARKING);
        validate!(DEFRAG => !REF_COUNT);
        validate!(CONCURRENT_MARKING => !DEFRAG);
        validate!(CONCURRENT_MARKING => !REF_COUNT);
        validate!(REF_COUNT => !CONCURRENT_MARKING);
        validate!(REF_COUNT => !DEFRAG);
        validate!(RC_EVACUATE_NURSERY => REF_COUNT);
        validate!(EAGER_INCREMENTS => !RC_EVACUATE_NURSERY);
        validate!(RC_EVACUATE_NURSERY => !EAGER_INCREMENTS);
        if BARRIER_MEASUREMENT {
            assert!(!EAGER_INCREMENTS);
            assert!(!LAZY_DECREMENTS);
            assert!(!REF_COUNT);
            assert!(!CONCURRENT_MARKING);
        }
    }
}
