pub mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

pub use self::global::IMMIX_CONSTRAINTS;
pub use self::global::{Immix, ALLOC_IMMIX};

use crossbeam_queue::SegQueue;

use crate::util::ObjectReference;

pub use self::gc_work::ImmixCopyContext;

pub const CONCURRENT_MARKING: bool = crate::args::CONCURRENT_MARKING;

pub const REF_COUNT: bool = crate::args::REF_COUNT;

const CYCLE_TRIGGER_THRESHOLD: usize = crate::args::CYCLE_TRIGGER_THRESHOLD;

pub static mut PREV_ROOTS: SegQueue<Vec<ObjectReference>> = SegQueue::new();
pub static mut CURR_ROOTS: SegQueue<Vec<ObjectReference>> = SegQueue::new();

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Pause {
    FullTraceFast,
    FullTraceDefrag,
    RefCount,
    InitialMark,
    FinalMark,
}
