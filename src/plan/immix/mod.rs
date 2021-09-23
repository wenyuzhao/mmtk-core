pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

use std::sync::{atomic::AtomicUsize, Arc};

pub use self::global::{get_active_barrier, Immix};

pub const CONCURRENT_MARKING: bool = crate::flags::CONCURRENT_MARKING;

pub const REF_COUNT: bool = crate::flags::REF_COUNT;

const CYCLE_TRIGGER_THRESHOLD: usize = crate::flags::CYCLE_TRIGGER_THRESHOLD;

pub static mut CURRENT_CONC_DECS_COUNTER: Option<Arc<AtomicUsize>> = None;
