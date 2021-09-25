pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

use std::sync::{atomic::AtomicUsize, Arc};

use spin::Mutex;

use crate::util::ObjectReference;

pub use self::gc_work::ImmixCopyContext;
pub use self::global::{get_active_barrier, Immix};

pub const CONCURRENT_MARKING: bool = crate::flags::CONCURRENT_MARKING;

pub const REF_COUNT: bool = crate::flags::REF_COUNT;

const CYCLE_TRIGGER_THRESHOLD: usize = crate::flags::CYCLE_TRIGGER_THRESHOLD;

pub static mut CURRENT_CONC_DECS_COUNTER: Option<Arc<AtomicUsize>> = None;

pub static PREV_ROOTS: Mutex<Vec<Vec<ObjectReference>>> = Mutex::new(Vec::new());
pub static CURR_ROOTS: Mutex<Vec<Vec<ObjectReference>>> = Mutex::new(Vec::new());
