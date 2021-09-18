pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

pub use self::global::{get_active_barrier, Immix};

pub const CONCURRENT_MARKING: bool = true;

pub const REF_COUNT: bool = false;

const CYCLE_TRIGGER_THRESHOLD: usize = 1024;
