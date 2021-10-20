pub mod block;
pub mod block_allocation;
pub mod chunk;
pub mod defrag;
pub mod immixspace;
pub mod line;

pub use crate::plan::immix::{CONCURRENT_MARKING, REF_COUNT};
pub use immixspace::*;
use spin::Mutex;

use crate::{policy::immix::block::Block, util::ObjectReference};

/// The max object size for immix: half of a block
pub const MAX_IMMIX_OBJECT_SIZE: usize = Block::BYTES >> 1;

/// Mark/sweep memory for block-level only
pub const BLOCK_ONLY: bool = crate::flags::BLOCK_ONLY;

/// Opportunistic copying
pub const DEFRAG: bool = crate::flags::DEFRAG;

/// Mark lines when scanning objects.
/// Otherwise, do it at mark time.
pub const MARK_LINE_AT_SCAN_TIME: bool = crate::flags::MARK_LINE_AT_SCAN_TIME;

macro_rules! validate {
    ($x: expr) => { assert!($x, stringify!($x)) };
    ($x: expr => $y: expr) => { if $x { assert!($y, stringify!($x implies $y)) } };
}

fn validate_features() {
    crate::flags::validate_features();
    // Number of lines in a block should not exceed BlockState::MARK_MARKED
    assert!(Block::LINES / 2 <= u8::MAX as usize - 2);
}


pub static LARGE_NURSERY_OBJECTS: Mutex<Vec<ObjectReference>> = Mutex::new(vec![]);