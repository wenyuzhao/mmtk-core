pub mod block;
pub mod block_allocation;
pub mod defrag;
pub mod immixspace;
pub mod line;
pub mod rc_work;

pub use immixspace::*;

use crate::policy::immix::block::Block;

/// The max object size for immix: half of a block
pub const MAX_IMMIX_OBJECT_SIZE: usize = {
    if cfg!(feature = "lxr_los_16k") {
        16 * 1024
    } else if cfg!(feature = "lxr_los_32k") {
        32 * 1024
    } else {
        Block::BYTES
    }
};

/// Mark/sweep memory for block-level only
pub const BLOCK_ONLY: bool = crate::args::BLOCK_ONLY;

/// Opportunistic copying
pub const DEFRAG: bool = crate::args::DEFRAG;

/// Make every GC a defragment GC. (for debugging)
pub const STRESS_DEFRAG: bool = false;

/// Mark every allocated block as defragmentation source before GC. (for debugging)
/// Set both this and `STRESS_DEFRAG` to true to make Immix move as many objects as possible.
pub const DEFRAG_EVERY_BLOCK: bool = false;

/// If Immix is used as a nursery space, do we prefer copy?
/// If this is true, we copy nursery objects if possible.
pub const PREFER_COPY_ON_NURSERY_GC: bool = !cfg!(feature = "immix_no_nursery_copy");

/// In some cases/settings, Immix may never move objects.
/// Currently we only have two cases where we move objects: 1. defrag, 2. nursery copy.
/// If we do neither, we will not move objects.
/// If we have other reasons to move objects, we need to add them here.
pub const NEVER_MOVE_OBJECTS: bool = !DEFRAG && !PREFER_COPY_ON_NURSERY_GC;

/// Mark lines when scanning objects.
/// Otherwise, do it at mark time.
pub const MARK_LINE_AT_SCAN_TIME: bool = crate::args::MARK_LINE_AT_SCAN_TIME;

macro_rules! validate {
    ($x: expr) => { assert!($x, stringify!($x)) };
    ($x: expr => $y: expr) => { if $x { assert!($y, stringify!($x implies $y)) } };
}

fn validate_features() {
    // Number of lines in a block should not exceed BlockState::MARK_MARKED
    // if !crate::args::REF_COUNT && !crate::args::BLOCK_ONLY {
    //     assert!(Block::LINES / 2 <= u8::MAX as usize - 2);
    // }
}
