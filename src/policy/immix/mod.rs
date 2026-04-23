pub mod block;
pub mod defrag;
pub mod immixspace;
pub mod line;

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
pub const BLOCK_ONLY: bool = false;

/// Mark lines when scanning objects.
/// Otherwise, do it at mark time.
pub const MARK_LINE_AT_SCAN_TIME: bool = true;
