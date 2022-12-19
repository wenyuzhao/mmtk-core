pub mod block;
pub mod block_allocation;
pub mod defrag;
pub mod immixspace;
pub mod line;
pub mod rc_work;

use std::marker::PhantomData;

pub use immixspace::*;

use crate::{
    policy::immix::block::Block,
    util::metadata::side_metadata::SideMetadataSpec,
    vm::{ObjectModel, VMBinding},
};

/// The max object size for immix: half of a block
pub const MAX_IMMIX_OBJECT_SIZE: usize = {
    if cfg!(feature = "lxr_los_16k") {
        16 * 1024
    } else if cfg!(feature = "lxr_los_32k") {
        32 * 1024
    } else {
        Block::BYTES >> 1
    }
};

/// Mark/sweep memory for block-level only
pub const BLOCK_ONLY: bool = crate::args::BLOCK_ONLY;

/// Opportunistic copying
pub const DEFRAG: bool = crate::args::DEFRAG;

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

pub struct UnlogBit<VM: VMBinding, const COMPRESSED: bool>(PhantomData<VM>);

impl<VM: VMBinding, const COMPRESSED: bool> UnlogBit<VM, COMPRESSED> {
    pub const SPEC: SideMetadataSpec = if COMPRESSED {
        *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC_COMPRESSED
            .as_spec()
            .extract_side_spec()
    } else {
        *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
            .as_spec()
            .extract_side_spec()
    };
}

#[inline(always)]
pub fn get_unlog_bit_slow<VM: VMBinding>() -> SideMetadataSpec {
    if VM::VMObjectModel::compressed_pointers_enabled() {
        *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC_COMPRESSED.extract_side_spec()
    } else {
        *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec()
    }
}
