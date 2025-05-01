use std::ops::Range;

use atomic::Ordering;

use super::block::Block;
use crate::util::constants::LOG_MIN_OBJECT_SIZE;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::*;
use crate::{
    util::{Address, ObjectReference},
    vm::*,
};

/// Data structure to reference a line within an immix block.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
pub struct Line(Address);

impl Region for Line {
    const LOG_BYTES: usize = 8;

    #[allow(clippy::assertions_on_constants)] // make sure line is not used when BLOCK_ONLY is turned on.
    fn from_aligned_address(address: Address) -> Self {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    fn start(&self) -> Address {
        self.0
    }
}

#[allow(clippy::assertions_on_constants)]
impl Line {
    pub const RESET_MARK_STATE: u8 = 1;
    pub const MAX_MARK_STATE: u8 = 127;

    /// Line mark table (side)
    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_LINE_MARK;

    /// Align the give address to the line boundary.
    pub fn align(address: Address) -> Address {
        debug_assert!(!super::BLOCK_ONLY);
        address.align_down(Self::BYTES)
    }

    /// Test if the given address is line-aligned
    pub fn is_aligned(address: Address) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        Self::align(address).as_usize() == address.as_usize()
    }

    /// Get the line from a given address.
    /// The address must be line-aligned.
    pub fn from(address: Address) -> Self {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    pub fn of(a: Address) -> Self {
        Self(a.align_down(Self::BYTES))
    }

    pub fn containing<VM: VMBinding>(object: ObjectReference) -> Self {
        Self(VM::VMObjectModel::ref_to_object_start(object).align_down(Self::BYTES))
    }

    /// Get the block containing the line.
    pub fn block(&self) -> Block {
        debug_assert!(!super::BLOCK_ONLY);
        Block::from_unaligned_address(self.0)
    }

    /// Get line start address
    pub const fn start(&self) -> Address {
        debug_assert!(!super::BLOCK_ONLY);
        self.0
    }

    pub const fn end(&self) -> Address {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe { Address::from_usize(self.0.as_usize() + Self::BYTES) }
    }

    /// Get line index within its containing block.
    pub const fn get_index_within_block(&self) -> usize {
        let addr = self.start();
        addr.get_extent(Block::align(addr)) >> Line::LOG_BYTES
    }

    /// Mark the line. This will update the side line mark table.
    pub fn mark(&self, state: u8) {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe {
            Self::MARK_TABLE.store::<u8>(self.start(), state);
        }
    }

    /// Test line mark state.
    pub fn is_marked(&self, state: u8) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe { Self::MARK_TABLE.load::<u8>(self.start()) == state }
    }

    pub fn is_marked_by_satb<VM: VMBinding>(&self) -> bool {
        for i in (0..Self::BYTES).step_by(8) {
            let ptr = self.start() + i;
            if VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
                .extract_side_spec()
                .load_atomic::<u8>(ptr, Ordering::SeqCst)
                == 1
            {
                return true;
            }
        }
        false
    }

    /// Mark all lines the object is spanned to.
    pub fn mark_lines_for_object<VM: VMBinding>(object: ObjectReference, state: u8) -> usize {
        debug_assert!(!super::BLOCK_ONLY);
        let start = object.to_object_start::<VM>();
        let end = start + VM::VMObjectModel::get_current_size(object);
        let start_line = Line::from_unaligned_address(start);
        let mut end_line = Line::from_unaligned_address(end);
        if !Line::is_aligned(end) {
            end_line = end_line.next();
        }
        let mut marked_lines = 0;
        let iter = RegionIterator::<Line>::new(start_line, end_line);
        for line in iter {
            if !line.is_marked(state) {
                marked_lines += 1;
            }
            line.mark(state)
        }
        marked_lines
    }

    pub fn clear_mark_table<VM: VMBinding>(lines: Range<Line>) {
        // FIXME: Performance
        let start = lines.start.start();
        let size = Line::steps_between(&lines.start, &lines.end).unwrap() << Line::LOG_BYTES;
        let mark_bit = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec();
        for i in (0..size).step_by(1 << LOG_MIN_OBJECT_SIZE) {
            mark_bit.store_atomic(start + i, 0u8, Ordering::SeqCst);
        }
    }

    pub(super) fn initialize_mark_table_as_marked<VM: VMBinding>(lines: Range<Line>) {
        let meta = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec();
        let start: *mut u8 = address_to_meta_address(&meta, lines.start.start()).to_mut_ptr();
        let limit: *mut u8 = address_to_meta_address(&meta, lines.end.start()).to_mut_ptr();
        unsafe {
            let bytes = limit.offset_from(start) as usize;
            std::ptr::write_bytes(start, 0xffu8, bytes);
        }
    }
}
