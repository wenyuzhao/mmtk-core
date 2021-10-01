use atomic::Ordering;

use super::block::Block;
use super::IMMIX_LOCAL_SIDE_METADATA_BASE_OFFSET;
use crate::plan::barriers::LOGGED_VALUE;
use crate::policy::immix::rc::RC_TABLE;
use crate::util::metadata::side_metadata::{self, *};
use crate::util::metadata::store_metadata;
use crate::{
    util::{Address, ObjectReference},
    vm::*,
};
use std::iter::Step;
use std::ops::Range;

/// Data structure to reference a line within an immix block.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
pub struct Line(Address);

#[allow(clippy::assertions_on_constants)]
impl Line {
    /// Log bytes in block
    pub const LOG_BYTES: usize = 8;
    /// Bytes in block
    pub const BYTES: usize = 1 << Self::LOG_BYTES;

    pub const RESET_MARK_STATE: u8 = 1;
    pub const MAX_MARK_STATE: u8 = 127;

    /// Line mark table (side)
    pub const MARK_TABLE: SideMetadataSpec = SideMetadataSpec {
        is_global: false,
        offset: IMMIX_LOCAL_SIDE_METADATA_BASE_OFFSET,
        log_num_of_bits: 3,
        log_min_obj_size: Self::LOG_BYTES,
    };

    /// Align the give address to the line boundary.
    #[inline(always)]
    pub fn align(address: Address) -> Address {
        debug_assert!(!super::BLOCK_ONLY);
        address.align_down(Self::BYTES)
    }

    /// Test if the given address is line-aligned
    #[inline(always)]
    pub fn is_aligned(address: Address) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        Self::align(address).as_usize() == address.as_usize()
    }

    /// Get the line from a given address.
    /// The address must be line-aligned.
    #[inline(always)]
    pub fn from(address: Address) -> Self {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    #[inline(always)]
    pub fn containing<VM: VMBinding>(object: ObjectReference) -> Self {
        Self(VM::VMObjectModel::ref_to_address(object).align_down(Self::BYTES))
    }

    /// Get the block containing the line.
    #[inline(always)]
    pub fn block(&self) -> Block {
        debug_assert!(!super::BLOCK_ONLY);
        Block::from(Block::align(self.0))
    }

    /// Get line start address
    #[inline(always)]
    pub fn start(&self) -> Address {
        debug_assert!(!super::BLOCK_ONLY);
        self.0
    }

    #[inline(always)]
    pub fn end(&self) -> Address {
        debug_assert!(!super::BLOCK_ONLY);
        self.0 + Self::BYTES
    }

    /// Get line index within its containing block.
    #[inline(always)]
    pub fn get_index_within_block(&self) -> usize {
        let addr = self.start();
        addr.get_extent(Block::align(addr)) >> Line::LOG_BYTES
    }

    /// Mark the line. This will update the side line mark table.
    #[inline]
    pub fn mark(&self, state: u8) {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe {
            side_metadata::store(&Self::MARK_TABLE, self.start(), state as _);
        }
    }

    /// Test line mark state.
    #[inline(always)]
    pub fn is_marked(&self, state: u8) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe { side_metadata::load(&Self::MARK_TABLE, self.start()) as u8 == state }
    }
    #[inline(always)]
    fn rc_dead_slow(&self) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(super::REF_COUNT);
        debug_assert!(Line::LOG_BYTES + RC_TABLE.log_num_of_bits >= 9);
        for i in (0..Self::BYTES).step_by(8) {
            let a = self.start() + i;
            let c = super::rc::count(unsafe { a.to_object_reference() });
            if c != 0 {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    pub fn rc_dead(&self) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(super::REF_COUNT);
        debug_assert!(Line::LOG_BYTES + RC_TABLE.log_num_of_bits >= 9);
        let start = address_to_meta_address(&RC_TABLE, self.start()).to_ptr::<u8>();
        let limit = address_to_meta_address(&RC_TABLE, self.end()).to_ptr::<u8>();
        let table = unsafe { std::slice::from_raw_parts(start, limit.offset_from(start) as _) };
        for x in table {
            if *x != 0 {
                if cfg!(debug_assertions) {
                    debug_assert!(!self.rc_dead_slow());
                }
                return false;
            }
        }
        if cfg!(debug_assertions) {
            debug_assert!(self.rc_dead_slow());
        }
        true
    }

    /// Mark all lines the object is spanned to.
    #[inline]
    pub fn mark_lines_for_object<VM: VMBinding>(object: ObjectReference, state: u8) -> usize {
        debug_assert!(!super::BLOCK_ONLY);
        let start = VM::VMObjectModel::object_start_ref(object);
        let end = start + VM::VMObjectModel::get_current_size(object);
        let start_line = Line::from(Line::align(start));
        let mut end_line = Line::from(Line::align(end));
        if !Line::is_aligned(end) {
            end_line = Line::forward(end_line, 1)
        }
        let mut marked_lines = 0;
        for line in start_line..end_line {
            if !line.is_marked(state) {
                marked_lines += 1;
            }
            line.mark(state)
        }
        marked_lines
    }

    #[inline(always)]
    pub fn clear_log_table<VM: VMBinding>(lines: Range<Line>) {
        let start = lines.start.start();
        let size = Line::steps_between(&lines.start, &lines.end).unwrap() << Line::LOG_BYTES;
        for i in (0..size).step_by(8) {
            let a = start + i;
            store_metadata::<VM>(
                &VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
                unsafe { a.to_object_reference() },
                LOGGED_VALUE,
                None,
                Some(Ordering::SeqCst),
            );
        }
    }
}

#[allow(clippy::assertions_on_constants)]
impl Step for Line {
    /// Get the number of lines between the given two lines.
    #[inline(always)]
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        debug_assert!(!super::BLOCK_ONLY);
        if start > end {
            return None;
        }
        Some((end.start() - start.start()) >> Line::LOG_BYTES)
    }
    /// result = line_address + count * block_size
    #[inline(always)]
    fn forward(start: Self, count: usize) -> Self {
        debug_assert!(!super::BLOCK_ONLY);
        Self::from(start.start() + (count << Self::LOG_BYTES))
    }
    /// result = line_address + count * block_size
    #[inline(always)]
    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        debug_assert!(!super::BLOCK_ONLY);
        if start.start().as_usize() > usize::MAX - (count << Self::LOG_BYTES) {
            return None;
        }
        Some(Self::forward(start, count))
    }
    /// result = line_address + count * block_size
    #[inline(always)]
    fn backward(start: Self, count: usize) -> Self {
        debug_assert!(!super::BLOCK_ONLY);
        Self::from(start.start() - (count << Self::LOG_BYTES))
    }
    /// result = line_address - count * block_size
    #[inline(always)]
    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        debug_assert!(!super::BLOCK_ONLY);
        if start.start().as_usize() < (count << Self::LOG_BYTES) {
            return None;
        }
        Some(Self::backward(start, count))
    }
}
