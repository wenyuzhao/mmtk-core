use atomic::Ordering;

use super::block::Block;
use crate::plan::barriers::LOGGED_VALUE;
use crate::util::metadata::side_metadata::{self, *};
use crate::util::metadata::store_metadata;
use crate::util::rc;
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
    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_LINE_MARK;

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
    pub const fn start(&self) -> Address {
        debug_assert!(!super::BLOCK_ONLY);
        self.0
    }

    #[inline(always)]
    pub const fn end(&self) -> Address {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe { Address::from_usize(self.0.as_usize() + Self::BYTES) }
    }

    /// Get line index within its containing block.
    #[inline(always)]
    pub const fn get_index_within_block(&self) -> usize {
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
    const fn load_rc_table_entry<T: Copy>(line_index: usize, table_block_start: Address) -> T {
        let ptr = table_block_start + (line_index * std::mem::size_of::<T>());
        unsafe { ptr.load() }
    }

    #[inline(always)]
    #[allow(unreachable_code)]
    pub const fn rc_dead(line_index: usize, table_block_start: Address) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(super::REF_COUNT);
        const LOG_BITS_PER_LINE: usize =
            Line::LOG_BYTES - rc::LOG_MIN_OBJECT_SIZE + rc::LOG_REF_COUNT_BITS;
        const BITS_PER_LINE: usize = 1 << LOG_BITS_PER_LINE;
        match BITS_PER_LINE {
            8 => Self::load_rc_table_entry::<u8>(line_index, table_block_start) == 0,
            16 => Self::load_rc_table_entry::<u16>(line_index, table_block_start) == 0,
            32 => Self::load_rc_table_entry::<u32>(line_index, table_block_start) == 0,
            64 => Self::load_rc_table_entry::<u64>(line_index, table_block_start) == 0,
            128 => Self::load_rc_table_entry::<u128>(line_index, table_block_start) == 0,
            _ => {
                unreachable!();
                // debug_assert!(BITS_PER_LINE > 128);
                // type UInt = u128;
                // const LOG_BITS_IN_UINT: usize =
                //     (std::mem::size_of::<UInt>() << 3).trailing_zeros() as usize;
                // debug_assert!(
                //     Self::LOG_BYTES - crate::util::rc::LOG_MIN_OBJECT_SIZE
                //         + crate::util::rc::LOG_REF_COUNT_BITS
                //         >= LOG_BITS_IN_UINT
                // );
                // let start =
                //     address_to_meta_address(&crate::util::rc::RC_TABLE, self.start()).as_usize();
                // #[allow(arithmetic_overflow)]
                // let entries: usize = 1usize << (LOG_BITS_PER_LINE - LOG_BITS_IN_UINT);
                // let limit = start + entries;
                // let mut cursor = start;
                // while cursor != limit {
                //     if unsafe { *(cursor as *const UInt) } != 0 {
                //         return false;
                //     }
                //     cursor = cursor + std::mem::size_of::<UInt>();
                // }
                // true
            }
        }
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
        // FIXME: Performance
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

    #[inline(always)]
    pub fn clear_mark_table<VM: VMBinding>(lines: Range<Line>) {
        // FIXME: Performance
        let start = lines.start.start();
        let size = Line::steps_between(&lines.start, &lines.end).unwrap() << Line::LOG_BYTES;
        for i in (0..size).step_by(16) {
            let a = start + i;
            store_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                unsafe { a.to_object_reference() },
                0,
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

// type UInt<const BITS: usize> =

pub trait UintType: 'static + Sized {
    type Type: 'static + Sized + Copy + Eq + PartialEq;
    fn is_zero(v: Self::Type) -> bool;
}

pub struct Uint<const BITS: usize> {}

impl const UintType for Uint<8> {
    type Type = u8;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

impl const UintType for Uint<16> {
    type Type = u16;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

impl const UintType for Uint<32> {
    type Type = u32;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

impl const UintType for Uint<64> {
    type Type = u64;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

impl const UintType for Uint<128> {
    type Type = u128;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

const LOG_BITS_PER_LINE: usize = Line::LOG_BYTES - rc::LOG_MIN_OBJECT_SIZE + rc::LOG_REF_COUNT_BITS;
const BITS_PER_LINE: usize = 1 << LOG_BITS_PER_LINE;
const LOG_BITS_PER_BLOCK: usize =
    Block::LOG_BYTES - rc::LOG_MIN_OBJECT_SIZE + rc::LOG_REF_COUNT_BITS;
const BITS_PER_BLOCK: usize = 1 << LOG_BITS_PER_BLOCK;

pub struct RCArray {
    table: &'static [<Uint<{ BITS_PER_LINE }> as UintType>::Type; BITS_PER_BLOCK / BITS_PER_LINE],
}

impl RCArray {
    pub const fn of(block: Block) -> Self {
        Self {
            table: unsafe { &*block.rc_table_start().to_ptr() },
        }
    }

    #[inline(always)]
    pub const fn is_dead(&self, i: usize) -> bool {
        <Uint<{ BITS_PER_LINE }> as UintType>::is_zero(self.table[i])
    }
}
