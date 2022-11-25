use std::ops::Range;

use atomic::Ordering;

use super::block::Block;
use crate::util::constants::{LOG_BITS_IN_BYTE, LOG_BYTES_IN_WORD};
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::*;
use crate::util::rc;
use crate::{
    util::{Address, ObjectReference},
    vm::*,
};

/// Data structure to reference a line within an immix block.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
pub struct Line(Address);

impl From<Address> for Line {
    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    fn from(address: Address) -> Line {
        debug_assert!(!super::BLOCK_ONLY);
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }
}

impl From<Line> for Address {
    #[inline(always)]
    fn from(line: Line) -> Address {
        line.0
    }
}

impl Region for Line {
    const LOG_BYTES: usize = {
        if cfg!(feature = "lxr_line_512b") {
            9
        } else if cfg!(feature = "lxr_line_1k") {
            10
        } else {
            8
        }
    };
}

#[allow(clippy::assertions_on_constants)]
impl Line {
    pub const RESET_MARK_STATE: u8 = 1;
    pub const MAX_MARK_STATE: u8 = 127;

    /// Line mark table (side)
    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_LINE_MARK;

    pub const VALIDITY_STATE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_LINE_VALIDITY;

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
    pub fn of(a: Address) -> Self {
        Self(a.align_down(Self::BYTES))
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
            Self::MARK_TABLE.store::<u8>(self.start(), state);
        }
    }

    /// Test line mark state.
    #[inline(always)]
    pub fn is_marked(&self, state: u8) -> bool {
        debug_assert!(!super::BLOCK_ONLY);
        unsafe { Self::MARK_TABLE.load::<u8>(self.start()) == state }
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

    #[inline(always)]
    pub fn encode_validity_state(ptr: Address, valid: u8) -> Address {
        unsafe { Address::from_usize(ptr.as_usize() | ((valid as usize) << 56)) }
    }

    #[inline(always)]
    pub fn decode_validity_state(x: Address) -> (Address, u8) {
        let v = (x.as_usize() >> 56) as u8;
        let p = unsafe { Address::from_usize(x.as_usize() & 0x0000_ffff_ffff_ffff_usize) };
        (p, v)
    }

    #[inline(always)]
    pub fn currrent_validity_state(&self) -> u8 {
        unsafe { Self::VALIDITY_STATE.load(self.start()) }
    }

    #[inline(always)]
    pub fn pointer_is_valid(&self, pointer_epoch: u8) -> bool {
        pointer_epoch == self.currrent_validity_state()
    }

    #[inline(always)]
    pub fn update_validity(lines: RegionIterator<Line>) {
        if !crate::REMSET_RECORDING.load(Ordering::SeqCst) {
            return;
        }
        let mut has_invalid_state = false;
        for line in lines {
            let old = line.currrent_validity_state();
            has_invalid_state = has_invalid_state || (old >= u8::MAX);
            unsafe { Self::VALIDITY_STATE.store(line.start(), old + 1) };
        }
        assert!(!has_invalid_state, "Over 255 RC pauses during SATB");
    }

    #[inline(always)]
    pub fn clear_log_table<VM: VMBinding>(lines: Range<Line>) {
        const LOG_META_BITS_PER_LINE: usize = Line::LOG_BYTES - LOG_BYTES_IN_WORD as usize;
        debug_assert!((1 << LOG_META_BITS_PER_LINE) >= 8);
        const LOG_META_BYTES_PER_LINE: usize = LOG_META_BITS_PER_LINE - LOG_BITS_IN_BYTE as usize;
        // FIXME: Performance
        let start = lines.start.start();
        let meta_start = address_to_meta_address(
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
            start,
        );
        let meta_bytes =
            Line::steps_between(&lines.start, &lines.end).unwrap() << LOG_META_BYTES_PER_LINE;
        crate::util::memory::zero(meta_start, meta_bytes)
    }

    #[inline(always)]
    pub fn initialize_log_table_as_unlogged<VM: VMBinding>(lines: Range<Line>) {
        const LOG_META_BITS_PER_LINE: usize = Line::LOG_BYTES - LOG_BYTES_IN_WORD as usize;
        debug_assert!((1 << LOG_META_BITS_PER_LINE) >= 8);
        const LOG_META_BYTES_PER_LINE: usize = LOG_META_BITS_PER_LINE - LOG_BITS_IN_BYTE as usize;
        // FIXME: Performance
        let start = lines.start.start();
        let meta_start = address_to_meta_address(
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
            start,
        );
        let meta_bytes =
            Line::steps_between(&lines.start, &lines.end).unwrap() << LOG_META_BYTES_PER_LINE;
        unsafe {
            std::ptr::write_bytes::<u8>(meta_start.to_mut_ptr(), 0xffu8, meta_bytes);
        }
    }

    #[inline(always)]
    pub fn clear_mark_table<VM: VMBinding>(lines: Range<Line>) {
        // FIXME: Performance
        let start = lines.start.start();
        let size = Line::steps_between(&lines.start, &lines.end).unwrap() << Line::LOG_BYTES;
        let mark_bit = VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec();
        for i in (0..size).step_by(16) {
            mark_bit.store_atomic(start + i, 0u8, Ordering::SeqCst);
        }
    }
}

// type UInt<const BITS: usize> =

pub trait UintType: 'static + Sized {
    type Type: 'static + Sized + Copy + Eq + PartialEq;
    fn is_zero(v: Self::Type) -> bool;
}

pub struct Uint<const BITS: usize> {}

impl UintType for Uint<8> {
    type Type = u8;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

impl UintType for Uint<16> {
    type Type = u16;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

impl UintType for Uint<32> {
    type Type = u32;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

impl UintType for Uint<64> {
    type Type = u64;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

impl UintType for Uint<128> {
    type Type = u128;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == 0
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct UInt512([u8; 512 / 8]);

impl UintType for Uint<512> {
    type Type = UInt512;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == UInt512([0; 512 / 8])
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct UInt1024([u8; 1024 / 8]);

impl UintType for Uint<1024> {
    type Type = UInt1024;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == UInt1024([0; 1024 / 8])
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct UInt2048([u8; 2048 / 8]);

impl UintType for Uint<2048> {
    type Type = UInt2048;
    #[inline(always)]
    fn is_zero(v: Self::Type) -> bool {
        v == UInt2048([0; 2048 / 8])
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
    #[inline(always)]
    pub fn of(block: Block) -> Self {
        Self {
            table: unsafe { &*block.rc_table_start().to_ptr() },
        }
    }

    #[inline(always)]
    pub fn is_dead(&self, i: usize) -> bool {
        <Uint<{ BITS_PER_LINE }> as UintType>::is_zero(self.table[i])
    }
}
