use super::block::{Block, BlockState};
use super::chunk::Chunk;
use super::cset::PerRegionRemSet;
use crate::util::constants::*;
use crate::util::metadata::side_metadata::{self, SideMetadataSpec};
use crate::util::{Address, ObjectReference};
use crate::vm::*;
use std::ops::Range;
use std::{iter::Step, sync::atomic::Ordering};

/// The block allocation state.
#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RegionState {
    Unallocated = 0,
    Allocated = 1,
    DefragSource = 2,
    DefragSourceActive = 3,
}

impl From<u8> for RegionState {
    #[inline(always)]
    fn from(state: u8) -> Self {
        unsafe { std::mem::transmute(state) }
    }
}

impl From<RegionState> for u8 {
    #[inline(always)]
    fn from(state: RegionState) -> Self {
        state as _
    }
}

/// Data structure to reference an immix block.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash)]
pub struct Region(Address);

impl Region {
    pub const LOG_BLOCKS: usize = {
        if cfg!(feature = "lxr_region_256k") {
            3
        } else if cfg!(feature = "lxr_region_512k") {
            4
        } else if cfg!(feature = "lxr_region_1m") {
            5
        } else if cfg!(feature = "lxr_region_2m") {
            6
        } else if cfg!(feature = "lxr_region_4m") {
            7
        } else {
            5
        }
    };
    pub const BLOCKS: usize = 1 << Self::LOG_BLOCKS;
    /// Log bytes in block
    pub const LOG_BYTES: usize = Self::LOG_BLOCKS + Block::LOG_BYTES;
    /// Bytes in block
    pub const BYTES: usize = 1 << Self::LOG_BYTES;
    /// Log pages in block
    pub const LOG_PAGES: usize = Self::LOG_BYTES - LOG_BYTES_IN_PAGE as usize;
    /// Pages in block
    pub const PAGES: usize = 1 << Self::LOG_PAGES;

    pub const ZERO: Self = Self(Address::ZERO);

    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_REGION_MARK;

    pub const REMSET: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_REGION_REMSET;

    pub const fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Align the address to a block boundary.
    pub const fn align(address: Address) -> Address {
        address.align_down(Self::BYTES)
    }

    /// Get the block from a given address.
    /// The address must be block-aligned.
    #[inline(always)]
    pub fn from(address: Address) -> Self {
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    #[inline(always)]
    pub fn of(a: Address) -> Self {
        Self::from(Self::align(a))
    }

    /// Get the block containing the given address.
    /// The input address does not need to be aligned.
    #[inline(always)]
    pub fn containing<VM: VMBinding>(object: ObjectReference) -> Self {
        Self(VM::VMObjectModel::ref_to_address(object).align_down(Self::BYTES))
    }

    /// Get block start address
    pub const fn start(&self) -> Address {
        self.0
    }

    /// Get block end address
    pub const fn end(&self) -> Address {
        self.0.add(Self::BYTES)
    }

    /// Get the chunk containing the block.
    #[inline(always)]
    pub fn chunk(&self) -> Chunk {
        Chunk::from(Chunk::align(self.0))
    }

    // Get block mark state.
    #[inline(always)]
    pub fn get_state(&self) -> RegionState {
        let byte =
            side_metadata::load_atomic(&Self::MARK_TABLE, self.start(), Ordering::SeqCst) as u8;
        byte.into()
    }

    /// Set block mark state.
    #[inline(always)]
    pub fn set_state(&self, state: RegionState) {
        let state = u8::from(state) as usize;
        side_metadata::store_atomic(&Self::MARK_TABLE, self.start(), state, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn remset(&self) -> &'static mut PerRegionRemSet {
        let ptr = side_metadata::load_atomic(&Self::REMSET, self.start(), Ordering::SeqCst)
            as *mut PerRegionRemSet;
        debug_assert!(!ptr.is_null(), "{:?} is not initialized", self);
        unsafe { &mut *ptr }
    }

    #[inline(always)]
    pub fn remset_is_initialized(&self) -> bool {
        let ptr = side_metadata::load_atomic(&Self::REMSET, self.start(), Ordering::SeqCst)
            as *mut PerRegionRemSet;
        !ptr.is_null()
    }

    #[inline(always)]
    pub fn init_remset(&self, gc_workers: usize) {
        let ptr = side_metadata::load_atomic(&Self::REMSET, self.start(), Ordering::SeqCst)
            as *mut PerRegionRemSet;
        if !ptr.is_null() {
            return;
        }
        let remset = Box::leak(box PerRegionRemSet::new(gc_workers));
        let ptr = remset as *const PerRegionRemSet as usize;
        let result = side_metadata::fetch_update(
            &Self::REMSET,
            self.start(),
            Ordering::SeqCst,
            Ordering::SeqCst,
            |x| {
                if x == 0 {
                    Some(ptr)
                } else {
                    None
                }
            },
        );
        if result.is_err() {
            let _boxed = unsafe { Box::from_raw(remset) };
        }
    }

    #[inline(always)]
    pub fn clear_remset(&self) {
        self.remset().clear()
    }

    // /// Set block mark state.
    // #[inline(always)]
    // pub fn fetch_update_state(
    //     &self,
    //     mut f: impl FnMut(BlockState) -> Option<BlockState>,
    // ) -> Result<BlockState, BlockState> {
    //     side_metadata::fetch_update(
    //         &Self::MARK_TABLE,
    //         self.start(),
    //         Ordering::SeqCst,
    //         Ordering::SeqCst,
    //         |s| f((s as u8).into()).map(|x| u8::from(x) as usize),
    //     )
    //     .map(|x| (x as u8).into())
    //     .map_err(|x| (x as u8).into())
    // }

    #[inline(always)]
    pub fn is_defrag_source(&self) -> bool {
        self.get_state() == RegionState::DefragSource
    }

    #[inline(always)]
    pub fn is_defrag_source_active(&self) -> bool {
        self.get_state() == RegionState::DefragSourceActive
    }

    #[inline(always)]
    pub fn set_active(&self) {
        self.set_state(RegionState::DefragSourceActive)
    }

    #[inline(always)]
    pub fn set_defrag_source(&self) {
        self.set_state(RegionState::DefragSource)
    }

    // #[inline]
    // pub fn record_remset(&self, region: Region, e: Address) {}

    /// Get a range of blocks within this chunk.
    #[inline(always)]
    pub fn blocks(&self) -> Range<Block> {
        let start = Block::from(Block::align(self.0));
        let end = Block::from(start.start() + (Self::BLOCKS << Block::LOG_BYTES));
        start..end
    }

    #[inline(always)]
    pub fn committed_blocks(&self) -> impl Iterator<Item = Block> {
        self.blocks()
            .filter(|block| block.get_state() != BlockState::Unallocated)
    }

    #[inline(always)]
    pub fn committed_mature_blocks(&self) -> impl Iterator<Item = Block> {
        self.blocks().filter(|block| {
            let state = block.get_state();
            state != BlockState::Unallocated && state != BlockState::Nursery
        })
    }
}

impl Step for Region {
    /// Get the number of blocks between the given two blocks.
    #[inline(always)]
    #[allow(clippy::assertions_on_constants)]
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        if start > end {
            return None;
        }
        Some((end.start() - start.start()) >> Self::LOG_BYTES)
    }
    /// result = block_address + count * block_size
    #[inline(always)]
    fn forward(start: Self, count: usize) -> Self {
        Self::from(start.start() + (count << Self::LOG_BYTES))
    }
    /// result = block_address + count * block_size
    #[inline(always)]
    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        if start.start().as_usize() > usize::MAX - (count << Self::LOG_BYTES) {
            return None;
        }
        Some(Self::forward(start, count))
    }
    /// result = block_address + count * block_size
    #[inline(always)]
    fn backward(start: Self, count: usize) -> Self {
        Self::from(start.start() - (count << Self::LOG_BYTES))
    }
    /// result = block_address - count * block_size
    #[inline(always)]
    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        if start.start().as_usize() < (count << Self::LOG_BYTES) {
            return None;
        }
        Some(Self::backward(start, count))
    }
}
