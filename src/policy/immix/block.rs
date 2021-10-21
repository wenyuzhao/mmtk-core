use super::chunk::Chunk;
use super::defrag::Histogram;
use super::line::Line;
use super::ImmixSpace;
use crate::plan::barriers::LOGGED_VALUE;
use crate::policy::space::Space;
use crate::util::constants::*;
use crate::util::metadata::side_metadata::{self, *};
use crate::util::metadata::store_metadata;
use crate::util::{Address, ObjectReference};
use crate::vm::*;
use spin::{Mutex, MutexGuard};
use std::{iter::Step, ops::Range, sync::atomic::Ordering};

/// The block allocation state.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BlockState {
    /// the block is not allocated.
    Unallocated,
    /// the block is a young block.
    Nursery,
    /// the block is allocated but not marked.
    Unmarked,
    /// the block is allocated and marked.
    Marked,
    /// RC mutator recycled blocks.
    Reusing,
    /// the block is marked as reusable.
    Reusable { unavailable_lines: u8 },
}

impl BlockState {
    /// Private constant
    const MARK_UNALLOCATED: u8 = 0;
    /// Private constant
    const MARK_UNMARKED: u8 = u8::MAX;
    /// Private constant
    const MARK_MARKED: u8 = u8::MAX - 1;
    const MARK_NURSERY: u8 = u8::MAX - 2;
    const MARK_REUSING: u8 = u8::MAX - 3;
}

impl From<u8> for BlockState {
    #[inline(always)]
    fn from(state: u8) -> Self {
        match state {
            Self::MARK_UNALLOCATED => BlockState::Unallocated,
            Self::MARK_UNMARKED => BlockState::Unmarked,
            Self::MARK_MARKED => BlockState::Marked,
            Self::MARK_NURSERY => BlockState::Nursery,
            Self::MARK_REUSING => BlockState::Reusing,
            unavailable_lines => BlockState::Reusable { unavailable_lines },
        }
    }
}

impl From<BlockState> for u8 {
    #[inline(always)]
    fn from(state: BlockState) -> Self {
        match state {
            BlockState::Unallocated => BlockState::MARK_UNALLOCATED,
            BlockState::Unmarked => BlockState::MARK_UNMARKED,
            BlockState::Marked => BlockState::MARK_MARKED,
            BlockState::Nursery => BlockState::MARK_NURSERY,
            BlockState::Reusing => BlockState::MARK_REUSING,
            BlockState::Reusable { unavailable_lines } => {
                debug_assert_ne!(unavailable_lines, BlockState::MARK_UNALLOCATED);
                debug_assert_ne!(unavailable_lines, BlockState::MARK_UNMARKED);
                debug_assert_ne!(unavailable_lines, BlockState::MARK_MARKED);
                debug_assert_ne!(unavailable_lines, BlockState::MARK_NURSERY);
                unavailable_lines
            }
        }
    }
}

impl BlockState {
    /// Test if the block is reuasable.
    pub const fn is_reusable(&self) -> bool {
        matches!(self, BlockState::Reusable { .. })
    }
}

/// Data structure to reference an immix block.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash)]
pub struct Block(Address);

impl Block {
    /// Log bytes in block
    pub const LOG_BYTES: usize = 15;
    /// Bytes in block
    pub const BYTES: usize = 1 << Self::LOG_BYTES;
    /// Log pages in block
    pub const LOG_PAGES: usize = Self::LOG_BYTES - LOG_BYTES_IN_PAGE as usize;
    /// Pages in block
    pub const PAGES: usize = 1 << Self::LOG_PAGES;
    /// Log lines in block
    pub const LOG_LINES: usize = Self::LOG_BYTES - Line::LOG_BYTES;
    /// Lines in block
    pub const LINES: usize = 1 << Self::LOG_LINES;

    /// Block defrag state table (side)
    pub const DEFRAG_STATE_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_DEFRAG;

    /// Block mark table (side)
    pub const MARK_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_MARK;

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

    /// Get the address range of the block's line mark table.
    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    pub fn line_mark_table(&self) -> MetadataByteArrayRef<{ Block::LINES }> {
        debug_assert!(!super::BLOCK_ONLY);
        MetadataByteArrayRef::<{ Block::LINES }>::new(&Line::MARK_TABLE, self.start(), Self::BYTES)
    }

    /// Get block mark state.
    #[inline(always)]
    pub fn get_state(&self) -> BlockState {
        let byte =
            side_metadata::load_atomic(&Self::MARK_TABLE, self.start(), Ordering::SeqCst) as u8;
        byte.into()
    }

    /// Set block mark state.
    #[inline(always)]
    pub fn set_state(&self, state: BlockState) {
        let state = u8::from(state) as usize;
        side_metadata::store_atomic(&Self::MARK_TABLE, self.start(), state, Ordering::SeqCst);
    }

    /// Set block mark state.
    #[inline(always)]
    pub fn fetch_update_state(
        &self,
        mut f: impl FnMut(BlockState) -> Option<BlockState>,
    ) -> Result<BlockState, BlockState> {
        side_metadata::fetch_update(
            &Self::MARK_TABLE,
            self.start(),
            Ordering::SeqCst,
            Ordering::SeqCst,
            |s| f((s as u8).into()).map(|x| u8::from(x) as usize),
        )
        .map(|x| (x as u8).into())
        .map_err(|x| (x as u8).into())
    }

    // Defrag byte

    const DEFRAG_SOURCE_STATE: u8 = u8::MAX;

    /// Test if the block is marked for defragmentation.
    #[inline(always)]
    pub fn is_defrag_source(&self) -> bool {
        let byte =
            side_metadata::load_atomic(&Self::DEFRAG_STATE_TABLE, self.start(), Ordering::SeqCst)
                as u8;
        byte == Self::DEFRAG_SOURCE_STATE
    }

    #[inline(always)]
    pub fn in_defrag_block<VM: VMBinding>(o: ObjectReference) -> bool {
        Block::containing::<VM>(o).is_defrag_source()
    }

    /// Mark the block for defragmentation.
    #[inline(always)]
    pub fn set_as_defrag_source(&self, defrag: bool) {
        let byte = if defrag { Self::DEFRAG_SOURCE_STATE } else { 0 };
        side_metadata::store_atomic(
            &Self::DEFRAG_STATE_TABLE,
            self.start(),
            byte as usize,
            Ordering::SeqCst,
        );
    }

    /// Record the number of holes in the block.
    #[inline(always)]
    pub fn set_holes(&self, holes: usize) {
        side_metadata::store_atomic(
            &Self::DEFRAG_STATE_TABLE,
            self.start(),
            holes,
            Ordering::SeqCst,
        );
    }

    /// Get the number of holes.
    #[inline(always)]
    pub fn get_holes(&self) -> usize {
        let byte =
            side_metadata::load_atomic(&Self::DEFRAG_STATE_TABLE, self.start(), Ordering::SeqCst)
                as u8;
        debug_assert_ne!(byte, Self::DEFRAG_SOURCE_STATE);
        byte as usize
    }

    /// Initialize a clean block after acquired from page-resource.
    #[inline]
    pub fn init<VM: VMBinding>(&self, copy: bool, reuse: bool, space: &ImmixSpace<VM>) {
        if !copy && reuse {
            self.set_state(BlockState::Reusing);
            space.mutator_recycled_blocks.push(*self);
            debug_assert!(!self.is_defrag_source());
        } else if copy {
            if reuse {
                debug_assert!(!self.is_defrag_source());
            }
            self.set_state(BlockState::Marked);
            side_metadata::store_atomic(
                &Self::DEFRAG_STATE_TABLE,
                self.start(),
                0,
                Ordering::SeqCst,
            );
        } else {
            self.set_state(BlockState::Nursery);
            side_metadata::store_atomic(
                &Self::DEFRAG_STATE_TABLE,
                self.start(),
                0,
                Ordering::SeqCst,
            );
        }
    }

    /// Deinitalize a block before releasing.
    #[inline]
    pub fn deinit(&self) {
        #[cfg(feature = "global_alloc_bit")]
        crate::util::alloc_bit::bzero_alloc_bit(self.start(), Self::BYTES);
        self.set_state(BlockState::Unallocated);
    }

    /// Get the range of lines within the block.
    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    pub fn lines(&self) -> Range<Line> {
        debug_assert!(!super::BLOCK_ONLY);
        Line::from(self.start())..Line::from(self.end())
    }

    #[inline(always)]
    pub fn clear_mark_table(&self) {
        bzero_metadata(&Self::MARK_TABLE, self.start(), Block::BYTES);
    }

    #[inline(always)]
    pub fn clear_rc_table<VM: VMBinding>(&self) {
        bzero_metadata(&crate::util::rc::RC_TABLE, self.start(), Block::BYTES);
    }

    #[inline(always)]
    pub fn clear_striddle_table<VM: VMBinding>(&self) {
        bzero_metadata(&crate::util::rc::RC_STRADDLE_LINES, self.start(), Block::BYTES);
    }

    #[inline(always)]
    pub fn clear_log_table<VM: VMBinding>(&self) {
        bzero_metadata(
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
            self.start(),
            Block::BYTES,
        );
    }

    #[inline(always)]
    pub fn assert_log_table_cleared<VM: VMBinding>(&self, meta: &SideMetadataSpec) {
        assert!(cfg!(debug_assertions));
        let start = address_to_meta_address(meta, self.start()).to_ptr::<u128>();
        let limit = address_to_meta_address(meta, self.end()).to_ptr::<u128>();
        let table = unsafe { std::slice::from_raw_parts(start, limit.offset_from(start) as _) };
        for x in table {
            assert_eq!(*x, 0);
        }
    }

    #[inline(always)]
    pub fn initialize_log_table_as_unlogged<VM: VMBinding>(&self) {
        let meta = VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec();
        let start: *mut u8 = address_to_meta_address(&meta, self.start()).to_mut_ptr();
        let limit: *mut u8 = address_to_meta_address(&meta, self.end()).to_mut_ptr();
        unsafe {
            let count = limit.offset_from(start) as usize;
            std::ptr::write_bytes(start, 0xffu8, count);
        }
    }

    #[inline(always)]
    pub fn rc_dead(&self) -> bool {
        type UInt = u128;
        const LOG_BITS_IN_UINT: usize =
            (std::mem::size_of::<UInt>() << 3).trailing_zeros() as usize;
        debug_assert!(
            Self::LOG_BYTES - crate::util::rc::LOG_MIN_OBJECT_SIZE
                + crate::util::rc::LOG_REF_COUNT_BITS
                >= LOG_BITS_IN_UINT
        );
        let start =
            address_to_meta_address(&crate::util::rc::RC_TABLE, self.start()).to_ptr::<UInt>();
        let limit =
            address_to_meta_address(&crate::util::rc::RC_TABLE, self.end()).to_ptr::<UInt>();
        let rc_table = unsafe { std::slice::from_raw_parts(start, limit.offset_from(start) as _) };
        for x in rc_table {
            if *x != 0 {
                return false;
            }
        }
        true
    }

    /// Sweep this block.
    /// Return true if the block is swept.
    #[inline(always)]
    pub fn sweep<VM: VMBinding>(
        &self,
        space: &ImmixSpace<VM>,
        mark_histogram: &mut Histogram,
        line_mark_state: Option<u8>,
        perform_cycle_collection: bool,
    ) -> bool {
        if crate::plan::barriers::BARRIER_MEASUREMENT && space.common().needs_log_bit {
            let meta = &VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC;
            for i in (0..Block::BYTES).step_by(8) {
                let o = unsafe { (self.start() + i).to_object_reference() };
                store_metadata::<VM>(meta, o, LOGGED_VALUE, None, None);
            }
        }
        if super::BLOCK_ONLY {
            if super::REF_COUNT && !perform_cycle_collection {
                let live = !self.rc_dead();
                if !live {
                    space.release_block(*self, false);
                }
                return !live;
            }
            match self.get_state() {
                BlockState::Unallocated => false,
                BlockState::Unmarked => {
                    // Release the block if it is allocated but not marked by the current GC.
                    space.release_block(*self, false);
                    true
                }
                BlockState::Nursery | BlockState::Marked | BlockState::Reusing => {
                    // The block is live.
                    false
                }
                BlockState::Reusable { .. } => unreachable!(),
            }
        } else {
            // Calculate number of marked lines and holes.
            let mut marked_lines = 0;
            let mut holes = 0;
            let mut prev_line_is_marked = true;
            let line_mark_state = line_mark_state.unwrap();

            for line in self.lines() {
                if line.is_marked(line_mark_state) {
                    marked_lines += 1;
                    prev_line_is_marked = true;
                } else {
                    if prev_line_is_marked {
                        holes += 1;
                    }
                    prev_line_is_marked = false;
                }
            }

            if marked_lines == 0 {
                // Release the block if non of its lines are marked.
                space.release_block(*self, false);
                true
            } else {
                // There are some marked lines. Keep the block live.
                if marked_lines != Block::LINES {
                    // There are holes. Mark the block as reusable.
                    self.set_state(BlockState::Reusable {
                        unavailable_lines: marked_lines as _,
                    });
                    space.reusable_blocks.push(*self)
                } else {
                    // Clear mark state.
                    self.set_state(BlockState::Unmarked);
                }
                // Update mark_histogram
                mark_histogram[holes] += marked_lines;
                // Record number of holes in block side metadata.
                self.set_holes(holes);
                false
            }
        }
    }

    #[inline(always)]
    pub fn rc_sweep_nursery<VM: VMBinding>(&self, space: &ImmixSpace<VM>) -> bool {
        debug_assert!(crate::flags::REF_COUNT);
        if crate::flags::RC_EVACUATE_NURSERY {
            debug_assert!(self.rc_dead(), "{:?} has live rc counts", self);
            space.release_block(*self, true);
            true
        } else {
            let live = !self.rc_dead();
            if !live {
                space.release_block(*self, true);
            }
            return !live;
        }
    }

    #[inline(always)]
    pub fn rc_sweep_mature<VM: VMBinding>(
        &self,
        space: &ImmixSpace<VM>,
    ) {
        debug_assert!(crate::flags::REF_COUNT);
        debug_assert_ne!(self.get_state(), BlockState::Unallocated, "{:?}", self);
        if self.is_defrag_source() {
            return;
        }
        if self.rc_dead() {
            if !crate::flags::IGNORE_REUSING_BLOCKS
                || self
                    .fetch_update_state(|s| {
                        if s == BlockState::Reusing {
                            None
                        } else {
                            Some(BlockState::Unallocated)
                        }
                    })
                    .is_ok()
            {
                space.release_block(*self, false);
            }
        } else if !crate::flags::BLOCK_ONLY {
            // See the caller of this function.
            // At least one object is dead in the block.
            let add_as_reusable = if !crate::flags::IGNORE_REUSING_BLOCKS {
                if !self.get_state().is_reusable() {
                    self.set_state(BlockState::Reusable {
                        unavailable_lines: 1 as _,
                    });
                    true
                } else {
                    false
                }
            } else {
                self.fetch_update_state(|s| {
                    if s == BlockState::Reusing || s.is_reusable() {
                        None
                    } else {
                        Some(BlockState::Reusable {
                            unavailable_lines: 1,
                        })
                    }
                })
                .is_ok()
            };
            if add_as_reusable {
                debug_assert!(self.get_state().is_reusable());
                space.reusable_blocks.push(*self)
            }
        }
    }
}

impl Step for Block {
    /// Get the number of blocks between the given two blocks.
    #[inline(always)]
    #[allow(clippy::assertions_on_constants)]
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        debug_assert!(!super::BLOCK_ONLY);
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

/// A non-block single-linked list to store blocks.
#[derive(Default)]
pub struct BlockList {
    queue: Mutex<Vec<Block>>,
}

impl BlockList {
    /// Get number of blocks in this list.
    #[inline]
    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }

    /// Add a block to the list.
    #[inline]
    pub fn push(&self, block: Block) {
        self.queue.lock().push(block)
    }

    /// Pop a block out of the list.
    #[inline]
    pub fn pop(&self) -> Option<Block> {
        self.queue.lock().pop()
    }

    /// Clear the list.
    #[inline]
    pub fn reset(&self) {
        *self.queue.lock() = Vec::new()
    }

    /// Get an array of all reusable blocks stored in this BlockList.
    #[inline]
    pub fn get_blocks(&self) -> MutexGuard<Vec<Block>> {
        self.queue.lock()
    }
}
