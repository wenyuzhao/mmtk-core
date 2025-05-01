use super::defrag::Histogram;
use super::line::Line;
use super::ImmixSpace;
use crate::util::heap::blockpageresource_nosweep::BlockPool;
use crate::util::heap::chunk_map::Chunk;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::spec_defs::{BLOCK_IN_USE, BLOCK_OWNER};
use crate::util::metadata::side_metadata::*;
#[cfg(feature = "vo_bit")]
use crate::util::metadata::vo_bit;
use crate::util::object_enum::BlockMayHaveObjects;
use crate::util::Address;
use crate::util::{constants::*, OpaquePointer, VMThread};
use crate::vm::*;
use std::sync::atomic::{AtomicU8, Ordering};

/// The block allocation state.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BlockState {
    /// the block is not allocated.
    Unallocated,
    /// the block is allocated but not marked.
    Unmarked,
    /// the block is allocated and marked.
    Marked,
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
}

impl From<u8> for BlockState {
    fn from(state: u8) -> Self {
        match state {
            Self::MARK_UNALLOCATED => BlockState::Unallocated,
            Self::MARK_UNMARKED => BlockState::Unmarked,
            Self::MARK_MARKED => BlockState::Marked,
            unavailable_lines => BlockState::Reusable { unavailable_lines },
        }
    }
}

impl From<BlockState> for u8 {
    fn from(state: BlockState) -> Self {
        match state {
            BlockState::Unallocated => BlockState::MARK_UNALLOCATED,
            BlockState::Unmarked => BlockState::MARK_UNMARKED,
            BlockState::Marked => BlockState::MARK_MARKED,
            BlockState::Reusable { unavailable_lines } => {
                assert_ne!(unavailable_lines, 0);
                u8::min(unavailable_lines, u8::MAX - 4)
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
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq)]
pub struct Block(Address);

impl Region for Block {
    #[cfg(not(feature = "immix_smaller_block"))]
    const LOG_BYTES: usize = 15;
    #[cfg(feature = "immix_smaller_block")]
    const LOG_BYTES: usize = 13;

    const BPR_ALLOC_TABLE: Option<SideMetadataSpec> =
        Some(crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_ALLOC_BITS);

    fn from_aligned_address(address: Address) -> Self {
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    fn start(&self) -> Address {
        self.0
    }
}

static GLOBAL_PHASE_EPOCH: AtomicU8 = AtomicU8::new(1);

impl BlockMayHaveObjects for Block {
    fn may_have_objects(&self) -> bool {
        self.get_state() != BlockState::Unallocated
    }
}

impl Block {
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
    pub const PHASE_EPOCH: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::PHASE_EPOCH;

    /// Get the chunk containing the block.
    pub fn chunk(&self) -> Chunk {
        Chunk::from_unaligned_address(self.0)
    }

    /// Get the address range of the block's line mark table.
    #[allow(clippy::assertions_on_constants)]
    pub fn line_mark_table(&self) -> MetadataByteArrayRef<{ Block::LINES }> {
        debug_assert!(!super::BLOCK_ONLY);
        MetadataByteArrayRef::<{ Block::LINES }>::new(&Line::MARK_TABLE, self.start(), Self::BYTES)
    }

    fn try_lock(&self) -> bool {
        let result = BLOCK_IN_USE.fetch_update_atomic::<u8, _>(
            self.start(),
            Ordering::Relaxed,
            Ordering::Relaxed,
            |b| {
                if b == 1 {
                    return None;
                }
                Some(1)
            },
        );
        result == Ok(0)
    }

    pub fn try_lock_with_condition(&self, predicate: impl Fn() -> bool) -> bool {
        if !predicate() {
            return false;
        }
        let locked = self.try_lock();
        if !locked {
            return false;
        }
        if !predicate() {
            self.unlock();
            return false;
        }
        true
    }

    pub fn is_locked(&self) -> bool {
        BLOCK_IN_USE.load_atomic::<u8>(self.start(), Ordering::Relaxed) != 0
    }

    pub fn unlock(&self) {
        BLOCK_IN_USE.store_atomic::<u8>(self.start(), 0u8, Ordering::Relaxed);
    }

    pub fn get_owner(&self) -> Option<VMThread> {
        let ptr = BLOCK_OWNER.load_atomic::<usize>(self.start(), Ordering::Relaxed);
        if ptr == 0 {
            None
        } else {
            Some(VMThread(OpaquePointer::from_mut_ptr(ptr as *mut ())))
        }
    }

    pub fn set_owner(&self, owner: Option<VMThread>) {
        let ptr = if let Some(owner) = owner {
            owner.0.to_address().as_usize()
        } else {
            0
        };
        BLOCK_OWNER.store_atomic(self.start(), ptr, Ordering::Relaxed);
    }

    /// The global phase epoch.
    /// This counter is bumped by one at the end of every mutator and GC phase.
    /// Any block matching this epoch are used for allocation in the current phase.
    pub fn global_phase_epoch() -> u8 {
        GLOBAL_PHASE_EPOCH.load(Ordering::Relaxed)
    }

    /// Get the current block phase epoch.
    /// This indicates the last phase that this block is used for object allocation.
    /// Either as a clean block or a partially-free block.
    ///
    /// Odd epoch means the block is in a mutator phase.
    /// Even epoch means the block is allocated in a GC phase.
    pub fn phase_epoch(&self) -> u8 {
        Self::PHASE_EPOCH.load_atomic::<u8>(self.start(), Ordering::Relaxed)
    }

    pub fn update_phase_epoch(&self) {
        Self::PHASE_EPOCH.store_atomic::<u8>(
            self.start(),
            Self::global_phase_epoch(),
            Ordering::Relaxed,
        );
    }

    pub fn is_reusing(&self) -> bool {
        self.get_state() != BlockState::Unallocated && self.is_nursery_or_reusing()
    }

    pub fn is_gc_reusing(&self) -> bool {
        if self.get_state() == BlockState::Unallocated {
            return false;
        }
        let ge = Self::global_phase_epoch();
        assert_eq!(ge & 1, 0);
        let e = self.phase_epoch();
        e == ge
    }

    pub fn is_nursery(&self) -> bool {
        self.get_state() == BlockState::Unallocated && self.is_nursery_or_reusing()
    }

    pub fn is_nursery_or_reusing(&self) -> bool {
        let ge = Self::global_phase_epoch();
        let e = self.phase_epoch();
        if (ge & 1) == 1 {
            return e == ge;
        } else {
            return e == ge - 1;
        }
    }

    pub fn update_global_phase_epoch<VM: VMBinding>(space: &ImmixSpace<VM>) {
        let old = GLOBAL_PHASE_EPOCH.load(Ordering::SeqCst);
        if old == 254 {
            GLOBAL_PHASE_EPOCH.store(1, Ordering::SeqCst);
            space.pr.reset_nursery_state();
        } else {
            GLOBAL_PHASE_EPOCH.store(old + 1, Ordering::SeqCst);
        }
    }

    pub fn is_reusable(&self) -> bool {
        if self.is_defrag_source() {
            return false;
        }
        self.get_state().is_reusable()
    }

    /// Get block mark state.
    pub fn get_state(&self) -> BlockState {
        let byte = Self::MARK_TABLE.load_atomic::<u8>(self.start(), Ordering::SeqCst);
        byte.into()
    }

    /// Set block mark state.
    pub fn set_state(&self, state: BlockState) {
        let state = u8::from(state);
        Self::MARK_TABLE.store_atomic::<u8>(self.start(), state, Ordering::SeqCst);
    }

    // Defrag byte

    const DEFRAG_SOURCE_STATE: u8 = u8::MAX;

    /// Test if the block is marked for defragmentation.
    pub fn is_defrag_source(&self) -> bool {
        let byte = Self::DEFRAG_STATE_TABLE.load_byte(self.start());
        // The byte should be 0 (not defrag source) or 255 (defrag source) if this is a major defrag GC, as we set the values in PrepareBlockState.
        // But it could be any value in a nursery GC.
        byte != 0
    }

    /// Mark the block for defragmentation.
    pub fn set_as_defrag_source(&self, defrag: bool) {
        let byte = if defrag { Self::DEFRAG_SOURCE_STATE } else { 0 };
        Self::DEFRAG_STATE_TABLE.store_atomic::<u8>(self.start(), byte, Ordering::SeqCst);
    }

    /// Record the number of holes in the block.
    pub fn set_holes(&self, holes: usize) {
        Self::DEFRAG_STATE_TABLE.store_atomic::<u8>(self.start(), holes as u8, Ordering::SeqCst);
    }

    /// Get the number of holes.
    pub fn get_holes(&self) -> usize {
        let byte = Self::DEFRAG_STATE_TABLE.load_atomic::<u8>(self.start(), Ordering::SeqCst);
        debug_assert_ne!(byte, Self::DEFRAG_SOURCE_STATE);
        byte as usize
    }

    /// Initialize a clean block after acquired from page-resource.
    pub fn init(&self, copy: bool, reuse: bool) {
        self.update_phase_epoch();
        self.set_state(if copy {
            BlockState::Marked
        } else {
            BlockState::Unmarked
        });
        if !reuse {
            Self::DEFRAG_STATE_TABLE.store_atomic::<u8>(self.start(), 0, Ordering::SeqCst);
        }
    }

    /// Deinitalize a block before releasing.
    pub fn deinit(&self) {
        self.set_state(BlockState::Unallocated);
    }

    pub fn start_line(&self) -> Line {
        Line::from_aligned_address(self.start())
    }

    pub fn end_line(&self) -> Line {
        Line::from_aligned_address(self.end())
    }

    /// Get the range of lines within the block.
    #[allow(clippy::assertions_on_constants)]
    pub fn lines(&self) -> RegionIterator<Line> {
        debug_assert!(!super::BLOCK_ONLY);
        RegionIterator::<Line>::new(self.start_line(), self.end_line())
    }

    /// Sweep this block.
    /// Return true if the block is swept.
    pub fn sweep<VM: VMBinding>(
        &self,
        space: &ImmixSpace<VM>,
        mark_histogram: &mut Histogram,
        line_mark_state: Option<u8>,
    ) -> bool {
        self.set_as_defrag_source(false);
        if super::BLOCK_ONLY {
            match self.get_state() {
                BlockState::Unallocated => false,
                BlockState::Unmarked => {
                    #[cfg(feature = "vo_bit")]
                    vo_bit::helper::on_region_swept::<VM, _>(self, false);
                    unimplemented!();
                }
                BlockState::Marked => {
                    #[cfg(feature = "vo_bit")]
                    vo_bit::helper::on_region_swept::<VM, _>(self, true);

                    // The block is live.
                    false
                }
                _ => unreachable!(),
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
                    // We need to clear the line mark state at least twice in every 128 GC
                    // otherwise, the line mark state of the last GC will stick around
                    if line_mark_state > Line::MAX_MARK_STATE - 2 {
                        line.mark(0);
                    }
                    #[cfg(feature = "immix_zero_on_release")]
                    crate::util::memory::zero(line.start(), Line::BYTES);

                    // We need to clear the pin bit if it is on the side, as this line can be reused
                    #[cfg(feature = "object_pinning")]
                    if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_PINNING_BIT_SPEC {
                        side.bzero_metadata(line.start(), Line::BYTES);
                    }

                    prev_line_is_marked = false;
                }
            }

            if marked_lines == 0 {
                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_region_swept::<VM, _>(self, false);

                // Release the block if non of its lines are marked.
                space.release_block(*self);
                true
            } else {
                // There are some marked lines. Keep the block live.
                if marked_lines != Block::LINES {
                    // There are holes. Mark the block as reusable.
                    self.set_state(BlockState::Reusable {
                        unavailable_lines: usize::min(marked_lines, u8::MAX as usize) as _,
                    });
                } else {
                    // Clear mark state.
                    self.set_state(BlockState::Unmarked);
                }
                // Update mark_histogram
                mark_histogram[holes] += marked_lines;
                // Record number of holes in block side metadata.
                self.set_holes(holes);

                #[cfg(feature = "vo_bit")]
                vo_bit::helper::on_region_swept::<VM, _>(self, true);

                false
            }
        }
    }

    /// Clear VO bits metadata for unmarked regions.
    /// This is useful for clearing VO bits during nursery GC for StickyImmix
    /// at which time young objects (allocated in unmarked regions) may die
    /// but we always consider old objects (in marked regions) as live.
    #[cfg(feature = "vo_bit")]
    pub fn clear_vo_bits_for_unmarked_regions(&self, line_mark_state: Option<u8>) {
        match line_mark_state {
            None => {
                match self.get_state() {
                    BlockState::Unmarked => {
                        // It may contain young objects.  Clear it.
                        vo_bit::bzero_vo_bit(self.start(), Self::BYTES);
                    }
                    BlockState::Marked => {
                        // It contains old objects.  Skip it.
                    }
                    _ => unreachable!(),
                }
            }
            Some(state) => {
                // With lines.
                for line in self.lines() {
                    if !line.is_marked(state) {
                        // It may contain young objects.  Clear it.
                        vo_bit::bzero_vo_bit(line.start(), Line::BYTES);
                    }
                }
            }
        }
    }
}

/// A non-block single-linked list to store blocks.
pub struct ReusableBlockPool {
    queue: BlockPool<Block>,
    num_workers: usize,
}

#[allow(unused)]
impl ReusableBlockPool {
    /// Create empty block list
    pub fn new(num_workers: usize) -> Self {
        Self {
            queue: BlockPool::new(num_workers),
            num_workers,
        }
    }

    /// Get number of blocks in this list.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Add a block to the list.
    pub fn push(&self, block: Block) {
        self.queue.push(block)
    }

    /// Pop a block out of the list.
    pub fn pop(&self) -> Option<Block> {
        self.queue.pop()
    }

    /// Clear the list.
    pub fn reset(&mut self) {
        self.queue = BlockPool::new(self.num_workers);
    }

    /// Iterate all the blocks in the queue. Call the visitor for each reported block.
    pub fn iterate_blocks(&self, mut f: impl FnMut(Block)) {
        self.queue.iterate_blocks(&mut f);
    }

    /// Flush the block queue
    pub fn flush_all(&self) {
        self.queue.flush_all();
    }
}
