use super::chunk::Chunk;
use super::defrag::Histogram;
use super::line::{Line, RCArray};
use super::ImmixSpace;
use crate::util::metadata::side_metadata::{self, *};
use crate::util::{constants::*, rc};
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
                debug_assert_ne!(unavailable_lines, BlockState::MARK_REUSING);
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
    pub const LOG_BYTES: usize = {
        if cfg!(feature = "lxr_block_16k") {
            14
        } else if cfg!(feature = "lxr_block_32k") {
            15
        } else if cfg!(feature = "lxr_block_64k") {
            16
        } else if cfg!(feature = "lxr_block_128k") {
            17
        } else if cfg!(feature = "lxr_block_256k") {
            18
        } else if cfg!(feature = "lxr_block_512k") {
            19
        } else if cfg!(feature = "lxr_block_1m") {
            20
        } else {
            15
        }
    };
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
    pub const LOG_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_LOG;
    pub const DEAD_WORDS: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_BLOCK_DEAD_WORDS;

    #[inline(always)]
    fn inc_dead_bytes_sloppy(&self, bytes: usize) {
        let max_words = Self::BYTES >> LOG_BYTES_IN_WORD;
        let words = bytes >> LOG_BYTES_IN_WORD;
        let old = unsafe { side_metadata::load(&Self::DEAD_WORDS, self.start()) };
        let mut new = old + words;
        if new >= max_words {
            new = max_words - 1;
        }
        unsafe { side_metadata::store(&Self::DEAD_WORDS, self.start(), new) };
    }

    #[inline(always)]
    pub fn dec_dead_bytes_sloppy(&self, bytes: usize) {
        let words = bytes >> LOG_BYTES_IN_WORD;
        let old = unsafe { side_metadata::load(&Self::DEAD_WORDS, self.start()) };
        let new = if old <= words { 0 } else { old - words };
        unsafe { side_metadata::store(&Self::DEAD_WORDS, self.start(), new) };
    }

    #[inline(always)]
    pub fn inc_dead_bytes_sloppy_for_object<VM: VMBinding>(o: ObjectReference) {
        let block = Block::containing::<VM>(o);
        block.inc_dead_bytes_sloppy(o.get_size::<VM>());
    }

    // #[inline(always)]
    // pub fn dec_dead_bytes_sloppy_for_object<VM: VMBinding>(o: ObjectReference) {
    //     let block = Block::containing::<VM>(o);
    //     block.dec_dead_bytes_sloppy(o.get_size::<VM>());
    // }

    #[inline(always)]
    pub fn calc_dead_lines(&self) -> usize {
        let mut dead_lines = 0;
        let rc_array = RCArray::of(*self);
        // let mut skip_next_dead = false;
        for i in 0..Self::LINES {
            if rc_array.is_dead(i) {
                // if i == 0 {
                //     dead_lines += 1;
                // } else if skip_next_dead {
                //     skip_next_dead = false;
                // } else {
                //     dead_lines += 1;
                // }
                dead_lines += 1;
            } else {
                // skip_next_dead = true;
            }
        }
        dead_lines
    }

    #[inline(always)]
    pub fn calc_dead_bytes<VM: VMBinding>(&self) -> usize {
        let mut live = 0usize;
        for o in (self.start()..self.end())
            .step_by(rc::MIN_OBJECT_SIZE)
            .map(|a| unsafe { a.to_object_reference() })
        {
            let c = rc::count(o);
            if c != 0 {
                if Line::is_aligned(o.to_address()) {
                    if c == 1 && rc::is_straddle_line(Line::from(o.to_address())) {
                        continue;
                    }
                }
                let o = o.fix_start_address::<VM>();
                live += o.get_size::<VM>();
            }
        }
        if live > Self::BYTES {
            return 0;
        }
        Self::BYTES - live
    }

    #[inline(always)]
    pub fn dead_bytes(&self) -> usize {
        let v = unsafe { side_metadata::load(&Self::DEAD_WORDS, self.start()) };
        v << LOG_BYTES_IN_WORD
    }

    #[inline(always)]
    fn reset_dead_bytes(&self) {
        unsafe { side_metadata::store(&Self::DEAD_WORDS, self.start(), 0) };
    }

    pub const ZERO: Self = Self(Address::ZERO);

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

    pub fn attempt_dealloc(&self, ignore_reusing_blocks: bool) -> bool {
        self.fetch_update_state(|s| {
            if (ignore_reusing_blocks && s == BlockState::Reusing) || s == BlockState::Unallocated {
                None
            } else {
                Some(BlockState::Unallocated)
            }
        })
        .is_ok()
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

    #[inline(always)]
    pub fn address_in_defrag_block(a: Address) -> bool {
        Block::from(Block::align(a)).is_defrag_source()
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

    #[inline(always)]
    pub fn attempt_to_set_as_defrag_source(&self) -> bool {
        loop {
            let old_value = side_metadata::load_atomic(
                &Self::DEFRAG_STATE_TABLE,
                self.start(),
                Ordering::SeqCst,
            ) as u8;
            if old_value == Self::DEFRAG_SOURCE_STATE {
                return false;
            }

            if side_metadata::compare_exchange_atomic(
                &Self::DEFRAG_STATE_TABLE,
                self.start(),
                old_value as usize,
                Self::DEFRAG_SOURCE_STATE as _,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                break;
            }
        }
        true
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
    pub fn init<VM: VMBinding>(&self, copy: bool, reuse: bool, _space: &ImmixSpace<VM>) {
        #[cfg(feature = "sanity")]
        if !copy && !reuse {
            self.assert_log_table_cleared::<VM>(
                VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
            );
        }
        if !copy && reuse {
            self.set_state(BlockState::Reusing);
            debug_assert!(!self.is_defrag_source());
        } else if copy {
            if reuse {
                debug_assert!(!self.is_defrag_source());
            }
            self.set_state(if crate::args::REF_COUNT {
                BlockState::Unmarked
            } else {
                BlockState::Marked
            });
            self.set_as_defrag_source(false);
        } else {
            self.set_state(BlockState::Nursery);
            self.set_as_defrag_source(false);
        }
        if !reuse {
            self.clear_line_validity_states();
        }
    }

    /// Deinitalize a block before releasing.
    #[inline]
    pub fn deinit(&self) {
        if !crate::args::HOLE_COUNTING && crate::args::REF_COUNT {
            self.reset_dead_bytes();
        }
        #[cfg(feature = "global_alloc_bit")]
        crate::util::alloc_bit::bzero_alloc_bit(self.start(), Self::BYTES);
        self.set_state(BlockState::Unallocated);
        self.set_as_defrag_source(false);
    }

    /// Get the range of lines within the block.
    #[allow(clippy::assertions_on_constants)]
    #[inline(always)]
    pub fn lines(&self) -> Range<Line> {
        debug_assert!(!super::BLOCK_ONLY);
        Line::from(self.start())..Line::from(self.end())
    }

    // #[inline(always)]
    // pub fn clear_mark_table(&self) {
    //     side_metadata::bzero_x(&Self::MARK_TABLE, self.start(), Block::BYTES);
    // }

    #[inline(always)]
    pub fn clear_line_validity_states(&self) {
        side_metadata::bzero_x(&Line::VALIDITY_STATE, self.start(), Block::BYTES);
    }

    #[inline(always)]
    pub fn clear_rc_table<VM: VMBinding>(&self) {
        side_metadata::bzero_x(&crate::util::rc::RC_TABLE, self.start(), Block::BYTES);
    }

    #[inline(always)]
    pub fn clear_striddle_table<VM: VMBinding>(&self) {
        side_metadata::bzero_x(
            &crate::util::rc::RC_STRADDLE_LINES,
            self.start(),
            Block::BYTES,
        );
    }

    #[inline(always)]
    pub fn log(&self) -> bool {
        loop {
            let old_value =
                side_metadata::load_atomic(&Self::LOG_TABLE, self.start(), Ordering::Relaxed);
            if old_value == 1 {
                return false;
            }
            if side_metadata::compare_exchange_atomic(
                &Self::LOG_TABLE,
                self.start(),
                0,
                1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return true;
            }
        }
    }

    #[inline(always)]
    pub fn unlog(&self) {
        side_metadata::store_atomic(&Self::LOG_TABLE, self.start(), 0, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn unlog_non_atomic(&self) {
        unsafe {
            side_metadata::store(&Self::LOG_TABLE, self.start(), 0);
        }
    }

    #[inline(always)]
    pub fn clear_log_table<VM: VMBinding>(&self) {
        side_metadata::bzero_x(
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
            self.start(),
            Block::BYTES,
        );
    }

    #[inline(always)]
    pub fn assert_log_table_cleared<VM: VMBinding>(&self, meta: &SideMetadataSpec) {
        assert!(cfg!(debug_assertions) || cfg!(feature = "sanity"));
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
            let bytes = limit.offset_from(start) as usize;
            if crate::args::ENABLE_NON_TEMPORAL_MEMSET && false {
                debug_assert_eq!(bytes & ((1 << 4) - 1), 0);
                crate::util::memory::write_nt(
                    start as *mut u128,
                    bytes >> 4,
                    0xffff_ffff_ffff_ffff_ffff_ffff_ffff_ffff_u128,
                );
            } else {
                std::ptr::write_bytes(start, 0xffu8, bytes);
            }
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
    pub fn rc_sweep_nursery<VM: VMBinding>(
        &self,
        space: &ImmixSpace<VM>,
        mutator_reused_blocks: bool,
    ) -> bool {
        debug_assert!(crate::args::REF_COUNT);
        if mutator_reused_blocks {
            if self.rc_dead() {
                space.deinit_block(*self, true, true);
                return true;
            }
        }
        if !mutator_reused_blocks && self.rc_dead() {
            if self.attempt_dealloc(false) {
                space.deinit_block(*self, true, false);
                true
            } else {
                false
            }
        } else {
            // See the caller of this function.
            // At least one object is dead in the block.
            let add_as_reusable = if !*crate::args::IGNORE_REUSING_BLOCKS {
                if !self.get_state().is_reusable() && self.has_holes() {
                    self.set_state(BlockState::Reusable {
                        unavailable_lines: 1 as _,
                    });
                    true
                } else {
                    false
                }
            } else {
                let holes = if crate::args::HOLE_COUNTING {
                    self.calc_holes()
                } else {
                    1
                };
                let has_holes = self.has_holes();
                self.fetch_update_state(|s| {
                    if s == BlockState::Reusing
                        || s == BlockState::Unallocated
                        || s.is_reusable()
                        || !has_holes
                    {
                        None
                    } else {
                        Some(BlockState::Reusable {
                            unavailable_lines: holes as _,
                        })
                    }
                })
                .is_ok()
            };
            if add_as_reusable {
                debug_assert!(self.get_state().is_reusable());
                // println!("reuse N {:?}", self);
                space.reusable_blocks.push(*self)
            } else if mutator_reused_blocks {
                // debug_assert_eq!(self.get_state(), BlockState::Reusing);
                self.set_state(BlockState::Marked);
            }
            false
        }
    }

    #[inline(always)]
    pub fn attempt_mutator_reuse(&self) -> bool {
        self.fetch_update_state(|s| {
            if let BlockState::Reusable { .. } = s {
                Some(BlockState::Reusing)
            } else {
                None
            }
        })
        .is_ok()
    }

    #[inline(always)]
    pub fn rc_sweep_mature<VM: VMBinding>(&self, space: &ImmixSpace<VM>, defrag: bool) -> bool {
        debug_assert!(crate::args::REF_COUNT);
        if (!defrag && self.is_defrag_source()) || self.get_state() == BlockState::Unallocated {
            return false;
        }
        if defrag || self.rc_dead() {
            if self.attempt_dealloc(*crate::args::IGNORE_REUSING_BLOCKS) {
                space.deinit_block(*self, false, true);
                return true;
            }
        } else if !crate::args::BLOCK_ONLY {
            // See the caller of this function.
            // At least one object is dead in the block.
            let add_as_reusable = if !*crate::args::IGNORE_REUSING_BLOCKS {
                if !self.get_state().is_reusable() && self.has_holes() {
                    self.set_state(BlockState::Reusable {
                        unavailable_lines: 1 as _,
                    });
                    true
                } else {
                    false
                }
            } else {
                let holes = if crate::args::HOLE_COUNTING {
                    self.calc_holes()
                } else {
                    1
                };
                let has_holes = self.has_holes();
                self.fetch_update_state(|s| {
                    if s == BlockState::Reusing
                        || s == BlockState::Unallocated
                        || s.is_reusable()
                        || !has_holes
                    {
                        None
                    } else {
                        Some(BlockState::Reusable {
                            unavailable_lines: holes as _,
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
        false
    }

    #[inline(always)]
    pub const fn rc_table_start(&self) -> Address {
        address_to_meta_address(&crate::util::rc::RC_TABLE, self.start())
    }

    #[inline(always)]
    pub fn has_holes(&self) -> bool {
        let rc_array = RCArray::of(*self);
        let mut find_free_line = false;
        for i in 0..Self::LINES {
            if rc_array.is_dead(i) {
                if i == 0 {
                    return true;
                } else if !find_free_line {
                    find_free_line = true;
                } else {
                    return true;
                }
            } else {
                find_free_line = false;
            }
        }
        false
    }

    #[inline(always)]
    pub fn calc_holes(&self) -> usize {
        let rc_array = RCArray::of(*self);
        let search_next_hole = |start: usize| -> Option<usize> {
            // Find start
            let first_free_cursor = {
                let start_cursor = start;
                let mut first_free_cursor = None;
                let mut find_free_line = false;
                for i in start_cursor..Block::LINES {
                    if rc_array.is_dead(i) {
                        if i == 0 {
                            first_free_cursor = Some(i);
                            break;
                        } else if !find_free_line {
                            find_free_line = true;
                        } else {
                            first_free_cursor = Some(i);
                            break;
                        }
                    } else {
                        find_free_line = false;
                    }
                }
                first_free_cursor
            };
            let start = match first_free_cursor {
                Some(c) => c,
                _ => return None,
            };
            // Find limit
            let end = {
                let mut cursor = start + 1;
                while cursor < Block::LINES {
                    if !rc_array.is_dead(cursor) {
                        break;
                    }
                    cursor += 1;
                }
                cursor
            };
            Some(end)
        };
        let mut holes = 0;
        let mut cursor = 0;
        while let Some(end) = search_next_hole(cursor) {
            cursor = end;
            holes += 1;
        }
        holes
    }
}

impl Step for Block {
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
