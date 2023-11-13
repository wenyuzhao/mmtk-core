use atomic::Ordering;

use super::allocator::{align_allocation_no_fill, fill_alignment_gap};
use super::BumpPointer;
use crate::plan::Plan;
use crate::policy::immix::block::{Block, BlockState};
use crate::policy::immix::line::*;
use crate::policy::immix::ImmixSpace;
use crate::policy::space::Space;
use crate::util::alloc::allocator::get_maximum_aligned_size;
use crate::util::alloc::Allocator;
use crate::util::linear_scan::Region;
use crate::util::metadata::side_metadata::spec_defs::{BLOCK_IN_USE, BLOCK_OWNER};
use crate::util::opaque_pointer::VMThread;
use crate::util::Address;
use crate::vm::*;

/// Immix allocator
#[repr(C)]
pub struct ImmixAllocator<VM: VMBinding> {
    pub tls: VMThread,
    pub(in crate::util::alloc) bump_pointer: BumpPointer,
    /// [`Space`](src/policy/space/Space) instance associated with this allocator instance.
    space: &'static ImmixSpace<VM>,
    /// [`Plan`] instance that this allocator instance is associated with.
    plan: &'static dyn Plan<VM = VM>,
    /// *unused*
    hot: bool,
    /// Is this a copy allocator?
    copy: bool,
    /// Bump pointer for large objects
    pub(in crate::util::alloc) large_bump_pointer: BumpPointer,
    /// Is the current request for large or small?
    request_for_large: bool,
    /// Hole-searching cursor
    line: Option<Line>,
    block: Option<Block>,
    mutator_recycled_blocks: Box<Vec<Block>>,
    local_clean_blocks: Box<Vec<Block>>,
    local_reuse_blocks: Box<Vec<Block>>,
    local_clean_blocks_cursor: usize,
    local_clean_blocks_cursor_boundary: usize,
    local_reuse_blocks_cursor: usize,
    local_reuse_blocks_cursor_boundary: usize,
    mutator_recycled_lines: usize,
    retry: bool,
}

impl<VM: VMBinding> ImmixAllocator<VM> {
    pub fn reset(&mut self) {
        if !self.copy {
            *self.local_clean_blocks = self
                .local_clean_blocks
                .iter()
                .filter(|b| {
                    b.get_state() == BlockState::Unallocated
                        && BLOCK_OWNER.load_atomic::<usize>(b.start(), Ordering::SeqCst)
                            == self.tls.0.to_address().as_usize()
                })
                .cloned()
                .collect();
            self.local_clean_blocks_cursor_boundary = self.local_clean_blocks.len();

            *self.local_reuse_blocks = self
                .local_reuse_blocks
                .iter()
                .filter(|b| {
                    b.get_state() != BlockState::Unallocated
                        && BLOCK_OWNER.load_atomic::<usize>(b.start(), Ordering::SeqCst)
                            == self.tls.0.to_address().as_usize()
                })
                .cloned()
                .collect();
            self.local_reuse_blocks_cursor_boundary = self.local_reuse_blocks.len();
        }
        // println!(
        //     "reset copy={:?} tls={:?} {} {:?}",
        //     self.copy,
        //     self.tls.0.to_address(),
        //     self.local_clean_blocks_cursor_boundary,
        //     if !self.copy {
        //         &self.local_clean_blocks as &[Block]
        //     } else {
        //         &[]
        //     }
        // );
        if let Some(b) = self.block {
            self.retire_block_impl(b)
        }
        self.retire_large_block();
        self.bump_pointer.reset(Address::ZERO, Address::ZERO);
        self.large_bump_pointer.reset(Address::ZERO, Address::ZERO);
        self.request_for_large = false;
        self.line = None;
        if self.copy {
            // println!("copy allocator reset");
            self.local_clean_blocks.clear();
            self.local_reuse_blocks.clear();
        }
        self.local_clean_blocks_cursor = 0;
        self.local_reuse_blocks_cursor = 0;
    }

    fn set_allocating_block(&mut self, block: Block) {
        if let Some(b) = self.block {
            self.retire_block_impl(b)
        }
        self.block = Some(block);
    }
    fn retire_large_block(&mut self) {
        if self.large_bump_pointer.cursor != Address::ZERO {
            let mut a = self.large_bump_pointer.cursor;
            if a == self.large_bump_pointer.limit {
                a -= 1;
            }
            self.retire_block_impl(Block::from_unaligned_address(a))
        }
    }

    fn retire_block_impl(&self, block: Block) {
        // if bump.cursor != Address::ZERO {
        //     let mut a = bump.cursor;
        //     if a == bump.limit {
        //         a -= 1;
        //     }
        // }

        BLOCK_IN_USE.store_atomic(block.start(), 0u8, Ordering::SeqCst);
    }

    // fn retry_alloc_slow_hot(&mut self, size: usize, align: usize, offset: usize) -> Address {
    //     if cfg!(feature = "ix_retry_small_object_alloc_small_only")
    //         && get_maximum_aligned_size::<VM>(size, align) > Line::BYTES
    //     {
    //         return Address::ZERO;
    //     }
    //     if self.acquire_recyclable_lines(size, align, offset) {
    //         let result = align_allocation_no_fill::<VM>(self.bump_pointer.cursor, align, offset);
    //         let new_cursor = result + size;
    //         if new_cursor > self.bump_pointer.limit {
    //             Address::ZERO
    //         } else {
    //             fill_alignment_gap::<VM>(self.bump_pointer.cursor, result);
    //             self.bump_pointer.cursor = new_cursor;
    //             result
    //         }
    //     } else {
    //         Address::ZERO
    //     }
    // }
}

impl<VM: VMBinding> Allocator<VM> for ImmixAllocator<VM> {
    fn get_space(&self) -> &'static dyn Space<VM> {
        self.space as _
    }

    fn get_plan(&self) -> &'static dyn Plan<VM = VM> {
        self.plan
    }

    fn does_thread_local_allocation(&self) -> bool {
        true
    }

    fn get_thread_local_buffer_granularity(&self) -> usize {
        crate::policy::immix::block::Block::BYTES
    }

    fn alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        // debug_assert!(
        //     size <= crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
        //     "Trying to allocate a {} bytes object, which is larger than MAX_IMMIX_OBJECT_SIZE {}",
        //     size,
        //     crate::policy::immix::MAX_IMMIX_OBJECT_SIZE
        // );
        let result = align_allocation_no_fill::<VM>(self.bump_pointer.cursor, align, offset);
        let new_cursor = result + size;

        if new_cursor > self.bump_pointer.limit {
            trace!(
                "{:?}: Thread local buffer used up, go to alloc slow path",
                self.tls
            );
            if (self.copy || !crate::args().no_mutator_line_recycling)
                && get_maximum_aligned_size::<VM>(size, align) > Line::BYTES
            {
                // Size larger than a line: do large allocation
                self.overflow_alloc(size, align, offset)
            } else {
                // Size smaller than a line: fit into holes
                self.alloc_slow_hot(size, align, offset)
            }
        } else {
            // Simple bump allocation.
            fill_alignment_gap::<VM>(self.bump_pointer.cursor, result);
            self.bump_pointer.cursor = new_cursor;
            trace!(
                "{:?}: Bump allocation size: {}, result: {}, new_cursor: {}, limit: {}",
                self.tls,
                size,
                result,
                self.bump_pointer.cursor,
                self.bump_pointer.limit
            );
            result
        }
    }

    /// Acquire a clean block from ImmixSpace for allocation.
    fn alloc_slow_once(&mut self, size: usize, align: usize, offset: usize) -> Address {
        trace!("{:?}: alloc_slow_once", self.tls);
        // if cfg!(feature = "ix_retry_small_object_alloc") {
        //     let result = self.retry_alloc_slow_hot(size, align, offset);
        //     if !result.is_zero() {
        //         return result;
        //     }
        // }
        self.acquire_clean_block(size, align, offset)
    }

    /// This is called when precise stress is used. We try use the thread local buffer for
    /// the allocation (after restoring the correct limit for thread local buffer). If we cannot
    /// allocate from thread local buffer, we will go to the actual slowpath. After allocation,
    /// we will set the fake limit so future allocations will fail the slowpath and get here as well.
    fn alloc_slow_once_precise_stress(
        &mut self,
        _size: usize,
        _align: usize,
        _offset: usize,
        _need_poll: bool,
    ) -> Address {
        unreachable!()
    }

    fn get_tls(&self) -> VMThread {
        self.tls
    }
}

impl<VM: VMBinding> ImmixAllocator<VM> {
    pub fn new(
        tls: VMThread,
        space: Option<&'static dyn Space<VM>>,
        plan: &'static dyn Plan<VM = VM>,
        copy: bool,
    ) -> Self {
        ImmixAllocator {
            tls,
            space: space.unwrap().downcast_ref::<ImmixSpace<VM>>().unwrap(),
            plan,
            bump_pointer: BumpPointer::new(Address::ZERO, Address::ZERO),
            hot: false,
            copy,
            large_bump_pointer: BumpPointer::new(Address::ZERO, Address::ZERO),
            request_for_large: false,
            line: None,
            block: None,
            mutator_recycled_blocks: Box::new(vec![]),
            mutator_recycled_lines: 0,
            local_clean_blocks: Box::new(vec![]),
            local_reuse_blocks: Box::new(vec![]),
            local_clean_blocks_cursor: 0,
            local_clean_blocks_cursor_boundary: 0,
            local_reuse_blocks_cursor: 0,
            local_reuse_blocks_cursor_boundary: 0,
            retry: false,
        }
    }

    pub fn flush(&mut self) {}

    pub fn immix_space(&self) -> &'static ImmixSpace<VM> {
        self.space
    }

    /// Large-object (larger than a line) bump allocation.
    fn overflow_alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        trace!("{:?}: overflow_alloc", self.tls);
        let start = align_allocation_no_fill::<VM>(self.large_bump_pointer.cursor, align, offset);
        let end = start + size;
        if end > self.large_bump_pointer.limit {
            self.request_for_large = true;
            let rtn = self.alloc_slow_inline(size, align, offset);
            self.request_for_large = false;
            rtn
        } else {
            fill_alignment_gap::<VM>(self.large_bump_pointer.cursor, start);
            self.large_bump_pointer.cursor = end;
            start
        }
    }

    /// Bump allocate small objects into recyclable lines (i.e. holes).
    fn alloc_slow_hot(&mut self, size: usize, align: usize, offset: usize) -> Address {
        trace!("{:?}: alloc_slow_hot", self.tls);
        if self.acquire_recyclable_lines(size, align, offset) {
            self.alloc(size, align, offset)
        } else {
            self.alloc_slow_inline(size, align, offset)
        }
    }

    /// Search for recyclable lines.
    fn acquire_recyclable_lines(&mut self, size: usize, align: usize, offset: usize) -> bool {
        while self.line.is_some() || self.acquire_recyclable_block() {
            let line = self.line.unwrap();
            if let Some((start_line, end_line)) =
                self.immix_space().get_next_available_lines(self.copy, line)
            {
                // Find recyclable lines. Update the bump allocation cursor and limit.
                self.bump_pointer.cursor = start_line.start();
                self.bump_pointer.limit = end_line.start();
                trace!(
                    "{:?}: acquire_recyclable_lines -> {:?} [{:?}, {:?}) {:?}",
                    self.tls,
                    self.line,
                    start_line,
                    end_line,
                    self.tls
                );
                if self.immix_space().common().zeroed
                    && !self.copy
                    && cfg!(feature = "force_zeroing")
                {
                    crate::util::memory::zero_w(
                        self.bump_pointer.cursor,
                        self.bump_pointer.limit - self.bump_pointer.cursor,
                    );
                }
                if cfg!(feature = "prefetch") {
                    crate::util::memory::prefetch(
                        self.bump_pointer.cursor,
                        self.bump_pointer.limit - self.bump_pointer.cursor,
                    );
                }
                debug_assert!(
                    align_allocation_no_fill::<VM>(self.bump_pointer.cursor, align, offset) + size
                        <= self.bump_pointer.limit
                );
                let block = line.block();
                self.line = if end_line == block.end_line() {
                    // Hole searching reached the end of a reusable block. Set the hole-searching cursor to None.
                    None
                } else {
                    // Update the hole-searching cursor to None.
                    Some(end_line)
                };
                return true;
            } else {
                // No more recyclable lines. Set the hole-searching cursor to None.
                self.line = None;
            }
        }
        false
    }

    /// Get a recyclable block from ImmixSpace.
    fn acquire_recyclable_block(&mut self) -> bool {
        if crate::args().no_mutator_line_recycling && !self.copy {
            return false;
        }
        if crate::args().no_line_recycling {
            return false;
        }
        match self.acquire_block(false) {
            Some(block) => {
                trace!("{:?}: acquire_recyclable_block -> {:?}", self.tls, block);
                // Set the hole-searching cursor to the start of this block.

                self.line = Some(block.start_line());
                self.set_allocating_block(block);

                true
            }
            _ => false,
        }
    }

    // Get a clean block from ImmixSpace.
    fn acquire_clean_block(&mut self, size: usize, align: usize, offset: usize) -> Address {
        match self.acquire_block(true) {
            None => Address::ZERO,
            Some(block) => {
                trace!(
                    "{:?}: Acquired a new block {:?} -> {:?}",
                    self.tls,
                    block.start(),
                    block.end()
                );
                if self.request_for_large {
                    self.retire_large_block();
                    self.large_bump_pointer.cursor = block.start();
                    self.large_bump_pointer.limit = block.end();
                } else {
                    if cfg!(feature = "prefetch") {
                        crate::util::memory::prefetch(block.start(), Block::BYTES);
                    }
                    self.set_allocating_block(block);
                    self.bump_pointer.cursor = block.start();
                    self.bump_pointer.limit = block.end();
                }
                self.alloc(size, align, offset)
            }
        }
    }

    fn try_acquire_block(&mut self, clean: bool) -> Option<Block> {
        if clean {
            if !self.copy
                && self.local_clean_blocks_cursor >= self.local_clean_blocks_cursor_boundary
            {
                // These blocks can never be stolen
                if self.local_clean_blocks_cursor < self.local_clean_blocks.len() {
                    let block = self.local_clean_blocks[self.local_clean_blocks_cursor];
                    self.local_clean_blocks_cursor += 1;
                    // We must be the owner of the block
                    debug_assert_eq!(
                        BLOCK_OWNER.load_atomic::<usize>(block.start(), Ordering::Relaxed),
                        self.tls.0.to_address().as_usize()
                    );
                    // Must be an nursery block
                    debug_assert!(block.is_nursery());
                    // Must be unallocated
                    debug_assert_eq!(block.get_state(), BlockState::Unallocated);
                    self.space.initialize_new_block(block, true, self.copy);
                    return Some(block);
                }
                return None;
            }
            while self.local_clean_blocks_cursor < self.local_clean_blocks.len() {
                let block = self.local_clean_blocks[self.local_clean_blocks_cursor];
                self.local_clean_blocks_cursor += 1;
                if self.copy {
                    if block.get_state() == BlockState::Unallocated && !block.is_nursery() {
                        self.space.initialize_new_block(block, true, self.copy);
                        return Some(block);
                    } else {
                        unreachable!()
                    }
                } else {
                    if block.get_state() == BlockState::Unallocated && !block.is_nursery() {
                        let result = BLOCK_IN_USE.fetch_update_atomic::<u8, _>(
                            block.start(),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |b| {
                                if b == 1 {
                                    return None;
                                }
                                if BLOCK_OWNER
                                    .load_atomic::<usize>(block.start(), Ordering::Relaxed)
                                    != self.tls.0.to_address().as_usize()
                                {
                                    return None;
                                }
                                Some(1)
                            },
                        );
                        if result.is_ok() {
                            // println!("try_acquire_block: {:?}", block,);
                            self.space.initialize_new_block(block, true, self.copy);
                            return Some(block);
                        }
                    }
                }
            }
        } else {
            while self.local_reuse_blocks_cursor < self.local_reuse_blocks.len() {
                let block = self.local_reuse_blocks[self.local_reuse_blocks_cursor];
                self.local_reuse_blocks_cursor += 1;
                if block.get_state() == BlockState::Unallocated
                    || block.is_reusing()
                    || block.is_defrag_source()
                {
                    continue;
                }
                if self.copy {
                    if !block.is_owned_by_copy_allocator() {
                        self.space.initialize_new_block(block, false, self.copy);
                        return Some(block);
                    }
                } else {
                    self.space.initialize_new_block(block, false, self.copy);
                    return Some(block);

                    // if block.get_state() == BlockState::Unallocated && !block.is_nursery() {
                    //     let result = BLOCK_IN_USE.fetch_update_atomic::<u8, _>(
                    //         block.start(),
                    //         Ordering::Relaxed,
                    //         Ordering::Relaxed,
                    //         |b| {
                    //             if b == 1 {
                    //                 return None;
                    //             }
                    //             if BLOCK_OWNER
                    //                 .load_atomic::<usize>(block.start(), Ordering::Relaxed)
                    //                 != self.tls.0.to_address().as_usize()
                    //             {
                    //                 return None;
                    //             }
                    //             Some(1)
                    //         },
                    //     );
                    //     if result.is_ok() {
                    //         // println!("try_acquire_block: {:?}", block,);
                    //         self.space.initialize_new_block(block, true, self.copy);
                    //         return Some(block);
                    //     }
                    // }
                }
            }
            //     while self.local_reuse_blocks_cursor < self.local_reuse_blocks.len() {
            //         let block = self.local_reuse_blocks[self.local_reuse_blocks_cursor];
            //         self.local_reuse_blocks_cursor += 1;
            //         let state = block.get_state();
            //         if state != BlockState::Unallocated
            //             && !block.is_nursery_or_reusing()
            //             && !block.is_defrag_source()
            //         {
            //             self.space.initialize_new_block(block, false, self.copy);
            //             return Some(block);
            //         }
            //     }
            // }
        }
        None
    }

    fn acquire_block(&mut self, clean: bool) -> Option<Block> {
        // Clean blocks: Check for GC
        if clean {
            self.space
                .get_clean_block_logically(self.tls, self.copy)
                .ok()?;
        }
        loop {
            // Try find a block
            if let Some(block) = self.try_acquire_block(clean) {
                return Some(block);
            }
            // Pull N blocks from page resource
            let result = if clean {
                self.space.acquire_blocks(
                    32,
                    16,
                    clean,
                    &mut self.local_clean_blocks,
                    self.copy,
                    self.tls.0.to_address().as_usize(),
                )
            } else {
                self.space.acquire_blocks(
                    32,
                    16,
                    clean,
                    &mut self.local_reuse_blocks,
                    self.copy,
                    self.tls.0.to_address().as_usize(),
                )
            };
            if !result {
                return None;
            }
            // Search for the block again
            if let Some(b) = self.try_acquire_block(clean) {
                return Some(b);
            }
        }
    }

    /// Return whether the TLAB has been exhausted and we need to acquire a new block. Assumes that
    /// the buffer limits have been restored using [`ImmixAllocator::restore_limit_for_stress`].
    /// Note that this function may implicitly change the limits of the allocator.
    fn require_new_block(&mut self, size: usize, align: usize, offset: usize) -> bool {
        let result = align_allocation_no_fill::<VM>(self.bump_pointer.cursor, align, offset);
        let new_cursor = result + size;
        let insufficient_space = new_cursor > self.bump_pointer.limit;

        // We want this function to behave as if `alloc()` has been called. Hence, we perform a
        // size check and then return the conditions where `alloc_slow_inline()` would be called
        // in an `alloc()` call, namely when both `overflow_alloc()` and `alloc_slow_hot()` fail
        // to service the allocation request
        if insufficient_space && get_maximum_aligned_size::<VM>(size, align) > Line::BYTES {
            let start =
                align_allocation_no_fill::<VM>(self.large_bump_pointer.cursor, align, offset);
            let end = start + size;
            end > self.large_bump_pointer.limit
        } else {
            // We try to acquire recyclable lines here just like `alloc_slow_hot()`
            insufficient_space && !self.acquire_recyclable_lines(size, align, offset)
        }
    }

    /// Set fake limits for the bump allocation for stress tests. The fake limit is the remaining
    /// thread local buffer size, which should be always smaller than the bump cursor. This method
    /// may be reentrant. We need to check before setting the values.
    fn set_limit_for_stress(&mut self) {
        if self.bump_pointer.cursor < self.bump_pointer.limit {
            let old_limit = self.bump_pointer.limit;
            let new_limit =
                unsafe { Address::from_usize(self.bump_pointer.limit - self.bump_pointer.cursor) };
            self.bump_pointer.limit = new_limit;
            trace!(
                "{:?}: set_limit_for_stress. normal c {} l {} -> {}",
                self.tls,
                self.bump_pointer.cursor,
                old_limit,
                new_limit,
            );
        }

        if self.large_bump_pointer.cursor < self.large_bump_pointer.limit {
            let old_lg_limit = self.large_bump_pointer.limit;
            let new_lg_limit = unsafe {
                Address::from_usize(self.large_bump_pointer.limit - self.large_bump_pointer.cursor)
            };
            self.large_bump_pointer.limit = new_lg_limit;
            trace!(
                "{:?}: set_limit_for_stress. large c {} l {} -> {}",
                self.tls,
                self.large_bump_pointer.cursor,
                old_lg_limit,
                new_lg_limit,
            );
        }
    }

    /// Restore the real limits for the bump allocation so we can properly do a thread local
    /// allocation. The fake limit is the remaining thread local buffer size, and we restore the
    /// actual limit from the size and the cursor. This method may be reentrant. We need to check
    /// before setting the values.
    fn restore_limit_for_stress(&mut self) {
        if self.bump_pointer.limit < self.bump_pointer.cursor {
            let old_limit = self.bump_pointer.limit;
            let new_limit = self.bump_pointer.cursor + self.bump_pointer.limit.as_usize();
            self.bump_pointer.limit = new_limit;
            trace!(
                "{:?}: restore_limit_for_stress. normal c {} l {} -> {}",
                self.tls,
                self.bump_pointer.cursor,
                old_limit,
                new_limit,
            );
        }

        if self.large_bump_pointer.limit < self.large_bump_pointer.cursor {
            let old_lg_limit = self.large_bump_pointer.limit;
            let new_lg_limit =
                self.large_bump_pointer.cursor + self.large_bump_pointer.limit.as_usize();
            self.large_bump_pointer.limit = new_lg_limit;
            trace!(
                "{:?}: restore_limit_for_stress. large c {} l {} -> {}",
                self.tls,
                self.large_bump_pointer.cursor,
                old_lg_limit,
                new_lg_limit,
            );
        }
    }
}
