use super::chunk_map::Chunk;
use super::pageresource::{PRAllocFail, PRAllocResult};
use super::{FreeListPageResource, PageResource};
use crate::policy::immix::block::{Block, BlockState};
use crate::policy::space::Space;
use crate::util::address::Address;
use crate::util::constants::*;
use crate::util::heap::layout::vm_layout::*;
use crate::util::heap::layout::VMMap;
use crate::util::heap::pageresource::CommonPageResource;
use crate::util::linear_scan::Region;
use crate::util::metadata::side_metadata::spec_defs::{BLOCK_IN_USE, BLOCK_OWNER};
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::opaque_pointer::*;
use crate::vm::*;
use atomic::Ordering;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;

/// A fast PageResource for fixed-size block allocation only.
pub struct BlockPageResource<VM: VMBinding, B: Region + 'static> {
    flpr: FreeListPageResource<VM>,
    pub(crate) total_chunks: AtomicUsize,
    chunks: RwLock<Vec<Chunk>>,
    clean_block_cursor: AtomicUsize,
    clean_block_steal_cursor: AtomicUsize,
    reuse_block_cursor: AtomicUsize,
    clean_block_cursor_before_gc: AtomicUsize,
    reuse_block_cursor_before_gc: AtomicUsize,
    _p: PhantomData<B>,
}

impl<VM: VMBinding, B: Region> PageResource<VM> for BlockPageResource<VM, B> {
    fn common(&self) -> &CommonPageResource {
        self.flpr.common()
    }

    fn common_mut(&mut self) -> &mut CommonPageResource {
        self.flpr.common_mut()
    }

    fn alloc_pages(
        &self,
        space: &dyn Space<VM>,
        reserved_pages: usize,
        required_pages: usize,
        tls: VMThread,
    ) -> Result<PRAllocResult, PRAllocFail> {
        unimplemented!()
    }

    fn get_available_physical_pages(&self) -> usize {
        self.flpr.get_available_physical_pages()
    }

    fn has_chunk_fragmentation_info(&self) -> bool {
        false
    }

    fn get_live_pages_in_chunk(&self, _: Chunk) -> usize {
        0
    }
}

impl<VM: VMBinding, B: Region> BlockPageResource<VM, B> {
    /// Block granularity in pages
    const LOG_PAGES: usize = B::LOG_BYTES - LOG_BYTES_IN_PAGE as usize;
    const BLOCKS_IN_CHUNK: usize = 1 << (Chunk::LOG_BYTES - B::LOG_BYTES);

    fn append_local_metadata(metadata: &mut SideMetadataContext) {
        metadata.local.push(BLOCK_IN_USE);
        metadata.local.push(BLOCK_OWNER);
    }

    pub fn new_contiguous(
        log_pages: usize,
        start: Address,
        bytes: usize,
        vm_map: &'static dyn VMMap,
        _num_workers: usize,
        mut metadata: SideMetadataContext,
    ) -> Self {
        assert!((1 << log_pages) <= PAGES_IN_CHUNK);
        Self::append_local_metadata(&mut metadata);
        Self {
            flpr: FreeListPageResource::new_contiguous(start, bytes, vm_map, metadata),
            total_chunks: AtomicUsize::new(0),
            chunks: RwLock::new(vec![]),
            clean_block_cursor: AtomicUsize::new(0),
            clean_block_steal_cursor: AtomicUsize::new(0),
            reuse_block_cursor: AtomicUsize::new(0),
            clean_block_cursor_before_gc: AtomicUsize::new(0),
            reuse_block_cursor_before_gc: AtomicUsize::new(0),
            _p: PhantomData,
        }
    }

    pub fn new_discontiguous(
        log_pages: usize,
        vm_map: &'static dyn VMMap,
        _num_workers: usize,
        mut metadata: SideMetadataContext,
    ) -> Self {
        assert!((1 << log_pages) <= PAGES_IN_CHUNK);
        Self::append_local_metadata(&mut metadata);
        Self {
            flpr: FreeListPageResource::new_discontiguous(vm_map, metadata),
            total_chunks: AtomicUsize::new(0),
            chunks: RwLock::new(vec![]),
            clean_block_cursor: AtomicUsize::new(0),
            clean_block_steal_cursor: AtomicUsize::new(0),
            reuse_block_cursor: AtomicUsize::new(0),
            clean_block_cursor_before_gc: AtomicUsize::new(0),
            reuse_block_cursor_before_gc: AtomicUsize::new(0),
            _p: PhantomData,
        }
    }

    fn block_index_to_block(&self, chunks: &[Chunk], i: usize) -> B {
        let c_index = i >> (Chunk::LOG_BYTES - B::LOG_BYTES);
        let chunk = chunks[c_index];
        let block = B::from_aligned_address(
            chunk.start() + ((i & (Self::BLOCKS_IN_CHUNK - 1)) << B::LOG_BYTES),
        );
        block
    }

    /// Check if a block is available for allocation
    fn block_is_available(&self, block: B, clean: bool, copy: bool, _owner: usize) -> bool {
        debug_assert!(clean);
        let state = Block::from_unaligned_address(block.start()).get_state();
        // Don't allocate into a non-empty block
        if state != BlockState::Unallocated {
            return false;
        }
        // Copy allocator: Skip young blocks in the previous mutator phase
        if copy {
            return !Block::from_unaligned_address(block.start()).is_nursery();
        }
        // Mutator allocator: Skip blocks owned by other mutators. We need to steal instead.
        let block_owner = BLOCK_OWNER.load_atomic::<usize>(block.start(), Ordering::Relaxed);
        // We only allocate clean blocks without an owner. For owned blocks, we need to steal them.
        block_owner == 0
    }

    /// Check if a block can be safely stolen from it's owner
    fn block_is_stealable(&self, block: B, clean: bool, _copy: bool, owner: usize) -> bool {
        debug_assert!(clean);
        let block = Block::from_unaligned_address(block.start());
        let state = block.get_state();
        // Don't steal non-empty blocks
        if state != BlockState::Unallocated {
            return false;
        }
        let block_owner = BLOCK_OWNER.load_atomic::<usize>(block.start(), Ordering::Relaxed);
        if block_owner == 0 || block_owner == owner {
            return false;
        }
        // Not filled by a mutator in the last mutator phase
        !block.is_nursery()
        // in_use state is not set
            && BLOCK_IN_USE.load_atomic::<u8>(block.start(), Ordering::Relaxed) == 0
    }

    // We successfully allocated or stole a block. Now add it to the local block list.
    fn append_to_buf(&self, buf: &mut Vec<B>, block: B, copy: bool, steal: bool, owner: usize) {
        if !steal {
            // Mutator-allocated clean blocks
            // println!(
            //     "Clean set owner {:?} {:?}",
            //     block.start(),
            //     owner as *const ()
            // );

            BLOCK_OWNER.store_atomic(block.start(), owner, Ordering::Relaxed);
            if !copy {
                let block = Block::from_unaligned_address(block.start());
                block.set_nursery();
            }
        }
        buf.push(block);
    }

    // Attempt to steal a block.
    fn attempt_to_steal(&self, block: B, owner: usize, copy: bool, clean: bool) -> bool {
        // Attempt to set as in-use
        let b = Block::from_unaligned_address(block.start());
        let result = BLOCK_IN_USE.fetch_update_atomic::<u8, _>(
            block.start(),
            Ordering::Relaxed,
            Ordering::Relaxed,
            |x| {
                if b.get_state() != BlockState::Unallocated || b.is_nursery() {
                    return None;
                }
                if x != 0 {
                    None
                } else {
                    Some(1)
                }
            },
        );
        if result.is_err() {
            return false;
        }
        // Set owner
        // println!(
        //     "Steal set owner {:?} {:?}",
        //     block.start(),
        //     owner as *const ()
        // );
        if !copy && clean {
            b.set_nursery();
        }
        BLOCK_OWNER.store_atomic(block.start(), owner, Ordering::Relaxed);
        // clear in-use
        BLOCK_IN_USE.store_atomic(block.start(), 0u8, Ordering::Relaxed);
        true
    }

    fn acquire_blocks_fast(
        &self,
        count: usize,
        buf: &mut Vec<B>,
        chunks: &Vec<Chunk>,
        copy: bool,
        owner: usize,
    ) -> bool {
        // linear scan the chunks to find a reusable block
        let max_b_index = chunks.len() << (Chunk::LOG_BYTES - B::LOG_BYTES);
        let b_index = self.clean_block_cursor.load(Ordering::Relaxed);
        // Bail out if we don't have any blocks to allocate
        if b_index >= max_b_index {
            return false;
        }
        // Grab 1~N Blocks
        let mut new_cursor = 0;
        let mut actual_count = 0;
        let old = self
            .clean_block_cursor
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                let mut i = c;
                let mut curr_count = 0;
                while i < max_b_index {
                    let block = self.block_index_to_block(chunks, i);
                    i += 1;
                    if self.block_is_available(block, true, copy, owner) {
                        curr_count += 1;
                        if curr_count >= count {
                            break;
                        }
                    }
                }
                new_cursor = i;
                actual_count = curr_count;  
                if i != c {
                    Some(i)
                } else {
                    None
                }
            });
        if actual_count != 0 {
            let old = old.unwrap();
            for i in old..usize::min(new_cursor, max_b_index) {
                let block = self.block_index_to_block(chunks, i);
                if self.block_is_available(block, true, copy, owner) {
                    self.append_to_buf(buf, block, copy, false, owner);
                }
            }
            true
        } else {
            false
        }
    }

    fn steal_blocks_fast(
        &self,
        count: usize,
        buf: &mut Vec<B>,
        chunks: &Vec<Chunk>,
        copy: bool,
        owner: usize,
    ) -> bool {
        // linear scan the chunks to find a reusable block
        let b_index = self.clean_block_steal_cursor.load(Ordering::Relaxed);
        // Bail out if we don't have any blocks to allocate
        if b_index == 0 || copy {
            // println!("no blocks to steal owner={:x?} copy={:?}", owner, copy);
            return false;
        }
        // Grab 1~N Blocks
        let mut new_cursor = 0;
        let mut actual_count = 0;
        let old =
            self.clean_block_steal_cursor
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    let mut i = c;
                    let mut curr_count = 0;
                    while i > 0 {
                        let block = self.block_index_to_block(chunks, i - 1);
                        i -= 1;
                        if self.block_is_stealable(block, true, copy, owner) {
                            curr_count += 1;
                            if curr_count >= count {
                                break;
                            }
                        }
                    }
                    new_cursor = i;
                    actual_count = curr_count;
                    if i != c {
                        Some(i)
                    } else {
                        None
                    }
                });
        if actual_count != 0 {
            let old = old.unwrap();
            for i in new_cursor..old {
                let block = self.block_index_to_block(chunks, i);
                if self.block_is_stealable(block, true, copy, owner) {
                    if self.attempt_to_steal(block, owner, copy, true) {
                        self.append_to_buf(buf, block, copy, true, owner);
                    }
                }
            }
            // println!("stolen some blocks owner={:x?} copy={:?}", owner, copy);
            true
        } else {
            // println!("Failed to steal some blocks owner={:x?} copy={:?}", owner, copy);
            false
        }
    }

    pub fn acquire_blocks(
        &self,
        alloc_count: usize,
        steal_count: usize,
        clean: bool,
        buf: &mut Vec<B>,
        space: &dyn Space<VM>,
        copy: bool,
        owner: usize,
    ) -> bool {
        let chunks = self.chunks.read().unwrap();
        if !clean {
            unimplemented!()
            // return self.acquire_blocks_fast(count, buf, &*chunks, copy, owner);
        }
        if self.acquire_blocks_fast(alloc_count, buf, &*chunks, copy, owner) {
            return true;
        }
        if self.steal_blocks_fast(steal_count, buf, &*chunks, copy, owner) {
            return true;
        }
        // Slow path
        // println!("slow path");
        std::mem::drop(chunks);
        let mut chunks = self.chunks.write().unwrap();
        if self.acquire_blocks_fast(alloc_count, buf, &*chunks, copy, owner) {
            return true;
        }
        if self.steal_blocks_fast(steal_count, buf, &*chunks, copy, owner) {
            return true;
        }
        // assert_eq!(
        //     self.clean_block_cursor.load(Ordering::Relaxed),
        //     chunks.len() * Self::BLOCKS_IN_CHUNK
        // );
        // 1. Get a new chunk
        let chunk = self.alloc_chunk(space).unwrap();
        // 2. Take the first N blocks in the chunk as the allocation result
        let count = usize::min(alloc_count, Self::BLOCKS_IN_CHUNK);
        for i in 0..count {
            let block = B::from_aligned_address(
                chunk.start() + ((i & (Self::BLOCKS_IN_CHUNK - 1)) << B::LOG_BYTES),
            );
            self.append_to_buf(buf, block, copy, false, owner);
        }
        // 3. Add the chunk to the chunk list
        let total_blocks = chunks.len() * Self::BLOCKS_IN_CHUNK;
        chunks.push(chunk);
        self.clean_block_cursor
            .store(total_blocks + count, Ordering::SeqCst);
        true
    }

    fn alloc_chunk(&self, space: &dyn Space<VM>) -> Option<Chunk> {
        let start = self
            .common()
            .grow_discontiguous_space(space.common().descriptor, 1);
        if start.is_zero() {
            return None;
        }
        if let Err(mmap_error) = crate::mmtk::MMAPPER
            .ensure_mapped(start, PAGES_IN_CHUNK as _)
            .and(
                self.common()
                    .metadata
                    .try_map_metadata_space(start, BYTES_IN_CHUNK),
            )
        {
            crate::util::memory::handle_mmap_error::<VM>(mmap_error, VMThread::UNINITIALIZED);
        }
        space.grow_space(start, BYTES_IN_CHUNK, true);
        self.total_chunks.fetch_add(1, Ordering::SeqCst);
        gc_log!("chunk: {:?}", start);
        Some(Chunk::from_aligned_address(start))
    }

    pub fn flush_all(&self) {}

    pub fn available_pages(&self) -> usize {
        let total = self.total_chunks.load(Ordering::SeqCst)
            << (LOG_BYTES_IN_CHUNK - LOG_BYTES_IN_PAGE as usize);
        total.saturating_sub(self.reserved_pages())
    }

    pub fn bulk_release_blocks(&self, count: usize) {
        // gc_log!("Bulk release blocks {}", count);
        let pages = count << Self::LOG_PAGES;
        debug_assert!(pages as usize <= self.common().accounting.get_committed_pages());
        self.common().accounting.release(pages as _);
        // gc_log!("Bulk release blocks {}", count);
    }

    pub fn prepare_gc(&self) {
        let chunks = self.chunks.write().unwrap();
        let max_b_index = chunks.len() << (Chunk::LOG_BYTES - B::LOG_BYTES);
        self.clean_block_cursor.store(0, Ordering::SeqCst);
        self.clean_block_steal_cursor
            .store(max_b_index, Ordering::SeqCst);
        self.reuse_block_cursor.store(0, Ordering::SeqCst);
    }

    pub fn reset(&self) {
        let chunks = self.chunks.write().unwrap();
        let max_b_index = chunks.len() << (Chunk::LOG_BYTES - B::LOG_BYTES);
        self.clean_block_cursor.store(0, Ordering::SeqCst);
        self.clean_block_steal_cursor
            .store(max_b_index, Ordering::SeqCst);
        self.reuse_block_cursor.store(0, Ordering::SeqCst);
    }

    pub fn reset_nursery_state(&self) {
        let chunks = self.chunks.write().unwrap();
        for c in &*chunks {
            Block::NURSERY_STATE_TABLE.bzero_metadata(c.start(), Chunk::BYTES);
        }
    }
}
