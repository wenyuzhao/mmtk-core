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
    block_cursor: AtomicUsize,
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
        // println!("alloc_pages: {:?}", required_pages);
        let chunks = self.chunks.read().unwrap();
        if let Some(block) = self.alloc_pages_fast(&*chunks) {
            // println!("alloc_pages1: {:?} -> {:?}", required_pages, block.start());
            self.commit_pages(reserved_pages, required_pages, tls);
            return Ok(PRAllocResult {
                start: block.start(),
                pages: required_pages,
                new_chunk: false,
            });
        }
        // Slow path
        std::mem::drop(chunks);
        let mut chunks = self.chunks.write().unwrap();
        if let Some(block) = self.alloc_pages_fast(&*chunks) {
            // println!("alloc_pages2: {:?} -> {:?}", required_pages, block.start());
            self.commit_pages(reserved_pages, required_pages, tls);
            return Ok(PRAllocResult {
                start: block.start(),
                pages: required_pages,
                new_chunk: false,
            });
        }
        assert_eq!(
            self.block_cursor.load(Ordering::Relaxed),
            chunks.len() * Self::BLOCKS_IN_CHUNK
        );
        // 1. Get a new chunk
        let chunk = self.alloc_chunk(space).ok_or(PRAllocFail)?;
        // 2. Take the first block int the chunk as the allocation result
        let first_block: Address = chunk.start();
        // 3. Add the chunk to the chunk list
        let total_blocks = chunks.len() * Self::BLOCKS_IN_CHUNK;
        chunks.push(chunk);
        self.block_cursor.store(total_blocks + 1, Ordering::SeqCst);
        // Finish slow-allocation
        self.commit_pages(reserved_pages, required_pages, tls);
        // println!("alloc_pages3: {:?} -> {:?}", required_pages, first_block);
        Result::Ok(PRAllocResult {
            start: first_block,
            pages: required_pages,
            new_chunk: true,
        })
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

    fn append_local_metadata(_metadata: &mut SideMetadataContext) {
        // metadata.local.push(CHUNK_LIVE_BLOCKS);
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
            block_cursor: AtomicUsize::new(0),
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
            block_cursor: AtomicUsize::new(0),
            _p: PhantomData,
        }
    }

    fn alloc_pages_fast(&self, chunks: &Vec<Chunk>) -> Option<B> {
        // linear scan the chunks to find a reusable block
        let max_b_index = chunks.len() << (Chunk::LOG_BYTES - B::LOG_BYTES);
        let result;
        loop {
            std::hint::spin_loop();
            let mut b_index = self.block_cursor.load(Ordering::Relaxed);
            let original_b_index = b_index;
            let mut next_b_index = None;
            while b_index < max_b_index {
                // println!("b_index: {:?} < {}", b_index, max_b_index);
                let c_index = b_index >> (Chunk::LOG_BYTES - B::LOG_BYTES);
                let chunk = chunks[c_index];
                let block = B::from_aligned_address(
                    chunk.start() + ((b_index & (Self::BLOCKS_IN_CHUNK - 1)) << B::LOG_BYTES),
                );
                if Block::from_aligned_address(block.start()).get_state() == BlockState::Unallocated
                {
                    // println!("selected: {:?} < {}", b_index, max_b_index);
                    next_b_index = Some(b_index + 1);
                    break;
                }
                b_index += 1;
            }
            if next_b_index.is_none() {
                return None;
            }
            let r = self.block_cursor.compare_exchange_weak(
                original_b_index,
                next_b_index.unwrap(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            if r.is_ok() {
                result = next_b_index;
                break;
            } else {
                // println!("compare_exchange_weak failed {} -> {}")
            }
        }
        if let Some(next_b_index) = result {
            assert!(next_b_index > 0);
            let prev_b_index = next_b_index - 1;
            let c_index = prev_b_index >> (Chunk::LOG_BYTES - B::LOG_BYTES);
            let chunk = chunks[c_index];
            let block = B::from_aligned_address(
                chunk.start() + ((prev_b_index & (Self::BLOCKS_IN_CHUNK - 1)) << B::LOG_BYTES),
            );
            // println!("alloc_pages_fast: {:?}", block.start());
            return Some(block);
        }
        None
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
        // println!("chunk: {:?}", start);
        Some(Chunk::from_aligned_address(start))
    }

    pub fn flush_all(&self) {}

    pub fn available_pages(&self) -> usize {
        let total = self.total_chunks.load(Ordering::SeqCst)
            << (LOG_BYTES_IN_CHUNK - LOG_BYTES_IN_PAGE as usize);
        total.saturating_sub(self.reserved_pages())
    }

    pub fn bulk_release_blocks(&self, count: usize) {
        let pages = count << Self::LOG_PAGES;
        debug_assert!(pages as usize <= self.common().accounting.get_committed_pages());
        self.common().accounting.release(pages as _);
    }

    pub fn reset(&self) {
        let _guard = self.chunks.write().unwrap();
        self.block_cursor.store(0, Ordering::SeqCst);
    }
}
