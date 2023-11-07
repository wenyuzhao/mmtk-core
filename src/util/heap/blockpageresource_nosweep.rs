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
use crate::util::metadata::side_metadata::spec_defs::BLOCK_IN_TLS;
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
        metadata.local.push(BLOCK_IN_TLS);
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
            reuse_block_cursor: AtomicUsize::new(0),
            clean_block_cursor_before_gc: AtomicUsize::new(0),
            reuse_block_cursor_before_gc: AtomicUsize::new(0),
            _p: PhantomData,
        }
    }

    fn acquire_blocks_fast(
        &self,
        count: usize,
        buf: &mut Vec<B>,
        chunks: &Vec<Chunk>,
        copy: bool,
    ) -> bool {
        // linear scan the chunks to find a reusable block
        let max_b_index = chunks.len() << (Chunk::LOG_BYTES - B::LOG_BYTES);
        let b_index = self.reuse_block_cursor.load(Ordering::Relaxed);
        // Bail out if we don't have any blocks to allocate
        if b_index >= max_b_index {
            return false;
        }
        // Grab 1~N Blocks
        let old = self
            .reuse_block_cursor
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                if c >= max_b_index {
                    None
                } else {
                    Some(usize::min(c + count, max_b_index))
                }
            });
        if let Ok(old) = old {
            for i in old..usize::min(old + count, max_b_index) {
                let c_index = i >> (Chunk::LOG_BYTES - B::LOG_BYTES);
                let chunk = chunks[c_index];
                let block = B::from_aligned_address(
                    chunk.start() + ((i & (Self::BLOCKS_IN_CHUNK - 1)) << B::LOG_BYTES),
                );
                if !copy {
                    if BLOCK_IN_TLS.load_atomic::<u8>(block.start(), Ordering::Relaxed) != 0 {
                        continue;
                    }
                    BLOCK_IN_TLS.store_atomic(block.start(), 1u8, Ordering::SeqCst);
                }
                buf.push(block);
            }
            true
        } else {
            false
        }
    }

    pub fn acquire_blocks(
        &self,
        count: usize,
        clean: bool,
        buf: &mut Vec<B>,
        space: &dyn Space<VM>,
        copy: bool,
    ) -> bool {
        let chunks = self.chunks.read().unwrap();
        if !clean {
            return self.acquire_blocks_fast(count, buf, &*chunks, copy);
        }
        if self.acquire_blocks_fast(count, buf, &*chunks, copy) {
            return true;
        }
        // Slow path
        // println!("slow path");
        std::mem::drop(chunks);
        let mut chunks = self.chunks.write().unwrap();
        if self.acquire_blocks_fast(count, buf, &*chunks, copy) {
            return true;
        }
        // assert_eq!(
        //     self.clean_block_cursor.load(Ordering::Relaxed),
        //     chunks.len() * Self::BLOCKS_IN_CHUNK
        // );
        // 1. Get a new chunk
        let chunk = self.alloc_chunk(space).unwrap();
        // 2. Take the first N blocks in the chunk as the allocation result
        let count = usize::min(count, Self::BLOCKS_IN_CHUNK);
        for i in 0..count {
            let block = B::from_aligned_address(
                chunk.start() + ((i & (Self::BLOCKS_IN_CHUNK - 1)) << B::LOG_BYTES),
            );
            if !copy {
                BLOCK_IN_TLS.store_atomic(block.start(), 1u8, Ordering::SeqCst);
            }
            buf.push(block);
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
        println!("chunk: {:?}", start);
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
        // let _guard = self.chunks.write().unwrap();
        // self.clean_block_cursor_before_gc.store(
        //     self.clean_block_cursor.load(Ordering::SeqCst),
        //     Ordering::SeqCst,
        // );
        // self.reuse_block_cursor_before_gc.store(
        //     self.reuse_block_cursor.load(Ordering::SeqCst),
        //     Ordering::SeqCst,
        // );
        // gc_log!(
        //     "BPR prepare {} {}",
        //     self.clean_block_cursor.load(Ordering::SeqCst),
        //     self.reuse_block_cursor.load(Ordering::SeqCst)
        // );
        self.clean_block_cursor.store(0, Ordering::SeqCst);
        self.reuse_block_cursor.store(0, Ordering::SeqCst);
    }

    pub fn reset(&self) {
        // gc_log!(
        //     "BPR reset {} {}",
        //     self.clean_block_cursor.load(Ordering::SeqCst),
        //     self.reuse_block_cursor.load(Ordering::SeqCst)
        // );
        self.clean_block_cursor.store(0, Ordering::SeqCst);
        self.reuse_block_cursor.store(0, Ordering::SeqCst);

        let t = std::time::SystemTime::now();
        let chunks = self.chunks.write().unwrap();
        let max_b_index = chunks.len() << (Chunk::LOG_BYTES - B::LOG_BYTES);
        let get_block = |i: usize| {
            let c_index = i >> (Chunk::LOG_BYTES - B::LOG_BYTES);
            let chunk = chunks[c_index];
            let block = Block::from_aligned_address(
                chunk.start() + ((i & (Self::BLOCKS_IN_CHUNK - 1)) << B::LOG_BYTES),
            );
            block
        };
        for i in 0..max_b_index {
            let b = get_block(i);
            if b.get_state() == BlockState::Nursery {
                b.set_state(BlockState::Unallocated);
            }
        }
        let ms = t.elapsed().unwrap().as_micros() as f64 / 1000.0;
        gc_log!("BPR reset took {:.3}ms ({} blocks)", ms, max_b_index);
    }
}
