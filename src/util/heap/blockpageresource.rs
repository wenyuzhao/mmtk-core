use super::chunk_map::Chunk;
use super::pageresource::{PRAllocFail, PRAllocResult};
use super::{FreeListPageResource, PageResource};
use crate::policy::space::Space;
use crate::util::address::Address;
use crate::util::constants::*;
use crate::util::heap::layout::vm_layout_constants::*;
use crate::util::heap::layout::VMMap;
use crate::util::heap::pageresource::CommonPageResource;
use crate::util::linear_scan::Region;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::opaque_pointer::*;
use crate::vm::*;
use atomic::{Atomic, Ordering};
use crossbeam::queue::SegQueue;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

type BlockAllocImpl<B> = BulkBlockAlloc<B>;

/// A fast PageResource for fixed-size block allocation only.
pub struct BlockPageResource<VM: VMBinding, B: Region + 'static> {
    flpr: FreeListPageResource<VM>,
    block_alloc: BlockAllocImpl<B>,
    chunk_queue: SegQueue<Chunk>,
    sync: Mutex<()>,
    pub(crate) total_chunks: AtomicUsize,
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
        if let Some((block, new_chunk)) = self.block_alloc.alloc(self, space) {
            self.commit_pages(reserved_pages, required_pages, tls);
            Ok(PRAllocResult {
                start: block.start(),
                pages: required_pages,
                new_chunk,
            })
        } else {
            Err(PRAllocFail)
        }
    }

    fn get_available_physical_pages(&self) -> usize {
        debug_assert!(self.common().contiguous);
        self.flpr.get_available_physical_pages()
    }
}

impl<VM: VMBinding, B: Region> BlockPageResource<VM, B> {
    /// Block granularity in pages
    const LOG_PAGES: usize = B::LOG_BYTES - LOG_BYTES_IN_PAGE as usize;

    fn append_local_metadata(_metadata: &mut SideMetadataContext) {}

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
            block_alloc: <BlockAllocImpl<B> as BlockAlloc<VM, B>>::new(),
            sync: Mutex::default(),
            chunk_queue: SegQueue::new(),
            total_chunks: AtomicUsize::new(0),
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
            block_alloc: <BlockAllocImpl<B> as BlockAlloc<VM, B>>::new(),
            sync: Mutex::default(),
            chunk_queue: SegQueue::new(),
            total_chunks: AtomicUsize::new(0),
            _p: PhantomData,
        }
    }

    fn alloc_chunk(&self, space: &dyn Space<VM>) -> Option<Chunk> {
        if self.common().contiguous {
            if let Some(chunk) = self.chunk_queue.pop() {
                self.total_chunks.fetch_add(1, Ordering::SeqCst);
                return Some(chunk);
            }
        }
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
        Some(Chunk::from_aligned_address(start))
    }

    fn free_chunk(&self, chunk: Chunk) {
        self.total_chunks.fetch_sub(1, Ordering::SeqCst);
        if self.common().contiguous {
            self.chunk_queue.push(chunk);
        } else {
            self.common().release_discontiguous_chunks(chunk.start());
        }
    }

    pub fn release_block(&self, block: B, single_thread: bool) {
        let pages = 1 << Self::LOG_PAGES;
        self.common().accounting.release(pages as _);
        self.block_alloc.free(self, block, single_thread);
    }

    pub fn flush_all(&self) {}

    pub fn available_pages(&self) -> usize {
        let total = self.total_chunks.load(Ordering::SeqCst)
            << (LOG_BYTES_IN_CHUNK - LOG_BYTES_IN_PAGE as usize);
        total.saturating_sub(self.reserved_pages())
    }
}

pub trait BlockAlloc<VM: VMBinding, B: Region> {
    fn new() -> Self;
    fn alloc(&self, bpr: &BlockPageResource<VM, B>, space: &dyn Space<VM>) -> Option<(B, bool)>;
    fn free(&self, bpr: &BlockPageResource<VM, B>, b: B, single_thread: bool);
}

struct LockFreeListBlockAlloc<B: Region> {
    cursor: Atomic<(u32, u32)>,
    head: Atomic<Address>,
    _p: PhantomData<B>,
}

impl<B: Region> LockFreeListBlockAlloc<B> {
    const LOG_BLOCKS_IN_CHUNK: usize = Chunk::LOG_BYTES - B::LOG_BYTES;
    const BLOCKS_IN_CHUNK: usize = 1 << Self::LOG_BLOCKS_IN_CHUNK;

    fn alloc_fast(&self) -> Option<B> {
        // 1. bump the cursor
        if self.cursor.load(Ordering::Relaxed).1 < Self::BLOCKS_IN_CHUNK as u32 {
            let result =
                self.cursor
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |(c, b)| {
                        if b <= Self::BLOCKS_IN_CHUNK as u32 {
                            Some((c, b + 1))
                        } else {
                            None
                        }
                    });
            if let Ok((c, b)) = result {
                let c = crate::util::conversions::chunk_index_to_address(c as usize);
                return Some(B::from_aligned_address(c + ((b as usize) << B::LOG_BYTES)));
            }
        }
        // 2. pop from list
        loop {
            std::hint::spin_loop();
            let top = self.head.load(Ordering::Relaxed);
            if top.is_zero() {
                return None;
            }
            let new_top = unsafe { top.load::<Address>() };
            if self
                .head
                .compare_exchange(top, new_top, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some(B::from_aligned_address(top));
            }
        }
    }

    fn add_chunk(&self, c: Chunk) -> B {
        let new_cursor = c.start() + B::BYTES;
        let chunk_index = new_cursor.chunk_index() as u32;
        self.cursor.store((chunk_index, 1), Ordering::Relaxed);
        B::from_aligned_address(c.start())
    }
}

impl<VM: VMBinding, B: Region> BlockAlloc<VM, B> for LockFreeListBlockAlloc<B> {
    fn new() -> Self {
        Self {
            head: Atomic::new(Address::ZERO),
            cursor: Atomic::new((0, Self::BLOCKS_IN_CHUNK as _)),
            _p: PhantomData,
        }
    }

    fn alloc(&self, bpr: &BlockPageResource<VM, B>, space: &dyn Space<VM>) -> Option<(B, bool)> {
        if let Some(b) = self.alloc_fast() {
            return Some((b, false));
        }
        let _sync = bpr.sync.lock().unwrap();
        if let Some(b) = self.alloc_fast() {
            return Some((b, false));
        }
        if let Some(chunk) = bpr.alloc_chunk(space) {
            let block = self.add_chunk(chunk);
            return Some((block, true));
        }
        return None;
    }

    fn free(&self, _bpr: &BlockPageResource<VM, B>, b: B, single_thread: bool) {
        if single_thread {
            let old_top = self.head.load(Ordering::Relaxed);
            unsafe {
                b.start().store(old_top);
            }
            self.head.store(b.start(), Ordering::Relaxed);
            return;
        }
        loop {
            std::hint::spin_loop();
            let old_top = self.head.load(Ordering::Relaxed);
            unsafe {
                b.start().store(old_top);
            }
            if self
                .head
                .compare_exchange(old_top, b.start(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }
}

struct BulkBlockAlloc<B: Region> {
    cursor: Atomic<(u32, u32)>,
    head: Atomic<Address>,
    _p: PhantomData<B>,
}

impl<B: Region> BulkBlockAlloc<B> {
    const LOG_BLOCKS_IN_CHUNK: usize = Chunk::LOG_BYTES - B::LOG_BYTES;
    const BLOCKS_IN_CHUNK: usize = 1 << Self::LOG_BLOCKS_IN_CHUNK;

    fn alloc_fast(&self) -> Option<B> {
        // 1. bump the cursor
        if self.cursor.load(Ordering::Relaxed).1 < Self::BLOCKS_IN_CHUNK as u32 {
            let result =
                self.cursor
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |(c, b)| {
                        if b <= Self::BLOCKS_IN_CHUNK as u32 {
                            Some((c, b + 1))
                        } else {
                            None
                        }
                    });
            if let Ok((c, b)) = result {
                let c = crate::util::conversions::chunk_index_to_address(c as usize);
                return Some(B::from_aligned_address(c + ((b as usize) << B::LOG_BYTES)));
            }
        }
        // 2. pop from list
        loop {
            std::hint::spin_loop();
            let top = self.head.load(Ordering::Relaxed);
            if top.is_zero() {
                return None;
            }
            let new_top = unsafe { top.load::<Address>() };
            if self
                .head
                .compare_exchange(top, new_top, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some(B::from_aligned_address(top));
            }
        }
    }

    fn add_chunk(&self, c: Chunk) -> B {
        let new_cursor = c.start() + B::BYTES;
        let chunk_index = new_cursor.chunk_index() as u32;
        self.cursor.store((chunk_index, 1), Ordering::Relaxed);
        B::from_aligned_address(c.start())
    }
}

impl<VM: VMBinding, B: Region> BlockAlloc<VM, B> for BulkBlockAlloc<B> {
    fn new() -> Self {
        Self {
            head: Atomic::new(Address::ZERO),
            cursor: Atomic::new((0, Self::BLOCKS_IN_CHUNK as _)),
            _p: PhantomData,
        }
    }

    fn alloc(&self, bpr: &BlockPageResource<VM, B>, space: &dyn Space<VM>) -> Option<(B, bool)> {
        if let Some(b) = self.alloc_fast() {
            return Some((b, false));
        }
        let _sync = bpr.sync.lock().unwrap();
        if let Some(b) = self.alloc_fast() {
            return Some((b, false));
        }
        if let Some(chunk) = bpr.alloc_chunk(space) {
            let block = self.add_chunk(chunk);
            return Some((block, true));
        }
        return None;
    }

    fn free(&self, _bpr: &BlockPageResource<VM, B>, b: B, single_thread: bool) {
        if single_thread {
            let old_top = self.head.load(Ordering::Relaxed);
            unsafe {
                b.start().store(old_top);
            }
            self.head.store(b.start(), Ordering::Relaxed);
            return;
        }
        loop {
            std::hint::spin_loop();
            let old_top = self.head.load(Ordering::Relaxed);
            unsafe {
                b.start().store(old_top);
            }
            if self
                .head
                .compare_exchange(old_top, b.start(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }
}
