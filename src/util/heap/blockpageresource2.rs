use super::chunk_map::Chunk;
use super::pageresource::{PRAllocFail, PRAllocResult};
use super::{FreeListPageResource, PageResource};
use crate::policy::space::Space;
use crate::util::address::Address;
use crate::util::constants::*;
use crate::util::heap::layout::vm_layout_constants::*;
use crate::util::heap::layout::VMMap;
use crate::util::heap::pageresource::CommonPageResource;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::spec_defs::{SB_IS_REUSABLE, SB_LIVE_BLOCKS};
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::opaque_pointer::*;
use crate::vm::*;
use atomic::Ordering;
use crossbeam::queue::SegQueue;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
pub struct SuperBlock(Address);

impl Region for SuperBlock {
    const LOG_BYTES: usize = if cfg!(feature = "ix_sb_1m") {
        20
    } else if cfg!(feature = "ix_sb_2m") {
        21
    } else {
        22
    };

    fn from_aligned_address(address: Address) -> Self {
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    fn start(&self) -> Address {
        self.0
    }
}

impl SuperBlock {
    /// Chunk constant with zero address
    // FIXME: We use this as an empty value. What if we actually use the first chunk?
    pub const ZERO: Self = Self(Address::ZERO);

    /// Get an iterator for regions within this chunk.
    pub fn iter_region<R: Region>(&self) -> RegionIterator<R> {
        // R should be smaller than a chunk
        debug_assert!(R::LOG_BYTES < Self::LOG_BYTES);
        // R should be aligned to chunk boundary
        debug_assert!(R::is_aligned(self.start()));
        debug_assert!(R::is_aligned(self.end()));

        let start = R::from_aligned_address(self.start());
        let end = R::from_aligned_address(self.end());
        RegionIterator::<R>::new(start, end)
    }
}

/// A fast PageResource for fixed-size block allocation only.
pub struct BlockPageResource<VM: VMBinding, B: Region + 'static> {
    flpr: FreeListPageResource<VM>,
    sb_queue: SegQueue<SuperBlock>,
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
        _space: &dyn Space<VM>,
        _reserved_pages: usize,
        _required_pages: usize,
        _tls: VMThread,
    ) -> Result<PRAllocResult, PRAllocFail> {
        unreachable!()
    }

    fn get_available_physical_pages(&self) -> usize {
        debug_assert!(self.common().contiguous);
        self.flpr.get_available_physical_pages()
    }
}

impl<VM: VMBinding, B: Region + 'static> BlockPageResource<VM, B> {
    /// Block granularity in pages
    const LOG_PAGES: usize = B::LOG_BYTES - LOG_BYTES_IN_PAGE as usize;

    fn append_local_metadata(metadata: &mut SideMetadataContext) {
        metadata.local.push(SB_LIVE_BLOCKS);
        metadata.local.push(SB_IS_REUSABLE);
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
            sync: Mutex::default(),
            sb_queue: SegQueue::new(),
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
            sync: Mutex::default(),
            sb_queue: SegQueue::new(),
            total_chunks: AtomicUsize::new(0),
            _p: PhantomData,
        }
    }

    fn alloc_chunk(&self, space: &dyn Space<VM>) -> Option<Chunk> {
        if self.common().contiguous {
            unreachable!();
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
            unreachable!();
        } else {
            self.common().release_discontiguous_chunks(chunk.start());
        }
    }

    pub fn notify_block_alloc(&self, b: B, tls: VMThread) {
        let sb = SuperBlock::from_unaligned_address(b.start());
        SB_LIVE_BLOCKS.fetch_add_atomic::<u16>(sb.start(), 1, Ordering::Relaxed);
        let pages = 1 << Self::LOG_PAGES;
        self.commit_pages(pages, pages, tls);
    }

    pub fn notify_block_dealloc(&self, b: B) {
        let sb = SuperBlock::from_unaligned_address(b.start());
        let _live_blocks =
            SB_LIVE_BLOCKS.fetch_sub_atomic::<u16>(sb.start(), 1, Ordering::Relaxed) - 1;
        let pages = 1 << Self::LOG_PAGES;
        self.common().accounting.release(pages as _);
        // if (live_blocks as usize) <= (Self::BLOCKS_IN_CHUNK * 3 / 4) {
        if SB_IS_REUSABLE.load_atomic::<u8>(sb.start(), Ordering::Relaxed) == 0 {
            let result = SB_IS_REUSABLE.fetch_update_atomic::<u8, _>(
                sb.start(),
                Ordering::Relaxed,
                Ordering::Relaxed,
                |x| {
                    if x == 0 {
                        Some(1)
                    } else {
                        None
                    }
                },
            );
            if result.is_ok() {
                self.sb_queue.push(sb);
            }
        }
        // }
    }

    pub fn pop_local_alloc_super_block(&self, space: &dyn Space<VM>) -> Option<SuperBlock> {
        if let Some(c) = self.sb_queue.pop() {
            return Some(c);
        }
        let c = self.alloc_chunk(space)?;
        for sb in c.iter_region::<SuperBlock>().skip(1) {
            SB_IS_REUSABLE.load_atomic::<u8>(sb.start(), Ordering::Relaxed);
            self.sb_queue.push(sb);
        }
        Some(SuperBlock::from_aligned_address(c.start()))
    }

    pub fn push_local_alloc_super_block(&self, sb: SuperBlock) {
        // self.chunk_queue.push(c);
        SB_IS_REUSABLE.store_atomic(sb.start(), 0u8, Ordering::SeqCst);
    }

    pub fn release_block(&self, block: B, _single_thread: bool) {
        self.notify_block_dealloc(block);
    }

    pub fn flush_all(&self) {}

    pub fn available_pages(&self) -> usize {
        let total = self.total_chunks.load(Ordering::SeqCst)
            << (LOG_BYTES_IN_CHUNK - LOG_BYTES_IN_PAGE as usize);
        total.saturating_sub(self.reserved_pages())
    }
}
