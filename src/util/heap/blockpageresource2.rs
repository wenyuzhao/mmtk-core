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
use crate::util::metadata::side_metadata::spec_defs::{
    SB_COPY_OWNER, SB_IS_REUSABLE, SB_OWNER, SB_USED_BLOCKS,
};
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::opaque_pointer::*;
use crate::vm::*;
use atomic::Ordering;
use crossbeam::queue::SegQueue;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::{Mutex, RwLock};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
pub struct SuperBlock(Address);

impl Region for SuperBlock {
    const LOG_BYTES: usize = if cfg!(feature = "ix_sb_1m") {
        20
    } else if cfg!(feature = "ix_sb_2m") {
        21
    } else if cfg!(feature = "ix_sb_4m") {
        22
    } else {
        21
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
    pub const LOG_BLOCKS: usize = Self::LOG_BYTES - crate::policy::immix::block::Block::LOG_BYTES;
    pub const BLOCKS: usize = 1 << Self::LOG_BLOCKS;

    // pub fn owner(&self) -> u32 {
    //     SB_OWNER.load_atomic::<u32>(self.start(), Ordering::Relaxed)
    // }

    fn init(&self) {
        SB_OWNER.store_atomic::<u32>(self.start(), 0, Ordering::SeqCst);
        SB_COPY_OWNER.store_atomic::<u32>(self.start(), 0, Ordering::SeqCst);
        SB_USED_BLOCKS.store_atomic::<u16>(self.start(), 0, Ordering::SeqCst);
    }

    pub fn inc_used_blocks(&self) {
        SB_USED_BLOCKS.fetch_add_atomic::<u16>(self.start(), 1, Ordering::Relaxed);
    }

    fn dec_used_blocks(&self) {
        SB_USED_BLOCKS.fetch_sub_atomic::<u16>(self.start(), 1, Ordering::Relaxed);
    }

    fn dec_used_blocks_relaxed(&self) {
        SB_USED_BLOCKS.store_atomic::<u16>(
            self.start(),
            self.used_blocks() as u16 - 1,
            Ordering::Relaxed,
        );
    }

    pub fn used_blocks(&self) -> usize {
        SB_USED_BLOCKS.load_atomic::<u16>(self.start(), Ordering::Relaxed) as _
    }

    pub fn is_owned_by(&self, owner: u32, copy: bool) -> bool {
        if !copy {
            SB_OWNER.load_atomic::<u32>(self.start(), Ordering::Relaxed) == owner
        } else {
            SB_COPY_OWNER.load_atomic::<u32>(self.start(), Ordering::Relaxed) == owner
        }
    }

    pub fn is_locked(&self, copy: bool) -> bool {
        if !copy {
            SB_OWNER.load_atomic::<u32>(self.start(), Ordering::Relaxed) == u32::MAX
        } else {
            SB_COPY_OWNER.load_atomic::<u32>(self.start(), Ordering::Relaxed) == u32::MAX
        }
    }

    pub fn try_lock(&self, owner: u32, copy: bool) -> Result<SuperBlockLockGuard, ()> {
        let f = |o| {
            if o != owner {
                None
            } else {
                Some(u32::MAX)
            }
        };
        let result = if !copy {
            SB_OWNER.fetch_update_atomic::<u32, _>(
                self.start(),
                Ordering::SeqCst,
                Ordering::SeqCst,
                f,
            )
        } else {
            SB_COPY_OWNER.fetch_update_atomic::<u32, _>(
                self.start(),
                Ordering::SeqCst,
                Ordering::SeqCst,
                f,
            )
        };
        if result.is_ok() {
            Ok(SuperBlockLockGuard {
                super_block: *self,
                owner,
                copy,
            })
        } else {
            Err(())
        }
    }

    fn unlock(&self, owner: u32, copy: bool) {
        if !copy {
            SB_OWNER.store_atomic::<u32>(self.start(), owner, Ordering::SeqCst);
        } else {
            SB_COPY_OWNER.store_atomic::<u32>(self.start(), owner, Ordering::SeqCst);
        }
    }

    pub fn steal_and_lock(&self, stealer: u32, copy: bool) -> Result<SuperBlockLockGuard, ()> {
        let f = |o| {
            if o == stealer || o == u32::MAX {
                None
            } else {
                Some(u32::MAX)
            }
        };
        let result = if !copy {
            SB_OWNER.fetch_update_atomic::<u32, _>(
                self.start(),
                Ordering::SeqCst,
                Ordering::SeqCst,
                f,
            )
        } else {
            SB_COPY_OWNER.fetch_update_atomic::<u32, _>(
                self.start(),
                Ordering::SeqCst,
                Ordering::SeqCst,
                f,
            )
        };
        if result.is_ok() {
            Ok(SuperBlockLockGuard {
                super_block: *self,
                owner: stealer,
                copy,
            })
        } else {
            Err(())
        }
    }

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

    pub fn blocks(&self) -> RegionIterator<crate::policy::immix::block::Block> {
        self.iter_region()
    }
}

pub struct SuperBlockLockGuard {
    super_block: SuperBlock,
    owner: u32,
    copy: bool,
}

impl Deref for SuperBlockLockGuard {
    type Target = SuperBlock;
    fn deref(&self) -> &Self::Target {
        &self.super_block
    }
}

impl Drop for SuperBlockLockGuard {
    fn drop(&mut self) {
        self.super_block.unlock(self.owner, self.copy)
    }
}

/// A fast PageResource for fixed-size block allocation only.
pub struct BlockPageResource<VM: VMBinding, B: Region + 'static> {
    flpr: FreeListPageResource<VM>,
    sb_queue: SegQueue<SuperBlock>,
    sync: Mutex<()>,
    super_block_alloc_cursor: AtomicUsize,
    all_chunks: RwLock<Vec<Chunk>>,
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
        metadata.local.push(SB_IS_REUSABLE);
        metadata.local.push(SB_OWNER);
        metadata.local.push(SB_COPY_OWNER);
        metadata.local.push(SB_USED_BLOCKS);
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
            super_block_alloc_cursor: AtomicUsize::new(0),
            all_chunks: RwLock::default(),
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
            super_block_alloc_cursor: AtomicUsize::new(0),
            all_chunks: RwLock::default(),
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
        let chunk = Chunk::from_aligned_address(start);
        Some(chunk)
    }

    fn free_chunk(&self, _chunk: Chunk) {
        unreachable!();
    }

    pub fn notify_block_alloc(&self, b: B, tls: VMThread) {
        let sb = SuperBlock::from_unaligned_address(b.start());
        sb.inc_used_blocks();
        let pages = 1 << Self::LOG_PAGES;
        self.commit_pages(pages, pages, tls);
    }

    pub fn notify_block_dealloc(&self, b: B, single_thread: bool) {
        let sb = SuperBlock::from_unaligned_address(b.start());
        let pages = 1 << Self::LOG_PAGES;
        if single_thread {
            sb.dec_used_blocks_relaxed();
            self.common().accounting.release_relaxed(pages as _);
        } else {
            sb.dec_used_blocks();
            self.common().accounting.release(pages as _);
        }
    }
    fn alloc_or_steal_super_block_in_address_order_fast(
        &self,
        owner: u32,
        copy: bool,
        all_chunks: &[Chunk],
    ) -> Option<SuperBlockLockGuard> {
        const LOG_SUPER_BLOCKS_IN_CHUNK: usize = Chunk::LOG_BYTES - SuperBlock::LOG_BYTES;
        let max_super_blocks = all_chunks.len() << LOG_SUPER_BLOCKS_IN_CHUNK;
        let start = self.super_block_alloc_cursor.load(Ordering::Relaxed);
        let mut i = start;
        while i < max_super_blocks {
            let c = all_chunks[i >> LOG_SUPER_BLOCKS_IN_CHUNK];
            let index_within_chunk = i & ((1 << LOG_SUPER_BLOCKS_IN_CHUNK) - 1);
            let sb = c
                .iter_region::<SuperBlock>()
                .nth(index_within_chunk)
                .unwrap();
            if sb.used_blocks() == SuperBlock::BLOCKS || sb.is_locked(copy) {
                i += 1;
                continue;
            }
            if let Ok(guard) = sb.steal_and_lock(owner, copy) {
                self.super_block_alloc_cursor.store(i, Ordering::Relaxed);
                let _ = self.super_block_alloc_cursor.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |cursor| {
                        if cursor < start {
                            None
                        } else {
                            Some(cursor.max(i))
                        }
                    },
                );
                return Some(guard);
            }
            i += 1;
        }
        None
    }

    pub fn alloc_or_steal_super_block_in_address_order(
        &self,
        space: &dyn Space<VM>,
        owner: u32,
        copy: bool,
    ) -> Option<SuperBlockLockGuard> {
        const LOG_SUPER_BLOCKS_IN_CHUNK: usize = Chunk::LOG_BYTES - SuperBlock::LOG_BYTES;
        let all_chunks = self.all_chunks.read().unwrap();
        if let Some(sb) =
            self.alloc_or_steal_super_block_in_address_order_fast(owner, copy, &all_chunks)
        {
            return Some(sb);
        }
        // allocate a new chunk
        std::mem::drop(all_chunks);
        let mut all_chunks = self.all_chunks.write().unwrap();
        if let Some(sb) =
            self.alloc_or_steal_super_block_in_address_order_fast(owner, copy, &all_chunks)
        {
            return Some(sb);
        }
        let c = self.alloc_chunk(space)?;
        for sb in c.iter_region::<SuperBlock>() {
            sb.init();
        }
        let first_sb = c.iter_region::<SuperBlock>().next().unwrap();
        let guard = first_sb.steal_and_lock(owner, copy).unwrap();
        self.super_block_alloc_cursor.store(
            (all_chunks.len() << LOG_SUPER_BLOCKS_IN_CHUNK) + 1,
            Ordering::Relaxed,
        );
        all_chunks.push(c);
        Some(guard)
    }

    pub fn release_block(&self, block: B, single_thread: bool) {
        self.notify_block_dealloc(block, single_thread);
    }

    pub fn flush_all(&self) {
        // let _guard = self.sync.lock().unwrap();
        crate::update_mutators();
        let _all_chunks = self.all_chunks.write().unwrap();
        self.super_block_alloc_cursor.store(0, Ordering::Relaxed);
    }

    pub fn available_pages(&self) -> usize {
        let total = self.total_chunks.load(Ordering::SeqCst)
            << (LOG_BYTES_IN_CHUNK - LOG_BYTES_IN_PAGE as usize);
        total.saturating_sub(self.reserved_pages())
    }
}
