use super::chunk_map::Chunk;
use super::pageresource::{PRAllocFail, PRAllocResult};
use super::{FreeListPageResource, PageResource};
use crate::policy::immix::block::Block;
use crate::util::address::Address;
use crate::util::constants::*;
use crate::util::heap::layout::heap_layout::Map;
use crate::util::heap::layout::vm_layout_constants::VM_LAYOUT_CONSTANTS;
use crate::util::heap::layout::vm_layout_constants::*;
use crate::util::heap::pageresource::CommonPageResource;
use crate::util::heap::space_descriptor::SpaceDescriptor;
use crate::util::linear_scan::Region;
use crate::util::metadata::side_metadata::{SideMetadataContext, SideMetadataSpec};
use crate::util::opaque_pointer::*;
use crate::vm::*;
use atomic::Ordering;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, RwLock};

const UNINITIALIZED_WATER_MARK: i32 = -1;
const LOCAL_BUFFER_SIZE: usize = 128;
const LOG_BLOCKS_IN_CHUNK: usize = LOG_BYTES_IN_CHUNK - 15;
const BLOCKS_IN_CHUNK: usize = 1 << (LOG_BYTES_IN_CHUNK - 15);

#[derive(Default)]
struct ChunkList {
    base: Address,
    head: Option<Chunk>,
}

impl ChunkList {
    const PREV: SideMetadataSpec = crate::util::metadata::side_metadata::spec_defs::CHUNK_PREV;
    const NEXT: SideMetadataSpec = crate::util::metadata::side_metadata::spec_defs::CHUNK_NEXT;

    fn get_prev(chunk: Chunk) -> Option<Chunk> {
        let prev = unsafe { Address::from_usize(ChunkList::PREV.load::<u64>(chunk.start()) as _) };
        if prev.is_zero() {
            None
        } else {
            Some(Chunk::from_aligned_address(prev))
        }
    }

    fn get_next(chunk: Chunk) -> Option<Chunk> {
        let next = unsafe { Address::from_usize(ChunkList::NEXT.load::<u64>(chunk.start()) as _) };
        if next.is_zero() {
            None
        } else {
            Some(Chunk::from_aligned_address(next))
        }
    }

    fn set_prev(chunk: Chunk, prev: Option<Chunk>) {
        let v = prev.map(|x| x.start()).unwrap_or(Address::ZERO);
        unsafe { ChunkList::PREV.store::<u64>(chunk.start(), v.as_usize() as _) }
    }

    fn set_next(chunk: Chunk, next: Option<Chunk>) {
        let v = next.map(|x| x.start()).unwrap_or(Address::ZERO);
        unsafe { ChunkList::NEXT.store::<u64>(chunk.start(), v.as_usize() as _) }
    }

    fn remove(&mut self, chunk: Chunk) {
        let prev = Self::get_prev(chunk);
        let next = Self::get_next(chunk);
        if let Some(next) = next {
            Self::set_prev(next, prev);
        }
        if let Some(prev) = prev {
            Self::set_next(prev, next);
        } else {
            self.head = next;
        }
        Self::set_next(chunk, None);
        Self::set_prev(chunk, None);
        // self.chunk_bin[c_index].store(u8::MAX, Ordering::Relaxed);
    }

    fn push(&mut self, chunk: Chunk) {
        Self::set_prev(chunk, None);
        Self::set_next(chunk, self.head);
        if let Some(head) = self.head {
            Self::set_prev(head, Some(chunk));
        }
        self.head = Some(chunk);
    }

    fn iter(&self) -> impl Iterator<Item = Chunk> {
        ChunkListIterator { next: self.head }
    }
}

struct ChunkListIterator {
    next: Option<Chunk>,
}

impl Iterator for ChunkListIterator {
    type Item = Chunk;

    fn next(&mut self) -> Option<Chunk> {
        let curr = self.next?;
        let next = unsafe { Address::from_usize(ChunkList::NEXT.load::<u64>(curr.start()) as _) };
        if next.is_zero() {
            self.next = None;
        } else {
            self.next = Some(Chunk::from_aligned_address(next));
        }
        Some(curr)
    }
}

const CHUNK_BIN: SideMetadataSpec = crate::util::metadata::side_metadata::spec_defs::CHUNK_BIN;
const CHUNK_LIVE_BLOCKS: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::CHUNK_LIVE_BLOCKS;

struct ChunkPool {
    base: Address,
    /// block alloc state
    alloc_state: Vec<AtomicBool>,
    /// bins
    bins: [RwLock<ChunkList>; 5],
    sync: Mutex<()>,
    alloc_chunk_lock: Mutex<()>,
}

impl ChunkPool {
    const MAX_CHUNKS: usize = 1 << (35 - LOG_BYTES_IN_CHUNK);

    fn new_compressed_pointers(base: Address) -> Self {
        Self {
            base,
            alloc_state: (0..(Self::MAX_CHUNKS << LOG_BLOCKS_IN_CHUNK))
                .map(|_| AtomicBool::new(true))
                .collect(),
            bins: [
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            ],
            sync: Mutex::default(),
            alloc_chunk_lock: Mutex::default(),
        }
    }

    fn get_bin(chunk: Chunk) -> u8 {
        CHUNK_BIN.load_atomic::<u8>(chunk.start(), Ordering::Relaxed)
    }

    fn set_bin(chunk: Chunk, bin: u8) {
        CHUNK_BIN.store_atomic::<u8>(chunk.start(), bin, Ordering::Relaxed)
    }

    fn get_live_blocks(chunk: Chunk) -> u8 {
        CHUNK_LIVE_BLOCKS.load_atomic::<u8>(chunk.start(), Ordering::Relaxed)
    }

    fn set_live_blocks(chunk: Chunk, live_blocks: u8) {
        CHUNK_LIVE_BLOCKS.store_atomic::<u8>(chunk.start(), live_blocks, Ordering::Relaxed)
    }

    fn chunk_index(&self, c: Address) -> usize {
        (c - self.base) >> LOG_BYTES_IN_CHUNK
    }

    fn calc_bin(live_blocks: u8) -> u8 {
        assert_eq!(BLOCKS_IN_CHUNK, 128);
        match live_blocks {
            x if x >= 128 => 0,
            x if x >= 96 => 1,
            x if x >= 64 => 2,
            x if x >= 32 => 3,
            _ => 4,
        }
    }

    fn cross_boundary(inc: bool, current_live_blocks: u8) -> bool {
        assert_eq!(BLOCKS_IN_CHUNK, 128);
        let mut x = current_live_blocks;
        if !inc {
            x += 1;
        }
        x == 128 || x == 96 || x == 64 || x == 32 || x == 1
    }

    fn move_chunk(&self, chunk: Chunk) -> bool {
        let _sync = self.sync.lock().unwrap();
        let old_bin_id = Self::get_bin(chunk);
        let live_blocks = Self::get_live_blocks(chunk);
        if live_blocks == 0 {
            // remove from the current bin
            let mut bin = self.bins[old_bin_id as usize].write().unwrap();
            bin.remove(chunk);
            return true;
        }
        assert_ne!(live_blocks, 0);
        let new_bin_id = Self::calc_bin(live_blocks);
        if new_bin_id != old_bin_id {
            let mut old_bin = self.bins[old_bin_id as usize].write().unwrap();
            let mut new_bin = self.bins[new_bin_id as usize].write().unwrap();
            old_bin.remove(chunk);
            new_bin.push(chunk);
            Self::set_bin(chunk, new_bin_id);
        }
        false
    }

    fn alloc_block(&self) -> Option<Address> {
        for bin in self.bins.iter().skip(1) {
            let bin = bin.read().unwrap();
            for c in bin.iter() {
                for i in 0..BLOCKS_IN_CHUNK {
                    let b = c.start() + (i << Block::LOG_BYTES);
                    let b_index = (b - self.base) >> Block::LOG_BYTES;
                    if self.alloc_state[b_index].fetch_or(true, Ordering::SeqCst) == false {
                        let result = CHUNK_LIVE_BLOCKS.fetch_update_atomic::<u8, _>(
                            c.start(),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                            |x| {
                                if x == 0 {
                                    None
                                } else {
                                    Some(x + 1)
                                }
                            },
                        );
                        if let Ok(old_live_blocks) = result {
                            if Self::cross_boundary(true, old_live_blocks + 1) {
                                std::mem::drop(bin);
                                self.move_chunk(c);
                            }
                        } else {
                            // skip this entire chunk as this chunk may be freed
                            break;
                        }
                        return Some(b);
                    }
                }
            }
        }
        None
    }

    fn alloc_block_from_new_chunk(&self, chunk: Chunk) -> Address {
        for i in 0..BLOCKS_IN_CHUNK {
            let b = chunk.start() + (i << Block::LOG_BYTES);
            let b_index = (b - self.base) >> Block::LOG_BYTES;
            self.alloc_state[b_index].store(false, Ordering::SeqCst);
        }
        let start_block_index = (chunk.start() - self.base) >> Block::LOG_BYTES;
        self.alloc_state[start_block_index].store(true, Ordering::SeqCst);
        Self::set_live_blocks(chunk, 1);
        Self::set_bin(chunk, self.bins.len() as u8 - 1);
        let _sync = self.sync.lock().unwrap();
        let mut bin = self.bins.last().unwrap().write().unwrap();
        bin.push(chunk);
        chunk.start()
    }

    /// Must be called when no allocation happens, and only one single thread is releasing the blocks
    fn free_block_fast(&mut self, b: Address) -> Option<Chunk> {
        let chunk = Chunk::from_unaligned_address(b);
        let b_index = (b - self.base) >> Block::LOG_BYTES;
        let old_live_blocks = Self::get_live_blocks(chunk);
        Self::set_live_blocks(chunk, old_live_blocks - 1);
        let live_blocks = old_live_blocks - 1;
        self.alloc_state[b_index].store(false, Ordering::Relaxed);
        if Self::cross_boundary(false, live_blocks) {
            let old_bin = Self::get_bin(chunk);
            if live_blocks == 0 {
                // release chunk
                let bin = self.bins[old_bin as usize].get_mut().unwrap();
                bin.remove(chunk);
                return Some(chunk);
            } else {
                assert_ne!(live_blocks, 0);
                let new_bin = Self::calc_bin(live_blocks);
                if new_bin != old_bin {
                    let bin = self.bins[old_bin as usize].get_mut().unwrap();
                    bin.remove(chunk);
                    let bin = self.bins[new_bin as usize].get_mut().unwrap();
                    bin.push(chunk);
                    Self::set_bin(chunk, new_bin);
                }
            }
        }
        None
    }

    fn free_block(&self, b: Address, single_thread: bool) -> Option<Chunk> {
        if single_thread {
            let me = unsafe { &mut *(self as *const Self as *mut Self) };
            return me.free_block_fast(b);
        }
        let chunk = Chunk::from_unaligned_address(b);
        let b_index = (b - self.base) >> Block::LOG_BYTES;
        let live_blocks =
            CHUNK_LIVE_BLOCKS.fetch_sub_atomic::<u8>(chunk.start(), 1, Ordering::SeqCst) - 1;
        self.alloc_state[b_index].fetch_and(false, Ordering::SeqCst);
        if Self::cross_boundary(false, live_blocks) {
            let chunk_is_removed = self.move_chunk(chunk);
            if chunk_is_removed {
                Some(chunk)
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// A fast PageResource for fixed-size block allocation only.
pub struct BlockPageResource<VM: VMBinding, B: Region + 'static> {
    flpr: FreeListPageResource<VM>,
    pool: ChunkPool,
    sync: Mutex<()>,
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
        space_descriptor: SpaceDescriptor,
        reserved_pages: usize,
        required_pages: usize,
        tls: VMThread,
    ) -> Result<PRAllocResult, PRAllocFail> {
        if let Some((block, new_chunk)) =
            self.allocate_block(space_descriptor, reserved_pages, required_pages, tls)
        {
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

    pub fn new_contiguous(
        log_pages: usize,
        start: Address,
        bytes: usize,
        vm_map: &'static dyn Map,
        _num_workers: usize,
    ) -> Self {
        assert!((1 << log_pages) <= PAGES_IN_CHUNK);
        Self {
            flpr: FreeListPageResource::new_contiguous(start, bytes, vm_map),
            pool: unimplemented!(),
            sync: Mutex::default(),
            _p: PhantomData,
        }
    }

    pub fn new_discontiguous(
        log_pages: usize,
        vm_map: &'static dyn Map,
        _num_workers: usize,
    ) -> Self {
        assert!((1 << log_pages) <= PAGES_IN_CHUNK);
        Self {
            flpr: FreeListPageResource::new_discontiguous(vm_map),
            pool: ChunkPool::new_compressed_pointers(VM_LAYOUT_CONSTANTS.available_start()),
            sync: Mutex::default(),
            _p: PhantomData,
        }
    }

    fn alloc_chunk(&self, descriptor: SpaceDescriptor, tls: VMThread) -> Option<Chunk> {
        let a = self.common().grow_discontiguous_space(descriptor, 1);
        if a.is_zero() {
            return None;
        }
        // map metadata
        let metadata = SideMetadataContext {
            global: vec![],
            local: vec![
                ChunkList::PREV,
                ChunkList::NEXT,
                CHUNK_BIN,
                CHUNK_LIVE_BLOCKS,
            ],
        };

        if let Err(mmap_error) = metadata.try_map_metadata_space(a, BYTES_IN_CHUNK) {
            crate::util::memory::handle_mmap_error::<VM>(mmap_error, tls);
        }
        Some(Chunk::from_aligned_address(a))
    }

    fn free_chunk(&self, chunk: Chunk) {
        self.common().release_discontiguous_chunks(chunk.start());
    }

    fn allocate_block_fast(
        &self,
        reserved_pages: usize,
        required_pages: usize,
        tls: VMThread,
    ) -> Option<(B, bool)> {
        if let Some(block) = self.pool.alloc_block() {
            self.commit_pages(reserved_pages, required_pages, tls);
            return Some((B::from_aligned_address(block), false));
        }
        None
    }

    fn allocate_block(
        &self,
        space_descriptor: SpaceDescriptor,
        reserved_pages: usize,
        required_pages: usize,
        tls: VMThread,
    ) -> Option<(B, bool)> {
        if let Some(result) = self.allocate_block_fast(reserved_pages, required_pages, tls) {
            return Some(result);
        }
        let _sync = self.sync.lock().unwrap();
        if let Some(result) = self.allocate_block_fast(reserved_pages, required_pages, tls) {
            return Some(result);
        }
        if let Some(chunk) = self.alloc_chunk(space_descriptor, tls) {
            let block = self.pool.alloc_block_from_new_chunk(chunk);
            self.commit_pages(reserved_pages, required_pages, tls);
            return Some((B::from_aligned_address(block), true));
        }
        None
    }

    pub fn release_block(&self, block: B, single_thread: bool) {
        let pages = 1 << Self::LOG_PAGES;
        self.common().accounting.release(pages as _);
        if let Some(chunk) = self.pool.free_block(block.start(), single_thread) {
            self.free_chunk(chunk)
        }
    }

    pub fn flush_all(&self) {
        // TODO: For 32-bit space, we may want to free some contiguous chunks.
    }
}
