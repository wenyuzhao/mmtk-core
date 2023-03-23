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
use crate::util::opaque_pointer::*;
use crate::vm::*;
use atomic::{Atomic, Ordering};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU8};
use std::sync::Mutex;

const UNINITIALIZED_WATER_MARK: i32 = -1;
const LOCAL_BUFFER_SIZE: usize = 128;
const LOG_BLOCKS_IN_CHUNK: usize = LOG_BYTES_IN_CHUNK - 15;
const BLOCKS_IN_CHUNK: usize = 1 << (LOG_BYTES_IN_CHUNK - 15);

trait ChunkAllocator {
    fn alloc_chunk(&self, descriptor: SpaceDescriptor) -> Option<Address>;
    fn free_chunk(&self, start: Address);
}

type Bin = std::sync::RwLock<Address>;

struct ChunkPool {
    base: Address,
    /// block alloc state
    alloc_state: Vec<AtomicBool>,
    /// bins
    bins: [Bin; 5],
    /// live block counter for each chunk
    live_blocks: Vec<AtomicU16>,
    /// current bin index  for each chunk
    chunk_bin: Vec<AtomicU8>,
    next_chunk: Vec<Atomic<Address>>,
    prev_chunk: Vec<Atomic<Address>>,
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
            live_blocks: (0..Self::MAX_CHUNKS).map(|_| AtomicU16::new(0)).collect(),
            chunk_bin: (0..Self::MAX_CHUNKS).map(|_| AtomicU8::new(0)).collect(),
            next_chunk: (0..Self::MAX_CHUNKS)
                .map(|_| Atomic::new(Address::ZERO))
                .collect(),
            prev_chunk: (0..Self::MAX_CHUNKS)
                .map(|_| Atomic::new(Address::ZERO))
                .collect(),
            sync: Mutex::default(),
            alloc_chunk_lock: Mutex::default(),
        }
    }
    fn chunk_index(&self, c: Address) -> usize {
        (c - self.base) >> LOG_BYTES_IN_CHUNK
    }
    fn remove_from_bin(&self, head: &mut Address, _c: Address, c_index: usize) {
        let prev = self.prev_chunk[c_index].load(Ordering::Relaxed);
        let next = self.next_chunk[c_index].load(Ordering::Relaxed);
        if !next.is_zero() {
            self.prev_chunk[self.chunk_index(next)].store(prev, Ordering::Relaxed);
        }
        if !prev.is_zero() {
            self.next_chunk[self.chunk_index(prev)].store(next, Ordering::Relaxed);
        } else {
            *head = next;
        }
        self.next_chunk[c_index].store(Address::ZERO, Ordering::Relaxed);
        self.prev_chunk[c_index].store(Address::ZERO, Ordering::Relaxed);
        self.chunk_bin[c_index].store(u8::MAX, Ordering::Relaxed);
    }
    fn add_to_bin(&self, head: &mut Address, c: Address, c_index: usize) {
        self.prev_chunk[c_index].store(Address::ZERO, Ordering::Relaxed);
        self.next_chunk[c_index].store(*head, Ordering::Relaxed);
        if !head.is_zero() {
            let h_index = self.chunk_index(*head);
            self.prev_chunk[h_index].store(c, Ordering::Relaxed);
        }
        *head = c;
    }
    fn move_chunk(&self, c: Address, ca: &impl ChunkAllocator) {
        let _sync = self.sync.lock().unwrap();
        let c_index = (c - self.base) >> LOG_BYTES_IN_CHUNK;
        let old_bin = self.chunk_bin[c_index].load(Ordering::Relaxed) as usize;
        let live_blocks = self.live_blocks[c_index].load(Ordering::Relaxed);
        if live_blocks == 0 {
            // remove from bin
            let mut bin = self.bins[old_bin].write().unwrap();
            self.remove_from_bin(&mut bin, c, c_index);
            // release chunk
            ca.free_chunk(c);
            return;
        }
        assert_ne!(live_blocks, 0);
        let new_bin = match live_blocks {
            x if x >= 128 => 0,
            x if x >= 96 => 1,
            x if x >= 64 => 2,
            x if x >= 32 => 3,
            _ => 4,
        } as usize;
        if new_bin != old_bin {
            let mut bin = self.bins[old_bin].write().unwrap();
            self.remove_from_bin(&mut bin, c, c_index);
            let mut bin = self.bins[new_bin].write().unwrap();
            self.add_to_bin(&mut bin, c, c_index);
            self.chunk_bin[c_index].store(new_bin as u8, Ordering::Relaxed);
        }
    }

    fn alloc_block_fast(&self, ca: &impl ChunkAllocator) -> Option<(Address, bool)> {
        for bin in self.bins.iter().skip(1) {
            let bin = bin.read().unwrap();
            let mut c = *bin;
            while !c.is_zero() {
                let c_index = (c - self.base) >> LOG_BYTES_IN_CHUNK;
                for i in 0..BLOCKS_IN_CHUNK {
                    let b = c + (i << Block::LOG_BYTES);
                    let b_index = (b - self.base) >> Block::LOG_BYTES;
                    if self.alloc_state[b_index].fetch_or(true, Ordering::SeqCst) == false {
                        let result = self.live_blocks[c_index].fetch_update(
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
                            let live_blocks = old_live_blocks + 1;
                            assert_eq!(BLOCKS_IN_CHUNK, 128);
                            if live_blocks == 128
                                || live_blocks == 96
                                || live_blocks == 64
                                || live_blocks == 32
                            {
                                std::mem::drop(bin);
                                self.move_chunk(c, ca);
                            }
                        } else {
                            break;
                        }
                        return Some((b, false));
                    }
                }
                c = self.next_chunk[c_index].load(Ordering::SeqCst);
            }
        }
        None
    }

    fn alloc_block(
        &self,
        ca: &impl ChunkAllocator,
        descriptor: SpaceDescriptor,
    ) -> Option<(Address, bool)> {
        if let Some(r) = self.alloc_block_fast(ca) {
            return Some(r);
        }
        // Acquire new chunk
        let _guard = self.alloc_chunk_lock.lock().unwrap();
        if let Some(r) = self.alloc_block_fast(ca) {
            return Some(r);
        }
        if let Some(c) = ca.alloc_chunk(descriptor) {
            let c_index = (c - self.base) >> LOG_BYTES_IN_CHUNK;
            for i in 0..BLOCKS_IN_CHUNK {
                let b = c + (i << Block::LOG_BYTES);
                let b_index = (b - self.base) >> Block::LOG_BYTES;
                self.alloc_state[b_index].store(false, Ordering::SeqCst);
            }
            let b = c;
            let b_index = (b - self.base) >> Block::LOG_BYTES;
            self.alloc_state[b_index].store(true, Ordering::SeqCst);
            self.live_blocks[c_index].store(1, Ordering::SeqCst);
            self.chunk_bin[c_index].store(self.bins.len() as u8 - 1, Ordering::SeqCst);
            self.add_to_bin(
                &mut self.bins[self.bins.len() - 1].write().unwrap(),
                c,
                c_index,
            );
            return Some((b, true));
        }
        None
    }

    fn free_block_fast(&mut self, b: Address, ca: &impl ChunkAllocator) {
        let c = b.align_down(BYTES_IN_CHUNK);
        let c_index = (c - self.base) >> LOG_BYTES_IN_CHUNK;
        let b_index = (b - self.base) >> Block::LOG_BYTES;
        let old_live_blocks = self.live_blocks[c_index].load(Ordering::Relaxed);
        self.live_blocks[c_index].store(old_live_blocks - 1, Ordering::Relaxed);
        let live_blocks = old_live_blocks - 1;
        self.alloc_state[b_index].store(false, Ordering::Relaxed);
        if live_blocks == 127
            || live_blocks == 95
            || live_blocks == 63
            || live_blocks == 31
            || live_blocks == 0
        {
            let old_bin = self.chunk_bin[c_index].load(Ordering::Relaxed) as usize;
            if live_blocks == 0 {
                // release chunk
                let me = unsafe { &*(self as *const Self) };
                let bin = self.bins[old_bin].get_mut().unwrap();
                me.remove_from_bin(bin, c, c_index);
                ca.free_chunk(c);
            } else {
                assert_ne!(live_blocks, 0);
                let new_bin = match live_blocks {
                    x if x >= 128 => 0,
                    x if x >= 96 => 1,
                    x if x >= 64 => 2,
                    x if x >= 32 => 3,
                    _ => 4,
                } as usize;
                if new_bin != old_bin {
                    let me = unsafe { &*(self as *const Self) };
                    let bin = self.bins[old_bin].get_mut().unwrap();
                    me.remove_from_bin(bin, c, c_index);
                    let bin = self.bins[new_bin].get_mut().unwrap();
                    me.add_to_bin(bin, c, c_index);
                    self.chunk_bin[c_index].store(new_bin as u8, Ordering::Relaxed);
                }
            }
        }
    }

    fn free_block(&self, b: Address, single_thread: bool, ca: &impl ChunkAllocator) {
        if single_thread {
            let me = unsafe { &mut *(self as *const Self as *mut Self) };
            me.free_block_fast(b, ca);
            return;
        }
        let c = b.align_down(BYTES_IN_CHUNK);
        let c_index = (c - self.base) >> LOG_BYTES_IN_CHUNK;
        let b_index = (b - self.base) >> Block::LOG_BYTES;
        let live_blocks = self.live_blocks[c_index].fetch_sub(1, Ordering::SeqCst) - 1;
        self.alloc_state[b_index].fetch_and(false, Ordering::SeqCst);
        if live_blocks == 127
            || live_blocks == 95
            || live_blocks == 63
            || live_blocks == 31
            || live_blocks == 0
        {
            self.move_chunk(c, ca);
        }
    }
}

/// A fast PageResource for fixed-size block allocation only.
pub struct BlockPageResource<VM: VMBinding, B: Region + 'static> {
    flpr: FreeListPageResource<VM>,
    /// Slow-path allocation synchronization
    sync: Mutex<()>,
    pool: ChunkPool,
    _p: PhantomData<B>,
}

impl<VM: VMBinding, B: Region> ChunkAllocator for BlockPageResource<VM, B> {
    fn alloc_chunk(&self, descriptor: SpaceDescriptor) -> Option<Address> {
        let a = self.common().grow_discontiguous_space(descriptor, 1);
        if a.is_zero() {
            None
        } else {
            Some(a)
        }
    }

    fn free_chunk(&self, start: Address) {
        self.common().release_discontiguous_chunks(start);
    }
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
        if let Some((start, new_chunk)) = self.pool.alloc_block(self, space_descriptor) {
            self.commit_pages(reserved_pages, required_pages, tls);
            Ok(PRAllocResult {
                start,
                pages: required_pages,
                new_chunk,
            })
        } else {
            Err(PRAllocFail)
        }
    }

    fn get_available_physical_pages(&self) -> usize {
        debug_assert!(self.common().contiguous);
        let _sync = self.sync.lock().unwrap();
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
            sync: Mutex::new(()),
            pool: unimplemented!(),
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
            sync: Mutex::new(()),
            pool: ChunkPool::new_compressed_pointers(VM_LAYOUT_CONSTANTS.available_start()),
            _p: PhantomData,
        }
    }

    pub fn release_block(&self, block: B, single_thread: bool) {
        let pages = 1 << Self::LOG_PAGES;
        self.common().accounting.release(pages as _);
        self.pool.free_block(block.start(), single_thread, self);
    }

    pub fn flush_all(&self) {
        // TODO: For 32-bit space, we may want to free some contiguous chunks.
    }
}
