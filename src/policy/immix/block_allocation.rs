use super::{block::Block, chunk::ChunkState, ImmixSpace};
use crate::{
    args::LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE,
    policy::space::Space,
    scheduler::{GCWork, GCWorker},
    util::{VMMutatorThread, VMThread},
    vm::*,
    MMTK,
};
use atomic::{Atomic, Ordering};
use spin::Lazy;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

static LOCK_FREE_BLOCKS_CAPACITY: Lazy<usize> =
    Lazy::new(|| usize::max(*LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE << 2, 32768 * 4));

pub struct BlockAllocation<VM: VMBinding> {
    space: Option<&'static ImmixSpace<VM>>,
    cursor: AtomicUsize,
    high_water: AtomicUsize,
    buffer: Vec<Atomic<Block>>,
    pub refill_lock: Mutex<()>,
    refill_count: usize,
}

impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: None,
            cursor: AtomicUsize::new(0),
            high_water: AtomicUsize::new(0),
            refill_lock: Mutex::new(()),
            buffer: (0..*LOCK_FREE_BLOCKS_CAPACITY)
                .map(|_| Atomic::new(Block::ZERO))
                .collect(),
            refill_count: *LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE, // num_cpus::get(),
        }
    }

    #[inline]
    pub fn nursery_blocks(&self) -> usize {
        self.cursor.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn nursery_mb(&self) -> usize {
        self.nursery_blocks() << Block::LOG_BYTES >> 20
    }

    pub fn init(&mut self, space: &'static ImmixSpace<VM>) {
        self.space = Some(space);
    }

    /// Reset allocated_block_buffer and free everything in clean_block_buffer.
    pub fn reset(&mut self) {
        let _guard = self.refill_lock.lock().unwrap();
        let cursor = self.cursor.load(Ordering::SeqCst);
        let high_water = self.high_water.load(Ordering::SeqCst);
        for i in cursor..high_water {
            self.space()
                .pr
                .release_block(self.buffer[i].load(Ordering::SeqCst))
        }
        self.high_water.store(0, Ordering::SeqCst);
        self.cursor.store(0, Ordering::SeqCst);
    }

    /// Drain allocated_block_buffer and free everything in clean_block_buffer.
    pub fn reset_and_generate_nursery_sweep_tasks(
        &mut self,
        _num_workers: usize,
    ) -> (Vec<Box<dyn GCWork<VM>>>, usize) {
        let _guard = self.refill_lock.lock().unwrap();
        let blocks = self.cursor.load(Ordering::SeqCst);
        let high_water = self.high_water.load(Ordering::SeqCst);
        let num_bins = 1;
        let bin_cap = blocks / num_bins + if blocks % num_bins == 0 { 0 } else { 1 };
        let mut bins = (0..num_bins)
            .map(|_| Vec::with_capacity(bin_cap))
            .collect::<Vec<Vec<Block>>>();
        'out: for i in 0..num_bins {
            for j in 0..bin_cap {
                let index = i * bin_cap + j;
                if index < blocks {
                    bins[i].push(self.buffer[index].load(Ordering::SeqCst));
                } else {
                    break 'out;
                }
            }
        }
        let space = self.space();
        let mut packets: Vec<Box<dyn GCWork<VM>>> = bins
            .into_iter()
            .map::<Box<dyn GCWork<VM>>, _>(|blocks| {
                Box::new(RCSweepNurseryBlocks { space, blocks })
            })
            .collect();
        let mut unallocated_nursery_blocks = Vec::with_capacity(high_water - blocks);
        for i in blocks..high_water {
            unallocated_nursery_blocks.push(self.buffer[i].load(Ordering::Relaxed));
        }
        packets.push(Box::new(RCReleaseUnallocatedNurseryBlocks {
            space,
            blocks: unallocated_nursery_blocks,
        }));
        self.high_water.store(0, Ordering::SeqCst);
        self.cursor.store(0, Ordering::SeqCst);
        (packets, blocks)
    }

    fn space(&self) -> &'static ImmixSpace<VM> {
        self.space.unwrap()
    }

    #[inline(always)]
    fn initialize_new_clean_block(&self, block: Block, copy: bool, cm_enabled: bool) {
        if self.space().in_defrag() {
            self.space().defrag.notify_new_clean_block(copy);
        }
        if cm_enabled && !super::BLOCK_ONLY && !self.space().rc_enabled {
            let current_state = self.space().line_mark_state.load(Ordering::Acquire);
            for line in block.lines() {
                line.mark(current_state);
            }
        }
        if self.space().rc_enabled && copy {
            block.initialize_log_table_as_unlogged::<VM>();
        }
        // println!("Alloc {:?} {}", block, copy);
        block.init(copy, false, self.space());
        self.space()
            .chunk_map
            .set(block.chunk(), ChunkState::Allocated);
    }

    #[inline(always)]
    fn alloc_clean_block_fast(&self) -> Option<Block> {
        let i = self
            .cursor
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |i| {
                let high_water = self.high_water.load(Ordering::SeqCst);
                if i >= high_water {
                    None
                } else {
                    Some(i + 1)
                }
            })
            .ok()?;
        Some(self.buffer[i].load(Ordering::SeqCst))
    }

    #[cold]
    fn alloc_clean_block_slow(&mut self, tls: VMThread) -> Option<Block> {
        let _guard = self.refill_lock.lock().unwrap();
        // Retry allocation
        if let Some(block) = self.alloc_clean_block_fast() {
            return Some(block);
        }
        // Fill buffer with N blocks
        if !self.space().rc_enabled {
            self.high_water.store(0, Ordering::SeqCst);
            self.cursor.store(0, Ordering::SeqCst);
        }
        // Check for GC
        if self
            .space()
            .bulk_poll(tls, self.refill_count << Block::LOG_PAGES)
            .is_err()
        {
            return None;
        }
        debug_assert_eq!(
            self.cursor.load(Ordering::SeqCst),
            self.high_water.load(Ordering::SeqCst)
        );
        let len = self.buffer.len();
        let self_mut = unsafe { &mut *(self as *const Self as *mut Self) };
        if len < self.high_water.load(Ordering::SeqCst) + self.refill_count {
            self_mut
                .buffer
                .resize_with(len << 1, || Atomic::new(Block::ZERO))
        }
        // Alloc first block
        let result = Block::from(unsafe {
            self.space()
                .bulk_acquire_uninterruptible(tls, Block::PAGES, false)?
        });
        let i = self.cursor.fetch_add(1, Ordering::SeqCst);
        self_mut.buffer[i].store(result, Ordering::Relaxed);
        self.high_water.store(
            self.high_water.load(Ordering::Relaxed) + 1,
            Ordering::SeqCst,
        );
        // Alloc other blocks
        let push_new_block = |block: Block| {
            let i = self.high_water.load(Ordering::Relaxed);
            self_mut.buffer[i].store(block, Ordering::Relaxed);
            self.high_water.store(i + 1, Ordering::SeqCst);
        };
        for _ in 1..self.refill_count {
            if let Some(a) = unsafe {
                self.space()
                    .bulk_acquire_uninterruptible(tls, Block::PAGES, false)
            } {
                push_new_block(Block::from(a));
            } else {
                break;
            }
        }
        Some(result)
    }

    /// Allocate a clean block.
    #[inline(always)]
    fn alloc_clean_block(&self, tls: VMThread) -> Option<Block> {
        if let Some(block) = self.alloc_clean_block_fast() {
            return Some(block);
        }
        let me = unsafe { &mut *(self as *const Self as *mut Self) };
        match me.alloc_clean_block_slow(tls) {
            Some(block) => Some(block),
            _ => {
                VM::VMCollection::block_for_gc(VMMutatorThread(tls)); // We asserted that this is mutator.
                None
            }
        }
    }

    /// Allocate a clean block.
    #[inline(never)]
    pub fn get_clean_block(&self, tls: VMThread, copy: bool, lock_free: bool) -> Option<Block> {
        let block = if lock_free {
            self.alloc_clean_block(tls)?
        } else {
            let block_address = self.space().acquire_no_lock(tls, Block::PAGES);
            if block_address.is_zero() {
                return None;
            }
            Block::from(block_address)
        };
        self.initialize_new_clean_block(block, copy, self.space().cm_enabled);
        Some(block)
    }

    /// Pop a reusable block from the reusable block list.
    #[inline(always)]
    pub fn get_reusable_block(&self, copy: bool) -> Option<Block> {
        if super::BLOCK_ONLY {
            return None;
        }
        loop {
            if let Some(block) = self.space().reusable_blocks.pop() {
                if copy && block.is_defrag_source() {
                    continue;
                }
                if self.space().rc_enabled {
                    if crate::args::RC_MATURE_EVACUATION && block.is_defrag_source() {
                        continue;
                    }
                    // Blocks in the `reusable_blocks` queue can be released after some RC collections.
                    // These blocks can either have `Unallocated` state, or be reallocated again.
                    // Skip these cases and only return the truly reusable blocks.
                    if !block.get_state().is_reusable() {
                        continue;
                    }
                    if !copy && !block.attempt_mutator_reuse() {
                        continue;
                    }
                }
                block.init(copy, true, self.space());
                return Some(block);
            } else {
                return None;
            }
        }
    }
}

pub struct RCSweepNurseryBlocks<VM: VMBinding> {
    pub space: &'static ImmixSpace<VM>,
    pub blocks: Vec<Block>,
}

impl<VM: VMBinding> GCWork<VM> for RCSweepNurseryBlocks<VM> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        for block in &self.blocks {
            block.rc_sweep_nursery(self.space)
        }
    }
}

struct RCReleaseUnallocatedNurseryBlocks<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    blocks: Vec<Block>,
}

impl<VM: VMBinding> GCWork<VM> for RCReleaseUnallocatedNurseryBlocks<VM> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        if self.blocks.is_empty() {
            return;
        }
        for block in &self.blocks {
            self.space.pr.release_block(*block);
        }
    }
}
