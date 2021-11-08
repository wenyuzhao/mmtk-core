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
use crossbeam_queue::ArrayQueue;
use spin::Lazy;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

static LOCK_FREE_BLOCKS_CAPACITY: Lazy<usize> = Lazy::new(|| {
    if crate::args::REF_COUNT {
        (*crate::args::INITIAL_NURSERY_BLOCKS + *LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE) << 2
    } else {
        *LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE << 2
    }
});

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
                .release_pages(self.buffer[i].load(Ordering::SeqCst).start())
        }
        self.high_water.store(0, Ordering::SeqCst);
        self.cursor.store(0, Ordering::SeqCst);
    }

    fn split_bins(&self, workers: usize, jobs: usize) -> (usize, usize, Vec<Vec<Block>>) {
        let num_bins = workers << 1;
        let bin_cap = jobs / num_bins + if jobs % num_bins == 0 { 0 } else { 1 };
        let bins = (0..num_bins)
            .map(|_| Vec::with_capacity(bin_cap))
            .collect::<Vec<Vec<Block>>>();
        (num_bins, bin_cap, bins)
    }

    /// Drain allocated_block_buffer and free everything in clean_block_buffer.
    pub fn reset_and_generate_nursery_sweep_tasks(
        &mut self,
        num_workers: usize,
    ) -> (Vec<Box<dyn GCWork<VM>>>, Vec<Box<dyn GCWork<VM>>>, usize) {
        let _guard = self.refill_lock.lock().unwrap();
        let blocks = self.cursor.load(Ordering::SeqCst);
        let high_water = self.high_water.load(Ordering::SeqCst);
        // Release unallocated blocks in the buffer
        for i in blocks..high_water {
            self.space()
                .pr
                .release_pages(self.buffer[i].load(Ordering::SeqCst).start())
        }
        // Block sweeping packets
        let mut block_cursor = 0;
        // 1. STW sweep packages
        let stw_sweep_bytes: usize = (1 << 20) * num_cpus::get();
        let stw_sweep_blocks: usize = usize::min(stw_sweep_bytes / Block::BYTES, blocks);
        println!("stw_sweep_blocks: {} / {}", stw_sweep_blocks, blocks);
        let (num_bins, bin_cap, mut bins) = self.split_bins(num_workers, stw_sweep_blocks);
        'out: for i in 0..num_bins {
            for _ in 0..bin_cap {
                if block_cursor < blocks {
                    bins[i].push(self.buffer[block_cursor].load(Ordering::Relaxed));
                    block_cursor += 1;
                } else {
                    break 'out;
                }
            }
        }
        let space = self.space();
        let stw_packets = bins
            .into_iter()
            .map::<Box<dyn GCWork<VM>>, _>(|blocks| box RCSweepNurseryBlocks { space, blocks })
            .collect();

        // 2. Conc sweep packets
        let delayed_sweep_blocks: usize = blocks - stw_sweep_blocks;
        let (num_bins, bin_cap, mut bins) = self.split_bins(num_workers, delayed_sweep_blocks);
        'out2: for i in 0..num_bins {
            for _ in 0..bin_cap {
                if block_cursor < blocks {
                    bins[i].push(self.buffer[block_cursor].load(Ordering::Relaxed));
                    block_cursor += 1;
                } else {
                    break 'out2;
                }
            }
        }
        let delayed_packets = bins
            .into_iter()
            .map::<Box<dyn GCWork<VM>>, _>(|blocks| box RCSweepNurseryBlocks { space, blocks })
            .collect();
        // Reset cursor and high_water
        self.high_water.store(0, Ordering::SeqCst);
        self.cursor.store(0, Ordering::SeqCst);
        (stw_packets, delayed_packets, blocks)
    }

    pub fn reset_and_generate_nursery_sweep_tasks3(
        &mut self,
        _num_workers: usize,
    ) -> (Vec<Box<dyn GCWork<VM>>>, usize) {
        let _guard = self.refill_lock.lock().unwrap();
        let blocks = self.cursor.load(Ordering::SeqCst);
        let high_water = self.high_water.load(Ordering::SeqCst);
        let mut packets: Vec<Box<dyn GCWork<VM>>> = Vec::with_capacity(blocks + 1);
        let space = self.space();
        for i in 0..blocks {
            let block = self.buffer[i].load(Ordering::Relaxed);
            packets.push(box RCSweepNurseryBlock { space, block })
        }
        let mut unallocated_nursery_blocks = Vec::with_capacity(high_water - blocks);
        for i in blocks..high_water {
            unallocated_nursery_blocks.push(self.buffer[i].load(Ordering::Relaxed));
        }
        packets.push(box RCReleaseUnallocatedNurseryBlocks {
            space,
            blocks: unallocated_nursery_blocks,
        });
        self.high_water.store(0, Ordering::SeqCst);
        self.cursor.store(0, Ordering::SeqCst);
        (packets, blocks)
    }

    pub fn reset_and_generate_nursery_sweep_tasks2(
        &mut self,
        num_workers: usize,
    ) -> (Vec<Box<dyn GCWork<VM>>>, usize) {
        let _guard = self.refill_lock.lock().unwrap();
        let blocks = self.cursor.load(Ordering::SeqCst);
        let high_water = self.high_water.load(Ordering::SeqCst);
        let num_bins = num_workers << 1;
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
            .map::<Box<dyn GCWork<VM>>, _>(|blocks| box RCSweepNurseryBlocks { space, blocks })
            .collect();
        let mut unallocated_nursery_blocks = Vec::with_capacity(high_water - blocks);
        for i in blocks..high_water {
            unallocated_nursery_blocks.push(self.buffer[i].load(Ordering::Relaxed));
        }
        packets.push(box RCReleaseUnallocatedNurseryBlocks {
            space,
            blocks: unallocated_nursery_blocks,
        });
        self.high_water.store(0, Ordering::SeqCst);
        self.cursor.store(0, Ordering::SeqCst);
        (packets, blocks)
    }

    const fn space(&self) -> &'static ImmixSpace<VM> {
        self.space.unwrap()
    }

    #[inline(always)]
    fn initialize_new_clean_block(&self, block: Block, copy: bool) {
        if self.space().in_defrag() {
            self.space().defrag.notify_new_clean_block(copy);
        }
        if crate::plan::immix::CONCURRENT_MARKING && !super::BLOCK_ONLY && !super::REF_COUNT {
            let current_state = self.space().line_mark_state.load(Ordering::Acquire);
            for line in block.lines() {
                line.mark(current_state);
            }
        }
        if crate::args::REF_COUNT && copy {
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
        if !crate::args::REF_COUNT {
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
                .bulk_acquire_uninterruptible(tls, Block::PAGES, true)?
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
    pub fn get_clean_block(&self, tls: VMThread, copy: bool) -> Option<Block> {
        let block = if crate::args::LOCK_FREE_BLOCK_ALLOCATION {
            self.alloc_clean_block(tls)?
        } else {
            let block_address = self.space().acquire(tls, Block::PAGES);
            if block_address.is_zero() {
                return None;
            }
            Block::from(block_address)
        };
        self.initialize_new_clean_block(block, copy);
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
                if crate::args::RC_MATURE_EVACUATION && block.is_defrag_source() {
                    continue;
                }
                if crate::args::REF_COUNT {
                    // Blocks in the `reusable_blocks` queue can be released after some RC collections.
                    // These blocks can either have `Unallocated` state, or be reallocated again.
                    // Skip these cases and only return the truly reusable blocks.
                    if !block.get_state().is_reusable() {
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

struct RCSweepNurseryBlocks<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    blocks: Vec<Block>,
}

impl<VM: VMBinding> GCWork<VM> for RCSweepNurseryBlocks<VM> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        if self.blocks.is_empty() {
            return;
        }
        let queue = ArrayQueue::new(self.blocks.len());
        for block in &self.blocks {
            block.deinit();
            queue.push(block.start()).unwrap();
        }
        self.space.pr.release_bulk(self.blocks.len(), queue)
    }
}

struct RCSweepNurseryBlock<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    block: Block,
}

impl<VM: VMBinding> GCWork<VM> for RCSweepNurseryBlock<VM> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        self.block.rc_sweep_nursery(self.space);
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
        let queue = ArrayQueue::new(self.blocks.len());
        for block in &self.blocks {
            queue.push(block.start()).unwrap();
        }
        self.space.pr.release_bulk(self.blocks.len(), queue)
    }
}
