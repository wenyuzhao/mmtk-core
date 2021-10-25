use super::{block::Block, chunk::ChunkState, ImmixSpace};
use crate::{
    flags::LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE,
    policy::space::Space,
    scheduler::{GCWork, GCWorker},
    util::{rc::RC_LOCK_BITS, VMMutatorThread, VMThread},
    vm::*,
    MMTK,
};
use atomic::{Atomic, Ordering};
use spin::{Lazy, Mutex};
use std::sync::atomic::AtomicUsize;

static LOCK_FREE_BLOCKS_CAPACITY: Lazy<usize> = Lazy::new(|| {
    if crate::flags::REF_COUNT {
        (crate::flags::NURSERY_BLOCKS_THRESHOLD_FOR_RC + *LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE)
            << 1
    } else {
        *LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE << 1
    }
});

pub struct BlockAllocation<VM: VMBinding> {
    space: Option<&'static ImmixSpace<VM>>,
    cursor: AtomicUsize,
    high_water: AtomicUsize,
    buffer: Vec<Atomic<Block>>,
    refill_lock: Mutex<()>,
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
        let _guard = self.refill_lock.lock();
        let cursor = self.cursor.load(Ordering::SeqCst);
        let high_water = self.high_water.load(Ordering::SeqCst);
        for i in cursor..high_water {
            self.space()
                .pr
                .release_pages(self.buffer[i].load(Ordering::SeqCst).start())
        }
        self.cursor.store(0, Ordering::SeqCst);
        self.high_water.store(0, Ordering::SeqCst);
    }

    /// Drain allocated_block_buffer and free everything in clean_block_buffer.
    pub fn reset_and_generate_nursery_sweep_tasks(
        &mut self,
        num_workers: usize,
    ) -> (Vec<Box<dyn GCWork<VM>>>, usize) {
        let _guard = self.refill_lock.lock();
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
        for i in blocks..high_water {
            self.space()
                .pr
                .release_pages(self.buffer[i].load(Ordering::SeqCst).start())
        }
        self.cursor.store(0, Ordering::SeqCst);
        self.high_water.store(0, Ordering::SeqCst);
        let space = self.space();
        let packets = bins
            .into_iter()
            .map::<Box<dyn GCWork<VM>>, _>(|blocks| box RCSweepNurseryBlocks { space, blocks })
            .collect();
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
        // println!("Alloc {:?} {}", block, copy);
        block.init(copy, false, self.space());
        if cfg!(debug_assertions) {
            if crate::flags::BARRIER_MEASUREMENT || self.space().common().needs_log_bit {
                block.assert_log_table_cleared::<VM>(&RC_LOCK_BITS);
                block.assert_log_table_cleared::<VM>(
                    VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                        .as_spec()
                        .extract_side_spec(),
                );
            }
        }
        self.space()
            .chunk_map
            .set(block.chunk(), ChunkState::Allocated);
    }

    #[inline(always)]
    fn alloc_clean_block_fast(&self) -> Option<Block> {
        let high_water = self.high_water.load(Ordering::SeqCst);
        let i = self
            .cursor
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |i| {
                if i >= high_water {
                    None
                } else {
                    Some(i + 1)
                }
            })
            .ok()?;
        Some(self.buffer[i].load(Ordering::SeqCst))
    }

    #[inline]
    fn alloc_clean_block_slow_rc(&mut self, tls: VMThread) -> Option<Block> {
        let _guard = self.refill_lock.lock();
        // Retry allocation
        if let Some(block) = self.alloc_clean_block_fast() {
            return Some(block);
        }
        // Fill buffer with N blocks
        // Check for GC
        if self
            .space()
            .bulk_poll(tls, self.refill_count << Block::LOG_PAGES)
            .is_err()
        {
            return None;
        }
        // Alloc first block
        let result = Block::from(self.space().bulk_acquire_uninterruptible(
            tls,
            Block::PAGES,
            true,
        )?);
        let i = self.cursor.fetch_add(1, Ordering::SeqCst);
        self.buffer[i].store(result, Ordering::Relaxed);
        self.high_water.store(
            self.high_water.load(Ordering::Relaxed) + 1,
            Ordering::SeqCst,
        );
        // Alloc other blocks
        let push_new_block = |block: Block| {
            let i = self.high_water.load(Ordering::Relaxed);
            self.buffer[i].store(block, Ordering::Relaxed);
            self.high_water.store(i + 1, Ordering::SeqCst);
        };
        for _ in 1..self.refill_count {
            if let Some(a) = self
                .space()
                .bulk_acquire_uninterruptible(tls, Block::PAGES, false)
            {
                push_new_block(Block::from(a));
            } else {
                break;
            }
        }
        Some(result)
    }

    #[inline]
    fn alloc_clean_block_slow_ix(&mut self, tls: VMThread) -> Option<Block> {
        unreachable!()
        // let _guard = self.refill_lock.lock();
        // // Retry allocation
        // if let Some(block) = self.alloc_clean_block_fast() {
        //     return Some(block);
        // }
        // // Fill buffer with N blocks
        // let r = Block::from(self.space().acquire_no_poll(tls, Block::PAGES)?);
        // self.high_water.store(0, Ordering::SeqCst);
        // self.high_water.store(0, Ordering::SeqCst);

        // let i = self.cursor.fetch_add(1, Ordering::SeqCst);
        // self.buffer[i].store(r, Ordering::Relaxed);
        // self.high_water.store(1, Ordering::SeqCst);
        // let push_new_block = |block: Block| {
        //     let i = self.high_water.load(Ordering::Relaxed);
        //     self.buffer[i].store(block, Ordering::Relaxed);
        //     self.high_water.store(i + 1, Ordering::SeqCst);
        // };
        // for _ in 0..self.refill_count {
        //     let a = self.space().acquire_no_poll(tls, Block::PAGES)?;
        //     push_new_block(Block::from(a));
        // }
        // Some(r)
    }

    #[cold]
    fn alloc_clean_block_slow(&mut self, tls: VMThread) -> Option<Block> {
        if crate::flags::REF_COUNT {
            self.alloc_clean_block_slow_rc(tls)
        } else {
            self.alloc_clean_block_slow_ix(tls)
        }
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
        let block = if crate::flags::LOCK_FREE_BLOCK_ALLOCATION {
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
                if crate::flags::RC_MATURE_EVACUATION && block.is_defrag_source() {
                    continue;
                }
                if crate::flags::REF_COUNT {
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
        for b in &self.blocks {
            b.rc_sweep_nursery(self.space);
        }
    }
}
