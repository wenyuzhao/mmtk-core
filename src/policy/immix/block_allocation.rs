use super::{block::Block, chunk::ChunkState, ImmixSpace};
use crate::{
    plan::lxr::LXR,
    policy::space::Space,
    scheduler::{GCWork, GCWorkScheduler, GCWorker},
    util::{VMMutatorThread, VMThread},
    vm::*,
    LazySweepingJobsCounter, MMTK,
};
use atomic::{Atomic, Ordering};
use spin::Lazy;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

static LOCK_FREE_BLOCKS_CAPACITY: Lazy<usize> =
    Lazy::new(|| usize::max(crate::args().lock_free_blocks << 2, 32768 * 4));

pub struct BlockAllocation<VM: VMBinding> {
    space: Option<&'static ImmixSpace<VM>>,
    cursor: AtomicUsize,
    high_water: AtomicUsize,
    buffer: Vec<Atomic<Block>>,
    pub refill_lock: Mutex<()>,
    refill_count: usize,
    previously_allocated_nursery_blocks: AtomicUsize,
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
            refill_count: crate::args().lock_free_blocks, // num_cpus::get(),
            previously_allocated_nursery_blocks: AtomicUsize::new(0),
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

    /// Reset allocated_block_buffer and free nursery blocks.
    pub fn sweep_and_reset(&mut self, scheduler: &GCWorkScheduler<VM>) {
        const MAX_STW_SWEEP_BLOCKS: usize = usize::MAX;
        let _guard = self.refill_lock.lock().unwrap();
        let space = self.space();
        // Sweep nursery blocks
        let limit = self
            .previously_allocated_nursery_blocks
            .load(Ordering::SeqCst);
        let stw_limit = usize::min(limit, MAX_STW_SWEEP_BLOCKS);
        for block in &self.buffer[0..stw_limit] {
            let block = block.load(Ordering::Relaxed);
            block.rc_sweep_nursery(space, false);
        }
        if limit > stw_limit {
            let packets = self.buffer[stw_limit..limit]
                .chunks(1024)
                .map(|c| {
                    let blocks: Vec<Block> = c.iter().map(|x| x.load(Ordering::Relaxed)).collect();
                    Box::new(RCSweepNurseryBlocks::new(blocks)) as Box<dyn GCWork<VM>>
                })
                .collect();
            scheduler.postpone_all_prioritized(packets);
            // scheduler.work_buckets[WorkBucketStage::Unconstrained].bulk_add(packets)
        }
        // Sweep unused pre-allocated blocks
        let cursor = self.cursor.load(Ordering::Relaxed);
        let high_water = self.high_water.load(Ordering::Relaxed);
        for block in &self.buffer[cursor..high_water] {
            space.pr.release_block(block.load(Ordering::Relaxed))
        }
        self.high_water.store(0, Ordering::Relaxed);
        self.cursor.store(0, Ordering::SeqCst);
    }

    /// Notify a GC pahse has started
    pub fn notify_mutator_phase_end(&mut self) {
        let _guard = self.refill_lock.lock().unwrap();
        let cursor = self.cursor.load(Ordering::SeqCst);
        self.previously_allocated_nursery_blocks
            .store(cursor, Ordering::SeqCst);
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
            if VM::VMObjectModel::compressed_pointers_enabled() {
                block.initialize_log_table_as_unlogged::<VM, true>();
            } else {
                block.initialize_log_table_as_unlogged::<VM, false>();
            }
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
        if self.space().common().zeroed && !copy && cfg!(feature = "force_zeroing") {
            crate::util::memory::zero_w(block.start(), Block::BYTES);
        }
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

pub struct RCSweepNurseryBlocks {
    blocks: Vec<Block>,
    _counter: LazySweepingJobsCounter,
}

impl RCSweepNurseryBlocks {
    #[inline]
    pub fn new(blocks: Vec<Block>) -> Self {
        Self {
            blocks,
            _counter: LazySweepingJobsCounter::new_desc(),
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCSweepNurseryBlocks {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let space = &mmtk.plan.downcast_ref::<LXR<VM>>().unwrap().immix_space;
        let mut released_blocks = 0;
        for block in &self.blocks {
            if block.rc_sweep_nursery(space, true) {
                released_blocks += 1;
            }
        }
        space
            .num_clean_blocks_released_lazy
            .fetch_add(released_blocks, Ordering::SeqCst);
    }
}
