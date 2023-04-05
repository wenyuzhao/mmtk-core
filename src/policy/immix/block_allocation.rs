use super::{block::Block, ImmixSpace};
use crate::plan::immix::Pause;
use crate::{
    plan::lxr::LXR,
    policy::space::Space,
    scheduler::{GCWork, GCWorkScheduler, GCWorker},
    util::{heap::chunk_map::ChunkState, linear_scan::Region, VMThread},
    vm::*,
    LazySweepingJobsCounter, MMTK,
};
use atomic::{Atomic, Ordering};
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;

pub struct BlockAllocation<VM: VMBinding> {
    space: Option<&'static ImmixSpace<VM>>,
    nursery_blocks: AtomicUsize,
    buffer: RwLock<Vec<Atomic<Block>>>,
    pub(crate) lxr: Option<&'static LXR<VM>>,
}

impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: None,
            nursery_blocks: AtomicUsize::new(0),
            buffer: RwLock::new((0..32768).map(|_| Atomic::new(Block::ZERO)).collect()),
            lxr: None,
        }
    }

    fn space(&self) -> &'static ImmixSpace<VM> {
        self.space.unwrap()
    }

    pub fn nursery_blocks(&self) -> usize {
        self.nursery_blocks.load(Ordering::SeqCst)
    }

    pub fn nursery_mb(&self) -> usize {
        self.nursery_blocks() << Block::LOG_BYTES >> 20
    }

    pub fn init(&mut self, space: &'static ImmixSpace<VM>) {
        self.space = Some(space);
    }

    /// Reset allocated_block_buffer and free nursery blocks.
    pub fn sweep_and_reset(&mut self, scheduler: &GCWorkScheduler<VM>) {
        const MAX_STW_SWEEP_BLOCKS: usize = usize::MAX;
        let space = self.space();
        // Sweep nursery blocks
        let total_nursery_blocks = self.nursery_blocks.load(Ordering::SeqCst);
        let stw_limit = usize::min(total_nursery_blocks, MAX_STW_SWEEP_BLOCKS);
        let blocks = self.buffer.read().unwrap();
        // 1. STW release a limited number of blocks
        for b in &blocks[0..stw_limit] {
            let block = b.load(Ordering::Relaxed);
            debug_assert_ne!(block.get_state(), super::block::BlockState::Unallocated);
            block.rc_sweep_nursery(space, true);
        }
        // 2. Release remaining blocks concurrently after the pause
        if total_nursery_blocks > stw_limit {
            let packets = blocks[stw_limit..total_nursery_blocks]
                .chunks(1024)
                .map(|c| {
                    let blocks: Vec<Block> = c.iter().map(|x| x.load(Ordering::Relaxed)).collect();
                    Box::new(RCSweepNurseryBlocks::new(blocks)) as Box<dyn GCWork<VM>>
                })
                .collect();
            scheduler.postpone_all_prioritized(packets);
        }
        self.nursery_blocks.store(0, Ordering::SeqCst);
    }

    /// Notify a GC pahse has started
    pub fn notify_mutator_phase_end(&self) {}

    pub fn concurrent_marking_in_progress_or_final_mark(&self) -> bool {
        let lxr = self.lxr.unwrap();
        lxr.concurrent_marking_in_progress() || lxr.current_pause() == Some(Pause::FinalMark)
    }

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
        // Initialize unlog table
        if self.space().rc_enabled && copy {
            block.initialize_log_table_as_unlogged::<VM>();
        }
        // Initialize mark table
        if self.space().rc_enabled {
            if self.concurrent_marking_in_progress_or_final_mark() {
                block.initialize_mark_table_as_marked::<VM>();
            } else {
                // TODO: Performance? Is this necessary?
                block.clear_mark_table::<VM>();
            }
        }
        // println!("Alloc {:?} {}", block, copy);
        block.init(copy, false, self.space());
        self.space()
            .chunk_map
            .set(block.chunk(), ChunkState::Allocated);
    }

    /// Allocate a clean block.
    pub fn get_clean_block(&self, tls: VMThread, copy: bool, _lock_free: bool) -> Option<Block> {
        let block = {
            let block_address = self.space().acquire(tls, Block::PAGES);
            if block_address.is_zero() {
                return None;
            }
            Block::from_aligned_address(block_address)
        };
        if !copy {
            let i = self.nursery_blocks.fetch_add(1, Ordering::SeqCst);
            let buffer = self.buffer.read().unwrap();
            if i < buffer.len() {
                buffer[i].store(block, Ordering::SeqCst);
            } else {
                // resize
                std::mem::drop(buffer);
                let mut buffer = self.buffer.write().unwrap();
                if i >= buffer.len() {
                    buffer.resize_with(i << 1, || Atomic::new(Block::ZERO));
                }
                buffer[i].store(block, Ordering::Relaxed);
            }
        }
        self.initialize_new_clean_block(block, copy, self.space().cm_enabled);
        if self.space().common().zeroed && !copy && cfg!(feature = "force_zeroing") {
            crate::util::memory::zero_w(block.start(), Block::BYTES);
        }
        Some(block)
    }

    /// Pop a reusable block from the reusable block list.
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
    pub fn new(blocks: Vec<Block>) -> Self {
        Self {
            blocks,
            _counter: LazySweepingJobsCounter::new_decs(),
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCSweepNurseryBlocks {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let space = &mmtk.plan.downcast_ref::<LXR<VM>>().unwrap().immix_space;
        let mut released_blocks = 0;
        for block in &self.blocks {
            if block.rc_sweep_nursery(space, false) {
                released_blocks += 1;
            }
        }
        space
            .num_clean_blocks_released_lazy
            .fetch_add(released_blocks, Ordering::SeqCst);
    }
}
