use super::block::BlockState;
use super::{block::Block, ImmixSpace};
use crate::plan::immix::Pause;
use crate::scheduler::WorkBucketStage;
use crate::{
    plan::lxr::LXR,
    policy::space::Space,
    scheduler::{GCWork, GCWorkScheduler, GCWorker},
    vm::*,
    LazySweepingJobsCounter, MMTK,
};
use atomic::{Atomic, Ordering};
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;

pub(crate) struct BlockCache {
    cursor: AtomicUsize,
    buffer: RwLock<Vec<Atomic<Block>>>,
}

impl BlockCache {
    fn new() -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::BLOCK_ALLOC_BUFFER_COUNTER.add(32768);
        }
        Self {
            cursor: AtomicUsize::new(0),
            buffer: RwLock::new((0..32768).map(|_| Atomic::new(Block::ZERO)).collect()),
        }
    }

    fn len(&self) -> usize {
        self.cursor.load(Ordering::SeqCst)
    }

    pub fn push(&self, block: Block) {
        let i = self.cursor.fetch_add(1, Ordering::SeqCst);
        let buffer = self.buffer.read().unwrap();
        if i < buffer.len() {
            buffer[i].store(block, Ordering::SeqCst);
        } else {
            // resize
            std::mem::drop(buffer);
            let mut buffer = self.buffer.write().unwrap();
            if i >= buffer.len() {
                if cfg!(feature = "rust_mem_counter") {
                    crate::rust_mem_counter::BLOCK_ALLOC_BUFFER_COUNTER.sub(buffer.len());
                    crate::rust_mem_counter::BLOCK_ALLOC_BUFFER_COUNTER.add(i << 1);
                }
                buffer.resize_with(i << 1, || Atomic::new(Block::ZERO));
            }
            buffer[i].store(block, Ordering::Relaxed);
        }
    }

    fn visit_slice(&self, f: impl Fn(&[Atomic<Block>])) {
        let count = self.cursor.load(Ordering::SeqCst);
        let blocks = self.buffer.read().unwrap();
        f(&blocks[0..count])
    }

    fn reset(&self) {
        self.cursor.store(0, Ordering::SeqCst);
    }
}

pub struct BlockAllocation<VM: VMBinding> {
    space: UnsafeCell<*const ImmixSpace<VM>>,
    pub(super) nursery_blocks: BlockCache,
    pub(crate) reused_blocks: BlockCache,
    pub(crate) lxr: Option<&'static LXR<VM>>,
}

impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: UnsafeCell::new(std::ptr::null()),
            nursery_blocks: BlockCache::new(),
            reused_blocks: BlockCache::new(),
            lxr: None,
        }
    }

    fn space(&self) -> &'static ImmixSpace<VM> {
        unsafe { &**self.space.get() }
    }

    pub fn nursery_blocks(&self) -> usize {
        self.nursery_blocks.len()
    }

    pub fn nursery_mb(&self) -> usize {
        self.nursery_blocks() << Block::LOG_BYTES >> 20
    }

    pub fn init(&self, space: &ImmixSpace<VM>) {
        unsafe { *self.space.get() = space as *const ImmixSpace<VM> }
    }

    pub fn reset_block_mark_for_mutator_reused_blocks(&self, pause: Pause) {
        if pause == Pause::RefCount || pause == Pause::InitialMark {
            return;
        }
        // SATB sweep has problem scanning mutator recycled blocks.
        // Remaing the block state as "reusing" and reset them here.
        self.reused_blocks.visit_slice(|blocks| {
            for b in blocks {
                let b = b.load(Ordering::Relaxed);
                b.set_state(BlockState::Marked);
            }
        });
    }

    pub fn sweep_mutator_reused_blocks(&self, scheduler: &GCWorkScheduler<VM>, pause: Pause) {
        if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
            self.reused_blocks.reset();
            return;
        }
        const MAX_STW_SWEEP_BLOCKS: usize = usize::MAX;
        self.reused_blocks.visit_slice(|blocks| {
            let total_blocks = blocks.len();
            #[cfg(feature = "lxr_release_stage_timer")]
            gc_log!([3] "    - Process {}/{} mutator-reused blocks in the pause (single-thread)", total_blocks, total_blocks);
            let stw_limit = usize::min(total_blocks, MAX_STW_SWEEP_BLOCKS);
            // 1. STW release a limited number of blocks
            for b in &blocks[0..stw_limit] {
                let block = b.load(Ordering::Relaxed);
                self.space()
                    .add_to_possibly_dead_mature_blocks(block, false);
            }
            // 2. Release remaining blocks concurrently after the pause
            if total_blocks > stw_limit {
                let packets = blocks[stw_limit..total_blocks]
                    .chunks(1024)
                    .map(|c| {
                        let blocks: Vec<Block> =
                            c.iter().map(|x| x.load(Ordering::Relaxed)).collect();
                        Box::new(RCLazySweepMutatorReusedBlocks::new(blocks)) as Box<dyn GCWork<VM>>
                    })
                    .collect();
                scheduler.postpone_all_prioritized(packets);
            }
        });
        self.reused_blocks.reset();
    }

    /// Reset allocated_block_buffer and free nursery blocks.
    pub fn sweep_nursery_blocks(&self, scheduler: &GCWorkScheduler<VM>, pause: Pause) {
        const PARALLEL_STW_SWEEPING: bool = false;
        let max_stw_sweep_blocks: usize = if cfg!(feature = "lxr_no_lazy_young_sweeping")
            || pause == Pause::FinalMark
            || pause == Pause::FullTraceFast
        {
            usize::MAX
        } else {
            (num_cpus::get() << 23) >> Block::LOG_BYTES // 2M for each core
        };
        let space = self.space();
        // Sweep nursery blocks
        self.nursery_blocks.visit_slice(|blocks| {
            if PARALLEL_STW_SWEEPING {
                return self.parallel_sweep_all_nursery_blocks(scheduler, blocks);
            }
            let total_nursery_blocks = blocks.len();
            let stw_limit = if pause == Pause::FullTraceFast {
                total_nursery_blocks
            } else {
                usize::min(total_nursery_blocks, max_stw_sweep_blocks)
            };
            #[cfg(feature = "lxr_release_stage_timer")]
            gc_log!([3] "    - Process {}/{} young blocks in the pause (single-thread)", stw_limit, total_nursery_blocks);
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
                        let blocks: Vec<Block> =
                            c.iter().map(|x| x.load(Ordering::Relaxed)).collect();
                        Box::new(RCLazySweepNurseryBlocks::new(blocks)) as Box<dyn GCWork<VM>>
                    })
                    .collect();
                scheduler.postpone_all_prioritized(packets);
            }
        });
        self.nursery_blocks.reset();
    }

    fn parallel_sweep_all_nursery_blocks(
        &self,
        scheduler: &GCWorkScheduler<VM>,
        blocks: &[Atomic<Block>],
    ) {
        let total_nursery_blocks = blocks.len();
        let packets = blocks[..total_nursery_blocks]
            .chunks(1024)
            .map(|c| {
                let blocks: Vec<Block> = c.iter().map(|x| x.load(Ordering::Relaxed)).collect();
                Box::new(RCSTWSweepNurseryBlocks::new(blocks)) as Box<dyn GCWork<VM>>
            })
            .collect();
        scheduler.work_buckets[WorkBucketStage::Unconstrained].bulk_add(packets);
    }

    /// Notify a GC pahse has started
    pub fn notify_mutator_phase_end(&self) {}

    pub fn concurrent_marking_in_progress_or_final_mark(&self) -> bool {
        let lxr = self.lxr.unwrap();
        lxr.concurrent_marking_in_progress() || lxr.current_pause() == Some(Pause::FinalMark)
    }

    pub(super) fn initialize_new_clean_block(&self, block: Block, copy: bool, cm_enabled: bool) {
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
        if (self.space().rc_enabled
            || (crate::args::BARRIER_MEASUREMENT && !crate::args::BARRIER_MEASUREMENT_NO_SLOW))
            && copy
        {
            block.initialize_field_unlog_table_as_unlogged::<VM>();
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
        if self.space().common().zeroed && !copy && cfg!(feature = "force_zeroing") {
            crate::util::memory::zero_w(block.start(), Block::BYTES);
        }
    }
}

pub struct RCLazySweepMutatorReusedBlocks {
    blocks: Vec<Block>,
    _counter: LazySweepingJobsCounter,
}

impl RCLazySweepMutatorReusedBlocks {
    pub fn new(blocks: Vec<Block>) -> Self {
        Self {
            blocks,
            _counter: LazySweepingJobsCounter::new_decs(),
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCLazySweepMutatorReusedBlocks {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let space = &mmtk.plan.downcast_ref::<LXR<VM>>().unwrap().immix_space;
        for block in &self.blocks {
            space.add_to_possibly_dead_mature_blocks(*block, false);
        }
    }
}

pub struct RCLazySweepNurseryBlocks {
    blocks: Vec<Block>,
    _counter: LazySweepingJobsCounter,
}

impl RCLazySweepNurseryBlocks {
    pub fn new(blocks: Vec<Block>) -> Self {
        Self {
            blocks,
            _counter: LazySweepingJobsCounter::new_decs(),
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCLazySweepNurseryBlocks {
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

pub struct RCSTWSweepNurseryBlocks {
    blocks: Vec<Block>,
    _counter: LazySweepingJobsCounter,
}

impl RCSTWSweepNurseryBlocks {
    pub fn new(blocks: Vec<Block>) -> Self {
        Self {
            blocks,
            _counter: LazySweepingJobsCounter::new_decs(),
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCSTWSweepNurseryBlocks {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let space = &mmtk.plan.downcast_ref::<LXR<VM>>().unwrap().immix_space;
        for block in &self.blocks {
            block.rc_sweep_nursery(space, false);
        }
    }
}
