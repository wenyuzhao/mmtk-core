use super::block::BlockState;
use super::{block::Block, ImmixSpace};
use crate::plan::immix::Pause;
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::{plan::lxr::LXR, policy::space::Space, scheduler::GCWorkScheduler, vm::*};
use atomic::{Atomic, Ordering};
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;

pub(super) struct BlockCache {
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
    pub(super) reused_blocks: BlockCache,
    pub(crate) lxr: Option<&'static LXR<VM>>,
    num_nursery_blocks: AtomicUsize,
}

impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: UnsafeCell::new(std::ptr::null()),
            reused_blocks: BlockCache::new(),
            lxr: None,
            num_nursery_blocks: AtomicUsize::new(0),
        }
    }

    fn space(&self) -> &'static ImmixSpace<VM> {
        unsafe { &**self.space.get() }
    }

    pub fn clean_nursery_blocks(&self) -> usize {
        self.num_nursery_blocks.load(Ordering::Relaxed)
    }

    pub fn clean_nursery_mb(&self) -> usize {
        self.clean_nursery_blocks() << Block::LOG_BYTES >> 20
    }

    pub fn total_young_allocation_in_bytes(&self) -> usize {
        (self.clean_nursery_blocks() << Block::LOG_BYTES)
            + (self.space().get_mutator_recycled_lines_in_pages() << LOG_BYTES_IN_PAGE)
    }

    pub fn init(&self, space: &ImmixSpace<VM>) {
        unsafe { *self.space.get() = space as *const ImmixSpace<VM> }
    }

    pub fn reset_block_mark_for_mutator_reused_blocks(&self, pause: Pause) {
        if cfg!(feature = "block_state_reset_bug") {
            if pause == Pause::RefCount || pause == Pause::InitialMark {
                return;
            }
        }
        // SATB sweep has problem scanning mutator recycled blocks.
        // Remaing the block state as "reusing" and reset them here.
        const BLOCK_STATE: BlockState = if cfg!(feature = "block_state_reset_bug") {
            BlockState::Unmarked
        } else {
            BlockState::Marked
        };
        self.reused_blocks.visit_slice(|blocks| {
            for b in blocks {
                let b = b.load(Ordering::Relaxed);
                b.set_state(BLOCK_STATE);
            }
        });
    }

    pub fn sweep_mutator_reused_blocks(&self, _scheduler: &GCWorkScheduler<VM>, pause: Pause) {
        if pause == Pause::Full || pause == Pause::FinalMark {
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
                unreachable!();
            }
        });
        self.reused_blocks.reset();
    }

    /// Reset allocated_block_buffer and free nursery blocks.
    #[cfg(feature = "ix_no_sweeping")]
    pub fn sweep_nursery_blocks(&self, _scheduler: &GCWorkScheduler<VM>, _pause: Pause) {
        let num_blocks = self.clean_nursery_blocks();
        self.space().pr.bulk_release_blocks(num_blocks);
        self.space().pr.reset();
        self.num_nursery_blocks.store(0, Ordering::SeqCst);
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
            if !copy {
                self.num_nursery_blocks.fetch_add(1, Ordering::Relaxed);
                if cfg!(feature = "ix_no_sweeping") {
                    block.clear_field_unlog_table::<VM>();
                }
            }
        }
        // println!("Alloc {:?} {}", block, copy);
        block.init(copy, false, self.space());
        if self.space().common().zeroed && !copy && cfg!(feature = "force_zeroing") {
            crate::util::memory::zero_w(block.start(), Block::BYTES);
        }
    }
}
