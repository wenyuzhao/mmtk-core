use crate::plan::concurrent::global::ConcurrentPlan;
use crate::plan::concurrent::Pause;
use crate::policy::immix::block::Block;
use crate::policy::immix::{ImmixHooks, ImmixSpace};
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::{plan::lxr::LXR, vm::*};
use atomic::Ordering;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;

pub struct BlockAllocation<VM: VMBinding> {
    space: UnsafeCell<*const ImmixSpace<VM>>,
    lxr: UnsafeCell<*const LXR<VM>>,
    num_nursery_blocks: AtomicUsize,
}

unsafe impl<VM: VMBinding> Sync for BlockAllocation<VM> {}
unsafe impl<VM: VMBinding> Send for BlockAllocation<VM> {}

impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: UnsafeCell::new(std::ptr::null()),
            lxr: UnsafeCell::new(std::ptr::null()),
            num_nursery_blocks: AtomicUsize::new(0),
        }
    }

    pub fn init(&self, space: &ImmixSpace<VM>, lxr: &'static LXR<VM>) {
        unsafe {
            *self.space.get() = space as *const ImmixSpace<VM>;
            *self.lxr.get() = lxr as *const LXR<VM>;
        }
    }

    fn space(&self) -> &'static ImmixSpace<VM> {
        unsafe { &**self.space.get() }
    }

    fn lxr(&self) -> &'static LXR<VM> {
        unsafe { &**self.lxr.get() }
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

    /// Reset allocated_block_buffer and free nursery blocks.
    fn sweep_nursery_blocks(&self) {
        let ix_space = &self.lxr().immix_space;
        let in_place_promoted_nursery_blocks = ix_space
            .in_place_promoted_nursery_blocks
            .load(Ordering::Relaxed);
        let num_blocks = self.clean_nursery_blocks();
        self.space()
            .pr
            .bulk_release_blocks(num_blocks - in_place_promoted_nursery_blocks);
        self.space().pr.reset();
        self.num_nursery_blocks.store(0, Ordering::SeqCst);
        ix_space
            .in_place_promoted_nursery_blocks
            .store(0, Ordering::SeqCst);
    }
}

impl<VM: VMBinding> ImmixHooks<VM> for BlockAllocation<VM> {
    fn on_clean_block_acquired(&self, block: Block, copy: bool) {
        // Initialize unlog table
        if copy {
            block.initialize_field_unlog_table_as_unlogged::<VM>();
        }
        // Initialize mark table
        if self.cm_in_progress_or_final_mark() {
            block.initialize_mark_table_as_marked::<VM>();
        } else {
            // TODO: Performance? Is this necessary?
            block.clear_mark_table::<VM>();
        }
        if !copy {
            self.num_nursery_blocks.fetch_add(1, Ordering::Relaxed);
            block.clear_field_unlog_table::<VM>();
        }
    }

    fn cm_in_progress_or_final_mark(&self) -> bool {
        let lxr = self.lxr();
        lxr.concurrent_work_in_progress() || lxr.current_pause() == Some(Pause::FinalMark)
    }

    fn sweep_nursery_blocks(&self) {
        self.sweep_nursery_blocks();
    }
}
