use super::{block::Block, ImmixSpace};

use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::{scheduler::GCWorkScheduler, vm::*};
use atomic::Ordering;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;

pub struct BlockAllocation<VM: VMBinding> {
    space: UnsafeCell<*const ImmixSpace<VM>>,
    num_nursery_blocks: AtomicUsize,
    pub(crate) in_place_promoted_nursery_blocks: AtomicUsize,
}

impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: UnsafeCell::new(std::ptr::null()),
            num_nursery_blocks: AtomicUsize::new(0),
            in_place_promoted_nursery_blocks: Default::default(),
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

    pub fn init(&self, space: &ImmixSpace<VM>) {
        unsafe { *self.space.get() = space as *const ImmixSpace<VM> }
    }

    /// Reset allocated_block_buffer and free nursery blocks.
    pub fn sweep_nursery_blocks(&self, _scheduler: &GCWorkScheduler<VM>) {
        let in_place_promoted_nursery_blocks = self
            .in_place_promoted_nursery_blocks
            .load(Ordering::Relaxed);
        let num_blocks = self.clean_nursery_blocks();
        self.space()
            .pr
            .bulk_release_blocks(num_blocks - in_place_promoted_nursery_blocks);
        self.space().pr.reset();
        self.num_nursery_blocks.store(0, Ordering::SeqCst);
        self.in_place_promoted_nursery_blocks
            .store(0, Ordering::SeqCst);
    }

    /// Notify a GC pahse has started
    pub fn notify_mutator_phase_end(&self) {}

    pub(super) fn initialize_new_clean_block(&self, block: Block, copy: bool, cm_enabled: bool) {
        if self.space().in_defrag() {
            self.space().defrag.notify_new_clean_block(copy);
        }
        // println!("Alloc {:?} {}", block, copy);
        block.init(copy, false, self.space());
    }
}
