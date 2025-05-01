use super::{block::Block, ImmixSpace};

use crate::vm::*;
use std::cell::UnsafeCell;

pub struct BlockAllocation<VM: VMBinding> {
    space: UnsafeCell<*const ImmixSpace<VM>>,
}

impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: UnsafeCell::new(std::ptr::null()),
        }
    }

    fn space(&self) -> &'static ImmixSpace<VM> {
        unsafe { &**self.space.get() }
    }

    pub fn init(&self, space: &ImmixSpace<VM>) {
        unsafe { *self.space.get() = space as *const ImmixSpace<VM> }
    }

    pub(super) fn initialize_new_clean_block(&self, block: Block, copy: bool) {
        if self.space().in_defrag() {
            self.space().defrag.notify_new_clean_block(copy);
        }
        block.init(copy, false);
    }
}
