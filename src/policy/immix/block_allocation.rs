use atomic::Ordering;
use crate::{policy::space::Space, util::{VMThread, metadata::side_metadata}, vm::VMBinding};
use super::{ImmixSpace, block::Block, chunk::ChunkState, rc::RC_TABLE};

pub struct BlockAllocation<VM: VMBinding> {
    space: Option<&'static ImmixSpace<VM>>,
    buffer: Vec<Block>,
}

impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: None,
            buffer: vec![],
        }
    }

    pub fn init(&mut self, space: &'static ImmixSpace<VM>) {
        self.space = Some(space);
    }

    const fn space(&self) -> &'static ImmixSpace<VM> {
        self.space.unwrap()
    }

    /// Allocate a clean block.
    #[inline(always)]
    pub fn get_clean_block(&self, tls: VMThread, copy: bool) -> Option<Block> {
        let block_address = self.space().acquire(tls, Block::PAGES);
        if block_address.is_zero() {
            return None;
        }
        if self.space().in_defrag() {
            self.space().defrag.notify_new_clean_block(copy);
        }
        let block = Block::from(block_address);
        if crate::plan::immix::REF_COUNT && !crate::plan::barriers::BARRIER_MEASUREMENT {
            side_metadata::bzero_metadata(&RC_TABLE, block.start(), Block::BYTES);
        }
        if crate::plan::immix::CONCURRENT_MARKING && !super::BLOCK_ONLY {
            let current_state = self.space().line_mark_state.load(Ordering::Acquire);
            for line in block.lines() {
                line.mark(current_state);
            }
        }
        block.init(copy);
        if self.space().common().needs_log_bit {
            block.clear_log_table::<VM>();
        }
        self.space().chunk_map.set(block.chunk(), ChunkState::Allocated);
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
                block.init(copy);
                return Some(block);
            } else {
                return None;
            }
        }
    }
}