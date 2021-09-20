use super::{block::Block, chunk::ChunkState, rc::RC_TABLE, ImmixSpace};
use crate::{
    policy::space::Space,
    util::{metadata::side_metadata, VMMutatorThread, VMThread},
    vm::*,
};
use atomic::Ordering;
use spin::Mutex;
use std::sync::atomic::AtomicUsize;

pub struct BlockAllocation<VM: VMBinding> {
    space: Option<&'static ImmixSpace<VM>>,
    clean_block_cursor: AtomicUsize,
    clean_block_buffer: Vec<Block>,
    clean_block_buffer_refill_lock: Mutex<()>,
}

impl<VM: VMBinding> BlockAllocation<VM> {
    const REFILL_INCREMENT: usize = 32;

    pub fn new() -> Self {
        Self {
            space: None,
            clean_block_cursor: AtomicUsize::new(0),
            clean_block_buffer: vec![],
            clean_block_buffer_refill_lock: Mutex::new(()),
        }
    }

    pub fn init(&mut self, space: &'static ImmixSpace<VM>) {
        self.space = Some(space);
    }

    pub fn reset(&mut self) {
        self.clean_block_buffer.clear();
        self.clean_block_cursor.store(0, Ordering::SeqCst);
    }

    const fn space(&self) -> &'static ImmixSpace<VM> {
        self.space.unwrap()
    }

    #[inline(always)]
    fn initialize_new_clean_block(&self, block: Block, copy: bool) {
        if self.space().in_defrag() {
            self.space().defrag.notify_new_clean_block(copy);
        }
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
        self.space()
            .chunk_map
            .set(block.chunk(), ChunkState::Allocated);
    }

    #[inline(always)]
    fn alloc_clean_block_fast(&self) -> Option<Block> {
        let i = self
            .clean_block_cursor
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |i| {
                if i >= self.clean_block_buffer.len() {
                    None
                } else {
                    Some(i + 1)
                }
            })
            .ok()?;
        Some(self.clean_block_buffer[i])
    }

    #[cold]
    fn alloc_clean_block_slow(&mut self, tls: VMThread) -> Option<Block> {
        let _guard = self.clean_block_buffer_refill_lock.lock();
        // Retry allocation
        if let Some(block) = self.alloc_clean_block_fast() {
            return Some(block);
        }
        // Fill buffer with N blocks
        for _ in 0..Self::REFILL_INCREMENT {
            let a = self.space().acquire_uninterruptable(tls, Block::PAGES)?;
            self.clean_block_buffer.push(Block::from(a));
        }
        let r = self.space().acquire_uninterruptable(tls, Block::PAGES)?;
        Some(Block::from(r))
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
    #[inline(always)]
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
                block.init(copy);
                return Some(block);
            } else {
                return None;
            }
        }
    }
}
