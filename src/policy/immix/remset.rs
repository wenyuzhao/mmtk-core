use crossbeam_queue::SegQueue;

use crate::util::{Address, ObjectReference};

use super::block::Block;

const LOG_BLOCKS_IN_REMSET: usize = 6;
const BLOCKS_IN_REMSET: usize = 1 << LOG_BLOCKS_IN_REMSET;
const LOG_BYETS_IN_REMSET: usize = LOG_BLOCKS_IN_REMSET + Block::LOG_BYTES;

#[derive(Clone)]
pub struct RemSet {
    buffer: Vec<ObjectReference>,
}

impl RemSet {
    pub const CAPACITY: usize = 4096;

    #[inline(always)]
    pub fn new() -> Self {
        Self { buffer: vec![] }
    }

    #[inline(always)]
    pub fn push(&mut self, o: ObjectReference) {
        self.buffer.push(o);
        if self.buffer.len() >= Self::CAPACITY {
            self.flush();
        }
    }

    #[inline(always)]
    pub fn test_and_add(&mut self, e: Address, o: ObjectReference) {
        if (e.as_usize() ^ o.to_address().as_usize()) >> LOG_BYETS_IN_REMSET == 0 {
            return;
        }
        if !Block::from(e).is_defrag_source() && Block::from(o.to_address()).is_defrag_source() {
            self.push(o);
        }
    }

    #[inline(always)]
    pub fn flush(&mut self) {
        if self.buffer.len() > 0 {
            let mut rs = Self::new();
            std::mem::swap(self, &mut rs);
            REMSETS.push(rs);
        }
    }
}

pub static REMSETS: SegQueue<RemSet> = SegQueue::new();
