use std::sync::{atomic::AtomicUsize, Mutex};

use crate::{
    scheduler::{GCWork, GCWorker},
    vm::VMBinding,
    MMTK,
};

use super::{chunk::Chunk, region::Region, ImmixSpace};

#[derive(Debug, Default)]
pub struct PerRegionRemSet {}

pub struct CollectionSet {
    regions: Mutex<Vec<Region>>,
}

impl CollectionSet {
    pub fn move_to_next_region(&self) {
        if let Some(region) = self.regions.lock().unwrap().pop() {
            debug_assert!(region.is_defrag_source());
            region.set_active();
        }
    }

    fn schedule_defrag_selection_packets<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        unimplemented!();
        // let tasks = space
        //     .chunk_map
        //     .generate_tasks(|chunk| box SelectDefragBlocksInChunk {
        //         chunk,
        //         defrag_threshold: 1,
        //     });
        // self.fragmented_blocks_size.store(0, Ordering::SeqCst);
        // SELECT_DEFRAG_BLOCK_JOB_COUNTER.store(tasks.len(), Ordering::SeqCst);
        // self.scheduler().work_buckets[WorkBucketStage::FinishConcurrentWork].bulk_add(tasks);
    }

    // pub fn select(&self) {
    //     debug_assert!(self.regions.lock().unwrap().is_empty());
    // }
}

static SELECT_DEFRAG_BLOCK_JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct SelectDefragRegionsInChunk {
    chunk: Chunk,
    defrag_threshold: usize,
}

impl<VM: VMBinding> GCWork<VM> for SelectDefragRegionsInChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        unimplemented!();
        // let mut blocks = vec![];
        // // Iterate over all blocks in this chunk
        // for block in self.chunk.committed_blocks() {
        //     let state = block.get_state();
        //     // Skip unallocated blocks.
        //     if state == BlockState::Unallocated
        //         || state == BlockState::Nursery
        //         || block.is_defrag_source()
        //     {
        //         continue;
        //     }
        //     let score = if crate::args::HOLE_COUNTING {
        //         unreachable!();
        //         // match state {
        //         //     BlockState::Reusable { unavailable_lines } => unavailable_lines as _,
        //         //     _ => block.calc_holes(),
        //         // }
        //     } else {
        //         // block.dead_bytes()
        //         // block.calc_dead_bytes::<VM>()
        //         block.calc_dead_lines() << Line::LOG_BYTES
        //     };
        //     if score >= self.defrag_threshold {
        //         blocks.push((block, score));
        //     }
        // }
        // let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        // immix
        //     .immix_space
        //     .fragmented_blocks_size
        //     .fetch_add(blocks.len(), Ordering::SeqCst);
        // immix.immix_space.fragmented_blocks.push(blocks);
        // if SELECT_DEFRAG_BLOCK_JOB_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
        //     immix.immix_space.select_mature_evacuation_candidates(
        //         immix.current_pause().unwrap(),
        //         mmtk.plan.get_total_pages(),
        //     )
        // }
    }
}
