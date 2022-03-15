use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Mutex,
};

use atomic::Ordering;
use crossbeam_queue::SegQueue;

use crate::{
    plan::immix::{Immix, Pause},
    scheduler::{GCWork, GCWorker, WorkBucketStage},
    util::Address,
    vm::VMBinding,
    MMTK,
};

use super::{
    block::{Block, BlockState},
    chunk::Chunk,
    line::Line,
    region::Region,
    ImmixSpace,
};

#[derive(Debug, Default)]
pub struct PerRegionRemSet {
    pub gc_buffers: Vec<Vec<Address>>,
    pub mu_buffers: SegQueue<Vec<Address>>,
}

impl PerRegionRemSet {
    pub fn new(gc_threads: usize) -> Self {
        let mut rs = Self::default();
        rs.gc_buffers.resize_with(gc_threads, Default::default);
        rs
    }

    #[inline]
    pub fn add(&mut self, e: Address) {
        let id = crate::gc_worker_id().unwrap();
        self.gc_buffers[id].push(e);
    }

    #[inline]
    pub fn mu_flush(&self, buf: Vec<Address>) {
        self.mu_buffers.push(buf);
    }
}

#[derive(Debug, Default)]
pub struct CollectionSet {
    regions: Mutex<Vec<Region>>,
    pub in_defrag: AtomicBool,
}

impl CollectionSet {
    pub fn enable_defrag(&self) {
        debug_assert!(!self.in_defrag.load(Ordering::SeqCst));
        self.in_defrag.store(true, Ordering::SeqCst);
    }

    pub fn move_to_next_region(&self) {
        if !self.in_defrag.load(Ordering::SeqCst) {
            return;
        }
        if let Some(region) = self.regions.lock().unwrap().pop() {
            debug_assert!(region.is_defrag_source());
            region.set_active();
        } else {
            self.in_defrag.store(false, Ordering::SeqCst);
        }
    }

    pub fn schedule_defrag_selection_packets<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        let tasks = space
            .chunk_map
            .generate_tasks(|chunk| box SelectDefragRegionsInChunk { chunk });
        space.fragmented_regions_size.store(0, Ordering::SeqCst);
        SELECT_DEFRAG_BLOCK_JOB_COUNTER.store(tasks.len(), Ordering::SeqCst);
        space.scheduler().work_buckets[WorkBucketStage::FinishConcurrentWork].bulk_add(tasks);
    }

    pub fn select_mature_evacuation_candidates<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        // let me = unsafe { &mut *(self as *const Self as *mut Self) };
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        // Select mature defrag blocks
        // let total_bytes = total_pages << 12;
        let defrag_bytes = space.defrag_headroom_pages() << 12;
        // let defrag_blocks = defrag_bytes >> Block::LOG_BYTES;
        let mut regions = Vec::with_capacity(space.fragmented_regions_size.load(Ordering::SeqCst));
        while let Some(mut x) = space.fragmented_regions.pop() {
            regions.append(&mut x);
        }
        let mut live_bytes = 0usize;
        let mut num_regions = 0usize;
        regions.sort_by_key(|x| x.1);
        let mut cset_regions = self.regions.lock().unwrap();
        while let Some((region, dead_bytes)) = regions.pop() {
            // if region.is_defrag_source()
            //     || region.get_state() == BlockState::Unallocated
            //     || region.get_state() == BlockState::Nursery
            // {
            //     // println!(" - skip defrag {:?} {:?}", block, block.get_state());
            //     continue;
            // }

            region.set_defrag_source();
            region.init_remset(space.scheduler().num_workers());
            cset_regions.push(region);

            // if !block.attempt_to_set_as_defrag_source() {
            //     continue;
            // }
            // println!(
            //     " - defrag {:?} {:?} {}",
            //     block,
            //     block.get_state(),
            //     block.dead_bytes()
            // );
            // me.defrag_blocks.push(block);
            live_bytes += (Region::BYTES - dead_bytes) * 30 / 100;
            num_regions += 1;
            if crate::args::COUNT_BYTES_FOR_MATURE_EVAC {
                if live_bytes >= defrag_bytes {
                    break;
                }
            } else {
                unreachable!();
            }
        }
        if crate::args::LOG_PER_GC_STATE {
            println!(
                " - Defrag {} mature bytes ({} blocks)",
                live_bytes, num_regions
            );
        }
        // self.num_defrag_blocks.store(num_blocks, Ordering::SeqCst);
    }

    // pub fn select(&self) {
    //     debug_assert!(self.regions.lock().unwrap().is_empty());
    // }
}

static SELECT_DEFRAG_BLOCK_JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct SelectDefragRegionsInChunk {
    chunk: Chunk,
}

impl<VM: VMBinding> GCWork<VM> for SelectDefragRegionsInChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut regions = vec![];
        // Iterate over all regions in this chunk
        for region in self.chunk.regions() {
            debug_assert!(!region.is_defrag_source());
            let mut live_blocks = 0usize;
            let mut live_bytes = 0usize;
            for block in region.committed_blocks() {
                let state = block.get_state();
                if state == BlockState::Unallocated || state == BlockState::Nursery {
                    continue;
                }
                let dead_bytes = block.calc_dead_lines() << Line::LOG_BYTES;
                live_bytes += Block::BYTES - dead_bytes;
                live_blocks += 1;
            }
            if live_blocks != 0 && live_bytes <= (Region::BYTES >> 1) {
                regions.push((region, Region::BYTES - live_bytes));
            }
        }
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        immix
            .immix_space
            .fragmented_regions_size
            .fetch_add(regions.len(), Ordering::SeqCst);
        immix.immix_space.fragmented_regions.push(regions);
        if SELECT_DEFRAG_BLOCK_JOB_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
            immix.immix_space.select_mature_evacuation_candidates(
                immix.current_pause().unwrap(),
                mmtk.plan.get_total_pages(),
            )
        }
    }
}
