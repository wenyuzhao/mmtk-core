use super::{
    block::{Block, BlockState},
    chunk::Chunk,
    cset::CollectionSet,
    region::{Region, RegionState},
    ImmixSpace,
};
use crate::{
    plan::immix::{Immix, Pause},
    scheduler::{GCWork, GCWorker, WorkBucketStage},
    vm::VMBinding,
    MMTK,
};
use atomic::Ordering;
use downcast_rs::Downcast;
use std::{ops::ControlFlow, sync::atomic::AtomicUsize};

pub fn create_defrag_policy<VM: VMBinding>() -> Box<dyn DefragPolicy<VM>> {
    if crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG.is_some() {
        box SimpleIntrementalDefragPolicy
    } else if crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG2.is_some() {
        box SimpleIntrementalDefragPolicy2
    } else {
        box DefaultDefragPolicy
    }
}

pub trait DefragPolicy<VM: VMBinding>: Downcast {
    fn should_stop(&self, cset: &CollectionSet) -> bool;
    fn select(&self, mmtk: &'static MMTK<VM>);
}

impl_downcast!(DefragPolicy<VM> where VM: VMBinding);

struct SimpleIntrementalDefragPolicy;

impl<VM: VMBinding> DefragPolicy<VM> for SimpleIntrementalDefragPolicy {
    fn select(&self, mmtk: &'static MMTK<VM>) {
        let immix_space = &mmtk.plan.downcast_ref::<Immix<VM>>().unwrap().immix_space;
        let n = crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG.unwrap()
            * *crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG_MULTIPLIER;
        let mut regions = vec![];
        immix_space.walk_regions_in_address_order(|region| {
            region.set_defrag_source();
            regions.push(region);
            for block in region.committed_mature_blocks() {
                block.set_as_defrag_source(true);
            }
            if regions.len() >= n {
                ControlFlow::BREAK
            } else {
                ControlFlow::CONTINUE
            }
        });
        immix_space.collection_set.set_reigons(regions);
    }
    fn should_stop(&self, cset: &CollectionSet) -> bool {
        cset.retired_regions.len() >= *crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG_MULTIPLIER
    }
}

struct SimpleIntrementalDefragPolicy2;

impl<VM: VMBinding> DefragPolicy<VM> for SimpleIntrementalDefragPolicy2 {
    fn select(&self, mmtk: &'static MMTK<VM>) {
        let immix_space = &mmtk.plan.downcast_ref::<Immix<VM>>().unwrap().immix_space;
        let n = crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG.unwrap()
            * *crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG_MULTIPLIER
            * Region::BLOCKS;
        let mut regions = vec![];
        let mut blocks = 0usize;
        let threshold = crate::args::SIMPLE_INCREMENTAL_DEFRAG2_THRESHOLD.unwrap();
        immix_space.walk_regions_in_address_order(|region| {
            region.set_defrag_source();
            regions.push(region);
            for block in region.committed_mature_blocks() {
                if block.live_bytes() * 100 <= threshold * Block::BYTES {
                    block.set_as_defrag_source(true);
                    blocks += 1;
                }
            }
            if regions.len() >= n {
                ControlFlow::BREAK
            } else {
                ControlFlow::CONTINUE
            }
        });
        immix_space.collection_set.set_reigons(regions);
    }
    fn should_stop(&self, cset: &CollectionSet) -> bool {
        cset.retired_regions.len() >= *crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG_MULTIPLIER
    }
}

struct DefaultDefragPolicy;

impl DefaultDefragPolicy {
    pub fn select_mature_evacuation_candidates<VM: VMBinding>(
        &self,
        _pause: Pause,
        _total_pages: usize,
        space: &ImmixSpace<VM>,
    ) {
        let collection_set = &space.collection_set;
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        // Select mature defrag blocks
        let defrag_bytes = space.defrag_headroom_pages() << 12;
        let mut regions = Vec::with_capacity(
            collection_set
                .fragmented_regions_size
                .load(Ordering::SeqCst),
        );
        while let Some(mut x) = collection_set.fragmented_regions.pop() {
            regions.append(&mut x);
        }
        let mut live_bytes = 0usize;
        // let mut num_regions = 0usize;
        regions.sort_by_key(|x| x.1);
        let mut cset = vec![];
        while let Some((region, dead_bytes)) = regions.pop() {
            live_bytes += (Region::BYTES - dead_bytes) * 30 / 100;
            // num_regions += 1;
            region.set_defrag_source();
            for block in region.committed_blocks() {
                if block.get_state() != BlockState::Nursery {
                    // println!(" ... defrag {:?} {:?}", block, block.get_state());
                    block.set_as_defrag_source(true)
                }
            }
            cset.push(region);
            if crate::args::LOG_PER_GC_STATE {
                println!(
                    " - Defrag {:?} live_bytes={:?} {:?}",
                    region,
                    Region::BYTES - dead_bytes,
                    region.get_state()
                );
            }
            if live_bytes >= defrag_bytes {
                break;
            }
        }
        collection_set.set_reigons(cset.clone());
    }
}

impl<VM: VMBinding> DefragPolicy<VM> for DefaultDefragPolicy {
    fn select(&self, mmtk: &'static MMTK<VM>) {
        let immix_space = &mmtk.plan.downcast_ref::<Immix<VM>>().unwrap().immix_space;
        let tasks = immix_space
            .chunk_map
            .generate_tasks(|chunk| box SelectDefragRegionsInChunk { chunk });
        immix_space
            .collection_set
            .fragmented_regions_size
            .store(0, Ordering::SeqCst);
        SELECT_DEFRAG_BLOCK_JOB_COUNTER.store(tasks.len(), Ordering::SeqCst);
        immix_space.scheduler().work_buckets[WorkBucketStage::RCCollectionSetSelection]
            .bulk_add(tasks);
    }
    fn should_stop(&self, cset: &CollectionSet) -> bool {
        if !*crate::args::LXR_INCREMENTAL_DEFRAG {
            return false;
        }
        cset.retired_regions.len() >= 8
    }
}

static SELECT_DEFRAG_BLOCK_JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct SelectDefragRegionsInChunk {
    chunk: Chunk,
}

impl<VM: VMBinding> GCWork<VM> for SelectDefragRegionsInChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut regions = vec![];
        // Iterate over all blocks in this chunk
        for region in self.chunk.regions() {
            region.set_state(RegionState::Allocated);
            let mut dead_bytes = 0usize;
            let mut has_live_mature_blocks = false;
            for block in region.blocks() {
                let state = block.get_state();
                if state == BlockState::Unallocated || state == BlockState::Nursery {
                    dead_bytes += Block::BYTES;
                } else {
                    has_live_mature_blocks = true;
                    assert!(!block.is_defrag_source(), "{:?} is defrag source", block);
                    dead_bytes += Block::BYTES - block.live_bytes()
                }
            }
            if has_live_mature_blocks {
                if crate::args::LOG_PER_GC_STATE {
                    // println!(" - candidate {:?} dead_bytes={:?}", region, dead_bytes);
                }
                regions.push((region, dead_bytes));
            }
        }
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        immix
            .immix_space
            .collection_set
            .fragmented_regions_size
            .fetch_add(regions.len(), Ordering::SeqCst);
        immix
            .immix_space
            .collection_set
            .fragmented_regions
            .push(regions);
        if SELECT_DEFRAG_BLOCK_JOB_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
            immix
                .immix_space
                .defrag_policy
                .downcast_ref::<DefaultDefragPolicy>()
                .unwrap()
                .select_mature_evacuation_candidates(
                    immix.current_pause().unwrap(),
                    mmtk.plan.get_total_pages(),
                    &immix.immix_space,
                )
        }
    }
}
