use super::{
    block::Block,
    chunk::Chunk,
    cset::CollectionSet,
    region::{Region, RegionState},
};
use crate::{
    plan::immix::{Immix, Pause},
    scheduler::{GCWork, GCWorker, WorkBucketStage},
    vm::VMBinding,
    Plan, MMTK,
};
use atomic::Ordering;
use crossbeam_queue::SegQueue;
use downcast_rs::Downcast;
use std::{ops::ControlFlow, sync::atomic::AtomicUsize};

pub fn create_defrag_policy<VM: VMBinding>() -> Box<dyn DefragPolicy<VM>> {
    if crate::args::LXR_NO_DEFRAG.is_some() {
        println!("NoDefragPolicy");
        box NoDefragPolicy
    } else if crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG.is_some() {
        println!("SimpleIntrementalDefragPolicy");
        box SimpleIntrementalDefragPolicy
    } else if crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG2.is_some() {
        println!("SimpleIntrementalDefragPolicy2");
        box SimpleIntrementalDefragPolicy2::default()
    } else {
        println!("DefaultDefragPolicy");
        box DefaultDefragPolicy::default()
    }
}

pub trait DefragPolicy<VM: VMBinding>: Downcast {
    fn select(&self, mmtk: &'static MMTK<VM>);
    fn should_stop(&self, cset: &CollectionSet) -> bool;
    fn notify_defrag_start(&self, _region: Region) {}
    fn notify_defrag_end(&self, _region: Region) {}
    fn notify_evacuation_stop(&self) {}
}

impl_downcast!(DefragPolicy<VM> where VM: VMBinding);

struct NoDefragPolicy;

impl<VM: VMBinding> DefragPolicy<VM> for NoDefragPolicy {
    fn select(&self, mmtk: &'static MMTK<VM>) {
        let immix_space = &mmtk.plan.downcast_ref::<Immix<VM>>().unwrap().immix_space;
        immix_space.collection_set.set_reigons(vec![]);
    }
    fn should_stop(&self, _cset: &CollectionSet) -> bool {
        unreachable!()
    }
}

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

#[derive(Default)]
struct SimpleIntrementalDefragPolicy2 {
    processed_blocks: AtomicUsize,
    per_pause_budget: AtomicUsize,
}

impl<VM: VMBinding> DefragPolicy<VM> for SimpleIntrementalDefragPolicy2 {
    fn select(&self, mmtk: &'static MMTK<VM>) {
        let immix_space = &mmtk.plan.downcast_ref::<Immix<VM>>().unwrap().immix_space;
        let regions = crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG2.unwrap()
            * *crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG_MULTIPLIER;
        let n = regions * Region::BLOCKS;
        let mut regions = vec![];
        let mut blocks = 0usize;
        let threshold = crate::args::SIMPLE_INCREMENTAL_DEFRAG2_THRESHOLD.unwrap();
        immix_space.walk_regions_in_address_order(|region| {
            region.set_defrag_source();
            regions.push(region);
            let mut c = 0usize;
            for block in region.committed_mature_blocks() {
                if block.live_bytes() * 100 <= threshold * Block::BYTES {
                    block.set_as_defrag_source(true);
                    c += 1;
                }
            }
            blocks += c;
            if crate::args::LOG_PER_GC_STATE {
                println!(
                    " - Defrag {:?} defrag {} (total {} / {}) blocks",
                    region, c, blocks, n
                );
            }
            if blocks >= n {
                ControlFlow::BREAK
            } else {
                ControlFlow::CONTINUE
            }
        });
        self.per_pause_budget.store(
            Region::BLOCKS * *crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG_MULTIPLIER,
            Ordering::SeqCst,
        );
        immix_space.collection_set.set_reigons(regions);
    }
    fn should_stop(&self, _cset: &CollectionSet) -> bool {
        self.processed_blocks.load(Ordering::Relaxed)
            >= self.per_pause_budget.load(Ordering::Relaxed)
    }
    fn notify_defrag_end(&self, region: Region) {
        let mut blocks = 0usize;
        for block in region.committed_blocks() {
            if block.is_defrag_source() {
                blocks += 1;
            }
        }
        self.processed_blocks.fetch_add(blocks, Ordering::Relaxed);
    }
    fn notify_evacuation_stop(&self) {
        self.processed_blocks.store(0, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct DefaultDefragPolicy {
    fragmented_regions: SegQueue<Vec<(Region, usize)>>,
    fragmented_regions_size: AtomicUsize,
}

impl DefaultDefragPolicy {
    pub fn select_mature_evacuation_candidates<VM: VMBinding>(
        &self,
        _pause: Pause,
        _total_pages: usize,
        immix: &Immix<VM>,
    ) {
        let collection_set = &immix.immix_space.collection_set;
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        let defrag_bytes = (immix.get_pages_avail() + immix.get_collection_reserve()) << 12;
        // Sort regions by scores
        let mut regions = Vec::with_capacity(self.fragmented_regions_size.load(Ordering::SeqCst));
        while let Some(mut x) = self.fragmented_regions.pop() {
            regions.append(&mut x);
        }
        let mut live_bytes = 0usize;
        regions.sort_by_key(|x| x.1);
        // Select regions
        let mut cset = vec![];
        while let Some((region, dead_bytes)) = regions.pop() {
            region.set_defrag_source();
            for block in region.committed_mature_blocks() {
                if block.live_bytes() * 100 <= 20 * Block::BYTES {
                    block.set_as_defrag_source(true);
                    live_bytes += block.live_bytes() * 30 / 100;
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
        self.fragmented_regions_size.store(0, Ordering::SeqCst);
        SELECT_DEFRAG_BLOCK_JOB_COUNTER.store(tasks.len(), Ordering::SeqCst);
        immix_space.scheduler().work_buckets[WorkBucketStage::RCCollectionSetSelection]
            .bulk_add(tasks);
    }
    fn should_stop(&self, cset: &CollectionSet) -> bool {
        if !*crate::args::LXR_INCREMENTAL_DEFRAG {
            return false;
        }
        if *crate::args::OPPORTUNISTIC_EVAC {
            let pause_time = crate::GC_START_TIME
                .load(Ordering::SeqCst)
                .elapsed()
                .unwrap()
                .as_millis();
            pause_time as usize >= *crate::args::OPPORTUNISTIC_EVAC_THRESHOLD
        } else {
            cset.retired_regions.len() >= 8
        }
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
            let mut live_blocks = 0usize;
            let mut defraggable_bytes = 0usize;
            for block in region.committed_mature_blocks() {
                live_blocks += 1;
                if block.live_bytes() * 100 <= 20 * Block::BYTES {
                    defraggable_bytes += 1;
                }
            }
            if live_blocks > 0 {
                if crate::args::LOG_PER_GC_STATE {
                    println!(" - candidate {:?} score={:?}", region, defraggable_bytes);
                }
                regions.push((region, defraggable_bytes));
            }
        }
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        let policy = immix
            .immix_space
            .defrag_policy
            .downcast_ref::<DefaultDefragPolicy>()
            .unwrap();
        policy
            .fragmented_regions_size
            .fetch_add(regions.len(), Ordering::SeqCst);
        policy.fragmented_regions.push(regions);
        if SELECT_DEFRAG_BLOCK_JOB_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
            policy.select_mature_evacuation_candidates(
                immix.current_pause().unwrap(),
                mmtk.plan.get_total_pages(),
                &immix,
            )
        }
    }
}
