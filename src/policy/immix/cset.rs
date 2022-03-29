use std::{
    intrinsics::unlikely,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Mutex,
    },
};

use atomic::{Atomic, Ordering};
use crossbeam_queue::{ArrayQueue, SegQueue};

use super::{
    block::{Block, BlockState},
    chunk::Chunk,
    line::Line,
    region::{Region, RegionState},
    ImmixSpace,
};
use crate::{
    plan::immix::{Immix, ImmixCopyContext, Pause},
    policy::{largeobjectspace::LargeObjectSpace, space::Space},
    scheduler::{gc_work::EvacuateMatureObjects, GCWork, GCWorker, WorkBucketStage},
    util::{cm::LXRMatureEvacRoots, Address, ObjectReference},
    vm::VMBinding,
    MMTK,
};
use crate::{util::metadata::side_metadata, vm::ObjectModel};

static RECORD: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
pub struct PerRegionRemSet {
    pub gc_buffers: Vec<Atomic<*mut Vec<Address>>>,
    pub size: AtomicUsize,
}

impl PerRegionRemSet {
    pub fn new(gc_threads: usize) -> Self {
        let mut rs = Self::default();
        rs.gc_buffers
            .resize_with(gc_threads, || Atomic::new(Box::leak(box vec![])));
        rs
    }

    #[inline]
    fn gc_buffer(&self, id: usize) -> &mut Vec<Address> {
        let ptr = self.gc_buffers[id].load(Ordering::SeqCst);
        unsafe { &mut *ptr }
    }

    #[inline]
    pub fn enable_recording() {
        RECORD.store(true, Ordering::SeqCst);
    }

    #[inline]
    fn disable_recording() {
        RECORD.store(false, Ordering::SeqCst);
    }

    #[inline]
    pub fn recording() -> bool {
        RECORD.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn clear(&mut self) {
        // println!("Clear remset@{:?}", self as *const Self);
        for i in 0..self.gc_buffers.len() {
            self.gc_buffer(i).clear()
        }
        if crate::args::LOG_REMSET_FOOTPRINT {
            self.size.store(0, Ordering::SeqCst);
        }
    }

    #[inline]
    fn add<VM: VMBinding>(&mut self, e: Address, _t: ObjectReference, space: &ImmixSpace<VM>) {
        let v = if space.address_in_space(e) {
            Line::of(e).currrent_validity_state()
        } else {
            debug_assert!(e.is_mapped(), "unmapped edge {:?}", e);
            LargeObjectSpace::<VM>::currrent_validity_state(e)
        };
        let id = crate::gc_worker_id().unwrap();
        self.gc_buffer(id).push(Line::encode_validity_state(e, v));
        // println!(
        //     "! record {:?} -> {:?} epoch={:?} remset@{:?} {:?} is-defrag={:?}",
        //     e,
        //     t,
        //     v,
        //     self as *const Self,
        //     Region::containing::<VM>(t),
        //     Region::containing::<VM>(t).is_defrag_source_active()
        // );
        if crate::args::LOG_REMSET_FOOTPRINT {
            self.size.fetch_add(8, Ordering::SeqCst);
        }
    }

    #[inline]
    pub fn record_unconditional<VM: VMBinding>(
        e: Address,
        t: ObjectReference,
        space: &ImmixSpace<VM>,
    ) {
        if t.is_null() {
            return;
        }
        let a = e.as_usize();
        let b = t.to_address().as_usize();
        if unlikely(((a ^ b) >> Region::LOG_BYTES) != 0 && space.in_space(t)) {
            if CollectionSet::defrag_in_progress() {
                if !Region::containing::<VM>(t).is_defrag_source() {
                    return;
                }
            }
            Region::containing::<VM>(t).remset().add(e, t, space);
        }
    }

    #[inline]
    pub fn record<VM: VMBinding>(e: Address, t: ObjectReference, space: &ImmixSpace<VM>) {
        if !Self::recording() {
            return;
        }
        Self::record_unconditional(e, t, space)
    }

    #[inline]
    pub fn dispatch<VM: VMBinding>(&mut self, space: &ImmixSpace<VM>) -> Vec<Box<dyn GCWork<VM>>> {
        // let mut buffers = vec![];
        // std::mem::swap(&mut buffers, &mut self.gc_buffers);
        // println!("dispatch remset@{:?}", self as *const Self);
        // for i in 0..self.gc_buffers.len() {
        //     println!(
        //         " - dispatch remset@{:?} {:?} {:?}",
        //         self as *const Self,
        //         i,
        //         self.gc_buffer(i)
        //     );
        // }
        // TODO: Optimize this
        (0..self.gc_buffers.len())
            .map(|i| self.gc_buffer(i))
            .filter(|buf| !buf.is_empty())
            .map(|buf| box EvacuateMatureObjects::new(buf.to_vec(), space) as Box<dyn GCWork<VM>>)
            .collect()
    }
}

static IN_DEFRAG: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
pub struct CollectionSet {
    regions: Mutex<Vec<Region>>,
    prev_region: Mutex<Option<Region>>,
    retired_regions: SegQueue<Region>,
    cached_roots: Mutex<Vec<Vec<Address>>>,
    fragmented_regions: SegQueue<Vec<(Region, usize)>>,
    fragmented_regions_size: AtomicUsize,
}

impl CollectionSet {
    pub fn defrag_in_progress() -> bool {
        IN_DEFRAG.load(Ordering::SeqCst)
    }

    pub fn set_reigons(&self, regions: Vec<Region>) {
        *self.regions.lock().unwrap() = regions;
    }

    pub fn clear_cached_roots(&self) {
        self.cached_roots.lock().unwrap().clear();
    }

    pub fn add_cached_roots(&self, x: Vec<Address>) {
        self.cached_roots.lock().unwrap().push(x);
    }

    pub fn schedule_evacuation_packets<VM: VMBinding>(&self, pause: Pause, space: &ImmixSpace<VM>) {
        if pause == Pause::FinalMark
            || pause == Pause::FullTraceFast
            || (pause == Pause::RefCount && CollectionSet::defrag_in_progress())
        {
            space.scheduler().work_buckets[WorkBucketStage::RCEvacuateMature]
                .add(StartMatureEvacuation);
        }
    }

    pub fn schedule_defrag_selection_packets<VM: VMBinding>(
        &self,
        _pause: Pause,
        space: &ImmixSpace<VM>,
    ) {
        if crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG.is_some() {
            assert!(crate::args::LXR_INCREMENTAL_MATURE_DEFRAG);
            space.scheduler().work_buckets[WorkBucketStage::RCCollectionSetSelection]
                .add(SimpleDefragRegionSelection);
            return;
        }
        let tasks = space
            .chunk_map
            .generate_tasks(|chunk| box SelectDefragRegionsInChunk { chunk });
        self.fragmented_regions_size.store(0, Ordering::SeqCst);
        SELECT_DEFRAG_BLOCK_JOB_COUNTER.store(tasks.len(), Ordering::SeqCst);
        space.scheduler().work_buckets[WorkBucketStage::RCCollectionSetSelection].bulk_add(tasks);
    }

    pub fn select_mature_evacuation_candidates<VM: VMBinding>(
        &self,
        _pause: Pause,
        _total_pages: usize,
        space: &ImmixSpace<VM>,
    ) {
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        // Select mature defrag blocks
        let defrag_bytes = space.defrag_headroom_pages() << 12;
        let mut regions = Vec::with_capacity(self.fragmented_regions_size.load(Ordering::SeqCst));
        while let Some(mut x) = self.fragmented_regions.pop() {
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
        self.set_reigons(cset.clone());
    }

    fn should_stop<VM: VMBinding>(&self, _space: &ImmixSpace<VM>) -> bool {
        if crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG.is_some() {
            return self.retired_regions.len()
                >= *crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG_MULTIPLIER;
        }
        self.retired_regions.len() >= 8
    }

    pub fn move_to_next_region<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        IN_DEFRAG.store(true, Ordering::SeqCst);
        let mut regions = self.regions.lock().unwrap();
        if regions.is_empty() {
            return self.finish_evacuation();
        }
        if crate::args::LXR_INCREMENTAL_MATURE_DEFRAG && self.should_stop(space) {
            return;
        }
        let region = regions.pop().unwrap();
        if crate::args::LOG_PER_GC_STATE {
            println!(" ! Defrag {:?}", region);
        }
        debug_assert!(region.is_defrag_source());
        // Activate current region
        region.set_active();
        side_metadata::bzero_x(
            &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec(),
            region.start(),
            Region::BYTES,
        );
        self.retired_regions.push(region);
        // Deactivate previous region
        {
            let mut prev_region = self.prev_region.lock().unwrap();
            if let Some(region) = *prev_region {
                region.set_state(RegionState::Allocated);
            }
            *prev_region = Some(region);
        }
        self.schedule_mature_remset_scanning_packets(region, space);
    }

    fn schedule_mature_remset_scanning_packets<VM: VMBinding>(
        &self,
        region: Region,
        space: &ImmixSpace<VM>,
    ) {
        // Reset allocators between the evacaution of two regions.
        // The allocator may reuse some memory in second region when evacuating first region. And when evacuating the second region,
        // the alloactor may still hold the local alloc buffer that belongs to a defrag block in the second region.
        // No need to do this for non-incremental defrag. Defrag blocks are all selected ahead of time and they'll never be recycled.
        // RemSets
        let mut packets = region.remset().dispatch(space);
        // Roots
        // FIXME
        // unsafe { crate::plan::immix::CURR_ROOTS = SegQueue::new() }
        while let Some(_roots) = unsafe { crate::plan::immix::CURR_ROOTS.pop() } {}
        for roots in &*self.cached_roots.lock().unwrap() {
            packets.push(
                (box LXRMatureEvacRoots::new(roots.clone(), unsafe { &*(space as *const _) }))
                    as Box<dyn GCWork<VM>>,
            );
        }
        if packets.is_empty() {
            packets.push(
                (box LXRMatureEvacRoots::new(vec![], unsafe { &*(space as *const _) }))
                    as Box<dyn GCWork<VM>>,
            );
        }
        // Schedule
        space.scheduler().work_buckets[WorkBucketStage::RCEvacuateMature].bulk_add(packets);
    }

    fn finish_evacuation(&self) {
        if crate::args::LOG_PER_GC_STATE {
            println!(" ! Defrag FINISH");
        }
        *self.prev_region.lock().unwrap() = None;
        self.cached_roots.lock().unwrap().clear();
        IN_DEFRAG.store(false, Ordering::SeqCst);
        PerRegionRemSet::disable_recording();
    }

    pub fn sweep_retired_defrag_regions<VM: VMBinding>(
        &self,
        _pause: Pause,
        space: &ImmixSpace<VM>,
    ) {
        if !self.retired_regions.is_empty() {
            let queue = ArrayQueue::new(self.retired_regions.len() << Region::LOG_BLOCKS);
            while let Some(region) = self.retired_regions.pop() {
                for block in region.committed_blocks() {
                    if block.is_defrag_source() {
                        block.clear_rc_table::<VM>();
                        block.clear_striddle_table::<VM>();
                        if block.rc_sweep_mature::<VM>(space, true) {
                            queue.push(block.start()).unwrap();
                        } else {
                            // unreachable!("{:?} still alive {:?}", block, block.get_state())
                        }
                    }
                }
                region.set_state(RegionState::Allocated);
            }
            space.pr.release_bulk(queue.len(), queue);
        }
    }
}

struct StartMatureEvacuation;

impl<VM: VMBinding> GCWork<VM> for StartMatureEvacuation {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        for w in &immix.immix_space.scheduler().worker_group().workers {
            let w = unsafe { &mut *(w as *const _ as *mut GCWorker<VM>) };
            unsafe { w.local::<ImmixCopyContext<VM>>() }.immix.reset();
        }
        immix
            .immix_space
            .collection_set
            .move_to_next_region(&immix.immix_space)
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
                .collection_set
                .select_mature_evacuation_candidates(
                    immix.current_pause().unwrap(),
                    mmtk.plan.get_total_pages(),
                    &immix.immix_space,
                )
        }
    }
}

struct SimpleDefragRegionSelection;

impl<VM: VMBinding> GCWork<VM> for SimpleDefragRegionSelection {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        // Select N regions in address order
        static CURSOR: Atomic<Address> = Atomic::new(Address::ZERO);
        let immix_space = &mmtk.plan.downcast_ref::<Immix<VM>>().unwrap().immix_space;
        let n = crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG.unwrap()
            * *crate::args::LXR_SIMPLE_INCREMENTAL_DEFRAG_MULTIPLIER;
        let mut cursor = CURSOR.load(Ordering::SeqCst);
        let limit = immix_space.pr.highwater.load(Ordering::SeqCst);
        if cursor.is_zero() {
            cursor = immix_space.pr.start;
        }
        let original_cursor = cursor;
        // Search N regions starting from `cursor`
        let mut regions = vec![];
        while regions.len() < n {
            if !Chunk::of(cursor).is_committed() {
                cursor = Region::align(cursor + Chunk::BYTES);
                continue;
            }
            let region = Region::of(cursor);
            let committed_blocks = region.committed_blocks().count();
            if committed_blocks == 0 {
                cursor += Region::BYTES;
                continue;
            }
            region.set_defrag_source();
            regions.push(region);
            cursor += Region::BYTES;
            if cursor >= limit {
                cursor = immix_space.pr.start;
            }
            if cursor == original_cursor {
                break;
            }
        }
        CURSOR.store(cursor, Ordering::SeqCst);
        // Commit
        immix_space.collection_set.set_reigons(regions);
    }
}

static MATURE_EVAC_JOBS_COUNTER: AtomicUsize = AtomicUsize::new(0);
pub struct MatureEvacJobsCounter<VM: VMBinding>(&'static ImmixSpace<VM>);

impl<VM: VMBinding> MatureEvacJobsCounter<VM> {
    #[inline(always)]
    pub fn new(space: &'static ImmixSpace<VM>) -> Self {
        MATURE_EVAC_JOBS_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self(space)
    }
}

impl<VM: VMBinding> Drop for MatureEvacJobsCounter<VM> {
    #[inline(always)]
    fn drop(&mut self) {
        if MATURE_EVAC_JOBS_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.collection_set.move_to_next_region::<VM>(self.0)
        }
    }
}
