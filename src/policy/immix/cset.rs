use std::{
    intrinsics::unlikely,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Mutex,
    },
};

use atomic::{Atomic, Ordering};
use crossbeam_queue::SegQueue;

use super::{
    block::Block,
    line::Line,
    region::{Region, RegionState},
    ImmixSpace,
};
use crate::scheduler::ProcessEdgesWork;
use crate::{
    plan::immix::{Immix, ImmixCopyContext, Pause},
    policy::{largeobjectspace::LargeObjectSpace, space::Space},
    scheduler::{gc_work::EvacuateMatureObjects, GCWork, GCWorker, WorkBucketStage},
    util::{cm::LXRMatureEvacRoots, Address, ObjectReference},
    vm::VMBinding,
    MMTK,
};
use crate::{plan::Plan, util::cm::LXRMatureEvacProcessEdges};
use crate::{
    util::metadata::side_metadata,
    vm::{ActivePlan, ObjectModel},
};

static RECORD: AtomicBool = AtomicBool::new(false);

pub type RegionSelection = Result<Vec<Region>, Vec<Region>>;
pub type PartialRegionSelection = Result<(), Vec<Region>>;

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
        RECORD.load(Ordering::Relaxed)
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
        if crate::args::LXR_EAGER_DEFRAG_SELECTION {
            if space.in_space(t) && Block::containing::<VM>(t).is_defrag_source() {
                if ((a ^ b) >> Region::LOG_BYTES) != 0 || !Block::of(e).is_defrag_source() {
                    Region::containing::<VM>(t).remset().add(e, t, space);
                }
            }
        } else {
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
        (0..self.gc_buffers.len())
            .filter(|i| !self.gc_buffer(*i).is_empty())
            .flat_map(|i| {
                let buf = self.gc_buffer(i);
                buf.chunks(LXRMatureEvacProcessEdges::<VM>::CAPACITY)
                    .map(|chunk| {
                        box EvacuateMatureObjects::new(chunk.to_vec(), space) as Box<dyn GCWork<VM>>
                    })
            })
            .collect()
    }
}

static IN_DEFRAG: AtomicBool = AtomicBool::new(false);
static FORCE_EVACUATE_ALL: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
pub struct CollectionSet {
    regions: Mutex<Vec<Region>>,
    prev_regions: Mutex<Vec<Region>>,
    retired_regions: SegQueue<Region>,
    cached_roots: Mutex<Vec<Vec<Address>>>,
}

impl CollectionSet {
    pub fn time_and_space_limit_test<VM: VMBinding>() -> PartialRegionSelection {
        if !*crate::args::OPPORTUNISTIC_EVAC {
            return Ok(());
        }
        let plan = VM::VMActivePlan::global()
            .downcast_ref::<Immix<VM>>()
            .unwrap();
        let over_space =
            plan.get_pages_used() - plan.get_collection_reserve() > plan.get_total_pages();
        if over_space {
            return Err(vec![]);
        }
        let pause_time = crate::GC_START_TIME
            .load(Ordering::SeqCst)
            .elapsed()
            .unwrap()
            .as_millis();
        if pause_time as usize >= *crate::args::OPPORTUNISTIC_EVAC_THRESHOLD {
            Err(vec![])
        } else {
            Ok(())
        }
    }
    pub fn incremental_test(regions: &mut Vec<Region>) -> PartialRegionSelection {
        if !*crate::args::LXR_INCREMENTAL_DEFRAG {
            Err(Self::take_all_regions(regions).unwrap())
        } else {
            Ok(())
        }
    }

    pub fn take_all_regions(regions: &mut Vec<Region>) -> RegionSelection {
        let mut selected = vec![];
        std::mem::swap(regions, &mut selected);
        Ok(selected)
    }

    pub fn pop_prioritized_regions(regions: &mut Vec<Region>) -> RegionSelection {
        let mut selected = vec![];
        let m = *crate::args::LXR_DEFRAG_COALESCE_M;
        while let Some(region) = regions.pop() {
            selected.push(region);
            if selected.len() >= m {
                break;
            }
        }
        Ok(selected)
    }

    pub fn filter_prioritized_regions(
        regions: &mut Vec<Region>,
        mut f: impl FnMut(Region) -> bool,
    ) -> RegionSelection {
        let mut selected = vec![];
        while let Some(region) = regions.pop() {
            if !f(region) {
                return Ok(selected);
            }
            selected.push(region);
        }
        Ok(selected)
    }

    pub fn retired_regions(&self) -> usize {
        self.retired_regions.len()
    }

    pub fn defrag_in_progress() -> bool {
        IN_DEFRAG.load(Ordering::SeqCst)
    }

    pub fn force_evacuate_all() {
        FORCE_EVACUATE_ALL.store(true, Ordering::SeqCst)
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
        space.scheduler().work_buckets[WorkBucketStage::RCCollectionSetSelection]
            .add(SelectDefragRegions);
    }

    fn prepare_region_for_evacuation<VM: VMBinding>(&self, region: Region) {
        region.set_active();
        if crate::args::LXR_EAGER_DEFRAG_SELECTION {
            for block in region.defrag_blocks() {
                side_metadata::bzero_x(
                    &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec(),
                    block.start(),
                    Block::BYTES,
                );
            }
        } else {
            side_metadata::bzero_x(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec(),
                region.start(),
                Region::BYTES,
            );
        }
        self.retired_regions.push(region);
    }

    fn sort_regions(regions: &mut Vec<Region>) {
        if *crate::args::SORT_REGIONS_AFTER_SATB {
            regions.sort_by_cached_key(|r| {
                let mut copy = 0usize;
                let mut reclaim = 0usize;
                for b in r.defrag_blocks() {
                    reclaim += 1;
                    copy += b.live_bytes()
                }
                reclaim * Block::BYTES * 1000 / copy
            });
        }
    }

    pub fn move_to_next_region<VM: VMBinding>(&self, space: &ImmixSpace<VM>, first: bool) {
        IN_DEFRAG.store(true, Ordering::SeqCst);
        let mut regions = self.regions.lock().unwrap();
        // Deactivate previous region
        {
            let mut prev_regions = self.prev_regions.lock().unwrap();
            for region in &*prev_regions {
                space.defrag_policy.notify_defrag_end(*region);
                region.set_state(RegionState::Allocated);
            }
            prev_regions.clear();
        }
        // Filter out all the empty regions
        let retired_regions = regions.drain_filter(|r| {
            let c = r.defrag_blocks().filter(|b| b.live_bytes() > 0).count();
            c == 0
        });
        for r in retired_regions {
            self.retired_regions.push(r);
        }
        // Should pause or finish evacuation?
        if regions.is_empty() {
            return self.finish_evacuation();
        }
        // Select regions to evacuate
        Self::sort_regions(&mut regions);
        let selected_regions = if FORCE_EVACUATE_ALL.load(Ordering::SeqCst) {
            Self::take_all_regions(&mut regions).unwrap()
        } else {
            match space.defrag_policy.schedule(self, &mut regions) {
                Ok(v) => v,
                Err(v) => v,
            }
        };
        if selected_regions.is_empty() {
            space.defrag_policy.notify_evacuation_stop();
            return;
        }
        for region in &selected_regions {
            // if crate::args::LOG_PER_GC_STATE {
            //     println!(" ! Defrag {:?}", region);
            // }
            debug_assert!(region.is_defrag_source());
            self.prepare_region_for_evacuation::<VM>(*region);
            self.retired_regions.push(*region);
            space.defrag_policy.notify_defrag_start(*region);
        }
        if crate::args::LOG_PER_GC_STATE {
            println!("--- move_to_next_regions ({}) ---", selected_regions.len());
        }
        if first {
            crate::COUNTERS.defrag.fetch_add(1, Ordering::Relaxed);
        }
        *self.prev_regions.lock().unwrap() = selected_regions.clone();
        self.schedule_mature_remset_scanning_packets(selected_regions, space);
    }

    fn schedule_mature_remset_scanning_packets<VM: VMBinding>(
        &self,
        regions: Vec<Region>,
        space: &ImmixSpace<VM>,
    ) {
        // Reset allocators between the evacaution of two regions.
        // The allocator may reuse some memory in second region when evacuating first region. And when evacuating the second region,
        // the alloactor may still hold the local alloc buffer that belongs to a defrag block in the second region.
        // No need to do this for non-incremental defrag. Defrag blocks are all selected ahead of time and they'll never be recycled.
        // RemSets
        let mut packets = vec![];
        for region in regions {
            packets.append(&mut region.remset().dispatch(space));
        }
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
        self.prev_regions.lock().unwrap().clear();
        self.cached_roots.lock().unwrap().clear();
        IN_DEFRAG.store(false, Ordering::SeqCst);
        FORCE_EVACUATE_ALL.store(false, Ordering::SeqCst);
        PerRegionRemSet::disable_recording();
    }

    pub fn sweep_retired_defrag_regions<VM: VMBinding>(
        &self,
        _pause: Pause,
        space: &ImmixSpace<VM>,
    ) {
        if !self.retired_regions.is_empty() {
            while let Some(region) = self.retired_regions.pop() {
                crate::COUNTERS
                    .evacuated_mature_regions
                    .fetch_add(1, Ordering::Relaxed);
                let mut blocks = 0usize;
                let mut bytes = 0usize;
                for block in region.committed_blocks() {
                    if block.is_defrag_source() {
                        blocks += 1;
                        bytes += block.live_bytes();
                        block.clear_rc_table::<VM>();
                        block.clear_striddle_table::<VM>();
                        if block.rc_sweep_mature::<VM>(space, true) {
                            space.pr.release_pages(block.start());
                        } else {
                            // unreachable!("{:?} still alive {:?}", block, block.get_state())
                        }
                    }
                }
                crate::COUNTERS
                    .evacuated_mature_blocks
                    .fetch_add(blocks, Ordering::Relaxed);
                crate::COUNTERS
                    .evacuated_mature_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
                region.set_state(RegionState::Allocated);
            }
        }
    }
}

struct SelectDefragRegions;

impl<VM: VMBinding> GCWork<VM> for SelectDefragRegions {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        immix.immix_space.defrag_policy.select(mmtk);
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
        let pause = immix.current_pause().unwrap();
        immix.immix_space.collection_set.move_to_next_region(
            &immix.immix_space,
            pause == Pause::FullTraceFast || pause == Pause::FinalMark,
        )
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
            self.0
                .collection_set
                .move_to_next_region::<VM>(self.0, false)
        }
    }
}
