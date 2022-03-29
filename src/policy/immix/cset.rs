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
static FORCE_EVACUATE_ALL: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
pub struct CollectionSet {
    regions: Mutex<Vec<Region>>,
    prev_region: Mutex<Option<Region>>,
    pub retired_regions: SegQueue<Region>,
    cached_roots: Mutex<Vec<Vec<Address>>>,
    pub fragmented_regions: SegQueue<Vec<(Region, usize)>>,
    pub fragmented_regions_size: AtomicUsize,
}

impl CollectionSet {
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

    fn should_stop<VM: VMBinding>(&self, space: &ImmixSpace<VM>) -> bool {
        if FORCE_EVACUATE_ALL.load(Ordering::SeqCst) {
            return false;
        }
        space.defrag_policy.should_stop(self)
    }

    pub fn move_to_next_region<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        IN_DEFRAG.store(true, Ordering::SeqCst);
        let mut regions = self.regions.lock().unwrap();
        if regions.is_empty() {
            return self.finish_evacuation();
        }
        if self.should_stop(space) {
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
        FORCE_EVACUATE_ALL.store(false, Ordering::SeqCst);
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
        immix
            .immix_space
            .collection_set
            .move_to_next_region(&immix.immix_space)
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
