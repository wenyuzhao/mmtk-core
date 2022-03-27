use std::{
    intrinsics::unlikely,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Mutex, MutexGuard,
    },
};

use atomic::{Atomic, Ordering};
use crossbeam_queue::SegQueue;

use super::{
    block::{Block, BlockState},
    chunk::Chunk,
    line::Line,
    region::Region,
    ImmixSpace,
};
use crate::{
    plan::immix::{Immix, Pause},
    policy::{largeobjectspace::LargeObjectSpace, space::Space},
    scheduler::{gc_work::EvacuateMatureObjects, GCWork, GCWorker, WorkBucketStage},
    util::{
        cm::{LXRMatureEvacProcessEdges, LXRMatureEvacRoots},
        Address, ObjectReference,
    },
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
    pub fn disable_recording() {
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
    fn add<VM: VMBinding>(&mut self, e: Address, t: ObjectReference, space: &ImmixSpace<VM>) {
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
        // if Region::containing::<VM>(t).is_defrag_source_active() {
        //     for i in 0..self.gc_buffers.len() {
        //         println!(
        //             " - add remset@{:?} {:?} {:?}",
        //             self as *const Self,
        //             i,
        //             self.gc_buffer(i)
        //         );
        //     }
        // }
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

#[derive(Debug, Default)]
pub struct CollectionSet {
    regions: Mutex<Vec<Region>>,
    retried_regions: Mutex<Vec<Region>>,
    cached_roots: Mutex<Vec<Vec<Address>>>,
    pub in_defrag: AtomicBool,
}

impl CollectionSet {
    pub fn enable_defrag(&self) {
        debug_assert!(!self.in_defrag.load(Ordering::SeqCst));
        self.in_defrag.store(true, Ordering::SeqCst);
    }

    pub fn set_reigons(&self, regions: Vec<Region>) {
        *self.regions.lock().unwrap() = regions;
    }

    pub fn add_cached_roots(&self, x: Vec<Address>) {
        self.cached_roots.lock().unwrap().push(x);
    }

    pub fn move_to_next_region<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        let mut regions = self.regions.lock().unwrap();
        if let Some(region) = regions.pop() {
            if crate::args::LOG_PER_GC_STATE {
                println!(" ! Defrag {:?}", region);
            }
            debug_assert!(region.is_defrag_source());
            region.set_active();
            side_metadata::bzero_x(
                &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec(),
                region.start(),
                Region::BYTES,
            );
            space.last_defrag_regions.push(region);
            self.schedule_mature_remset_scanning_packets(region, space);
        } else {
            self.finish_evacuation();
        }
    }

    fn schedule_mature_remset_scanning_packets<VM: VMBinding>(
        &self,
        region: Region,
        space: &ImmixSpace<VM>,
    ) {
        // RemSets
        let mut packets = region.remset().dispatch(space);
        // Roots
        while let Some(roots) = unsafe { crate::plan::immix::CURR_ROOTS.pop() } {}
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
        self.cached_roots.lock().unwrap().clear();
    }
}

static SELECT_DEFRAG_BLOCK_JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct SelectDefragRegionsInChunk {
    chunk: Chunk,
}

impl<VM: VMBinding> GCWork<VM> for SelectDefragRegionsInChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        // let mut regions = vec![];
        // // Iterate over all regions in this chunk
        // for region in self.chunk.regions() {
        //     debug_assert!(!region.is_defrag_source());
        //     let mut live_blocks = 0usize;
        //     let mut live_bytes = 0usize;
        //     for block in region.committed_blocks() {
        //         let state = block.get_state();
        //         if state == BlockState::Unallocated || state == BlockState::Nursery {
        //             continue;
        //         }
        //         let dead_bytes = block.calc_dead_lines() << Line::LOG_BYTES;
        //         live_bytes += Block::BYTES - dead_bytes;
        //         live_blocks += 1;
        //     }
        //     if live_blocks != 0 && live_bytes <= (Region::BYTES >> 1) {
        //         regions.push((region, Region::BYTES - live_bytes));
        //     }
        // }
        // let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        // immix
        //     .immix_space
        //     .fragmented_regions_size
        //     .fetch_add(regions.len(), Ordering::SeqCst);
        // immix.immix_space.fragmented_regions.push(regions);
        // if SELECT_DEFRAG_BLOCK_JOB_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
        //     immix.immix_space.select_mature_evacuation_candidates(
        //         immix.current_pause().unwrap(),
        //         mmtk.plan.get_total_pages(),
        //     )
        // }
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
