use std::marker::PhantomData;

use crate::util::rc;
use crate::vm::edge_shape::Edge;
use crate::{
    plan::{immix::Pause, lxr::cm::LXRStopTheWorldProcessEdges},
    policy::{
        immix::{
            block::{Block, BlockState},
            line::Line,
        },
        space::Space,
    },
    scheduler::{GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    util::{Address, ObjectReference},
    vm::VMBinding,
    MMTK,
};

use super::LXR;

pub struct EvacuateMatureObjects<VM: VMBinding> {
    remset: Vec<VM::VMEdge>,
    _p: PhantomData<VM>,
}

unsafe impl<VM: VMBinding> Send for EvacuateMatureObjects<VM> {}
unsafe impl<VM: VMBinding> Sync for EvacuateMatureObjects<VM> {}

impl<VM: VMBinding> EvacuateMatureObjects<VM> {
    pub const CAPACITY: usize = 512;

    pub fn new(remset: Vec<VM::VMEdge>) -> Self {
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        Self {
            remset,
            _p: PhantomData,
        }
    }

    #[inline(always)]
    fn address_is_valid_oop_edge(&self, e: Address, epoch: u8, lxr: &LXR<VM>) -> bool {
        // Skip edges not in the mmtk heap
        if !lxr.immix_space.address_in_space(e) && !lxr.los().address_in_space(e) {
            return false;
        }
        // Skip edges in collection set
        if lxr.address_in_defrag(e) {
            return false;
        }
        if crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING {
            return true;
        }
        // Check if it is a real oop field
        if lxr.immix_space.address_in_space(e) {
            let block = Block::of(e);
            if block.get_state() == BlockState::Unallocated {
                return false;
            }
            if Line::of(e).pointer_is_valid(epoch) {
                return true;
            }
            false
        } else {
            if lxr.los().pointer_is_valid(e, epoch) {
                return true;
            }
            false
        }
    }

    #[inline]
    fn process_edge(&mut self, e: Address, epoch: u8, lxr: &LXR<VM>) -> bool {
        debug_assert!(e.is_mapped());
        // Skip edges that does not contain a real oop
        if !self.address_is_valid_oop_edge(e, epoch, lxr) {
            return false;
        }
        // Skip objects that are dead or out of the collection set.
        let o = unsafe { e.load::<ObjectReference>() };
        if !lxr.immix_space.in_space(o) || !o.is_in_any_space() {
            return false;
        }
        if rc::count(o) != 0 && Block::in_defrag_block::<VM>(o) {
            return true;
        }
        false
        // Maybe a forwarded nursery or mature object from inc processing.
        // if object_forwarding::is_forwarded_or_being_forwarded::<VM>(o) {
        //     return true;
        // }
        // rc::count(o) != 0 && Block::in_defrag_block::<VM>(o)
    }
}

impl<VM: VMBinding> GCWork<VM> for EvacuateMatureObjects<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        debug_assert!(
            lxr.current_pause() == Some(Pause::FinalMark)
                || lxr.current_pause() == Some(Pause::FullTraceFast)
        );
        // cleanup edges
        let remset = std::mem::take(&mut self.remset);
        let edges = remset
            .into_iter()
            .filter(|e| {
                let (e, epoch) = Line::decode_validity_state(e.to_address());
                self.process_edge(e, epoch, lxr)
            })
            .map(|e| VM::VMEdge::from_address(Line::decode_validity_state(e.to_address()).0))
            .collect::<Vec<_>>();
        // transitive closure
        let w = LXRStopTheWorldProcessEdges::new(edges, false, mmtk);
        worker.add_work(WorkBucketStage::Closure, w);
    }
}
