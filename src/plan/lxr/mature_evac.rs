use std::marker::PhantomData;

use crate::util::ObjectReference;
use crate::vm::edge_shape::Edge;
use crate::{
    plan::{immix::Pause, lxr::cm::LXRStopTheWorldProcessEdges},
    policy::{
        immix::block::{Block, BlockState},
        space::Space,
    },
    scheduler::{GCWork, GCWorker, WorkBucketStage},
    vm::VMBinding,
    MMTK,
};

use super::remset::RemSetEntry;
use super::LXR;

pub struct EvacuateMatureObjects<VM: VMBinding> {
    remset: Vec<RemSetEntry>,
    _p: PhantomData<VM>,
}

impl<VM: VMBinding> EvacuateMatureObjects<VM> {
    pub const CAPACITY: usize = 1024;

    pub(super) fn new(remset: Vec<RemSetEntry>) -> Self {
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        Self {
            remset,
            _p: PhantomData,
        }
    }

    fn address_is_valid_oop_edge(&self, e: VM::VMEdge, lxr: &LXR<VM>) -> bool {
        // Keep edges not in the mmtk heap
        // These should be edges in the c++ `ClassLoaderData` objects. We remember these edges
        // in the remembered-set to avoid expensive CLD scanning.
        if !lxr.immix_space.address_in_space(e.to_address())
            && !lxr.los().address_in_space(e.to_address())
        {
            return true;
        }
        // Skip edges in collection set
        if lxr.address_in_defrag(e.to_address()) {
            return false;
        }
        if crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING {
            return true;
        }
        // Check if it is a real oop field
        if lxr.immix_space.address_in_space(e.to_address()) {
            let block = Block::of(e.to_address());
            if block.get_state() == BlockState::Unallocated {
                return false;
            }
        }
        true
    }

    fn process_edge(&mut self, e: VM::VMEdge, old_ref: ObjectReference, lxr: &LXR<VM>) -> bool {
        // Skip edges that does not contain a real oop
        if !self.address_is_valid_oop_edge(e, lxr) {
            return false;
        }
        // Skip objects that are dead or out of the collection set.
        let Some(o) = e.load() else {
            return false;
        };
        if old_ref != o {
            return false;
        }
        if !o.is_in_any_space::<VM>() || !lxr.immix_space.in_space(o) {
            return false;
        }
        if !lxr.rc.is_dead(o) && Block::in_defrag_block::<VM>(o) {
            return true;
        }
        false
        // Maybe a forwarded nursery or mature object from inc processing.
        // if object_forwarding::is_forwarded_or_being_forwarded::<VM>(o) {
        //     return true;
        // }
        // rc::count(o) != 0 && Block::in_defrag_block::<VM>(o)
    }

    fn process_edges(&mut self, mmtk: &'static MMTK<VM>) -> Option<Box<dyn GCWork<VM>>> {
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        debug_assert!(
            lxr.current_pause() == Some(Pause::FinalMark)
                || lxr.current_pause() == Some(Pause::Full)
        );
        let remset = std::mem::take(&mut self.remset);
        let mut edges = vec![];
        let mut refs = vec![];
        for entry in remset {
            let (e, o) = entry.decode::<VM>();
            if self.process_edge(e, o, lxr) {
                edges.push(e);
                refs.push(o);
            }
        }
        if !edges.is_empty() {
            Some(Box::new(LXRStopTheWorldProcessEdges::new_remset(
                edges, refs, mmtk,
            )))
        } else {
            None
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for EvacuateMatureObjects<VM> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let Some(work) = self.process_edges(mmtk) else {
            return;
        };
        // transitive closure
        worker.add_boxed_work(WorkBucketStage::Closure, work)
    }
}
