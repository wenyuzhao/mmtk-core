use std::{cell::UnsafeCell, marker::PhantomData};

use crate::{
    plan::lxr::LXR,
    policy::{largeobjectspace::LargeObjectSpace, space::Space},
    scheduler::{GCWork, GCWorker},
    util::Address,
    vm::{edge_shape::Edge, VMBinding},
    MMTK,
};

use crate::policy::immix::{line::Line, ImmixSpace};

use super::mature_evac::EvacuateMatureObjects;

#[repr(transparent)]
pub(super) struct RemSetEntry(usize);

impl RemSetEntry {
    #[inline(always)]
    fn encode<VM: VMBinding>(edge: VM::VMEdge, epoch: u8) -> Self {
        Self(edge.to_address().as_usize() | ((epoch as usize) << 48))
    }

    #[inline(always)]
    pub fn decode<VM: VMBinding>(&self) -> (VM::VMEdge, u8) {
        let v = ((self.0 >> 48) & 0xff) as u8;
        let p = unsafe { Address::from_usize(self.0 & 0xff00_ffff_ffff_ffff_usize) };
        (VM::VMEdge::from_address(p), v)
    }
}

pub struct RemSet<VM: VMBinding> {
    pub(super) gc_buffers: Vec<UnsafeCell<Vec<RemSetEntry>>>,
    _p: PhantomData<VM>,
}

impl<VM: VMBinding> RemSet<VM> {
    pub fn new(workers: usize) -> Self {
        let mut rs = RemSet {
            gc_buffers: vec![],
            _p: PhantomData,
        };
        rs.gc_buffers
            .resize_with(workers, || UnsafeCell::new(vec![]));
        rs
    }

    #[inline(always)]
    fn gc_buffer(&self, id: usize) -> &mut Vec<RemSetEntry> {
        unsafe { &mut *self.gc_buffers[id].get() }
    }

    fn flush_all(&self, space: &ImmixSpace<VM>) {
        let mut mature_evac_remsets = space.mature_evac_remsets.lock();
        for id in 0..self.gc_buffers.len() {
            if self.gc_buffer(id).len() > 0 {
                let remset = std::mem::take(self.gc_buffer(id));
                mature_evac_remsets.push(Box::new(EvacuateMatureObjects::new(remset)));
            }
        }
    }

    #[cold]
    fn flush(&self, id: usize, space: &ImmixSpace<VM>) {
        if self.gc_buffer(id).len() > 0 {
            let remset = std::mem::take(self.gc_buffer(id));
            let w = EvacuateMatureObjects::new(remset);
            space.mature_evac_remsets.lock().push(Box::new(w));
        }
    }

    #[inline(always)]
    pub fn record(&self, e: VM::VMEdge, space: &ImmixSpace<VM>) {
        // FIXME: performance?
        let v = if !e.to_address().is_mapped() {
            0
        } else if space.address_in_space(e.to_address()) {
            Line::of(e.to_address()).currrent_validity_state()
        } else {
            LargeObjectSpace::<VM>::currrent_validity_state(e.to_address())
        };
        let id = crate::gc_worker_id().unwrap();
        self.gc_buffer(id).push(RemSetEntry::encode::<VM>(e, v));
        if self.gc_buffer(id).len() >= EvacuateMatureObjects::<VM>::CAPACITY {
            self.flush(id, space)
        }
    }
}

pub struct FlushMatureEvacRemsets;

impl<VM: VMBinding> GCWork<VM> for FlushMatureEvacRemsets {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix_space = &mmtk.plan.downcast_ref::<LXR<VM>>().unwrap().immix_space;
        immix_space.remset.flush_all(immix_space);
        immix_space.process_mature_evacuation_remset();
    }
}
