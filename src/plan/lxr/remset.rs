use std::{cell::UnsafeCell, marker::PhantomData};

use crate::policy::immix::{line::Line, ImmixSpace};
use crate::util::ObjectReference;
use crate::{
    plan::lxr::LXR,
    policy::{largeobjectspace::LargeObjectSpace, space::Space},
    scheduler::{GCWork, GCWorker},
    util::Address,
    vm::{edge_shape::Edge, VMBinding},
    MMTK,
};

use super::mature_evac::EvacuateMatureObjects;

#[repr(C)]
pub(super) struct RemSetEntry(usize, ObjectReference);

impl RemSetEntry {
    fn encode<VM: VMBinding>(edge: VM::VMEdge, epoch: u8, o: ObjectReference) -> Self {
        Self(edge.raw_address().as_usize() | ((epoch as usize) << 48), o)
    }

    pub fn decode<VM: VMBinding>(&self) -> (VM::VMEdge, u8, ObjectReference) {
        let v = ((self.0 >> 48) & 0xff) as u8;
        let p = unsafe { Address::from_usize(self.0 & 0xff00_ffff_ffff_ffff_usize) };
        (VM::VMEdge::from_address(p), v, self.1)
    }
}

pub struct RemSet<VM: VMBinding> {
    pub(super) gc_buffers: Vec<UnsafeCell<Vec<RemSetEntry>>>,
    local_packets: Vec<UnsafeCell<Vec<Box<dyn GCWork<VM>>>>>,
    _p: PhantomData<VM>,
}

impl<VM: VMBinding> RemSet<VM> {
    pub const NO_VALIDITY_STATE: bool = true;

    pub fn new(workers: usize) -> Self {
        let mut rs = RemSet {
            gc_buffers: vec![],
            local_packets: vec![],
            _p: PhantomData,
        };
        rs.gc_buffers
            .resize_with(workers, || UnsafeCell::new(vec![]));
        rs.local_packets
            .resize_with(workers, || UnsafeCell::new(vec![]));
        rs
    }

    fn gc_buffer(&self, id: usize) -> &mut Vec<RemSetEntry> {
        unsafe { &mut *self.gc_buffers[id].get() }
    }

    fn flush_all(&self, space: &ImmixSpace<VM>) {
        let mut mature_evac_remsets = space.mature_evac_remsets.lock().unwrap();
        for id in 0..self.gc_buffers.len() {
            if self.gc_buffer(id).len() > 0 {
                let remset = std::mem::take(self.gc_buffer(id));
                if cfg!(feature = "rust_mem_counter") {
                    crate::rust_mem_counter::MATURE_EVAC_REMSET_COUNTER.sub(remset.len());
                }
                mature_evac_remsets.push(Box::new(EvacuateMatureObjects::new(remset)));
            }
        }
        for id in 0..self.local_packets.len() {
            let buf = unsafe { &mut *self.local_packets[id].get() };
            if buf.len() > 0 {
                let packets = std::mem::take(buf);
                for p in packets {
                    mature_evac_remsets.push(p);
                }
            }
        }
    }

    #[cold]
    fn flush(&self, id: usize, _space: &ImmixSpace<VM>) {
        if self.gc_buffer(id).len() > 0 {
            let remset = std::mem::take(self.gc_buffer(id));
            if cfg!(feature = "rust_mem_counter") {
                crate::rust_mem_counter::MATURE_EVAC_REMSET_COUNTER.sub(remset.len());
            }
            let w = EvacuateMatureObjects::new(remset);
            let packet_buffer = unsafe { &mut *self.local_packets[id].get() };
            packet_buffer.push(Box::new(w));
        }
    }

    pub fn get_currrent_validity_state(&self, e: VM::VMEdge, space: &ImmixSpace<VM>) -> u8 {
        if Self::NO_VALIDITY_STATE {
            return 0;
        }
        // FIXME: performance?
        if !e.to_address().is_mapped() {
            0
        } else if space.address_in_space(e.to_address()) {
            Line::of(e.to_address()).currrent_validity_state()
        } else {
            LargeObjectSpace::<VM>::currrent_validity_state(e.to_address())
        }
    }

    pub fn record_with_validity_state(
        &self,
        e: VM::VMEdge,
        o: ObjectReference,
        space: &ImmixSpace<VM>,
        validity_state: u8,
    ) {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::MATURE_EVAC_REMSET_COUNTER.add(1);
        }
        let id = crate::gc_worker_id().unwrap();
        self.gc_buffer(id)
            .push(RemSetEntry::encode::<VM>(e, validity_state, o));
        if self.gc_buffer(id).len() >= EvacuateMatureObjects::<VM>::CAPACITY {
            self.flush(id, space)
        }
    }

    pub fn record(&self, e: VM::VMEdge, o: ObjectReference, space: &ImmixSpace<VM>) {
        let validity_state = self.get_currrent_validity_state(e, space);
        self.record_with_validity_state(e, o, space, validity_state);
    }
}

pub struct FlushMatureEvacRemsets;

impl<VM: VMBinding> GCWork<VM> for FlushMatureEvacRemsets {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix_space = &mmtk
            .get_plan()
            .downcast_ref::<LXR<VM>>()
            .unwrap()
            .immix_space;
        immix_space.remset.flush_all(immix_space);
        immix_space.process_mature_evacuation_remset();
    }
}
