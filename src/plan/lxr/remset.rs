use std::{cell::UnsafeCell, marker::PhantomData};

use crate::policy::immix::ImmixSpace;
use crate::util::ObjectReference;
use crate::{
    plan::lxr::LXR,
    scheduler::{GCWork, GCWorker},
    util::Address,
    vm::{slot::Slot, VMBinding},
    MMTK,
};

use super::mature_evac::EvacuateMatureObjects;
use atomic::Ordering;
use std::sync::atomic::AtomicUsize;

#[repr(C)]
pub(super) struct RemSetEntry(Address, ObjectReference);

impl RemSetEntry {
    fn encode<VM: VMBinding>(slot: VM::VMSlot, o: ObjectReference) -> Self {
        Self(slot.raw_address(), o)
    }

    pub fn decode<VM: VMBinding>(&self) -> (VM::VMSlot, ObjectReference) {
        (VM::VMSlot::from_address(self.0), self.1)
    }
}

pub struct MatureEvecRemSet<VM: VMBinding> {
    pub(super) gc_buffers: Vec<UnsafeCell<Vec<RemSetEntry>>>,
    local_packets: Vec<UnsafeCell<Vec<Box<dyn GCWork<VM>>>>>,
    _p: PhantomData<VM>,
    size: AtomicUsize,
}

impl<VM: VMBinding> MatureEvecRemSet<VM> {
    pub fn new(workers: usize) -> Self {
        let mut rs = Self {
            gc_buffers: vec![],
            local_packets: vec![],
            _p: PhantomData,
            size: AtomicUsize::new(0),
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
        let mut size = self.size.load(Ordering::SeqCst);
        self.size.store(0, Ordering::SeqCst);
        for id in 0..self.gc_buffers.len() {
            if self.gc_buffer(id).len() > 0 {
                let remset = std::mem::take(self.gc_buffer(id));
                size += remset.len();
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
        if cfg!(feature = "remset_counter") {
            gc_log!("REMSET ENTRIES: {}", size);
        }
    }

    #[cold]
    fn flush(&self, id: usize) {
        if self.gc_buffer(id).len() > 0 {
            let remset = std::mem::take(self.gc_buffer(id));
            self.size.fetch_add(remset.len(), Ordering::SeqCst);
            if cfg!(feature = "rust_mem_counter") {
                crate::rust_mem_counter::MATURE_EVAC_REMSET_COUNTER.sub(remset.len());
            }
            let w = EvacuateMatureObjects::new(remset);
            let packet_buffer = unsafe { &mut *self.local_packets[id].get() };
            packet_buffer.push(Box::new(w));
        }
    }

    pub fn record(&self, s: VM::VMSlot, o: ObjectReference) {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::MATURE_EVAC_REMSET_COUNTER.add(1);
        }
        let id = crate::gc_worker_id().unwrap();
        self.gc_buffer(id).push(RemSetEntry::encode::<VM>(s, o));
        if self.gc_buffer(id).len() >= EvacuateMatureObjects::<VM>::CAPACITY {
            self.flush(id)
        }
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
        immix_space.mature_evac_remset.flush_all(immix_space);
        immix_space.process_mature_evacuation_remset();
    }
}
