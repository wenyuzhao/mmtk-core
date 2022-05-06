use atomic::{Atomic, Ordering};

use crate::{
    plan::immix::Immix,
    policy::{largeobjectspace::LargeObjectSpace, space::Space},
    scheduler::{gc_work::EvacuateMatureObjects, GCWork, GCWorker},
    util::{Address, ObjectReference},
    vm::VMBinding,
    MMTK,
};

use super::{line::Line, ImmixSpace};

pub struct RemSet {
    pub gc_buffers: Vec<Atomic<*mut Vec<Address>>>,
}

impl RemSet {
    pub fn new() -> Self {
        let workers = *crate::CALC_WORKERS;
        let mut rs = RemSet { gc_buffers: vec![] };
        rs.gc_buffers
            .resize_with(workers, || Atomic::new(Box::leak(box vec![])));
        rs
    }

    #[inline]
    fn gc_buffer(&self, id: usize) -> &mut Vec<Address> {
        let ptr = self.gc_buffers[id].load(Ordering::SeqCst);
        unsafe { &mut *ptr }
    }

    fn flush_all<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        for id in 0..self.gc_buffers.len() {
            self.flush(id, space)
        }
    }

    fn flush<VM: VMBinding>(&self, id: usize, space: &ImmixSpace<VM>) {
        if self.gc_buffer(id).len() > 0 {
            let mut remset = vec![];
            std::mem::swap(&mut remset, self.gc_buffer(id));
            let w = EvacuateMatureObjects::new(remset);
            space.mature_evac_remsets.lock().push(box w);
        }
    }

    #[inline]
    pub fn record<VM: VMBinding>(&self, e: Address, space: &ImmixSpace<VM>) {
        let v = if space.address_in_space(e) {
            Line::of(e).currrent_validity_state()
        } else {
            LargeObjectSpace::<VM>::currrent_validity_state(e)
        };
        let id = crate::gc_worker_id().unwrap();
        self.gc_buffer(id).push(Line::encode_validity_state(e, v));
        if self.gc_buffer(id).len() >= EvacuateMatureObjects::<VM>::CAPACITY {
            self.flush(id, space)
        }
    }
}

pub struct FlushMatureEvacRemsets;

impl<VM: VMBinding> GCWork<VM> for FlushMatureEvacRemsets {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix_space = &mmtk.plan.downcast_ref::<Immix<VM>>().unwrap().immix_space;
        immix_space.remset.flush_all(immix_space);
    }
}
