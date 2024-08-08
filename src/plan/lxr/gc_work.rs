use super::cm::LXRWeakRefProcessEdges;
use super::LXR;
use crate::scheduler::{gc_work::*, GCWork, GCWorker};
use crate::{vm::*, Plan, MMTK};

pub(super) struct LXRGCWorkContext<E: ProcessEdgesWork>(std::marker::PhantomData<E>);

impl<E: ProcessEdgesWork> crate::scheduler::GCWorkContext for LXRGCWorkContext<E> {
    type VM = E::VM;
    type PlanType = LXR<E::VM>;
    type DefaultProcessEdges = E;
    type PinningProcessEdges = UnsupportedProcessEdges<Self::VM>;
}

pub(super) struct LXRWeakRefWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);

impl<VM: VMBinding> crate::scheduler::GCWorkContext for LXRWeakRefWorkContext<VM> {
    type VM = VM;
    type PlanType = LXR<VM>;
    type DefaultProcessEdges = LXRWeakRefProcessEdges<VM>;
    type PinningProcessEdges = UnsupportedProcessEdges<Self::VM>;
}

#[derive(Default)]
pub struct FastRCPrepare<VM>(std::marker::PhantomData<VM>);

impl<VM: VMBinding> GCWork for FastRCPrepare<VM> {
    fn do_work(&mut self) {
        let worker = GCWorker::<VM>::current();
        let mmtk = worker.mmtk;
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        #[allow(invalid_reference_casting)]
        let lxr = unsafe { &mut *(lxr as *const LXR<VM> as *mut LXR<VM>) };
        lxr.prepare(worker.tls)
    }
}

#[derive(Default)]
pub struct ReleaseLOSNursery<VM>(std::marker::PhantomData<VM>);

impl<VM: VMBinding> GCWork for ReleaseLOSNursery<VM> {
    fn do_work(&mut self) {
        let worker = GCWorker::<VM>::current();
        let mmtk = worker.mmtk;
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        lxr.los().release_rc_nursery_objects();
    }
}
