use super::cm::LXRWeakRefProcessEdges;
use super::emergency::LXREmergencyWeakRefProcessEdges;
use super::LXR;
use crate::scheduler::{gc_work::*, GCWork, GCWorker};
use crate::{vm::*, Plan, MMTK};

pub(in crate::plan) type TraceKind = u8;
pub(in crate::plan) const TRACE_KIND_DEFAULT: TraceKind = 0;

pub(super) struct LXRGCWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);

impl<VM: VMBinding> crate::scheduler::GCWorkContext for LXRGCWorkContext<VM> {
    type VM = VM;
    type PlanType = LXR<VM>;
    type ProcessEdgesWorkType = PlanProcessEdges<VM, LXR<VM>, TRACE_KIND_DEFAULT>;
}

pub(super) struct LXRWeakRefWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);

impl<VM: VMBinding> crate::scheduler::GCWorkContext for LXRWeakRefWorkContext<VM> {
    type VM = VM;
    type PlanType = LXR<VM>;
    type ProcessEdgesWorkType = LXRWeakRefProcessEdges<VM>;
}

pub(super) struct LXREmergencyGCWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);

impl<VM: VMBinding> crate::scheduler::GCWorkContext for LXREmergencyGCWorkContext<VM> {
    type VM = VM;
    type PlanType = LXR<VM>;
    type ProcessEdgesWorkType = PlanProcessEdges<VM, LXR<VM>, TRACE_KIND_DEFAULT>;
}

pub(super) struct LXREmergencyWeakRefWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);

impl<VM: VMBinding> crate::scheduler::GCWorkContext for LXREmergencyWeakRefWorkContext<VM> {
    type VM = VM;
    type PlanType = LXR<VM>;
    type ProcessEdgesWorkType = LXREmergencyWeakRefProcessEdges<VM>;
}

pub struct FastRCPrepare;

impl<VM: VMBinding> GCWork<VM> for FastRCPrepare {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        let lxr = unsafe { &mut *(lxr as *const LXR<VM> as *mut LXR<VM>) };
        lxr.prepare(worker.tls)
    }
}

pub struct ReleaseLOSNursery;

impl<VM: VMBinding> GCWork<VM> for ReleaseLOSNursery {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        lxr.los().release_rc_nursery_objects();
        VM::VMCollection::update_weak_processor(true);
    }
}
