use super::cm::LXRWeakRefProcessEdges;
use super::LXR;
use crate::scheduler::gc_work::*;
use crate::vm::*;

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
