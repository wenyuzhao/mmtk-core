use self::tracing::LXRWeakRefProcessEdges;
use super::global::LXR;
use crate::scheduler::gc_work::*;
use crate::vm::*;

pub mod mature_evac;
pub mod mature_sweeping;
pub mod nursery_sweeping;
pub mod prepare;
pub mod rc;
pub mod tracing;

pub struct LXRGCWorkContext<E: ProcessEdgesWork>(std::marker::PhantomData<E>);

impl<E: ProcessEdgesWork> crate::scheduler::GCWorkContext for LXRGCWorkContext<E> {
    type VM = E::VM;
    type PlanType = LXR<E::VM>;
    type DefaultProcessEdges = E;
    type PinningProcessEdges = UnsupportedProcessEdges<Self::VM>;
}

pub struct LXRWeakRefWorkContext<VM: VMBinding>(std::marker::PhantomData<VM>);

impl<VM: VMBinding> crate::scheduler::GCWorkContext for LXRWeakRefWorkContext<VM> {
    type VM = VM;
    type PlanType = LXR<VM>;
    type DefaultProcessEdges = LXRWeakRefProcessEdges<VM>;
    type PinningProcessEdges = UnsupportedProcessEdges<Self::VM>;
}
