use super::rc::{ProcessIncs, EDGE_KIND_ROOT};
use super::LXR;
use crate::plan::immix::Pause;
use crate::policy::space::Space;
use crate::scheduler::{gc_work::*, WorkBucketStage};
use crate::util::copy::CopySemantics;
use crate::util::{Address, ObjectReference};
use crate::vm::*;
use crate::MMTK;
use std::ops::{Deref, DerefMut};

// It would be better if we use an enum for this. However, we use this as
// a constant type parameter, and Rust only accepts integer and bool for
// constant type parameters for now. We need to wait until `adt_const_params` is
// stablized.
pub(in crate::plan) type TraceKind = u8;
pub(in crate::plan) const TRACE_KIND_FAST: TraceKind = 0;
pub(in crate::plan) const TRACE_KIND_DEFRAG: TraceKind = 1;

/// Object tracing for Immix.
/// Note that it is possible to use [`SFTProcessEdges`](mmtk/scheduler/gc_work/SFTProcessEdges) for immix.
/// We need to: 1. add a plan-specific method to create scan work packets, as most plans use `ScanObjects` while
/// immix uses `ScanObjectsAndMarkLines`, 2. use `ImmixSpace.trace_object()` which has an overhead of checking
/// which trace method to use (with ImmixProcessEdges, we can know which trace method to use by statically checking `TraceKind`).
pub struct ImmixProcessEdges<VM: VMBinding, const KIND: TraceKind> {
    // Use a static ref to the specific plan to avoid overhead from dynamic dispatch or
    // downcast for each traced object.
    plan: &'static LXR<VM>,
    base: ProcessEdgesBase<VM>,
}

impl<VM: VMBinding, const KIND: TraceKind> ImmixProcessEdges<VM, KIND> {
    fn lxr(&self) -> &'static LXR<VM> {
        self.plan
    }

    #[inline(always)]
    fn fast_trace_object(&mut self, _slot: Address, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.lxr().immix_space.in_space(object) {
            debug_assert!(
                self.plan.current_pause() == Some(Pause::FinalMark)
                    || self.plan.current_pause() == Some(Pause::FullTraceFast)
            );
            self.lxr().immix_space.fast_trace_object(self, object);
            object
        } else {
            self.lxr().common.trace_object(self, object)
        }
    }

    /// Trace objects without evacuation.
    #[inline(always)]
    fn fast_process_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        self.fast_trace_object(slot, object);
    }
}

impl<VM: VMBinding, const KIND: TraceKind> ProcessEdgesWork for ImmixProcessEdges<VM, KIND> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = crate::policy::immix::DEFRAG
        && if KIND == TRACE_KIND_DEFRAG {
            true
        } else {
            false
        };

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let plan = base.plan().downcast_ref::<LXR<VM>>().unwrap();
        Self { plan, base }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.nodes.is_empty() {
            debug_assert_ne!(self.lxr().current_pause(), Some(Pause::InitialMark));
            let scan_objects_work = crate::policy::immix::ScanObjectsAndMarkLines::<Self>::new(
                self.pop_nodes(),
                false,
                &self.lxr().immix_space,
            );
            self.start_or_dispatch_scan_work(Box::new(scan_objects_work));
        }
    }

    /// Trace  and evacuate objects.
    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.lxr().immix_space.in_space(object) {
            if KIND == TRACE_KIND_FAST {
                self.lxr().immix_space.fast_trace_object(self, object)
            } else {
                self.lxr().immix_space.trace_object(
                    self,
                    object,
                    CopySemantics::DefaultCopy,
                    self.worker(),
                )
            }
        } else {
            self.lxr().common.trace_object::<Self>(self, object)
        }
    }

    #[inline]
    fn process_edges(&mut self) {
        if KIND == TRACE_KIND_FAST {
            for i in 0..self.edges.len() {
                // Use fast_process_edge since we don't need to forward any objects.
                self.fast_process_edge(self.edges[i])
            }
        } else {
            for i in 0..self.edges.len() {
                ProcessEdgesWork::process_edge(self, self.edges[i])
            }
        }
        if crate::args::REF_COUNT && !crate::plan::barriers::BARRIER_MEASUREMENT && self.roots {
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.edges);
            let bucket = WorkBucketStage::rc_process_incs_stage();
            self.mmtk().scheduler.work_buckets[bucket]
                .add(ProcessIncs::<_, { EDGE_KIND_ROOT }>::new(roots));
        }
        self.flush();
    }
}

impl<VM: VMBinding, const KIND: TraceKind> Deref for ImmixProcessEdges<VM, KIND> {
    type Target = ProcessEdgesBase<VM>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, const KIND: TraceKind> DerefMut for ImmixProcessEdges<VM, KIND> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub(super) struct ImmixGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for ImmixGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = LXR<VM>;
    type ProcessEdgesWorkType = PlanProcessEdges<VM, LXR<VM>, KIND>;
}
