use atomic::Ordering;

use super::global::Immix;
use crate::plan::immix::Pause;
use crate::plan::PlanConstraints;
use crate::policy::immix::line::Line;
use crate::policy::immix::ScanObjectsAndMarkLines;
use crate::policy::space::Space;
use crate::scheduler::gc_work::*;
use crate::scheduler::{GCWorkerLocal, WorkBucketStage};
use crate::util::alloc::{Allocator, ImmixAllocator};
use crate::util::object_forwarding;
use crate::util::{Address, ObjectReference};
use crate::vm::*;
use crate::MMTK;
use crate::{
    plan::CopyContext,
    util::opaque_pointer::{VMThread, VMWorkerThread},
};
use std::{
    mem,
    ops::{Deref, DerefMut},
};

/// Immix copy allocator
pub struct ImmixCopyContext<VM: VMBinding> {
    immix: ImmixAllocator<VM>,
}

impl<VM: VMBinding> CopyContext for ImmixCopyContext<VM> {
    type VM = VM;

    fn constraints(&self) -> &'static PlanConstraints {
        super::global::get_immix_constraints()
    }
    fn init(&mut self, tls: VMWorkerThread) {
        self.immix.tls = tls.0;
    }
    fn prepare(&mut self) {
        self.immix.reset()
    }
    fn release(&mut self) {
        self.immix.reset()
    }
    #[inline(always)]
    fn alloc_copy(
        &mut self,
        _original: ObjectReference,
        bytes: usize,
        align: usize,
        offset: isize,
        _semantics: crate::AllocationSemantics,
    ) -> Address {
        self.immix.alloc(bytes, align, offset)
    }
    #[inline(always)]
    fn post_copy(
        &mut self,
        obj: ObjectReference,
        _tib: Address,
        bytes: usize,
        _semantics: crate::AllocationSemantics,
    ) {
        object_forwarding::clear_forwarding_bits::<VM>(obj);
        if cfg!(debug_assertions) {
            if crate::flags::REF_COUNT {
                crate::policy::immix::rc::assert_zero_ref_count::<VM>(obj);
                for i in (0..bytes).step_by(8) {
                    debug_assert!(unsafe {
                        (obj.to_address() + i)
                            .to_object_reference()
                            .is_logged::<VM>()
                    });
                }
            }
        }
        if crate::flags::REF_COUNT && !crate::flags::BLOCK_ONLY {
            if bytes > Line::BYTES {
                crate::policy::immix::rc::mark_striddle_object::<VM>(obj);
            }
        }
        if crate::flags::REF_COUNT {
            debug_assert_eq!(crate::policy::immix::rc::count(obj), 0);
        }
    }
}

impl<VM: VMBinding> ImmixCopyContext<VM> {
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        Self {
            immix: ImmixAllocator::new(
                VMThread::UNINITIALIZED,
                Some(&mmtk.plan.downcast_ref::<Immix<VM>>().unwrap().immix_space),
                &*mmtk.plan,
                true,
            ),
        }
    }
}

impl<VM: VMBinding> GCWorkerLocal for ImmixCopyContext<VM> {
    fn init(&mut self, tls: VMWorkerThread) {
        CopyContext::init(self, tls);
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum TraceKind {
    Fast,
    Defrag,
}

pub struct ImmixProcessEdges<VM: VMBinding, const KIND: TraceKind, const CONCURRENT: bool> {
    // Use a static ref to the specific plan to avoid overhead from dynamic dispatch or
    // downcast for each traced object.
    plan: &'static Immix<VM>,
    base: ProcessEdgesBase<Self>,
    mmtk: &'static MMTK<VM>,
    roots: bool,
    root_slots: Vec<Address>,
}

impl<VM: VMBinding, const KIND: TraceKind, const CONCURRENT: bool>
    ImmixProcessEdges<VM, KIND, CONCURRENT>
{
    fn immix(&self) -> &'static Immix<VM> {
        self.plan
    }

    #[inline(always)]
    fn fast_trace_object(&mut self, slot: Address, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.immix().immix_space.in_space(object) {
            self.immix().immix_space.fast_trace_object(self, object);
            if super::REF_COUNT && !crate::plan::barriers::BARRIER_MEASUREMENT {
                if self.roots {
                    self.root_slots.push(slot);
                }
            }
            object
        } else {
            self.immix()
                .common
                .trace_object::<Self, ImmixCopyContext<VM>>(self, object)
        }
    }

    /// Trace objects without evacuation.
    #[inline(always)]
    fn fast_process_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        self.fast_trace_object(slot, object);
    }
}

impl<VM: VMBinding, const KIND: TraceKind, const CONCURRENT: bool> ProcessEdgesWork
    for ImmixProcessEdges<VM, KIND, CONCURRENT>
{
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = crate::policy::immix::DEFRAG
        && if let TraceKind::Defrag = KIND {
            true
        } else {
            false
        };

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, mmtk);
        let plan = base.plan().downcast_ref::<Immix<VM>>().unwrap();
        if CONCURRENT {
            crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        }
        Self {
            plan,
            base,
            mmtk,
            roots,
            root_slots: vec![],
        }
    }

    #[cold]
    fn flush(&mut self) {
        if self.nodes.is_empty() {
            return;
        }
        let mut new_nodes = vec![];
        mem::swap(&mut new_nodes, &mut self.nodes);
        if CONCURRENT {
            // This packet is executed in concurrent.
            debug_assert!(crate::flags::CONCURRENT_MARKING);
            let w =
                ScanObjectsAndMarkLines::<Self>::new(new_nodes, true, &self.immix().immix_space);
            if Self::SCAN_OBJECTS_IMMEDIATELY {
                self.worker().do_work(w);
            } else {
                self.worker().add_work(WorkBucketStage::Unconstrained, w);
            }
        } else {
            // This packet is executed within a GC pause.
            // Further generated packets can either be postponed or run in the same pause.
            if self.immix().current_pause() == Some(Pause::InitialMark) {
                debug_assert!(crate::flags::CONCURRENT_MARKING);
                let w = ScanObjectsAndMarkLines::<ImmixProcessEdges<VM, KIND, true>>::new(
                    new_nodes,
                    true,
                    &self.immix().immix_space,
                );
                self.mmtk().scheduler.postpone(w);
            } else {
                let w = ScanObjectsAndMarkLines::<Self>::new(
                    new_nodes,
                    false,
                    &self.immix().immix_space,
                );
                if Self::SCAN_OBJECTS_IMMEDIATELY {
                    self.worker().do_work(w);
                } else {
                    self.worker().add_work(WorkBucketStage::Closure, w);
                }
            }
        }
    }

    /// Trace  and evacuate objects.
    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.immix().immix_space.in_space(object) {
            self.immix().immix_space.trace_object(
                self,
                object,
                super::global::ALLOC_IMMIX,
                unsafe { self.worker().local::<ImmixCopyContext<VM>>() },
            )
        } else {
            self.immix()
                .common
                .trace_object::<Self, ImmixCopyContext<VM>>(self, object)
        }
    }

    #[inline]
    fn process_edges(&mut self) {
        if KIND == TraceKind::Fast {
            for i in 0..self.edges.len() {
                // Use fast_process_edge since we don't need to forward any objects.
                self.fast_process_edge(self.edges[i])
            }
        } else {
            for i in 0..self.edges.len() {
                self.process_edge(self.edges[i])
            }
        }
        if self.roots && !self.root_slots.is_empty() {
            let bucket = WorkBucketStage::rc_process_incs_stage();
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.root_slots);
            self.mmtk.scheduler.work_buckets[bucket].add(ProcessIncs::new_roots(roots));
        }
    }
}

impl<VM: VMBinding, const KIND: TraceKind, const CONCURRENT: bool> Deref
    for ImmixProcessEdges<VM, KIND, CONCURRENT>
{
    type Target = ProcessEdgesBase<Self>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, const KIND: TraceKind, const CONCURRENT: bool> DerefMut
    for ImmixProcessEdges<VM, KIND, CONCURRENT>
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl<VM: VMBinding, const KIND: TraceKind, const CONCURRENT: bool> Drop
    for ImmixProcessEdges<VM, KIND, CONCURRENT>
{
    fn drop(&mut self) {
        if CONCURRENT {
            crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

pub struct RCImmixProcessEdges<VM: VMBinding, const KIND: TraceKind> {
    // Use a static ref to the specific plan to avoid overhead from dynamic dispatch or
    // downcast for each traced object.
    plan: &'static Immix<VM>,
    base: ProcessEdgesBase<Self>,
    mmtk: &'static MMTK<VM>,
    roots: Vec<Address>,
}

impl<VM: VMBinding, const KIND: TraceKind> RCImmixProcessEdges<VM, KIND> {
    fn immix(&self) -> &'static Immix<VM> {
        self.plan
    }

    #[inline(always)]
    fn trace_object(&mut self, slot: Address, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.immix().immix_space.in_space(object) {
            self.roots.push(slot);
        }
        object
    }
}

impl<VM: VMBinding, const KIND: TraceKind> ProcessEdgesWork for RCImmixProcessEdges<VM, KIND> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = crate::policy::immix::DEFRAG
        && if let TraceKind::Defrag = KIND {
            true
        } else {
            false
        };
    const RC_ROOTS: bool = true;

    fn new(edges: Vec<Address>, _roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, mmtk);
        let plan = base.plan().downcast_ref::<Immix<VM>>().unwrap();
        Self {
            plan,
            base,
            mmtk,
            roots: vec![],
        }
    }

    /// Trace  and evacuate objects.
    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    #[inline(always)]
    fn process_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        let new_object = self.trace_object(slot, object);
        if Self::OVERWRITE_REFERENCE {
            unsafe { slot.store(new_object) };
        }
    }

    #[inline]
    fn process_edges(&mut self) {
        for i in 0..self.edges.len() {
            self.process_edge(self.edges[i])
        }
        if !self.roots.is_empty() {
            let bucket = WorkBucketStage::rc_process_incs_stage();
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.roots);
            self.mmtk.scheduler.work_buckets[bucket].add(ProcessIncs::new_roots(roots));
        }
    }

    const CAPACITY: usize = 4096;

    const SCAN_OBJECTS_IMMEDIATELY: bool = true;
}

impl<VM: VMBinding, const KIND: TraceKind> Deref for RCImmixProcessEdges<VM, KIND> {
    type Target = ProcessEdgesBase<Self>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, const KIND: TraceKind> DerefMut for RCImmixProcessEdges<VM, KIND> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
