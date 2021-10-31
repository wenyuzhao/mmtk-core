use super::global::Immix;
use crate::plan::immix::Pause;
use crate::plan::PlanConstraints;
use crate::policy::immix::block::Block;
use crate::policy::space::Space;
use crate::scheduler::gc_work::*;
use crate::scheduler::{GCWorkerLocal, WorkBucketStage};
use crate::util::alloc::{Allocator, ImmixAllocator};
use crate::util::object_forwarding;
use crate::util::rc::ProcessIncs;
use crate::util::{Address, ObjectReference};
use crate::vm::*;
use crate::MMTK;
use crate::{
    plan::CopyContext,
    util::opaque_pointer::{VMThread, VMWorkerThread},
};
use std::mem;
use std::ops::{Deref, DerefMut};
use thread_priority::{self, ThreadPriority};

/// Immix copy allocator
pub struct ImmixCopyContext<VM: VMBinding> {
    immix: ImmixAllocator<VM>,
}

impl<VM: VMBinding> CopyContext for ImmixCopyContext<VM> {
    type VM = VM;

    fn constraints(&self) -> &'static PlanConstraints {
        &super::global::IMMIX_CONSTRAINTS
    }
    fn init(&mut self, tls: VMWorkerThread) {
        self.immix.tls = tls.0;
    }
    fn prepare(&mut self) {
        if *crate::args::LOWER_CONCURRENT_GC_THREAD_PRIORITY {
            let _ = thread_priority::set_current_thread_priority(ThreadPriority::Max);
        }
        self.immix.reset()
    }
    fn release(&mut self) {
        self.immix.reset();
        if *crate::args::LOWER_CONCURRENT_GC_THREAD_PRIORITY {
            let _ = thread_priority::set_current_thread_priority(ThreadPriority::Min);
        }
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
            if crate::args::REF_COUNT {
                crate::util::rc::assert_zero_ref_count::<VM>(obj);
                for i in (0..bytes).step_by(8) {
                    debug_assert!((obj.to_address() + i).is_logged::<VM>());
                }
            }
        }
        if crate::args::REF_COUNT {
            debug_assert_eq!(crate::util::rc::count(obj), 0);
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

pub struct ImmixProcessEdges<VM: VMBinding, const KIND: TraceKind> {
    // Use a static ref to the specific plan to avoid overhead from dynamic dispatch or
    // downcast for each traced object.
    plan: &'static Immix<VM>,
    base: ProcessEdgesBase<Self>,
    mature_evac_remset_roots: Vec<Address>,
}

impl<VM: VMBinding, const KIND: TraceKind> ImmixProcessEdges<VM, KIND> {
    fn immix(&self) -> &'static Immix<VM> {
        self.plan
    }

    #[inline(always)]
    fn fast_trace_object(&mut self, slot: Address, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.immix().immix_space.in_space(object) {
            debug_assert!(
                self.plan.current_pause() == Some(Pause::FinalMark)
                    || self.plan.current_pause() == Some(Pause::FullTraceFast)
            );
            if crate::args::REF_COUNT
                && crate::args::RC_MATURE_EVACUATION
                && self.roots
                && Block::in_defrag_block::<VM>(object)
            {
                self.mature_evac_remset_roots.push(slot);
                if self.mature_evac_remset_roots.len() >= Self::CAPACITY {
                    self.flush();
                }
            }
            self.immix().immix_space.fast_trace_object(self, object);
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

impl<VM: VMBinding, const KIND: TraceKind> ProcessEdgesWork for ImmixProcessEdges<VM, KIND> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = crate::policy::immix::DEFRAG
        && if let TraceKind::Defrag = KIND {
            true
        } else {
            false
        };

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let plan = base.plan().downcast_ref::<Immix<VM>>().unwrap();
        Self {
            plan,
            base,
            mature_evac_remset_roots: vec![],
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.nodes.is_empty() {
            debug_assert_ne!(self.immix().current_pause(), Some(Pause::InitialMark));
            let scan_objects_work = crate::policy::immix::ScanObjectsAndMarkLines::<Self>::new(
                self.pop_nodes(),
                false,
                Some(self.immix()),
                &self.immix().immix_space,
            );
            self.new_scan_work(scan_objects_work);
        }
        if !self.mature_evac_remset_roots.is_empty() {
            let mut roots_remset = vec![];
            mem::swap(&mut roots_remset, &mut self.mature_evac_remset_roots);
            let w = EvacuateMatureObjects::new_roots(roots_remset);
            self.mmtk().scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].add(w);
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
                ProcessEdgesWork::process_edge(self, self.edges[i])
            }
        }
        if super::REF_COUNT && !crate::plan::barriers::BARRIER_MEASUREMENT && self.roots {
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.edges);
            let bucket = WorkBucketStage::rc_process_incs_stage();
            self.mmtk().scheduler.work_buckets[bucket].add(ProcessIncs::new(roots, true));
        }
        self.flush();
    }
}

impl<VM: VMBinding, const KIND: TraceKind> Deref for ImmixProcessEdges<VM, KIND> {
    type Target = ProcessEdgesBase<Self>;
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
