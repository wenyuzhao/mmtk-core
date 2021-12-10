use super::global::Immix;
use crate::plan::immix::Pause;
use crate::plan::PlanConstraints;
use crate::policy::space::Space;
use crate::scheduler::{gc_work::*, GCWork, GCWorker};
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
use std::ops::{Deref, DerefMut};
use thread_priority::{self, ThreadPriority};

/// Immix copy allocator
pub struct ImmixCopyContext<VM: VMBinding> {
    immix: ImmixAllocator<VM>,
    mature_evac_remset: Vec<Address>,
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
        _bytes: usize,
        _semantics: crate::AllocationSemantics,
    ) {
        object_forwarding::clear_forwarding_bits::<VM>(obj);
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
            mature_evac_remset: vec![],
        }
    }

    pub fn flush_mature_evac_remset(&mut self) {
        if self.mature_evac_remset.len() > 0 {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.mature_evac_remset);
            let w = EvacuateMatureObjects::new(remset);
            self.immix
                .immix_space()
                .mature_evac_remsets
                .lock()
                .push(box w);
        }
    }

    pub fn flush_mature_evac_remset_to_scheduler(&mut self) {
        if self.mature_evac_remset.len() > 0 {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.mature_evac_remset);
            let w = EvacuateMatureObjects::new(remset);
            self.immix.immix_space().scheduler().work_buckets[WorkBucketStage::RCEvacuateMature]
                .add(w);
        }
    }

    #[inline(always)]
    pub fn add_mature_evac_remset(&mut self, e: Address) {
        self.mature_evac_remset.push(e);
        if self.mature_evac_remset.len() >= EvacuateMatureObjects::<VM>::CAPACITY {
            self.flush_mature_evac_remset()
        }
    }
}

impl<VM: VMBinding> GCWorkerLocal for ImmixCopyContext<VM> {
    fn init(&mut self, tls: VMWorkerThread) {
        CopyContext::init(self, tls);
    }
}

pub struct FlushMatureEvacRemsets;

impl<VM: VMBinding> GCWork<VM> for FlushMatureEvacRemsets {
    fn do_work(&mut self, worker: &mut crate::scheduler::GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        for w in &worker.scheduler().worker_group().workers {
            let cc = unsafe {
                let w = &mut *(w as *const GCWorker<VM> as *mut GCWorker<VM>);
                w.local::<ImmixCopyContext<VM>>()
            };
            cc.flush_mature_evac_remset_to_scheduler();
        }
        mmtk.plan
            .downcast_ref::<Immix<VM>>()
            .unwrap()
            .immix_space
            .process_mature_evacuation_remset();
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
}

impl<VM: VMBinding, const KIND: TraceKind> ImmixProcessEdges<VM, KIND> {
    fn immix(&self) -> &'static Immix<VM> {
        self.plan
    }

    #[inline(always)]
    fn fast_trace_object(&mut self, _slot: Address, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.immix().immix_space.in_space(object) {
            debug_assert!(
                self.plan.current_pause() == Some(Pause::FinalMark)
                    || self.plan.current_pause() == Some(Pause::FullTraceFast)
            );
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
        Self { plan, base }
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
    }

    /// Trace  and evacuate objects.
    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.immix().immix_space.in_space(object) {
            if KIND == TraceKind::Fast {
                self.immix().immix_space.fast_trace_object(self, object)
            } else {
                self.immix().immix_space.trace_object(
                    self,
                    object,
                    super::global::ALLOC_IMMIX,
                    unsafe { self.worker().local::<ImmixCopyContext<VM>>() },
                )
            }
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

pub(super) struct ImmixGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for ImmixGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = Immix<VM>;
    type CopyContextType = ImmixCopyContext<VM>;
    type ProcessEdgesWorkType = ImmixProcessEdges<VM, KIND>;
}
