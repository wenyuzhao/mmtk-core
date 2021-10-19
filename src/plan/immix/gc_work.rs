use atomic::Ordering;

use super::global::Immix;
use crate::plan::immix::Pause;
use crate::plan::{EdgeIterator, PlanConstraints};
use crate::policy::space::Space;
use crate::scheduler::{gc_work::*, GCWork, GCWorker};
use crate::scheduler::{GCWorkerLocal, WorkBucketStage};
use crate::util::alloc::{Allocator, ImmixAllocator};
use crate::util::object_forwarding;
use crate::util::rc::ProcessIncs;
use crate::util::{Address, ObjectReference};
use crate::MMTK;
use crate::{
    plan::CopyContext,
    util::opaque_pointer::{VMThread, VMWorkerThread},
};
use crate::{vm::*, TransitiveClosure};
use std::ptr;
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
                crate::util::rc::assert_zero_ref_count::<VM>(obj);
                for i in (0..bytes).step_by(8) {
                    debug_assert!((obj.to_address() + i).is_logged::<VM>());
                }
            }
        }
        if crate::flags::REF_COUNT {
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
    roots: bool,
    root_slots: Vec<Address>,
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
            roots,
            root_slots: vec![],
        }
    }

    #[cold]
    fn flush(&mut self) {
        if self.nodes.is_empty() {
            return;
        }
        debug_assert_ne!(self.immix().current_pause(), Some(Pause::InitialMark));
        let scan_objects_work = crate::policy::immix::ScanObjectsAndMarkLines::<Self>::new(
            self.pop_nodes(),
            false,
            &self.immix().immix_space,
        );
        self.new_scan_work(scan_objects_work);
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
        if self.roots && !self.root_slots.is_empty() {
            debug_assert!(crate::flags::REF_COUNT);
            let bucket = WorkBucketStage::rc_process_incs_stage();
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.root_slots);
            self.mmtk().scheduler.work_buckets[bucket].add(ProcessIncs::new(roots, true));
        }
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

pub struct ImmixConcurrentTraceObject<VM: VMBinding> {
    plan: &'static Immix<VM>,
    mmtk: &'static MMTK<VM>,
    objects: Vec<ObjectReference>,
    next_objects: Vec<ObjectReference>,
    worker: *mut GCWorker<VM>,
}

unsafe impl<VM: VMBinding> Send for ImmixConcurrentTraceObject<VM> {}

impl<VM: VMBinding> ImmixConcurrentTraceObject<VM> {
    const CAPACITY: usize = 4096;

    pub fn new(objects: Vec<ObjectReference>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            mmtk,
            objects,
            next_objects: vec![],
            worker: ptr::null_mut(),
        }
    }

    #[inline(always)]
    fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[cold]
    fn flush(&mut self) {
        if self.next_objects.is_empty() {
            return;
        }
        let mut new_nodes = vec![];
        mem::swap(&mut new_nodes, &mut self.next_objects);
        // This packet is executed in concurrent.
        debug_assert!(crate::flags::CONCURRENT_MARKING);
        let w = ImmixConcurrentTraceObject::<VM>::new(new_nodes, self.mmtk);
        self.worker().add_work(WorkBucketStage::Unconstrained, w);
    }

    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.plan.immix_space.in_space(object) {
            let no_trace = crate::flags::REF_COUNT
                && !crate::flags::NO_RC_PAUSES_DURING_CONCURRENT_MARKING
                && crate::util::rc::count(object) == 0;
            if !no_trace {
                self.plan.immix_space.fast_trace_object(self, object);
            }
            object
        } else {
            self.plan
                .common
                .trace_object::<Self, ImmixCopyContext<VM>>(self, object)
        }
    }
}

impl<VM: VMBinding> TransitiveClosure for ImmixConcurrentTraceObject<VM> {
    #[inline(always)]
    fn process_edge(&mut self, slot: Address) {
        self.next_objects.push(unsafe { slot.load() });
        if self.next_objects.len() >= Self::CAPACITY {
            self.flush();
        }
    }

    #[inline]
    fn process_node(&mut self, object: ObjectReference) {
        if crate::flags::MARK_LINE_AT_SCAN_TIME
            && !crate::flags::BLOCK_ONLY
            && self.plan.immix_space.in_space(object)
        {
            self.plan.immix_space.mark_lines(object);
        }
        EdgeIterator::<VM>::iterate(object, |e| {
            // println!("Trace {:?}.{:?} -> {:?}", object, e, unsafe { e.load::<Address>() });
            self.process_edge(e)
        })
    }
}

impl<VM: VMBinding> GCWork<VM> for ImmixConcurrentTraceObject<VM> {
    #[inline]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        if !crate::flags::NO_RC_PAUSES_DURING_CONCURRENT_MARKING
            && crate::flags::SLOW_CONCURRENT_MARKING
        {
            if mmtk
                .plan
                .downcast_ref::<Immix<VM>>()
                .unwrap()
                .current_pause()
                .is_none()
            {
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
        }
        for i in 0..self.objects.len() {
            self.trace_object(self.objects[i]);
        }
        // CM: Decrease counter
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
        self.flush();
    }
}

pub struct RCImmixCollectRootEdges<VM: VMBinding> {
    base: ProcessEdgesBase<Self>,
}

impl<VM: VMBinding> ProcessEdgesWork for RCImmixCollectRootEdges<VM> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = false;
    const RC_ROOTS: bool = true;
    const CAPACITY: usize = 4096;
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        debug_assert!(roots);
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        Self { base }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    #[inline]
    fn process_edges(&mut self) {
        if !self.edges.is_empty() {
            let bucket = WorkBucketStage::rc_process_incs_stage();
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.edges);
            self.mmtk().scheduler.work_buckets[bucket].add(ProcessIncs::<VM>::new(roots, true));
        }
    }
}

impl<VM: VMBinding> Deref for RCImmixCollectRootEdges<VM> {
    type Target = ProcessEdgesBase<Self>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for RCImmixCollectRootEdges<VM> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct CMImmixCollectRootEdges<VM: VMBinding> {
    base: ProcessEdgesBase<Self>,
}

impl<VM: VMBinding> ProcessEdgesWork for CMImmixCollectRootEdges<VM> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = false;
    const RC_ROOTS: bool = true;
    const CAPACITY: usize = 4096;
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        debug_assert!(roots);
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        Self { base }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    #[inline]
    fn process_edges(&mut self) {
        debug_assert!(crate::flags::CONCURRENT_MARKING);
        if !self.edges.is_empty() {
            let mut roots = vec![];
            for e in &self.edges {
                roots.push(unsafe { e.load() })
            }
            let w = ImmixConcurrentTraceObject::<VM>::new(roots, self.mmtk());
            self.mmtk().scheduler.postpone(w);
        }
    }
}

impl<VM: VMBinding> Deref for CMImmixCollectRootEdges<VM> {
    type Target = ProcessEdgesBase<Self>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for CMImmixCollectRootEdges<VM> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
