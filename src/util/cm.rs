use super::{Address, ObjectReference};
use crate::plan::immix::Pause;
use crate::policy::space::Space;
use crate::scheduler::gc_work::ScanObjects;
use crate::{
    plan::{
        immix::{Immix, ImmixCopyContext},
        EdgeIterator,
    },
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    util::metadata::side_metadata::address_to_meta_address,
    vm::*,
    TransitiveClosure, MMTK,
};
use atomic::Ordering;
use std::{
    marker::PhantomData,
    mem,
    ops::{Deref, DerefMut},
    ptr,
};

pub struct ImmixConcurrentTraceObjects<VM: VMBinding> {
    plan: &'static Immix<VM>,
    mmtk: &'static MMTK<VM>,
    objects: Vec<ObjectReference>,
    next_objects: Vec<ObjectReference>,
    worker: *mut GCWorker<VM>,
}

unsafe impl<VM: VMBinding> Send for ImmixConcurrentTraceObjects<VM> {}

impl<VM: VMBinding> ImmixConcurrentTraceObjects<VM> {
    const CAPACITY: usize = 512;

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
        if !self.next_objects.is_empty() {
            let mut new_nodes = vec![];
            mem::swap(&mut new_nodes, &mut self.next_objects);
            // This packet is executed in concurrent.
            debug_assert!(crate::args::CONCURRENT_MARKING);
            let w = ImmixConcurrentTraceObjects::<VM>::new(new_nodes, self.mmtk);
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
        }
    }

    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        let no_trace = crate::args::REF_COUNT
            && !crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING
            && crate::util::rc::count(object) == 0;
        if no_trace {
            return object;
        }
        if self.plan.immix_space.in_space(object) {
            self.plan.immix_space.fast_trace_object(self, object);
            object
        } else {
            self.plan
                .common
                .trace_object::<Self, ImmixCopyContext<VM>>(self, object)
        }
    }
}

impl<VM: VMBinding> TransitiveClosure for ImmixConcurrentTraceObjects<VM> {
    #[inline(always)]
    fn process_edge(&mut self, _: Address) {
        unreachable!()
    }

    #[inline]
    fn process_node(&mut self, object: ObjectReference) {
        if crate::args::MARK_LINE_AT_SCAN_TIME
            && !crate::args::BLOCK_ONLY
            && self.plan.immix_space.in_space(object)
        {
            self.plan.immix_space.mark_lines(object);
        }
        let mut should_add_to_mature_evac_remset = false;
        EdgeIterator::<VM>::iterate(object, |e| {
            let t = unsafe { e.load() };
            if crate::args::REF_COUNT
                && crate::args::RC_MATURE_EVACUATION
                && !should_add_to_mature_evac_remset
            {
                if !self.plan.in_defrag(object) && self.plan.in_defrag(t) {
                    should_add_to_mature_evac_remset = true;
                }
            }
            self.next_objects.push(t);
            if self.next_objects.len() >= Self::CAPACITY {
                self.flush();
            }
        });
        if should_add_to_mature_evac_remset {
            unsafe {
                self.worker()
                    .local::<ImmixCopyContext<VM>>()
                    .add_mature_evac_remset(object)
            }
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ImmixConcurrentTraceObjects<VM> {
    #[inline]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        if !crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING
            && crate::args::SLOW_CONCURRENT_MARKING
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

pub struct CMImmixCollectRootEdges<VM: VMBinding> {
    base: ProcessEdgesBase<Self>,
}

impl<VM: VMBinding> ProcessEdgesWork for CMImmixCollectRootEdges<VM> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = false;
    const RC_ROOTS: bool = true;
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
        debug_assert!(crate::args::CONCURRENT_MARKING);
        if !self.edges.is_empty() {
            let mut roots = vec![];
            for e in &self.edges {
                roots.push(unsafe { e.load() })
            }
            let w = ImmixConcurrentTraceObjects::<VM>::new(roots, self.mmtk());
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

pub struct ProcessModBufSATB<E: ProcessEdgesWork> {
    edges: Vec<Address>,
    nodes: Vec<ObjectReference>,
    phantom: PhantomData<E>,
}

impl<E: ProcessEdgesWork> ProcessModBufSATB<E> {
    pub fn new(edges: Vec<Address>, nodes: Vec<ObjectReference>) -> Self {
        Self {
            edges,
            nodes,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ProcessModBufSATB<E> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        if self.edges.is_empty() && self.nodes.is_empty() {
            return;
        }
        if !crate::args::REF_COUNT {
            for edge in &self.edges {
                let ptr = address_to_meta_address(
                    <E::VM as VMBinding>::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
                    *edge,
                );
                unsafe {
                    ptr.store(0b11111111u8);
                }
            }
        }
        if crate::concurrent_marking_in_progress() {
            GCWork::do_work(
                &mut ImmixConcurrentTraceObjects::<E::VM>::new(self.nodes.clone(), mmtk),
                worker,
                mmtk,
            );
        } else {
            let immix = mmtk.plan.downcast_ref::<Immix<E::VM>>().unwrap();
            if immix.current_pause() == Some(Pause::FinalMark) {
                let edges = self.nodes.iter().map(|e| Address::from_ptr(e)).collect();
                let mut w = FinalMarkProcessEdgesWithMatureEvac::<E::VM>::new(edges, false, mmtk);
                GCWork::do_work(&mut w, worker, mmtk);
            } else {
                let edges = self.nodes.iter().map(|e| Address::from_ptr(e)).collect();
                let mut w = E::new(edges, false, mmtk);
                GCWork::do_work(&mut w, worker, mmtk);
            }
        }
    }
}

pub struct FinalMarkProcessEdgesWithMatureEvac<VM: VMBinding> {
    plan: &'static Immix<VM>,
    base: ProcessEdgesBase<Self>,
}

impl<VM: VMBinding> FinalMarkProcessEdgesWithMatureEvac<VM> {
    const fn immix(&self) -> &'static Immix<VM> {
        self.plan
    }
}

impl<VM: VMBinding> ProcessEdgesWork for FinalMarkProcessEdgesWithMatureEvac<VM> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = crate::args::RC_MATURE_EVACUATION;

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let plan = base.plan().downcast_ref::<Immix<VM>>().unwrap();
        Self { plan, base }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.nodes.is_empty() {
            debug_assert_ne!(self.immix().current_pause(), Some(Pause::InitialMark));
            let scan_objects_work = ScanObjects::<Self>::new(self.pop_nodes(), false);
            self.new_scan_work(scan_objects_work);
        }
    }

    /// Trace  and evacuate objects.
    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        let x = if self.immix().immix_space.in_space(object) {
            if !crate::args::RC_MATURE_EVACUATION {
                self.immix().immix_space.fast_trace_object(self, object)
            } else {
                self.immix().immix_space.trace_object(
                    self,
                    object,
                    crate::plan::immix::ALLOC_IMMIX,
                    unsafe { self.worker().local::<ImmixCopyContext<VM>>() },
                )
            }
        } else {
            self.immix()
                .common
                .trace_object::<Self, ImmixCopyContext<VM>>(self, object)
        };
        x
    }

    #[inline]
    fn process_edges(&mut self) {
        debug_assert_eq!(self.plan.current_pause(), Some(Pause::FinalMark));
        for i in 0..self.edges.len() {
            ProcessEdgesWork::process_edge(self, self.edges[i])
        }
        if crate::args::REF_COUNT && !crate::plan::barriers::BARRIER_MEASUREMENT && self.roots {
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.edges);
            for x in &mut roots {
                unsafe { *x = x.load::<Address>() }
            }
            unsafe {
                crate::plan::immix::CURR_ROOTS.push(std::mem::transmute(roots));
            }
        }
        self.flush();
    }
}

impl<VM: VMBinding> Deref for FinalMarkProcessEdgesWithMatureEvac<VM> {
    type Target = ProcessEdgesBase<Self>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for FinalMarkProcessEdgesWithMatureEvac<VM> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
