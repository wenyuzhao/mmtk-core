use super::{metadata::MetadataSpec, Address, ObjectReference};
use crate::policy::space::Space;
use crate::scheduler::gc_work::EvacuateMatureObjects;
use crate::{
    plan::{
        immix::{Immix, ImmixCopyContext},
        EdgeIterator,
    },
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    util::metadata::side_metadata::address_to_meta_address,
    vm::VMBinding,
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
    remset: Vec<ObjectReference>,
}

unsafe impl<VM: VMBinding> Send for ImmixConcurrentTraceObjects<VM> {}

impl<VM: VMBinding> ImmixConcurrentTraceObjects<VM> {
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
            remset: vec![],
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
            debug_assert!(crate::flags::CONCURRENT_MARKING);
            let w = ImmixConcurrentTraceObjects::<VM>::new(new_nodes, self.mmtk);
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
        }
        if !self.remset.is_empty() {
            let mut remset = vec![];
            mem::swap(&mut remset, &mut self.remset);
            let w = EvacuateMatureObjects::new(remset);
            self.plan.immix_space.remsets.lock().push(w);
        }
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

    fn add_remset(&mut self, src: ObjectReference) {
        self.remset.push(src);
        if self.remset.len() >= Self::CAPACITY {
            self.flush();
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
        if crate::flags::MARK_LINE_AT_SCAN_TIME
            && !crate::flags::BLOCK_ONLY
            && self.plan.immix_space.in_space(object)
        {
            self.plan.immix_space.mark_lines(object);
        }
        // println!("Mark {:?}", object.to_address());
        self.add_remset(object);
        EdgeIterator::<VM>::iterate(object, |e| {
            let t = unsafe { e.load() };
            // println!("Trace {:?}.{:?} -> {:?}", object, e, unsafe { e.load::<Address>() });
            self.next_objects.push(t);
            if self.next_objects.len() >= Self::CAPACITY {
                self.flush();
            }
        })
    }
}

impl<VM: VMBinding> GCWork<VM> for ImmixConcurrentTraceObjects<VM> {
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
    meta: MetadataSpec,
}

impl<E: ProcessEdgesWork> ProcessModBufSATB<E> {
    pub fn new(edges: Vec<Address>, nodes: Vec<ObjectReference>, meta: MetadataSpec) -> Self {
        Self {
            edges,
            nodes,
            meta,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ProcessModBufSATB<E> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        debug_assert!(!crate::flags::BARRIER_MEASUREMENT);
        if !self.edges.is_empty() {
            if !crate::flags::REF_COUNT {
                for edge in &self.edges {
                    let ptr = address_to_meta_address(self.meta.extract_side_spec(), *edge);
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
                let edges = self.nodes.iter().map(|e| Address::from_ptr(e)).collect();
                let mut w = E::new(edges, false, mmtk);
                GCWork::do_work(&mut w, worker, mmtk);
            }
        }
    }
}
