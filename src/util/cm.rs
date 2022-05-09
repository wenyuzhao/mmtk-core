use super::copy::CopySemantics;
use super::{Address, ObjectReference};
use crate::plan::immix::Pause;
use crate::policy::immix::ImmixCopyContext;
use crate::policy::space::Space;
use crate::scheduler::gc_work::ScanObjects;
use crate::{
    plan::{immix::Immix, EdgeIterator},
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
    slice: Option<&'static [ObjectReference]>,
}

unsafe impl<VM: VMBinding> Send for ImmixConcurrentTraceObjects<VM> {}

impl<VM: VMBinding> ImmixConcurrentTraceObjects<VM> {
    const CAPACITY: usize = crate::args::BUFFER_SIZE;

    pub fn new(objects: Vec<ObjectReference>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            mmtk,
            objects,
            next_objects: vec![],
            worker: ptr::null_mut(),
            slice: None,
        }
    }

    pub fn new_slice(slice: &'static [ObjectReference], mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            mmtk,
            objects: vec![],
            next_objects: vec![],
            worker: ptr::null_mut(),
            slice: Some(slice),
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
        if object.is_null() || !object.is_in_any_space() {
            return object;
        }
        let no_trace = crate::args::REF_COUNT && crate::util::rc::count(object) == 0;
        if no_trace {
            return object;
        }
        if !object.class_is_valid() {
            return object;
        }
        if self.plan.immix_space.in_space(object) {
            self.plan.immix_space.fast_trace_object(self, object);
            object
        } else {
            self.plan.common.trace_object(self, object)
        }
    }

    fn process_objects(&mut self, objects: &[ObjectReference], slice: bool) {
        if slice {
            for o in objects {
                if !self.plan.address_in_defrag(Address::from_ref(o))
                    && self.plan.in_defrag(*o)
                    && crate::util::rc::count(*o) != 0
                {
                    self.plan
                        .immix_space
                        .remset
                        .record(Address::from_ref(o), &self.plan.immix_space);
                }
                self.trace_object(*o);
            }
        } else {
            for i in 0..objects.len() {
                self.trace_object(objects[i]);
            }
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
        if !crate::args::REF_COUNT
            && crate::args::MARK_LINE_AT_SCAN_TIME
            && !crate::args::BLOCK_ONLY
            && self.plan.immix_space.in_space(object)
        {
            self.plan.immix_space.mark_lines(object);
        }
        let should_check_remset = !self.plan.in_defrag(object);
        if crate::args::CM_LARGE_ARRAY_OPTIMIZATION
            && VM::VMScanning::is_obj_array(object)
            && VM::VMScanning::obj_array_data(object).len() > 1024
        {
            let data = VM::VMScanning::obj_array_data(object);
            let mut packets = vec![];
            for chunk in data.chunks(Self::CAPACITY) {
                packets.push((box Self::new_slice(chunk, self.mmtk)) as Box<dyn GCWork<VM>>);
            }
            self.worker().scheduler().work_buckets[WorkBucketStage::Unconstrained]
                .bulk_add(packets);
        } else {
            EdgeIterator::<VM>::iterate(object, |e| {
                let t: ObjectReference = unsafe { e.load() };
                if t.is_null() || crate::util::rc::count(t) == 0 {
                    return;
                }
                if crate::args::REF_COUNT
                    && crate::args::RC_MATURE_EVACUATION
                    && should_check_remset
                    && self.plan.in_defrag(t)
                    && crate::util::rc::count(t) != 0
                {
                    self.plan
                        .immix_space
                        .remset
                        .record(e, &self.plan.immix_space);
                }
                if self.next_objects.is_empty() {
                    self.next_objects.reserve(Self::CAPACITY);
                }
                self.next_objects.push(t);
                if self.next_objects.len() >= Self::CAPACITY {
                    self.flush();
                }
            });
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ImmixConcurrentTraceObjects<VM> {
    fn should_defer(&self) -> bool {
        crate::PAUSE_CONCURRENT_MARKING.load(Ordering::SeqCst)
    }
    #[inline(always)]
    fn should_move_to_stw(&self) -> Option<WorkBucketStage> {
        if crate::MOVE_CONCURRENT_MARKING_TO_STW.load(Ordering::SeqCst) {
            Some(WorkBucketStage::RCProcessIncs)
        } else {
            None
        }
    }
    #[inline]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        if let Some(slice) = self.slice {
            self.process_objects(slice, true)
        } else {
            let mut objects = vec![];
            std::mem::swap(&mut objects, &mut self.objects);
            self.process_objects(&objects, false)
        }
        let mut objects = vec![];
        while !self.next_objects.is_empty() {
            objects.clear();
            std::mem::swap(&mut objects, &mut self.next_objects);
            for i in 0..objects.len() {
                self.trace_object(objects[i]);
            }
        }
        // CM: Decrease counter
        self.flush();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct CMImmixCollectRootEdges<VM: VMBinding> {
    base: ProcessEdgesBase<VM>,
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
    type Target = ProcessEdgesBase<VM>;
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
        GCWork::do_work(
            &mut ImmixConcurrentTraceObjects::<E::VM>::new(self.nodes.clone(), mmtk),
            worker,
            mmtk,
        );
    }
}

pub struct LXRStopTheWorldProcessEdges<VM: VMBinding> {
    immix: &'static Immix<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
    forwarded_roots: Vec<ObjectReference>,
}

impl<VM: VMBinding> ProcessEdgesWork for LXRStopTheWorldProcessEdges<VM> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = crate::args::RC_MATURE_EVACUATION;

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let immix = base.plan().downcast_ref::<Immix<VM>>().unwrap();
        Self {
            immix,
            base,
            pause: Pause::RefCount,
            forwarded_roots: vec![],
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.nodes.is_empty() {
            let scan_objects_work = ScanObjects::<Self>::new(self.pop_nodes(), false);
            self.new_scan_work(scan_objects_work);
        }
    }

    /// Trace  and evacuate objects.
    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null()
            || !object.is_in_any_space()
            || !object.to_address().is_aligned_to(8)
            || !object.class_is_valid()
        {
            return object;
        }
        let x = if self.immix.immix_space.in_space(object) {
            self.immix.immix_space.rc_trace_object(
                self,
                object,
                CopySemantics::DefaultCopy,
                self.pause,
                false,
                self.worker(),
            )
        } else {
            object
        };
        if self.roots {
            self.forwarded_roots.push(x)
        }
        x
    }

    #[inline]
    fn process_edges(&mut self) {
        self.pause = self.immix.current_pause().unwrap();
        if self.roots {
            self.forwarded_roots.reserve(self.edges.len());
        }
        if self.pause == Pause::FullTraceFast {
            for i in 0..self.edges.len() {
                self.process_mark_edge(self.edges[i])
            }
        } else {
            for i in 0..self.edges.len() {
                ProcessEdgesWork::process_edge(self, self.edges[i])
            }
        }
        self.flush();
        if self.roots {
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.forwarded_roots);
            unsafe {
                crate::plan::immix::CURR_ROOTS.push(roots);
            }
        }
    }
}

impl<VM: VMBinding> LXRStopTheWorldProcessEdges<VM> {
    #[inline(always)]
    fn trace_and_mark_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null()
            || !object.is_in_any_space()
            || !object.to_address().is_aligned_to(8)
            || !object.class_is_valid()
        {
            return object;
        }
        let x = if self.immix.immix_space.in_space(object) {
            self.immix.immix_space.rc_trace_object(
                self,
                object,
                CopySemantics::DefaultCopy,
                self.pause,
                true,
                self.worker(),
            )
        } else {
            self.immix.los().trace_object(self, object)
        };
        if self.roots {
            self.forwarded_roots.push(x)
        }
        x
    }

    #[inline]
    fn process_mark_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        let new_object = self.trace_and_mark_object(object);
        if Self::OVERWRITE_REFERENCE {
            unsafe { slot.store(new_object) };
        }
    }
}

impl<VM: VMBinding> Deref for LXRStopTheWorldProcessEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for LXRStopTheWorldProcessEdges<VM> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
