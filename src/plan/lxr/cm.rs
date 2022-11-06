use crate::plan::immix::Pause;
use crate::plan::VectorQueue;
use crate::policy::space::Space;
use crate::scheduler::gc_work::{EdgeOf, ScanObjects};
use crate::util::copy::CopySemantics;
use crate::util::{Address, ObjectReference};
use crate::vm::edge_shape::Edge;
use crate::{
    plan::{EdgeIterator, ObjectQueue},
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    vm::*,
    MMTK,
};
use atomic::Ordering;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr,
};

use super::LXR;

pub struct LXRConcurrentTraceObjects<VM: VMBinding> {
    plan: &'static LXR<VM>,
    mmtk: &'static MMTK<VM>,
    objects: Vec<ObjectReference>,
    next_objects: VectorQueue<ObjectReference>,
    worker: *mut GCWorker<VM>,
    slice: Option<&'static [ObjectReference]>,
    discovery: bool,
}

unsafe impl<VM: VMBinding> Send for LXRConcurrentTraceObjects<VM> {}

impl<VM: VMBinding> LXRConcurrentTraceObjects<VM> {
    const CAPACITY: usize = crate::args::BUFFER_SIZE;

    pub fn new(objects: Vec<ObjectReference>, mmtk: &'static MMTK<VM>, discovery: bool) -> Self {
        let plan = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            mmtk,
            objects,
            next_objects: VectorQueue::default(),
            worker: ptr::null_mut(),
            slice: None,
            discovery,
        }
    }

    pub fn new_slice(
        slice: &'static [ObjectReference],
        mmtk: &'static MMTK<VM>,
        discovery: bool,
    ) -> Self {
        let plan = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            mmtk,
            objects: vec![],
            next_objects: VectorQueue::default(),
            worker: ptr::null_mut(),
            slice: Some(slice),
            discovery,
        }
    }

    #[inline(always)]
    fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_objects.is_empty() {
            let new_nodes = self.next_objects.take();
            // This packet is executed in concurrent.
            debug_assert!(self.plan.concurrent_marking_enabled());
            let w = LXRConcurrentTraceObjects::<VM>::new(new_nodes, self.mmtk, self.discovery);
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
        }
    }

    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() || !object.is_in_any_space() {
            return object;
        }
        let no_trace = crate::util::rc::count(object) == 0;
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
            let worker = self.worker();
            let queue = unsafe { &mut *(self as *const Self as *mut Self) };
            self.plan.common.trace_object(queue, object, worker)
        }
    }

    // FIXME: Compressed pointers
    fn process_objects(&mut self, objects: &[ObjectReference], slice: bool) {
        if slice {
            for o in objects {
                if !self.plan.address_in_defrag(Address::from_ref(o))
                    && self.plan.in_defrag(*o)
                    && crate::util::rc::count(*o) != 0
                {
                    self.plan.immix_space.remset.record(
                        VM::VMEdge::from_address(Address::from_ref(o)),
                        &self.plan.immix_space,
                    );
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

impl<VM: VMBinding> ObjectQueue for LXRConcurrentTraceObjects<VM> {
    #[inline]
    fn enqueue(&mut self, object: ObjectReference) {
        let should_check_remset = !self.plan.in_defrag(object);
        if crate::args::CM_LARGE_ARRAY_OPTIMIZATION
            && VM::VMScanning::is_obj_array(object)
            && VM::VMScanning::obj_array_data(object).len() > 1024
        {
            let data = VM::VMScanning::obj_array_data(object);
            let mut packets = vec![];
            for chunk in data.chunks(Self::CAPACITY) {
                packets.push(
                    (Box::new(Self::new_slice(chunk, self.mmtk, self.discovery)))
                        as Box<dyn GCWork<VM>>,
                );
            }
            self.worker().scheduler().work_buckets[WorkBucketStage::Unconstrained]
                .bulk_add(packets);
        } else {
            EdgeIterator::<VM>::iterate(object, self.discovery, |e: VM::VMEdge| {
                let t: ObjectReference = e.load();

                // println!("Trace {:?}.{:?} -> {:?}", object, e, t);
                if t.is_null() || crate::util::rc::count(t) == 0 {
                    return;
                }
                if crate::args::RC_MATURE_EVACUATION
                    && should_check_remset
                    && self.plan.in_defrag(t)
                {
                    self.plan
                        .immix_space
                        .remset
                        .record(e, &self.plan.immix_space);
                }
                self.next_objects.push(t);
                if self.next_objects.is_full() {
                    self.flush();
                }
            });
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for LXRConcurrentTraceObjects<VM> {
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
    #[inline(always)]
    fn is_concurrent_marking_work(&self) -> bool {
        true
    }
    #[inline]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        if let Some(slice) = self.slice {
            self.process_objects(slice, true)
        } else {
            let objects = std::mem::take(&mut self.objects);
            self.process_objects(&objects, false)
        }
        let mut objects = vec![];
        while !self.next_objects.is_empty() {
            objects.clear();
            self.next_objects.swap(&mut objects);
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
    type ScanObjectsWorkType = ScanObjects<Self>;

    const OVERWRITE_REFERENCE: bool = false;
    const RC_ROOTS: bool = true;
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;

    fn new(edges: Vec<EdgeOf<Self>>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        Self { base }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    #[inline]
    fn process_edges(&mut self) {
        if !self.edges.is_empty() {
            let mut roots = vec![];
            for e in &self.edges {
                roots.push(e.load())
            }
            let w = LXRConcurrentTraceObjects::<VM>::new(roots, self.mmtk(), true);
            self.mmtk().scheduler.postpone(w);
        }
    }

    #[inline(always)]
    fn create_scan_work(&self, nodes: Vec<ObjectReference>, roots: bool) -> ScanObjects<Self> {
        unreachable!()
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

pub struct ProcessModBufSATB<VM: VMBinding> {
    nodes: Vec<ObjectReference>,
    phantom: PhantomData<VM>,
}

impl<VM: VMBinding> ProcessModBufSATB<VM> {
    pub fn new(nodes: Vec<ObjectReference>) -> Self {
        Self {
            nodes,
            phantom: PhantomData,
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessModBufSATB<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        if self.nodes.is_empty() {
            return;
        }
        GCWork::do_work(
            &mut LXRConcurrentTraceObjects::<VM>::new(self.nodes.clone(), mmtk, true),
            worker,
            mmtk,
        );
    }
}

pub struct LXRStopTheWorldProcessEdges<VM: VMBinding> {
    lxr: &'static LXR<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
    forwarded_roots: Vec<ObjectReference>,
}

impl<VM: VMBinding> ProcessEdgesWork for LXRStopTheWorldProcessEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = crate::args::RC_MATURE_EVACUATION;

    fn new(edges: Vec<EdgeOf<Self>>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        // println!("LXRStopTheWorldProcessEdges::new {:?}", object);
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let lxr = base.plan().downcast_ref::<LXR<VM>>().unwrap();
        Self {
            lxr,
            base,
            pause: Pause::RefCount,
            forwarded_roots: vec![],
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.nodes.is_empty() {
            let mut scan_objects_work = ScanObjects::<Self>::new(self.pop_nodes(), false, false);
            scan_objects_work.discovery = true;
            self.start_or_dispatch_scan_work(scan_objects_work)
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
            // println!("Trace STW {:?} SKIP", object);
            return object;
        }
        let x = if self.lxr.immix_space.in_space(object) {
            let pause = self.pause;
            let worker = self.worker();
            // println!("Trace STW {:?} NOI MARK", object);
            self.lxr.immix_space.rc_trace_object(
                &mut self.nodes,
                object,
                CopySemantics::DefaultCopy,
                pause,
                false,
                worker,
            )
        } else {
            // println!("Trace STW {:?} SKIP LOS", object);
            object
        };
        if self.roots {
            self.forwarded_roots.push(x)
        }
        x
    }

    #[inline]
    fn process_edges(&mut self) {
        self.pause = self.lxr.current_pause().unwrap();
        if self.roots {
            self.forwarded_roots.reserve(self.edges.len());
        }
        if self.pause == Pause::FullTraceFast {
            for i in 0..self.edges.len() {
                // println!(
                //     "Trace STW M {:?} -> {:?}",
                //     self.edges[i].to_address(),
                //     self.edges[i].load()
                // );
                self.process_mark_edge(self.edges[i].to_address())
            }
        } else {
            for i in 0..self.edges.len() {
                // println!(
                //     "Trace STW {:?} -> {:?}",
                //     self.edges[i].to_address(),
                //     self.edges[i].load()
                // );
                ProcessEdgesWork::process_edge(self, self.edges[i])
            }
        }
        self.flush();
        if self.roots {
            let roots = std::mem::take(&mut self.forwarded_roots);
            unsafe {
                crate::plan::lxr::CURR_ROOTS.push(roots);
            }
        }
    }

    #[inline(always)]
    fn create_scan_work(&self, nodes: Vec<ObjectReference>, roots: bool) -> ScanObjects<Self> {
        ScanObjects::<Self>::new(nodes, false, roots)
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
        let x = if self.lxr.immix_space.in_space(object) {
            let pause = self.pause;
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object(
                &mut self.nodes,
                object,
                CopySemantics::DefaultCopy,
                pause,
                true,
                worker,
            )
        } else {
            self.lxr.los().trace_object(&mut self.nodes, object)
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

pub struct LXRWeakRefProcessEdges<VM: VMBinding> {
    lxr: &'static LXR<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
}

impl<VM: VMBinding> ProcessEdgesWork for LXRWeakRefProcessEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = crate::args::RC_MATURE_EVACUATION;

    fn new(edges: Vec<EdgeOf<Self>>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let lxr = base.plan().downcast_ref::<LXR<VM>>().unwrap();
        Self {
            lxr,
            base,
            pause: Pause::RefCount,
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.nodes.is_empty() {
            let scan_objects_work = ScanObjects::<Self>::new(self.pop_nodes(), false, false);
            self.start_or_dispatch_scan_work(scan_objects_work)
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
        let x = if self.lxr.immix_space.in_space(object) {
            let pause = self.pause;
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object(
                &mut self.nodes,
                object,
                CopySemantics::DefaultCopy,
                pause,
                true,
                worker,
            )
        } else {
            self.lxr.los().trace_object(&mut self.nodes, object)
        };
        x
    }

    // #[inline]
    // fn process_edges(&mut self) {
    //     unreachable!()
    // }

    #[inline]
    fn process_edges(&mut self) {
        self.pause = self.lxr.current_pause().unwrap();
        for i in 0..self.edges.len() {
            ProcessEdgesWork::process_edge(self, self.edges[i])
        }
        self.flush();
    }

    #[inline(always)]
    fn create_scan_work(&self, nodes: Vec<ObjectReference>, roots: bool) -> ScanObjects<Self> {
        unreachable!()
    }
}

impl<VM: VMBinding> Deref for LXRWeakRefProcessEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for LXRWeakRefProcessEdges<VM> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
