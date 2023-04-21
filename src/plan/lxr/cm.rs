use super::LXR;
use crate::plan::immix::Pause;
use crate::plan::VectorQueue;
use crate::policy::space::Space;
use crate::scheduler::gc_work::{EdgeOf, ScanObjects};
use crate::util::address::{CLDScanPolicy, RefScanPolicy};
use crate::util::copy::CopySemantics;
use crate::util::rc::RefCountHelper;
use crate::util::{Address, ObjectReference};
use crate::vm::edge_shape::Edge;
use crate::{
    plan::ObjectQueue,
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    vm::*,
    MMTK,
};
use atomic::Ordering;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub struct LXRConcurrentTraceObjects<VM: VMBinding> {
    plan: &'static LXR<VM>,
    objects: Option<Vec<ObjectReference>>,
    objects_arc: Option<Arc<Vec<ObjectReference>>>,
    next_objects: VectorQueue<ObjectReference>,
    klass: Address,
    rc: RefCountHelper<VM>,
}

impl<VM: VMBinding> LXRConcurrentTraceObjects<VM> {
    pub fn new(objects: Vec<ObjectReference>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            objects: Some(objects),
            objects_arc: None,
            next_objects: VectorQueue::default(),
            rc: RefCountHelper::NEW,
            klass: Address::ZERO,
        }
    }

    pub fn new_arc(objects: Arc<Vec<ObjectReference>>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            objects: None,
            objects_arc: Some(objects),
            next_objects: VectorQueue::default(),
            rc: RefCountHelper::NEW,
            klass: Address::ZERO,
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_objects.is_empty() {
            let new_nodes = self.next_objects.take();
            // This packet is executed in concurrent.
            let worker = GCWorker::<VM>::current();
            debug_assert!(self.plan.concurrent_marking_enabled());
            let w = LXRConcurrentTraceObjects::<VM>::new(new_nodes, worker.mmtk);
            if self.plan.current_pause() == Some(Pause::RefCount) {
                worker.scheduler().postpone(w);
            } else {
                worker.add_work(WorkBucketStage::Unconstrained, w);
            }
        }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if cfg!(any(feature = "sanity", debug_assertions)) {
            assert!(
                object.to_address::<VM>().is_mapped(),
                "Invalid object {:?}: address is not mapped",
                object
            );
        }

        let no_trace = self.rc.count(object) == 0;
        if no_trace || self.plan.is_marked(object) {
            return object;
        }
        // During concurrent marking, decs-processing can kill the object and mutators can reusing the memory afterwards.
        // If the RC of the object is dec to zero after it is marked by the marker, and before it is scanned,
        // the object scanning step can crash if another mutator is also simultaneously reusing the memory.
        // To solve this problem:
        // 1. We cache the klass pointer before marking the object.
        // 2. If we're the thread to successfully mark the object (instead of the RC decrement thread),
        //    the previously cached class pointer is guaranteed to be valid, as no reuse can happen before we mark the object.
        // 3. Scan the object use the cached klass pointer, and cache the collected child nodes without pushing them to the mark queue.
        //    Note that if the memory is being reused simultaneously, our cached child nodes are invalid.
        // 4. Check the RC of the object after scanning.
        //    Push the previously cached child nodes to the mark queue only if the object RC is not zero -- the object is not overwritten yet and the cached children must be valid.
        self.klass = object.class_pointer::<VM>();
        debug_assert!(object.is_in_any_space(), "Invalid object {:?}", object);
        debug_assert!(object.class_is_valid::<VM>());
        if self.plan.immix_space.in_space(object) {
            self.plan.immix_space.fast_trace_object(self, object);
        } else if self.plan.los().in_space(object) {
            self.plan.los().trace_object(self, object);
        }
        object
    }

    fn process_objects(&mut self, objects: &[ObjectReference]) {
        for o in objects {
            self.trace_object(*o);
        }
    }
}

impl<VM: VMBinding> ObjectQueue for LXRConcurrentTraceObjects<VM> {
    fn enqueue(&mut self, object: ObjectReference) {
        if cfg!(feature = "sanity") {
            assert!(
                object.to_address::<VM>().is_mapped(),
                "Invalid obj {:?}: address is not mapped",
                object
            );
        }
        if self.rc.count(object) == 0 {
            return;
        }
        let should_check_remset = !self.plan.in_defrag(object);
        let mut cached_children: Vec<(VM::VMEdge, ObjectReference, u8)> = vec![];
        object.iterate_fields_with_klass::<VM, _>(
            CLDScanPolicy::Claim,
            RefScanPolicy::Discover,
            self.klass,
            |e| {
                let t: ObjectReference = e.load();
                if t.is_null() {
                    return;
                }
                let validity = self
                    .plan
                    .immix_space
                    .remset
                    .get_currrent_validity_state(e, &self.plan.immix_space);
                cached_children.push((e, t, validity));
            },
        );
        if self.rc.count(object) != 0 {
            if cfg!(feature = "object_size_distribution") {
                crate::record_obj(object.get_size::<VM>());
            }
            if cfg!(feature = "lxr_satb_live_bytes_counter") {
                crate::record_live_bytes(object.get_size::<VM>());
            }
            for (e, t, validity) in cached_children {
                if t.is_null() || self.rc.count(t) == 0 {
                    continue;
                }
                if cfg!(feature = "sanity") {
                    assert!(
                        t.to_address::<VM>().is_mapped(),
                        "Invalid edge {:?}.{:?} -> {:?}: target is not mapped",
                        object,
                        e,
                        t
                    );
                }
                if crate::args::RC_MATURE_EVACUATION
                    && (should_check_remset || !e.to_address().is_mapped())
                    && self.plan.in_defrag(t)
                {
                    self.plan.immix_space.remset.record_with_validity_state(
                        e,
                        t,
                        &self.plan.immix_space,
                        validity,
                    );
                }
                if !self.plan.is_marked(t) {
                    if cfg!(any(feature = "sanity", debug_assertions)) {
                        assert!(
                            t.to_address::<VM>().is_mapped(),
                            "Invalid object {:?}.{:?} -> {:?}: address is not mapped",
                            object,
                            e,
                            t
                        );
                    }
                    self.next_objects.push(t);
                    if self.next_objects.is_full() {
                        self.flush();
                    }
                }
            }
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for LXRConcurrentTraceObjects<VM> {
    fn should_defer(&self) -> bool {
        crate::PAUSE_CONCURRENT_MARKING.load(Ordering::SeqCst)
    }
    fn is_concurrent_marking_work(&self) -> bool {
        true
    }
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        debug_assert!(!mmtk.scheduler.work_buckets[WorkBucketStage::Initial].is_activated());
        if let Some(objects) = self.objects.take() {
            self.process_objects(&objects)
        } else if let Some(objects) = self.objects_arc.take() {
            self.process_objects(&objects)
        }
        // CM: Decrease counter
        self.flush();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(!mmtk.scheduler.work_buckets[WorkBucketStage::Initial].is_activated());
    }
}

pub struct ProcessModBufSATB {
    nodes: Option<Vec<ObjectReference>>,
    nodes_arc: Option<Arc<Vec<ObjectReference>>>,
}

impl ProcessModBufSATB {
    pub fn new(nodes: Vec<ObjectReference>) -> Self {
        Self {
            nodes: Some(nodes),
            nodes_arc: None,
        }
    }
    pub fn new_arc(nodes: Arc<Vec<ObjectReference>>) -> Self {
        Self {
            nodes: None,
            nodes_arc: Some(nodes),
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessModBufSATB {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        let mut w = if let Some(nodes) = self.nodes.take() {
            if nodes.is_empty() {
                return;
            }
            if cfg!(any(feature = "sanity", debug_assertions)) {
                for o in &nodes {
                    assert!(
                        o.is_null() || o.to_address::<VM>().is_mapped(),
                        "Invalid object {:?}: address is not mapped",
                        o
                    );
                }
            }
            LXRConcurrentTraceObjects::new(nodes, mmtk)
        } else if let Some(nodes) = self.nodes_arc.take() {
            if nodes.is_empty() {
                return;
            }
            if cfg!(any(feature = "sanity", debug_assertions)) {
                for o in &*nodes {
                    assert!(
                        o.is_null() || o.to_address::<VM>().is_mapped(),
                        "Invalid object {:?}: address is not mapped",
                        o
                    );
                }
            }
            LXRConcurrentTraceObjects::new_arc(nodes, mmtk)
        } else {
            return;
        };
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        let pause = lxr.current_pause().unwrap();
        if pause == Pause::FinalMark {
            GCWork::do_work(&mut w, worker, mmtk);
        } else {
            worker.scheduler().postpone(w);
        }
    }
}

pub struct LXRStopTheWorldProcessEdges<VM: VMBinding> {
    lxr: &'static LXR<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
    forwarded_roots: Vec<ObjectReference>,
    next_edges: VectorQueue<EdgeOf<Self>>,
    remset_recorded_edges: bool,
    refs: Vec<ObjectReference>,
}

impl<VM: VMBinding> LXRStopTheWorldProcessEdges<VM> {
    pub(super) fn new_remset(
        edges: Vec<EdgeOf<Self>>,
        refs: Vec<ObjectReference>,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let mut me = Self::new(edges, false, mmtk);
        me.remset_recorded_edges = true;
        me.refs = refs;
        me
    }
}

impl<VM: VMBinding> ProcessEdgesWork for LXRStopTheWorldProcessEdges<VM> {
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
            forwarded_roots: vec![],
            next_edges: VectorQueue::new(),
            remset_recorded_edges: false,
            refs: vec![],
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_edges.is_empty() {
            let edges = self.next_edges.take();
            self.worker().add_boxed_work(
                WorkBucketStage::Unconstrained,
                Box::new(Self::new(edges, false, self.mmtk())),
            );
        }
        assert!(self.nodes.is_empty());
    }

    /// Trace  and evacuate objects.
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        // The memory (lines) of these edges can be reused at any time during mature evacuation.
        // Filter out invalid target objects.
        if self.remset_recorded_edges
            && (!object.is_in_any_space() || !object.to_address::<VM>().is_aligned_to(8))
        {
            return object;
        }
        if self.lxr.rc.count(object) == 0 {
            return object;
        }
        debug_assert!(object.is_in_any_space(), "Invalid {:?}", object);
        debug_assert!(
            object.to_address::<VM>().is_aligned_to(8),
            "Invalid {:?} remset={}",
            object,
            self.remset_recorded_edges
        );
        debug_assert!(object.class_is_valid::<VM>());
        let new_object = if self.lxr.immix_space.in_space(object) {
            if self
                .lxr
                .rc
                .address_is_in_straddle_line(object.to_address::<VM>())
            {
                return object;
            }
            let pause = self.pause;
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object(
                self,
                object,
                CopySemantics::DefaultCopy,
                pause,
                true,
                worker,
            )
        } else {
            self.lxr.los().trace_object(self, object)
        };
        if self.roots {
            self.forwarded_roots.push(new_object)
        }
        new_object
    }

    fn process_edges(&mut self) {
        self.pause = self.lxr.current_pause().unwrap();
        if self.roots {
            self.forwarded_roots.reserve(self.edges.len());
        }
        if self.pause == Pause::FullTraceFast {
            for i in 0..self.edges.len() {
                self.process_mark_edge(self.edges[i])
            }
        } else if self.remset_recorded_edges {
            for i in 0..self.edges.len() {
                self.process_remset_edge(self.edges[i], i)
            }
        } else {
            for i in 0..self.edges.len() {
                self.process_edge(self.edges[i])
            }
        }
        self.flush();
        if self.roots {
            let roots = std::mem::take(&mut self.forwarded_roots);
            self.lxr.curr_roots.read().unwrap().push(roots);
        }
    }

    fn process_edge(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE && new_object != object && !new_object.is_null() {
            debug_assert!(!self.remset_recorded_edges);
            slot.store(new_object);
        }
        super::record_edge_for_validation(slot, new_object);
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>, _roots: bool) -> ScanObjects<Self> {
        unreachable!()
    }
}

impl<VM: VMBinding> LXRStopTheWorldProcessEdges<VM> {
    fn trace_and_mark_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        debug_assert_ne!(self.lxr.rc.count(object), 0);
        debug_assert!(object.is_in_any_space());
        debug_assert!(object.to_address::<VM>().is_aligned_to(8));
        debug_assert!(object.class_is_valid::<VM>());
        let x = if self.lxr.immix_space.in_space(object) {
            let pause = self.pause;
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object(
                self,
                object,
                CopySemantics::DefaultCopy,
                pause,
                true,
                worker,
            )
        } else {
            self.lxr.los().trace_object(self, object)
        };
        if self.roots {
            self.forwarded_roots.push(x)
        }
        x
    }

    fn process_remset_edge(&mut self, slot: EdgeOf<Self>, i: usize) {
        let object = slot.load();
        if object != self.refs[i] {
            return;
        }
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE && new_object != object && !new_object.is_null() {
            debug_assert!(self.remset_recorded_edges);
            // Don't do the store if the original is already overwritten
            let _ = slot.compare_exchange(object, new_object, Ordering::SeqCst, Ordering::SeqCst);
        }
        super::record_edge_for_validation(slot, new_object);
    }

    fn process_mark_edge(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_and_mark_object(object);
        super::record_edge_for_validation(slot, new_object);
        if Self::OVERWRITE_REFERENCE && new_object != object && !new_object.is_null() {
            slot.store(new_object);
        }
    }
}

impl<VM: VMBinding> ObjectQueue for LXRStopTheWorldProcessEdges<VM> {
    fn enqueue(&mut self, object: ObjectReference) {
        if cfg!(feature = "object_size_distribution") {
            crate::record_obj(object.get_size::<VM>());
        }
        if cfg!(feature = "lxr_satb_live_bytes_counter") {
            crate::record_live_bytes(object.get_size::<VM>());
        }
        object.iterate_fields::<VM, _>(CLDScanPolicy::Claim, RefScanPolicy::Discover, |e| {
            self.next_edges.push(e);
            if self.next_edges.is_full() {
                self.flush();
            }
        })
    }
}

impl<VM: VMBinding> Deref for LXRStopTheWorldProcessEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for LXRStopTheWorldProcessEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct LXRWeakRefProcessEdges<VM: VMBinding> {
    lxr: &'static LXR<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
    next_edges: VectorQueue<EdgeOf<Self>>,
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
            next_edges: VectorQueue::new(),
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_edges.is_empty() {
            let edges = self.next_edges.take();
            self.worker().add_boxed_work(
                WorkBucketStage::Unconstrained,
                Box::new(Self::new(edges, false, self.mmtk())),
            );
        }
        assert!(self.nodes.is_empty());
    }

    /// Trace  and evacuate objects.
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.lxr.immix_space.in_space(object) {
            let pause = self.pause;
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object(
                self,
                object,
                CopySemantics::DefaultCopy,
                pause,
                true,
                worker,
            )
        } else {
            self.lxr.los().trace_object(self, object)
        }
    }

    fn process_edge(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE && new_object != object && !new_object.is_null() {
            slot.store(new_object);
        }
    }

    fn process_edges(&mut self) {
        self.pause = self.lxr.current_pause().unwrap();
        for i in 0..self.edges.len() {
            ProcessEdgesWork::process_edge(self, self.edges[i])
        }
        self.flush();
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>, _roots: bool) -> ScanObjects<Self> {
        unreachable!()
    }
}

impl<VM: VMBinding> ObjectQueue for LXRWeakRefProcessEdges<VM> {
    fn enqueue(&mut self, object: ObjectReference) {
        if cfg!(feature = "object_size_distribution") {
            crate::record_obj(object.get_size::<VM>());
        }
        if cfg!(feature = "lxr_satb_live_bytes_counter") {
            crate::record_live_bytes(object.get_size::<VM>());
        }
        object.iterate_fields::<VM, _>(CLDScanPolicy::Claim, RefScanPolicy::Follow, |e| {
            self.next_edges.push(e);
            if self.next_edges.is_full() {
                self.flush();
            }
        })
    }
}

impl<VM: VMBinding> Deref for LXRWeakRefProcessEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for LXRWeakRefProcessEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
