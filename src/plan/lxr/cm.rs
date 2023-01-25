use crate::plan::immix::Pause;
use crate::plan::VectorQueue;
use crate::policy::space::Space;
use crate::scheduler::gc_work::{EdgeOf, ScanObjects};
use crate::util::address::{CLDScanPolicy, RefScanPolicy};
use crate::util::copy::CopySemantics;
use crate::util::rc::RefCountHelper;
use crate::util::{Address, ObjectReference};
use crate::vm::edge_shape::{Edge, MemorySlice};
use crate::{
    plan::ObjectQueue,
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    vm::*,
    MMTK,
};
use atomic::Ordering;
use std::sync::Arc;
use std::{
    ops::{Deref, DerefMut},
    ptr,
};

use super::LXR;

pub struct LXRConcurrentTraceObjects<VM: VMBinding, const COMPRESSED: bool> {
    plan: &'static LXR<VM>,
    mmtk: &'static MMTK<VM>,
    objects: Option<Vec<ObjectReference>>,
    objects_arc: Option<Arc<Vec<ObjectReference>>>,
    slice: Option<&'static [ObjectReference]>,
    next_objects: VectorQueue<ObjectReference>,
    worker: *mut GCWorker<VM>,
    rc: RefCountHelper<VM>,
}

unsafe impl<VM: VMBinding, const COMPRESSED: bool> Send
    for LXRConcurrentTraceObjects<VM, COMPRESSED>
{
}

impl<VM: VMBinding, const COMPRESSED: bool> LXRConcurrentTraceObjects<VM, COMPRESSED> {
    pub fn new(objects: Vec<ObjectReference>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            mmtk,
            objects: Some(objects),
            objects_arc: None,
            slice: None,
            next_objects: VectorQueue::default(),
            worker: ptr::null_mut(),
            rc: RefCountHelper::NEW,
        }
    }

    pub fn new_arc(objects: Arc<Vec<ObjectReference>>, mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            mmtk,
            objects: None,
            objects_arc: Some(objects),
            slice: None,
            next_objects: VectorQueue::default(),
            worker: ptr::null_mut(),
            rc: RefCountHelper::NEW,
        }
    }

    #[allow(unused)]
    pub fn new_slice(slice: &'static [ObjectReference], mmtk: &'static MMTK<VM>) -> Self {
        let plan = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            mmtk,
            objects: None,
            objects_arc: None,
            slice: Some(slice),
            next_objects: VectorQueue::default(),
            worker: ptr::null_mut(),
            rc: RefCountHelper::NEW,
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
            let w = LXRConcurrentTraceObjects::<VM, COMPRESSED>::new(new_nodes, self.mmtk);
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
        }
    }

    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        debug_assert!(object.is_in_any_space());
        let no_trace = self.rc.count(object) == 0;
        if no_trace || self.plan.is_marked(object) {
            return object;
        }
        debug_assert!(object.class_is_valid::<VM>());
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
                    && self.rc.count(*o) != 0
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

impl<VM: VMBinding, const COMPRESSED: bool> ObjectQueue
    for LXRConcurrentTraceObjects<VM, COMPRESSED>
{
    #[inline]
    fn enqueue(&mut self, object: ObjectReference) {
        let should_check_remset = !self.plan.in_defrag(object);
        if crate::args::CM_LARGE_ARRAY_OPTIMIZATION
            && VM::VMScanning::is_obj_array::<COMPRESSED>(object)
            && VM::VMScanning::obj_array_data::<COMPRESSED>(object).bytes() > 1024
        {
            // Buggy. Dead array can be recycled at any time.
            unimplemented!()
        } else {
            object.iterate_fields::<VM, _, COMPRESSED>(
                CLDScanPolicy::Claim,
                RefScanPolicy::Discover,
                |e| {
                    let t: ObjectReference = e.load::<COMPRESSED>();
                    if t.is_null() || self.rc.count(t) == 0 {
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
                },
            );
        }
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> GCWork<VM>
    for LXRConcurrentTraceObjects<VM, COMPRESSED>
{
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
        if let Some(objects) = self.objects.take() {
            self.process_objects(&objects, false)
        } else if let Some(objects) = self.objects_arc.take() {
            self.process_objects(&objects, false)
        } else if let Some(slice) = self.slice {
            self.process_objects(slice, true)
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

pub struct ProcessModBufSATB<const COMPRESSED: bool> {
    nodes: Option<Vec<ObjectReference>>,
    nodes_arc: Option<Arc<Vec<ObjectReference>>>,
}

impl<const COMPRESSED: bool> ProcessModBufSATB<COMPRESSED> {
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

impl<VM: VMBinding, const COMPRESSED: bool> GCWork<VM> for ProcessModBufSATB<COMPRESSED> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        if let Some(nodes) = self.nodes.take() {
            if nodes.is_empty() {
                return;
            }
            GCWork::do_work(
                &mut LXRConcurrentTraceObjects::<VM, COMPRESSED>::new(nodes, mmtk),
                worker,
                mmtk,
            );
        }
        if let Some(nodes) = self.nodes_arc.take() {
            if nodes.is_empty() {
                return;
            }
            GCWork::do_work(
                &mut LXRConcurrentTraceObjects::<VM, COMPRESSED>::new_arc(nodes, mmtk),
                worker,
                mmtk,
            );
        }
    }
}

pub struct LXRStopTheWorldProcessEdges<VM: VMBinding, const COMPRESSED: bool> {
    lxr: &'static LXR<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
    forwarded_roots: Vec<ObjectReference>,
    next_edges: VectorQueue<EdgeOf<Self>>,
    remset_recorded_edges: bool,
}

impl<VM: VMBinding, const COMPRESSED: bool> LXRStopTheWorldProcessEdges<VM, COMPRESSED> {
    pub(super) fn new_remset(edges: Vec<EdgeOf<Self>>, mmtk: &'static MMTK<VM>) -> Self {
        let mut me = Self::new(edges, false, mmtk);
        me.remset_recorded_edges = true;
        me
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> ProcessEdgesWork
    for LXRStopTheWorldProcessEdges<VM, COMPRESSED>
{
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
    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        // The memory (lines) of these edges can be reused at any time during mature evacuation.
        // Filter out invalid target objects.
        if self.remset_recorded_edges && (!object.is_in_any_space() || !object.to_address::<VM>().is_aligned_to(8))  {
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
            let pause = self.pause;
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object::<_, COMPRESSED>(
                self,
                object,
                CopySemantics::DefaultCopy,
                pause,
                false,
                worker,
            )
        } else {
            object
        };
        if self.roots {
            self.forwarded_roots.push(new_object)
        }
        new_object
    }

    #[inline]
    fn process_edges<const COMPRESSED2: bool>(&mut self) {
        self.pause = self.lxr.current_pause().unwrap();
        if self.roots {
            self.forwarded_roots.reserve(self.edges.len());
        }
        if self.pause == Pause::FullTraceFast {
            for i in 0..self.edges.len() {
                self.process_mark_edge::<COMPRESSED2>(self.edges[i])
            }
        } else {
            for i in 0..self.edges.len() {
                ProcessEdgesWork::process_edge::<COMPRESSED2>(self, self.edges[i])
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

    #[inline]
    fn process_edge<const COMPRESSED2: bool>(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load::<COMPRESSED2>();
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE && new_object != object {
            if !self.remset_recorded_edges {
                slot.store::<COMPRESSED2>(new_object);
            } else {
                // Don't do the store if the original is already overwritten
                let _ = slot.compare_exchange::<COMPRESSED2>(
                    object,
                    new_object,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
        super::record_edge_for_validation(slot, new_object);
    }

    #[inline(always)]
    fn create_scan_work(&self, _nodes: Vec<ObjectReference>, _roots: bool) -> ScanObjects<Self> {
        unreachable!()
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> LXRStopTheWorldProcessEdges<VM, COMPRESSED> {
    #[inline(always)]
    fn trace_and_mark_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        debug_assert!(object.is_in_any_space());
        debug_assert!(object.to_address::<VM>().is_aligned_to(8));
        debug_assert!(object.class_is_valid::<VM>());
        let x = if self.lxr.immix_space.in_space(object) {
            let pause = self.pause;
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object::<_, COMPRESSED>(
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

    #[inline]
    fn process_mark_edge<const COMPRESSED2: bool>(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load::<COMPRESSED2>();
        let new_object = self.trace_and_mark_object(object);
        super::record_edge_for_validation(slot, new_object);
        if Self::OVERWRITE_REFERENCE {
            slot.store::<COMPRESSED2>(new_object);
        }
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> ObjectQueue
    for LXRStopTheWorldProcessEdges<VM, COMPRESSED>
{
    #[inline]
    fn enqueue(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _, COMPRESSED>(
            CLDScanPolicy::Claim,
            RefScanPolicy::Discover,
            |e| {
                self.next_edges.push(e);
                if self.next_edges.is_full() {
                    self.flush();
                }
            },
        )
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> Deref for LXRStopTheWorldProcessEdges<VM, COMPRESSED> {
    type Target = ProcessEdgesBase<VM>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> DerefMut
    for LXRStopTheWorldProcessEdges<VM, COMPRESSED>
{
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct LXRWeakRefProcessEdges<VM: VMBinding, const COMPRESSED: bool> {
    lxr: &'static LXR<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
    next_edges: VectorQueue<EdgeOf<Self>>,
}

impl<VM: VMBinding, const COMPRESSED: bool> ProcessEdgesWork
    for LXRWeakRefProcessEdges<VM, COMPRESSED>
{
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
    #[inline(always)]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        if self.lxr.immix_space.in_space(object) {
            let pause = self.pause;
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object::<_, COMPRESSED>(
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

    #[inline]
    fn process_edges<const COMPRESSED2: bool>(&mut self) {
        self.pause = self.lxr.current_pause().unwrap();
        for i in 0..self.edges.len() {
            ProcessEdgesWork::process_edge::<COMPRESSED2>(self, self.edges[i])
        }
        self.flush();
    }

    #[inline(always)]
    fn create_scan_work(&self, _nodes: Vec<ObjectReference>, _roots: bool) -> ScanObjects<Self> {
        unreachable!()
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> ObjectQueue for LXRWeakRefProcessEdges<VM, COMPRESSED> {
    #[inline]
    fn enqueue(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _, COMPRESSED>(
            CLDScanPolicy::Claim,
            RefScanPolicy::Follow,
            |e| {
                self.next_edges.push(e);
                if self.next_edges.is_full() {
                    self.flush();
                }
            },
        )
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> Deref for LXRWeakRefProcessEdges<VM, COMPRESSED> {
    type Target = ProcessEdgesBase<VM>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> DerefMut for LXRWeakRefProcessEdges<VM, COMPRESSED> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
