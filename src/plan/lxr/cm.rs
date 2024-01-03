use super::LXR;
use crate::plan::immix::Pause;
use crate::plan::VectorQueue;
use crate::policy::immix::line::Line;
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
use std::ops::{Deref, DerefMut};
#[cfg(feature = "measure_trace_rate")]
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub struct LXRConcurrentTraceObjects<VM: VMBinding> {
    plan: &'static LXR<VM>,
    // objects to mark and scan
    objects: Option<Vec<ObjectReference>>,
    objects_arc: Option<Arc<Vec<ObjectReference>>>,
    ref_arrays: Option<Vec<(ObjectReference, Address, usize, VM::VMMemorySlice)>>,
    // recursively generated objects
    next_objects: VectorQueue<ObjectReference>,
    next_ref_arrays: VectorQueue<(ObjectReference, Address, usize, VM::VMMemorySlice)>,
    next_ref_arrays_size: usize,
    rc: RefCountHelper<VM>,
    #[cfg(feature = "measure_trace_rate")]
    scanned_non_null_slots: usize,
    #[cfg(feature = "measure_trace_rate")]
    enqueued_objs: usize,
}

impl<VM: VMBinding> LXRConcurrentTraceObjects<VM> {
    pub fn new(objects: Vec<ObjectReference>, mmtk: &'static MMTK<VM>) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.add(objects.len());
        }
        let plan = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            objects: Some(objects),
            objects_arc: None,
            ref_arrays: None,
            next_objects: VectorQueue::default(),
            next_ref_arrays: VectorQueue::default(),
            next_ref_arrays_size: 0,
            rc: RefCountHelper::NEW,
            #[cfg(feature = "measure_trace_rate")]
            scanned_non_null_slots: 0,
            #[cfg(feature = "measure_trace_rate")]
            enqueued_objs: 0,
        }
    }

    pub fn new_arc(objects: Arc<Vec<ObjectReference>>, mmtk: &'static MMTK<VM>) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.add(objects.len());
        }
        let plan = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            objects: None,
            objects_arc: Some(objects),
            ref_arrays: None,
            next_objects: VectorQueue::default(),
            next_ref_arrays: VectorQueue::default(),
            next_ref_arrays_size: 0,
            rc: RefCountHelper::NEW,
            #[cfg(feature = "measure_trace_rate")]
            scanned_non_null_slots: 0,
            #[cfg(feature = "measure_trace_rate")]
            enqueued_objs: 0,
        }
    }

    fn new_ref_arrays(
        arrays: Vec<(ObjectReference, Address, usize, VM::VMMemorySlice)>,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        let plan = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            plan,
            objects: None,
            objects_arc: None,
            ref_arrays: Some(arrays),
            next_objects: VectorQueue::default(),
            next_ref_arrays: VectorQueue::default(),
            next_ref_arrays_size: 0,
            rc: RefCountHelper::NEW,
            #[cfg(feature = "measure_trace_rate")]
            scanned_non_null_slots: 0,
            #[cfg(feature = "measure_trace_rate")]
            enqueued_objs: 0,
        }
    }

    #[cold]
    fn flush(&mut self) {
        self.flush_arrs();
        self.flush_objs();
    }

    #[cold]
    fn flush_arrs(&mut self) {
        if !self.next_ref_arrays.is_empty() {
            let next_ref_arrays = self.next_ref_arrays.take();
            let worker = GCWorker::<VM>::current();
            debug_assert!(self.plan.concurrent_marking_enabled());
            let w = Self::new_ref_arrays(next_ref_arrays, worker.mmtk);
            if self.plan.current_pause() == Some(Pause::RefCount) {
                worker.scheduler().postpone(w);
            } else {
                worker.add_work(WorkBucketStage::Unconstrained, w);
            }
        }
    }

    #[cold]
    fn flush_objs(&mut self) {
        if !self.next_objects.is_empty() {
            let objects = self.next_objects.take();
            let worker = GCWorker::<VM>::current();
            debug_assert!(self.plan.concurrent_marking_enabled());
            let w = Self::new(objects, worker.mmtk);
            if self.plan.current_pause() == Some(Pause::RefCount) {
                worker.scheduler().postpone(w);
            } else {
                worker.add_work(WorkBucketStage::Unconstrained, w);
            }
        }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        debug_assert!(!object.is_null());
        debug_assert!(object.is_in_any_space(), "Invalid object {:?}", object);
        debug_assert!(object.class_is_valid::<VM>());
        if self.plan.immix_space.in_space(object) {
            self.plan
                .immix_space
                .trace_object_without_moving_rc(self, object);
        } else {
            self.plan.los().trace_object_rc(self, object);
        }
        object
    }

    fn trace_objects(&mut self, objects: &[ObjectReference]) {
        for o in objects {
            self.trace_object(*o);
        }
    }

    fn trace_slice<const SRC_IN_DEFRAG: bool, const SRC_IN_IMMIX: bool>(
        &mut self,
        slice: &VM::VMMemorySlice,
    ) {
        let e = slice.iter_edges().next().unwrap();
        if SRC_IN_IMMIX
            && self
                .plan
                .immix_space
                .is_marked(e.to_address().to_object_reference::<VM>())
        {
            return;
        }
        for e in slice.iter_edges() {
            let t = e.load();
            if t.is_null() {
                continue;
            }
            if SRC_IN_IMMIX
                && Line::is_aligned(e.to_address())
                && self.plan.immix_space.line_is_marked(e.to_address())
            {
                return;
            }
            #[cfg(feature = "measure_trace_rate")]
            {
                self.scanned_non_null_slots += 1;
            }
            if crate::args::RC_MATURE_EVACUATION && !SRC_IN_DEFRAG && self.plan.in_defrag(t) {
                self.plan.immix_space.mature_evac_remset.record(e, t);
            }
            self.trace_object(t);
        }
    }

    fn trace_arrays(&mut self, arrays: &[(ObjectReference, Address, usize, VM::VMMemorySlice)]) {
        for (o, cls, size, slice) in arrays {
            if !(o.class_pointer::<VM>() == *cls
                && o.get_size::<VM>() == *size
                && !self.rc.object_or_line_is_dead(*o))
            {
                continue;
            }
            let ix = self.plan.immix_space.in_space(*o);
            if ix {
                let src_in_defrag = self.plan.in_defrag(*o);
                if src_in_defrag {
                    self.trace_slice::<true, true>(slice)
                } else {
                    self.trace_slice::<false, true>(slice)
                }
            } else {
                self.trace_slice::<false, false>(slice)
            }
        }
    }

    fn scan_and_enqueue<const CHECK_REMSET: bool>(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _>(
            CLDScanPolicy::Claim,
            RefScanPolicy::Discover,
            |e, out_of_heap| {
                let t = e.load();
                if t.is_null() {
                    return;
                }
                #[cfg(feature = "measure_trace_rate")]
                {
                    self.scanned_non_null_slots += 1;
                }
                if crate::args::RC_MATURE_EVACUATION
                    && (CHECK_REMSET || out_of_heap)
                    && self.plan.in_defrag(t)
                {
                    self.plan.immix_space.mature_evac_remset.record(e, t);
                }
                self.next_objects.push(t);
                if self.next_objects.len() > 8192 {
                    self.flush_objs();
                }
            },
        );
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
        match VM::VMScanning::get_obj_kind(object) {
            ObjectKind::ObjArray(len) if len >= 1024 => {
                let data = VM::VMScanning::obj_array_data(object);
                let cls = object.class_pointer::<VM>();
                let len = object.get_size::<VM>();

                for chunk in data.chunks(1024) {
                    self.next_ref_arrays_size += chunk.len();
                    self.next_ref_arrays.push((object, cls, len, chunk));
                    if self.next_ref_arrays_size > 8192 {
                        self.flush_arrs();
                    }
                }
                #[cfg(feature = "measure_trace_rate")]
                {
                    self.enqueued_objs += 1;
                }
            }
            ObjectKind::ValArray => {}
            _ => {
                let should_check_remset = !self.plan.in_defrag(object);
                if should_check_remset {
                    self.scan_and_enqueue::<true>(object)
                } else {
                    self.scan_and_enqueue::<false>(object)
                }
                #[cfg(feature = "measure_trace_rate")]
                {
                    self.enqueued_objs += 1;
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
        #[cfg(feature = "measure_trace_rate")]
        let t = std::time::SystemTime::now();
        #[cfg(feature = "measure_trace_rate")]
        let record = if crate::verbose(3) && !mmtk.scheduler.in_concurrent() {
            STW_CM_PACKETS.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            false
        };
        // mark objects
        if let Some(objects) = self.objects.take() {
            self.trace_objects(&objects)
        } else if let Some(objects) = self.objects_arc.take() {
            self.trace_objects(&objects)
        } else if let Some(arrays) = self.ref_arrays.take() {
            self.trace_arrays(&arrays)
        }
        let pause_opt = self.plan.current_pause();
        if pause_opt == Some(Pause::FinalMark) || pause_opt.is_none() {
            let mut next_objects = vec![];
            let mut next_ref_arrays = vec![];
            while !self.next_ref_arrays.is_empty() || !self.next_objects.is_empty() {
                let pause_opt = self.plan.current_pause();
                if !(pause_opt == Some(Pause::FinalMark) || pause_opt.is_none()) {
                    break;
                }
                next_objects.clear();
                next_ref_arrays.clear();
                self.next_objects.swap(&mut next_objects);
                self.trace_objects(&next_objects);
                self.next_ref_arrays.swap(&mut next_ref_arrays);
                self.trace_arrays(&next_ref_arrays);
            }
        }
        self.flush();
        // CM: Decrease counter
        crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(!mmtk.scheduler.work_buckets[WorkBucketStage::Initial].is_activated());
        #[cfg(feature = "measure_trace_rate")]
        if record {
            let us = t.elapsed().unwrap().as_micros() as usize;
            STW_CM_PACKETS_TIME.fetch_add(us, Ordering::SeqCst);
            STW_SCAN_NON_NULL_SLOTS.fetch_add(self.scanned_non_null_slots, Ordering::SeqCst);
            STW_ENQUEUE_OBJS.fetch_add(self.enqueued_objs, Ordering::SeqCst);
        }
    }
}

pub struct ProcessModBufSATB {
    nodes: Option<Vec<ObjectReference>>,
    nodes_arc: Option<Arc<Vec<ObjectReference>>>,
}

impl ProcessModBufSATB {
    pub fn new(nodes: Vec<ObjectReference>) -> Self {
        // crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            nodes: Some(nodes),
            nodes_arc: None,
        }
    }
    pub fn new_arc(nodes: Arc<Vec<ObjectReference>>) -> Self {
        // crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_add(1, Ordering::SeqCst);
        Self {
            nodes: None,
            nodes_arc: Some(nodes),
        }
    }
}

#[cfg(feature = "measure_trace_rate")]
pub static STW_CM_PACKETS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "measure_trace_rate")]
pub static STW_MODBUF_PACKETS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "measure_trace_rate")]
pub static STW_CM_PACKETS_TIME: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "measure_trace_rate")]
pub static STW_ENQUEUE_OBJS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "measure_trace_rate")]
pub static STW_SCAN_NON_NULL_SLOTS: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "measure_trace_rate")]
pub fn dump_trace_rate() {
    gc_log!(
        " - STW_CM_PACKETS={} STW_MODBUF_PACKETS={}",
        STW_CM_PACKETS.load(Ordering::SeqCst),
        STW_MODBUF_PACKETS.load(Ordering::SeqCst),
    );
    STW_CM_PACKETS.store(0, Ordering::SeqCst);
    STW_MODBUF_PACKETS.store(0, Ordering::SeqCst);
    gc_log!(
        " - STW_CM_PACKETS_TIME={}ms STW_ENQUEUE_OBJS={} STW_SCAN_NON_NULL_SLOTS={}",
        STW_CM_PACKETS_TIME.load(Ordering::SeqCst) / 1000,
        STW_ENQUEUE_OBJS.load(Ordering::SeqCst),
        STW_SCAN_NON_NULL_SLOTS.load(Ordering::SeqCst),
    );
    STW_CM_PACKETS_TIME.store(0, Ordering::SeqCst);
    STW_ENQUEUE_OBJS.store(0, Ordering::SeqCst);
    STW_SCAN_NON_NULL_SLOTS.store(0, Ordering::SeqCst);
}

impl<VM: VMBinding> GCWork<VM> for ProcessModBufSATB {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        debug_assert!(!crate::args::BARRIER_MEASUREMENT);
        #[cfg(feature = "measure_trace_rate")]
        if crate::verbose(3) && !mmtk.scheduler.in_concurrent() {
            STW_MODBUF_PACKETS.fetch_add(1, Ordering::SeqCst);
        }
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
        GCWork::do_work(&mut w, worker, mmtk);

        // crate::NUM_CONCURRENT_TRACING_PACKETS.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct LXRStopTheWorldProcessEdges<VM: VMBinding> {
    lxr: &'static LXR<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
    array_slices: Vec<VM::VMMemorySlice>,
    forwarded_roots: Vec<ObjectReference>,
    next_edges: VectorQueue<EdgeOf<Self>>,
    next_array_slices: VectorQueue<VM::VMMemorySlice>,
    next_edge_count: u32,
    remset_recorded_edges: bool,
    refs: Vec<ObjectReference>,
    should_record_forwarded_roots: bool,
}

impl<VM: VMBinding> LXRStopTheWorldProcessEdges<VM> {
    pub(super) fn new_remset(
        edges: Vec<EdgeOf<Self>>,
        refs: Vec<ObjectReference>,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.add(edges.len());
        }
        let mut me = Self::new(edges, false, mmtk, WorkBucketStage::Closure);
        me.remset_recorded_edges = true;
        me.refs = refs;
        me
    }
}

impl<VM: VMBinding> ProcessEdgesWork for LXRStopTheWorldProcessEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = crate::args::RC_MATURE_EVACUATION;

    fn new(
        edges: Vec<EdgeOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.add(edges.len());
        }
        let base = ProcessEdgesBase::new(edges, roots, mmtk, bucket);
        let lxr = base.plan().downcast_ref::<LXR<VM>>().unwrap();
        Self {
            lxr,
            base,
            pause: Pause::RefCount,
            forwarded_roots: vec![],
            array_slices: vec![],
            next_edges: VectorQueue::new(),
            next_array_slices: VectorQueue::new(),
            next_edge_count: 0,
            remset_recorded_edges: false,
            refs: vec![],
            should_record_forwarded_roots: false,
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_edges.is_empty() || !self.next_array_slices.is_empty() {
            let edges = self.next_edges.take();
            let slices = self.next_array_slices.take();
            let mut w = Self::new(edges, false, self.mmtk(), self.bucket);
            w.array_slices = slices;
            self.worker()
                .add_boxed_work(WorkBucketStage::Unconstrained, Box::new(w));
        }
        assert!(self.nodes.is_empty());
        self.next_edge_count = 0;
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
        let object = object.get_forwarded_object().unwrap_or(object);
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
        if self.should_record_forwarded_roots {
            self.forwarded_roots.push(new_object)
        }
        new_object
    }

    fn process_edges(&mut self) {
        self.should_record_forwarded_roots = self.roots
            && !self
                .root_kind
                .map(|r| r.is_incomplete())
                .unwrap_or_default();
        self.pause = self.lxr.current_pause().unwrap();
        if self.should_record_forwarded_roots {
            self.forwarded_roots.reserve(self.edges.len());
        }
        let edges = std::mem::take(&mut self.edges);
        let slices = std::mem::take(&mut self.array_slices);
        self.process_edges_impl(&edges, &slices, self.remset_recorded_edges);
        self.remset_recorded_edges = false;
        let should_record_forwarded_roots = self.should_record_forwarded_roots;
        self.should_record_forwarded_roots = false;
        let mut edges = vec![];
        let mut slices = vec![];
        while !self.next_edges.is_empty() || !self.next_array_slices.is_empty() {
            self.next_edge_count = 0;
            edges.clear();
            slices.clear();
            self.next_edges.swap(&mut edges);
            self.next_array_slices.swap(&mut slices);
            self.process_edges_impl(&edges, &slices, false);
        }
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.sub(self.edges.len());
        }
        self.flush();
        if should_record_forwarded_roots {
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
        debug_assert!(object.is_in_any_space());
        debug_assert!(object.to_address::<VM>().is_aligned_to(8));
        // debug_assert!(object.class_is_valid::<VM>());
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
            let x = self.lxr.los().trace_object_rc(self, object);
            debug_assert_ne!(
                self.lxr.rc.count(x),
                0,
                "ERROR Invalid {:?} los={} rc={}",
                x,
                self.lxr.los().in_space(x),
                self.lxr.rc.count(x)
            );
            x
        };
        if self.should_record_forwarded_roots {
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
            if slot.to_address().is_in_mmtk_heap() {
                debug_assert!(self.remset_recorded_edges);
                // Don't do the store if the original is already overwritten
                let _ =
                    slot.compare_exchange(object, new_object, Ordering::SeqCst, Ordering::SeqCst);
            } else {
                slot.store(new_object);
            }
        }
        super::record_edge_for_validation(slot, new_object);
    }

    fn process_mark_edge(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_and_mark_object(object);
        super::record_edge_for_validation(slot, new_object);
        if Self::OVERWRITE_REFERENCE && !new_object.is_null() && new_object != object {
            slot.store(new_object);
        }
    }

    fn process_edges_impl(
        &mut self,
        edges: &[VM::VMEdge],
        slices: &[VM::VMMemorySlice],
        remset_edges: bool,
    ) {
        if self.pause == Pause::Full {
            for e in edges {
                self.process_mark_edge(*e)
            }
        } else if remset_edges {
            for (i, e) in edges.iter().enumerate() {
                self.process_remset_edge(*e, i)
            }
        } else {
            for e in edges {
                self.process_edge(*e)
            }
        }
        if self.pause == Pause::Full {
            for slice in slices {
                for e in slice.iter_edges() {
                    self.process_mark_edge(e)
                }
            }
        } else {
            for slice in slices {
                for e in slice.iter_edges() {
                    self.process_edge(e)
                }
            }
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
        // Skip primitive array
        match VM::VMScanning::get_obj_kind(object) {
            ObjectKind::ObjArray(len) if len >= 1024 => {
                let data = VM::VMScanning::obj_array_data(object);
                for chunk in data.chunks(Self::CAPACITY) {
                    let len: usize = chunk.len();
                    if self.next_edge_count as usize + len >= Self::CAPACITY {
                        self.flush();
                    }
                    self.next_edge_count += len as u32;
                    self.next_array_slices.push(chunk);
                    if self.next_edge_count as usize >= Self::CAPACITY {
                        self.flush();
                    }
                }
            }
            ObjectKind::ValArray => {}
            _ => {
                object.iterate_fields::<VM, _>(
                    CLDScanPolicy::Claim,
                    RefScanPolicy::Discover,
                    |e, _| {
                        let o = e.load();
                        if o.is_null() {
                            return;
                        }
                        if self.lxr.is_marked(o) && !self.lxr.in_defrag(o) {
                            return;
                        }
                        self.next_edges.push(e);
                        self.next_edge_count += 1;
                        if self.next_edge_count as usize >= Self::CAPACITY {
                            self.flush();
                        }
                    },
                );
            }
        }
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

    fn new(
        edges: Vec<EdgeOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.add(edges.len());
        }
        let base = ProcessEdgesBase::new(edges, roots, mmtk, bucket);
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
                Box::new(Self::new(edges, false, self.mmtk(), self.bucket)),
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
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.sub(self.edges.len());
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
        object.iterate_fields::<VM, _>(CLDScanPolicy::Claim, RefScanPolicy::Follow, |e, _| {
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
