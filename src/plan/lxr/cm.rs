use super::LXR;
use crate::plan::immix::Pause;
use crate::plan::VectorQueue;
use crate::policy::immix::block::Block;
use crate::policy::immix::line::Line;
use crate::policy::immix::ImmixSpace;
use crate::policy::space::Space;
use crate::scheduler::gc_work::{ScanObjects, SlotOf};
use crate::scheduler::RootKind;
use crate::util::address::{CLDScanPolicy, RefScanPolicy};
use crate::util::copy::CopySemantics;
use crate::util::rc::RefCountHelper;
use crate::util::{Address, ObjectReference};
use crate::vm::slot::{MemorySlice, Slot};
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

#[inline]
fn prefetch_object<VM: VMBinding>(o: ObjectReference, ix: &ImmixSpace<VM>) {
    if crate::args::PREFETCH_HEADER {
        o.prefetch_read();
    }
    if crate::args::PREFETCH_MARK {
        if ix.in_space(o) {
            VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
                .as_spec()
                .extract_side_spec()
                .prefetch_read(o.to_raw_address())
        }
    }
}

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
    pushes: usize,
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
            pushes: 0,
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
            pushes: 0,
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
            pushes: 0,
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
            let objects = if cfg!(feature = "flush_half_cm") && self.next_objects.len() > 1 {
                let half = self.next_objects.len() / 2;
                self.next_objects.split_off(half)
            } else {
                self.next_objects.take()
            };
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
        // debug_assert!(object.is_in_any_space(), "Invalid object {:?}", object);
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
        for (i, o) in objects.iter().enumerate() {
            self.trace_object(*o);
            if crate::args::PREFETCH {
                if let Some(o) = objects.get(i + crate::args::PREFETCH_STEP) {
                    prefetch_object(*o, &self.plan.immix_space);
                }
            }
        }
    }

    fn trace_slice<const SRC_IN_DEFRAG: bool, const SRC_IN_IMMIX: bool>(
        &mut self,
        slice: &VM::VMMemorySlice,
    ) {
        let s = slice.iter_slots().next().unwrap();
        if SRC_IN_IMMIX
            && self
                .plan
                .immix_space
                .is_marked(s.to_address().to_object_reference::<VM>())
        {
            return;
        }
        let n = slice.len();
        for (i, s) in slice.iter_slots().enumerate() {
            let Some(t) = s.load() else {
                continue;
            };
            if SRC_IN_IMMIX
                && Line::is_aligned(s.to_address())
                && self.plan.immix_space.line_is_marked(s.to_address())
            {
                return;
            }
            #[cfg(feature = "measure_trace_rate")]
            {
                self.scanned_non_null_slots += 1;
            }
            if crate::args::RC_MATURE_EVACUATION && !SRC_IN_DEFRAG && self.plan.in_defrag(t) {
                self.plan.immix_space.mature_evac_remset.record(s, t);
            }
            self.trace_object(t);
            if crate::args::PREFETCH {
                if i + crate::args::PREFETCH_STEP < n {
                    if let Some(o) = slice.get(i + crate::args::PREFETCH_STEP).load() {
                        prefetch_object(o, &self.plan.immix_space);
                    }
                }
            }
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
            |s, out_of_heap| {
                let Some(t) = s.load() else {
                    return;
                };
                #[cfg(feature = "measure_trace_rate")]
                {
                    self.scanned_non_null_slots += 1;
                }
                if crate::args::RC_MATURE_EVACUATION
                    && (CHECK_REMSET || out_of_heap)
                    && self.plan.in_defrag(t)
                {
                    self.plan.immix_space.mature_evac_remset.record(s, t);
                }
                if self.next_objects.len() as usize + 1 > 8192 {
                    self.flush_objs();
                }
                if cfg!(feature = "flush_half_cm") {
                    if self.pushes >= crate::args::FLUSH_HALF_THRESHOLD {
                        self.flush_objs();
                    }
                }
                self.next_objects.push(t);
                self.pushes += 1;
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
                        o.to_address::<VM>().is_mapped(),
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
                        o.to_address::<VM>().is_mapped(),
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
    next_slots: VectorQueue<SlotOf<Self>>,
    next_array_slices: VectorQueue<VM::VMMemorySlice>,
    next_slot_count: u32,
    remset_recorded_slots: bool,
    refs: Vec<ObjectReference>,
    should_record_forwarded_roots: bool,
    pushes: usize,
}

impl<VM: VMBinding> LXRStopTheWorldProcessEdges<VM> {
    pub(super) fn new_remset(
        slots: Vec<SlotOf<Self>>,
        refs: Vec<ObjectReference>,
        mmtk: &'static MMTK<VM>,
    ) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.add(slots.len());
        }
        let mut me = Self::new(slots, false, mmtk, WorkBucketStage::Closure);
        me.remset_recorded_slots = true;
        me.refs = refs;
        me
    }
}

impl<VM: VMBinding> ProcessEdgesWork for LXRStopTheWorldProcessEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = crate::args::RC_MATURE_EVACUATION;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.add(slots.len());
        }
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        let lxr = base.plan().downcast_ref::<LXR<VM>>().unwrap();
        Self {
            lxr,
            base,
            pause: Pause::RefCount,
            forwarded_roots: vec![],
            array_slices: vec![],
            next_slots: VectorQueue::new(),
            next_array_slices: VectorQueue::new(),
            next_slot_count: 0,
            remset_recorded_slots: false,
            refs: vec![],
            should_record_forwarded_roots: false,
            pushes: 0,
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_slots.is_empty() || !self.next_array_slices.is_empty() {
            let slots = if cfg!(feature = "flush_half_mature") && self.next_slots.len() > 1 {
                let half = self.next_slots.len() / 2;
                self.next_slots.split_off(half)
            } else {
                self.next_slots.take()
            };
            let slices = self.next_array_slices.take();
            let mut w = Self::new(slots, false, self.mmtk(), self.bucket);
            w.array_slices = slices;
            self.worker()
                .add_boxed_work(WorkBucketStage::Unconstrained, Box::new(w));
        }
        assert!(self.nodes.is_empty());
        self.next_slot_count = self.next_slots.len() as u32;
        self.pushes = self.next_slots.len();
    }

    /// Trace  and evacuate objects.
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        // The memory (lines) of these slots can be reused at any time during mature evacuation.
        // Filter out invalid target objects.
        if self.remset_recorded_slots
            && (!object.is_in_any_space::<VM>() || !object.to_address::<VM>().is_aligned_to(8))
        {
            return object;
        }
        if self.lxr.rc.count(object) == 0 {
            return object;
        }
        if self.roots
            && self.root_kind == Some(RootKind::Weak)
            && !Block::containing::<VM>(object).is_defrag_source()
        {
            return object;
        }
        debug_assert!(object.is_in_any_space::<VM>(), "Invalid {:?}", object);
        debug_assert!(
            object.to_address::<VM>().is_aligned_to(8),
            "Invalid {:?} remset={}",
            object,
            self.remset_recorded_slots
        );
        debug_assert!(object.class_is_valid::<VM>());
        let object = object.get_forwarded_object::<VM>().unwrap_or(object);
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

    fn process_slots(&mut self) {
        self.should_record_forwarded_roots = self.roots
            && !self
                .root_kind
                .map(|r| r.should_skip_decs())
                .unwrap_or_default();
        self.pause = self.lxr.current_pause().unwrap();
        if self.should_record_forwarded_roots {
            self.forwarded_roots.reserve(self.slots.len());
        }
        let slots = std::mem::take(&mut self.slots);
        let slices = std::mem::take(&mut self.array_slices);
        self.process_slots_impl(&slots, &slices, self.remset_recorded_slots);
        self.roots = false;
        self.remset_recorded_slots = false;
        let should_record_forwarded_roots = self.should_record_forwarded_roots;
        self.should_record_forwarded_roots = false;
        let mut slots = vec![];
        let mut slices = vec![];
        while !self.next_slots.is_empty() || !self.next_array_slices.is_empty() {
            self.next_slot_count = 0;
            slots.clear();
            slices.clear();
            self.next_slots.swap(&mut slots);
            self.next_array_slices.swap(&mut slices);
            self.process_slots_impl(&slots, &slices, false);
        }
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.sub(self.slots.len());
        }
        self.flush();
        if should_record_forwarded_roots {
            let roots = std::mem::take(&mut self.forwarded_roots);
            self.lxr.curr_roots.read().unwrap().push(roots);
        }
    }

    fn process_slot(&mut self, slot: SlotOf<Self>) {
        let Some(object) = slot.load() else {
            return;
        };
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE && new_object != object {
            debug_assert!(!self.remset_recorded_slots);
            slot.store(Some(new_object));
        }
        super::record_slot_for_validation(slot, Some(new_object));
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> ScanObjects<Self> {
        unreachable!()
    }
}

impl<VM: VMBinding> LXRStopTheWorldProcessEdges<VM> {
    fn trace_and_mark_object(&mut self, object: ObjectReference) -> ObjectReference {
        debug_assert!(object.is_in_any_space::<VM>());
        debug_assert!(object.to_address::<VM>().is_aligned_to(8));
        // debug_assert!(object.class_is_valid::<VM>());
        if self.roots
            && self.root_kind == Some(RootKind::Weak)
            && !Block::containing::<VM>(object).is_defrag_source()
        {
            return object;
        }
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

    fn process_remset_slot(&mut self, slot: SlotOf<Self>, i: usize) {
        let Some(object) = slot.load() else {
            return;
        };
        if object != self.refs[i] {
            return;
        }
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE && new_object != object {
            if slot.to_address().is_in_mmtk_heap() {
                debug_assert!(self.remset_recorded_slots);
                // Don't do the store if the original is already overwritten
                let _ = slot.compare_exchange(
                    Some(object),
                    Some(new_object),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            } else {
                slot.store(Some(new_object));
            }
        }
        super::record_slot_for_validation(slot, Some(new_object));
    }

    fn process_mark_slot(&mut self, slot: SlotOf<Self>) {
        let Some(object) = slot.load() else {
            return;
        };
        let new_object = self.trace_and_mark_object(object);
        super::record_slot_for_validation(slot, Some(new_object));
        if Self::OVERWRITE_REFERENCE && new_object != object {
            slot.store(Some(new_object));
        }
    }

    fn process_slots_impl(
        &mut self,
        slots: &[VM::VMSlot],
        slices: &[VM::VMMemorySlice],
        remset_slots: bool,
    ) {
        if self.pause == Pause::Full {
            for (i, s) in slots.iter().enumerate() {
                self.process_mark_slot(*s);
                if crate::args::PREFETCH {
                    if let Some(s) = slots.get(i + crate::args::PREFETCH_STEP) {
                        if let Some(o) = s.load() {
                            prefetch_object(o, &self.lxr.immix_space);
                        }
                    }
                }
            }
        } else if remset_slots {
            for (i, s) in slots.iter().enumerate() {
                self.process_remset_slot(*s, i);
                if crate::args::PREFETCH {
                    if let Some(s) = slots.get(i + crate::args::PREFETCH_STEP) {
                        if let Some(o) = s.load() {
                            prefetch_object(o, &self.lxr.immix_space);
                        }
                    }
                }
            }
        } else {
            for (i, s) in slots.iter().enumerate() {
                self.process_slot(*s);
                if crate::args::PREFETCH {
                    if let Some(s) = slots.get(i + crate::args::PREFETCH_STEP) {
                        if let Some(o) = s.load() {
                            prefetch_object(o, &self.lxr.immix_space);
                        }
                    }
                }
            }
        }
        if self.pause == Pause::Full {
            for slice in slices {
                let n = slice.len();
                for (i, s) in slice.iter_slots().enumerate() {
                    self.process_mark_slot(s);
                    if crate::args::PREFETCH {
                        if i + crate::args::PREFETCH_STEP < n {
                            let s = slice.get(i + crate::args::PREFETCH_STEP);
                            if let Some(o) = s.load() {
                                prefetch_object(o, &self.lxr.immix_space);
                            }
                        }
                    }
                }
            }
        } else {
            for slice in slices {
                let n = slice.len();
                for (i, s) in slice.iter_slots().enumerate() {
                    self.process_slot(s);
                    if crate::args::PREFETCH {
                        if i + crate::args::PREFETCH_STEP < n {
                            let s = slice.get(i + crate::args::PREFETCH_STEP);
                            if let Some(o) = s.load() {
                                prefetch_object(o, &self.lxr.immix_space);
                            }
                        }
                    }
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
                    if self.next_slot_count as usize + len >= Self::CAPACITY {
                        self.flush();
                    }
                    self.next_slot_count += len as u32;
                    self.next_array_slices.push(chunk);
                    if self.next_slot_count as usize >= Self::CAPACITY {
                        self.flush();
                    }
                }
            }
            ObjectKind::ValArray => {}
            _ => {
                object.iterate_fields::<VM, _>(
                    CLDScanPolicy::Claim,
                    RefScanPolicy::Discover,
                    |s, _| {
                        let Some(o) = s.load() else {
                            return;
                        };
                        if self.lxr.is_marked(o) && !self.lxr.in_defrag(o) {
                            return;
                        }
                        if self.next_slot_count as usize + 1 > Self::CAPACITY {
                            self.flush();
                        }
                        if cfg!(feature = "flush_half_mature") {
                            if self.pushes >= crate::args::FLUSH_HALF_THRESHOLD {
                                self.flush();
                            }
                        }
                        self.next_slots.push(s);
                        self.next_slot_count += 1;
                        self.pushes += 1;
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
    array_slices: Vec<VM::VMMemorySlice>,
    next_slots: VectorQueue<SlotOf<Self>>,
    next_array_slices: VectorQueue<VM::VMMemorySlice>,
    next_slot_count: u32,
    pushes: usize,
}

impl<VM: VMBinding> LXRWeakRefProcessEdges<VM> {
    fn process_slots_impl(&mut self, slots: &[VM::VMSlot], slices: &[VM::VMMemorySlice]) {
        for (i, s) in slots.iter().enumerate() {
            self.process_slot(*s);
            if crate::args::PREFETCH {
                if let Some(s) = slots.get(i + crate::args::PREFETCH_STEP) {
                    if let Some(o) = s.load() {
                        prefetch_object(o, &self.lxr.immix_space);
                    }
                }
            }
        }
        for slice in slices {
            let n = slice.len();
            for (i, s) in slice.iter_slots().enumerate() {
                self.process_slot(s);
                if crate::args::PREFETCH {
                    if i + crate::args::PREFETCH_STEP < n {
                        let s = slice.get(i + crate::args::PREFETCH_STEP);
                        if let Some(o) = s.load() {
                            prefetch_object(o, &self.lxr.immix_space);
                        }
                    }
                }
            }
        }
    }

    #[cold]
    fn flush_half(&mut self) {
        if !self.next_slots.is_empty() || !self.next_array_slices.is_empty() {
            // let slots = self.next_slots.take();
            let slots = if cfg!(feature = "flush_half_weak") && self.next_slots.len() > 1 {
                let half = self.next_slots.len() / 2;
                self.next_slots.split_off(half)
            } else {
                self.next_slots.take()
            };
            let slices = self.next_array_slices.take();
            let mut w = Self::new(slots, false, self.mmtk(), self.bucket);
            w.array_slices = slices;
            self.worker()
                .add_boxed_work(WorkBucketStage::Unconstrained, Box::new(w));
        }
        assert!(self.nodes.is_empty());
        self.next_slot_count = self.next_slots.len() as u32;
        self.pushes = self.next_slots.len();
    }
}

impl<VM: VMBinding> ProcessEdgesWork for LXRWeakRefProcessEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = crate::args::RC_MATURE_EVACUATION;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.add(slots.len());
        }
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        let lxr = base.plan().downcast_ref::<LXR<VM>>().unwrap();
        Self {
            lxr,
            base,
            array_slices: vec![],
            pause: Pause::RefCount,
            next_slots: VectorQueue::new(),
            next_array_slices: VectorQueue::new(),
            next_slot_count: 0,
            pushes: 0,
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.next_slots.is_empty() || !self.next_array_slices.is_empty() {
            let slots = self.next_slots.take();
            let slices = self.next_array_slices.take();
            let mut w = Self::new(slots, false, self.mmtk(), self.bucket);
            w.array_slices = slices;
            self.worker()
                .add_boxed_work(WorkBucketStage::Unconstrained, Box::new(w));
        }
        assert!(self.nodes.is_empty());
        self.next_slot_count = self.next_slots.len() as u32;
        self.pushes = self.next_slots.len();
    }

    /// Trace  and evacuate objects.
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
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

    fn process_slot(&mut self, slot: SlotOf<Self>) {
        let Some(object) = slot.load() else {
            return;
        };
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE && new_object != object {
            slot.store(Some(new_object));
        }
    }

    fn process_slots(&mut self) {
        self.pause = self.lxr.current_pause().unwrap();

        let slots = std::mem::take(&mut self.slots);
        let slices = std::mem::take(&mut self.array_slices);
        self.process_slots_impl(&slots, &slices);

        let mut slots = vec![];
        let mut slices = vec![];
        while !self.next_slots.is_empty() || !self.next_array_slices.is_empty() {
            self.next_slot_count = 0;
            slots.clear();
            slices.clear();
            self.next_slots.swap(&mut slots);
            self.next_array_slices.swap(&mut slices);
            self.process_slots_impl(&slots, &slices);
        }
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::SATB_BUFFER_COUNTER.sub(self.slots.len());
        }
        self.flush();
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> ScanObjects<Self> {
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
        match VM::VMScanning::get_obj_kind(object) {
            ObjectKind::ObjArray(len) if len >= 1024 => {
                let data = VM::VMScanning::obj_array_data(object);
                for chunk in data.chunks(Self::CAPACITY) {
                    let len: usize = chunk.len();
                    if self.next_slot_count as usize + len >= Self::CAPACITY {
                        self.flush_half();
                    }
                    self.next_slot_count += len as u32;
                    self.next_array_slices.push(chunk);
                    if self.next_slot_count as usize >= Self::CAPACITY {
                        self.flush_half();
                    }
                }
            }
            ObjectKind::ValArray => {}
            _ => {
                object.iterate_fields::<VM, _>(
                    CLDScanPolicy::Claim,
                    RefScanPolicy::Follow,
                    |s, _| {
                        let Some(_) = s.load() else {
                            return;
                        };
                        if self.next_slot_count as usize + 1 > Self::CAPACITY {
                            self.flush();
                        }
                        if cfg!(feature = "flush_half_weak") {
                            if self.pushes >= crate::args::FLUSH_HALF_THRESHOLD {
                                self.flush_half();
                            }
                        }
                        self.next_slots.push(s);
                        self.next_slot_count += 1;
                        self.pushes += 1;
                    },
                );
            }
        }
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
