use super::LXR;
use crate::plan::immix::Pause;
use crate::plan::VectorQueue;
use crate::policy::space::Space;
use crate::scheduler::gc_work::{EdgeOf, ScanObjects};
use crate::scheduler::GCWorkScheduler;
use crate::util::address::{CLDScanPolicy, RefScanPolicy};
use crate::util::copy::CopySemantics;
use crate::util::heap::chunk_map::Chunk;
use crate::util::heap::layout::vm_layout_constants::BYTES_IN_CHUNK;
use crate::util::linear_scan::Region;
use crate::util::rc::{RC_STRADDLE_LINES, RC_TABLE};
use crate::util::ObjectReference;
use crate::vm::edge_shape::Edge;
use crate::{
    plan::ObjectQueue,
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    vm::*,
    MMTK,
};
use std::ops::{Deref, DerefMut};

pub fn schedule_first_pass_preparation_tasks<VM: VMBinding>(scheduler: &GCWorkScheduler<VM>) {
    scheduler.work_buckets[WorkBucketStage::ResetRC].bulk_add(vec![
        Box::new(LXREmergencyResetImmixRCTable),
        Box::new(LXREmergencyResetLOSRCTable),
    ]);
}

struct LXREmergencyResetRCTableForChunk(pub(super) Chunk);

impl<VM: VMBinding> GCWork<VM> for LXREmergencyResetRCTableForChunk {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        RC_TABLE.bzero_metadata(self.0.start(), BYTES_IN_CHUNK);
        RC_STRADDLE_LINES.bzero_metadata(self.0.start(), BYTES_IN_CHUNK);
    }
}

struct LXREmergencyResetLOSRCTable;

impl<VM: VMBinding> GCWork<VM> for LXREmergencyResetLOSRCTable {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        mmtk.get_plan().common().los.zero_rc_bits();
    }
}

struct LXREmergencyResetImmixRCTable;

impl<VM: VMBinding> GCWork<VM> for LXREmergencyResetImmixRCTable {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        mmtk.scheduler.work_buckets[WorkBucketStage::ResetRC].bulk_add(
            lxr.immix_space
                .chunk_map
                .generate_tasks(|chunk| Box::new(LXREmergencyResetRCTableForChunk(chunk))),
        );
    }
}

pub fn schedule_second_pass_preparation_tasks<VM: VMBinding>(scheduler: &GCWorkScheduler<VM>) {
    scheduler.work_buckets[WorkBucketStage::PrepareForSecondClosure].bulk_add(vec![
        Box::new(LXREmergencyBlockSweepingAndEvacuationSetSelection),
        Box::new(LXRSweepMatureLOSAfterGCFirstTrace),
        Box::new(LXREmergencyResetImmixMarkTable),
        Box::new(LXREmergencyResetLOSMarkTable),
        Box::new(LXREmergencyClearCLDReclaimedMarks),
    ]);
}

struct LXREmergencyBlockSweepingAndEvacuationSetSelection;

impl<VM: VMBinding> GCWork<VM> for LXREmergencyBlockSweepingAndEvacuationSetSelection {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        lxr.immix_space
            .schedule_defrag_selection_packets(Pause::FullTraceFast);
    }
}

struct LXRSweepMatureLOSAfterGCFirstTrace;

impl<VM: VMBinding> GCWork<VM> for LXRSweepMatureLOSAfterGCFirstTrace {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk
            .plan
            .downcast_ref::<crate::plan::lxr::LXR<VM>>()
            .unwrap();
        let los = mmtk.plan.common().get_los();
        los.sweep_rc_mature_objects(mmtk, &|o| lxr.rc.count(o) != 0);
    }
}

struct LXREmergencyResetMarkTableForChunk(pub(super) Chunk);

impl<VM: VMBinding> GCWork<VM> for LXREmergencyResetMarkTableForChunk {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
            .extract_side_spec()
            .bzero_metadata(self.0.start(), Chunk::BYTES);
    }
}

struct LXREmergencyResetLOSMarkTable;

impl<VM: VMBinding> GCWork<VM> for LXREmergencyResetLOSMarkTable {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        mmtk.get_plan().common().los.zero_mark_bits();
    }
}

struct LXREmergencyResetImmixMarkTable;

impl<VM: VMBinding> GCWork<VM> for LXREmergencyResetImmixMarkTable {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        mmtk.scheduler.work_buckets[WorkBucketStage::PrepareForSecondClosure].bulk_add(
            lxr.immix_space
                .chunk_map
                .generate_tasks(|chunk| Box::new(LXREmergencyResetMarkTableForChunk(chunk))),
        );
    }
}

struct LXREmergencyClearCLDReclaimedMarks;

impl<VM: VMBinding> GCWork<VM> for LXREmergencyClearCLDReclaimedMarks {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        <VM as VMBinding>::VMCollection::clear_cld_claimed_marks();
    }
}

/// First full-trace: Reset RC count and mark live objects
pub struct LXREmergencyMarkTrace<VM: VMBinding> {
    lxr: &'static LXR<VM>,
    base: ProcessEdgesBase<VM>,
    next_edges: VectorQueue<EdgeOf<Self>>,
}

impl<VM: VMBinding> ProcessEdgesWork for LXREmergencyMarkTrace<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = false;

    fn new(edges: Vec<EdgeOf<Self>>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let lxr = base.plan().downcast_ref::<LXR<VM>>().unwrap();
        Self {
            lxr,
            base,
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
        let _ = self.lxr.rc.inc(object);
        // debug_assert_ne!(self.lxr.rc.count(object), 0);
        debug_assert!(object.is_in_any_space());
        debug_assert!(object.to_address::<VM>().is_aligned_to(8));
        debug_assert!(object.class_is_valid::<VM>());
        let forwarded = if self.lxr.immix_space.in_space(object) {
            self.lxr.immix_space.trace_mark_rc_mature_object(
                self,
                object,
                Pause::FullTraceFast,
                true,
            )
        } else {
            self.lxr.los().trace_object(self, object)
        };
        assert_eq!(forwarded, object);
        forwarded
    }

    fn process_edges(&mut self) {
        self.process_edges_impl();
        if self.roots {
            let roots = std::mem::take(&mut self.edges);
            self.worker().scheduler().work_buckets[WorkBucketStage::ForwardClosure].add(
                LXREmergencyForwardTrace::new(roots, !self.cld_roots, self.worker().mmtk),
            )
        }
        if !self.next_edges.is_empty() {
            self.edges = self.next_edges.take();
            self.process_edges_impl();
        }
    }

    fn process_edge(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_object(object);
        super::record_edge_for_validation(slot, new_object);
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>, _roots: bool) -> ScanObjects<Self> {
        unreachable!()
    }
}

impl<VM: VMBinding> LXREmergencyMarkTrace<VM> {
    fn process_edges_impl(&mut self) {
        assert_eq!(self.lxr.current_pause(), Some(Pause::FullTraceFast));
        for i in 0..self.edges.len() {
            self.process_edge(self.edges[i])
        }
        self.flush();
    }
}

impl<VM: VMBinding> ObjectQueue for LXREmergencyMarkTrace<VM> {
    fn enqueue(&mut self, object: ObjectReference) {
        if cfg!(feature = "object_size_distribution") {
            crate::record_obj(object.get_size::<VM>());
        }
        if self.lxr.immix_space.in_space(object) {
            self.lxr.rc.mark_potential_straddle_object(object);
        }
        gc_log!([4] "scan {:?}", object);
        object.iterate_fields::<VM, _>(CLDScanPolicy::Claim, RefScanPolicy::Follow, |e| {
            gc_log!([4] " - {:?}.{:?} -> {:?}", object, e.to_address(), e.load());
            self.next_edges.push(e);
            if self.next_edges.is_full() {
                self.flush();
            }
        })
    }
}

impl<VM: VMBinding> Deref for LXREmergencyMarkTrace<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for LXREmergencyMarkTrace<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct LXREmergencyWeakRefProcessEdges<VM: VMBinding> {
    lxr: &'static LXR<VM>,
    pause: Pause,
    base: ProcessEdgesBase<VM>,
    next_edges: VectorQueue<EdgeOf<Self>>,
}

impl<VM: VMBinding> ProcessEdgesWork for LXREmergencyWeakRefProcessEdges<VM> {
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
        let _ = self.lxr.rc.inc(object);
        assert_ne!(self.lxr.rc.count(object), 0);
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

impl<VM: VMBinding> ObjectQueue for LXREmergencyWeakRefProcessEdges<VM> {
    fn enqueue(&mut self, object: ObjectReference) {
        if cfg!(feature = "object_size_distribution") {
            crate::record_obj(object.get_size::<VM>());
        }
        gc_log!([4] "scan weak {:?}", object);
        object.iterate_fields::<VM, _>(CLDScanPolicy::Claim, RefScanPolicy::Follow, |e| {
            self.next_edges.push(e);
            if self.next_edges.is_full() {
                self.flush();
            }
        })
    }
}

impl<VM: VMBinding> Deref for LXREmergencyWeakRefProcessEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for LXREmergencyWeakRefProcessEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

/// First full-trace: Reset RC count and mark live objects
struct LXREmergencyForwardTrace<VM: VMBinding> {
    lxr: &'static LXR<VM>,
    base: ProcessEdgesBase<VM>,
    forwarded_roots: Vec<ObjectReference>,
    next_edges: VectorQueue<EdgeOf<Self>>,
}

impl<VM: VMBinding> ProcessEdgesWork for LXREmergencyForwardTrace<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = crate::args::RC_MATURE_EVACUATION;

    fn new(edges: Vec<EdgeOf<Self>>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        let lxr = base.plan().downcast_ref::<LXR<VM>>().unwrap();
        Self {
            lxr,
            base,
            forwarded_roots: vec![],
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
        let _ = self.lxr.rc.inc(object);
        assert_ne!(self.lxr.rc.count(object), 0);
        debug_assert!(object.is_in_any_space());
        debug_assert!(object.to_address::<VM>().is_aligned_to(8));
        debug_assert!(object.class_is_valid::<VM>());
        let forwarded = if self.lxr.immix_space.in_space(object) {
            let worker = self.worker();
            self.lxr.immix_space.rc_trace_object(
                self,
                object,
                CopySemantics::DefaultCopy,
                Pause::FullTraceFast,
                true,
                worker,
            )
        } else {
            self.lxr.los().trace_object(self, object)
        };
        if self.roots {
            self.forwarded_roots.push(forwarded)
        }
        forwarded
    }

    fn process_edges(&mut self) {
        if self.roots {
            self.forwarded_roots.reserve(self.edges.len());
        }
        self.process_edges_impl();
        if self.roots {
            let roots = std::mem::take(&mut self.forwarded_roots);
            self.lxr.curr_roots.read().unwrap().push(roots);
        }
        if !self.next_edges.is_empty() {
            self.edges = self.next_edges.take();
            self.process_edges_impl();
        }
    }

    fn process_edge(&mut self, slot: EdgeOf<Self>) {
        let object = slot.load();
        let new_object = self.trace_object(object);
        super::record_edge_for_validation(slot, new_object);
        if Self::OVERWRITE_REFERENCE && new_object != object && !new_object.is_null() {
            slot.store(new_object);
        }
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>, _roots: bool) -> ScanObjects<Self> {
        unreachable!()
    }
}

impl<VM: VMBinding> LXREmergencyForwardTrace<VM> {
    fn process_edges_impl(&mut self) {
        assert_eq!(self.lxr.current_pause(), Some(Pause::FullTraceFast));
        for i in 0..self.edges.len() {
            self.process_edge(self.edges[i])
        }
        self.flush();
    }
}

impl<VM: VMBinding> ObjectQueue for LXREmergencyForwardTrace<VM> {
    fn enqueue(&mut self, object: ObjectReference) {
        if cfg!(feature = "object_size_distribution") {
            crate::record_obj(object.get_size::<VM>());
        }
        gc_log!([4] "scan fwd {:?}", object);
        object.iterate_fields::<VM, _>(CLDScanPolicy::Claim, RefScanPolicy::Discover, |e| {
            self.next_edges.push(e);
            if self.next_edges.is_full() {
                self.flush();
            }
        })
    }
}

impl<VM: VMBinding> Deref for LXREmergencyForwardTrace<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for LXREmergencyForwardTrace<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
