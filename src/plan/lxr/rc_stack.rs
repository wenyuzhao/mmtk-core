use super::cm::LXRConcurrentTraceObjects;
use super::cm::LXRStopTheWorldProcessEdges;
use super::SurvivalRatioPredictorLocal;
use super::LXR;
use crate::plan::VectorQueue;
use crate::scheduler::gc_work::RootKind;
use crate::scheduler::gc_work::ScanObjects;
use crate::scheduler::gc_work::SlotOf;
use crate::scheduler::worker::GCWorkerShared;
use crate::scheduler::GCWorkScheduler;
use crate::util::address::CLDScanPolicy;
use crate::util::address::RefScanPolicy;
use crate::util::copy::CopySemantics;
use crate::util::copy::GCWorkerCopyContext;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::rc::*;
use crate::vm::slot::MemorySlice;
use crate::vm::slot::Slot;
use crate::LazySweepingJobsCounter;
use crate::{
    plan::immix::Pause,
    policy::{immix::block::Block, space::Space},
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
    util::{metadata::side_metadata, object_forwarding, ObjectReference},
    vm::*,
    MMTK,
};
use atomic::Ordering;
use crossbeam::deque::Steal;
use std::ops::{Deref, DerefMut};
#[cfg(feature = "measure_rc_rate")]
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[inline]
fn prefetch_object<VM: VMBinding>(o: ObjectReference, rc: &RefCountHelper<VM>) {
    if crate::args::PREFETCH_HEADER {
        o.prefetch_read();
    }
    if crate::args::PREFETCH_RC {
        rc.prefetch_read(o);
    }
}

pub struct ProcessIncs<VM: VMBinding, const KIND: EdgeKind> {
    /// Increments to process
    incs: Vec<VM::VMSlot>,
    inc_slices: Vec<VM::VMMemorySlice>,
    inc_slices_count: usize,
    pause: Pause,
    in_cm: bool,
    no_evac: bool,
    pub root_kind: Option<RootKind>,
    depth: u32,
    lxr: &'static LXR<VM>,
    rc: RefCountHelper<VM>,
    survival_ratio_predictor_local: SurvivalRatioPredictorLocal,
    copy_context: *mut GCWorkerCopyContext<VM>,
    #[cfg(feature = "lxr_precise_incs_counter")]
    stat: crate::LocalRCStat,
    #[cfg(feature = "measure_rc_rate")]
    inc_objs: usize,
    #[cfg(feature = "measure_rc_rate")]
    copy_objs: usize,
    pushes: usize,
}

unsafe impl<VM: VMBinding, const KIND: EdgeKind> Send for ProcessIncs<VM, KIND> {}

impl<VM: VMBinding, const KIND: EdgeKind> ProcessIncs<VM, KIND> {
    const CAPACITY: usize = crate::args::BUFFER_SIZE;
    const UNLOG_BITS: SideMetadataSpec = *VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
        .as_spec()
        .extract_side_spec();

    fn worker(&self) -> &'static mut GCWorker<VM> {
        GCWorker::<VM>::current()
    }

    fn copy_context(&self) -> &mut GCWorkerCopyContext<VM> {
        unsafe { &mut *self.copy_context }
    }

    fn __default(lxr: &'static LXR<VM>) -> Self {
        Self {
            incs: vec![],
            inc_slices: vec![],
            inc_slices_count: 0,
            lxr,
            pause: Pause::RefCount,
            in_cm: false,
            no_evac: false,
            depth: 1,
            rc: RefCountHelper::NEW,
            root_kind: None,
            survival_ratio_predictor_local: SurvivalRatioPredictorLocal::default(),
            copy_context: std::ptr::null_mut(),
            #[cfg(feature = "lxr_precise_incs_counter")]
            stat: crate::LocalRCStat::default(),
            #[cfg(feature = "measure_rc_rate")]
            inc_objs: 0,
            #[cfg(feature = "measure_rc_rate")]
            copy_objs: 0,
            pushes: 0,
        }
    }

    fn stack_size(&self) -> usize {
        self.incs.len() + self.inc_slices_count
    }

    fn add_new_slot(&mut self, s: VM::VMSlot) {
        if cfg!(feature = "steal") {
            let worker = self.worker();
            if worker.deque.push(s).is_err() {
                self.incs.push(s);
                self.pushes += 1;
                if self.incs.len() >= crate::args::BUFFER_SIZE
                    || (cfg!(feature = "push") && self.pushes >= 512)
                {
                    self.flush_half_slots();
                }
            }
        } else {
            if self.stack_size() + 1 > Self::CAPACITY {
                self.flush();
            }
            if cfg!(feature = "push") {
                if self.pushes >= 512 {
                    self.flush_half_slots();
                }
            }
            assert!(self.incs.len() <= Self::CAPACITY);
            self.incs.push(s);
            self.pushes += 1;
        }
    }

    fn add_new_slice(&mut self, s: VM::VMMemorySlice) {
        let len = s.len();
        if self.stack_size() + len > Self::CAPACITY {
            self.flush();
        }
        self.inc_slices_count += len as usize;
        self.inc_slices.push(s);
    }

    pub fn new_objects(_objects: Vec<ObjectReference>) -> Self {
        unreachable!()
    }

    pub fn new(incs: Vec<VM::VMSlot>, lxr: &'static LXR<VM>) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::INC_BUFFER_COUNTER.add(incs.len());
        }
        Self {
            incs,
            ..Self::__default(lxr)
        }
    }

    fn promote(&mut self, o: ObjectReference, copied: bool, los: bool, depth: u32) {
        o.verify::<VM>();
        crate::stat(|s| {
            s.promoted_objects += 1;
            s.promoted_volume += o.get_size::<VM>();
            if self.lxr.los().in_space(o) {
                s.promoted_los_objects += 1;
                s.promoted_los_volume += o.get_size::<VM>();
            }
            if copied {
                s.promoted_copy_objects += 1;
                s.promoted_copy_volume += o.get_size::<VM>();
            }
        });
        #[cfg(feature = "lxr_srv_ratio_counter")]
        self.survival_ratio_predictor_local
            .record_total_promotion(o.get_size::<VM>(), los);
        let size = o.get_size::<VM>();

        if !los {
            let block = Block::containing::<VM>(o);
            if !copied && block.is_nursery() {
                block.set_as_in_place_promoted(&self.lxr.immix_space);
            }
            self.rc.promote_with_size(o, size);
            if copied {
                self.survival_ratio_predictor_local
                    .record_copied_promotion(size);
            }
        } else {
            // println!("promote los {:?} {}", o, self.immix().is_marked(o));
        }
        // Don't mark copied objects in initial mark pause. The concurrent marker will do it (and can also resursively mark the old objects).
        if self.in_cm || self.pause == Pause::FinalMark {
            debug_assert!(self.lxr.is_marked(o), "{:?} is not marked", o);
        }
        self.scan_nursery_object(o, los, !copied, depth, size);
    }

    fn record_mature_evac_remset2(
        &mut self,
        slot_in_defrag: bool,
        s: VM::VMSlot,
        o: ObjectReference,
    ) {
        if !(crate::args::RC_MATURE_EVACUATION && (self.in_cm || self.pause == Pause::FinalMark)) {
            return;
        }
        if !slot_in_defrag && self.lxr.in_defrag(o) {
            self.lxr.immix_space.mature_evac_remset.record(s, o);
        }
    }

    fn record_mature_evac_remset(&mut self, s: VM::VMSlot, o: ObjectReference) {
        if !(crate::args::RC_MATURE_EVACUATION && (self.in_cm || self.pause == Pause::FinalMark)) {
            return;
        }
        self.record_mature_evac_remset2(self.lxr.address_in_defrag(s.to_address()), s, o);
    }

    fn scan_nursery_object(
        &mut self,
        o: ObjectReference,
        los: bool,
        in_place_promotion: bool,
        _depth: u32,
        size: usize,
    ) {
        let heap_bytes_per_unlog_byte = if VM::VMObjectModel::COMPRESSED_PTR_ENABLED {
            32usize
        } else {
            64
        };
        let is_val_array = VM::VMScanning::is_val_array(o);
        if los {
            if !is_val_array {
                let start =
                    side_metadata::address_to_meta_address(&Self::UNLOG_BITS, o.to_address::<VM>())
                        .to_mut_ptr::<u8>();
                let limit = side_metadata::address_to_meta_address(
                    &Self::UNLOG_BITS,
                    (o.to_address::<VM>() + size).align_up(heap_bytes_per_unlog_byte),
                )
                .to_mut_ptr::<u8>();
                unsafe {
                    let bytes = limit.offset_from(start) as usize;
                    std::ptr::write_bytes(start, 0xffu8, bytes);
                }
            }
            o.to_address::<VM>().unlog_field_relaxed::<VM>();
        } else if in_place_promotion && !is_val_array {
            let header_size = if VM::VMObjectModel::COMPRESSED_PTR_ENABLED {
                12usize
            } else {
                16
            };
            let step = heap_bytes_per_unlog_byte << 2;
            let end = o.to_address::<VM>() + size;
            let aligned_end = end.align_up(step);
            let cursor = o.to_address::<VM>() + header_size;
            let mut cursor = cursor.align_down(step);
            let mut meta = side_metadata::address_to_meta_address(&Self::UNLOG_BITS, cursor);
            while cursor < aligned_end {
                unsafe { meta.store(0xffffffffu32) }
                meta += 4usize;
                cursor += step;
            }
        };
        if VM::VMScanning::is_obj_array(o) && VM::VMScanning::obj_array_data(o).len() > 1024 {
            let data = VM::VMScanning::obj_array_data(o);
            for chunk in data.chunks(Self::CAPACITY) {
                #[cfg(feature = "lxr_precise_incs_counter")]
                {
                    self.stat.rec_incs += chunk.len();
                    if los {
                        self.stat.los_rec_incs += chunk.len();
                    }
                }
                self.add_new_slice(chunk);
            }
        } else if !is_val_array {
            let obj_in_defrag = !los && Block::in_defrag_block::<VM>(o);
            o.iterate_fields::<VM, _>(CLDScanPolicy::Ignore, RefScanPolicy::Follow, |slot, _| {
                #[cfg(feature = "lxr_precise_incs_counter")]
                {
                    self.stat.rec_incs += 1;
                    if los {
                        self.stat.los_rec_incs += 1;
                    }
                }
                let Some(target) = slot.load() else {
                    return;
                };
                // println!(" -- rec inc opt {:?}.{:?} -> {:?}", o, slot, target);
                debug_assert!(
                    target.to_address::<VM>().is_mapped(),
                    "Unmapped obj {:?}.{:?} -> {:?}",
                    o,
                    slot,
                    target
                );
                debug_assert!(
                    target.is_in_any_space::<VM>(),
                    "Unmapped obj {:?}.{:?} -> {:?}",
                    o,
                    slot,
                    target
                );
                // debug_assert!(
                //     target.class_is_valid::<VM>(),
                //     "Invalid object {:?}.{:?} -> {:?}",
                //     o,
                //     slot,
                //     target
                // );
                let rc = self.rc.count(target);
                if rc == 0 {
                    // println!(" -- rec inc {:?}.{:?} -> {:?}", o, slot, target);
                    self.add_new_slot(slot);
                } else {
                    if rc != crate::util::rc::MAX_REF_COUNT {
                        let _ = self.rc.inc(target);
                        #[cfg(feature = "measure_rc_rate")]
                        {
                            self.inc_objs += 1;
                        }
                    }
                    self.record_mature_evac_remset2(obj_in_defrag, slot, target);
                }
                super::record_slot_for_validation(slot, Some(target));
            });
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.incs.is_empty() || !self.inc_slices.is_empty() {
            let new_incs = if cfg!(feature = "flush_half") && self.incs.len() > 1 {
                let half = self.incs.len() / 2;
                self.incs.split_off(half)
            } else {
                std::mem::take(&mut self.incs)
            };
            let new_inc_slices = std::mem::take(&mut self.inc_slices);
            let mut w = ProcessIncs::<VM, EDGE_KIND_NURSERY>::new(new_incs, self.lxr);
            w.depth += 1;
            w.inc_slices = new_inc_slices;
            w.inc_slices_count = self.inc_slices_count;
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
            self.incs.reserve(Self::CAPACITY - self.incs.len());
        }
        self.inc_slices_count = 0;
        self.pushes = self.incs.len();
    }

    #[cold]
    fn flush_half_slots(&mut self) {
        let new_incs = if self.incs.len() > 1 {
            let half = self.incs.len() / 2;
            self.incs.split_off(half)
        } else {
            std::mem::take(&mut self.incs)
        };
        let mut w = ProcessIncs::<VM, EDGE_KIND_NURSERY>::new(new_incs, self.lxr);
        w.depth += 1;
        self.worker().add_work(WorkBucketStage::Unconstrained, w);
        self.incs.reserve(Self::CAPACITY - self.incs.len());
        self.pushes = self.incs.len();
    }

    fn inc(&self, o: ObjectReference) -> bool {
        self.rc.inc(o) == Ok(0)
    }

    fn dont_evacuate(&self, o: ObjectReference, los: bool) -> bool {
        if los {
            return true;
        }
        // Skip mature object
        if self.rc.count(o) != 0 {
            return true;
        }
        // Skip recycled lines
        let block = Block::containing::<VM>(o);
        if crate::args::RC_DONT_EVACUATE_NURSERY_IN_RECYCLED_LINES && !block.is_nursery() {
            return true;
        }
        if cfg!(debug_assertions) {
            let cls = unsafe { (o.to_address::<VM>() + 8usize).load::<u32>() };
            assert!(cls != 0, "ERROR {:?} rc={}", o, self.rc.count(o));
        }
        if o.get_size::<VM>() >= crate::args().max_young_evac_size {
            return true;
        }
        false
    }
    #[inline(always)]
    fn process_inc_and_evacuate(&mut self, o: ObjectReference, depth: u32) -> ObjectReference {
        o.verify::<VM>();
        crate::stat(|s| {
            s.inc_objects += 1;
            s.inc_volume += o.get_size::<VM>();
        });
        #[cfg(feature = "measure_rc_rate")]
        {
            self.inc_objs += 1;
        }
        let los = self.lxr.los().in_space(o);
        if !los && object_forwarding::is_forwarded_or_being_forwarded::<VM>(o) {
            while object_forwarding::is_being_forwarded::<VM>(o) {
                std::hint::spin_loop();
            }
            let new = if object_forwarding::is_forwarded::<VM>(o) {
                object_forwarding::read_forwarding_pointer::<VM>(o)
            } else {
                o
            };
            let promoted = self.inc(new);
            if promoted && new == o {
                self.promote(o, false, los, depth);
            }
            return new;
        }
        if self.dont_evacuate(o, los) {
            if self.inc(o) {
                self.promote(o, false, los, depth);
            }
            return o;
        }
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(o);
        if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            // Object is moved to a new location.
            let new = object_forwarding::spin_and_get_forwarded_object::<VM>(o, forwarding_status);
            self.inc(new);
            new
        } else {
            let is_nursery = self.rc.count(o) == 0;
            let copy_depth_reached = crate::args::INC_MAX_COPY_DEPTH && depth > 16;
            if is_nursery && !self.no_evac && !copy_depth_reached {
                // Evacuate the object
                let new = object_forwarding::try_forward_object::<VM>(
                    o,
                    CopySemantics::DefaultCopy,
                    self.copy_context(),
                );
                #[cfg(feature = "measure_rc_rate")]
                {
                    self.copy_objs += 1;
                }
                if let Some(new) = new {
                    self.inc(new);
                    self.promote(new, true, false, depth);
                    new
                } else {
                    gc_log!([1] "to-space overflow");
                    // Object is not moved.
                    let promoted = self.inc(o);
                    object_forwarding::clear_forwarding_bits::<VM>(o);
                    if promoted {
                        self.promote(o, false, los, depth);
                    }
                    crate::NO_EVAC.store(true, Ordering::Relaxed);
                    self.no_evac = true;
                    o
                }
            } else {
                // Object is not moved.
                let promoted = self.inc(o);
                object_forwarding::clear_forwarding_bits::<VM>(o);
                if promoted {
                    self.promote(o, false, los, depth);
                }
                o
            }
        }
    }

    /// Return `None` if the increment of the slot should be delayed
    fn unlog_and_load_rc_object<const K: EdgeKind>(
        &mut self,
        s: VM::VMSlot,
    ) -> Option<ObjectReference> {
        debug_assert!(!crate::args::EAGER_INCREMENTS);
        let o = s.load();
        // unlog slot
        if K == EDGE_KIND_MATURE {
            s.to_address().unlog_field_relaxed::<VM>();
        }
        o
    }

    #[inline(always)]
    fn process_slot<const K: EdgeKind>(
        &mut self,
        s: VM::VMSlot,
        depth: u32,
        add_root_to_remset: bool,
    ) -> Option<ObjectReference> {
        let o = match self.unlog_and_load_rc_object::<K>(s) {
            Some(o) => o,
            _ => {
                super::record_slot_for_validation(s, ObjectReference::NULL);
                return None;
            }
        };
        // println!(" - inc {:?}: {:?} rc={}", s, o, self.rc.count(o));
        o.verify::<VM>();
        let new = self.process_inc_and_evacuate(o, depth);
        // Put this into remset if this is a mature slot, or a weak root
        if K != EDGE_KIND_ROOT || add_root_to_remset {
            self.record_mature_evac_remset(s, new);
        }
        if new != o {
            // gc_log!(
            //     " -- inc {:?}: {:?} => {:?} rc={} {:?}",
            //     s,
            //     o,
            //     new.range::<VM>(),
            //     self.rc.count(new),
            //     K
            // );
            s.store(Some(new))
        } else {
            // gc_log!(
            //     " -- inc {:?}: {:?} rc={} {:?}",
            //     s,
            //     o.range::<VM>(),
            //     self.rc.count(o),
            //     K
            // );
        }
        super::record_slot_for_validation(s, Some(new));
        Some(new)
    }

    #[inline]
    fn prefetch_object(&self, o: ObjectReference) {
        prefetch_object(o, &self.rc);
    }

    fn process_incs<const K: EdgeKind>(
        &mut self,
        mut incs: AddressBuffer<'_, VM::VMSlot>,
        depth: u32,
        add_root_to_remset: bool,
    ) -> Option<Vec<ObjectReference>> {
        if K == EDGE_KIND_ROOT {
            let roots = incs.as_mut_ptr() as *mut ObjectReference;
            let mut num_roots = 0usize;
            for (i, s) in incs.iter().enumerate() {
                if let Some(new) = self.process_slot::<K>(*s, depth, add_root_to_remset) {
                    unsafe {
                        roots.add(num_roots).write(new);
                    }
                    num_roots += 1;
                }
                if crate::args::PREFETCH {
                    if let Some(s) = incs.get(i + crate::args::PREFETCH_STEP) {
                        if let Some(o) = s.load() {
                            self.prefetch_object(o);
                        }
                    }
                }
            }
            if num_roots != 0 {
                let cap = incs.capacity();
                std::mem::forget(incs);
                let roots =
                    unsafe { Vec::<ObjectReference>::from_raw_parts(roots, num_roots, cap) };
                Some(roots)
            } else {
                None
            }
        } else {
            for (i, s) in incs.iter().enumerate() {
                self.process_slot::<K>(*s, depth, false);
                if crate::args::PREFETCH {
                    if let Some(s) = incs.get(i + crate::args::PREFETCH_STEP) {
                        if let Some(o) = s.load() {
                            self.prefetch_object(o);
                        }
                    }
                }
            }
            None
        }
    }

    fn process_incs_for_obj_array<const K: EdgeKind>(
        &mut self,
        slice: VM::VMMemorySlice,
        depth: u32,
    ) -> Option<Vec<ObjectReference>> {
        let n = slice.len();
        for (i, s) in slice.iter_slots().enumerate() {
            self.process_slot::<K>(s, depth, false);
            if crate::args::PREFETCH {
                if i + crate::args::PREFETCH_STEP < n {
                    let s = slice.get(i + crate::args::PREFETCH_STEP);
                    if let Some(o) = s.load() {
                        self.prefetch_object(o);
                    }
                }
            }
        }
        None
    }
}

pub type EdgeKind = u8;
pub const EDGE_KIND_ROOT: u8 = 0;
pub const EDGE_KIND_NURSERY: u8 = 1;
pub const EDGE_KIND_MATURE: u8 = 2;

enum AddressBuffer<'a, S: Slot> {
    Owned(Vec<S>),
    Ref(&'a mut Vec<S>),
}

impl<S: Slot> Deref for AddressBuffer<'_, S> {
    type Target = Vec<S>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(x) => x,
            Self::Ref(x) => x,
        }
    }
}

impl<S: Slot> DerefMut for AddressBuffer<'_, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Owned(x) => x,
            Self::Ref(x) => x,
        }
    }
}

impl<VM: VMBinding, const KIND: EdgeKind> GCWork<VM> for ProcessIncs<VM, KIND> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        #[cfg(any(feature = "log_outstanding_packets", feature = "measure_rc_rate"))]
        let t = std::time::SystemTime::now();

        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        self.lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        self.pause = self.lxr.current_pause().unwrap();
        self.in_cm = self.lxr.concurrent_marking_in_progress();
        self.copy_context = self.worker().get_copy_context_mut() as *mut GCWorkerCopyContext<VM>;
        let count = if cfg!(feature = "rust_mem_counter") {
            self.incs.len()
        } else {
            0
        };
        if crate::NO_EVAC.load(Ordering::Relaxed) {
            self.no_evac = true;
        } else {
            let over_time = crate::args()
                .max_pause_millis
                .map(|threshold| crate::GC_START_TIME.elapsed().as_millis() >= threshold as u128)
                .unwrap_or(false);
            let over_space = mmtk.get_plan().get_used_pages()
                - mmtk.get_plan().get_collection_reserved_pages()
                > mmtk.get_plan().get_total_pages();
            if over_space || over_time {
                self.no_evac = true;
                crate::NO_EVAC.store(true, Ordering::Relaxed);
                gc_log!([2]
                    " - Stop evacuation. over_space={} over_time={}",
                    over_space,
                    over_time
                );
            }
        }
        // Process main buffer
        #[cfg(feature = "lxr_precise_incs_counter")]
        if KIND == EDGE_KIND_ROOT {
            self.stat.roots = self.incs.len();
        }
        let root_slots = if KIND == EDGE_KIND_ROOT
            && (self.pause == Pause::FinalMark || self.pause == Pause::Full)
        {
            self.incs.clone()
        } else {
            vec![]
        };
        let add_root_to_remset = self
            .root_kind
            .map(|r| r.should_record_remset())
            .unwrap_or_default();
        let roots = if KIND != EDGE_KIND_NURSERY {
            let incs = std::mem::take(&mut self.incs);
            self.process_incs::<KIND>(AddressBuffer::Owned(incs), self.depth, false)
        } else {
            None
        };
        if cfg!(debug_assertions) && !self.inc_slices.is_empty() {
            assert!(!add_root_to_remset);
        }
        if let Some(roots) = roots {
            if self.lxr.concurrent_marking_enabled()
                && self.pause == Pause::InitialMark
                && !self.root_kind.unwrap().should_skip_mark_and_decs()
            {
                if cfg!(any(feature = "sanity", debug_assertions)) {
                    for r in &roots {
                        assert!(
                            r.to_address::<VM>().is_mapped(),
                            "Invalid object {:?}: address is not mapped",
                            r
                        );
                    }
                }
                worker
                    .scheduler()
                    .postpone(LXRConcurrentTraceObjects::new(roots.clone(), mmtk));
            }
            if self.pause == Pause::FinalMark || self.pause == Pause::Full {
                if !root_slots.is_empty() {
                    let mut w = LXRStopTheWorldProcessEdges::new(
                        root_slots,
                        true,
                        mmtk,
                        WorkBucketStage::Closure,
                    );
                    w.root_kind = self.root_kind;
                    worker.add_work(WorkBucketStage::Closure, w)
                }
            } else if !self.root_kind.unwrap().should_skip_decs() {
                self.lxr.curr_roots.read().unwrap().push(roots);
            }
        }
        // Process recursively generated buffer
        if self.incs.len() < Self::CAPACITY {
            self.incs.reserve(Self::CAPACITY - self.incs.len());
        }
        if cfg!(feature = "steal") {
            let worker = self.worker();
            let depth = self.depth;
            'outer: loop {
                // depth += 1;
                // Drain local stack
                while let Some(s) = self.incs.pop().or_else(|| worker.deque.pop()) {
                    self.process_slot::<EDGE_KIND_NURSERY>(s, depth, false);
                }
                while let Some(s) = self.inc_slices.pop() {
                    self.inc_slices_count -= s.len() as usize;
                    self.process_incs_for_obj_array::<EDGE_KIND_NURSERY>(s.clone(), self.depth);
                }
                if !self.incs.is_empty() || !worker.deque.is_empty() {
                    continue;
                }
                let workers = &self.worker().scheduler().worker_group.workers_shared;
                if let Steal::Success(w) = self.worker().scheduler().try_steal(worker) {
                    worker.cache = Some(w);
                    break;
                }
                let n = workers.len();
                for _i in 0..n / 2 {
                    const BULK: bool = cfg!(feature = "steal_bulk");
                    if let Some(s) = steal_best_of_2::<VM, BULK>(worker, workers) {
                        self.process_slot::<EDGE_KIND_NURSERY>(s, depth, false);
                        continue 'outer;
                    }
                }
                break;
            }
        } else {
            let mut depth = self.depth;
            while self.stack_size() != 0 {
                depth += 1;
                while let Some(s) = self.incs.pop() {
                    self.process_slot::<EDGE_KIND_NURSERY>(s, depth, false);
                }
                while let Some(s) = self.inc_slices.pop() {
                    self.inc_slices_count -= s.len() as usize;
                    self.process_incs_for_obj_array::<EDGE_KIND_NURSERY>(s.clone(), self.depth);
                }
            }
        }
        self.survival_ratio_predictor_local.sync();
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::INC_BUFFER_COUNTER.sub(count);
        }
        #[cfg(feature = "lxr_precise_incs_counter")]
        {
            crate::RC_STAT.merge(&mut self.stat);
        }

        #[cfg(feature = "log_outstanding_packets")]
        {
            let ms = t.elapsed().unwrap().as_micros() as f32 / 1000f32;
            if ms > 10f32 || cfg!(feature = "log_all_inc_packets") {
                gc_log!(
                        "WARNING: Incs packet took {:.3}ms! KIND={} RootKind={:?} depth={} counters={:?}",
                        ms,
                        KIND,
                        self.root_kind,
                        depth,
                        self.counters,
                    );
            }
        }
        #[cfg(feature = "measure_rc_rate")]
        {
            let us = t.elapsed().unwrap().as_micros() as usize;
            INC_PACKETS_TIME.fetch_add(us, Ordering::SeqCst);
            INC_PACKETS.fetch_add(1, Ordering::SeqCst);
            INC_OBJS.fetch_add(self.inc_objs, Ordering::SeqCst);
            COPY_OBJS.fetch_add(self.copy_objs, Ordering::SeqCst);
        }
    }
}

#[cfg(feature = "measure_rc_rate")]
pub static INC_PACKETS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "measure_rc_rate")]
pub static INC_PACKETS_TIME: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "measure_rc_rate")]
pub static INC_OBJS: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "measure_rc_rate")]
pub static COPY_OBJS: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "measure_rc_rate")]
pub fn dump_rc_rate() {
    gc_log!(
        " - RC-INCS: packets={} total-time={}ms inc-objs={} cp-objs={}",
        INC_PACKETS.load(Ordering::SeqCst),
        INC_PACKETS_TIME.load(Ordering::SeqCst) / 1000,
        INC_OBJS.load(Ordering::SeqCst),
        COPY_OBJS.load(Ordering::SeqCst),
    );
    let t = INC_PACKETS_TIME.load(Ordering::SeqCst) as f32 / 1000f32;
    gc_log!(
        " - RC-INCS-RATE: {:.1}",
        INC_OBJS.load(Ordering::SeqCst) as f32 / t,
    );
    INC_PACKETS.store(0, Ordering::SeqCst);
    INC_PACKETS_TIME.store(0, Ordering::SeqCst);
    INC_OBJS.store(0, Ordering::SeqCst);
    COPY_OBJS.store(0, Ordering::SeqCst);
}

pub struct ProcessDecs<VM: VMBinding> {
    /// Decrements to process
    decs: Option<Vec<ObjectReference>>,
    decs_arc: Option<Arc<Vec<ObjectReference>>>,
    /// Recursively generated new decrements
    stack: Vec<ObjectReference>,
    counter: LazySweepingJobsCounter,
    mark_objects: VectorQueue<ObjectReference>,
    concurrent_marking_in_progress: bool,
    mature_sweeping_in_progress: bool,
    rc: RefCountHelper<VM>,
    pushes: usize,
}

impl<VM: VMBinding> ProcessDecs<VM> {
    pub const CAPACITY: usize = crate::args::BUFFER_SIZE;

    fn worker(&self) -> &mut GCWorker<VM> {
        GCWorker::<VM>::current()
    }

    pub fn new(decs: Vec<ObjectReference>, counter: LazySweepingJobsCounter) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::DEC_BUFFER_COUNTER.add(decs.len());
        }
        Self {
            decs: Some(decs),
            decs_arc: None,
            stack: Default::default(),
            counter,
            mark_objects: VectorQueue::default(),
            concurrent_marking_in_progress: false,
            mature_sweeping_in_progress: false,
            rc: RefCountHelper::NEW,
            pushes: 0,
        }
    }

    pub fn new_arc(decs: Arc<Vec<ObjectReference>>, counter: LazySweepingJobsCounter) -> Self {
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::DEC_BUFFER_COUNTER.add(decs.len());
        }
        Self {
            decs: None,
            decs_arc: Some(decs),
            stack: Default::default(),
            counter,
            mark_objects: VectorQueue::default(),
            concurrent_marking_in_progress: false,
            mature_sweeping_in_progress: false,
            rc: RefCountHelper::NEW,
            pushes: 0,
        }
    }

    fn recursive_dec(&mut self, o: ObjectReference) {
        self.stack.push(o);
        self.pushes += 1;
        if self.stack.len() >= crate::args::BUFFER_SIZE
            || (cfg!(feature = "push") && self.pushes >= 512)
        {
            self.flush()
        }
    }

    fn new_work(&self, lxr: &LXR<VM>, w: ProcessDecs<VM>) {
        if lxr.current_pause().is_none() {
            self.worker()
                .add_work_prioritized(WorkBucketStage::Unconstrained, w);
        } else {
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
        }
    }

    fn flush(&mut self) {
        let mmtk = GCWorker::<VM>::current().mmtk;
        if !self.stack.is_empty() {
            let new_decs = if cfg!(feature = "flush_half") && self.stack.len() > 1 {
                let half = self.stack.len() / 2;
                self.stack.split_off(half)
            } else {
                std::mem::take(&mut self.stack)
            };
            let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
            self.new_work(
                lxr,
                ProcessDecs::new(new_decs, self.counter.clone_with_decs()),
            );
            self.pushes = self.stack.len();
        }
        if !self.mark_objects.is_empty() {
            let objects = self.mark_objects.take();
            let w = LXRConcurrentTraceObjects::new(objects, mmtk);
            if crate::args::LAZY_DECREMENTS {
                self.worker().add_work(WorkBucketStage::Unconstrained, w);
            } else {
                self.worker().scheduler().postpone(w);
            }
        }
    }

    fn record_mature_evac_remset(&mut self, lxr: &LXR<VM>, s: VM::VMSlot, o: ObjectReference) {
        if !(crate::args::RC_MATURE_EVACUATION && self.concurrent_marking_in_progress) {
            return;
        }
        if !lxr.address_in_defrag(s.to_address()) && lxr.in_defrag(o) {
            lxr.immix_space.mature_evac_remset.record(s, o);
        }
    }

    #[cold]
    fn process_dead_object(&mut self, o: ObjectReference, lxr: &LXR<VM>) -> bool {
        crate::stat(|s| {
            s.dead_mature_objects += 1;
            s.dead_mature_volume += o.get_size::<VM>();

            s.dead_mature_rc_objects += 1;
            s.dead_mature_rc_volume += o.get_size::<VM>();

            if !lxr.immix_space.in_space(o) {
                s.dead_mature_los_objects += 1;
                s.dead_mature_los_volume += o.get_size::<VM>();

                s.dead_mature_rc_los_objects += 1;
                s.dead_mature_rc_los_volume += o.get_size::<VM>();
            }
        });
        if self.concurrent_marking_in_progress {
            let marked = lxr.mark(o);
            if cfg!(feature = "lxr_satb_live_bytes_counter") && marked {
                crate::record_live_bytes(o.get_size::<VM>());
            }
        }
        // println!(" - dead {:?}", o);
        // debug_assert_eq!(self::count(o), 0);
        // Recursively decrease field ref counts
        if false
            && VM::VMScanning::is_obj_array(o)
            && VM::VMScanning::obj_array_data(o).bytes() > 1024
        {
            // Buggy. Dead array can be recycled at any time.
            unimplemented!()
        } else if !cfg!(feature = "lxr_no_recursive_dec") {
            o.iterate_fields::<VM, _>(
                CLDScanPolicy::Claim,
                RefScanPolicy::Follow,
                |slot, out_of_heap| {
                    if let Some(x) = slot.load() {
                        // println!(" -- rec dec {:?}.{:?} -> {:?}", o, slot, x);
                        if !out_of_heap {
                            let rc = self.rc.count(x);
                            if rc != MAX_REF_COUNT && rc != 0 {
                                self.recursive_dec(x);
                            }
                        } else {
                            self.record_mature_evac_remset(lxr, slot, x);
                        }
                        if self.concurrent_marking_in_progress && !lxr.is_marked(x) {
                            if cfg!(any(feature = "sanity", debug_assertions)) {
                                assert!(
                                    x.to_address::<VM>().is_mapped(),
                                    "Invalid object {:?}.{:?} -> {:?}: address is not mapped",
                                    o,
                                    slot,
                                    x
                                );
                            }
                            self.mark_objects.push(x);
                            if self.mark_objects.is_full() {
                                self.flush();
                            }
                        }
                    }
                },
            );
        }
        let in_ix_space = lxr.immix_space.in_space(o);
        if !crate::args::BLOCK_ONLY && in_ix_space {
            self.rc.unmark_straddle_object(o);
        }
        if cfg!(feature = "sanity") || ObjectReference::STRICT_VERIFICATION {
            unsafe { o.to_address::<VM>().store(0xdeadusize) };
        }
        if in_ix_space {
            if cfg!(feature = "lxr_log_reclaim") {
                lxr.immix_space
                    .rc_killed_bytes
                    .fetch_add(o.get_size::<VM>(), Ordering::Relaxed);
            }
            let block = Block::containing::<VM>(o);
            lxr.immix_space
                .add_to_possibly_dead_mature_blocks(block, false);
            false
        } else {
            if cfg!(feature = "lxr_log_reclaim") {
                lxr.los()
                    .rc_killed_bytes
                    .fetch_add(o.get_size::<VM>(), Ordering::Relaxed);
            }
            true
        }
    }

    #[inline]
    fn prefetch_object(&self, o: ObjectReference) {
        prefetch_object(o, &self.rc);
    }

    #[inline]
    fn process_dec(&mut self, o: ObjectReference, lxr: &LXR<VM>) {
        if self.rc.is_dead_or_stuck(o) || (self.mature_sweeping_in_progress && !lxr.is_marked(o)) {
            return;
        }
        let o = if crate::args::RC_MATURE_EVACUATION && object_forwarding::is_forwarded::<VM>(o) {
            object_forwarding::read_forwarding_pointer::<VM>(o)
        } else {
            o
        };
        let mut dead = false;
        let mut is_los = false;
        let result = self.rc.clone().fetch_update(o, |c| {
            if c == 1 && !dead {
                dead = true;
                is_los = self.process_dead_object(o, lxr);
            }
            debug_assert!(c <= MAX_REF_COUNT);
            if c == 0 || c == MAX_REF_COUNT {
                None /* sticky */
            } else {
                Some(c - 1)
            }
        });
        if result == Ok(1) && is_los {
            lxr.los().rc_free(o);
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessDecs<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        if cfg!(feature = "lxr_no_decs") {
            return;
        }
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        self.concurrent_marking_in_progress = lxr.concurrent_marking_in_progress()
            || (!crate::args::LAZY_DECREMENTS && lxr.current_pause() == Some(Pause::InitialMark));
        self.mature_sweeping_in_progress = if crate::args::LAZY_DECREMENTS {
            lxr.previous_pause() == Some(Pause::FinalMark)
                || lxr.current_pause() == Some(Pause::Full)
        } else {
            lxr.current_pause() == Some(Pause::FinalMark)
                || lxr.current_pause() == Some(Pause::Full)
        };
        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        let count = if cfg!(feature = "rust_mem_counter") {
            self.decs.as_ref().map(|x| x.len()).unwrap_or(0)
                + self.decs_arc.as_ref().map(|x| x.len()).unwrap_or(0)
        } else {
            0
        };
        if let Some(decs) = std::mem::take(&mut self.decs) {
            for o in decs {
                self.process_dec(o, lxr);
            }
        } else if let Some(decs) = std::mem::take(&mut self.decs_arc) {
            for o in &*decs {
                self.process_dec(*o, lxr);
            }
        }
        if cfg!(feature = "steal") {
            let worker = GCWorker::<VM>::current();
            'outer: loop {
                // Drain local stack
                while let Some(o) = self.stack.pop().or_else(|| worker.obj_deque.pop()) {
                    self.process_dec(o, lxr);
                }
                if !self.stack.is_empty() || !worker.obj_deque.is_empty() {
                    continue;
                }
                let workers = &self.worker().scheduler().worker_group.workers_shared;
                if let Steal::Success(w) = self.worker().scheduler().try_steal(worker) {
                    worker.cache = Some(w);
                    break;
                }
                let n = workers.len();
                for _i in 0..n / 2 {
                    const BULK: bool = cfg!(feature = "steal_bulk");
                    if let Some(o) = steal_best_of_2_obj::<VM, BULK>(worker, workers) {
                        self.process_dec(o, lxr);
                        continue 'outer;
                    }
                }
                break;
            }
        } else {
            while let Some(o) = self.stack.pop() {
                self.process_dec(o, lxr);
            }
        }
        self.flush();
        if cfg!(feature = "rust_mem_counter") {
            crate::rust_mem_counter::DEC_BUFFER_COUNTER.sub(count);
        }
    }
}

pub struct RCImmixCollectRootEdges<VM: VMBinding> {
    base: ProcessEdgesBase<VM>,
}

impl<VM: VMBinding> ProcessEdgesWork for RCImmixCollectRootEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = false;
    const RC_ROOTS: bool = true;
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        debug_assert!(roots);
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        Self { base }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    fn process_slots(&mut self) {
        if !self.slots.is_empty() {
            #[cfg(feature = "sanity")]
            if self.roots
                && !self.mmtk().get_plan().is_in_sanity()
                && (cfg!(feature = "fragmentation_analysis") || crate::frag_exp_enabled())
                && self.root_kind != Some(RootKind::Weak)
            {
                self.cache_roots_for_sanity_gc(self.slots.clone());
            }
            let lxr = self.mmtk().get_plan().downcast_ref::<LXR<VM>>().unwrap();
            let roots = std::mem::take(&mut self.slots);
            let mut w = ProcessIncs::<_, EDGE_KIND_ROOT>::new(roots, lxr);
            w.root_kind = self.root_kind;
            GCWork::do_work(&mut w, self.worker(), self.mmtk());
        }
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType {
        unimplemented!()
    }
}

impl<VM: VMBinding> Deref for RCImmixCollectRootEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for RCImmixCollectRootEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

fn steal_best_of_2<VM: VMBinding, const BULK: bool>(
    worker: &GCWorker<VM>,
    workers: &[Arc<GCWorkerShared<VM>>],
) -> Option<VM::VMSlot> {
    let n = workers.len();
    if n > 2 {
        let (k1, k2) =
            GCWorkScheduler::<VM>::get_random_steal_index(worker.ordinal, &worker.hash_seed, n);
        let sz1 = workers[k1].deque_stealer.size();
        let sz2 = workers[k2].deque_stealer.size();
        if sz1 < sz2 {
            return workers[k2]
                .deque_stealer
                .steal_and_pop(&worker.deque, |n| if BULK { n / 2 } else { 1 })
                .ok()
                .map(|x| x.0);
        } else {
            return workers[k1]
                .deque_stealer
                .steal_and_pop(&worker.deque, |n| if BULK { n / 2 } else { 1 })
                .ok()
                .map(|x| x.0);
        }
    } else if n == 2 {
        let k = (worker.ordinal + 1) % 2;
        return workers[k]
            .deque_stealer
            .steal_and_pop(&worker.deque, |n| if BULK { n / 2 } else { 1 })
            .ok()
            .map(|x| x.0);
    } else {
        return None;
    }
}

fn steal_best_of_2_obj<VM: VMBinding, const BULK: bool>(
    worker: &GCWorker<VM>,
    workers: &[Arc<GCWorkerShared<VM>>],
) -> Option<ObjectReference> {
    let n = workers.len();
    if n > 2 {
        let (k1, k2) =
            GCWorkScheduler::<VM>::get_random_steal_index(worker.ordinal, &worker.hash_seed, n);
        let sz1 = workers[k1].obj_deque_stealer.size();
        let sz2 = workers[k2].obj_deque_stealer.size();
        if sz1 < sz2 {
            return workers[k2]
                .obj_deque_stealer
                .steal_and_pop(&worker.obj_deque, |n| if BULK { n / 2 } else { 1 })
                .ok()
                .map(|x| x.0);
        } else {
            return workers[k1]
                .obj_deque_stealer
                .steal_and_pop(&worker.obj_deque, |n| if BULK { n / 2 } else { 1 })
                .ok()
                .map(|x| x.0);
        }
    } else if n == 2 {
        let k = (worker.ordinal + 1) % 2;
        return workers[k]
            .obj_deque_stealer
            .steal_and_pop(&worker.obj_deque, |n| if BULK { n / 2 } else { 1 })
            .ok()
            .map(|x| x.0);
    } else {
        return None;
    }
}
