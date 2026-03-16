//! Read/Write barrier implementations.

use std::sync::Arc;

use atomic::Ordering;

use super::LXR;
use crate::plan::barriers::BarrierSemantics;
use crate::plan::barriers::{FAST_COUNT, SLOW_COUNT};
use crate::plan::immix::Pause;
use crate::plan::lxr::cm::ProcessModBufSATB;
use crate::plan::lxr::rc::ProcessDecs;
use crate::plan::lxr::rc::ProcessIncs;
use crate::plan::lxr::rc::EDGE_KIND_MATURE;
use crate::plan::VectorQueue;
#[cfg(feature = "lxr_precise_incs_counter")]
use crate::policy::space::Space;
use crate::scheduler::WorkBucketStage;
use crate::util::address::CLDScanPolicy;
use crate::util::address::RefScanPolicy;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::*;
use crate::vm::slot::MemorySlice;
use crate::vm::slot::Slot;
use crate::vm::*;
use crate::LazySweepingJobsCounter;
use crate::MMTK;

pub const TAKERATE_MEASUREMENT: bool = crate::args::TAKERATE_MEASUREMENT;

pub struct LXRFieldBarrierSemantics<VM: VMBinding> {
    mmtk: &'static MMTK<VM>,
    incs: VectorQueue<VM::VMSlot>,
    decs: VectorQueue<ObjectReference>,
    refs: VectorQueue<ObjectReference>,
    lxr: &'static LXR<VM>,
    #[cfg(feature = "lxr_precise_incs_counter")]
    stat: crate::LocalRCStat,
}

impl<VM: VMBinding> LXRFieldBarrierSemantics<VM> {
    const UNLOG_BITS: SideMetadataSpec = *VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
        .as_spec()
        .extract_side_spec();

    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        Self {
            mmtk,
            incs: VectorQueue::default(),
            decs: VectorQueue::default(),
            refs: VectorQueue::default(),
            lxr: mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap(),
            #[cfg(feature = "lxr_precise_incs_counter")]
            stat: crate::LocalRCStat::default(),
        }
    }

    fn log_slot_and_get_old_target(&self, slot: VM::VMSlot) -> Option<Option<ObjectReference>> {
        let bit_ref = Self::UNLOG_BITS.extract_bit_location(slot.to_address());
        let byte = bit_ref.load_raw_byte();
        if byte == 0 {
            return None;
        }
        let mask: u8 = 1 << bit_ref.lshift;
        if (byte & mask) == 0 {
            return None;
        }
        let old = slot.load();
        let r = bit_ref.fetch_update(Ordering::SeqCst, Ordering::Relaxed, |byte| {
            if byte == 0 {
                None
            } else if (byte & mask) == 0 {
                None
            } else {
                Some(byte & !mask)
            }
        });
        if r.is_ok() {
            Some(old)
        } else {
            None
        }
    }

    fn slow(
        &mut self,
        _src: Option<ObjectReference>,
        slot: VM::VMSlot,
        old: Option<ObjectReference>,
    ) {
        // FIXME: This assertion may fail!
        // #[cfg(any(
        //     feature = "sanity",
        //     feature = "field_barrier_validation",
        //     debug_assertions
        // ))]
        // debug_assert!(
        //     old.is_null() || self.lxr.rc.count(old) != 0,
        //     "zero rc count {:?} -> {:?}",
        //     slot,
        //     old
        // );
        if cfg!(feature = "field_barrier_validation") {
            let o = super::LAST_REFERENTS
                .lock()
                .unwrap()
                .get(&slot.to_address())
                .cloned()
                .expect(&format!("Unknown slot {:?} -> {:?}", slot, old));
            if old != o {
                println!("barrier {:?} old={:?}", slot, old);
                {
                    let _g = super::LAST_REFERENTS.lock();
                    // println!("{:?} {}", old, VM::VMObjectModel::dump_object_s(old));
                    // println!("{:?} {}", _src, VM::VMObjectModel::dump_object_s(_src));
                }
                assert!(
                    old == o,
                    "Untracked old referent {:?} -> {:?} should be {:?}  ",
                    slot,
                    old,
                    o,
                )
            }
        }
        // Reference counting
        if let Some(old) = old {
            if !cfg!(feature = "lxr_no_decs") {
                self.decs.push(old);
                if self.decs.is_full() {
                    self.flush_decs_and_satb();
                }
            } else if !self.lxr.is_marked(old) {
                self.decs.push(old);
                if self.decs.is_full() {
                    self.flush_decs_and_satb();
                }
            }
        }
        self.incs.push(slot);
        #[cfg(feature = "lxr_precise_incs_counter")]
        {
            self.stat.total_incs += 1;
            if self.lxr.los().address_in_space(slot.to_address()) {
                self.stat.los_incs += 1;
            }
        }
        if self.incs.is_full() {
            self.flush_incs();
        }
    }

    fn enqueue_node(&mut self, src: Option<ObjectReference>, slot: VM::VMSlot) -> bool {
        if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
            FAST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(old) = self.log_slot_and_get_old_target(slot) {
            if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
                SLOW_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            self.slow(src, slot, old);
            true
        } else {
            false
        }
    }

    fn should_create_satb_packets(&self) -> bool {
        self.lxr.cm_enabled()
            && (self.lxr.cm_in_progress() || self.lxr.current_pause() == Some(Pause::FinalMark))
    }

    #[cold]
    fn flush_incs(&mut self) {
        if !self.incs.is_empty() {
            let incs = self.incs.take();
            self.lxr.rc.increase_inc_buffer_size(incs.len());
            self.mmtk.scheduler.work_buckets[WorkBucketStage::RCProcessIncs].add(ProcessIncs::<
                _,
                EDGE_KIND_MATURE,
            >::new(
                incs, self.lxr
            ));
        }
    }

    #[cold]
    fn flush_decs_and_satb(&mut self) {
        if !self.decs.is_empty() {
            if cfg!(feature = "decs_counter") {
                self.lxr
                    .barrier_decs
                    .fetch_add(self.decs.len(), Ordering::SeqCst);
            }
            let w = if self.should_create_satb_packets() {
                let decs = Arc::new(self.decs.take());
                self.mmtk.scheduler.work_buckets[WorkBucketStage::FinishConcurrentWork]
                    .add(ProcessModBufSATB::new_arc(decs.clone()));
                ProcessDecs::new_arc(decs, LazySweepingJobsCounter::new_decs())
            } else {
                let decs = self.decs.take();
                ProcessDecs::new(decs, LazySweepingJobsCounter::new_decs())
            };
            if crate::args::LAZY_DECREMENTS {
                self.mmtk.scheduler.postpone_prioritized(w);
            } else {
                self.mmtk.scheduler.work_buckets[WorkBucketStage::STWRCDecsAndSweep].add(w);
            }
        }
    }

    #[cold]
    fn flush_weak_refs(&mut self) {
        if !self.refs.is_empty() {
            debug_assert!(self.should_create_satb_packets());
            let nodes = self.refs.take();
            self.mmtk.scheduler.work_buckets[WorkBucketStage::FinishConcurrentWork]
                .add(ProcessModBufSATB::new(nodes));
        }
    }
}

impl<VM: VMBinding> BarrierSemantics for LXRFieldBarrierSemantics<VM> {
    type VM = VM;

    #[cold]
    fn flush(&mut self) {
        self.flush_weak_refs();
        self.flush_incs();
        self.flush_decs_and_satb();
        #[cfg(feature = "lxr_precise_incs_counter")]
        {
            crate::RC_STAT.merge(&mut self.stat);
        }
    }

    fn object_reference_write_slow(
        &mut self,
        src: Option<ObjectReference>,
        slot: VM::VMSlot,
        _target: Option<ObjectReference>,
    ) {
        self.enqueue_node(src, slot);
    }

    fn memory_region_copy_slow(&mut self, _src: VM::VMMemorySlice, dst: VM::VMMemorySlice) {
        #[cfg(feature = "lxr_precise_incs_counter")]
        let mut slots = 0;
        for s in dst.iter_slots() {
            let _succ = self.enqueue_node(ObjectReference::NULL, s);
            #[cfg(feature = "lxr_precise_incs_counter")]
            if _succ {
                slots += 1;
            }
        }
        #[cfg(feature = "lxr_precise_incs_counter")]
        {
            self.stat.ac_incs += slots;
            self.stat.ac_calls += 1;
            if self.lxr.los().address_in_space(dst.start()) {
                self.stat.los_ac_incs += slots;
                self.stat.los_ac_calls += 1;
            }
        }
    }

    fn load_weak_reference(&mut self, o: ObjectReference) {
        if !self.lxr.cm_in_progress() || self.lxr.is_marked(o) {
            return;
        }
        self.refs.push(o);
        if self.refs.is_full() {
            self.flush_weak_refs();
        }
    }

    fn object_probable_write_slow(&mut self, obj: ObjectReference) {
        // assert_eq!(self.lxr.rc.count(obj), 1);
        #[cfg(feature = "lxr_precise_incs_counter")]
        let mut slots = 0;
        obj.iterate_fields::<VM, _>(CLDScanPolicy::Ignore, RefScanPolicy::Follow, |s, _| {
            let _succ = self.enqueue_node(Some(obj), s);
            #[cfg(feature = "lxr_precise_incs_counter")]
            {
                assert!(_succ);
                slots += 1;
            }
        });
        #[cfg(feature = "lxr_precise_incs_counter")]
        {
            self.stat.opw_calls += 1;
            self.stat.opw_incs += slots;
            if self.lxr.los().in_space(obj) {
                self.stat.los_opw_calls += 1;
                self.stat.los_opw_incs += slots;
            }
        }
    }
}
