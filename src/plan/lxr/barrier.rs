//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use atomic::Ordering;

use super::LXR;
use crate::plan::barriers::BarrierSemantics;
use crate::plan::barriers::LOGGED_VALUE;
use crate::plan::immix::Pause;
use crate::plan::lxr::cm::ProcessModBufSATB;
use crate::plan::lxr::rc::ProcessDecs;
use crate::plan::lxr::rc::ProcessIncs;
use crate::plan::lxr::rc::EDGE_KIND_MATURE;
use crate::plan::VectorQueue;
use crate::scheduler::WorkBucketStage;
use crate::util::address::CLDScanPolicy;
use crate::util::address::RefScanPolicy;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::rc::RC_LOCK_BITS;
use crate::util::*;
use crate::vm::edge_shape::Edge;
use crate::vm::edge_shape::MemorySlice;
use crate::vm::*;
use crate::LazySweepingJobsCounter;
use crate::MMTK;

pub const TAKERATE_MEASUREMENT: bool = crate::args::TAKERATE_MEASUREMENT;
pub static FAST_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static SLOW_COUNT: AtomicUsize = AtomicUsize::new(0);

pub const UNLOCKED_VALUE: u8 = 0b0;
pub const LOCKED_VALUE: u8 = 0b1;

pub struct LXRFieldBarrierSemantics<VM: VMBinding> {
    mmtk: &'static MMTK<VM>,
    incs: VectorQueue<VM::VMEdge>,
    decs: VectorQueue<ObjectReference>,
    refs: VectorQueue<ObjectReference>,
    lxr: &'static LXR<VM>,
}

impl<VM: VMBinding> LXRFieldBarrierSemantics<VM> {
    const UNLOG_BITS: SideMetadataSpec = *<VM as VMBinding>::VMObjectModel::GLOBAL_LOG_BIT_SPEC
        .as_spec()
        .extract_side_spec();
    const LOCK_BITS: SideMetadataSpec = RC_LOCK_BITS;

    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        Self {
            mmtk,
            incs: VectorQueue::default(),
            decs: VectorQueue::default(),
            refs: VectorQueue::default(),
            lxr: mmtk.plan.downcast_ref::<LXR<VM>>().unwrap(),
        }
    }

    #[inline(always)]
    fn get_edge_logging_state(&self, edge: VM::VMEdge) -> u8 {
        unsafe { Self::UNLOG_BITS.load(edge.to_address()) }
    }

    #[inline(always)]
    fn attempt_to_lock_edge_bailout_if_logged(&self, edge: VM::VMEdge) -> bool {
        loop {
            // Bailout if logged
            if self.get_edge_logging_state(edge) == LOGGED_VALUE {
                return false;
            }
            // Attempt to lock the edges
            if Self::LOCK_BITS
                .compare_exchange_atomic(
                    edge.to_address(),
                    UNLOCKED_VALUE,
                    LOCKED_VALUE,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                if self.get_edge_logging_state(edge) == LOGGED_VALUE {
                    self.unlock_edge(edge);
                    return false;
                }
                return true;
            }
            // Failed to lock the edge. Spin.
            std::hint::spin_loop();
        }
    }

    #[inline(always)]
    fn unlock_edge(&self, edge: VM::VMEdge) {
        RC_LOCK_BITS.store_atomic(edge.to_address(), UNLOCKED_VALUE, Ordering::Relaxed);
    }

    #[inline(always)]
    fn log_and_unlock_edge(&self, edge: VM::VMEdge) {
        if (1 << crate::args::LOG_BYTES_PER_RC_LOCK_BIT) >= 64 {
            unsafe { Self::UNLOG_BITS.store(edge.to_address(), LOGGED_VALUE) };
        } else {
            Self::UNLOG_BITS.store_atomic(edge.to_address(), LOGGED_VALUE, Ordering::Relaxed);
        }
        RC_LOCK_BITS.store_atomic(edge.to_address(), UNLOCKED_VALUE, Ordering::Relaxed);
    }

    #[inline(always)]
    fn log_edge_and_get_old_target(&self, edge: VM::VMEdge) -> Result<ObjectReference, ()> {
        if self.attempt_to_lock_edge_bailout_if_logged(edge) {
            let old = if VM::VMObjectModel::compressed_pointers_enabled() {
                edge.load::<true>()
            } else {
                edge.load::<false>()
            };
            self.log_and_unlock_edge(edge);
            Ok(old)
        } else {
            Err(())
        }
    }

    #[inline(always)]
    #[allow(unused)]
    fn log_edge_and_get_old_target_sloppy(&self, edge: VM::VMEdge) -> Result<ObjectReference, ()> {
        if !edge.to_address().is_logged::<VM>() {
            let old = if VM::VMObjectModel::compressed_pointers_enabled() {
                edge.load::<true>()
            } else {
                edge.load::<false>()
            };
            edge.to_address().log::<VM>();
            Ok(old)
        } else {
            Err(())
        }
    }

    #[inline(always)]
    fn slow(&mut self, _src: ObjectReference, edge: VM::VMEdge, old: ObjectReference) {
        #[cfg(any(
            feature = "sanity",
            feature = "field_barrier_validation",
            debug_assertions
        ))]
        assert!(
            old.is_null() || crate::util::rc::count(old) != 0,
            "zero rc count {:?} -> {:?}",
            edge,
            old
        );
        if cfg!(feature = "field_barrier_validation") {
            let o = super::LAST_REFERENTS
                .lock()
                .unwrap()
                .get(&edge.to_address())
                .cloned()
                .expect(&format!("Unknown edge {:?} -> {:?}", edge, old));
            if old != o {
                println!("barrier {:?} old={:?}", edge, old);
                {
                    let _g = super::LAST_REFERENTS.lock();
                    println!("{:?} {}", old, VM::VMObjectModel::dump_object_s(old));
                    println!("{:?} {}", _src, VM::VMObjectModel::dump_object_s(_src));
                }
                assert!(
                    old == o,
                    "Untracked old referent {:?} -> {:?} should be {:?}  ",
                    edge,
                    old,
                    o,
                )
            }
        }
        // Reference counting
        if !old.is_null() {
            self.decs.push(old);
            if self.decs.is_full() {
                self.flush_decs_and_satb();
            }
        }
        crate::util::rc::inc_inc_buffer_size();
        self.incs.push(edge);
        if self.incs.is_full() {
            self.flush_incs();
        }
    }

    #[inline(always)]
    fn enqueue_node(
        &mut self,
        src: ObjectReference,
        edge: VM::VMEdge,
        _new: Option<ObjectReference>,
    ) {
        if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
            FAST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if let Ok(old) = self.log_edge_and_get_old_target(edge) {
            if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
                SLOW_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            self.slow(src, edge, old)
        }
    }

    #[inline(always)]
    fn should_create_satb_packets(&self) -> bool {
        self.lxr.concurrent_marking_enabled()
            && (self.lxr.concurrent_marking_in_progress()
                || self.lxr.current_pause() == Some(Pause::FinalMark))
    }

    #[cold]
    fn flush_incs(&mut self) {
        if !self.incs.is_empty() {
            let incs = self.incs.take();
            if VM::VMObjectModel::compressed_pointers_enabled() {
                self.mmtk.scheduler.work_buckets[WorkBucketStage::RCProcessIncs]
                    .add(ProcessIncs::<_, EDGE_KIND_MATURE, true>::new(incs));
            } else {
                self.mmtk.scheduler.work_buckets[WorkBucketStage::RCProcessIncs]
                    .add(ProcessIncs::<_, EDGE_KIND_MATURE, false>::new(incs));
            }
        }
    }

    #[cold]
    fn flush_decs_and_satb(&mut self) {
        if VM::VMObjectModel::compressed_pointers_enabled() {
            self.flush_decs_and_satb_impl::<true>()
        } else {
            self.flush_decs_and_satb_impl::<false>()
        }
    }

    fn flush_decs_and_satb_impl<const COMPRESSED: bool>(&mut self) {
        if !self.decs.is_empty() {
            let w = if self.should_create_satb_packets() {
                let decs = Arc::new(self.decs.take());
                self.mmtk.scheduler.work_buckets[WorkBucketStage::Initial]
                    .add(ProcessModBufSATB::new_arc(decs.clone()));
                ProcessDecs::<_, COMPRESSED>::new_arc(decs, LazySweepingJobsCounter::new_desc())
            } else {
                let decs = self.decs.take();
                ProcessDecs::<_, COMPRESSED>::new(decs, LazySweepingJobsCounter::new_desc())
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
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Initial]
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
    }

    fn object_reference_write_slow(
        &mut self,
        src: ObjectReference,
        slot: VM::VMEdge,
        target: ObjectReference,
    ) {
        self.enqueue_node(src, slot, Some(target));
    }

    fn memory_region_copy_slow(&mut self, _src: VM::VMMemorySlice, dst: VM::VMMemorySlice) {
        for e in dst.iter_edges() {
            self.enqueue_node(ObjectReference::NULL, e, None);
        }
    }

    fn load_reference(&mut self, o: ObjectReference) {
        self.refs.push(o);
        if self.refs.is_full() {
            self.flush_weak_refs();
        }
    }

    fn object_reference_clone_pre(&mut self, obj: ObjectReference) {
        let compressed = VM::VMObjectModel::compressed_pointers_enabled();
        if compressed {
            obj.iterate_fields::<VM, _, true>(CLDScanPolicy::Ignore, RefScanPolicy::Follow, |e| {
                if !e.to_address().is_mapped() {
                    return;
                }
                self.enqueue_node(obj, e, None);
            })
        } else {
            obj.iterate_fields::<VM, _, false>(CLDScanPolicy::Ignore, RefScanPolicy::Follow, |e| {
                if !e.to_address().is_mapped() {
                    return;
                }
                self.enqueue_node(obj, e, None);
            })
        }
    }
}
