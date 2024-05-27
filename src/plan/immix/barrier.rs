//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;

use atomic::Ordering;

use crate::plan::barriers::BarrierSemantics;
use crate::plan::barriers::LOGGED_VALUE;
use crate::plan::VectorQueue;
use crate::scheduler::gc_work::DummyPacket;
use crate::scheduler::gc_work::UnlogEdges;
use crate::scheduler::WorkBucketStage;
use crate::util::address::CLDScanPolicy;
use crate::util::address::RefScanPolicy;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::rc::RC_LOCK_BITS;
use crate::util::*;
use crate::vm::edge_shape::Edge;
use crate::vm::edge_shape::MemorySlice;
use crate::vm::*;
use crate::MMTK;

pub const TAKERATE_MEASUREMENT: bool = crate::args::TAKERATE_MEASUREMENT;
pub static FAST_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static SLOW_COUNT: AtomicUsize = AtomicUsize::new(0);

pub const UNLOCKED_VALUE: u8 = 0b0;
pub const LOCKED_VALUE: u8 = 0b1;

pub struct ImmixFakeFieldBarrierSemantics<VM: VMBinding> {
    mmtk: &'static MMTK<VM>,
    incs: VectorQueue<VM::VMEdge>,
    decs: VectorQueue<ObjectReference>,
    refs: VectorQueue<ObjectReference>,
}

impl<VM: VMBinding> ImmixFakeFieldBarrierSemantics<VM> {
    pub(super) const UNLOG_BITS: SideMetadataSpec = *VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
        .as_spec()
        .extract_side_spec();
    pub(super) const LOCK_BITS: SideMetadataSpec = RC_LOCK_BITS;

    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        Self {
            mmtk,
            incs: VectorQueue::default(),
            decs: VectorQueue::default(),
            refs: VectorQueue::default(),
        }
    }

    fn get_edge_logging_state(&self, edge: VM::VMEdge) -> u8 {
        unsafe { Self::UNLOG_BITS.load(edge.to_address()) }
    }

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

    fn unlock_edge(&self, edge: VM::VMEdge) {
        RC_LOCK_BITS.store_atomic(edge.to_address(), UNLOCKED_VALUE, Ordering::Relaxed);
    }

    fn log_and_unlock_edge(&self, edge: VM::VMEdge) {
        let heap_bytes_per_unlog_byte = if VM::VMObjectModel::COMPRESSED_PTR_ENABLED {
            32usize
        } else {
            64
        };
        if (1 << crate::args::LOG_BYTES_PER_RC_LOCK_BIT) >= heap_bytes_per_unlog_byte {
            unsafe { Self::UNLOG_BITS.store(edge.to_address(), LOGGED_VALUE) };
        } else {
            Self::UNLOG_BITS.store_atomic(edge.to_address(), LOGGED_VALUE, Ordering::Relaxed);
        }
        RC_LOCK_BITS.store_atomic(edge.to_address(), UNLOCKED_VALUE, Ordering::Relaxed);
    }

    fn log_edge_and_get_old_target(&self, edge: VM::VMEdge) -> Result<ObjectReference, ()> {
        if self.attempt_to_lock_edge_bailout_if_logged(edge) {
            let old = edge.load();
            self.log_and_unlock_edge(edge);
            Ok(old)
        } else {
            Err(())
        }
    }

    #[allow(unused)]
    fn log_edge_and_get_old_target_sloppy(&self, edge: VM::VMEdge) -> Result<ObjectReference, ()> {
        if !edge.to_address().is_field_logged::<VM>() {
            let old = edge.load();
            edge.to_address().log_field::<VM>();
            Ok(old)
        } else {
            Err(())
        }
    }

    fn slow(&mut self, _src: ObjectReference, edge: VM::VMEdge, old: ObjectReference) {
        if !old.is_null() {
            self.decs.push(old);
            if self.decs.is_full() {
                self.flush_decs();
            }
        }
        self.incs.push(edge);
        if self.incs.is_full() {
            self.flush_incs();
        }
    }

    fn enqueue_node(
        &mut self,
        src: ObjectReference,
        edge: VM::VMEdge,
        _new: Option<ObjectReference>,
    ) {
        if crate::args::BARRIER_MEASUREMENT_NO_SLOW {
            return;
        }
        if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
            FAST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if let Ok(old) = self.log_edge_and_get_old_target(edge) {
            if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
                SLOW_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            self.slow(src, edge, old);
        }
    }

    #[cold]
    fn flush_incs(&mut self) {
        if !self.incs.is_empty() {
            let incs = self.incs.take();
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Prepare].add(UnlogEdges(incs));
        }
    }

    #[cold]
    fn flush_decs(&mut self) {
        if !self.refs.is_empty() {
            let decs = self.decs.take();
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Prepare].add(DummyPacket(decs));
        }
    }

    #[cold]
    fn flush_weak_refs(&mut self) {
        if !self.refs.is_empty() {
            let nodes = self.refs.take();
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Prepare].add(DummyPacket(nodes));
        }
    }
}

impl<VM: VMBinding> BarrierSemantics for ImmixFakeFieldBarrierSemantics<VM> {
    type VM = VM;

    #[cold]
    fn flush(&mut self) {
        self.flush_weak_refs();
        self.flush_incs();
        self.flush_decs();
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
        if crate::args::BARRIER_MEASUREMENT_NO_SLOW {
            return;
        }
        self.refs.push(o);
        if self.refs.is_full() {
            self.flush_weak_refs();
        }
    }

    fn object_probable_write_slow(&mut self, obj: ObjectReference) {
        obj.iterate_fields::<VM, _>(CLDScanPolicy::Ignore, RefScanPolicy::Follow, |e, _| {
            self.enqueue_node(obj, e, None);
        })
    }
}
