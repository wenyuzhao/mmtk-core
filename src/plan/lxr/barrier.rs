//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;

use atomic::Ordering;

use super::LXR;
use crate::plan::barriers::Barrier;
use crate::plan::barriers::BarrierWriteTarget;
use crate::plan::barriers::LOGGED_VALUE;
use crate::plan::immix::Pause;
use crate::plan::lxr::cm::ProcessModBufSATB;
use crate::plan::lxr::rc::ProcessDecs;
use crate::plan::lxr::rc::ProcessIncs;
use crate::plan::lxr::rc::EDGE_KIND_MATURE;
use crate::plan::EdgeIterator;
use crate::scheduler::gc_work::*;
use crate::scheduler::WorkBucketStage;
use crate::util::metadata::load_metadata;
use crate::util::metadata::side_metadata::compare_exchange_atomic2;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::metadata::store_metadata;
use crate::util::metadata::MetadataSpec;
use crate::util::rc::RC_LOCK_BIT_SPEC;
use crate::util::*;
use crate::vm::*;
use crate::LazySweepingJobsCounter;
use crate::MMTK;

pub const TAKERATE_MEASUREMENT: bool = crate::args::TAKERATE_MEASUREMENT;
pub static FAST_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static SLOW_COUNT: AtomicUsize = AtomicUsize::new(0);

pub const UNLOCKED_VALUE: usize = 0b0;
pub const LOCKED_VALUE: usize = 0b1;

pub struct FieldLoggingBarrier<E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
    edges: Vec<Address>,
    nodes: Vec<ObjectReference>,
    incs: Vec<Address>,
    decs: Vec<ObjectReference>,
    lxr: &'static LXR<E::VM>,
}

impl<E: ProcessEdgesWork> FieldLoggingBarrier<E> {
    const CAPACITY: usize = crate::args::BUFFER_SIZE;
    const UNLOG_BITS: MetadataSpec =
        *<E::VM as VMBinding>::VMObjectModel::GLOBAL_LOG_BIT_SPEC.as_spec();
    const LOCK_BITS: SideMetadataSpec = *RC_LOCK_BIT_SPEC.extract_side_spec();

    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<E::VM>, meta: MetadataSpec) -> Self {
        Self {
            mmtk,
            edges: vec![],
            nodes: vec![],
            incs: Vec::with_capacity(Self::CAPACITY),
            decs: Vec::with_capacity(Self::CAPACITY),
            lxr: mmtk.plan.downcast_ref::<LXR<E::VM>>().unwrap(),
        }
    }

    #[inline(always)]
    fn get_edge_logging_state(&self, edge: Address) -> usize {
        load_metadata::<E::VM>(
            &Self::UNLOG_BITS,
            unsafe { edge.to_object_reference() },
            None,
            None,
        )
    }

    #[inline(always)]
    fn attempt_to_lock_edge_bailout_if_logged(&self, edge: Address) -> bool {
        loop {
            // Bailout if logged
            if self.get_edge_logging_state(edge) == LOGGED_VALUE {
                return false;
            }
            // Attempt to lock the edges
            if compare_exchange_atomic2(
                &Self::LOCK_BITS,
                edge,
                UNLOCKED_VALUE,
                LOCKED_VALUE,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                if self.get_edge_logging_state(edge) == LOGGED_VALUE {
                    self.unlock_edge(edge);
                    return false;
                }
                return true;
            }
            // Failed to lock the edge. Spin.
        }
    }

    #[inline(always)]
    fn unlock_edge(&self, edge: Address) {
        store_metadata::<E::VM>(
            &RC_LOCK_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            UNLOCKED_VALUE,
            None,
            Some(Ordering::Relaxed),
        );
    }

    #[inline(always)]
    fn log_and_unlock_edge(&self, edge: Address) {
        store_metadata::<E::VM>(
            &Self::UNLOG_BITS,
            unsafe { edge.to_object_reference() },
            LOGGED_VALUE,
            None,
            if (1 << crate::args::LOG_BYTES_PER_RC_LOCK_BIT) >= 64 {
                None
            } else {
                Some(Ordering::Relaxed)
            },
        );
        store_metadata::<E::VM>(
            &RC_LOCK_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            UNLOCKED_VALUE,
            None,
            Some(Ordering::Relaxed),
        );
    }

    #[inline(always)]
    fn log_edge_and_get_old_target(&self, edge: Address) -> Result<ObjectReference, ()> {
        if self.attempt_to_lock_edge_bailout_if_logged(edge) {
            let old: ObjectReference = unsafe { edge.load() };
            self.log_and_unlock_edge(edge);
            Ok(old)
        } else {
            Err(())
        }
    }

    #[inline(always)]
    #[allow(unused)]
    fn log_edge_and_get_old_target_sloppy(&self, edge: Address) -> Result<ObjectReference, ()> {
        if !edge.is_logged::<E::VM>() {
            let old: ObjectReference = unsafe { edge.load() };
            edge.log::<E::VM>();
            Ok(old)
        } else {
            Err(())
        }
    }

    #[inline(always)]
    fn slow(&mut self, _src: ObjectReference, edge: Address, old: ObjectReference) {
        #[cfg(any(feature = "sanity", debug_assertions))]
        assert!(
            old.is_null() || crate::util::rc::count(old) != 0,
            "zero rc count {:?}",
            old
        );
        // Concurrent Marking
        // if !crate::args::REF_COUNT && self.lxr.concurrent_marking_in_progress() {
        //     self.edges.push(edge);
        //     if !old.is_null() {
        //         self.nodes.push(old);
        //     }
        // }
        // Reference counting
        // if crate::args::BARRIER_MEASUREMENT || crate::args::REF_COUNT {
        if !old.is_null() {
            self.decs.push(old);
        }
        self.incs.push(edge);
        crate::util::rc::inc_inc_buffer_size();
        // }
        // Flush
        if self.edges.len() >= Self::CAPACITY
            || self.incs.len() >= Self::CAPACITY
            || self.decs.len() >= Self::CAPACITY
        {
            self.flush();
        }
    }

    #[inline(always)]
    fn enqueue_node(&mut self, src: ObjectReference, edge: Address, _new: Option<ObjectReference>) {
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
}

impl<E: ProcessEdgesWork> Barrier for FieldLoggingBarrier<E> {
    fn assert_is_flushed(&self) {
        debug_assert!(self.edges.is_empty());
        debug_assert!(self.nodes.is_empty());
        debug_assert!(self.incs.is_empty());
        debug_assert!(self.decs.is_empty());
    }

    #[cold]
    fn flush(&mut self) {
        // Barrier measurement: simply unlog remembered edges
        if crate::args::BARRIER_MEASUREMENT {
            // Unlog inc edges
            if !self.incs.is_empty() {
                let mut decs = Vec::with_capacity(Self::CAPACITY);
                std::mem::swap(&mut decs, &mut self.decs);
                let mut incs = vec![];
                std::mem::swap(&mut incs, &mut self.incs);
                self.mmtk.scheduler.work_buckets[WorkBucketStage::CalculateForwarding]
                    .add(UnlogEdges::new(incs));
            }
            return;
        }
        // Concurrent Marking: Flush satb buffer
        #[allow(clippy::collapsible_if)]
        if self.lxr.concurrent_marking_enabled()
            && (self.lxr.concurrent_marking_in_progress()
                || self.lxr.current_pause() == Some(Pause::FinalMark))
        {
            if !self.edges.is_empty() || !self.nodes.is_empty() || !self.decs.is_empty() {
                let edges = vec![];
                // let mut nodes = vec![];
                // if !crate::args::REF_COUNT {
                //     std::mem::swap(&mut edges, &mut self.edges);
                //     std::mem::swap(&mut nodes, &mut self.nodes);
                // } else {
                let nodes = self.decs.clone();
                // }
                self.mmtk.scheduler.work_buckets[WorkBucketStage::Initial]
                    .add(ProcessModBufSATB::<E>::new(edges, nodes));
            }
        }
        // Flush inc and dec buffer
        if !self.incs.is_empty() {
            // Inc buffer
            let mut incs = Vec::with_capacity(Self::CAPACITY);
            std::mem::swap(&mut incs, &mut self.incs);
            let bucket = WorkBucketStage::rc_process_incs_stage();
            self.mmtk.scheduler.work_buckets[bucket]
                .add(ProcessIncs::<_, { EDGE_KIND_MATURE }>::new(incs));
            // Dec buffer
            let mut decs = Vec::with_capacity(Self::CAPACITY);
            std::mem::swap(&mut decs, &mut self.decs);
            let w = ProcessDecs::new(decs, LazySweepingJobsCounter::new_desc());
            if crate::args::LAZY_DECREMENTS && !crate::args::BARRIER_MEASUREMENT {
                self.mmtk.scheduler.postpone_prioritized(w);
            } else {
                self.mmtk.scheduler.work_buckets[WorkBucketStage::RCProcessDecs].add(w);
            }
        }
    }

    #[inline(always)]
    fn write_barrier(&mut self, target: BarrierWriteTarget) {
        match target {
            BarrierWriteTarget::Field { src, slot, val, .. } => {
                self.enqueue_node(src, slot, Some(val));
            }
            BarrierWriteTarget::ArrayCopy {
                dst,
                dst_offset,
                len,
                ..
            } => {
                if dst.is_null() {
                    return;
                }
                let dst_base = dst.to_address() + dst_offset;
                for i in 0..len {
                    self.enqueue_node(dst, dst_base + (i << 3), None);
                }
            }
            BarrierWriteTarget::Clone { dst, .. } => {
                EdgeIterator::<E::VM>::iterate(dst, |x| self.enqueue_node(dst, x, None))
            }
        }
    }
}
