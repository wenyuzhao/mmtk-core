//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;

use atomic::Ordering;

use crate::plan::immix::Immix;
use crate::scheduler::gc_work::*;
use crate::scheduler::WorkBucketStage;
use crate::util::cm::ProcessModBufSATB;
use crate::util::metadata::load_metadata;
use crate::util::metadata::side_metadata::compare_exchange_atomic2;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::metadata::store_metadata;
use crate::util::metadata::{compare_exchange_metadata, MetadataSpec};
use crate::util::rc::ProcessDecs;
use crate::util::rc::ProcessIncs;
use crate::util::rc::RC_LOCK_BIT_SPEC;
use crate::util::*;
use crate::vm::*;
use crate::LazySweepingJobsCounter;
use crate::MMTK;

use super::immix::Pause;
use super::EdgeIterator;

pub const BARRIER_MEASUREMENT: bool = crate::args::BARRIER_MEASUREMENT;
pub const TAKERATE_MEASUREMENT: bool = crate::args::TAKERATE_MEASUREMENT;
pub static FAST_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static SLOW_COUNT: AtomicUsize = AtomicUsize::new(0);

/// BarrierSelector describes which barrier to use.
#[derive(Copy, Clone, Debug)]
pub enum BarrierSelector {
    NoBarrier,
    ObjectBarrier,
    FieldLoggingBarrier,
}

impl const PartialEq for BarrierSelector {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (BarrierSelector::NoBarrier, BarrierSelector::NoBarrier) => true,
            (BarrierSelector::ObjectBarrier, BarrierSelector::ObjectBarrier) => true,
            (BarrierSelector::FieldLoggingBarrier, BarrierSelector::FieldLoggingBarrier) => true,
            _ => false,
        }
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

/// For field writes in HotSpot, we cannot always get the source object pointer and the field address\
#[derive(Debug)]
pub enum WriteTarget {
    Field {
        src: ObjectReference,
        slot: Address,
        val: ObjectReference,
    },
    ArrayCopy {
        src: ObjectReference,
        src_offset: usize,
        dst: ObjectReference,
        dst_offset: usize,
        len: usize,
    },
    Clone {
        src: ObjectReference,
        dst: ObjectReference,
    },
}

pub trait Barrier: 'static + Send {
    fn flush(&mut self);
    fn write_barrier(&mut self, target: WriteTarget);
    fn assert_is_flushed(&self) {}
}

pub struct NoBarrier;

impl Barrier for NoBarrier {
    fn flush(&mut self) {}
    fn write_barrier(&mut self, _target: WriteTarget) {
        unreachable!("write_barrier called on NoBarrier")
    }
}

pub struct ObjectRememberingBarrier<E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
    modbuf: Vec<ObjectReference>,
    /// The metadata used for log bit. Though this allows taking an arbitrary metadata spec,
    /// for this field, 0 means logged, and 1 means unlogged (the same as the vm::object_model::VMGlobalLogBitSpec).
    meta: MetadataSpec,
}

impl<E: ProcessEdgesWork> ObjectRememberingBarrier<E> {
    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<E::VM>, meta: MetadataSpec) -> Self {
        Self {
            mmtk,
            modbuf: vec![],
            meta,
        }
    }

    /// Attepmt to atomically log an object.
    /// Returns true if the object is not logged previously.
    #[inline(always)]
    fn log_object(&self, object: ObjectReference) -> bool {
        loop {
            let old_value =
                load_metadata::<E::VM>(&self.meta, object, None, Some(Ordering::SeqCst));
            if old_value == LOGGED_VALUE {
                return false;
            }
            if compare_exchange_metadata::<E::VM>(
                &self.meta,
                object,
                UNLOGGED_VALUE,
                LOGGED_VALUE,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return true;
            }
        }
    }

    #[inline(always)]
    fn enqueue_node(&mut self, obj: ObjectReference) {
        // If the objecct is unlogged, log it and push it to mod buffer
        if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
            FAST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if self.log_object(obj) {
            if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
                SLOW_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            self.modbuf.push(obj);
            if self.modbuf.len() >= E::CAPACITY {
                self.flush();
            }
        }
    }
}

impl<E: ProcessEdgesWork> Barrier for ObjectRememberingBarrier<E> {
    #[cold]
    fn flush(&mut self) {
        if self.modbuf.is_empty() {
            return;
        }
        let mut modbuf = vec![];
        std::mem::swap(&mut modbuf, &mut self.modbuf);
        debug_assert!(
            !self.mmtk.scheduler.work_buckets[WorkBucketStage::Final].is_activated(),
            "{:?}",
            self as *const _
        );
        if !modbuf.is_empty() {
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Closure]
                .add(ProcessModBuf::<E>::new(modbuf, self.meta));
        }
    }

    #[inline(always)]
    fn write_barrier(&mut self, target: WriteTarget) {
        match target {
            WriteTarget::Field { src, .. } => {
                self.enqueue_node(src);
            }
            WriteTarget::ArrayCopy { dst, .. } => {
                self.enqueue_node(dst);
            }
            WriteTarget::Clone { dst, .. } => {
                self.enqueue_node(dst);
            }
        }
    }
}

pub const UNLOGGED_VALUE: usize = 0b1;
pub const LOGGED_VALUE: usize = 0b0;

pub const UNLOCKED_VALUE: usize = 0b0;
pub const LOCKED_VALUE: usize = 0b1;

pub struct FieldLoggingBarrier<E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
    edges: Vec<Address>,
    nodes: Vec<ObjectReference>,
    incs: Vec<Address>,
    decs: Vec<ObjectReference>,
    immix: &'static Immix<E::VM>,
}

impl<E: ProcessEdgesWork> FieldLoggingBarrier<E> {
    const CAPACITY: usize = 4096;
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
            immix: mmtk.plan.downcast_ref::<Immix<E::VM>>().unwrap(),
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
        // Concurrent Marking
        if !crate::args::REF_COUNT
            && crate::args::CONCURRENT_MARKING
            && crate::concurrent_marking_in_progress()
        {
            self.edges.push(edge);
            if !old.is_null() {
                self.nodes.push(old);
            }
        }
        // Reference counting
        if crate::args::BARRIER_MEASUREMENT || crate::plan::immix::REF_COUNT {
            if !old.is_null() {
                self.decs.push(old);
            }
            self.incs.push(edge);
        }
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
        debug_assert!(!src.is_null());
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
                self.mmtk.scheduler.work_buckets[WorkBucketStage::RefClosure]
                    .add(UnlogEdges::new(incs));
            }
            return;
        }
        // Concurrent Marking: Flush satb buffer
        if crate::plan::immix::CONCURRENT_MARKING
            && (crate::concurrent_marking_in_progress()
                || self.immix.current_pause() == Some(Pause::FinalMark))
        {
            if !self.edges.is_empty() || !self.nodes.is_empty() || !self.decs.is_empty() {
                let mut edges = vec![];
                let mut nodes = vec![];
                if !crate::args::REF_COUNT {
                    std::mem::swap(&mut edges, &mut self.edges);
                    std::mem::swap(&mut nodes, &mut self.nodes);
                } else {
                    nodes = self.decs.clone();
                }
                self.mmtk.scheduler.work_buckets[WorkBucketStage::Closure]
                    .add(ProcessModBufSATB::<E>::new(edges, nodes));
            }
        }
        // Flush inc and dec buffer
        if !self.incs.is_empty() {
            // Inc buffer
            let mut incs = Vec::with_capacity(Self::CAPACITY);
            std::mem::swap(&mut incs, &mut self.incs);
            let bucket = WorkBucketStage::rc_process_incs_stage();
            self.mmtk.scheduler.work_buckets[bucket].add(ProcessIncs::new(incs, false));
            // Dec buffer
            let mut decs = Vec::with_capacity(Self::CAPACITY);
            std::mem::swap(&mut decs, &mut self.decs);
            let w = ProcessDecs::new(decs, LazySweepingJobsCounter::new_desc());
            if crate::args::LAZY_DECREMENTS && !crate::args::BARRIER_MEASUREMENT {
                self.mmtk.scheduler.postpone(w);
            } else {
                self.mmtk.scheduler.work_buckets[WorkBucketStage::RCProcessDecs].add(w);
            }
        }
    }

    #[inline(always)]
    fn write_barrier(&mut self, target: WriteTarget) {
        match target {
            WriteTarget::Field { src, slot, val, .. } => {
                self.enqueue_node(src, slot, Some(val));
            }
            WriteTarget::ArrayCopy {
                dst,
                dst_offset,
                len,
                ..
            } => {
                // if len == 0 {
                //     return;
                // }

                // if len == 1 {
                //     self.enqueue_node(dst, dst.to_address() + dst_offset, None);
                //     return;
                // }

                // type UInt = u128;
                // const BYTES_IN_UINT: usize = mem::size_of::<UInt>();
                // const BITS_IN_UINT: usize = BYTES_IN_UINT * 8;
                // const LOG_BITS_IN_UINT: usize = BITS_IN_UINT.trailing_zeros() as _;
                // let mut cursor = dst.to_address() + dst_offset;
                // let limit = cursor + (len << 3);
                // let mut meta_addr: *const UInt = unsafe {
                //     GLOBAL_SIDE_METADATA_VM_BASE_ADDRESS
                //         .to_ptr::<UInt>()
                //         .add(cursor.as_usize() >> (LOG_BITS_IN_UINT + 3))
                // };
                // let val = unsafe { *meta_addr };
                // while cursor < limit {
                //     if cursor.is_aligned_to(BITS_IN_UINT * 8) {
                //         let val = unsafe { *meta_addr };
                //         if val != 0 {
                //             'x: for j in 0usize..BITS_IN_UINT {
                //                 if ((val >> j) & 1) != 0 {
                //                     let e = cursor + (j << 3);
                //                     if e < limit {
                //                         self.enqueue_node(dst, e, None);
                //                     } else {
                //                         break 'x;
                //                     }
                //                 }
                //             }
                //         }
                //         cursor += BITS_IN_UINT * 8;
                //         meta_addr = unsafe { meta_addr.add(1) };
                //     } else {
                //         let shift = (cursor.as_usize() >> 3) & (BITS_IN_UINT - 1);
                //         if (val >> shift) & 1 != 0 {
                //             self.enqueue_node(dst, cursor, None);
                //         }
                //         cursor += 8usize;
                //     }
                // }

                let dst_base = dst.to_address() + dst_offset;
                for i in 0..len {
                    self.enqueue_node(dst, dst_base + (i << 3), None);
                }

                // const BYTES_PER_LOCK_BIT: usize = 1 << crate::args::LOG_BYTES_PER_RC_LOCK_BIT;
                // let dst_base = dst.to_address() + dst_offset;
                // let dst_limit = dst_base + (len << 3);
                // for a in (dst_base..dst_limit).step_by(BYTES_PER_LOCK_BIT) {
                //     // let lock_top = (a + BYTES_PER_LOCK_BIT).align_down(BYTES_PER_LOCK_BIT);
                //     // let mut locked = false;
                //     for e in a..Address::min(
                //         (a + BYTES_PER_LOCK_BIT).align_down(BYTES_PER_LOCK_BIT),
                //         dst_limit,
                //     ) {
                //         if !e.is_logged::<E::VM>() {
                //             // if !locked {
                //             //     a.lock();
                //             //     locked = true;
                //             // }
                //             let old = unsafe { e.load() };
                //             e.log::<E::VM>();
                //             self.slow(ObjectReference::NULL, e, old);
                //         }
                //     }
                //     // if locked {
                //     //     a.unlock::<E::VM>();
                //     // }
                // }
                // a.unlock::<E::VM>();
                // for e in (dst_base..dst_limit).step_by(BYTES_IN_WORD) {
                //     let old = unsafe { e.load() };
                //     e.log::<E::VM>();
                //     self.slow(ObjectReference::NULL, e, old);
                // }
                // for a in (dst_base..dst_limit).step_by(1 << crate::args::LOG_BYTES_PER_RC_LOCK_BIT)
                // {
                // }
            }
            WriteTarget::Clone { dst, .. } => {
                EdgeIterator::<E::VM>::iterate(dst, |x| self.enqueue_node(dst, x, None))
            }
        }
    }
}

pub struct GenFieldLoggingBarrier<E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
    edges: Vec<Address>,
    nodes: Vec<ObjectReference>,
    meta: MetadataSpec,
}

impl<E: ProcessEdgesWork> GenFieldLoggingBarrier<E> {
    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<E::VM>, meta: MetadataSpec) -> Self {
        Self {
            mmtk,
            edges: vec![],
            nodes: vec![],
            meta,
        }
    }

    #[inline(always)]
    fn log_object(&self, object: ObjectReference) -> bool {
        loop {
            let old_value =
                load_metadata::<E::VM>(&self.meta, object, None, Some(Ordering::SeqCst));
            if old_value == 0 {
                return false;
            }
            if compare_exchange_metadata::<E::VM>(
                &self.meta,
                object,
                1,
                0,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return true;
            }
        }
    }

    #[inline(always)]
    fn log_edge(&self, edge: Address) -> bool {
        loop {
            let old_value = load_metadata::<E::VM>(
                &self.meta,
                unsafe { edge.to_object_reference() },
                None,
                Some(Ordering::SeqCst),
            );
            if old_value == 0 {
                return false;
            }
            if compare_exchange_metadata::<E::VM>(
                &self.meta,
                unsafe { edge.to_object_reference() },
                1,
                0,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return true;
            }
        }
    }

    #[inline(always)]
    fn enqueue_edge(&mut self, edge: Address) {
        if self.log_edge(edge) {
            self.edges.push(edge);
            if self.edges.len() >= E::CAPACITY {
                self.flush();
            }
        }
    }

    #[inline(always)]
    fn enqueue_node(&mut self, obj: ObjectReference) {
        if self.log_object(obj) {
            self.nodes.push(obj);
            if self.nodes.len() >= E::CAPACITY {
                self.flush();
            }
        }
    }
}

impl<E: ProcessEdgesWork> Barrier for GenFieldLoggingBarrier<E> {
    #[cold]
    fn flush(&mut self) {
        if !self.nodes.is_empty() {
            let mut nodes = vec![];
            std::mem::swap(&mut nodes, &mut self.nodes);
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Closure]
                .add(ProcessModBuf::<E>::new(nodes, self.meta));
        }
        if !self.edges.is_empty() {
            let mut edges = vec![];
            std::mem::swap(&mut edges, &mut self.edges);
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Closure]
                .add(EdgesProcessModBuf::<E>::new(edges, self.meta));
        }
    }

    #[inline(always)]
    fn write_barrier(&mut self, target: WriteTarget) {
        match target {
            WriteTarget::Field { slot, .. } => {
                self.enqueue_edge(slot);
            }
            WriteTarget::ArrayCopy {
                dst,
                dst_offset,
                len,
                ..
            } => {
                let dst_base = dst.to_address() + dst_offset;
                for i in 0..len {
                    self.enqueue_edge(dst_base + (i << 3));
                }
            }
            WriteTarget::Clone { dst, .. } => self.enqueue_node(dst),
        }
    }
}
