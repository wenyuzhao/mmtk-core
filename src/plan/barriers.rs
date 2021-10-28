//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;

use atomic::Ordering;

use crate::plan::immix::Immix;
use crate::plan::immix::CURRENT_CONC_DECS_COUNTER;
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
use crate::MMTK;

pub const BARRIER_MEASUREMENT: bool = crate::flags::BARRIER_MEASUREMENT;
pub const TAKERATE_MEASUREMENT: bool = crate::flags::TAKERATE_MEASUREMENT;
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
    mature_evac_remset: Vec<ObjectReference>,
}

impl<E: ProcessEdgesWork> FieldLoggingBarrier<E> {
    const CAPACITY: usize = 1024;
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
            mature_evac_remset: vec![],
        }
    }

    #[inline(always)]
    fn get_edge_lock_state(&self, edge: Address) -> usize {
        load_metadata::<E::VM>(
            &RC_LOCK_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            None,
            None,
        )
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
            Some(Ordering::Relaxed),
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
    fn enqueue_node(&mut self, src: ObjectReference, edge: Address, _new: Option<ObjectReference>) {
        if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
            FAST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if let Ok(old) = self.log_edge_and_get_old_target(edge) {
            if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
                SLOW_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            // Concurrent Marking
            if crate::plan::immix::CONCURRENT_MARKING && crate::concurrent_marking_in_progress() {
                self.edges.push(edge);
                if !old.is_null() {
                    self.nodes.push(old);
                }
            }
            // Reference counting
            if crate::flags::BARRIER_MEASUREMENT || crate::plan::immix::REF_COUNT {
                if !old.is_null() {
                    self.decs.push(old);
                }
                self.incs.push(edge);
            }
            if crate::flags::REF_COUNT
                && crate::flags::RC_MATURE_EVACUATION
                && crate::concurrent_marking_in_progress()
            {
                self.mature_evac_remset.push(src);
            }
            // Flush
            if self.edges.len() >= Self::CAPACITY
                || self.incs.len() >= Self::CAPACITY
                || self.decs.len() >= Self::CAPACITY
                || self.mature_evac_remset.len() >= Self::CAPACITY
            {
                self.flush();
            }
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
        if crate::flags::BARRIER_MEASUREMENT {
            // Unlog inc edges
            if !self.incs.is_empty() {
                let mut decs = Vec::with_capacity(Self::CAPACITY);
                std::mem::swap(&mut decs, &mut self.decs);
                let mut incs = vec![];
                std::mem::swap(&mut incs, &mut self.incs);
                self.mmtk.scheduler.work_buckets[WorkBucketStage::RefClosure]
                    .add_no_notify(UnlogEdges::new(incs));
            }
            return;
        }
        // Concurrent Marking: Flush satb buffer
        if crate::plan::immix::CONCURRENT_MARKING {
            if !self.edges.is_empty() || !self.nodes.is_empty() {
                let mut edges = vec![];
                std::mem::swap(&mut edges, &mut self.edges);
                let mut nodes = vec![];
                std::mem::swap(&mut nodes, &mut self.nodes);
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
            self.mmtk.scheduler.work_buckets[bucket]
                .add_no_notify(ProcessIncs::<E::VM>::new(incs, false));
            // Dec buffer
            let mut decs = Vec::with_capacity(Self::CAPACITY);
            std::mem::swap(&mut decs, &mut self.decs);
            let w = ProcessDecs::new(decs, unsafe { CURRENT_CONC_DECS_COUNTER.clone().unwrap() });
            if crate::flags::LAZY_DECREMENTS && !crate::flags::BARRIER_MEASUREMENT {
                self.mmtk.scheduler.postpone(w);
            } else {
                self.mmtk.scheduler.work_buckets[WorkBucketStage::RCProcessDecs].add_no_notify(w);
            }
        }
        // Flush dec buffer
        if crate::flags::RC_MATURE_EVACUATION && !self.mature_evac_remset.is_empty() {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.mature_evac_remset);
            let w = EvacuateMatureObjects::new(remset);
            self.mmtk
                .plan
                .downcast_ref::<Immix<E::VM>>()
                .unwrap()
                .immix_space
                .mature_evac_remsets
                .lock()
                .push(box w);
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
                let dst_base = dst.to_address() + dst_offset;
                for i in 0..len {
                    self.enqueue_node(dst, dst_base + (i << 3), None);
                }
            }
            WriteTarget::Clone { .. } => {}
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
