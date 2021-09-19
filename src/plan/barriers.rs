//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;

use atomic::Ordering;

use crate::plan::immix::Immix;
use crate::policy::immix::block::Block;
use crate::scheduler::gc_work::*;
use crate::scheduler::WorkBucketStage;
use crate::util::metadata::load_metadata;
use crate::util::metadata::store_metadata;
use crate::util::metadata::{compare_exchange_metadata, MetadataSpec};
use crate::util::*;
use crate::MMTK;

use super::transitive_closure::EdgeIterator;

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
        let unlogged_value =
            if crate::plan::immix::get_active_barrier() == BarrierSelector::ObjectBarrier {
                0
            } else {
                1
            };
        let logged_value = unlogged_value ^ 1;
        loop {
            let old_value =
                load_metadata::<E::VM>(&self.meta, object, None, Some(Ordering::SeqCst));
            if old_value == logged_value {
                return false;
            }
            if compare_exchange_metadata::<E::VM>(
                &self.meta,
                object,
                unlogged_value,
                logged_value,
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

pub const UNLOGGED_VALUE: usize = 0b01;
pub const LOGGED_VALUE: usize = 0b00;
pub const LOGGING_IN_PROGRESS: usize = 0b11;

pub struct FieldLoggingBarrier<E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
    edges: Vec<Address>,
    nodes: Vec<ObjectReference>,
    /// The metadata used for log bit. Though this allows taking an arbitrary metadata spec,
    /// for this field, 0 means logged, and 1 means unlogged (the same as the vm::object_model::VMGlobalLogBitSpec).
    meta: MetadataSpec,
    incs: Vec<Address>,
    decs: Vec<ObjectReference>,
}

impl<E: ProcessEdgesWork> FieldLoggingBarrier<E> {
    #[allow(unused)]
    pub fn new(mmtk: &'static MMTK<E::VM>, meta: MetadataSpec) -> Self {
        Self {
            mmtk,
            edges: vec![],
            nodes: vec![],
            meta,
            incs: vec![],
            decs: vec![],
        }
    }

    #[inline(always)]
    fn attempt_to_log_edge(&self, edge: Address) -> bool {
        loop {
            std::hint::spin_loop();
            let old_value = load_metadata::<E::VM>(
                &self.meta,
                unsafe { edge.to_object_reference() },
                None,
                None,
            );
            if old_value == LOGGED_VALUE {
                return false;
            }
            if old_value == LOGGING_IN_PROGRESS {
                continue;
            }
            debug_assert!(old_value == UNLOGGED_VALUE);
            if compare_exchange_metadata::<E::VM>(
                &self.meta,
                unsafe { edge.to_object_reference() },
                UNLOGGED_VALUE,
                LOGGING_IN_PROGRESS,
                None,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                return true;
            }
        }
    }

    #[inline(always)]
    fn mark_edge_as_logged(&self, edge: Address) {
        store_metadata::<E::VM>(
            &self.meta,
            unsafe { edge.to_object_reference() },
            LOGGED_VALUE,
            None,
            Some(Ordering::SeqCst),
        );
    }

    #[inline(always)]
    fn enqueue_node(&mut self, edge: Address) {
        if crate::plan::immix::get_active_barrier() == BarrierSelector::ObjectBarrier {
            unreachable!()
        }
        if crate::plan::immix::CONCURRENT_MARKING
            && !BARRIER_MEASUREMENT
            && !crate::IN_CONCURRENT_GC.load(Ordering::SeqCst)
        {
            return;
        }
        if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
            FAST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if self.attempt_to_log_edge(edge) {
            if TAKERATE_MEASUREMENT && self.mmtk.inside_harness() {
                SLOW_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            // Concurrent Marking
            if crate::plan::immix::CONCURRENT_MARKING {
                self.edges.push(edge);
                let node: ObjectReference = unsafe { edge.load() };
                if !node.is_null() {
                    self.nodes.push(node);
                }
                if self.edges.len() >= E::CAPACITY || self.nodes.len() >= E::CAPACITY {
                    self.flush();
                }
            }
            // Reference counting
            if crate::plan::immix::REF_COUNT {
                debug_assert!(!crate::plan::immix::CONCURRENT_MARKING);
                let old: ObjectReference = unsafe { edge.load() };
                if !old.is_null() {
                    if crate::policy::immix::SANITY {
                        let immix = self.mmtk.plan.downcast_ref::<Immix<E::VM>>().unwrap();
                        debug_assert!(!immix
                            .immix_space
                            .new_blocks
                            .lock()
                            .contains(&Block::containing::<E::VM>(old)));
                    }
                    self.decs.push(old);
                }
                self.incs.push(edge);
                if self.edges.len() >= E::CAPACITY
                    || self.incs.len() >= E::CAPACITY
                    || self.decs.len() >= E::CAPACITY
                {
                    self.flush();
                }
            }
            self.mark_edge_as_logged(edge);
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
        // Concurrent Marking: Flush satb buffer
        if crate::plan::immix::CONCURRENT_MARKING {
            let mut edges = vec![];
            std::mem::swap(&mut edges, &mut self.edges);
            let mut nodes = vec![];
            std::mem::swap(&mut nodes, &mut self.nodes);
            self.mmtk.scheduler.work_buckets[WorkBucketStage::RefClosure]
                .add(ProcessModBufSATB::<E>::new(edges, nodes, self.meta));
        }
        // Flush inc buffer
        if !self.incs.is_empty() {
            let mut incs = vec![];
            std::mem::swap(&mut incs, &mut self.incs);
            let bucket = if crate::flags::EAGER_INCREMENTS && !crate::flags::BARRIER_MEASUREMENT {
                WorkBucketStage::Unconstrained
            } else {
                WorkBucketStage::PostClosure
            };
            self.mmtk.scheduler.work_buckets[bucket].add(ProcessIncs::<E::VM, true>::new(incs));
        }
        // Flush dec buffer
        if !self.decs.is_empty() {
            let mut decs = vec![];
            std::mem::swap(&mut decs, &mut self.decs);
            self.mmtk.scheduler.work_buckets[WorkBucketStage::RefClosure]
                .add(ProcessDecs::<E::VM>::new(decs));
        }
    }

    #[inline(always)]
    fn write_barrier(&mut self, target: WriteTarget) {
        match target {
            WriteTarget::Field { slot, .. } => {
                self.enqueue_node(slot);
            }
            WriteTarget::ArrayCopy {
                dst,
                dst_offset,
                len,
                ..
            } => {
                let dst_base = dst.to_address() + dst_offset;
                for i in 0..len {
                    self.enqueue_node(dst_base + (i << 3));
                }
            }
            WriteTarget::Clone { dst, .. } => {
                // How to deal with this?
                // println!("Clone {:?}", src);
                EdgeIterator::<E::VM>::iterate(dst, |edge| {
                    debug_assert!(
                        unsafe { edge.load::<ObjectReference>() }.is_null(),
                        "{:?}",
                        unsafe { edge.load::<ObjectReference>() }
                    );
                    self.enqueue_node(edge);
                })
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
