//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;

use crate::scheduler::gc_work::*;
use crate::scheduler::WorkBucketStage;
use crate::util::metadata::load_metadata;
use crate::util::metadata::{compare_exchange_metadata, MetadataSpec};
use crate::util::*;
use crate::MMTK;
use atomic::Ordering;

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

impl BarrierSelector {
    pub const fn equals(&self, other: BarrierSelector) -> bool {
        // cast enum to u8 then compare. Otherwise, we cannot do it in a const fn.
        *self as u8 == other as u8
    }
}

/// For field writes in HotSpot, we cannot always get the source object pointer and the field address
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
