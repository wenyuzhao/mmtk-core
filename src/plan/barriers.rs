//! Read/Write barrier implementations.

use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;

use atomic::Ordering;

use crate::plan::immix::Immix;
use crate::policy::space::Space;
use crate::scheduler::GCWork;
use crate::scheduler::GCWorker;
use crate::scheduler::gc_work::*;
use crate::scheduler::WorkBucketStage;
use crate::util::metadata::load_metadata;
use crate::util::metadata::store_metadata;
use crate::util::metadata::{compare_exchange_metadata, MetadataSpec};
use crate::util::*;
use crate::MMTK;
use crate::util::metadata::side_metadata;

use super::transitive_closure::EdgeIterator;

pub const BARRIER_MEASUREMENT: bool = false;
pub const TAKERATE_MEASUREMENT: bool = false;
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
        let unlogged_value = if option_env!("IX_OBJ_BARRIER").is_some() { 0 } else { 1 };
        let logged_value = if option_env!("IX_OBJ_BARRIER").is_some() { 1 } else { 0 };
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
        if TAKERATE_MEASUREMENT && crate::INSIDE_HARNESS.load(Ordering::SeqCst) {
            FAST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if self.log_object(obj) {
            if TAKERATE_MEASUREMENT && crate::INSIDE_HARNESS.load(Ordering::SeqCst) {
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

#[derive(PartialEq, Eq)]
pub enum FLBKind {
    SATB,
    IU,
}

pub struct FieldLoggingBarrier<E: ProcessEdgesWork, const KIND: FLBKind> {
    mmtk: &'static MMTK<E::VM>,
    edges: Vec<Address>,
    nodes: Vec<ObjectReference>,
    /// The metadata used for log bit. Though this allows taking an arbitrary metadata spec,
    /// for this field, 0 means logged, and 1 means unlogged (the same as the vm::object_model::VMGlobalLogBitSpec).
    meta: MetadataSpec,
    incs: Vec<Address>,
    decs: Vec<ObjectReference>,
}

impl<E: ProcessEdgesWork, const KIND: FLBKind> FieldLoggingBarrier<E, KIND> {
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
    fn log_edge(&self, edge: Address) -> bool {
        loop {
            let old_value = load_metadata::<E::VM>(
                &self.meta,
                unsafe { edge.to_object_reference() },
                None,
                Some(Ordering::SeqCst),
            );
            if old_value == 1 {
                return false;
            }
            if compare_exchange_metadata::<E::VM>(
                &self.meta,
                unsafe { edge.to_object_reference() },
                0,
                1,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return true;
            }
        }
    }

    #[inline(always)]
    fn enqueue_node(&mut self, edge: Address) {
        if option_env!("IX_OBJ_BARRIER").is_some() {
            unreachable!()
        }
        if crate::plan::immix::CONCURRENT_MARKING && !BARRIER_MEASUREMENT && !*crate::IN_CONCURRENT_GC.lock() {
            return;
        }
        if TAKERATE_MEASUREMENT && crate::INSIDE_HARNESS.load(Ordering::SeqCst) {
            FAST_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        if self.log_edge(edge) {
            if TAKERATE_MEASUREMENT && crate::INSIDE_HARNESS.load(Ordering::SeqCst) {
                SLOW_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            // Concurrent Marking
            if crate::plan::immix::CONCURRENT_MARKING {
                self.edges.push(edge);
                if KIND == FLBKind::SATB {
                    let node: ObjectReference = unsafe { edge.load() };
                    if !node.is_null() {
                        self.nodes.push(node);
                    }
                }
                if self.edges.len() >= E::CAPACITY {
                    self.flush();
                }
            }
            // Reference counting
            if crate::plan::immix::REF_COUNT {
                debug_assert!(!crate::plan::immix::CONCURRENT_MARKING);
                self.edges.push(edge);
                let old: ObjectReference = unsafe { edge.load() };
                if !old.is_null() {
                    self.decs.push(old);
                }
                self.incs.push(edge);
                if self.edges.len() >= E::CAPACITY || self.incs.len() >= E::CAPACITY || self.decs.len() >= E::CAPACITY {
                    self.flush();
                }
            }
        }
    }
}

impl<E: ProcessEdgesWork, const KIND: FLBKind> Barrier for FieldLoggingBarrier<E, KIND> {
    #[cold]
    fn flush(&mut self) {
        if KIND == FLBKind::SATB {
            if !self.edges.is_empty() || !self.nodes.is_empty() {
                let mut edges = vec![];
                std::mem::swap(&mut edges, &mut self.edges);
                let mut nodes = vec![];
                std::mem::swap(&mut nodes, &mut self.nodes);
                self.mmtk.scheduler.work_buckets[WorkBucketStage::RefClosure]
                    .add(ProcessModBufSATB::<E>::new(edges, nodes, self.meta));
            }
        } else {
            unreachable!()
        }
        if !crate::plan::immix::CONCURRENT_MARKING && !self.edges.is_empty() {
            let mut edges = vec![];
            std::mem::swap(&mut edges, &mut self.edges);
            let meta = self.meta;
            self.mmtk.scheduler.work_buckets[WorkBucketStage::PostClosure].add_lambda(move || {
                for e in edges {
                    store_metadata::<E::VM>(
                        &meta,
                        unsafe { e.to_object_reference() },
                        0,
                        None,
                        Some(Ordering::Relaxed),
                    )
                }
            });
        }
        if !self.incs.is_empty() {
            let mut incs = vec![];
            std::mem::swap(&mut incs, &mut self.incs);
            let immix = self.mmtk.plan.downcast_ref::<Immix<E::VM>>().unwrap();
            self.mmtk.scheduler.work_buckets[WorkBucketStage::PostClosure].add_lambda(move || {
                for e in incs {
                    let o: ObjectReference = unsafe { e.load() };
                    if !o.is_null() &&  immix.immix_space.in_space(o) {
                        let _ = crate::policy::immix::rc::inc(o);
                    }
                }
            });
        }
        if !self.decs.is_empty() {
            struct ProcessDecs<E: ProcessEdgesWork> {
                decs: Vec<ObjectReference>,
                phantom: PhantomData<E>,
            }
            impl<E: ProcessEdgesWork> ProcessDecs<E> {
                pub fn new(decs: Vec<ObjectReference>) -> Self {
                    Self { decs, phantom: PhantomData }
                }
            }
            impl<E: ProcessEdgesWork> GCWork<E::VM> for ProcessDecs<E> {
                #[inline(always)]
                fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
                    let mut new_decs = vec![];
                    for o in &self.decs {
                        let immix = mmtk.plan.downcast_ref::<Immix<E::VM>>().unwrap();
                        if !immix.immix_space.in_space(*o) { continue }
                        let r = crate::policy::immix::rc::dec(*o);
                        if r == Ok(0) {
                            // Recursively decrease field ref counts
                            EdgeIterator::<E::VM>::iterate(*o, |edge| {
                                let o = unsafe { edge.load::<ObjectReference>() };
                                if !o.is_null() {
                                    new_decs.push(o);
                                }
                            });
                        }
                    }
                    if !new_decs.is_empty() {
                        worker.local_work_bucket.add(ProcessDecs::<E>::new(new_decs));
                    }
                }
            }
            let mut decs = vec![];
            std::mem::swap(&mut decs, &mut self.decs);
            self.mmtk.scheduler.work_buckets[WorkBucketStage::RefClosure].add(ProcessDecs::<E>::new(decs));
        }
    }

    #[inline(always)]
    fn write_barrier(&mut self, target: WriteTarget) {
        match target {
            WriteTarget::Field { slot, src, .. } => {
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
                    debug_assert!(unsafe { edge.load::<ObjectReference>() }.is_null(), "{:?}", unsafe { edge.load::<ObjectReference>() });
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
