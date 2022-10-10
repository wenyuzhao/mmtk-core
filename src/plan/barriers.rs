//! Read/Write barrier implementations.

use std::sync::atomic::AtomicUsize;

use crate::scheduler::gc_work::*;
use crate::scheduler::WorkBucketStage;
use crate::util::metadata::MetadataSpec;
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
pub enum BarrierWriteTarget {
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
    fn write_barrier(&mut self, target: BarrierWriteTarget);
    fn assert_is_flushed(&self) {}
}

pub struct NoBarrier;

impl Barrier for NoBarrier {
    fn flush(&mut self) {}
    fn write_barrier(&mut self, _target: BarrierWriteTarget) {
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
            // Try set the bit from 1 to 0 (log object). This may fail, if
            // 1. the bit is cleared by others, or
            // 2. other bits in the same byte may get modified if we use side metadata
            if self
                .meta
                .compare_exchange_metadata::<E::VM, u8>(
                    object,
                    1,
                    0,
                    None,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                // We just logged the object
                return true;
            } else {
                let old_value = self
                    .meta
                    .load_atomic::<E::VM, u8>(object, None, Ordering::SeqCst);
                // If the bit is cleared before, someone else has logged the object. Return false.
                if old_value == 0 {
                    return false;
                }
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

    #[inline(always)]
    fn barrier(&mut self, obj: ObjectReference) {
        // Perform a relaxed load for performance.
        // It is okay if this check fails occasionally and
        // the execution goes to the slowpath, we can take care of that in the slowpath.
        if self
            .meta
            .load_atomic::<E::VM, u8>(obj, None, Ordering::Relaxed)
            == 0
        {
            return;
        }
        self.barrier_slow(obj);
    }

    #[inline(never)]
    fn barrier_slow(&mut self, obj: ObjectReference) {
        self.enqueue_node(obj);
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
    fn write_barrier(&mut self, target: BarrierWriteTarget) {
        match target {
            BarrierWriteTarget::Field { src, .. } => {
                self.enqueue_node(src);
            }
            BarrierWriteTarget::ArrayCopy { dst, .. } => {
                self.enqueue_node(dst);
            }
            BarrierWriteTarget::Clone { dst, .. } => {
                self.enqueue_node(dst);
            }
        }
    }
}

pub const UNLOGGED_VALUE: u8 = 0b1;
pub const LOGGED_VALUE: u8 = 0b0;

pub const UNLOCKED_VALUE: u8 = 0b0;
pub const LOCKED_VALUE: u8 = 0b1;
