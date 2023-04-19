//! This module contains code useful for tracing,
//! i.e. visiting the reachable objects by traversing all or part of an object graph.

use std::marker::PhantomData;

use crate::scheduler::gc_work::{EdgeOf, ProcessEdgesWork};
use crate::scheduler::{GCWorker, WorkBucketStage};
use crate::util::Address;
use crate::util::{ObjectReference, VMThread, VMWorkerThread};
use crate::vm::EdgeVisitor;
use crate::vm::{Scanning, VMBinding};

/// This trait represents an object queue to enqueue objects during tracing.
pub trait ObjectQueue {
    /// Enqueue an object into the queue.
    fn enqueue(&mut self, object: ObjectReference);
}

pub type VectorObjectQueue = VectorQueue<ObjectReference>;

/// An implementation of `ObjectQueue` using a `Vec`.
///
/// This can also be used as a buffer. For example, the mark stack or the write barrier mod-buffer.
pub struct VectorQueue<T> {
    /// Enqueued nodes.
    buffer: Vec<T>,
}

impl<T: Clone> VectorQueue<T> {
    pub fn clone_buffer(&self) -> Vec<T> {
        self.buffer.clone()
    }
}

impl<T> VectorQueue<T> {
    /// Reserve a capacity of this on first enqueue to avoid frequent resizing.
    const CAPACITY: usize = crate::args::BUFFER_SIZE;

    /// Create an empty `VectorObjectQueue`.
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Return `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Return the contents of the underlying vector.  It will empty the queue.
    pub fn take(&mut self) -> Vec<T> {
        std::mem::take(&mut self.buffer)
    }

    /// Consume this `VectorObjectQueue` and return its underlying vector.
    pub fn into_vec(self) -> Vec<T> {
        self.buffer
    }

    /// Check if the buffer size reaches `CAPACITY`.
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= Self::CAPACITY
    }

    pub fn push(&mut self, v: T) {
        if self.buffer.is_empty() {
            self.buffer.reserve(Self::CAPACITY);
        }
        self.buffer.push(v);
    }

    pub fn swap(&mut self, new_buffer: &mut Vec<T>) {
        std::mem::swap(&mut self.buffer, new_buffer)
    }

    pub fn clear(&mut self) {
        self.buffer.clear()
    }
}

impl<T> Default for VectorQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectQueue for VectorQueue<ObjectReference> {
    fn enqueue(&mut self, v: ObjectReference) {
        self.push(v);
    }
}

/// A transitive closure visitor to collect all the edges of an object.
pub struct ObjectsClosure<'a, E: ProcessEdgesWork> {
    buffer: VectorQueue<EdgeOf<E>>,
    worker: &'a mut GCWorker<E::VM>,
    should_discover_references: bool,
    should_claim_and_scan_clds: bool,
}

impl<'a, E: ProcessEdgesWork> ObjectsClosure<'a, E> {
    pub fn new(
        worker: &'a mut GCWorker<E::VM>,
        should_discover_references: bool,
        should_claim_and_scan_clds: bool,
    ) -> Self {
        Self {
            buffer: VectorQueue::new(),
            worker,
            should_discover_references,
            should_claim_and_scan_clds,
        }
    }

    fn flush(&mut self) {
        let buf = self.buffer.take();
        if !buf.is_empty() {
            self.worker.add_work(
                WorkBucketStage::Closure,
                E::new(buf, false, self.worker.mmtk),
            );
        }
    }
}

impl<'a, E: ProcessEdgesWork> EdgeVisitor<EdgeOf<E>> for ObjectsClosure<'a, E> {
    fn should_discover_references(&self) -> bool {
        self.should_discover_references
    }
    fn should_claim_clds(&self) -> bool {
        self.should_claim_and_scan_clds
    }
    fn should_follow_clds(&self) -> bool {
        self.should_claim_and_scan_clds
    }
    fn visit_edge(&mut self, slot: EdgeOf<E>) {
        #[cfg(debug_assertions)]
        {
            use crate::vm::edge_shape::Edge;
            trace!(
                "(ObjectsClosure) Visit edge {:?} (pointing to {})",
                slot,
                slot.load()
            );
        }
        self.buffer.push(slot);
        if self.buffer.is_full() {
            self.flush();
        }
    }
}

impl<'a, E: ProcessEdgesWork> Drop for ObjectsClosure<'a, E> {
    fn drop(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.flush();
    }
}

struct EdgeIteratorImpl<VM: VMBinding, F: FnMut(VM::VMEdge)> {
    f: F,
    should_discover_references: bool,
    should_claim_clds: bool,
    should_follow_clds: bool,
    _p: PhantomData<VM>,
}

impl<VM: VMBinding, F: FnMut(VM::VMEdge)> EdgeVisitor<VM::VMEdge> for EdgeIteratorImpl<VM, F> {
    fn should_discover_references(&self) -> bool {
        self.should_discover_references
    }
    fn should_claim_clds(&self) -> bool {
        self.should_claim_clds
    }
    fn should_follow_clds(&self) -> bool {
        self.should_follow_clds
    }
    fn visit_edge(&mut self, slot: VM::VMEdge) {
        (self.f)(slot);
    }
}

pub struct EdgeIterator<VM: VMBinding> {
    _p: PhantomData<VM>,
}

impl<VM: VMBinding> EdgeIterator<VM> {
    pub fn iterate(
        o: ObjectReference,
        should_discover_references: bool,
        should_claim_clds: bool,
        should_follow_clds: bool,
        f: impl FnMut(VM::VMEdge),
        klass: Option<Address>,
    ) {
        let mut x = EdgeIteratorImpl::<VM, _> {
            f,
            should_discover_references,
            should_claim_clds,
            should_follow_clds,
            _p: PhantomData,
        };
        if let Some(klass) = klass {
            <VM::VMScanning as Scanning<VM>>::scan_object_with_klass(
                VMWorkerThread(VMThread::UNINITIALIZED),
                o,
                &mut x,
                klass,
            );
        } else {
            <VM::VMScanning as Scanning<VM>>::scan_object(
                VMWorkerThread(VMThread::UNINITIALIZED),
                o,
                &mut x,
            );
        }
    }
}
