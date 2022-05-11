//! The fundamental mechanism for performing a transitive closure over an object graph.

use std::marker::PhantomData;
use std::mem;

use crate::scheduler::gc_work::ProcessEdgesWork;
use crate::scheduler::{GCWorker, WorkBucketStage};
use crate::util::{Address, ObjectReference, VMThread, VMWorkerThread};
use crate::vm::EdgeVisitor;
use crate::vm::{Scanning, VMBinding};

/// This trait is the fundamental mechanism for performing a
/// transitive closure over an object graph.
pub trait TransitiveClosure {
    // The signature of this function changes during the port
    // because the argument `ObjectReference source` is never used in the original version
    // See issue #5
    fn process_node(&mut self, object: ObjectReference);
}

impl<T: ProcessEdgesWork> TransitiveClosure for T {
    #[inline]
    fn process_node(&mut self, object: ObjectReference) {
        ProcessEdgesWork::process_node(self, object);
    }
}

/// A transitive closure visitor to collect all the edges of an object.
pub struct ObjectsClosure<'a, E: ProcessEdgesWork> {
    buffer: Vec<Address>,
    worker: &'a mut GCWorker<E::VM>,
}

impl<'a, E: ProcessEdgesWork> ObjectsClosure<'a, E> {
    pub fn new(worker: &'a mut GCWorker<E::VM>) -> Self {
        Self {
            buffer: vec![],
            worker,
        }
    }

    fn flush(&mut self) {
        let mut new_edges = Vec::new();
        mem::swap(&mut new_edges, &mut self.buffer);
        self.worker.add_work(
            WorkBucketStage::Closure,
            E::new(new_edges, false, self.worker.mmtk),
        );
    }
}

impl<'a, E: ProcessEdgesWork> EdgeVisitor for ObjectsClosure<'a, E> {
    #[inline(always)]
    fn visit_edge(&mut self, slot: Address) {
        if self.buffer.is_empty() {
            self.buffer.reserve(E::CAPACITY);
        }
        self.buffer.push(slot);
        if self.buffer.len() >= E::CAPACITY {
            let mut new_edges = Vec::new();
            mem::swap(&mut new_edges, &mut self.buffer);
            self.worker.add_work(
                WorkBucketStage::Closure,
                E::new(new_edges, false, self.worker.mmtk),
            );
        }
    }
}

impl<'a, E: ProcessEdgesWork> Drop for ObjectsClosure<'a, E> {
    #[inline(always)]
    fn drop(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.flush();
    }
}

struct EdgeIteratorImpl<VM: VMBinding, F: FnMut(Address)> {
    f: F,
    _p: PhantomData<VM>,
}

impl<VM: VMBinding, F: FnMut(Address)> EdgeVisitor for EdgeIteratorImpl<VM, F> {
    #[inline(always)]
    fn visit_edge(&mut self, slot: Address) {
        (self.f)(slot);
    }
}

pub struct EdgeIterator<VM: VMBinding> {
    _p: PhantomData<VM>,
}

impl<VM: VMBinding> EdgeIterator<VM> {
    #[inline(always)]
    pub fn iterate(o: ObjectReference, f: impl FnMut(Address)) {
        let mut x = EdgeIteratorImpl::<VM, _> { f, _p: PhantomData };
        <VM::VMScanning as Scanning<VM>>::scan_object(
            VMWorkerThread(VMThread::UNINITIALIZED),
            o,
            &mut x,
        );
    }
}
