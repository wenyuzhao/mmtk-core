//! The fundamental mechanism for performing a transitive closure over an object graph.

use std::marker::PhantomData;
use std::mem;

use atomic_traits::fetch::Add;

use crate::scheduler::gc_work::ProcessEdgesWork;
use crate::scheduler::{GCWorker, WorkBucketStage};
use crate::util::{Address, ObjectReference, VMThread, VMWorkerThread};
use crate::MMTK;
use crate::vm::{Scanning, VMBinding};

/// This trait is the fundamental mechanism for performing a
/// transitive closure over an object graph.
pub trait TransitiveClosure {
    // The signature of this function changes during the port
    // because the argument `ObjectReference source` is never used in the original version
    // See issue #5
    fn process_edge(&mut self, slot: Address);
    fn process_node(&mut self, object: ObjectReference);
}

impl<T: ProcessEdgesWork> TransitiveClosure for T {
    fn process_edge(&mut self, _slot: Address) {
        unreachable!();
    }
    #[inline]
    fn process_node(&mut self, object: ObjectReference) {
        ProcessEdgesWork::process_node(self, object);
    }
}

/// A transitive closure visitor to collect all the edges of an object.
pub struct ObjectsClosure<'a, E: ProcessEdgesWork> {
    mmtk: &'static MMTK<E::VM>,
    buffer: Vec<Address>,
    worker: &'a mut GCWorker<E::VM>,
}

impl<'a, E: ProcessEdgesWork> ObjectsClosure<'a, E> {
    pub fn new(
        mmtk: &'static MMTK<E::VM>,
        buffer: Vec<Address>,
        worker: &'a mut GCWorker<E::VM>,
    ) -> Self {
        Self {
            mmtk,
            buffer,
            worker,
        }
    }
}

impl<'a, E: ProcessEdgesWork> TransitiveClosure for ObjectsClosure<'a, E> {
    #[inline(always)]
    fn process_edge(&mut self, slot: Address) {
        if self.buffer.is_empty() {
            self.buffer.reserve(E::CAPACITY);
        }
        self.buffer.push(slot);
        if self.buffer.len() >= E::CAPACITY {
            let mut new_edges = Vec::new();
            mem::swap(&mut new_edges, &mut self.buffer);
            self.worker.add_work(
                WorkBucketStage::Closure,
                E::new(new_edges, false, self.mmtk),
            );
        }
    }
    fn process_node(&mut self, _object: ObjectReference) {
        unreachable!()
    }
}

impl<'a, E: ProcessEdgesWork> Drop for ObjectsClosure<'a, E> {
    #[inline(always)]
    fn drop(&mut self) {
        let mut new_edges = Vec::new();
        mem::swap(&mut new_edges, &mut self.buffer);
        self.worker.add_work(
            WorkBucketStage::Closure,
            E::new(new_edges, false, self.mmtk),
        );
    }
}

pub struct EdgeIterator<'a, VM: VMBinding> {
    f: Box<dyn FnMut(Address) + 'a>,
    _p: PhantomData<VM>,
}

impl<'a, VM: VMBinding> EdgeIterator<'a, VM> {
    pub fn iterate(o: ObjectReference, f: impl FnMut(Address) + 'a) {
        let mut x = Self { f: box f, _p: PhantomData };
        <VM::VMScanning as Scanning<VM>>::scan_object(&mut x, o, VMWorkerThread(VMThread::UNINITIALIZED));
    }
}

impl<'a, VM: VMBinding> TransitiveClosure for EdgeIterator<'a, VM> {
    #[inline(always)]
    fn process_edge(&mut self, slot: Address) {
        (self.f)(slot);
    }
    fn process_node(&mut self, _object: ObjectReference) {
        unreachable!()
    }
}
