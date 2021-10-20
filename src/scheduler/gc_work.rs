use super::work_bucket::WorkBucketStage;
use super::*;
use crate::plan::immix::Immix;
use crate::plan::immix::ImmixCopyContext;
use crate::plan::immix::Pause;
use crate::plan::EdgeIterator;
use crate::plan::GcStatus;
use crate::policy::immix::block::Block;
use crate::policy::immix::line::Line;
use crate::policy::immix::ImmixSpace;
use crate::policy::space::Space;
use crate::util::metadata::side_metadata::address_to_meta_address;
use crate::util::metadata::*;
use crate::util::rc::ProcessIncs;
use crate::util::*;
use crate::vm::*;
use crate::*;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::time::SystemTime;

pub struct ScheduleCollection;

impl<VM: VMBinding> GCWork<VM> for ScheduleCollection {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        if crate::flags::LOG_PER_GC_STATE {
            *crate::GC_TRIGGER_TIME.lock() = Some(SystemTime::now());
        }
        mmtk.plan.schedule_collection(worker.scheduler());
    }
}

impl<VM: VMBinding> CoordinatorWork<VM> for ScheduleCollection {}

/// The global GC Preparation Work
/// This work packet invokes prepare() for the plan (which will invoke prepare() for each space), and
/// pushes work packets for preparing mutators and collectors.
/// We should only have one such work packet per GC, before any actual GC work starts.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct Prepare<P: Plan, W: CopyContext + GCWorkerLocal> {
    pub plan: &'static P,
    _p: PhantomData<W>,
}

impl<P: Plan, W: CopyContext + GCWorkerLocal> Prepare<P, W> {
    pub fn new(plan: &'static P) -> Self {
        Self {
            plan,
            _p: PhantomData,
        }
    }
}

impl<P: Plan, W: CopyContext + GCWorkerLocal> GCWork<P::VM> for Prepare<P, W> {
    fn do_work(&mut self, worker: &mut GCWorker<P::VM>, mmtk: &'static MMTK<P::VM>) {
        trace!("Prepare Global");
        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut P = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.prepare(worker.tls);

        for mutator in <P::VM as VMBinding>::VMActivePlan::mutators() {
            mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                .add(PrepareMutator::<P::VM>::new(mutator));
        }
        for w in &mmtk.scheduler.worker_group().workers {
            w.local_work_bucket.add(PrepareCollector::<W>::new());
        }
    }
}

/// The mutator GC Preparation Work
pub struct PrepareMutator<VM: VMBinding> {
    // The mutator reference has static lifetime.
    // It is safe because the actual lifetime of this work-packet will not exceed the lifetime of a GC.
    pub mutator: &'static mut Mutator<VM>,
}

impl<VM: VMBinding> PrepareMutator<VM> {
    pub fn new(mutator: &'static mut Mutator<VM>) -> Self {
        Self { mutator }
    }
}

impl<VM: VMBinding> GCWork<VM> for PrepareMutator<VM> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        trace!("Prepare Mutator");
        self.mutator.prepare(worker.tls);
    }
}

/// The collector GC Preparation Work
#[derive(Default)]
pub struct PrepareCollector<W: CopyContext + GCWorkerLocal>(PhantomData<W>);

impl<W: CopyContext + GCWorkerLocal> PrepareCollector<W> {
    pub fn new() -> Self {
        PrepareCollector(PhantomData)
    }
}

impl<VM: VMBinding, W: CopyContext + GCWorkerLocal> GCWork<VM> for PrepareCollector<W> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        trace!("Prepare Collector");
        unsafe { worker.local::<W>() }.prepare();
    }
}

/// The global GC release Work
/// This work packet invokes release() for the plan (which will invoke release() for each space), and
/// pushes work packets for releasing mutators and collectors.
/// We should only have one such work packet per GC, after all actual GC work ends.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct Release<P: Plan, W: CopyContext + GCWorkerLocal> {
    pub plan: &'static P,
    _p: PhantomData<W>,
}

impl<P: Plan, W: CopyContext + GCWorkerLocal> Release<P, W> {
    pub fn new(plan: &'static P) -> Self {
        Self {
            plan,
            _p: PhantomData,
        }
    }
}

impl<P: Plan, W: CopyContext + GCWorkerLocal> GCWork<P::VM> for Release<P, W> {
    fn do_work(&mut self, worker: &mut GCWorker<P::VM>, mmtk: &'static MMTK<P::VM>) {
        trace!("Release Global");
        <P::VM as VMBinding>::VMCollection::vm_release();
        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut P = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.release(worker.tls);

        for mutator in <P::VM as VMBinding>::VMActivePlan::mutators() {
            mmtk.scheduler.work_buckets[WorkBucketStage::Release]
                .add(ReleaseMutator::<P::VM>::new(mutator));
        }
        for w in &mmtk.scheduler.worker_group().workers {
            w.local_work_bucket.add(ReleaseCollector::<W>::new());
        }
        // TODO: Process weak references properly
        mmtk.reference_processors.clear();
    }
}

/// The mutator release Work
pub struct ReleaseMutator<VM: VMBinding> {
    // The mutator reference has static lifetime.
    // It is safe because the actual lifetime of this work-packet will not exceed the lifetime of a GC.
    pub mutator: &'static mut Mutator<VM>,
}

impl<VM: VMBinding> ReleaseMutator<VM> {
    pub fn new(mutator: &'static mut Mutator<VM>) -> Self {
        Self { mutator }
    }
}

impl<VM: VMBinding> GCWork<VM> for ReleaseMutator<VM> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        trace!("Release Mutator");
        self.mutator.release(worker.tls);
    }
}

/// The collector release Work
#[derive(Default)]
pub struct ReleaseCollector<W: CopyContext + GCWorkerLocal>(PhantomData<W>);

impl<W: CopyContext + GCWorkerLocal> ReleaseCollector<W> {
    pub fn new() -> Self {
        ReleaseCollector(PhantomData)
    }
}

impl<VM: VMBinding, W: CopyContext + GCWorkerLocal> GCWork<VM> for ReleaseCollector<W> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        trace!("Release Collector");
        unsafe { worker.local::<W>() }.release();
    }
}

/// Stop all mutators
///
/// Schedule a `ScanStackRoots` immediately after a mutator is paused
///
/// TODO: Smaller work granularity
#[derive(Default)]
pub struct StopMutators<ScanEdges: ProcessEdgesWork>(PhantomData<ScanEdges>);

impl<ScanEdges: ProcessEdgesWork> StopMutators<ScanEdges> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for StopMutators<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        // If the VM requires that only the coordinator thread can stop the world,
        // we delegate the work to the coordinator.
        if <E::VM as VMBinding>::VMCollection::COORDINATOR_ONLY_STW && !worker.is_coordinator() {
            mmtk.scheduler
                .add_coordinator_work(StopMutators::<E>::new(), worker);
            return;
        }

        trace!("stop_all_mutators start");
        debug_assert_eq!(mmtk.plan.base().scanned_stacks.load(Ordering::SeqCst), 0);
        <E::VM as VMBinding>::VMCollection::stop_all_mutators::<E>(worker.tls);
        if crate::flags::LOG_PER_GC_STATE {
            *crate::GC_START_TIME.lock() = Some(SystemTime::now());
        }
        mmtk.plan.gc_pause_start();
        trace!("stop_all_mutators end");
        mmtk.scheduler.notify_mutators_paused(mmtk);
        if <E::VM as VMBinding>::VMScanning::SCAN_MUTATORS_IN_SAFEPOINT {
            // Prepare mutators if necessary
            // FIXME: This test is probably redundant. JikesRVM requires to call `prepare_mutator` once after mutators are paused
            if !mmtk.plan.base().stacks_prepared() {
                for mutator in <E::VM as VMBinding>::VMActivePlan::mutators() {
                    <E::VM as VMBinding>::VMCollection::prepare_mutator(
                        worker.tls,
                        mutator.get_tls(),
                        mutator,
                    );
                }
            }
            // Scan mutators
            if <E::VM as VMBinding>::VMScanning::SINGLE_THREAD_MUTATOR_SCANNING {
                mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                    .add(ScanStackRoots::<E>::new());
            } else {
                for mutator in <E::VM as VMBinding>::VMActivePlan::mutators() {
                    mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                        .add(ScanStackRoot::<E>(mutator));
                }
            }
        }
        mmtk.scheduler.work_buckets[WorkBucketStage::ScanGlobalRoots]
            .add(ScanVMSpecificRoots::<E>::new());
    }
}

impl<E: ProcessEdgesWork> CoordinatorWork<E::VM> for StopMutators<E> {}

#[derive(Default)]
pub struct EndOfGC;

impl<VM: VMBinding> GCWork<VM> for EndOfGC {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        info!("End of GC");
        if crate::flags::LOG_PER_GC_STATE {
            let released =
                crate::policy::immix::immixspace::RELEASED_NURSERY_BLOCKS.load(Ordering::SeqCst);
            println!("Eager released {} nursery blocks", released);
            let released = crate::policy::immix::immixspace::RELEASED_BLOCKS.load(Ordering::SeqCst);
            println!("Released {} blocks", released);
            crate::policy::immix::immixspace::RELEASED_NURSERY_BLOCKS.store(0, Ordering::SeqCst);
            crate::policy::immix::immixspace::RELEASED_BLOCKS.store(0, Ordering::SeqCst);

            println!(
                "> {}ms {}ms",
                crate::GC_TRIGGER_TIME
                    .lock()
                    .as_ref()
                    .unwrap()
                    .elapsed()
                    .unwrap()
                    .as_millis(),
                crate::GC_START_TIME
                    .lock()
                    .as_ref()
                    .unwrap()
                    .elapsed()
                    .unwrap()
                    .as_millis()
            );
        }

        #[cfg(feature = "extreme_assertions")]
        if crate::util::edge_logger::should_check_duplicate_edges(&*mmtk.plan) {
            // reset the logging info at the end of each GC
            crate::util::edge_logger::reset();
        }

        mmtk.plan.gc_pause_end();
        if <VM as VMBinding>::VMCollection::COORDINATOR_ONLY_STW {
            assert!(worker.is_coordinator(),
                    "VM only allows coordinator to resume mutators, but the current worker is not the coordinator.");
        }

        mmtk.plan.base().set_gc_status(GcStatus::NotInGC);

        // Reset the triggering information.
        mmtk.plan.base().reset_collection_trigger();

        <VM as VMBinding>::VMCollection::resume_mutators(worker.tls);
    }
}

impl<VM: VMBinding> CoordinatorWork<VM> for EndOfGC {}

/// Delegate to the VM binding for reference processing.
///
/// Some VMs (e.g. v8) do not have a Java-like global weak reference storage, and the
/// processing of those weakrefs may be more complex. For such case, we delegate to the
/// VM binding to process weak references.
#[derive(Default)]
pub struct ProcessWeakRefs<E: ProcessEdgesWork>(PhantomData<E>);

impl<E: ProcessEdgesWork> ProcessWeakRefs<E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ProcessWeakRefs<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, _mmtk: &'static MMTK<E::VM>) {
        trace!("ProcessWeakRefs");
        <E::VM as VMBinding>::VMCollection::process_weak_refs::<E>(worker);
    }
}

#[derive(Default)]
pub struct ScanStackRoots<Edges: ProcessEdgesWork>(PhantomData<Edges>);

impl<E: ProcessEdgesWork> ScanStackRoots<E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ScanStackRoots<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("ScanStackRoots");
        <E::VM as VMBinding>::VMScanning::scan_thread_roots::<E>();
        <E::VM as VMBinding>::VMScanning::notify_initial_thread_scan_complete(false, worker.tls);
        for mutator in <E::VM as VMBinding>::VMActivePlan::mutators() {
            mutator.flush();
        }
        mmtk.plan.common().base.set_gc_status(GcStatus::GcProper);
    }
}

pub struct ScanStackRoot<Edges: ProcessEdgesWork>(pub &'static mut Mutator<Edges::VM>);

impl<E: ProcessEdgesWork> GCWork<E::VM> for ScanStackRoot<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("ScanStackRoot for mutator {:?}", self.0.get_tls());
        let base = &mmtk.plan.base();
        let mutators = <E::VM as VMBinding>::VMActivePlan::number_of_mutators();
        <E::VM as VMBinding>::VMScanning::scan_thread_root::<E>(
            unsafe { &mut *(self.0 as *mut _) },
            worker.tls,
        );
        self.0.flush();
        let old = base.scanned_stacks.fetch_add(1, Ordering::SeqCst);
        trace!(
            "mutator {:?} old scanned_stacks = {}, new scanned_stacks = {}",
            self.0.get_tls(),
            old,
            base.scanned_stacks.load(Ordering::Relaxed)
        );

        if old + 1 >= mutators {
            loop {
                let current = base.scanned_stacks.load(Ordering::Relaxed);
                if current < mutators {
                    break;
                } else if base.scanned_stacks.compare_exchange(
                    current,
                    current - mutators,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) == Ok(current)
                {
                    trace!(
                        "mutator {:?} old scanned_stacks = {}, new scanned_stacks = {}, number_of_mutators = {}",
                        self.0.get_tls(),
                        current,
                        base.scanned_stacks.load(Ordering::Relaxed),
                        mutators
                    );
                    <E::VM as VMBinding>::VMScanning::notify_initial_thread_scan_complete(
                        false, worker.tls,
                    );
                    base.set_gc_status(GcStatus::GcProper);
                    break;
                }
            }
        }
    }
}

#[derive(Default)]
pub struct ScanVMSpecificRoots<Edges: ProcessEdgesWork>(PhantomData<Edges>);

impl<E: ProcessEdgesWork> ScanVMSpecificRoots<E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ScanVMSpecificRoots<E> {
    fn do_work(&mut self, _worker: &mut GCWorker<E::VM>, _mmtk: &'static MMTK<E::VM>) {
        trace!("ScanStaticRoots");
        <E::VM as VMBinding>::VMScanning::scan_vm_specific_roots::<E>();
    }
}

pub struct ProcessEdgesBase<E: ProcessEdgesWork> {
    pub edges: Vec<Address>,
    pub nodes: Vec<ObjectReference>,
    mmtk: &'static MMTK<E::VM>,
    // Use raw pointer for fast pointer dereferencing, instead of using `Option<&'static mut GCWorker<E::VM>>`.
    // Because a copying gc will dereference this pointer at least once for every object copy.
    worker: *mut GCWorker<E::VM>,
    pub roots: bool,
}

unsafe impl<E: ProcessEdgesWork> Send for ProcessEdgesBase<E> {}

impl<E: ProcessEdgesWork> ProcessEdgesBase<E> {
    // Requires an MMTk reference. Each plan-specific type that uses ProcessEdgesBase can get a static plan reference
    // at creation. This avoids overhead for dynamic dispatch or downcasting plan for each object traced.
    pub fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<E::VM>) -> Self {
        #[cfg(feature = "extreme_assertions")]
        if crate::util::edge_logger::should_check_duplicate_edges(&*mmtk.plan) {
            for edge in &edges {
                // log edge, panic if already logged
                crate::util::edge_logger::log_edge(*edge);
            }
        }
        Self {
            edges,
            nodes: vec![],
            mmtk,
            worker: std::ptr::null_mut(),
            roots,
        }
    }
    pub fn set_worker(&mut self, worker: &mut GCWorker<E::VM>) {
        self.worker = worker;
    }
    #[inline]
    pub fn worker(&self) -> &'static mut GCWorker<E::VM> {
        unsafe { &mut *self.worker }
    }
    #[inline]
    pub fn mmtk(&self) -> &'static MMTK<E::VM> {
        self.mmtk
    }
    #[inline]
    pub fn plan(&self) -> &'static dyn Plan<VM = E::VM> {
        &*self.mmtk.plan
    }
    /// Pop all nodes from nodes, and clear nodes to an empty vector.
    #[inline]
    pub fn pop_nodes(&mut self) -> Vec<ObjectReference> {
        debug_assert!(
            !self.nodes.is_empty(),
            "Attempted to flush nodes in ProcessEdgesWork while nodes set is empty."
        );
        let mut new_nodes = vec![];
        mem::swap(&mut new_nodes, &mut self.nodes);
        new_nodes
    }
}

/// Scan & update a list of object slots
pub trait ProcessEdgesWork:
    Send + 'static + Sized + DerefMut + Deref<Target = ProcessEdgesBase<Self>>
{
    type VM: VMBinding;
    const CAPACITY: usize = 4096;
    const OVERWRITE_REFERENCE: bool = true;
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;
    const RC_ROOTS: bool = false;
    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<Self::VM>) -> Self;
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference;

    #[cfg(feature = "sanity")]
    fn cache_roots_for_sanity_gc(&mut self) {
        assert!(self.roots);
        self.mmtk()
            .sanity_checker
            .lock()
            .unwrap()
            .add_roots(self.edges.clone());
    }

    #[inline]
    fn process_node(&mut self, object: ObjectReference) {
        if self.nodes.is_empty() {
            self.nodes.reserve(Self::CAPACITY);
        }
        self.nodes.push(object);
        // No need to flush this `nodes` local buffer to some global pool.
        // The max length of `nodes` buffer is equal to `CAPACITY` (when every edge produces a node)
        // So maximum 1 `ScanObjects` work can be created from `nodes` buffer
    }

    /// Create a new scan work packet. If SCAN_OBJECTS_IMMEDIATELY, the work packet will be executed immediately, in this method.
    /// Otherwise, the work packet will be added the Closure work bucket and will be dispatched later by the scheduler.
    #[inline]
    fn new_scan_work(&mut self, work_packet: impl GCWork<Self::VM>) {
        if Self::SCAN_OBJECTS_IMMEDIATELY {
            // We execute this `scan_objects_work` immediately.
            // This is expected to be a useful optimization because,
            // say for _pmd_ with 200M heap, we're likely to have 50000~60000 `ScanObjects` work packets
            // being dispatched (similar amount to `ProcessEdgesWork`).
            // Executing these work packets now can remarkably reduce the global synchronization time.
            self.worker().do_work(work_packet);
        } else {
            self.mmtk.scheduler.work_buckets[WorkBucketStage::Closure].add(work_packet);
        }
    }

    /// Flush the nodes in ProcessEdgesBase, and create a ScanObjects work packet for it. If the node set is empty,
    /// this method will simply return with no work packet created.
    #[cold]
    fn flush(&mut self) {
        if self.nodes.is_empty() {
            return;
        }
        let scan_objects_work = ScanObjects::<Self>::new(self.pop_nodes(), false);
        self.new_scan_work(scan_objects_work);
    }

    #[inline]
    fn process_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE {
            unsafe { slot.store(new_object) };
        }
    }

    #[inline]
    fn process_edges(&mut self) {
        for i in 0..self.edges.len() {
            self.process_edge(self.edges[i])
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for E {
    #[inline]
    default fn do_work(&mut self, worker: &mut GCWorker<E::VM>, _mmtk: &'static MMTK<E::VM>) {
        trace!("ProcessEdgesWork");
        self.set_worker(worker);
        self.process_edges();
        self.flush();
        #[cfg(feature = "sanity")]
        if self.roots {
            self.cache_roots_for_sanity_gc();
        }
        trace!("ProcessEdgesWork End");
    }
}

/// Scan & update a list of object slots
pub struct ScanObjects<Edges: ProcessEdgesWork> {
    buffer: Vec<ObjectReference>,
    #[allow(unused)]
    concurrent: bool,
    phantom: PhantomData<Edges>,
}

impl<Edges: ProcessEdgesWork> ScanObjects<Edges> {
    pub fn new(buffer: Vec<ObjectReference>, concurrent: bool) -> Self {
        Self {
            buffer,
            concurrent,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ScanObjects<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, _mmtk: &'static MMTK<E::VM>) {
        trace!("ScanObjects");
        <E::VM as VMBinding>::VMScanning::scan_objects::<E>(&self.buffer, worker);
        trace!("ScanObjects End");
    }
}

pub struct ProcessModBuf<E: ProcessEdgesWork> {
    modbuf: Vec<ObjectReference>,
    phantom: PhantomData<E>,
    meta: MetadataSpec,
}

impl<E: ProcessEdgesWork> ProcessModBuf<E> {
    pub fn new(modbuf: Vec<ObjectReference>, meta: MetadataSpec) -> Self {
        Self {
            modbuf,
            meta,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ProcessModBuf<E> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        if !self.modbuf.is_empty() {
            for obj in &self.modbuf {
                store_metadata::<E::VM>(
                    &self.meta,
                    *obj,
                    crate::plan::barriers::UNLOGGED_VALUE,
                    None,
                    Some(Ordering::SeqCst),
                );
            }
        }
        if mmtk.plan.is_current_gc_nursery() {
            if !self.modbuf.is_empty() {
                let mut modbuf = vec![];
                ::std::mem::swap(&mut modbuf, &mut self.modbuf);
                GCWork::do_work(&mut ScanObjects::<E>::new(modbuf, false), worker, mmtk)
            }
        } else {
            // Do nothing
        }
    }
}

pub struct EdgesProcessModBuf<E: ProcessEdgesWork> {
    modbuf: Vec<Address>,
    phantom: PhantomData<E>,
    meta: MetadataSpec,
}

impl<E: ProcessEdgesWork> EdgesProcessModBuf<E> {
    pub fn new(modbuf: Vec<Address>, meta: MetadataSpec) -> Self {
        Self {
            modbuf,
            meta,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for EdgesProcessModBuf<E> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        if !self.modbuf.is_empty() {
            for e in &self.modbuf {
                store_metadata::<E::VM>(
                    &self.meta,
                    unsafe { e.to_object_reference() },
                    crate::plan::barriers::UNLOGGED_VALUE,
                    None,
                    Some(Ordering::SeqCst),
                );
            }
        }
        if mmtk.plan.is_current_gc_nursery() {
            if !self.modbuf.is_empty() {
                let mut modbuf = vec![];
                ::std::mem::swap(&mut modbuf, &mut self.modbuf);
                GCWork::do_work(&mut E::new(modbuf, false, mmtk), worker, mmtk)
            }
        } else {
            // Do nothing
        }
    }
}

pub struct UnlogEdges {
    edges: Vec<Address>,
    meta: MetadataSpec,
}

impl UnlogEdges {
    pub fn new(edges: Vec<Address>, meta: MetadataSpec) -> Self {
        Self { edges, meta }
    }
}

impl<VM: VMBinding> GCWork<VM> for UnlogEdges {
    #[inline(always)]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        if !self.edges.is_empty() {
            for edge in &self.edges {
                let ptr = address_to_meta_address(self.meta.extract_side_spec(), *edge);
                unsafe {
                    ptr.store(0b11111111u8);
                }
            }
        }
    }
}

pub struct EvacuateMatureObjects<VM: VMBinding> {
    remset: Vec<ObjectReference>,
    roots: Vec<Address>,
    next_remset: Vec<ObjectReference>,
    mmtk: Option<&'static MMTK<VM>>,
    worker: *mut GCWorker<VM>,
}

unsafe impl<VM: VMBinding> Send for EvacuateMatureObjects<VM> {}
unsafe impl<VM: VMBinding> Sync for EvacuateMatureObjects<VM> {}

impl<VM: VMBinding> EvacuateMatureObjects<VM> {
    const CAPACITY: usize = 1024;

    pub fn new(remset: Vec<ObjectReference>) -> Self {
        // println!("EvacuateMatureObjects {:?}", remset.iter().map(|o| (o.to_address()..o.to_address()+o.get_size::<VM>())).collect::<Vec<_>>());
        Self {
            remset,
            next_remset: vec![],
            mmtk: None,
            roots: vec![],
            worker: std::ptr::null_mut(),
        }
    }

    pub fn new_roots(roots: Vec<Address>) -> Self {
        // println!("EvacuateMatureObjects {:?} {}", remset, roots);
        Self {
            remset: vec![],
            next_remset: vec![],
            mmtk: None,
            roots,
            worker: std::ptr::null_mut(),
        }
    }

    const fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    fn forward(&mut self, o: ObjectReference, immix_space: &ImmixSpace<VM>) -> ObjectReference {
        let copy_context = unsafe { self.worker().local::<ImmixCopyContext<VM>>() };
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(o);
        if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            object_forwarding::spin_and_get_forwarded_object::<VM>(o, forwarding_status)
        } else {
            // Evacuate the young object
            let new = object_forwarding::forward_object::<VM, _>(
                o,
                AllocationSemantics::Default,
                copy_context,
            );
            // Transfer RC countsf
            new.log_start_address::<VM>();
            if !crate::flags::BLOCK_ONLY {
                if new.get_size::<VM>() > Line::BYTES {
                    rc::mark_straddle_object::<VM>(new);
                }
            }
            rc::set(new, rc::count(o));
            rc::set(o, 0);
            rc::unmark_straddle_object::<VM>(o);
            immix_space.attempt_mark(new);
            self.process_node(new);
            new
        }
    }

    fn process_node(&mut self, o: ObjectReference) {
        self.next_remset.push(o);
        if self.next_remset.len() >= Self::CAPACITY {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if !self.next_remset.is_empty() {
            let mut remset = vec![];
            mem::swap(&mut remset, &mut self.next_remset);
            let w = Self::new(remset);
            self.mmtk.unwrap().scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].add(w);
        }
    }

    fn forward_edge(&mut self, e: Address, root: bool, immix_space: &ImmixSpace<VM>) {
        // println!("fwd {:?} {}", e, root);
        if !root {
            ProcessIncs::<VM>::unlog_edge(e);
        }
        // println!("fwd 1 {:?} {}", e, root);
        let o = unsafe { e.load::<ObjectReference>() };
        // println!("fwd 1 {:?} -> {:?}", e, o.to_address());
        // println!("fwd 2 {:?} {} {:?} defrag={} rc={}", e, root, o, Block::containing::<VM>(o).is_defrag_source(), rc::count(o));
        if !immix_space.in_space(o) || !Block::containing::<VM>(o).is_defrag_source() {
            return;
        }
        // println!("fwd 3 {:?} {} {:?}", e, root, o);
        let new = self.forward(o, immix_space);
        // println!("fwd {:?}: {:?} -> {:?}", e, o.to_address()..o.to_address()+o.get_size::<VM>(), new.to_address()..new.to_address()+new.get_size::<VM>());
        // println!("fwd 4 {:?} {} {:?}", e, root, o);
        unsafe {
            e.store(new);
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for EvacuateMatureObjects<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        // println!("R EvacuateMatureObjects {:?}", self.remset);
        self.mmtk = Some(mmtk);
        self.worker = worker;
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        debug_assert_eq!(immix.current_pause(), Some(Pause::FinalMark));
        let immix_space = &immix.immix_space;
        // Roots
        let mut roots = vec![];
        mem::swap(&mut roots, &mut self.roots);
        for e in roots {
            // println!("scan r {:?}", e);
            self.forward_edge(e, true, immix_space)
        }
        // Objects
        let mut remset = vec![];
        mem::swap(&mut remset, &mut self.remset);
        for mut o in remset {
            // println!("scan {:?}", o);
            if o.is_null() {
                continue;
            }
            debug_assert!(o.is_mapped());
            if immix_space.in_space(o)
                && (rc::count(o) == 0 || Block::containing::<VM>(o).is_defrag_source())
            {
                // println!("scan {:?}", o.to_address()..o.to_address()+o.get_size::<VM>());
                // println!("scan {:?} skip", o);
                continue;
            }
            if !crate::flags::BLOCK_ONLY && immix_space.in_space(o) {
                let line = Line::containing::<VM>(o);
                if rc::count(unsafe { line.start().to_object_reference() }) == 1
                    && rc::is_straddle_line(line)
                {
                    continue;
                }
            }
            if immix_space.in_space(o) {
                // let a = unsafe { Address::from_usize(o.to_address().as_usize() >> 4 << 4) };
                // let b = a + 8usize;
                let o2 = o.fix_start_address::<VM>();
                // let x = unsafe {
                //     Address::from_usize(o2.to_address().as_usize() ^ 0b1000).to_object_reference()
                // };
                o = o2;
                // println!("scan x {:?} {:?} cls={:?} o?={:?} cls={:?} log({:?})={} log({:?})={} straddle={}", o, o2, o2.class_pointer(), x, x.class_pointer(), a, a.is_logged::<VM>(), b, b.is_logged::<VM>(), rc::is_straddle_line(Line::from(Line::align(o.to_address()))));
            }
            if !crate::flags::BLOCK_ONLY
                && immix_space.in_space(o)
                && Line::is_aligned(o.to_address())
            {
                if rc::count(o) == 1 && rc::is_straddle_line(Line::from(o.to_address())) {
                    continue;
                }
            }
            EdgeIterator::<VM>::iterate(o, |e| self.forward_edge(e, false, immix_space));
        }
        self.flush();
    }
}
