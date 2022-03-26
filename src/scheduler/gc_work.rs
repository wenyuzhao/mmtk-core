use super::work_bucket::WorkBucketStage;
use super::*;
use crate::plan::immix::Immix;
use crate::plan::immix::Pause;
use crate::plan::GcStatus;
use crate::policy::immix::block::Block;
use crate::policy::immix::block::BlockState;
use crate::policy::immix::cset::MatureEvacJobsCounter;
use crate::policy::immix::line::Line;
use crate::policy::immix::region::Region;
use crate::policy::immix::ImmixSpace;
use crate::policy::space::Space;
use crate::util::cm::LXRMatureEvacProcessEdges;
use crate::util::metadata::side_metadata::address_to_meta_address;
use crate::util::metadata::*;
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
        crate::GC_TRIGGER_TIME.store(SystemTime::now(), Ordering::SeqCst);
        crate::GC_EPOCH.fetch_add(1, Ordering::SeqCst);
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
pub struct Prepare<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
}

impl<C: GCWorkContext> Prepare<C> {
    pub fn new(plan: &'static C::PlanType) -> Self {
        Self { plan }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for Prepare<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Prepare Global");
        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut C::PlanType = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.prepare(worker.tls);

        if !crate::args::REF_COUNT {
            for mutator in <C::VM as VMBinding>::VMActivePlan::mutators() {
                mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                    .add(PrepareMutator::<C::VM>::new(mutator));
            }
            for w in &mmtk.scheduler.worker_group().workers {
                w.local_work_bucket
                    .add(PrepareCollector::<C::CopyContextType>::new());
            }
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
    #[allow(unused)]
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
    #[allow(unused)]
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
pub struct Release<C: GCWorkContext> {
    pub plan: &'static C::PlanType,
}

impl<C: GCWorkContext> Release<C> {
    pub fn new(plan: &'static C::PlanType) -> Self {
        Self { plan }
    }
}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for Release<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Release Global");
        <C::VM as VMBinding>::VMCollection::vm_release();
        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        let plan_mut: &mut C::PlanType = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.release(worker.tls);

        if !crate::args::REF_COUNT {
            for mutator in <C::VM as VMBinding>::VMActivePlan::mutators() {
                mmtk.scheduler.work_buckets[WorkBucketStage::Release]
                    .add(ReleaseMutator::<C::VM>::new(mutator));
            }
        }
        for w in &mmtk.scheduler.worker_group().workers {
            // w.local_work_bucket.add(ReleaseCollector::<C::CopyContextType>::new());
            let w = unsafe { &mut *(w as *const _ as *mut GCWorker<C::VM>) };
            unsafe { w.local::<C::CopyContextType>() }.release();
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
    #[allow(unused)]
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
    #[allow(unused)]
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
        mmtk.plan.base().prepare_for_stack_scanning();
        <E::VM as VMBinding>::VMCollection::stop_all_mutators::<E>(worker.tls);
        if crate::args::LOG_PER_GC_STATE {
            crate::RESERVED_PAGES_AT_GC_START
                .store(mmtk.plan.get_pages_reserved(), Ordering::SeqCst);
        }
        if crate::args::LOG_STAGES {
            println!(
                " - [{:.6}ms] Stop mutators done",
                crate::gc_trigger_time() as f64 / 1000000f64
            );
        }
        mmtk.plan.gc_pause_start();
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
        <E::VM as VMBinding>::VMScanning::scan_vm_specific_roots::<E>();
    }
}

impl<E: ProcessEdgesWork> CoordinatorWork<E::VM> for StopMutators<E> {}

#[derive(Default)]
pub struct EndOfGC;

impl<VM: VMBinding> GCWork<VM> for EndOfGC {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        info!("End of GC");
        let pause_time = crate::GC_START_TIME
            .load(Ordering::SeqCst)
            .elapsed()
            .unwrap();
        crate::add_copy_bytes(unsafe { crate::SLOPPY_COPY_BYTES });
        let pause = mmtk
            .plan
            .downcast_ref::<Immix<VM>>()
            .map(|ix| ix.current_pause().unwrap())
            .unwrap_or(Pause::FullTraceFast);
        crate::add_pause_time(pause, pause_time.as_nanos());
        if crate::args::LOG_PER_GC_STATE {
            let _released_n =
                crate::policy::immix::immixspace::RELEASED_NURSERY_BLOCKS.load(Ordering::SeqCst);
            let _released =
                crate::policy::immix::immixspace::RELEASED_BLOCKS.load(Ordering::SeqCst);
            // println!("Released {} blocks ({} nursery)", released, released_n);
            crate::policy::immix::immixspace::RELEASED_NURSERY_BLOCKS.store(0, Ordering::SeqCst);
            crate::policy::immix::immixspace::RELEASED_BLOCKS.store(0, Ordering::SeqCst);

            let pause_time = pause_time.as_micros() as f64 / 1000f64;
            let boot_time = crate::BOOT_TIME.elapsed().unwrap().as_millis() as f64 / 1000f64;
            let pause = match pause {
                Pause::RefCount => "RefCount",
                Pause::InitialMark => "InitialMark",
                Pause::FinalMark => "FinalMark",
                _ => "Full",
            };
            println!(
                "[{:.3}s][info][gc] GC({}) Pause {} {}M->{}M({}M) {:.3}ms",
                boot_time,
                crate::GC_EPOCH.load(Ordering::SeqCst),
                pause,
                crate::RESERVED_PAGES_AT_GC_START.load(Ordering::SeqCst) / 256,
                mmtk.plan.get_pages_reserved() / 256,
                mmtk.plan.get_total_pages() / 256,
                pause_time
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
        self.0.prepare(worker.tls);
        self.0.flush();

        if mmtk.plan.base().inform_stack_scanned(mutators) {
            <E::VM as VMBinding>::VMScanning::notify_initial_thread_scan_complete(
                false, worker.tls,
            );
            base.set_gc_status(GcStatus::GcProper);
        }
    }
}

#[derive(Default)]
pub struct ScanVMSpecificRoots<Edges: ProcessEdgesWork>(PhantomData<Edges>);

impl<E: ProcessEdgesWork> ScanVMSpecificRoots<E> {
    #[allow(unused)]
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
    const CAPACITY: usize = 512;
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
            self.worker()
                .add_work(WorkBucketStage::Closure, work_packet);
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
}

impl UnlogEdges {
    pub fn new(edges: Vec<Address>) -> Self {
        Self { edges }
    }
}

impl<VM: VMBinding> GCWork<VM> for UnlogEdges {
    #[inline(always)]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        if !self.edges.is_empty() {
            for edge in &self.edges {
                let ptr = address_to_meta_address(
                    VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
                    *edge,
                );
                unsafe {
                    ptr.store(0b11111111u8);
                }
            }
        }
    }
}

pub struct EvacuateMatureObjects<VM: VMBinding> {
    remset: Vec<Address>,
    _p: PhantomData<VM>,
    _counter: MatureEvacJobsCounter<VM>,
}

unsafe impl<VM: VMBinding> Send for EvacuateMatureObjects<VM> {}
unsafe impl<VM: VMBinding> Sync for EvacuateMatureObjects<VM> {}

impl<VM: VMBinding> EvacuateMatureObjects<VM> {
    pub const CAPACITY: usize = 128;

    pub fn new(remset: Vec<Address>, space: &ImmixSpace<VM>) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        Self {
            remset,
            _p: PhantomData,
            _counter: MatureEvacJobsCounter::new(unsafe { &*(space as *const _) }),
        }
    }

    #[inline(always)]
    fn address_is_valid_oop_edge(&self, e: Address, epoch: u8, immix: &Immix<VM>) -> bool {
        // Skip edges not in the mmtk heap
        if !immix.immix_space.address_in_space(e) && !immix.los().address_in_space(e) {
            return false;
        }
        if crate::args::NO_RC_PAUSES_DURING_CONCURRENT_MARKING {
            return true;
        }
        // Check if it is a real oop field
        if immix.immix_space.address_in_space(e) {
            let block = Block::of(e);
            if block.get_state() == BlockState::Unallocated {
                return false;
            }
            if cfg!(feature = "slow_edge_check") {
                let mut cursor = e - 16;
                while cursor >= block.start() {
                    // Skip straddle lines
                    if rc::address_is_in_straddle_line(cursor) {
                        cursor = Line::align(cursor);
                        cursor = cursor - rc::MIN_OBJECT_SIZE;
                        continue;
                    }
                    // Check if the edge points to a real oop
                    let mut o = unsafe { cursor.to_object_reference() };
                    if rc::count(o) != 0 {
                        o = o.fix_start_address::<VM>();
                        let end = o.to_address() + o.get_size::<VM>();
                        if end <= e {
                            return false;
                        }
                        return VM::VMScanning::is_oop_field(o, e);
                    }
                    cursor = cursor - rc::MIN_OBJECT_SIZE;
                }
                return false;
            } else {
                if Line::of(e).pointer_is_valid(epoch) {
                    return true;
                }
                false
            }
        } else {
            true
        }
    }

    #[inline]
    fn process_edge(&mut self, e: Address, epoch: u8, immix: &Immix<VM>) -> bool {
        if !e.is_mapped() {
            return false;
        }
        // Skip edges that does not contain a real oop
        if !self.address_is_valid_oop_edge(e, epoch, immix) {
            return false;
        }
        // Skip objects that are dead or out of the collection set.
        let o = unsafe { e.load::<ObjectReference>() };
        if !immix.immix_space.in_space(o) {
            return false;
        }
        // Maybe a forwarded nursery or mature object from inc processing.
        if object_forwarding::is_forwarded_or_being_forwarded::<VM>(o) {
            return true;
        }
        rc::count(o) != 0 && Region::containing::<VM>(o).is_defrag_source_active()
    }
}

impl<VM: VMBinding> GCWork<VM> for EvacuateMatureObjects<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        debug_assert!(
            immix.current_pause() == Some(Pause::FinalMark)
                || immix.current_pause() == Some(Pause::FullTraceFast)
        );
        // cleanup edges
        let mut remset = vec![];
        mem::swap(&mut remset, &mut self.remset);
        let edges = remset
            .drain_filter(|e| {
                let (e, epoch) = Line::decode_validity_state(*e);
                self.process_edge(e, epoch, immix)
            })
            .map(|e| Line::decode_validity_state(e).0)
            .collect::<Vec<_>>();
        // transitive closure
        let w = LXRMatureEvacProcessEdges::new(edges, false, mmtk);
        worker.add_work(WorkBucketStage::Closure, w);
    }
}
