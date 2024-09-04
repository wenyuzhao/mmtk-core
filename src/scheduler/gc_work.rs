use crossbeam::deque::Steal;

use self::global_state::GcStatus;
use self::util::address::CLDScanPolicy;
use self::util::address::RefScanPolicy;
use self::util::deque::Stolen;
use self::worker::GCWorkerShared;
use super::work_bucket::WorkBucketStage;
use super::*;
use crate::plan::lxr::LXR;
use crate::plan::ObjectsClosure;
use crate::plan::VectorObjectQueue;
use crate::util::metadata::side_metadata::address_to_meta_address;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::*;
use crate::vm::slot::Slot;
use crate::vm::*;
use crate::*;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;

pub struct ScheduleCollection;

impl<VM: VMBinding> GCWork<VM> for ScheduleCollection {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        crate::GC_TRIGGER_TIME.start();
        crate::GC_EPOCH.fetch_add(1, Ordering::SeqCst);
        // Tell GC trigger that GC started.
        mmtk.gc_trigger.policy.on_gc_start(mmtk);

        // Determine collection kind
        let is_emergency = mmtk.state.set_collection_kind(
            mmtk.get_plan().last_collection_was_exhaustive(),
            mmtk.gc_trigger.policy.can_heap_size_grow(),
        );
        if is_emergency {
            mmtk.get_plan().notify_emergency_collection();
        }
        // Set to GcPrepare
        mmtk.set_gc_status(GcStatus::GcPrepare);

        // Let the plan to schedule collection work
        mmtk.get_plan().schedule_collection(worker.scheduler());
    }
}

/// The global GC Preparation Work
/// This work packet invokes prepare() for the plan (which will invoke prepare() for each space), and
/// pushes work packets for preparing mutators and collectors.
/// We should only have one such work packet per GC, before any actual GC work starts.
/// We assume this work packet is the only running work packet that accesses plan, and there should
/// be no other concurrent work packet that accesses plan (read or write). Otherwise, there may
/// be a race condition.
pub struct Prepare<C: GCWorkContext> {
    pub plan: *const C::PlanType,
}

unsafe impl<C: GCWorkContext> Send for Prepare<C> {}

impl<C: GCWorkContext> Prepare<C> {
    pub fn new(plan: *const C::PlanType) -> Self {
        Self { plan }
    }
}

impl<C: GCWorkContext> GCWork<C::VM> for Prepare<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Prepare Global");
        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        #[allow(invalid_reference_casting)]
        let plan_mut: &mut C::PlanType =
            unsafe { &mut *(self.plan as *const C::PlanType as *mut C::PlanType) };
        plan_mut.prepare(worker.tls);

        if !plan_mut.no_mutator_prepare_release() && plan_mut.constraints().needs_prepare_mutator {
            for mutator in <C::VM as VMBinding>::VMActivePlan::mutators() {
                mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                    .add(PrepareMutator::<C::VM>::new(mutator));
            }
        }
        if !plan_mut.no_worker_prepare() {
            // for w in &mmtk.scheduler.worker_group.workers_shared {
            //     let result = w
            //         .designated_work
            //         .push(Box::new(PrepareCollector::<C::VM>::default()));
            //     debug_assert!(result.is_ok());
            // }
            unimplemented!()
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
pub struct PrepareCollector;

impl<VM: VMBinding> GCWork<VM> for PrepareCollector {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        trace!("Prepare Collector");
        worker.get_copy_context_mut().prepare();
        mmtk.get_plan().prepare_worker(worker);
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
    pub plan: *const C::PlanType,
}

impl<C: GCWorkContext> Release<C> {
    pub fn new(plan: *const C::PlanType) -> Self {
        Self { plan }
    }
}

unsafe impl<C: GCWorkContext> Send for Release<C> {}

impl<C: GCWorkContext + 'static> GCWork<C::VM> for Release<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("Release Global");

        if mmtk.get_plan().downcast_ref::<LXR<C::VM>>().is_none() {
            <C::VM as VMBinding>::VMCollection::update_weak_processor(false);
        }
        mmtk.gc_trigger.policy.on_gc_release(mmtk);
        // We assume this is the only running work packet that accesses plan at the point of execution
        #[allow(clippy::cast_ref_to_mut)]
        #[allow(invalid_reference_casting)]
        let plan_mut: &mut C::PlanType =
            unsafe { &mut *(self.plan as *const C::PlanType as *mut C::PlanType) };
        plan_mut.release(worker.tls);

        if !plan_mut.no_mutator_prepare_release() {
            for mutator in <C::VM as VMBinding>::VMActivePlan::mutators() {
                mmtk.scheduler.work_buckets[WorkBucketStage::Release]
                    .add(ReleaseMutator::<C::VM>::new(mutator));
            }
        }
        if !plan_mut.fast_worker_release() {
            // for w in &mmtk.scheduler.worker_group.workers_shared {
            //     let result = w
            //         .designated_work
            //         .push(Box::new(ReleaseCollector::<C::VM>::default()));
            //     debug_assert!(result.is_ok());
            // }
            unimplemented!()
        } else {
            crate::scheduler::worker::reset_workers::<C::VM>();
        }

        #[cfg(feature = "count_live_bytes_in_gc")]
        {
            let live_bytes = mmtk
                .scheduler
                .worker_group
                .get_and_clear_worker_live_bytes();
            mmtk.state.set_live_bytes_in_last_gc(live_bytes);
        }
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
pub struct ReleaseCollector;

impl<VM: VMBinding> GCWork<VM> for ReleaseCollector {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        trace!("Release Collector");
        worker.get_copy_context_mut().release();
    }
}

/// Stop all mutators
///
/// TODO: Smaller work granularity
#[derive(Default)]
pub struct StopMutators<C: GCWorkContext>(PhantomData<C>);

impl<C: GCWorkContext> StopMutators<C> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<C: GCWorkContext> GCWork<C::VM> for StopMutators<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("stop_all_mutators start");
        mmtk.state.prepare_for_stack_scanning();
        const BULK_THREAD_SCAN: bool = true;
        let mut mutators: Vec<VMMutatorThread> = vec![];
        let mut n = 0;
        <C::VM as VMBinding>::VMCollection::stop_all_mutators(
            worker.tls,
            |mutator| {
                mutator.flush();
                mutator.prepare(worker.tls);
                if BULK_THREAD_SCAN {
                    mutators.push(mutator.get_tls());
                } else {
                    // TODO: The stack scanning work won't start immediately, as the `Prepare` bucket is not opened yet (the bucket is opened in notify_mutators_paused).
                    // Should we push to Unconstrained instead?
                    // mmtk.scheduler.work_buckets[WorkBucketStage::RCProcessIncs]
                    //     .add(ScanMutatorRoots::<C>(mutator));
                    unimplemented!()
                }
                n += 1;
            },
            mmtk.get_plan()
                .current_gc_should_prepare_for_class_unloading(),
        );
        mmtk.scheduler.set_in_gc_pause(true);
        gc_log!([3] "Discovered {} mutators", n);
        if BULK_THREAD_SCAN {
            let n_workers: usize = mmtk.scheduler.num_workers();
            for ms in mutators.chunks(std::cmp::max(mutators.len() / n_workers / 2, 1)) {
                mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                    .add(ScanMultipleStacks::<C>(ms.to_vec(), PhantomData));
            }
        }
        trace!("stop_all_mutators end");
        if crate::verbose(2) {
            crate::RESERVED_PAGES_AT_GC_START
                .store(mmtk.get_plan().get_reserved_pages(), Ordering::SeqCst);
        }
        gc_log!([3] " - ({:.3}ms) Mutators stopped", crate::gc_start_time_ms());
        #[cfg(feature = "sanity")]
        mmtk.sanity_checker.lock().unwrap().clear_roots_cache();
        mmtk.get_plan().gc_pause_start(&mmtk.scheduler);
        let factory = ProcessEdgesWorkRootsWorkFactory::<
            C::VM,
            C::DefaultProcessEdges,
            C::PinningProcessEdges,
        >::new(mmtk);
        <C::VM as VMBinding>::VMScanning::scan_vm_specific_roots(worker.tls, factory);
        mmtk.scheduler.notify_mutators_paused(mmtk);
    }
}

/// This implements `ObjectTracer` by forwarding the `trace_object` calls to the wrapped
/// `ProcessEdgesWork` instance.
pub(crate) struct ProcessEdgesWorkTracer<E: ProcessEdgesWork> {
    process_edges_work: E,
    stage: WorkBucketStage,
}

impl<E: ProcessEdgesWork> ObjectTracer for ProcessEdgesWorkTracer<E> {
    /// Forward the `trace_object` call to the underlying `ProcessEdgesWork`,
    /// and flush as soon as the underlying buffer of `process_edges_work` is full.
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        let result = self.process_edges_work.trace_object(object);
        self.flush_if_full();
        result
    }
}

impl<E: ProcessEdgesWork> ProcessEdgesWorkTracer<E> {
    fn flush_if_full(&mut self) {
        if self.process_edges_work.nodes.is_full() {
            self.flush();
        }
    }

    pub fn flush_if_not_empty(&mut self) {
        if !self.process_edges_work.nodes.is_empty() {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let next_nodes = self.process_edges_work.pop_nodes();
        assert!(!next_nodes.is_empty());
        let work_packet = self.process_edges_work.create_scan_work(next_nodes);
        let worker = self.process_edges_work.worker();
        worker.scheduler().work_buckets[self.stage].add(work_packet);
    }
}

/// This type implements `ObjectTracerContext` by creating a temporary `ProcessEdgesWork` during
/// the call to `with_tracer`, making use of its `trace_object` method.  It then creates work
/// packets using the methods of the `ProcessEdgesWork` and add the work packet into the given
/// `stage`.
pub(crate) struct ProcessEdgesWorkTracerContext<E: ProcessEdgesWork> {
    stage: WorkBucketStage,
    phantom_data: PhantomData<E>,
}

impl<E: ProcessEdgesWork> Clone for ProcessEdgesWorkTracerContext<E> {
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

impl<E: ProcessEdgesWork> ObjectTracerContext<E::VM> for ProcessEdgesWorkTracerContext<E> {
    type TracerType = ProcessEdgesWorkTracer<E>;

    fn with_tracer<R, F>(&self, worker: &mut GCWorker<E::VM>, func: F) -> R
    where
        F: FnOnce(&mut Self::TracerType) -> R,
    {
        let mmtk = worker.mmtk;

        // Prepare the underlying ProcessEdgesWork
        let mut process_edges_work = E::new(vec![], false, mmtk, self.stage);
        // FIXME: This line allows us to omit the borrowing lifetime of worker.
        // We should refactor ProcessEdgesWork so that it uses `worker` locally, not as a member.
        process_edges_work.set_worker(worker);

        // Cretae the tracer.
        let mut tracer = ProcessEdgesWorkTracer {
            process_edges_work,
            stage: self.stage,
        };

        // The caller can use the tracer here.
        let result = func(&mut tracer);

        // Flush the queued nodes.
        tracer.flush_if_not_empty();

        result
    }
}

/// Delegate to the VM binding for weak reference processing.
///
/// Some VMs (e.g. v8) do not have a Java-like global weak reference storage, and the
/// processing of those weakrefs may be more complex. For such case, we delegate to the
/// VM binding to process weak references.
///
/// NOTE: This will replace `{Soft,Weak,Phantom}RefProcessing` and `Finalization` in the future.
pub struct VMProcessWeakRefs<E: ProcessEdgesWork> {
    phantom_data: PhantomData<E>,
}

impl<E: ProcessEdgesWork> VMProcessWeakRefs<E> {
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for VMProcessWeakRefs<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, _mmtk: &'static MMTK<E::VM>) {
        trace!("VMProcessWeakRefs");
        <<E::VM as VMBinding>::VMCollection as Collection<E::VM>>::process_weak_refs::<E>(worker);
        // TODO: Pass a factory/callback to decide what work packet to create.
    }
}

/// Delegate to the VM binding for forwarding weak references.
///
/// Some VMs (e.g. v8) do not have a Java-like global weak reference storage, and the
/// processing of those weakrefs may be more complex. For such case, we delegate to the
/// VM binding to process weak references.
///
/// NOTE: This will replace `RefForwarding` and `ForwardFinalization` in the future.
pub struct VMForwardWeakRefs<E: ProcessEdgesWork> {
    phantom_data: PhantomData<E>,
}

impl<E: ProcessEdgesWork> VMForwardWeakRefs<E> {
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for VMForwardWeakRefs<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, _mmtk: &'static MMTK<E::VM>) {
        trace!("VMForwardWeakRefs");

        // let stage = WorkBucketStage::VMRefForwarding;

        unimplemented!()

        // let tracer_factory = ProcessEdgesWorkTracerContext::<E> {
        //     stage,
        //     phantom_data: PhantomData,
        // };
        // <E::VM as VMBinding>::VMScanning::forward_weak_refs(worker, tracer_factory)
    }
}

/// This work packet calls `Collection::post_forwarding`.
///
/// NOTE: This will replace `RefEnqueue` in the future.
///
/// NOTE: Although this work packet runs in parallel with the `Release` work packet, it does not
/// access the `Plan` instance.
#[derive(Default)]
pub struct VMPostForwarding<VM: VMBinding> {
    phantom_data: PhantomData<VM>,
}

impl<VM: VMBinding> GCWork<VM> for VMPostForwarding<VM> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        trace!("VMPostForwarding start");
        <VM as VMBinding>::VMCollection::post_forwarding(worker.tls);
        trace!("VMPostForwarding end");
    }
}

pub struct ScanMutatorRoots<C: GCWorkContext>(pub &'static mut Mutator<C::VM>);

impl<C: GCWorkContext> GCWork<C::VM> for ScanMutatorRoots<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("ScanMutatorRoots for mutator {:?}", self.0.get_tls());
        let mutators = <C::VM as VMBinding>::VMActivePlan::number_of_mutators();
        let factory = ProcessEdgesWorkRootsWorkFactory::<
            C::VM,
            C::DefaultProcessEdges,
            C::PinningProcessEdges,
        >::new(mmtk);
        <C::VM as VMBinding>::VMScanning::scan_roots_in_mutator_thread(
            worker.tls,
            unsafe { &mut *(self.0 as *mut Mutator<C::VM>) },
            factory,
        );
        self.0.prepare(worker.tls);
        self.0.flush();

        if mmtk.state.inform_stack_scanned(mutators) {
            <C::VM as VMBinding>::VMScanning::notify_initial_thread_scan_complete(
                false, worker.tls,
            );
            mmtk.set_gc_status(GcStatus::GcProper);
        }
    }
}

pub struct ScanMultipleStacks<C: GCWorkContext>(Vec<VMMutatorThread>, PhantomData<C>);

unsafe impl<C: GCWorkContext> Send for ScanMultipleStacks<C> {}

impl<C: GCWorkContext> GCWork<C::VM> for ScanMultipleStacks<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        let factory = ProcessEdgesWorkRootsWorkFactory::<
            C::VM,
            C::DefaultProcessEdges,
            C::PinningProcessEdges,
        >::new(mmtk);
        <C::VM as VMBinding>::VMScanning::scan_multiple_thread_root(
            worker.tls,
            std::mem::take(&mut self.0),
            factory,
        );
    }
}

#[derive(Default)]
pub struct ScanVMSpecificRoots<C: GCWorkContext>(PhantomData<C>);

impl<C: GCWorkContext> ScanVMSpecificRoots<C> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<C: GCWorkContext> GCWork<C::VM> for ScanVMSpecificRoots<C> {
    fn do_work(&mut self, worker: &mut GCWorker<C::VM>, mmtk: &'static MMTK<C::VM>) {
        trace!("ScanStaticRoots");
        let factory = ProcessEdgesWorkRootsWorkFactory::<
            C::VM,
            C::DefaultProcessEdges,
            C::PinningProcessEdges,
        >::new(mmtk);
        <C::VM as VMBinding>::VMScanning::scan_vm_specific_roots(worker.tls, factory);
    }
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum RootKind {
    Strong,
    /// Some OpenJDK roots are slow to scan. We only collect newly created or modified ones in the previous mutator phase.
    ///
    /// Possible roots:
    /// * Strong CLD roots
    /// * Weak CLD roots (weak)
    /// * CodeCache roots
    /// * WeakHandle roots (weak)
    ///
    /// FOR LXR:
    /// * Don't apply decrements to these roots
    /// * Mark them at the start of SATB
    /// * StrongCLD / CodeCache roots: for SATB correctness, collect the complete set of them at Initial/Final SATB pause, to make sure they are marked and forwarded properly.
    /// * Mature evac: Add these roots to remset, so that any roots that is modified during the concurrent phase will be scanned.
    StrongCLDRoots,
    YoungStrongCLDRoots,
    YoungWeakCLDRoots,
    YoungCodeCacheRoots,
    YoungWeakHandleRoots,
    Weak,
}

impl RootKind {
    pub fn should_record_remset(&self) -> bool {
        matches!(self, RootKind::YoungWeakCLDRoots)
            || matches!(self, RootKind::YoungWeakHandleRoots)
            || matches!(self, RootKind::YoungCodeCacheRoots)
    }

    pub fn should_skip_mark_and_decs(&self) -> bool {
        matches!(self, RootKind::YoungCodeCacheRoots)
            || matches!(self, RootKind::YoungWeakHandleRoots)
    }

    pub fn should_skip_decs(&self) -> bool {
        matches!(self, RootKind::YoungCodeCacheRoots)
            || matches!(self, RootKind::StrongCLDRoots)
            || matches!(self, RootKind::YoungStrongCLDRoots)
            || matches!(self, RootKind::YoungWeakCLDRoots)
            || matches!(self, RootKind::YoungWeakHandleRoots)
            || matches!(self, RootKind::Weak)
    }
}

pub struct ProcessEdgesBase<VM: VMBinding> {
    pub slots: Vec<VM::VMSlot>,
    pub nodes: VectorObjectQueue,
    mmtk: &'static MMTK<VM>,
    // Use raw pointer for fast pointer dereferencing, instead of using `Option<&'static mut GCWorker<E::VM>>`.
    // Because a copying gc will dereference this pointer at least once for every object copy.
    worker: *mut GCWorker<VM>,
    pub roots: bool,
    pub root_kind: Option<RootKind>,
    pub bucket: WorkBucketStage,
}

unsafe impl<VM: VMBinding> Send for ProcessEdgesBase<VM> {}

impl<VM: VMBinding> ProcessEdgesBase<VM> {
    // Requires an MMTk reference. Each plan-specific type that uses ProcessEdgesBase can get a static plan reference
    // at creation. This avoids overhead for dynamic dispatch or downcasting plan for each object traced.
    pub fn new(
        slots: Vec<VM::VMSlot>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        #[cfg(feature = "extreme_assertions")]
        if crate::util::slot_logger::should_check_duplicate_slots(mmtk.get_plan()) {
            for slot in &slots {
                // log slot, panic if already logged
                mmtk.slot_logger.log_slot(*slot);
            }
        }
        Self {
            slots,
            nodes: VectorObjectQueue::new(),
            mmtk,
            worker: std::ptr::null_mut(),
            roots,
            root_kind: if roots { Some(RootKind::Strong) } else { None },
            bucket,
        }
    }
    pub fn set_worker(&mut self, worker: &mut GCWorker<VM>) {
        self.worker = worker;
    }

    pub fn worker(&self) -> &'static mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn mmtk(&self) -> &'static MMTK<VM> {
        self.mmtk
    }

    pub fn plan(&self) -> &'static dyn Plan<VM = VM> {
        self.mmtk.get_plan()
    }

    /// Pop all nodes from nodes, and clear nodes to an empty vector.
    pub fn pop_nodes(&mut self) -> Vec<ObjectReference> {
        self.nodes.take()
    }

    pub fn is_roots(&self) -> bool {
        self.roots
    }
}

/// A short-hand for `<E::VM as VMBinding>::VMSlot`.
pub type SlotOf<E> = <<E as ProcessEdgesWork>::VM as VMBinding>::VMSlot;

/// An abstract trait for work packets that process object graph edges.  Its method
/// [`ProcessEdgesWork::trace_object`] traces an object and, upon first visit, enqueues it into an
/// internal queue inside the `ProcessEdgesWork` instance.  Each implementation of this trait
/// implement `trace_object` differently.  During [`Plan::schedule_collection`], plans select
/// (usually via `GCWorkContext`) specialized implementations of this trait to be used during each
/// trace according the nature of each trace, such as whether it is a nursery collection, whether it
/// is a defrag collection, whether it pins objects, etc.
///
/// This trait was originally designed for work packets that process object graph edges represented
/// as slots.  The constructor [`ProcessEdgesWork::new`] takes a vector of slots, and the created
/// work packet will trace the objects pointed by the object reference in each slot using the
/// `trace_object` method, and update the slot if the GC moves the target object when tracing.
///
/// This trait can also be used merely as a provider of the `trace_object` method by giving it an
/// empty vector of slots.  This is useful for node-enqueuing tracing
/// ([`Scanning::scan_object_and_trace_edges`]) as well as weak reference processing
/// ([`Scanning::process_weak_refs`] as well as `ReferenceProcessor` and `FinalizableProcessor`).
/// In those cases, the caller passes the reference to the target object to `trace_object`, an the
/// caller is responsible for updating the slots according the return value of `trace_object`.
///
/// TODO: We should refactor this trait to decouple it from slots. See:
/// <https://github.com/mmtk/mmtk-core/issues/599>
pub trait ProcessEdgesWork:
    Send + 'static + Sized + DerefMut + Deref<Target = ProcessEdgesBase<Self::VM>>
{
    /// The associate type for the VM.
    type VM: VMBinding;

    /// The work packet type for scanning objects when using this ProcessEdgesWork.
    type ScanObjectsWorkType: ScanObjectsWork<Self::VM>;

    /// The maximum number of slots that should be put to one of this work packets.
    /// The caller who creates a work packet of this trait should be responsible to
    /// comply with this capacity.
    /// Higher capacity means the packet will take longer to finish, and may lead to
    /// bad load balancing. On the other hand, lower capacity would lead to higher cost
    /// on scheduling many small work packets. It is important to find a proper capacity.
    const CAPACITY: usize = crate::args::BUFFER_SIZE;
    /// Do we update object reference? This has to be true for a moving GC.
    const OVERWRITE_REFERENCE: bool = true;
    /// If true, we do object scanning in this work packet with the same worker without scheduling overhead.
    /// If false, we will add object scanning work packets to the global queue and allow other workers to work on it.
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;

    const RC_ROOTS: bool = false;

    /// Create a [`ProcessEdgesWork`].
    ///
    /// Arguments:
    /// * `slots`: a vector of slots.
    /// * `roots`: are the objects root reachable objects?
    /// * `mmtk`: a reference to the MMTK instance.
    /// * `bucket`: which work bucket this packet belongs to. Further work generated from this packet will also be put to the same bucket.
    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<Self::VM>,
        bucket: WorkBucketStage,
    ) -> Self;

    /// Trace an MMTk object. The implementation should forward this call to the policy-specific
    /// `trace_object()` methods, depending on which space this object is in.
    /// If the object is not in any MMTk space, the implementation should forward the call to
    /// `ActivePlan::vm_trace_object()` to let the binding handle the tracing.
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference;

    /// If the work includes roots, we will store the roots somewhere so for sanity GC, we can do another
    /// transitive closure from the roots.
    #[cfg(feature = "sanity")]
    fn cache_roots_for_sanity_gc(&mut self, _roots: Vec<EdgeOf<Self>>) {
        assert!(self.roots);
        self.mmtk()
            .sanity_checker
            .lock()
            .unwrap()
            .add_root_slots(self.slots.clone(), self.root_kind.unwrap());
    }

    /// Start the a scan work packet. If SCAN_OBJECTS_IMMEDIATELY, the work packet will be executed immediately, in this method.
    /// Otherwise, the work packet will be added the Closure work bucket and will be dispatched later by the scheduler.
    fn start_or_dispatch_scan_work(&mut self, mut work_packet: impl GCWork<Self::VM>) {
        if Self::SCAN_OBJECTS_IMMEDIATELY {
            // We execute this `scan_objects_work` immediately.
            // This is expected to be a useful optimization because,
            // say for _pmd_ with 200M heap, we're likely to have 50000~60000 `ScanObjects` work packets
            // being dispatched (similar amount to `ProcessEdgesWork`).
            // Executing these work packets now can remarkably reduce the global synchronization time.
            work_packet.do_work(self.worker(), self.mmtk);
        } else {
            debug_assert!(self.bucket != WorkBucketStage::Unconstrained);
            self.mmtk.scheduler.work_buckets[self.bucket].add(work_packet);
        }
    }

    /// Create an object-scanning work packet to be used for this ProcessEdgesWork.
    ///
    /// `roots` indicates if we are creating a packet for root scanning.  It is only true when this
    /// method is called to handle `RootsWorkFactory::create_process_pinning_roots_work`.
    fn create_scan_work(&self, nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType;

    /// Flush the nodes in ProcessEdgesBase, and create a ScanObjects work packet for it. If the node set is empty,
    /// this method will simply return with no work packet created.
    fn flush(&mut self) {
        let nodes = self.pop_nodes();
        if !nodes.is_empty() {
            self.start_or_dispatch_scan_work(self.create_scan_work(nodes));
        }
    }

    /// Process a slot, including loading the object reference from the memory slot,
    /// trace the object and store back the new object reference if necessary.
    fn process_slot(&mut self, slot: SlotOf<Self>) {
        let Some(object) = slot.load() else {
            // Skip slots that are not holding an object reference.
            return;
        };
        let new_object = self.trace_object(object);
        if Self::OVERWRITE_REFERENCE && new_object != object {
            slot.store(Some(new_object));
        }
    }

    /// Process all the slots in the work packet.
    fn process_slots(&mut self) {
        #[cfg(feature = "tracing")]
        probe!(mmtk, process_slots, self.slots.len(), self.is_roots());
        for i in 0..self.slots.len() {
            self.process_slot(self.slots[i])
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for E {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, _mmtk: &'static MMTK<E::VM>) {
        self.set_worker(worker);
        #[cfg(feature = "sanity")]
        let roots = if self.roots {
            Some(self.slots.clone())
        } else {
            None
        };
        self.process_slots();
        if !self.nodes.is_empty() {
            self.flush();
        }
        #[cfg(feature = "sanity")]
        if self.roots && !_mmtk.is_in_sanity() {
            self.cache_roots_for_sanity_gc(roots.unwrap());
        }
        trace!("ProcessEdgesWork End");
    }
}

/// A general implementation of [`ProcessEdgesWork`] using SFT. A plan can always implement their
/// own [`ProcessEdgesWork`] instances. However, most plans can use this work packet for tracing amd
/// they do not need to provide a plan-specific trace object work packet. If they choose to use this
/// type, they need to provide a correct implementation for some related methods (such as
/// `Space.set_copy_for_sft_trace()`, `SFT.sft_trace_object()`). Some plans are not using this type,
/// mostly due to more complex tracing. Either it is impossible to use this type, or there is
/// performance overheads for using this general trace type. In such cases, they implement their
/// specific [`ProcessEdgesWork`] instances.
// TODO: This is not used any more. Should we remove it?
pub struct SFTProcessEdges<VM: VMBinding> {
    pub base: ProcessEdgesBase<VM>,
}

impl<VM: VMBinding> ProcessEdgesWork for SFTProcessEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        Self { base }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        use crate::policy::sft::GCWorkerMutRef;

        // Erase <VM> type parameter
        let worker = GCWorkerMutRef::new(self.worker());

        // Invoke trace object on sft
        let sft = unsafe { crate::mmtk::SFT_MAP.get_unchecked(object.to_address::<VM>()) };
        sft.sft_trace_object(&mut self.base.nodes, object, worker)
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> ScanObjects<Self> {
        unimplemented!()
    }
}

/// An implementation of `RootsWorkFactory` that creates work packets based on `ProcessEdgesWork`
/// for handling roots.  The `DPE` and the `PPE` type parameters correspond to the
/// `DefaultProcessEdge` and the `PinningProcessEdges` type members of the [`GCWorkContext`] trait.
pub(crate) struct ProcessEdgesWorkRootsWorkFactory<
    VM: VMBinding,
    DPE: ProcessEdgesWork<VM = VM>,
    PPE: ProcessEdgesWork<VM = VM>,
> {
    mmtk: &'static MMTK<VM>,
    phantom: PhantomData<(DPE, PPE)>,
}

impl<VM: VMBinding, DPE: ProcessEdgesWork<VM = VM>, PPE: ProcessEdgesWork<VM = VM>> Clone
    for ProcessEdgesWorkRootsWorkFactory<VM, DPE, PPE>
{
    fn clone(&self) -> Self {
        Self {
            mmtk: self.mmtk,
            phantom: PhantomData,
        }
    }
}

impl<VM: VMBinding, DPE: ProcessEdgesWork<VM = VM>, PPE: ProcessEdgesWork<VM = VM>>
    RootsWorkFactory<VM::VMSlot> for ProcessEdgesWorkRootsWorkFactory<VM, DPE, PPE>
{
    fn create_process_roots_work(&mut self, slots: Vec<VM::VMSlot>, kind: RootKind) {
        let mut w = DPE::new(
            slots,
            true,
            self.mmtk,
            if DPE::RC_ROOTS {
                // WorkBucketStage::RCProcessIncs
                unimplemented!()
            } else {
                WorkBucketStage::Closure
            },
        );
        w.root_kind = Some(kind);
        crate::memory_manager::add_work_packet(
            self.mmtk,
            if DPE::RC_ROOTS {
                // WorkBucketStage::RCProcessIncs
                unimplemented!()
            } else {
                WorkBucketStage::Closure
            },
            w,
        );
    }

    fn create_process_pinning_roots_work(&mut self, nodes: Vec<ObjectReference>) {
        // Will process roots within the PinningRootsTrace bucket
        // And put work in the Closure bucket
        // crate::memory_manager::add_work_packet(
        //     self.mmtk,
        //     WorkBucketStage::PinningRootsTrace,
        //     ProcessRootNode::<VM, PPE, DPE>::new(nodes, WorkBucketStage::Closure),
        // );
        unimplemented!()
    }

    fn create_process_tpinning_roots_work(&mut self, nodes: Vec<ObjectReference>) {
        // crate::memory_manager::add_work_packet(
        //     self.mmtk,
        //     WorkBucketStage::TPinningClosure,
        //     ProcessRootNode::<VM, PPE, PPE>::new(nodes, WorkBucketStage::TPinningClosure),
        // );
        unimplemented!()
    }
}

impl<VM: VMBinding, DPE: ProcessEdgesWork<VM = VM>, PPE: ProcessEdgesWork<VM = VM>>
    ProcessEdgesWorkRootsWorkFactory<VM, DPE, PPE>
{
    fn new(mmtk: &'static MMTK<VM>) -> Self {
        Self {
            mmtk,
            phantom: PhantomData,
        }
    }
}

impl<VM: VMBinding> Deref for SFTProcessEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for SFTProcessEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

/// Trait for a work packet that scans objects
pub trait ScanObjectsWork<VM: VMBinding>: GCWork<VM> + Sized {
    /// The associated ProcessEdgesWork for processing the outgoing edges of the objects in this
    /// packet.
    type E: ProcessEdgesWork<VM = VM>;

    /// Called after each object is scanned.
    fn post_scan_object(&self, object: ObjectReference);

    /// Return the work bucket for this work packet and its derived work packets.
    fn get_bucket(&self) -> WorkBucketStage;

    /// The common code for ScanObjects and PlanScanObjects.
    fn do_work_common(
        &self,
        buffer: &[ObjectReference],
        worker: &mut GCWorker<<Self::E as ProcessEdgesWork>::VM>,
        _mmtk: &'static MMTK<<Self::E as ProcessEdgesWork>::VM>,
        should_discover_reference: bool,
        should_claim_and_scan_clds: bool,
    ) {
        let tls = worker.tls;

        let objects_to_scan = buffer;

        // Scan the objects in the list that supports slot-enququing.
        let mut scan_later = vec![];
        {
            let mut closure = ObjectsClosure::<Self::E>::new(
                worker,
                should_discover_reference,
                should_claim_and_scan_clds,
                self.get_bucket(),
            );
            for object in objects_to_scan.iter().copied() {
                // For any object we need to scan, we count its liv bytes
                #[cfg(feature = "count_live_bytes_in_gc")]
                closure
                    .worker
                    .shared
                    .increase_live_bytes(VM::VMObjectModel::get_current_size(object));

                if <VM as VMBinding>::VMScanning::support_slot_enqueuing(tls, object) {
                    trace!("Scan object (slot) {}", object);
                    // If an object supports slot-enqueuing, we enqueue its slots.
                    <VM as VMBinding>::VMScanning::scan_object(tls, object, &mut closure);
                    self.post_scan_object(object);
                } else {
                    // If an object does not support slot-enqueuing, we have to use
                    // `Scanning::scan_object_and_trace_edges` and offload the job of updating the
                    // reference field to the VM.
                    //
                    // However, at this point, `closure` is borrowing `worker`.
                    // So we postpone the processing of objects that needs object enqueuing
                    scan_later.push(object);
                }
            }
        }

        // If any object does not support slot-enqueuing, we process them now.
        if !scan_later.is_empty() {
            let object_tracer_context = ProcessEdgesWorkTracerContext::<Self::E> {
                stage: self.get_bucket(),
                phantom_data: PhantomData,
            };

            object_tracer_context.with_tracer(worker, |object_tracer| {
                // Scan objects and trace their outgoing edges at the same time.
                for object in scan_later.iter().copied() {
                    trace!("Scan object (node) {}", object);
                    <VM as VMBinding>::VMScanning::scan_object_and_trace_edges(
                        tls,
                        object,
                        object_tracer,
                    );
                    self.post_scan_object(object);
                }
            });
        }
    }
}

/// Scan objects and enqueue the slots of the objects.  For objects that do not support
/// slot-enqueuing, this work packet also traces their outgoing edges directly.
///
/// This work packet does not execute policy-specific post-scanning hooks
/// (it won't call `post_scan_object()` in [`policy::gc_work::PolicyTraceObject`]).
/// It should be used only for policies that do not perform policy-specific actions when scanning
/// an object.
pub struct ScanObjects<Edges: ProcessEdgesWork> {
    buffer: Vec<ObjectReference>,
    #[allow(unused)]
    concurrent: bool,
    phantom: PhantomData<Edges>,
    pub discovery: bool,
    claim_and_scan_clds: bool,
    bucket: WorkBucketStage,
}

impl<Edges: ProcessEdgesWork> ScanObjects<Edges> {
    pub fn new(
        buffer: Vec<ObjectReference>,
        concurrent: bool,
        discovery: bool,
        claim_and_scan_clds: bool,
        bucket: WorkBucketStage,
    ) -> Self {
        Self {
            buffer,
            concurrent,
            phantom: PhantomData,
            discovery,
            claim_and_scan_clds,
            bucket,
        }
    }
}

impl<VM: VMBinding, E: ProcessEdgesWork<VM = VM>> ScanObjectsWork<VM> for ScanObjects<E> {
    type E = E;

    fn get_bucket(&self) -> WorkBucketStage {
        self.bucket
    }

    fn post_scan_object(&self, _object: ObjectReference) {
        // Do nothing.
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ScanObjects<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("ScanObjects");
        self.do_work_common(
            &self.buffer,
            worker,
            mmtk,
            self.discovery,
            self.claim_and_scan_clds,
        );
        trace!("ScanObjects End");
    }
}

pub struct UnlogSlots<VM: VMBinding>(pub Vec<VM::VMSlot>);

impl<VM: VMBinding> UnlogSlots<VM> {
    fn unlog_slots(&self, meta: &SideMetadataSpec) {
        if !self.0.is_empty() {
            for slot in &self.0 {
                let ptr = address_to_meta_address(meta, slot.to_address());
                unsafe {
                    ptr.store(0b11111111u8);
                }
            }
        }
    }
}
impl<VM: VMBinding> GCWork<VM> for UnlogSlots<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        self.unlog_slots(
            VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
                .as_spec()
                .extract_side_spec(),
        );
    }
}

pub struct DummyPacket<T: 'static + Send>(pub T);

impl<T: 'static + Send, VM: VMBinding> GCWork<VM> for DummyPacket<T> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {}
}

use crate::mmtk::MMTK;
use crate::plan::Plan;
use crate::plan::PlanTraceObject;
use crate::policy::gc_work::TraceKind;

/// This provides an implementation of [`crate::scheduler::gc_work::ProcessEdgesWork`]. A plan that implements
/// `PlanTraceObject` can use this work packet for tracing objects.
pub struct PlanProcessEdges<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
    const KIND: TraceKind,
> {
    plan: &'static P,
    base: ProcessEdgesBase<VM>,
    next_slots: Vec<SlotOf<Self>>,
    pushes: usize,
}

impl<VM: VMBinding, P: PlanTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> ProcessEdgesWork
    for PlanProcessEdges<VM, P, KIND>
{
    type VM = VM;
    type ScanObjectsWorkType = PlanScanObjects<Self, P>;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        let plan = base.plan().downcast_ref::<P>().unwrap();
        Self {
            plan,
            base,
            next_slots: vec![],
            pushes: 0,
        }
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType {
        unreachable!()
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        // We cannot borrow `self` twice in a call, so we extract `worker` as a local variable.
        let worker = self.worker();
        self.plan.trace_object::<_, KIND>(self, object, worker)
    }

    fn process_slot(&mut self, slot: SlotOf<Self>) {
        let Some(object) = slot.load() else {
            // Skip slots that are not holding an object reference.
            return;
        };
        let new_object = self.trace_object(object);
        if P::may_move_objects::<KIND>() && new_object != object {
            slot.store(Some(new_object));
        }
    }

    fn process_slots(&mut self) {
        if !cfg!(feature = "no_stack") {
            if cfg!(feature = "steal") {
                let worker = self.worker();
                'outer: loop {
                    // Drain local stack
                    while let Some(slot) = self.slots.pop().or_else(|| worker.deque.pop()) {
                        self.process_slot(slot);
                    }
                    let workers = &self.worker().scheduler().worker_group.workers_shared;
                    // Steal from other workers
                    if !worker.scheduler().work_buckets[WorkBucketStage::Closure].is_empty()
                        || (worker.scheduler().work_buckets[WorkBucketStage::WeakRefClosure]
                            .is_activated()
                            && !worker.scheduler().work_buckets[WorkBucketStage::WeakRefClosure]
                                .is_empty())
                        || !worker.scheduler().work_buckets[WorkBucketStage::Unconstrained]
                            .is_empty()
                        || !worker.local_work_buffer.is_empty()
                        || workers
                            .iter()
                            .any(|w| !w.stealer.as_ref().unwrap().is_empty())
                    {
                        break;
                    }
                    let n = workers.len();
                    for _i in 0..n / 2 {
                        if let Stolen::Data(slot) =
                            Self::steal_best_of_2(worker.ordinal, &mut worker.hash_seed, workers)
                        {
                            self.process_slot(slot);
                            continue 'outer;
                        }
                    }
                    break;
                }
            } else {
                while let Some(slot) = self.slots.pop() {
                    self.process_slot(slot);
                }
            }
        } else {
            for i in 0..self.slots.len() {
                self.process_slot(self.slots[i]);
            }
            let mut slots = vec![];
            while !self.next_slots.is_empty() {
                slots.clear();
                std::mem::swap(&mut slots, &mut self.next_slots);
                for s in &slots {
                    self.process_slot(*s);
                }
            }
        }
    }

    fn flush(&mut self) {
        if !cfg!(feature = "no_stack") {
            if !self.slots.is_empty() {
                let slots = std::mem::take(&mut self.slots);
                let w = Self::new(slots, false, self.mmtk, self.bucket);
                self.worker().add_work(self.bucket, w);
            }
        } else {
            if !self.next_slots.is_empty() {
                let slots = std::mem::take(&mut self.next_slots);
                let w = Self::new(slots, false, self.mmtk, self.bucket);
                self.worker().add_work(self.bucket, w);
            }
        }
        self.pushes = 0;
    }
}

impl<VM: VMBinding, P: PlanTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> ObjectQueue
    for PlanProcessEdges<VM, P, KIND>
{
    fn enqueue(&mut self, object: ObjectReference) {
        object.iterate_fields::<VM, _>(CLDScanPolicy::Claim, RefScanPolicy::Discover, |s, _| {
            let Some(_) = s.load() else { return };
            if !cfg!(feature = "no_stack") && cfg!(feature = "steal") {
                let worker = self.worker();
                if worker.deque.is_full() || !worker.deque.push(s) {
                    self.slots.push(s);
                    self.pushes += 1;
                    if self.slots.len() >= crate::args::BUFFER_SIZE
                        || (cfg!(feature = "push") && self.pushes >= 512)
                    {
                        self.flush_half();
                    }
                }
            } else if !cfg!(feature = "no_stack") {
                self.slots.push(s);
                self.pushes += 1;
                if self.slots.len() >= crate::args::BUFFER_SIZE
                    || (cfg!(feature = "push") && self.pushes >= 512)
                {
                    self.flush_half();
                }
            } else {
                self.next_slots.push(s);
                if self.next_slots.len() >= crate::args::BUFFER_SIZE {
                    self.flush_half();
                }
            }
        });
        self.plan.post_scan_object(object);
    }
}
impl<VM: VMBinding, P: PlanTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind>
    PlanProcessEdges<VM, P, KIND>
{
    fn random_park_and_miller(seed0: &mut usize) -> usize {
        let a = 16807;
        let m = 2147483647;
        let q = 127773;
        let r = 2836;
        let seed = *seed0;
        let hi = seed / q;
        let lo = seed % q;
        let test = a * lo - r * hi;
        *seed0 = if test > 0 { test } else { test + m };
        *seed0
    }
    fn steal_best_of_2(
        worker_id: usize,
        hash_seed: &mut usize,
        workers: &[Arc<GCWorkerShared<VM>>],
    ) -> Stolen<VM::VMSlot> {
        let n = workers.len();
        if n > 2 {
            let mut k1 = worker_id;
            while k1 == worker_id {
                k1 = Self::random_park_and_miller(hash_seed) % n;
            }
            let mut k2 = worker_id;
            while k2 == worker_id || k2 == k1 {
                k2 = Self::random_park_and_miller(hash_seed) % n;
            }
            let sz1 = workers[k1].deque_stealer.size();
            let sz2 = workers[k2].deque_stealer.size();
            if sz1 < sz2 {
                return workers[k2].deque_stealer.steal();
            } else {
                return workers[k1].deque_stealer.steal();
            }
        } else if n == 2 {
            let k = (worker_id + 1) % 2;
            return workers[k].deque_stealer.steal();
        } else {
            return Stolen::Empty;
        }
    }
    fn flush_half(&mut self) {
        let slots = if !cfg!(feature = "no_stack") {
            if cfg!(feature = "flush_half") {
                if self.slots.len() > 1 {
                    let half = self.slots.len() / 2;
                    self.slots.split_off(half)
                } else {
                    return;
                }
            } else {
                std::mem::take(&mut self.slots)
            }
        } else {
            if cfg!(feature = "flush_half") {
                if self.next_slots.len() > 1 {
                    let half = self.next_slots.len() / 2;
                    self.next_slots.split_off(half)
                } else {
                    return;
                }
            } else {
                std::mem::take(&mut self.next_slots)
            }
        };
        self.pushes = self.slots.len();
        if slots.is_empty() {
            return;
        }
        let w = Self::new(slots, false, self.mmtk, self.bucket);
        self.worker().add_work(self.bucket, w);
    }
}

// Impl Deref/DerefMut to ProcessEdgesBase for PlanProcessEdges
impl<VM: VMBinding, P: PlanTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> Deref
    for PlanProcessEdges<VM, P, KIND>
{
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, P: PlanTraceObject<VM> + Plan<VM = VM>, const KIND: TraceKind> DerefMut
    for PlanProcessEdges<VM, P, KIND>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

/// This is an alternative to `ScanObjects` that calls the `post_scan_object` of the policy
/// selected by the plan.  It is applicable to plans that derive `PlanTraceObject`.
pub struct PlanScanObjects<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanTraceObject<E::VM>> {
    plan: &'static P,
    buffer: Vec<ObjectReference>,
    #[allow(dead_code)]
    concurrent: bool,
    discover_refs: bool,
    claim_and_scan_clds: bool,
    bucket: WorkBucketStage,
    _p: PhantomData<E>,
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanTraceObject<E::VM>> PlanScanObjects<E, P> {
    pub fn new(
        plan: &'static P,
        buffer: Vec<ObjectReference>,
        concurrent: bool,
        discover_refs: bool,
        claim_and_scan_clds: bool,
        bucket: WorkBucketStage,
    ) -> Self {
        Self {
            plan,
            buffer,
            concurrent,
            discover_refs,
            claim_and_scan_clds,
            bucket,
            _p: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanTraceObject<E::VM>> ScanObjectsWork<E::VM>
    for PlanScanObjects<E, P>
{
    type E = E;

    fn get_bucket(&self) -> WorkBucketStage {
        self.bucket
    }

    fn post_scan_object(&self, object: ObjectReference) {
        self.plan.post_scan_object(object);
    }
}

impl<E: ProcessEdgesWork, P: Plan<VM = E::VM> + PlanTraceObject<E::VM>> GCWork<E::VM>
    for PlanScanObjects<E, P>
{
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        trace!("PlanScanObjects");
        self.do_work_common(
            &self.buffer,
            worker,
            mmtk,
            self.discover_refs,
            self.claim_and_scan_clds,
        );
        trace!("PlanScanObjects End");
    }
}

/// This work packet processes pinning roots.
///
/// The `roots` member holds a list of `ObjectReference` to objects directly pointed by roots.
/// These objects will be traced using `R2OPE` (Root-to-Object Process Edges).
///
/// After that, it will create work packets for tracing their children.  Those work packets (and
/// the work packets further created by them) will use `O2OPE` (Object-to-Object Process Edges) as
/// their `ProcessEdgesWork` implementations.
///
/// Because `roots` are pinning roots, `R2OPE` must be a `ProcessEdgesWork` that never moves any
/// object.
///
/// The choice of `O2OPE` determines whether the `roots` are transitively pinning or not.
///
/// -   If `O2OPE` is set to a `ProcessEdgesWork` that never moves objects, all descendents of
///     `roots` will not be moved in this GC.  That implements transitive pinning roots.
/// -   If `O2OPE` may move objects, then this `ProcessRootsNode<VM, R2OPE, O2OPE>` work packet
///     will only pin the objects in `roots` (because `R2OPE` must not move objects anyway), but
///     not their descendents.
pub(crate) struct ProcessRootNode<
    VM: VMBinding,
    R2OPE: ProcessEdgesWork<VM = VM>,
    O2OPE: ProcessEdgesWork<VM = VM>,
> {
    phantom: PhantomData<(VM, R2OPE, O2OPE)>,
    roots: Vec<ObjectReference>,
    bucket: WorkBucketStage,
}

impl<VM: VMBinding, R2OPE: ProcessEdgesWork<VM = VM>, O2OPE: ProcessEdgesWork<VM = VM>>
    ProcessRootNode<VM, R2OPE, O2OPE>
{
    pub fn new(nodes: Vec<ObjectReference>, bucket: WorkBucketStage) -> Self {
        Self {
            phantom: PhantomData,
            roots: nodes,
            bucket,
        }
    }
}

impl<VM: VMBinding, R2OPE: ProcessEdgesWork<VM = VM>, O2OPE: ProcessEdgesWork<VM = VM>> GCWork<VM>
    for ProcessRootNode<VM, R2OPE, O2OPE>
{
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        trace!("ProcessRootNode");

        #[cfg(feature = "sanity")]
        {
            if !mmtk.is_in_sanity() {
                mmtk.sanity_checker
                    .lock()
                    .unwrap()
                    .add_root_nodes(self.roots.clone());
            }
        }

        // This step conceptually traces the edges from root slots to the objects they point to.
        // However, VMs that deliver root objects instead of root slots are incapable of updating
        // root slots.  Therefore, we call `trace_object` on those objects, and assert the GC
        // doesn't move those objects because we cannot store the updated references back to the
        // slots.
        //
        // The `scanned_root_objects` variable will hold those root objects which are traced for the
        // first time.  We will create a work packet for scanning those roots.
        // let scanned_root_objects = {
        //     // We create an instance of E to use its `trace_object` method and its object queue.
        //     let mut process_edges_work =
        //         R2OPE::new(vec![], true, mmtk, WorkBucketStage::PinningRootsTrace);
        //     process_edges_work.set_worker(worker);

        //     for object in self.roots.iter().copied() {
        //         let new_object = process_edges_work.trace_object(object);
        //         debug_assert_eq!(
        //             object, new_object,
        //             "Object moved while tracing root unmovable root object: {} -> {}",
        //             object, new_object
        //         );
        //     }

        //     // This contains root objects that are visited the first time.
        //     // It is sufficient to only scan these objects.
        //     VectorQueue::take(&mut process_edges_work.nodes)
        // };

        // let process_edges_work = O2OPE::new(vec![], false, mmtk, self.bucket);
        // let work = process_edges_work.create_scan_work(scanned_root_objects);
        // crate::memory_manager::add_work_packet(mmtk, self.bucket, work);

        unimplemented!();

        trace!("ProcessRootNode End");
    }
}

/// A `ProcessEdgesWork` type that panics when any of its method is used.
/// This is currently used for plans that do not support transitively pinning.
#[derive(Default)]
pub struct UnsupportedProcessEdges<VM: VMBinding> {
    phantom: PhantomData<VM>,
}

impl<VM: VMBinding> Deref for UnsupportedProcessEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        panic!("unsupported!")
    }
}

impl<VM: VMBinding> DerefMut for UnsupportedProcessEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        panic!("unsupported!")
    }
}

impl<VM: VMBinding> ProcessEdgesWork for UnsupportedProcessEdges<VM> {
    type VM = VM;

    type ScanObjectsWorkType = ScanObjects<Self>;

    fn new(
        _slots: Vec<SlotOf<Self>>,
        _roots: bool,
        _mmtk: &'static MMTK<Self::VM>,
        _bucket: WorkBucketStage,
    ) -> Self {
        panic!("unsupported!")
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        panic!("unsupported!")
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType {
        panic!("unsupported!")
    }
}
