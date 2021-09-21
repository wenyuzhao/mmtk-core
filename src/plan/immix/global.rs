use super::gc_work::{ImmixCopyContext, ImmixProcessEdges, RCImmixProcessEdges, TraceKind};
use super::mutator::ALLOCATOR_MAPPING;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::GcStatus;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::space::Space;
use crate::scheduler::gc_work::*;
use crate::util::alloc::allocators::AllocatorSelector;
#[cfg(feature = "analysis")]
use crate::util::analysis::GcHookWork;
use crate::util::heap::layout::heap_layout::Mmapper;
use crate::util::heap::layout::heap_layout::VMMap;
use crate::util::heap::layout::vm_layout_constants::{HEAP_END, HEAP_START};
use crate::util::heap::HeapMeta;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSanity;
use crate::util::options::UnsafeOptionsWrapper;
#[cfg(feature = "sanity")]
use crate::util::sanity::sanity_checker::*;
use crate::util::{metadata, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use crate::{
    mmtk::MMTK,
    policy::immix::{block::Block, ImmixSpace},
    util::opaque_pointer::VMWorkerThread,
};
use crate::{scheduler::*, BarrierSelector};
use std::env;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic::Ordering;
use enum_map::EnumMap;

pub const ALLOC_IMMIX: AllocationSemantics = AllocationSemantics::Default;

pub struct Immix<VM: VMBinding> {
    pub immix_space: ImmixSpace<VM>,
    pub common: CommonPlan<VM>,
    /// Always true for non-rc immix.
    /// For RC immix, this is used for enable backup tracing.
    pub perform_cycle_collection: AtomicBool,
}

#[inline]
pub fn get_active_barrier() -> BarrierSelector {
    static mut B: Option<BarrierSelector> = None;
    unsafe {
        if B.is_none() {
            B = Some({
                if crate::plan::barriers::BARRIER_MEASUREMENT {
                    match env::var("IX_BARRIER") {
                        Ok(s) if s == "ObjectBarrier" => BarrierSelector::ObjectBarrier,
                        Ok(s) if s == "NoBarrier" => BarrierSelector::NoBarrier,
                        Ok(s) if s == "FieldBarrier" => BarrierSelector::FieldLoggingBarrier,
                        _ => unreachable!("Please explicitly specify barrier"),
                    }
                } else if super::CONCURRENT_MARKING || super::REF_COUNT {
                    BarrierSelector::FieldLoggingBarrier
                } else {
                    BarrierSelector::NoBarrier
                }
            });
        }
        B.unwrap()
    }
}

pub fn get_immix_constraints() -> &'static PlanConstraints {
    static mut C: PlanConstraints = PlanConstraints {
        moves_objects: true,
        gc_header_bits: 2,
        gc_header_words: 0,
        num_specialized_scans: 1,
        /// Max immix object size is half of a block.
        max_non_los_default_alloc_bytes: Block::BYTES >> 1,
        barrier: BarrierSelector::NoBarrier,
        needs_log_bit: super::CONCURRENT_MARKING || super::REF_COUNT,
        needs_field_log_bit: super::CONCURRENT_MARKING || super::REF_COUNT,
        ..PlanConstraints::default()
    };
    unsafe {
        C.barrier = get_active_barrier();
        &C
    }
}

impl<VM: VMBinding> Plan for Immix<VM> {
    type VM = VM;

    fn collection_required(&self, space_full: bool, space: &dyn Space<Self::VM>) -> bool {
        self.base().collection_required(self, space_full, space)
            || (crate::flags::REF_COUNT
                && crate::flags::LOCK_FREE_BLOCK_ALLOCATION
                && self.immix_space.block_allocation.nursery_blocks()
                    >= crate::flags::NURSERY_BLOCKS_THRESHOLD_FOR_RC)
    }

    fn concurrent_collection_required(&self) -> bool {
        super::CONCURRENT_MARKING
            && !crate::plan::barriers::BARRIER_MEASUREMENT
            && self.base().gc_status() == GcStatus::NotInGC
            && self.get_pages_reserved() * 100 / 45 > self.get_total_pages()
    }

    fn constraints(&self) -> &'static PlanConstraints {
        get_immix_constraints()
    }

    fn create_worker_local(
        &self,
        tls: VMWorkerThread,
        mmtk: &'static MMTK<Self::VM>,
    ) -> GCWorkerLocalPtr {
        let mut c = ImmixCopyContext::new(mmtk);
        c.init(tls);
        GCWorkerLocalPtr::new(c)
    }

    fn gc_init(
        &mut self,
        heap_size: usize,
        vm_map: &'static VMMap,
        scheduler: &Arc<GCWorkScheduler<VM>>,
    ) {
        println!(
            "BARRIER_MEASUREMENT: {}",
            crate::plan::barriers::BARRIER_MEASUREMENT
        );
        println!(
            "TAKERATE_MEASUREMENT: {}",
            crate::plan::barriers::TAKERATE_MEASUREMENT
        );
        println!("CONCURRENT_MARKING: {}", super::CONCURRENT_MARKING);
        println!("REF_COUNT: {}", super::REF_COUNT);
        println!("BARRIER: {:?}", get_active_barrier());
        self.common.gc_init(heap_size, vm_map, scheduler);
        self.immix_space.init(vm_map);
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>, concurrent: bool) {
        self.base().set_collection_kind();
        self.base().set_gc_status(GcStatus::GcPrepare);
        let in_defrag = self.immix_space.decide_whether_to_defrag(
            self.is_emergency_collection(),
            true,
            self.base().cur_collection_attempts.load(Ordering::SeqCst),
            self.base().is_user_triggered_collection(),
            self.base().options.full_heap_system_gc,
        );
        let cc_force_full = self.base().options.full_heap_system_gc;
        let mut perform_cycle_collection = !super::REF_COUNT
            || crate::plan::barriers::BARRIER_MEASUREMENT
            || self.perform_cycle_collection.load(Ordering::SeqCst);
        perform_cycle_collection |= (self.base().cur_collection_attempts.load(Ordering::SeqCst)
            > 1)
            || self.is_emergency_collection()
            || cc_force_full;
        self.perform_cycle_collection
            .store(perform_cycle_collection, Ordering::SeqCst);
        scheduler.drop_postponed_work.store(perform_cycle_collection, Ordering::SeqCst);
        if crate::flags::LOG_PER_GC_STATE {
            println!(
                "[STW] RC={} emergency={} defrag={}",
                !perform_cycle_collection,
                self.is_emergency_collection(),
                in_defrag
            );
        }
        if in_defrag {
            self.schedule_immix_collection::<ImmixProcessEdges<VM, { TraceKind::Defrag }>>(
                scheduler, concurrent, in_defrag,
            );
        } else {
            if !super::REF_COUNT || perform_cycle_collection {
                self.schedule_immix_collection::<ImmixProcessEdges<VM, { TraceKind::Fast }>>(
                    scheduler, concurrent, in_defrag,
                );
            } else {
                self.schedule_immix_collection::<RCImmixProcessEdges<VM, { TraceKind::Fast }>>(
                    scheduler, concurrent, in_defrag,
                );
            }
        }
        scheduler.set_finalizer(Some(EndOfGC));
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &*ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        if !super::REF_COUNT || self.perform_cycle_collection() {
            self.common.prepare(tls, true);
            self.immix_space.prepare();
        } else {
            self.immix_space.prepare_rc();
        }
    }

    fn release(&mut self, tls: VMWorkerThread) {
        if !super::REF_COUNT || self.perform_cycle_collection() {
            self.common.release(tls, true);
            self.immix_space.release();
        } else {
            self.immix_space.release_rc();
        }
        if super::REF_COUNT {
            let mut curr_roots = super::gc_work::CURR_ROOTS.lock();
            let mut old_roots = super::gc_work::OLD_ROOTS.lock();
            std::mem::swap::<Vec<ObjectReference>>(&mut curr_roots, &mut old_roots);
            self.base()
                .control_collector_context
                .scheduler()
                .work_buckets[WorkBucketStage::Final]
                .add(UpdatePerformCycleCollection);
        }
    }

    fn get_collection_reserve(&self) -> usize {
        self.immix_space.defrag_headroom_pages()
    }

    fn get_pages_used(&self) -> usize {
        self.immix_space.reserved_pages() + self.common.get_pages_used()
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.common.base
    }

    fn common(&self) -> &CommonPlan<VM> {
        &self.common
    }
}

impl<VM: VMBinding> Immix<VM> {
    pub fn new(
        vm_map: &'static VMMap,
        mmapper: &'static Mmapper,
        options: Arc<UnsafeOptionsWrapper>,
        scheduler: Arc<GCWorkScheduler<VM>>,
    ) -> Self {
        let mut heap = HeapMeta::new(HEAP_START, HEAP_END);
        let immix_specs = if get_immix_constraints().barrier != BarrierSelector::NoBarrier
            || crate::plan::barriers::BARRIER_MEASUREMENT
        {
            metadata::extract_side_metadata(&[*VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC])
        } else {
            vec![]
        };
        let global_metadata_specs = SideMetadataContext::new_global_specs(&immix_specs);
        let immix = Immix {
            immix_space: ImmixSpace::new(
                "immix",
                vm_map,
                mmapper,
                &mut heap,
                scheduler,
                global_metadata_specs.clone(),
                get_immix_constraints(),
            ),
            common: CommonPlan::new(
                vm_map,
                mmapper,
                options,
                heap,
                get_immix_constraints(),
                global_metadata_specs,
            ),
            perform_cycle_collection: AtomicBool::new(false),
        };

        {
            let mut side_metadata_sanity_checker = SideMetadataSanity::new();
            immix
                .common
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
            immix
                .immix_space
                .verify_side_metadata_sanity(&mut side_metadata_sanity_checker);
        }

        immix
    }

    fn schedule_immix_collection<E: ProcessEdgesWork<VM = VM>>(
        &'static self,
        scheduler: &GCWorkScheduler<VM>,
        concurrent: bool,
        _in_defrag: bool,
    ) {
        // Stop & scan mutators (mutator scanning can happen before STW)
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E>::new());
        // Prepare global/collectors/mutators
        if concurrent {
            scheduler.work_buckets[WorkBucketStage::PreClosure].add(ConcurrentWorkStart);
            scheduler.work_buckets[WorkBucketStage::PostClosure].add(ConcurrentWorkEnd::<E>::new());
        }
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<Self, ImmixCopyContext<VM>>::new(self));
        scheduler.work_buckets[WorkBucketStage::RefClosure].add(ProcessWeakRefs::<E>::new());
        scheduler.work_buckets[WorkBucketStage::RefClosure].add(FlushMutators::<VM>::new());
        // Release global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<Self, ImmixCopyContext<VM>>::new(self));

        if super::REF_COUNT {
            let mut decs = vec![];
            let mut old_roots = super::gc_work::OLD_ROOTS.lock();
            std::mem::swap(&mut decs, &mut old_roots);
            if crate::policy::immix::SANITY {
                for x in &decs {
                    debug_assert!(!self
                        .immix_space
                        .new_blocks
                        .lock()
                        .contains(&Block::containing::<E::VM>(*x)));
                }
            }
            scheduler.work_buckets[WorkBucketStage::RefClosure].add(ProcessDecs::<VM>::new(decs));
        }

        // Analysis routine that is ran. It is generally recommended to take advantage
        // of the scheduling system we have in place for more performance
        #[cfg(feature = "analysis")]
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(GcHookWork);
        // Resume mutators
        #[cfg(feature = "sanity")]
        scheduler.work_buckets[WorkBucketStage::Final]
            .add(ScheduleSanityGC::<Self, ImmixCopyContext<VM>>::new(self));
    }

    pub fn perform_cycle_collection(&self) -> bool {
        self.perform_cycle_collection.load(Ordering::SeqCst)
    }
}

struct UpdatePerformCycleCollection;

impl<VM: VMBinding> GCWork<VM> for UpdatePerformCycleCollection {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        let perform_cycle_collection = immix.get_pages_avail() < super::CYCLE_TRIGGER_THRESHOLD;
        immix
            .perform_cycle_collection
            .store(perform_cycle_collection, Ordering::SeqCst);
    }
}
