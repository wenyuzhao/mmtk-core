use super::gc_work::{
    CMImmixCollectRootEdges, ImmixCopyContext, ImmixProcessEdges, RCImmixCollectRootEdges,
    TraceKind,
};
use super::mutator::ALLOCATOR_MAPPING;
use super::{Pause, CURRENT_CONC_DECS_COUNTER};
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
use crate::util::rc::RC_LOCK_BIT_SPEC;
use crate::util::options::UnsafeOptionsWrapper;
use crate::util::rc::ProcessDecs;
#[cfg(feature = "sanity")]
use crate::util::sanity::sanity_checker::*;
use crate::util::{metadata, ObjectReference};
use crate::vm::{ObjectModel, VMBinding};
use crate::{
    mmtk::MMTK,
    policy::immix::{block::Block, ImmixSpace},
    util::opaque_pointer::VMWorkerThread,
};
use crate::{scheduler::*, BarrierSelector};
use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

use atomic::{Atomic, Ordering};
use enum_map::EnumMap;

pub const ALLOC_IMMIX: AllocationSemantics = AllocationSemantics::Default;

pub struct Immix<VM: VMBinding> {
    pub immix_space: ImmixSpace<VM>,
    pub common: CommonPlan<VM>,
    /// Always true for non-rc immix.
    /// For RC immix, this is used for enable backup tracing.
    perform_cycle_collection: AtomicBool,
    current_pause: Atomic<Option<Pause>>,
    previous_pause: Atomic<Option<Pause>>,
    next_gc_may_perform_cycle_collection: AtomicBool,
}

#[inline(always)]
pub fn get_active_barrier() -> BarrierSelector {
    static mut V: Option<BarrierSelector> = None;
    unsafe {
        *V.get_or_insert_with(|| {
            if crate::plan::barriers::BARRIER_MEASUREMENT {
                match env::var("BARRIER") {
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
        })
    }
}

#[inline(always)]
pub fn get_immix_constraints() -> &'static PlanConstraints {
    static mut C: PlanConstraints = PlanConstraints {
        moves_objects: true,
        gc_header_bits: 2,
        gc_header_words: 0,
        num_specialized_scans: 1,
        /// Max immix object size is half of a block.
        max_non_los_default_alloc_bytes: Block::BYTES >> 1,
        barrier: BarrierSelector::NoBarrier,
        needs_log_bit: false,
        needs_field_log_bit: false,
        ..PlanConstraints::default()
    };
    unsafe {
        let barrier = get_active_barrier();
        C.barrier = barrier;
        C.needs_log_bit =
            crate::flags::BARRIER_MEASUREMENT || barrier != BarrierSelector::NoBarrier;
        C.needs_field_log_bit = barrier == BarrierSelector::FieldLoggingBarrier
            || (crate::flags::BARRIER_MEASUREMENT && barrier == BarrierSelector::NoBarrier);
        &C
    }
}

impl<VM: VMBinding> Plan for Immix<VM> {
    type VM = VM;

    fn collection_required(&self, space_full: bool, space: &dyn Space<Self::VM>) -> bool {
        // Spaces or heap full
        if self.base().collection_required(self, space_full, space) {
            return true;
        }
        // Concurrent tracing finished
        if crate::flags::CONCURRENT_MARKING
            && crate::concurrent_marking_in_progress()
            && crate::concurrent_marking_packets_drained()
        {
            return true;
        }
        // RC nursery full
        if crate::flags::REF_COUNT
            && crate::flags::LOCK_FREE_BLOCK_ALLOCATION
            && self.immix_space.block_allocation.nursery_blocks()
                >= crate::flags::NURSERY_BLOCKS_THRESHOLD_FOR_RC
        {
            return true;
        }
        false
    }

    fn concurrent_collection_required(&self) -> bool {
        super::CONCURRENT_MARKING
            && !crate::plan::barriers::BARRIER_MEASUREMENT
            && !crate::concurrent_marking_in_progress()
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
        unsafe {
            CURRENT_CONC_DECS_COUNTER = Some(Default::default());
        }
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>, concurrent: bool) {
        #[cfg(feature = "nogc_no_zeroing")]
        if true {
            unreachable!();
        }
        let pause = self.select_collection_kind(concurrent);
        match pause {
            Pause::FullTraceFast => self
                .schedule_immix_collection::<ImmixProcessEdges<VM, { TraceKind::Fast }>>(scheduler),
            Pause::FullTraceDefrag => self
                .schedule_immix_collection::<ImmixProcessEdges<VM, { TraceKind::Defrag }>>(
                    scheduler,
                ),
            Pause::RefCount => self.schedule_rc_collection(scheduler),
            Pause::InitialMark => self.schedule_concurrent_marking_initial_pause(scheduler),
            Pause::FinalMark => self.schedule_concurrent_marking_final_pause(scheduler),
        }

        // Analysis routine that is ran. It is generally recommended to take advantage
        // of the scheduling system we have in place for more performance
        #[cfg(feature = "analysis")]
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(GcHookWork);
        // Resume mutators
        #[cfg(feature = "sanity")]
        scheduler.work_buckets[WorkBucketStage::Final]
            .add(ScheduleSanityGC::<Self, ImmixCopyContext<VM>>::new(self));

        scheduler.set_finalizer(Some(EndOfGC));
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &*ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        if !super::REF_COUNT {
            if pause != Pause::FinalMark {
                self.common.prepare(tls, true);
                self.immix_space.prepare(true, true);
            }
        } else {
            if pause == Pause::FullTraceFast || pause == Pause::InitialMark {
                self.common.prepare(tls, true);
            }
            self.immix_space.prepare_rc(pause);
        }
    }

    fn release(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        if !super::REF_COUNT {
            if pause != Pause::InitialMark {
                self.common.release(tls, true);
                self.immix_space.release(true);
            }
        } else {
            if pause == Pause::FullTraceFast || pause == Pause::FinalMark {
                self.common.release(tls, true);
            }
            self.immix_space.release_rc(pause);
        }
        if super::REF_COUNT {
            let mut curr_roots = super::CURR_ROOTS.lock();
            let mut old_roots = super::PREV_ROOTS.lock();
            std::mem::swap::<Vec<Vec<ObjectReference>>>(&mut curr_roots, &mut old_roots);
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

    fn gc_pause_start(&self) {
        if self.current_pause().unwrap() == Pause::FinalMark {
            crate::IN_CONCURRENT_GC.store(false, Ordering::SeqCst);
        }
    }

    fn gc_pause_end(&self) {
        if self.current_pause().unwrap() == Pause::InitialMark {
            crate::IN_CONCURRENT_GC.store(true, Ordering::SeqCst);
        }
        self.previous_pause.store(
            Some(self.current_pause.load(Ordering::SeqCst).unwrap()),
            Ordering::SeqCst,
        );
        self.current_pause.store(None, Ordering::SeqCst);
        unsafe { CURRENT_CONC_DECS_COUNTER = Some(Arc::new(AtomicUsize::new(0))) };
        let perform_cycle_collection = self.get_pages_avail() < super::CYCLE_TRIGGER_THRESHOLD;
        self.next_gc_may_perform_cycle_collection
            .store(perform_cycle_collection, Ordering::SeqCst);
        self.perform_cycle_collection.store(false, Ordering::SeqCst);
    }

    #[cfg(feature = "nogc_no_zeroing")]
    fn handle_user_collection_request(&self, _tls: crate::util::VMMutatorThread, _force: bool) {
        println!("Warning: User attempted a collection request. The request is ignored.");
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        let x = self.previous_pause.load(Ordering::SeqCst);
        x == Some(Pause::FullTraceFast) || x == Some(Pause::FullTraceDefrag)
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
        let immix_specs = if crate::flags::BARRIER_MEASUREMENT
            || get_active_barrier() != BarrierSelector::NoBarrier
        {
            metadata::extract_side_metadata(&[
                RC_LOCK_BIT_SPEC,
                *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
            ])
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
            next_gc_may_perform_cycle_collection: AtomicBool::new(false),
            current_pause: Atomic::new(None),
            previous_pause: Atomic::new(None),
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

    fn select_collection_kind(&self, concurrent: bool) -> Pause {
        self.base().set_collection_kind(self);
        self.base().set_gc_status(GcStatus::GcPrepare);
        let in_defrag = self.immix_space.decide_whether_to_defrag(
            self.is_emergency_collection(),
            true,
            self.base().cur_collection_attempts.load(Ordering::SeqCst),
            self.base().is_user_triggered_collection(),
            self.base().options.full_heap_system_gc,
        );
        let full_trace = || {
            if in_defrag {
                Pause::FullTraceDefrag
            } else {
                Pause::FullTraceFast
            }
        };
        let emergency_collection = (self.base().cur_collection_attempts.load(Ordering::SeqCst) > 1)
            || self.is_emergency_collection()
            || self.base().options.full_heap_system_gc;

        let concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        let concurrent_marking_packets_drained = crate::concurrent_marking_packets_drained();
        let force_no_rc = self
            .next_gc_may_perform_cycle_collection
            .load(Ordering::SeqCst);
        let pause = if emergency_collection {
            if concurrent_marking_in_progress {
                Pause::FinalMark
            } else {
                full_trace()
            }
        } else if concurrent_marking_in_progress && concurrent_marking_packets_drained {
            Pause::FinalMark
        } else if concurrent {
            Pause::InitialMark
        } else {
            if (!super::REF_COUNT || crate::flags::NO_RC_PAUSES_DURING_CONCURRENT_MARKING)
                && concurrent_marking_in_progress
            {
                Pause::FinalMark
            } else if super::REF_COUNT && (!force_no_rc || concurrent_marking_in_progress) {
                Pause::RefCount
            } else {
                full_trace()
            }
        };
        self.current_pause.store(Some(pause), Ordering::SeqCst);
        self.perform_cycle_collection
            .store(pause != Pause::RefCount, Ordering::SeqCst);
        if crate::flags::LOG_PER_GC_STATE {
            println!(
                "[STW] {:?} emergency={}",
                pause,
                self.is_emergency_collection()
            );
        }
        pause
    }

    fn schedule_immix_collection<E: ProcessEdgesWork<VM = VM>>(
        &'static self,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        // Before start yielding, wrap all the roots from the previous GC with work-packets.
        if super::REF_COUNT {
            Self::process_prev_roots(scheduler);
        }
        // Stop & scan mutators (mutator scanning can happen before STW)
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E>::new());
        // Prepare global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<Self, ImmixCopyContext<VM>>::new(self));
        // Release global/collectors/mutators
        if super::REF_COUNT {
            scheduler.work_buckets[WorkBucketStage::RCFullHeapRelease]
                .add(Release::<Self, ImmixCopyContext<VM>>::new(self));
        } else {
            scheduler.work_buckets[WorkBucketStage::Release]
                .add(Release::<Self, ImmixCopyContext<VM>>::new(self));
        }
    }

    fn schedule_rc_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        if crate::flags::CONCURRENT_MARKING && !crate::flags::NO_RC_PAUSES_DURING_CONCURRENT_MARKING
        {
            if crate::concurrent_marking_in_progress() {
                scheduler.pause_concurrent_work_packets_during_gc();
            }
        }
        debug_assert!(super::REF_COUNT);
        type E<VM> = RCImmixCollectRootEdges<VM>;
        // Before start yielding, wrap all the roots from the previous GC with work-packets.
        Self::process_prev_roots(scheduler);
        // Stop & scan mutators (mutator scanning can happen before STW)
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E<VM>>::new());
        // Prepare global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<Self, ImmixCopyContext<VM>>::new(self));
        // Release global/collectors/mutators
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<Self, ImmixCopyContext<VM>>::new(self));
    }

    fn schedule_concurrent_marking_initial_pause(&'static self, scheduler: &GCWorkScheduler<VM>) {
        if super::REF_COUNT {
            Self::process_prev_roots(scheduler);
        }
        if crate::flags::REF_COUNT {
            scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(StopMutators::<RCImmixCollectRootEdges<VM>>::new())
        } else {
            scheduler.work_buckets[WorkBucketStage::Unconstrained]
                .add(StopMutators::<CMImmixCollectRootEdges<VM>>::new())
        };
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<Self, ImmixCopyContext<VM>>::new(self));
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<Self, ImmixCopyContext<VM>>::new(self));
    }

    fn schedule_concurrent_marking_final_pause(&'static self, scheduler: &GCWorkScheduler<VM>) {
        type E<VM> = ImmixProcessEdges<VM, { TraceKind::Fast }>;
        if super::REF_COUNT {
            Self::process_prev_roots(scheduler);
        }
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(StopMutators::<E<VM>>::new());
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(Prepare::<Self, ImmixCopyContext<VM>>::new(self));
        if super::REF_COUNT {
            scheduler.work_buckets[WorkBucketStage::RCFullHeapRelease]
                .add(Release::<Self, ImmixCopyContext<VM>>::new(self));
        } else {
            scheduler.work_buckets[WorkBucketStage::Release]
                .add(Release::<Self, ImmixCopyContext<VM>>::new(self));
        }
    }

    fn process_prev_roots(scheduler: &GCWorkScheduler<VM>) {
        debug_assert!(super::REF_COUNT);
        let mut roots = super::PREV_ROOTS.lock();
        let mut work_packets: Vec<Box<dyn GCWork<VM>>> = Vec::with_capacity(roots.len());
        for decs in roots.drain(..) {
            let w = ProcessDecs::new(decs, unsafe { CURRENT_CONC_DECS_COUNTER.clone().unwrap() });
            work_packets.push(box w);
        }
        if crate::flags::LAZY_DECREMENTS {
            debug_assert!(!crate::flags::BARRIER_MEASUREMENT);
            scheduler.postpone_all(work_packets);
        } else {
            scheduler.work_buckets[WorkBucketStage::RCProcessDecs].bulk_add(work_packets);
        }
    }

    pub fn perform_cycle_collection(&self) -> bool {
        self.perform_cycle_collection.load(Ordering::SeqCst)
    }

    pub fn current_pause(&self) -> Option<Pause> {
        self.current_pause.load(Ordering::SeqCst)
    }
}
