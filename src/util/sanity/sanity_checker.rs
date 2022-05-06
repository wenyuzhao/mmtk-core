use crate::plan::immix::Immix;
use crate::plan::immix::Pause;
use crate::plan::Plan;
use crate::policy::immix::block::Block;
use crate::policy::space::Space;
use crate::scheduler::gc_work::*;
use crate::scheduler::*;
use crate::util::object_forwarding;
use crate::util::{Address, ObjectReference};
use crate::vm::*;
use crate::MMTK;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;

#[allow(dead_code)]
pub struct SanityChecker {
    /// Visited objects
    refs: HashSet<ObjectReference>,
    /// Cached root edges for sanity root scanning
    roots: Vec<Vec<Address>>,
}

impl Default for SanityChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl SanityChecker {
    pub fn new() -> Self {
        Self {
            refs: HashSet::new(),
            roots: vec![],
        }
    }

    /// Cache a list of root edges to the sanity checker.
    pub fn add_roots(&mut self, roots: Vec<Address>) {
        self.roots.push(roots)
    }

    /// Reset roots cache at the end of the sanity gc.
    fn clear_roots_cache(&mut self) {
        self.roots.clear();
    }
}

pub struct ScheduleSanityGC<P: Plan> {
    _plan: &'static P,
}

impl<P: Plan> ScheduleSanityGC<P> {
    pub fn new(plan: &'static P) -> Self {
        ScheduleSanityGC { _plan: plan }
    }
}

impl<P: Plan> GCWork<P::VM> for ScheduleSanityGC<P> {
    fn do_work(&mut self, worker: &mut GCWorker<P::VM>, mmtk: &'static MMTK<P::VM>) {
        let scheduler = worker.scheduler();
        let plan = &mmtk.plan;

        scheduler.reset_state();

        plan.base().inside_sanity.store(true, Ordering::SeqCst);
        // Stop & scan mutators (mutator scanning can happen before STW)

        // We use the cached roots for sanity gc, based on the assumption that
        // the stack scanning triggered by the selected plan is correct and precise.
        // FIXME(Wenyu,Tianle): When working on eager stack scanning on OpenJDK,
        // the stack scanning may be broken. Uncomment the following lines to
        // collect the roots again.
        // Also, remember to call `DerivedPointerTable::update_pointers(); DerivedPointerTable::clear();`
        // in openjdk binding before the second round of roots scanning.
        // for mutator in <P::VM as VMBinding>::VMActivePlan::mutators() {
        //     scheduler.work_buckets[WorkBucketStage::Prepare]
        //         .add(ScanStackRoot::<SanityGCProcessEdges<P::VM>>(mutator));
        // }
        for roots in &mmtk.sanity_checker.lock().unwrap().roots {
            scheduler.work_buckets[WorkBucketStage::Closure].add(
                SanityGCProcessEdges::<P::VM>::new(roots.clone(), true, mmtk),
            );
        }
        scheduler.work_buckets[WorkBucketStage::Prepare]
            .add(ScanVMSpecificRoots::<SanityGCProcessEdges<P::VM>>::new());
        // Prepare global/collectors/mutators
        worker.scheduler().work_buckets[WorkBucketStage::Prepare]
            .add(SanityPrepare::<P>::new(plan.downcast_ref::<P>().unwrap()));
        // Release global/collectors/mutators
        worker.scheduler().work_buckets[WorkBucketStage::Release]
            .add(SanityRelease::<P>::new(plan.downcast_ref::<P>().unwrap()));
    }
}

pub struct SanityPrepare<P: Plan> {
    pub plan: &'static P,
}

impl<P: Plan> SanityPrepare<P> {
    pub fn new(plan: &'static P) -> Self {
        Self { plan }
    }
}

impl<P: Plan> GCWork<P::VM> for SanityPrepare<P> {
    fn do_work(&mut self, _worker: &mut GCWorker<P::VM>, mmtk: &'static MMTK<P::VM>) {
        mmtk.plan.enter_sanity();
        {
            let mut sanity_checker = mmtk.sanity_checker.lock().unwrap();
            sanity_checker.refs.clear();
        }
        for mutator in <P::VM as VMBinding>::VMActivePlan::mutators() {
            mmtk.scheduler.work_buckets[WorkBucketStage::Prepare]
                .add(PrepareMutator::<P::VM>::new(mutator));
        }
        for w in &mmtk.scheduler.worker_group().workers {
            w.local_work_bucket.add(PrepareCollector);
        }
    }
}

pub struct SanityRelease<P: Plan> {
    pub plan: &'static P,
}

impl<P: Plan> SanityRelease<P> {
    pub fn new(plan: &'static P) -> Self {
        Self { plan }
    }
}

impl<P: Plan> GCWork<P::VM> for SanityRelease<P> {
    fn do_work(&mut self, _worker: &mut GCWorker<P::VM>, mmtk: &'static MMTK<P::VM>) {
        mmtk.plan.leave_sanity();
        mmtk.sanity_checker.lock().unwrap().clear_roots_cache();
        for mutator in <P::VM as VMBinding>::VMActivePlan::mutators() {
            mmtk.scheduler.work_buckets[WorkBucketStage::Release]
                .add(ReleaseMutator::<P::VM>::new(mutator));
        }
        for w in &mmtk.scheduler.worker_group().workers {
            w.local_work_bucket.add(ReleaseCollector);
        }
    }
}

// #[derive(Default)]
pub struct SanityGCProcessEdges<VM: VMBinding> {
    base: ProcessEdgesBase<SanityGCProcessEdges<VM>>,
    // phantom: PhantomData<VM>,
}

impl<VM: VMBinding> Deref for SanityGCProcessEdges<VM> {
    type Target = ProcessEdgesBase<Self>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for SanityGCProcessEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl<VM: VMBinding> SanityGCProcessEdges<VM> {
    #[inline]
    fn trace_object(&mut self, slot: Address, object: ObjectReference) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        let mut sanity_checker = self.mmtk().sanity_checker.lock().unwrap();
        if !sanity_checker.refs.contains(&object) {
            sanity_checker.refs.insert(object); // "Mark" it
            std::mem::drop(sanity_checker);
            // FIXME steveb consider VM-specific integrity check on reference.
            if !object.is_sane() {
                panic!("Invalid reference {:?} -> {:?}", slot, object);
            }
            if let Some(immix) = self.mmtk().plan.downcast_ref::<Immix<VM>>() {
                assert!(!immix.current_pause().is_none());
                if immix.immix_space.in_space(object) {
                    if immix.current_pause() == Some(Pause::FinalMark) {
                        // println!("ix {:?} rc={:?} defrag={}", object, crate::util::rc::count(object), Block::containing::<VM>(object).is_defrag_source());
                        assert!(
                            !Block::containing::<VM>(object).is_defrag_source(),
                            "ix {:?} -> {:?} rc={:?} defrag={}",
                            slot,
                            object,
                            crate::util::rc::count(object),
                            Block::containing::<VM>(object).is_defrag_source()
                        );
                    }
                }
            }
            if !object.is_live() {
                if let Some(immix) = self.mmtk().plan.downcast_ref::<Immix<VM>>() {
                    if immix.immix_space.in_space(object) {
                        println!(
                            "[object] ix {:?} {:?} rc={:?} defrag={} mark={} forwarded={}",
                            object,
                            Block::containing::<VM>(object).get_state(),
                            crate::util::rc::count(object),
                            Block::containing::<VM>(object).is_defrag_source(),
                            immix.immix_space.mark_bit(object),
                            object_forwarding::is_forwarded::<VM>(object),
                        );
                    }
                    if immix.immix_space.address_in_space(slot) {
                        println!(
                            "[slot] ix {:?} {:?} defrag={}",
                            slot,
                            Block::from(Block::align(slot)).get_state(),
                            Block::from(Block::align(slot)).is_defrag_source()
                        );
                    }
                }
            }
            assert!(object.is_live(), "{:?}: {:?} is dead", slot, object,);
            assert!(
                object.is_live(),
                "{:?}: {:?} is dead, e@{:?}({:?}), o@{:?}({:?}) o.end={:} o.rc={} o.mark={} {:?} ",
                slot,
                object,
                Block::from(slot),
                Block::from(slot).get_state(),
                Block::containing::<VM>(object),
                Block::containing::<VM>(object).get_state(),
                object.to_address() + object.get_size::<VM>(),
                if crate::args::REF_COUNT {
                    crate::util::rc::count(object)
                } else {
                    0
                },
                self.mmtk()
                    .plan
                    .downcast_ref::<Immix<VM>>()
                    .unwrap()
                    .immix_space
                    .mark_bit(object),
                self.mmtk()
                    .plan
                    .downcast_ref::<Immix<VM>>()
                    .unwrap()
                    .current_pause(),
                // object.dump_s::<VM>()
            );
            assert!(
                unsafe { object.to_address().load::<usize>() } != 0xdead,
                "{:?} -> {:?} is dead, {:?}",
                slot,
                object,
                Block::containing::<VM>(object)
            );
            assert!(
                unsafe { (object.to_address() + 8usize).load::<usize>() } != 0xdead,
                "{:?} -> {:?} is dead, {:?}",
                slot,
                object,
                Block::containing::<VM>(object)
            );
            assert!(
                !object_forwarding::is_forwarded::<VM>(object),
                "{:?} -> {:?} is forwarded, {:?} {:?}",
                slot,
                object,
                Block::containing::<VM>(object),
                object_forwarding::read_forwarding_pointer::<VM>(object),
            );
            // Object is not "marked"
            ProcessEdgesWork::process_node(self, object);
        }
        object
    }
}

impl<VM: VMBinding> ProcessEdgesWork for SanityGCProcessEdges<VM> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = false;
    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        Self {
            base: ProcessEdgesBase::new(edges, roots, mmtk),
            // ..Default::default()
        }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    fn process_edge(&mut self, slot: Address) {
        let object = unsafe { slot.load::<ObjectReference>() };
        self.trace_object(slot, object);
    }
}
