use std::{
    collections::HashSet,
    iter::Step,
    sync::{atomic::AtomicUsize, Arc},
};

use atomic::Ordering;

use crate::{
    plan::{
        barriers::{LOCKED_VALUE, UNLOCKED_VALUE, UNLOGGED_VALUE},
        immix::{Immix, ImmixCopyContext},
        EdgeIterator,
    },
    policy::{
        immix::{block::Block, chunk::ChunkMap, line::Line},
        space::Space,
    },
    scheduler::{GCWork, GCWorkScheduler, GCWorker, WorkBucketStage},
    util::{
        metadata::side_metadata::{self, SideMetadataOffset, SideMetadataSpec},
        object_forwarding, ObjectReference,
    },
    vm::*,
    AllocationSemantics, CopyContext, MMTK,
};

use super::{
    metadata::{compare_exchange_metadata, load_metadata, store_metadata, RC_LOCK_BIT_SPEC},
    Address,
};

pub const LOG_REF_COUNT_BITS: usize = 2;
const MAX_REF_COUNT: usize = (1 << (1 << LOG_REF_COUNT_BITS)) - 1;

pub const LOG_MIN_OBJECT_SIZE: usize = 4;
pub const MIN_OBJECT_SIZE: usize = 1 << LOG_MIN_OBJECT_SIZE;

pub const RC_TABLE: SideMetadataSpec = SideMetadataSpec {
    is_global: false,
    offset: SideMetadataOffset::layout_after(&ChunkMap::ALLOC_TABLE),
    log_num_of_bits: LOG_REF_COUNT_BITS,
    log_min_obj_size: LOG_MIN_OBJECT_SIZE as _,
};

#[inline(always)]
pub fn inc(o: ObjectReference) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    let r = side_metadata::fetch_update(
        &RC_TABLE,
        o.to_address(),
        Ordering::SeqCst,
        Ordering::SeqCst,
        |x| {
            if x == MAX_REF_COUNT {
                None
            } else {
                Some(x + 1)
            }
        },
    );
    // println!("inc {:?} {:?} -> {:?}", o, r, count(o));
    r
}

#[inline(always)]
pub fn dec(o: ObjectReference) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    let r = side_metadata::fetch_update(
        &RC_TABLE,
        o.to_address(),
        Ordering::SeqCst,
        Ordering::SeqCst,
        |x| {
            if x == 0 || x == MAX_REF_COUNT
            /* sticky */
            {
                None
            } else {
                Some(x - 1)
            }
        },
    );
    // println!("dec {:?} {:?} -> {:?}", o, r, count(o));
    r
}

#[inline(always)]
pub fn set(o: ObjectReference, count: usize) {
    debug_assert!(!o.is_null());
    side_metadata::store_atomic(&RC_TABLE, o.to_address(), count, Ordering::SeqCst)
}

pub fn count(o: ObjectReference) -> usize {
    side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::SeqCst)
}

#[allow(unused)]
#[inline(always)]
pub fn is_dead(o: ObjectReference) -> bool {
    let v = side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::SeqCst);
    v == 0
}

#[inline(always)]
pub fn mark_striddle_object<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(!crate::flags::BLOCK_ONLY);
    debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
    let size = VM::VMObjectModel::get_current_size(o);
    debug_assert!(size > Line::BYTES);
    let start_line = Line::forward(Line::containing::<VM>(o), 1);
    let end_line = Line::from(Line::align(o.to_address() + size));
    for line in start_line..end_line {
        self::set(unsafe { line.start().to_object_reference() }, 1);
    }
}

#[inline(always)]
pub fn unmark_striddle_object<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(!crate::flags::BLOCK_ONLY);
    debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
    let size = VM::VMObjectModel::get_current_size(o);
    if size > Line::BYTES {
        let start_line = Line::forward(Line::containing::<VM>(o), 1);
        let end_line = Line::from(Line::align(o.to_address() + size));
        for line in start_line..end_line {
            self::set(unsafe { line.start().to_object_reference() }, 0);
        }
    }
}

#[inline(always)]
pub fn assert_zero_ref_count<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(crate::flags::REF_COUNT);
    let size = VM::VMObjectModel::get_current_size(o);
    for i in (0..size).step_by(MIN_OBJECT_SIZE) {
        let a = o.to_address() + i;
        assert_eq!(0, self::count(unsafe { a.to_object_reference() }));
    }
}

pub struct ProcessIncs {
    incs: Vec<Address>,
    new_incs: Vec<Address>,
    remset: Vec<Address>,
    roots: bool,
}

impl ProcessIncs {
    const CAPACITY: usize = 1024;

    pub const DELAYED_EVACUATION: bool = true;

    pub fn new(incs: Vec<Address>) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        Self {
            incs,
            roots: false,
            new_incs: vec![],
            remset: vec![],
        }
    }

    pub fn new_roots(incs: Vec<Address>) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        Self {
            incs,
            roots: true,
            new_incs: vec![],
            remset: vec![],
        }
    }

    #[inline(always)]
    fn lock_edge<VM: VMBinding>(&self, edge: Address) {
        loop {
            let v = load_metadata::<VM>(
                &RC_LOCK_BIT_SPEC,
                unsafe { edge.to_object_reference() },
                None,
                Some(Ordering::SeqCst),
            );
            if v == LOCKED_VALUE {
                continue;
            }
            if compare_exchange_metadata::<VM>(
                &RC_LOCK_BIT_SPEC,
                unsafe { edge.to_object_reference() },
                UNLOCKED_VALUE,
                LOCKED_VALUE,
                None,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                return;
            }
        }
    }

    #[inline(always)]
    fn unlock_edge<VM: VMBinding>(edge: Address) {
        store_metadata::<VM>(
            &RC_LOCK_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            UNLOCKED_VALUE,
            None,
            Some(Ordering::SeqCst),
        );
    }

    #[inline(always)]
    fn unlog_edge<VM: VMBinding>(edge: Address) {
        store_metadata::<VM>(
            &VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            UNLOGGED_VALUE,
            None,
            Some(Ordering::SeqCst),
        );
    }

    #[inline(always)]
    fn unlog_and_unlock_edge<VM: VMBinding>(edge: Address) {
        store_metadata::<VM>(
            &VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            UNLOGGED_VALUE,
            None,
            Some(Ordering::SeqCst),
        );
        store_metadata::<VM>(
            &RC_LOCK_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            UNLOCKED_VALUE,
            None,
            Some(Ordering::SeqCst),
        );
    }

    #[inline(always)]
    fn should_skip_inc_processing<VM: VMBinding>(_mmtk: &'static MMTK<VM>) -> bool {
        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        false
    }

    #[inline(always)]
    fn scan_nursery_object<VM: VMBinding>(
        &mut self,
        worker: &mut GCWorker<VM>,
        o: ObjectReference,
    ) {
        EdgeIterator::<VM>::iterate(o, |edge| {
            debug_assert!(unsafe { edge.to_object_reference().is_logged::<VM>() });
            self.recursive_inc(worker, edge);
        });
    }

    #[inline(always)]
    fn recursive_inc<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>, e: Address) {
        if self.new_incs.is_empty() {
            self.new_incs.reserve(Self::CAPACITY);
        }
        self.new_incs.push(e);
        if self.new_incs.len() > Self::CAPACITY {
            self.flush(worker)
        }
    }

    #[inline(always)]
    fn add_remset<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>, e: Address) {
        if !self.roots {
            debug_assert!(!e.is_locked::<VM>());
        }
        if self.remset.is_empty() {
            self.remset.reserve(Self::CAPACITY);
        }
        self.remset.push(e);
        if self.remset.len() > Self::CAPACITY {
            self.flush(worker)
        }
    }

    #[inline(always)]
    fn process_inc<VM: VMBinding>(
        &mut self,
        e: Address,
        o: ObjectReference,
        worker: &mut GCWorker<VM>,
        cycle_collection: bool,
    ) -> (ObjectReference, bool) {
        if crate::flags::RC_EVACUATE_NURSERY && !cycle_collection && self::count(o) == 0 {
            debug_assert!(self.roots);
            self.add_remset(worker, e);
            return (o, true);
        }
        if let Ok(0) = self::inc(o) {
            self.scan_nursery_object::<VM>(worker, o);
            debug_assert!(!crate::flags::RC_EVACUATE_NURSERY || cycle_collection);
            if crate::flags::RC_EVACUATE_NURSERY && !crate::flags::BLOCK_ONLY {
                // Young object is not evacuated. Mark as striddle in place if required.
                // FIXME: Fix this when allocating young objects to recyclable lines or doing young evacuation in a tracing pause.
                if o.get_size::<VM>() > Line::BYTES {
                    self::mark_striddle_object::<VM>(o);
                }
            }
        }
        (o, false)
    }

    #[inline(always)]
    fn process_inc_and_evacuate<VM: VMBinding, CC: CopyContext<VM = VM>>(
        &mut self,
        e: Address,
        o: ObjectReference,
        copy_context: &mut CC,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(o);
        if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            // Object is moved to a new location.
            let new = object_forwarding::spin_and_get_forwarded_object::<VM>(o, forwarding_status);
            if new != o {
                unsafe {
                    e.store(new);
                }
            }
            let r = self::inc(new);
            debug_assert_ne!(r, Ok(0));
            new
        } else {
            if let Ok(0) = self::inc(o) {
                // Evacuate the object
                let new = object_forwarding::forward_object::<VM, _>(
                    o,
                    AllocationSemantics::Default,
                    copy_context,
                );
                unsafe {
                    e.store(new);
                }
                self::set(o, 0);
                if crate::flags::RC_EVACUATE_NURSERY && !crate::flags::BLOCK_ONLY {
                    self::unmark_striddle_object::<VM>(o);
                }
                self.scan_nursery_object::<VM>(worker, new);
                new
            } else {
                // Object is not moved.
                object_forwarding::clear_forwarding_bits::<VM>(o);
                o
            }
        }
    }

    #[inline]
    fn flush<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>) {
        if !self.new_incs.is_empty() {
            let mut new_incs = vec![];
            std::mem::swap(&mut new_incs, &mut self.new_incs);
            worker.add_work(
                WorkBucketStage::rc_process_incs_stage(),
                ProcessIncs::new(new_incs),
            );
        }
        if !self.remset.is_empty() {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.remset);
            worker.add_work(
                WorkBucketStage::rc_process_incs_stage(),
                RCEvacuateNursery::new(remset, self.roots),
            );
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessIncs {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        if Self::should_skip_inc_processing(mmtk) {
            return;
        }
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        let tracing = immix.perform_cycle_collection();
        let rc_evacuation = crate::flags::RC_EVACUATE_NURSERY && !tracing;
        let mut roots = vec![];
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        let mut incs = vec![];
        std::mem::swap(&mut incs, &mut self.incs);
        for e in incs {
            if !self.roots {
                debug_assert!(unsafe { e.to_object_reference().is_logged::<VM>() });
                self.lock_edge::<VM>(e);
            }
            let o: ObjectReference = unsafe { e.load() };
            if !self.roots {
                let in_refcount_space = immix.immix_space.in_space(o);
                if Self::DELAYED_EVACUATION
                    && crate::flags::RC_EVACUATE_NURSERY
                    && !tracing
                    && in_refcount_space
                    && self::count(o) == 0
                {
                    // Don't unlog this edge. Delay the increment to the STW time.
                    Self::unlock_edge::<VM>(e);
                    self.add_remset(worker, e);
                    continue;
                } else {
                    Self::unlog_and_unlock_edge::<VM>(e);
                }
                if !in_refcount_space {
                    continue;
                }
            }
            debug_assert!(!o.is_null());
            let (o, movable) = if Self::DELAYED_EVACUATION || !rc_evacuation {
                self.process_inc(e, o, worker, tracing)
            } else {
                debug_assert!(crate::flags::RC_EVACUATE_NURSERY && !tracing);
                (
                    self.process_inc_and_evacuate(e, o, copy_context, worker),
                    false,
                )
            };
            if self.roots {
                if !crate::flags::RC_EVACUATE_NURSERY || tracing || !movable {
                    if !movable {
                        roots.push(o);
                    }
                }
            }
        }
        if self.roots {
            if !roots.is_empty() {
                crate::plan::immix::CURR_ROOTS.lock().push(roots);
            }
        } else {
            debug_assert!(roots.is_empty());
        }
        self.flush(worker);
    }
}

pub struct ProcessDecs {
    decs: Vec<ObjectReference>,
    count_down: Arc<AtomicUsize>,
    new_decs: Vec<ObjectReference>,
}

impl ProcessDecs {
    const CAPACITY: usize = 1024;

    pub fn new(decs: Vec<ObjectReference>, count_down: Arc<AtomicUsize>) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        count_down.fetch_add(1, Ordering::SeqCst);
        Self {
            decs,
            count_down,
            new_decs: vec![],
        }
    }

    #[inline(always)]
    pub fn recursive_dec<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>, o: ObjectReference) {
        if self.new_decs.is_empty() {
            self.new_decs.reserve(Self::CAPACITY);
        }
        self.new_decs.push(o);
        if self.new_decs.len() > Self::CAPACITY {
            self.flush(worker)
        }
    }

    #[inline]
    pub fn flush<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>) {
        if !self.new_decs.is_empty() {
            let mut new_decs = vec![];
            std::mem::swap(&mut new_decs, &mut self.new_decs);
            worker.add_work(
                WorkBucketStage::Unconstrained,
                ProcessDecs::new(new_decs, self.count_down.clone()),
            );
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessDecs {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        let mut decs = vec![];
        std::mem::swap(&mut decs, &mut self.decs);
        for o in decs {
            if !immix.immix_space.in_space(o) {
                continue;
            }
            if let Ok(1) = self::dec(o) {
                debug_assert_eq!(self::count(o), 0);
                // Recursively decrease field ref counts
                EdgeIterator::<VM>::iterate(o, |edge| {
                    let o = unsafe { edge.load::<ObjectReference>() };
                    if !o.is_null() {
                        self.recursive_dec(worker, o);
                    }
                });
                if crate::flags::RC_EVACUATE_NURSERY && !crate::flags::BLOCK_ONLY {
                    self::unmark_striddle_object::<VM>(o);
                }
                #[cfg(feature = "sanity")]
                unsafe {
                    o.to_address().store(0xdeadusize);
                }
                immix
                    .immix_space
                    .possibly_dead_mature_blocks
                    .lock()
                    .insert(Block::containing::<VM>(o));
            }
        }
        self.flush(worker);

        // If all decs are finished, start sweeping blocks
        if self.count_down.fetch_sub(1, Ordering::SeqCst) == 1 {
            // Finished processing all the decrements. Start releasing blocks.
            let mut blocks = immix.immix_space.possibly_dead_mature_blocks.lock();
            if immix.last_gc_was_cycle_collection() {
                // Blocks are swept in `Release` stage.
                blocks.clear();
            } else {
                SweepBlocksAfterDecs::schedule(&mmtk.scheduler, &mut blocks);
            }
        }
    }
}

struct SweepBlocksAfterDecs {
    blocks: Vec<Block>,
}

impl SweepBlocksAfterDecs {
    fn schedule<VM: VMBinding>(scheduler: &GCWorkScheduler<VM>, blocks_set: &mut HashSet<Block>) {
        // This may happen either within a pause, or in concurrent.
        let size = blocks_set.len();
        let num_bins = scheduler.num_workers() << 1;
        let bin_cap = size / num_bins + if size % num_bins == 0 { 0 } else { 1 };
        let mut bins = (0..num_bins)
            .map(|_| Vec::with_capacity(bin_cap))
            .collect::<Vec<Vec<Block>>>();
        let mut blocks = blocks_set.iter().cloned().collect::<Vec<Block>>();
        blocks_set.clear();
        for i in 0..num_bins {
            for _ in 0..bin_cap {
                if let Some(block) = blocks.pop() {
                    bins[i].push(block);
                }
            }
        }
        debug_assert!(blocks.is_empty());
        let packets = bins
            .into_iter()
            .map::<Box<dyn GCWork<VM>>, _>(|blocks| box SweepBlocksAfterDecs { blocks })
            .collect();
        scheduler.work_buckets[WorkBucketStage::Unconstrained].bulk_add(packets);
    }
}

impl<VM: VMBinding> GCWork<VM> for SweepBlocksAfterDecs {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        for b in &self.blocks {
            b.rc_sweep_mature::<VM>(&immix.immix_space);
        }
    }
}

pub struct RCEvacuateNursery {
    slots: Vec<Address>,
    new_slots: Vec<Address>,
    roots: bool,
}

impl RCEvacuateNursery {
    const CAPACITY: usize = 1024;

    pub fn new(slots: Vec<Address>, roots: bool) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        debug_assert!(ProcessIncs::DELAYED_EVACUATION);
        Self {
            slots,
            roots,
            new_slots: vec![],
        }
    }

    #[inline(always)]
    fn scan_nursery_object<VM: VMBinding>(
        &mut self,
        worker: &mut GCWorker<VM>,
        o: ObjectReference,
    ) {
        EdgeIterator::<VM>::iterate(o, |edge| {
            self.add_new_slot(worker, edge);
        });
    }

    #[inline(always)]
    pub fn add_new_slot<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>, e: Address) {
        debug_assert!(!e.is_locked::<VM>());
        if self.new_slots.is_empty() {
            self.new_slots.reserve(Self::CAPACITY);
        }
        self.new_slots.push(e);
        if self.new_slots.len() > Self::CAPACITY {
            self.flush(worker)
        }
    }

    #[inline]
    pub fn flush<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>) {
        if !self.new_slots.is_empty() {
            let mut new_slots = vec![];
            std::mem::swap(&mut new_slots, &mut self.new_slots);
            worker.add_work(
                WorkBucketStage::RCEvacuateNursery,
                RCEvacuateNursery::new(new_slots, false),
            );
        }
    }

    #[inline(always)]
    fn forward<VM: VMBinding, CC: CopyContext<VM = VM>>(
        &mut self,
        e: Address,
        o: ObjectReference,
        immix: &Immix<VM>,
        copy_context: &mut CC,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        debug_assert!(!immix.perform_cycle_collection());
        if !immix.immix_space.in_space(o) {
            return o;
        }
        if self::count(o) != 0 {
            // Points to a mature object. Increment the object's refcount.
            let _ = self::inc(o);
            return o;
        }
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(o);
        if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            // Young object is moved to a new location.
            let new = object_forwarding::spin_and_get_forwarded_object::<VM>(o, forwarding_status);
            if new != o {
                unsafe {
                    e.store(new);
                }
            }
            let _ = self::inc(new);
            new
        } else {
            // Evacuate the young object
            let new = object_forwarding::forward_object::<VM, _>(
                o,
                AllocationSemantics::Default,
                copy_context,
            );
            unsafe {
                e.store(new);
            }
            // Update RC counts
            debug_assert_eq!(self::count(o), 0);
            let _ = self::inc(new);
            self::unmark_striddle_object::<VM>(o);
            self.scan_nursery_object::<VM>(worker, new);
            new
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCEvacuateNursery {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        if immix.perform_cycle_collection() {
            return;
        }
        let mut roots = vec![];
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        let mut slots = vec![];
        std::mem::swap(&mut slots, &mut self.slots);
        for e in slots {
            let o: ObjectReference = unsafe { e.load() };
            let o = self.forward::<_, _>(e, o, immix, copy_context, worker);
            if !self.roots {
                debug_assert!(!e.is_locked::<VM>());
                ProcessIncs::unlog_edge::<VM>(e);
            } else {
                if !o.is_null() {
                    roots.push(o);
                }
            }
        }
        if self.roots {
            if !roots.is_empty() {
                crate::plan::immix::CURR_ROOTS.lock().push(roots);
            }
        } else {
            debug_assert!(roots.is_empty());
        }
        self.flush(worker);
    }
}
