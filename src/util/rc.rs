use std::{
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
        immix::{block::Block, chunk::ChunkMap, line::Line, ImmixSpace},
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
    metadata::{compare_exchange_metadata, store_metadata, RC_LOCK_BIT_SPEC},
    Address,
};

pub const LOG_REF_COUNT_BITS: usize = 2;
const MAX_REF_COUNT: usize = (1 << (1 << LOG_REF_COUNT_BITS)) - 2;
pub const MARKER: usize = MAX_REF_COUNT + 1;

pub const LOG_MIN_OBJECT_SIZE: usize = 3;
pub const MIN_OBJECT_SIZE: usize = 1 << LOG_MIN_OBJECT_SIZE;

pub const RC_TABLE: SideMetadataSpec = SideMetadataSpec {
    is_global: false,
    offset: SideMetadataOffset::layout_after(&ChunkMap::ALLOC_TABLE),
    log_num_of_bits: LOG_REF_COUNT_BITS,
    log_min_obj_size: LOG_MIN_OBJECT_SIZE as _,
};

#[inline(always)]
pub fn fetch_update(
    o: ObjectReference,
    f: impl FnMut(usize) -> Option<usize>,
) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    let r = side_metadata::fetch_update(
        &RC_TABLE,
        o.to_address(),
        Ordering::SeqCst,
        Ordering::SeqCst,
        f,
    );
    // println!("fetch_update {:?} {:?} -> {:?}", o, r, count(o));
    r
}

#[inline(always)]
pub fn inc(o: ObjectReference) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    let r = side_metadata::fetch_update(
        &RC_TABLE,
        o.to_address(),
        Ordering::SeqCst,
        Ordering::SeqCst,
        |x| {
            debug_assert!(x <= MAX_REF_COUNT);
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
            debug_assert!(x <= MAX_REF_COUNT);
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
    // debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
    let size = VM::VMObjectModel::get_current_size(o);
    debug_assert!(size > Line::BYTES);
    let start_line = Line::forward(Line::containing::<VM>(o), 1);
    let end_line = Line::from(Line::align(o.to_address() + size));
    for line in start_line..end_line {
        self::set(unsafe { line.start().to_object_reference() }, MARKER);
    }
}

#[inline(always)]
pub fn unmark_striddle_object<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(!crate::flags::BLOCK_ONLY);
    // debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
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

pub struct ProcessIncs<VM: VMBinding> {
    /// Increments to process
    incs: Vec<Address>,
    /// Recursively generated new increments
    new_incs: Vec<Address>,
    /// Delayed nursery increments
    remset: Vec<Address>,
    /// Root edges?
    roots: bool,
    /// Execution worker
    worker: *mut GCWorker<VM>,
}

unsafe impl<VM: VMBinding> Send for ProcessIncs<VM> {}

impl<VM: VMBinding> ProcessIncs<VM> {
    const CAPACITY: usize = 1024;
    pub const DELAYED_EVACUATION: bool = true;

    #[inline(always)]
    fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn new(incs: Vec<Address>, roots: bool) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        Self {
            incs,
            roots,
            new_incs: vec![],
            remset: vec![],
            worker: std::ptr::null_mut(),
        }
    }

    /// Spin and lock edge
    #[inline(always)]
    fn lock_edge(&self, edge: Address) {
        loop {
            std::hint::spin_loop();
            if edge.is_locked::<VM>() {
                continue; // Spin
            }
            if compare_exchange_metadata::<VM>(
                &RC_LOCK_BIT_SPEC,
                unsafe { edge.to_object_reference() },
                UNLOCKED_VALUE,
                LOCKED_VALUE,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                return;
            }
        }
    }

    #[inline(always)]
    fn unlock_edge(edge: Address) {
        store_metadata::<VM>(
            &RC_LOCK_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            UNLOCKED_VALUE,
            None,
            Some(Ordering::SeqCst),
        );
    }

    #[inline(always)]
    fn unlog_edge(edge: Address) {
        store_metadata::<VM>(
            &VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
            unsafe { edge.to_object_reference() },
            UNLOGGED_VALUE,
            None,
            Some(Ordering::SeqCst),
        );
    }

    #[inline(always)]
    fn scan_nursery_object(&mut self, o: ObjectReference) {
        EdgeIterator::<VM>::iterate(o, |edge| {
            debug_assert!(edge.is_logged::<VM>(), "{:?}.{:?} is unlogged", o, edge);
            self.recursive_inc(edge);
        });
    }

    #[inline(always)]
    fn recursive_inc(&mut self, e: Address) {
        if self.new_incs.is_empty() {
            self.new_incs.reserve(Self::CAPACITY);
        }
        self.new_incs.push(e);
        if self.new_incs.len() > Self::CAPACITY {
            self.flush()
        }
    }

    #[inline(always)]
    fn add_remset(&mut self, e: Address) {
        if self.remset.is_empty() {
            self.remset.reserve(Self::CAPACITY);
        }
        self.remset.push(e);
        if self.remset.len() > Self::CAPACITY {
            self.flush()
        }
    }

    #[inline(always)]
    fn process_inc(&mut self, o: ObjectReference) -> ObjectReference {
        if let Ok(0) = self::inc(o) {
            self.scan_nursery_object(o);
            debug_assert!(!(Self::DELAYED_EVACUATION && crate::flags::RC_EVACUATE_NURSERY));
            debug_assert!(!crate::flags::RC_EVACUATE_NURSERY);
            if !crate::flags::BLOCK_ONLY {
                // Young object is not evacuated. Mark as striddle in place if required.
                // FIXME: Fix this when allocating young objects to recyclable lines or doing young evacuation in a tracing pause.
                if o.get_size::<VM>() > Line::BYTES {
                    self::mark_striddle_object::<VM>(o);
                }
            }
        }
        o
    }

    #[inline(always)]
    fn process_inc_and_evacuate(
        &mut self,
        e: Address,
        o: ObjectReference,
        copy_context: &mut impl CopyContext<VM = VM>,
    ) -> ObjectReference {
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        debug_assert!(!Self::DELAYED_EVACUATION);
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
                if !crate::flags::BLOCK_ONLY {
                    self::unmark_striddle_object::<VM>(o);
                }
                self.scan_nursery_object(new);
                new
            } else {
                // Object is not moved.
                object_forwarding::clear_forwarding_bits::<VM>(o);
                o
            }
        }
    }

    #[inline]
    fn flush(&mut self) {
        if !self.new_incs.is_empty() {
            let mut new_incs = vec![];
            std::mem::swap(&mut new_incs, &mut self.new_incs);
            self.worker().add_work(
                WorkBucketStage::rc_process_incs_stage(),
                ProcessIncs::<VM>::new(new_incs, false),
            );
        }
        if !self.remset.is_empty() {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.remset);
            self.worker().add_work(
                WorkBucketStage::RCEvacuateNursery,
                RCEvacuateNursery::new(remset, self.roots),
            );
        }
    }

    /// Return `None` if the increment of the edge should be delayed
    #[inline(always)]
    fn load_mature_object_and_unlog_edge(
        &mut self,
        e: Address,
        immix: &Immix<VM>,
    ) -> Option<ObjectReference> {
        // Delay the increment if this object points to a young object
        if Self::DELAYED_EVACUATION && crate::flags::RC_EVACUATE_NURSERY {
            let o = unsafe { e.load() };
            if immix.immix_space.in_space(o) && self::count(o) == 0 {
                self.add_remset(e);
                return None;
            }
        }
        // Load mature object and unlog edge
        if !self.roots {
            debug_assert!(e.is_logged::<VM>(), "{:?}", e);
            self.lock_edge(e);
            debug_assert!(e.is_logged::<VM>(), "{:?}", e);
            Self::unlog_edge(e);
        }
        let o: ObjectReference = unsafe { e.load() };
        if !self.roots {
            Self::unlock_edge(e);
        }
        Some(o)
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessIncs<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        let mut roots = vec![];
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        let mut incs = vec![];
        std::mem::swap(&mut incs, &mut self.incs);
        for e in incs {
            let o = match self.load_mature_object_and_unlog_edge(e, immix) {
                Some(o) => o,
                _ => continue,
            };
            if !immix.immix_space.in_space(o) {
                continue;
            }
            debug_assert_ne!(unsafe { o.to_address().load::<usize>() }, 0xdeadusize);
            let o = if !crate::flags::RC_EVACUATE_NURSERY || Self::DELAYED_EVACUATION {
                self.process_inc(o)
            } else {
                self.process_inc_and_evacuate(e, o, copy_context)
            };
            if self.roots {
                if !crate::flags::RC_EVACUATE_NURSERY {
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
        self.flush();
    }
}

pub struct ProcessDecs<VM: VMBinding> {
    /// Decrements to process
    decs: Vec<ObjectReference>,
    /// Counter for the number of remaining `ProcessDecs` packages
    count_down: Arc<AtomicUsize>,
    /// Recursively generated new decrements
    new_decs: Vec<ObjectReference>,
    /// Execution worker
    worker: *mut GCWorker<VM>,
}

unsafe impl<VM: VMBinding> Send for ProcessDecs<VM> {}

impl<VM: VMBinding> ProcessDecs<VM> {
    const CAPACITY: usize = 1024;

    #[inline(always)]
    fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn new(decs: Vec<ObjectReference>, count_down: Arc<AtomicUsize>) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        count_down.fetch_add(1, Ordering::SeqCst);
        Self {
            decs,
            count_down,
            new_decs: vec![],
            worker: std::ptr::null_mut(),
        }
    }

    #[inline(always)]
    pub fn recursive_dec(&mut self, o: ObjectReference) {
        if self.new_decs.is_empty() {
            self.new_decs.reserve(Self::CAPACITY);
        }
        self.new_decs.push(o);
        if self.new_decs.len() > Self::CAPACITY {
            self.flush()
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        if !self.new_decs.is_empty() {
            let mut new_decs = vec![];
            std::mem::swap(&mut new_decs, &mut self.new_decs);
            self.worker().add_work(
                WorkBucketStage::Unconstrained,
                ProcessDecs::<VM>::new(new_decs, self.count_down.clone()),
            );
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessDecs<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
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
                    let x = unsafe { edge.load::<ObjectReference>() };
                    if !x.is_null() && immix.immix_space.in_space(x) {
                        self.recursive_dec(x);
                    }
                });
                if !crate::flags::BLOCK_ONLY {
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
        self.flush();

        // If all decs are finished, start sweeping blocks
        if self.count_down.fetch_sub(1, Ordering::SeqCst) == 1 {
            SweepBlocksAfterDecs::schedule(&mmtk.scheduler, &immix.immix_space);
        }
    }
}

pub struct SweepBlocksAfterDecs {
    blocks: Vec<Block>,
}

impl SweepBlocksAfterDecs {
    pub fn schedule<VM: VMBinding>(scheduler: &GCWorkScheduler<VM>, immix_space: &ImmixSpace<VM>) {
        // This may happen either within a pause, or in concurrent.
        let mut blocks_set = immix_space.possibly_dead_mature_blocks.lock();
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

pub struct RCEvacuateNursery<VM: VMBinding> {
    /// Edges to forward
    slots: Vec<Address>,
    /// Recursively generated edges
    new_slots: Vec<Address>,
    /// Root edges?
    roots: bool,
    /// Execution worker
    worker: *mut GCWorker<VM>,
}

unsafe impl<VM: VMBinding> Send for RCEvacuateNursery<VM> {}

impl<VM: VMBinding> RCEvacuateNursery<VM> {
    const CAPACITY: usize = 1024;

    #[inline(always)]
    fn worker(&mut self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn new(slots: Vec<Address>, roots: bool) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        debug_assert!(ProcessIncs::<VM>::DELAYED_EVACUATION);
        Self {
            slots,
            roots,
            new_slots: vec![],
            worker: std::ptr::null_mut(),
        }
    }

    #[inline(always)]
    fn scan_nursery_object(&mut self, o: ObjectReference) {
        EdgeIterator::<VM>::iterate(o, |edge| {
            self.add_new_slot(edge);
        });
    }

    #[inline(always)]
    pub fn add_new_slot(&mut self, e: Address) {
        debug_assert!(!e.is_locked::<VM>());
        if self.new_slots.is_empty() {
            self.new_slots.reserve(Self::CAPACITY);
        }
        self.new_slots.push(e);
        if self.new_slots.len() > Self::CAPACITY {
            self.flush()
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        if !self.new_slots.is_empty() {
            let mut new_slots = vec![];
            std::mem::swap(&mut new_slots, &mut self.new_slots);
            self.worker().add_work(
                WorkBucketStage::RCEvacuateNursery,
                RCEvacuateNursery::new(new_slots, false),
            );
        }
    }

    #[inline(always)]
    fn forward(
        &mut self,
        e: Address,
        o: ObjectReference,
        immix: &Immix<VM>,
        copy_context: &mut impl CopyContext<VM = VM>,
    ) -> ObjectReference {
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        if !immix.immix_space.in_space(o) {
            return o;
        }
        if self::count(o) != 0 {
            // Points to a mature object. Remain in place and increment the object's refcount.
            let r = self::inc(o);
            debug_assert_ne!(r, Ok(0));
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
            if !crate::flags::BLOCK_ONLY {
                self::unmark_striddle_object::<VM>(o);
            }
            self.scan_nursery_object(new);
            new
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCEvacuateNursery<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        let mut roots = vec![];
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        let mut slots = vec![];
        std::mem::swap(&mut slots, &mut self.slots);
        for e in slots {
            let o: ObjectReference = unsafe { e.load() };
            let o = self.forward(e, o, immix, copy_context);
            if !self.roots {
                debug_assert!(!e.is_locked::<VM>());
                ProcessIncs::<VM>::unlog_edge(e);
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
        self.flush();
    }
}
