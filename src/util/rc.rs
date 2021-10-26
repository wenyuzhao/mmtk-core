use super::metadata::MetadataSpec;
use super::{metadata::side_metadata::address_to_meta_address, Address};
use crate::policy::immix::block::BlockState;
use crate::policy::largeobjectspace::RCReleaseMatureLOS;
use crate::scheduler::gc_work::EvacuateMatureObjects;
use crate::{
    plan::{
        immix::{Immix, ImmixCopyContext, Pause},
        EdgeIterator,
    },
    policy::{
        immix::{block::Block, line::Line, ImmixSpace},
        space::Space,
    },
    scheduler::{
        gc_work::ProcessEdgesBase, GCWork, GCWorkScheduler, GCWorker, ProcessEdgesWork,
        WorkBucketStage,
    },
    util::{
        cm::ImmixConcurrentTraceObjects,
        metadata::side_metadata::{self, SideMetadataSpec},
        object_forwarding, ObjectReference,
    },
    vm::*,
    AllocationSemantics, CopyContext, MMTK,
};
use atomic::Ordering;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::{
    iter::Step,
    sync::{atomic::AtomicUsize, Arc},
};

pub const LOG_REF_COUNT_BITS: usize = 1;
pub const REF_COUNT_BITS: usize = 1 << LOG_REF_COUNT_BITS;
pub const REF_COUNT_MASK: usize = (1 << REF_COUNT_BITS) - 1;
const MAX_REF_COUNT: usize = (1 << REF_COUNT_BITS) - 1;

pub const LOG_MIN_OBJECT_SIZE: usize = 4;
pub const MIN_OBJECT_SIZE: usize = 1 << LOG_MIN_OBJECT_SIZE;

pub const RC_STRADDLE_LINES: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::RC_STRADDLE_LINES;

pub const RC_TABLE: SideMetadataSpec = crate::util::metadata::side_metadata::spec_defs::RC_TABLE;

pub const RC_LOCK_BITS: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::RC_LOCK_BITS;
pub const RC_LOCK_BIT_SPEC: MetadataSpec = MetadataSpec::OnSide(RC_LOCK_BITS);

#[inline(always)]
pub fn fetch_update(
    o: ObjectReference,
    f: impl FnMut(usize) -> Option<usize>,
) -> Result<usize, usize> {
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
    let r = fetch_update(o, |x| {
        debug_assert!(x <= MAX_REF_COUNT);
        if x == MAX_REF_COUNT {
            None
        } else {
            Some(x + 1)
        }
    });
    // println!("inc {:?} {:?} -> {:?}", o, r, count(o));
    r
}

#[inline(always)]
pub fn dec(o: ObjectReference) -> Result<usize, usize> {
    let r = fetch_update(o, |x| {
        debug_assert!(x <= MAX_REF_COUNT);
        if x == 0 || x == MAX_REF_COUNT
        /* sticky */
        {
            None
        } else {
            Some(x - 1)
        }
    });
    // println!("dec {:?} {:?} -> {:?}", o, r, count(o));
    r
}

#[inline(always)]
pub fn set(o: ObjectReference, count: usize) {
    side_metadata::store_atomic(&RC_TABLE, o.to_address(), count, Ordering::SeqCst)
}

#[inline(always)]
pub fn count(o: ObjectReference) -> usize {
    side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::SeqCst)
}

pub fn rc_table_range<UInt: Sized>(b: Block) -> &'static [UInt] {
    debug_assert!({
        let log_bits_in_uint: usize = (std::mem::size_of::<UInt>() << 3).trailing_zeros() as usize;
        Block::LOG_BYTES - crate::util::rc::LOG_MIN_OBJECT_SIZE
            + crate::util::rc::LOG_REF_COUNT_BITS
            >= log_bits_in_uint
    });
    let start = address_to_meta_address(&crate::util::rc::RC_TABLE, b.start()).to_ptr::<UInt>();
    let limit = address_to_meta_address(&crate::util::rc::RC_TABLE, b.end()).to_ptr::<UInt>();
    let rc_table = unsafe { std::slice::from_raw_parts(start, limit.offset_from(start) as _) };
    rc_table
}

#[allow(unused)]
#[inline(always)]
pub fn is_dead(o: ObjectReference) -> bool {
    let v = side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::SeqCst);
    v == 0
}

#[inline(always)]
pub fn is_straddle_line(line: Line) -> bool {
    let v = side_metadata::load_atomic(&RC_STRADDLE_LINES, line.start(), Ordering::SeqCst);
    v != 0
}

#[inline(always)]
pub fn mark_straddle_object<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(!crate::flags::BLOCK_ONLY);
    // debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
    let size = VM::VMObjectModel::get_current_size(o);
    debug_assert!(size > Line::BYTES);
    let start_line = Line::forward(Line::containing::<VM>(o), 1);
    let end_line = Line::from(Line::align(o.to_address() + size));
    for line in start_line..end_line {
        side_metadata::store_atomic(&RC_STRADDLE_LINES, line.start(), 1, Ordering::SeqCst);
        self::set(unsafe { line.start().to_object_reference() }, 1);
    }
}

#[inline(always)]
pub fn unmark_straddle_object<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(!crate::flags::BLOCK_ONLY);
    // debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
    let size = VM::VMObjectModel::get_current_size(o);
    if size > Line::BYTES {
        let start_line = Line::forward(Line::containing::<VM>(o), 1);
        let end_line = Line::from(Line::align(o.to_address() + size));
        for line in start_line..end_line {
            self::set(unsafe { line.start().to_object_reference() }, 0);
            std::sync::atomic::fence(Ordering::SeqCst);
            side_metadata::store_atomic(&RC_STRADDLE_LINES, line.start(), 0, Ordering::SeqCst);
            std::sync::atomic::fence(Ordering::SeqCst);
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
    mature_evac_remset: Vec<ObjectReference>,
    /// Root edges?
    roots: bool,
    /// Execution worker
    worker: *mut GCWorker<VM>,
    immix: *const Immix<VM>,
    current_pause: Pause,
    concurrent_marking_in_progress: bool,
}

unsafe impl<VM: VMBinding> Send for ProcessIncs<VM> {}

impl<VM: VMBinding> ProcessIncs<VM> {
    const CAPACITY: usize = 1024;
    pub const DELAYED_EVACUATION: bool = true;

    #[inline(always)]
    const fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline(always)]
    const fn immix(&self) -> &Immix<VM> {
        unsafe { &*self.immix }
    }

    #[inline]
    pub fn new(incs: Vec<Address>, roots: bool) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        Self {
            incs,
            roots,
            new_incs: vec![],
            remset: vec![],
            worker: std::ptr::null_mut(),
            mature_evac_remset: vec![],
            immix: std::ptr::null(),
            current_pause: Pause::RefCount,
            concurrent_marking_in_progress: false,
        }
    }

    #[inline(always)]
    fn promote(o: ObjectReference) {
        o.log_start_address::<VM>();
        if !crate::flags::BLOCK_ONLY {
            if o.get_size::<VM>() > Line::BYTES {
                self::mark_straddle_object::<VM>(o);
            }
        }
    }

    #[inline(always)]
    fn scan_nursery_object(&mut self, o: ObjectReference) {
        EdgeIterator::<VM>::iterate(o, |edge| {
            debug_assert!(edge.is_logged::<VM>(), "{:?}.{:?} is unlogged", o, edge);
            // println!(" - rec inc {:?}.{:?} -> {:?}", o, edge, unsafe { edge.load::<ObjectReference>() });
            self.recursive_inc(edge);
        });
    }

    #[inline(always)]
    fn recursive_inc(&mut self, e: Address) {
        self.new_incs.push(e);
        if self.new_incs.len() > Self::CAPACITY {
            self.flush()
        }
    }

    #[inline(always)]
    fn add_remset(&mut self, e: Address) {
        self.remset.push(e);
        if self.remset.len() > Self::CAPACITY {
            self.flush()
        }
    }

    #[inline(always)]
    fn process_inc(
        &mut self,
        _e: Address,
        o: ObjectReference,
        immix: &Immix<VM>,
    ) -> ObjectReference {
        let r = self::inc(o);
        // println!(" - inc e={:?} {:?} rc: {:?} -> {:?}", _e, o, r, count(o));
        if let Ok(0) = r {
            if immix.immix_space.in_space(o) {
                Self::promote(o);
            }
            if self.concurrent_marking_in_progress || self.current_pause == Pause::FinalMark {
                immix.mark(o);
            }
            self.scan_nursery_object(o);
            // debug_assert!(
            //     !(Self::DELAYED_EVACUATION && crate::flags::RC_EVACUATE_NURSERY)
            //         || immix.los().in_space(o)
            // );
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
            let _ = self::inc(new);
            new
        } else {
            if self::count(o) == 0 {
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
                let _ = self::inc(new);
                Self::promote(new);
                self.scan_nursery_object(new);
                new
            } else {
                let _ = self::inc(o);
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
        if !self.mature_evac_remset.is_empty() {
            let mut remset = vec![];
            mem::swap(&mut remset, &mut self.mature_evac_remset);
            let w = EvacuateMatureObjects::new(remset);
            if self.concurrent_marking_in_progress {
                self.immix()
                    .immix_space
                    .mature_evac_remsets
                    .lock()
                    .push(box w)
            } else {
                self.worker().add_work(WorkBucketStage::RCEvacuateMature, w);
            }
            unreachable!();
        }
    }

    /// Return `None` if the increment of the edge should be delayed
    #[inline(always)]
    fn load_mature_object_and_unlog_edge(
        &mut self,
        e: Address,
        immix: &Immix<VM>,
    ) -> Option<ObjectReference> {
        debug_assert!(!crate::flags::EAGER_INCREMENTS);
        let o = unsafe { e.load() };
        let in_immix_space = immix.immix_space.in_space(o);
        // Delay the increment if this object points to a young object
        if Self::DELAYED_EVACUATION && crate::flags::RC_EVACUATE_NURSERY {
            if in_immix_space && self::count(o) == 0 {
                self.add_remset(e);
                return None;
            }
        }
        // unlog edge
        if !self.roots {
            debug_assert!(e.is_logged::<VM>(), "{:?}", e);
            e.unlog::<VM>();
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
        self.immix = immix;
        self.current_pause = immix.current_pause().unwrap();
        self.concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        let mut roots = vec![];
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        let mut incs = vec![];
        std::mem::swap(&mut incs, &mut self.incs);
        for e in incs {
            // println!(" - inc e {:?}", e);
            let o = match self.load_mature_object_and_unlog_edge(e, immix) {
                Some(o) => o,
                _ => continue,
            };
            if o.is_null() {
                continue;
            }
            debug_assert_ne!(unsafe { o.to_address().load::<usize>() }, 0xdeadusize);
            let o = if !crate::flags::RC_EVACUATE_NURSERY || Self::DELAYED_EVACUATION {
                self.process_inc(e, o, immix)
            } else {
                self.process_inc_and_evacuate(e, o, copy_context)
            };
            if self.roots {
                roots.push(o);
            }
        }
        if self.roots {
            if !roots.is_empty() {
                if crate::flags::CONCURRENT_MARKING && self.current_pause == Pause::InitialMark {
                    worker
                        .scheduler()
                        .postpone(ImmixConcurrentTraceObjects::<VM>::new(roots.clone(), mmtk));
                }
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
    const fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline]
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

    #[inline]
    fn process_dead_object(&mut self, o: ObjectReference, immix: &Immix<VM>) {
        immix.mark(o);
        // println!(" - dead {:?}", o);
        // debug_assert_eq!(self::count(o), 0);
        // Recursively decrease field ref counts
        EdgeIterator::<VM>::iterate(o, |edge| {
            let x = unsafe { edge.load::<ObjectReference>() };
            if !x.is_null() {
                self.recursive_dec(x);
            }
        });
        let in_ix_space = immix.immix_space.in_space(o);
        if !crate::flags::BLOCK_ONLY && in_ix_space {
            self::unmark_straddle_object::<VM>(o);
        }
        if in_ix_space {
            o.clear_start_address_log::<VM>();
        }
        #[cfg(feature = "sanity")]
        unsafe {
            o.to_address().store(0xdeadusize);
        }
        if in_ix_space {
            immix
                .immix_space
                .possibly_dead_mature_blocks
                .lock()
                .insert(Block::containing::<VM>(o));
        } else {
            immix.los().rc_free(o);
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
            if o.is_null() {
                continue;
            }
            let o = if crate::flags::REF_COUNT
                && crate::flags::RC_MATURE_EVACUATION
                && object_forwarding::is_forwarded::<VM>(o)
            {
                object_forwarding::read_forwarding_pointer::<VM>(o)
            } else {
                o
            };
            let mut dead = false;
            let _ = self::fetch_update(o, |c| {
                if c == 1 {
                    if !dead {
                        dead = true;
                        self.process_dead_object(o, immix);
                    }
                }
                debug_assert!(c <= MAX_REF_COUNT);
                if c == 0 || c == MAX_REF_COUNT {
                    None /* sticky */
                } else {
                    Some(c - 1)
                }
            });
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
        while let Some(x) = immix_space.last_mutator_recycled_blocks.pop() {
            x.set_state(BlockState::Nursery);
        }
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
        scheduler.work_buckets[WorkBucketStage::Unconstrained].add(RCReleaseMatureLOS);
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
    mature_evac_remset: Vec<ObjectReference>,
    immix: *const Immix<VM>,
    current_pause: Pause,
    concurrent_marking_in_progress: bool,
}

unsafe impl<VM: VMBinding> Send for RCEvacuateNursery<VM> {}

impl<VM: VMBinding> RCEvacuateNursery<VM> {
    const CAPACITY: usize = 1024;

    #[inline(always)]
    const fn worker(&mut self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline(always)]
    const fn immix(&self) -> &Immix<VM> {
        unsafe { &*self.immix }
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
            mature_evac_remset: vec![],
            immix: std::ptr::null(),
            current_pause: Pause::RefCount,
            concurrent_marking_in_progress: false,
        }
    }

    #[inline(always)]
    fn scan_nursery_object(&mut self, o: ObjectReference) {
        let check_mature_evac_remset = crate::flags::RC_MATURE_EVACUATION
            && (self.concurrent_marking_in_progress
                || self.current_pause == Pause::FinalMark
                || self.current_pause == Pause::FullTraceFast);
        let mut should_add_to_mature_evac_remset = false;
        EdgeIterator::<VM>::iterate(o, |edge| {
            if crate::flags::RC_MATURE_EVACUATION
                && check_mature_evac_remset
                && !should_add_to_mature_evac_remset
            {
                if !self.immix().in_defrag(o) && self.immix().in_defrag(unsafe { edge.load() }) {
                    should_add_to_mature_evac_remset = true;
                }
            }
            self.add_new_slot(edge);
        });
        if should_add_to_mature_evac_remset {
            self.mature_evac_remset.push(o);
            if self.mature_evac_remset.len() >= Self::CAPACITY {
                self.flush();
            }
        }
    }

    #[inline(always)]
    pub fn add_new_slot(&mut self, e: Address) {
        debug_assert!(!e.is_locked::<VM>());
        self.new_slots.push(e);
        if self.new_slots.len() > Self::CAPACITY {
            self.flush()
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        if !self.new_slots.is_empty() {
            let mut new_slots = vec![];
            mem::swap(&mut new_slots, &mut self.new_slots);
            self.worker().add_work(
                WorkBucketStage::RCEvacuateNursery,
                RCEvacuateNursery::new(new_slots, false),
            );
        }
        if !self.mature_evac_remset.is_empty() {
            let mut remset = vec![];
            mem::swap(&mut remset, &mut self.mature_evac_remset);
            let w = EvacuateMatureObjects::new(remset);
            if self.concurrent_marking_in_progress {
                self.immix()
                    .immix_space
                    .mature_evac_remsets
                    .lock()
                    .push(box w);
            } else {
                if self.current_pause == Pause::FinalMark
                    || self.current_pause == Pause::FullTraceFast
                {
                    self.worker().add_work(WorkBucketStage::RCEvacuateMature, w);
                }
            }
        }
    }

    #[inline(always)]
    fn promote<const IMMIX_OBJECT: bool>(&mut self, new: ObjectReference) {
        // Don't mark copied objects in initial mark pause. The concurrent marker will do it (and can also resursively mark the old objects).
        if self.concurrent_marking_in_progress
            || self.current_pause == Pause::FinalMark
            || self.current_pause == Pause::FullTraceFast
        {
            self.immix().mark(new);
        }
        if IMMIX_OBJECT {
            ProcessIncs::<VM>::promote(new);
        }
        self.scan_nursery_object(new);
    }

    #[inline(always)]
    fn forward(
        &mut self,
        e: Address,
        o: ObjectReference,
        copy_context: &mut impl CopyContext<VM = VM>,
    ) -> ObjectReference {
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        if o.is_null() {
            return o;
        }
        if self::count(o) != 0
            || self.immix().los().in_space(o)
            || (crate::flags::RC_DONT_EVACUATE_NURSERY_IN_RECYCLED_LINES
                && self.immix().immix_space.in_space(o)
                && Block::containing::<VM>(o).get_state() == BlockState::Reusing)
        {
            if let Ok(0) = self::inc(o) {
                if self.immix().immix_space.in_space(o) {
                    self.promote::<true>(o)
                } else {
                    self.promote::<false>(o)
                }
            }
            return o;
        }
        if object_forwarding::is_forwarded::<VM>(o) {
            let new = object_forwarding::read_forwarding_pointer::<VM>(o);
            unsafe {
                e.store(new);
            }
            let _ = self::inc(new);
            return new;
        }
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(o);
        let new = if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            // Young object is moved to a new location.
            object_forwarding::spin_and_get_forwarded_object::<VM>(o, forwarding_status)
        } else {
            // Evacuate the young object
            let new = object_forwarding::forward_object::<VM, _>(
                o,
                AllocationSemantics::Default,
                copy_context,
            );
            self.promote::<true>(new);
            new
        };
        let _ = self::inc(new);
        unsafe {
            e.store(new);
        }
        new
    }
}

impl<VM: VMBinding> GCWork<VM> for RCEvacuateNursery<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        self.immix = immix;
        self.current_pause = immix.current_pause().unwrap();
        self.concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        let mut roots = if self.roots {
            Vec::with_capacity(self.slots.len())
        } else {
            vec![]
        };
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        let mut slots = vec![];
        std::mem::swap(&mut slots, &mut self.slots);
        for e in slots {
            let o: ObjectReference = unsafe { e.load() };
            let o = self.forward(e, o, copy_context);
            if !self.roots {
                e.unlog::<VM>();
            } else {
                if !o.is_null() {
                    roots.push(o);
                }
            }
        }
        if self.roots {
            if !roots.is_empty() {
                if crate::flags::CONCURRENT_MARKING && self.current_pause == Pause::InitialMark {
                    worker
                        .scheduler()
                        .postpone(ImmixConcurrentTraceObjects::<VM>::new(roots.clone(), mmtk));
                }
                crate::plan::immix::CURR_ROOTS.lock().push(roots);
            }
        } else {
            debug_assert!(roots.is_empty());
        }
        self.flush();
    }
}

pub struct RCImmixCollectRootEdges<VM: VMBinding> {
    base: ProcessEdgesBase<Self>,
}

impl<VM: VMBinding> ProcessEdgesWork for RCImmixCollectRootEdges<VM> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = false;
    const RC_ROOTS: bool = true;
    const CAPACITY: usize = 4096;
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;

    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        debug_assert!(roots);
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        Self { base }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    #[inline]
    fn process_edges(&mut self) {
        if !self.edges.is_empty() {
            let bucket = WorkBucketStage::rc_process_incs_stage();
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.edges);
            self.mmtk().scheduler.work_buckets[bucket].add(ProcessIncs::<VM>::new(roots, true));
        }
    }
}

impl<VM: VMBinding> Deref for RCImmixCollectRootEdges<VM> {
    type Target = ProcessEdgesBase<Self>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for RCImmixCollectRootEdges<VM> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
