use super::metadata::MetadataSpec;
use super::{metadata::side_metadata::address_to_meta_address, Address};
use crate::plan::immix::gc_work::{ImmixProcessEdges, TraceKind};
use crate::policy::immix::block::BlockState;
use crate::policy::immix::ScanObjectsAndMarkLines;
use crate::{
    plan::{
        immix::{Immix, ImmixCopyContext, Pause},
        EdgeIterator,
    },
    policy::{
        immix::{block::Block, line::Line},
        space::Space,
    },
    scheduler::{gc_work::ProcessEdgesBase, GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage},
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
        Ordering::Relaxed,
        Ordering::Relaxed,
        f,
    );
    // println!("fetch_update {:?} {:?} -> {:?}", o, r, count(o));
    r
}

#[inline(always)]
pub fn rc_stick(o: ObjectReference) -> bool {
    self::count(o) == MAX_REF_COUNT
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
    side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::Relaxed)
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
    debug_assert!(!crate::args::BLOCK_ONLY);
    // debug_assert!(crate::args::RC_NURSERY_EVACUATION);
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
    debug_assert!(!crate::args::BLOCK_ONLY);
    // debug_assert!(crate::args::RC_NURSERY_EVACUATION);
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
    debug_assert!(crate::args::REF_COUNT);
    let size = VM::VMObjectModel::get_current_size(o);
    for i in (0..size).step_by(MIN_OBJECT_SIZE) {
        let a = o.to_address() + i;
        assert_eq!(0, self::count(unsafe { a.to_object_reference() }));
    }
}

#[inline(always)]
fn promote<VM: VMBinding>(o: ObjectReference) {
    o.log_start_address::<VM>();
    if !crate::args::BLOCK_ONLY {
        if o.get_size::<VM>() > Line::BYTES {
            self::mark_straddle_object::<VM>(o);
        }
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
    scan_objects: Vec<ObjectReference>,
    /// Root edges?
    roots: bool,
    /// Execution worker
    worker: *mut GCWorker<VM>,
    immix: *const Immix<VM>,
    current_pause: Pause,
    concurrent_marking_in_progress: bool,
    nursery: bool,
}

unsafe impl<VM: VMBinding> Send for ProcessIncs<VM> {}

impl<VM: VMBinding> ProcessIncs<VM> {
    const CAPACITY: usize = 512;
    pub const DELAYED_EVACUATION: bool = cfg!(feature = "ix_delayed_nursery_evacuation");

    #[inline(always)]
    const fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline(always)]
    const fn immix(&self) -> &'static Immix<VM> {
        unsafe { &*self.immix }
    }

    #[inline]
    pub fn new(incs: Vec<Address>, roots: bool) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        Self {
            incs,
            roots,
            new_incs: vec![],
            remset: vec![],
            worker: std::ptr::null_mut(),
            mature_evac_remset: vec![],
            scan_objects: vec![],
            immix: std::ptr::null(),
            current_pause: Pause::RefCount,
            concurrent_marking_in_progress: false,
            nursery: false,
        }
    }

    #[inline]
    pub fn new_nursery(incs: Vec<Address>, roots: bool) -> Self {
        let mut w = Self::new(incs, roots);
        w.nursery = true;
        w
    }

    #[inline]
    pub fn new_x(incs: Vec<Address>, roots: bool, nursery: bool) -> Self {
        let mut w = Self::new(incs, roots);
        w.nursery = nursery;
        w
    }

    #[inline(always)]
    fn promote(&mut self, o: ObjectReference, copy: bool, los: bool, in_place: bool) {
        crate::stat(|s| {
            s.promoted_objects += 1;
            s.promoted_volume += o.get_size::<VM>();
            if self.immix().los().in_space(o) {
                s.promoted_los_objects += 1;
                s.promoted_los_volume += o.get_size::<VM>();
            }
        });
        if !los {
            self::promote::<VM>(o);
        }
        // Don't mark copied objects in initial mark pause. The concurrent marker will do it (and can also resursively mark the old objects).
        if self.concurrent_marking_in_progress || self.current_pause == Pause::FinalMark {
            self.immix().mark2(o, los);
        } else if self.current_pause == Pause::FullTraceFast {
            // Create object scanning packets
            if copy {
                if self.immix().mark2(o, los) {
                    self.scan_objects.push(o)
                }
            }
        }
        self.scan_nursery_object(o, los, in_place);
    }

    #[inline(always)]
    fn scan_nursery_object(&mut self, o: ObjectReference, los: bool, in_place_promotion: bool) {
        let check_mature_evac_remset = crate::args::RC_MATURE_EVACUATION
            && (self.concurrent_marking_in_progress
                || self.current_pause == Pause::FinalMark
                || self.current_pause == Pause::FullTraceFast);
        let mut should_add_to_mature_evac_remset = false;
        if los {
            let start = side_metadata::address_to_meta_address(
                VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
                o.to_address() + 16usize,
            )
            .to_mut_ptr::<u8>();
            let limit = side_metadata::address_to_meta_address(
                VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
                (o.to_address() + o.get_size::<VM>()).align_up(64),
            )
            .to_mut_ptr::<u8>();
            unsafe {
                let count = limit.offset_from(start) as usize;
                std::ptr::write_bytes(start, 0xffu8, count);
            }
        }
        let x = in_place_promotion && !los;
        EdgeIterator::<VM>::iterate(o, |edge| {
            let o = unsafe { edge.load() };
            if crate::args::RC_MATURE_EVACUATION
                && check_mature_evac_remset
                && !should_add_to_mature_evac_remset
            {
                if !self.immix().in_defrag(o) && self.immix().in_defrag(unsafe { edge.load() }) {
                    should_add_to_mature_evac_remset = true;
                }
            }
            if x {
                edge.unlog::<VM>();
            }
            if !o.is_null() && !self::rc_stick(o) {
                self.recursive_inc(edge);
            }
        });
        if should_add_to_mature_evac_remset {
            self.mature_evac_remset.push(o);
            if self.mature_evac_remset.len() >= Self::CAPACITY {
                self.flush();
            }
        }
    }

    #[inline(always)]
    fn recursive_inc(&mut self, e: Address) {
        self.new_incs.push(e);
        if self.new_incs.len() >= Self::CAPACITY {
            self.flush()
        }
    }

    #[inline(always)]
    fn add_remset(&mut self, e: Address) {
        self.remset.push(e);
    }

    #[inline(always)]
    fn process_inc(&mut self, _e: Address, o: ObjectReference) -> ObjectReference {
        let r = self::inc(o);
        // println!(" - inc e={:?} {:?} rc: {:?} -> {:?}", _e, o, r, count(o));
        if let Ok(0) = r {
            self.promote(o, false, self.immix().los().in_space(o), true);
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
        crate::stat(|s| {
            s.inc_objects += 1;
            s.inc_volume += o.get_size::<VM>();
        });
        debug_assert!(crate::args::RC_NURSERY_EVACUATION);
        debug_assert!(!Self::DELAYED_EVACUATION);
        let los = self.immix().los().in_space(o);
        if self::count(o) != 0
            || los
            || (crate::args::RC_DONT_EVACUATE_NURSERY_IN_RECYCLED_LINES
                && Block::containing::<VM>(o).get_state() == BlockState::Reusing)
        {
            if let Ok(0) = self::inc(o) {
                self.promote(o, false, los, true);
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
                let _ = self::inc(new);
                self.promote(new, true, false, false);
                new
            } else {
                // Object is not moved.
                object_forwarding::clear_forwarding_bits::<VM>(o);
                let _ = self::inc(o);
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
                WorkBucketStage::Unconstrained,
                ProcessIncs::<VM>::new_nursery(new_incs, false),
            );
        }
        #[cfg(feature = "ix_delayed_nursery_evacuation")]
        if !self.remset.is_empty() {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.remset);
            self.worker().add_work(
                WorkBucketStage::RCEvacuateNursery,
                RCEvacuateNursery::new(remset, self.roots),
            );
        }
        if !self.scan_objects.is_empty() {
            let mut scan_objects = vec![];
            mem::swap(&mut scan_objects, &mut self.scan_objects);
            let w = ScanObjectsAndMarkLines::<ImmixProcessEdges<VM, { TraceKind::Fast }>>::new(
                scan_objects,
                false,
                Some(self.immix()),
                &self.immix().immix_space,
            );
            self.worker().add_work(WorkBucketStage::Closure, w);
        }
    }

    /// Return `None` if the increment of the edge should be delayed
    #[inline(always)]
    fn load_mature_object_and_unlog_edge(
        &mut self,
        root: bool,
        nursery: bool,
        e: Address,
    ) -> Option<ObjectReference> {
        debug_assert!(!crate::args::EAGER_INCREMENTS);
        let o = unsafe { e.load::<ObjectReference>() };
        // Delay the increment if this object points to a young object
        if Self::DELAYED_EVACUATION && crate::args::RC_NURSERY_EVACUATION {
            let in_immix_space = self.immix().immix_space.in_space(o);
            if in_immix_space && self::count(o) == 0 {
                self.add_remset(e);
                return None;
            }
        }
        // unlog edge
        if !root && !nursery {
            e.unlog::<VM>();
        }
        Some(o)
    }

    fn process_incs(
        &mut self,
        root_slots: bool,
        nursery: bool,
        mut incs: Vec<Address>,
        copy_context: &mut impl CopyContext<VM = VM>,
    ) -> Option<Vec<ObjectReference>> {
        let roots = incs.as_mut_ptr() as *mut ObjectReference;
        let mut num_roots = 0usize;
        for e in &mut incs {
            // println!(" - inc e {:?}", e);
            let o = match self.load_mature_object_and_unlog_edge(root_slots, nursery, *e) {
                Some(o) => o,
                _ => continue,
            };
            if o.is_null() {
                continue;
            }
            debug_assert_ne!(unsafe { o.to_address().load::<usize>() }, 0xdeadusize);
            let o = if !crate::args::RC_NURSERY_EVACUATION || Self::DELAYED_EVACUATION {
                self.process_inc(*e, o)
            } else {
                self.process_inc_and_evacuate(*e, o, copy_context)
            };
            if self.roots {
                unsafe {
                    roots.add(num_roots).write(o);
                }
                num_roots += 1;
            }
        }
        if root_slots && num_roots != 0 {
            let cap = incs.capacity();
            std::mem::forget(incs);
            let roots = unsafe { Vec::<ObjectReference>::from_raw_parts(roots, num_roots, cap) };
            Some(roots)
        } else {
            None
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessIncs<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        // println!("[{}] ProcessIncs {}", worker.ordinal, self.incs.len());
        self.worker = worker;
        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        self.immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        self.current_pause = self.immix().current_pause().unwrap();
        self.concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        // Process main buffer
        let mut incs = vec![];
        std::mem::swap(&mut incs, &mut self.incs);
        let roots = self.process_incs(self.roots, self.nursery, incs, copy_context);
        if let Some(roots) = roots {
            if crate::args::CONCURRENT_MARKING && self.current_pause == Pause::InitialMark {
                worker
                    .scheduler()
                    .postpone(ImmixConcurrentTraceObjects::<VM>::new(roots.clone(), mmtk));
            }
            unsafe {
                crate::plan::immix::CURR_ROOTS.push(roots);
            }
        }
        // Process recursively generated buffer
        if !self.new_incs.is_empty() {
            if !self.scan_objects.is_empty() {
                let mut scan_objects = vec![];
                mem::swap(&mut scan_objects, &mut self.scan_objects);
                let w = ScanObjectsAndMarkLines::<ImmixProcessEdges<VM, { TraceKind::Fast }>>::new(
                    scan_objects,
                    false,
                    Some(self.immix()),
                    &self.immix().immix_space,
                );
                self.worker().add_work(WorkBucketStage::Closure, w);
            }
        }
        while !self.new_incs.is_empty() && self.new_incs.len() <= 32 {
            let mut incs = vec![];
            std::mem::swap(&mut incs, &mut self.new_incs);
            self.process_incs(false, true, incs, copy_context);
        }
        self.flush();
        // println!("[{}] ProcessIncs END", worker.ordinal);
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
    const CAPACITY: usize = 512;

    #[inline(always)]
    const fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline]
    pub fn new(decs: Vec<ObjectReference>, count_down: Arc<AtomicUsize>) -> Self {
        debug_assert!(crate::args::REF_COUNT);
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
        crate::stat(|s| {
            s.dead_objects += 1;
            s.dead_volume += o.get_size::<VM>();
            if !immix.immix_space.in_space(o) {
                s.dead_los_objects += 1;
                s.dead_los_volume += o.get_size::<VM>();
            }
        });
        immix.mark(o);
        // println!(" - dead {:?}", o);
        // debug_assert_eq!(self::count(o), 0);
        // Recursively decrease field ref counts
        EdgeIterator::<VM>::iterate(o, |edge| {
            let x = unsafe { edge.load::<ObjectReference>() };
            if !x.is_null() && !self::rc_stick(x) {
                self.recursive_dec(x);
            }
        });
        let in_ix_space = immix.immix_space.in_space(o);
        if !crate::args::BLOCK_ONLY && in_ix_space {
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
            let block = Block::containing::<VM>(o);
            immix.immix_space.add_to_possibly_dead_mature_blocks(block);
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
            let o = if crate::args::REF_COUNT
                && crate::args::RC_MATURE_EVACUATION
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
            immix.immix_space.schedule_rc_block_sweeping_tasks();
        }
    }
}

pub struct SweepBlocksAfterDecs {
    pub blocks: Vec<Block>,
}

impl<VM: VMBinding> GCWork<VM> for SweepBlocksAfterDecs {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        for b in &self.blocks {
            b.unlog_non_atomic();
            b.rc_sweep_mature::<VM>(&immix.immix_space);
        }
    }
}

#[cfg(feature = "ix_delayed_nursery_evacuation")]
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

#[cfg(feature = "ix_delayed_nursery_evacuation")]
unsafe impl<VM: VMBinding> Send for RCEvacuateNursery<VM> {}

#[cfg(feature = "ix_delayed_nursery_evacuation")]
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
        debug_assert!(crate::args::REF_COUNT);
        debug_assert!(crate::args::RC_NURSERY_EVACUATION);
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
        let check_mature_evac_remset = crate::args::RC_MATURE_EVACUATION
            && (self.concurrent_marking_in_progress
                || self.current_pause == Pause::FinalMark
                || self.current_pause == Pause::FullTraceFast);
        let mut should_add_to_mature_evac_remset = false;
        EdgeIterator::<VM>::iterate(o, |edge| {
            if crate::args::RC_MATURE_EVACUATION
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
            self::promote::<VM>(new);
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
        debug_assert!(crate::args::RC_NURSERY_EVACUATION);
        if o.is_null() {
            return o;
        }
        if self::count(o) != 0
            || self.immix().los().in_space(o)
            || (crate::args::RC_DONT_EVACUATE_NURSERY_IN_RECYCLED_LINES
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

#[cfg(feature = "ix_delayed_nursery_evacuation")]
impl<VM: VMBinding> GCWork<VM> for RCEvacuateNursery<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        debug_assert!(crate::args::RC_NURSERY_EVACUATION);
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
                if crate::args::CONCURRENT_MARKING && self.current_pause == Pause::InitialMark {
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
            self.mmtk().scheduler.work_buckets[bucket]
                .add_no_notify(ProcessIncs::<VM>::new(roots, true));
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
