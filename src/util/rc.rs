use super::metadata::MetadataSpec;
use super::{metadata::side_metadata::address_to_meta_address, Address};
use crate::policy::immix::block::BlockState;
use crate::util::cm::LXRStopTheWorldProcessEdges;
use crate::LazySweepingJobsCounter;
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
    AllocationSemantics, MMTK,
};
use atomic::Ordering;
use crossbeam_queue::ArrayQueue;
use std::intrinsics::unlikely;
use std::iter::Step;
use std::ops::{Deref, DerefMut};

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
pub fn promote<VM: VMBinding>(o: ObjectReference) {
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
    /// Root edges?
    kind: EdgeKind,
    /// Execution worker
    worker: *mut GCWorker<VM>,
    immix: *const Immix<VM>,
    current_pause: Pause,
    concurrent_marking_in_progress: bool,
    current_pause_should_do_mature_evac: bool,
}

unsafe impl<VM: VMBinding> Send for ProcessIncs<VM> {}

impl<VM: VMBinding> ProcessIncs<VM> {
    const CAPACITY: usize = 128;
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
            kind: if roots {
                EdgeKind::Root
            } else {
                EdgeKind::Mature
            },
            new_incs: vec![],
            remset: vec![],
            worker: std::ptr::null_mut(),
            immix: std::ptr::null(),
            current_pause: Pause::RefCount,
            concurrent_marking_in_progress: false,
            current_pause_should_do_mature_evac: false,
        }
    }

    #[inline]
    fn new_nursery(incs: Vec<Address>) -> Self {
        let mut w = Self::new(incs, false);
        w.kind = EdgeKind::Nursery;
        w
    }

    #[inline(always)]
    fn promote(&mut self, o: ObjectReference, copied: bool, los: bool) {
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
        } else {
            // println!("promote los {:?} {}", o, self.immix().is_marked(o));
        }
        // Don't mark copied objects in initial mark pause. The concurrent marker will do it (and can also resursively mark the old objects).
        if self.concurrent_marking_in_progress || self.current_pause == Pause::FinalMark {
            self.immix().mark2(o, los);
        }
        self.scan_nursery_object(o, los, !copied);
    }

    #[inline(always)]
    fn scan_nursery_object(&mut self, o: ObjectReference, los: bool, in_place_promotion: bool) {
        let check_mature_evac_remset =
            crate::args::RC_MATURE_EVACUATION && (self.concurrent_marking_in_progress);
        let mut should_add_to_mature_evac_remset = false;
        if los {
            let start = side_metadata::address_to_meta_address(
                VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
                o.to_address(),
            )
            .to_mut_ptr::<u8>();
            let limit = side_metadata::address_to_meta_address(
                VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
                (o.to_address() + o.get_size::<VM>()).align_up(64),
            )
            .to_mut_ptr::<u8>();
            let bytes = unsafe { limit.offset_from(start) as usize };
            if crate::args::ENABLE_NON_TEMPORAL_MEMSET {
                let bytes = (bytes + 15usize) & !15usize;
                debug_assert_eq!(bytes & 15usize, 0);
                crate::util::memory::write_nt(
                    start as *mut u128,
                    bytes >> 4,
                    0xffffffff_ffffffff_ffffffff_ffffffffu128,
                );
            } else {
                unsafe {
                    std::ptr::write_bytes(start, 0xffu8, bytes);
                }
            }
        }
        let x = in_place_promotion && !los;
        EdgeIterator::<VM>::iterate(o, |edge| {
            let target = unsafe { edge.load::<ObjectReference>() };
            if crate::args::RC_MATURE_EVACUATION
                && check_mature_evac_remset
                && !should_add_to_mature_evac_remset
            {
                if !self.immix().in_defrag(o) && self.immix().in_defrag(target) {
                    should_add_to_mature_evac_remset = true;
                }
            }
            if x {
                edge.unlog::<VM>();
            }
            if !target.is_null()
                && (!self::rc_stick(target)
                    || (self.should_do_mature_evac() && self.immix().in_defrag(target)))
            {
                self.new_incs.push(edge);
            }
        });
        if self.new_incs.len() >= Self::CAPACITY {
            self.flush(true)
        }
        if should_add_to_mature_evac_remset {
            unsafe {
                self.worker()
                    .local::<ImmixCopyContext<VM>>()
                    .add_mature_evac_remset(o)
            }
        }
    }

    #[inline(always)]
    fn add_remset(&mut self, e: Address) {
        self.remset.push(e);
    }

    #[inline]
    fn flush(&mut self, new_incs: bool) {
        if new_incs && !self.new_incs.is_empty() {
            let mut new_incs = vec![];
            std::mem::swap(&mut new_incs, &mut self.new_incs);
            self.worker().add_work(
                WorkBucketStage::Unconstrained,
                ProcessIncs::new_nursery(new_incs),
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
    }

    #[inline(always)]
    fn process_inc(&mut self, o: ObjectReference) -> ObjectReference {
        let r = self::inc(o);
        // println!(" - inc e={:?} {:?} rc: {:?} -> {:?}", _e, o, r, count(o));
        if let Ok(0) = r {
            self.promote(o, false, self.immix().los().in_space(o));
        }
        o
    }

    #[inline(always)]
    const fn should_do_mature_evac(&self) -> bool {
        crate::args::RC_MATURE_EVACUATION && self.current_pause_should_do_mature_evac
    }

    #[inline(always)]
    fn dont_evacuate(&self, o: ObjectReference, los: bool) -> bool {
        if los {
            return true;
        }
        let b = Block::containing::<VM>(o);
        let state = b.get_state();
        // Evacuate objects in defrag blocks
        if self.should_do_mature_evac() && b.is_defrag_source() {
            return false;
        }
        // Skip mature object
        if self::count(o) != 0 || state == BlockState::Marked {
            return true;
        }
        // Skip recycled lines
        if crate::args::RC_DONT_EVACUATE_NURSERY_IN_RECYCLED_LINES && state == BlockState::Reusing {
            return true;
        }
        false
    }

    #[inline(always)]
    fn process_inc_and_evacuate(
        &mut self,
        o: ObjectReference,
        copy_context: &mut ImmixCopyContext<VM>,
    ) -> ObjectReference {
        crate::stat(|s| {
            s.inc_objects += 1;
            s.inc_volume += o.get_size::<VM>();
        });
        debug_assert!(crate::args::RC_NURSERY_EVACUATION);
        debug_assert!(!Self::DELAYED_EVACUATION);
        let los = self.immix().los().in_space(o);
        if self.dont_evacuate(o, los) {
            if let Ok(0) = self::inc(o) {
                self.promote(o, false, los);
            }
            return o;
        }
        if object_forwarding::is_forwarded::<VM>(o) {
            let new = object_forwarding::read_forwarding_pointer::<VM>(o);
            let _ = self::inc(new);
            return new;
        }
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(o);
        if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            // Object is moved to a new location.
            let new = object_forwarding::spin_and_get_forwarded_object::<VM>(o, forwarding_status);
            let _ = self::inc(new);
            new
        } else {
            let mature_defrag =
                self.should_do_mature_evac() && Block::containing::<VM>(o).is_defrag_source();
            let is_nursery = self::count(o) == 0;
            if is_nursery || mature_defrag {
                // Evacuate the object
                if !is_nursery && mature_defrag {
                    debug_assert!(
                        self.current_pause == Pause::FullTraceFast
                            || self.current_pause == Pause::FinalMark
                    );
                    let new = object_forwarding::copy_object::<VM, _>(
                        o,
                        AllocationSemantics::Default,
                        copy_context,
                    );
                    self::set(new, self::count(o));
                    let _ = self::inc(new);
                    object_forwarding::set_forwarding_pointer::<VM>(o, new);
                    self::promote::<VM>(new);
                    if self.current_pause == Pause::FinalMark {
                        self.immix().immix_space.attempt_mark(new);
                        self.immix().immix_space.unmark(o);
                        copy_context.add_mature_evac_remset(new);
                    }
                    new
                } else {
                    let new = object_forwarding::forward_object::<VM, _>(
                        o,
                        AllocationSemantics::Default,
                        copy_context,
                    );
                    let _ = self::inc(new);
                    self.promote(new, true, false);
                    if mature_defrag && self.current_pause == Pause::FinalMark {
                        copy_context.add_mature_evac_remset(new);
                    }
                    new
                }
            } else {
                // Object is not moved.
                object_forwarding::clear_forwarding_bits::<VM>(o);
                let _ = self::inc(o);
                o
            }
        }
    }

    /// Return `None` if the increment of the edge should be delayed
    #[inline(always)]
    fn unlog_and_load_rc_object(&mut self, kind: EdgeKind, e: Address) -> Option<ObjectReference> {
        debug_assert!(!crate::args::EAGER_INCREMENTS);
        debug_assert!(!Self::DELAYED_EVACUATION);
        let o = unsafe { e.load::<ObjectReference>() };
        // unlog edge
        if kind == EdgeKind::Mature {
            e.unlog::<VM>();
        }
        if o.is_null() {
            return None;
        }
        Some(o)
    }

    #[inline(always)]
    fn process_edge(
        &mut self,
        kind: EdgeKind,
        e: Address,
        cc: &mut ImmixCopyContext<VM>,
    ) -> Option<ObjectReference> {
        let o = match self.unlog_and_load_rc_object(kind, e) {
            Some(o) => o,
            _ => return None,
        };
        // println!(" - inc {:?}: {:?} rc={}", e, o, count(o));
        debug_assert_ne!(unsafe { o.to_address().load::<usize>() }, 0xdeadusize);
        let new = if !crate::args::RC_NURSERY_EVACUATION || Self::DELAYED_EVACUATION {
            self.process_inc(o)
        } else {
            self.process_inc_and_evacuate(o, cc)
        };
        if new != o {
            // println!(" -- inc {:?}: {:?} => {:?} rc={}", e, o, new, count(new));
            unsafe { e.store(new) }
        } else {
            // println!(" -- inc {:?}: {:?} rc={}", e, o, count(o));
        }
        Some(new)
    }

    #[inline(always)]
    fn process_incs(
        &mut self,
        kind: EdgeKind,
        mut incs: AddressBuffer<'_>,
        copy_context: &mut ImmixCopyContext<VM>,
    ) -> Option<Vec<ObjectReference>> {
        let roots = incs.as_mut_ptr() as *mut ObjectReference;
        let mut num_roots = 0usize;
        for e in &mut *incs {
            let new = self.process_edge(kind, *e, copy_context);
            if kind == EdgeKind::Root {
                if let Some(new) = new {
                    unsafe {
                        roots.add(num_roots).write(new);
                    }
                    num_roots += 1;
                }
            }
        }
        if kind == EdgeKind::Root && num_roots != 0 {
            let cap = incs.capacity();
            std::mem::forget(incs);
            let roots = unsafe { Vec::<ObjectReference>::from_raw_parts(roots, num_roots, cap) };
            Some(roots)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Root,
    Nursery,
    Mature,
}

enum AddressBuffer<'a> {
    Owned(Vec<Address>),
    Ref(&'a mut Vec<Address>),
}

impl Deref for AddressBuffer<'_> {
    type Target = Vec<Address>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(x) => x,
            Self::Ref(x) => x,
        }
    }
}

impl DerefMut for AddressBuffer<'_> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Owned(x) => x,
            Self::Ref(x) => x,
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessIncs<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        self.immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        self.current_pause = self.immix().current_pause().unwrap();
        self.concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        self.current_pause_should_do_mature_evac = crate::args::RC_MATURE_EVACUATION
            && self.current_pause == Pause::FullTraceFast
            || self.current_pause == Pause::FinalMark;
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        // Process main buffer
        let root_edges = if self.kind == EdgeKind::Root
            && (self.current_pause == Pause::FinalMark
                || self.current_pause == Pause::FullTraceFast)
        {
            self.incs.clone()
        } else {
            vec![]
        };
        let mut incs = vec![];
        std::mem::swap(&mut incs, &mut self.incs);
        let roots = self.process_incs(self.kind, AddressBuffer::Owned(incs), copy_context);
        if let Some(roots) = roots {
            if crate::args::CONCURRENT_MARKING && self.current_pause == Pause::InitialMark {
                worker
                    .scheduler()
                    .postpone(ImmixConcurrentTraceObjects::<VM>::new(roots.clone(), mmtk));
            }
            if self.current_pause == Pause::FinalMark || self.current_pause == Pause::FullTraceFast
            {
                worker.add_work(
                    WorkBucketStage::Closure,
                    LXRStopTheWorldProcessEdges::<VM>::new(root_edges, true, mmtk),
                )
            } else {
                unsafe {
                    crate::plan::immix::CURR_ROOTS.push(roots);
                }
            }
        }
        // Process recursively generated buffer
        if !self.new_incs.is_empty() {
            self.flush(false);
            let mut incs = vec![];
            while !self.new_incs.is_empty() && self.new_incs.len() <= Self::CAPACITY {
                if self.new_incs.len() == 1 {
                    // println!(" - {} {}", worker.ordinal, c);
                    let e = self.new_incs[0];
                    self.new_incs.clear();
                    self.process_edge(EdgeKind::Nursery, e, copy_context);
                } else {
                    incs.clear();
                    std::mem::swap(&mut incs, &mut self.new_incs);
                    self.process_incs(
                        EdgeKind::Nursery,
                        AddressBuffer::Ref(&mut incs),
                        copy_context,
                    );
                }
            }
        }
        self.flush(true);
        // println!("[{}] ProcessIncs END", worker.ordinal);
    }
}

pub struct ProcessDecs<VM: VMBinding> {
    /// Decrements to process
    decs: Vec<ObjectReference>,
    /// Recursively generated new decrements
    new_decs: Vec<ObjectReference>,
    /// Execution worker
    worker: *mut GCWorker<VM>,
    mmtk: *const MMTK<VM>,
    counter: LazySweepingJobsCounter,
    mark_objects: Vec<ObjectReference>,
    concurrent_marking_in_progress: bool,
}

unsafe impl<VM: VMBinding> Send for ProcessDecs<VM> {}

impl<VM: VMBinding> ProcessDecs<VM> {
    const CAPACITY: usize = 128;

    #[inline(always)]
    const fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline]
    pub fn new(decs: Vec<ObjectReference>, counter: LazySweepingJobsCounter) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        Self {
            decs,
            new_decs: vec![],
            worker: std::ptr::null_mut(),
            mmtk: std::ptr::null_mut(),
            counter,
            mark_objects: vec![],
            concurrent_marking_in_progress: false,
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
                ProcessDecs::new(new_decs, self.counter.clone_with_decs()),
            );
        }
        if !self.mark_objects.is_empty() {
            let mut objects = vec![];
            std::mem::swap(&mut objects, &mut self.mark_objects);
            let w = ImmixConcurrentTraceObjects::new(objects, unsafe { &*self.mmtk });
            if crate::args::LAZY_DECREMENTS {
                self.worker().add_work(WorkBucketStage::Unconstrained, w);
            } else {
                self.worker().scheduler().postpone(w);
            }
        }
    }

    #[cold]
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
            if !x.is_null() {
                let rc = self::count(x);
                if rc != MAX_REF_COUNT && rc != 0 {
                    self.recursive_dec(x);
                }
            }
            if self.concurrent_marking_in_progress {
                if !x.is_null() && !immix.is_marked(x) {
                    self.mark_objects.push(x);
                }
            }
        });
        let in_ix_space = immix.immix_space.in_space(o);
        if !crate::args::HOLE_COUNTING && in_ix_space {
            Block::inc_dead_bytes_sloppy_for_object::<VM>(o);
        }
        if !crate::args::BLOCK_ONLY && in_ix_space {
            self::unmark_straddle_object::<VM>(o);
        }
        #[cfg(feature = "sanity")]
        unsafe {
            o.to_address().store(0xdeadusize);
        }
        if in_ix_space {
            let block = Block::containing::<VM>(o);
            immix
                .immix_space
                .add_to_possibly_dead_mature_blocks(block, false);
        } else {
            immix.los().rc_free(o);
        }
    }

    #[inline]
    fn process_decs(&mut self, decs: &Vec<ObjectReference>, immix: &Immix<VM>) {
        for o in decs {
            if o.is_null() {
                continue;
            }
            if self::count(*o) == 0 {
                continue;
            }
            let o =
                if crate::args::RC_MATURE_EVACUATION && object_forwarding::is_forwarded::<VM>(*o) {
                    object_forwarding::read_forwarding_pointer::<VM>(*o)
                } else {
                    *o
                };
            let mut dead = false;
            let _ = self::fetch_update(o, |c| {
                if c == 1 {
                    if unlikely(!dead) {
                        dead = true;
                        self.process_dead_object(o, immix);
                    }
                }
                debug_assert!(c <= MAX_REF_COUNT);
                if unlikely(c == 0 || c == MAX_REF_COUNT) {
                    None /* sticky */
                } else {
                    Some(c - 1)
                }
            });
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessDecs<VM> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        self.mmtk = mmtk;
        self.concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        let mut decs = vec![];
        std::mem::swap(&mut decs, &mut self.decs);
        self.process_decs(&decs, immix);
        while !self.new_decs.is_empty() {
            decs.clear();
            std::mem::swap(&mut decs, &mut self.new_decs);
            self.process_decs(&decs, immix);
        }
        self.flush();
    }
}

pub struct SweepBlocksAfterDecs {
    blocks: Vec<(Block, bool)>,
    _counter: LazySweepingJobsCounter,
}

impl SweepBlocksAfterDecs {
    pub fn new(blocks: Vec<(Block, bool)>, counter: LazySweepingJobsCounter) -> Self {
        Self {
            blocks,
            _counter: counter,
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for SweepBlocksAfterDecs {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        if self.blocks.is_empty() {
            return;
        }
        let mut count = 0;
        let queue = ArrayQueue::new(self.blocks.len());
        for (block, defrag) in &self.blocks {
            block.unlog();
            if block.rc_sweep_mature::<VM>(&immix.immix_space, *defrag) {
                count += 1;
                queue.push(block.start()).unwrap();
                if *defrag {
                    block.set_as_defrag_source(false);
                }
            } else {
                assert!(
                    !*defrag,
                    "defrag block is freed? {:?} {:?} {}",
                    block,
                    block.get_state(),
                    block.is_defrag_source()
                );
            }
        }
        immix.immix_space.pr.release_bulk(count, queue)
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

    #[inline(always)]
    fn new(edges: Vec<Address>, roots: bool, mmtk: &'static MMTK<VM>) -> Self {
        debug_assert!(roots);
        let base = ProcessEdgesBase::new(edges, roots, mmtk);
        Self { base }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    #[inline(always)]
    fn process_edges(&mut self) {
        if !self.edges.is_empty() {
            let bucket = WorkBucketStage::rc_process_incs_stage();
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.edges);
            self.worker()
                .add_work(bucket, ProcessIncs::new(roots, true));
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
