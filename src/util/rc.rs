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
use std::intrinsics::unlikely;
use std::iter::Step;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;

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
    side_metadata::fetch_update(
        &RC_TABLE,
        o.to_address(),
        Ordering::Relaxed,
        Ordering::Relaxed,
        f,
    )
}

#[inline(always)]
pub fn rc_stick(o: ObjectReference) -> bool {
    self::count(o) == MAX_REF_COUNT
}

#[inline(always)]
pub fn inc(o: ObjectReference) -> Result<usize, usize> {
    fetch_update(o, |x| {
        debug_assert!(x <= MAX_REF_COUNT);
        if x == MAX_REF_COUNT {
            None
        } else {
            Some(x + 1)
        }
    })
}

#[inline(always)]
pub fn dec(o: ObjectReference) -> Result<usize, usize> {
    fetch_update(o, |x| {
        debug_assert!(x <= MAX_REF_COUNT);
        if x == 0 || x == MAX_REF_COUNT
        /* sticky */
        {
            None
        } else {
            Some(x - 1)
        }
    })
}

#[inline(always)]
pub fn set(o: ObjectReference, count: usize) {
    side_metadata::store_atomic(&RC_TABLE, o.to_address(), count, Ordering::Relaxed)
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
    let v = side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::Relaxed);
    v == 0
}

#[inline(always)]
pub fn is_straddle_line(line: Line) -> bool {
    let v = side_metadata::load_atomic(&RC_STRADDLE_LINES, line.start(), Ordering::Relaxed);
    v != 0
}

#[inline(always)]
pub fn address_is_in_straddle_line(a: Address) -> bool {
    let line = Line::from(Line::align(a));
    self::count(unsafe { a.to_object_reference() }) != 0 && self::is_straddle_line(line)
}

#[inline(always)]
fn mark_straddle_object_with_size<VM: VMBinding>(o: ObjectReference, size: usize) {
    debug_assert!(!crate::args::BLOCK_ONLY);
    debug_assert!(size > Line::BYTES);
    let start_line = Line::forward(Line::containing::<VM>(o), 1);
    let end_line = Line::from(Line::align(o.to_address() + size));
    for line in start_line..end_line {
        side_metadata::store_atomic(&RC_STRADDLE_LINES, line.start(), 1, Ordering::Relaxed);
        self::set(unsafe { line.start().to_object_reference() }, 1);
    }
}

#[inline(always)]
pub fn mark_straddle_object<VM: VMBinding>(o: ObjectReference) {
    let size = VM::VMObjectModel::get_current_size(o);
    mark_straddle_object_with_size::<VM>(o, size)
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
            // std::sync::atomic::fence(Ordering::Relaxed);
            side_metadata::store_atomic(&RC_STRADDLE_LINES, line.start(), 0, Ordering::Relaxed);
            // std::sync::atomic::fence(Ordering::Relaxed);
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
    let size = o.get_size::<VM>();
    if size > Line::BYTES {
        self::mark_straddle_object_with_size::<VM>(o, size);
    }
}

pub struct ProcessIncs<VM: VMBinding, const KIND: EdgeKind> {
    /// Increments to process
    incs: Vec<Address>,
    /// Recursively generated new increments
    new_incs: Vec<Address>,
    /// Delayed nursery increments
    remset: Vec<Address>,
    /// Execution worker
    worker: *mut GCWorker<VM>,
    immix: *const Immix<VM>,
    current_pause: Pause,
    concurrent_marking_in_progress: bool,
    no_evac: bool,
    slice: Option<&'static [ObjectReference]>,
}

static INC_BUFFER_SIZE: AtomicUsize = AtomicUsize::new(0);

#[inline(always)]
pub fn inc_buffer_size() -> usize {
    INC_BUFFER_SIZE.load(Ordering::Relaxed)
}

#[inline(always)]
pub fn inc_inc_buffer_size() {
    INC_BUFFER_SIZE.store(
        INC_BUFFER_SIZE.load(Ordering::Relaxed) + 1,
        Ordering::Relaxed,
    );
}

#[inline(always)]
pub fn reset_inc_buffer_size() {
    crate::add_incs(inc_buffer_size());
    INC_BUFFER_SIZE.store(0, Ordering::Relaxed)
}

unsafe impl<VM: VMBinding, const KIND: EdgeKind> Send for ProcessIncs<VM, KIND> {}

impl<VM: VMBinding, const KIND: EdgeKind> ProcessIncs<VM, KIND> {
    const CAPACITY: usize = 128;

    #[inline(always)]
    const fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline(always)]
    const fn immix(&self) -> &'static Immix<VM> {
        unsafe { &*self.immix }
    }

    #[inline]
    pub fn new_array_slice(slice: &'static [ObjectReference]) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        Self {
            incs: vec![],
            new_incs: vec![],
            remset: vec![],
            worker: std::ptr::null_mut(),
            immix: std::ptr::null(),
            current_pause: Pause::RefCount,
            concurrent_marking_in_progress: false,
            no_evac: false,
            slice: Some(slice),
        }
    }

    #[inline]
    pub fn new(incs: Vec<Address>) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        Self {
            incs,
            new_incs: vec![],
            remset: vec![],
            worker: std::ptr::null_mut(),
            immix: std::ptr::null(),
            current_pause: Pause::RefCount,
            concurrent_marking_in_progress: false,
            no_evac: false,
            slice: None,
        }
    }

    #[inline(always)]
    fn promote(&mut self, o: ObjectReference, copied: bool, los: bool) {
        o.verify();
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
    fn record_mature_evac_remset(&mut self, e: Address, o: ObjectReference, force: bool) {
        if !(crate::args::RC_MATURE_EVACUATION
            && (self.concurrent_marking_in_progress || self.current_pause == Pause::FinalMark))
        {
            return;
        }
        if force || (!self.immix().address_in_defrag(e) && self.immix().in_defrag(o)) {
            unsafe {
                self.worker()
                    .local::<ImmixCopyContext<VM>>()
                    .add_mature_evac_remset(e)
            }
        }
    }

    #[inline(always)]
    fn scan_nursery_object(&mut self, o: ObjectReference, los: bool, in_place_promotion: bool) {
        if VM::VMScanning::is_type_array(o) {
            return;
        }
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
                    0xffff_ffff_ffff_ffff_ffff_ffff_ffff_ffff_u128,
                );
            } else {
                unsafe {
                    std::ptr::write_bytes(start, 0xffu8, bytes);
                }
            }
        } else if in_place_promotion {
            let size = o.get_size::<VM>();
            let end = o.to_address() + size;
            let aligned_end = end.align_down(64);
            let mut cursor = o.to_address() + 16usize;
            let mut meta = side_metadata::address_to_meta_address(
                VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.extract_side_spec(),
                cursor.align_up(64),
            );
            while cursor < aligned_end {
                if cursor.is_aligned_to(64) {
                    unsafe { meta.store(0xffu8) }
                    meta += 1usize;
                    cursor += 64usize;
                } else {
                    cursor.unlog::<VM>();
                    cursor += 8usize;
                }
            }
            while cursor < end {
                cursor.unlog::<VM>();
                cursor += 8usize;
            }
        };
        if los && VM::VMScanning::is_obj_array(o) {
            let data = VM::VMScanning::obj_array_data(o);
            let mut packets = vec![];
            for chunk in data.chunks(512) {
                let w = box ProcessIncs::<VM, { EdgeKind::Nursery }>::new_array_slice(chunk);
                packets.push(w as Box<dyn GCWork<VM>>);
            }
            self.worker().scheduler().work_buckets[WorkBucketStage::RCProcessIncs]
                .bulk_add(packets);
        } else {
            EdgeIterator::<VM>::iterate(o, |edge| {
                let target = unsafe { edge.load::<ObjectReference>() };
                if !target.is_null() {
                    if !self::rc_stick(target) {
                        self.new_incs.push(edge);
                        if self.new_incs.len() >= Self::CAPACITY {
                            self.flush()
                        }
                    } else {
                        self.record_mature_evac_remset(edge, target, false);
                    }
                }
            });
        }
    }

    #[inline(always)]
    fn add_remset(&mut self, e: Address) {
        self.remset.push(e);
    }

    #[cold]
    fn flush(&mut self) {
        if !self.new_incs.is_empty() {
            let mut new_incs = vec![];
            std::mem::swap(&mut new_incs, &mut self.new_incs);
            let w = ProcessIncs::<VM, { EdgeKind::Nursery }>::new(new_incs);
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
        }
    }

    #[inline(always)]
    fn process_inc(&mut self, _o: ObjectReference) -> ObjectReference {
        unreachable!();
        // let r = self::inc(o);
        // // println!(" - inc e={:?} {:?} rc: {:?} -> {:?}", _e, o, r, count(o));
        // if let Ok(0) = r {
        //     self.promote(o, false, self.immix().los().in_space(o));
        // }
        // o
    }

    #[inline(always)]
    fn dont_evacuate(&self, o: ObjectReference, los: bool) -> bool {
        if los {
            return true;
        }
        // Skip mature object
        if self::count(o) != 0 {
            return true;
        }
        // Skip recycled lines
        if crate::args::RC_DONT_EVACUATE_NURSERY_IN_RECYCLED_LINES
            && Block::containing::<VM>(o).get_state() == BlockState::Reusing
        {
            return true;
        }
        // if o.get_size::<VM>() >= 4096 {
        //     return true;
        // }
        false
    }

    #[inline(always)]
    fn process_inc_and_evacuate(
        &mut self,
        o: ObjectReference,
        copy_context: &mut ImmixCopyContext<VM>,
    ) -> ObjectReference {
        o.verify();
        crate::stat(|s| {
            s.inc_objects += 1;
            s.inc_volume += o.get_size::<VM>();
        });
        debug_assert!(crate::args::RC_NURSERY_EVACUATION);
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
            let is_nursery = self::count(o) == 0;
            if is_nursery && !self.no_evac {
                // Evacuate the object
                let new = object_forwarding::forward_object::<VM, _>(
                    o,
                    AllocationSemantics::Default,
                    copy_context,
                );
                if crate::should_record_copy_bytes() {
                    unsafe { crate::SLOPPY_COPY_BYTES += new.get_size::<VM>() }
                }
                let _ = self::inc(new);
                self.promote(new, true, false);
                new
            } else {
                // Object is not moved.
                let r = self::inc(o);
                object_forwarding::clear_forwarding_bits::<VM>(o);
                if let Ok(0) = r {
                    self.promote(o, false, los);
                }
                o
            }
        }
    }

    /// Return `None` if the increment of the edge should be delayed
    #[inline(always)]
    fn unlog_and_load_rc_object<const K: EdgeKind>(
        &mut self,
        e: Address,
    ) -> Option<ObjectReference> {
        debug_assert!(!crate::args::EAGER_INCREMENTS);
        let o = unsafe { e.load::<ObjectReference>() };
        // unlog edge
        if K == EdgeKind::Mature {
            e.unlog::<VM>();
        }
        if o.is_null() {
            return None;
        }
        Some(o)
    }

    #[inline(always)]
    fn process_edge<const K: EdgeKind>(
        &mut self,
        e: Address,
        cc: &mut ImmixCopyContext<VM>,
    ) -> Option<ObjectReference> {
        let o = match self.unlog_and_load_rc_object::<K>(e) {
            Some(o) => o,
            _ => return None,
        };
        // println!(" - inc {:?}: {:?} rc={}", e, o, count(o));
        o.verify();
        let new = if !crate::args::RC_NURSERY_EVACUATION {
            self.process_inc(o)
        } else {
            self.process_inc_and_evacuate(o, cc)
        };
        if K != EdgeKind::Root {
            self.record_mature_evac_remset(e, o, false);
        }
        if new != o {
            // println!(" -- inc {:?}: {:?} => {:?} rc={}", e, o, new, count(new));
            unsafe { e.store(new) }
        } else {
            // println!(" -- inc {:?}: {:?} rc={}", e, o, count(o));
        }
        Some(new)
    }

    #[inline(always)]
    fn process_incs<const K: EdgeKind>(
        &mut self,
        mut incs: AddressBuffer<'_>,
        copy_context: &mut ImmixCopyContext<VM>,
    ) -> Option<Vec<ObjectReference>> {
        if K == EdgeKind::Root {
            let roots = incs.as_mut_ptr() as *mut ObjectReference;
            let mut num_roots = 0usize;
            for e in &mut *incs {
                if let Some(new) = self.process_edge::<K>(*e, copy_context) {
                    unsafe {
                        roots.add(num_roots).write(new);
                    }
                    num_roots += 1;
                }
            }
            if num_roots != 0 {
                let cap = incs.capacity();
                std::mem::forget(incs);
                let roots =
                    unsafe { Vec::<ObjectReference>::from_raw_parts(roots, num_roots, cap) };
                Some(roots)
            } else {
                None
            }
        } else {
            for e in &mut *incs {
                self.process_edge::<K>(*e, copy_context);
            }
            None
        }
    }

    #[inline(always)]
    fn process_incs_for_obj_array<const K: EdgeKind>(
        &mut self,
        slice: &[ObjectReference],
        copy_context: &mut ImmixCopyContext<VM>,
    ) -> Option<Vec<ObjectReference>> {
        for e in slice {
            let e = Address::from_ref(e);
            self.process_edge::<K>(e, copy_context);
        }
        None
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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

impl<VM: VMBinding, const KIND: EdgeKind> GCWork<VM> for ProcessIncs<VM, KIND> {
    #[inline(always)]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        debug_assert!(!crate::plan::barriers::BARRIER_MEASUREMENT);
        self.immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        self.current_pause = self.immix().current_pause().unwrap();
        self.concurrent_marking_in_progress = crate::concurrent_marking_in_progress();
        let copy_context =
            unsafe { &mut *(worker.local::<ImmixCopyContext<VM>>() as *mut ImmixCopyContext<VM>) };
        if *crate::args::OPPORTUNISTIC_EVAC {
            if crate::NO_EVAC.load(Ordering::Relaxed) {
                self.no_evac = true;
            } else {
                let over_time = crate::GC_START_TIME
                    .load(Ordering::Relaxed)
                    .elapsed()
                    .unwrap()
                    .as_millis()
                    >= *crate::args::OPPORTUNISTIC_EVAC_THRESHOLD as u128;
                let over_space = mmtk.plan.get_pages_used() - mmtk.plan.get_collection_reserve()
                    > mmtk.plan.get_total_pages();
                if over_space || over_time {
                    self.no_evac = true;
                    crate::NO_EVAC.store(true, Ordering::Relaxed);
                    if crate::args::LOG_PER_GC_STATE {
                        println!(" - no evac");
                    }
                }
            }
        }
        // Process main buffer
        let root_edges = if KIND == EdgeKind::Root
            && (self.current_pause == Pause::FinalMark
                || self.current_pause == Pause::FullTraceFast)
        {
            self.incs.clone()
        } else {
            vec![]
        };
        let roots = {
            if let Some(slice) = self.slice {
                assert_eq!(KIND, EdgeKind::Nursery);
                self.process_incs_for_obj_array::<KIND>(slice, copy_context)
            } else {
                let mut incs = vec![];
                std::mem::swap(&mut incs, &mut self.incs);
                self.process_incs::<KIND>(AddressBuffer::Owned(incs), copy_context)
            }
        };
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
        let mut incs = vec![];
        while !self.new_incs.is_empty() {
            incs.clear();
            std::mem::swap(&mut incs, &mut self.new_incs);
            self.process_incs::<{ EdgeKind::Nursery }>(AddressBuffer::Ref(&mut incs), copy_context);
        }
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
            let mmtk = unsafe { &*self.mmtk };
            let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
            if immix.current_pause().is_none() {
                self.worker().add_work_prioritized(
                    WorkBucketStage::Unconstrained,
                    ProcessDecs::new(new_decs, self.counter.clone_with_decs()),
                );
            } else {
                self.worker().add_work(
                    WorkBucketStage::Unconstrained,
                    ProcessDecs::new(new_decs, self.counter.clone_with_decs()),
                );
            }
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
            s.dead_mature_objects += 1;
            s.dead_mature_volume += o.get_size::<VM>();

            s.dead_mature_rc_objects += 1;
            s.dead_mature_rc_volume += o.get_size::<VM>();

            if !immix.immix_space.in_space(o) {
                s.dead_mature_los_objects += 1;
                s.dead_mature_los_volume += o.get_size::<VM>();

                s.dead_mature_rc_los_objects += 1;
                s.dead_mature_rc_los_volume += o.get_size::<VM>();
            }
        });
        let not_marked = immix.mark(o);
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
                if not_marked && self.concurrent_marking_in_progress && !immix.is_marked(x) {
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
        if cfg!(feature = "sanity") || ObjectReference::STRICT_VERIFICATION {
            unsafe { o.to_address().store(0xdeadusize) };
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
            // println!("dec {:?}", o);
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
                if c == 1 && unlikely(!dead) {
                    dead = true;
                    self.process_dead_object(o, immix);
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
        for (block, defrag) in &self.blocks {
            block.unlog();
            if block.rc_sweep_mature::<VM>(&immix.immix_space, *defrag) {
                count += 1;
                immix.immix_space.pr.release_pages(block.start());
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
        if count != 0 && immix.current_pause().is_none() {
            immix
                .immix_space
                .num_clean_blocks_released_lazy
                .fetch_add(count, Ordering::Relaxed);
        }
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
            let mut roots = vec![];
            std::mem::swap(&mut roots, &mut self.edges);
            let w = ProcessIncs::<_, { EdgeKind::Root }>::new(roots);
            // if crate::args::LAZY_DECREMENTS {
            //     GCWork::do_work(&mut w, self.worker(), self.mmtk())
            // } else {
            let bucket = WorkBucketStage::rc_process_incs_stage();
            self.worker().add_work(bucket, w);
            // }
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
