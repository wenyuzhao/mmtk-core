use crate::{
    plan::{
        immix::{Immix, ImmixCopyContext},
        EdgeIterator,
    },
    policy::space::Space,
    scheduler::{GCWork, GCWorkScheduler, GCWorker, WorkBucketStage},
    util::{
        metadata::side_metadata::{self, SideMetadataSpec},
        object_forwarding, rc, Address, ObjectReference,
    },
    vm::VMBinding,
    AllocationSemantics, MMTK,
};
use atomic::{Atomic, Ordering};
use crossbeam_queue::{ArrayQueue, SegQueue};
use spin::RwLock;
use std::{marker::PhantomData, ops::Range, sync::atomic::AtomicPtr};

use super::{block::Block, line::Line, ImmixSpace};

const LOG_BLOCKS_IN_REGION: usize = 4;
const BLOCKS_IN_REGION: usize = 1 << LOG_BLOCKS_IN_REGION;
pub const LOG_BYTES_IN_REGION: usize = LOG_BLOCKS_IN_REGION + Block::LOG_BYTES;
const BYTES_IN_REGION: usize = 1 << LOG_BYTES_IN_REGION;

// #[derive(Clone)]
pub struct RemSet {
    buffer: ArrayQueue<Address>,
}

impl RemSet {
    pub const CAPACITY: usize = 4096;

    #[inline(always)]
    pub fn new() -> Self {
        Self {
            buffer: ArrayQueue::new(Self::CAPACITY),
        }
    }

    #[inline(always)]
    fn add(&self, a: Address) -> bool {
        self.buffer.push(a).is_ok()
    }
}

pub struct ProcessRemSet<VM: VMBinding> {
    remset: Option<RemSet>,
    remset_v: Vec<Address>,
    new_remset: Vec<Address>,
    immix: *const Immix<VM>,
    worker: *mut GCWorker<VM>,
    _p: PhantomData<VM>,
}

unsafe impl<VM: VMBinding> Send for ProcessRemSet<VM> {}

impl<VM: VMBinding> ProcessRemSet<VM> {
    pub fn new(remset: RemSet) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        debug_assert!(crate::args::RC_MATURE_EVACUATION2);
        Self {
            remset: Some(remset),
            remset_v: vec![],
            new_remset: vec![],
            worker: std::ptr::null_mut(),
            immix: std::ptr::null_mut(),
            _p: PhantomData,
        }
    }
    pub fn new_rec(remset: Vec<Address>) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        debug_assert!(crate::args::RC_MATURE_EVACUATION2);
        Self {
            remset_v: remset,
            remset: None,
            new_remset: vec![],
            worker: std::ptr::null_mut(),
            immix: std::ptr::null_mut(),
            _p: PhantomData,
        }
    }

    const fn immix_space(&self) -> &ImmixSpace<VM> {
        unsafe { &(*self.immix).immix_space }
    }

    const fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline]
    fn process_node(&mut self, o: ObjectReference) {
        EdgeIterator::<VM>::iterate(o, |e| {
            let x = unsafe { e.load::<ObjectReference>() };
            if REMSETS.in_collection_set(x) {
                self.new_remset.push(e);
                if self.new_remset.len() >= RemSet::CAPACITY {
                    self.flush()
                }
            }
        })
    }

    fn flush(&mut self) {
        if self.new_remset.len() > 0 {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.new_remset);
            self.worker()
                .add_work(WorkBucketStage::RCEvacuateMature, Self::new_rec(remset));
        }
    }

    fn forward(&mut self, o: ObjectReference) -> ObjectReference {
        let copy_context = unsafe { self.worker().local::<ImmixCopyContext<VM>>() };
        let forwarding_status = object_forwarding::attempt_to_forward::<VM>(o);
        if object_forwarding::state_is_forwarded_or_being_forwarded(forwarding_status) {
            object_forwarding::spin_and_get_forwarded_object::<VM>(o, forwarding_status)
        } else {
            // Evacuate the young object
            let new = object_forwarding::forward_object::<VM, _>(
                o,
                AllocationSemantics::Default,
                copy_context,
            );
            // Transfer RC countsf
            new.log_start_address::<VM>();
            if !crate::args::BLOCK_ONLY {
                if new.get_size::<VM>() > Line::BYTES {
                    rc::mark_straddle_object::<VM>(new);
                }
            }
            rc::set(new, rc::count(o));
            self.immix_space().attempt_mark(new);
            self.immix_space().unmark(o);
            self.process_node(new);
            new
        }
    }

    fn forward_edge(&mut self, e: Address) {
        let o = unsafe { e.load::<ObjectReference>() };
        // println!("forward edge {:?} -> {:?}", e, o);
        if !self.immix_space().in_space(o) || !Block::containing::<VM>(o).is_defrag_source() {
            return;
        }
        // println!("forward edge {:?} -> {:?} START", e, o);
        let new = self.forward(o);
        unsafe {
            e.store(new);
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessRemSet<VM> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        self.immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        if let Some(rs) = self.remset.take() {
            while let Some(e) = rs.buffer.pop() {
                self.forward_edge(e);
            }
        }
        if !self.remset_v.is_empty() {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.remset_v);
            for e in remset {
                self.forward_edge(e);
            }
        }
        self.flush()
    }
}

pub struct PerRegionRemset {
    region: Address,
    last: RwLock<RemSet>,
    remsets: SegQueue<RemSet>,
}

impl PerRegionRemset {
    pub fn new(region: Address) -> Self {
        Self {
            region,
            last: RwLock::new(RemSet::new()),
            remsets: SegQueue::new(),
        }
    }
    pub fn flush(&self) {
        let remset = self.last.upgradeable_read();
        if remset.buffer.len() != 0 {
            let mut remset = remset.upgrade();
            let mut old_remset = RemSet::new();
            std::mem::swap::<RemSet>(&mut remset, &mut old_remset);
            self.remsets.push(old_remset);
        }
    }

    pub fn add(&self, a: Address) {
        let remset = self.last.upgradeable_read();
        if !remset.add(a) {
            let old_remset = {
                let mut remset = remset.upgrade();
                if remset.add(a) {
                    return;
                }
                let mut old_remset = RemSet::new();
                std::mem::swap::<RemSet>(&mut remset, &mut old_remset);
                let ok = remset.add(a);
                debug_assert!(ok);
                old_remset
            };
            self.remsets.push(old_remset);
        }
    }
}

pub static REMSETS: RemSets = RemSets::new();

pub struct RemSets {
    range: Atomic<(Address, Address)>,
    all_remsets: SegQueue<*mut PerRegionRemset>,
}

unsafe impl Sync for RemSets {}

impl RemSets {
    pub const REMSET_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::LXR_REMSETS;

    pub const fn new() -> Self {
        Self {
            range: Atomic::new((Address::ZERO, Address::ZERO)),
            all_remsets: SegQueue::new(),
        }
    }

    pub fn init(&self, range: Range<Address>) {
        self.range.store((range.start, range.end), Ordering::SeqCst)
    }

    #[cold]
    fn record_slow(&self, e: Address, o: ObjectReference) {
        let remset = self.get(o.to_address());
        // println!("record_slow {:?} -> {:?}", e, o);
        remset.add(e)
    }

    #[inline]
    pub fn address_in_collection_set(&self, a: Address) -> bool {
        let (start, end) = self.range.load(Ordering::Relaxed);
        if a < start || a >= end {
            return false;
        }
        Block::from(Block::align(a)).is_defrag_source()
    }

    #[inline]
    pub fn in_collection_set(&self, o: ObjectReference) -> bool {
        self.address_in_collection_set(o.to_address())
    }

    #[inline]
    pub fn should_record(e: Address, o: ObjectReference) -> bool {
        if (e.as_usize() ^ o.to_address().as_usize()) >> LOG_BYTES_IN_REGION == 0 {
            return false;
        }
        !Block::from(e).is_defrag_source() && Block::from(o.to_address()).is_defrag_source()
    }

    #[inline]
    pub fn record(&self, e: Address, o: ObjectReference) {
        if (e.as_usize() ^ o.to_address().as_usize()) >> LOG_BYTES_IN_REGION == 0 {
            return;
        }
        if !self.address_in_collection_set(e) && self.in_collection_set(o) {
            self.record_slow(e, o);
        }
    }

    #[inline]
    fn get(&self, a: Address) -> &PerRegionRemset {
        let region = a.align_down(BYTES_IN_REGION);
        let slot = side_metadata::address_to_meta_address(&Self::REMSET_TABLE, a);
        let slot = unsafe { &*slot.to_ptr::<AtomicPtr<PerRegionRemset>>() };
        let ptr = slot.load(Ordering::Relaxed);
        if (ptr as usize) != 0 && (ptr as usize) != 1 {
            return unsafe { &*ptr };
        }
        // Attempt to alloc
        match slot.compare_exchange(0 as _, 1 as _, Ordering::SeqCst, Ordering::SeqCst) {
            Err(_) => loop {
                let ptr = slot.load(Ordering::SeqCst);
                if (ptr as usize) != 0 && (ptr as usize) != 1 {
                    return unsafe { &*ptr };
                }
            },
            Ok(_) => {
                let ptr = Box::leak(box PerRegionRemset::new(region));
                slot.store(ptr, Ordering::SeqCst);
                self.all_remsets.push(ptr);
                ptr
            }
        }
    }

    #[inline]
    fn get_opt(&self, a: Address) -> Option<&PerRegionRemset> {
        let slot = side_metadata::address_to_meta_address(&Self::REMSET_TABLE, a);
        let slot = unsafe { &*slot.to_ptr::<AtomicPtr<PerRegionRemset>>() };
        let ptr = slot.load(Ordering::Relaxed);
        if (ptr as usize) != 0 && (ptr as usize) != 1 {
            return Some(unsafe { &*ptr });
        }
        None
    }

    #[inline]
    fn clear(&self, a: Address) {
        let slot = side_metadata::address_to_meta_address(&Self::REMSET_TABLE, a);
        unsafe { slot.store(0usize) }
    }

    pub fn schedule_all<VM: VMBinding>(&self, scheduler: &GCWorkScheduler<VM>) {
        // let mut remsets: Vec<Box<dyn GCWork<VM>>> = vec![];
        // while let Some(rs) = self.all_remsets.pop() {
        //     let prrs = unsafe { &*rs };
        //     prrs.flush();
        //     while let Some(remset) = prrs.remsets.pop() {
        //         remsets.push(box ProcessRemSet::new(remset))
        //     }
        // }
        // scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].bulk_add(remsets);
        scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].add(SchduleEvacuation);
    }
}

struct SchduleEvacuation;

impl<VM: VMBinding> GCWork<VM> for SchduleEvacuation {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut remsets: Vec<Box<dyn GCWork<VM>>> = vec![];
        while let Some(rs) = REMSETS.all_remsets.pop() {
            let prrs = unsafe { &*rs };
            prrs.flush();
            REMSETS.clear(prrs.region);
            while let Some(remset) = prrs.remsets.pop() {
                remsets.push(box ProcessRemSet::new(remset))
            }
            let _ = unsafe { Box::from_raw(rs) };
        }
        mmtk.scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].bulk_add(remsets);
    }
}
