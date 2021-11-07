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
use crossbeam_queue::SegQueue;
use std::{marker::PhantomData, ops::Range, sync::atomic::AtomicPtr};

use super::{block::Block, line::Line, ImmixSpace};

const LOG_BLOCKS_IN_REGION: usize = 4;
const BLOCKS_IN_REGION: usize = 1 << LOG_BLOCKS_IN_REGION;
pub const LOG_BYTES_IN_REGION: usize = LOG_BLOCKS_IN_REGION + Block::LOG_BYTES;
const BYTES_IN_REGION: usize = 1 << LOG_BYTES_IN_REGION;

pub struct ProcessRemSet<VM: VMBinding> {
    remset: Vec<Address>,
    new_remset: Vec<Address>,
    immix: *const Immix<VM>,
    worker: *mut GCWorker<VM>,
    _p: PhantomData<VM>,
}

unsafe impl<VM: VMBinding> Send for ProcessRemSet<VM> {}

impl<VM: VMBinding> ProcessRemSet<VM> {
    pub fn new(remset: Vec<Address>) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        debug_assert!(crate::args::RC_MATURE_EVACUATION2);
        Self {
            remset,
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
                if self.new_remset.len() >= LocalPerRegionRemSet::CAPACITY {
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
                .add_work(WorkBucketStage::RCEvacuateMature, Self::new(remset));
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
        if !self.remset.is_empty() {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.remset);
            for e in remset {
                self.forward_edge(e);
            }
        }
        self.flush()
    }
}

pub struct PerRegionRemset {
    region: Address,
    remsets: SegQueue<Vec<Address>>,
}

impl PerRegionRemset {
    pub fn new(region: Address) -> Self {
        Self {
            region,
            remsets: SegQueue::new(),
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

    #[inline]
    fn address_in_collection_set(&self, a: Address) -> bool {
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
    fn clear(&self, a: Address) {
        let slot = side_metadata::address_to_meta_address(&Self::REMSET_TABLE, a);
        unsafe { slot.store(0usize) }
    }

    pub fn schedule_all<VM: VMBinding>(&self, scheduler: &GCWorkScheduler<VM>) {
        scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].add(SchduleEvacuation);
    }
}

struct SchduleEvacuation;

impl<VM: VMBinding> GCWork<VM> for SchduleEvacuation {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        FlushWorkerRemSet::schedule(&mmtk.scheduler);
        let mut remsets: Vec<Box<dyn GCWork<VM>>> = vec![];
        while let Some(rs) = REMSETS.all_remsets.pop() {
            let prrs = unsafe { &*rs };
            REMSETS.clear(prrs.region);
            while let Some(remset) = prrs.remsets.pop() {
                remsets.push(box ProcessRemSet::new(remset))
            }
            let _ = unsafe { Box::from_raw(rs) };
        }
        mmtk.scheduler.work_buckets[WorkBucketStage::RCEvacuateMature].bulk_add(remsets);
    }
}

pub struct LocalPerRegionRemSet {
    region: Address,
    remset: Vec<Address>,
}

impl LocalPerRegionRemSet {
    const CAPACITY: usize = 4096;

    pub fn new(region: Address) -> Self {
        Self {
            region,
            remset: vec![],
        }
    }

    fn is_empty(&self) -> bool {
        self.remset.is_empty()
    }

    fn add(&mut self, a: Address) {
        self.remset.push(a);
        if self.remset.len() >= Self::CAPACITY {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if !self.remset.is_empty() {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.remset);
            REMSETS.get(self.region).remsets.push(remset)
        }
    }

    fn flush_to_scheduler<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>) {
        if !self.remset.is_empty() {
            let mut remset = vec![];
            std::mem::swap(&mut remset, &mut self.remset);
            worker.add_work(
                WorkBucketStage::RCEvacuateMature,
                ProcessRemSet::new(remset),
            );
        }
    }
}

pub struct LocalRemSet {
    base: Address,
    table: Vec<Option<LocalPerRegionRemSet>>,
    remset_list: Vec<usize>,
}

impl LocalRemSet {
    pub fn new(base: Address) -> Self {
        Self {
            base,
            table: vec![],
            remset_list: vec![],
        }
    }

    #[cold]
    fn record_slow(&mut self, e: Address, o: ObjectReference) {
        let region = o.to_address().align_down(BYTES_IN_REGION);
        let index = (o.to_address() - self.base) >> LOG_BYTES_IN_REGION;
        if index >= self.table.len() {
            self.table.resize_with(1 + index << 1, || None);
        }
        if self.table[index].is_none() {
            self.table[index] = Some(LocalPerRegionRemSet::new(region));
        }
        let remset = self.table[index].as_mut().unwrap();
        if remset.is_empty() {
            self.remset_list.push(index);
        }
        remset.add(e)
    }

    #[inline]
    pub fn record(&mut self, e: Address, o: ObjectReference) {
        if (e.as_usize() ^ o.to_address().as_usize()) >> LOG_BYTES_IN_REGION == 0 {
            return;
        }
        if !REMSETS.address_in_collection_set(e) && REMSETS.in_collection_set(o) {
            self.record_slow(e, o);
        }
    }

    pub fn flush(&mut self) {
        while let Some(i) = self.remset_list.pop() {
            let entry = self.table[i].as_mut().unwrap();
            entry.flush();
        }
    }

    fn flush_to_scheduler<VM: VMBinding>(&mut self, worker: &mut GCWorker<VM>) {
        while let Some(i) = self.remset_list.pop() {
            let entry = self.table[i].as_mut().unwrap();
            entry.flush_to_scheduler(worker);
        }
    }
}

pub struct FlushWorkerRemSet;

impl FlushWorkerRemSet {
    pub fn schedule<VM: VMBinding>(scheduler: &GCWorkScheduler<VM>) {
        for w in &scheduler.worker_group().workers {
            w.local_work_bucket.add(FlushWorkerRemSet);
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for FlushWorkerRemSet {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let w = unsafe { &mut *(worker as *mut _) };
        unsafe { worker.local::<ImmixCopyContext<VM>>() }
            .remset
            .flush_to_scheduler(w);
    }
}
