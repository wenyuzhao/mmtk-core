use crate::plan::immix::Immix;
use crate::plan::EdgeIterator;
use crate::plan::PlanConstraints;
use crate::plan::TransitiveClosure;
use crate::policy::space::SpaceOptions;
use crate::policy::space::{CommonSpace, Space, SFT};
use crate::scheduler::GCWork;
use crate::scheduler::GCWorker;
use crate::scheduler::WorkBucketStage;
use crate::util::constants::BYTES_IN_PAGE;
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::util::heap::layout::heap_layout::{Mmapper, VMMap};
use crate::util::heap::HeapMeta;
use crate::util::heap::{FreeListPageResource, PageResource, VMRequest};
use crate::util::metadata;
use crate::util::metadata::compare_exchange_metadata;
use crate::util::metadata::load_metadata;
use crate::util::metadata::side_metadata;
use crate::util::metadata::side_metadata::spec_defs::LOS_PAGE_VALIDITY;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::metadata::store_metadata;
use crate::util::metadata::MetadataSpec;
use crate::util::opaque_pointer::*;
use crate::util::rc;
use crate::util::rc::ProcessDecs;
use crate::util::treadmill::TreadMill;
use crate::util::{Address, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use crate::LazySweepingJobsCounter;
use atomic::Ordering;
use crossbeam_queue::SegQueue;
use spin::Mutex;
use std::collections::HashSet;
use std::intrinsics::unlikely;
use std::sync::atomic::AtomicUsize;

#[allow(unused)]
const PAGE_MASK: usize = !(BYTES_IN_PAGE - 1);
const MARK_BIT: usize = 0b01;
const NURSERY_BIT: usize = 0b10;
const LOS_BIT_MASK: usize = 0b11;

/// This type implements a policy for large objects. Each instance corresponds
/// to one Treadmill space.
pub struct LargeObjectSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pr: FreeListPageResource<VM>,
    mark_state: usize,
    in_nursery_gc: bool,
    treadmill: TreadMill,
    trace_in_progress: bool,
    rc_nursery_objects: SegQueue<ObjectReference>,
    rc_mature_objects: Mutex<HashSet<ObjectReference>>,
    rc_dead_objects: SegQueue<ObjectReference>,
    pub num_pages_released_lazy: AtomicUsize,
}

impl<VM: VMBinding> SFT for LargeObjectSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }
    fn is_live(&self, object: ObjectReference) -> bool {
        if crate::args::REF_COUNT {
            return crate::util::rc::count(object) > 0;
        }
        if self.trace_in_progress {
            return true;
        }
        self.test_mark_bit(object, self.mark_state)
    }
    fn is_movable(&self) -> bool {
        false
    }
    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        true
    }
    fn initialize_object_metadata(&self, object: ObjectReference, bytes: usize, alloc: bool) {
        if crate::args::REF_COUNT {
            debug_assert!(alloc);
            // Add to object set
            self.rc_nursery_objects.push(object);
            // Initialize mark bit
            let old_value = load_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
                object,
                None,
                None,
            );
            let new_value = (old_value & (!LOS_BIT_MASK)) | self.mark_state;
            store_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
                object,
                new_value,
                None,
                Some(Ordering::SeqCst),
            );
            // CM: Alloc as marked
            if crate::args::CONCURRENT_MARKING && crate::concurrent_marking_in_progress() {
                self.test_and_mark(object, self.mark_state);
            }
            self.update_validity(
                object.to_address(),
                (bytes + (BYTES_IN_PAGE - 1)) >> LOG_BYTES_IN_PAGE,
            );
            return;
        }
        let old_value = load_metadata::<VM>(
            &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
            object,
            None,
            Some(Ordering::SeqCst),
        );
        let mut new_value = (old_value & (!LOS_BIT_MASK)) | self.mark_state;
        if alloc {
            new_value |= NURSERY_BIT;
        }
        store_metadata::<VM>(
            &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
            object,
            new_value,
            None,
            Some(Ordering::SeqCst),
        );

        // If this object is freshly allocated, we do not set it as unlogged
        // if !alloc && self.common.needs_log_bit {
        //     VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.mark_as_unlogged::<VM>(object, Ordering::SeqCst);
        // }

        // Concurrent marking: allocate as marked
        if crate::args::CONCURRENT_MARKING && crate::concurrent_marking_in_progress() {
            self.test_and_mark(object, self.mark_state);
        }
        #[cfg(feature = "global_alloc_bit")]
        crate::util::alloc_bit::set_alloc_bit(object);
        let cell = VM::VMObjectModel::object_start_ref(object);
        self.treadmill.add_to_treadmill(cell, alloc);
    }
}

impl<VM: VMBinding> Space<VM> for LargeObjectSpace<VM> {
    fn as_space(&self) -> &dyn Space<VM> {
        self
    }
    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }
    #[inline(always)]
    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        &self.pr
    }
    fn init(&mut self, _vm_map: &'static VMMap) {
        self.common().init(self.as_space());
    }

    #[inline(always)]
    fn common(&self) -> &CommonSpace<VM> {
        &self.common
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        unreachable!()
    }
}

impl<VM: VMBinding> LargeObjectSpace<VM> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &'static str,
        zeroed: bool,
        vmrequest: VMRequest,
        global_side_metadata_specs: Vec<SideMetadataSpec>,
        vm_map: &'static VMMap,
        mmapper: &'static Mmapper,
        heap: &mut HeapMeta,
        constraints: &'static PlanConstraints,
        protect_memory_on_release: bool,
    ) -> Self {
        let common = CommonSpace::new(
            SpaceOptions {
                name,
                movable: false,
                immortal: false,
                zeroed,
                needs_log_bit: constraints.needs_log_bit,
                needs_field_log_bit: constraints.needs_field_log_bit,
                vmrequest,
                side_metadata_specs: SideMetadataContext {
                    global: global_side_metadata_specs,
                    local: metadata::extract_side_metadata(&[
                        *VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
                        MetadataSpec::OnSide(LOS_PAGE_VALIDITY),
                    ]),
                },
            },
            vm_map,
            mmapper,
            heap,
        );
        let mut pr = if vmrequest.is_discontiguous() {
            FreeListPageResource::new_discontiguous(0, vm_map)
        } else {
            FreeListPageResource::new_contiguous(common.start, common.extent, 0, vm_map)
        };
        pr.protect_memory_on_release = protect_memory_on_release;
        LargeObjectSpace {
            pr,
            common,
            mark_state: 0,
            in_nursery_gc: false,
            treadmill: TreadMill::new(),
            trace_in_progress: false,
            rc_nursery_objects: Default::default(),
            rc_mature_objects: Default::default(),
            rc_dead_objects: Default::default(),
            num_pages_released_lazy: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn update_validity(&self, start: Address, pages: usize) {
        if !crate::REMSET_RECORDING.load(Ordering::SeqCst) {
            side_metadata::bzero_x(&LOS_PAGE_VALIDITY, start, pages << LOG_BYTES_IN_PAGE);
            return;
        }
        for i in 0..pages {
            let page = start + (i << LOG_BYTES_IN_PAGE);
            unsafe {
                let old = side_metadata::load(&LOS_PAGE_VALIDITY, page);
                debug_assert_ne!(old, 255);
                side_metadata::store(&LOS_PAGE_VALIDITY, page, old + 1);
            }
        }
    }

    #[inline(always)]
    pub fn currrent_validity_state(e: Address) -> u8 {
        let page = e.align_down(BYTES_IN_PAGE);
        unsafe { side_metadata::load(&LOS_PAGE_VALIDITY, page) as u8 }
    }

    #[inline(always)]
    pub fn pointer_is_valid(&self, e: Address, epoch: u8) -> bool {
        let page = e.align_down(BYTES_IN_PAGE);
        let recorded = unsafe { side_metadata::load(&LOS_PAGE_VALIDITY, page) as u8 };
        epoch == recorded
    }

    #[inline]
    fn release_object(&self, start: Address) -> usize {
        if crate::args::BARRIER_MEASUREMENT
            || (self.common.needs_log_bit && self.common.needs_field_log_bit)
        {
            if crate::args::REF_COUNT {
                rc::set(unsafe { start.to_object_reference() }, 0);
            }
            self.pr.release_pages_and_reset_unlog_bits(start)
        } else {
            self.pr.release_pages(start)
        }
    }

    pub fn prepare(&mut self, full_heap: bool) {
        self.trace_in_progress = true;
        if full_heap {
            debug_assert!(self.treadmill.is_from_space_empty());
            self.mark_state = MARK_BIT - self.mark_state;
        }
        if crate::args::REF_COUNT {
            return;
        }
        self.treadmill.flip(full_heap);
        self.in_nursery_gc = !full_heap;
        self.num_pages_released_lazy.store(0, Ordering::Relaxed);
    }

    pub fn release(&mut self, full_heap: bool) {
        self.trace_in_progress = false;
        if crate::args::REF_COUNT {
            // promote nursery objects or release dead nursery
            let mut mature_blocks = self.rc_mature_objects.lock();
            while let Some(o) = self.rc_nursery_objects.pop() {
                if rc::count(o) == 0 {
                    self.release_object(o.to_address());
                } else {
                    mature_blocks.insert(o);
                }
            }
            return;
        }
        self.sweep_large_pages(true);
        debug_assert!(self.treadmill.is_nursery_empty());
        if full_heap {
            self.sweep_large_pages(false);
        }
    }
    // Allow nested-if for this function to make it clear that test_and_mark() is only executed
    // for the outer condition is met.
    #[allow(clippy::collapsible_if)]
    pub fn trace_object<T: TransitiveClosure>(
        &self,
        trace: &mut T,
        object: ObjectReference,
    ) -> ObjectReference {
        #[cfg(feature = "global_alloc_bit")]
        debug_assert!(
            crate::util::alloc_bit::is_alloced(object),
            "{:x}: alloc bit not set",
            object
        );
        if crate::args::REF_COUNT {
            if self.test_and_mark(object, self.mark_state) {
                trace.process_node(object);
            }
            return object;
        }
        let nursery_object = self.is_in_nursery(object);
        if !self.in_nursery_gc || nursery_object {
            // Note that test_and_mark() has side effects
            if self.test_and_mark(object, self.mark_state) {
                let cell = VM::VMObjectModel::object_start_ref(object);
                self.treadmill.copy(cell, nursery_object);
                self.clear_nursery(object);
                // We just moved the object out of the logical nursery, mark it as unlogged.
                if !crate::args::REF_COUNT && nursery_object && self.common.needs_log_bit {
                    if self.common.needs_field_log_bit {
                        for i in (0..object.get_size::<VM>()).step_by(8) {
                            let a = object.to_address() + i;
                            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.mark_as_unlogged::<VM>(
                                unsafe { a.to_object_reference() },
                                Ordering::SeqCst,
                            );
                        }
                    } else {
                        VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                            .mark_as_unlogged::<VM>(object, Ordering::SeqCst);
                    }
                }
                trace.process_node(object);
            }
        }
        object
    }

    fn sweep_large_pages(&mut self, sweep_nursery: bool) {
        // FIXME: borrow checker fighting
        // didn't call self.release_multiple_pages
        // so the compiler knows I'm borrowing two different fields
        if sweep_nursery {
            for cell in self.treadmill.collect_nursery() {
                // println!("- cn {}", cell);
                #[cfg(feature = "global_alloc_bit")]
                crate::util::alloc_bit::unset_addr_alloc_bit(cell);
                self.release_object(get_super_page(cell));
            }
        } else {
            for cell in self.treadmill.collect() {
                // println!("- ts {}", cell);
                #[cfg(feature = "global_alloc_bit")]
                crate::util::alloc_bit::unset_addr_alloc_bit(cell);
                self.release_object(get_super_page(cell));
            }
        }
    }

    /// Allocate an object
    pub fn allocate_pages(&self, tls: VMThread, pages: usize) -> Address {
        self.acquire(tls, pages)
    }

    #[inline]
    pub fn attempt_mark(&self, object: ObjectReference) -> bool {
        self.test_and_mark(object, self.mark_state)
    }

    #[inline]
    pub fn rc_free(&self, o: ObjectReference) {
        if o.to_address().attempt_log::<VM>() {
            // println!(" - add to rc_dead_objects {:?}", o);
            self.rc_dead_objects.push(o);
        }
    }

    #[inline(always)]
    pub fn is_marked(&self, object: ObjectReference) -> bool {
        self.test_mark_bit(object, self.mark_state)
    }

    #[inline(always)]
    fn test_and_mark(&self, object: ObjectReference, value: usize) -> bool {
        loop {
            let mask = if crate::args::REF_COUNT {
                MARK_BIT
            } else if self.in_nursery_gc {
                LOS_BIT_MASK
            } else {
                MARK_BIT
            };
            let old_value = load_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
                object,
                None,
                Some(Ordering::SeqCst),
            );
            let mark_bit = old_value & mask;
            if mark_bit == value {
                return false;
            }
            if compare_exchange_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
                object,
                old_value,
                old_value & !LOS_BIT_MASK | value,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                break;
            }
        }
        true
    }

    fn test_mark_bit(&self, object: ObjectReference, value: usize) -> bool {
        load_metadata::<VM>(
            &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
            object,
            None,
            Some(Ordering::SeqCst),
        ) & MARK_BIT
            == value
    }

    /// Check if a given object is in nursery
    fn is_in_nursery(&self, object: ObjectReference) -> bool {
        load_metadata::<VM>(
            &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
            object,
            None,
            Some(Ordering::Relaxed),
        ) & NURSERY_BIT
            == NURSERY_BIT
    }

    /// Move a given object out of nursery
    fn clear_nursery(&self, object: ObjectReference) {
        loop {
            let old_val = load_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
                object,
                None,
                Some(Ordering::Relaxed),
            );
            let new_val = old_val & !NURSERY_BIT;
            if compare_exchange_metadata::<VM>(
                &VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC,
                object,
                old_val,
                new_val,
                None,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                break;
            }
        }
    }
}

fn get_super_page(cell: Address) -> Address {
    cell.align_down(BYTES_IN_PAGE)
}

pub struct RCSweepMatureLOS<VM: VMBinding> {
    counter: LazySweepingJobsCounter,
    immix: *const Immix<VM>,
    worker: *mut GCWorker<VM>,
    recursive_dec_objects: Vec<ObjectReference>,
}

impl<VM: VMBinding> RCSweepMatureLOS<VM> {
    pub fn new(counter: LazySweepingJobsCounter) -> Self {
        Self {
            counter,
            recursive_dec_objects: vec![],
            immix: std::ptr::null_mut(),
            worker: std::ptr::null_mut(),
        }
    }

    #[inline(always)]
    fn worker(&self) -> &'static mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline(always)]
    const fn immix(&self) -> &Immix<VM> {
        unsafe { &*self.immix }
    }

    #[cold]
    fn flush_recursive_dec_objects(&mut self, end: bool) {
        if self.recursive_dec_objects.is_empty() {
            return;
        }
        let mut recursive_dec_objects = vec![];
        std::mem::swap(&mut recursive_dec_objects, &mut self.recursive_dec_objects);
        let immix = self.immix();
        let w = ProcessDecs::new(recursive_dec_objects, self.counter.clone_with_decs());
        if end {
            self.worker().do_work(w)
        } else if immix.current_pause().is_none() {
            self.worker()
                .add_work_prioritized(WorkBucketStage::Unconstrained, w);
        } else {
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
        }
    }

    #[inline(never)]
    fn scan_and_recursively_dec_objects(&mut self, dead: ObjectReference) {
        EdgeIterator::<VM>::iterate(dead, |edge| {
            let mut x = unsafe { edge.load::<ObjectReference>() };
            if !x.is_null() && !rc::is_dead(x) && self.immix().is_marked(x) {
                debug_assert!(x.is_mapped());
                x = x.fix_start_address::<VM>();
                debug_assert!(x.class_is_valid());
                if unlikely(self.recursive_dec_objects.is_empty()) {
                    self.recursive_dec_objects
                        .reserve(ProcessDecs::<VM>::CAPACITY);
                }
                self.recursive_dec_objects.push(x);
                if self.recursive_dec_objects.len() >= ProcessDecs::<VM>::CAPACITY {
                    self.flush_recursive_dec_objects(false);
                }
            }
        });
    }
}
unsafe impl<VM: VMBinding> Send for RCSweepMatureLOS<VM> {}

impl<VM: VMBinding> GCWork<VM> for RCSweepMatureLOS<VM> {
    fn do_work(
        &mut self,
        worker: &mut crate::scheduler::GCWorker<VM>,
        mmtk: &'static crate::MMTK<VM>,
    ) {
        self.worker = worker;
        self.immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        let los = mmtk.plan.common().get_los();
        let mature_objects = los.rc_mature_objects.lock();
        for o in mature_objects.iter() {
            if !los.is_marked(*o) && rc::count(*o) != 0 {
                crate::stat(|s| {
                    s.dead_mature_objects += 1;
                    s.dead_mature_volume += o.get_size::<VM>();
                    s.dead_mature_los_objects += 1;
                    s.dead_mature_los_volume += o.get_size::<VM>();

                    s.dead_mature_tracing_objects += 1;
                    s.dead_mature_tracing_volume += o.get_size::<VM>();
                    s.dead_mature_tracing_los_objects += 1;
                    s.dead_mature_tracing_los_volume += o.get_size::<VM>();

                    if rc::rc_stick(*o) {
                        s.dead_mature_tracing_stuck_objects += 1;
                        s.dead_mature_tracing_stuck_volume += o.get_size::<VM>();
                        s.dead_mature_tracing_stuck_los_objects += 1;
                        s.dead_mature_tracing_stuck_los_volume += o.get_size::<VM>();
                    }
                });
                if crate::args::SATB_SWEEP_APPLY_DECS {
                    self.scan_and_recursively_dec_objects(*o);
                }
                rc::set(*o, 0);
                los.rc_free(*o);
            }
        }
        if crate::args::SATB_SWEEP_APPLY_DECS {
            self.flush_recursive_dec_objects(true);
        }
    }
}

pub struct RCReleaseMatureLOS {
    _counter: LazySweepingJobsCounter,
}

impl RCReleaseMatureLOS {
    pub fn new(counter: LazySweepingJobsCounter) -> Self {
        Self { _counter: counter }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCReleaseMatureLOS {
    fn do_work(
        &mut self,
        _worker: &mut crate::scheduler::GCWorker<VM>,
        mmtk: &'static crate::MMTK<VM>,
    ) {
        let los = mmtk.plan.common().get_los();
        let mut mature_objects = los.rc_mature_objects.lock();
        while let Some(o) = los.rc_dead_objects.pop() {
            let removed = mature_objects.remove(&o);
            o.to_address().unlog::<VM>();
            if removed {
                // println!("kill los {:?}", o);
                let pages = los.release_object(o.to_address());
                los.num_pages_released_lazy
                    .fetch_add(pages, Ordering::Relaxed);
            } else {
                // println!("keep los {:?}", o);
            }
        }
    }
}
