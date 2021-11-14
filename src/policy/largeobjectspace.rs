use crate::plan::PlanConstraints;
use crate::plan::TransitiveClosure;
use crate::policy::space::SpaceOptions;
use crate::policy::space::{CommonSpace, Space, SFT};
use crate::scheduler::GCWork;
use crate::util::constants::BYTES_IN_PAGE;
use crate::util::heap::layout::heap_layout::{Mmapper, VMMap};
use crate::util::heap::HeapMeta;
use crate::util::heap::{FreeListPageResource, PageResource, VMRequest};
use crate::util::metadata;
use crate::util::metadata::compare_exchange_metadata;
use crate::util::metadata::load_metadata;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::metadata::store_metadata;
use crate::util::opaque_pointer::*;
use crate::util::rc;
use crate::util::treadmill::TreadMill;
use crate::util::{Address, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use crate::LazySweepingJobsCounter;
use atomic::Ordering;
use crossbeam_queue::SegQueue;
use spin::Mutex;
use std::collections::HashSet;

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
    fn initialize_object_metadata(&self, object: ObjectReference, _bytes: usize, alloc: bool) {
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
        }
    }

    #[inline]
    fn release_object(&self, start: Address) {
        if crate::args::BARRIER_MEASUREMENT
            || (self.common.needs_log_bit && self.common.needs_field_log_bit)
        {
            if crate::args::REF_COUNT {
                rc::set(unsafe { start.to_object_reference() }, 0);
            }
            self.pr.release_pages_and_reset_unlog_bits(start);
        } else {
            self.pr.release_pages(start);
        }
    }

    pub fn prepare(&mut self, full_heap: bool) {
        self.trace_in_progress = true;
        if full_heap {
            debug_assert!(self.treadmill.from_space_empty());
            self.mark_state = MARK_BIT - self.mark_state;
        }
        if crate::args::REF_COUNT {
            return;
        }
        self.treadmill.flip(full_heap);
        self.in_nursery_gc = !full_heap;
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
        debug_assert!(self.treadmill.nursery_empty());
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
            let mask = MARK_BIT;
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

pub struct RCSweepMatureLOS {
    _counter: LazySweepingJobsCounter,
}

impl RCSweepMatureLOS {
    pub fn new(counter: LazySweepingJobsCounter) -> Self {
        Self { _counter: counter }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCSweepMatureLOS {
    fn do_work(
        &mut self,
        _worker: &mut crate::scheduler::GCWorker<VM>,
        mmtk: &'static crate::MMTK<VM>,
    ) {
        let los = mmtk.plan.common().get_los();
        let mature_objects = los.rc_mature_objects.lock();
        for o in mature_objects.iter() {
            if !los.is_marked(*o) && rc::count(*o) != 0 {
                crate::stat(|s| {
                    s.dead_objects += 1;
                    s.dead_volume += o.get_size::<VM>();
                    s.dead_los_objects += 1;
                    s.dead_los_volume += o.get_size::<VM>();
                    s.dead_mature_objects += 1;
                    s.dead_mature_volume += o.get_size::<VM>();
                    s.dead_mature_los_objects += 1;
                    s.dead_mature_los_volume += o.get_size::<VM>();
                });
                rc::set(*o, 0);
                // println!("dead los 2 {:?}", o);
                los.rc_free(*o);
            }
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
                los.release_object(o.to_address());
            } else {
                // println!("keep los {:?}", o);
            }
        }
    }
}
