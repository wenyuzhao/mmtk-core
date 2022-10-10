use atomic::Ordering;

use crate::plan::ObjectQueue;
use crate::plan::PlanConstraints;
use crate::plan::VectorObjectQueue;
use crate::policy::sft::GCWorkerMutRef;
use crate::policy::sft::SFT;
use crate::policy::space::SpaceOptions;
use crate::policy::space::{CommonSpace, Space};
use crate::scheduler::GCWork;
use crate::scheduler::GCWorker;
use crate::util::constants::BYTES_IN_PAGE;
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::util::heap::layout::heap_layout::{Mmapper, VMMap};
use crate::util::heap::HeapMeta;
use crate::util::heap::{FreeListPageResource, PageResource, VMRequest};
use crate::util::metadata;
use crate::util::metadata::side_metadata::spec_defs::LOS_PAGE_VALIDITY;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::metadata::MetadataSpec;
use crate::util::opaque_pointer::*;
use crate::util::rc;
use crate::util::treadmill::TreadMill;
use crate::util::{Address, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use crate::LazySweepingJobsCounter;
use crossbeam::queue::SegQueue;
use spin::Mutex;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;

#[allow(unused)]
const PAGE_MASK: usize = !(BYTES_IN_PAGE - 1);
const MARK_BIT: u8 = 0b01;
const NURSERY_BIT: u8 = 0b10;
const LOS_BIT_MASK: u8 = 0b11;

/// This type implements a policy for large objects. Each instance corresponds
/// to one Treadmill space.
pub struct LargeObjectSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pr: FreeListPageResource<VM>,
    mark_state: u8,
    in_nursery_gc: bool,
    treadmill: TreadMill,
    trace_in_progress: bool,
    rc_nursery_objects: SegQueue<ObjectReference>,
    rc_mature_objects: Mutex<HashSet<ObjectReference>>,
    rc_dead_objects: SegQueue<ObjectReference>,
    pub num_pages_released_lazy: AtomicUsize,
    pub rc_enabled: bool,
}

impl<VM: VMBinding> SFT for LargeObjectSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }
    fn is_live(&self, object: ObjectReference) -> bool {
        if self.rc_enabled {
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
        if self.rc_enabled {
            debug_assert!(alloc);
            // Add to object set
            self.rc_nursery_objects.push(object);
            // Initialize mark bit
            let old_value = unsafe {
                VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.load::<VM, u8>(object, None)
            };
            let new_value = (old_value & (!LOS_BIT_MASK)) | self.mark_state;
            VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.store_atomic::<VM, u8>(
                object,
                new_value,
                None,
                Ordering::SeqCst,
            );
            // CM: Alloc as marked
            // TODO: Only mark during concurrent marking
            self.test_and_mark(object, self.mark_state);
            self.update_validity(
                object.to_address(),
                (bytes + (BYTES_IN_PAGE - 1)) >> LOG_BYTES_IN_PAGE,
            );
            return;
        }
        let old_value = VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.load_atomic::<VM, u8>(
            object,
            None,
            Ordering::SeqCst,
        );
        let mut new_value = (old_value & (!LOS_BIT_MASK)) | self.mark_state;
        if alloc {
            new_value |= NURSERY_BIT;
        }
        VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.store_atomic::<VM, u8>(
            object,
            new_value,
            None,
            Ordering::SeqCst,
        );

        // If this object is freshly allocated, we do not set it as unlogged
        // if !alloc && self.common.needs_log_bit {
        //     VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.mark_as_unlogged::<VM>(object, Ordering::SeqCst);
        // }
        // TODO: Only mark during concurrent marking
        self.test_and_mark(object, self.mark_state);
        #[cfg(feature = "global_alloc_bit")]
        crate::util::alloc_bit::set_alloc_bit(object);
        let cell = VM::VMObjectModel::object_start_ref(object);
        self.treadmill.add_to_treadmill(cell, alloc);
    }
    #[inline(always)]
    fn sft_trace_object(
        &self,
        queue: &mut VectorObjectQueue,
        object: ObjectReference,
        _worker: GCWorkerMutRef,
    ) -> ObjectReference {
        self.trace_object(queue, object)
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

    fn initialize_sft(&self) {
        self.common().initialize_sft(self.as_sft())
    }

    #[inline(always)]
    fn common(&self) -> &CommonSpace<VM> {
        &self.common
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        unreachable!()
    }
}

use crate::util::copy::CopySemantics;

impl<VM: VMBinding> crate::policy::gc_work::PolicyTraceObject<VM> for LargeObjectSpace<VM> {
    #[inline(always)]
    fn trace_object<Q: ObjectQueue, const KIND: crate::policy::gc_work::TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        _copy: Option<CopySemantics>,
        _worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        self.trace_object(queue, object)
    }
    #[inline(always)]
    fn may_move_objects<const KIND: crate::policy::gc_work::TraceKind>() -> bool {
        false
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
            rc_enabled: false,
        }
    }

    #[inline(always)]
    fn update_validity(&self, start: Address, pages: usize) {
        if !crate::REMSET_RECORDING.load(Ordering::SeqCst) {
            LOS_PAGE_VALIDITY.bzero_metadata(start, pages << LOG_BYTES_IN_PAGE);
            return;
        }
        let mut has_invalid_state = false;
        for i in 0..pages {
            let page = start + (i << LOG_BYTES_IN_PAGE);
            unsafe {
                let old = LOS_PAGE_VALIDITY.load::<u8>(page);
                has_invalid_state = has_invalid_state || (old >= u8::MAX);
                LOS_PAGE_VALIDITY.store(page, old + 1);
            }
        }
        assert!(!has_invalid_state, "Over 255 RC pauses during SATB");
    }

    #[inline(always)]
    pub fn currrent_validity_state(e: Address) -> u8 {
        let page = e.align_down(BYTES_IN_PAGE);
        unsafe { LOS_PAGE_VALIDITY.load::<u8>(page) }
    }

    #[inline(always)]
    pub fn pointer_is_valid(&self, e: Address, epoch: u8) -> bool {
        let page = e.align_down(BYTES_IN_PAGE);
        let recorded = unsafe { LOS_PAGE_VALIDITY.load::<u8>(page) };
        epoch == recorded
    }

    #[inline]
    fn release_object(&self, start: Address) -> usize {
        if crate::args::BARRIER_MEASUREMENT
            || (self.common.needs_log_bit && self.common.needs_field_log_bit)
        {
            if self.rc_enabled {
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
        if self.rc_enabled {
            return;
        }
        self.treadmill.flip(full_heap);
        self.in_nursery_gc = !full_heap;
        self.num_pages_released_lazy.store(0, Ordering::Relaxed);
    }

    pub fn release(&mut self, full_heap: bool) {
        self.trace_in_progress = false;
        if self.rc_enabled {
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
    pub fn trace_object<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
    ) -> ObjectReference {
        #[cfg(feature = "global_alloc_bit")]
        debug_assert!(
            crate::util::alloc_bit::is_alloced(object),
            "{:x}: alloc bit not set",
            object
        );
        if self.rc_enabled {
            if self.test_and_mark(object, self.mark_state) {
                queue.enqueue(object);
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
                if !self.rc_enabled && nursery_object && self.common.needs_log_bit {
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
                queue.enqueue(object);
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
    fn test_and_mark(&self, object: ObjectReference, value: u8) -> bool {
        loop {
            let mask = if self.rc_enabled {
                MARK_BIT
            } else if self.in_nursery_gc {
                LOS_BIT_MASK
            } else {
                MARK_BIT
            };
            let old_value = VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.load_atomic::<VM, u8>(
                object,
                None,
                Ordering::SeqCst,
            );
            let mark_bit = old_value & mask;
            if mark_bit == value {
                return false;
            }
            if VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC
                .compare_exchange_metadata::<VM, u8>(
                    object,
                    old_value,
                    old_value & !LOS_BIT_MASK | value,
                    None,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break;
            }
        }
        true
    }

    fn test_mark_bit(&self, object: ObjectReference, value: u8) -> bool {
        VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.load_atomic::<VM, u8>(
            object,
            None,
            Ordering::SeqCst,
        ) & MARK_BIT
            == value
    }

    /// Check if a given object is in nursery
    fn is_in_nursery(&self, object: ObjectReference) -> bool {
        VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.load_atomic::<VM, u8>(
            object,
            None,
            Ordering::Relaxed,
        ) & NURSERY_BIT
            == NURSERY_BIT
    }

    /// Move a given object out of nursery
    fn clear_nursery(&self, object: ObjectReference) {
        loop {
            let old_val = VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.load_atomic::<VM, u8>(
                object,
                None,
                Ordering::Relaxed,
            );
            let new_val = old_val & !NURSERY_BIT;
            if VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC
                .compare_exchange_metadata::<VM, u8>(
                    object,
                    old_val,
                    new_val,
                    None,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
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
                rc::set(*o, 0);
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
                let pages = los.release_object(o.to_address());
                los.num_pages_released_lazy
                    .fetch_add(pages, Ordering::Relaxed);
            } else {
                // println!("keep los {:?}", o);
            }
        }
    }
}
