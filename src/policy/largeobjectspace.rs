use atomic::Ordering;

use crate::plan::ObjectQueue;
use crate::plan::VectorObjectQueue;
use crate::policy::sft::GCWorkerMutRef;
use crate::policy::sft::SFT;
use crate::policy::space::{CommonSpace, Space};
use crate::scheduler::GCWork;
use crate::scheduler::GCWorker;
use crate::util::constants::BYTES_IN_PAGE;
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::util::heap::{FreeListPageResource, PageResource};
use crate::util::metadata;
use crate::util::opaque_pointer::*;
use crate::util::rc::RefCountHelper;
use crate::util::treadmill::TreadMill;
use crate::util::{Address, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use crate::LazySweepingJobsCounter;
use crossbeam::queue::SegQueue;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

#[allow(unused)]
const PAGE_MASK: usize = !(BYTES_IN_PAGE - 1);
const MARK_BIT: u8 = 0b01;
const NURSERY_BIT: u8 = 0b10;
const LOS_BIT_MASK: u8 = 0b11;

/// This type implements a policy for large objects. Each instance corresponds
/// to one Treadmill space.
pub struct LargeObjectSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pub(crate) pr: FreeListPageResource<VM>,
    mark_state: u8,
    in_nursery_gc: bool,
    treadmill: TreadMill,
    trace_in_progress: bool,
    rc_nursery_objects: SegQueue<ObjectReference>,
    rc_mature_objects: Mutex<HashMap<ObjectReference, usize>>,
    pub num_pages_released_lazy: AtomicUsize,
    pub rc_killed_bytes: AtomicUsize,
    pub young_alloc_size: AtomicUsize,
    pub rc_enabled: bool,
    rc: RefCountHelper<VM>,
    pub is_end_of_satb_or_full_gc: bool,
}

impl<VM: VMBinding> SFT for LargeObjectSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }
    fn is_live(&self, object: ObjectReference) -> bool {
        if self.rc_enabled {
            if self.is_end_of_satb_or_full_gc {
                return self.is_marked(object) && self.rc.count(object) > 0;
            }
            return self.rc.count(object) > 0;
        }
        if self.trace_in_progress {
            return true;
        }
        self.test_mark_bit(object, self.mark_state)
    }
    fn is_reachable(&self, object: ObjectReference) -> bool {
        if self.rc_enabled {
            self.test_mark_bit(object, self.mark_state) && self.rc.count(object) > 0
        } else {
            self.is_live(object)
        }
    }
    #[cfg(feature = "object_pinning")]
    fn pin_object(&self, _object: ObjectReference) -> bool {
        false
    }
    #[cfg(feature = "object_pinning")]
    fn unpin_object(&self, _object: ObjectReference) -> bool {
        false
    }
    #[cfg(feature = "object_pinning")]
    fn is_object_pinned(&self, _object: ObjectReference) -> bool {
        true
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
            self.young_alloc_size.fetch_add(bytes, Ordering::Relaxed);
            debug_assert!(alloc);
            // Add to object set
            self.rc_nursery_objects.push(object);
            // Initialize mark bit
            self.test_and_mark(object, self.mark_state);
            for off in (0..bytes).step_by(BYTES_IN_PAGE) {
                let a = object.to_raw_address() + off;
                self.test_and_mark(a.to_object_reference::<VM>(), self.mark_state);
            }
            #[cfg(feature = "lxr_srv_ratio_counter")]
            crate::plan::lxr::SURVIVAL_RATIO_PREDICTOR
                .los_alloc_vol
                .fetch_add(bytes, Ordering::SeqCst);
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
        if !alloc && self.common.needs_log_bit {
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.mark_as_unlogged::<VM>(object, Ordering::SeqCst);
        }
        #[cfg(feature = "vo_bit")]
        crate::util::metadata::vo_bit::set_vo_bit::<VM>(object);
        self.treadmill.add_to_treadmill(object, alloc);
    }
    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> bool {
        crate::util::metadata::vo_bit::is_vo_bit_set_for_addr::<VM>(addr).is_some()
    }
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
    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        &self.pr
    }

    fn initialize_sft(&self, sft_map: &mut dyn crate::policy::sft_map::SFTMap) {
        self.common().initialize_sft(
            self.as_sft(),
            sft_map,
            &self.get_page_resource().common().metadata,
        )
    }

    fn common(&self) -> &CommonSpace<VM> {
        &self.common
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        unreachable!()
    }
}

use crate::util::copy::CopySemantics;

impl<VM: VMBinding> crate::policy::gc_work::PolicyTraceObject<VM> for LargeObjectSpace<VM> {
    fn trace_object<Q: ObjectQueue, const KIND: crate::policy::gc_work::TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        _copy: Option<CopySemantics>,
        _worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        self.trace_object(queue, object)
    }
    fn may_move_objects<const KIND: crate::policy::gc_work::TraceKind>() -> bool {
        false
    }
}

impl<VM: VMBinding> LargeObjectSpace<VM> {
    pub fn new(
        args: crate::policy::space::PlanCreateSpaceArgs<VM>,
        protect_memory_on_release: bool,
    ) -> Self {
        let is_discontiguous = args.vmrequest.is_discontiguous();
        let vm_map = args.vm_map;
        let policy_args = args.into_policy_args(
            false,
            false,
            metadata::extract_side_metadata(&[*VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC]),
        );
        let metadata = policy_args.metadata();
        let common = CommonSpace::new(policy_args);
        let mut pr = if is_discontiguous {
            FreeListPageResource::new_discontiguous(vm_map, metadata)
        } else {
            FreeListPageResource::new_contiguous(common.start, common.extent, vm_map, metadata)
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
            num_pages_released_lazy: Default::default(),
            rc_killed_bytes: Default::default(),
            young_alloc_size: Default::default(),
            rc_enabled: false,
            rc: RefCountHelper::NEW,
            is_end_of_satb_or_full_gc: false,
        }
    }

    pub fn dump_memory(&self, lxr: &crate::plan::lxr::LXR<VM>) {
        unreachable!()
    }

    fn release_object(&self, start: Address) -> usize {
        if crate::args::BARRIER_MEASUREMENT
            || (self.common.needs_log_bit && self.common.needs_field_log_bit)
        {
            if self.rc_enabled {
                self.rc.set(start.to_object_reference::<VM>(), 0);
            }
            self.pr.release_pages_and_reset_unlog_bits(start)
        } else {
            self.pr.release_pages(start)
        }
    }

    pub fn release_rc_nursery_objects(&self) {
        debug_assert!(self.rc_enabled);
        // promote nursery objects or release dead nursery
        let mut mature_blocks = self.rc_mature_objects.lock().unwrap();
        while let Some(o) = self.rc_nursery_objects.pop() {
            if self.rc.count(o) == 0 {
                self.release_object(o.to_address::<VM>());
            } else {
                mature_blocks.insert(o, o.get_size::<VM>());
            }
        }
    }

    pub fn prepare(&mut self, full_heap: bool) {
        self.trace_in_progress = true;
        if full_heap {
            debug_assert!(self.treadmill.is_from_space_empty());
            self.mark_state = MARK_BIT - self.mark_state;
        }
        self.num_pages_released_lazy.store(0, Ordering::Relaxed);
        self.rc_killed_bytes.store(0, Ordering::Relaxed);
        self.young_alloc_size.store(0, Ordering::Relaxed);
        if self.rc_enabled {
            return;
        }
        self.treadmill.flip(full_heap);
        self.in_nursery_gc = !full_heap;
    }

    pub fn release(&mut self, full_heap: bool) {
        self.trace_in_progress = false;
        if self.rc_enabled {
            self.release_rc_nursery_objects();
            return;
        }
        self.sweep_large_pages(true);
        debug_assert!(self.treadmill.is_nursery_empty());
        if full_heap {
            self.sweep_large_pages(false);
        }
    }

    pub fn trace_object_rc<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
    ) -> ObjectReference {
        if self.test_and_mark(object, self.mark_state) {
            queue.enqueue(object);
        }
        return object;
    }

    // Allow nested-if for this function to make it clear that test_and_mark() is only executed
    // for the outer condition is met.
    #[allow(clippy::collapsible_if)]
    pub fn trace_object<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
    ) -> ObjectReference {
        #[cfg(feature = "vo_bit")]
        debug_assert!(
            crate::util::metadata::vo_bit::is_vo_bit_set::<VM>(object),
            "{:x}: VO bit not set",
            object
        );
        if self.rc_enabled {
            if self.test_and_mark(object, self.mark_state) {
                queue.enqueue(object);
            }
            return object;
        }
        let nursery_object = self.is_in_nursery(object);
        trace!(
            "LOS object {} {} a nursery object",
            object,
            if nursery_object { "is" } else { "is not" }
        );
        if !self.in_nursery_gc || nursery_object {
            // Note that test_and_mark() has side effects of
            // clearing nursery bit/moving objects out of logical nursery
            if self.test_and_mark(object, self.mark_state) {
                trace!("LOS object {} is being marked now", object);
                self.treadmill.copy(object, nursery_object);
                // We just moved the object out of the logical nursery, mark it as unlogged.
                if !self.rc_enabled
                    && self.common.needs_log_bit
                    && !crate::args::BARRIER_MEASUREMENT_NO_SLOW
                {
                    if self.common.needs_field_log_bit {
                        let step = if VM::VMObjectModel::COMPRESSED_PTR_ENABLED {
                            4
                        } else {
                            8
                        };
                        for i in (0..object.get_size::<VM>()).step_by(step) {
                            let a = object.to_address::<VM>() + i;
                            VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC.mark_as_unlogged::<VM>(
                                a.to_object_reference::<VM>(),
                                Ordering::SeqCst,
                            );
                        }
                    } else {
                        VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                            .mark_as_unlogged::<VM>(object, Ordering::SeqCst);
                    }
                }
                queue.enqueue(object);
            } else {
                trace!(
                    "LOS object {} is not being marked now, it was marked before",
                    object
                );
            }
        }
        object
    }

    fn sweep_large_pages(&mut self, sweep_nursery: bool) {
        let sweep = |object: ObjectReference| {
            #[cfg(feature = "vo_bit")]
            crate::util::metadata::vo_bit::unset_vo_bit::<VM>(object);
            self.release_object(get_super_page(object.to_object_start::<VM>()));
        };
        if sweep_nursery {
            for object in self.treadmill.collect_nursery() {
                sweep(object);
            }
        } else {
            for object in self.treadmill.collect() {
                sweep(object)
            }
        }
    }

    /// Allocate an object
    pub fn allocate_pages(&self, tls: VMThread, pages: usize) -> Address {
        self.acquire(tls, pages)
    }

    pub fn attempt_mark(&self, object: ObjectReference) -> bool {
        self.test_and_mark(object, self.mark_state)
    }

    pub fn rc_free(&self, o: ObjectReference) {
        let mut rc_mature_objects = self.rc_mature_objects.lock().unwrap();
        if rc_mature_objects.remove(&o).is_some() {
            let pages = self.release_object(o.to_address::<VM>());
            self.num_pages_released_lazy
                .fetch_add(pages, Ordering::Relaxed);
        }
    }

    pub fn is_marked(&self, object: ObjectReference) -> bool {
        self.test_mark_bit(object, self.mark_state)
    }

    /// Test if the object's mark bit is the same as the given value. If it is not the same,
    /// the method will attemp to mark the object and clear its nursery bit. If the attempt
    /// succeeds, the method will return true, meaning the object is marked by this invocation.
    /// Otherwise, it returns false.
    fn test_and_mark(&self, object: ObjectReference, value: u8) -> bool {
        let mask = if self.rc_enabled {
            MARK_BIT
        } else if self.in_nursery_gc {
            LOS_BIT_MASK
        } else {
            MARK_BIT
        };
        let result = VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC
            .as_spec()
            .extract_side_spec()
            .fetch_update_atomic::<u8, _>(
                object.to_raw_address(),
                Ordering::Relaxed,
                Ordering::Relaxed,
                |old_value| {
                    let mark_bit = old_value & mask;
                    if mark_bit == value {
                        return None;
                    }
                    Some(old_value & !LOS_BIT_MASK | value)
                },
            );
        result.is_ok()
    }

    fn test_mark_bit(&self, object: ObjectReference, value: u8) -> bool {
        VM::VMObjectModel::LOCAL_LOS_MARK_NURSERY_SPEC.load_atomic::<VM, u8>(
            object,
            None,
            Ordering::Relaxed,
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

    fn update_stat_for_dead_mature_object(&self, o: ObjectReference) {
        crate::stat(|s| {
            s.dead_mature_objects += 1;
            s.dead_mature_volume += o.get_size::<VM>();
            s.dead_mature_los_objects += 1;
            s.dead_mature_los_volume += o.get_size::<VM>();

            s.dead_mature_tracing_objects += 1;
            s.dead_mature_tracing_volume += o.get_size::<VM>();
            s.dead_mature_tracing_los_objects += 1;
            s.dead_mature_tracing_los_volume += o.get_size::<VM>();

            if self.rc.is_stuck(o) {
                s.dead_mature_tracing_stuck_objects += 1;
                s.dead_mature_tracing_stuck_volume += o.get_size::<VM>();
                s.dead_mature_tracing_stuck_los_objects += 1;
                s.dead_mature_tracing_stuck_los_volume += o.get_size::<VM>();
            }
        });
    }

    pub fn sweep_rc_mature_objects_after_satb(&self, is_live: &impl Fn(ObjectReference) -> bool) {
        let mut mature_objects = self.rc_mature_objects.lock().unwrap();
        let mut released_objects = vec![];
        for (o, _size) in mature_objects.iter() {
            if !is_live(*o) {
                self.update_stat_for_dead_mature_object(*o);
                self.rc.set(*o, 0);
                let pages = self.release_object(o.to_address::<VM>());
                self.num_pages_released_lazy
                    .fetch_add(pages, Ordering::Relaxed);
                released_objects.push(*o);
            }
        }
        for o in released_objects {
            mature_objects.remove(&o);
        }
    }
}

fn get_super_page(cell: Address) -> Address {
    cell.align_down(BYTES_IN_PAGE)
}

pub struct RCSweepMatureAfterSATBLOS {
    _counter: LazySweepingJobsCounter,
}

impl RCSweepMatureAfterSATBLOS {
    pub fn new(counter: LazySweepingJobsCounter) -> Self {
        Self { _counter: counter }
    }
}

impl<VM: VMBinding> GCWork<VM> for RCSweepMatureAfterSATBLOS {
    fn do_work(
        &mut self,
        _worker: &mut crate::scheduler::GCWorker<VM>,
        mmtk: &'static crate::MMTK<VM>,
    ) {
        let los = mmtk.get_plan().common().get_los();
        los.sweep_rc_mature_objects_after_satb(&|o| !(!los.is_marked(o) && los.rc.count(o) != 0));
    }
}
