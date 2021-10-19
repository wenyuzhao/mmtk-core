///! Generational plans
use crate::plan::barriers::BarrierSelector;
use crate::plan::AllocationSemantics;
use crate::plan::PlanConstraints;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::{Address, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;

use std::sync::atomic::Ordering;

// Generational plans:

/// Generational copying (GenCopy)
pub mod copying;
/// Generational immix (GenImmix)
pub mod immix;

// Common generational code

pub(super) mod gc_work;
pub(super) mod global;

/// # Barrier overhead measurement:
///  - Set `FULL_NURSERY_GC` to `true`.
/// ## 1. Baseline: No barrier
///  - Set `ACTIVE_BARRIER` to `BarrierSelector::NoBarrier`.
/// ## 2. Object barrier
///  - Set `ACTIVE_BARRIER` to `BarrierSelector::ObjectBarrier`.
#[inline(always)]
pub fn active_barrier() -> BarrierSelector {
    static mut V: Option<BarrierSelector> = None;
    unsafe {
        *V.get_or_insert_with(|| {
            if FULL_NURSERY_GC {
                match std::env::var("BARRIER") {
                    Ok(s) if s == "ObjectBarrier" => BarrierSelector::ObjectBarrier,
                    Ok(s) if s == "NoBarrier" => BarrierSelector::NoBarrier,
                    Ok(s) if s == "FieldBarrier" => BarrierSelector::FieldLoggingBarrier,
                    _ => unreachable!("Please explicitly specify barrier"),
                }
            } else {
                BarrierSelector::ObjectBarrier
            }
        })
    }
}
/// Full heap collection as nursery GC.
pub const FULL_NURSERY_GC: bool = crate::flags::BARRIER_MEASUREMENT;
/// Force object barrier never enters the slow-path.
/// If enabled,
///  - `FULL_NURSERY_GC` must be `true`.
///  - `ACTIVE_BARRIER` must be `ObjectBarrier`.
pub const NO_SLOW: bool = false;

/// Constraints for generational plans. Each generational plan should overwrite based on this constant.
#[inline(always)]
pub fn gen_constraints() -> &'static PlanConstraints {
    static mut X: PlanConstraints = PlanConstraints {
        moves_objects: true,
        gc_header_bits: 2,
        gc_header_words: 0,
        num_specialized_scans: 1,
        needs_log_bit: true,
        needs_field_log_bit: false,
        barrier: BarrierSelector::NoBarrier,
        max_non_los_default_alloc_bytes: crate::util::rust_util::min_of_usize(
            crate::plan::plan_constraints::MAX_NON_LOS_ALLOC_BYTES_COPYING_PLAN,
            crate::util::options::NURSERY_SIZE,
        ),
        ..PlanConstraints::default()
    };
    let barrier = active_barrier();
    unsafe {
        X.barrier = barrier;
        X.needs_field_log_bit = barrier == BarrierSelector::FieldLoggingBarrier;
        &X
    }
}

/// Create global side metadata specs for generational plans. This will call SideMetadataContext::new_global_specs().
/// So if a plan calls this, it should not call SideMetadataContext::new_global_specs() again.
pub fn new_generational_global_metadata_specs<VM: VMBinding>() -> Vec<SideMetadataSpec> {
    let specs =
        crate::util::metadata::extract_side_metadata(&[*VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC]);
    SideMetadataContext::new_global_specs(&specs)
}

/// Post copying operation for generational plans.
pub fn generational_post_copy<VM: VMBinding>(
    obj: ObjectReference,
    _tib: Address,
    bytes: usize,
    _semantics: AllocationSemantics,
) {
    crate::util::object_forwarding::clear_forwarding_bits::<VM>(obj);
    if !FULL_NURSERY_GC {
        debug_assert_eq!(active_barrier(), BarrierSelector::ObjectBarrier);
        VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.mark_as_unlogged::<VM>(obj, Ordering::SeqCst);
    } else if !NO_SLOW {
        for i in (0..bytes).step_by(8) {
            let a = obj.to_address() + i;
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                .mark_as_unlogged::<VM>(unsafe { a.to_object_reference() }, Ordering::SeqCst);
        }
    }
}
