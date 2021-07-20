use super::gc_work::*;
use super::GenCopy;
use crate::plan::barriers::*;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::AllocationSemantics as AllocationType;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::util::alloc::allocators::{ReservedAllocators, common_allocator_mapping, common_space_mapping};
use crate::util::alloc::BumpAllocator;
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::{ObjectModel, VMBinding};
use crate::MMTK;
use enum_map::EnumMap;

pub fn gencopy_mutator_prepare<VM: VMBinding>(_mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    // Do nothing
}

pub fn gencopy_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    // reset nursery allocator
    let bump_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationType::Default])
    }
    .downcast_mut::<BumpAllocator<VM>>()
    .unwrap();
    bump_allocator.reset();
}

const GENCOPY_RESERVED_ALLOCATOR: ReservedAllocators = ReservedAllocators {
    n_bump_pointer: 1,
    n_large_object: 0,
    n_malloc: 0
};

lazy_static! {
    pub static ref ALLOCATOR_MAPPING: EnumMap<AllocationType, AllocatorSelector> = {
        let mut map = common_allocator_mapping(GENCOPY_RESERVED_ALLOCATOR);
        map[AllocationType::Default] = AllocatorSelector::BumpPointer(0);
        map
    };
}

pub fn create_gencopy_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let gencopy = mmtk.plan.downcast_ref::<GenCopy<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &*ALLOCATOR_MAPPING,
        space_mapping: box {
            let mut vec = common_space_mapping(GENCOPY_RESERVED_ALLOCATOR, &*mmtk.plan);
            vec.push((AllocatorSelector::BumpPointer(0), &gencopy.nursery));
            vec
        },
        prepare_func: &gencopy_mutator_prepare,
        release_func: &gencopy_mutator_release,
    };

    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, &*mmtk.plan, &config.space_mapping),
        barrier: box ObjectRememberingBarrier::<GenCopyNurseryProcessEdges<VM>>::new(
            mmtk,
            *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
        ),
        mutator_tls,
        config,
        plan: gencopy,
    }
}
