use std::sync::Arc;

use atomic::Ordering;

use crate::{
    policy::{marksweepspace::native_ms::*, sft::GCWorkerMutRef},
    scheduler::{GCWorkScheduler, GCWorker},
    util::{
        copy::CopySemantics,
        heap::FreeListPageResource,
        metadata::{self, side_metadata::SideMetadataSpec, MetadataSpec},
        ObjectReference,
    },
    vm::VMBinding,
};

#[cfg(feature = "is_mmtk_object")]
use crate::util::Address;

use crate::plan::ObjectQueue;
use crate::plan::VectorObjectQueue;
use crate::policy::sft::SFT;
use crate::policy::space::{CommonSpace, Space};
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::util::heap::chunk_map::*;
use crate::util::linear_scan::Region;
use crate::util::VMThread;
use crate::vm::ObjectModel;
use std::sync::Mutex;

/// The result for `MarkSweepSpace.acquire_block()`. `MarkSweepSpace` will attempt
/// to allocate from abandoned blocks first. If none found, it will get a new block
/// from the page resource.
pub enum BlockAcquireResult {
    Exhausted,
    /// A new block we just acquired from the page resource
    Fresh(Block),
    /// An available block. The block can be directly used if there is any free cell in it.
    AbandonedAvailable(Block),
    /// An unswept block. The block needs to be swept first before it can be used.
    AbandonedUnswept(Block),
}

/// A mark sweep space.
pub struct MarkSweepSpace<VM: VMBinding> {
    pub common: CommonSpace<VM>,
    pr: FreeListPageResource<VM>,
    /// Allocation status for all chunks in MS space
    pub chunk_map: ChunkMap,
    /// Work packet scheduler
    scheduler: Arc<GCWorkScheduler<VM>>,
    /// Abandoned blocks. If a mutator dies, all its blocks go to this abandoned block
    /// lists. In a GC, we also 'flush' all the local blocks to this global pool so they
    /// can be used by allocators from other threads.
    pub abandoned: Mutex<AbandonedBlockLists>,
}

pub struct AbandonedBlockLists {
    pub available: BlockLists,
    pub unswept: BlockLists,
    pub consumed: BlockLists,
}

impl AbandonedBlockLists {
    fn move_consumed_to_unswept(&mut self) {
        let mut i = 0;
        while i < MI_BIN_FULL {
            if !self.consumed[i].is_empty() {
                self.unswept[i].append(&mut self.consumed[i]);
            }
            i += 1;
        }
    }

    fn sweep<VM: VMBinding>(&mut self, space: &MarkSweepSpace<VM>) {
        for i in 0..MI_BIN_FULL {
            self.available[i].sweep_blocks(space);
            self.consumed[i].sweep_blocks(space);
            self.unswept[i].sweep_blocks(space);

            // As we have swept blocks, move blocks in the unswept list to available or consumed list.
            while let Some(block) = self.unswept[i].pop() {
                if block.has_free_cells() {
                    self.available[i].push(block);
                } else {
                    self.consumed[i].push(block);
                }
            }
        }
    }
}

impl<VM: VMBinding> SFT for MarkSweepSpace<VM> {
    fn name(&self) -> &str {
        self.common.name
    }

    fn is_live(&self, object: crate::util::ObjectReference) -> bool {
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_marked::<VM>(object, Ordering::SeqCst)
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
        false
    }

    fn is_movable(&self) -> bool {
        false
    }

    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        true
    }

    fn initialize_object_metadata(
        &self,
        _object: crate::util::ObjectReference,
        _bytes: usize,
        _alloc: bool,
    ) {
        #[cfg(feature = "vo_bit")]
        crate::util::metadata::vo_bit::set_vo_bit::<VM>(_object);
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

impl<VM: VMBinding> Space<VM> for MarkSweepSpace<VM> {
    fn as_space(&self) -> &dyn Space<VM> {
        self
    }

    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }

    fn get_page_resource(&self) -> &dyn crate::util::heap::PageResource<VM> {
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

    fn release_multiple_pages(&mut self, _start: crate::util::Address) {
        todo!()
    }
}

impl<VM: VMBinding> crate::policy::gc_work::PolicyTraceObject<VM> for MarkSweepSpace<VM> {
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

// We cannot allocate objects that are larger than the max bin size.
#[allow(dead_code)]
pub const MAX_OBJECT_SIZE: usize = crate::policy::marksweepspace::native_ms::MI_LARGE_OBJ_SIZE_MAX;

impl<VM: VMBinding> MarkSweepSpace<VM> {
    // Allow ptr_arg as we want to keep the function signature the same as for malloc marksweep
    #[allow(clippy::ptr_arg)]
    pub fn extend_global_side_metadata_specs(_specs: &mut Vec<SideMetadataSpec>) {
        // MarkSweepSpace does not need any special global specs. This method exists, as
        // we need this method for MallocSpace, and we want those two spaces to be used interchangably.
    }

    pub fn new(args: crate::policy::space::PlanCreateSpaceArgs<VM>) -> MarkSweepSpace<VM> {
        let scheduler = args.scheduler.clone();
        let vm_map = args.vm_map;
        let is_discontiguous = args.vmrequest.is_discontiguous();
        let local_specs = {
            metadata::extract_side_metadata(&vec![
                MetadataSpec::OnSide(Block::NEXT_BLOCK_TABLE),
                MetadataSpec::OnSide(Block::PREV_BLOCK_TABLE),
                MetadataSpec::OnSide(Block::FREE_LIST_TABLE),
                MetadataSpec::OnSide(Block::SIZE_TABLE),
                #[cfg(feature = "malloc_native_mimalloc")]
                MetadataSpec::OnSide(Block::LOCAL_FREE_LIST_TABLE),
                #[cfg(feature = "malloc_native_mimalloc")]
                MetadataSpec::OnSide(Block::THREAD_FREE_LIST_TABLE),
                MetadataSpec::OnSide(Block::BLOCK_LIST_TABLE),
                MetadataSpec::OnSide(Block::TLS_TABLE),
                MetadataSpec::OnSide(Block::MARK_TABLE),
                MetadataSpec::OnSide(ChunkMap::ALLOC_TABLE),
                *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
            ])
        };
        let policy_args = args.into_policy_args(false, false, local_specs);
        let metadata = policy_args.metadata();
        let common = CommonSpace::new(policy_args);
        MarkSweepSpace {
            pr: if is_discontiguous {
                FreeListPageResource::new_discontiguous(vm_map, metadata)
            } else {
                FreeListPageResource::new_contiguous(common.start, common.extent, vm_map, metadata)
            },
            common,
            chunk_map: ChunkMap::new(),
            scheduler,
            abandoned: Mutex::new(AbandonedBlockLists {
                available: new_empty_block_lists(),
                unswept: new_empty_block_lists(),
                consumed: new_empty_block_lists(),
            }),
        }
    }

    fn trace_object<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
    ) -> ObjectReference {
        debug_assert!(!object.is_null());
        debug_assert!(
            self.in_space(object),
            "Cannot mark an object {} that was not alloced by free list allocator.",
            object,
        );
        if !VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.is_marked::<VM>(object, Ordering::SeqCst) {
            VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.mark::<VM>(object, Ordering::SeqCst);
            let block = Block::containing::<VM>(object);
            block.set_state(BlockState::Marked);
            queue.enqueue(object);
        }
        object
    }

    pub fn record_new_block(&self, block: Block) {
        block.init();
        self.chunk_map.set(block.chunk(), ChunkState::Allocated);
    }

    pub fn get_next_metadata_spec(&self) -> SideMetadataSpec {
        Block::NEXT_BLOCK_TABLE
    }

    pub fn prepare(&mut self) {
        if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC {
            for chunk in self.chunk_map.all_chunks() {
                side.bzero_metadata(chunk.start(), Chunk::BYTES);
            }
        } else {
            unimplemented!("in header mark bit is not supported");
        }
    }

    pub fn release(&mut self) {
        // We sweep and release unmarked blocks here. For sweeping cells inside each block, we either
        // do that when we release mutators (eager sweeping), or do that at allocation time (lazy sweeping).
        use crate::scheduler::WorkBucketStage;
        let work_packets = self.generate_sweep_tasks();
        self.scheduler.work_buckets[WorkBucketStage::Release].bulk_add(work_packets);

        if cfg!(feature = "eager_sweeping") {
            // For eager sweeping, we have to sweep the lists that are abandoned to these global lists.
            let mut abandoned = self.abandoned.lock().unwrap();
            abandoned.sweep(self);
        } else {
            // For lazy sweeping, we just move blocks from consumed to unswept. When an allocator tries
            // to use them, they will sweep the block.
            let mut abandoned = self.abandoned.lock().unwrap();
            abandoned.move_consumed_to_unswept();
        }
    }

    /// Release a block.
    pub fn release_block(&self, block: Block) {
        self.block_clear_metadata(block);

        block.deinit();
        self.pr.release_pages(block.start());
    }

    pub fn block_clear_metadata(&self, block: Block) {
        for metadata_spec in Block::METADATA_SPECS {
            metadata_spec.set_zero_atomic(block.start(), Ordering::SeqCst);
        }
        #[cfg(feature = "vo_bit")]
        crate::util::metadata::vo_bit::bzero_vo_bit(block.start(), Block::BYTES);
    }

    pub fn acquire_block(&self, tls: VMThread, size: usize, align: usize) -> BlockAcquireResult {
        {
            let mut abandoned = self.abandoned.lock().unwrap();
            let bin = mi_bin::<VM>(size, align);

            {
                let abandoned_available = &mut abandoned.available;
                if !abandoned_available[bin].is_empty() {
                    let block = abandoned_available[bin].pop().unwrap();
                    return BlockAcquireResult::AbandonedAvailable(block);
                }
            }

            {
                let abandoned_unswept = &mut abandoned.unswept;
                if !abandoned_unswept[bin].is_empty() {
                    let block = abandoned_unswept[bin].pop().unwrap();
                    return BlockAcquireResult::AbandonedUnswept(block);
                }
            }
        }

        let acquired = self.acquire(tls, Block::BYTES >> LOG_BYTES_IN_PAGE);
        if acquired.is_zero() {
            BlockAcquireResult::Exhausted
        } else {
            BlockAcquireResult::Fresh(Block::from_unaligned_address(acquired))
        }
    }

    pub fn generate_sweep_tasks(&self) -> Vec<Box<dyn GCWork<VM>>> {
        // # Safety: ImmixSpace reference is always valid within this collection cycle.
        let space = unsafe { &*(self as *const Self) };
        self.chunk_map
            .generate_tasks(|chunk| Box::new(SweepChunk { space, chunk }))
    }
}

use crate::scheduler::GCWork;
use crate::MMTK;

/// Chunk sweeping work packet.
struct SweepChunk<VM: VMBinding> {
    space: &'static MarkSweepSpace<VM>,
    chunk: Chunk,
}

impl<VM: VMBinding> GCWork<VM> for SweepChunk<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        debug_assert!(self.space.chunk_map.get(self.chunk) == ChunkState::Allocated);
        // number of allocated blocks.
        let mut allocated_blocks = 0;
        // Iterate over all allocated blocks in this chunk.
        for block in self
            .chunk
            .iter_region::<Block>()
            .filter(|block| block.get_state() != BlockState::Unallocated)
        {
            if !block.attempt_release(self.space) {
                // Block is live. Increment the allocated block count.
                allocated_blocks += 1;
            }
        }
        // Set this chunk as free if there is not live blocks.
        if allocated_blocks == 0 {
            self.space.chunk_map.set(self.chunk, ChunkState::Free)
        }
    }
}
