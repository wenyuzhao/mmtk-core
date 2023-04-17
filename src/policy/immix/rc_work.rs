use std::sync::{atomic::AtomicUsize, Mutex};

use atomic::Ordering;
use crossbeam::queue::SegQueue;

use crate::{
    plan::{immix::Pause, lxr::LXR},
    scheduler::{GCWork, GCWorker, WorkBucketStage},
    util::{
        constants::LOG_BYTES_IN_PAGE,
        heap::{chunk_map::Chunk, layout::vm_layout_constants::LOG_BYTES_IN_CHUNK},
        linear_scan::Region,
        rc::{self, RefCountHelper},
        ObjectReference,
    },
    vm::{Collection, ObjectModel, VMBinding},
    LazySweepingJobsCounter, Plan, MMTK,
};

use super::{
    block::{Block, BlockState},
    line::Line,
    ImmixSpace,
};

pub struct MatureSweeping;

impl<VM: VMBinding> GCWork<VM> for MatureSweeping {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        lxr.immix_space
            .schedule_mature_sweeping(lxr.current_pause().unwrap())
    }
}

static SELECT_DEFRAG_BLOCK_JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct SelectDefragBlocksInChunk {
    pub chunk: Chunk,
    #[allow(unused)]
    pub defrag_threshold: usize,
}

impl<VM: VMBinding> GCWork<VM> for SelectDefragBlocksInChunk {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut fragmented_blocks = vec![];
        let mut blocks_in_fragmented_chunks = vec![];
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        let is_emergency_gc = lxr.current_pause().unwrap() == Pause::FullTraceFast;
        const BLOCKS_IN_CHUNK: usize = 1 << (LOG_BYTES_IN_CHUNK - Block::LOG_BYTES);
        let threshold = {
            let chunk_defarg_percent = if is_emergency_gc {
                crate::args().chunk_defarg_percent << 1
            } else {
                crate::args().chunk_defarg_percent
            };
            let chunk_defarg_percent = chunk_defarg_percent.min(100);
            let threshold = BLOCKS_IN_CHUNK * chunk_defarg_percent / 100;
            threshold.max(1)
        };
        // Iterate over all blocks in this chunk
        for block in self.chunk.iter_region::<Block>() {
            // Emergency: We've already marked all the live objects.
            // If the block is already dead, directly release the block (and clear the defrag state).
            if is_emergency_gc && block.get_state() != BlockState::Unallocated && block.rc_dead() {
                lxr.immix_space.release_block(block, false, true, false);
                continue;
            }
            // Skip unallocated blocks.
            if MatureEvacuationSet::skip_block(block) {
                continue;
            }
            // This is a block in a fragmented chunk?
            let live_blocks_in_chunk = lxr
                .immix_space
                .pr
                .get_live_blocks_in_chunk(Chunk::from_unaligned_address(block.start()));
            if live_blocks_in_chunk < threshold {
                let dead_blocks = BLOCKS_IN_CHUNK - live_blocks_in_chunk;
                blocks_in_fragmented_chunks.push((block, dead_blocks));
                continue;
            }
            // This is a fragmented block?
            let score = if crate::args::HOLE_COUNTING {
                unreachable!();
                // match state {
                //     BlockState::Reusable { unavailable_lines } => unavailable_lines as _,
                //     _ => block.calc_holes(),
                // }
            } else {
                // block.dead_bytes()
                // block.calc_dead_bytes::<VM>()
                block.calc_dead_lines() << Line::LOG_BYTES
            };
            if lxr.current_pause().unwrap() == Pause::FullTraceFast || score >= (Block::BYTES >> 1)
            {
                fragmented_blocks.push((block, score));
            }
        }
        // Flush to global fragmented_blocks
        lxr.immix_space
            .evac_set
            .fragmented_blocks_size
            .fetch_add(fragmented_blocks.len(), Ordering::SeqCst);
        lxr.immix_space
            .evac_set
            .fragmented_blocks
            .push(fragmented_blocks);
        // Flush to global blocks_in_fragmented_chunks
        lxr.immix_space
            .evac_set
            .blocks_in_fragmented_chunks_size
            .fetch_add(blocks_in_fragmented_chunks.len(), Ordering::SeqCst);
        lxr.immix_space
            .evac_set
            .blocks_in_fragmented_chunks
            .push(blocks_in_fragmented_chunks);

        if SELECT_DEFRAG_BLOCK_JOB_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
            lxr.immix_space
                .evac_set
                .select_mature_evacuation_candidates(
                    lxr,
                    lxr.current_pause().unwrap(),
                    mmtk.plan.get_total_pages(),
                )
        }
    }
}

pub struct UpdateWeakProcessor;

impl<VM: VMBinding> GCWork<VM> for UpdateWeakProcessor {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        VM::VMCollection::update_weak_processor(true);
    }
}

pub(super) struct SweepBlocksAfterDecs {
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
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        if self.blocks.is_empty() {
            return;
        }
        let mut count = 0;
        for (block, defrag) in &self.blocks {
            block.unlog();
            if block.rc_sweep_mature::<VM>(&lxr.immix_space, *defrag) {
                count += 1;
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
        if count != 0 && lxr.current_pause().is_none() {
            lxr.immix_space
                .num_clean_blocks_released_lazy
                .fetch_add(count, Ordering::Relaxed);
        }
    }
}

/// Chunk sweeping work packet.
pub(super) struct SweepDeadCyclesChunk<VM: VMBinding> {
    chunk: Chunk,
    _counter: LazySweepingJobsCounter,
    rc: RefCountHelper<VM>,
}

#[allow(unused)]
impl<VM: VMBinding> SweepDeadCyclesChunk<VM> {
    const CAPACITY: usize = 1024;

    pub fn new(chunk: Chunk, counter: LazySweepingJobsCounter) -> Self {
        Self {
            chunk,
            _counter: counter,
            rc: RefCountHelper::NEW,
        }
    }

    fn process_dead_object(&mut self, mut o: ObjectReference) {
        o = o.fix_start_address::<VM>();
        crate::stat(|s| {
            s.dead_mature_objects += 1;
            s.dead_mature_volume += o.get_size::<VM>();

            s.dead_mature_tracing_objects += 1;
            s.dead_mature_tracing_volume += o.get_size::<VM>();

            if self.rc.is_stuck(o) {
                s.dead_mature_tracing_stuck_objects += 1;
                s.dead_mature_tracing_stuck_volume += o.get_size::<VM>();
            }
        });
        if !crate::args::HOLE_COUNTING {
            Block::inc_dead_bytes_sloppy_for_object::<VM>(o);
        }
        if ObjectReference::STRICT_VERIFICATION {
            unsafe {
                o.to_address::<VM>().store(0xdeadusize);
            }
        }
        if !crate::args::BLOCK_ONLY {
            self.rc.unmark_straddle_object(o)
        }
        self.rc.set(o, 0);
    }

    fn process_block(&mut self, block: Block, immix_space: &ImmixSpace<VM>) {
        let mut has_dead_object = false;
        let mut has_live = false;
        let mut cursor = block.start();
        let limit = block.end();
        while cursor < limit {
            let o = unsafe { cursor.to_object_reference::<VM>() };
            cursor = cursor + rc::MIN_OBJECT_SIZE;
            let c = self.rc.count(o);
            if c != 0 && !immix_space.is_marked(o) {
                if !crate::args::BLOCK_ONLY && Line::is_aligned(o.to_address::<VM>()) {
                    if c == 1 && self.rc.is_straddle_line(Line::from(o.to_address::<VM>())) {
                        continue;
                    } else {
                        std::sync::atomic::fence(Ordering::SeqCst);
                        if self.rc.count(o) == 0 {
                            continue;
                        }
                    }
                }
                self.process_dead_object(o);
                has_dead_object = true;
            } else {
                if c != 0 {
                    has_live = true;
                }
            }
        }
        if has_dead_object || !has_live {
            immix_space.add_to_possibly_dead_mature_blocks(block, false);
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for SweepDeadCyclesChunk<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        let immix_space = &lxr.immix_space;
        for block in self
            .chunk
            .iter_region::<Block>()
            .filter(|block| block.get_state() != BlockState::Unallocated)
        {
            if block.is_defrag_source() || block.get_state() == BlockState::Nursery {
                continue;
            } else {
                self.process_block(block, immix_space)
            }
        }
    }
}

pub(super) struct ConcurrentChunkMetadataZeroing {
    pub chunk: Chunk,
}

impl ConcurrentChunkMetadataZeroing {
    /// Clear object mark table
    #[allow(unused)]
    fn reset_object_mark<VM: VMBinding>(chunk: Chunk) {
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
            .extract_side_spec()
            .bzero_metadata(chunk.start(), Chunk::BYTES);
    }
}

impl<VM: VMBinding> GCWork<VM> for ConcurrentChunkMetadataZeroing {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        Self::reset_object_mark::<VM>(self.chunk);
    }
}

/// A work packet to prepare each block for GC.
/// Performs the action on a range of chunks.
pub(super) struct PrepareChunk {
    pub chunk: Chunk,
    pub cm_enabled: bool,
    pub rc_enabled: bool,
    pub defrag_threshold: Option<usize>,
}

impl PrepareChunk {
    /// Clear object mark table
    #[allow(unused)]
    fn reset_object_mark<VM: VMBinding>(chunk: Chunk) {
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
            .extract_side_spec()
            .bzero_metadata(chunk.start(), Chunk::BYTES);
    }
}

impl<VM: VMBinding> GCWork<VM> for PrepareChunk {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let defrag_threshold = self.defrag_threshold.unwrap_or(0);
        if !self.rc_enabled {
            Self::reset_object_mark::<VM>(self.chunk);
        }
        // Iterate over all blocks in this chunk
        for block in self.chunk.iter_region::<Block>() {
            let state = block.get_state();
            if self.rc_enabled {
                block.clear_line_validity_states();
            }
            // Skip unallocated blocks.
            if state == BlockState::Unallocated {
                continue;
            }
            // Clear unlog table on CM
            if crate::args::BARRIER_MEASUREMENT || (self.cm_enabled && !self.rc_enabled) {
                block.initialize_log_table_as_unlogged::<VM>();
            }
            // Check if this block needs to be defragmented.
            if super::DEFRAG && defrag_threshold != 0 && block.get_holes() > defrag_threshold {
                block.set_as_defrag_source(true);
            } else if !self.rc_enabled {
                block.set_as_defrag_source(false);
            }
            // Clear block mark data.
            if block.get_state() != BlockState::Nursery {
                block.set_state(BlockState::Unmarked);
            }
            debug_assert!(!block.get_state().is_reusable());
            debug_assert_ne!(block.get_state(), BlockState::Marked);
            // debug_assert_ne!(block.get_state(), BlockState::Nursery);
        }
    }
}

#[derive(Default)]
pub(super) struct MatureEvacuationSet {
    fragmented_blocks: SegQueue<Vec<(Block, usize)>>,
    fragmented_blocks_size: AtomicUsize,
    blocks_in_fragmented_chunks: SegQueue<Vec<(Block, usize)>>,
    blocks_in_fragmented_chunks_size: AtomicUsize,
    defrag_blocks: Mutex<Vec<Block>>,
    num_defrag_blocks: AtomicUsize,
}

impl MatureEvacuationSet {
    /// Release all the mature defrag source blocks
    pub fn sweep_mature_evac_candidates<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        let mut defrag_blocks: Vec<Block> =
            std::mem::take(&mut *self.defrag_blocks.lock().unwrap());
        if defrag_blocks.is_empty() {
            return;
        }
        while let Some(block) = defrag_blocks.pop() {
            if !block.is_defrag_source() || block.get_state() == BlockState::Unallocated {
                // This block has been eagerly released (probably be reused again). Skip it.
                continue;
            }
            block.clear_rc_table::<VM>();
            block.clear_striddle_table::<VM>();
            block.rc_sweep_mature::<VM>(space, true);
            assert!(!block.is_defrag_source());
        }
    }

    pub fn schedule_defrag_selection_packets<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        let tasks = space.chunk_map.generate_tasks(|chunk| {
            Box::new(SelectDefragBlocksInChunk {
                chunk,
                defrag_threshold: 1,
            })
        });
        self.fragmented_blocks_size.store(0, Ordering::SeqCst);
        SELECT_DEFRAG_BLOCK_JOB_COUNTER.store(tasks.len(), Ordering::SeqCst);
        space.scheduler().work_buckets[WorkBucketStage::Unconstrained].bulk_add(tasks);
    }

    fn skip_block(b: Block) -> bool {
        let s = b.get_state();
        b.is_defrag_source() || s == BlockState::Unallocated || s == BlockState::Nursery
    }

    fn select_blocks_in_fragmented_chunks(
        &self,
        selected_blocks: &mut Vec<Block>,
        copy_bytes: &mut usize,
        max_copy_bytes: usize,
    ) {
        let mut blocks =
            Vec::with_capacity(self.blocks_in_fragmented_chunks_size.load(Ordering::SeqCst));
        while let Some(mut x) = self.blocks_in_fragmented_chunks.pop() {
            blocks.append(&mut x);
        }
        blocks.sort_by_key(|x| x.1);
        while let Some((block, _)) = blocks.pop() {
            if Self::skip_block(block) {
                continue;
            }
            block.set_as_defrag_source(true);
            selected_blocks.push(block);
            *copy_bytes += (Block::BYTES - (block.calc_dead_lines() << Line::LOG_BYTES)) >> 1;
            if *copy_bytes >= max_copy_bytes {
                break;
            }
        }
    }

    fn select_fragmented_blocks(
        &self,
        selected_blocks: &mut Vec<Block>,
        copy_bytes: &mut usize,
        max_copy_bytes: usize,
    ) {
        let mut blocks = Vec::with_capacity(self.fragmented_blocks_size.load(Ordering::SeqCst));
        while let Some(mut x) = self.fragmented_blocks.pop() {
            blocks.append(&mut x);
        }
        blocks.sort_by_key(|x| x.1);
        while let Some((block, _dead_bytes)) = blocks.pop() {
            if Self::skip_block(block) {
                continue;
            }
            block.set_as_defrag_source(true);
            selected_blocks.push(block);
            *copy_bytes += (Block::BYTES - (block.calc_dead_lines() << Line::LOG_BYTES)) >> 1;
            if *copy_bytes >= max_copy_bytes {
                break;
            }
        }
    }

    pub fn select_mature_evacuation_candidates<VM: VMBinding>(
        &self,
        lxr: &LXR<VM>,
        _pause: Pause,
        _total_pages: usize,
    ) {
        debug_assert!(crate::args::RC_MATURE_EVACUATION);
        if lxr.current_pause().unwrap() == Pause::FullTraceFast {
            // Make sure LOS sweeping finishes before evac selectino begin
            // FIXME: This can be done in parallel with SelectDefragBlocksInChunk packets
            let los = lxr.common().get_los();
            los.sweep_rc_mature_objects(false, &|o| lxr.rc.count(o) != 0);
            // Update weak processor and remove dead objects in it
            // FIXME: This can be done in parallel with SelectDefragBlocksInChunk packets
            VM::VMCollection::update_weak_processor(true);
        }
        // Select mature defrag blocks
        let available_clean_pages_for_defrag =
            if lxr.current_pause().unwrap() == Pause::FullTraceFast {
                lxr.get_total_pages().saturating_sub(lxr.get_used_pages())
            } else {
                lxr.immix_space.defrag_headroom_pages()
            };
        let max_copy_bytes = available_clean_pages_for_defrag << LOG_BYTES_IN_PAGE;
        let mut copy_bytes = 0usize;
        let mut selected_blocks = vec![];
        self.select_blocks_in_fragmented_chunks(
            &mut selected_blocks,
            &mut copy_bytes,
            max_copy_bytes,
        );
        let count1 = selected_blocks.len();
        self.select_fragmented_blocks(&mut selected_blocks, &mut copy_bytes, max_copy_bytes);
        gc_log!([2]
            " - defrag {} mature bytes ({} blocks, {} blocks in fragmented chunks)",
            copy_bytes,
            selected_blocks.len(),
            count1,
        );
        lxr.dump_heap_usage();
        self.num_defrag_blocks
            .store(selected_blocks.len(), Ordering::SeqCst);
        let mut defrag_blocks = self.defrag_blocks.lock().unwrap();
        *defrag_blocks = selected_blocks;
    }
}
