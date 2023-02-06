use std::sync::atomic::AtomicUsize;

use atomic::Ordering;

use crate::{
    plan::lxr::LXR,
    scheduler::{GCWork, GCWorker},
    util::{
        heap::chunk_map::Chunk,
        linear_scan::Region,
        rc::{self, RefCountHelper},
        ObjectReference,
    },
    vm::{Collection, ObjectModel, VMBinding},
    LazySweepingJobsCounter, MMTK,
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
        let lxr_mut = unsafe { &mut *(lxr as *const LXR<VM> as *mut LXR<VM>) };
        lxr_mut
            .immix_space
            .schedule_mature_sweeping(lxr.current_pause().unwrap())
    }
}

pub(super) static SELECT_DEFRAG_BLOCK_JOB_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(super) struct SelectDefragBlocksInChunk {
    pub chunk: Chunk,
    #[allow(unused)]
    pub defrag_threshold: usize,
}

impl<VM: VMBinding> GCWork<VM> for SelectDefragBlocksInChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut blocks = vec![];
        // Iterate over all blocks in this chunk
        for block in self
            .chunk
            .iter_region::<Block>()
            .filter(|block| block.get_state() != BlockState::Unallocated)
        {
            let state = block.get_state();
            // Skip unallocated blocks.
            if state == BlockState::Unallocated
                || state == BlockState::Nursery
                || block.is_defrag_source()
            {
                continue;
            }
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
            if score >= (Block::BYTES >> 1) {
                blocks.push((block, score));
            }
        }
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        lxr.immix_space
            .fragmented_blocks_size
            .fetch_add(blocks.len(), Ordering::SeqCst);
        lxr.immix_space.fragmented_blocks.push(blocks);
        if SELECT_DEFRAG_BLOCK_JOB_COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
            lxr.immix_space.select_mature_evacuation_candidates(
                lxr.current_pause().unwrap(),
                mmtk.plan.get_total_pages(),
            )
        }
    }
}

pub struct UpdateWeakProcessor;

impl<VM: VMBinding> GCWork<VM> for UpdateWeakProcessor {
    #[inline]
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
pub(super) struct SweepDeadCyclesChunk<VM: VMBinding, const COMPRESSED: bool> {
    chunk: Chunk,
    _counter: LazySweepingJobsCounter,
    lxr: *const LXR<VM>,
    rc: RefCountHelper<VM>,
}

unsafe impl<VM: VMBinding, const COMPRESSED: bool> Send for SweepDeadCyclesChunk<VM, COMPRESSED> {}

#[allow(unused)]
impl<VM: VMBinding, const COMPRESSED: bool> SweepDeadCyclesChunk<VM, COMPRESSED> {
    const CAPACITY: usize = 1024;

    #[inline(always)]
    fn lxr(&self) -> &LXR<VM> {
        unsafe { &*self.lxr }
    }

    pub fn new(chunk: Chunk, counter: LazySweepingJobsCounter) -> Self {
        Self {
            chunk,
            lxr: std::ptr::null_mut(),
            _counter: counter,
            rc: RefCountHelper::NEW,
        }
    }

    #[inline(never)]
    fn process_dead_object(&mut self, mut o: ObjectReference) {
        o = o.fix_start_address::<VM, COMPRESSED>();
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
        self.rc.set(o, 0);
        if !crate::args::BLOCK_ONLY {
            self.rc.unmark_straddle_object(o)
        }
    }

    #[inline]
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

impl<VM: VMBinding, const COMPRESSED: bool> GCWork<VM> for SweepDeadCyclesChunk<VM, COMPRESSED> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        self.lxr = lxr;
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
    #[inline(always)]
    #[allow(unused)]
    fn reset_object_mark<VM: VMBinding>(chunk: Chunk) {
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
            .extract_side_spec()
            .bzero_metadata(chunk.start(), Chunk::BYTES);
    }
}

impl<VM: VMBinding> GCWork<VM> for ConcurrentChunkMetadataZeroing {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        Self::reset_object_mark::<VM>(self.chunk);
    }
}

/// A work packet to prepare each block for GC.
/// Performs the action on a range of chunks.
pub(super) struct PrepareChunk<const COMPRESSED: bool> {
    pub chunk: Chunk,
    pub cm_enabled: bool,
    pub rc_enabled: bool,
    pub defrag_threshold: Option<usize>,
}

impl<const COMPRESSED: bool> PrepareChunk<COMPRESSED> {
    /// Clear object mark table
    #[inline(always)]
    #[allow(unused)]
    fn reset_object_mark<VM: VMBinding>(chunk: Chunk) {
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
            .extract_side_spec()
            .bzero_metadata(chunk.start(), Chunk::BYTES);
    }
}

impl<VM: VMBinding, const COMPRESSED: bool> GCWork<VM> for PrepareChunk<COMPRESSED> {
    #[inline]
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
                block.initialize_log_table_as_unlogged::<VM, COMPRESSED>();
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
