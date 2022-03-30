use super::block::{Block, BlockState};
use super::defrag::Histogram;
use super::immixspace::ImmixSpace;
use super::line::Line;
use super::region::Region;
use crate::plan::immix::Immix;
use crate::util::metadata::side_metadata::{self, SideMetadataSpec};
use crate::util::rc::{self};
use crate::util::ObjectReference;
use crate::LazySweepingJobsCounter;
use crate::{
    scheduler::*,
    util::{heap::layout::vm_layout_constants::LOG_BYTES_IN_CHUNK, Address},
    vm::*,
    MMTK,
};
use atomic::Atomic;
use spin::Mutex;
use std::ops::ControlFlow;
use std::{iter::Step, ops::Range, sync::atomic::Ordering};

/// Data structure to reference a MMTk 4 MB chunk.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
pub struct Chunk(Address);

impl Chunk {
    /// Chunk constant with zero address
    const ZERO: Self = Self(Address::ZERO);
    /// Log bytes in chunk
    pub const LOG_BYTES: usize = LOG_BYTES_IN_CHUNK;
    /// Bytes in chunk
    pub const BYTES: usize = 1 << Self::LOG_BYTES;
    /// Log blocks in chunk
    pub const LOG_BLOCKS: usize = Self::LOG_BYTES - Block::LOG_BYTES;
    /// Blocks in chunk
    pub const BLOCKS: usize = 1 << Self::LOG_BLOCKS;
    pub const LOG_REGIONS: usize = Self::LOG_BYTES - Region::LOG_BYTES;
    pub const REGIONS: usize = 1 << Self::LOG_REGIONS;

    /// Align the give address to the chunk boundary.
    pub const fn align(address: Address) -> Address {
        address.align_down(Self::BYTES)
    }

    /// Get the chunk from a given address.
    /// The address must be chunk-aligned.
    #[inline(always)]
    pub fn from(address: Address) -> Self {
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    /// Get chunk start address
    pub const fn start(&self) -> Address {
        self.0
    }

    /// Get a range of blocks within this chunk.
    #[inline(always)]
    pub fn blocks(&self) -> Range<Block> {
        let start = Block::from(Block::align(self.0));
        let end = Block::from(start.start() + (Self::BLOCKS << Block::LOG_BYTES));
        start..end
    }

    #[inline(always)]
    pub fn committed_blocks(&self) -> impl Iterator<Item = Block> {
        self.blocks()
            .filter(|block| block.get_state() != BlockState::Unallocated)
    }

    #[inline(always)]
    pub fn committed_mature_blocks(&self) -> impl Iterator<Item = Block> {
        self.blocks().filter(|block| {
            let state = block.get_state();
            state != BlockState::Unallocated && state != BlockState::Nursery
        })
    }

    #[inline(always)]
    pub fn regions(&self) -> Range<Region> {
        let start = Region::from(Region::align(self.0));
        let end = Region::from(start.start() + (Self::REGIONS << Region::LOG_BYTES));
        start..end
    }

    #[inline(always)]
    pub fn of(a: Address) -> Self {
        Self(a.align_down(Self::BYTES))
    }

    #[inline(always)]
    pub fn containing<VM: VMBinding>(object: ObjectReference) -> Self {
        Self(VM::VMObjectModel::ref_to_address(object).align_down(Self::BYTES))
    }

    /// Sweep this chunk.
    pub fn sweep<VM: VMBinding>(
        &self,
        space: &ImmixSpace<VM>,
        mark_histogram: &mut Histogram,
        perform_cycle_collection: bool,
    ) {
        let line_mark_state = if super::BLOCK_ONLY {
            None
        } else {
            Some(space.line_mark_state.load(Ordering::Acquire))
        };
        // number of allocated blocks.
        let mut allocated_blocks = 0;
        // Iterate over all allocated blocks in this chunk.
        for block in self
            .blocks()
            .filter(|block| block.get_state() != BlockState::Unallocated)
        {
            if !block.sweep(
                space,
                mark_histogram,
                line_mark_state,
                perform_cycle_collection,
            ) {
                // Block is live. Increment the allocated block count.
                allocated_blocks += 1;
            }
        }
        // Set this chunk as free if there is not live blocks.
        if allocated_blocks == 0 {
            space.chunk_map.set(*self, ChunkState::Free)
        }
    }

    #[inline(always)]
    pub fn is_committed(&self) -> bool {
        let byte = unsafe { side_metadata::load(&ChunkMap::ALLOC_TABLE, self.start()) as u8 };
        let state = match byte {
            0 => ChunkState::Free,
            1 => ChunkState::Allocated,
            _ => unreachable!(),
        };
        state == ChunkState::Allocated
    }
}

impl Step for Chunk {
    /// Get the number of chunks between the given two chunks.
    #[inline(always)]
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        if start > end {
            return None;
        }
        Some((end.start() - start.start()) >> Self::LOG_BYTES)
    }
    /// result = chunk_address + count * block_size
    #[inline(always)]
    fn forward(start: Self, count: usize) -> Self {
        Self::from(start.start() + (count << Self::LOG_BYTES))
    }
    /// result = chunk_address + count * block_size
    #[inline(always)]
    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        if start.start().as_usize() > usize::MAX - (count << Self::LOG_BYTES) {
            return None;
        }
        Some(Self::forward(start, count))
    }
    /// result = chunk_address + count * block_size
    #[inline(always)]
    fn backward(start: Self, count: usize) -> Self {
        Self::from(start.start() - (count << Self::LOG_BYTES))
    }
    /// result = chunk_address - count * block_size
    #[inline(always)]
    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        if start.start().as_usize() < (count << Self::LOG_BYTES) {
            return None;
        }
        Some(Self::backward(start, count))
    }
}

/// Chunk allocation state
#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ChunkState {
    /// The chunk is not allocated.
    Free = 0,
    /// The chunk is allocated.
    Allocated = 1,
}

/// A byte-map to record all the allocated chunks
pub struct ChunkMap {
    chunk_range: Mutex<Range<Chunk>>,
}

impl ChunkMap {
    /// Chunk alloc table
    pub const ALLOC_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::IX_CHUNK_MARK;

    pub fn new() -> Self {
        Self {
            chunk_range: Mutex::new(Chunk::ZERO..Chunk::ZERO),
        }
    }

    /// Set chunk state
    pub fn set(&self, chunk: Chunk, state: ChunkState) {
        // Do nothing if the chunk is already in the expected state.
        if self.get(chunk) == state {
            return;
        }
        // Update alloc byte
        unsafe { side_metadata::store(&Self::ALLOC_TABLE, chunk.start(), state as u8 as _) };
        // If this is a newly allcoated chunk, then expand the chunk range.
        if state == ChunkState::Allocated {
            let workers = *crate::CALC_WORKERS;
            debug_assert_ne!(workers, 0);
            for region in chunk.regions() {
                region.init_remset(workers);
            }
            debug_assert!(!chunk.start().is_zero());
            let mut range = self.chunk_range.lock();
            if range.start == Chunk::ZERO {
                range.start = chunk;
                range.end = Chunk::forward(chunk, 1);
            } else if chunk < range.start {
                range.start = chunk;
            } else if range.end <= chunk {
                range.end = Chunk::forward(chunk, 1);
            }
        }
    }

    /// Get chunk state
    pub fn get(&self, chunk: Chunk) -> ChunkState {
        let byte = unsafe { side_metadata::load(&Self::ALLOC_TABLE, chunk.start()) as u8 };
        match byte {
            0 => ChunkState::Free,
            1 => ChunkState::Allocated,
            _ => unreachable!(),
        }
    }

    /// A range of all chunks in the heap.
    pub fn all_chunks(&self) -> Range<Chunk> {
        self.chunk_range.lock().clone()
    }

    pub fn committed_chunks(&self) -> impl Iterator<Item = Chunk> {
        self.all_chunks().filter(|c| {
            let byte = unsafe { side_metadata::load(&Self::ALLOC_TABLE, c.start()) as u8 };
            let state = match byte {
                0 => ChunkState::Free,
                1 => ChunkState::Allocated,
                _ => unreachable!(),
            };
            state == ChunkState::Allocated
        })
    }

    /// Helper function to create per-chunk processing work packets.
    pub fn generate_tasks<VM: VMBinding>(
        &self,
        func: impl Fn(Chunk) -> Box<dyn GCWork<VM>>,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        let mut work_packets: Vec<Box<dyn GCWork<VM>>> = vec![];
        for chunk in self
            .all_chunks()
            .filter(|c| self.get(*c) == ChunkState::Allocated)
        {
            work_packets.push(func(chunk));
        }
        work_packets
    }

    /// Generate chunk sweep work packets.
    pub fn generate_prepare_tasks<VM: VMBinding>(
        &self,
        _space: &'static ImmixSpace<VM>,
        defrag_threshold: Option<usize>,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        self.generate_tasks(|chunk| box PrepareChunk {
            chunk,
            defrag_threshold,
        })
    }

    pub fn generate_concurrent_mark_table_zeroing_tasks<VM: VMBinding>(
        &self,
        _space: &'static ImmixSpace<VM>,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        self.generate_tasks(|chunk| box ConcurrentChunkMetadataZeroing { chunk })
    }

    /// Generate chunk sweep work packets.
    pub fn generate_sweep_tasks<VM: VMBinding>(
        &self,
        space: &'static ImmixSpace<VM>,
        rc: bool,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        if !rc {
            space.defrag.mark_histograms.lock().clear();
        }
        self.generate_tasks(|chunk| box SweepChunk {
            space,
            chunk,
            nursery_only: rc,
        })
    }

    /// Generate chunk sweep work packets.
    pub fn generate_dead_cycle_sweep_tasks<VM: VMBinding>(&self) -> Vec<Box<dyn GCWork<VM>>> {
        self.generate_tasks(|chunk| {
            box SweepDeadCyclesChunk::new(chunk, LazySweepingJobsCounter::new_desc())
        })
    }

    pub fn walk_regions_in_address_order<VM: VMBinding>(
        &self,
        space: &ImmixSpace<VM>,
        mut f: impl FnMut(Region) -> ControlFlow<(), ()>,
    ) {
        static CURSOR: Atomic<Address> = Atomic::new(Address::ZERO);
        let mut cursor = CURSOR.load(Ordering::SeqCst);
        let limit = space
            .pr
            .highwater
            .load(Ordering::SeqCst)
            .align_up(Chunk::BYTES);
        if cursor.is_zero() {
            cursor = space.pr.start;
        }
        let original_cursor = cursor;
        let mut first_block = true;
        loop {
            if cursor >= limit {
                cursor = space.pr.start;
            }
            if !first_block && cursor == original_cursor {
                break;
            }
            first_block = false;
            if !Chunk::of(cursor).is_committed() {
                cursor += Region::BYTES;
                continue;
            }
            let region = Region::of(cursor);
            let committed_blocks = region.committed_blocks().count();
            if committed_blocks == 0 {
                cursor += Region::BYTES;
                continue;
            }
            let control_flow = f(region);
            cursor += Region::BYTES;
            if let ControlFlow::Break(_) = control_flow {
                break;
            }
        }
        CURSOR.store(cursor, Ordering::SeqCst);
    }
}

/// A work packet to prepare each block for GC.
/// Performs the action on a range of chunks.
struct PrepareChunk {
    chunk: Chunk,
    defrag_threshold: Option<usize>,
}

impl PrepareChunk {
    /// Clear object mark table
    #[inline(always)]
    #[allow(unused)]
    fn reset_object_mark<VM: VMBinding>(chunk: Chunk) {
        side_metadata::bzero_x(
            &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec(),
            chunk.start(),
            Chunk::BYTES,
        );
    }
}

impl<VM: VMBinding> GCWork<VM> for PrepareChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let defrag_threshold = self.defrag_threshold.unwrap_or(0);
        if !crate::args::HEAP_HEALTH_GUIDED_GC {
            Self::reset_object_mark::<VM>(self.chunk);
        }
        // Iterate over all blocks in this chunk
        for block in self.chunk.blocks() {
            let state = block.get_state();
            block.clear_line_validity_states();
            // Skip unallocated blocks.
            if state == BlockState::Unallocated {
                continue;
            }
            // FIXME: Don't need this when doing RC
            if crate::args::BARRIER_MEASUREMENT
                || (crate::args::CONCURRENT_MARKING && !crate::args::REF_COUNT)
            {
                block.initialize_log_table_as_unlogged::<VM>();
            }
            // Check if this block needs to be defragmented.
            if super::DEFRAG && defrag_threshold != 0 && block.get_holes() > defrag_threshold {
                block.set_as_defrag_source(true);
            } else if !crate::args::REF_COUNT {
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

/// Chunk sweeping work packet.
struct SweepChunk<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    chunk: Chunk,
    nursery_only: bool,
}

impl<VM: VMBinding> GCWork<VM> for SweepChunk<VM> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        if self.nursery_only {
            unreachable!()
        } else {
            let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
            let mut histogram = self.space.defrag.new_histogram();
            if self.space.chunk_map.get(self.chunk) == ChunkState::Allocated {
                self.chunk
                    .sweep(self.space, &mut histogram, immix.perform_cycle_collection());
            }
            if super::DEFRAG {
                self.space.defrag.add_completed_mark_histogram(histogram);
            }
        }
    }
}

/// Chunk sweeping work packet.
struct SweepDeadCyclesChunk<VM: VMBinding> {
    chunk: Chunk,
    worker: *mut GCWorker<VM>,
    _counter: LazySweepingJobsCounter,
    immix: *const Immix<VM>,
}

unsafe impl<VM: VMBinding> Send for SweepDeadCyclesChunk<VM> {}

#[allow(unused)]
impl<VM: VMBinding> SweepDeadCyclesChunk<VM> {
    const CAPACITY: usize = 1024;

    #[inline(always)]
    fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    #[inline(always)]
    const fn immix(&self) -> &Immix<VM> {
        unsafe { &*self.immix }
    }

    pub fn new(chunk: Chunk, counter: LazySweepingJobsCounter) -> Self {
        debug_assert!(crate::args::REF_COUNT);
        Self {
            chunk,
            worker: std::ptr::null_mut(),
            immix: std::ptr::null_mut(),
            _counter: counter,
        }
    }

    #[inline(always)]
    fn process_dead_object(&mut self, mut o: ObjectReference) {
        o = o.fix_start_address::<VM>();
        crate::stat(|s| {
            s.dead_mature_objects += 1;
            s.dead_mature_volume += o.get_size::<VM>();

            s.dead_mature_tracing_objects += 1;
            s.dead_mature_tracing_volume += o.get_size::<VM>();

            if rc::rc_stick(o) {
                s.dead_mature_tracing_stuck_objects += 1;
                s.dead_mature_tracing_stuck_volume += o.get_size::<VM>();
            }
        });
        // self.immix().mark(o);
        rc::set(o, 0);
        if !crate::args::BLOCK_ONLY {
            rc::unmark_straddle_object::<VM>(o)
        }
    }

    #[inline]
    fn process_block(&mut self, block: Block, immix_space: &ImmixSpace<VM>) {
        let mut has_dead_object = false;
        let mut has_live = false;
        for o in (block.start()..block.end())
            .step_by(rc::MIN_OBJECT_SIZE)
            .map(|a| unsafe { a.to_object_reference() })
        {
            let c = rc::count(o);
            if c != 0 && !immix_space.is_marked(o) {
                if !crate::args::BLOCK_ONLY && Line::is_aligned(o.to_address()) {
                    if c == 1 && rc::is_straddle_line(Line::from(o.to_address())) {
                        continue;
                    } else {
                        std::sync::atomic::fence(Ordering::SeqCst);
                        if rc::count(o) == 0 {
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
    #[inline]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        self.immix = immix;
        let immix_space = &immix.immix_space;
        for block in self.chunk.committed_blocks() {
            if block.is_defrag_source() {
                continue;
            } else {
                let state = block.get_state();
                if state == BlockState::Nursery || state == BlockState::Reusing {
                    continue;
                }
                self.process_block(block, immix_space)
            }
        }
    }
}

struct ConcurrentChunkMetadataZeroing {
    chunk: Chunk,
}

impl ConcurrentChunkMetadataZeroing {
    /// Clear object mark table
    #[inline(always)]
    #[allow(unused)]
    fn reset_object_mark<VM: VMBinding>(chunk: Chunk) {
        side_metadata::bzero_x(
            &VM::VMObjectModel::LOCAL_MARK_BIT_SPEC.extract_side_spec(),
            chunk.start(),
            Chunk::BYTES,
        );
        side_metadata::bzero_x(&Block::LIVE_WORDS, chunk.start(), Chunk::BYTES);
    }
}

impl<VM: VMBinding> GCWork<VM> for ConcurrentChunkMetadataZeroing {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        Self::reset_object_mark::<VM>(self.chunk);
        for region in self.chunk.regions() {
            region.remset().clear();
        }
    }
}
