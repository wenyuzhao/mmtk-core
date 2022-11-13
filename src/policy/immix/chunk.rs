use super::block::{Block, BlockState};
use super::defrag::Histogram;
use super::immixspace::ImmixSpace;
use super::line::Line;
use crate::plan::lxr::LXR;
use crate::util::linear_scan::{Region, RegionIterator};
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::rc;
use crate::util::ObjectReference;
use crate::LazySweepingJobsCounter;
use crate::{
    scheduler::*,
    util::{heap::layout::vm_layout_constants::LOG_BYTES_IN_CHUNK, Address},
    vm::*,
    MMTK,
};
use spin::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::{ops::Range, sync::atomic::Ordering};

/// Data structure to reference a MMTk 4 MB chunk.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
pub struct Chunk(Address);

impl From<Address> for Chunk {
    #[inline(always)]
    fn from(address: Address) -> Chunk {
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }
}

impl From<Chunk> for Address {
    #[inline(always)]
    fn from(chunk: Chunk) -> Address {
        chunk.0
    }
}

impl Region for Chunk {
    const LOG_BYTES: usize = LOG_BYTES_IN_CHUNK;
}

impl Chunk {
    /// Chunk constant with zero address
    const ZERO: Self = Self(Address::ZERO);
    /// Log blocks in chunk
    pub const LOG_BLOCKS: usize = Self::LOG_BYTES - Block::LOG_BYTES;
    /// Blocks in chunk
    pub const BLOCKS: usize = 1 << Self::LOG_BLOCKS;

    /// Get a range of blocks within this chunk.
    #[inline(always)]
    pub fn blocks(&self) -> RegionIterator<Block> {
        let start = Block::from(Block::align(self.0));
        let end = Block::from(start.start() + (Self::BLOCKS << Block::LOG_BYTES));
        RegionIterator::<Block>::new(start, end)
    }

    #[inline(always)]
    pub fn committed_blocks(&self) -> impl Iterator<Item = Block> {
        self.blocks()
            .filter(|block| block.get_state() != BlockState::Unallocated)
    }

    /// Sweep this chunk.
    pub fn sweep<VM: VMBinding>(&self, space: &ImmixSpace<VM>, mark_histogram: &mut Histogram) {
        let line_mark_state = if super::BLOCK_ONLY {
            None
        } else {
            Some(space.line_mark_state.load(Ordering::Acquire))
        };
        // number of allocated blocks.
        let mut allocated_blocks = 0;
        // Iterate over all allocated blocks in this chunk.
        for block in self.committed_blocks() {
            if !block.sweep(space, mark_histogram, line_mark_state) {
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
        let byte: u8 = unsafe { ChunkMap::ALLOC_TABLE.load(self.start()) };
        let state = match byte {
            0 => ChunkState::Free,
            1 => ChunkState::Allocated,
            _ => unreachable!(),
        };
        state == ChunkState::Allocated
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
        unsafe { Self::ALLOC_TABLE.store::<u8>(chunk.start(), state as u8) };
        // If this is a newly allcoated chunk, then expand the chunk range.
        if state == ChunkState::Allocated {
            debug_assert!(!chunk.start().is_zero());
            let mut range = self.chunk_range.lock();
            if range.start == Chunk::ZERO {
                range.start = chunk;
                range.end = chunk.next();
            } else if chunk < range.start {
                range.start = chunk;
            } else if range.end <= chunk {
                range.end = chunk.next();
            }
        }
    }

    /// Get chunk state
    pub fn get(&self, chunk: Chunk) -> ChunkState {
        let byte = unsafe { Self::ALLOC_TABLE.load::<u8>(chunk.start()) };
        match byte {
            0 => ChunkState::Free,
            1 => ChunkState::Allocated,
            _ => unreachable!(),
        }
    }

    /// A range of all chunks in the heap.
    pub fn all_chunks(&self) -> RegionIterator<Chunk> {
        let chunk_range = self.chunk_range.lock();
        RegionIterator::<Chunk>::new(chunk_range.start, chunk_range.end)
    }

    pub fn committed_chunks(&self) -> impl Iterator<Item = Chunk> {
        self.all_chunks().filter(|c| {
            let byte: u8 = unsafe { ChunkMap::ALLOC_TABLE.load(c.start()) };
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
        space: &'static ImmixSpace<VM>,
        defrag_threshold: Option<usize>,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        self.generate_tasks(|chunk| {
            Box::new(PrepareChunk {
                chunk,
                defrag_threshold,
                rc_enabled: space.rc_enabled,
                cm_enabled: space.cm_enabled,
            })
        })
    }

    pub fn generate_concurrent_mark_table_zeroing_tasks<VM: VMBinding>(
        &self,
        _space: &'static ImmixSpace<VM>,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        self.generate_tasks(|chunk| Box::new(ConcurrentChunkMetadataZeroing { chunk }))
    }

    /// Generate chunk sweep work packets.
    pub fn generate_sweep_tasks<VM: VMBinding>(
        &self,
        space: &'static ImmixSpace<VM>,
        rc: bool,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        assert!(!rc);
        space.defrag.mark_histograms.lock().clear();
        let epilogue = Arc::new(FlushPageResource {
            space,
            counter: AtomicUsize::new(0),
        });
        let tasks = self.generate_tasks(|chunk| {
            Box::new(SweepChunk {
                space,
                chunk,
                epilogue: epilogue.clone(),
            })
        });
        epilogue.counter.store(tasks.len(), Ordering::SeqCst);
        tasks
    }

    /// Generate chunk sweep work packets.
    pub fn generate_dead_cycle_sweep_tasks<VM: VMBinding>(&self) -> Vec<Box<dyn GCWork<VM>>> {
        self.generate_tasks(|chunk| {
            Box::new(SweepDeadCyclesChunk::new(
                chunk,
                LazySweepingJobsCounter::new_desc(),
            ))
        })
    }
}

/// A work packet to prepare each block for GC.
/// Performs the action on a range of chunks.
struct PrepareChunk {
    chunk: Chunk,
    cm_enabled: bool,
    rc_enabled: bool,
    defrag_threshold: Option<usize>,
}

impl PrepareChunk {
    /// Clear object mark table
    #[inline(always)]
    #[allow(unused)]
    fn reset_object_mark<VM: VMBinding>(chunk: Chunk) {
        VM::VMObjectModel::LOCAL_MARK_BIT_SPEC
            .extract_side_spec()
            .bzero_metadata(chunk.start(), Chunk::BYTES);
    }
}

impl<VM: VMBinding> GCWork<VM> for PrepareChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let defrag_threshold = self.defrag_threshold.unwrap_or(0);
        if !self.rc_enabled || !crate::args::HEAP_HEALTH_GUIDED_GC {
            Self::reset_object_mark::<VM>(self.chunk);
        }
        // Iterate over all blocks in this chunk
        for block in self.chunk.blocks() {
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

impl Default for ChunkMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Chunk sweeping work packet.
struct SweepChunk<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    chunk: Chunk,
    /// A destructor invoked when all `SweepChunk` packets are finished.
    epilogue: Arc<FlushPageResource<VM>>,
}

impl<VM: VMBinding> GCWork<VM> for SweepChunk<VM> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let mut histogram = self.space.defrag.new_histogram();
        if self.space.chunk_map.get(self.chunk) == ChunkState::Allocated {
            self.chunk.sweep(self.space, &mut histogram);
        }
        if super::DEFRAG {
            self.space.defrag.add_completed_mark_histogram(histogram);
        }
        self.epilogue.finish_one_work_packet();
    }
}

/// Chunk sweeping work packet.
struct SweepDeadCyclesChunk<VM: VMBinding> {
    chunk: Chunk,
    _counter: LazySweepingJobsCounter,
    lxr: *const LXR<VM>,
}

unsafe impl<VM: VMBinding> Send for SweepDeadCyclesChunk<VM> {}

#[allow(unused)]
impl<VM: VMBinding> SweepDeadCyclesChunk<VM> {
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
        }
    }

    #[inline(never)]
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
        if !crate::args::HOLE_COUNTING {
            Block::inc_dead_bytes_sloppy_for_object::<VM>(o);
        }
        rc::set(o, 0);
        if !crate::args::BLOCK_ONLY {
            rc::unmark_straddle_object::<VM>(o)
        }
    }

    #[inline]
    fn process_block(&mut self, block: Block, immix_space: &ImmixSpace<VM>) {
        let mut has_dead_object = false;
        let mut has_live = false;
        let mut cursor = block.start();
        let limit = block.end();
        while cursor < limit {
            let o = unsafe { cursor.to_object_reference() };
            cursor = cursor + rc::MIN_OBJECT_SIZE;
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
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.plan.downcast_ref::<LXR<VM>>().unwrap();
        self.lxr = lxr;
        let immix_space = &lxr.immix_space;
        for block in self.chunk.committed_blocks() {
            if block.is_defrag_source() || block.get_state() == BlockState::Nursery {
                continue;
            } else {
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

/// Count number of remaining work pacets, and flush page resource if all packets are finished.
struct FlushPageResource<VM: VMBinding> {
    space: &'static ImmixSpace<VM>,
    counter: AtomicUsize,
}

impl<VM: VMBinding> FlushPageResource<VM> {
    /// Called after a related work packet is finished.
    fn finish_one_work_packet(&self) {
        if 1 == self.counter.fetch_sub(1, Ordering::SeqCst) {
            // We've finished releasing all the dead blocks to the BlockPageResource's thread-local queues.
            // Now flush the BlockPageResource.
            self.space.flush_page_resource()
        }
    }
}
