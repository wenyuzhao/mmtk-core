use super::block::{Block, BlockState};
use super::defrag::Histogram;
use super::immixspace::ImmixSpace;
use super::line::Line;
use crate::plan::immix::{Immix, CURRENT_CONC_DECS_COUNTER};
use crate::util::metadata::side_metadata::{self, SideMetadataSpec};
use crate::util::metadata::MetadataSpec;
use crate::util::rc::{self, ProcessDecs, SweepBlocksAfterDecs};
use crate::util::ObjectReference;
use crate::{
    scheduler::*,
    util::{heap::layout::vm_layout_constants::LOG_BYTES_IN_CHUNK, Address},
    vm::*,
    MMTK,
};
use spin::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
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

    pub fn sweep_nursery<VM: VMBinding>(&self, space: &ImmixSpace<VM>) {
        debug_assert!(crate::flags::REF_COUNT);
        // number of allocated blocks.
        let mut allocated_blocks = 0;
        // Iterate over all allocated blocks in this chunk.
        for block in self
            .blocks()
            .filter(|block| block.get_state() != BlockState::Unallocated)
        {
            if !block.rc_sweep_nursery(space) {
                // Block is live. Increment the allocated block count.
                allocated_blocks += 1;
            }
        }
        // Set this chunk as free if there is not live blocks.
        if allocated_blocks == 0 {
            space.chunk_map.set(*self, ChunkState::Free)
        }
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
        self
            .all_chunks()
            .filter(|c| {
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
    fn generate_tasks<VM: VMBinding>(
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
            box SweepDeadCyclesChunk::new(chunk, unsafe {
                CURRENT_CONC_DECS_COUNTER.clone().unwrap()
            })
        })
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
    fn reset_object_mark<VM: VMBinding>(chunk: Chunk) {
        if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC {
            side_metadata::bzero_metadata(&side, chunk.start(), Chunk::BYTES);
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for PrepareChunk {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let defrag_threshold = self.defrag_threshold.unwrap_or(0);
        // Clear object mark table for this chunk
        Self::reset_object_mark::<VM>(self.chunk);
        // Iterate over all blocks in this chunk
        for block in self.chunk.blocks() {
            let state = block.get_state();
            // Skip unallocated blocks.
            if state == BlockState::Unallocated {
                continue;
            }
            // if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC {
            //     side_metadata::bzero_metadata(&side, block.start(), Block::BYTES);
            // }
            // FIXME: Don't need this when doing RC
            if crate::flags::BARRIER_MEASUREMENT
                || (crate::flags::CONCURRENT_MARKING && !crate::flags::REF_COUNT)
            {
                block.initialize_log_table_as_unlogged::<VM>();
            }
            // Check if this block needs to be defragmented.
            if super::DEFRAG && defrag_threshold != 0 && block.get_holes() > defrag_threshold {
                block.set_as_defrag_source(true);
            } else if !crate::flags::REF_COUNT {
                block.set_as_defrag_source(false);
            }
            // Clear block mark data.
            block.set_state(BlockState::Unmarked);
            debug_assert!(!block.get_state().is_reusable());
            debug_assert_ne!(block.get_state(), BlockState::Marked);
            debug_assert_ne!(block.get_state(), BlockState::Nursery);
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
            self.chunk.sweep_nursery(self.space)
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
    decs: Vec<ObjectReference>,
    worker: *mut GCWorker<VM>,
    /// Counter for the number of remaining `ProcessDecs` packages
    count_down: Arc<AtomicUsize>,
}

unsafe impl<VM: VMBinding> Send for SweepDeadCyclesChunk<VM> {}

#[allow(unused)]
impl<VM: VMBinding> SweepDeadCyclesChunk<VM> {
    const CAPACITY: usize = 1024;

    #[inline(always)]
    fn worker(&self) -> &mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn new(chunk: Chunk, count_down: Arc<AtomicUsize>) -> Self {
        debug_assert!(crate::flags::REF_COUNT);
        count_down.fetch_add(1, Ordering::SeqCst);
        Self {
            chunk,
            decs: vec![],
            worker: std::ptr::null_mut(),
            count_down,
        }
    }

    #[inline(always)]
    pub fn add_dec(&mut self, o: ObjectReference) {
        if self.decs.is_empty() {
            self.decs.reserve(Self::CAPACITY);
        }
        self.decs.push(o);
        if self.decs.len() > Self::CAPACITY {
            self.flush()
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        if !self.decs.is_empty() {
            let mut decs = vec![];
            std::mem::swap(&mut decs, &mut self.decs);
            self.worker().add_work(
                WorkBucketStage::Unconstrained,
                ProcessDecs::<VM>::new(decs, self.count_down.clone()),
            );
        }
    }

    #[inline(always)]
    fn process_dead_object(&mut self, mut o: ObjectReference) {
        o = o.fix_start_address::<VM>();
        // Attempt to set refcount to 1
        // let r = crate::util::rc::fetch_update(o, |c| {
        //     if c <= 1 {
        //         return None; // already dead
        //     }
        //     Some(1)
        // });
        // if r == Err(1) || r.is_ok() {
        //     // This is a dead object in a dead cycle. Perform cyclic decrements
        //     self.add_dec(o)
        // }
        // println!("kill cyclic {:?}", o);
        rc::set(o, 0);
        if !crate::flags::BLOCK_ONLY {
            rc::unmark_straddle_object::<VM>(o)
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for SweepDeadCyclesChunk<VM> {
    #[inline]
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.worker = worker;
        let immix = mmtk.plan.downcast_ref::<Immix<VM>>().unwrap();
        let immix_space = &immix.immix_space;
        for block in self.chunk.committed_blocks() {
            // if immix.current_pause() == Some(Pause::FinalMark) && block.get_state() == BlockState::Nursery {
            //     continue;
            // }
            // const LOG_OBJECTS_IN_BYTE: usize = LOG_BITS_IN_BYTE as usize - rc::LOG_REF_COUNT_BITS;
            // const OBJECTS_IN_BYTE: usize = 1 << LOG_OBJECTS_IN_BYTE;

            // type UInt = u128;
            // const LOG_BITS_IN_ENTRY: usize =
            //     (std::mem::size_of::<UInt>() << LOG_BITS_IN_BYTE).trailing_zeros() as _;
            // const LOG_OBJECTS_IN_ENTRY: usize = LOG_BITS_IN_ENTRY - rc::LOG_REF_COUNT_BITS;
            // let rc_table = crate::util::rc::rc_table_range::<UInt>(block);
            // for (i, entry) in rc_table.iter().enumerate() {
            //     let entry = *entry;
            //     if likely(entry == 0) {
            //         continue;
            //     }
            //     let base = block.start() + (i << (LOG_OBJECTS_IN_ENTRY + rc::LOG_MIN_OBJECT_SIZE));
            //     for (j, byte) in entry.to_le_bytes().iter().enumerate() {
            //         let byte = *byte;
            //         if likely(byte == 0) {
            //             continue;
            //         }
            //         let base = base + (j << (LOG_OBJECTS_IN_BYTE + rc::LOG_MIN_OBJECT_SIZE));
            //         for k in 0..OBJECTS_IN_BYTE {
            //             let c =
            //                 (byte >> (k << rc::LOG_REF_COUNT_BITS)) as usize & rc::REF_COUNT_MASK;
            //             if unlikely(c != 0 && c != rc::MARKER) {
            //                 let o = unsafe {
            //                     (base + (k << rc::LOG_MIN_OBJECT_SIZE)).to_object_reference()
            //                 };
            //                 if !immix_space.is_marked(o) {
            //                     self.process_dead_object(o)
            //                 }
            //             }
            //         }
            //     }
            // }

            // let rc_table = crate::util::rc::rc_table_range::<u8>(block);
            // for (i, entry) in rc_table.iter().enumerate() {
            //     let entry = *entry;
            //     if likely(entry == 0) {
            //         continue
            //     }
            //     let base = block.start() + (i << (LOG_OBJECTS_IN_BYTE + rc::LOG_MIN_OBJECT_SIZE));
            //     for j in 0..OBJECTS_IN_BYTE {
            //         let c = (entry >> (j << rc::LOG_REF_COUNT_BITS)) as usize & rc::REF_COUNT_MASK;
            //         if unlikely(c != 0 && c != rc::MARKER) {
            //             let o = unsafe { (base + (j << rc::LOG_MIN_OBJECT_SIZE)).to_object_reference() };
            //             if !immix_space.is_marked(o) {
            //                 self.process_dead_object(o)
            //             }
            //         }
            //     }
            // }

            // FIXME: Performance
            // println!(" - satb reclaim {:?} defrag={}", block, block.is_defrag_source());
            block.set_as_defrag_source(false);
            let mut has_dead_object = false;
            for o in (block.start()..block.end())
                .step_by(rc::MIN_OBJECT_SIZE)
                .map(|a| unsafe { a.to_object_reference() })
            {
                let c = rc::count(o);
                if c != 0 && !immix_space.is_marked(o) {
                    if !crate::flags::BLOCK_ONLY && Line::is_aligned(o.to_address()) {
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
                }
            }
            if has_dead_object {
                immix_space.possibly_dead_mature_blocks.lock().insert(block);
            }
        }
        // self.flush();
        // If all decs are finished, start sweeping blocks
        if self.count_down.fetch_sub(1, Ordering::SeqCst) == 1 {
            SweepBlocksAfterDecs::schedule(&mmtk.scheduler, &immix_space);
        }
    }
}
