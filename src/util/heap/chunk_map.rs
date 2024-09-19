use crate::scheduler::GCWork;
use crate::scheduler::GCWorker;
use crate::util::linear_scan::Region;
use crate::util::linear_scan::RegionIterator;
use crate::util::metadata::side_metadata::SideMetadataSpec;
use crate::util::Address;
use crate::vm::VMBinding;
use spin::Mutex;
use std::ops::Range;

/// Data structure to reference a MMTk 4 MB chunk.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq)]
pub struct Chunk(Address);

impl Region for Chunk {
    const LOG_BYTES: usize = crate::util::heap::layout::vm_layout::LOG_BYTES_IN_CHUNK;

    fn from_aligned_address(address: Address) -> Self {
        debug_assert!(address.is_aligned_to(Self::BYTES));
        Self(address)
    }

    fn start(&self) -> Address {
        self.0
    }
}

impl Chunk {
    /// Chunk constant with zero address
    // FIXME: We use this as an empty value. What if we actually use the first chunk?
    pub const ZERO: Self = Self(Address::ZERO);
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

/// A byte-map to record all the allocated chunks.
/// A plan can use this to maintain records for the chunks that they used, and the states of the chunks.
/// Any plan that uses the chunk map should include the `ALLOC_TABLE` spec in their local sidemetadata specs
pub struct ChunkMap {
    chunk_range: Mutex<Range<Chunk>>,
}

impl ChunkMap {
    /// Chunk alloc table
    pub const ALLOC_TABLE: SideMetadataSpec =
        crate::util::metadata::side_metadata::spec_defs::CHUNK_MARK;

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
                // FIXME: what if we actually use the first chunk?
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

    /// Helper function to create per-chunk processing work packets for each allocated chunks.
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

    pub fn generate_tasks_batched<VM: VMBinding>(
        &self,
        func: impl Fn(Range<Chunk>) -> Box<dyn GCWork<VM>>,
    ) -> Vec<Box<dyn GCWork<VM>>> {
        let mut work_packets: Vec<Box<dyn GCWork<VM>>> = vec![];
        let chunk_range = self.chunk_range.lock();
        let chunks = (chunk_range.end.start() - chunk_range.start.start()) >> Chunk::LOG_BYTES;
        let num_bins = GCWorker::<VM>::current().mmtk.scheduler.num_workers() * 4;
        let bin_size = (chunks + num_bins - 1) / num_bins;
        for i in (0..chunks).step_by(bin_size) {
            let start = chunk_range.start.next_nth(i);
            let end = chunk_range.start.next_nth(i + bin_size);
            let end = if end > chunk_range.end {
                chunk_range.end
            } else {
                end
            };
            work_packets.push(func(start..end));
        }
        work_packets
    }
}

impl Default for ChunkMap {
    fn default() -> Self {
        Self::new()
    }
}
