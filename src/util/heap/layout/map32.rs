use super::chunk_freelist::ChunkFreelist;
use super::map::VMMap;
use crate::mmtk::SFT_MAP;
use crate::util::conversions;
use crate::util::freelist::FreeList;
use crate::util::heap::freelistpageresource::CommonFreeListPageResource;
use crate::util::heap::layout::heap_parameters::*;
use crate::util::heap::layout::vm_layout_constants::*;
use crate::util::heap::space_descriptor::SpaceDescriptor;
use crate::util::int_array_freelist::IntArrayFreeList;
use crate::util::Address;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::sync::{Mutex, MutexGuard};

pub struct Map32 {
    prev_link: Vec<i32>,
    next_link: Vec<i32>,
    region_map: RwLock<ChunkFreelist>,
    global_page_map: IntArrayFreeList,
    shared_discontig_fl_count: usize,
    shared_fl_map: Vec<Option<&'static CommonFreeListPageResource>>,
    total_available_discontiguous_chunks: usize,
    finalized: bool,
    sync: Mutex<()>,
    descriptor_map: Vec<SpaceDescriptor>,

    // TODO: Is this the right place for this field?
    // This used to be a global variable. When we remove global states, this needs to be put somewhere.
    // Currently I am putting it here, as for where this variable is used, we already have
    // references to vm_map - so it is convenient to put it here.
    cumulative_committed_pages: AtomicUsize,
}

impl Map32 {
    pub fn new() -> Self {
        Map32 {
            prev_link: vec![0; VM_LAYOUT_CONSTANTS.max_chunks()],
            next_link: vec![0; VM_LAYOUT_CONSTANTS.max_chunks()],
            region_map: RwLock::new(ChunkFreelist::new(VM_LAYOUT_CONSTANTS.available_start())),
            global_page_map: IntArrayFreeList::new(1, 1, MAX_SPACES),
            shared_discontig_fl_count: 0,
            shared_fl_map: vec![None; MAX_SPACES],
            total_available_discontiguous_chunks: 0,
            finalized: false,
            sync: Mutex::new(()),
            descriptor_map: vec![SpaceDescriptor::UNINITIALIZED; VM_LAYOUT_CONSTANTS.max_chunks()],
            cumulative_committed_pages: AtomicUsize::new(0),
        }
    }
}

impl VMMap for Map32 {
    fn insert(&self, start: Address, extent: usize, descriptor: SpaceDescriptor) {
        // Each space will call this on exclusive address ranges. It is fine to mutate the descriptor map,
        // as each space will update different indices.
        // let mut region_map = self.region_map.read().unwrap();
        // let self_mut: &mut Self = unsafe { self.mut_self() };
        // let mut e = 0;
        // while e < extent {
        //     let index = region_map.address_to_unit(start + e);
        //     assert!(
        //         self.descriptor_map[index].is_empty(),
        //         "Conflicting virtual address request"
        //     );
        //     // println!(
        //     //     "Set descriptor {:?} for Chunk {}",
        //     //     descriptor,
        //     //     conversions::chunk_index_to_address(index)
        //     // );
        //     self_mut.descriptor_map[index] = descriptor;
        //     //   VM.barriers.objectArrayStoreNoGCBarrier(spaceMap, index, space);
        //     e += BYTES_IN_CHUNK;
        // }
        unimplemented!()
    }

    fn create_freelist(&self, _start: Address) -> Box<dyn FreeList> {
        Box::new(IntArrayFreeList::from_parent(
            &self.global_page_map,
            self.get_discontig_freelist_pr_ordinal() as _,
        ))
    }

    fn create_parent_freelist(
        &self,
        _start: Address,
        units: usize,
        grain: i32,
    ) -> Box<dyn FreeList> {
        Box::new(IntArrayFreeList::new(units, grain, 1))
    }

    fn bind_freelist(&self, pr: &'static CommonFreeListPageResource) {
        let ordinal: usize = pr
            .free_list
            .downcast_ref::<IntArrayFreeList>()
            .unwrap()
            .get_ordinal() as usize;
        let self_mut: &mut Self = unsafe { self.mut_self() };
        self_mut.shared_fl_map[ordinal] = Some(pr);
    }

    fn allocate_contiguous_chunks(
        &self,
        descriptor: SpaceDescriptor,
        chunks: usize,
        head: Address,
    ) -> Address {
        let mut region_map = self.region_map.write().unwrap();
        let (_sync, self_mut) = self.mut_self_with_sync();
        let chunk = region_map.alloc(chunks as _);

        println!("alloc {:?}", chunk);
        // debug_assert!(chunk != 0);
        if chunk.is_none() {
            // if cfg!(feature = "sanity") {
            gc_log!(
                "WARNING: Failed to allocate {} chunks. total_available_discontiguous_chunks={}",
                chunks,
                self.total_available_discontiguous_chunks
            );
            // }
            return unsafe { Address::zero() };
        }
        let chunk = chunk.unwrap();
        self_mut.total_available_discontiguous_chunks -= chunks;
        let rtn = region_map.unit_to_address(chunk);
        // self.insert(rtn, chunks << LOG_BYTES_IN_CHUNK, descriptor);
        {
            let start = rtn;
            let extent = chunks << LOG_BYTES_IN_CHUNK;
            // Each space will call this on exclusive address ranges. It is fine to mutate the descriptor map,
            // as each space will update different indices.
            // let mut region_map = self.region_map.read().unwrap();
            let self_mut: &mut Self = unsafe { self.mut_self() };
            let mut e = 0;
            while e < extent {
                let index = region_map.address_to_unit(start + e);
                assert!(
                    self.descriptor_map[index].is_empty(),
                    "Conflicting virtual address request"
                );
                // println!(
                //     "Set descriptor {:?} for Chunk {}",
                //     descriptor,
                //     conversions::chunk_index_to_address(index)
                // );
                self_mut.descriptor_map[index] = descriptor;
                //   VM.barriers.objectArrayStoreNoGCBarrier(spaceMap, index, space);
                e += BYTES_IN_CHUNK;
            }
        }
        if head.is_zero() {
            debug_assert!(self.next_link[chunk as usize] == 0);
        } else {
            self_mut.next_link[chunk as usize] = region_map.address_to_unit(head) as _;
            self_mut.prev_link[region_map.address_to_unit(head)] = chunk as _;
        }
        debug_assert!(self.prev_link[chunk as usize] == 0);
        rtn
    }

    fn get_next_contiguous_region(&self, start: Address) -> Address {
        let mut region_map = self.region_map.read().unwrap();
        debug_assert!(start == conversions::chunk_align_down(start));
        let chunk = region_map.address_to_unit(start);
        if chunk == 0 || self.next_link[chunk] == 0 {
            unsafe { Address::zero() }
        } else {
            let a = self.next_link[chunk];
            let x = region_map.unit_to_address(a as _);
            println!("next chunk {:?} -> {:?}", start, x);
            x
        }
    }

    fn get_contiguous_region_chunks(&self, start: Address) -> usize {
        let mut region_map = self.region_map.read().unwrap();
        debug_assert!(start == conversions::chunk_align_down(start));
        let chunk = region_map.address_to_unit(start);
        region_map.size(chunk) as _
    }

    fn get_contiguous_region_size(&self, start: Address) -> usize {
        self.get_contiguous_region_chunks(start) << LOG_BYTES_IN_CHUNK
    }

    fn get_available_discontiguous_chunks(&self) -> usize {
        self.total_available_discontiguous_chunks
    }

    fn get_chunk_consumer_count(&self) -> usize {
        self.shared_discontig_fl_count
    }

    fn free_all_chunks(&self, any_chunk: Address) {
        debug!("free_all_chunks: {}", any_chunk);
        let mut region_map = self.region_map.write().unwrap();
        let (_sync, self_mut) = self.mut_self_with_sync();
        debug_assert!(any_chunk == conversions::chunk_align_down(any_chunk));
        if !any_chunk.is_zero() {
            let chunk = region_map.address_to_unit(any_chunk);
            while self_mut.next_link[chunk] != 0 {
                let x = self_mut.next_link[chunk];
                self_mut.free_contiguous_chunks_no_lock(x as _, &mut region_map);
            }
            while self_mut.prev_link[chunk] != 0 {
                let x = self_mut.prev_link[chunk];
                self_mut.free_contiguous_chunks_no_lock(x as _, &mut region_map);
            }
            self_mut.free_contiguous_chunks_no_lock(chunk as _, &mut region_map);
        }
    }

    fn free_contiguous_chunks(&self, start: Address) -> usize {
        debug!("free_contiguous_chunks: {}", start);
        let mut region_map = self.region_map.write().unwrap();
        let (_sync, self_mut) = self.mut_self_with_sync();
        debug_assert!(start == conversions::chunk_align_down(start));
        let chunk = region_map.address_to_unit(start);
        self_mut.free_contiguous_chunks_no_lock(chunk as _, &mut region_map)
    }

    fn finalize_static_space_map(&self, from: Address, to: Address) {
        // This is only called during boot process by a single thread.
        // It is fine to get a mutable reference.
        let mut region_map = self.region_map.write().unwrap();
        let self_mut: &mut Self = unsafe { self.mut_self() };
        /* establish bounds of discontiguous space */
        let start_address = from;
        let first_chunk = region_map.address_to_unit(start_address);
        let last_chunk = region_map.address_to_unit(to);
        let pages = (1 + last_chunk - first_chunk) * PAGES_IN_CHUNK;
        // start_address=0xb0000000, first_chunk=704, last_chunk=703, unavail_start_chunk=704, trailing_chunks=320, pages=0
        // startAddress=0x68000000 firstChunk=416 lastChunk=703 unavailStartChunk=704 trailingChunks=320 pages=294912
        self_mut.global_page_map.resize_freelist(pages, pages as _);
        // TODO: Clippy favors using iter().flatten() rather than iter() with if-let.
        // https://rust-lang.github.io/rust-clippy/master/index.html#manual_flatten
        // Yi: I am not doing this refactoring right now, as I am not familiar with flatten() and
        // there is no test to ensure the refactoring will be correct.
        #[allow(clippy::manual_flatten)]
        for fl in self_mut.shared_fl_map.iter() {
            if let Some(fl) = fl {
                #[allow(clippy::cast_ref_to_mut)]
                let fl_mut: &mut CommonFreeListPageResource = unsafe {
                    &mut *(*fl as *const CommonFreeListPageResource
                        as *mut CommonFreeListPageResource)
                };
                fl_mut.resize_freelist(start_address);
            }
        }
        // [
        //  2: -1073741825
        //  3: -1073741825
        //  5: -2147482624
        //  2048: -2147483648
        //  2049: -2147482624
        //  2050: 1024
        //  2051: 1024
        // ]
        /* set up the region map free list */
        // self_mut.region_map.alloc_first_fit(first_chunk as _); // block out entire bottom of address range
        // for _ in first_chunk..=last_chunk {
        //     self_mut.region_map.alloc_first_fit(1);
        // }
        // if trailing_chunks != 0 {
        //     let alloced_chunk = self_mut.region_map.alloc_first_fit(trailing_chunks as _);
        //     debug_assert!(
        //         alloced_chunk == unavail_start_chunk as i32,
        //         "{} != {}",
        //         alloced_chunk,
        //         unavail_start_chunk
        //     );
        // }
        let units = region_map.address_to_unit(to + 1usize) - first_chunk;
        region_map.insert(first_chunk, units);
        /* set up the global page map and place chunks on free list */
        let mut first_page = 0;
        for _chunk_index in first_chunk..=last_chunk {
            self_mut.total_available_discontiguous_chunks += 1;
            // self_mut.region_map.free(chunk_index as _, false); // put this chunk on the free list
            self_mut.global_page_map.set_uncoalescable(first_page);
            let alloced_pages = self_mut
                .global_page_map
                .alloc_first_fit(PAGES_IN_CHUNK as _); // populate the global page map
            debug_assert!(alloced_pages == first_page);
            first_page += PAGES_IN_CHUNK as i32;
        }
        self_mut.finalized = true;
    }

    fn is_finalized(&self) -> bool {
        self.finalized
    }

    fn get_descriptor_for_address(&self, address: Address) -> SpaceDescriptor {
        let mut region_map = self.region_map.read().unwrap();
        let index = region_map.address_to_unit(address);
        self.descriptor_map[index]
    }

    fn add_to_cumulative_committed_pages(&self, pages: usize) {
        self.cumulative_committed_pages
            .fetch_add(pages, Ordering::Relaxed);
    }
}

impl Map32 {
    /// # Safety
    ///
    /// The caller needs to guarantee there is no race condition. Either only one single thread
    /// is using this method, or multiple threads are accessing mutally exclusive data (e.g. different indices in arrays).
    /// In other cases, use mut_self_with_sync().
    #[allow(clippy::cast_ref_to_mut)]
    #[allow(clippy::mut_from_ref)]
    unsafe fn mut_self(&self) -> &mut Self {
        &mut *(self as *const Self as *mut Self)
    }

    fn mut_self_with_sync(&self) -> (MutexGuard<()>, &mut Self) {
        let guard = self.sync.lock().unwrap();
        (guard, unsafe { self.mut_self() })
    }

    fn free_contiguous_chunks_no_lock(
        &mut self,
        chunk: usize,
        region_map: &mut ChunkFreelist,
    ) -> usize {
        let (chunks, _) = region_map.free(chunk);
        self.total_available_discontiguous_chunks += chunks as usize;
        let next = self.next_link[chunk as usize];
        let prev = self.prev_link[chunk as usize];
        if next != 0 {
            self.prev_link[next as usize] = prev
        };
        if prev != 0 {
            self.next_link[prev as usize] = next
        };
        self.prev_link[chunk as usize] = 0;
        self.next_link[chunk as usize] = 0;
        for offset in 0..chunks {
            let index = (chunk + offset) as usize;
            let chunk_start = region_map.unit_to_address(index);
            debug!("Clear descriptor for Chunk {}", chunk_start);
            self.descriptor_map[index] = SpaceDescriptor::UNINITIALIZED;
            unsafe { SFT_MAP.clear(chunk_start) };
        }
        chunks as _
    }

    fn get_discontig_freelist_pr_ordinal(&self) -> usize {
        // This is only called during creating a page resource/space/plan/mmtk instance, which is single threaded.
        let self_mut: &mut Self = unsafe { self.mut_self() };
        self_mut.shared_discontig_fl_count += 1;
        self.shared_discontig_fl_count
    }
}

impl Default for Map32 {
    fn default() -> Self {
        Self::new()
    }
}
