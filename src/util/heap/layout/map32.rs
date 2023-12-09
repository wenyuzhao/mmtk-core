use super::map::VMMap;
use crate::mmtk::SFT_MAP;
use crate::util::alloc::embedded_meta_data::LOG_PAGES_IN_REGION;
use crate::util::conversions;
use crate::util::freelist::FreeList;
use crate::util::heap::freelistpageresource::CommonFreeListPageResource;
use crate::util::heap::layout::heap_parameters::*;
use crate::util::heap::layout::vm_layout::*;
use crate::util::heap::space_descriptor::SpaceDescriptor;
use crate::util::int_array_freelist::IntArrayFreeList;
use crate::util::Address;
use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

const NO_SPACE: u8 = 0;
const SMALL_SPACE: u8 = 1;
const LARGE_SPACE: u8 = 2;

pub struct Map32 {
    sync: Mutex<()>,
    inner: UnsafeCell<Map32Inner>,
}

#[doc(hidden)]
pub struct Map32Inner {
    prev_link: Vec<i32>,
    next_link: Vec<i32>,
    chunk_space: Vec<u8>,
    region_map: IntArrayFreeList,
    large_region_map: IntArrayFreeList,
    global_page_map: IntArrayFreeList,
    shared_discontig_fl_count: usize,
    shared_fl_map: Vec<Option<NonNull<CommonFreeListPageResource>>>,
    total_available_discontiguous_chunks: usize,
    total_available_large_discontiguous_chunks: usize,
    finalized: bool,
    descriptor_map: Vec<SpaceDescriptor>,

    // TODO: Is this the right place for this field?
    // This used to be a global variable. When we remove global states, this needs to be put somewhere.
    // Currently I am putting it here, as for where this variable is used, we already have
    // references to vm_map - so it is convenient to put it here.
    cumulative_committed_pages: AtomicUsize,
    out_of_virtual_space: AtomicBool,
}

unsafe impl Send for Map32 {}
unsafe impl Sync for Map32 {}

impl Map32 {
    pub fn new() -> Self {
        let max_chunks = vm_layout().max_chunks();
        Map32 {
            inner: UnsafeCell::new(Map32Inner {
                prev_link: vec![-1; max_chunks],
                next_link: vec![-1; max_chunks],
                chunk_space: vec![NO_SPACE; max_chunks],
                region_map: IntArrayFreeList::new(max_chunks, max_chunks as _, 1),
                large_region_map: IntArrayFreeList::new(max_chunks, max_chunks as _, 1),
                global_page_map: IntArrayFreeList::new(1, 1, MAX_SPACES),
                shared_discontig_fl_count: 0,
                shared_fl_map: vec![None; MAX_SPACES],
                total_available_discontiguous_chunks: 0,
                total_available_large_discontiguous_chunks: 0,
                finalized: false,
                descriptor_map: vec![SpaceDescriptor::UNINITIALIZED; max_chunks],
                cumulative_committed_pages: AtomicUsize::new(0),
                out_of_virtual_space: AtomicBool::new(false),
            }),
            sync: Mutex::new(()),
        }
    }

    fn is_large_chunk_allocation(chunks: usize) -> bool {
        chunks > 1
    }

    fn get_large_chunk_reserve_boundary() -> Address {
        if let Some(small_chunk_space_size) = vm_layout().small_chunk_space_size {
            let small_chunk_space_end =
                (vm_layout().heap_start + small_chunk_space_size).align_up(BYTES_IN_CHUNK);
            return small_chunk_space_end;
        }
        const LARGE_CHUNK_RESERVE_RATIO: f64 = 1f64 / 32f64;
        const MIN_LARGE_CHUNK_RESERVE: usize = 512 << 20;
        let virtual_space_size = vm_layout().heap_end - vm_layout().heap_start;
        let large_chunk_reserve_bytes = usize::max(
            MIN_LARGE_CHUNK_RESERVE,
            (virtual_space_size as f64 * LARGE_CHUNK_RESERVE_RATIO).ceil() as usize,
        );
        let large_chunk_reserve_chunks =
            (large_chunk_reserve_bytes >> LOG_BYTES_IN_CHUNK).next_power_of_two();
        vm_layout().heap_end - (large_chunk_reserve_chunks << LOG_BYTES_IN_CHUNK)
    }
}

impl std::ops::Deref for Map32 {
    type Target = Map32Inner;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
    }
}

impl VMMap for Map32 {
    fn insert(&self, start: Address, extent: usize, descriptor: SpaceDescriptor) {
        // Each space will call this on exclusive address ranges. It is fine to mutate the descriptor map,
        // as each space will update different indices.
        let self_mut: &mut Map32Inner = unsafe { self.mut_self() };
        let mut e = 0;
        while e < extent {
            let index = (start + e).chunk_index();
            assert!(
                self.descriptor_map[index].is_empty(),
                "Conflicting virtual address request"
            );
            self_mut.descriptor_map[index] = descriptor;
            e += BYTES_IN_CHUNK;
        }
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

    unsafe fn bind_freelist(&self, pr: *const CommonFreeListPageResource) {
        let ordinal: usize = (*pr)
            .free_list
            .downcast_ref::<IntArrayFreeList>()
            .unwrap()
            .get_ordinal() as usize;
        let self_mut: &mut Map32Inner = self.mut_self();
        self_mut.shared_fl_map[ordinal] = Some(NonNull::new_unchecked(pr as *mut _));
    }

    unsafe fn allocate_contiguous_chunks(
        &self,
        descriptor: SpaceDescriptor,
        chunks: usize,
        head: Address,
    ) -> Address {
        let (_sync, self_mut) = self.mut_self_with_sync();
        let mut large = false;
        let chunk = if !Self::is_large_chunk_allocation(chunks) {
            self_mut.region_map.alloc(chunks as _)
        } else {
            if crate::verbose(3) {
                gc_log!("Alloc {} large chunks", chunks);
            }
            large = true;
            self_mut.large_region_map.alloc(chunks as _)
        };
        if chunk == -1 {
            self.out_of_virtual_space.store(true, Ordering::SeqCst);
            gc_log!([1]
                "WARNING: Failed to allocate {} chunks. total-free-chunks={} total-free-large-chunks={}",
                chunks,
                self.total_available_discontiguous_chunks,
                self.total_available_large_discontiguous_chunks,
            );
            return Address::zero();
        }
        self_mut.total_available_discontiguous_chunks -= chunks;
        if large {
            self_mut.total_available_large_discontiguous_chunks -= chunks;
        }
        self_mut.chunk_space[chunk as usize] = if large { LARGE_SPACE } else { SMALL_SPACE };
        let rtn: Address = conversions::chunk_index_to_address(chunk as _);
        self.insert(rtn, chunks << LOG_BYTES_IN_CHUNK, descriptor);
        if head.is_zero() {
            debug_assert!(self.next_link[chunk as usize] == -1);
        } else {
            self_mut.next_link[chunk as usize] = head.chunk_index() as _;
            self_mut.prev_link[head.chunk_index()] = chunk;
        }
        debug_assert!(self.prev_link[chunk as usize] == -1);
        rtn
    }

    fn get_next_contiguous_region(&self, start: Address) -> Address {
        if start.is_zero() {
            return Address::ZERO;
        }
        debug_assert!(start == conversions::chunk_align_down(start));
        let chunk = start.chunk_index();
        if self.next_link[chunk] == -1 {
            unsafe { Address::zero() }
        } else {
            let a = self.next_link[chunk];
            conversions::chunk_index_to_address(a as _)
        }
    }

    fn get_contiguous_region_chunks(&self, start: Address) -> usize {
        debug_assert!(start == conversions::chunk_align_down(start));
        let chunk = start.chunk_index();
        if self.chunk_space[chunk as usize] == SMALL_SPACE {
            self.region_map.size(chunk as i32) as _
        } else {
            self.large_region_map.size(chunk as i32) as _
        }
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
    #[allow(clippy::while_immutable_condition)]
    fn free_all_chunks(&self, any_chunk: Address) {
        debug!("free_all_chunks: {}", any_chunk);
        let (_sync, self_mut) = self.mut_self_with_sync();
        debug_assert!(any_chunk == conversions::chunk_align_down(any_chunk));
        if !any_chunk.is_zero() {
            let chunk = any_chunk.chunk_index();
            while self_mut.next_link[chunk] != -1 {
                let x = self_mut.next_link[chunk];
                self.free_contiguous_chunks_no_lock(x);
            }
            while self_mut.prev_link[chunk] != -1 {
                let x = self_mut.prev_link[chunk];
                self.free_contiguous_chunks_no_lock(x);
            }
            self.free_contiguous_chunks_no_lock(chunk as _);
        }
    }

    unsafe fn free_contiguous_chunks(&self, start: Address) -> usize {
        debug!("free_contiguous_chunks: {}", start);
        let (_sync, _) = self.mut_self_with_sync();
        debug_assert!(start == conversions::chunk_align_down(start));
        let chunk = start.chunk_index();
        let freed_chunks = self.free_contiguous_chunks_no_lock(chunk as _);
        if cfg!(feature = "munmap") {
            let result =
                crate::mmtk::MMAPPER.ensure_unmapped(start, freed_chunks << LOG_PAGES_IN_REGION);
            assert!(result.is_ok(), "{:?}", result);
        } else if cfg!(feature = "madv_dontneed") {
            unsafe {
                let result = libc::madvise(
                    start.to_mut_ptr(),
                    freed_chunks << LOG_BYTES_IN_CHUNK,
                    libc::MADV_DONTNEED,
                );
                assert_ne!(result, -1, "{:?}", std::io::Error::last_os_error());
            }
        }
        freed_chunks
    }

    fn finalize_static_space_map(&self, from: Address, to: Address) {
        // This is only called during boot process by a single thread.
        // It is fine to get a mutable reference.
        let self_mut: &mut Map32Inner = unsafe { self.mut_self() };
        /* establish bounds of discontiguous space */
        let start_address = from;
        let first_chunk = start_address.chunk_index();
        let last_chunk = to.chunk_index();
        let unavail_start_chunk = last_chunk + 1;
        let trailing_chunks = vm_layout().max_chunks() - unavail_start_chunk;
        let pages = (1 + last_chunk - first_chunk) * PAGES_IN_CHUNK;
        self_mut.global_page_map.resize_freelist(pages, pages as _);
        // TODO: Clippy favors using iter().flatten() rather than iter() with if-let.
        // https://rust-lang.github.io/rust-clippy/master/index.html#manual_flatten
        // Yi: I am not doing this refactoring right now, as I am not familiar with flatten() and
        // there is no test to ensure the refactoring will be correct.
        #[allow(clippy::manual_flatten)]
        for fl in self_mut.shared_fl_map.iter().copied() {
            if let Some(mut fl) = fl {
                let fl_mut = unsafe { fl.as_mut() };
                fl_mut.resize_freelist(start_address);
            }
        }
        /* set up the region map free list */
        if first_chunk != 0 {
            self_mut.region_map.alloc_first_fit(first_chunk as _); // block out entire bottom of address range
        }
        for _ in first_chunk..=last_chunk {
            self_mut.region_map.alloc_first_fit(1);
        }
        if first_chunk != 0 {
            self_mut.large_region_map.alloc_first_fit(first_chunk as _); // block out entire bottom of address range
        }
        for _ in first_chunk..=last_chunk {
            self_mut.large_region_map.alloc_first_fit(1);
        }
        if trailing_chunks != 0 {
            let alloced_chunk = self_mut.region_map.alloc_first_fit(trailing_chunks as _);
            debug_assert!(
                alloced_chunk == unavail_start_chunk as i32,
                "{} != {}",
                alloced_chunk,
                unavail_start_chunk
            );
            let alloced_chunk = self_mut
                .large_region_map
                .alloc_first_fit(trailing_chunks as _);
            debug_assert!(
                alloced_chunk == unavail_start_chunk as i32,
                "{} != {}",
                alloced_chunk,
                unavail_start_chunk
            );
        }
        /* set up the global page map and place chunks on free list */
        let mut first_page = 0;
        let large_chunk_boundary = Self::get_large_chunk_reserve_boundary();
        for chunk_index in first_chunk..=last_chunk {
            self_mut.total_available_discontiguous_chunks += 1;
            if conversions::chunk_index_to_address(chunk_index) >= large_chunk_boundary {
                self_mut.total_available_large_discontiguous_chunks += 1;
                self_mut.large_region_map.free(chunk_index as _, false); // put this chunk on the free list
            } else {
                self_mut.region_map.free(chunk_index as _, false); // put this chunk on the free list
            }
            self_mut.global_page_map.set_uncoalescable(first_page);
            let alloced_pages = self_mut
                .global_page_map
                .alloc_first_fit(PAGES_IN_CHUNK as _); // populate the global page map
            debug_assert!(alloced_pages == first_page);
            first_page += PAGES_IN_CHUNK as i32;
        }
        if crate::verbose(3) {
            gc_log!(
                "total_available_discontiguous_chunks = {}",
                self.total_available_discontiguous_chunks
            );
            gc_log!(
                "total_available_large_discontiguous_chunks = {}",
                self.total_available_large_discontiguous_chunks
            );
        }
        self_mut.finalized = true;
    }

    fn is_finalized(&self) -> bool {
        self.finalized
    }

    #[cfg_attr(feature = "inline_pragmas", inline)]
    fn get_descriptor_for_address(&self, address: Address) -> SpaceDescriptor {
        let index = address.chunk_index();
        self.descriptor_map[index]
    }

    fn add_to_cumulative_committed_pages(&self, pages: usize) {
        self.cumulative_committed_pages
            .fetch_add(pages, Ordering::Relaxed);
    }

    fn out_of_virtual_space(&self) -> bool {
        self.out_of_virtual_space.load(Ordering::SeqCst)
    }

    fn reset_out_of_virtual_space(&self) {
        self.out_of_virtual_space.store(false, Ordering::SeqCst);
    }

    fn available_chunks(&self) -> usize {
        self.total_available_discontiguous_chunks
    }
}

impl Map32 {
    /// # Safety
    ///
    /// The caller needs to guarantee there is no race condition. Either only one single thread
    /// is using this method, or multiple threads are accessing mutally exclusive data (e.g. different indices in arrays).
    /// In other cases, use mut_self_with_sync().
    #[allow(clippy::mut_from_ref)]
    unsafe fn mut_self(&self) -> &mut Map32Inner {
        &mut *self.inner.get()
    }

    fn mut_self_with_sync(&self) -> (MutexGuard<()>, &mut Map32Inner) {
        let guard = self.sync.lock().unwrap();
        (guard, unsafe { self.mut_self() })
    }

    fn free_contiguous_chunks_no_lock(&self, chunk: i32) -> usize {
        unsafe { Self::free_contiguous_chunks_no_lock_impl(self.mut_self(), chunk) }
    }

    fn free_contiguous_chunks_no_lock_impl(inner: &mut Map32Inner, chunk: i32) -> usize {
        let mut large = false;
        let chunks = if inner.chunk_space[chunk as usize] == SMALL_SPACE {
            inner.region_map.free(chunk, false)
        } else {
            large = true;
            inner.large_region_map.free(chunk, false)
        };
        inner.total_available_discontiguous_chunks += chunks as usize;
        for c in chunk..chunk + chunks {
            inner.chunk_space[c as usize] = NO_SPACE;
        }
        if large {
            inner.total_available_large_discontiguous_chunks += chunks as usize;
        }
        let next = inner.next_link[chunk as usize];
        let prev = inner.prev_link[chunk as usize];
        if next != -1 {
            inner.prev_link[next as usize] = prev
        };
        if prev != -1 {
            inner.next_link[prev as usize] = next
        };
        inner.prev_link[chunk as usize] = -1;
        inner.next_link[chunk as usize] = -1;
        for offset in 0..chunks {
            let index = (chunk + offset) as usize;
            let chunk_start = conversions::chunk_index_to_address(index);
            debug!("Clear descriptor for Chunk {}", chunk_start);
            inner.descriptor_map[index] = SpaceDescriptor::UNINITIALIZED;
            unsafe { SFT_MAP.clear(chunk_start) };
        }
        chunks as _
    }

    fn get_discontig_freelist_pr_ordinal(&self) -> usize {
        // This is only called during creating a page resource/space/plan/mmtk instance, which is single threaded.
        let self_mut: &mut Map32Inner = unsafe { self.mut_self() };
        self_mut.shared_discontig_fl_count += 1;
        self.shared_discontig_fl_count
    }
}

impl Default for Map32 {
    fn default() -> Self {
        Self::new()
    }
}
