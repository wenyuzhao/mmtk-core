use super::freelistpageresource::CommonFreeListPageResource;
use super::layout::map::Map;
use super::pageresource::{PRAllocFail, PRAllocResult};
use super::PageResource;
use crate::util::address::Address;
use crate::util::alloc::embedded_meta_data::*;
use crate::util::constants::*;
use crate::util::conversions;
use crate::util::generic_freelist;
use crate::util::generic_freelist::GenericFreeList;
use crate::util::heap::layout::heap_layout::VMMap;
use crate::util::heap::layout::vm_layout_constants::*;
use crate::util::heap::pageresource::CommonPageResource;
use crate::util::heap::space_descriptor::SpaceDescriptor;
use crate::util::opaque_pointer::*;
use crate::vm::*;
use atomic::Ordering;
use crossbeam_queue::SegQueue;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicI32;
use std::sync::{Mutex, MutexGuard};

const UNINITIALIZED_WATER_MARK: i32 = -1;

pub struct BlockPageResource<VM: VMBinding> {
    common: CommonPageResource,
    common_flpr: Box<CommonFreeListPageResource>,
    log_pages: usize,
    highwater_mark: AtomicI32,
    sync: Mutex<()>,
    released_blocks: SegQueue<Address>,
    _p: PhantomData<VM>,
}

impl<VM: VMBinding> Deref for BlockPageResource<VM> {
    type Target = CommonFreeListPageResource;

    fn deref(&self) -> &CommonFreeListPageResource {
        &self.common_flpr
    }
}

impl<VM: VMBinding> DerefMut for BlockPageResource<VM> {
    fn deref_mut(&mut self) -> &mut CommonFreeListPageResource {
        &mut self.common_flpr
    }
}

impl<VM: VMBinding> PageResource<VM> for BlockPageResource<VM> {
    fn common(&self) -> &CommonPageResource {
        &self.common
    }
    fn common_mut(&mut self) -> &mut CommonPageResource {
        &mut self.common
    }

    fn alloc_pages(
        &self,
        space_descriptor: SpaceDescriptor,
        reserved_pages: usize,
        required_pages: usize,
        tls: VMThread,
    ) -> Result<PRAllocResult, PRAllocFail> {
        debug_assert_eq!(reserved_pages, required_pages);
        debug_assert_eq!(reserved_pages, 1 << self.log_pages);
        if let Some(block) = self.released_blocks.pop() {
            self.commit_pages(reserved_pages, required_pages, tls);
            return Result::Ok(PRAllocResult {
                start: block,
                pages: required_pages,
                new_chunk: false,
            });
        }
        // FIXME: We need a safe implementation
        #[allow(clippy::cast_ref_to_mut)]
        let self_mut: &mut Self = unsafe { &mut *(self as *const _ as *mut _) };
        let mut sync = self.sync.lock().unwrap();
        let mut new_chunk = false;
        let mut page_offset = self_mut.free_list.alloc(required_pages as _);
        if page_offset == generic_freelist::FAILURE && self.common.growable {
            page_offset =
                self_mut.allocate_contiguous_chunks(space_descriptor, required_pages, &mut sync);
            new_chunk = true;
        }

        if page_offset == generic_freelist::FAILURE {
            return Result::Err(PRAllocFail);
        } else {
            if page_offset > self.highwater_mark.load(Ordering::SeqCst) {
                if self.highwater_mark.load(Ordering::SeqCst) == UNINITIALIZED_WATER_MARK
                    || (page_offset ^ self.highwater_mark.load(Ordering::SeqCst))
                        > PAGES_IN_REGION as i32
                {
                    new_chunk = true;
                }
                self.highwater_mark.store(page_offset, Ordering::SeqCst);
            }
        }

        let rtn = self.start + conversions::pages_to_bytes(page_offset as _);
        // The meta-data portion of reserved Pages was committed above.
        self.commit_pages(reserved_pages, required_pages, tls);
        Result::Ok(PRAllocResult {
            start: rtn,
            pages: required_pages,
            new_chunk,
        })
    }

    fn adjust_for_metadata(&self, pages: usize) -> usize {
        pages
    }

    fn bpr(&self) -> Option<&BlockPageResource<VM>> {
        Some(self)
    }
}

impl<VM: VMBinding> BlockPageResource<VM> {
    pub fn new_contiguous(
        log_pages: usize,
        start: Address,
        bytes: usize,
        vm_map: &'static VMMap,
    ) -> Self {
        let pages = conversions::bytes_to_pages(bytes);
        // We use MaybeUninit::uninit().assume_init(), which is nul, for a Box value, which cannot be null.
        // FIXME: We should try either remove this kind of circular dependency or use MaybeUninit<T> instead of Box<T>
        #[allow(invalid_value)]
        #[allow(clippy::uninit_assumed_init)]
        let common_flpr = unsafe {
            let mut common_flpr = Box::new(CommonFreeListPageResource {
                free_list: MaybeUninit::uninit().assume_init(),
                start,
            });
            ::std::ptr::write(
                &mut common_flpr.free_list,
                vm_map.create_parent_freelist(&common_flpr, pages, PAGES_IN_REGION as _),
            );
            common_flpr
        };
        let growable = cfg!(target_pointer_width = "64");
        Self {
            log_pages,
            common: CommonPageResource::new(true, growable, vm_map),
            common_flpr,
            highwater_mark: AtomicI32::new(UNINITIALIZED_WATER_MARK),
            sync: Mutex::new(()),
            released_blocks: SegQueue::new(),
            _p: PhantomData,
        }
    }

    fn allocate_contiguous_chunks(
        &mut self,
        space_descriptor: SpaceDescriptor,
        pages: usize,
        _sync: &mut MutexGuard<()>,
    ) -> i32 {
        let mut rtn = generic_freelist::FAILURE;
        let required_chunks = crate::policy::space::required_chunks(pages);
        let region = self
            .common
            .grow_discontiguous_space(space_descriptor, required_chunks);

        if !region.is_zero() {
            let region_start = conversions::bytes_to_pages(region - self.start);
            let region_end = region_start + (required_chunks * PAGES_IN_CHUNK) - 1;
            self.free_list.set_uncoalescable(region_start as _);
            self.free_list.set_uncoalescable(region_end as i32 + 1);
            for p in (region_start..region_end).step_by(PAGES_IN_CHUNK) {
                let liberated;
                if p != region_start {
                    self.free_list.clear_uncoalescable(p as _);
                }
                liberated = self.free_list.free(p as _, true); // add chunk to our free list
                debug_assert!(liberated as usize == PAGES_IN_CHUNK + (p - region_start));
            }
            rtn = self.free_list.alloc(pages as _); // re-do the request which triggered this call
        }
        rtn
    }

    /// The caller needs to ensure this is called by only one thread.
    pub unsafe fn alloc_pages_no_lock(
        &self,
        space_descriptor: SpaceDescriptor,
        reserved_pages: usize,
        required_pages: usize,
        tls: VMThread,
    ) -> Result<PRAllocResult, PRAllocFail> {
        debug_assert_eq!(reserved_pages, required_pages);
        debug_assert_eq!(reserved_pages, 1 << self.log_pages);
        if let Some(block) = self.released_blocks.pop() {
            self.commit_pages(reserved_pages, required_pages, tls);
            return Result::Ok(PRAllocResult {
                start: block,
                pages: required_pages,
                new_chunk: false,
            });
        }
        // FIXME: We need a safe implementation
        #[allow(clippy::cast_ref_to_mut)]
        let self_mut: &mut Self = &mut *(self as *const _ as *mut _);
        let mut new_chunk = false;
        let mut page_offset = self_mut.free_list.alloc(required_pages as _);
        if page_offset == generic_freelist::FAILURE && self.common.growable {
            page_offset =
                self_mut.allocate_contiguous_chunks_no_lock(space_descriptor, required_pages);
            new_chunk = true;
        }

        if page_offset == generic_freelist::FAILURE {
            return Result::Err(PRAllocFail);
        } else {
            if page_offset > self.highwater_mark.load(Ordering::SeqCst) {
                if self.highwater_mark.load(Ordering::SeqCst) == UNINITIALIZED_WATER_MARK
                    || (page_offset ^ self.highwater_mark.load(Ordering::SeqCst))
                        > PAGES_IN_REGION as i32
                {
                    new_chunk = true;
                }
                self.highwater_mark.store(page_offset, Ordering::SeqCst);
            }
        }

        let rtn = self.start + conversions::pages_to_bytes(page_offset as _);
        // The meta-data portion of reserved Pages was committed above.
        self.commit_pages(reserved_pages, required_pages, tls);
        Result::Ok(PRAllocResult {
            start: rtn,
            pages: required_pages,
            new_chunk,
        })
    }

    /// The caller needs to ensure this is called by only one thread.
    unsafe fn allocate_contiguous_chunks_no_lock(
        &mut self,
        space_descriptor: SpaceDescriptor,
        pages: usize,
    ) -> i32 {
        let mut rtn = generic_freelist::FAILURE;
        let required_chunks = crate::policy::space::required_chunks(pages);
        let region = self
            .common
            .grow_discontiguous_space(space_descriptor, required_chunks);

        if !region.is_zero() {
            let region_start = conversions::bytes_to_pages(region - self.start);
            let region_end = region_start + (required_chunks * PAGES_IN_CHUNK) - 1;
            self.free_list.set_uncoalescable(region_start as _);
            self.free_list.set_uncoalescable(region_end as i32 + 1);
            for p in (region_start..region_end).step_by(PAGES_IN_CHUNK) {
                let liberated;
                if p != region_start {
                    self.free_list.clear_uncoalescable(p as _);
                }
                liberated = self.free_list.free(p as _, true); // add chunk to our free list
                debug_assert!(liberated as usize == PAGES_IN_CHUNK + (p - region_start));
            }
            rtn = self.free_list.alloc(pages as _); // re-do the request which triggered this call
        }
        rtn
    }

    pub fn release_pages(&self, first: Address) {
        debug_assert!(self.common.contiguous);
        debug_assert!(first.is_aligned_to(1usize << (self.log_pages + LOG_BYTES_IN_PAGE as usize)));
        let pages = 1 << self.log_pages;
        debug_assert!(pages as usize <= self.common.accounting.get_committed_pages());
        self.common.accounting.release(pages as _);
        self.released_blocks.push(first);
    }
}
