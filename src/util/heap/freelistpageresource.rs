use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use util::address::Address;
use util::heap::pageresource::CommonPageResource;
use super::layout::heap_layout::VM_MAP;
use util::heap::layout::vm_layout_constants::*;
use util::heap::layout::freelist::Freelist;
use util::constants::*;
use policy::space::Space;
use super::PageResource;



#[derive(Debug)]
pub struct FreeListPageResource {
    common: CommonPageResource,
    freelist: Mutex<Freelist>,
}



impl PageResource for FreeListPageResource {
    unsafe fn unsafe_common(&self) -> *mut CommonPageResource {
        &self.common as *const _ as usize as *mut _
    }

    fn alloc_pages(&self, reserved_pages: usize, required_pages: usize, zeroed: bool, space: &impl Space, tls: *mut ::libc::c_void) -> Option<Address> {
        match self.alloc_pages_aux(reserved_pages, required_pages, zeroed, false, tls) {
            Some((rtn, new_chunk)) => {
                space.grow_space(rtn, ::util::conversions::pages_to_bytes(required_pages), new_chunk);
                Some(rtn)
            },
            _ => None
        }
    }

    fn release_pages(&self, first: Address) -> usize {
        debug_assert!(::util::conversions::is_page_aligned(first));
        let page_index = ::util::conversions::bytes_to_pages(first.as_usize());
        let mut freelist = self.freelist.lock().unwrap();
        let pages = freelist.get_size(page_index);
        self.common.reserved.fetch_sub(pages, Ordering::Relaxed);
        self.common.committed.fetch_sub(pages, Ordering::Relaxed);
        freelist.dealloc(page_index)
    }

    fn release_all(&self) {
        self.common.reserved.store(0, Ordering::Relaxed);
        self.common.committed.store(0, Ordering::Relaxed);
        
        let mut chunk_start = {
            let mut lock = self.common().head_discontiguous_region.lock().unwrap();
            let start = *lock;
            *lock = unsafe { Address::zero() };
            start
        };
        while !chunk_start.is_zero() {
            VM_MAP.release_contiguous_chunks(chunk_start);
            chunk_start = ::util::heap::layout::heap_layout::VM_MAP.get_next_contiguous_region(chunk_start).unwrap_or(unsafe { Address::zero() });
        }
        self.freelist.lock().unwrap().reset()
    }
}

impl FreeListPageResource {
    pub fn new_contiguous(start: Address, bytes: usize,  metadata_pages_per_region: usize) -> Self {
        unimplemented!()
    }

    pub fn new_discontiguous(metadata_pages_per_region: usize, space_descriptor: usize) -> Self {
        Self {
            common: CommonPageResource {
                reserved: AtomicUsize::new(0),
                committed: AtomicUsize::new(0),
                contiguous: false,
                growable: true,
                space_descriptor,
                metadata_pages_per_region,
                head_discontiguous_region: Mutex::new(unsafe { Address::zero() }),
            },
            freelist: Mutex::new(Freelist::new()),
        }
    }

    fn alloc_pages_aux(&self, reserved_pages: usize, required_pages: usize, zeroed: bool, is_retrial: bool, tls: *mut ::libc::c_void) -> Option<(Address, bool)> {
        let mut freelist = self.freelist.lock().unwrap();
        match freelist.alloc(required_pages) {
            Some(page_index) => {
                self.commit_pages(reserved_pages, required_pages, tls);
                let page_address = unsafe { Address::from_usize(page_index << LOG_BYTES_IN_PAGE) };
                if zeroed {
                    unsafe { ::std::ptr::write_bytes::<u8>(page_address.to_ptr_mut(), 0, required_pages << LOG_BYTES_IN_PAGE) }
                }
                Some((page_address, false))
            },
            _ => {
                if is_retrial {
                    return None;
                }
                let required_chunks = ::policy::space::required_chunks(required_pages);
                match self.allocate_contiguous_chunks(required_chunks) {
                    Some(chunk_start) => {
                        let page_index = chunk_start.as_usize() >> LOG_BYTES_IN_PAGE;
                        let pages = (required_chunks << LOG_BYTES_IN_CHUNK) >> LOG_BYTES_IN_PAGE;
                        freelist.insert_free(page_index + self.common.metadata_pages_per_region, pages - self.common.metadata_pages_per_region);
                        ::std::mem::drop(freelist);
                        self.alloc_pages_aux(reserved_pages, required_pages, zeroed, true, tls).map(|(a, _)| (a, true))
                    },
                    _ => return None
                }
            }
        }
    }

}