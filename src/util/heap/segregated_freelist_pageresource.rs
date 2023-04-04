use super::pageresource::{PRAllocFail, PRAllocResult};
use super::PageResource;
use crate::mmtk::VM_MAP;
use crate::util::address::Address;
use crate::util::heap::layout::vm_layout_constants::*;
use crate::util::heap::layout::VMMap;
use crate::util::heap::pageresource::CommonPageResource;
use crate::util::heap::space_descriptor::SpaceDescriptor;
use crate::util::opaque_pointer::*;
use crate::util::segregated_freelist::SegregatedFreelist;
use crate::util::{constants::*, conversions};
use crate::vm::*;
use std::sync::RwLock;

pub struct SegregatedFreelistPageResource {
    common: CommonPageResource,
    freelist: RwLock<SegregatedFreelist>,
    pub protect_memory_on_release: bool,
}

impl<VM: VMBinding> PageResource<VM> for SegregatedFreelistPageResource {
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
        let mut freelist = self.freelist.write().unwrap();
        if let Some(unit) = freelist.alloc(required_pages) {
            let start = freelist.unit_to_address(unit);
            let units = freelist.size(unit);
            <Self as PageResource<VM>>::commit_pages(self, reserved_pages, units, tls);
            Ok(PRAllocResult {
                start,
                pages: required_pages,
                chunks: 0,
                new_chunk: false,
            })
        } else {
            // expand space
            let required_chunks = crate::policy::space::required_chunks(required_pages);
            let start = self
                .common
                .grow_discontiguous_space(space_descriptor, required_chunks);
            let chunks = VM_MAP.get_contiguous_region_chunks(start);
            if start.is_zero() {
                return Err(PRAllocFail);
            }
            let unit = freelist.address_to_unit(start);
            freelist.insert(unit, chunks * PAGES_IN_CHUNK);
            if let Some(unit) = freelist.alloc(required_pages) {
                let start = freelist.unit_to_address(unit);
                let units = freelist.size(unit);
                <Self as PageResource<VM>>::commit_pages(self, reserved_pages, units, tls);
                Ok(PRAllocResult {
                    start,
                    pages: required_pages,
                    chunks: chunks,
                    new_chunk: true,
                })
            } else {
                Err(PRAllocFail)
            }
        }
    }

    fn get_available_physical_pages(&self) -> usize {
        let mut rtn = self.freelist.read().unwrap().units_on_freelist();
        if !self.common.contiguous {
            let chunks: usize = self
                .common
                .vm_map
                .get_available_discontiguous_chunks()
                .saturating_sub(self.common.vm_map.get_chunk_consumer_count());
            rtn += chunks * PAGES_IN_CHUNK;
        } else if self.common.growable && cfg!(target_pointer_width = "64") {
            rtn = PAGES_IN_SPACE64
                - <SegregatedFreelistPageResource as PageResource<VM>>::reserved_pages(self);
        }

        rtn
    }
}

impl SegregatedFreelistPageResource {
    pub fn new_contiguous(start: Address, bytes: usize, vm_map: &'static dyn VMMap) -> Self {
        let _pages = conversions::bytes_to_pages(bytes);
        let growable = cfg!(target_pointer_width = "64");
        Self {
            common: CommonPageResource::new(true, growable, vm_map),
            freelist: RwLock::new(SegregatedFreelist::new(start)),
            protect_memory_on_release: false,
        }
    }

    pub fn new_discontiguous(vm_map: &'static dyn VMMap) -> Self {
        let _growable = cfg!(target_pointer_width = "64");
        Self {
            common: CommonPageResource::new(false, true, vm_map),
            freelist: RwLock::new(SegregatedFreelist::new(VM_LAYOUT_CONSTANTS.heap_start)),
            protect_memory_on_release: false,
        }
    }

    pub fn release_pages_and_reset_unlog_bits<VM: VMBinding>(&self, first: Address) -> usize {
        debug_assert!(conversions::is_page_aligned(first));
        let mut freelist = self.freelist.write().unwrap();
        let unit = freelist.address_to_unit(first);
        let pages = freelist.size(unit);
        VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
            .as_spec()
            .extract_side_spec()
            .bzero_metadata(first, (pages as usize) << LOG_BYTES_IN_PAGE);
        debug_assert!(pages as usize <= self.common.accounting.get_committed_pages());
        self.common.accounting.release(pages as _);
        let result = freelist.free(unit);
        if !self.common.contiguous {
            // only discontiguous spaces use chunks
            if result.coalesced_units % PAGES_IN_CHUNK == 0 {
                freelist.remove(result.first_unit);
                let start = freelist.unit_to_address(result.first_unit);
                self.common.release_discontiguous_chunks(start);
            }
        }
        pages as _
    }

    pub fn release_pages(&self, first: Address) -> usize {
        debug_assert!(conversions::is_page_aligned(first));
        let mut freelist = self.freelist.write().unwrap();
        let unit = freelist.address_to_unit(first);
        let pages = freelist.size(unit);
        debug_assert!(pages as usize <= self.common.accounting.get_committed_pages());
        self.common.accounting.release(pages as _);
        let result = freelist.free(unit);
        if !self.common.contiguous {
            // only discontiguous spaces use chunks
            if result.coalesced_units % PAGES_IN_CHUNK == 0 {
                freelist.remove(result.first_unit);
                let start = freelist.unit_to_address(result.first_unit);
                self.common.release_discontiguous_chunks(start);
            }
        }
        pages as _
    }
}
