use super::pageresource::{PRAllocFail, PRAllocResult};
use super::PageResource;
use crate::util::address::Address;
use crate::util::constants::*;
use crate::util::heap::layout::heap_layout::VMMap;
use crate::util::heap::layout::vm_layout_constants::*;
use crate::util::heap::pageresource::CommonPageResource;
use crate::util::heap::space_descriptor::SpaceDescriptor;
use crate::util::opaque_pointer::*;
use crate::vm::*;
use atomic::{Atomic, Ordering};
use crossbeam_queue::ArrayQueue;
use crossbeam_queue::SegQueue;
use std::marker::PhantomData;
use std::sync::Mutex;

const UNINITIALIZED_WATER_MARK: i32 = -1;

pub struct BlockPageResource<VM: VMBinding> {
    common: CommonPageResource,
    log_pages: usize,
    sync: Mutex<()>,
    released_blocks: SegQueue<Address>,
    head_locally_freed_blocks: spin::RwLock<Option<ArrayQueue<Address>>>,
    locally_freed_blocks: SegQueue<ArrayQueue<Address>>,
    highwater: Atomic<Address>,
    limit: Address,
    _p: PhantomData<VM>,
}

impl<VM: VMBinding> PageResource<VM> for BlockPageResource<VM> {
    #[inline(always)]
    fn common(&self) -> &CommonPageResource {
        &self.common
    }

    #[inline(always)]
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
        let _sync = self.sync.lock().unwrap();
        unsafe { self.alloc_pages_no_lock(space_descriptor, reserved_pages, required_pages, tls) }
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
        let growable = cfg!(target_pointer_width = "64");
        Self {
            log_pages,
            common: CommonPageResource::new(true, growable, vm_map),
            sync: Mutex::new(()),
            released_blocks: Default::default(),
            head_locally_freed_blocks: Default::default(),
            locally_freed_blocks: Default::default(),
            highwater: Atomic::new(start),
            limit: (start + bytes).align_up(BYTES_IN_CHUNK),
            _p: PhantomData,
        }
    }

    /// The caller needs to ensure this is called by only one thread.
    pub unsafe fn alloc_pages_no_lock(
        &self,
        _space_descriptor: SpaceDescriptor,
        reserved_pages: usize,
        required_pages: usize,
        tls: VMThread,
    ) -> Result<PRAllocResult, PRAllocFail> {
        debug_assert_eq!(reserved_pages, required_pages);
        debug_assert_eq!(reserved_pages, 1 << self.log_pages);
        // Fast allocate from the released-blocks list
        if let Some(block) = self.released_blocks.pop() {
            self.commit_pages(reserved_pages, required_pages, tls);
            return Result::Ok(PRAllocResult {
                start: block,
                pages: required_pages,
                new_chunk: false,
            });
        }
        // Fast allocate from the locally-released-blocks list
        let head_locally_freed_blocks = self.head_locally_freed_blocks.upgradeable_read();
        if let Some(block) = head_locally_freed_blocks
            .as_ref()
            .map(|q| q.pop())
            .flatten()
        {
            self.commit_pages(reserved_pages, required_pages, tls);
            return Result::Ok(PRAllocResult {
                start: block,
                pages: required_pages,
                new_chunk: false,
            });
        } else if let Some(blocks) = self.locally_freed_blocks.pop() {
            let block = blocks.pop().unwrap();
            let mut head_locally_freed_blocks = head_locally_freed_blocks.upgrade();
            *head_locally_freed_blocks = Some(blocks);
            self.commit_pages(reserved_pages, required_pages, tls);
            return Result::Ok(PRAllocResult {
                start: block,
                pages: required_pages,
                new_chunk: false,
            });
        }
        // Grow space
        let start: Address =
            match self
                .highwater
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                    if x >= self.limit {
                        None
                    } else {
                        Some(x + (1usize << (self.log_pages + LOG_BYTES_IN_PAGE as usize)))
                    }
                }) {
                Ok(a) => a,
                _ => return Result::Err(PRAllocFail),
            };
        let new_chunk = start.is_aligned_to(BYTES_IN_CHUNK);

        self.commit_pages(reserved_pages, required_pages, tls);
        Result::Ok(PRAllocResult {
            start: start,
            pages: required_pages,
            new_chunk: new_chunk,
        })
    }

    pub fn release_pages(&self, first: Address) {
        debug_assert!(self.common.contiguous);
        debug_assert!(first.is_aligned_to(1usize << (self.log_pages + LOG_BYTES_IN_PAGE as usize)));
        let pages = 1 << self.log_pages;
        debug_assert!(pages as usize <= self.common.accounting.get_committed_pages());
        self.common.accounting.release(pages as _);
        self.released_blocks.push(first);
    }

    pub fn release_bulk(&self, blocks: usize, queue: ArrayQueue<Address>) {
        if blocks == 0 {
            return;
        }
        let pages = blocks << self.log_pages;
        self.common.accounting.release(pages as _);
        self.locally_freed_blocks.push(queue);
    }
}
