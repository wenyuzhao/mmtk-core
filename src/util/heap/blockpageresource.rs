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
use spin::rwlock::RwLock;

const UNINITIALIZED_WATER_MARK: i32 = -1;
const LOCAL_BUFFER_SIZE: usize = 128;

pub struct BlockPageResource<VM: VMBinding> {
    common: CommonPageResource,
    log_pages: usize,
    sync: Mutex<()>,
    head_global_freed_blocks: RwLock<Option<ArrayQueue<Address>>, spin::Yield>,
    global_freed_blocks: SegQueue<ArrayQueue<Address>>,
    worker_local_freed_blocks: Vec<RwLock<ArrayQueue<Address>, spin::Yield>>,
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

    #[inline(always)]
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

    #[inline(always)]
    fn adjust_for_metadata(&self, pages: usize) -> usize {
        pages
    }

    #[inline(always)]
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
        assert!((1 << log_pages) <= PAGES_IN_CHUNK);
        let mut worker_local_freed_blocks = vec![];
        worker_local_freed_blocks.resize_with(*crate::CALC_WORKERS, || {
            spin::rwlock::RwLock::new(ArrayQueue::new(LOCAL_BUFFER_SIZE))
        });
        Self {
            log_pages,
            common: CommonPageResource::new(true, growable, vm_map),
            sync: Mutex::new(()),
            head_global_freed_blocks: Default::default(),
            global_freed_blocks: Default::default(),
            highwater: Atomic::new(start),
            limit: (start + bytes).align_up(BYTES_IN_CHUNK),
            worker_local_freed_blocks,
            _p: PhantomData,
        }
    }

    /// The caller needs to ensure this is called by only one thread.
    #[inline]
    pub unsafe fn alloc_pages_no_lock(
        &self,
        _space_descriptor: SpaceDescriptor,
        reserved_pages: usize,
        required_pages: usize,
        tls: VMThread,
    ) -> Result<PRAllocResult, PRAllocFail> {
        debug_assert_eq!(reserved_pages, required_pages);
        debug_assert_eq!(reserved_pages, 1 << self.log_pages);
        // Fast allocate from the locally-released-blocks list
        let head_global_freed_blocks = self.head_global_freed_blocks.upgradeable_read();
        if let Some(block) = head_global_freed_blocks.as_ref().map(|q| q.pop()).flatten() {
            self.commit_pages(reserved_pages, required_pages, tls);
            return Result::Ok(PRAllocResult {
                start: block,
                pages: required_pages,
                new_chunk: false,
            });
        } else if let Some(blocks) = self.global_freed_blocks.pop() {
            let block = blocks.pop().unwrap();
            let mut head_global_freed_blocks = head_global_freed_blocks.upgrade();
            *head_global_freed_blocks = Some(blocks);
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
                        Some(x + BYTES_IN_CHUNK)
                    }
                }) {
                Ok(a) => a,
                _ => return Result::Err(PRAllocFail),
            };
        assert!(start.is_aligned_to(BYTES_IN_CHUNK));
        let first_block = start;
        let last_block = start + BYTES_IN_CHUNK;
        let block_size = 1usize << (self.log_pages + LOG_BYTES_IN_PAGE as usize);
        let queue = ArrayQueue::new(BYTES_IN_CHUNK / block_size);
        let mut cursor = start + block_size;
        while cursor < last_block {
            queue.push(cursor).unwrap();
            cursor = cursor + block_size;
        }
        self.global_freed_blocks.push(queue);
        self.commit_pages(reserved_pages, required_pages, tls);
        Result::Ok(PRAllocResult {
            start: first_block,
            pages: PAGES_IN_CHUNK,
            new_chunk: true,
        })
    }

    #[inline]
    pub fn release_pages(&self, first: Address) {
        debug_assert!(self.common.contiguous);
        debug_assert!(first.is_aligned_to(1usize << (self.log_pages + LOG_BYTES_IN_PAGE as usize)));
        let pages = 1 << self.log_pages;
        debug_assert!(pages as usize <= self.common.accounting.get_committed_pages());
        self.common.accounting.release(pages as _);
        let id = crate::gc_worker_id().unwrap();
        let failed = self.worker_local_freed_blocks[id]
            .read()
            .push(first)
            .is_err();
        if failed {
            let mut queue = ArrayQueue::new(LOCAL_BUFFER_SIZE);
            {
                let mut lock = self.worker_local_freed_blocks[id].write();
                std::mem::swap(&mut queue, &mut *lock);
                lock.push(first).unwrap();
            }
            if !queue.is_empty() {
                self.global_freed_blocks.push(queue);
            }
        }
    }

    pub fn flush(&self, id: usize) {
        let read = self.worker_local_freed_blocks[id].upgradeable_read();
        if !read.is_empty() {
            let mut queue = ArrayQueue::new(LOCAL_BUFFER_SIZE);
            let mut write = read.upgrade();
            std::mem::swap(&mut queue, &mut *write);
            if !queue.is_empty() {
                self.global_freed_blocks.push(queue)
            }
        }
    }

    pub fn flush_all(&self) {
        for i in 0..self.worker_local_freed_blocks.len() {
            self.flush(i)
        }
    }
}
