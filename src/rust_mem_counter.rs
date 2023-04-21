use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::AtomicUsize,
};

use atomic::Ordering;

use crate::util::constants::LOG_BYTES_IN_PAGE;

struct SystemAllocatorWithCounter {
    live_size: AtomicUsize,
    max_live_size: AtomicUsize,
}

unsafe impl GlobalAlloc for SystemAllocatorWithCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        assert!(cfg!(feature = "rust_mem_counter"));
        let size = layout.size();
        let current_size = self.live_size.fetch_add(size, Ordering::SeqCst) + size;
        self.max_live_size.fetch_max(current_size, Ordering::SeqCst);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        assert!(cfg!(feature = "rust_mem_counter"));
        self.live_size.fetch_sub(layout.size(), Ordering::SeqCst);
        System.dealloc(ptr, layout)
    }
}

#[cfg_attr(feature = "rust_mem_counter", global_allocator)]
static GLOBAL: SystemAllocatorWithCounter = SystemAllocatorWithCounter {
    live_size: AtomicUsize::new(0),
    max_live_size: AtomicUsize::new(0),
};

static MMAP_SIZE: AtomicUsize = AtomicUsize::new(0);

static PEAK_MMAP_SIZE: AtomicUsize = AtomicUsize::new(0);

static RSS: AtomicUsize = AtomicUsize::new(0);
static PEAK_RSS: AtomicUsize = AtomicUsize::new(0);
static VIRT: AtomicUsize = AtomicUsize::new(0);
static PEAK_VIRT: AtomicUsize = AtomicUsize::new(0);

pub fn dump() {
    if cfg!(feature = "rust_mem_counter") {
        update_rss();
        gc_log!(
            " - rust heap: {} bytes, peak = {} bytes",
            GLOBAL.live_size.load(Ordering::SeqCst),
            GLOBAL.max_live_size.load(Ordering::SeqCst),
        );
        gc_log!(
            " - mmap: {} bytes, peak = {} bytes",
            MMAP_SIZE.load(Ordering::SeqCst),
            PEAK_MMAP_SIZE.load(Ordering::SeqCst),
        );
        if RSS.load(Ordering::SeqCst) != 0 {
            gc_log!(
                " - VmRss: {} bytes, peak = {} bytes",
                RSS.load(Ordering::SeqCst),
                PEAK_RSS.load(Ordering::SeqCst),
            );
            gc_log!(
                " - VmSize: {} bytes, peak = {} bytes",
                VIRT.load(Ordering::SeqCst),
                PEAK_VIRT.load(Ordering::SeqCst),
            );
        }
    }
}

pub fn record_mmap(bytes: usize) {
    if cfg!(feature = "rust_mem_counter") {
        let current_size = MMAP_SIZE.fetch_add(bytes, Ordering::SeqCst) + bytes;
        PEAK_MMAP_SIZE.fetch_max(current_size, Ordering::SeqCst);
    }
}

pub fn record_munmap(bytes: usize) {
    if cfg!(feature = "rust_mem_counter") {
        MMAP_SIZE.fetch_sub(bytes, Ordering::SeqCst);
    }
}

pub fn update_rss() {
    if cfg!(feature = "rust_mem_counter") {
        match std::fs::read_to_string("/proc/self/statm") {
            Ok(statm) => {
                let mut values = statm.trim().split_ascii_whitespace();
                let virt = values.next().unwrap().parse::<usize>().unwrap();
                let rss = values.next().unwrap().parse::<usize>().unwrap();
                VIRT.store(virt << LOG_BYTES_IN_PAGE, Ordering::SeqCst);
                PEAK_VIRT.fetch_max(virt << LOG_BYTES_IN_PAGE, Ordering::SeqCst);
                RSS.store(rss << LOG_BYTES_IN_PAGE, Ordering::SeqCst);
                PEAK_RSS.fetch_max(rss << LOG_BYTES_IN_PAGE, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}
