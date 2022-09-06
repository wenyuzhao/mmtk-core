use crate::util::address::{Address, ByteSize};
use crate::util::heap::layout::vm_layout_constants::*;
use std::panic;
use std::sync::mpsc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

// Sometimes we need to mmap for tests. We want to ensure that the mmapped addresses do not overlap
// for different tests, so we organize them here.

pub(crate) struct MmapTestRegion {
    pub start: Address,
    pub size: ByteSize,
}
impl MmapTestRegion {
    pub const fn reserve_before(prev: MmapTestRegion, size: ByteSize) -> MmapTestRegion {
        Self::reserve_before_address(prev.start, size)
    }
    pub const fn reserve_before_address(addr: Address, size: ByteSize) -> MmapTestRegion {
        MmapTestRegion {
            start: addr.sub(size),
            size,
        }
    }
}

// util::heap::layout::fragmented_mmapper
pub(crate) fn fragmented_mmapper_test_region() -> MmapTestRegion {
    MmapTestRegion::reserve_before_address(VM_LAYOUT_CONSTANTS.heap_start, MMAP_CHUNK_BYTES * 2)
}
// util::heap::layout::byte_map_mmaper
pub(crate) fn byte_map_mmapper_test_region() -> MmapTestRegion {
    MmapTestRegion::reserve_before(fragmented_mmapper_test_region(), MMAP_CHUNK_BYTES * 2)
}
// util::memory
pub(crate) fn memory_test_region() -> MmapTestRegion {
    MmapTestRegion::reserve_before(byte_map_mmapper_test_region(), MMAP_CHUNK_BYTES)
}

// https://github.com/rust-lang/rfcs/issues/2798#issuecomment-552949300
pub fn panic_after<T, F>(millis: u64, f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T,
    F: Send + 'static,
{
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let val = f();
        done_tx.send(()).expect("Unable to send completion signal");
        val
    });

    match done_rx.recv_timeout(Duration::from_millis(millis)) {
        Ok(_) => handle.join().expect("Thread panicked"),
        Err(e) => panic!("Thread took too long: {}", e),
    }
}

lazy_static! {
    // A global lock to make tests serial.
    // If we do want more parallelism, we can allow each set of tests to have their own locks. But it seems unnecessary for now.
    static ref SERIAL_TEST_LOCK: Mutex<()> = Mutex::default();
}

// force some tests to be executed serially
pub fn serial_test<F>(f: F)
where
    F: FnOnce(),
{
    let _lock = SERIAL_TEST_LOCK.lock();
    f();
}

// Always execute a cleanup closure no matter the test panics or not.
pub fn with_cleanup<T, C>(test: T, cleanup: C)
where
    T: FnOnce() + panic::UnwindSafe,
    C: FnOnce(),
{
    let res = panic::catch_unwind(test);
    cleanup();
    if let Err(e) = res {
        panic::resume_unwind(e);
    }
}
