use spin::Mutex;

use super::heap_parameters::*;
use crate::util::constants::*;
use crate::util::Address;

use crate::util::conversions::{chunk_align_down, chunk_align_up};

/**
 * log_2 of the coarsest unit of address space allocation.
 * <p>
 * In the 32-bit VM layout, this determines the granularity of
 * allocation in a discontigouous space.  In the 64-bit layout,
 * this determines the growth factor of the large contiguous spaces
 * that we provide.
 */
pub const LOG_BYTES_IN_CHUNK: usize = 22;

/** Coarsest unit of address space allocation. */
pub const BYTES_IN_CHUNK: usize = 1 << LOG_BYTES_IN_CHUNK;
pub const CHUNK_MASK: usize = (1 << LOG_BYTES_IN_CHUNK) - 1;

/** Coarsest unit of address space allocation, in pages */
pub const PAGES_IN_CHUNK: usize = 1 << (LOG_BYTES_IN_CHUNK - LOG_BYTES_IN_PAGE as usize);

/** Granularity at which we map and unmap virtual address space in the heap */
pub const LOG_MMAP_CHUNK_BYTES: usize = LOG_BYTES_IN_CHUNK;

pub const MMAP_CHUNK_BYTES: usize = 1 << LOG_MMAP_CHUNK_BYTES;

/** log_2 of the number of pages in a 64-bit space */
pub const LOG_PAGES_IN_SPACE64: usize = LOG_SPACE_SIZE_64 - LOG_BYTES_IN_PAGE as usize;

/** The number of pages in a 64-bit space */
pub const PAGES_IN_SPACE64: usize = 1 << LOG_PAGES_IN_SPACE64;

pub struct VMLayoutConstants {
    pub log_address_space: usize,
    pub heap_start: Address,
    /// log_2 of the maximum number of chunks we need to track.  Only used in 32-bit layout.
    pub max_chunks: usize,
    pub heap_end: Address,
    pub log_space_extent: usize,
    pub vm_space_size: usize,
    pub space_shift_64: usize,
    pub space_mask_64: usize,
    pub space_size_64: usize,
}

impl VMLayoutConstants {
    #[cfg(target_pointer_width = "32")]
    pub const LOG_ARCH_ADDRESS_SPACE: usize = 32;
    #[cfg(target_pointer_width = "64")]
    pub const LOG_ARCH_ADDRESS_SPACE: usize = 47;

    pub const fn max_space_extent(&self) -> usize {
        1 << self.log_space_extent
    }
    pub const fn available_start(&self) -> Address {
        if cfg!(feature = "vm_space") {
            unsafe { Address::from_usize(self.heap_start.as_usize() + self.vm_space_size) }
        } else {
            self.heap_start
        }
    }
    pub const fn available_end(&self) -> Address {
        self.heap_end
    }
    pub const fn available_bytes(&self) -> usize {
        self.available_end().get_extent(self.available_start())
    }
    /// Maximum number of chunks we need to track.  Only used in 32-bit layout.
    pub const fn max_chunks(&self) -> usize {
        self.max_chunks
    }
}

impl VMLayoutConstants {
    pub const fn new_32bit() -> Self {
        unimplemented!()
    }
    pub fn new_64bit() -> Self {
        Self {
            log_address_space: 47,
            heap_start: chunk_align_down(unsafe {
                Address::from_usize(0x0000_0200_0000_0000usize)
            }),
            heap_end: chunk_align_up(unsafe { Address::from_usize(0x0000_2000_0000_0000usize) }),
            vm_space_size: chunk_align_up(unsafe { Address::from_usize(0xdc0_0000) }).as_usize(),
            max_chunks: 1 << (Self::LOG_ARCH_ADDRESS_SPACE - LOG_BYTES_IN_CHUNK),
            log_space_extent: 41,
            space_shift_64: 41,
            space_mask_64: ((1 << 4) - 1) << 41,
            space_size_64: 1 << 41,
        }
    }
    pub fn new_64bit_with_pointer_compression(heap_size: usize) -> Self {
        assert!(
            heap_size <= (32usize << 30),
            "Heap size is larger than 32 GB"
        );
        let mut start: usize = 0x4000_0000;
        let mut end: usize = match start + heap_size {
            end if end <= (4usize << 30) => 4usize << 30,
            end if end <= (32usize << 30) => 32usize << 30,
            _ => 0x4000_0000 + (32usize << 30) - BYTES_IN_CHUNK,
        };
        // A workaround to avoid address conflict with the OpenJDK
        // MetaSpace, which may start from 0x8_0000_0000
        if end > 0x8_0000_0000 {
            start = 0x200_0000_0000;
            end = start + 0x8_0000_0000 - BYTES_IN_CHUNK;
            // Non-zero based compressed pointer space
            // Protect the first page as we will never access it.
            // The base address of compressed heap is set to one page before the heap start, so that NULL can be encoded nonambigous.
            crate::util::memory::mmap_noreserve(unsafe { Address::from_usize(start - 4096) }, 4096)
                .unwrap();
            crate::util::memory::mprotect(unsafe { Address::from_usize(start - 4096) }, 4096)
                .unwrap();
        }

        let heap_end = if cfg!(feature = "virt_constraint") {
            usize::min(
                end,
                (start + heap_size - 1 + BYTES_IN_CHUNK) & !(BYTES_IN_CHUNK - 1),
            )
        } else {
            end
        };

        Self {
            log_address_space: 35,
            heap_start: chunk_align_down(unsafe { Address::from_usize(start) }),
            heap_end: chunk_align_up(unsafe { Address::from_usize(heap_end) }),
            vm_space_size: chunk_align_up(unsafe { Address::from_usize(0x800_0000) }).as_usize(),
            max_chunks: (end - start) >> LOG_BYTES_IN_CHUNK,
            log_space_extent: 31,
            space_shift_64: 0,
            space_mask_64: 0,
            space_size_64: 0,
        }
    }

    pub fn set_address_space(kind: AddressSpaceKind) {
        let mut guard = ADDRESS_SPACE_KIND.lock();
        assert!(guard.is_none(), "Address space can only be set once");
        *guard = Some(kind);
    }

    pub fn get_address_space() -> AddressSpaceKind {
        ADDRESS_SPACE_KIND.lock().unwrap()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AddressSpaceKind {
    _32Bits,
    _64Bits,
    _64BitsWithPointerCompression { heap_size: usize },
}

impl AddressSpaceKind {
    pub const fn pointer_compression(&self) -> bool {
        match self {
            Self::_64BitsWithPointerCompression { .. } => true,
            _ => false,
        }
    }
}

static ADDRESS_SPACE_KIND: Mutex<Option<AddressSpaceKind>> = Mutex::new(None);

lazy_static! {
    pub static ref VM_LAYOUT_CONSTANTS: VMLayoutConstants = {
        let las = ADDRESS_SPACE_KIND
            .lock()
            .expect("Address space is not initialized");
        match las {
            AddressSpaceKind::_32Bits => unimplemented!(),
            AddressSpaceKind::_64Bits => VMLayoutConstants::new_64bit(),
            AddressSpaceKind::_64BitsWithPointerCompression { heap_size } => {
                VMLayoutConstants::new_64bit_with_pointer_compression(heap_size)
            }
        }
    };
}

pub fn init() {
    // VM_LAYOUT_CONSTANTS.set(VMLayoutConstants::new_64bit_with_pointer_compression());
}
