use atomic_traits::Atomic;
use std::fmt;
use std::mem;
use std::ops::*;
use std::sync::atomic::Ordering;

use crate::mmtk::{MMAPPER, SFT_MAP};
use crate::plan::barriers::LOCKED_VALUE;
use crate::plan::barriers::LOGGED_VALUE;
use crate::plan::barriers::UNLOCKED_VALUE;
use crate::plan::barriers::UNLOGGED_VALUE;
use crate::plan::EdgeIterator;
use crate::util::rc::RC_LOCK_BITS;
use crate::vm::{ObjectModel, VMBinding};

use super::heap::layout::vm_layout_constants::VM_LAYOUT_CONSTANTS;

/// size in bytes
pub type ByteSize = usize;
/// offset in byte
pub type ByteOffset = isize;

/// Address represents an arbitrary address. This is designed to represent
/// address and do address arithmetic mostly in a safe way, and to allow
/// mark some operations as unsafe. This type needs to be zero overhead
/// (memory wise and time wise). The idea is from the paper
/// High-level Low-level Programming (VEE09) and JikesRVM.
#[repr(transparent)]
#[derive(Copy, Clone, Eq, Hash, PartialOrd, Ord, PartialEq)]
pub struct Address(usize);

/// Address + ByteSize (positive)
impl Add<ByteSize> for Address {
    type Output = Address;
    fn add(self, offset: ByteSize) -> Address {
        Address(self.0 + offset)
    }
}

/// Address += ByteSize (positive)
impl AddAssign<ByteSize> for Address {
    fn add_assign(&mut self, offset: ByteSize) {
        self.0 += offset;
    }
}

/// Address + ByteOffset (positive or negative)
impl Add<ByteOffset> for Address {
    type Output = Address;
    fn add(self, offset: ByteOffset) -> Address {
        Address((self.0 as isize + offset) as usize)
    }
}

/// Address += ByteOffset (positive or negative)
impl AddAssign<ByteOffset> for Address {
    fn add_assign(&mut self, offset: ByteOffset) {
        self.0 = (self.0 as isize + offset) as usize
    }
}

/// Address - ByteSize (positive)
impl Sub<ByteSize> for Address {
    type Output = Address;
    fn sub(self, offset: ByteSize) -> Address {
        Address(self.0 - offset)
    }
}

/// Address -= ByteSize (positive)
impl SubAssign<ByteSize> for Address {
    fn sub_assign(&mut self, offset: ByteSize) {
        self.0 -= offset;
    }
}

/// Address - Address (the first address must be higher)
impl Sub<Address> for Address {
    type Output = ByteSize;
    fn sub(self, other: Address) -> ByteSize {
        debug_assert!(
            self.0 >= other.0,
            "for (addr_a - addr_b), a({}) needs to be larger than b({})",
            self,
            other
        );
        self.0 - other.0
    }
}

/// Address & mask
impl BitAnd<usize> for Address {
    type Output = usize;
    fn bitand(self, other: usize) -> usize {
        self.0 & other
    }
}
// Be careful about the return type here. Address & u8 = u8
// This is different from Address | u8 = usize
impl BitAnd<u8> for Address {
    type Output = u8;
    fn bitand(self, other: u8) -> u8 {
        (self.0 as u8) & other
    }
}

/// Address | mask
impl BitOr<usize> for Address {
    type Output = usize;
    fn bitor(self, other: usize) -> usize {
        self.0 | other
    }
}
// Be careful about the return type here. Address | u8 = size
// This is different from Address & u8 = u8
impl BitOr<u8> for Address {
    type Output = usize;
    fn bitor(self, other: u8) -> usize {
        self.0 | (other as usize)
    }
}

/// Address >> shift (get an index)
impl Shr<usize> for Address {
    type Output = usize;
    fn shr(self, shift: usize) -> usize {
        self.0 >> shift
    }
}

/// Address << shift (get an index)
impl Shl<usize> for Address {
    type Output = usize;
    fn shl(self, shift: usize) -> usize {
        self.0 << shift
    }
}

/// Default constructor
impl Default for Address {
    fn default() -> Self {
        Self::ZERO
    }
}

impl Address {
    pub const ZERO: Self = Address(0);
    pub const MAX: Self = Address(usize::max_value());

    /// creates Address from a pointer
    pub fn from_ptr<T>(ptr: *const T) -> Address {
        Address(ptr as usize)
    }

    pub fn from_ref<T>(r: &T) -> Address {
        Address(r as *const T as usize)
    }

    /// creates Address from a mutable pointer
    pub fn from_mut_ptr<T>(ptr: *mut T) -> Address {
        Address(ptr as usize)
    }

    /// creates a null Address (0)
    /// # Safety
    /// It is unsafe and the user needs to be aware that they are creating an invalid address.
    /// The zero address should only be used as unininitialized or sentinel values in performance critical code (where you dont want to use Option<Address>).
    pub const unsafe fn zero() -> Address {
        Address(0)
    }

    /// creates an Address of (usize::MAX)
    /// # Safety
    /// It is unsafe and the user needs to be aware that they are creating an invalid address.
    /// The max address should only be used as unininitialized or sentinel values in performance critical code (where you dont want to use Option<Address>).
    pub unsafe fn max() -> Address {
        use std::usize;
        Address(usize::MAX)
    }

    /// creates an arbitrary Address
    /// # Safety
    /// It is unsafe and the user needs to be aware that they may create an invalid address.
    /// This creates arbitrary addresses which may not be valid. This should only be used for hard-coded addresses. Any other uses of this function could be
    /// replaced with more proper alternatives.
    pub const unsafe fn from_usize(raw: usize) -> Address {
        Address(raw)
    }

    /// shifts the address by N T-typed objects (returns addr + N * size_of(T))
    pub fn shift<T>(self, offset: isize) -> Self {
        self + mem::size_of::<T>() as isize * offset
    }

    // These const functions are duplicated with the operator traits. But we need them,
    // as we need them to declare constants.

    pub const fn get_extent(self, other: Address) -> ByteSize {
        self.0 - other.0
    }

    pub const fn get_offset(self, other: Address) -> ByteOffset {
        self.0 as isize - other.0 as isize
    }

    // We implemented the Add trait but we still keep this add function.
    // The add() function is const fn, and we can use it to declare Address constants.
    // The Add trait function cannot be const.
    #[allow(clippy::should_implement_trait)]
    pub const fn add(self, size: usize) -> Address {
        Address(self.0 + size)
    }

    // We implemented the Sub trait but we still keep this sub function.
    // The sub() function is const fn, and we can use it to declare Address constants.
    // The Sub trait function cannot be const.
    #[allow(clippy::should_implement_trait)]
    pub const fn sub(self, size: usize) -> Address {
        Address(self.0 - size)
    }

    pub const fn and(self, mask: usize) -> usize {
        self.0 & mask
    }

    // Perform a saturating subtract on the Address
    pub const fn saturating_sub(self, size: usize) -> Address {
        Address(self.0.saturating_sub(size))
    }

    /// loads a value of type T from the address
    /// # Safety
    /// This could throw a segment fault if the address is invalid
    pub unsafe fn load<T: Copy>(self) -> T {
        std::ptr::read_unaligned(self.0 as *const T)
    }

    /// stores a value of type T to the address
    /// # Safety
    /// This could throw a segment fault if the address is invalid
    pub unsafe fn store<T>(self, value: T) {
        // We use a ptr.write() operation as directly setting the pointer would drop the old value
        // which may result in unexpected behaviour
        std::ptr::write_unaligned(self.0 as *mut T, value);
    }

    /// atomic operation: load
    /// # Safety
    /// This could throw a segment fault if the address is invalid
    pub unsafe fn atomic_load<T: Atomic>(self, order: Ordering) -> T::Type {
        let loc = &*(self.0 as *const T);
        loc.load(order)
    }

    /// atomic operation: store
    /// # Safety
    /// This could throw a segment fault if the address is invalid
    pub unsafe fn atomic_store<T: Atomic>(self, val: T::Type, order: Ordering) {
        let loc = &*(self.0 as *const T);
        loc.store(val, order)
    }

    /// atomic operation: compare and exchange usize
    /// # Safety
    /// This could throw a segment fault if the address is invalid
    pub unsafe fn compare_exchange<T: Atomic>(
        self,
        old: T::Type,
        new: T::Type,
        success: Ordering,
        failure: Ordering,
    ) -> Result<T::Type, T::Type> {
        let loc = &*(self.0 as *const T);
        loc.compare_exchange(old, new, success, failure)
    }

    /// is this address zero?
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// aligns up the address to the given alignment
    pub const fn align_up(self, align: ByteSize) -> Address {
        use crate::util::conversions;
        Address(conversions::raw_align_up(self.0, align))
    }

    /// aligns down the address to the given alignment
    pub const fn align_down(self, align: ByteSize) -> Address {
        use crate::util::conversions;
        Address(conversions::raw_align_down(self.0, align))
    }

    /// is this address aligned to the given alignment
    pub fn is_aligned_to(self, align: usize) -> bool {
        use crate::util::conversions;
        conversions::raw_is_aligned(self.0, align)
    }

    /// converts the Address to a pointer
    pub fn to_ptr<T>(self) -> *const T {
        self.0 as *const T
    }

    /// converts the Address to a mutable pointer
    pub fn to_mut_ptr<T>(self) -> *mut T {
        self.0 as *mut T
    }

    /// converts the Address to a Rust reference
    ///
    /// # Safety
    /// The caller must guarantee the address actually points to a Rust object.
    pub unsafe fn as_ref<'a, T>(self) -> &'a T {
        &*self.to_mut_ptr()
    }

    /// converts the Address to a pointer-sized integer
    pub const fn as_usize(self) -> usize {
        self.0
    }

    /// returns the chunk index for this address
    pub fn chunk_index(self) -> usize {
        use crate::util::conversions;
        conversions::address_to_chunk_index(self)
    }

    /// return true if the referenced memory is mapped
    pub fn is_mapped(self) -> bool {
        if self.0 == 0 {
            false
        } else {
            MMAPPER.is_mapped_address(self)
        }
    }

    pub fn unlock<VM: VMBinding>(self) {
        debug_assert!(!self.is_zero());
        RC_LOCK_BITS.store_atomic(self, UNLOCKED_VALUE, Ordering::Relaxed)
    }

    pub fn lock(&self) {
        loop {
            // Attempt to lock the edges
            if RC_LOCK_BITS
                .compare_exchange_atomic(
                    *self,
                    UNLOCKED_VALUE,
                    LOCKED_VALUE,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return;
            }
            // Failed to lock the edge. Spin.
        }
    }

    pub fn is_locked<VM: VMBinding>(self) -> bool {
        debug_assert!(!self.is_zero());
        unsafe { RC_LOCK_BITS.load::<u8>(self) == LOCKED_VALUE }
    }

    pub fn is_field_logged<VM: VMBinding>(self) -> bool {
        debug_assert!(!self.is_zero());
        unsafe {
            VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
                .as_spec()
                .extract_side_spec()
                .load::<u8>(self)
                == LOGGED_VALUE
        }
    }

    pub fn attempt_log_field<VM: VMBinding>(self) -> bool {
        debug_assert!(!self.is_zero());
        let log_bit = *VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
            .as_spec()
            .extract_side_spec();
        loop {
            let old_value: u8 = log_bit.load_atomic(self, Ordering::SeqCst);
            if old_value == LOGGED_VALUE {
                return false;
            }
            if log_bit
                .compare_exchange_atomic(
                    self,
                    UNLOGGED_VALUE,
                    LOGGED_VALUE,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    pub fn log_field<VM: VMBinding>(self) {
        debug_assert!(!self.is_zero());
        VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
            .as_spec()
            .extract_side_spec()
            .store_atomic(self, LOGGED_VALUE, Ordering::Relaxed)
    }

    pub fn unlog_field<VM: VMBinding>(self) {
        debug_assert!(!self.is_zero());
        VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
            .as_spec()
            .extract_side_spec()
            .store_atomic(self, UNLOGGED_VALUE, Ordering::Relaxed)
    }

    pub fn unlog_field_relaxed<VM: VMBinding>(self) {
        debug_assert!(!self.is_zero());
        let heap_bytes_per_unlog_byte = if VM::VMObjectModel::COMPRESSED_PTR_ENABLED {
            32usize
        } else {
            64
        };
        let a = self.align_down(heap_bytes_per_unlog_byte);
        unsafe {
            VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC
                .as_spec()
                .extract_side_spec()
                .store(a, 0xffu8)
        }
    }

    pub fn to_object_reference<VM: VMBinding>(self) -> ObjectReference {
        debug_assert!(!self.is_zero());
        VM::VMObjectModel::address_to_ref(self)
    }
}

/// allows print Address as upper-case hex value
impl fmt::UpperHex for Address {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:X}", self.0)
    }
}

/// allows print Address as lower-case hex value
impl fmt::LowerHex for Address {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

/// allows Display format the Address (as upper-case hex value with 0x prefix)
impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

/// allows Debug format the Address (as upper-case hex value with 0x prefix)
impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::util::Address;

    #[test]
    fn align_up() {
        unsafe {
            assert_eq!(
                Address::from_usize(0x10).align_up(0x10),
                Address::from_usize(0x10)
            );
            assert_eq!(
                Address::from_usize(0x11).align_up(0x10),
                Address::from_usize(0x20)
            );
            assert_eq!(
                Address::from_usize(0x20).align_up(0x10),
                Address::from_usize(0x20)
            );
        }
    }

    #[test]
    fn align_down() {
        unsafe {
            assert_eq!(
                Address::from_usize(0x10).align_down(0x10),
                Address::from_usize(0x10)
            );
            assert_eq!(
                Address::from_usize(0x11).align_down(0x10),
                Address::from_usize(0x10)
            );
            assert_eq!(
                Address::from_usize(0x20).align_down(0x10),
                Address::from_usize(0x20)
            );
        }
    }

    #[test]
    fn is_aligned_to() {
        unsafe {
            assert!(Address::from_usize(0x10).is_aligned_to(0x10));
            assert!(!Address::from_usize(0x11).is_aligned_to(0x10));
            assert!(Address::from_usize(0x10).is_aligned_to(0x8));
            assert!(!Address::from_usize(0x10).is_aligned_to(0x20));
        }
    }

    #[test]
    fn bit_and() {
        unsafe {
            assert_eq!(
                Address::from_usize(0b1111_1111_1100usize) & 0b1010u8,
                0b1000u8
            );
            assert_eq!(
                Address::from_usize(0b1111_1111_1100usize) & 0b1000_0000_1010usize,
                0b1000_0000_1000usize
            );
        }
    }

    #[test]
    fn bit_or() {
        unsafe {
            assert_eq!(
                Address::from_usize(0b1111_1111_1100usize) | 0b1010u8,
                0b1111_1111_1110usize
            );
            assert_eq!(
                Address::from_usize(0b1111_1111_1100usize) | 0b1000_0000_1010usize,
                0b1111_1111_1110usize
            );
        }
    }
}

/// ObjectReference represents address for an object. Compared with Address,
/// operations allowed on ObjectReference are very limited. No address arithmetics
/// are allowed for ObjectReference. The idea is from the paper
/// High-level Low-level Programming (VEE09) and JikesRVM.
///
/// A runtime may define its "object references" differently. It may define an object reference as
/// the address of an object, a handle that points to an indirection table entry where a pointer to
/// the object is held, or anything else. Regardless, MMTk expects each object reference to have a
/// pointer to the object (an address) in each object reference, and that address should be used
/// for this `ObjectReference` type.
///
/// We currently do not allow an opaque `ObjectReference` type for which a binding can define
/// their layout. We now only allow a binding to define their semantics through a set of
/// methods in [`crate::vm::ObjectModel`]. Major refactoring is needed in MMTk to allow
/// the opaque `ObjectReference` type, and we haven't seen a use case for now.
#[repr(transparent)]
#[derive(Copy, Clone, Eq, Hash, PartialOrd, PartialEq)]
pub struct ObjectReference(usize);

impl ObjectReference {
    pub const NULL: Self = Self(0);
    pub const STRICT_VERIFICATION: bool =
        cfg!(debug_assertions) || cfg!(feature = "sanity") || false;

    /// Cast the object reference to its raw address. This method is mostly for the convinience of a binding.
    ///
    /// MMTk should not make any assumption on the actual location of the address with the object reference.
    /// MMTk should not assume the address returned by this method is in our allocation. For the purposes of
    /// setting object metadata, MMTk should use [`crate::vm::ObjectModel::ref_to_address()`] or [`crate::vm::ObjectModel::ref_to_header()`].
    pub fn to_raw_address(self) -> Address {
        Address(self.0)
    }

    /// Cast a raw address to an object reference. This method is mostly for the convinience of a binding.
    /// This is how a binding creates `ObjectReference` instances.
    ///
    /// MMTk should not assume an arbitrary address can be turned into an object reference. MMTk can use [`crate::vm::ObjectModel::address_to_ref()`]
    /// to turn addresses that are from [`crate::vm::ObjectModel::ref_to_address()`] back to object.
    pub fn from_raw_address(addr: Address) -> ObjectReference {
        ObjectReference(addr.0)
    }

    /// Get the in-heap address from an object reference. This method is used by MMTk to get an in-heap address
    /// for an object reference. This method is syntactic sugar for [`crate::vm::ObjectModel::ref_to_address`]. See the
    /// comments on [`crate::vm::ObjectModel::ref_to_address`].
    pub fn to_address<VM: VMBinding>(self) -> Address {
        let to_address = VM::VMObjectModel::ref_to_address(self);
        debug_assert!(!VM::VMObjectModel::UNIFIED_OBJECT_REFERENCE_ADDRESS || to_address == self.to_raw_address(), "The binding claims unified object reference address, but for object reference {}, ref_to_address() returns {}", self, to_address);
        to_address
    }

    /// Get the header base address from an object reference. This method is used by MMTk to get a base address for the
    /// object header, and access the object header. This method is syntactic sugar for [`crate::vm::ObjectModel::ref_to_header`].
    /// See the comments on [`crate::vm::ObjectModel::ref_to_header`].
    pub fn to_header<VM: VMBinding>(self) -> Address {
        VM::VMObjectModel::ref_to_header(self)
    }

    pub fn to_object_start<VM: VMBinding>(self) -> Address {
        let object_start = VM::VMObjectModel::ref_to_object_start(self);
        debug_assert!(!VM::VMObjectModel::UNIFIED_OBJECT_REFERENCE_ADDRESS || object_start == self.to_raw_address(), "The binding claims unified object reference address, but for object reference {}, ref_to_address() returns {}", self, object_start);
        object_start
    }

    /// Get the object reference from an address that is returned from [`crate::util::address::ObjectReference::to_address`]
    /// or [`crate::vm::ObjectModel::ref_to_address`]. This method is syntactic sugar for [`crate::vm::ObjectModel::address_to_ref`].
    /// See the comments on [`crate::vm::ObjectModel::address_to_ref`].
    pub fn from_address<VM: VMBinding>(addr: Address) -> ObjectReference {
        let obj = VM::VMObjectModel::address_to_ref(addr);
        debug_assert!(!VM::VMObjectModel::UNIFIED_OBJECT_REFERENCE_ADDRESS || addr == obj.to_raw_address(), "The binding claims unified object reference address, but for address {}, address_to_ref() returns {}", addr, obj);
        obj
    }

    /// is this object reference null reference?
    pub fn is_null(self) -> bool {
        self.0 == 0
    }

    /// returns the ObjectReference
    pub fn value(self) -> usize {
        self.0
    }

    /// Is the object reachable, determined by the policy?
    /// Note: Objects in ImmortalSpace may have `is_live = true` but are actually unreachable.
    pub fn is_reachable(self) -> bool {
        if self.is_null() {
            false
        } else {
            unsafe { SFT_MAP.get_unchecked(Address(self.0)) }.is_reachable(self)
        }
    }

    /// Is the object live, determined by the policy?
    pub fn is_live(self) -> bool {
        if self.is_null() {
            false
        } else {
            unsafe { SFT_MAP.get_unchecked(Address(self.0)) }.is_live(self)
        }
    }

    pub fn is_movable(self) -> bool {
        if self.is_null() {
            return false;
        }
        unsafe { SFT_MAP.get_unchecked(Address(self.0)) }.is_movable()
    }

    /// Get forwarding pointer if the object is forwarded.
    pub fn get_forwarded_object(self) -> Option<Self> {
        if self.is_null() {
            return None;
        }
        debug_assert!({
            let addr = self.to_raw_address();
            addr >= VM_LAYOUT_CONSTANTS.heap_start && addr < VM_LAYOUT_CONSTANTS.heap_end
        });
        unsafe { SFT_MAP.get_unchecked(Address(self.0)) }.get_forwarded_object(self)
    }

    pub fn is_in_any_space(self) -> bool {
        let addr = self.to_raw_address();
        if addr < VM_LAYOUT_CONSTANTS.heap_start || addr >= VM_LAYOUT_CONSTANTS.heap_end {
            return false;
        }
        unsafe { SFT_MAP.get_unchecked(Address(self.0)) }.is_in_space(self)
    }

    #[cfg(feature = "sanity")]
    pub fn is_sane(self) -> bool {
        let addr = self.to_raw_address();
        if addr < VM_LAYOUT_CONSTANTS.heap_start || addr >= VM_LAYOUT_CONSTANTS.heap_end {
            return false;
        }
        unsafe { SFT_MAP.get_unchecked(Address(self.0)) }.is_sane()
    }

    pub fn get_size<VM: VMBinding>(self) -> usize {
        debug_assert!(!self.is_null());
        VM::VMObjectModel::get_current_size(self)
    }

    pub fn range<VM: VMBinding>(self) -> Range<Address> {
        if self.is_null() {
            return self.to_address::<VM>()..self.to_address::<VM>();
        }
        let a = VM::VMObjectModel::ref_to_object_start(self);
        a..a + self.get_size::<VM>()
    }

    pub fn log_start_address<VM: VMBinding>(self) {}

    pub fn class_pointer<VM: VMBinding>(self) -> Address {
        VM::VMObjectModel::get_class_pointer(self)
    }

    pub fn class_is_valid<VM: VMBinding>(self) -> bool {
        let klass = self.class_pointer::<VM>();
        let v = klass.as_usize();
        // if -1 == unsafe { libc::msync((v >> 12 << 12) as *mut libc::c_void, 4096, 0) } {
        //     println!("Unmapped klass {:?} object {:?}", klass, self);
        //     return false;
        // }
        let valid = if VM::VMObjectModel::COMPRESSED_PTR_ENABLED {
            klass.is_aligned_to(8) && v >= 0x10000_0000 && v <= 0x20000_0000
        } else {
            ((klass.as_usize() & 0xff000_00000000) == 0x7000_00000000) && klass.is_aligned_to(8)
        };
        if !valid {
            println!("invalid klass {:?}", klass);
        }
        valid
    }

    fn assert_class_is_valid<VM: VMBinding>(self) {
        // assert!(
        //     self.class_is_valid::<VM>(),
        //     "Invalid class pointer obj={:?} cls={:?}",
        //     self,
        //     self.class_pointer::<VM>()
        // );
    }

    pub fn fix_start_address<VM: VMBinding>(self) -> Self {
        self
    }

    pub fn verify<VM: VMBinding>(self) {
        if cfg!(debug_assertions) || Self::STRICT_VERIFICATION {
            if self.is_null() {
                return;
            }
            assert!(
                self.to_address::<VM>().is_mapped(),
                "unmapped object {:?}",
                self
            );
            assert!(self.is_in_any_space());
            assert_ne!(
                unsafe { self.to_address::<VM>().load::<usize>() },
                0xdead,
                "object {:?} is dead",
                self
            );
            self.assert_class_is_valid::<VM>();
        }
    }

    pub fn iterate_fields<VM: VMBinding, F: FnMut(VM::VMEdge)>(
        self,
        cld_scan: CLDScanPolicy,
        ref_scan: RefScanPolicy,
        f: F,
    ) {
        EdgeIterator::<VM>::iterate(
            self,
            ref_scan == RefScanPolicy::Discover,
            cld_scan == CLDScanPolicy::Claim,
            cld_scan != CLDScanPolicy::Ignore,
            f,
            None,
        )
    }

    pub fn iterate_fields_with_klass<VM: VMBinding, F: FnMut(VM::VMEdge)>(
        self,
        cld_scan: CLDScanPolicy,
        ref_scan: RefScanPolicy,
        klass: Address,
        f: F,
    ) {
        EdgeIterator::<VM>::iterate(
            self,
            ref_scan == RefScanPolicy::Discover,
            cld_scan == CLDScanPolicy::Claim,
            cld_scan != CLDScanPolicy::Ignore,
            f,
            Some(klass),
        )
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CLDScanPolicy {
    /// Don't scan CLDs
    Ignore,
    /// Scan CLDs
    Follow,
    /// Scan and mark CLDs. CLDs that are previously marked will be ignored.
    Claim,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RefScanPolicy {
    /// Treat weak or soft edges as strong
    Follow,
    /// Perform weak/soft/phantom/final reference discovery
    Discover,
}

/// allows print Address as upper-case hex value
impl fmt::UpperHex for ObjectReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:X}", self.0)
    }
}

/// allows print Address as lower-case hex value
impl fmt::LowerHex for ObjectReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

/// allows Display format the Address (as upper-case hex value with 0x prefix)
impl fmt::Display for ObjectReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

/// allows Debug format the Address (as upper-case hex value with 0x prefix)
impl fmt::Debug for ObjectReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}
