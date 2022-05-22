use std::sync::atomic::AtomicUsize;

use crate::util::linear_scan::Region;
use crate::util::metadata::MetadataSpec;
use crate::util::{metadata::side_metadata::address_to_meta_address, Address};
use crate::{
    policy::immix::{block::Block, line::Line},
    util::{
        metadata::side_metadata::{self, SideMetadataSpec},
        ObjectReference,
    },
    vm::*,
};
use atomic::Ordering;

pub const LOG_REF_COUNT_BITS: usize = {
    if cfg!(feature = "lxr_rc_bits_2") {
        1
    } else if cfg!(feature = "lxr_rc_bits_4") {
        2
    } else if cfg!(feature = "lxr_rc_bits_8") {
        3
    } else {
        1
    }
};
pub const REF_COUNT_BITS: usize = 1 << LOG_REF_COUNT_BITS;
pub const REF_COUNT_MASK: usize = (1 << REF_COUNT_BITS) - 1;
pub const MAX_REF_COUNT: usize = (1 << REF_COUNT_BITS) - 1;

pub const LOG_MIN_OBJECT_SIZE: usize = 4;
pub const MIN_OBJECT_SIZE: usize = 1 << LOG_MIN_OBJECT_SIZE;

pub const RC_STRADDLE_LINES: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::RC_STRADDLE_LINES;

pub const RC_TABLE: SideMetadataSpec = crate::util::metadata::side_metadata::spec_defs::RC_TABLE;

pub const RC_LOCK_BITS: SideMetadataSpec =
    crate::util::metadata::side_metadata::spec_defs::RC_LOCK_BITS;
pub const RC_LOCK_BIT_SPEC: MetadataSpec = MetadataSpec::OnSide(RC_LOCK_BITS);

#[inline(always)]
pub fn fetch_update(
    o: ObjectReference,
    f: impl FnMut(usize) -> Option<usize>,
) -> Result<usize, usize> {
    side_metadata::fetch_update(
        &RC_TABLE,
        o.to_address(),
        Ordering::Relaxed,
        Ordering::Relaxed,
        f,
    )
}

#[inline(always)]
pub fn rc_stick(o: ObjectReference) -> bool {
    self::count(o) == MAX_REF_COUNT
}

#[inline(always)]
pub fn inc(o: ObjectReference) -> Result<usize, usize> {
    fetch_update(o, |x| {
        debug_assert!(x <= MAX_REF_COUNT);
        if x == MAX_REF_COUNT {
            None
        } else {
            Some(x + 1)
        }
    })
}

#[inline(always)]
pub fn dec(o: ObjectReference) -> Result<usize, usize> {
    fetch_update(o, |x| {
        debug_assert!(x <= MAX_REF_COUNT);
        if x == 0 || x == MAX_REF_COUNT
        /* sticky */
        {
            None
        } else {
            Some(x - 1)
        }
    })
}

#[inline(always)]
pub fn set(o: ObjectReference, count: usize) {
    side_metadata::store_atomic(&RC_TABLE, o.to_address(), count, Ordering::Relaxed)
}

#[inline(always)]
pub fn count(o: ObjectReference) -> usize {
    side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::Relaxed)
}

pub fn rc_table_range<UInt: Sized>(b: Block) -> &'static [UInt] {
    debug_assert!({
        let log_bits_in_uint: usize = (std::mem::size_of::<UInt>() << 3).trailing_zeros() as usize;
        Block::LOG_BYTES - super::rc::LOG_MIN_OBJECT_SIZE + super::rc::LOG_REF_COUNT_BITS
            >= log_bits_in_uint
    });
    let start = address_to_meta_address(&super::rc::RC_TABLE, b.start()).to_ptr::<UInt>();
    let limit = address_to_meta_address(&super::rc::RC_TABLE, b.end()).to_ptr::<UInt>();
    let rc_table = unsafe { std::slice::from_raw_parts(start, limit.offset_from(start) as _) };
    rc_table
}

#[allow(unused)]
#[inline(always)]
pub fn is_dead(o: ObjectReference) -> bool {
    let v = side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::Relaxed);
    v == 0
}

#[inline(always)]
pub fn is_dead_or_stick(o: ObjectReference) -> bool {
    let v = side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::Relaxed);
    v == 0 || v == MAX_REF_COUNT
}

#[inline(always)]
pub fn is_straddle_line(line: Line) -> bool {
    let v = side_metadata::load_atomic(&RC_STRADDLE_LINES, line.start(), Ordering::Relaxed);
    v != 0
}

#[inline(always)]
pub fn address_is_in_straddle_line(a: Address) -> bool {
    let line = Line::from(Line::align(a));
    self::count(unsafe { a.to_object_reference() }) != 0 && self::is_straddle_line(line)
}

#[inline(always)]
fn mark_straddle_object_with_size<VM: VMBinding>(o: ObjectReference, size: usize) {
    debug_assert!(!crate::args::BLOCK_ONLY);
    debug_assert!(size > Line::BYTES);
    let start_line = Line::containing::<VM>(o).next();
    let end_line = Line::from(Line::align(o.to_address() + size));
    let mut line = start_line;
    while line != end_line {
        side_metadata::store_atomic(&RC_STRADDLE_LINES, line.start(), 1, Ordering::Relaxed);
        self::set(unsafe { line.start().to_object_reference() }, 1);
        line = line.next();
    }
}

#[inline(always)]
pub fn mark_straddle_object<VM: VMBinding>(o: ObjectReference) {
    let size = VM::VMObjectModel::get_current_size(o);
    mark_straddle_object_with_size::<VM>(o, size)
}

#[inline(always)]
pub fn unmark_straddle_object<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(!crate::args::BLOCK_ONLY);
    // debug_assert!(crate::args::RC_NURSERY_EVACUATION);
    let size = VM::VMObjectModel::get_current_size(o);
    if size > Line::BYTES {
        let start_line = Line::containing::<VM>(o).next();
        let end_line = Line::from(Line::align(o.to_address() + size));
        let mut line = start_line;
        while line != end_line {
            self::set(unsafe { line.start().to_object_reference() }, 0);
            // std::sync::atomic::fence(Ordering::Relaxed);
            side_metadata::store_atomic(&RC_STRADDLE_LINES, line.start(), 0, Ordering::Relaxed);
            // std::sync::atomic::fence(Ordering::Relaxed);
            line = line.next();
        }
    }
}

#[inline(always)]
pub fn assert_zero_ref_count<VM: VMBinding>(o: ObjectReference) {
    let size = VM::VMObjectModel::get_current_size(o);
    for i in (0..size).step_by(MIN_OBJECT_SIZE) {
        let a = o.to_address() + i;
        assert_eq!(0, self::count(unsafe { a.to_object_reference() }));
    }
}

#[inline(always)]
pub fn promote<VM: VMBinding>(o: ObjectReference) {
    o.log_start_address::<VM>();
    let size = o.get_size::<VM>();
    if size > Line::BYTES {
        self::mark_straddle_object_with_size::<VM>(o, size);
    }
}

static INC_BUFFER_SIZE: AtomicUsize = AtomicUsize::new(0);

#[inline(always)]
pub fn inc_buffer_size() -> usize {
    INC_BUFFER_SIZE.load(Ordering::Relaxed)
}

#[inline(always)]
pub fn inc_inc_buffer_size() {
    INC_BUFFER_SIZE.store(
        INC_BUFFER_SIZE.load(Ordering::Relaxed) + 1,
        Ordering::Relaxed,
    );
}

#[inline(always)]
pub fn reset_inc_buffer_size() {
    crate::add_incs(inc_buffer_size());
    INC_BUFFER_SIZE.store(0, Ordering::Relaxed)
}
