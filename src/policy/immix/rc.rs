use atomic::Ordering;

use crate::{policy::immix::line::Line, util::{
        metadata::side_metadata::{self, SideMetadataOffset, SideMetadataSpec},
        ObjectReference,
    }, vm::*};

use super::chunk::ChunkMap;

const LOG_REF_COUNT_BITS: usize = 2;
const MAX_REF_COUNT: usize = (1 << (1 << LOG_REF_COUNT_BITS)) - 1;

pub const LOG_MIN_OBJECT_SIZE: usize = 3;
pub const MIN_OBJECT_SIZE: usize = 1 << LOG_MIN_OBJECT_SIZE;

pub const RC_TABLE: SideMetadataSpec = SideMetadataSpec {
    is_global: false,
    offset: SideMetadataOffset::layout_after(&ChunkMap::ALLOC_TABLE),
    log_num_of_bits: LOG_REF_COUNT_BITS,
    log_min_obj_size: LOG_MIN_OBJECT_SIZE as _,
};

#[inline(always)]
pub fn inc(o: ObjectReference) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    let r = side_metadata::fetch_update(
        &RC_TABLE,
        o.to_address(),
        Ordering::SeqCst,
        Ordering::SeqCst,
        |x| {
            if x == MAX_REF_COUNT {
                None
            } else {
                Some(x + 1)
            }
        },
    );
    // println!("inc {:?} {:?} -> {:?}", o, r, count(o));
    r
}

#[inline(always)]
pub fn dec(o: ObjectReference) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    let r = side_metadata::fetch_update(
        &RC_TABLE,
        o.to_address(),
        Ordering::SeqCst,
        Ordering::SeqCst,
        |x| {
            if x == 0 || x == MAX_REF_COUNT
            /* sticky */
            {
                None
            } else {
                Some(x - 1)
            }
        },
    );
    // println!("dec {:?} {:?} -> {:?}", o, r, count(o));
    r
}

#[inline(always)]
pub fn set(o: ObjectReference, count: usize) {
    debug_assert!(!o.is_null());
    side_metadata::store_atomic(&RC_TABLE, o.to_address(), count, Ordering::SeqCst)
}

pub fn count(o: ObjectReference) -> usize {
    side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::SeqCst)
}

#[allow(unused)]
#[inline(always)]
pub fn is_dead(o: ObjectReference) -> bool {
    let v = side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::SeqCst);
    v == 0
}

#[inline(always)]
pub fn mark_striddle_object<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(!crate::flags::BLOCK_ONLY);
    debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
    let size = VM::VMObjectModel::get_current_size(o);
    debug_assert!(size > Line::BYTES);
    // FIXME: Only store one marker per line is enough.
    for i in (0..size).step_by(MIN_OBJECT_SIZE).skip(1) {
        let a = o.to_address() + i;
        crate::policy::immix::rc::set(unsafe { a.to_object_reference() }, 1);
    }
}

#[inline(always)]
pub fn unmark_striddle_object<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(!crate::flags::BLOCK_ONLY);
    debug_assert!(crate::flags::RC_EVACUATE_NURSERY);
    let size = VM::VMObjectModel::get_current_size(o);
    if size > Line::BYTES {
        for i in (0..size).step_by(MIN_OBJECT_SIZE).skip(1) {
            let a = o.to_address() + i;
            crate::policy::immix::rc::set(unsafe { a.to_object_reference() }, 0);
        }
    }
}

#[inline(always)]
pub fn assert_zero_ref_count<VM: VMBinding>(o: ObjectReference) {
    debug_assert!(crate::flags::REF_COUNT);
    let size = VM::VMObjectModel::get_current_size(o);
    for i in (0..size).step_by(MIN_OBJECT_SIZE) {
        let a = o.to_address() + i;
        assert_eq!(
            0,
            crate::policy::immix::rc::count(unsafe { a.to_object_reference() })
        );
    }
}

