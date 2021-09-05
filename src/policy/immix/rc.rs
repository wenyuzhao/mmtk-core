use atomic::Ordering;

use crate::{plan::immix::RC, util::{ObjectReference, metadata::side_metadata}};

#[inline(always)]
pub fn inc(o: ObjectReference) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    side_metadata::fetch_update(RC.extract_side_spec(), o.to_address(), Ordering::SeqCst, Ordering::SeqCst, |x| {
        if x == 0b1111 {
            None
        } else {
            Some(x + 1)
        }
    })
}

#[inline(always)]
pub fn dec(o: ObjectReference) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    side_metadata::fetch_update(RC.extract_side_spec(), o.to_address(), Ordering::SeqCst, Ordering::SeqCst, |x| {
        if x == 0 || x == 0b1111 /* sticky */ {
            None
        } else {
            Some(x - 1)
        }
    })
}


pub fn is_dead(o: ObjectReference) -> bool {
    let v = side_metadata::load_atomic(RC.extract_side_spec(), o.to_address(), Ordering::SeqCst);
    v == 0
}

#[inline(always)]
pub fn reset(o: ObjectReference) {
    debug_assert!(!o.is_null());
    side_metadata::store_atomic(RC.extract_side_spec(), o.to_address(), 0, Ordering::SeqCst)
}