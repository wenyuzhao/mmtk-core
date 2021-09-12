use atomic::Ordering;

use crate::{util::{ObjectReference, metadata::side_metadata::{self, SideMetadataOffset, SideMetadataSpec}}};

use super::chunk::ChunkMap;

pub const RC_TABLE: SideMetadataSpec = SideMetadataSpec {
    is_global: false,
    offset: SideMetadataOffset::layout_after(&ChunkMap::ALLOC_TABLE),
    log_num_of_bits: 2,
    log_min_obj_size: 3,
};

#[inline(always)]
pub fn inc(o: ObjectReference) -> Result<usize, usize> {
    debug_assert!(!o.is_null());
    side_metadata::fetch_update(&RC_TABLE, o.to_address(), Ordering::SeqCst, Ordering::SeqCst, |x| {
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
    side_metadata::fetch_update(&RC_TABLE, o.to_address(), Ordering::SeqCst, Ordering::SeqCst, |x| {
        if x == 0 || x == 0b1111 /* sticky */ {
            None
        } else {
            Some(x - 1)
        }
    })
}

pub fn count(o: ObjectReference) -> usize {
    side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::SeqCst)
}

pub fn is_dead(o: ObjectReference) -> bool {
    let v = side_metadata::load_atomic(&RC_TABLE, o.to_address(), Ordering::SeqCst);
    v == 0
}

#[inline(always)]
pub fn reset(o: ObjectReference) {
    debug_assert!(!o.is_null());
    side_metadata::store_atomic(&RC_TABLE, o.to_address(), 0, Ordering::SeqCst)
}