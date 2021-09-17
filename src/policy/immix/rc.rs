use atomic::Ordering;

use crate::util::{
    constants::LOG_MIN_OBJECT_SIZE,
    metadata::side_metadata::{self, SideMetadataOffset, SideMetadataSpec},
    ObjectReference,
};

use super::chunk::ChunkMap;

const LOG_REF_COUNT_BITS: usize = 2;
const MAX_REF_COUNT: usize = (1 << (1 << LOG_REF_COUNT_BITS)) - 1;

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
    // println!("inc {:?} {:?}", o, count(o));
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
    // println!("dec {:?} {:?}", o, count(o));
    r
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
