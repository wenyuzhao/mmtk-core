pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;
use crate::util::metadata::{MetadataSpec};
use crate::util::metadata::side_metadata::{GLOBAL_SIDE_METADATA_VM_BASE_OFFSET, SideMetadataSpec};

pub use self::global::Immix;

pub const CONCURRENT_MARKING: bool = false;

pub const RC_ENABLED: bool = true;

pub const RC: MetadataSpec = MetadataSpec::OnSide(SideMetadataSpec {
    is_global: true,
    offset: GLOBAL_SIDE_METADATA_VM_BASE_OFFSET,
    log_num_of_bits: 2,
    log_min_obj_size: 3,
});

