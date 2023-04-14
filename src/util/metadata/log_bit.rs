use crate::util::ObjectReference;
use crate::vm::VMBinding;
use crate::vm::VMGlobalLogBitSpec;
use std::sync::atomic::Ordering;

use super::MetadataSpec;

impl VMGlobalLogBitSpec {
    /// Mark the log bit as unlogged (1 means unlogged)
    pub fn mark_as_unlogged<VM: VMBinding>(&self, object: ObjectReference, order: Ordering) {
        self.store_atomic::<VM, u8>(object, 1, None, order)
    }

    pub fn is_unlogged<VM: VMBinding>(&self, object: ObjectReference, order: Ordering) -> bool {
        self.load_atomic::<VM, u8>(object, None, order) == 1
    }
}

impl MetadataSpec {
    /// Mark the log bit as unlogged (1 means unlogged)
    pub fn mark_as_unlogged<VM: VMBinding>(&self, object: ObjectReference, order: Ordering) {
        self.store_atomic::<VM, u8>(object, 1, None, order)
    }
}
