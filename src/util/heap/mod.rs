mod accounting;
#[macro_use]
pub mod layout;
mod blockpageresource;
pub mod blockpageresource_legacy;
pub mod blockpageresource_nosweep;
pub mod chunk_map;
pub mod freelistpageresource;
pub mod gc_trigger;
mod heap_meta;
pub(crate) mod monotonepageresource;
pub(crate) mod pageresource;
pub(crate) mod space_descriptor;
mod vmrequest;

pub(crate) use self::accounting::PageAccounting;
#[cfg(not(feature = "ix_no_sweeping"))]
pub(crate) use self::blockpageresource::BlockPageResource;
#[cfg(feature = "ix_no_sweeping")]
pub(crate) use self::blockpageresource_nosweep::BlockPageResource;
pub(crate) use self::freelistpageresource::FreeListPageResource;
pub(crate) use self::heap_meta::HeapMeta;
pub use self::layout::vm_layout;
pub(crate) use self::monotonepageresource::MonotonePageResource;
pub(crate) use self::pageresource::PageResource;
pub(crate) use self::vmrequest::VMRequest;
