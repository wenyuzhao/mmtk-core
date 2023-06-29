pub(super) mod barrier;
pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

pub use self::global::Immix;
pub use self::global::IMMIX_CONSTRAINTS;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Pause {
    Full,
    FullDefrag,
    RefCount,
    InitialMark,
    FinalMark,
}

// pub static ACTIVE_BARRIER: BarrierSelector = BarrierSelector::FieldBarrier;
