use spin::Lazy;
use std::{env, sync::atomic::AtomicUsize};

use crate::{policy::immix::block::Block, BarrierSelector};

// ---------- Immix flags ---------- //
pub const CONCURRENT_MARKING: bool = cfg!(feature = "ix_concurrent_marking");
pub const REF_COUNT: bool = cfg!(feature = "ix_ref_count");
pub const CYCLE_TRIGGER_THRESHOLD: usize = 1024;
/// Mark/sweep memory for block-level only
pub const BLOCK_ONLY: bool = cfg!(feature = "ix_block_only");
/// Opportunistic copying
pub const DEFRAG: bool = cfg!(feature = "ix_defrag");
/// Mark lines when scanning objects. Otherwise, do it at mark time.
pub const MARK_LINE_AT_SCAN_TIME: bool = true;

// ---------- CM/RC Immix flags ---------- //
pub const EAGER_INCREMENTS: bool = false;
pub const LAZY_DECREMENTS: bool = cfg!(feature = "lxr_lazy_decrements");
pub const LOCK_FREE_BLOCK_ALLOCATION: bool = cfg!(feature = "ix_lock_free_block_allocation");
pub const NO_LAZY_DEC_THRESHOLD: usize = 100;
pub const RC_NURSERY_EVACUATION: bool = cfg!(feature = "lxr_nursery_evacuation");
pub const RC_MATURE_EVACUATION: bool = cfg!(feature = "lxr_mature_evacuation");
pub const RC_MATURE_EVACUATION2: bool = true;
/// One more atomic-store per barrier slow-path if this value is smaller than 6.
pub const LOG_BYTES_PER_RC_LOCK_BIT: usize = (super::constants::LOG_BYTES_IN_PAGE - 6) as _;
pub const RC_DONT_EVACUATE_NURSERY_IN_RECYCLED_LINES: bool =
    !cfg!(feature = "lxr_evacuate_nursery_in_recycled_lines");
pub static DISABLE_MUTATOR_LINE_REUSING: Lazy<bool> =
    Lazy::new(|| env::var("DISABLE_MUTATOR_LINE_REUSING").is_ok());
pub static LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE: Lazy<usize> = Lazy::new(|| {
    env::var("LOCK_FREE_BLOCKS")
        .map(|x| x.parse().unwrap())
        .unwrap_or(32)
});
pub static NURSERY_BLOCKS: Lazy<Option<usize>> = Lazy::new(|| {
    Some(
        env::var("NURSERY_BLOCKS")
            .map(|x| x.parse().unwrap())
            .ok()
            .unwrap_or((1 << (22 - Block::LOG_BYTES)) * num_cpus::get()),
    )
});
pub static MIN_NURSERY_BLOCKS: Lazy<usize> = Lazy::new(|| {
    env::var("MIN_NURSERY_BLOCKS")
        .map(|x| x.parse().unwrap())
        .unwrap_or(*LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE)
});
pub static MAX_NURSERY_BLOCKS: Lazy<Option<usize>> = Lazy::new(|| {
    env::var("MAX_NURSERY_BLOCKS")
        .map(|x| x.parse().unwrap())
        .ok()
});
pub static INITIAL_NURSERY_BLOCKS: Lazy<usize> =
    Lazy::new(|| NURSERY_BLOCKS.unwrap_or((1 << (22 - Block::LOG_BYTES)) * num_cpus::get()));
pub static ADAPTIVE_NURSERY_BLOCKS: Lazy<AtomicUsize> =
    Lazy::new(|| AtomicUsize::new(*INITIAL_NURSERY_BLOCKS));
pub static LOWER_CONCURRENT_GC_THREAD_PRIORITY: Lazy<bool> = Lazy::new(|| {
    env::var("LOWER_CONCURRENT_GC_THREAD_PRIORITY").unwrap_or_else(|_| "1".to_string()) != "0"
});
pub static CONCURRENT_GC_THREADS_RATIO: Lazy<usize> = Lazy::new(|| {
    env::var("CONCURRENT_GC_THREADS_RATIO")
        .map(|x| x.parse().unwrap())
        .unwrap_or(50)
});
pub static CONCURRENT_MARKING_THRESHOLD: Lazy<usize> = Lazy::new(|| {
    env::var("CONCURRENT_MARKING_THRESHOLD")
        .map(|x| x.parse().unwrap())
        .unwrap_or(90)
});

// ---------- Barrier flags ---------- //
pub const BARRIER_MEASUREMENT: bool = cfg!(feature = "barrier_measurement");
pub const TAKERATE_MEASUREMENT: bool = false;
pub const INSTRUMENTATION: bool = cfg!(feature = "instrumentation");

// ---------- Debugging flags ---------- //
pub const HARNESS_PRETTY_PRINT: bool = false || cfg!(feature = "log_gc");
pub const LOG_PER_GC_STATE: bool = cfg!(feature = "log_gc");
pub const LOG_STAGES: bool = cfg!(feature = "log_stages");
pub const LOG_WORK_PACKETS: bool = cfg!(feature = "log_work_packets");
pub const NO_RC_PAUSES_DURING_CONCURRENT_MARKING: bool = false;
pub const SLOW_CONCURRENT_MARKING: bool = false;

// ---------- Derived flags ---------- //
pub static IGNORE_REUSING_BLOCKS: Lazy<bool> =
    Lazy::new(|| REF_COUNT && LAZY_DECREMENTS && !*DISABLE_MUTATOR_LINE_REUSING);

macro_rules! dump_feature {
    ($name: literal, $value: expr) => {
        println!(" * {}: {:?}", $name, $value)
    };
    ($name: literal) => {
        dump_feature!($name, cfg!(feature = $name))
    };
}

fn dump_features(active_barrier: BarrierSelector) {
    println!("-------------------- Immix Args --------------------");

    dump_feature!("barrier", format!("{:?}", active_barrier));

    dump_feature!("barrier_measurement");
    dump_feature!("instrumentation");
    dump_feature!("ix_block_only");
    dump_feature!("ix_defrag");
    dump_feature!("ix_lock_free_block_allocation");
    dump_feature!("ix_concurrent_marking");
    dump_feature!("ix_ref_count");
    dump_feature!("lxr_lazy_decrements");
    dump_feature!("lxr_nursery_evacuation");
    dump_feature!("lxr_mature_evacuation");
    dump_feature!("lxr_evacuate_nursery_in_recycled_lines");
    dump_feature!("lxr_delayed_nursery_evacuation");

    dump_feature!(
        "disable_mutator_line_reusing",
        *DISABLE_MUTATOR_LINE_REUSING
    );
    dump_feature!("lock_free_blocks", *LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE);
    dump_feature!("nursery_blocks", *NURSERY_BLOCKS);
    dump_feature!(
        "low_concurrent_worker_priority",
        *LOWER_CONCURRENT_GC_THREAD_PRIORITY
    );
    dump_feature!("concurrent_worker_ratio", *CONCURRENT_GC_THREADS_RATIO);
    dump_feature!(
        "concurrent_marking_threshold",
        *CONCURRENT_MARKING_THRESHOLD
    );
    dump_feature!("ignore_reusing_blocks", *IGNORE_REUSING_BLOCKS);

    println!("----------------------------------------------------");
}

pub fn validate_features(active_barrier: BarrierSelector) {
    dump_features(active_barrier);
    validate!(DEFRAG => !BLOCK_ONLY);
    validate!(DEFRAG => !CONCURRENT_MARKING);
    validate!(DEFRAG => !REF_COUNT);
    validate!(CONCURRENT_MARKING => !DEFRAG);
    validate!(REF_COUNT => !DEFRAG);
    validate!(EAGER_INCREMENTS => !RC_NURSERY_EVACUATION);
    validate!(RC_NURSERY_EVACUATION => !EAGER_INCREMENTS);
    if BARRIER_MEASUREMENT {
        assert!(!EAGER_INCREMENTS);
        assert!(!LAZY_DECREMENTS);
        assert!(!REF_COUNT);
        assert!(!CONCURRENT_MARKING);
    }
}
