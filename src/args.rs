use spin::Lazy;
use std::env;

use crate::{
    policy::immix::{block::Block, line::Line},
    util::{linear_scan::Region, options::Options},
    BarrierSelector,
};

pub const CM_LARGE_ARRAY_OPTIMIZATION: bool = false;
pub const BUFFER_SIZE: usize = {
    if cfg!(feature = "lxr_buf_2048") {
        2048
    } else if cfg!(feature = "lxr_buf_1024") {
        1024
    } else if cfg!(feature = "lxr_buf_512") {
        512
    } else if cfg!(feature = "lxr_buf_256") {
        256
    } else {
        1024
    }
};

pub const HEAP_HEALTH_GUIDED_GC: bool = true;
pub static NO_GC_UNTIL_LAZY_SWEEPING_FINISHED: Lazy<bool> = Lazy::new(|| {
    env::var("NO_GC_UNTIL_LAZY_SWEEPING_FINISHED").unwrap_or_else(|_| "0".to_string()) != "0"
});
pub const HOLE_COUNTING: bool = cfg!(feature = "lxr_hole_counting");
pub const NO_LAZY_SWEEP_WHEN_STW_CANNOT_RELEASE_ENOUGH_MEMORY: bool = false;

pub static INC_BUFFER_LIMIT: Lazy<Option<usize>> =
    Lazy::new(|| env::var("INCS_LIMIT").map(|x| x.parse().unwrap()).ok());

// ---------- Immix flags ---------- //
pub const CYCLE_TRIGGER_THRESHOLD: usize = 1024;
/// Mark/sweep memory for block-level only
pub const BLOCK_ONLY: bool = cfg!(feature = "ix_block_only");
/// Opportunistic copying
pub const DEFRAG: bool = !cfg!(feature = "ix_no_defrag");
/// Mark lines when scanning objects. Otherwise, do it at mark time.
pub const MARK_LINE_AT_SCAN_TIME: bool = true;

// ---------- CM/RC Immix flags ---------- //
pub const EAGER_INCREMENTS: bool = false;
pub const LAZY_DECREMENTS: bool = !cfg!(feature = "lxr_no_evac");
pub const NO_LAZY_DEC_THRESHOLD: usize = 100;
pub const RC_NURSERY_EVACUATION: bool = !cfg!(feature = "lxr_no_nursery_evac");
pub const RC_MATURE_EVACUATION: bool = !cfg!(feature = "lxr_no_mature_evac");
pub const ENABLE_INITIAL_ALLOC_LIMIT: bool = cfg!(feature = "lxr_enable_initial_alloc_limit");

/// One more atomic-store per barrier slow-path if this value is smaller than 6.
pub const LOG_BYTES_PER_RC_LOCK_BIT: usize = {
    if cfg!(feature = "lxr_lock_3") {
        3
    } else if cfg!(feature = "lxr_lock_4") {
        4
    } else if cfg!(feature = "lxr_lock_5") {
        5
    } else if cfg!(feature = "lxr_lock_6") {
        6
    } else if cfg!(feature = "lxr_lock_7") {
        7
    } else if cfg!(feature = "lxr_lock_8") {
        8
    } else if cfg!(feature = "lxr_lock_9") {
        9
    } else {
        9
    }
};
pub const RC_DONT_EVACUATE_NURSERY_IN_RECYCLED_LINES: bool =
    !cfg!(feature = "lxr_evacuate_nursery_in_recycled_lines");
pub static DISABLE_MUTATOR_LINE_REUSING: Lazy<bool> =
    Lazy::new(|| env::var("DISABLE_MUTATOR_LINE_REUSING").is_ok());
pub static LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE: Lazy<usize> = Lazy::new(|| {
    env::var("LOCK_FREE_BLOCKS")
        .map(|x| x.parse().unwrap())
        .ok()
        .unwrap_or_else(|| 1 * num_cpus::get())
});
pub static NURSERY_BLOCKS: Lazy<Option<usize>> =
    Lazy::new(|| env::var("NURSERY_BLOCKS").map(|x| x.parse().unwrap()).ok());
pub static NURSERY_RATIO: Lazy<Option<usize>> =
    Lazy::new(|| env::var("NURSERY_RATIO").map(|x| x.parse().unwrap()).ok());
// pub static MIN_NURSERY_BLOCKS: Lazy<usize> = Lazy::new(|| {
//     env::var("MIN_NURSERY_BLOCKS")
//         .map(|x| x.parse().unwrap())
//         .unwrap_or(*LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE)
// });
// pub static MAX_NURSERY_BLOCKS: Lazy<Option<usize>> = Lazy::new(|| {
//     env::var("MAX_NURSERY_BLOCKS")
//         .map(|x| x.parse().unwrap())
//         .ok()
// });
// pub static INITIAL_NURSERY_BLOCKS: Lazy<usize> =
//     Lazy::new(|| NURSERY_BLOCKS.unwrap_or((1 << (22 - Block::LOG_BYTES)) * num_cpus::get()));
// pub static ADAPTIVE_NURSERY_BLOCKS: Lazy<AtomicUsize> =
//     Lazy::new(|| AtomicUsize::new(*INITIAL_NURSERY_BLOCKS));
pub static LOWER_CONCURRENT_GC_THREAD_PRIORITY: Lazy<bool> = Lazy::new(|| {
    env::var("LOWER_CONCURRENT_GC_THREAD_PRIORITY").unwrap_or_else(|_| "0".to_string()) != "0"
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
// Do a tracing GC if the previous pause cannot yield more tnan 20% of clean blocks.
pub static TRACE_THRESHOLD: Lazy<f32> = Lazy::new(|| {
    env::var("TRACE_THRESHOLD")
        .map(|x| x.parse().unwrap())
        .unwrap_or(20f32)
});
pub static MAX_MATURE_DEFRAG_PERCENT: Lazy<usize> = Lazy::new(|| {
    env::var("MAX_MATURE_DEFRAG_PERCENT")
        .map(|x| x.parse().unwrap())
        .unwrap_or(15)
});

pub static MAX_PAUSE_MILLIS: Lazy<Option<usize>> = Lazy::new(|| {
    env::var("MAX_PAUSE_MILLIS")
        .map(|x| x.parse().unwrap())
        .ok()
});

pub static MAX_COPY_SIZE: Lazy<usize> = Lazy::new(|| {
    env::var("MAX_COPY_SIZE")
        .map(|x| x.parse().unwrap())
        .unwrap_or(512)
});

pub static CM_STOP_BLOCKS: Lazy<usize> = Lazy::new(|| {
    env::var("CM_STOP_BLOCKS")
        .map(|x| x.parse().unwrap())
        .unwrap_or(128)
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
pub const NO_RC_PAUSES_DURING_CONCURRENT_MARKING: bool = cfg!(feature = "lxr_no_rc_in_cm");
pub const SLOW_CONCURRENT_MARKING: bool = false;
pub const LXR_RC_ONLY: bool = cfg!(feature = "lxr_rc_only");
pub const INC_MAX_COPY_DEPTH: bool = false;

pub static MAX_SURVIVAL_MB: Lazy<Option<usize>> = Lazy::new(|| {
    env::var("MAX_SURVIVAL_MB")
        .map(|x| x.parse().unwrap())
        .ok()
        .or(Some(128))
});

pub static SURVIVAL_PREDICTOR_HARMONIC_MEAN: Lazy<bool> = Lazy::new(|| {
    env::var("SURVIVAL_PREDICTOR_HARMONIC_MEAN")
        .map(|x| x != "0")
        .unwrap_or(false)
});
pub static SURVIVAL_PREDICTOR_WEIGHTED: Lazy<bool> = Lazy::new(|| {
    env::var("SURVIVAL_PREDICTOR_WEIGHTED")
        .map(|x| x != "0")
        .unwrap_or(true)
});
pub static TRACE_THRESHOLD2: Lazy<Option<usize>> = Lazy::new(|| {
    env::var("TRACE_THRESHOLD2")
        .map(|x| x.parse().unwrap())
        .ok()
        .or(Some(5))
});
// ---------- Derived flags ---------- //
pub static IGNORE_REUSING_BLOCKS: bool = true;

macro_rules! dump_feature {
    ($name: literal, $value: expr) => {
        println!(" * {}: {:?}", $name, $value)
    };
    ($name: literal) => {
        dump_feature!($name, cfg!(feature = $name))
    };
}

fn dump_features(active_barrier: BarrierSelector, options: &Options) {
    println!("-------------------- Immix Args --------------------");

    dump_feature!("barrier", format!("{:?}", active_barrier));

    dump_feature!("barrier_measurement");
    dump_feature!("instrumentation");
    dump_feature!("ix_block_only");
    dump_feature!("ix_no_defrag");
    dump_feature!("ix_ref_count");
    dump_feature!("lxr_evacuate_nursery_in_recycled_lines");
    dump_feature!("lxr_delayed_nursery_evacuation");
    dump_feature!("lxr_enable_initial_alloc_limit");

    dump_feature!(
        "disable_mutator_line_reusing",
        *DISABLE_MUTATOR_LINE_REUSING
    );
    dump_feature!("lock_free_blocks", *LOCK_FREE_BLOCK_ALLOCATION_BUFFER_SIZE);
    dump_feature!("nursery_blocks", *NURSERY_BLOCKS);
    dump_feature!("nursery_ratio", *NURSERY_RATIO);
    dump_feature!(
        "low_concurrent_worker_priority",
        *LOWER_CONCURRENT_GC_THREAD_PRIORITY
    );
    dump_feature!("concurrent_worker_ratio", *CONCURRENT_GC_THREADS_RATIO);
    dump_feature!(
        "concurrent_marking_threshold",
        *CONCURRENT_MARKING_THRESHOLD
    );
    dump_feature!("ignore_reusing_blocks", IGNORE_REUSING_BLOCKS);
    dump_feature!("log_block_size", Block::LOG_BYTES);
    dump_feature!("log_line_size", Line::LOG_BYTES);
    dump_feature!("max_mature_defrag_percent", *MAX_MATURE_DEFRAG_PERCENT);
    dump_feature!(
        "no_gc_until_lazy_sweeping_finished",
        *NO_GC_UNTIL_LAZY_SWEEPING_FINISHED
    );
    dump_feature!("log_bytes_per_rc_lock_bit", LOG_BYTES_PER_RC_LOCK_BIT);
    dump_feature!("heap_health_guided_gc", HEAP_HEALTH_GUIDED_GC);
    dump_feature!("max_pause_millis", *MAX_PAUSE_MILLIS);
    dump_feature!("incs_limit", *INC_BUFFER_LIMIT);
    dump_feature!("lxr_rc_only");

    dump_feature!("lxr_trace_threshold", *TRACE_THRESHOLD);

    dump_feature!("max_survival_mb", *MAX_SURVIVAL_MB);
    dump_feature!(
        "survival_predictor_harmonic_mean",
        *SURVIVAL_PREDICTOR_HARMONIC_MEAN
    );
    dump_feature!("survival_predictor_weighted", *SURVIVAL_PREDICTOR_WEIGHTED);
    dump_feature!("trace_threshold2", *TRACE_THRESHOLD2);
    dump_feature!("max_copy_size", *MAX_COPY_SIZE);
    dump_feature!("buffer_size", BUFFER_SIZE);
    dump_feature!("nontemporal");
    dump_feature!("cm_large_array_optimization", CM_LARGE_ARRAY_OPTIMIZATION);
    dump_feature!("inc_max_copy_depth", INC_MAX_COPY_DEPTH);

    dump_feature!("no_finalizer", *options.no_finalizer);
    dump_feature!("no_reference_types", *options.no_reference_types);

    println!("----------------------------------------------------");
}

pub fn validate_features(active_barrier: BarrierSelector, options: &Options) {
    dump_features(active_barrier, options);
    validate!(DEFRAG => !BLOCK_ONLY);
    validate!(EAGER_INCREMENTS => !RC_NURSERY_EVACUATION);
    validate!(RC_NURSERY_EVACUATION => !EAGER_INCREMENTS);
    if BARRIER_MEASUREMENT {
        assert!(!EAGER_INCREMENTS);
        assert!(!LAZY_DECREMENTS);
    }
}
