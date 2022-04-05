pub mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

use std::sync::atomic::AtomicUsize;
use std::time::SystemTime;

pub use self::global::Immix;
pub use self::global::IMMIX_CONSTRAINTS;

use atomic::Atomic;
use atomic::Ordering;
use crossbeam_queue::SegQueue;

use crate::util::ObjectReference;

pub use self::gc_work::ImmixCopyContext;

pub const CONCURRENT_MARKING: bool = crate::args::CONCURRENT_MARKING;

pub const REF_COUNT: bool = crate::args::REF_COUNT;

const CYCLE_TRIGGER_THRESHOLD: usize = crate::args::CYCLE_TRIGGER_THRESHOLD;

pub static mut PREV_ROOTS: SegQueue<Vec<ObjectReference>> = SegQueue::new();
pub static mut CURR_ROOTS: SegQueue<Vec<ObjectReference>> = SegQueue::new();

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Pause {
    FullTraceFast,
    FullTraceDefrag,
    RefCount,
    InitialMark,
    FinalMark,
}

pub static SURVIVAL_RATIO_PREDICTOR: SurvivalRatioPredictor = SurvivalRatioPredictor {
    prev_ratio: Atomic::new(0.2),
    alloc_vol: AtomicUsize::new(0),
    promote_vol: AtomicUsize::new(0),
    pause_start: Atomic::new(SystemTime::UNIX_EPOCH),
};

#[thread_local]
pub static SURVIVAL_RATIO_PREDICTOR_LOCAL: SurvivalRatioPredictorLocal =
    SurvivalRatioPredictorLocal {
        promote_vol: AtomicUsize::new(0),
    };

pub struct SurvivalRatioPredictor {
    prev_ratio: Atomic<f64>,
    alloc_vol: AtomicUsize,
    promote_vol: AtomicUsize,
    pub pause_start: Atomic<SystemTime>,
}

impl SurvivalRatioPredictor {
    #[inline(always)]
    pub fn set_alloc_size(&self, size: usize) {
        // println!("set_alloc_size {}", size);
        assert_eq!(self.alloc_vol.load(Ordering::Relaxed), 0);
        self.alloc_vol.store(size, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn ratio(&self) -> f64 {
        self.prev_ratio.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn update_ratio(&self) -> f64 {
        if self.alloc_vol.load(Ordering::Relaxed) == 0 {
            return self.ratio();
        }
        let prev = self.prev_ratio.load(Ordering::Relaxed);
        let curr = self.promote_vol.load(Ordering::Relaxed) as f64
            / self.alloc_vol.load(Ordering::Relaxed) as f64;
        let ratio = (curr + prev) / 2f64;
        crate::add_survival_ratio(curr, prev);
        self.prev_ratio.store(ratio, Ordering::Relaxed);
        self.alloc_vol.store(0, Ordering::Relaxed);
        self.promote_vol.store(0, Ordering::Relaxed);
        ratio
    }
}

#[derive(Default)]
pub struct SurvivalRatioPredictorLocal {
    promote_vol: AtomicUsize,
}

impl SurvivalRatioPredictorLocal {
    #[inline(always)]
    pub fn record_promotion(&self, size: usize) {
        self.promote_vol.store(
            self.promote_vol.load(Ordering::Relaxed) + size,
            Ordering::Relaxed,
        );
    }

    #[inline(always)]
    pub fn sync(&self) {
        SURVIVAL_RATIO_PREDICTOR
            .promote_vol
            .fetch_add(self.promote_vol.load(Ordering::Relaxed), Ordering::Relaxed);
        self.promote_vol.store(0, Ordering::Relaxed);
    }
}
