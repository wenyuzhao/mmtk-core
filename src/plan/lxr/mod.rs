mod barrier;
pub(super) mod cm;
mod gc_work;
pub(super) mod global;
mod mature_evac;
pub(super) mod mutator;
pub mod rc;
mod remset;

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

pub use self::global::LXR;
pub use self::global::LXR_CONSTRAINTS;
pub use self::remset::RemSet;

use atomic::Ordering;
use crossbeam::queue::SegQueue;

use crate::policy::immix::block::Block;
use crate::util::exponential_decay::ExponentialDecayValue;
use crate::util::Address;
use crate::util::ObjectReference;
use crate::vm::edge_shape::Edge;

const CYCLE_TRIGGER_THRESHOLD: usize = crate::args::CYCLE_TRIGGER_THRESHOLD;

pub static mut PREV_ROOTS: SegQueue<Vec<ObjectReference>> = SegQueue::new();
pub static mut CURR_ROOTS: SegQueue<Vec<ObjectReference>> = SegQueue::new();

pub static SURVIVAL_RATIO_PREDICTOR: SurvivalRatioPredictor = SurvivalRatioPredictor {
    prev_ratio: ExponentialDecayValue::new(0.0),
};

pub struct SurvivalRatioPredictor {
    prev_ratio: ExponentialDecayValue<f64>,
}

impl SurvivalRatioPredictor {
    #[inline(always)]
    pub fn ratio(&self) -> f64 {
        self.prev_ratio.get()
    }

    #[inline(always)]
    pub(super) fn update(&self) {
        let curr = RUNTIME_STAT.promoted_volume.load(Ordering::Relaxed) as f64
            / (RUNTIME_STAT.young_blocks.load(Ordering::Relaxed) << Block::LOG_BYTES) as f64;
        let ratio = self.prev_ratio.update(curr);
        crate::add_survival_ratio(curr, ratio);
    }
}

pub struct PauseTimePredictor {
    /// Pridicated ratio of bytes allocated within reused blocks
    mutator_reuse_rate: ExponentialDecayValue<f64>,
    /// Pridicated promotion rate
    promotion_rate: ExponentialDecayValue<f64>,
    /// Time in nanos to process an RC increment
    inc_time: ExponentialDecayValue<f64>,
    /// Time in nanos to promote (including copy and RC-update) a byte
    promotion_time: ExponentialDecayValue<f64>,
}

impl PauseTimePredictor {
    pub fn new() -> Self {
        Self {
            mutator_reuse_rate: ExponentialDecayValue::new(0.0),
            promotion_rate: ExponentialDecayValue::new(0.0),
            inc_time: ExponentialDecayValue::new(0.0),
            promotion_time: ExponentialDecayValue::new(0.0),
        }
    }

    pub(super) fn update(&self, mutator_reuse_rate: f64, promotion_rate: f64) {
        self.mutator_reuse_rate.update(mutator_reuse_rate);
        self.promotion_rate.update(promotion_rate);
    }

    #[inline(always)]
    pub fn predict(&self, incs: usize, young_blocks: usize, threshold_ms: usize) -> bool {
        let young_blocks_size = young_blocks << Block::LOG_BYTES;
        let total_young_size = young_blocks_size as f64 / (1.0 - self.mutator_reuse_rate.get());
        let incs_cost = incs as f64 * self.inc_time.get();
        let promotion_cost =
            total_young_size * self.promotion_rate.get() * self.promotion_time.get();
        let total_cost_in_ms = (incs_cost + promotion_cost) / 1000000.0;
        total_cost_in_ms as usize >= threshold_ms
    }
}

pub struct RuntimeStat {
    pub promoted_volume: AtomicUsize,
    pub young_blocks: AtomicUsize,
    pub incs_time_ns: AtomicUsize,
    pub incs_size: AtomicUsize,
}

static RUNTIME_STAT: RuntimeStat = RuntimeStat {
    promoted_volume: AtomicUsize::new(0),
    young_blocks: AtomicUsize::new(0),
    incs_time_ns: AtomicUsize::new(0),
    incs_size: AtomicUsize::new(0),
};

impl RuntimeStat {
    fn reset(&self) {
        self.promoted_volume.store(0, Ordering::SeqCst);
        self.young_blocks.store(0, Ordering::SeqCst);
    }
}

lazy_static! {
    static ref LAST_REFERENTS: Mutex<HashMap<Address, ObjectReference>> = Default::default();
}

#[inline(always)]
pub fn record_edge_for_validation(slot: impl Edge, obj: ObjectReference) {
    if cfg!(feature = "field_barrier_validation") {
        LAST_REFERENTS
            .lock()
            .unwrap()
            .insert(slot.to_address(), obj);
    }
}
