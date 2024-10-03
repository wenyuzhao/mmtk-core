//! A general scheduler implementation. MMTk uses it to schedule GC-related work.

pub(crate) mod affinity;

#[allow(clippy::module_inception)]
mod scheduler;
use std::sync::atomic::AtomicUsize;

use crossbeam::queue::SegQueue;
pub(crate) use scheduler::GCWorkScheduler;

mod stat;
mod work_counter;

mod work;
pub use work::GCWork;
pub(crate) use work::GCWorkContext;

mod work_bucket;
pub use work_bucket::WorkBucketStage;

pub mod worker;
mod worker_goals;
mod worker_monitor;
pub(crate) use worker::current_worker_ordinal;
pub use worker::GCWorker;

pub(crate) mod gc_work;
pub use gc_work::{ProcessEdgesWork, RootKind};

use crate::Timer;

// Total utilization (exclude Release phase)
static TOTAL_RELEASE_TIME_US: AtomicUsize = AtomicUsize::new(0);
static TOTAL_BUSY_TIME_US: AtomicUsize = AtomicUsize::new(0);
static UTILIZATIONS: SegQueue<f32> = SegQueue::new();
static TOTAL_TIME_US: AtomicUsize = AtomicUsize::new(0);
static RELEASE_START: Timer = Timer::new();
static FINAL_START: Timer = Timer::new();
// Transitive closure utilization
static TOTAL_TRACE_TIME_US: AtomicUsize = AtomicUsize::new(0);
static TOTAL_TRACE_BUSY_TIME_US: AtomicUsize = AtomicUsize::new(0);
static TRACE_UTILIZATIONS: SegQueue<f32> = SegQueue::new();
static TRACE_START: Timer = Timer::new();
// RC increment utilization
static INC_UTILIZATIONS: SegQueue<f32> = SegQueue::new();
static TOTAL_INC_TIME_US: AtomicUsize = AtomicUsize::new(0);
static TOTAL_INC_BUSY_TIME_US: AtomicUsize = AtomicUsize::new(0);
