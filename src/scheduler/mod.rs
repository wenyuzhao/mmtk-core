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

static TOTAL_BUSY_TIME_US: AtomicUsize = AtomicUsize::new(0);
static UTILIZATIONS: SegQueue<f32> = SegQueue::new();
static TRACE_UTILIZATIONS: SegQueue<f32> = SegQueue::new();

static TOTAL_TRACE_TIME_US: AtomicUsize = AtomicUsize::new(0);
static TOTAL_TRACE_BUSY_TIME_US: AtomicUsize = AtomicUsize::new(0);
static TRACE_START: Timer = Timer::new();
