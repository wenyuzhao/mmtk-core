use enum_map::Enum;
use spin::Lazy;

use crate::scheduler::scheduler::BucketGraph;
use crate::scheduler::BucketId;
use crate::scheduler::WorkBucket;

static START: Lazy<WorkBucket> = Lazy::new(|| WorkBucket::new("start"));
static ROOTS: Lazy<WorkBucket> = Lazy::new(|| WorkBucket::new("roots"));
static PREPARE: Lazy<WorkBucket> = Lazy::new(|| WorkBucket::new("prepare"));
static CLOSURE: Lazy<WorkBucket> = Lazy::new(|| WorkBucket::new("closure"));
static RELEASE: Lazy<WorkBucket> = Lazy::new(|| WorkBucket::new("release"));

pub static DEFAULT_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Start, vec![BucketId::Prepare]);
    g.dep(BucketId::Start, vec![BucketId::Roots]);

    g.dep(BucketId::Prepare, vec![BucketId::Closure]);

    g.dep(BucketId::Roots, vec![BucketId::Release]);
    g.dep(BucketId::Closure, vec![BucketId::Release]);

    g.dep(BucketId::Release, vec![BucketId::Finish]);

    g
});
