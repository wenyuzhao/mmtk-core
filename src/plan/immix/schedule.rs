use spin::Lazy;

use crate::scheduler::scheduler::BucketGraph;
use crate::scheduler::BucketId;

pub static DEFAULT_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Start, vec![BucketId::Prepare]);
    g.dep(BucketId::Start, vec![BucketId::Roots]);

    g.dep(BucketId::Prepare, vec![BucketId::Closure]);

    g.dep(BucketId::Roots, vec![BucketId::WeakRefClosure]);
    g.dep(BucketId::Closure, vec![BucketId::WeakRefClosure]);

    g.dep(BucketId::WeakRefClosure, vec![BucketId::FinalRefClosure]);

    g.dep(BucketId::FinalRefClosure, vec![BucketId::PhantomRefClosure]);

    g.dep(BucketId::PhantomRefClosure, vec![BucketId::Release]);

    g.dep(BucketId::Release, vec![BucketId::Finish]);

    g
});
