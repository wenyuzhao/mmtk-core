use spin::Lazy;

use crate::scheduler::scheduler::BucketGraph;
use crate::scheduler::BucketId;

pub static RC_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(
        BucketId::Start,
        vec![BucketId::Incs, BucketId::Roots, BucketId::Prepare],
    );

    g.dep(BucketId::Incs, vec![BucketId::Release]);
    g.dep(BucketId::Roots, vec![BucketId::Release]);
    g.dep(BucketId::Prepare, vec![BucketId::Release]);

    if !crate::args::LAZY_DECREMENTS {
        g.dep(BucketId::Release, vec![BucketId::Decs]);
        g.dep(BucketId::Decs, vec![BucketId::Finish]);
    }

    g.dep(BucketId::Release, vec![BucketId::Finish]);

    g
});

pub static INITIAL_MARK_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Start, vec![BucketId::Incs, BucketId::Roots]);

    g.dep(BucketId::Incs, vec![BucketId::Prepare]);
    g.dep(BucketId::Roots, vec![BucketId::Prepare]);

    g.dep(BucketId::Prepare, vec![BucketId::Release]);

    g.dep(BucketId::Release, vec![BucketId::Finish]);

    g
});

pub static FINAL_MARK_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Start, vec![BucketId::Incs, BucketId::Roots]);

    g.dep(BucketId::Incs, vec![BucketId::Prepare]);
    g.dep(BucketId::Roots, vec![BucketId::Prepare]);

    g.dep(BucketId::Prepare, vec![BucketId::Closure]);

    g.dep(BucketId::Closure, vec![BucketId::WeakRefClosure]);

    g.dep(BucketId::WeakRefClosure, vec![BucketId::FinalRefClosure]);

    g.dep(BucketId::FinalRefClosure, vec![BucketId::PhantomRefClosure]);

    g.dep(BucketId::Release, vec![BucketId::Finish]);

    g
});

pub static FULL_GC_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Start, vec![BucketId::Incs, BucketId::Roots]);

    g.dep(BucketId::Incs, vec![BucketId::Prepare]);
    g.dep(BucketId::Roots, vec![BucketId::Prepare]);

    g.dep(BucketId::Prepare, vec![BucketId::Closure]);

    g.dep(BucketId::Closure, vec![BucketId::WeakRefClosure]);

    g.dep(BucketId::WeakRefClosure, vec![BucketId::FinalRefClosure]);

    g.dep(BucketId::FinalRefClosure, vec![BucketId::PhantomRefClosure]);

    g.dep(BucketId::PhantomRefClosure, vec![BucketId::Release]);

    g.dep(BucketId::Release, vec![BucketId::Decs]);

    g.dep(BucketId::Decs, vec![BucketId::Finish]);

    g
});
