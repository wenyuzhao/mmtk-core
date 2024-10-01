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
        g.dep(BucketId::Decs, vec![BucketId::LazySweep]);
        g.dep(BucketId::LazySweep, vec![BucketId::Finish]);
    }

    g.dep(BucketId::Release, vec![BucketId::Finish]);

    g
});

pub static RC_CONC_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Decs, vec![BucketId::LazySweep]);
    g.dep(BucketId::LazySweep, vec![]);

    g
});

pub static CONC_MARK_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    if crate::args::LAZY_DECREMENTS {
        g.dep(BucketId::Decs, vec![BucketId::LazySweep]);
        g.dep(BucketId::LazySweep, vec![]);
        g.dep(BucketId::ConcClosure, vec![]);
    } else {
        g.dep(BucketId::ConcClosure, vec![]);
    }

    g
});

pub static POST_SATB_SWEEPING_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Decs, vec![BucketId::LazySweep]);
    g.dep(BucketId::LazySweep, vec![]);

    g
});

pub static INITIAL_MARK_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Start, vec![BucketId::Incs, BucketId::Roots]);

    g.dep(BucketId::Incs, vec![BucketId::Prepare]);
    g.dep(BucketId::Roots, vec![BucketId::Prepare]);

    g.dep(BucketId::Prepare, vec![BucketId::Release]);

    if !crate::args::LAZY_DECREMENTS {
        g.dep(BucketId::Release, vec![BucketId::Decs]);
        g.dep(BucketId::Decs, vec![BucketId::LazySweep]);
        g.dep(BucketId::LazySweep, vec![BucketId::Finish]);
    }

    g.dep(BucketId::Release, vec![BucketId::Finish]);

    g
});

pub static FINAL_MARK_SCHEDULE: Lazy<BucketGraph> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(BucketId::Start, vec![BucketId::FinishMark]);

    g.dep(BucketId::FinishMark, vec![BucketId::Incs, BucketId::Roots]);

    g.dep(BucketId::Incs, vec![BucketId::Prepare]);
    g.dep(BucketId::Roots, vec![BucketId::Prepare]);

    g.dep(BucketId::Prepare, vec![BucketId::Closure]);

    g.dep(BucketId::Closure, vec![BucketId::WeakRefClosure]);

    g.dep(BucketId::WeakRefClosure, vec![BucketId::FinalRefClosure]);

    g.dep(BucketId::FinalRefClosure, vec![BucketId::PhantomRefClosure]);

    g.dep(BucketId::PhantomRefClosure, vec![BucketId::Release]);

    if !crate::args::LAZY_DECREMENTS {
        g.dep(BucketId::Release, vec![BucketId::Decs]);
        g.dep(BucketId::Decs, vec![BucketId::LazySweep]);
        g.dep(BucketId::LazySweep, vec![BucketId::Finish]);
    }

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

    g.dep(BucketId::Decs, vec![BucketId::LazySweep]);

    g.dep(BucketId::LazySweep, vec![BucketId::Finish]);

    g
});
