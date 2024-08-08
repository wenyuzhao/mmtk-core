use enum_map::Enum;
use spin::Lazy;

use crate::scheduler::scheduler::{BucketGraph, BucketKey};
use crate::scheduler::{WorkBucket, WorkBucketStage};

#[derive(Debug, Enum, Copy, Clone, Eq, PartialEq)]
pub enum ImmixBuckets {
    Start,
    Prepare,
    Roots,
    Closure,
    Release,
    Finish,
}

impl BucketKey for ImmixBuckets {
    fn get_bucket(&self) -> &WorkBucket {
        unimplemented!()
    }
}

pub static DEFAULT_SCHEDULE: Lazy<BucketGraph<ImmixBuckets>> = Lazy::new(|| {
    let mut g = BucketGraph::new();

    g.dep(ImmixBuckets::Start, vec![ImmixBuckets::Prepare]);
    g.dep(ImmixBuckets::Start, vec![ImmixBuckets::Roots]);

    g.dep(ImmixBuckets::Prepare, vec![ImmixBuckets::Closure]);

    g.dep(ImmixBuckets::Roots, vec![ImmixBuckets::Release]);
    g.dep(ImmixBuckets::Closure, vec![ImmixBuckets::Release]);

    g.dep(ImmixBuckets::Release, vec![ImmixBuckets::Finish]);

    g
});
