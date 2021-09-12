use super::worker::*;
use crate::mmtk::MMTK;
use crate::vm::VMBinding;
use std::any::{type_name, TypeId};

/// A special kind of work that will execute on the coordinator (i.e. controller) thread
///
/// The coordinator thread holds the global monitor lock when executing `CoordinatorWork`s.
/// So, directly adding new work to any buckets will cause dead lock.
/// For this case, use `WorkBucket::add_with_priority_unsync` instead.
pub trait CoordinatorWork<VM: VMBinding>: 'static + Send + GCWork<VM> {}

pub trait GCWork<VM: VMBinding>: 'static + Send {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>);
    #[inline]
    fn do_work_with_stat(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let stat = worker
            .stat
            .measure_work(TypeId::of::<Self>(), type_name::<Self>(), mmtk);
        // println!(" - {:?}", type_name::<Self>());
        self.do_work(worker, mmtk);
        stat.end_of_work(&mut worker.stat);
    }
}

pub struct Lambda(pub Box<dyn FnOnce() + 'static + Send>);

impl<VM: VMBinding> GCWork<VM> for Lambda {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        let mut f: Box<dyn FnOnce() + 'static + Send> = box || {};
        std::mem::swap(&mut f, &mut self.0);
        f();
    }
}
