use super::worker::*;
use crate::mmtk::MMTK;
use crate::vm::VMBinding;
use std::any::{type_name, Any};

/// A special kind of work that will execute on the coordinator (i.e. controller) thread
///
/// The coordinator thread holds the global monitor lock when executing `CoordinatorWork`s.
/// So, directly adding new work to any buckets will cause dead lock.
/// For this case, use `WorkBucket::add_with_priority_unsync` instead.
pub trait CoordinatorWork<VM: VMBinding>: 'static + Send + GCWork<VM> {}

pub trait GCWork<VM: VMBinding>: 'static + Send + Any {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>);
    #[inline]
    fn do_work_with_stat(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        debug!("{}", std::any::type_name::<Self>());
        #[cfg(feature = "work_packet_timer")]
        let stat =
            worker
                .stat
                .measure_work(std::any::TypeId::of::<Self>(), type_name::<Self>(), mmtk);
        if crate::args::LOG_WORK_PACKETS {
            println!("{} > {}", worker.ordinal, type_name::<Self>());
        }
        self.do_work(worker, mmtk);
        #[cfg(feature = "work_packet_timer")]
        stat.end_of_work(&mut worker.stat);
    }
}
