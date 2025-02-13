//! Statistics for work packets
use super::work_counter::{WorkCounter, WorkCounterBase, WorkDuration};
#[cfg(feature = "perf_counter")]
use crate::scheduler::work_counter::WorkPerfEvent;
use crate::vm::VMBinding;
use crate::MMTK;
use std::any::TypeId;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

/// Merge and print the work-packet level statistics from all worker threads
#[derive(Default)]
pub struct SchedulerStat {
    /// Map work packet type IDs to work packet names
    work_id_name_map: HashMap<TypeId, &'static str>,
    /// Count the number of work packets executed for different types
    work_counts: HashMap<TypeId, usize>,
    /// Collect work counters from work threads.
    /// Two dimensional vectors are used, e.g.
    /// `[[foo_0, ..., foo_n], ..., [bar_0, ..., bar_n]]`.
    /// The first dimension is for different types of work counters,
    /// (`foo` and `bar` in the above example).
    /// The second dimension if for work counters of the same type but from
    /// different threads (`foo_0` and `bar_0` are from the same thread).
    /// The order of insertion is determined by when [`SchedulerStat::merge`] is
    /// called for each [`WorkerLocalStat`].
    /// We assume different threads have the same set of work counters
    /// (in the same order).
    work_counters: HashMap<TypeId, Vec<Vec<Box<dyn WorkCounter>>>>,
}

impl SchedulerStat {
    /// Extract the work-packet name from the full type name.
    /// i.e. simplifies `crate::scheduler::gc_work::SomeWorkPacket<Semispace>` to `SomeWorkPacket`.
    fn work_name(&self, name: &str) -> String {
        let end_index = name.find('<').unwrap_or(name.len());
        let name = name[..end_index].to_owned();
        match name.rfind(':') {
            Some(start_index) => name[(start_index + 1)..end_index].to_owned(),
            _ => name,
        }
    }

    /// Used during statistics printing at [`crate::memory_manager::harness_end`]
    pub fn harness_stat(&self) -> HashMap<String, String> {
        let mut stat = HashMap::new();
        // Block reusability
        let mut report_reusability = |tag: &str, data: &Vec<(bool, usize, usize)>| {
            if data.len() == 0 {
                println!("No data for {}", tag);
                return;
            }
            println!("{}: {:.3?}", tag, data);
            let no_reusable = data
                .iter()
                .map(|(r, _, _)| if *r { 1 } else { 0 })
                .sum::<usize>() as f64
                / data.len() as f64;
            let no_partially_free = data
                .iter()
                .map(|(_, x, _)| if *x == 0 { 1 } else { 0 })
                .sum::<usize>() as f64
                / data.len() as f64;
            stat.insert(format!("no_reusable.{tag}"), format!("{:.3}", no_reusable));
            stat.insert(
                format!("no_partially_free.{tag}"),
                format!("{:.3}", no_partially_free),
            );
            let partially_free_ratio = data
                .iter()
                .filter(|(_, _, y)| *y != 0)
                .map(|(_, x, y)| *x as f64 / *y as f64)
                .collect::<Vec<f64>>();
            if partially_free_ratio.len() == 0 {
                return;
            }
            let min = partially_free_ratio
                .iter()
                .fold(f64::INFINITY, |a, &b| a.min(b));
            let max = partially_free_ratio
                .iter()
                .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            let mean = partially_free_ratio.iter().sum::<f64>() / partially_free_ratio.len() as f64;
            stat.insert(
                format!("partially_free_ratio.min.{tag}"),
                format!("{:.3}", min),
            );
            stat.insert(
                format!("partially_free_ratio.max.{tag}"),
                format!("{:.3}", max),
            );
            stat.insert(
                format!("partially_free_ratio.mean.{tag}"),
                format!("{:.3}", mean),
            );
            let partially_free_ratio_non_zero = partially_free_ratio
                .iter()
                .filter(|&&x| x != 0.0)
                .collect::<Vec<&f64>>();
            fn geometric_mean(numbers: &Vec<&f64>) -> f64 {
                let log_sum: f64 = numbers.iter().map(|&x| x.ln()).sum();
                let n = numbers.len() as f64;
                (log_sum / n).exp()
            }
            let geomean = geometric_mean(&partially_free_ratio_non_zero);
            stat.insert(
                format!("partially_free_ratio.geomean.{tag}"),
                format!("{:.3}", geomean),
            );
        };
        report_reusability("beforegc", &*crate::REUSABLE_BLOCKS_BEFORE_GC.lock());
        report_reusability("aftergc", &*crate::REUSABLE_BLOCKS_AFTER_GC.lock());

        if cfg!(not(feature = "work_packet_counter")) {
            return stat;
        }
        // let mut stat = HashMap::new();
        let mut counts = HashMap::<String, usize>::new();
        let mut times = HashMap::<String, f64>::new();
        // Work counts
        let mut total_count = 0;
        for (t, c) in &self.work_counts {
            total_count += c;
            let n = self.work_id_name_map[t];
            // We can have the same work names for different TypeIDs since work names strip
            // type parameters away, while the same work packet with different type parameters
            // are given different TypeIDs. Hence, we check if the key exists and update instead of
            // overwrite it
            let pkt = format!("work.{}.count", self.work_name(n));
            let val = counts.entry(pkt).or_default();
            *val += c;
        }
        stat.insert("total-work.count".to_owned(), format!("{}", total_count));
        // Work execution times
        let mut duration_overall: WorkCounterBase = Default::default();
        let mut total: u128 = 0;
        let mut work_counters = self.work_counters.iter().collect::<Vec<_>>();
        work_counters.sort_by_cached_key(|(t, _)| self.work_id_name_map[t]);
        for (t, vs) in work_counters {
            // Name of the work packet type
            let n = self.work_id_name_map[t];
            // Iterate through different types of work counters
            for v in vs.iter() {
                // Aggregate work counters of the same type but from different
                // worker threads
                let fold = v
                    .iter()
                    .fold(Default::default(), |acc: WorkCounterBase, x| {
                        acc.merge(x.get_base())
                    });
                // Update the overall execution time
                duration_overall.merge_inplace(&fold);
                let name = v.first().unwrap().name();
                let pkt_total = format!("work.{}.{}.total", self.work_name(n), name);
                let pkt_min = format!("work.{}.{}.min", self.work_name(n), name);
                let pkt_max = format!("work.{}.{}.max", self.work_name(n), name);

                // We can have the same work names for different TypeIDs since work names strip
                // type parameters away, while the same work packet with different type parameters
                // are given different TypeIDs. Hence, we check if the key exists and update
                // instead of overwrite it
                let val = times.entry(pkt_total).or_default();
                *val += fold.total;
                let val = times.entry(pkt_min).or_default();
                *val = f64::min(*val, fold.min);
                let val = times.entry(pkt_max).or_default();
                *val = f64::max(*val, fold.max);

                if crate::args::HARNESS_PRETTY_PRINT && name == "time" {
                    println!(" - {:<35} total={:15}    min={:10}    max={:15}    avg={:15.2}    count={:10}", self.work_name(n), fold.total, fold.min, fold.max, fold.total / self.work_counts[t] as f64, self.work_counts[t]);
                    total += fold.total as u128;
                }
            }
        }
        if crate::args::HARNESS_PRETTY_PRINT {
            println!("SUM: {} ns", total);
            if crate::args::INSTRUMENTATION {
                crate::STAT.lock().pretty_print();
            }
        }

        // Convert to ms and print out overall execution time
        stat.insert(
            "total-work.time.total".to_owned(),
            format!("{:.3}", duration_overall.total / 1e6),
        );
        stat.insert(
            "total-work.time.min".to_owned(),
            format!("{:.3}", duration_overall.min / 1e6),
        );
        stat.insert(
            "total-work.time.max".to_owned(),
            format!("{:.3}", duration_overall.max / 1e6),
        );

        for (pkt, count) in counts {
            stat.insert(pkt, format!("{}", count));
        }

        for (pkt, time) in times {
            stat.insert(pkt, format!("{:.3}", time / 1e6));
        }

        stat
    }
    /// Merge work counters from different worker threads
    pub fn merge<C>(&mut self, stat: &WorkerLocalStat<C>) {
        // Merge work packet type ID to work packet name mapping
        for (id, name) in &stat.work_id_name_map {
            self.work_id_name_map.insert(*id, *name);
        }
        // Merge work count for different work packet types
        for (id, count) in &stat.work_counts {
            if self.work_counts.contains_key(id) {
                *self.work_counts.get_mut(id).unwrap() += *count;
            } else {
                self.work_counts.insert(*id, *count);
            }
        }
        // Merge work counter for different work packet types
        for (id, counters) in &stat.work_counters {
            // Initialize the two dimensional vector
            // [
            //    [], // foo counter
            //    [], // bar counter
            // ]
            let vs = self
                .work_counters
                .entry(*id)
                .or_insert_with(|| vec![vec![]; counters.len()]);
            // [
            //    [counters[0] of type foo],
            //    [counters[1] of type bar]
            // ]
            for (v, c) in vs.iter_mut().zip(counters.iter()) {
                v.push(c.clone());
            }
        }
    }
}

/// Describing a single work packet
pub struct WorkStat {
    type_id: TypeId,
    type_name: &'static str,
}

impl WorkStat {
    /// Stop all work counters for the work packet type of the just executed
    /// work packet
    pub fn end_of_work<VM: VMBinding>(&self, worker_stat: &mut WorkerLocalStat<VM>) {
        if !worker_stat.is_enabled() {
            return;
        };
        // Insert type ID, name pair
        worker_stat
            .work_id_name_map
            .insert(self.type_id, self.type_name);
        // Increment work count
        *worker_stat.work_counts.entry(self.type_id).or_insert(0) += 1;
        // Stop counters
        worker_stat
            .work_counters
            .entry(self.type_id)
            .and_modify(|v| {
                v.iter_mut().for_each(|c| c.stop());
            });
    }
}

/// Worker thread local counterpart of [`SchedulerStat`]
pub struct WorkerLocalStat<C> {
    work_id_name_map: HashMap<TypeId, &'static str>,
    work_counts: HashMap<TypeId, usize>,
    work_counters: HashMap<TypeId, Vec<Box<dyn WorkCounter>>>,
    enabled: AtomicBool,
    _phantom: PhantomData<C>,
}

unsafe impl<C> Send for WorkerLocalStat<C> {}

impl<C> Default for WorkerLocalStat<C> {
    fn default() -> Self {
        WorkerLocalStat {
            work_id_name_map: Default::default(),
            work_counts: Default::default(),
            work_counters: Default::default(),
            enabled: AtomicBool::new(false),
            _phantom: Default::default(),
        }
    }
}

impl<VM: VMBinding> WorkerLocalStat<VM> {
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }
    /// Measure the execution of a work packet by starting all counters for that
    /// type
    pub fn measure_work(
        &mut self,
        work_id: TypeId,
        work_name: &'static str,
        mmtk: &'static MMTK<VM>,
    ) -> WorkStat {
        let stat = WorkStat {
            type_id: work_id,
            type_name: work_name,
        };
        if self.is_enabled() {
            self.work_counters
                .entry(work_id)
                .or_insert_with(|| Self::counter_set(mmtk))
                .iter_mut()
                .for_each(|c| c.start());
        }
        stat
    }

    #[allow(unused_variables, unused_mut)]
    fn counter_set(mmtk: &'static MMTK<VM>) -> Vec<Box<dyn WorkCounter>> {
        let mut counters: Vec<Box<dyn WorkCounter>> = vec![Box::new(WorkDuration::new())];
        #[cfg(feature = "perf_counter")]
        for e in &mmtk.options.work_perf_events.events {
            counters.push(Box::new(WorkPerfEvent::new(
                &e.0,
                e.1,
                e.2,
                *mmtk.options.perf_exclude_kernel,
            )));
        }
        counters
    }
}
