use crate::metrics::METRICS;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ProfileData {
    pub operation: String,
    pub duration: Duration,
    pub timestamp: Instant,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CallGraph {
    pub name: String,
    pub duration: Duration,
    pub self_time: Duration,
    pub children: Vec<CallGraph>,
}

pub struct PerformanceProfiler {
    enabled: bool,
    samples: Arc<RwLock<Vec<ProfileData>>>,
    call_stack: Arc<RwLock<Vec<(String, Instant)>>>,
    flame_graph_data: Arc<RwLock<Vec<String>>>,
}

impl Default for PerformanceProfiler {
    fn default() -> Self {
        Self::new()
    }
}

impl PerformanceProfiler {
    pub fn new() -> Self {
        Self {
            enabled: false,
            samples: Arc::new(RwLock::new(Vec::new())),
            call_stack: Arc::new(RwLock::new(Vec::new())),
            flame_graph_data: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn enable(&mut self) {
        self.enabled = true;
    }

    pub fn disable(&mut self) {
        self.enabled = false;
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    // Start profiling an operation
    pub fn start_operation(&self, operation: &str) -> ProfileGuard {
        ProfileGuard::new(self, operation)
    }

    // Record a sample
    fn record_sample(&self, data: ProfileData) {
        if self.enabled {
            let operation = data.operation.clone();
            self.samples.write().push(data);

            // Update metrics
            let recorder = METRICS.read();
            let _ = recorder.record_operation(&operation);
        }
    }

    // Enter a function for call graph tracking
    pub fn enter_function(&self, name: &str) {
        if self.enabled {
            self.call_stack
                .write()
                .push((name.to_string(), Instant::now()));

            // Record for flame graph
            let stack = self.call_stack.read();
            let trace = stack
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(";");
            self.flame_graph_data.write().push(trace);
        }
    }

    // Exit a function
    pub fn exit_function(&self) {
        if self.enabled {
            self.call_stack.write().pop();
        }
    }

    // Get performance summary
    pub fn get_summary(&self) -> HashMap<String, OperationStats> {
        let samples = self.samples.read();
        let mut summary: HashMap<String, OperationStats> = HashMap::new();

        for sample in samples.iter() {
            let stats = summary
                .entry(sample.operation.clone())
                .or_insert(OperationStats {
                    count: 0,
                    total_time: Duration::ZERO,
                    min_time: Duration::MAX,
                    max_time: Duration::ZERO,
                    avg_time: Duration::ZERO,
                });

            stats.count += 1;
            stats.total_time += sample.duration;
            stats.min_time = stats.min_time.min(sample.duration);
            stats.max_time = stats.max_time.max(sample.duration);
        }

        // Calculate averages
        for stats in summary.values_mut() {
            if stats.count > 0 {
                stats.avg_time = stats.total_time / stats.count as u32;
            }
        }

        summary
    }

    // Get hot paths (operations taking most time)
    pub fn get_hot_paths(&self, top_n: usize) -> Vec<(String, Duration)> {
        let summary = self.get_summary();
        let mut paths: Vec<_> = summary
            .into_iter()
            .map(|(op, stats)| (op, stats.total_time))
            .collect();

        paths.sort_by(|a, b| b.1.cmp(&a.1));
        paths.truncate(top_n);
        paths
    }

    // Generate flame graph data
    pub fn get_flame_graph_data(&self) -> Vec<String> {
        self.flame_graph_data.read().clone()
    }

    // Clear profiling data
    pub fn clear(&self) {
        self.samples.write().clear();
        self.call_stack.write().clear();
        self.flame_graph_data.write().clear();
    }

    // Export profiling data
    pub fn export(&self) -> ProfilingReport {
        ProfilingReport {
            summary: self.get_summary(),
            hot_paths: self.get_hot_paths(10),
            flame_graph_data: self.get_flame_graph_data(),
            total_samples: self.samples.read().len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperationStats {
    pub count: u64,
    pub total_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
    pub avg_time: Duration,
}

#[derive(Debug)]
pub struct ProfilingReport {
    pub summary: HashMap<String, OperationStats>,
    pub hot_paths: Vec<(String, Duration)>,
    pub flame_graph_data: Vec<String>,
    pub total_samples: usize,
}

impl ProfilingReport {
    pub fn print_summary(&self) {
        println!("=== Performance Profiling Report ===");
        println!("Total samples: {}", self.total_samples);
        println!("\nOperation Summary:");
        println!(
            "{:<30} {:>10} {:>15} {:>15} {:>15} {:>15}",
            "Operation", "Count", "Total (ms)", "Avg (ms)", "Min (μs)", "Max (ms)"
        );
        println!("{}", "-".repeat(100));

        let mut operations: Vec<_> = self.summary.iter().collect();
        operations.sort_by(|a, b| b.1.total_time.cmp(&a.1.total_time));

        for (op, stats) in operations {
            println!(
                "{:<30} {:>10} {:>15.2} {:>15.2} {:>15.2} {:>15.2}",
                op,
                stats.count,
                stats.total_time.as_secs_f64() * 1000.0,
                stats.avg_time.as_secs_f64() * 1000.0,
                stats.min_time.as_secs_f64() * 1_000_000.0,
                stats.max_time.as_secs_f64() * 1000.0
            );
        }

        println!("\nHot Paths (Top 10):");
        for (i, (path, duration)) in self.hot_paths.iter().enumerate() {
            println!(
                "{}. {} - {:.2}ms",
                i + 1,
                path,
                duration.as_secs_f64() * 1000.0
            );
        }
    }
}

pub struct ProfileGuard<'a> {
    profiler: &'a PerformanceProfiler,
    operation: String,
    start: Instant,
    metadata: HashMap<String, String>,
}

impl<'a> ProfileGuard<'a> {
    fn new(profiler: &'a PerformanceProfiler, operation: &str) -> Self {
        profiler.enter_function(operation);
        Self {
            profiler,
            operation: operation.to_string(),
            start: Instant::now(),
            metadata: HashMap::new(),
        }
    }

    pub fn add_metadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
    }
}

impl Drop for ProfileGuard<'_> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        self.profiler.exit_function();

        self.profiler.record_sample(ProfileData {
            operation: self.operation.clone(),
            duration,
            timestamp: self.start,
            metadata: self.metadata.clone(),
        });
    }
}

// Global profiler instance
lazy_static::lazy_static! {
    pub static ref PROFILER: Arc<RwLock<PerformanceProfiler>> =
        Arc::new(RwLock::new(PerformanceProfiler::new()));
}

// Convenience macro for profiling
#[macro_export]
macro_rules! profile {
    ($op:expr) => {{
        let profiler = $crate::profiling::PROFILER.read();
        profiler.start_operation($op)
    }};
    ($op:expr, $($key:expr => $value:expr),*) => {{
        let profiler = $crate::profiling::PROFILER.read();
        let mut guard = profiler.start_operation($op);
        $(
            guard.add_metadata($key, $value);
        )*
        guard
    }};
}

// Helper function for profiling with the global profiler
pub fn profile_operation(operation: &str) -> Option<ProfileGuard<'static>> {
    let profiler_lock = PROFILER.read();
    if profiler_lock.is_enabled() {
        // This is safe because PROFILER is a static and lives for the entire program
        let profiler_ref: &'static PerformanceProfiler =
            unsafe { &*(profiler_lock.deref() as *const PerformanceProfiler) };
        drop(profiler_lock);
        Some(profiler_ref.start_operation(operation))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_profiling() {
        let mut profiler = PerformanceProfiler::new();
        profiler.enable();

        // Profile some operations
        {
            let _guard = profiler.start_operation("test_op1");
            thread::sleep(Duration::from_millis(10));
        }

        {
            let _guard = profiler.start_operation("test_op2");
            thread::sleep(Duration::from_millis(5));
        }

        // Get summary
        let summary = profiler.get_summary();
        assert_eq!(summary.len(), 2);
        assert_eq!(summary.get("test_op1").unwrap().count, 1);
        assert_eq!(summary.get("test_op2").unwrap().count, 1);
    }

    #[test]
    fn test_nested_profiling() {
        let mut profiler = PerformanceProfiler::new();
        profiler.enable();

        {
            let _guard1 = profiler.start_operation("outer");
            thread::sleep(Duration::from_millis(5));

            {
                let _guard2 = profiler.start_operation("inner");
                thread::sleep(Duration::from_millis(5));
            }
        }

        let summary = profiler.get_summary();
        assert_eq!(summary.len(), 2);
    }

    #[test]
    fn test_hot_paths() {
        let mut profiler = PerformanceProfiler::new();
        profiler.enable();

        // Create operations with different durations
        for i in 0..5 {
            let _guard = profiler.start_operation(&format!("op_{}", i));
            thread::sleep(Duration::from_millis(i * 10));
        }

        let hot_paths = profiler.get_hot_paths(3);
        assert_eq!(hot_paths.len(), 3);

        // Should be sorted by duration descending
        assert!(hot_paths[0].1 >= hot_paths[1].1);
        assert!(hot_paths[1].1 >= hot_paths[2].1);
    }

    #[test]
    fn test_disabled_profiling() {
        let profiler = PerformanceProfiler::new();
        // Profiler is disabled by default

        {
            let _guard = profiler.start_operation("test");
            thread::sleep(Duration::from_millis(10));
        }

        let summary = profiler.get_summary();
        assert!(summary.is_empty());
    }

    #[test]
    fn test_profile_function() {
        PROFILER.write().enable();

        {
            let _guard = profile_operation("function_test");
            thread::sleep(Duration::from_millis(5));
        }

        let report = PROFILER.read().export();
        assert!(report.summary.contains_key("function_test"));

        PROFILER.write().clear();
    }
}
