use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct PerformanceMonitor {
    operation_timers: Arc<RwLock<HashMap<String, OperationTimer>>>,
    resource_tracker: Arc<ResourceTracker>,
    overhead_tracker: Arc<OverheadTracker>,
    alert_thresholds: AlertThresholds,
    enabled: bool,
}

#[derive(Debug, Clone)]
struct OperationTimer {
    name: String,
    start_time: Instant,
    samples: VecDeque<Duration>,
    max_samples: usize,
    total_operations: u64,
    slow_operations: u64,
}

#[derive(Debug, Clone)]
pub struct AlertThresholds {
    pub slow_operation_threshold: Duration,
    pub memory_usage_threshold: u64,
    pub cpu_usage_threshold: f64,
    pub error_rate_threshold: f64,
    pub cache_miss_rate_threshold: f64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            slow_operation_threshold: Duration::from_millis(100),
            memory_usage_threshold: 1024 * 1024 * 1024, // 1GB
            cpu_usage_threshold: 80.0,                  // 80%
            error_rate_threshold: 0.05,                 // 5%
            cache_miss_rate_threshold: 0.3,             // 30%
        }
    }
}

impl PerformanceMonitor {
    pub fn new(enabled: bool, alert_thresholds: Option<AlertThresholds>) -> Self {
        Self {
            operation_timers: Arc::new(RwLock::new(HashMap::new())),
            resource_tracker: Arc::new(ResourceTracker::new()),
            overhead_tracker: Arc::new(OverheadTracker::new()),
            alert_thresholds: alert_thresholds.unwrap_or_default(),
            enabled,
        }
    }

    pub fn start_operation(&self, name: &str) -> Option<PerformanceToken> {
        if !self.enabled {
            return None;
        }

        let start_time = Instant::now();
        let overhead_start = Instant::now();

        let token = PerformanceToken {
            operation_name: name.to_string(),
            start_time,
            monitor: Arc::downgrade(&Arc::new(self.clone())),
        };

        // Track overhead of starting the operation
        self.overhead_tracker
            .record_overhead("start_operation", overhead_start.elapsed());

        Some(token)
    }

    pub fn end_operation(&self, name: &str, start_time: Instant) {
        if !self.enabled {
            return;
        }

        let overhead_start = Instant::now();
        let duration = start_time.elapsed();

        let mut timers = self.operation_timers.write();
        let timer = timers
            .entry(name.to_string())
            .or_insert_with(|| OperationTimer {
                name: name.to_string(),
                start_time: Instant::now(),
                samples: VecDeque::new(),
                max_samples: 1000,
                total_operations: 0,
                slow_operations: 0,
            });

        timer.total_operations += 1;

        // Check for slow operation
        if duration > self.alert_thresholds.slow_operation_threshold {
            timer.slow_operations += 1;
            tracing::warn!(
                operation = name,
                duration_ms = duration.as_millis(),
                "Slow operation detected"
            );
        }

        // Add sample, maintaining max samples
        timer.samples.push_back(duration);
        if timer.samples.len() > timer.max_samples {
            timer.samples.pop_front();
        }

        // Track overhead of ending the operation
        self.overhead_tracker
            .record_overhead("end_operation", overhead_start.elapsed());
    }

    pub fn get_operation_stats(&self, name: &str) -> Option<OperationStats> {
        let timers = self.operation_timers.read();
        if let Some(timer) = timers.get(name) {
            let samples: Vec<Duration> = timer.samples.iter().copied().collect();
            Some(OperationStats::from_samples(
                name,
                &samples,
                timer.total_operations,
                timer.slow_operations,
            ))
        } else {
            None
        }
    }

    pub fn get_all_operation_stats(&self) -> HashMap<String, OperationStats> {
        let timers = self.operation_timers.read();
        let mut stats = HashMap::new();

        for (name, timer) in timers.iter() {
            let samples: Vec<Duration> = timer.samples.iter().copied().collect();
            stats.insert(
                name.clone(),
                OperationStats::from_samples(
                    name,
                    &samples,
                    timer.total_operations,
                    timer.slow_operations,
                ),
            );
        }

        stats
    }

    pub fn get_resource_usage(&self) -> ResourceUsage {
        self.resource_tracker.get_current_usage()
    }

    pub fn get_overhead_stats(&self) -> OverheadStats {
        self.overhead_tracker.get_stats()
    }

    pub fn check_alerts(&self) -> Vec<PerformanceAlert> {
        let mut alerts = Vec::new();

        // Check operation performance
        let stats = self.get_all_operation_stats();
        for (name, stat) in stats {
            if stat.slow_operation_rate > 0.1 {
                // More than 10% slow operations
                alerts.push(PerformanceAlert {
                    alert_type: AlertType::SlowOperations,
                    message: format!(
                        "Operation '{}' has high slow operation rate: {:.2}%",
                        name,
                        stat.slow_operation_rate * 100.0
                    ),
                    severity: AlertSeverity::Warning,
                    timestamp: std::time::SystemTime::now(),
                });
            }
        }

        // Check resource usage
        let resource_usage = self.get_resource_usage();
        if resource_usage.memory_bytes > self.alert_thresholds.memory_usage_threshold {
            alerts.push(PerformanceAlert {
                alert_type: AlertType::HighMemoryUsage,
                message: format!(
                    "High memory usage: {} MB",
                    resource_usage.memory_bytes / 1024 / 1024
                ),
                severity: AlertSeverity::Critical,
                timestamp: std::time::SystemTime::now(),
            });
        }

        alerts
    }

    pub fn reset_stats(&self) {
        self.operation_timers.write().clear();
        self.resource_tracker.reset();
        self.overhead_tracker.reset();
    }
}

impl Clone for PerformanceMonitor {
    fn clone(&self) -> Self {
        Self {
            operation_timers: self.operation_timers.clone(),
            resource_tracker: self.resource_tracker.clone(),
            overhead_tracker: self.overhead_tracker.clone(),
            alert_thresholds: self.alert_thresholds.clone(),
            enabled: self.enabled,
        }
    }
}

pub struct PerformanceToken {
    operation_name: String,
    start_time: Instant,
    monitor: std::sync::Weak<PerformanceMonitor>,
}

impl Drop for PerformanceToken {
    fn drop(&mut self) {
        if let Some(monitor) = self.monitor.upgrade() {
            monitor.end_operation(&self.operation_name, self.start_time);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStats {
    pub name: String,
    pub total_operations: u64,
    pub slow_operations: u64,
    pub slow_operation_rate: f64,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub avg_duration: Duration,
    pub p50_duration: Duration,
    pub p95_duration: Duration,
    pub p99_duration: Duration,
    pub operations_per_second: f64,
}

impl OperationStats {
    fn from_samples(name: &str, samples: &[Duration], total_ops: u64, slow_ops: u64) -> Self {
        if samples.is_empty() {
            return Self {
                name: name.to_string(),
                total_operations: total_ops,
                slow_operations: slow_ops,
                slow_operation_rate: if total_ops > 0 {
                    slow_ops as f64 / total_ops as f64
                } else {
                    0.0
                },
                min_duration: Duration::ZERO,
                max_duration: Duration::ZERO,
                avg_duration: Duration::ZERO,
                p50_duration: Duration::ZERO,
                p95_duration: Duration::ZERO,
                p99_duration: Duration::ZERO,
                operations_per_second: 0.0,
            };
        }

        let mut sorted_samples = samples.to_vec();
        sorted_samples.sort();

        let min_duration = *sorted_samples.first().unwrap();
        let max_duration = *sorted_samples.last().unwrap();

        let total_micros: u64 = samples.iter().map(|d| d.as_micros() as u64).sum();
        let avg_duration = Duration::from_micros(total_micros / samples.len() as u64);

        let p50_index = samples.len() * 50 / 100;
        let p95_index = samples.len() * 95 / 100;
        let p99_index = samples.len() * 99 / 100;

        let p50_duration = sorted_samples[p50_index.min(sorted_samples.len() - 1)];
        let p95_duration = sorted_samples[p95_index.min(sorted_samples.len() - 1)];
        let p99_duration = sorted_samples[p99_index.min(sorted_samples.len() - 1)];

        // Rough calculation of ops per second based on average duration
        let operations_per_second = if avg_duration > Duration::ZERO {
            1.0 / avg_duration.as_secs_f64()
        } else {
            0.0
        };

        Self {
            name: name.to_string(),
            total_operations: total_ops,
            slow_operations: slow_ops,
            slow_operation_rate: if total_ops > 0 {
                slow_ops as f64 / total_ops as f64
            } else {
                0.0
            },
            min_duration,
            max_duration,
            avg_duration,
            p50_duration,
            p95_duration,
            p99_duration,
            operations_per_second,
        }
    }
}

pub struct ResourceTracker {
    memory_samples: Arc<Mutex<VecDeque<u64>>>,
    cpu_samples: Arc<Mutex<VecDeque<f64>>>,
    max_samples: usize,
    last_update: Arc<Mutex<Instant>>,
}

impl ResourceTracker {
    fn new() -> Self {
        Self {
            memory_samples: Arc::new(Mutex::new(VecDeque::new())),
            cpu_samples: Arc::new(Mutex::new(VecDeque::new())),
            max_samples: 100,
            last_update: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn get_current_usage(&self) -> ResourceUsage {
        self.update_samples();

        let memory_samples = self.memory_samples.lock();
        let cpu_samples = self.cpu_samples.lock();

        let current_memory = memory_samples.back().copied().unwrap_or(0);
        let avg_memory = if !memory_samples.is_empty() {
            memory_samples.iter().sum::<u64>() / memory_samples.len() as u64
        } else {
            0
        };

        let current_cpu = cpu_samples.back().copied().unwrap_or(0.0);
        let avg_cpu = if !cpu_samples.is_empty() {
            cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64
        } else {
            0.0
        };

        ResourceUsage {
            memory_bytes: current_memory,
            avg_memory_bytes: avg_memory,
            cpu_percent: current_cpu,
            avg_cpu_percent: avg_cpu,
            timestamp: std::time::SystemTime::now(),
        }
    }

    fn update_samples(&self) {
        let mut last_update = self.last_update.lock();
        let now = Instant::now();

        // Only update every second to avoid overhead
        if now.duration_since(*last_update) < Duration::from_secs(1) {
            return;
        }

        *last_update = now;

        // Get memory usage
        let memory_usage = self.get_memory_usage();
        let mut memory_samples = self.memory_samples.lock();
        memory_samples.push_back(memory_usage);
        if memory_samples.len() > self.max_samples {
            memory_samples.pop_front();
        }

        // Get CPU usage (simplified)
        let cpu_usage = self.get_cpu_usage();
        let mut cpu_samples = self.cpu_samples.lock();
        cpu_samples.push_back(cpu_usage);
        if cpu_samples.len() > self.max_samples {
            cpu_samples.pop_front();
        }
    }

    #[cfg(target_os = "linux")]
    fn get_memory_usage(&self) -> u64 {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(rss) = line.split_whitespace().nth(1) {
                        if let Ok(rss_kb) = rss.parse::<u64>() {
                            return rss_kb * 1024; // Convert to bytes
                        }
                    }
                }
            }
        }
        0
    }

    #[cfg(not(target_os = "linux"))]
    fn get_memory_usage(&self) -> u64 {
        // Fallback implementation
        0
    }

    fn get_cpu_usage(&self) -> f64 {
        // Simplified CPU usage calculation
        // In a real implementation, you'd use system APIs
        0.0
    }

    fn reset(&self) {
        self.memory_samples.lock().clear();
        self.cpu_samples.lock().clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub memory_bytes: u64,
    pub avg_memory_bytes: u64,
    pub cpu_percent: f64,
    pub avg_cpu_percent: f64,
    pub timestamp: std::time::SystemTime,
}

pub struct OverheadTracker {
    overhead_samples: Arc<Mutex<HashMap<String, VecDeque<Duration>>>>,
    max_samples: usize,
}

impl OverheadTracker {
    fn new() -> Self {
        Self {
            overhead_samples: Arc::new(Mutex::new(HashMap::new())),
            max_samples: 1000,
        }
    }

    fn record_overhead(&self, operation: &str, overhead: Duration) {
        let mut samples = self.overhead_samples.lock();
        let entry = samples
            .entry(operation.to_string())
            .or_insert_with(VecDeque::new);

        entry.push_back(overhead);
        if entry.len() > self.max_samples {
            entry.pop_front();
        }
    }

    fn get_stats(&self) -> OverheadStats {
        let samples = self.overhead_samples.lock();
        let mut stats = HashMap::new();

        for (operation, durations) in samples.iter() {
            if !durations.is_empty() {
                let total_micros: u64 = durations.iter().map(|d| d.as_micros() as u64).sum();
                let avg_overhead = Duration::from_micros(total_micros / durations.len() as u64);
                let max_overhead = *durations.iter().max().unwrap();

                stats.insert(
                    operation.clone(),
                    OperationOverhead {
                        operation: operation.clone(),
                        sample_count: durations.len(),
                        avg_overhead,
                        max_overhead,
                    },
                );
            }
        }

        OverheadStats { operations: stats }
    }

    fn reset(&self) {
        self.overhead_samples.lock().clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverheadStats {
    pub operations: HashMap<String, OperationOverhead>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationOverhead {
    pub operation: String,
    pub sample_count: usize,
    pub avg_overhead: Duration,
    pub max_overhead: Duration,
}

#[derive(Debug, Clone)]
pub struct PerformanceAlert {
    pub alert_type: AlertType,
    pub message: String,
    pub severity: AlertSeverity,
    pub timestamp: std::time::SystemTime,
}

#[derive(Debug, Clone)]
pub enum AlertType {
    SlowOperations,
    HighMemoryUsage,
    HighCpuUsage,
    HighErrorRate,
    HighCacheMissRate,
    HighOverhead,
}

#[derive(Debug, Clone)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

// Performance monitoring macros
#[macro_export]
macro_rules! perf_monitor {
    ($monitor:expr, $operation:expr) => {
        $monitor.start_operation($operation)
    };
}

#[macro_export]
macro_rules! with_performance_monitoring {
    ($monitor:expr, $operation:expr, $code:block) => {{
        let _token = $monitor.start_operation($operation);
        $code
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_performance_monitor() {
        let monitor = PerformanceMonitor::new(true, None);

        // Simulate some operations
        {
            let _token = monitor.start_operation("test_op");
            thread::sleep(Duration::from_millis(10));
        }

        {
            let _token = monitor.start_operation("test_op");
            thread::sleep(Duration::from_millis(20));
        }

        let stats = monitor.get_operation_stats("test_op").unwrap();
        assert_eq!(stats.total_operations, 2);
        assert!(stats.avg_duration >= Duration::from_millis(10));
    }

    #[test]
    fn test_operation_stats() {
        let samples = vec![
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(30),
            Duration::from_millis(40),
            Duration::from_millis(50),
        ];

        let stats = OperationStats::from_samples("test", &samples, 5, 0);

        assert_eq!(stats.name, "test");
        assert_eq!(stats.total_operations, 5);
        assert_eq!(stats.min_duration, Duration::from_millis(10));
        assert_eq!(stats.max_duration, Duration::from_millis(50));
        assert_eq!(stats.avg_duration, Duration::from_millis(30));
    }

    #[test]
    fn test_overhead_tracker() {
        let tracker = OverheadTracker::new();

        tracker.record_overhead("test_op", Duration::from_micros(100));
        tracker.record_overhead("test_op", Duration::from_micros(200));

        let stats = tracker.get_stats();
        let op_stats = stats.operations.get("test_op").unwrap();

        assert_eq!(op_stats.sample_count, 2);
        assert_eq!(op_stats.avg_overhead, Duration::from_micros(150));
        assert_eq!(op_stats.max_overhead, Duration::from_micros(200));
    }
}
