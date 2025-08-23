use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSnapshot {
    pub timestamp: u64,
    pub rss_bytes: u64,
    pub vms_bytes: u64,
    pub heap_bytes: u64,
    pub cpu_usage_percent: f64,
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    pub page_faults: u64,
    pub context_switches: u64,
    pub open_file_descriptors: u64,
}

impl Default for PerformanceSnapshot {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            rss_bytes: 0,
            vms_bytes: 0,
            heap_bytes: 0,
            cpu_usage_percent: 0.0,
            io_read_bytes: 0,
            io_write_bytes: 0,
            page_faults: 0,
            context_switches: 0,
            open_file_descriptors: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkMetrics {
    pub benchmark_name: String,
    pub timestamp: u64,
    pub thread_count: usize,
    pub value_size: usize,
    pub dataset_size: usize,
    pub cache_size: u64,
    pub compression_enabled: bool,
    
    // Performance metrics
    pub operations_completed: u64,
    pub duration: Duration,
    pub ops_per_sec: f64,
    pub avg_latency: Duration,
    pub p50_latency: Duration,
    pub p95_latency: Duration,
    pub p99_latency: Duration,
    
    // Resource usage
    pub memory_before: PerformanceSnapshot,
    pub memory_after: PerformanceSnapshot,
    pub peak_memory_usage: u64,
    pub cpu_usage_percent: f64,
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    pub cache_hit_rate: f64,
    
    // Error tracking
    pub error_count: u64,
    pub timeout_count: u64,
    
    // Regression analysis
    pub regression_detected: bool,
    pub regression_percent: f64,
    pub statistical_significance: f64,
}

impl Default for BenchmarkMetrics {
    fn default() -> Self {
        Self {
            benchmark_name: String::new(),
            timestamp: 0,
            thread_count: 1,
            value_size: 1024,
            dataset_size: 10_000,
            cache_size: 64 * 1024 * 1024,
            compression_enabled: false,
            operations_completed: 0,
            duration: Duration::default(),
            ops_per_sec: 0.0,
            avg_latency: Duration::default(),
            p50_latency: Duration::default(),
            p95_latency: Duration::default(),
            p99_latency: Duration::default(),
            memory_before: PerformanceSnapshot::default(),
            memory_after: PerformanceSnapshot::default(),
            peak_memory_usage: 0,
            cpu_usage_percent: 0.0,
            io_read_bytes: 0,
            io_write_bytes: 0,
            cache_hit_rate: 0.0,
            error_count: 0,
            timeout_count: 0,
            regression_detected: false,
            regression_percent: 0.0,
            statistical_significance: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyHistogram {
    pub buckets: Vec<(u64, u64)>, // (latency_ns, count)
    pub total_samples: u64,
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub sum_latency: Duration,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            buckets: Vec::new(),
            total_samples: 0,
            min_latency: Duration::from_nanos(u64::MAX),
            max_latency: Duration::from_nanos(0),
            sum_latency: Duration::from_nanos(0),
        }
    }

    pub fn record(&mut self, latency: Duration) {
        self.total_samples += 1;
        self.sum_latency += latency;
        
        if latency < self.min_latency {
            self.min_latency = latency;
        }
        if latency > self.max_latency {
            self.max_latency = latency;
        }

        // Simple bucketing - could be improved with more sophisticated histograms
        let latency_ns = latency.as_nanos() as u64;
        let bucket_index = self.find_bucket_index(latency_ns);
        
        if bucket_index < self.buckets.len() {
            self.buckets[bucket_index].1 += 1;
        } else {
            self.buckets.push((latency_ns, 1));
            self.buckets.sort_by_key(|&(ns, _)| ns);
        }
    }

    pub fn percentile(&self, p: f64) -> Duration {
        if self.total_samples == 0 {
            return Duration::from_nanos(0);
        }

        let target_sample = ((self.total_samples as f64) * (p / 100.0)) as u64;
        let mut current_count = 0;

        for &(latency_ns, count) in &self.buckets {
            current_count += count;
            if current_count >= target_sample {
                return Duration::from_nanos(latency_ns);
            }
        }

        self.max_latency
    }

    fn find_bucket_index(&self, latency_ns: u64) -> usize {
        self.buckets
            .binary_search_by_key(&latency_ns, |&(ns, _)| ns)
            .unwrap_or_else(|i| i)
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MetricsCollector {
    track_memory: bool,
    track_cpu: bool,
    track_io: bool,
    latency_histogram: LatencyHistogram,
}

impl MetricsCollector {
    pub fn new(track_memory: bool, track_cpu: bool, track_io: bool) -> Self {
        Self {
            track_memory,
            track_cpu,
            track_io,
            latency_histogram: LatencyHistogram::new(),
        }
    }

    pub fn take_snapshot(&self) -> Result<PerformanceSnapshot, Box<dyn std::error::Error>> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let mut snapshot = PerformanceSnapshot {
            timestamp,
            ..Default::default()
        };

        if self.track_memory {
            if let Ok(memory) = self.get_memory_usage() {
                snapshot.rss_bytes = memory.0;
                snapshot.vms_bytes = memory.1;
                snapshot.heap_bytes = memory.2;
            }
        }

        if self.track_cpu {
            if let Ok(cpu_usage) = self.get_cpu_usage() {
                snapshot.cpu_usage_percent = cpu_usage;
            }
        }

        if self.track_io {
            if let Ok(io_stats) = self.get_io_stats() {
                snapshot.io_read_bytes = io_stats.0;
                snapshot.io_write_bytes = io_stats.1;
            }
        }

        // System-specific metrics
        #[cfg(target_os = "linux")]
        {
            if let Ok(sys_stats) = self.get_linux_system_stats() {
                snapshot.page_faults = sys_stats.0;
                snapshot.context_switches = sys_stats.1;
                snapshot.open_file_descriptors = sys_stats.2;
            }
        }

        #[cfg(target_os = "macos")]
        {
            if let Ok(sys_stats) = self.get_macos_system_stats() {
                snapshot.page_faults = sys_stats.0;
                snapshot.context_switches = sys_stats.1;
                snapshot.open_file_descriptors = sys_stats.2;
            }
        }

        Ok(snapshot)
    }

    fn get_memory_usage(&self) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        #[cfg(target_os = "linux")]
        {
            self.get_linux_memory_usage()
        }
        #[cfg(target_os = "macos")]
        {
            self.get_macos_memory_usage()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Ok((0, 0, 0))
        }
    }

    #[cfg(target_os = "linux")]
    fn get_linux_memory_usage(&self) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        let mut file = File::open("/proc/self/status")?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let mut rss_kb = 0u64;
        let mut vms_kb = 0u64;

        for line in contents.lines() {
            if line.starts_with("VmRSS:") {
                if let Some(value_str) = line.split_whitespace().nth(1) {
                    rss_kb = value_str.parse().unwrap_or(0);
                }
            } else if line.starts_with("VmSize:") {
                if let Some(value_str) = line.split_whitespace().nth(1) {
                    vms_kb = value_str.parse().unwrap_or(0);
                }
            }
        }

        // Approximate heap usage - this is a rough estimate
        let heap_bytes = self.estimate_heap_usage();

        Ok((rss_kb * 1024, vms_kb * 1024, heap_bytes))
    }

    #[cfg(target_os = "macos")]
    fn get_macos_memory_usage(&self) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        use std::process::Command;
        
        let output = Command::new("ps")
            .args(&["-o", "rss,vsz", "-p", &std::process::id().to_string()])
            .output()?;

        let output_str = String::from_utf8_lossy(&output.stdout);
        let lines: Vec<&str> = output_str.lines().collect();
        
        if lines.len() >= 2 {
            let values: Vec<&str> = lines[1].split_whitespace().collect();
            if values.len() >= 2 {
                let rss_kb: u64 = values[0].parse().unwrap_or(0);
                let vms_kb: u64 = values[1].parse().unwrap_or(0);
                let heap_bytes = self.estimate_heap_usage();
                
                return Ok((rss_kb * 1024, vms_kb * 1024, heap_bytes));
            }
        }
        
        Ok((0, 0, 0))
    }

    fn estimate_heap_usage(&self) -> u64 {
        // This is a rough approximation - in a real implementation,
        // you might want to use jemalloc stats or similar
        #[cfg(not(target_env = "msvc"))]
        {
            // If using jemalloc, we could get more precise stats
            0
        }
        #[cfg(target_env = "msvc")]
        {
            0
        }
    }

    fn get_cpu_usage(&self) -> Result<f64, Box<dyn std::error::Error>> {
        #[cfg(target_os = "linux")]
        {
            self.get_linux_cpu_usage()
        }
        #[cfg(target_os = "macos")]
        {
            self.get_macos_cpu_usage()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Ok(0.0)
        }
    }

    #[cfg(target_os = "linux")]
    fn get_linux_cpu_usage(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let mut file = File::open("/proc/self/stat")?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let fields: Vec<&str> = contents.split_whitespace().collect();
        if fields.len() > 15 {
            let utime: u64 = fields[13].parse().unwrap_or(0);
            let stime: u64 = fields[14].parse().unwrap_or(0);
            let total_time = utime + stime;
            
            // This is a simplified calculation - real CPU usage would require
            // tracking over time and comparing with system stats
            Ok(total_time as f64 / 100.0) // Rough approximation
        } else {
            Ok(0.0)
        }
    }

    #[cfg(target_os = "macos")]
    fn get_macos_cpu_usage(&self) -> Result<f64, Box<dyn std::error::Error>> {
        use std::process::Command;
        
        let output = Command::new("ps")
            .args(&["-o", "pcpu", "-p", &std::process::id().to_string()])
            .output()?;

        let output_str = String::from_utf8_lossy(&output.stdout);
        let lines: Vec<&str> = output_str.lines().collect();
        
        if lines.len() >= 2 {
            if let Ok(cpu_percent) = lines[1].trim().parse::<f64>() {
                return Ok(cpu_percent);
            }
        }
        
        Ok(0.0)
    }

    fn get_io_stats(&self) -> Result<(u64, u64), Box<dyn std::error::Error>> {
        #[cfg(target_os = "linux")]
        {
            self.get_linux_io_stats()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok((0, 0))
        }
    }

    #[cfg(target_os = "linux")]
    fn get_linux_io_stats(&self) -> Result<(u64, u64), Box<dyn std::error::Error>> {
        let mut file = File::open("/proc/self/io")?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let mut read_bytes = 0u64;
        let mut write_bytes = 0u64;

        for line in contents.lines() {
            if line.starts_with("read_bytes:") {
                if let Some(value_str) = line.split_whitespace().nth(1) {
                    read_bytes = value_str.parse().unwrap_or(0);
                }
            } else if line.starts_with("write_bytes:") {
                if let Some(value_str) = line.split_whitespace().nth(1) {
                    write_bytes = value_str.parse().unwrap_or(0);
                }
            }
        }

        Ok((read_bytes, write_bytes))
    }

    #[cfg(target_os = "linux")]
    fn get_linux_system_stats(&self) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        let mut page_faults = 0u64;
        let mut context_switches = 0u64;
        let mut open_fds = 0u64;

        // Get page faults from /proc/self/stat
        if let Ok(mut file) = File::open("/proc/self/stat") {
            let mut contents = String::new();
            if file.read_to_string(&mut contents).is_ok() {
                let fields: Vec<&str> = contents.split_whitespace().collect();
                if fields.len() > 11 {
                    page_faults = fields[9].parse().unwrap_or(0) + fields[11].parse().unwrap_or(0);
                }
            }
        }

        // Count open file descriptors
        if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
            open_fds = entries.count() as u64;
        }

        // Context switches are harder to get per-process in real-time
        // This is a placeholder
        context_switches = 0;

        Ok((page_faults, context_switches, open_fds))
    }

    #[cfg(target_os = "macos")]
    fn get_macos_system_stats(&self) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        // MacOS system stats would require more complex system calls
        // This is a simplified placeholder
        Ok((0, 0, 0))
    }

    pub fn record_latency(&mut self, latency: Duration) {
        self.latency_histogram.record(latency);
    }

    pub fn get_latency_percentiles(&self) -> (Duration, Duration, Duration) {
        (
            self.latency_histogram.percentile(50.0),
            self.latency_histogram.percentile(95.0),
            self.latency_histogram.percentile(99.0),
        )
    }

    pub fn reset(&mut self) {
        self.latency_histogram = LatencyHistogram::new();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMetrics {
    pub benchmark_name: String,
    pub total_runs: usize,
    pub mean_ops_per_sec: f64,
    pub median_ops_per_sec: f64,
    pub std_dev_ops_per_sec: f64,
    pub min_ops_per_sec: f64,
    pub max_ops_per_sec: f64,
    pub mean_latency: Duration,
    pub latency_distribution: LatencyHistogram,
    pub memory_usage_trend: Vec<u64>,
    pub regression_trend: Vec<f64>,
}

impl AggregatedMetrics {
    pub fn from_metrics(metrics: &[BenchmarkMetrics]) -> Self {
        if metrics.is_empty() {
            return Self::default();
        }

        let benchmark_name = metrics[0].benchmark_name.clone();
        let total_runs = metrics.len();

        let ops_per_sec_values: Vec<f64> = metrics.iter().map(|m| m.ops_per_sec).collect();
        let mean_ops_per_sec = ops_per_sec_values.iter().sum::<f64>() / total_runs as f64;
        
        let mut sorted_ops = ops_per_sec_values.clone();
        sorted_ops.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_ops_per_sec = if sorted_ops.len() % 2 == 0 {
            (sorted_ops[sorted_ops.len() / 2 - 1] + sorted_ops[sorted_ops.len() / 2]) / 2.0
        } else {
            sorted_ops[sorted_ops.len() / 2]
        };

        let variance = ops_per_sec_values
            .iter()
            .map(|x| (x - mean_ops_per_sec).powi(2))
            .sum::<f64>() / total_runs as f64;
        let std_dev_ops_per_sec = variance.sqrt();

        let min_ops_per_sec = sorted_ops[0];
        let max_ops_per_sec = sorted_ops[sorted_ops.len() - 1];

        let mean_latency_ns: u64 = metrics
            .iter()
            .map(|m| m.avg_latency.as_nanos() as u64)
            .sum::<u64>() / total_runs as u64;
        let mean_latency = Duration::from_nanos(mean_latency_ns);

        let memory_usage_trend = metrics
            .iter()
            .map(|m| m.memory_after.rss_bytes)
            .collect();

        let regression_trend = metrics
            .iter()
            .map(|m| m.regression_percent)
            .collect();

        Self {
            benchmark_name,
            total_runs,
            mean_ops_per_sec,
            median_ops_per_sec,
            std_dev_ops_per_sec,
            min_ops_per_sec,
            max_ops_per_sec,
            mean_latency,
            latency_distribution: LatencyHistogram::new(), // TODO: Aggregate from individual metrics
            memory_usage_trend,
            regression_trend,
        }
    }
}

impl Default for AggregatedMetrics {
    fn default() -> Self {
        Self {
            benchmark_name: String::new(),
            total_runs: 0,
            mean_ops_per_sec: 0.0,
            median_ops_per_sec: 0.0,
            std_dev_ops_per_sec: 0.0,
            min_ops_per_sec: 0.0,
            max_ops_per_sec: 0.0,
            mean_latency: Duration::default(),
            latency_distribution: LatencyHistogram::new(),
            memory_usage_trend: Vec::new(),
            regression_trend: Vec::new(),
        }
    }
}