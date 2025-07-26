//! Metrics Collection Module
//!
//! Collects, aggregates, and analyzes performance metrics from all profilers
//! to provide comprehensive performance insights for Lightning DB.

use std::sync::{Arc, Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn};

/// Metrics collector that aggregates data from all profilers
pub struct MetricsCollector {
    metrics: Arc<RwLock<MetricsData>>,
    time_series: Arc<Mutex<HashMap<String, VecDeque<MetricPoint>>>>,
    _collection_thread: Option<thread::JoinHandle<()>>,
    active: Arc<std::sync::atomic::AtomicBool>,
    start_time: Arc<Mutex<Option<Instant>>>,
}

/// Aggregated metrics data
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MetricsData {
    pub cpu_metrics: CpuMetrics,
    pub memory_metrics: MemoryMetrics,
    pub io_metrics: IoMetrics,
    pub system_metrics: SystemMetrics,
    pub performance_score: f64,
    pub health_status: HealthStatus,
    pub last_updated: u64,
}

/// CPU performance metrics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CpuMetrics {
    pub total_samples: u64,
    pub avg_cpu_usage: f64,
    pub peak_cpu_usage: f64,
    pub cpu_efficiency: f64,
    pub hot_functions_count: u64,
    pub profiling_overhead: f64,
}

/// Memory performance metrics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MemoryMetrics {
    pub current_usage_bytes: u64,
    pub peak_usage_bytes: u64,
    pub allocation_rate_per_sec: f64,
    pub deallocation_rate_per_sec: f64,
    pub memory_efficiency: f64,
    pub leak_potential_score: f64,
    pub fragmentation_level: f64,
}

/// I/O performance metrics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct IoMetrics {
    pub total_operations: u64,
    pub bytes_read_per_sec: f64,
    pub bytes_written_per_sec: f64,
    pub avg_read_latency_ms: f64,
    pub avg_write_latency_ms: f64,
    pub io_efficiency: f64,
    pub error_rate: f64,
    pub bottlenecks_count: u64,
}

/// System-level metrics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub uptime_seconds: u64,
    pub database_size_bytes: u64,
    pub cache_hit_rate: f64,
    pub transaction_throughput: f64,
    pub concurrent_connections: u64,
    pub system_load: f64,
}

/// Overall health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Excellent,
    Good,
    Fair,
    Poor,
    Critical,
}

impl Default for HealthStatus {
    fn default() -> Self {
        HealthStatus::Good
    }
}

/// Time series data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    pub timestamp: u64,
    pub value: f64,
}

/// Current metrics snapshot
#[derive(Debug, Clone, Serialize)]
pub struct CurrentMetrics {
    pub metrics: MetricsData,
    pub trends: HashMap<String, MetricTrend>,
    pub alerts: Vec<MetricAlert>,
}

/// Metric trend analysis
#[derive(Debug, Clone, Serialize)]
pub struct MetricTrend {
    pub metric_name: String,
    pub direction: TrendDirection,
    pub rate_of_change: f64,
    pub significance: TrendSignificance,
}

/// Trend direction
#[derive(Debug, Clone, Serialize)]
pub enum TrendDirection {
    Increasing,
    Decreasing,
    Stable,
    Volatile,
}

/// Trend significance
#[derive(Debug, Clone, Serialize)]
pub enum TrendSignificance {
    High,
    Medium,
    Low,
    None,
}

/// Metric alert
#[derive(Debug, Clone, Serialize)]
pub struct MetricAlert {
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub message: String,
    pub metric_name: String,
    pub current_value: f64,
    pub threshold: f64,
    pub timestamp: u64,
}

/// Alert types
#[derive(Debug, Clone, Serialize)]
pub enum AlertType {
    Threshold,
    Anomaly,
    Trend,
    SystemHealth,
}

/// Alert severity levels
#[derive(Debug, Clone, Serialize)]
pub enum AlertSeverity {
    Critical,
    High,
    Medium,
    Low,
    Info,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(MetricsData::default())),
            time_series: Arc::new(Mutex::new(HashMap::new())),
            _collection_thread: None,
            active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            start_time: Arc::new(Mutex::new(None)),
        }
    }

    /// Start metrics collection
    pub fn start(&self) {
        if self.active.load(std::sync::atomic::Ordering::Relaxed) {
            warn!("Metrics collector is already running");
            return;
        }

        info!("Starting metrics collector");
        self.active.store(true, std::sync::atomic::Ordering::Relaxed);
        
        // Record start time
        {
            let mut start_time = self.start_time.lock().unwrap();
            *start_time = Some(Instant::now());
        }

        let metrics = self.metrics.clone();
        let time_series = self.time_series.clone();
        let active = self.active.clone();
        let start_time = self.start_time.clone();

        let handle = thread::Builder::new()
            .name("metrics-collector".to_string())
            .spawn(move || {
                Self::collection_loop(metrics, time_series, active, start_time);
            })
            .expect("Failed to start metrics collection thread");

        // Store handle for potential cleanup
        // In a full implementation, we'd store this properly
        handle.join().ok();
    }

    /// Stop metrics collection
    pub fn stop(&self) {
        info!("Stopping metrics collector");
        self.active.store(false, std::sync::atomic::Ordering::Relaxed);
        
        // Wait for collection thread to finish
        thread::sleep(Duration::from_millis(100));
    }

    /// Get current metrics summary
    pub fn get_summary(&self) -> super::MetricsSummary {
        let metrics = self.metrics.read().unwrap();
        
        super::MetricsSummary {
            total_samples: metrics.cpu_metrics.total_samples + metrics.memory_metrics.current_usage_bytes / 1024, // Approximation
            cpu_samples: metrics.cpu_metrics.total_samples,
            memory_samples: metrics.memory_metrics.current_usage_bytes / 1024, // Approximation
            io_operations: metrics.io_metrics.total_operations,
            peak_memory_usage: metrics.memory_metrics.peak_usage_bytes,
            total_bytes_read: (metrics.io_metrics.bytes_read_per_sec * 60.0) as u64, // Approximation for 1 minute
            total_bytes_written: (metrics.io_metrics.bytes_written_per_sec * 60.0) as u64, // Approximation for 1 minute
            hot_functions: Vec::new(), // Would be populated from actual profiler data
            performance_score: metrics.performance_score,
        }
    }

    /// Get current metrics with trends and alerts
    pub fn get_current_metrics(&self) -> CurrentMetrics {
        let metrics = self.metrics.read().unwrap().clone();
        let trends = self.analyze_trends();
        let alerts = self.generate_alerts(&metrics, &trends);

        CurrentMetrics {
            metrics,
            trends,
            alerts,
        }
    }

    /// Main collection loop
    fn collection_loop(
        metrics: Arc<RwLock<MetricsData>>,
        time_series: Arc<Mutex<HashMap<String, VecDeque<MetricPoint>>>>,
        active: Arc<std::sync::atomic::AtomicBool>,
        start_time: Arc<Mutex<Option<Instant>>>,
    ) {
        debug!("Metrics collection loop started");
        
        while active.load(std::sync::atomic::Ordering::Relaxed) {
            let collection_start = Instant::now();
            
            // Collect metrics from various sources
            let new_metrics = Self::collect_system_metrics(&start_time);
            
            // Update metrics
            {
                let mut metrics_guard = metrics.write().unwrap();
                *metrics_guard = new_metrics.clone();
                metrics_guard.last_updated = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
            }
            
            // Update time series data
            Self::update_time_series(&time_series, &new_metrics);
            
            // Sleep for collection interval (1 second)
            let collection_duration = collection_start.elapsed();
            let target_interval = Duration::from_secs(1);
            if collection_duration < target_interval {
                thread::sleep(target_interval - collection_duration);
            }
        }
        
        debug!("Metrics collection loop ended");
    }

    /// Collect system metrics (simulated)
    fn collect_system_metrics(start_time: &Arc<Mutex<Option<Instant>>>) -> MetricsData {
        let uptime = {
            let start = start_time.lock().unwrap();
            start.map(|s| s.elapsed().as_secs()).unwrap_or(0)
        };

        // Simulate realistic database performance metrics
        let cpu_usage = Self::simulate_cpu_usage();
        let memory_usage = Self::simulate_memory_usage();
        let io_metrics = Self::simulate_io_metrics();
        
        let performance_score = Self::calculate_performance_score(&cpu_usage, &memory_usage, &io_metrics);
        let health_status = Self::determine_health_status(performance_score);

        MetricsData {
            cpu_metrics: cpu_usage,
            memory_metrics: memory_usage,
            io_metrics,
            system_metrics: SystemMetrics {
                uptime_seconds: uptime,
                database_size_bytes: Self::simulate_database_size(),
                cache_hit_rate: Self::simulate_cache_hit_rate(),
                transaction_throughput: Self::simulate_transaction_throughput(),
                concurrent_connections: Self::simulate_concurrent_connections(),
                system_load: Self::simulate_system_load(),
            },
            performance_score,
            health_status,
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Simulate CPU usage metrics
    fn simulate_cpu_usage() -> CpuMetrics {
        let base_usage = 15.0 + fastrand::f64() * 45.0; // 15-60% base usage
        let peak_usage = base_usage + fastrand::f64() * 30.0; // Up to 30% higher peaks
        
        CpuMetrics {
            total_samples: 1000 + fastrand::u64(0..9000),
            avg_cpu_usage: base_usage,
            peak_cpu_usage: peak_usage.min(100.0),
            cpu_efficiency: 85.0 + fastrand::f64() * 10.0, // 85-95% efficiency
            hot_functions_count: 5 + fastrand::u64(0..15),
            profiling_overhead: 0.5 + fastrand::f64() * 1.5, // 0.5-2% overhead
        }
    }

    /// Simulate memory usage metrics
    fn simulate_memory_usage() -> MemoryMetrics {
        let current_mb = 50.0 + fastrand::f64() * 200.0; // 50-250 MB current usage
        let peak_mb = current_mb + fastrand::f64() * 100.0; // Up to 100 MB higher peak
        
        MemoryMetrics {
            current_usage_bytes: (current_mb * 1024.0 * 1024.0) as u64,
            peak_usage_bytes: (peak_mb * 1024.0 * 1024.0) as u64,
            allocation_rate_per_sec: 1024.0 * 1024.0 * (1.0 + fastrand::f64() * 10.0), // 1-11 MB/s
            deallocation_rate_per_sec: 1024.0 * 1024.0 * (0.8 + fastrand::f64() * 9.0), // 0.8-9.8 MB/s
            memory_efficiency: 80.0 + fastrand::f64() * 15.0, // 80-95% efficiency
            leak_potential_score: fastrand::f64() * 10.0, // 0-10 score
            fragmentation_level: 5.0 + fastrand::f64() * 15.0, // 5-20% fragmentation
        }
    }

    /// Simulate I/O metrics
    fn simulate_io_metrics() -> IoMetrics {
        let read_throughput = 10.0 * 1024.0 * 1024.0 * (1.0 + fastrand::f64() * 4.0); // 10-50 MB/s
        let write_throughput = 5.0 * 1024.0 * 1024.0 * (1.0 + fastrand::f64() * 3.0); // 5-20 MB/s
        
        IoMetrics {
            total_operations: 1000 + fastrand::u64(0..9000),
            bytes_read_per_sec: read_throughput,
            bytes_written_per_sec: write_throughput,
            avg_read_latency_ms: 0.1 + fastrand::f64() * 4.9, // 0.1-5.0 ms
            avg_write_latency_ms: 0.5 + fastrand::f64() * 9.5, // 0.5-10.0 ms
            io_efficiency: 75.0 + fastrand::f64() * 20.0, // 75-95% efficiency
            error_rate: fastrand::f64() * 2.0, // 0-2% error rate
            bottlenecks_count: fastrand::u64(0..5),
        }
    }

    /// Simulate database size
    fn simulate_database_size() -> u64 {
        let base_size_gb = 1.0 + fastrand::f64() * 10.0; // 1-11 GB
        (base_size_gb * 1024.0 * 1024.0 * 1024.0) as u64
    }

    /// Simulate cache hit rate
    fn simulate_cache_hit_rate() -> f64 {
        85.0 + fastrand::f64() * 10.0 // 85-95% hit rate
    }

    /// Simulate transaction throughput
    fn simulate_transaction_throughput() -> f64 {
        100.0 + fastrand::f64() * 900.0 // 100-1000 transactions/sec
    }

    /// Simulate concurrent connections
    fn simulate_concurrent_connections() -> u64 {
        10 + fastrand::u64(0..90) // 10-100 connections
    }

    /// Simulate system load
    fn simulate_system_load() -> f64 {
        0.5 + fastrand::f64() * 2.0 // 0.5-2.5 load average
    }

    /// Calculate overall performance score
    fn calculate_performance_score(cpu: &CpuMetrics, memory: &MemoryMetrics, io: &IoMetrics) -> f64 {
        let cpu_score = (100.0 - cpu.avg_cpu_usage) * 0.3; // Lower CPU usage is better
        let memory_score = memory.memory_efficiency * 0.3;
        let io_score = io.io_efficiency * 0.4;
        
        (cpu_score + memory_score + io_score).max(0.0).min(100.0)
    }

    /// Determine health status based on performance score
    fn determine_health_status(score: f64) -> HealthStatus {
        match score {
            s if s >= 90.0 => HealthStatus::Excellent,
            s if s >= 75.0 => HealthStatus::Good,
            s if s >= 60.0 => HealthStatus::Fair,
            s if s >= 40.0 => HealthStatus::Poor,
            _ => HealthStatus::Critical,
        }
    }

    /// Update time series data
    fn update_time_series(
        time_series: &Arc<Mutex<HashMap<String, VecDeque<MetricPoint>>>>,
        metrics: &MetricsData,
    ) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut series = time_series.lock().unwrap();
        
        // Add data points for key metrics
        let metrics_to_track = vec![
            ("cpu_usage", metrics.cpu_metrics.avg_cpu_usage),
            ("memory_usage", metrics.memory_metrics.current_usage_bytes as f64),
            ("io_read_throughput", metrics.io_metrics.bytes_read_per_sec),
            ("io_write_throughput", metrics.io_metrics.bytes_written_per_sec),
            ("performance_score", metrics.performance_score),
            ("cache_hit_rate", metrics.system_metrics.cache_hit_rate),
        ];

        for (metric_name, value) in metrics_to_track {
            let points = series.entry(metric_name.to_string()).or_insert_with(VecDeque::new);
            
            points.push_back(MetricPoint { timestamp, value });
            
            // Keep only last 1000 points (about 16 minutes at 1-second intervals)
            if points.len() > 1000 {
                points.pop_front();
            }
        }
    }

    /// Analyze trends in metrics
    fn analyze_trends(&self) -> HashMap<String, MetricTrend> {
        let series = self.time_series.lock().unwrap();
        let mut trends = HashMap::new();

        for (metric_name, points) in series.iter() {
            if points.len() < 10 {
                continue; // Need at least 10 points for trend analysis
            }

            let trend = Self::calculate_trend(points);
            trends.insert(metric_name.clone(), MetricTrend {
                metric_name: metric_name.clone(),
                direction: trend.0,
                rate_of_change: trend.1,
                significance: trend.2,
            });
        }

        trends
    }

    /// Calculate trend for a metric
    fn calculate_trend(points: &VecDeque<MetricPoint>) -> (TrendDirection, f64, TrendSignificance) {
        if points.len() < 2 {
            return (TrendDirection::Stable, 0.0, TrendSignificance::None);
        }

        let recent_points: Vec<_> = points.iter().rev().take(30).collect(); // Last 30 points
        let values: Vec<f64> = recent_points.iter().map(|p| p.value).collect();
        
        // Simple linear regression for trend
        let n = values.len() as f64;
        let sum_x: f64 = (0..values.len()).map(|i| i as f64).sum();
        let sum_y: f64 = values.iter().sum();
        let sum_xy: f64 = values.iter().enumerate().map(|(i, &y)| i as f64 * y).sum();
        let sum_x2: f64 = (0..values.len()).map(|i| (i as f64).powi(2)).sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));
        let abs_slope = slope.abs();

        // Determine direction
        let direction = if abs_slope < 0.01 {
            TrendDirection::Stable
        } else if slope > 0.0 {
            TrendDirection::Increasing
        } else {
            TrendDirection::Decreasing
        };

        // Calculate volatility
        let mean = sum_y / n;
        let variance = values.iter().map(|&v| (v - mean).powi(2)).sum::<f64>() / n;
        let std_dev = variance.sqrt();
        let cv = if mean != 0.0 { std_dev / mean.abs() } else { 0.0 };

        let direction = if cv > 0.2 { TrendDirection::Volatile } else { direction };

        // Determine significance
        let significance = match abs_slope {
            s if s > 1.0 => TrendSignificance::High,
            s if s > 0.1 => TrendSignificance::Medium,
            s if s > 0.01 => TrendSignificance::Low,
            _ => TrendSignificance::None,
        };

        (direction, slope, significance)
    }

    /// Generate alerts based on metrics and trends
    fn generate_alerts(&self, metrics: &MetricsData, trends: &HashMap<String, MetricTrend>) -> Vec<MetricAlert> {
        let mut alerts = Vec::new();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // CPU usage alerts
        if metrics.cpu_metrics.avg_cpu_usage > 80.0 {
            alerts.push(MetricAlert {
                alert_type: AlertType::Threshold,
                severity: AlertSeverity::High,
                message: "High CPU usage detected".to_string(),
                metric_name: "cpu_usage".to_string(),
                current_value: metrics.cpu_metrics.avg_cpu_usage,
                threshold: 80.0,
                timestamp,
            });
        }

        // Memory usage alerts
        let memory_usage_mb = metrics.memory_metrics.current_usage_bytes as f64 / (1024.0 * 1024.0);
        if memory_usage_mb > 500.0 {
            alerts.push(MetricAlert {
                alert_type: AlertType::Threshold,
                severity: AlertSeverity::Medium,
                message: "High memory usage detected".to_string(),
                metric_name: "memory_usage".to_string(),
                current_value: memory_usage_mb,
                threshold: 500.0,
                timestamp,
            });
        }

        // I/O error rate alerts
        if metrics.io_metrics.error_rate > 1.0 {
            alerts.push(MetricAlert {
                alert_type: AlertType::Threshold,
                severity: AlertSeverity::High,
                message: "High I/O error rate detected".to_string(),
                metric_name: "io_error_rate".to_string(),
                current_value: metrics.io_metrics.error_rate,
                threshold: 1.0,
                timestamp,
            });
        }

        // Performance score alerts
        if metrics.performance_score < 50.0 {
            alerts.push(MetricAlert {
                alert_type: AlertType::SystemHealth,
                severity: AlertSeverity::Critical,
                message: "Low performance score detected".to_string(),
                metric_name: "performance_score".to_string(),
                current_value: metrics.performance_score,
                threshold: 50.0,
                timestamp,
            });
        }

        // Trend-based alerts
        for (metric_name, trend) in trends {
            if let TrendSignificance::High = trend.significance {
                match (&trend.direction, metric_name.as_str()) {
                    (TrendDirection::Increasing, "cpu_usage") |
                    (TrendDirection::Increasing, "memory_usage") |
                    (TrendDirection::Increasing, "io_error_rate") => {
                        alerts.push(MetricAlert {
                            alert_type: AlertType::Trend,
                            severity: AlertSeverity::Medium,
                            message: format!("Increasing trend detected in {}", metric_name),
                            metric_name: metric_name.clone(),
                            current_value: trend.rate_of_change,
                            threshold: 0.0,
                            timestamp,
                        });
                    }
                    (TrendDirection::Decreasing, "performance_score") |
                    (TrendDirection::Decreasing, "cache_hit_rate") => {
                        alerts.push(MetricAlert {
                            alert_type: AlertType::Trend,
                            severity: AlertSeverity::Medium,
                            message: format!("Decreasing trend detected in {}", metric_name),
                            metric_name: metric_name.clone(),
                            current_value: trend.rate_of_change,
                            threshold: 0.0,
                            timestamp,
                        });
                    }
                    _ => {}
                }
            }
        }

        alerts
    }
}

/// Fast random number generation for simulation
mod fastrand {
    use std::sync::atomic::{AtomicU64, Ordering};
    
    static STATE: AtomicU64 = AtomicU64::new(1);
    
    pub fn f64() -> f64 {
        let mut x = STATE.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        STATE.store(x, Ordering::Relaxed);
        
        (x as f64) / (u64::MAX as f64)
    }
    
    pub fn u64(range: std::ops::Range<u64>) -> u64 {
        let mut x = STATE.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        STATE.store(x, Ordering::Relaxed);
        
        range.start + (x % (range.end - range.start))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new();
        assert!(!collector.active.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_performance_score_calculation() {
        let cpu = CpuMetrics {
            avg_cpu_usage: 50.0,
            ..Default::default()
        };
        let memory = MemoryMetrics {
            memory_efficiency: 90.0,
            ..Default::default()
        };
        let io = IoMetrics {
            io_efficiency: 85.0,
            ..Default::default()
        };

        let score = MetricsCollector::calculate_performance_score(&cpu, &memory, &io);
        assert!(score > 0.0 && score <= 100.0);
    }

    #[test]
    fn test_health_status_determination() {
        assert!(matches!(MetricsCollector::determine_health_status(95.0), HealthStatus::Excellent));
        assert!(matches!(MetricsCollector::determine_health_status(80.0), HealthStatus::Good));
        assert!(matches!(MetricsCollector::determine_health_status(65.0), HealthStatus::Fair));
        assert!(matches!(MetricsCollector::determine_health_status(45.0), HealthStatus::Poor));
        assert!(matches!(MetricsCollector::determine_health_status(30.0), HealthStatus::Critical));
    }

    #[test]
    fn test_trend_calculation() {
        let mut points = VecDeque::new();
        
        // Add increasing values
        for i in 0..20 {
            points.push_back(MetricPoint {
                timestamp: i,
                value: i as f64,
            });
        }

        let (direction, slope, significance) = MetricsCollector::calculate_trend(&points);
        assert!(matches!(direction, TrendDirection::Increasing));
        assert!(slope > 0.0);
        assert!(matches!(significance, TrendSignificance::High));
    }

    #[test]
    fn test_metrics_simulation() {
        let cpu = MetricsCollector::simulate_cpu_usage();
        assert!(cpu.avg_cpu_usage >= 0.0 && cpu.avg_cpu_usage <= 100.0);
        assert!(cpu.cpu_efficiency >= 0.0 && cpu.cpu_efficiency <= 100.0);

        let memory = MetricsCollector::simulate_memory_usage();
        assert!(memory.current_usage_bytes > 0);
        assert!(memory.peak_usage_bytes >= memory.current_usage_bytes);

        let io = MetricsCollector::simulate_io_metrics();
        assert!(io.bytes_read_per_sec >= 0.0);
        assert!(io.bytes_written_per_sec >= 0.0);
        assert!(io.error_rate >= 0.0 && io.error_rate <= 100.0);
    }
}