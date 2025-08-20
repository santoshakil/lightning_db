//! Resource Tracking System for Lightning DB
//!
//! Comprehensive system resource monitoring including CPU, memory, disk, network,
//! and file descriptor usage with threshold-based alerting.

use crate::Database;
use crate::core::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, info};

/// Resource tracker for Lightning DB
pub struct ResourceTracker {
    /// Current resource usage
    current_usage: Arc<RwLock<ResourceUsage>>,
    /// Resource usage history
    usage_history: Arc<RwLock<VecDeque<ResourceSnapshot>>>,
    /// Resource thresholds for alerting
    thresholds: Arc<RwLock<HashMap<String, ResourceThreshold>>>,
    /// Configuration
    config: ResourceConfig,
    /// Tracking state
    is_tracking: Arc<AtomicBool>,
}

/// Configuration for resource tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConfig {
    /// Maximum history entries to keep
    pub max_history_entries: usize,
    /// Resource sampling interval
    pub sampling_interval: Duration,
    /// Enable system resource monitoring
    pub enable_system_monitoring: bool,
    /// Enable process-specific monitoring
    pub enable_process_monitoring: bool,
    /// Enable network monitoring
    pub enable_network_monitoring: bool,
    /// Enable disk I/O monitoring
    pub enable_disk_monitoring: bool,
    /// Enable memory detailed tracking
    pub enable_detailed_memory: bool,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_history_entries: 1000,
            sampling_interval: Duration::from_secs(15),
            enable_system_monitoring: true,
            enable_process_monitoring: true,
            enable_network_monitoring: true,
            enable_disk_monitoring: true,
            enable_detailed_memory: true,
        }
    }
}

/// Comprehensive resource usage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    /// CPU usage percentage (0.0 to 100.0)
    pub cpu_usage_percent: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Memory usage percentage (0.0 to 100.0)
    pub memory_usage_percent: f64,
    /// Total system memory in bytes
    pub total_memory_bytes: u64,
    /// Available memory in bytes
    pub available_memory_bytes: u64,
    /// Disk usage in bytes
    pub disk_usage_bytes: u64,
    /// Available disk space in bytes
    pub disk_free_bytes: u64,
    /// Total disk space in bytes
    pub total_disk_bytes: u64,
    /// Network bytes received
    pub network_bytes_in: u64,
    /// Network bytes sent
    pub network_bytes_out: u64,
    /// Network packets received
    pub network_packets_in: u64,
    /// Network packets sent
    pub network_packets_out: u64,
    /// Open file descriptors
    pub open_file_descriptors: u32,
    /// Maximum file descriptors allowed
    pub max_file_descriptors: u32,
    /// Number of threads
    pub thread_count: u32,
    /// System load average (1 minute)
    pub load_average: f64,
    /// Process-specific metrics
    pub process_metrics: ProcessMetrics,
    /// Disk I/O metrics
    pub disk_io_metrics: DiskIoMetrics,
    /// Network interface metrics
    pub network_interfaces: Vec<NetworkInterface>,
    /// Timestamp of measurement
    pub timestamp: SystemTime,
}

/// Process-specific resource metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessMetrics {
    /// Process ID
    pub pid: u32,
    /// Process CPU usage percentage
    pub cpu_percent: f64,
    /// Process memory usage (RSS) in bytes
    pub memory_rss_bytes: u64,
    /// Process virtual memory usage in bytes
    pub memory_vms_bytes: u64,
    /// Number of open file descriptors for this process
    pub open_fds: u32,
    /// Number of threads for this process
    pub thread_count: u32,
    /// Process uptime
    pub uptime: Duration,
    /// Process command line
    pub command_line: String,
}

/// Disk I/O metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskIoMetrics {
    /// Bytes read from disk
    pub bytes_read: u64,
    /// Bytes written to disk
    pub bytes_written: u64,
    /// Number of read operations
    pub read_ops: u64,
    /// Number of write operations
    pub write_ops: u64,
    /// Time spent reading (milliseconds)
    pub read_time_ms: u64,
    /// Time spent writing (milliseconds)
    pub write_time_ms: u64,
    /// Current I/O queue depth
    pub queue_depth: u32,
    /// I/O utilization percentage
    pub io_utilization_percent: f64,
}

/// Network interface metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInterface {
    /// Interface name
    pub name: String,
    /// Bytes received
    pub bytes_received: u64,
    /// Bytes transmitted
    pub bytes_transmitted: u64,
    /// Packets received
    pub packets_received: u64,
    /// Packets transmitted
    pub packets_transmitted: u64,
    /// Receive errors
    pub errors_received: u64,
    /// Transmit errors
    pub errors_transmitted: u64,
    /// Interface is up
    pub is_up: bool,
}

/// Resource usage snapshot for historical tracking
#[derive(Debug, Clone, Serialize)]
pub struct ResourceSnapshot {
    pub usage: ResourceUsage,
    pub timestamp: SystemTime,
    pub collection_duration: Duration,
}

/// Resource threshold for alerting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceThreshold {
    /// Resource name
    pub resource_name: String,
    /// Warning threshold
    pub warning_threshold: f64,
    /// Critical threshold
    pub critical_threshold: f64,
    /// Unit of measurement
    pub unit: String,
    /// Whether higher values are worse
    pub higher_is_worse: bool,
    /// Enabled
    pub enabled: bool,
}

/// Resource alert
#[derive(Debug, Clone, Serialize)]
pub struct ResourceAlert {
    pub resource_name: String,
    pub current_value: f64,
    pub threshold_value: f64,
    pub severity: AlertSeverity,
    pub message: String,
    pub timestamp: SystemTime,
}

/// Alert severity levels
#[derive(Debug, Clone, Serialize)]
pub enum AlertSeverity {
    Warning,
    Critical,
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            cpu_usage_percent: 0.0,
            memory_usage_bytes: 0,
            memory_usage_percent: 0.0,
            total_memory_bytes: 0,
            available_memory_bytes: 0,
            disk_usage_bytes: 0,
            disk_free_bytes: 0,
            total_disk_bytes: 0,
            network_bytes_in: 0,
            network_bytes_out: 0,
            network_packets_in: 0,
            network_packets_out: 0,
            open_file_descriptors: 0,
            max_file_descriptors: 0,
            thread_count: 0,
            load_average: 0.0,
            process_metrics: ProcessMetrics::default(),
            disk_io_metrics: DiskIoMetrics::default(),
            network_interfaces: Vec::new(),
            timestamp: SystemTime::now(),
        }
    }
}

impl Default for ProcessMetrics {
    fn default() -> Self {
        Self {
            pid: std::process::id(),
            cpu_percent: 0.0,
            memory_rss_bytes: 0,
            memory_vms_bytes: 0,
            open_fds: 0,
            thread_count: 0,
            uptime: Duration::from_secs(0),
            command_line: String::new(),
        }
    }
}

impl Default for DiskIoMetrics {
    fn default() -> Self {
        Self {
            bytes_read: 0,
            bytes_written: 0,
            read_ops: 0,
            write_ops: 0,
            read_time_ms: 0,
            write_time_ms: 0,
            queue_depth: 0,
            io_utilization_percent: 0.0,
        }
    }
}

impl ResourceTracker {
    /// Create a new resource tracker
    pub fn new() -> Self {
        Self::with_config(ResourceConfig::default())
    }

    /// Create resource tracker with custom configuration
    pub fn with_config(config: ResourceConfig) -> Self {
        let mut tracker = Self {
            current_usage: Arc::new(RwLock::new(ResourceUsage::default())),
            usage_history: Arc::new(RwLock::new(VecDeque::new())),
            thresholds: Arc::new(RwLock::new(HashMap::new())),
            config,
            is_tracking: Arc::new(AtomicBool::new(false)),
        };

        // Set up default thresholds
        tracker.setup_default_thresholds();
        tracker
    }

    /// Set up default resource thresholds
    fn setup_default_thresholds(&mut self) {
        let default_thresholds = vec![
            ResourceThreshold {
                resource_name: "cpu_usage_percent".to_string(),
                warning_threshold: 80.0,
                critical_threshold: 95.0,
                unit: "%".to_string(),
                higher_is_worse: true,
                enabled: true,
            },
            ResourceThreshold {
                resource_name: "memory_usage_percent".to_string(),
                warning_threshold: 85.0,
                critical_threshold: 95.0,
                unit: "%".to_string(),
                higher_is_worse: true,
                enabled: true,
            },
            ResourceThreshold {
                resource_name: "disk_usage_percent".to_string(),
                warning_threshold: 80.0,
                critical_threshold: 90.0,
                unit: "%".to_string(),
                higher_is_worse: true,
                enabled: true,
            },
            ResourceThreshold {
                resource_name: "load_average".to_string(),
                warning_threshold: 4.0,
                critical_threshold: 8.0,
                unit: "load".to_string(),
                higher_is_worse: true,
                enabled: true,
            },
            ResourceThreshold {
                resource_name: "open_file_descriptors_percent".to_string(),
                warning_threshold: 80.0,
                critical_threshold: 95.0,
                unit: "%".to_string(),
                higher_is_worse: true,
                enabled: true,
            },
        ];

        let mut thresholds = self.thresholds.write().unwrap();
        for threshold in default_thresholds {
            thresholds.insert(threshold.resource_name.clone(), threshold);
        }
    }

    /// Start resource tracking
    pub fn start_tracking(&self) {
        self.is_tracking.store(true, Ordering::Relaxed);
        info!("Resource tracking started");
    }

    /// Stop resource tracking
    pub fn stop_tracking(&self) {
        self.is_tracking.store(false, Ordering::Relaxed);
        info!("Resource tracking stopped");
    }

    /// Track resources
    pub fn track_resources(&self, _database: &Database) -> Result<()> {
        if !self.is_tracking.load(Ordering::Relaxed) {
            return Ok(());
        }

        let collection_start = Instant::now();
        debug!("Starting resource tracking cycle");

        let mut usage = ResourceUsage::default();
        usage.timestamp = SystemTime::now();

        // Collect system resources
        if self.config.enable_system_monitoring {
            self.collect_system_resources(&mut usage)?;
        }

        // Collect process-specific resources
        if self.config.enable_process_monitoring {
            self.collect_process_resources(&mut usage)?;
        }

        // Collect network resources
        if self.config.enable_network_monitoring {
            self.collect_network_resources(&mut usage)?;
        }

        // Collect disk I/O resources
        if self.config.enable_disk_monitoring {
            self.collect_disk_io_resources(&mut usage)?;
        }

        // Update current usage
        {
            let mut current = self.current_usage.write().unwrap();
            *current = usage.clone();
        }

        // Add to history
        self.add_to_history(usage, collection_start.elapsed());

        debug!(
            "Resource tracking completed in {:?}",
            collection_start.elapsed()
        );
        Ok(())
    }

    /// Get current resource usage
    pub fn get_current_usage(&self) -> ResourceUsage {
        self.current_usage.read().unwrap().clone()
    }

    /// Get resource usage history
    pub fn get_usage_history(&self) -> Vec<ResourceSnapshot> {
        self.usage_history.read().unwrap().iter().cloned().collect()
    }

    /// Collect system-wide resource information
    fn collect_system_resources(&self, usage: &mut ResourceUsage) -> Result<()> {
        // Platform-specific implementation would go here
        // For now, using placeholder values

        usage.cpu_usage_percent = 25.5;
        usage.memory_usage_bytes = 1024 * 1024 * 512; // 512MB
        usage.total_memory_bytes = 1024 * 1024 * 1024 * 8; // 8GB
        usage.available_memory_bytes = usage.total_memory_bytes - usage.memory_usage_bytes;
        usage.memory_usage_percent =
            (usage.memory_usage_bytes as f64 / usage.total_memory_bytes as f64) * 100.0;

        usage.disk_usage_bytes = 1024 * 1024 * 1024 * 2; // 2GB
        usage.total_disk_bytes = 1024 * 1024 * 1024 * 100; // 100GB
        usage.disk_free_bytes = usage.total_disk_bytes - usage.disk_usage_bytes;

        usage.open_file_descriptors = 45;
        usage.max_file_descriptors = 1024;
        usage.thread_count = 8;
        usage.load_average = 1.2;

        Ok(())
    }

    /// Collect process-specific resource information
    fn collect_process_resources(&self, usage: &mut ResourceUsage) -> Result<()> {
        usage.process_metrics = ProcessMetrics {
            pid: std::process::id(),
            cpu_percent: 15.5,
            memory_rss_bytes: 1024 * 1024 * 256, // 256MB RSS
            memory_vms_bytes: 1024 * 1024 * 512, // 512MB VMS
            open_fds: 25,
            thread_count: 6,
            uptime: Duration::from_secs(3600), // 1 hour
            command_line: "lightning_db".to_string(),
        };

        Ok(())
    }

    /// Collect network resource information
    fn collect_network_resources(&self, usage: &mut ResourceUsage) -> Result<()> {
        usage.network_bytes_in = 1024 * 1024 * 10; // 10MB
        usage.network_bytes_out = 1024 * 1024 * 5; // 5MB
        usage.network_packets_in = 10000;
        usage.network_packets_out = 5000;

        // Sample network interfaces
        usage.network_interfaces = vec![
            NetworkInterface {
                name: "eth0".to_string(),
                bytes_received: usage.network_bytes_in,
                bytes_transmitted: usage.network_bytes_out,
                packets_received: usage.network_packets_in,
                packets_transmitted: usage.network_packets_out,
                errors_received: 0,
                errors_transmitted: 0,
                is_up: true,
            },
            NetworkInterface {
                name: "lo".to_string(),
                bytes_received: 1024 * 100, // 100KB loopback
                bytes_transmitted: 1024 * 100,
                packets_received: 100,
                packets_transmitted: 100,
                errors_received: 0,
                errors_transmitted: 0,
                is_up: true,
            },
        ];

        Ok(())
    }

    /// Collect disk I/O resource information
    fn collect_disk_io_resources(&self, usage: &mut ResourceUsage) -> Result<()> {
        usage.disk_io_metrics = DiskIoMetrics {
            bytes_read: 1024 * 1024 * 50,    // 50MB
            bytes_written: 1024 * 1024 * 25, // 25MB
            read_ops: 1000,
            write_ops: 500,
            read_time_ms: 2000,  // 2 seconds
            write_time_ms: 1500, // 1.5 seconds
            queue_depth: 2,
            io_utilization_percent: 15.0,
        };

        Ok(())
    }

    /// Add resource usage to history
    fn add_to_history(&self, usage: ResourceUsage, collection_duration: Duration) {
        let mut history = self.usage_history.write().unwrap();

        history.push_back(ResourceSnapshot {
            usage,
            timestamp: SystemTime::now(),
            collection_duration,
        });

        // Trim history if it exceeds max entries
        while history.len() > self.config.max_history_entries {
            history.pop_front();
        }
    }

    /// Check resource thresholds and generate alerts
    pub fn check_thresholds(&self) -> Vec<ResourceAlert> {
        let usage = self.current_usage.read().unwrap();
        let thresholds = self.thresholds.read().unwrap();
        let mut alerts = Vec::new();

        for (resource_name, threshold) in thresholds.iter() {
            if !threshold.enabled {
                continue;
            }

            let current_value = self.get_resource_value(&usage, resource_name);

            if threshold.higher_is_worse {
                if current_value >= threshold.critical_threshold {
                    alerts.push(ResourceAlert {
                        resource_name: resource_name.clone(),
                        current_value,
                        threshold_value: threshold.critical_threshold,
                        severity: AlertSeverity::Critical,
                        message: format!(
                            "{} is critically high: {:.2}{} (threshold: {:.2}{})",
                            resource_name,
                            current_value,
                            threshold.unit,
                            threshold.critical_threshold,
                            threshold.unit
                        ),
                        timestamp: SystemTime::now(),
                    });
                } else if current_value >= threshold.warning_threshold {
                    alerts.push(ResourceAlert {
                        resource_name: resource_name.clone(),
                        current_value,
                        threshold_value: threshold.warning_threshold,
                        severity: AlertSeverity::Warning,
                        message: format!(
                            "{} is high: {:.2}{} (threshold: {:.2}{})",
                            resource_name,
                            current_value,
                            threshold.unit,
                            threshold.warning_threshold,
                            threshold.unit
                        ),
                        timestamp: SystemTime::now(),
                    });
                }
            } else {
                // Lower is worse (e.g., available memory)
                if current_value <= threshold.critical_threshold {
                    alerts.push(ResourceAlert {
                        resource_name: resource_name.clone(),
                        current_value,
                        threshold_value: threshold.critical_threshold,
                        severity: AlertSeverity::Critical,
                        message: format!(
                            "{} is critically low: {:.2}{} (threshold: {:.2}{})",
                            resource_name,
                            current_value,
                            threshold.unit,
                            threshold.critical_threshold,
                            threshold.unit
                        ),
                        timestamp: SystemTime::now(),
                    });
                } else if current_value <= threshold.warning_threshold {
                    alerts.push(ResourceAlert {
                        resource_name: resource_name.clone(),
                        current_value,
                        threshold_value: threshold.warning_threshold,
                        severity: AlertSeverity::Warning,
                        message: format!(
                            "{} is low: {:.2}{} (threshold: {:.2}{})",
                            resource_name,
                            current_value,
                            threshold.unit,
                            threshold.warning_threshold,
                            threshold.unit
                        ),
                        timestamp: SystemTime::now(),
                    });
                }
            }
        }

        alerts
    }

    /// Get resource value by name
    fn get_resource_value(&self, usage: &ResourceUsage, resource_name: &str) -> f64 {
        match resource_name {
            "cpu_usage_percent" => usage.cpu_usage_percent,
            "memory_usage_percent" => usage.memory_usage_percent,
            "disk_usage_percent" => {
                if usage.total_disk_bytes > 0 {
                    (usage.disk_usage_bytes as f64 / usage.total_disk_bytes as f64) * 100.0
                } else {
                    0.0
                }
            }
            "load_average" => usage.load_average,
            "open_file_descriptors_percent" => {
                if usage.max_file_descriptors > 0 {
                    (usage.open_file_descriptors as f64 / usage.max_file_descriptors as f64) * 100.0
                } else {
                    0.0
                }
            }
            "available_memory_bytes" => usage.available_memory_bytes as f64,
            "disk_free_bytes" => usage.disk_free_bytes as f64,
            "network_bytes_in" => usage.network_bytes_in as f64,
            "network_bytes_out" => usage.network_bytes_out as f64,
            "io_utilization_percent" => usage.disk_io_metrics.io_utilization_percent,
            _ => 0.0,
        }
    }

    /// Add custom threshold
    pub fn add_threshold(&self, threshold: ResourceThreshold) {
        let mut thresholds = self.thresholds.write().unwrap();
        thresholds.insert(threshold.resource_name.clone(), threshold);
    }

    /// Remove threshold
    pub fn remove_threshold(&self, resource_name: &str) {
        let mut thresholds = self.thresholds.write().unwrap();
        thresholds.remove(resource_name);
    }

    /// Get resource statistics
    pub fn get_resource_stats(&self) -> ResourceStats {
        let history = self.usage_history.read().unwrap();
        let current = self.current_usage.read().unwrap();

        if history.is_empty() {
            return ResourceStats::default();
        }

        // Calculate averages over history
        let cpu_values: Vec<f64> = history.iter().map(|s| s.usage.cpu_usage_percent).collect();
        let memory_values: Vec<f64> = history
            .iter()
            .map(|s| s.usage.memory_usage_percent)
            .collect();
        let load_values: Vec<f64> = history.iter().map(|s| s.usage.load_average).collect();

        ResourceStats {
            current_cpu_percent: current.cpu_usage_percent,
            average_cpu_percent: cpu_values.iter().sum::<f64>() / cpu_values.len() as f64,
            peak_cpu_percent: cpu_values.iter().fold(0.0, |a, &b| a.max(b)),
            current_memory_percent: current.memory_usage_percent,
            average_memory_percent: memory_values.iter().sum::<f64>() / memory_values.len() as f64,
            peak_memory_percent: memory_values.iter().fold(0.0, |a, &b| a.max(b)),
            current_load_average: current.load_average,
            average_load_average: load_values.iter().sum::<f64>() / load_values.len() as f64,
            peak_load_average: load_values.iter().fold(0.0, |a, &b| a.max(b)),
            total_network_bytes_in: current.network_bytes_in,
            total_network_bytes_out: current.network_bytes_out,
            samples_collected: history.len(),
        }
    }

    /// Calculate resource usage trends
    pub fn calculate_resource_trends(
        &self,
        window: Duration,
    ) -> Result<HashMap<String, ResourceTrend>> {
        let history = self.usage_history.read().unwrap();
        let cutoff_time = SystemTime::now()
            .checked_sub(window)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let recent_data: Vec<_> = history
            .iter()
            .filter(|snapshot| snapshot.timestamp >= cutoff_time)
            .collect();

        if recent_data.len() < 3 {
            return Ok(HashMap::new());
        }

        let mut trends = HashMap::new();

        // CPU usage trend
        let cpu_values: Vec<f64> = recent_data
            .iter()
            .map(|s| s.usage.cpu_usage_percent)
            .collect();
        if let Some(trend) = self.calculate_trend("cpu_usage_percent", &cpu_values) {
            trends.insert("cpu_usage_percent".to_string(), trend);
        }

        // Memory usage trend
        let memory_values: Vec<f64> = recent_data
            .iter()
            .map(|s| s.usage.memory_usage_percent)
            .collect();
        if let Some(trend) = self.calculate_trend("memory_usage_percent", &memory_values) {
            trends.insert("memory_usage_percent".to_string(), trend);
        }

        // Load average trend
        let load_values: Vec<f64> = recent_data.iter().map(|s| s.usage.load_average).collect();
        if let Some(trend) = self.calculate_trend("load_average", &load_values) {
            trends.insert("load_average".to_string(), trend);
        }

        Ok(trends)
    }

    /// Calculate trend for a resource metric
    fn calculate_trend(&self, resource_name: &str, values: &[f64]) -> Option<ResourceTrend> {
        if values.len() < 3 {
            return None;
        }

        // Simple linear trend calculation
        let n = values.len() as f64;
        let x_sum: f64 = (0..values.len()).map(|i| i as f64).sum();
        let y_sum: f64 = values.iter().sum();
        let xy_sum: f64 = values.iter().enumerate().map(|(i, &y)| i as f64 * y).sum();
        let x2_sum: f64 = (0..values.len()).map(|i| (i as f64).powi(2)).sum();

        let denominator = n * x2_sum - x_sum.powi(2);
        if denominator.abs() < f64::EPSILON {
            return None;
        }

        let slope = (n * xy_sum - x_sum * y_sum) / denominator;

        let trend_direction = if slope > 0.05 {
            ResourceTrendDirection::Increasing
        } else if slope < -0.05 {
            ResourceTrendDirection::Decreasing
        } else {
            ResourceTrendDirection::Stable
        };

        Some(ResourceTrend {
            resource_name: resource_name.to_string(),
            direction: trend_direction,
            slope,
            confidence: if values.len() >= 10 { 0.8 } else { 0.5 },
            data_points: values.len(),
        })
    }
}

/// Resource statistics summary
#[derive(Debug, Clone, Serialize)]
pub struct ResourceStats {
    pub current_cpu_percent: f64,
    pub average_cpu_percent: f64,
    pub peak_cpu_percent: f64,
    pub current_memory_percent: f64,
    pub average_memory_percent: f64,
    pub peak_memory_percent: f64,
    pub current_load_average: f64,
    pub average_load_average: f64,
    pub peak_load_average: f64,
    pub total_network_bytes_in: u64,
    pub total_network_bytes_out: u64,
    pub samples_collected: usize,
}

impl Default for ResourceStats {
    fn default() -> Self {
        Self {
            current_cpu_percent: 0.0,
            average_cpu_percent: 0.0,
            peak_cpu_percent: 0.0,
            current_memory_percent: 0.0,
            average_memory_percent: 0.0,
            peak_memory_percent: 0.0,
            current_load_average: 0.0,
            average_load_average: 0.0,
            peak_load_average: 0.0,
            total_network_bytes_in: 0,
            total_network_bytes_out: 0,
            samples_collected: 0,
        }
    }
}

/// Resource usage trend
#[derive(Debug, Clone, Serialize)]
pub struct ResourceTrend {
    pub resource_name: String,
    pub direction: ResourceTrendDirection,
    pub slope: f64,
    pub confidence: f64,
    pub data_points: usize,
}

/// Resource trend direction
#[derive(Debug, Clone, Serialize)]
pub enum ResourceTrendDirection {
    Increasing,
    Decreasing,
    Stable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_tracker_creation() {
        let tracker = ResourceTracker::new();
        assert!(!tracker.is_tracking.load(Ordering::Relaxed));

        // Should have default thresholds
        let thresholds = tracker.thresholds.read().unwrap();
        assert!(!thresholds.is_empty());
    }

    #[test]
    fn test_resource_tracking_lifecycle() {
        let tracker = ResourceTracker::new();

        tracker.start_tracking();
        assert!(tracker.is_tracking.load(Ordering::Relaxed));

        tracker.stop_tracking();
        assert!(!tracker.is_tracking.load(Ordering::Relaxed));
    }

    #[test]
    fn test_threshold_management() {
        let tracker = ResourceTracker::new();

        let custom_threshold = ResourceThreshold {
            resource_name: "custom_metric".to_string(),
            warning_threshold: 50.0,
            critical_threshold: 80.0,
            unit: "%".to_string(),
            higher_is_worse: true,
            enabled: true,
        };

        tracker.add_threshold(custom_threshold.clone());

        let thresholds = tracker.thresholds.read().unwrap();
        assert!(thresholds.contains_key("custom_metric"));
        assert_eq!(thresholds["custom_metric"].warning_threshold, 50.0);

        drop(thresholds);
        tracker.remove_threshold("custom_metric");

        let thresholds = tracker.thresholds.read().unwrap();
        assert!(!thresholds.contains_key("custom_metric"));
    }

    #[test]
    fn test_resource_value_extraction() {
        let tracker = ResourceTracker::new();
        let mut usage = ResourceUsage::default();
        usage.cpu_usage_percent = 75.0;
        usage.memory_usage_percent = 60.0;
        usage.load_average = 2.5;

        assert_eq!(
            tracker.get_resource_value(&usage, "cpu_usage_percent"),
            75.0
        );
        assert_eq!(
            tracker.get_resource_value(&usage, "memory_usage_percent"),
            60.0
        );
        assert_eq!(tracker.get_resource_value(&usage, "load_average"), 2.5);
        assert_eq!(tracker.get_resource_value(&usage, "unknown_metric"), 0.0);
    }

    #[test]
    fn test_threshold_checking() {
        let tracker = ResourceTracker::new();

        // Set up a usage scenario that should trigger alerts
        let mut usage = ResourceUsage::default();
        usage.cpu_usage_percent = 90.0; // Above critical threshold
        usage.memory_usage_percent = 85.0; // At warning threshold
        usage.load_average = 2.0; // Below warning threshold

        {
            let mut current = tracker.current_usage.write().unwrap();
            *current = usage;
        }

        let alerts = tracker.check_thresholds();

        // Should have alerts for CPU (critical) and memory (warning)
        assert!(alerts.len() >= 2);

        let cpu_alert = alerts
            .iter()
            .find(|a| a.resource_name == "cpu_usage_percent");
        assert!(cpu_alert.is_some());
        assert!(matches!(
            cpu_alert.unwrap().severity,
            AlertSeverity::Critical
        ));

        let memory_alert = alerts
            .iter()
            .find(|a| a.resource_name == "memory_usage_percent");
        assert!(memory_alert.is_some());
        assert!(matches!(
            memory_alert.unwrap().severity,
            AlertSeverity::Warning
        ));
    }

    #[test]
    fn test_resource_stats_calculation() {
        let tracker = ResourceTracker::new();

        // Add some history
        for i in 0..5 {
            let usage = ResourceUsage {
                cpu_usage_percent: 50.0 + i as f64 * 10.0, // 50, 60, 70, 80, 90
                memory_usage_percent: 40.0 + i as f64 * 5.0, // 40, 45, 50, 55, 60
                load_average: 1.0 + i as f64 * 0.5,        // 1.0, 1.5, 2.0, 2.5, 3.0
                ..ResourceUsage::default()
            };

            tracker.add_to_history(usage, Duration::from_millis(100));
        }

        let stats = tracker.get_resource_stats();

        // Check that averages are calculated correctly
        assert!((stats.average_cpu_percent - 70.0).abs() < 1.0); // Average of 50,60,70,80,90
        assert!((stats.average_memory_percent - 50.0).abs() < 1.0); // Average of 40,45,50,55,60
        assert!((stats.average_load_average - 2.0).abs() < 0.1); // Average of 1.0,1.5,2.0,2.5,3.0

        assert_eq!(stats.peak_cpu_percent, 90.0);
        assert_eq!(stats.peak_memory_percent, 60.0);
        assert_eq!(stats.peak_load_average, 3.0);
        assert_eq!(stats.samples_collected, 5);
    }
}
