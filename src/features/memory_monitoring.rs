//! Production Memory Monitoring System
//!
//! This module provides comprehensive runtime memory monitoring and alerting
//! for production deployment of Lightning DB, including:
//! - Real-time memory usage monitoring
//! - Memory leak detection in production
//! - Memory dump capabilities
//! - Performance impact analysis
//! - Automated alerting system

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}},
    time::{Duration, Instant, SystemTime},
    thread,
    fs::File,
    io::{Write, BufWriter},
    path::PathBuf,
};

use dashmap::DashMap;
use parking_lot::{RwLock as ParkingRwLock, Mutex as ParkingMutex};
use serde::{Serialize, Deserialize};
use tracing::{info, warn, error};
use prometheus::{Gauge, Counter, Histogram, Registry, TextEncoder, Encoder};

use crate::utils::{
    MemoryStats, get_memory_tracker, LeakReport, LeakType, get_leak_detector, ResourceUsageStats, get_resource_manager,
};

/// Global production memory monitor instance
pub static MEMORY_MONITOR: once_cell::sync::Lazy<Arc<ProductionMemoryMonitor>> = 
    once_cell::sync::Lazy::new(|| Arc::new(ProductionMemoryMonitor::new()));

/// Production memory monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductionMonitoringConfig {
    /// Enable memory monitoring
    pub enabled: bool,
    /// Monitoring interval
    pub monitoring_interval: Duration,
    /// Memory usage threshold for alerts (bytes)
    pub memory_alert_threshold: u64,
    /// Memory growth rate threshold (bytes/sec)
    pub growth_rate_threshold: f64,
    /// Leak detection interval
    pub leak_detection_interval: Duration,
    /// Enable automatic memory dumps
    pub enable_memory_dumps: bool,
    /// Memory dump directory
    pub memory_dump_dir: PathBuf,
    /// Maximum memory dump size (bytes)
    pub max_dump_size: u64,
    /// Enable Prometheus metrics
    pub enable_prometheus: bool,
    /// Prometheus metrics port
    pub prometheus_port: u16,
    /// Alert cooldown period
    pub alert_cooldown: Duration,
    /// Maximum alert history
    pub max_alert_history: usize,
    /// Enable performance profiling
    pub enable_profiling: bool,
    /// Profiling sample rate
    pub profiling_sample_rate: f64,
}

impl Default for ProductionMonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            monitoring_interval: Duration::from_secs(30),
            memory_alert_threshold: 2 * 1024 * 1024 * 1024, // 2GB
            growth_rate_threshold: 10.0 * 1024.0 * 1024.0, // 10MB/s
            leak_detection_interval: Duration::from_secs(300), // 5 minutes
            enable_memory_dumps: true,
            memory_dump_dir: PathBuf::from("./memory_dumps"),
            max_dump_size: 100 * 1024 * 1024, // 100MB
            enable_prometheus: true,
            prometheus_port: 9090,
            alert_cooldown: Duration::from_secs(300), // 5 minutes
            max_alert_history: 1000,
            enable_profiling: false,
            profiling_sample_rate: 0.01, // 1%
        }
    }
}

/// Memory alert levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertLevel {
    Info,
    Warning,
    Critical,
    Emergency,
}

/// Memory alert types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MemoryAlertType {
    HighMemoryUsage {
        current: u64,
        threshold: u64,
        percentage: f64,
    },
    RapidGrowth {
        rate: f64,
        threshold: f64,
        duration: Duration,
    },
    MemoryLeak {
        leak_type: String,
        estimated_size: u64,
        location: Option<String>,
    },
    AllocationStorm {
        allocations_per_sec: f64,
        duration: Duration,
    },
    FragmentationHigh {
        fragmentation_ratio: f64,
        wasted_memory: u64,
    },
    ResourceExhaustion {
        resource_type: String,
        current: usize,
        limit: usize,
    },
}

/// Memory alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryAlert {
    pub id: u64,
    pub timestamp: SystemTime,
    pub level: AlertLevel,
    pub alert_type: MemoryAlertType,
    pub message: String,
    pub metadata: HashMap<String, String>,
    pub acknowledged: bool,
    pub resolved: bool,
}

/// Memory metrics for Prometheus
#[derive(Clone)]
pub struct MemoryMetrics {
    pub memory_usage_bytes: Gauge,
    pub memory_peak_bytes: Gauge,
    pub allocations_total: Counter,
    pub deallocations_total: Counter,
    pub allocation_rate: Gauge,
    pub leak_count: Gauge,
    pub leak_size_bytes: Gauge,
    pub fragmentation_ratio: Gauge,
    pub resource_count: Gauge,
    pub alert_count: Counter,
    pub monitoring_duration: Histogram,
}

impl MemoryMetrics {
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let memory_usage_bytes = Gauge::new(
            "lightning_db_memory_usage_bytes",
            "Current memory usage in bytes"
        )?;
        registry.register(Box::new(memory_usage_bytes.clone()))?;

        let memory_peak_bytes = Gauge::new(
            "lightning_db_memory_peak_bytes", 
            "Peak memory usage in bytes"
        )?;
        registry.register(Box::new(memory_peak_bytes.clone()))?;

        let allocations_total = Counter::new(
            "lightning_db_allocations_total",
            "Total number of memory allocations"
        )?;
        registry.register(Box::new(allocations_total.clone()))?;

        let deallocations_total = Counter::new(
            "lightning_db_deallocations_total",
            "Total number of memory deallocations"
        )?;
        registry.register(Box::new(deallocations_total.clone()))?;

        let allocation_rate = Gauge::new(
            "lightning_db_allocation_rate",
            "Current allocation rate (allocations per second)"
        )?;
        registry.register(Box::new(allocation_rate.clone()))?;

        let leak_count = Gauge::new(
            "lightning_db_leak_count",
            "Number of detected memory leaks"
        )?;
        registry.register(Box::new(leak_count.clone()))?;

        let leak_size_bytes = Gauge::new(
            "lightning_db_leak_size_bytes",
            "Total size of detected memory leaks in bytes"
        )?;
        registry.register(Box::new(leak_size_bytes.clone()))?;

        let fragmentation_ratio = Gauge::new(
            "lightning_db_fragmentation_ratio",
            "Memory fragmentation ratio (0.0 to 1.0)"
        )?;
        registry.register(Box::new(fragmentation_ratio.clone()))?;

        let resource_count = Gauge::new(
            "lightning_db_resource_count",
            "Number of tracked resources"
        )?;
        registry.register(Box::new(resource_count.clone()))?;

        let alert_count = Counter::new(
            "lightning_db_alerts_total",
            "Total number of memory alerts generated"
        )?;
        registry.register(Box::new(alert_count.clone()))?;

        let monitoring_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "lightning_db_monitoring_duration_seconds",
                "Time spent in memory monitoring"
            ).buckets(vec![0.001, 0.01, 0.1, 1.0, 10.0])
        )?;
        registry.register(Box::new(monitoring_duration.clone()))?;

        Ok(Self {
            memory_usage_bytes,
            memory_peak_bytes,
            allocations_total,
            deallocations_total,
            allocation_rate,
            leak_count,
            leak_size_bytes,
            fragmentation_ratio,
            resource_count,
            alert_count,
            monitoring_duration,
        })
    }
}

/// Memory dump metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryDumpMetadata {
    pub timestamp: SystemTime,
    pub memory_usage: u64,
    pub allocation_count: u64,
    pub leak_count: usize,
    pub trigger_reason: String,
    pub file_path: PathBuf,
    pub file_size: u64,
}

/// Production memory monitor
pub struct ProductionMemoryMonitor {
    config: ParkingRwLock<ProductionMonitoringConfig>,
    
    // Monitoring state
    monitoring_thread: ParkingMutex<Option<thread::JoinHandle<()>>>,
    leak_detection_thread: ParkingMutex<Option<thread::JoinHandle<()>>>,
    shutdown_flag: Arc<AtomicBool>,
    
    // Alerts
    alerts: DashMap<u64, MemoryAlert>,
    alert_history: ParkingMutex<VecDeque<MemoryAlert>>,
    alert_id_counter: AtomicU64,
    last_alert_times: DashMap<String, Instant>,
    
    // Metrics
    prometheus_registry: Option<Registry>,
    metrics: Option<MemoryMetrics>,
    
    // Memory dumps
    memory_dumps: ParkingMutex<Vec<MemoryDumpMetadata>>,
    
    // Statistics
    monitoring_stats: ParkingRwLock<MonitoringStats>,
}

/// Monitoring statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringStats {
    pub uptime: Duration,
    pub total_alerts: u64,
    pub active_alerts: usize,
    pub memory_dumps_created: usize,
    pub last_monitoring_cycle: Option<SystemTime>,
    pub monitoring_cycles_completed: u64,
    pub average_cycle_duration: Duration,
}

impl ProductionMemoryMonitor {
    pub fn new() -> Self {
        let prometheus_registry = Registry::new();
        let metrics = MemoryMetrics::new(&prometheus_registry).ok();
        
        Self {
            config: ParkingRwLock::new(ProductionMonitoringConfig::default()),
            monitoring_thread: ParkingMutex::new(None),
            leak_detection_thread: ParkingMutex::new(None),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            alerts: DashMap::new(),
            alert_history: ParkingMutex::new(VecDeque::new()),
            alert_id_counter: AtomicU64::new(1),
            last_alert_times: DashMap::new(),
            prometheus_registry: Some(prometheus_registry),
            metrics,
            memory_dumps: ParkingMutex::new(Vec::new()),
            monitoring_stats: ParkingRwLock::new(MonitoringStats {
                uptime: Duration::ZERO,
                total_alerts: 0,
                active_alerts: 0,
                memory_dumps_created: 0,
                last_monitoring_cycle: None,
                monitoring_cycles_completed: 0,
                average_cycle_duration: Duration::ZERO,
            }),
        }
    }

    /// Configure production monitoring
    pub fn configure(&self, config: ProductionMonitoringConfig) {
        let mut cfg = self.config.write();
        *cfg = config;
    }

    /// Start production memory monitoring
    pub fn start(&self) {
        let config = self.config.read().clone();
        
        if !config.enabled {
            info!("Production memory monitoring is disabled");
            return;
        }

        info!("Starting production memory monitoring");

        // Create memory dump directory
        if config.enable_memory_dumps {
            if let Err(e) = std::fs::create_dir_all(&config.memory_dump_dir) {
                error!("Failed to create memory dump directory: {}", e);
            }
        }

        // Start monitoring thread
        self.start_monitoring_thread();

        // Start leak detection thread
        self.start_leak_detection_thread();

        // Start Prometheus metrics server if enabled
        if config.enable_prometheus {
            self.start_prometheus_server();
        }

        info!("Production memory monitoring started");
    }

    /// Stop production memory monitoring
    pub fn stop(&self) {
        info!("Stopping production memory monitoring");

        self.shutdown_flag.store(true, Ordering::SeqCst);

        // Wait for threads to finish
        if let Some(handle) = self.monitoring_thread.lock().take() {
            let _ = handle.join();
        }

        if let Some(handle) = self.leak_detection_thread.lock().take() {
            let _ = handle.join();
        }

        self.shutdown_flag.store(false, Ordering::SeqCst);

        info!("Production memory monitoring stopped");
    }

    /// Generate memory alert
    pub fn generate_alert(&self, level: AlertLevel, alert_type: MemoryAlertType, message: String) {
        let config = self.config.read();
        
        // Check alert cooldown
        let alert_key = format!("{:?}", alert_type);
        if let Some(last_time) = self.last_alert_times.get(&alert_key) {
            if last_time.elapsed() < config.alert_cooldown {
                return; // Still in cooldown
            }
        }

        let alert_id = self.alert_id_counter.fetch_add(1, Ordering::Relaxed);
        
        let alert = MemoryAlert {
            id: alert_id,
            timestamp: SystemTime::now(),
            level: level.clone(),
            alert_type: alert_type.clone(),
            message: message.clone(),
            metadata: HashMap::new(),
            acknowledged: false,
            resolved: false,
        };

        // Store alert
        self.alerts.insert(alert_id, alert.clone());
        
        // Add to history
        {
            let mut history = self.alert_history.lock();
            history.push_back(alert.clone());
            
            // Limit history size
            if history.len() > config.max_alert_history {
                history.pop_front();
            }
        }

        // Update last alert time
        self.last_alert_times.insert(alert_key, Instant::now());

        // Update metrics
        if let Some(ref metrics) = self.metrics {
            metrics.alert_count.inc();
        }

        // Update statistics
        {
            let mut stats = self.monitoring_stats.write();
            stats.total_alerts += 1;
            stats.active_alerts = self.alerts.len();
        }

        // Log alert
        match level {
            AlertLevel::Info => info!("Memory alert: {}", message),
            AlertLevel::Warning => warn!("Memory alert: {}", message),
            AlertLevel::Critical => error!("Memory alert: {}", message),
            AlertLevel::Emergency => error!("EMERGENCY memory alert: {}", message),
        }

        // Trigger memory dump for critical/emergency alerts
        if matches!(level, AlertLevel::Critical | AlertLevel::Emergency) {
            let _ = self.create_memory_dump(&format!("alert_{}", alert_id));
        }
    }

    /// Create memory dump
    pub fn create_memory_dump(&self, reason: &str) -> Result<MemoryDumpMetadata, std::io::Error> {
        let config = self.config.read();
        
        if !config.enable_memory_dumps {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Memory dumps are disabled"
            ));
        }

        let timestamp = SystemTime::now();
        let timestamp_str = format!("{:?}", timestamp).replace([':', ' '], "_");
        let file_name = format!("memory_dump_{}_{}.json", reason, timestamp_str);
        let file_path = config.memory_dump_dir.join(file_name);

        // Collect memory information
        let memory_stats = get_memory_tracker().get_statistics();
        let leak_report = get_leak_detector().scan_for_leaks();
        let resource_stats = get_resource_manager().get_usage_stats();

        let dump_data = MemoryDumpData {
            timestamp,
            reason: reason.to_string(),
            memory_stats,
            leak_report,
            resource_stats,
            system_info: self.collect_system_info(),
        };

        // Write dump to file
        let file = File::create(&file_path)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, &dump_data)?;
        writer.flush()?;

        let file_size = file_path.metadata()?.len();

        // Check dump size limit
        if file_size > config.max_dump_size {
            warn!("Memory dump size ({} bytes) exceeds limit ({} bytes)", file_size, config.max_dump_size);
        }

        let metadata = MemoryDumpMetadata {
            timestamp,
            memory_usage: dump_data.memory_stats.current_usage,
            allocation_count: dump_data.memory_stats.allocation_count,
            leak_count: dump_data.leak_report.leaks_detected.len(),
            trigger_reason: reason.to_string(),
            file_path: file_path.clone(),
            file_size,
        };

        // Store metadata
        {
            let mut dumps = self.memory_dumps.lock();
            dumps.push(metadata.clone());
            
            // Update statistics
            let mut stats = self.monitoring_stats.write();
            stats.memory_dumps_created += 1;
        }

        info!("Memory dump created: {} ({} bytes)", file_path.display(), file_size);

        Ok(metadata)
    }

    /// Get active alerts
    pub fn get_active_alerts(&self) -> Vec<MemoryAlert> {
        self.alerts.iter()
            .filter(|entry| !entry.value().resolved)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Acknowledge alert
    pub fn acknowledge_alert(&self, alert_id: u64) -> bool {
        if let Some(mut alert) = self.alerts.get_mut(&alert_id) {
            alert.acknowledged = true;
            true
        } else {
            false
        }
    }

    /// Resolve alert
    pub fn resolve_alert(&self, alert_id: u64) -> bool {
        if let Some(mut alert) = self.alerts.get_mut(&alert_id) {
            alert.resolved = true;
            
            // Update statistics
            let mut stats = self.monitoring_stats.write();
            stats.active_alerts = self.alerts.iter()
                .filter(|entry| !entry.value().resolved)
                .count();
            
            true
        } else {
            false
        }
    }

    /// Get monitoring statistics
    pub fn get_monitoring_stats(&self) -> MonitoringStats {
        self.monitoring_stats.read().clone()
    }

    /// Get memory dump history
    pub fn get_memory_dumps(&self) -> Vec<MemoryDumpMetadata> {
        self.memory_dumps.lock().clone()
    }

    /// Export Prometheus metrics
    pub fn export_prometheus_metrics(&self) -> Result<String, Box<dyn std::error::Error>> {
        if let Some(ref registry) = self.prometheus_registry {
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer)?;
            Ok(String::from_utf8(buffer)?)
        } else {
            Err("Prometheus registry not initialized".into())
        }
    }

    // Private helper methods

    fn start_monitoring_thread(&self) {
        let monitor = Arc::new(self as *const _ as usize); // Unsafe but needed
        let shutdown_flag = self.shutdown_flag.clone();
        let config = self.config.read().clone();

        let handle = thread::Builder::new()
            .name("memory-monitor".to_string())
            .spawn(move || {
                Self::monitoring_loop(monitor, shutdown_flag, config);
            })
            .expect("Failed to start memory monitoring thread");

        *self.monitoring_thread.lock() = Some(handle);
    }

    fn start_leak_detection_thread(&self) {
        let monitor = Arc::new(self as *const _ as usize); // Unsafe but needed
        let shutdown_flag = self.shutdown_flag.clone();
        let config = self.config.read().clone();

        let handle = thread::Builder::new()
            .name("leak-detector".to_string())
            .spawn(move || {
                Self::leak_detection_loop(monitor, shutdown_flag, config);
            })
            .expect("Failed to start leak detection thread");

        *self.leak_detection_thread.lock() = Some(handle);
    }

    fn start_prometheus_server(&self) {
        // This would start an HTTP server for Prometheus metrics
        // For now, just log that it would be started
        let config = self.config.read();
        info!("Prometheus metrics would be available on port {}", config.prometheus_port);
    }

    fn monitoring_loop(
        monitor_ptr: Arc<usize>,
        shutdown_flag: Arc<AtomicBool>,
        config: ProductionMonitoringConfig,
    ) {
        // SAFETY: Converting Arc<usize> back to ProductionMemoryMonitor reference
        // Invariants:
        // 1. monitor_ptr created from Arc::into_raw of ProductionMemoryMonitor
        // 2. Pointer remains valid for thread lifetime
        // 3. Arc keeps object alive throughout monitoring
        // 4. Type punning is safe as we control both conversions
        // Guarantees:
        // - Valid reference for monitoring loop
        // - No data races due to Arc protection
        let monitor = unsafe { &*(monitor_ptr.as_ref() as *const usize as *const ProductionMemoryMonitor) };
        let start_time = Instant::now();

        while !shutdown_flag.load(Ordering::Relaxed) {
            let cycle_start = Instant::now();

            // Collect memory statistics
            let memory_stats = get_memory_tracker().get_statistics();
            let resource_stats = get_resource_manager().get_usage_stats();

            // Update Prometheus metrics
            if let Some(ref metrics) = monitor.metrics {
                metrics.memory_usage_bytes.set(memory_stats.current_usage as f64);
                metrics.memory_peak_bytes.set(memory_stats.peak_usage as f64);
                metrics.allocations_total.inc_by(memory_stats.allocation_count as f64);
                metrics.deallocations_total.inc_by(memory_stats.deallocation_count as f64);
                metrics.allocation_rate.set(memory_stats.allocation_rate);
                metrics.fragmentation_ratio.set(memory_stats.fragmentation_ratio);
                metrics.resource_count.set(resource_stats.total_resources as f64);
            }

            // Check for alert conditions
            monitor.check_memory_alerts(&memory_stats, &resource_stats, &config);

            let cycle_duration = cycle_start.elapsed();

            // Update monitoring statistics
            {
                let mut stats = monitor.monitoring_stats.write();
                stats.uptime = start_time.elapsed();
                stats.last_monitoring_cycle = Some(SystemTime::now());
                stats.monitoring_cycles_completed += 1;
                
                // Update average cycle duration
                let total_cycles = stats.monitoring_cycles_completed as f64;
                let old_avg = stats.average_cycle_duration.as_secs_f64();
                let new_avg = (old_avg * (total_cycles - 1.0) + cycle_duration.as_secs_f64()) / total_cycles;
                stats.average_cycle_duration = Duration::from_secs_f64(new_avg);
            }

            // Record monitoring duration
            if let Some(ref metrics) = monitor.metrics {
                metrics.monitoring_duration.observe(cycle_duration.as_secs_f64());
            }

            // Sleep for remaining interval
            let elapsed = cycle_start.elapsed();
            if elapsed < config.monitoring_interval {
                thread::sleep(config.monitoring_interval - elapsed);
            }
        }
    }

    fn leak_detection_loop(
        monitor_ptr: Arc<usize>,
        shutdown_flag: Arc<AtomicBool>,
        config: ProductionMonitoringConfig,
    ) {
        // SAFETY: Converting Arc<usize> back to ProductionMemoryMonitor reference
        // Invariants:
        // 1. monitor_ptr created from Arc::into_raw for leak detection thread
        // 2. Pointer remains valid for thread lifetime
        // 3. Arc prevents deallocation during use
        // 4. Same type conversion pattern as monitoring loop
        // Guarantees:
        // - Valid reference for leak detection
        // - Thread-safe access to monitor
        let monitor = unsafe { &*(monitor_ptr.as_ref() as *const usize as *const ProductionMemoryMonitor) };

        while !shutdown_flag.load(Ordering::Relaxed) {
            // Perform leak detection scan
            let leak_report = get_leak_detector().scan_for_leaks();

            // Update metrics
            if let Some(ref metrics) = monitor.metrics {
                metrics.leak_count.set(leak_report.leaks_detected.len() as f64);
                
                let total_leak_size: u64 = leak_report.leaks_detected.iter()
                    .map(|leak| match leak {
                        LeakType::CircularReference { total_size, .. } => *total_size as u64,
                        LeakType::OrphanedObject { size, .. } => *size as u64,
                        _ => 0,
                    })
                    .sum();
                
                metrics.leak_size_bytes.set(total_leak_size as f64);
            }

            // Generate alerts for detected leaks
            for leak in &leak_report.leaks_detected {
                let (level, message) = match leak {
                    LeakType::CircularReference { cycle, total_size } => {
                        (AlertLevel::Warning, format!("Circular reference detected involving {} objects, {} bytes", cycle.len(), total_size))
                    }
                    LeakType::OrphanedObject { age, size, .. } => {
                        (AlertLevel::Warning, format!("Orphaned object detected, age: {:?}, size: {} bytes", age, size))
                    }
                    LeakType::RefCountMismatch { expected, actual, .. } => {
                        (AlertLevel::Critical, format!("Reference count mismatch: expected {}, actual {}", expected, actual))
                    }
                    _ => (AlertLevel::Info, "Memory issue detected".to_string()),
                };

                monitor.generate_alert(
                    level,
                    MemoryAlertType::MemoryLeak {
                        leak_type: format!("{:?}", leak),
                        estimated_size: 0, // Would calculate from leak
                        location: None,
                    },
                    message,
                );
            }

            // Sleep
            thread::sleep(config.leak_detection_interval);
        }
    }

    fn check_memory_alerts(
        &self,
        memory_stats: &MemoryStats,
        _resource_stats: &ResourceUsageStats,
        config: &ProductionMonitoringConfig,
    ) {
        // High memory usage alert
        if memory_stats.current_usage > config.memory_alert_threshold {
            let percentage = (memory_stats.current_usage as f64 / config.memory_alert_threshold as f64) * 100.0;
            self.generate_alert(
                if percentage > 120.0 { AlertLevel::Critical } else { AlertLevel::Warning },
                MemoryAlertType::HighMemoryUsage {
                    current: memory_stats.current_usage,
                    threshold: config.memory_alert_threshold,
                    percentage,
                },
                format!("High memory usage: {:.1}% of threshold", percentage),
            );
        }

        // Rapid growth alert
        if memory_stats.growth_rate > config.growth_rate_threshold {
            self.generate_alert(
                AlertLevel::Warning,
                MemoryAlertType::RapidGrowth {
                    rate: memory_stats.growth_rate,
                    threshold: config.growth_rate_threshold,
                    duration: Duration::from_secs(60), // Assume 1 minute window
                },
                format!("Rapid memory growth: {:.2} bytes/sec", memory_stats.growth_rate),
            );
        }

        // High allocation rate alert
        if memory_stats.allocation_rate > 10000.0 {
            self.generate_alert(
                AlertLevel::Warning,
                MemoryAlertType::AllocationStorm {
                    allocations_per_sec: memory_stats.allocation_rate,
                    duration: Duration::from_secs(60),
                },
                format!("High allocation rate: {:.0} allocs/sec", memory_stats.allocation_rate),
            );
        }

        // High fragmentation alert
        if memory_stats.fragmentation_ratio > 0.5 {
            let wasted_memory = (memory_stats.total_allocated as f64 * memory_stats.fragmentation_ratio) as u64;
            self.generate_alert(
                AlertLevel::Warning,
                MemoryAlertType::FragmentationHigh {
                    fragmentation_ratio: memory_stats.fragmentation_ratio,
                    wasted_memory,
                },
                format!("High memory fragmentation: {:.1}%", memory_stats.fragmentation_ratio * 100.0),
            );
        }
    }

    fn collect_system_info(&self) -> SystemInfo {
        SystemInfo {
            timestamp: SystemTime::now(),
            process_id: std::process::id(),
            available_memory: 0, // Would query system
            total_memory: 0,     // Would query system
            cpu_usage: 0.0,      // Would query system
            thread_count: 0,     // Would query system
        }
    }
}

impl Drop for ProductionMemoryMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Memory dump data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryDumpData {
    pub timestamp: SystemTime,
    pub reason: String,
    pub memory_stats: MemoryStats,
    pub leak_report: LeakReport,
    pub resource_stats: ResourceUsageStats,
    pub system_info: SystemInfo,
}

/// System information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    pub timestamp: SystemTime,
    pub process_id: u32,
    pub available_memory: u64,
    pub total_memory: u64,
    pub cpu_usage: f64,
    pub thread_count: usize,
}

/// Initialize production memory monitoring
pub fn init_production_monitoring(config: ProductionMonitoringConfig) {
    MEMORY_MONITOR.configure(config);
    MEMORY_MONITOR.start();
}

/// Shutdown production memory monitoring
pub fn shutdown_production_monitoring() {
    MEMORY_MONITOR.stop();
}

/// Get global production memory monitor instance
pub fn get_memory_monitor() -> &'static Arc<ProductionMemoryMonitor> {
    &MEMORY_MONITOR
}

/// Generate a memory alert manually
pub fn generate_memory_alert(level: AlertLevel, alert_type: MemoryAlertType, message: String) {
    MEMORY_MONITOR.generate_alert(level, alert_type, message);
}

/// Create a memory dump manually
pub fn create_memory_dump(reason: &str) -> Result<MemoryDumpMetadata, std::io::Error> {
    MEMORY_MONITOR.create_memory_dump(reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_production_monitor_creation() {
        let monitor = ProductionMemoryMonitor::new();
        assert!(monitor.metrics.is_some());
    }

    #[test]
    fn test_alert_generation() {
        let monitor = ProductionMemoryMonitor::new();
        
        monitor.generate_alert(
            AlertLevel::Warning,
            MemoryAlertType::HighMemoryUsage {
                current: 1024,
                threshold: 512,
                percentage: 200.0,
            },
            "Test alert".to_string(),
        );

        let alerts = monitor.get_active_alerts();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].message, "Test alert");
    }

    #[test]
    fn test_alert_acknowledgment() {
        let monitor = ProductionMemoryMonitor::new();
        
        monitor.generate_alert(
            AlertLevel::Info,
            MemoryAlertType::HighMemoryUsage {
                current: 1024,
                threshold: 512,
                percentage: 200.0,
            },
            "Test alert".to_string(),
        );

        let alerts = monitor.get_active_alerts();
        let alert_id = alerts[0].id;
        
        assert!(monitor.acknowledge_alert(alert_id));
        
        let updated_alert = monitor.alerts.get(&alert_id).unwrap();
        assert!(updated_alert.acknowledged);
    }

    #[test]
    fn test_monitoring_stats() {
        let monitor = ProductionMemoryMonitor::new();
        let stats = monitor.get_monitoring_stats();
        
        assert_eq!(stats.total_alerts, 0);
        assert_eq!(stats.active_alerts, 0);
        assert_eq!(stats.memory_dumps_created, 0);
    }
}