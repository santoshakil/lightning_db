use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::collections::HashMap;

/// Comprehensive monitoring and alerting setup for Lightning DB
/// 
/// This example demonstrates:
/// - Real-time metrics collection
/// - Health monitoring with thresholds
/// - Performance tracking and alerting
/// - Resource usage monitoring
/// - Custom alert conditions
/// - Prometheus-style metrics export
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📊 Lightning DB Monitoring & Alerting Setup");
    println!("=".repeat(50));
    
    // Create database
    let config = LightningDbConfig {
        cache_size: 256 * 1024 * 1024, // 256MB
        max_active_transactions: 5000,
        use_improved_wal: true,
        ..Default::default()
    };
    
    let db = Arc::new(Database::create("/tmp/lightning_db_monitoring", config)?);
    println!("✅ Database created");
    
    // Initialize monitoring system
    let monitor = Arc::new(DatabaseMonitor::new());
    
    // Start monitoring loops
    let should_stop = Arc::new(AtomicBool::new(false));
    
    start_metrics_collection(Arc::clone(&db), Arc::clone(&monitor), Arc::clone(&should_stop))?;
    start_health_monitoring(Arc::clone(&db), Arc::clone(&monitor), Arc::clone(&should_stop))?;
    start_alert_engine(Arc::clone(&monitor), Arc::clone(&should_stop))?;
    
    // Simulate workload to generate metrics
    println!("\n🔥 Starting workload to generate monitoring data...");
    simulate_workload(Arc::clone(&db), Arc::clone(&monitor))?;
    
    // Run for a bit to collect data
    thread::sleep(Duration::from_secs(5));
    
    // Display monitoring dashboard
    println!("\n📊 MONITORING DASHBOARD");
    println!("=".repeat(50));
    display_dashboard(&monitor);
    
    // Export Prometheus metrics
    println!("\n📈 PROMETHEUS METRICS EXPORT");
    println!("=".repeat(30));
    export_prometheus_metrics(&monitor);
    
    // Check alerts
    println!("\n🚨 ACTIVE ALERTS");
    println!("=".repeat(20));
    display_alerts(&monitor);
    
    // Graceful shutdown
    should_stop.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(100));
    
    println!("\n✅ Monitoring setup demonstration completed!");
    println!("🎯 Ready for production monitoring deployment");
    
    Ok(())
}

/// Database monitoring system
pub struct DatabaseMonitor {
    // Metrics
    pub operations_total: AtomicU64,
    pub operations_per_second: AtomicU64,
    pub errors_total: AtomicU64,
    pub latency_sum_us: AtomicU64,
    pub latency_count: AtomicU64,
    
    // Health status
    pub last_health_check: AtomicU64,
    pub health_status: AtomicBool, // true = healthy
    
    // Performance metrics
    pub cache_hit_rate: AtomicU64, // As percentage * 100
    pub active_transactions: AtomicU64,
    pub memory_usage_mb: AtomicU64,
    
    // Alerts
    pub alerts: parking_lot::RwLock<Vec<Alert>>,
    
    // Thresholds
    pub max_latency_us: u64,
    pub min_ops_per_sec: u64,
    pub max_error_rate: f64,
    pub max_memory_mb: u64,
}

#[derive(Debug, Clone)]
pub struct Alert {
    pub id: String,
    pub severity: AlertSeverity,
    pub message: String,
    pub timestamp: u64,
    pub metric_value: f64,
    pub threshold: f64,
}

#[derive(Debug, Clone)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl DatabaseMonitor {
    pub fn new() -> Self {
        Self {
            operations_total: AtomicU64::new(0),
            operations_per_second: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            
            last_health_check: AtomicU64::new(0),
            health_status: AtomicBool::new(true),
            
            cache_hit_rate: AtomicU64::new(0),
            active_transactions: AtomicU64::new(0),
            memory_usage_mb: AtomicU64::new(0),
            
            alerts: parking_lot::RwLock::new(Vec::new()),
            
            // Default thresholds
            max_latency_us: 10_000, // 10ms
            min_ops_per_sec: 1_000,
            max_error_rate: 0.01, // 1%
            max_memory_mb: 2048, // 2GB
        }
    }
    
    pub fn record_operation(&self, latency_us: u64, success: bool) {
        self.operations_total.fetch_add(1, Ordering::Relaxed);
        
        if success {
            self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
            self.latency_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    pub fn update_performance_metrics(&self, ops_per_sec: u64, cache_hit_rate: f64, active_txns: u64, memory_mb: u64) {
        self.operations_per_second.store(ops_per_sec, Ordering::Relaxed);
        self.cache_hit_rate.store((cache_hit_rate * 10000.0) as u64, Ordering::Relaxed); // Store as basis points
        self.active_transactions.store(active_txns, Ordering::Relaxed);
        self.memory_usage_mb.store(memory_mb, Ordering::Relaxed);
    }
    
    pub fn add_alert(&self, alert: Alert) {
        let mut alerts = self.alerts.write();
        alerts.push(alert);
        
        // Keep only last 100 alerts
        if alerts.len() > 100 {
            alerts.remove(0);
        }
    }
    
    pub fn get_average_latency_us(&self) -> f64 {
        let sum = self.latency_sum_us.load(Ordering::Relaxed);
        let count = self.latency_count.load(Ordering::Relaxed);
        
        if count > 0 {
            sum as f64 / count as f64
        } else {
            0.0
        }
    }
    
    pub fn get_error_rate(&self) -> f64 {
        let total_ops = self.operations_total.load(Ordering::Relaxed);
        let errors = self.errors_total.load(Ordering::Relaxed);
        
        if total_ops > 0 {
            errors as f64 / total_ops as f64
        } else {
            0.0
        }
    }
}

/// Start metrics collection loop
fn start_metrics_collection(
    db: Arc<Database>,
    monitor: Arc<DatabaseMonitor>,
    should_stop: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    thread::spawn(move || {
        let mut last_ops = 0;
        let mut last_time = Instant::now();
        
        while !should_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));
            
            if should_stop.load(Ordering::Relaxed) {
                break;
            }
            
            // Calculate ops per second
            let current_ops = monitor.operations_total.load(Ordering::Relaxed);
            let current_time = Instant::now();
            let duration = current_time.duration_since(last_time);
            
            if duration.as_secs() > 0 {
                let ops_per_sec = ((current_ops - last_ops) as f64 / duration.as_secs_f64()) as u64;
                
                // Get database statistics
                if let Ok(stats) = db.get_statistics() {
                    // Simulate cache hit rate (in real implementation, this would come from actual cache stats)
                    let cache_hit_rate = 0.85; // 85% hit rate
                    let active_txns = stats.active_transactions as u64;
                    let memory_mb = 128; // Simulated memory usage
                    
                    monitor.update_performance_metrics(ops_per_sec, cache_hit_rate, active_txns, memory_mb);
                }
                
                last_ops = current_ops;
                last_time = current_time;
            }
        }
    });
    
    Ok(())
}

/// Start health monitoring loop
fn start_health_monitoring(
    db: Arc<Database>,
    monitor: Arc<DatabaseMonitor>,
    should_stop: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    thread::spawn(move || {
        while !should_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(10)); // Health check every 10 seconds
            
            if should_stop.load(Ordering::Relaxed) {
                break;
            }
            
            // Perform health check
            let start = Instant::now();
            let healthy = perform_health_check(&db).unwrap_or(false);
            let latency = start.elapsed().as_micros() as u64;
            
            monitor.health_status.store(healthy, Ordering::Relaxed);
            monitor.last_health_check.store(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                Ordering::Relaxed
            );
            
            // Record health check as operation
            monitor.record_operation(latency, healthy);
            
            if !healthy {
                let alert = Alert {
                    id: "health_check_failed".to_string(),
                    severity: AlertSeverity::Critical,
                    message: "Database health check failed".to_string(),
                    timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    metric_value: 0.0,
                    threshold: 1.0,
                };
                monitor.add_alert(alert);
            }
        }
    });
    
    Ok(())
}

/// Start alert monitoring engine
fn start_alert_engine(
    monitor: Arc<DatabaseMonitor>,
    should_stop: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    thread::spawn(move || {
        while !should_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(5)); // Check alerts every 5 seconds
            
            if should_stop.load(Ordering::Relaxed) {
                break;
            }
            
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            
            // Check latency threshold
            let avg_latency = monitor.get_average_latency_us();
            if avg_latency > monitor.max_latency_us as f64 {
                let alert = Alert {
                    id: "high_latency".to_string(),
                    severity: if avg_latency > monitor.max_latency_us as f64 * 2.0 { 
                        AlertSeverity::Critical 
                    } else { 
                        AlertSeverity::Warning 
                    },
                    message: format!("High average latency: {:.1}μs", avg_latency),
                    timestamp: now,
                    metric_value: avg_latency,
                    threshold: monitor.max_latency_us as f64,
                };
                monitor.add_alert(alert);
            }
            
            // Check operations per second threshold
            let ops_per_sec = monitor.operations_per_second.load(Ordering::Relaxed);
            if ops_per_sec < monitor.min_ops_per_sec && ops_per_sec > 0 {
                let alert = Alert {
                    id: "low_throughput".to_string(),
                    severity: AlertSeverity::Warning,
                    message: format!("Low throughput: {} ops/sec", ops_per_sec),
                    timestamp: now,
                    metric_value: ops_per_sec as f64,
                    threshold: monitor.min_ops_per_sec as f64,
                };
                monitor.add_alert(alert);
            }
            
            // Check error rate threshold
            let error_rate = monitor.get_error_rate();
            if error_rate > monitor.max_error_rate {
                let alert = Alert {
                    id: "high_error_rate".to_string(),
                    severity: if error_rate > monitor.max_error_rate * 5.0 { 
                        AlertSeverity::Critical 
                    } else { 
                        AlertSeverity::Warning 
                    },
                    message: format!("High error rate: {:.2}%", error_rate * 100.0),
                    timestamp: now,
                    metric_value: error_rate,
                    threshold: monitor.max_error_rate,
                };
                monitor.add_alert(alert);
            }
            
            // Check memory usage threshold
            let memory_mb = monitor.memory_usage_mb.load(Ordering::Relaxed);
            if memory_mb > monitor.max_memory_mb {
                let alert = Alert {
                    id: "high_memory_usage".to_string(),
                    severity: AlertSeverity::Warning,
                    message: format!("High memory usage: {}MB", memory_mb),
                    timestamp: now,
                    metric_value: memory_mb as f64,
                    threshold: monitor.max_memory_mb as f64,
                };
                monitor.add_alert(alert);
            }
        }
    });
    
    Ok(())
}

/// Perform basic health check
fn perform_health_check(db: &Database) -> Result<bool, Box<dyn std::error::Error>> {
    // Basic read/write test
    db.put(b"__health__", b"ok")?;
    let value = db.get(b"__health__")?;
    db.delete(b"__health__")?;
    
    Ok(value.as_deref() == Some(b"ok"))
}

/// Simulate workload to generate monitoring data
fn simulate_workload(
    db: Arc<Database>,
    monitor: Arc<DatabaseMonitor>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_operations = 10000;
    
    thread::spawn(move || {
        for i in 0..num_operations {
            let start = Instant::now();
            
            let key = format!("monitor_test_{}", i);
            let value = format!("data_{}", i);
            
            let success = match i % 10 {
                0..=7 => {
                    // 80% successful operations
                    db.put(key.as_bytes(), value.as_bytes()).is_ok()
                }
                8 => {
                    // 10% reads
                    db.get(key.as_bytes()).is_ok()
                }
                9 => {
                    // 10% simulate failures (by using invalid operation)
                    // For simulation, we'll just record as failure
                    false
                }
                _ => unreachable!(),
            };
            
            let latency = start.elapsed().as_micros() as u64;
            monitor.record_operation(latency, success);
            
            // Small delay to simulate realistic load
            if i % 100 == 0 {
                thread::sleep(Duration::from_micros(100));
            }
        }
    });
    
    Ok(())
}

/// Display monitoring dashboard
fn display_dashboard(monitor: &DatabaseMonitor) {
    let total_ops = monitor.operations_total.load(Ordering::Relaxed);
    let ops_per_sec = monitor.operations_per_second.load(Ordering::Relaxed);
    let errors = monitor.errors_total.load(Ordering::Relaxed);
    let avg_latency = monitor.get_average_latency_us();
    let error_rate = monitor.get_error_rate() * 100.0;
    let cache_hit_rate = monitor.cache_hit_rate.load(Ordering::Relaxed) as f64 / 100.0;
    let active_txns = monitor.active_transactions.load(Ordering::Relaxed);
    let memory_mb = monitor.memory_usage_mb.load(Ordering::Relaxed);
    let healthy = monitor.health_status.load(Ordering::Relaxed);
    
    println!("📊 Operations:");
    println!("  Total:        {}", total_ops);
    println!("  Per Second:   {}", ops_per_sec);
    println!("  Errors:       {} ({:.2}%)", errors, error_rate);
    
    println!("\n⚡ Performance:");
    println!("  Avg Latency:  {:.1}μs", avg_latency);
    println!("  Cache Hit:    {:.1}%", cache_hit_rate);
    println!("  Active Txns:  {}", active_txns);
    println!("  Memory:       {}MB", memory_mb);
    
    println!("\n💚 Health:");
    println!("  Status:       {}", if healthy { "HEALTHY" } else { "UNHEALTHY" });
    
    let alerts = monitor.alerts.read();
    println!("  Alerts:       {}", alerts.len());
}

/// Export Prometheus-style metrics
fn export_prometheus_metrics(monitor: &DatabaseMonitor) {
    let total_ops = monitor.operations_total.load(Ordering::Relaxed);
    let errors = monitor.errors_total.load(Ordering::Relaxed);
    let ops_per_sec = monitor.operations_per_second.load(Ordering::Relaxed);
    let avg_latency = monitor.get_average_latency_us();
    let cache_hit_rate = monitor.cache_hit_rate.load(Ordering::Relaxed) as f64 / 10000.0;
    let active_txns = monitor.active_transactions.load(Ordering::Relaxed);
    let memory_mb = monitor.memory_usage_mb.load(Ordering::Relaxed);
    
    println!("# HELP lightning_db_operations_total Total database operations");
    println!("# TYPE lightning_db_operations_total counter");
    println!("lightning_db_operations_total {}", total_ops);
    
    println!("# HELP lightning_db_errors_total Total database errors");
    println!("# TYPE lightning_db_errors_total counter");
    println!("lightning_db_errors_total {}", errors);
    
    println!("# HELP lightning_db_operations_per_second Current operations per second");
    println!("# TYPE lightning_db_operations_per_second gauge");
    println!("lightning_db_operations_per_second {}", ops_per_sec);
    
    println!("# HELP lightning_db_latency_microseconds Average operation latency");
    println!("# TYPE lightning_db_latency_microseconds gauge");
    println!("lightning_db_latency_microseconds {:.1}", avg_latency);
    
    println!("# HELP lightning_db_cache_hit_rate Cache hit rate");
    println!("# TYPE lightning_db_cache_hit_rate gauge");
    println!("lightning_db_cache_hit_rate {:.4}", cache_hit_rate);
    
    println!("# HELP lightning_db_active_transactions Current active transactions");
    println!("# TYPE lightning_db_active_transactions gauge");
    println!("lightning_db_active_transactions {}", active_txns);
    
    println!("# HELP lightning_db_memory_usage_mb Memory usage in megabytes");
    println!("# TYPE lightning_db_memory_usage_mb gauge");
    println!("lightning_db_memory_usage_mb {}", memory_mb);
}

/// Display active alerts
fn display_alerts(monitor: &DatabaseMonitor) {
    let alerts = monitor.alerts.read();
    
    if alerts.is_empty() {
        println!("✅ No active alerts");
        return;
    }
    
    // Show last 5 alerts
    let recent_alerts = alerts.iter().rev().take(5).collect::<Vec<_>>();
    
    for alert in recent_alerts {
        let severity_emoji = match alert.severity {
            AlertSeverity::Info => "ℹ️",
            AlertSeverity::Warning => "⚠️",
            AlertSeverity::Critical => "🚨",
        };
        
        let timestamp = chrono::DateTime::from_timestamp(alert.timestamp as i64, 0)
            .unwrap_or_default()
            .format("%H:%M:%S");
        
        println!("{} [{}] {}: {} (value: {:.1}, threshold: {:.1})", 
                 severity_emoji,
                 timestamp,
                 alert.id,
                 alert.message,
                 alert.metric_value,
                 alert.threshold);
    }
}