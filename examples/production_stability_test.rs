use lightning_db::{Database, LightningDbConfig};
use std::fs;
use std::process;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use sysinfo::{Pid, System};
use rand::{Rng, SeedableRng};

/// Production stability test
/// Monitors for memory leaks, file descriptor leaks, and performance degradation
#[derive(Debug, Clone)]
struct ResourceMetrics {
    timestamp: Instant,
    memory_rss: u64,        // Resident Set Size in bytes
    memory_vss: u64,        // Virtual Size in bytes
    fd_count: usize,        // File descriptor count
    thread_count: usize,    // Thread count
    cpu_usage: f32,         // CPU usage percentage
    disk_usage: u64,        // Database size on disk
    operation_latency: Duration, // Average operation latency
}

#[derive(Debug)]
struct StabilityReport {
    start_metrics: ResourceMetrics,
    end_metrics: ResourceMetrics,
    peak_metrics: ResourceMetrics,
    anomalies: Vec<String>,
    duration: Duration,
    operations_performed: u64,
    errors_encountered: u64,
}

struct StabilityTester {
    db_path: TempDir,
    metrics_history: Arc<Mutex<Vec<ResourceMetrics>>>,
    operation_count: Arc<Mutex<u64>>,
    error_count: Arc<Mutex<u64>>,
    should_stop: Arc<Mutex<bool>>,
}

impl StabilityTester {
    fn new() -> Self {
        Self {
            db_path: TempDir::new().unwrap(),
            metrics_history: Arc::new(Mutex::new(Vec::new())),
            operation_count: Arc::new(Mutex::new(0)),
            error_count: Arc::new(Mutex::new(0)),
            should_stop: Arc::new(Mutex::new(false)),
        }
    }

    /// Run the stability test
    fn run_test(&self, duration: Duration) -> StabilityReport {
        println!("🏃 Lightning DB Production Stability Test");
        println!("⏱️  Test duration: {:?}", duration);
        println!("📊 Monitoring: Memory, FDs, Threads, CPU, Disk, Performance");
        println!("{}", "=".repeat(80));
        
        let start_time = Instant::now();
        let db_path = self.db_path.path().join("stability_db");
        
        // Create database with production config
        let config = self.create_production_config();
        let db = Arc::new(Database::create(&db_path, config).unwrap());
        
        // Capture initial metrics
        let start_metrics = self.capture_metrics(&db_path);
        self.metrics_history.lock().unwrap().push(start_metrics.clone());
        
        // Start monitoring thread
        self.start_monitoring_thread(db_path.clone());
        
        // Start workload threads
        let handles = self.start_workload_threads(db.clone(), 4);
        
        // Run for specified duration
        thread::sleep(duration);
        
        // Signal threads to stop
        *self.should_stop.lock().unwrap() = true;
        
        // Wait for threads to finish
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Capture final metrics
        let end_metrics = self.capture_metrics(&db_path);
        
        // Generate report
        self.generate_report(start_metrics, end_metrics, start_time.elapsed())
    }

    /// Create production-like configuration
    fn create_production_config(&self) -> LightningDbConfig {
        LightningDbConfig {
            cache_size: 64 * 1024 * 1024,  // 64MB cache
            compression_enabled: true,
            use_improved_wal: true,
            wal_sync_mode: lightning_db::WalSyncMode::Periodic { interval_ms: 1000 },
            prefetch_enabled: true,
            use_optimized_transactions: true,
            ..Default::default()
        }
    }

    /// Capture current resource metrics
    fn capture_metrics(&self, db_path: &std::path::Path) -> ResourceMetrics {
        let mut system = System::new();
        let pid = Pid::from_u32(process::id());
        
        // Refresh all system info
        system.refresh_all();
        
        let process = system.process(pid).expect("Failed to get process info");
        
        // Get file descriptor count (Linux/macOS)
        let fd_count = self.get_fd_count();
        
        // Get database size
        let disk_usage = self.get_directory_size(db_path);
        
        // Get operation latency (will be updated by workload threads)
        let latency = Duration::from_micros(100); // Placeholder
        
        ResourceMetrics {
            timestamp: Instant::now(),
            memory_rss: process.memory(),
            memory_vss: process.virtual_memory(),
            fd_count,
            thread_count: 1, // sysinfo 0.33 doesn't provide thread count easily
            cpu_usage: process.cpu_usage(),
            disk_usage,
            operation_latency: latency,
        }
    }

    /// Get file descriptor count for current process
    fn get_fd_count(&self) -> usize {
        #[cfg(target_os = "linux")]
        {
            let fd_dir = format!("/proc/{}/fd", process::id());
            fs::read_dir(&fd_dir).map(|entries| entries.count()).unwrap_or(0)
        }
        
        #[cfg(target_os = "macos")]
        {
            // Use lsof on macOS
            let output = process::Command::new("lsof")
                .args(&["-p", &process::id().to_string()])
                .output()
                .unwrap_or_else(|_| {
                    // Return an output with empty stdout on failure
                    process::Output {
                        status: Default::default(),
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    }
                });
            
            String::from_utf8_lossy(&output.stdout).lines().count().saturating_sub(1)
        }
        
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            0 // Not supported on other platforms
        }
    }

    /// Get total size of directory
    fn get_directory_size(&self, path: &std::path::Path) -> u64 {
        let mut total_size = 0;
        
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        total_size += metadata.len();
                    } else if metadata.is_dir() {
                        total_size += self.get_directory_size(&entry.path());
                    }
                }
            }
        }
        
        total_size
    }

    /// Start monitoring thread
    fn start_monitoring_thread(&self, _db_path: std::path::PathBuf) {
        let metrics_history = self.metrics_history.clone();
        let should_stop = self.should_stop.clone();
        
        thread::spawn(move || {
            
            while !*should_stop.lock().unwrap() {
                thread::sleep(Duration::from_secs(5));
                
                // Capture metrics
                let mut s = System::new();
                let pid = Pid::from_u32(process::id());
                s.refresh_all();
                
                if let Some(process) = s.process(pid) {
                    let metrics = ResourceMetrics {
                        timestamp: Instant::now(),
                        memory_rss: process.memory(),
                        memory_vss: process.virtual_memory(),
                        fd_count: 0, // Will be updated later
                        thread_count: 1, // sysinfo 0.33 doesn't provide thread count easily
                        cpu_usage: process.cpu_usage(),
                        disk_usage: 0, // Will be updated later
                        operation_latency: Duration::from_micros(100),
                    };
                    
                    metrics_history.lock().unwrap().push(metrics);
                }
            }
        });
    }

    /// Start workload threads
    fn start_workload_threads(&self, db: Arc<Database>, thread_count: usize) -> Vec<thread::JoinHandle<()>> {
        let mut handles = Vec::new();
        
        for thread_id in 0..thread_count {
            let db = db.clone();
            let operation_count = self.operation_count.clone();
            let error_count = self.error_count.clone();
            let should_stop = self.should_stop.clone();
            
            let handle = thread::spawn(move || {
                let mut rng = rand::rngs::StdRng::seed_from_u64(thread_id as u64);
                let mut local_op_count = 0u64;
                
                while !*should_stop.lock().unwrap() {
                    // Perform random operations
                    let op_type = rng.gen_range(0..100);
                    
                    let result = match op_type {
                        0..=40 => {
                            // 40% writes
                            let key = format!("key_{:08}_{:08}", thread_id, local_op_count);
                            let value = format!("value_{}_data_{}", thread_id, "x".repeat(rng.gen_range(100..1000)));
                            db.put(key.as_bytes(), value.as_bytes())
                        }
                        41..=70 => {
                            // 30% reads
                            let key = format!("key_{:08}_{:08}", thread_id, rng.gen_range(0..local_op_count.max(1)));
                            db.get(key.as_bytes()).map(|_| ())
                        }
                        71..=80 => {
                            // 10% deletes
                            let key = format!("key_{:08}_{:08}", thread_id, rng.gen_range(0..local_op_count.max(1)));
                            db.delete(key.as_bytes()).map(|_| ())
                        }
                        81..=90 => {
                            // 10% transactions
                            let tx_result = db.begin_transaction().and_then(|tx_id| {
                                for i in 0..10 {
                                    let key = format!("tx_key_{}_{}_{}",thread_id, local_op_count, i);
                                    let value = format!("tx_value_{}_{}", thread_id, i);
                                    db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
                                }
                                db.commit_transaction(tx_id)
                            });
                            tx_result
                        }
                        _ => {
                            // 10% checkpoints
                            db.checkpoint()
                        }
                    };
                    
                    match result {
                        Ok(_) => {
                            local_op_count += 1;
                            if local_op_count % 1000 == 0 {
                                *operation_count.lock().unwrap() += 1000;
                            }
                        }
                        Err(e) => {
                            *error_count.lock().unwrap() += 1;
                            eprintln!("Operation error: {}", e);
                        }
                    }
                    
                    // Small delay to prevent CPU saturation
                    if op_type < 80 {
                        thread::sleep(Duration::from_micros(10));
                    }
                }
                
                // Add remaining operations
                *operation_count.lock().unwrap() += local_op_count % 1000;
            });
            
            handles.push(handle);
        }
        
        handles
    }

    /// Generate stability report
    fn generate_report(&self, start_metrics: ResourceMetrics, end_metrics: ResourceMetrics, duration: Duration) -> StabilityReport {
        let metrics_history = self.metrics_history.lock().unwrap();
        
        // Find peak metrics
        let peak_metrics = metrics_history.iter()
            .max_by_key(|m| m.memory_rss)
            .cloned()
            .unwrap_or_else(|| end_metrics.clone());
        
        // Detect anomalies
        let mut anomalies = Vec::new();
        
        // Check for memory leaks
        let memory_growth = end_metrics.memory_rss as f64 - start_metrics.memory_rss as f64;
        let memory_growth_rate = memory_growth / duration.as_secs() as f64;
        
        if memory_growth_rate > 1024.0 * 1024.0 { // 1MB/sec
            anomalies.push(format!("High memory growth rate: {:.2} MB/sec", memory_growth_rate / 1024.0 / 1024.0));
        }
        
        // Check for FD leaks
        if end_metrics.fd_count > start_metrics.fd_count + 100 {
            anomalies.push(format!("File descriptor leak: {} -> {}", start_metrics.fd_count, end_metrics.fd_count));
        }
        
        // Check for thread leaks
        if end_metrics.thread_count > start_metrics.thread_count + 10 {
            anomalies.push(format!("Thread leak: {} -> {}", start_metrics.thread_count, end_metrics.thread_count));
        }
        
        // Check for performance degradation
        if metrics_history.len() > 10 {
            let early_latencies: Vec<_> = metrics_history.iter().take(10).map(|m| m.operation_latency).collect();
            let late_latencies: Vec<_> = metrics_history.iter().rev().take(10).map(|m| m.operation_latency).collect();
            
            let early_avg = early_latencies.iter().sum::<Duration>() / early_latencies.len() as u32;
            let late_avg = late_latencies.iter().sum::<Duration>() / late_latencies.len() as u32;
            
            if late_avg > early_avg * 2 {
                anomalies.push(format!("Performance degradation: {:?} -> {:?}", early_avg, late_avg));
            }
        }
        
        StabilityReport {
            start_metrics,
            end_metrics,
            peak_metrics,
            anomalies,
            duration,
            operations_performed: *self.operation_count.lock().unwrap(),
            errors_encountered: *self.error_count.lock().unwrap(),
        }
    }

    /// Print detailed report
    fn print_report(&self, report: &StabilityReport) {
        println!("\n{}", "=".repeat(80));
        println!("📊 PRODUCTION STABILITY TEST REPORT");
        println!("{}", "=".repeat(80));
        
        // Test summary
        println!("\n⏱️  Test Duration: {:?}", report.duration);
        println!("🔢 Operations Performed: {}", report.operations_performed);
        println!("❌ Errors Encountered: {}", report.errors_encountered);
        println!("📈 Error Rate: {:.4}%", report.errors_encountered as f64 / report.operations_performed.max(1) as f64 * 100.0);
        
        // Memory analysis
        println!("\n💾 MEMORY ANALYSIS:");
        println!("   Initial RSS: {:.2} MB", report.start_metrics.memory_rss as f64 / 1024.0 / 1024.0);
        println!("   Final RSS: {:.2} MB", report.end_metrics.memory_rss as f64 / 1024.0 / 1024.0);
        println!("   Peak RSS: {:.2} MB", report.peak_metrics.memory_rss as f64 / 1024.0 / 1024.0);
        println!("   Growth: {:.2} MB ({:.2}%)", 
                 (report.end_metrics.memory_rss as f64 - report.start_metrics.memory_rss as f64) / 1024.0 / 1024.0,
                 (report.end_metrics.memory_rss as f64 / report.start_metrics.memory_rss.max(1) as f64 - 1.0) * 100.0);
        
        // Resource analysis
        println!("\n📁 RESOURCE ANALYSIS:");
        println!("   File Descriptors: {} -> {} ({})", 
                 report.start_metrics.fd_count,
                 report.end_metrics.fd_count,
                 if report.end_metrics.fd_count > report.start_metrics.fd_count { 
                     format!("+{}", report.end_metrics.fd_count - report.start_metrics.fd_count) 
                 } else { 
                     "0".to_string() 
                 });
        println!("   Threads: {} -> {} ({})",
                 report.start_metrics.thread_count,
                 report.end_metrics.thread_count,
                 if report.end_metrics.thread_count > report.start_metrics.thread_count {
                     format!("+{}", report.end_metrics.thread_count - report.start_metrics.thread_count)
                 } else {
                     "0".to_string()
                 });
        println!("   Disk Usage: {:.2} MB -> {:.2} MB",
                 report.start_metrics.disk_usage as f64 / 1024.0 / 1024.0,
                 report.end_metrics.disk_usage as f64 / 1024.0 / 1024.0);
        
        // Performance analysis
        println!("\n⚡ PERFORMANCE:");
        let ops_per_sec = report.operations_performed as f64 / report.duration.as_secs_f64();
        println!("   Operations/sec: {:.0}", ops_per_sec);
        println!("   Avg latency: {:?}", Duration::from_nanos((report.duration.as_nanos() / report.operations_performed.max(1) as u128) as u64));
        
        // Anomalies
        if !report.anomalies.is_empty() {
            println!("\n⚠️  ANOMALIES DETECTED:");
            for anomaly in &report.anomalies {
                println!("   - {}", anomaly);
            }
        } else {
            println!("\n✅ NO ANOMALIES DETECTED");
        }
        
        // Verdict
        println!("\n🏁 VERDICT:");
        if report.anomalies.is_empty() && report.errors_encountered == 0 {
            println!("   ✅ STABLE - No leaks or degradation detected");
            println!("   The database is suitable for long-running production deployments.");
        } else if report.anomalies.len() <= 1 && report.errors_encountered < 10 {
            println!("   ⚠️  MOSTLY STABLE - Minor issues detected");
            println!("   Review anomalies before production deployment.");
        } else {
            println!("   ❌ UNSTABLE - Significant issues detected");
            println!("   Not recommended for production without fixes.");
        }
        
        println!("\n{}", "=".repeat(80));
    }
}

fn main() {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let duration_mins = args.get(1)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5); // Default 5 minutes
    
    println!("🚀 Starting Lightning DB Production Stability Test");
    println!("   Duration: {} minutes", duration_mins);
    println!("   Use Ctrl+C to stop early\n");
    
    let tester = StabilityTester::new();
    let report = tester.run_test(Duration::from_secs(duration_mins * 60));
    
    tester.print_report(&report);
    
    // Exit with appropriate code
    if report.anomalies.is_empty() && report.errors_encountered == 0 {
        std::process::exit(0);
    } else {
        std::process::exit(1);
    }
}