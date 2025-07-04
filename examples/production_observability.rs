use lightning_db::{Database, LightningDbConfig, observability::{Metrics, HealthStatus}};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use sysinfo::{System, Pid};
use std::process;

/// Production observability example
/// Demonstrates real-time metrics collection and monitoring
fn main() {
    println!("🔍 Lightning DB Production Observability Demo");
    println!("📊 Real-time metrics monitoring and alerting");
    println!("{}", "=".repeat(70));
    
    // Create database with production config
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("observability_db");
    
    let config = LightningDbConfig {
        cache_size: 32 * 1024 * 1024, // 32MB
        compression_enabled: true,
        use_improved_wal: true,
        ..Default::default()
    };
    
    let db = Arc::new(Database::create(&db_path, config).unwrap());
    let metrics = Arc::new(Metrics::new());
    
    // Start monitoring thread
    let metrics_clone = metrics.clone();
    let monitor_handle = thread::spawn(move || {
        monitor_metrics(metrics_clone);
    });
    
    // Start workload threads
    let mut handles = Vec::new();
    for thread_id in 0..4 {
        let db_clone = db.clone();
        let metrics_clone = metrics.clone();
        
        let handle = thread::spawn(move || {
            run_workload(thread_id, db_clone, metrics_clone);
        });
        
        handles.push(handle);
    }
    
    // Run for 30 seconds
    thread::sleep(Duration::from_secs(30));
    
    // Print final report
    print_final_report(&metrics);
    
    // Wait for threads to finish
    for handle in handles {
        handle.join().unwrap();
    }
    
    monitor_handle.join().unwrap();
}

/// Run database workload and record metrics
fn run_workload(thread_id: usize, db: Arc<Database>, metrics: Arc<Metrics>) {
    let mut rng = rand::rng();
    let start_time = Instant::now();
    
    while start_time.elapsed() < Duration::from_secs(30) {
        // Mix of operations
        use rand::Rng;
        let op_type = rng.random_range(0..100);
        
        match op_type {
            0..=40 => {
                // 40% writes
                let key = format!("key_{:08}_{:08}", thread_id, rng.random::<u32>());
                let value = format!("value_{}_{}", thread_id, "x".repeat(rng.random_range(100..1000)));
                
                let start = Instant::now();
                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => metrics.record_write(start.elapsed()),
                    Err(_) => metrics.record_error("write"),
                }
            }
            41..=80 => {
                // 40% reads
                let key = format!("key_{:08}_{:08}", thread_id, rng.random::<u32>());
                
                let start = Instant::now();
                match db.get(key.as_bytes()) {
                    Ok(Some(_)) => {
                        metrics.record_read(start.elapsed());
                        metrics.record_cache_hit();
                    }
                    Ok(None) => {
                        metrics.record_read(start.elapsed());
                        metrics.record_cache_miss();
                    }
                    Err(_) => metrics.record_error("read"),
                }
            }
            81..=90 => {
                // 10% deletes
                let key = format!("key_{:08}_{:08}", thread_id, rng.random::<u32>());
                
                let start = Instant::now();
                match db.delete(key.as_bytes()) {
                    Ok(_) => metrics.record_delete(start.elapsed()),
                    Err(_) => metrics.record_error("delete"),
                }
            }
            _ => {
                // 10% transactions
                let start = Instant::now();
                metrics.start_transaction();
                
                match db.begin_transaction() {
                    Ok(tx_id) => {
                        let mut ops = 0;
                        for i in 0..10 {
                            let key = format!("tx_key_{}_{}", thread_id, i);
                            let value = format!("tx_value_{}_{}", thread_id, i);
                            if db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                                ops += 1;
                            }
                        }
                        
                        match db.commit_transaction(tx_id) {
                            Ok(_) => metrics.record_transaction_commit(start.elapsed(), ops),
                            Err(_) => {
                                metrics.record_transaction_abort();
                                metrics.record_error("transaction");
                            }
                        }
                    }
                    Err(_) => {
                        metrics.record_transaction_abort();
                        metrics.record_error("transaction");
                    }
                }
            }
        }
        
        // Small delay to prevent CPU saturation
        thread::sleep(Duration::from_micros(10));
    }
}

/// Monitor metrics and print updates
fn monitor_metrics(metrics: Arc<Metrics>) {
    let mut last_snapshot = metrics.snapshot();
    let mut system = System::new();
    let pid = Pid::from_u32(process::id());
    
    loop {
        thread::sleep(Duration::from_secs(5));
        
        // Update system metrics
        system.refresh_all();
        if let Some(process) = system.process(pid) {
            let memory = process.memory();
            let cpu = process.cpu_usage();
            let disk_usage = 0; // Would need to calculate actual disk usage
            let open_files = get_open_files();
            let thread_count = get_thread_count();
            
            metrics.update_resources(memory, disk_usage, open_files, thread_count, cpu as f64);
        }
        
        // Get current snapshot
        let snapshot = metrics.snapshot();
        
        // Calculate rates
        let read_rate = (snapshot.operations.reads - last_snapshot.operations.reads) as f64 / 5.0;
        let write_rate = (snapshot.operations.writes - last_snapshot.operations.writes) as f64 / 5.0;
        let error_rate = (snapshot.errors.total_errors - last_snapshot.errors.total_errors) as f64 / 5.0;
        
        // Print update
        println!("\n📊 [{}s] Metrics Update:", snapshot.health.uptime.as_secs());
        println!("   Throughput: {:.0} reads/s, {:.0} writes/s", read_rate, write_rate);
        println!("   Latency: avg={:?}, p95={:?}, p99={:?}", 
                 snapshot.performance.latency_avg,
                 snapshot.performance.latency_p95,
                 snapshot.performance.latency_p99);
        println!("   Cache: {:.1}% hit rate ({} hits, {} misses)",
                 snapshot.cache.hit_rate_percent,
                 snapshot.cache.hits,
                 snapshot.cache.misses);
        println!("   Memory: {:.1} MB, CPU: {:.1}%",
                 snapshot.resources.memory_usage_bytes as f64 / 1024.0 / 1024.0,
                 snapshot.resources.cpu_usage_percent);
        
        // Check for issues
        if error_rate > 10.0 {
            println!("   ⚠️  HIGH ERROR RATE: {:.1} errors/s", error_rate);
            metrics.update_health(HealthStatus::Degraded(format!("High error rate: {:.1}/s", error_rate)));
        } else if error_rate > 0.0 {
            println!("   ⚠️  Errors: {:.1} errors/s", error_rate);
        }
        
        if snapshot.performance.latency_p99 > Duration::from_millis(10) {
            println!("   ⚠️  HIGH LATENCY: p99={:?}", snapshot.performance.latency_p99);
        }
        
        if snapshot.cache.hit_rate_percent < 50.0 {
            println!("   ⚠️  LOW CACHE HIT RATE: {:.1}%", snapshot.cache.hit_rate_percent);
        }
        
        last_snapshot = snapshot;
    }
}

/// Print final comprehensive report
fn print_final_report(metrics: &Arc<Metrics>) {
    let snapshot = metrics.snapshot();
    
    println!("\n{}", "=".repeat(70));
    println!("📊 FINAL OBSERVABILITY REPORT");
    println!("{}", "=".repeat(70));
    
    // Operations summary
    println!("\n📈 OPERATIONS SUMMARY:");
    println!("   Total reads: {}", snapshot.operations.reads);
    println!("   Total writes: {}", snapshot.operations.writes);
    println!("   Total deletes: {}", snapshot.operations.deletes);
    println!("   Total operations: {}", 
             snapshot.operations.reads + snapshot.operations.writes + snapshot.operations.deletes);
    
    // Performance summary
    println!("\n⚡ PERFORMANCE SUMMARY:");
    println!("   Throughput: {:.0} ops/sec", snapshot.performance.throughput_ops_sec);
    println!("   Latency percentiles:");
    println!("      p50: {:?}", snapshot.performance.latency_p50);
    println!("      p95: {:?}", snapshot.performance.latency_p95);
    println!("      p99: {:?}", snapshot.performance.latency_p99);
    println!("      max: {:?}", snapshot.performance.latency_max);
    
    // Cache performance
    println!("\n💾 CACHE PERFORMANCE:");
    println!("   Hit rate: {:.2}%", snapshot.cache.hit_rate_percent);
    println!("   Total hits: {}", snapshot.cache.hits);
    println!("   Total misses: {}", snapshot.cache.misses);
    println!("   Evictions: {}", snapshot.cache.evictions);
    
    // Transaction summary
    println!("\n🔄 TRANSACTION SUMMARY:");
    println!("   Committed: {}", snapshot.transactions.committed);
    println!("   Aborted: {}", snapshot.transactions.aborted);
    println!("   Conflicts: {}", snapshot.transactions.conflicts);
    println!("   Avg size: {} ops", snapshot.transactions.avg_size);
    
    // Error summary
    println!("\n❌ ERROR SUMMARY:");
    println!("   Total errors: {}", snapshot.errors.total_errors);
    if snapshot.errors.total_errors > 0 {
        println!("   IO errors: {}", snapshot.errors.io_errors);
        println!("   Corruption errors: {}", snapshot.errors.corruption_errors);
        println!("   OOM errors: {}", snapshot.errors.oom_errors);
        println!("   Timeout errors: {}", snapshot.errors.timeout_errors);
        
        if let Some((time, error)) = &snapshot.errors.last_error {
            println!("   Last error: {} at {:?}", error, time);
        }
    }
    
    // Resource usage
    println!("\n🖥️  RESOURCE USAGE:");
    println!("   Memory: {:.2} MB", snapshot.resources.memory_usage_bytes as f64 / 1024.0 / 1024.0);
    println!("   Open files: {}", snapshot.resources.open_files);
    println!("   Threads: {}", snapshot.resources.thread_count);
    println!("   CPU usage: {:.1}%", snapshot.resources.cpu_usage_percent);
    
    // Health status
    println!("\n🏥 HEALTH STATUS:");
    match &snapshot.health.status {
        HealthStatus::Healthy => println!("   ✅ HEALTHY"),
        HealthStatus::Degraded(reason) => println!("   ⚠️  DEGRADED: {}", reason),
        HealthStatus::Unhealthy(reason) => println!("   ❌ UNHEALTHY: {}", reason),
    }
    println!("   Uptime: {:?}", snapshot.health.uptime);
    println!("   Warnings: {}", snapshot.health.warnings);
    
    // Export sample Prometheus metrics
    println!("\n📊 SAMPLE PROMETHEUS METRICS:");
    let prometheus = metrics.export_prometheus();
    for line in prometheus.lines().take(10) {
        println!("   {}", line);
    }
    println!("   ... (truncated)");
    
    // Overall assessment
    println!("\n🏁 PRODUCTION READINESS:");
    let error_rate = snapshot.errors.total_errors as f64 / 
                     (snapshot.operations.reads + snapshot.operations.writes + snapshot.operations.deletes) as f64;
    
    if error_rate < 0.001 && snapshot.cache.hit_rate_percent > 70.0 && 
       snapshot.performance.latency_p99 < Duration::from_millis(10) {
        println!("   ✅ EXCELLENT - Ready for production");
    } else if error_rate < 0.01 && snapshot.cache.hit_rate_percent > 50.0 {
        println!("   ⚠️  GOOD - Monitor closely in production");
    } else {
        println!("   ❌ NEEDS IMPROVEMENT - Address issues before production");
    }
    
    println!("\n{}", "=".repeat(70));
}

fn get_open_files() -> usize {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir(format!("/proc/{}/fd", process::id()))
            .map(|entries| entries.count())
            .unwrap_or(0)
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        0 // Simplified for other platforms
    }
}

fn get_thread_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string(format!("/proc/{}/stat", process::id()))
            .ok()
            .and_then(|stat| {
                let fields: Vec<&str> = stat.split_whitespace().collect();
                fields.get(19).and_then(|s| s.parse().ok())
            })
            .unwrap_or(1)
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        1 // Simplified for other platforms
    }
}