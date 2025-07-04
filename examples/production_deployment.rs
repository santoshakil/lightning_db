use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

/// Production deployment example demonstrating:
/// - Optimized configuration for production workloads
/// - Background monitoring and health checks
/// - Graceful shutdown handling
/// - Performance monitoring
/// - Error handling and recovery
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Lightning DB Production Deployment Example");
    println!("=".repeat(50));
    
    // Production-optimized configuration
    let config = LightningDbConfig {
        // Memory configuration
        cache_size: 512 * 1024 * 1024, // 512MB cache
        max_active_transactions: 10000,
        
        // Performance optimizations
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Async, // Better performance, slight durability trade-off
        compression_enabled: true,
        compression_type: 1, // Zstd compression
        
        // I/O optimizations
        prefetch_enabled: true,
        prefetch_distance: 16,
        prefetch_workers: 4,
        
        // Disable experimental features for production stability
        use_optimized_transactions: false,
        use_optimized_page_manager: false,
        
        ..Default::default()
    };
    
    // Create database with production config
    let db_path = "/tmp/lightning_db_production_example";
    println!("📂 Creating database at: {}", db_path);
    
    let db = Arc::new(Database::create(db_path, config)?);
    println!("✅ Database created successfully");
    
    // Start background monitoring
    let should_stop = Arc::new(AtomicBool::new(false));
    let operation_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    
    start_monitoring(
        Arc::clone(&db),
        Arc::clone(&should_stop),
        Arc::clone(&operation_count),
        Arc::clone(&error_count),
    )?;
    
    // Simulate production workload
    println!("\n🔥 Starting production workload simulation...");
    run_production_workload(
        Arc::clone(&db),
        Arc::clone(&operation_count),
        Arc::clone(&error_count),
    )?;
    
    // Simulate health checks
    println!("\n🏥 Running health checks...");
    run_health_checks(&db)?;
    
    // Demonstrate backup operations
    println!("\n💾 Creating backup...");
    create_backup(&db)?;
    
    // Graceful shutdown simulation
    println!("\n🛑 Initiating graceful shutdown...");
    should_stop.store(true, Ordering::Relaxed);
    
    // Wait for background tasks to complete
    thread::sleep(Duration::from_millis(500));
    
    // Final statistics
    println!("\n📊 Final Statistics:");
    println!("  Total operations: {}", operation_count.load(Ordering::Relaxed));
    println!("  Total errors: {}", error_count.load(Ordering::Relaxed));
    
    let stats = db.get_statistics()?;
    println!("  Active transactions: {}", stats.active_transactions);
    
    println!("\n✅ Production deployment example completed successfully!");
    println!("🎯 Ready for production deployment with proper configuration");
    
    Ok(())
}

/// Start background monitoring tasks
fn start_monitoring(
    db: Arc<Database>,
    should_stop: Arc<AtomicBool>,
    operation_count: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📊 Starting background monitoring...");
    
    // Health monitoring thread
    let db_health = Arc::clone(&db);
    let stop_health = Arc::clone(&should_stop);
    thread::spawn(move || {
        while !stop_health.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(30));
            
            if stop_health.load(Ordering::Relaxed) {
                break;
            }
            
            // Perform health check
            match perform_health_check(&db_health) {
                Ok(healthy) => {
                    if healthy {
                        println!("💚 Health check: HEALTHY");
                    } else {
                        println!("⚠️  Health check: DEGRADED");
                    }
                }
                Err(e) => {
                    println!("🔴 Health check: ERROR - {}", e);
                }
            }
        }
    });
    
    // Performance monitoring thread
    let op_count = Arc::clone(&operation_count);
    let err_count = Arc::clone(&error_count);
    let stop_perf = Arc::clone(&should_stop);
    thread::spawn(move || {
        let mut last_ops = 0;
        let mut last_time = Instant::now();
        
        while !stop_perf.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(10));
            
            if stop_perf.load(Ordering::Relaxed) {
                break;
            }
            
            let current_ops = op_count.load(Ordering::Relaxed);
            let current_time = Instant::now();
            let duration = current_time.duration_since(last_time);
            
            if duration.as_secs() > 0 {
                let ops_per_sec = (current_ops - last_ops) as f64 / duration.as_secs_f64();
                let total_errors = err_count.load(Ordering::Relaxed);
                
                println!("📈 Performance: {:.0} ops/sec, {} total errors", ops_per_sec, total_errors);
                
                last_ops = current_ops;
                last_time = current_time;
            }
        }
    });
    
    println!("✅ Background monitoring started");
    Ok(())
}

/// Perform basic health check
fn perform_health_check(db: &Database) -> Result<bool, Box<dyn std::error::Error>> {
    // Test basic operations
    let test_key = b"__health_check__";
    let test_value = b"healthy";
    
    // Write test
    db.put(test_key, test_value)?;
    
    // Read test
    let read_value = db.get(test_key)?;
    if read_value.as_deref() != Some(test_value) {
        return Ok(false);
    }
    
    // Delete test
    db.delete(test_key)?;
    if db.get(test_key)?.is_some() {
        return Ok(false);
    }
    
    // Transaction test
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"tx_test", b"value")?;
    db.commit_transaction(tx_id)?;
    db.delete(b"tx_test")?;
    
    Ok(true)
}

/// Simulate production workload
fn run_production_workload(
    db: Arc<Database>,
    operation_count: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_workers = 4;
    let operations_per_worker = 5000;
    
    let mut handles = Vec::new();
    
    for worker_id in 0..num_workers {
        let db_worker = Arc::clone(&db);
        let op_count = Arc::clone(&operation_count);
        let err_count = Arc::clone(&error_count);
        
        let handle = thread::spawn(move || {
            println!("👷 Worker {} starting...", worker_id);
            
            for i in 0..operations_per_worker {
                let key = format!("worker_{}_{}", worker_id, i);
                let value = format!("data_{}_{}_{}", worker_id, i, Instant::now().elapsed().as_micros());
                
                // Mix of operations
                match i % 4 {
                    0 | 1 => {
                        // 50% writes
                        match db_worker.put(key.as_bytes(), value.as_bytes()) {
                            Ok(_) => op_count.fetch_add(1, Ordering::Relaxed),
                            Err(_) => err_count.fetch_add(1, Ordering::Relaxed),
                        };
                    }
                    2 => {
                        // 25% reads
                        match db_worker.get(key.as_bytes()) {
                            Ok(_) => op_count.fetch_add(1, Ordering::Relaxed),
                            Err(_) => err_count.fetch_add(1, Ordering::Relaxed),
                        };
                    }
                    3 => {
                        // 25% transactions
                        match run_transaction(&db_worker, &key, &value) {
                            Ok(_) => op_count.fetch_add(3, Ordering::Relaxed), // Count as 3 ops
                            Err(_) => err_count.fetch_add(1, Ordering::Relaxed),
                        }
                    }
                    _ => unreachable!(),
                }
                
                // Small delay to simulate realistic workload
                if i % 100 == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
            
            println!("✅ Worker {} completed", worker_id);
        });
        
        handles.push(handle);
    }
    
    // Wait for all workers to complete
    for handle in handles {
        handle.join().map_err(|_| "Worker thread panicked")?;
    }
    
    println!("✅ Production workload completed");
    Ok(())
}

/// Run a sample transaction
fn run_transaction(
    db: &Database,
    key: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let tx_id = db.begin_transaction()?;
    
    // Read
    let _existing = db.get_tx(tx_id, key.as_bytes())?;
    
    // Write
    db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
    
    // Another read
    let _verify = db.get_tx(tx_id, key.as_bytes())?;
    
    // Commit
    db.commit_transaction(tx_id)?;
    
    Ok(())
}

/// Run comprehensive health checks
fn run_health_checks(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Running comprehensive health checks...");
    
    // Basic operations check
    print!("  • Basic operations... ");
    perform_health_check(db)?;
    println!("✅");
    
    // Transaction isolation check
    print!("  • Transaction isolation... ");
    test_transaction_isolation(db)?;
    println!("✅");
    
    // Performance sanity check
    print!("  • Performance sanity check... ");
    test_performance_sanity(db)?;
    println!("✅");
    
    // Resource usage check
    print!("  • Resource usage... ");
    let stats = db.get_statistics()?;
    if stats.active_transactions > 1000 {
        return Err("Too many active transactions".into());
    }
    println!("✅");
    
    println!("✅ All health checks passed");
    Ok(())
}

/// Test transaction isolation
fn test_transaction_isolation(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let tx1 = db.begin_transaction()?;
    let tx2 = db.begin_transaction()?;
    
    // Both transactions should see consistent state
    db.put_tx(tx1, b"isolation_test", b"tx1_value")?;
    db.put_tx(tx2, b"isolation_test", b"tx2_value")?;
    
    // Only one should succeed
    let result1 = db.commit_transaction(tx1);
    let result2 = db.commit_transaction(tx2);
    
    if result1.is_ok() && result2.is_ok() {
        return Err("Both transactions succeeded - isolation failure".into());
    }
    
    Ok(())
}

/// Test performance sanity
fn test_performance_sanity(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let test_ops = 1000;
    
    for i in 0..test_ops {
        let key = format!("perf_test_{}", i);
        db.put(key.as_bytes(), b"test_value")?;
    }
    
    let duration = start.elapsed();
    let ops_per_sec = test_ops as f64 / duration.as_secs_f64();
    
    // Should achieve at least 10K ops/sec for basic operations
    if ops_per_sec < 10_000.0 {
        return Err(format!("Performance too slow: {:.0} ops/sec", ops_per_sec).into());
    }
    
    Ok(())
}

/// Create a backup
fn create_backup(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    use lightning_db::backup::{BackupManager, BackupConfig};
    
    let backup_config = BackupConfig {
        include_wal: true,
        compress: true,
        verify: true,
        max_incremental_size: 100 * 1024 * 1024, // 100MB
        online_backup: true,
        io_throttle_mb_per_sec: 50,
    };
    
    let backup_manager = BackupManager::new(backup_config);
    let backup_path = "/tmp/lightning_db_backup";
    
    let start = Instant::now();
    let _backup = backup_manager.create_backup("/tmp/lightning_db_production_example", backup_path)?;
    let duration = start.elapsed();
    
    println!("✅ Backup created in {:?}", duration);
    Ok(())
}