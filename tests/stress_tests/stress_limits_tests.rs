use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct StressTestResult {
    pub max_achieved: u64,
    pub avg_latency_us: f64,
    pub p99_latency_us: f64,
    pub error_rate: f64,
    pub throughput_ops_sec: f64,
    pub resource_usage: ResourceUsage,
}

#[derive(Debug, Clone)]
pub struct ResourceUsage {
    pub memory_mb: f64,
    pub cpu_percent: f64,
    pub disk_io_mb_sec: f64,
    pub open_file_descriptors: u64,
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            memory_mb: 0.0,
            cpu_percent: 0.0,
            disk_io_mb_sec: 0.0,
            open_file_descriptors: 0,
        }
    }
}

pub struct StressLimitsTester {
    db: Arc<Database>,
}

impl StressLimitsTester {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn test_max_concurrent_connections(&self) -> StressTestResult {
        let max_threads = num_cpus::get() * 8; // Start with 8x CPU cores
        let mut successful_connections = 0;
        let mut total_attempts = 0;

        println!("Testing maximum concurrent connections...");

        for num_connections in (1..=max_threads).step_by(10) {
            total_attempts = num_connections;
            let barrier = Arc::new(Barrier::new(num_connections + 1));
            let success_count = Arc::new(AtomicUsize::new(0));
            let error_count = Arc::new(AtomicUsize::new(0));

            let mut handles = vec![];

            // Spawn connection threads
            for thread_id in 0..num_connections {
                let db_clone = Arc::clone(&self.db);
                let barrier_clone = Arc::clone(&barrier);
                let success_clone = Arc::clone(&success_count);
                let error_clone = Arc::clone(&error_count);

                let handle = thread::spawn(move || {
                    barrier_clone.wait();

                    // Simulate connection work
                    for i in 0..100 {
                        let key = format!("conn_test_{}_{}", thread_id, i);
                        let value = format!("value_{}", i);

                        match db_clone.put(key.as_bytes(), value.as_bytes()) {
                            Ok(_) => {
                                if let Ok(Some(_)) = db_clone.get(key.as_bytes()) {
                                    success_clone.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    error_clone.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(_) => {
                                error_clone.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                });
                handles.push(handle);
            }

            barrier.wait(); // Start all threads
            let start = Instant::now();

            // Wait for completion with timeout
            let mut completed = 0;
            for handle in handles {
                if handle.join().is_ok() {
                    completed += 1;
                }
            }

            let duration = start.elapsed();
            let successes = success_count.load(Ordering::Relaxed);
            let errors = error_count.load(Ordering::Relaxed);

            let success_rate = if successes + errors > 0 {
                successes as f64 / (successes + errors) as f64
            } else {
                0.0
            };

            println!(
                "  {} connections: {} completed, success rate: {:.2}%, duration: {:?}",
                num_connections, completed, success_rate * 100.0, duration
            );

            if success_rate > 0.95 && completed == num_connections {
                successful_connections = num_connections;
            } else {
                break;
            }

            thread::sleep(Duration::from_millis(100)); // Cool down between tests
        }

        StressTestResult {
            max_achieved: successful_connections as u64,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate: 0.0,
            throughput_ops_sec: 0.0,
            resource_usage: ResourceUsage::default(),
        }
    }

    pub fn test_max_transaction_rate(&self) -> StressTestResult {
        println!("Testing maximum transaction rate...");

        let test_duration = Duration::from_secs(30);
        let num_threads = num_cpus::get();
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let running = Arc::new(AtomicBool::new(true));
        let transaction_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&self.db);
            let barrier_clone = Arc::clone(&barrier);
            let running_clone = Arc::clone(&running);
            let tx_count_clone = Arc::clone(&transaction_count);
            let error_count_clone = Arc::clone(&error_count);

            let handle = thread::spawn(move || {
                barrier_clone.wait();
                let mut local_tx_count = 0u64;

                while running_clone.load(Ordering::Relaxed) {
                    let tx_result = (|| -> Result<(), lightning_db::core::error::LightningError> {
                        let tx_id = db_clone.begin_transaction()?;

                        // Small transaction with multiple operations
                        for i in 0..5 {
                            let key = format!("tx_rate_{}_{}_key_{}", thread_id, local_tx_count, i);
                            let value = format!("value_{}", i);
                            db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
                        }

                        db_clone.commit_transaction(tx_id)?;
                        Ok(())
                    })();

                    match tx_result {
                        Ok(_) => {
                            tx_count_clone.fetch_add(1, Ordering::Relaxed);
                            local_tx_count += 1;
                        }
                        Err(_) => {
                            error_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
            handles.push(handle);
        }

        let start = Instant::now();
        barrier.wait(); // Start all threads

        thread::sleep(test_duration);
        running.store(false, Ordering::Relaxed);

        // Wait for threads to complete
        for handle in handles {
            let _ = handle.join();
        }

        let elapsed = start.elapsed();
        let total_transactions = transaction_count.load(Ordering::Relaxed);
        let total_errors = error_count.load(Ordering::Relaxed);

        let throughput = total_transactions as f64 / elapsed.as_secs_f64();
        let error_rate = if total_transactions + total_errors > 0 {
            total_errors as f64 / (total_transactions + total_errors) as f64
        } else {
            0.0
        };

        println!(
            "  Max transaction rate: {:.0} tx/sec ({} transactions in {:?}, {:.2}% error rate)",
            throughput, total_transactions, elapsed, error_rate * 100.0
        );

        StressTestResult {
            max_achieved: total_transactions,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate,
            throughput_ops_sec: throughput,
            resource_usage: ResourceUsage::default(),
        }
    }

    pub fn test_max_database_size(&self) -> StressTestResult {
        println!("Testing maximum database size (1 hour test)...");

        let test_duration = Duration::from_secs(3600); // 1 hour
        let large_value_size = 100 * 1024; // 100KB values
        let large_value = vec![0u8; large_value_size];

        let start = Instant::now();
        let mut operation_count = 0u64;
        let mut total_size_bytes = 0u64;
        let mut last_checkpoint = Instant::now();

        while start.elapsed() < test_duration {
            let key = format!("large_db_key_{:016}", operation_count);
            
            match self.db.put(key.as_bytes(), &large_value) {
                Ok(_) => {
                    operation_count += 1;
                    total_size_bytes += key.len() as u64 + large_value_size as u64;

                    // Periodic checkpoint and status update
                    if last_checkpoint.elapsed() > Duration::from_secs(300) {
                        if let Err(e) = self.db.checkpoint() {
                            println!("  Checkpoint failed: {:?}", e);
                        }
                        
                        let size_gb = total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                        println!(
                            "  Progress: {} operations, estimated size: {:.2} GB, elapsed: {:?}",
                            operation_count, size_gb, start.elapsed()
                        );
                        last_checkpoint = Instant::now();
                    }
                }
                Err(e) => {
                    println!("  Database write failed after {} operations: {:?}", operation_count, e);
                    break;
                }
            }

            // Small delay to prevent overwhelming the system
            if operation_count % 1000 == 0 {
                thread::sleep(Duration::from_millis(100));
            }
        }

        let final_size_gb = total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        println!(
            "  Max database size achieved: {:.2} GB ({} operations)",
            final_size_gb, operation_count
        );

        StressTestResult {
            max_achieved: operation_count,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate: 0.0,
            throughput_ops_sec: operation_count as f64 / start.elapsed().as_secs_f64(),
            resource_usage: ResourceUsage::default(),
        }
    }

    pub fn test_max_key_value_sizes(&self) -> HashMap<String, StressTestResult> {
        println!("Testing maximum key and value sizes...");
        let mut results = HashMap::new();

        // Test maximum key sizes
        let max_key_size = self.test_max_key_size();
        results.insert("max_key_size".to_string(), max_key_size);

        // Test maximum value sizes
        let max_value_size = self.test_max_value_size();
        results.insert("max_value_size".to_string(), max_value_size);

        // Test maximum batch size
        let max_batch_size = self.test_max_batch_size();
        results.insert("max_batch_size".to_string(), max_batch_size);

        results
    }

    fn test_max_key_size(&self) -> StressTestResult {
        println!("  Testing maximum key size...");
        
        let mut max_key_size = 0;
        let mut current_size = 1024; // Start with 1KB

        loop {
            let key = "k".repeat(current_size);
            let value = b"test_value";

            match self.db.put(key.as_bytes(), value) {
                Ok(_) => {
                    if let Ok(Some(_)) = self.db.get(key.as_bytes()) {
                        max_key_size = current_size;
                        println!("    Key size {} bytes: OK", current_size);
                        current_size *= 2;

                        // Safety limit to prevent runaway test
                        if current_size > 64 * 1024 * 1024 {
                            break;
                        }
                    } else {
                        println!("    Key size {} bytes: Write OK but read failed", current_size);
                        break;
                    }
                }
                Err(e) => {
                    println!("    Key size {} bytes: Failed - {:?}", current_size, e);
                    break;
                }
            }
        }

        println!("    Maximum key size: {} bytes", max_key_size);

        StressTestResult {
            max_achieved: max_key_size as u64,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate: 0.0,
            throughput_ops_sec: 0.0,
            resource_usage: ResourceUsage::default(),
        }
    }

    fn test_max_value_size(&self) -> StressTestResult {
        println!("  Testing maximum value size...");
        
        let mut max_value_size = 0;
        let mut current_size = 1024 * 1024; // Start with 1MB

        loop {
            let key = format!("value_size_test_{}", current_size);
            let value = vec![0u8; current_size];

            match self.db.put(key.as_bytes(), &value) {
                Ok(_) => {
                    if let Ok(Some(retrieved)) = self.db.get(key.as_bytes()) {
                        if retrieved.len() == current_size {
                            max_value_size = current_size;
                            println!("    Value size {} MB: OK", current_size / (1024 * 1024));
                            current_size *= 2;

                            // Safety limit to prevent runaway test
                            if current_size > 1024 * 1024 * 1024 {
                                break;
                            }
                        } else {
                            println!("    Value size {} MB: Size mismatch", current_size / (1024 * 1024));
                            break;
                        }
                    } else {
                        println!("    Value size {} MB: Write OK but read failed", current_size / (1024 * 1024));
                        break;
                    }
                }
                Err(e) => {
                    println!("    Value size {} MB: Failed - {:?}", current_size / (1024 * 1024), e);
                    break;
                }
            }
        }

        println!("    Maximum value size: {} MB", max_value_size / (1024 * 1024));

        StressTestResult {
            max_achieved: max_value_size as u64,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate: 0.0,
            throughput_ops_sec: 0.0,
            resource_usage: ResourceUsage::default(),
        }
    }

    fn test_max_batch_size(&self) -> StressTestResult {
        println!("  Testing maximum batch transaction size...");
        
        let mut max_batch_size = 0;
        let mut current_batch_size = 100;

        loop {
            let tx_result = (|| -> Result<(), lightning_db::core::error::LightningError> {
                let tx_id = self.db.begin_transaction()?;

                for i in 0..current_batch_size {
                    let key = format!("batch_test_{}_{}", current_batch_size, i);
                    let value = format!("batch_value_{}", i);
                    self.db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
                }

                self.db.commit_transaction(tx_id)?;
                Ok(())
            })();

            match tx_result {
                Ok(_) => {
                    max_batch_size = current_batch_size;
                    println!("    Batch size {}: OK", current_batch_size);
                    current_batch_size *= 2;

                    // Safety limit
                    if current_batch_size > 1_000_000 {
                        break;
                    }
                }
                Err(e) => {
                    println!("    Batch size {}: Failed - {:?}", current_batch_size, e);
                    break;
                }
            }
        }

        println!("    Maximum batch size: {} operations", max_batch_size);

        StressTestResult {
            max_achieved: max_batch_size as u64,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate: 0.0,
            throughput_ops_sec: 0.0,
            resource_usage: ResourceUsage::default(),
        }
    }

    pub fn test_resource_exhaustion_scenarios(&self) -> HashMap<String, StressTestResult> {
        println!("Testing resource exhaustion scenarios...");
        let mut results = HashMap::new();

        // Test memory exhaustion resistance
        let memory_stress = self.test_memory_exhaustion();
        results.insert("memory_exhaustion".to_string(), memory_stress);

        // Test disk space exhaustion (simulated)
        let disk_stress = self.test_disk_exhaustion_simulation();
        results.insert("disk_exhaustion".to_string(), disk_stress);

        // Test file descriptor exhaustion
        let fd_stress = self.test_file_descriptor_exhaustion();
        results.insert("fd_exhaustion".to_string(), fd_stress);

        results
    }

    fn test_memory_exhaustion(&self) -> StressTestResult {
        println!("  Testing memory exhaustion resistance...");

        let initial_memory = Self::get_memory_usage_mb();
        let target_memory_mb = initial_memory + 500.0; // Try to use 500MB more
        let value_size = 1024 * 1024; // 1MB values
        let mut operations = 0u64;
        let mut errors = 0u64;

        let start = Instant::now();

        while Self::get_memory_usage_mb() < target_memory_mb && start.elapsed() < Duration::from_secs(300) {
            let key = format!("memory_stress_{:08}", operations);
            let value = vec![(operations % 256) as u8; value_size];

            match self.db.put(key.as_bytes(), &value) {
                Ok(_) => operations += 1,
                Err(_) => {
                    errors += 1;
                    // If we're getting too many errors, stop
                    if errors > 10 {
                        break;
                    }
                }
            }

            if operations % 10 == 0 {
                let current_memory = Self::get_memory_usage_mb();
                println!(
                    "    Memory usage: {:.1} MB, operations: {}, errors: {}",
                    current_memory, operations, errors
                );
            }
        }

        let final_memory = Self::get_memory_usage_mb();
        let memory_increase = final_memory - initial_memory;

        println!(
            "    Memory stress test: {} operations, {:.1} MB increase, {} errors",
            operations, memory_increase, errors
        );

        StressTestResult {
            max_achieved: operations,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate: errors as f64 / (operations + errors) as f64,
            throughput_ops_sec: operations as f64 / start.elapsed().as_secs_f64(),
            resource_usage: ResourceUsage {
                memory_mb: final_memory,
                ..Default::default()
            },
        }
    }

    fn test_disk_exhaustion_simulation(&self) -> StressTestResult {
        println!("  Testing disk exhaustion simulation...");

        // Simulate disk pressure by writing large amounts of data quickly
        let large_value_size = 10 * 1024 * 1024; // 10MB values
        let large_value = vec![0u8; large_value_size];
        let mut operations = 0u64;
        let mut errors = 0u64;

        let start = Instant::now();
        let test_duration = Duration::from_secs(60); // 1 minute test

        while start.elapsed() < test_duration {
            let key = format!("disk_stress_{:08}", operations);

            match self.db.put(key.as_bytes(), &large_value) {
                Ok(_) => {
                    operations += 1;
                    
                    // Force checkpoint periodically to stress disk I/O
                    if operations % 5 == 0 {
                        let _ = self.db.checkpoint();
                    }
                }
                Err(_) => {
                    errors += 1;
                    if errors > 5 {
                        break;
                    }
                }
            }

            // Rate limiting to prevent overwhelming the system
            thread::sleep(Duration::from_millis(100));
        }

        let total_data_gb = (operations * large_value_size as u64) as f64 / (1024.0 * 1024.0 * 1024.0);

        println!(
            "    Disk stress test: {} operations, {:.2} GB written, {} errors",
            operations, total_data_gb, errors
        );

        StressTestResult {
            max_achieved: operations,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate: errors as f64 / (operations + errors) as f64,
            throughput_ops_sec: operations as f64 / start.elapsed().as_secs_f64(),
            resource_usage: ResourceUsage::default(),
        }
    }

    fn test_file_descriptor_exhaustion(&self) -> StressTestResult {
        println!("  Testing file descriptor exhaustion resistance...");

        // This test ensures the database can handle file descriptor pressure
        // by opening many simultaneous transactions and iterators
        let mut active_transactions = Vec::new();
        let mut operations = 0u64;
        let mut errors = 0u64;

        for i in 0..1000 {
            match self.db.begin_transaction() {
                Ok(tx_id) => {
                    // Add some data to the transaction
                    let key = format!("fd_test_{}", i);
                    let value = format!("value_{}", i);
                    
                    if self.db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                        active_transactions.push(tx_id);
                        operations += 1;
                    } else {
                        let _ = self.db.abort_transaction(tx_id);
                        errors += 1;
                    }
                }
                Err(_) => {
                    errors += 1;
                    break;
                }
            }

            if i % 100 == 0 {
                println!("    Active transactions: {}, errors: {}", active_transactions.len(), errors);
            }
        }

        // Clean up transactions
        for tx_id in active_transactions {
            let _ = self.db.commit_transaction(tx_id);
        }

        println!(
            "    File descriptor test: {} successful transactions, {} errors",
            operations, errors
        );

        StressTestResult {
            max_achieved: operations,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            error_rate: errors as f64 / (operations + errors) as f64,
            throughput_ops_sec: 0.0,
            resource_usage: ResourceUsage::default(),
        }
    }

    fn get_memory_usage_mb() -> f64 {
        #[cfg(not(target_env = "msvc"))]
        {
            tikv_jemalloc_ctl::stats::allocated::read()
                .map(|bytes| bytes as f64 / 1024.0 / 1024.0)
                .unwrap_or(0.0)
        }
        #[cfg(target_env = "msvc")]
        {
            0.0 // Fallback for MSVC
        }
    }
}

// Individual stress tests

#[test]
#[ignore = "Stress test - run with: cargo test test_max_concurrent_connections -- --ignored"]
fn test_max_concurrent_connections() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024,
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 1000 },
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let tester = StressLimitsTester::new(db);

    let result = tester.test_max_concurrent_connections();
    
    println!("Maximum concurrent connections test result:");
    println!("  Max connections achieved: {}", result.max_achieved);
    
    assert!(result.max_achieved >= 20, "Should support at least 20 concurrent connections");
}

#[test]
#[ignore = "Stress test - run with: cargo test test_max_transaction_rate -- --ignored"]
fn test_max_transaction_rate() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024,
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let tester = StressLimitsTester::new(db);

    let result = tester.test_max_transaction_rate();
    
    println!("Maximum transaction rate test result:");
    println!("  Throughput: {:.0} tx/sec", result.throughput_ops_sec);
    println!("  Error rate: {:.2}%", result.error_rate * 100.0);
    
    assert!(result.throughput_ops_sec >= 1000.0, "Should achieve at least 1000 tx/sec");
    assert!(result.error_rate < 0.01, "Error rate should be less than 1%");
}

#[test]
#[ignore = "Long-running stress test - run with: cargo test test_max_database_size -- --ignored"]
fn test_max_database_size() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 200 * 1024 * 1024, // Large cache for better performance
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 5000 },
        compression_enabled: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let tester = StressLimitsTester::new(db);

    let result = tester.test_max_database_size();
    
    println!("Maximum database size test result:");
    println!("  Operations completed: {}", result.max_achieved);
    println!("  Throughput: {:.0} ops/sec", result.throughput_ops_sec);
    
    assert!(result.max_achieved >= 1000, "Should complete at least 1000 large operations");
}

#[test]
#[ignore = "Stress test - run with: cargo test test_max_key_value_sizes -- --ignored"]
fn test_max_key_value_sizes() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 500 * 1024 * 1024, // Large cache for large values
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let tester = StressLimitsTester::new(db);

    let results = tester.test_max_key_value_sizes();
    
    for (test_name, result) in &results {
        println!("{}: max size {} bytes", test_name, result.max_achieved);
    }
    
    // Verify reasonable limits
    assert!(results["max_key_size"].max_achieved >= 1024, "Should support at least 1KB keys");
    assert!(results["max_value_size"].max_achieved >= 1024 * 1024, "Should support at least 1MB values");
    assert!(results["max_batch_size"].max_achieved >= 100, "Should support at least 100-operation batches");
}

#[test]
#[ignore = "Resource stress test - run with: cargo test test_resource_exhaustion -- --ignored"]
fn test_resource_exhaustion() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024,
        use_improved_wal: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let tester = StressLimitsTester::new(db);

    let results = tester.test_resource_exhaustion_scenarios();
    
    for (test_name, result) in &results {
        println!("{}: {} operations, {:.2}% error rate", 
            test_name, result.max_achieved, result.error_rate * 100.0);
    }
    
    // Verify the database handles resource pressure gracefully
    for result in results.values() {
        assert!(result.error_rate < 0.1, "Error rate should be less than 10% under resource pressure");
    }
}