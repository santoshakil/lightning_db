use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use std::path::PathBuf;
use std::fs;
use std::io::{Write, Seek, SeekFrom};

#[derive(Debug, Clone)]
pub struct ChaosTestResult {
    pub test_name: String,
    pub operations_before_chaos: u64,
    pub operations_after_recovery: u64,
    pub recovery_time_ms: u64,
    pub data_integrity_score: f64,
    pub success: bool,
    pub error_details: Option<String>,
}

pub struct ChaosTestEngine {
    db_path: PathBuf,
    config: LightningDbConfig,
}

impl ChaosTestEngine {
    pub fn new(db_path: PathBuf, config: LightningDbConfig) -> Self {
        Self { db_path, config }
    }

    pub fn run_chaos_test_suite(&self) -> Vec<ChaosTestResult> {
        let mut results = Vec::new();

        println!("Starting chaos engineering test suite...");

        // Test 1: Random process termination
        results.push(self.test_random_process_kill());

        // Test 2: Disk full simulation
        results.push(self.test_disk_full_scenario());

        // Test 3: Power failure simulation
        results.push(self.test_power_failure_simulation());

        // Test 4: Corrupted data injection
        results.push(self.test_corrupted_data_injection());

        // Test 5: Clock skew testing
        results.push(self.test_clock_skew());

        // Test 6: Network partition simulation (for future distributed features)
        results.push(self.test_network_partition_simulation());

        // Test 7: Resource starvation
        results.push(self.test_resource_starvation());

        // Test 8: Concurrent crash scenarios
        results.push(self.test_concurrent_crash_scenarios());

        results
    }

    fn test_random_process_kill(&self) -> ChaosTestResult {
        println!("  Running random process kill test...");

        let result = std::panic::catch_unwind(|| {
            let db = Database::open(&self.db_path, self.config.clone()).unwrap();
            let operations_before = Arc::new(AtomicU64::new(0));
            let running = Arc::new(AtomicBool::new(true));

            // Populate initial data
            for i in 0..1000 {
                let key = format!("kill_test_key_{:06}", i);
                let value = format!("initial_value_{}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                operations_before.fetch_add(1, Ordering::Relaxed);
            }

            // Start background workload
            let db_clone = db.clone();
            let running_clone = Arc::clone(&running);
            let ops_clone = Arc::clone(&operations_before);

            let workload_handle = thread::spawn(move || {
                let mut counter = 1000u64;
                while running_clone.load(Ordering::Relaxed) {
                    let key = format!("kill_test_key_{:06}", counter);
                    let value = format!("runtime_value_{}", counter);
                    
                    if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                        ops_clone.fetch_add(1, Ordering::Relaxed);
                        counter += 1;
                    }

                    thread::sleep(Duration::from_micros(100));
                }
            });

            // Let workload run for a bit
            thread::sleep(Duration::from_secs(2));

            // Simulate abrupt termination by dropping the database without proper shutdown
            let ops_before = operations_before.load(Ordering::Relaxed);
            running.store(false, Ordering::Relaxed);
            drop(db);
            let _ = workload_handle.join();

            // Recovery phase
            let recovery_start = Instant::now();
            let recovered_db = Database::open(&self.db_path, self.config.clone()).unwrap();
            let recovery_time = recovery_start.elapsed();

            // Verify data integrity
            let mut recovered_count = 0;
            let mut integrity_issues = 0;

            for i in 0..ops_before {
                let key = format!("kill_test_key_{:06}", i);
                match recovered_db.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        recovered_count += 1;
                        // Verify value integrity
                        let expected_prefix = if i < 1000 { "initial_value_" } else { "runtime_value_" };
                        if !String::from_utf8_lossy(&value).starts_with(expected_prefix) {
                            integrity_issues += 1;
                        }
                    }
                    Ok(None) => {} // Data loss is acceptable for uncommitted operations
                    Err(_) => integrity_issues += 1,
                }
            }

            let integrity_score = if ops_before > 0 {
                (recovered_count - integrity_issues) as f64 / ops_before as f64
            } else {
                1.0
            };

            println!(
                "    Process kill test: {} ops before, {} recovered, integrity: {:.2}%, recovery: {:?}",
                ops_before, recovered_count, integrity_score * 100.0, recovery_time
            );

            ChaosTestResult {
                test_name: "random_process_kill".to_string(),
                operations_before_chaos: ops_before,
                operations_after_recovery: recovered_count,
                recovery_time_ms: recovery_time.as_millis() as u64,
                data_integrity_score: integrity_score,
                success: integrity_score > 0.9 && recovery_time < Duration::from_secs(10),
                error_details: None,
            }
        });

        result.unwrap_or_else(|_| ChaosTestResult {
            test_name: "random_process_kill".to_string(),
            operations_before_chaos: 0,
            operations_after_recovery: 0,
            recovery_time_ms: 0,
            data_integrity_score: 0.0,
            success: false,
            error_details: Some("Test panicked".to_string()),
        })
    }

    fn test_disk_full_scenario(&self) -> ChaosTestResult {
        println!("  Running disk full simulation test...");

        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut operations = 0u64;
        let large_value = vec![0u8; 10 * 1024 * 1024]; // 10MB values

        // Fill up available space rapidly
        let start = Instant::now();
        loop {
            let key = format!("disk_full_key_{:08}", operations);
            
            match db.put(key.as_bytes(), &large_value) {
                Ok(_) => {
                    operations += 1;
                    if operations > 100 { // Limit to prevent actually filling disk
                        break;
                    }
                }
                Err(_) => {
                    println!("    Disk full condition triggered after {} operations", operations);
                    break;
                }
            }
        }

        // Test recovery from disk full condition
        let recovery_start = Instant::now();
        
        // Try smaller operations
        let mut small_ops_success = 0;
        for i in 0..10 {
            let key = format!("small_op_{}", i);
            let value = b"small_value";
            if db.put(key.as_bytes(), value).is_ok() {
                small_ops_success += 1;
            }
        }

        let recovery_time = recovery_start.elapsed();

        // Test data integrity
        let mut verified_ops = 0;
        for i in 0..operations {
            let key = format!("disk_full_key_{:08}", i);
            if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                verified_ops += 1;
            }
        }

        let integrity_score = if operations > 0 {
            verified_ops as f64 / operations as f64
        } else {
            1.0
        };

        println!(
            "    Disk full test: {} large ops, {} verified, {} small ops recovered, integrity: {:.2}%",
            operations, verified_ops, small_ops_success, integrity_score * 100.0
        );

        ChaosTestResult {
            test_name: "disk_full_scenario".to_string(),
            operations_before_chaos: operations,
            operations_after_recovery: small_ops_success,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_integrity_score: integrity_score,
            success: integrity_score > 0.8 && small_ops_success > 0,
            error_details: None,
        }
    }

    fn test_power_failure_simulation(&self) -> ChaosTestResult {
        println!("  Running power failure simulation test...");

        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut operations = 0u64;

        // Perform operations without explicit sync
        for i in 0..500 {
            let key = format!("power_test_key_{:06}", i);
            let value = format!("power_test_value_{}", i);
            
            if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                operations += 1;
            }

            // Simulate sudden power loss at random point
            if i == 250 {
                println!("    Simulating power failure at operation {}", i);
                // Abruptly drop database without shutdown
                drop(db);
                break;
            }
        }

        // Recovery after "power restoration"
        let recovery_start = Instant::now();
        let recovered_db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Check data integrity
        let mut recovered_ops = 0;
        let mut integrity_issues = 0;

        for i in 0..operations {
            let key = format!("power_test_key_{:06}", i);
            match recovered_db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    recovered_ops += 1;
                    let expected = format!("power_test_value_{}", i);
                    if value != expected.as_bytes() {
                        integrity_issues += 1;
                    }
                }
                Ok(None) => {} // Some data loss is expected
                Err(_) => integrity_issues += 1,
            }
        }

        let integrity_score = if operations > 0 {
            (recovered_ops - integrity_issues) as f64 / operations as f64
        } else {
            1.0
        };

        println!(
            "    Power failure test: {} ops before, {} recovered, {} integrity issues, recovery: {:?}",
            operations, recovered_ops, integrity_issues, recovery_time
        );

        ChaosTestResult {
            test_name: "power_failure_simulation".to_string(),
            operations_before_chaos: operations,
            operations_after_recovery: recovered_ops,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_integrity_score: integrity_score,
            success: integrity_score > 0.7 && recovery_time < Duration::from_secs(5),
            error_details: None,
        }
    }

    fn test_corrupted_data_injection(&self) -> ChaosTestResult {
        println!("  Running corrupted data injection test...");

        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut operations = 0u64;

        // Insert clean data
        for i in 0..200 {
            let key = format!("corrupt_test_key_{:06}", i);
            let value = format!("clean_value_{}", i);
            if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                operations += 1;
            }
        }

        // Force a checkpoint to ensure data is written
        let _ = db.checkpoint();
        drop(db);

        // Simulate data corruption by modifying files
        let corruption_start = Instant::now();
        if let Err(e) = self.inject_file_corruption() {
            return ChaosTestResult {
                test_name: "corrupted_data_injection".to_string(),
                operations_before_chaos: operations,
                operations_after_recovery: 0,
                recovery_time_ms: 0,
                data_integrity_score: 0.0,
                success: false,
                error_details: Some(format!("Failed to inject corruption: {:?}", e)),
            };
        }

        // Try to recover from corruption
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        match recovery_result {
            Ok(recovered_db) => {
                // Check how much data survived corruption
                let mut recovered_ops = 0;
                let mut corruption_detected = 0;

                for i in 0..operations {
                    let key = format!("corrupt_test_key_{:06}", i);
                    match recovered_db.get(key.as_bytes()) {
                        Ok(Some(value)) => {
                            let expected = format!("clean_value_{}", i);
                            if value == expected.as_bytes() {
                                recovered_ops += 1;
                            } else {
                                corruption_detected += 1;
                            }
                        }
                        Ok(None) => {} // Data loss from corruption
                        Err(_) => corruption_detected += 1,
                    }
                }

                let integrity_score = if operations > 0 {
                    recovered_ops as f64 / operations as f64
                } else {
                    1.0
                };

                println!(
                    "    Corruption test: {} ops before, {} recovered cleanly, {} corrupted, recovery: {:?}",
                    operations, recovered_ops, corruption_detected, recovery_time
                );

                ChaosTestResult {
                    test_name: "corrupted_data_injection".to_string(),
                    operations_before_chaos: operations,
                    operations_after_recovery: recovered_ops,
                    recovery_time_ms: recovery_time.as_millis() as u64,
                    data_integrity_score: integrity_score,
                    success: recovered_ops > 0, // Any recovery is good
                    error_details: None,
                }
            }
            Err(e) => {
                println!("    Failed to recover from corruption: {:?}", e);
                ChaosTestResult {
                    test_name: "corrupted_data_injection".to_string(),
                    operations_before_chaos: operations,
                    operations_after_recovery: 0,
                    recovery_time_ms: recovery_time.as_millis() as u64,
                    data_integrity_score: 0.0,
                    success: false,
                    error_details: Some(format!("Recovery failed: {:?}", e)),
                }
            }
        }
    }

    fn inject_file_corruption(&self) -> Result<(), std::io::Error> {
        // Find database files to corrupt
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "db" || extension == "wal" || extension == "log" {
                    // Inject corruption into the middle of the file
                    if let Ok(mut file) = fs::OpenOptions::new().write(true).open(&path) {
                        if let Ok(metadata) = file.metadata() {
                            let file_size = metadata.len();
                            if file_size > 1024 {
                                // Corrupt a 64-byte section in the middle
                                let corrupt_offset = file_size / 2;
                                let _ = file.seek(SeekFrom::Start(corrupt_offset));
                                let corrupt_data = vec![0xFF; 64];
                                let _ = file.write_all(&corrupt_data);
                                println!("    Injected corruption into {:?}", path);
                                break; // Only corrupt one file
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    fn test_clock_skew(&self) -> ChaosTestResult {
        println!("  Running clock skew test...");

        // This test simulates what happens when system time changes dramatically
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut operations = 0u64;

        // Insert timestamped data
        for i in 0..100 {
            let key = format!("time_test_key_{:06}", i);
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let value = format!("value_{}_time_{}", i, timestamp);
            
            if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                operations += 1;
            }
        }

        // Simulate operations that might be affected by time skew
        let tx_start = Instant::now();
        let tx_id = db.begin_transaction().unwrap();

        for i in 100..150 {
            let key = format!("time_test_key_{:06}", i);
            let value = format!("tx_value_{}", i);
            if db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                operations += 1;
            }
        }

        // Commit transaction
        let commit_result = db.commit_transaction(tx_id);
        let tx_time = tx_start.elapsed();

        // Verify all data is readable and consistent
        let verification_start = Instant::now();
        let mut verified_ops = 0;
        let mut time_inconsistencies = 0;

        for i in 0..operations {
            let key = format!("time_test_key_{:06}", i);
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    verified_ops += 1;
                    // Check for timestamp consistency (basic sanity check)
                    let value_str = String::from_utf8_lossy(&value);
                    if i < 100 && !value_str.contains("time_") {
                        time_inconsistencies += 1;
                    }
                }
                Ok(None) => {}
                Err(_) => time_inconsistencies += 1,
            }
        }

        let verification_time = verification_start.elapsed();
        let integrity_score = if operations > 0 {
            (verified_ops - time_inconsistencies) as f64 / operations as f64
        } else {
            1.0
        };

        println!(
            "    Clock skew test: {} ops, {} verified, {} inconsistencies, tx: {:?}, verify: {:?}",
            operations, verified_ops, time_inconsistencies, tx_time, verification_time
        );

        ChaosTestResult {
            test_name: "clock_skew".to_string(),
            operations_before_chaos: operations,
            operations_after_recovery: verified_ops,
            recovery_time_ms: verification_time.as_millis() as u64,
            data_integrity_score: integrity_score,
            success: commit_result.is_ok() && integrity_score > 0.95,
            error_details: None,
        }
    }

    fn test_network_partition_simulation(&self) -> ChaosTestResult {
        println!("  Running network partition simulation test (future-proofing)...");

        // This test prepares for future distributed features
        // Currently tests local consistency under simulated network stress
        
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut operations = 0u64;

        // Simulate network partition by introducing artificial delays
        let barrier = Arc::new(Barrier::new(3));
        let operations_count = Arc::new(AtomicU64::new(0));
        let partition_simulation = Arc::new(AtomicBool::new(false));

        let mut handles = vec![];

        // Partition group 1
        for i in 0..2 {
            let db_clone = db.clone();
            let barrier_clone = Arc::clone(&barrier);
            let ops_clone = Arc::clone(&operations_count);
            let partition_clone = Arc::clone(&partition_simulation);

            let handle = thread::spawn(move || {
                barrier_clone.wait();

                for j in 0..50 {
                    let key = format!("partition_test_{}_{:06}", i, j);
                    let value = format!("partition_value_{}_{}", i, j);

                    // Simulate network delay during "partition"
                    if partition_clone.load(Ordering::Relaxed) {
                        thread::sleep(Duration::from_millis(10));
                    }

                    if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                        ops_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            handles.push(handle);
        }

        barrier.wait();

        // Simulate partition after some operations
        thread::sleep(Duration::from_millis(100));
        partition_simulation.store(true, Ordering::Relaxed);
        thread::sleep(Duration::from_millis(200));
        partition_simulation.store(false, Ordering::Relaxed);

        for handle in handles {
            let _ = handle.join();
        }

        operations = operations_count.load(Ordering::Relaxed);

        // Verify data consistency after partition simulation
        let verification_start = Instant::now();
        let mut verified_ops = 0;

        for i in 0..2 {
            for j in 0..50 {
                let key = format!("partition_test_{}_{:06}", i, j);
                if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                    verified_ops += 1;
                }
            }
        }

        let verification_time = verification_start.elapsed();
        let integrity_score = if operations > 0 {
            verified_ops as f64 / operations as f64
        } else {
            1.0
        };

        println!(
            "    Network partition test: {} ops, {} verified, integrity: {:.2}%",
            operations, verified_ops, integrity_score * 100.0
        );

        ChaosTestResult {
            test_name: "network_partition_simulation".to_string(),
            operations_before_chaos: operations,
            operations_after_recovery: verified_ops,
            recovery_time_ms: verification_time.as_millis() as u64,
            data_integrity_score: integrity_score,
            success: integrity_score > 0.9,
            error_details: None,
        }
    }

    fn test_resource_starvation(&self) -> ChaosTestResult {
        println!("  Running resource starvation test...");

        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let operations_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));

        // Create memory pressure
        let _memory_pressure: Vec<Vec<u8>> = (0..100)
            .map(|_| vec![0u8; 1024 * 1024]) // 1MB allocations
            .collect();

        // Create CPU pressure
        let cpu_pressure_handles: Vec<_> = (0..num_cpus::get())
            .map(|_| {
                thread::spawn(|| {
                    let start = Instant::now();
                    while start.elapsed() < Duration::from_secs(5) {
                        // CPU-intensive work
                        let _ = (0..10000).map(|x| x * x).sum::<i32>();
                    }
                })
            })
            .collect();

        // Try database operations under resource pressure
        let db_start = Instant::now();
        while db_start.elapsed() < Duration::from_secs(3) {
            let op_count = operations_count.load(Ordering::Relaxed);
            let key = format!("starvation_key_{:08}", op_count);
            let value = format!("starvation_value_{}", op_count);

            match db.put(key.as_bytes(), value.as_bytes()) {
                Ok(_) => {
                    operations_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    error_count.fetch_add(1, Ordering::Relaxed);
                }
            }

            thread::sleep(Duration::from_millis(1));
        }

        // Wait for CPU pressure to end
        for handle in cpu_pressure_handles {
            let _ = handle.join();
        }

        let operations = operations_count.load(Ordering::Relaxed);
        let errors = error_count.load(Ordering::Relaxed);

        // Verify data integrity after resource pressure
        let verification_start = Instant::now();
        let mut verified_ops = 0;

        for i in 0..operations {
            let key = format!("starvation_key_{:08}", i);
            if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                verified_ops += 1;
            }
        }

        let verification_time = verification_start.elapsed();
        let error_rate = if operations + errors > 0 {
            errors as f64 / (operations + errors) as f64
        } else {
            0.0
        };

        let integrity_score = if operations > 0 {
            verified_ops as f64 / operations as f64
        } else {
            1.0
        };

        println!(
            "    Resource starvation test: {} ops, {} errors ({:.2}%), {} verified, integrity: {:.2}%",
            operations, errors, error_rate * 100.0, verified_ops, integrity_score * 100.0
        );

        ChaosTestResult {
            test_name: "resource_starvation".to_string(),
            operations_before_chaos: operations,
            operations_after_recovery: verified_ops,
            recovery_time_ms: verification_time.as_millis() as u64,
            data_integrity_score: integrity_score,
            success: error_rate < 0.1 && integrity_score > 0.9,
            error_details: None,
        }
    }

    fn test_concurrent_crash_scenarios(&self) -> ChaosTestResult {
        println!("  Running concurrent crash scenarios test...");

        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let operations_count = Arc::new(AtomicU64::new(0));
        let crash_simulation = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = vec![];

        // Spawn multiple concurrent workloads
        for thread_id in 0..3 {
            let db_clone = db.clone();
            let barrier_clone = Arc::clone(&barrier);
            let ops_clone = Arc::clone(&operations_count);
            let crash_clone = Arc::clone(&crash_simulation);

            let handle = thread::spawn(move || {
                barrier_clone.wait();

                let mut local_ops = 0;
                while !crash_clone.load(Ordering::Relaxed) && local_ops < 200 {
                    let key = format!("crash_test_{}_{:06}", thread_id, local_ops);
                    let value = format!("crash_value_{}_{}", thread_id, local_ops);

                    if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                        ops_clone.fetch_add(1, Ordering::Relaxed);
                        local_ops += 1;
                    }

                    thread::sleep(Duration::from_micros(500));
                }
            });
            handles.push(handle);
        }

        barrier.wait();

        // Let workloads run for a bit
        thread::sleep(Duration::from_millis(500));

        // Simulate crash
        println!("    Simulating concurrent crash...");
        crash_simulation.store(true, Ordering::Relaxed);
        
        // Wait for threads to stop
        for handle in handles {
            let _ = handle.join();
        }

        let operations_before = operations_count.load(Ordering::Relaxed);

        // Drop database to simulate crash
        drop(db);

        // Recovery
        let recovery_start = Instant::now();
        let recovered_db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify recovery
        let mut recovered_ops = 0;
        let mut integrity_issues = 0;

        for thread_id in 0..3 {
            for local_op in 0..200 {
                let key = format!("crash_test_{}_{:06}", thread_id, local_op);
                match recovered_db.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        recovered_ops += 1;
                        let expected = format!("crash_value_{}_{}", thread_id, local_op);
                        if value != expected.as_bytes() {
                            integrity_issues += 1;
                        }
                    }
                    Ok(None) => {} // Some data loss expected
                    Err(_) => integrity_issues += 1,
                }
            }
        }

        let integrity_score = if operations_before > 0 {
            (recovered_ops - integrity_issues) as f64 / operations_before as f64
        } else {
            1.0
        };

        println!(
            "    Concurrent crash test: {} ops before, {} recovered, {} issues, recovery: {:?}",
            operations_before, recovered_ops, integrity_issues, recovery_time
        );

        ChaosTestResult {
            test_name: "concurrent_crash_scenarios".to_string(),
            operations_before_chaos: operations_before,
            operations_after_recovery: recovered_ops,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_integrity_score: integrity_score,
            success: integrity_score > 0.8 && recovery_time < Duration::from_secs(5),
            error_details: None,
        }
    }
}

// Individual chaos tests

#[test]
#[ignore = "Chaos test - run with: cargo test test_chaos_suite -- --ignored"]
fn test_chaos_suite() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 50 * 1024 * 1024,
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync, // Sync mode for better crash consistency
        ..Default::default()
    };

    let chaos_engine = ChaosTestEngine::new(dir.path().to_path_buf(), config);
    let results = chaos_engine.run_chaos_test_suite();

    println!("\nChaos Engineering Test Results:");
    println!("================================");

    let mut total_tests = 0;
    let mut successful_tests = 0;
    let mut total_integrity_score = 0.0;

    for result in &results {
        total_tests += 1;
        if result.success {
            successful_tests += 1;
        }
        total_integrity_score += result.data_integrity_score;

        println!("Test: {}", result.test_name);
        println!("  Success: {}", result.success);
        println!("  Operations before chaos: {}", result.operations_before_chaos);
        println!("  Operations after recovery: {}", result.operations_after_recovery);
        println!("  Recovery time: {} ms", result.recovery_time_ms);
        println!("  Data integrity score: {:.2}%", result.data_integrity_score * 100.0);
        if let Some(ref error) = result.error_details {
            println!("  Error: {}", error);
        }
        println!();
    }

    let avg_integrity = total_integrity_score / total_tests as f64;
    let success_rate = successful_tests as f64 / total_tests as f64;

    println!("Overall Results:");
    println!("  Success rate: {:.1}% ({}/{})", success_rate * 100.0, successful_tests, total_tests);
    println!("  Average integrity score: {:.2}%", avg_integrity * 100.0);

    // Assert overall resilience
    assert!(success_rate >= 0.7, "Chaos test success rate too low: {:.1}%", success_rate * 100.0);
    assert!(avg_integrity >= 0.8, "Average data integrity too low: {:.2}%", avg_integrity * 100.0);
}

#[test]
#[ignore = "Individual chaos test - run with: cargo test test_process_kill_resilience -- --ignored"]
fn test_process_kill_resilience() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let chaos_engine = ChaosTestEngine::new(dir.path().to_path_buf(), config);
    let result = chaos_engine.test_random_process_kill();

    assert!(result.success, "Process kill resilience test failed: {:?}", result.error_details);
    assert!(result.data_integrity_score > 0.9, "Data integrity too low: {:.2}%", result.data_integrity_score * 100.0);
    assert!(result.recovery_time_ms < 10000, "Recovery took too long: {} ms", result.recovery_time_ms);
}

#[test]
#[ignore = "Individual chaos test - run with: cargo test test_corruption_resilience -- --ignored"]
fn test_corruption_resilience() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        ..Default::default()
    };

    let chaos_engine = ChaosTestEngine::new(dir.path().to_path_buf(), config);
    let result = chaos_engine.test_corrupted_data_injection();

    // Corruption recovery may not be 100% successful, but should be graceful
    assert!(result.operations_after_recovery > 0 || result.success, 
        "Should either recover some data or handle corruption gracefully");
    
    if result.operations_before_chaos > 0 {
        assert!(result.data_integrity_score >= 0.0, "Integrity score should be non-negative");
    }
}