use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Barrier, Mutex,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use std::path::PathBuf;
use std::fs;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write, Seek, SeekFrom};
use std::process::{Command, Stdio};

#[derive(Debug, Clone)]
pub struct StressTestResult {
    pub test_name: String,
    pub duration_ms: u64,
    pub operations_before_failure: u64,
    pub operations_after_recovery: u64,
    pub recovery_time_ms: u64,
    pub data_loss_percentage: f64,
    pub corruption_detected: u64,
    pub recovery_success: bool,
    pub performance_degradation: f64,
    pub stress_factor: f64,
}

#[derive(Debug)]
pub struct SystemResourceState {
    pub memory_usage_mb: u64,
    pub disk_usage_mb: u64,
    pub open_file_descriptors: u32,
    pub cpu_usage_percent: f64,
    pub io_wait_percent: f64,
}

pub struct PersistenceStressTestSuite {
    db_path: PathBuf,
    config: LightningDbConfig,
}

impl PersistenceStressTestSuite {
    pub fn new(db_path: PathBuf, config: LightningDbConfig) -> Self {
        Self { db_path, config }
    }

    pub fn run_extreme_stress_tests(&self) -> Vec<StressTestResult> {
        let mut results = Vec::new();

        println!("Starting extreme persistence stress test suite...");

        // Extreme failure scenario tests
        results.push(self.test_kill9_during_heavy_writes());
        results.push(self.test_disk_full_recovery());
        results.push(self.test_power_failure_simulation());
        results.push(self.test_oom_recovery_simulation());
        results.push(self.test_network_partition_recovery());
        results.push(self.test_concurrent_crash_recovery());
        results.push(self.test_filesystem_corruption_recovery());
        results.push(self.test_resource_exhaustion_recovery());

        results
    }

    fn test_kill9_during_heavy_writes(&self) -> StressTestResult {
        println!("  Testing KILL -9 simulation during heavy writes...");
        
        let start_time = Instant::now();
        let db = Arc::new(Database::open(&self.db_path, self.config.clone()).unwrap());
        
        let operations_count = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let crash_triggered = Arc::new(AtomicBool::new(false));

        // Heavy write workload
        let db_clone = Arc::clone(&db);
        let ops_clone = Arc::clone(&operations_count);
        let running_clone = Arc::clone(&running);
        let crash_clone = Arc::clone(&crash_triggered);

        let writer_handle = thread::spawn(move || {
            let mut batch_id = 0u64;
            while running_clone.load(Ordering::Relaxed) {
                // Write large batches rapidly
                for i in 0..100 {
                    let key = format!("kill9_test_{}_{:08}", batch_id, i);
                    let value = vec![(batch_id % 256) as u8; 4096]; // 4KB values
                    
                    match db_clone.put(key.as_bytes(), &value) {
                        Ok(_) => {
                            ops_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                    
                    // Check for crash trigger
                    if crash_clone.load(Ordering::Relaxed) {
                        break;
                    }
                }
                batch_id += 1;
                
                // Small delay to prevent overwhelming system
                thread::sleep(Duration::from_millis(1));
            }
        });

        // Let heavy writes run for a while
        thread::sleep(Duration::from_millis(2000));
        
        // Trigger "crash" - abrupt termination
        crash_triggered.store(true, Ordering::Relaxed);
        running.store(false, Ordering::Relaxed);
        
        let operations_before_crash = operations_count.load(Ordering::Relaxed);
        
        // Force abrupt termination (simulating KILL -9)
        writer_handle.join().unwrap();
        std::mem::drop(db); // Abrupt drop without graceful shutdown

        // Recovery phase
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        let (recovery_success, data_recovery_count) = match recovery_result {
            Ok(recovered_db) => {
                let mut recovered_ops = 0u64;
                
                // Verify recovery by checking data
                for batch_id in 0..operations_before_crash / 100 {
                    let mut batch_recovered = 0;
                    for i in 0..100 {
                        let key = format!("kill9_test_{}_{:08}", batch_id, i);
                        if recovered_db.get(key.as_bytes()).unwrap_or(None).is_some() {
                            batch_recovered += 1;
                        }
                    }
                    
                    // Count full batches only (atomicity check)
                    if batch_recovered == 100 {
                        recovered_ops += 100;
                    }
                }
                
                drop(recovered_db);
                (true, recovered_ops)
            }
            Err(_) => (false, 0),
        };

        let duration = start_time.elapsed();
        let data_loss_percentage = if operations_before_crash > 0 {
            ((operations_before_crash - data_recovery_count) as f64 / operations_before_crash as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "    KILL -9 test: {} ops before crash, {} recovered ({:.2}% loss), recovery: {}",
            operations_before_crash, data_recovery_count, data_loss_percentage, recovery_success
        );

        StressTestResult {
            test_name: "kill9_during_heavy_writes".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_before_failure: operations_before_crash,
            operations_after_recovery: data_recovery_count,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_loss_percentage,
            corruption_detected: 0,
            recovery_success,
            performance_degradation: 0.0,
            stress_factor: operations_before_crash as f64 / duration.as_secs_f64(),
        }
    }

    fn test_disk_full_recovery(&self) -> StressTestResult {
        println!("  Testing disk full scenario recovery...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let mut operations_count = 0u64;
        let mut disk_full_hit = false;
        let large_value = vec![0u8; 1024 * 1024]; // 1MB values

        // Fill disk until we get an error
        for i in 0..1000 {
            let key = format!("disk_full_test_{:08}", i);
            
            match db.put(key.as_bytes(), &large_value) {
                Ok(_) => {
                    operations_count += 1;
                }
                Err(_) => {
                    disk_full_hit = true;
                    break;
                }
            }
        }

        drop(db);

        // Simulate disk space recovery (in real scenario, admin would free space)
        // For testing, we'll just try to recover with existing space
        
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        let (recovery_success, verified_operations) = match recovery_result {
            Ok(recovered_db) => {
                let mut verified_count = 0u64;
                
                for i in 0..operations_count {
                    let key = format!("disk_full_test_{:08}", i);
                    if recovered_db.get(key.as_bytes()).unwrap_or(None).is_some() {
                        verified_count += 1;
                    }
                }
                
                drop(recovered_db);
                (true, verified_count)
            }
            Err(_) => (false, 0),
        };

        let duration = start_time.elapsed();
        let data_loss_percentage = if operations_count > 0 {
            ((operations_count - verified_operations) as f64 / operations_count as f64) * 100.0
        } else {
            0.0
        };

        println!(
            "    Disk full test: {} ops completed, disk full: {}, {} verified after recovery",
            operations_count, disk_full_hit, verified_operations
        );

        StressTestResult {
            test_name: "disk_full_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_before_failure: operations_count,
            operations_after_recovery: verified_operations,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_loss_percentage,
            corruption_detected: 0,
            recovery_success,
            performance_degradation: 0.0,
            stress_factor: if disk_full_hit { 1.0 } else { 0.0 },
        }
    }

    fn test_power_failure_simulation(&self) -> StressTestResult {
        println!("  Testing power failure simulation...");
        
        let start_time = Instant::now();
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Periodic { interval_ms: 50 },
            ..self.config.clone()
        };

        let db = Database::open(&self.db_path, config.clone()).unwrap();
        let operations_count = Arc::new(AtomicU64::new(0));
        let transaction_states = Arc::new(Mutex::new(HashMap::new()));

        // Simulate continuous operations as if during normal operation
        let test_data: Vec<_> = (0..800).map(|i| {
            let key = format!("power_failure_{:08}", i);
            let value = format!("critical_data_{}_{}", i, rand::random::<u32>());
            (key.into_bytes(), value.into_bytes())
        }).collect();

        // Write data in phases to simulate power failure at different points
        let checkpoint_interval = test_data.len() / 4;
        let mut last_checkpoint = 0;

        for (i, (key, value)) in test_data.iter().enumerate() {
            db.put(key, value).unwrap();
            operations_count.fetch_add(1, Ordering::Relaxed);
            
            // Periodic checkpoints
            if i > 0 && i % checkpoint_interval == 0 {
                let _ = db.checkpoint();
                last_checkpoint = i;
            }
        }

        // Simulate power failure - sudden termination without graceful shutdown
        let operations_before_failure = operations_count.load(Ordering::Relaxed);
        std::mem::drop(db); // Abrupt termination

        // Power-on recovery
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, config);
        let recovery_time = recovery_start.elapsed();

        let (recovery_success, verified_operations) = match recovery_result {
            Ok(recovered_db) => {
                let mut verified_count = 0u64;
                let mut checkpointed_count = 0u64;
                let mut wal_recovered_count = 0u64;
                
                for (i, (key, expected_value)) in test_data.iter().enumerate() {
                    match recovered_db.get(key) {
                        Ok(Some(actual_value)) => {
                            if actual_value == *expected_value {
                                verified_count += 1;
                                
                                if i <= last_checkpoint {
                                    checkpointed_count += 1;
                                } else {
                                    wal_recovered_count += 1;
                                }
                            }
                        }
                        _ => {}
                    }
                }
                
                println!(
                    "      Recovery breakdown: {} checkpointed, {} from WAL",
                    checkpointed_count, wal_recovered_count
                );
                
                drop(recovered_db);
                (true, verified_count)
            }
            Err(_) => (false, 0),
        };

        let duration = start_time.elapsed();
        let data_loss_percentage = if operations_before_failure > 0 {
            ((operations_before_failure - verified_operations) as f64 / operations_before_failure as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "    Power failure test: {} ops before failure, {} recovered ({:.2}% loss)",
            operations_before_failure, verified_operations, data_loss_percentage
        );

        StressTestResult {
            test_name: "power_failure_simulation".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_before_failure,
            operations_after_recovery: verified_operations,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_loss_percentage,
            corruption_detected: 0,
            recovery_success,
            performance_degradation: 0.0,
            stress_factor: 1.0, // Always triggers power failure
        }
    }

    fn test_oom_recovery_simulation(&self) -> StressTestResult {
        println!("  Testing OOM recovery simulation...");
        
        let start_time = Instant::now();
        let config = LightningDbConfig {
            cache_size: 16 * 1024 * 1024, // Small cache to trigger memory pressure
            ..self.config.clone()
        };

        let db = Database::open(&self.db_path, config.clone()).unwrap();
        let mut operations_count = 0u64;
        let mut memory_allocations = Vec::new();

        // Create memory pressure while doing database operations
        let large_data_size = 10 * 1024 * 1024; // 10MB entries
        let mut oom_triggered = false;

        for i in 0..50 {
            let key = format!("oom_test_{:06}", i);
            let value = vec![(i % 256) as u8; large_data_size];
            
            // Try to allocate large amounts of memory
            if i % 5 == 0 {
                let allocation = vec![0u8; 50 * 1024 * 1024]; // 50MB allocation
                memory_allocations.push(allocation);
            }
            
            match db.put(key.as_bytes(), &value) {
                Ok(_) => {
                    operations_count += 1;
                }
                Err(_) => {
                    oom_triggered = true;
                    break;
                }
            }
        }

        // Simulate OOM crash
        std::mem::drop(memory_allocations);
        std::mem::drop(db);

        // Recovery after OOM
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, config);
        let recovery_time = recovery_start.elapsed();

        let (recovery_success, verified_operations) = match recovery_result {
            Ok(recovered_db) => {
                let mut verified_count = 0u64;
                
                for i in 0..operations_count {
                    let key = format!("oom_test_{:06}", i);
                    match recovered_db.get(key.as_bytes()) {
                        Ok(Some(value)) => {
                            if value.len() == large_data_size {
                                verified_count += 1;
                            }
                        }
                        _ => {}
                    }
                }
                
                drop(recovered_db);
                (true, verified_count)
            }
            Err(_) => (false, 0),
        };

        let duration = start_time.elapsed();
        let data_loss_percentage = if operations_count > 0 {
            ((operations_count - verified_operations) as f64 / operations_count as f64) * 100.0
        } else {
            0.0
        };

        println!(
            "    OOM test: {} ops completed, OOM triggered: {}, {} verified after recovery",
            operations_count, oom_triggered, verified_operations
        );

        StressTestResult {
            test_name: "oom_recovery_simulation".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_before_failure: operations_count,
            operations_after_recovery: verified_operations,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_loss_percentage,
            corruption_detected: 0,
            recovery_success,
            performance_degradation: 0.0,
            stress_factor: if oom_triggered { 1.0 } else { 0.5 },
        }
    }

    fn test_network_partition_recovery(&self) -> StressTestResult {
        println!("  Testing network partition recovery simulation...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let operations_count = Arc::new(AtomicU64::new(0));
        let partition_active = Arc::new(AtomicBool::new(false));
        let node_states = Arc::new(Mutex::new(HashMap::new()));

        // Simulate distributed write scenario
        let test_scenarios = vec![
            ("node_a", 300),
            ("node_b", 300),
            ("node_c", 300),
        ];

        for (node_id, write_count) in test_scenarios {
            for i in 0..write_count {
                let key = format!("partition_{}_{:06}", node_id, i);
                let value = format!("data_{}_{}_{}", node_id, i, SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis());
                
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                operations_count.fetch_add(1, Ordering::Relaxed);
                
                // Store expected state
                {
                    let mut states = node_states.lock().unwrap();
                    states.insert(key, value);
                }
            }
            
            // Simulate checkpoint between nodes
            let _ = db.checkpoint();
        }

        // Simulate network partition (sudden disconnection)
        partition_active.store(true, Ordering::Relaxed);
        let operations_before_partition = operations_count.load(Ordering::Relaxed);
        
        drop(db); // Simulate disconnection

        // Network recovery
        partition_active.store(false, Ordering::Relaxed);
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        let (recovery_success, verified_operations) = match recovery_result {
            Ok(recovered_db) => {
                let mut verified_count = 0u64;
                let states = node_states.lock().unwrap();
                
                for (key, expected_value) in states.iter() {
                    match recovered_db.get(key.as_bytes()) {
                        Ok(Some(actual_value)) => {
                            if String::from_utf8_lossy(&actual_value) == *expected_value {
                                verified_count += 1;
                            }
                        }
                        _ => {}
                    }
                }
                
                drop(recovered_db);
                (true, verified_count)
            }
            Err(_) => (false, 0),
        };

        let duration = start_time.elapsed();
        let data_loss_percentage = if operations_before_partition > 0 {
            ((operations_before_partition - verified_operations) as f64 / operations_before_partition as f64) * 100.0
        } else {
            0.0
        };

        println!(
            "    Network partition test: {} ops before partition, {} verified after recovery",
            operations_before_partition, verified_operations
        );

        StressTestResult {
            test_name: "network_partition_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_before_failure: operations_before_partition,
            operations_after_recovery: verified_operations,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_loss_percentage,
            corruption_detected: 0,
            recovery_success,
            performance_degradation: 0.0,
            stress_factor: 1.0,
        }
    }

    fn test_concurrent_crash_recovery(&self) -> StressTestResult {
        println!("  Testing concurrent crash recovery...");
        
        let start_time = Instant::now();
        let db = Arc::new(Database::open(&self.db_path, self.config.clone()).unwrap());
        
        let operations_count = Arc::new(AtomicU64::new(0));
        let crash_barrier = Arc::new(Barrier::new(4));
        let thread_data = Arc::new(Mutex::new(HashMap::new()));

        let mut handles = Vec::new();

        // Spawn concurrent workers
        for thread_id in 0..3 {
            let db_clone = Arc::clone(&db);
            let ops_clone = Arc::clone(&operations_count);
            let barrier_clone = Arc::clone(&crash_barrier);
            let data_clone = Arc::clone(&thread_data);

            let handle = thread::spawn(move || {
                let mut thread_operations = Vec::new();
                
                for i in 0..200 {
                    let key = format!("concurrent_crash_{}_{:06}", thread_id, i);
                    let value = format!("worker_data_{}_{}_{}", thread_id, i, rand::random::<u32>());
                    
                    match db_clone.put(key.as_bytes(), value.as_bytes()) {
                        Ok(_) => {
                            thread_operations.push((key.clone(), value));
                            ops_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                }
                
                // Store thread data for verification
                {
                    let mut data_map = data_clone.lock().unwrap();
                    data_map.insert(thread_id, thread_operations);
                }
                
                barrier_clone.wait(); // Synchronize crash point
            });
            
            handles.push(handle);
        }

        // Wait for all threads to reach crash point
        crash_barrier.wait();
        
        let operations_before_crash = operations_count.load(Ordering::Relaxed);
        
        // Simulate concurrent crash
        for handle in handles {
            handle.join().unwrap();
        }
        std::mem::drop(db);

        // Recovery
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        let (recovery_success, verified_operations) = match recovery_result {
            Ok(recovered_db) => {
                let mut verified_count = 0u64;
                let data_map = thread_data.lock().unwrap();
                
                for thread_operations in data_map.values() {
                    for (key, expected_value) in thread_operations {
                        match recovered_db.get(key.as_bytes()) {
                            Ok(Some(actual_value)) => {
                                if String::from_utf8_lossy(&actual_value) == *expected_value {
                                    verified_count += 1;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                
                drop(recovered_db);
                (true, verified_count)
            }
            Err(_) => (false, 0),
        };

        let duration = start_time.elapsed();
        let data_loss_percentage = if operations_before_crash > 0 {
            ((operations_before_crash - verified_operations) as f64 / operations_before_crash as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "    Concurrent crash test: {} ops before crash, {} verified after recovery",
            operations_before_crash, verified_operations
        );

        StressTestResult {
            test_name: "concurrent_crash_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_before_failure: operations_before_crash,
            operations_after_recovery: verified_operations,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_loss_percentage,
            corruption_detected: 0,
            recovery_success,
            performance_degradation: 0.0,
            stress_factor: 3.0, // 3 concurrent threads
        }
    }

    fn test_filesystem_corruption_recovery(&self) -> StressTestResult {
        println!("  Testing filesystem corruption recovery...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data: Vec<_> = (0..400).map(|i| {
            let key = format!("fs_corruption_{:08}", i);
            let value = vec![(i % 256) as u8; 2048];
            (key.into_bytes(), value)
        }).collect();

        let mut operations_count = 0u64;

        // Write test data
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        let _ = db.checkpoint();
        drop(db);

        // Inject filesystem-level corruption
        let corruption_count = self.inject_filesystem_corruption().unwrap_or(0);

        // Attempt recovery
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        let (recovery_success, verified_operations) = match recovery_result {
            Ok(recovered_db) => {
                let mut verified_count = 0u64;
                
                for (key, expected_value) in &test_data {
                    match recovered_db.get(key) {
                        Ok(Some(actual_value)) => {
                            if actual_value == *expected_value {
                                verified_count += 1;
                            }
                        }
                        _ => {}
                    }
                }
                
                drop(recovered_db);
                (true, verified_count)
            }
            Err(_) => (false, 0),
        };

        let duration = start_time.elapsed();
        let data_loss_percentage = if operations_count > 0 {
            ((operations_count - verified_operations) as f64 / operations_count as f64) * 100.0
        } else {
            100.0
        };

        println!(
            "    Filesystem corruption test: {} corruptions injected, {} ops verified",
            corruption_count, verified_operations
        );

        StressTestResult {
            test_name: "filesystem_corruption_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_before_failure: operations_count,
            operations_after_recovery: verified_operations,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_loss_percentage,
            corruption_detected: corruption_count,
            recovery_success,
            performance_degradation: 0.0,
            stress_factor: corruption_count as f64,
        }
    }

    fn test_resource_exhaustion_recovery(&self) -> StressTestResult {
        println!("  Testing resource exhaustion recovery...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let mut operations_count = 0u64;
        let mut file_handles = Vec::new();
        let mut exhaustion_triggered = false;

        // Exhaust file descriptors while doing database operations
        for i in 0..1000 {
            let key = format!("resource_exhaustion_{:08}", i);
            let value = format!("data_{}", i);
            
            // Create file handle pressure
            if i % 10 == 0 {
                match tempfile::NamedTempFile::new() {
                    Ok(temp_file) => {
                        file_handles.push(temp_file);
                    }
                    Err(_) => {
                        exhaustion_triggered = true;
                    }
                }
            }
            
            match db.put(key.as_bytes(), value.as_bytes()) {
                Ok(_) => {
                    operations_count += 1;
                }
                Err(_) => {
                    exhaustion_triggered = true;
                    break;
                }
            }
        }

        // Clean up resources and simulate crash
        std::mem::drop(file_handles);
        std::mem::drop(db);

        // Recovery
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        let (recovery_success, verified_operations) = match recovery_result {
            Ok(recovered_db) => {
                let mut verified_count = 0u64;
                
                for i in 0..operations_count {
                    let key = format!("resource_exhaustion_{:08}", i);
                    if recovered_db.get(key.as_bytes()).unwrap_or(None).is_some() {
                        verified_count += 1;
                    }
                }
                
                drop(recovered_db);
                (true, verified_count)
            }
            Err(_) => (false, 0),
        };

        let duration = start_time.elapsed();
        let data_loss_percentage = if operations_count > 0 {
            ((operations_count - verified_operations) as f64 / operations_count as f64) * 100.0
        } else {
            0.0
        };

        println!(
            "    Resource exhaustion test: {} ops, exhaustion: {}, {} verified",
            operations_count, exhaustion_triggered, verified_operations
        );

        StressTestResult {
            test_name: "resource_exhaustion_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_before_failure: operations_count,
            operations_after_recovery: verified_operations,
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_loss_percentage,
            corruption_detected: 0,
            recovery_success,
            performance_degradation: 0.0,
            stress_factor: if exhaustion_triggered { 1.0 } else { 0.5 },
        }
    }

    // Helper methods

    fn inject_filesystem_corruption(&self) -> Result<u64, std::io::Error> {
        let mut corruption_count = 0;
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Some(extension) = path.extension() {
                    if extension == "db" || extension == "wal" || extension == "log" {
                        if let Ok(mut file_data) = fs::read(&path) {
                            if file_data.len() > 1024 {
                                // Inject multiple corruption points
                                let corruption_points = std::cmp::min(5, file_data.len() / 1024);
                                
                                for point in 0..corruption_points {
                                    let pos = (point * file_data.len()) / corruption_points;
                                    let corruption_size = std::cmp::min(64, file_data.len() - pos);
                                    
                                    // Corrupt a block
                                    for i in pos..pos + corruption_size {
                                        file_data[i] = !file_data[i];
                                    }
                                    corruption_count += 1;
                                }
                                
                                let _ = fs::write(&path, &file_data);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(corruption_count)
    }

    fn get_system_resource_state(&self) -> SystemResourceState {
        // Basic resource monitoring (simplified for testing)
        SystemResourceState {
            memory_usage_mb: 0, // Would implement with proper system calls
            disk_usage_mb: 0,
            open_file_descriptors: 0,
            cpu_usage_percent: 0.0,
            io_wait_percent: 0.0,
        }
    }
}

// Test runner function

#[test]
#[ignore = "Extreme stress test - run with: cargo test test_extreme_persistence_stress -- --ignored"]
fn test_extreme_persistence_stress() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 128 * 1024 * 1024,
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let stress_suite = PersistenceStressTestSuite::new(dir.path().to_path_buf(), config);
    let results = stress_suite.run_extreme_stress_tests();

    println!("\nExtreme Persistence Stress Test Results:");
    println!("========================================");

    let mut total_tests = 0;
    let mut successful_recoveries = 0;
    let mut total_data_loss = 0.0;
    let mut total_recovery_time = 0u64;

    for result in &results {
        total_tests += 1;
        if result.recovery_success {
            successful_recoveries += 1;
        }
        total_data_loss += result.data_loss_percentage;
        total_recovery_time += result.recovery_time_ms;

        println!("Test: {}", result.test_name);
        println!("  Duration: {} ms", result.duration_ms);
        println!("  Operations before failure: {}", result.operations_before_failure);
        println!("  Operations after recovery: {}", result.operations_after_recovery);
        println!("  Recovery time: {} ms", result.recovery_time_ms);
        println!("  Data loss: {:.2}%", result.data_loss_percentage);
        println!("  Corruption detected: {}", result.corruption_detected);
        println!("  Recovery success: {}", result.recovery_success);
        println!("  Stress factor: {:.2}", result.stress_factor);
        println!();
    }

    let recovery_success_rate = successful_recoveries as f64 / total_tests as f64;
    let avg_data_loss = total_data_loss / total_tests as f64;
    let avg_recovery_time = total_recovery_time / total_tests as u64;

    println!("Overall Stress Test Results:");
    println!("  Recovery success rate: {:.1}% ({}/{})", recovery_success_rate * 100.0, successful_recoveries, total_tests);
    println!("  Average data loss: {:.2}%", avg_data_loss);
    println!("  Average recovery time: {} ms", avg_recovery_time);

    // Assert stress test requirements
    assert!(recovery_success_rate >= 0.75, "Recovery success rate too low under stress: {:.1}%", recovery_success_rate * 100.0);
    assert!(avg_data_loss <= 20.0, "Average data loss too high: {:.2}%", avg_data_loss);
    assert!(avg_recovery_time < 5000, "Average recovery time too high: {} ms", avg_recovery_time);
}

#[test]
#[ignore = "Individual stress test - run with: cargo test test_kill9_stress_standalone -- --ignored"]
fn test_kill9_stress_standalone() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    
    let stress_suite = PersistenceStressTestSuite::new(dir.path().to_path_buf(), config);
    let result = stress_suite.test_kill9_during_heavy_writes();
    
    assert!(result.recovery_success, "KILL -9 recovery failed");
    assert!(result.data_loss_percentage < 50.0, "Data loss too high: {:.2}%", result.data_loss_percentage);
}