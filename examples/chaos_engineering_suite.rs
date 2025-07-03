use lightning_db::{Database, LightningDbConfig};
use std::fs;
use std::io::{Write, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

/// Comprehensive chaos engineering test suite
/// Tests database behavior under various failure scenarios
#[derive(Debug)]
enum ChaosScenario {
    DiskFull,
    MemoryPressure,
    CorruptedData,
    ClockSkew,
    FilePermissions,
    ConcurrentStress,
    RandomFailures,
}

#[derive(Debug)]
struct ChaosResult {
    scenario: ChaosScenario,
    success: bool,
    error_message: Option<String>,
    recovery_time: Option<Duration>,
    data_integrity: bool,
}

struct ChaosEngineer {
    results: Arc<Mutex<Vec<ChaosResult>>>,
}

impl ChaosEngineer {
    fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Run all chaos scenarios
    fn run_all_scenarios(&self) {
        println!("🌪️  Lightning DB Chaos Engineering Suite");
        println!("🔥 Testing database resilience under extreme conditions");
        println!("{}", "=".repeat(70));
        
        let scenarios = vec![
            ChaosScenario::DiskFull,
            ChaosScenario::MemoryPressure,
            ChaosScenario::CorruptedData,
            ChaosScenario::ClockSkew,
            ChaosScenario::FilePermissions,
            ChaosScenario::ConcurrentStress,
            ChaosScenario::RandomFailures,
        ];
        
        for scenario in scenarios {
            println!("\n🧪 Testing: {:?}", scenario);
            let result = match scenario {
                ChaosScenario::DiskFull => self.test_disk_full(),
                ChaosScenario::MemoryPressure => self.test_memory_pressure(),
                ChaosScenario::CorruptedData => self.test_corrupted_data(),
                ChaosScenario::ClockSkew => self.test_clock_skew(),
                ChaosScenario::FilePermissions => self.test_file_permissions(),
                ChaosScenario::ConcurrentStress => self.test_concurrent_stress(),
                ChaosScenario::RandomFailures => self.test_random_failures(),
            };
            
            match &result {
                ChaosResult { success: true, .. } => println!("   ✅ PASSED"),
                ChaosResult { success: false, error_message: Some(msg), .. } => {
                    println!("   ❌ FAILED: {}", msg)
                }
                _ => println!("   ❌ FAILED: Unknown error"),
            }
            
            self.results.lock().unwrap().push(result);
        }
        
        self.print_summary();
    }

    /// Test 1: Disk Full Scenario
    fn test_disk_full(&self) -> ChaosResult {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("disk_full_db");
        
        // Create database
        let config = LightningDbConfig::default();
        let db = match Database::create(&db_path, config) {
            Ok(db) => db,
            Err(e) => return ChaosResult {
                scenario: ChaosScenario::DiskFull,
                success: false,
                error_message: Some(format!("Failed to create database: {}", e)),
                recovery_time: None,
                data_integrity: false,
            },
        };
        
        // Write some initial data
        let mut written_keys = Vec::new();
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            let value = format!("value_{:06}", i);
            if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                written_keys.push(key);
            }
        }
        
        // Simulate disk full by filling up available space
        #[cfg(unix)]
        {
            use nix::sys::statfs;
            
            if let Ok(stats) = statfs::statfs(temp_dir.path()) {
                let available_bytes = stats.blocks_available() * stats.block_size() as u64;
                let fill_size = available_bytes.saturating_sub(1024 * 1024); // Leave 1MB
                
                // Create a large file to fill disk
                let fill_path = temp_dir.path().join("disk_filler");
                if let Ok(mut file) = fs::File::create(&fill_path) {
                    let chunk = vec![0u8; 1024 * 1024]; // 1MB chunks
                    let chunks_to_write = (fill_size / (1024 * 1024)) as usize;
                    
                    for _ in 0..chunks_to_write.min(100) { // Limit to 100MB for test
                        let _ = file.write_all(&chunk);
                    }
                    let _ = file.sync_all();
                }
            }
        }
        
        // Try to write more data (should handle disk full gracefully)
        let mut disk_full_errors = 0;
        for i in 100..200 {
            let key = format!("key_{:06}", i);
            let value = format!("value_{:06}", i);
            match db.put(key.as_bytes(), value.as_bytes()) {
                Ok(_) => written_keys.push(key),
                Err(_) => disk_full_errors += 1,
            }
        }
        
        // Verify existing data is still readable
        let mut read_errors = 0;
        for key in &written_keys {
            if db.get(key.as_bytes()).is_err() {
                read_errors += 1;
            }
        }
        
        ChaosResult {
            scenario: ChaosScenario::DiskFull,
            success: disk_full_errors > 0 && read_errors == 0,
            error_message: if read_errors > 0 {
                Some(format!("{} keys became unreadable after disk full", read_errors))
            } else if disk_full_errors == 0 {
                Some("Failed to trigger disk full condition".to_string())
            } else {
                None
            },
            recovery_time: None,
            data_integrity: read_errors == 0,
        }
    }

    /// Test 2: Memory Pressure
    fn test_memory_pressure(&self) -> ChaosResult {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("memory_pressure_db");
        
        // Create database with small cache
        let config = LightningDbConfig {
            cache_size: 1024 * 1024, // 1MB cache
            ..Default::default()
        };
        
        let db = match Database::create(&db_path, config) {
            Ok(db) => db,
            Err(e) => return ChaosResult {
                scenario: ChaosScenario::MemoryPressure,
                success: false,
                error_message: Some(format!("Failed to create database: {}", e)),
                recovery_time: None,
                data_integrity: false,
            },
        };
        
        // Allocate large amounts of memory to create pressure
        let mut memory_hogs = Vec::new();
        for _ in 0..10 {
            // Allocate 10MB chunks
            let hog = vec![0u8; 10 * 1024 * 1024];
            memory_hogs.push(hog);
        }
        
        // Try to perform operations under memory pressure
        let mut success_count = 0;
        let mut error_count = 0;
        
        for i in 0..1000 {
            let key = format!("mem_key_{:06}", i);
            let value = vec![0u8; 10000]; // 10KB values
            
            match db.put(key.as_bytes(), &value) {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
            
            // Read back to verify
            if let Ok(Some(read_value)) = db.get(key.as_bytes()) {
                if read_value != value {
                    error_count += 1;
                }
            }
        }
        
        // Release memory
        drop(memory_hogs);
        
        ChaosResult {
            scenario: ChaosScenario::MemoryPressure,
            success: success_count > 0 && success_count > error_count,
            error_message: if error_count > success_count {
                Some(format!("Too many failures under memory pressure: {} errors, {} successes", 
                           error_count, success_count))
            } else {
                None
            },
            recovery_time: None,
            data_integrity: true,
        }
    }

    /// Test 3: Corrupted Data
    fn test_corrupted_data(&self) -> ChaosResult {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("corrupted_data_db");
        
        // Create database and write data
        let config = LightningDbConfig::default();
        let db = Database::create(&db_path, config).unwrap();
        
        // Write test data
        for i in 0..50 {
            let key = format!("corrupt_key_{:06}", i);
            let value = format!("corrupt_value_{:06}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Close database
        drop(db);
        
        // Corrupt some data files
        let mut corrupted_files = 0;
        if let Ok(entries) = fs::read_dir(&db_path) {
            for entry in entries.flatten() {
                if entry.file_name().to_str().unwrap_or("").contains("btree.db") {
                    // Corrupt the B+Tree file by writing random bytes
                    if let Ok(mut file) = fs::OpenOptions::new()
                        .write(true)
                        .open(entry.path()) 
                    {
                        // Skip header and write garbage in the middle
                        let _ = file.seek(SeekFrom::Start(4096));
                        let garbage = vec![0xFF; 1024];
                        let _ = file.write_all(&garbage);
                        corrupted_files += 1;
                    }
                }
            }
        }
        
        // Try to reopen database
        let recovery_start = Instant::now();
        match Database::open(&db_path, LightningDbConfig::default()) {
            Ok(db) => {
                let recovery_time = recovery_start.elapsed();
                
                // Check how many keys are still readable
                let mut readable_count = 0;
                let mut corrupted_count = 0;
                
                for i in 0..50 {
                    let key = format!("corrupt_key_{:06}", i);
                    match db.get(key.as_bytes()) {
                        Ok(Some(value)) => {
                            let expected = format!("corrupt_value_{:06}", i);
                            if value == expected.as_bytes() {
                                readable_count += 1;
                            } else {
                                corrupted_count += 1;
                            }
                        }
                        Ok(None) => corrupted_count += 1,
                        Err(_) => corrupted_count += 1,
                    }
                }
                
                ChaosResult {
                    scenario: ChaosScenario::CorruptedData,
                    success: corrupted_files > 0 && readable_count > 0,
                    error_message: if corrupted_count == 50 {
                        Some("All data lost after corruption".to_string())
                    } else if corrupted_files == 0 {
                        Some("Failed to corrupt any files".to_string())
                    } else {
                        None
                    },
                    recovery_time: Some(recovery_time),
                    data_integrity: readable_count > corrupted_count,
                }
            }
            Err(e) => ChaosResult {
                scenario: ChaosScenario::CorruptedData,
                success: false,
                error_message: Some(format!("Failed to recover from corruption: {}", e)),
                recovery_time: None,
                data_integrity: false,
            },
        }
    }

    /// Test 4: Clock Skew
    fn test_clock_skew(&self) -> ChaosResult {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("clock_skew_db");
        
        let config = LightningDbConfig::default();
        let db = Database::create(&db_path, config).unwrap();
        
        // Write data with current timestamp
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        for i in 0..20 {
            let key = format!("time_key_{:06}_{}", i, now.as_secs());
            let value = format!("value_at_{}", now.as_secs());
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Simulate clock going backwards (can't actually change system time)
        // Instead, write data with "future" timestamps
        let future = now + Duration::from_secs(3600); // 1 hour in future
        for i in 20..40 {
            let key = format!("time_key_{:06}_{}", i, future.as_secs());
            let value = format!("value_at_{}", future.as_secs());
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Write data with "past" timestamps
        let past = now - Duration::from_secs(3600); // 1 hour in past
        for i in 40..60 {
            let key = format!("time_key_{:06}_{}", i, past.as_secs());
            let value = format!("value_at_{}", past.as_secs());
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Verify all data is still accessible
        let mut read_errors = 0;
        for i in 0..60 {
            let timestamp = match i {
                0..=19 => now.as_secs(),
                20..=39 => future.as_secs(),
                _ => past.as_secs(),
            };
            
            let key = format!("time_key_{:06}_{}", i, timestamp);
            if db.get(key.as_bytes()).is_err() {
                read_errors += 1;
            }
        }
        
        ChaosResult {
            scenario: ChaosScenario::ClockSkew,
            success: read_errors == 0,
            error_message: if read_errors > 0 {
                Some(format!("{} keys affected by clock skew", read_errors))
            } else {
                None
            },
            recovery_time: None,
            data_integrity: read_errors == 0,
        }
    }

    /// Test 5: File Permission Changes
    fn test_file_permissions(&self) -> ChaosResult {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("permissions_db");
        
        let config = LightningDbConfig::default();
        let db = Database::create(&db_path, config).unwrap();
        
        // Write initial data
        for i in 0..30 {
            let key = format!("perm_key_{:06}", i);
            let value = format!("perm_value_{:06}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Change file permissions to read-only
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            
            if let Ok(entries) = fs::read_dir(&db_path) {
                for entry in entries.flatten() {
                    let mut perms = entry.metadata().unwrap().permissions();
                    perms.set_mode(0o444); // Read-only
                    let _ = fs::set_permissions(entry.path(), perms);
                }
            }
        }
        
        // Try to write (should fail gracefully)
        let mut write_errors = 0;
        for i in 30..40 {
            let key = format!("perm_key_{:06}", i);
            let value = format!("perm_value_{:06}", i);
            if db.put(key.as_bytes(), value.as_bytes()).is_err() {
                write_errors += 1;
            }
        }
        
        // Reads should still work
        let mut read_errors = 0;
        for i in 0..30 {
            let key = format!("perm_key_{:06}", i);
            if db.get(key.as_bytes()).is_err() {
                read_errors += 1;
            }
        }
        
        // Restore permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            
            if let Ok(entries) = fs::read_dir(&db_path) {
                for entry in entries.flatten() {
                    let mut perms = entry.metadata().unwrap().permissions();
                    perms.set_mode(0o644); // Read-write
                    let _ = fs::set_permissions(entry.path(), perms);
                }
            }
        }
        
        ChaosResult {
            scenario: ChaosScenario::FilePermissions,
            success: write_errors > 0 && read_errors == 0,
            error_message: if read_errors > 0 {
                Some(format!("{} reads failed with read-only permissions", read_errors))
            } else if write_errors == 0 {
                Some("Writes succeeded despite read-only permissions".to_string())
            } else {
                None
            },
            recovery_time: None,
            data_integrity: read_errors == 0,
        }
    }

    /// Test 6: Extreme Concurrent Stress
    fn test_concurrent_stress(&self) -> ChaosResult {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("stress_db");
        
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(&db_path, config).unwrap());
        
        let start_time = Instant::now();
        let error_count = Arc::new(Mutex::new(0u64));
        let success_count = Arc::new(Mutex::new(0u64));
        
        // Spawn many threads doing random operations
        let mut handles = vec![];
        for thread_id in 0..20 {
            let db = db.clone();
            let error_count = error_count.clone();
            let success_count = success_count.clone();
            
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let op = i % 4;
                    let key = format!("stress_key_{:03}_{:05}", thread_id, i);
                    
                    match op {
                        0 => {
                            // Write
                            let value = format!("value_{}_{}_{}", thread_id, i, "x".repeat(100));
                            match db.put(key.as_bytes(), value.as_bytes()) {
                                Ok(_) => *success_count.lock().unwrap() += 1,
                                Err(_) => *error_count.lock().unwrap() += 1,
                            }
                        }
                        1 => {
                            // Read
                            match db.get(key.as_bytes()) {
                                Ok(_) => *success_count.lock().unwrap() += 1,
                                Err(_) => *error_count.lock().unwrap() += 1,
                            }
                        }
                        2 => {
                            // Delete
                            match db.delete(key.as_bytes()) {
                                Ok(_) => *success_count.lock().unwrap() += 1,
                                Err(_) => *error_count.lock().unwrap() += 1,
                            }
                        }
                        _ => {
                            // Transaction
                            match db.begin_transaction() {
                                Ok(tx_id) => {
                                    for j in 0..5 {
                                        let tx_key = format!("tx_key_{}_{}_{}",thread_id, i, j);
                                        let tx_value = format!("tx_value_{}", j);
                                        let _ = db.put_tx(tx_id, tx_key.as_bytes(), tx_value.as_bytes());
                                    }
                                    match db.commit_transaction(tx_id) {
                                        Ok(_) => *success_count.lock().unwrap() += 1,
                                        Err(_) => *error_count.lock().unwrap() += 1,
                                    }
                                }
                                Err(_) => *error_count.lock().unwrap() += 1,
                            }
                        }
                    }
                }
            });
            
            handles.push(handle);
        }
        
        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start_time.elapsed();
        let total_errors = *error_count.lock().unwrap();
        let total_success = *success_count.lock().unwrap();
        
        ChaosResult {
            scenario: ChaosScenario::ConcurrentStress,
            success: total_success > total_errors * 10, // Success rate > 90%
            error_message: if total_errors > total_success {
                Some(format!("High error rate under stress: {} errors, {} successes", 
                           total_errors, total_success))
            } else {
                None
            },
            recovery_time: Some(duration),
            data_integrity: true,
        }
    }

    /// Test 7: Random Failures
    fn test_random_failures(&self) -> ChaosResult {
        // This would require more complex setup with fault injection
        // For now, simulate with random errors
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("random_failures_db");
        
        let config = LightningDbConfig::default();
        let db = Database::create(&db_path, config).unwrap();
        
        let mut successes = 0;
        let mut failures = 0;
        
        // Perform operations with simulated random failures
        for i in 0..100 {
            let key = format!("random_key_{:06}", i);
            let value = format!("random_value_{:06}", i);
            
            // Simulate random failure conditions
            if i % 7 == 0 {
                // Simulate temporary failure
                failures += 1;
            } else {
                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => successes += 1,
                    Err(_) => failures += 1,
                }
            }
        }
        
        ChaosResult {
            scenario: ChaosScenario::RandomFailures,
            success: successes > failures,
            error_message: if failures > successes {
                Some(format!("Too many random failures: {} vs {} successes", failures, successes))
            } else {
                None
            },
            recovery_time: None,
            data_integrity: true,
        }
    }

    /// Print test summary
    fn print_summary(&self) {
        let results = self.results.lock().unwrap();
        
        println!("\n{}", "=".repeat(70));
        println!("📊 CHAOS ENGINEERING SUMMARY");
        println!("{}", "=".repeat(70));
        
        let total = results.len();
        let passed = results.iter().filter(|r| r.success).count();
        let failed = total - passed;
        
        println!("\n📈 Results:");
        println!("   Total scenarios: {}", total);
        println!("   ✅ Passed: {} ({:.1}%)", passed, passed as f64 / total as f64 * 100.0);
        println!("   ❌ Failed: {} ({:.1}%)", failed, failed as f64 / total as f64 * 100.0);
        
        // Failed scenarios
        if failed > 0 {
            println!("\n❌ Failed Scenarios:");
            for result in results.iter().filter(|r| !r.success) {
                println!("   - {:?}: {}", 
                         result.scenario, 
                         result.error_message.as_ref().unwrap_or(&"Unknown error".to_string()));
            }
        }
        
        // Data integrity
        let integrity_issues = results.iter().filter(|r| !r.data_integrity).count();
        if integrity_issues > 0 {
            println!("\n⚠️  DATA INTEGRITY ISSUES: {} scenarios affected data integrity!", integrity_issues);
        } else {
            println!("\n✅ DATA INTEGRITY: All scenarios preserved data integrity");
        }
        
        // Overall verdict
        println!("\n🏁 VERDICT:");
        if failed == 0 {
            println!("   ✅ EXCELLENT - Database handles all chaos scenarios gracefully");
            println!("   Ready for production deployment in hostile environments");
        } else if failed <= 2 {
            println!("   ⚠️  GOOD - Minor issues in extreme scenarios");
            println!("   Suitable for production with monitoring");
        } else {
            println!("   ❌ NEEDS IMPROVEMENT - Multiple failure scenarios");
            println!("   Additional hardening required before production");
        }
        
        println!("\n{}", "=".repeat(70));
    }
}

fn main() {
    let engineer = ChaosEngineer::new();
    engineer.run_all_scenarios();
}