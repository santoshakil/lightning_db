use lightning_db::{Database, LightningDbConfig};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use std::fs;
use std::collections::HashMap;

/// Kill -9 recovery test suite
/// Tests database recovery after process termination at every critical point
#[derive(Debug, Clone)]
struct KillPoint {
    name: &'static str,
    operation: &'static str,
    timing: &'static str,
}

impl KillPoint {
    fn all() -> Vec<Self> {
        let operations = vec![
            "put", "get", "delete", "begin_transaction", "commit_transaction",
            "checkpoint", "compaction", "cache_eviction", "wal_write", "page_write",
            "btree_split", "btree_merge", "lsm_flush", "wal_rotation"
        ];
        
        let timings = vec!["before", "during", "after"];
        
        let mut points = Vec::new();
        for op in &operations {
            for timing in &timings {
                points.push(KillPoint {
                    name: "kill_test",
                    operation: op,
                    timing,
                });
            }
        }
        points
    }
}

/// Result of a kill test
#[derive(Debug)]
struct KillTestResult {
    kill_point: KillPoint,
    data_before: HashMap<String, String>,
    data_after: HashMap<String, String>,
    recovery_time: Duration,
    success: bool,
    error: Option<String>,
}

/// Test harness for kill -9 recovery
struct KillRecoveryHarness {
    test_dir: TempDir,
    results: Arc<Mutex<Vec<KillTestResult>>>,
}

impl KillRecoveryHarness {
    fn new() -> Self {
        Self {
            test_dir: TempDir::new().unwrap(),
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Run kill test for a specific point
    fn test_kill_point(&self, kill_point: KillPoint) -> KillTestResult {
        println!("\n🔪 Testing kill -9 at: {} {} {}", 
                 kill_point.operation, kill_point.timing, kill_point.name);
        
        let db_path = self.test_dir.path().join(format!("db_{}", kill_point.name));
        let marker_file = self.test_dir.path().join(format!("marker_{}", kill_point.name));
        
        // Prepare test data
        let test_data = self.prepare_test_data(100);
        
        // Spawn child process to run database operations
        let child_result = self.spawn_test_process(
            &db_path,
            &marker_file,
            &kill_point,
            &test_data,
        );
        
        match child_result {
            Ok((pid, data_written)) => {
                // Kill process at specified point
                self.kill_process(pid, &kill_point, &marker_file);
                
                // Verify recovery
                let recovery_start = Instant::now();
                let (success, data_after, error) = self.verify_recovery(&db_path, &data_written, &kill_point);
                let recovery_time = recovery_start.elapsed();
                
                KillTestResult {
                    kill_point,
                    data_before: data_written,
                    data_after,
                    recovery_time,
                    success,
                    error,
                }
            }
            Err(e) => KillTestResult {
                kill_point,
                data_before: HashMap::new(),
                data_after: HashMap::new(),
                recovery_time: Duration::ZERO,
                success: false,
                error: Some(format!("Failed to spawn process: {}", e)),
            }
        }
    }

    /// Prepare test data
    fn prepare_test_data(&self, count: usize) -> HashMap<String, String> {
        let mut data = HashMap::new();
        for i in 0..count {
            data.insert(
                format!("key_{:06}", i),
                format!("value_{:06}_data_{}", i, "x".repeat(100)),
            );
        }
        data
    }

    /// Spawn a child process that performs database operations
    fn spawn_test_process(
        &self,
        db_path: &std::path::Path,
        marker_file: &std::path::Path,
        kill_point: &KillPoint,
        _test_data: &HashMap<String, String>,
    ) -> Result<(u32, HashMap<String, String>), Box<dyn std::error::Error>> {
        // Create a test program that will be killed
        let test_program = format!(
            r#"
use lightning_db::{{Database, LightningDbConfig}};
use std::collections::HashMap;
use std::fs;
use std::thread;
use std::time::Duration;

fn main() {{
    let db_path = "{}";
    let marker_file = "{}";
    let operation = "{}";
    let timing = "{}";
    
    // Configure for maximum durability
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = lightning_db::WalSyncMode::Sync;
    config.checkpoint_interval = Duration::from_secs(1);
    
    let db = Database::create(db_path, config).unwrap();
    
    // Track what we've written
    let mut written = HashMap::new();
    
    // Perform operations based on kill point
    match operation {{
        "put" => {{
            // Write data and signal at different points
            for i in 0..50 {{
                if timing == "before" && i == 25 {{
                    fs::write(marker_file, "ready").unwrap();
                    thread::sleep(Duration::from_millis(100));
                }}
                
                let key = format!("key_{{:06}}", i);
                let value = format!("value_{{:06}}_data", i);
                
                if timing == "during" && i == 25 {{
                    fs::write(marker_file, "ready").unwrap();
                    // Kill should happen during this operation
                }}
                
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                written.insert(key, value);
                
                if timing == "after" && i == 25 {{
                    fs::write(marker_file, "ready").unwrap();
                    thread::sleep(Duration::from_millis(100));
                }}
            }}
        }}
        "commit_transaction" => {{
            let tx_id = db.begin_transaction().unwrap();
            
            // Add data to transaction
            for i in 0..30 {{
                let key = format!("tx_key_{{:06}}", i);
                let value = format!("tx_value_{{:06}}", i);
                db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
                written.insert(key, value);
            }}
            
            if timing == "before" {{
                fs::write(marker_file, "ready").unwrap();
                thread::sleep(Duration::from_millis(100));
            }}
            
            if timing == "during" {{
                fs::write(marker_file, "ready").unwrap();
                // Kill during commit
            }}
            
            db.commit_transaction(tx_id).unwrap();
            
            if timing == "after" {{
                fs::write(marker_file, "ready").unwrap();
                thread::sleep(Duration::from_millis(100));
            }}
        }}
        "checkpoint" => {{
            // Write data first
            for i in 0..40 {{
                let key = format!("key_{{:06}}", i);
                let value = format!("value_{{:06}}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                written.insert(key, value);
            }}
            
            if timing == "before" {{
                fs::write(marker_file, "ready").unwrap();
                thread::sleep(Duration::from_millis(100));
            }}
            
            if timing == "during" {{
                fs::write(marker_file, "ready").unwrap();
            }}
            
            db.checkpoint().unwrap();
            
            if timing == "after" {{
                fs::write(marker_file, "ready").unwrap();
                thread::sleep(Duration::from_millis(100));
            }}
        }}
        _ => {{
            // Generic operation pattern
            for i in 0..50 {{
                let key = format!("key_{{:06}}", i);
                let value = format!("value_{{:06}}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                written.insert(key, value);
            }}
            fs::write(marker_file, "ready").unwrap();
        }}
    }}
    
    // Save what we wrote for verification
    let written_json = serde_json::to_string(&written).unwrap();
    fs::write(format!("{{}}.written", db_path), written_json).unwrap();
    
    // Keep process alive for kill test
    thread::sleep(Duration::from_secs(10));
}}
"#,
            db_path.to_str().unwrap(),
            marker_file.to_str().unwrap(),
            kill_point.operation,
            kill_point.timing,
        );

        // Write and compile test program
        let test_file = self.test_dir.path().join(format!("{}_test.rs", kill_point.name));
        fs::write(&test_file, test_program)?;
        
        // Compile the test program
        let output = Command::new("rustc")
            .args(&[
                "--edition", "2021",
                "-L", "target/debug/deps",
                "--extern", "lightning_db=target/debug/liblightning_db.rlib",
                "--extern", "serde_json",
                "-o", &format!("{}/{}_test", self.test_dir.path().to_str().unwrap(), kill_point.name),
                test_file.to_str().unwrap(),
            ])
            .output()?;
        
        if !output.status.success() {
            return Err(format!("Compilation failed: {}", String::from_utf8_lossy(&output.stderr)).into());
        }
        
        // Spawn the test process
        let child = Command::new(format!("{}/{}_test", self.test_dir.path().to_str().unwrap(), kill_point.name))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        
        let pid = child.id();
        
        // Wait for marker file
        let start = Instant::now();
        while !marker_file.exists() && start.elapsed() < Duration::from_secs(5) {
            thread::sleep(Duration::from_millis(10));
        }
        
        if !marker_file.exists() {
            return Err("Marker file not created - process failed to reach kill point".into());
        }
        
        // Read what was written
        let written_file = format!("{}.written", db_path.to_str().unwrap());
        let written_data = if std::path::Path::new(&written_file).exists() {
            let json = fs::read_to_string(&written_file)?;
            serde_json::from_str(&json)?
        } else {
            HashMap::new()
        };
        
        Ok((pid, written_data))
    }

    /// Kill the process with SIGKILL
    fn kill_process(&self, pid: u32, kill_point: &KillPoint, marker_file: &std::path::Path) {
        // Add slight delay for "during" operations
        if kill_point.timing == "during" {
            thread::sleep(Duration::from_millis(10));
        }
        
        // Kill -9 the process
        let _ = Command::new("kill")
            .args(&["-9", &pid.to_string()])
            .output();
        
        // Clean up marker file
        let _ = fs::remove_file(marker_file);
        
        // Wait a bit to ensure process is dead
        thread::sleep(Duration::from_millis(100));
    }

    /// Verify database recovery and data integrity
    fn verify_recovery(
        &self,
        db_path: &std::path::Path,
        expected_data: &HashMap<String, String>,
        kill_point: &KillPoint,
    ) -> (bool, HashMap<String, String>, Option<String>) {
        // Try to open the database
        let config = LightningDbConfig::default();
        
        match Database::open(db_path, config) {
            Ok(db) => {
                let mut found_data = HashMap::new();
                let mut missing_keys = Vec::new();
                let mut corrupted_values = Vec::new();
                
                // Verify all expected data
                for (key, expected_value) in expected_data {
                    match db.get(key.as_bytes()) {
                        Ok(Some(value)) => {
                            let value_str = String::from_utf8_lossy(&value).to_string();
                            if value_str == *expected_value {
                                found_data.insert(key.clone(), value_str);
                            } else {
                                corrupted_values.push((key.clone(), expected_value.clone(), value_str));
                            }
                        }
                        Ok(None) => {
                            missing_keys.push(key.clone());
                        }
                        Err(e) => {
                            return (false, found_data, Some(format!("Error reading key {}: {}", key, e)));
                        }
                    }
                }
                
                // Check for data integrity
                if !corrupted_values.is_empty() {
                    return (false, found_data, Some(format!("Corrupted values: {:?}", corrupted_values)));
                }
                
                // Some missing keys might be acceptable depending on kill timing
                let acceptable_loss = match kill_point.timing {
                    "before" => 0,  // No data loss before operation
                    "during" => 1,  // At most one operation lost
                    "after" => 0,   // No data loss after operation
                    _ => 0,
                };
                
                if missing_keys.len() > acceptable_loss {
                    return (false, found_data, Some(format!("Missing keys: {:?}", missing_keys)));
                }
                
                // Verify database is functional
                match db.put(b"recovery_test_key", b"recovery_test_value") {
                    Ok(_) => {
                        match db.get(b"recovery_test_key") {
                            Ok(Some(v)) if v == b"recovery_test_value" => {
                                (true, found_data, None)
                            }
                            _ => (false, found_data, Some("Post-recovery operations failed".to_string()))
                        }
                    }
                    Err(e) => (false, found_data, Some(format!("Post-recovery write failed: {}", e)))
                }
            }
            Err(e) => {
                (false, HashMap::new(), Some(format!("Failed to open database after kill: {}", e)))
            }
        }
    }

    /// Run all kill tests
    fn run_all_tests(&self) {
        let kill_points = KillPoint::all();
        let total = kill_points.len();
        
        println!("🧪 Starting Kill -9 Recovery Test Suite");
        println!("📊 Total test points: {}", total);
        println!("⏱️  Estimated time: {} minutes", total * 2 / 60);
        println!("");
        
        let mut passed = 0;
        let mut failed = 0;
        
        for (i, kill_point) in kill_points.into_iter().enumerate() {
            println!("\n[{}/{}] Testing: {} {} {}", 
                     i + 1, total, kill_point.operation, kill_point.timing, kill_point.name);
            
            let result = self.test_kill_point(kill_point.clone());
            
            if result.success {
                println!("✅ PASSED - Recovery time: {:?}", result.recovery_time);
                passed += 1;
            } else {
                println!("❌ FAILED - Error: {:?}", result.error);
                failed += 1;
            }
            
            self.results.lock().unwrap().push(result);
        }
        
        // Generate report
        self.generate_report(passed, failed);
    }

    /// Generate comprehensive test report
    fn generate_report(&self, passed: usize, failed: usize) {
        println!("\n{}", "=".repeat(80));
        println!("📊 KILL -9 RECOVERY TEST REPORT");
        println!("{}", "=".repeat(80));
        
        let results = self.results.lock().unwrap();
        
        // Summary
        println!("\n📈 SUMMARY:");
        println!("  Total tests: {}", passed + failed);
        println!("  ✅ Passed: {} ({:.1}%)", passed, passed as f64 / (passed + failed) as f64 * 100.0);
        println!("  ❌ Failed: {} ({:.1}%)", failed, failed as f64 / (passed + failed) as f64 * 100.0);
        
        // Recovery time statistics
        let recovery_times: Vec<_> = results.iter()
            .filter(|r| r.success)
            .map(|r| r.recovery_time)
            .collect();
        
        if !recovery_times.is_empty() {
            let avg_recovery = recovery_times.iter().sum::<Duration>() / recovery_times.len() as u32;
            let max_recovery = recovery_times.iter().max().unwrap();
            let min_recovery = recovery_times.iter().min().unwrap();
            
            println!("\n⏱️  RECOVERY TIME STATISTICS:");
            println!("  Average: {:?}", avg_recovery);
            println!("  Maximum: {:?}", max_recovery);
            println!("  Minimum: {:?}", min_recovery);
        }
        
        // Failed test details
        if failed > 0 {
            println!("\n❌ FAILED TESTS:");
            for result in results.iter().filter(|r| !r.success) {
                println!("  - {} {} {}: {}", 
                         result.kill_point.operation,
                         result.kill_point.timing,
                         result.kill_point.name,
                         result.error.as_ref().unwrap_or(&"Unknown error".to_string()));
            }
        }
        
        // Critical findings
        println!("\n🔍 CRITICAL FINDINGS:");
        
        // Check for data loss
        let data_loss_tests = results.iter()
            .filter(|r| !r.success && r.error.as_ref().map_or(false, |e| e.contains("Missing keys")))
            .count();
        
        if data_loss_tests > 0 {
            println!("  ⚠️  DATA LOSS detected in {} tests!", data_loss_tests);
        } else {
            println!("  ✅ No data loss detected");
        }
        
        // Check for corruption
        let corruption_tests = results.iter()
            .filter(|r| !r.success && r.error.as_ref().map_or(false, |e| e.contains("Corrupted")))
            .count();
        
        if corruption_tests > 0 {
            println!("  ⚠️  CORRUPTION detected in {} tests!", corruption_tests);
        } else {
            println!("  ✅ No corruption detected");
        }
        
        // Overall verdict
        println!("\n🏁 VERDICT:");
        if failed == 0 {
            println!("  ✅ ALL TESTS PASSED - Database is resilient to kill -9!");
        } else if (failed as f64 / (passed + failed) as f64) < 0.05 {
            println!("  ⚠️  MOSTLY PASSED - {} minor issues need attention", failed);
        } else {
            println!("  ❌ CRITICAL ISSUES - {} tests failed, immediate attention required!", failed);
        }
        
        println!("\n{}", "=".repeat(80));
    }
}

#[test]
#[ignore] // This test is expensive and should be run explicitly
fn test_kill_recovery_comprehensive() {
    let harness = KillRecoveryHarness::new();
    harness.run_all_tests();
}

#[test]
fn test_kill_recovery_critical_points() {
    // Test only the most critical kill points
    let harness = KillRecoveryHarness::new();
    
    let critical_points = vec![
        KillPoint { name: "kill_commit_during", operation: "commit_transaction", timing: "during" },
        KillPoint { name: "kill_checkpoint_during", operation: "checkpoint", timing: "during" },
        KillPoint { name: "kill_put_during", operation: "put", timing: "during" },
    ];
    
    for point in critical_points {
        let result = harness.test_kill_point(point);
        assert!(result.success, "Critical kill point failed: {:?}", result.error);
    }
}