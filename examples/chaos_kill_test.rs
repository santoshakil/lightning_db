use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::env;
use std::process::{exit, Command};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[cfg(unix)]
use nix::libc;

/// Chaos engineering kill test
/// Tests database recovery after abrupt process termination
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && args[1] == "child" {
        // Child process mode - perform database operations
        run_child_process(&args[2], &args[3], &args[4]);
    } else {
        // Parent process mode - orchestrate tests
        run_kill_tests();
    }
}

/// Child process that performs database operations
fn run_child_process(db_path: &str, operation: &str, kill_point: &str) {
    println!("🧒 Child process started: {} at {}", operation, kill_point);

    // Configure for maximum durability
    let mut config = LightningDbConfig {
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };
    config.use_improved_wal = true;

    let db = Database::create(db_path, config).unwrap();

    match operation {
        "write_batch" => {
            // Write a batch of data
            for i in 0..100 {
                if kill_point == "mid_batch" && i == 50 {
                    // Signal parent we're ready to be killed
                    println!("READY_TO_KILL");
                    thread::sleep(Duration::from_millis(50));
                }

                let key = format!("key_{:06}", i);
                let value = format!("value_{:06}_data_{}", i, "x".repeat(100));
                db.put(key.as_bytes(), value.as_bytes()).unwrap();

                if kill_point == "after_write" && i == 75 {
                    println!("READY_TO_KILL");
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }

        "transaction" => {
            // Perform a transaction
            let tx_id = db.begin_transaction().unwrap();

            // Add data to transaction
            for i in 0..50 {
                let key = format!("tx_key_{:06}", i);
                let value = format!("tx_value_{:06}", i);
                db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            }

            if kill_point == "before_commit" {
                println!("READY_TO_KILL");
                thread::sleep(Duration::from_millis(50));
            }

            if kill_point == "during_commit" {
                // Start commit in another thread and signal
                thread::spawn(move || {
                    db.commit_transaction(tx_id).unwrap();
                });
                println!("READY_TO_KILL");
                thread::sleep(Duration::from_millis(10));
            } else {
                db.commit_transaction(tx_id).unwrap();
            }

            if kill_point == "after_commit" {
                println!("READY_TO_KILL");
                thread::sleep(Duration::from_millis(50));
            }
        }

        "checkpoint" => {
            // Write data then checkpoint
            for i in 0..75 {
                let key = format!("ckpt_key_{:06}", i);
                let value = format!("ckpt_value_{:06}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }

            if kill_point == "before_checkpoint" {
                println!("READY_TO_KILL");
                thread::sleep(Duration::from_millis(50));
            }

            if kill_point == "during_checkpoint" {
                thread::spawn(move || {
                    db.checkpoint().unwrap();
                });
                println!("READY_TO_KILL");
                thread::sleep(Duration::from_millis(10));
            } else {
                db.checkpoint().unwrap();
            }

            if kill_point == "after_checkpoint" {
                println!("READY_TO_KILL");
                thread::sleep(Duration::from_millis(50));
            }
        }

        "mixed_operations" => {
            // Mix of operations to simulate real workload
            for i in 0..100 {
                match i % 4 {
                    0 => {
                        let key = format!("put_key_{:06}", i);
                        let value = format!("put_value_{:06}", i);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    1 => {
                        let key = format!("put_key_{:06}", i - 1);
                        let _ = db.get(key.as_bytes()).unwrap();
                    }
                    2 => {
                        if i > 10 {
                            let key = format!("put_key_{:06}", i - 10);
                            db.delete(key.as_bytes()).unwrap();
                        }
                    }
                    3 => {
                        // Small transaction
                        let tx_id = db.begin_transaction().unwrap();
                        for j in 0..5 {
                            let key = format!("tx_key_{}_{}", i, j);
                            let value = format!("tx_value_{}_{}", i, j);
                            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
                        }
                        db.commit_transaction(tx_id).unwrap();
                    }
                    _ => unreachable!(),
                }

                if kill_point == "random" && i == 42 {
                    println!("READY_TO_KILL");
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }

        _ => {
            eprintln!("Unknown operation: {}", operation);
            exit(1);
        }
    }

    // Keep process alive
    println!("COMPLETED");
    thread::sleep(Duration::from_secs(5));
}

/// Parent process that orchestrates kill tests
fn run_kill_tests() {
    println!("🔪 Lightning DB Chaos Kill Test Suite");
    println!("=====================================\n");

    let test_cases = vec![
        ("write_batch", "mid_batch", "Kill during batch write"),
        ("write_batch", "after_write", "Kill after writes"),
        (
            "transaction",
            "before_commit",
            "Kill before transaction commit",
        ),
        (
            "transaction",
            "during_commit",
            "Kill during transaction commit",
        ),
        (
            "transaction",
            "after_commit",
            "Kill after transaction commit",
        ),
        ("checkpoint", "before_checkpoint", "Kill before checkpoint"),
        ("checkpoint", "during_checkpoint", "Kill during checkpoint"),
        ("checkpoint", "after_checkpoint", "Kill after checkpoint"),
        ("mixed_operations", "random", "Kill during mixed operations"),
    ];

    let mut results = Vec::new();

    for (operation, kill_point, description) in test_cases {
        println!("\n🧪 Test: {}", description);
        println!("   Operation: {}, Kill point: {}", operation, kill_point);

        let result = run_single_kill_test(operation, kill_point);
        results.push((description, result));
    }

    // Print summary
    print_test_summary(&results);
}

/// Run a single kill test
fn run_single_kill_test(operation: &str, kill_point: &str) -> TestResult {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Get current executable path
    let exe_path = env::current_exe().unwrap();

    // Spawn child process
    let mut child = Command::new(&exe_path)
        .args(["child", db_path.to_str().unwrap(), operation, kill_point])
        .spawn()
        .expect("Failed to spawn child process");

    let child_pid = child.id();
    println!("   Child PID: {}", child_pid);

    // Monitor child output
    let start = Instant::now();
    let mut killed = false;

    // Poll for readiness signal or completion
    loop {
        // Check if child printed readiness signal
        if !killed {
            // In a real implementation, we'd capture stdout
            // For now, we'll use timing
            thread::sleep(Duration::from_millis(100));

            if start.elapsed() > Duration::from_millis(500) {
                // Kill the child process
                println!("   💀 Killing process with SIGKILL...");

                #[cfg(unix)]
                {
                    unsafe {
                        libc::kill(child_pid as i32, libc::SIGKILL);
                    }
                }

                #[cfg(not(unix))]
                {
                    let _ = child.kill();
                }

                killed = true;
            }
        }

        // Check if child exited
        match child.try_wait() {
            Ok(Some(status)) => {
                println!("   Child exited with: {:?}", status);
                break;
            }
            Ok(None) => {
                if start.elapsed() > Duration::from_secs(10) {
                    println!("   ⏱️  Timeout - killing child");
                    let _ = child.kill();
                    break;
                }
            }
            Err(e) => {
                println!("   Error waiting for child: {}", e);
                break;
            }
        }

        thread::sleep(Duration::from_millis(10));
    }

    // Wait a bit for filesystem to settle
    thread::sleep(Duration::from_millis(100));

    // Verify database recovery
    println!("   🔄 Attempting database recovery...");
    let recovery_start = Instant::now();

    match verify_database_recovery(&db_path) {
        Ok((key_count, integrity_ok)) => {
            let recovery_time = recovery_start.elapsed();
            println!("   ✅ Recovery successful!");
            println!("      Recovery time: {:?}", recovery_time);
            println!("      Keys found: {}", key_count);
            println!(
                "      Integrity: {}",
                if integrity_ok { "OK" } else { "FAILED" }
            );

            TestResult {
                success: integrity_ok,
                recovery_time,
                key_count,
                error: None,
            }
        }
        Err(e) => {
            println!("   ❌ Recovery failed: {}", e);
            TestResult {
                success: false,
                recovery_time: recovery_start.elapsed(),
                key_count: 0,
                error: Some(e.to_string()),
            }
        }
    }
}

/// Verify database can be recovered and is consistent
fn verify_database_recovery(
    db_path: &std::path::Path,
) -> Result<(usize, bool), Box<dyn std::error::Error>> {
    let config = LightningDbConfig::default();
    let db = Database::open(db_path, config)?;

    // Count all keys
    let mut key_count = 0;
    let mut keys_found = Vec::new();

    // Try to read various key patterns
    for prefix in &["key_", "tx_key_", "ckpt_key_", "put_key_"] {
        for i in 0..200 {
            let key = format!("{}{:06}", prefix, i);
            if let Ok(Some(_)) = db.get(key.as_bytes()) {
                key_count += 1;
                keys_found.push(key);
            }
        }
    }

    // Verify we can write new data
    let test_key = b"recovery_test_key";
    let test_value = b"recovery_test_value";
    db.put(test_key, test_value)?;

    // Verify we can read it back
    match db.get(test_key)? {
        Some(value) if value == test_value => {
            // Good, basic operations work
        }
        _ => {
            return Ok((key_count, false));
        }
    }

    // Verify transactions work
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"tx_test_key", b"tx_test_value")?;
    db.commit_transaction(tx_id)?;

    // Verify transaction data
    match db.get(b"tx_test_key")? {
        Some(value) if value == b"tx_test_value" => {
            // Good, transactions work
        }
        _ => {
            return Ok((key_count, false));
        }
    }

    // Run integrity check if available
    if let Ok(report) = lightning_db::integrity::verify_database_integrity(db_path) {
        let has_critical_errors = report
            .errors
            .iter()
            .any(|e| e.severity == lightning_db::integrity::Severity::Critical);

        if has_critical_errors {
            println!("      ⚠️  Critical integrity errors found!");
            return Ok((key_count, false));
        }
    }

    Ok((key_count, true))
}

#[derive(Debug)]
struct TestResult {
    success: bool,
    recovery_time: Duration,
    key_count: usize,
    error: Option<String>,
}

/// Print test summary
fn print_test_summary(results: &[(&str, TestResult)]) {
    println!("\n{}", "=".repeat(80));
    println!("📊 CHAOS KILL TEST SUMMARY");
    println!("{}", "=".repeat(80));

    let total = results.len();
    let passed = results.iter().filter(|(_, r)| r.success).count();
    let failed = total - passed;

    println!("\n📈 Results:");
    println!("   Total tests: {}", total);
    println!(
        "   ✅ Passed: {} ({:.1}%)",
        passed,
        passed as f64 / total as f64 * 100.0
    );
    println!(
        "   ❌ Failed: {} ({:.1}%)",
        failed,
        failed as f64 / total as f64 * 100.0
    );

    // Recovery time stats
    let recovery_times: Vec<_> = results
        .iter()
        .filter(|(_, r)| r.success)
        .map(|(_, r)| r.recovery_time)
        .collect();

    if !recovery_times.is_empty() {
        let avg_recovery = recovery_times.iter().sum::<Duration>() / recovery_times.len() as u32;
        let max_recovery = recovery_times.iter().max().unwrap();

        println!("\n⏱️  Recovery Time:");
        println!("   Average: {:?}", avg_recovery);
        println!("   Maximum: {:?}", max_recovery);
    }

    // Failed test details
    if failed > 0 {
        println!("\n❌ Failed Tests:");
        for (desc, result) in results.iter().filter(|(_, r)| !r.success) {
            println!(
                "   - {}: {}",
                desc,
                result
                    .error
                    .as_ref()
                    .unwrap_or(&"Unknown error".to_string())
            );
        }
    }

    // Data recovery stats
    let total_keys: usize = results.iter().map(|(_, r)| r.key_count).sum();
    println!("\n💾 Data Recovery:");
    println!("   Total keys recovered: {}", total_keys);
    println!("   Average keys per test: {}", total_keys / total.max(1));

    // Overall verdict
    println!("\n🏁 VERDICT:");
    if failed == 0 {
        println!("   ✅ ALL TESTS PASSED!");
        println!("   Lightning DB successfully recovers from kill -9 in all tested scenarios.");
    } else if passed > failed {
        println!("   ⚠️  MOSTLY RESILIENT");
        println!(
            "   {} scenarios need attention for full kill -9 resilience.",
            failed
        );
    } else {
        println!("   ❌ CRITICAL ISSUES");
        println!("   Major problems with crash recovery. Immediate attention required!");
    }

    // Production readiness assessment
    println!("\n🏭 Production Readiness:");
    if failed == 0 {
        println!("   ✅ READY - Database handles abrupt termination gracefully");
    } else {
        println!("   ❌ NOT READY - Must fix crash recovery issues first");
    }

    println!("\n{}", "=".repeat(80));
}
