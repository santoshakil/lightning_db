/// Comprehensive Production Readiness Test Suite
///
/// This test simulates real production workloads and stress conditions
/// to validate if Lightning DB is truly production ready.
use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[derive(Debug)]
struct TestResults {
    test_name: String,
    passed: bool,
    duration: Duration,
    details: String,
    errors: Vec<String>,
}

impl TestResults {
    fn new(name: &str) -> Self {
        Self {
            test_name: name.to_string(),
            passed: false,
            duration: Duration::ZERO,
            details: String::new(),
            errors: Vec::new(),
        }
    }

    fn success(mut self, duration: Duration, details: &str) -> Self {
        self.passed = true;
        self.duration = duration;
        self.details = details.to_string();
        self
    }

    fn failure(mut self, duration: Duration, error: &str) -> Self {
        self.passed = false;
        self.duration = duration;
        self.errors.push(error.to_string());
        self
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 COMPREHENSIVE PRODUCTION READINESS TEST");
    println!("==========================================");
    println!("Testing Lightning DB under real production conditions...\n");

    let mut all_results = Vec::new();

    // Test 1: Basic Functionality Under Load
    println!("1️⃣ Basic Functionality Stress Test");
    all_results.push(test_basic_functionality_stress()?);

    // Test 2: Concurrent Operations
    println!("\n2️⃣ Concurrent Operations Test");
    all_results.push(test_concurrent_operations()?);

    // Test 3: Large Dataset Handling
    println!("\n3️⃣ Large Dataset Test (100MB+)");
    all_results.push(test_large_dataset()?);

    // Test 4: Memory Pressure Test
    println!("\n4️⃣ Memory Pressure Test");
    all_results.push(test_memory_pressure()?);

    // Test 5: Crash Recovery Test
    println!("\n5️⃣ Crash Recovery Test");
    all_results.push(test_crash_recovery()?);

    // Test 6: Data Integrity Under Stress
    println!("\n6️⃣ Data Integrity Under Stress");
    all_results.push(test_data_integrity_stress()?);

    // Test 7: Performance Sustainability
    println!("\n7️⃣ Performance Sustainability Test (10 min)");
    all_results.push(test_performance_sustainability()?);

    // Test 8: Transaction Consistency
    println!("\n8️⃣ Transaction Consistency Test");
    all_results.push(test_transaction_consistency()?);

    // Test 9: Edge Cases and Error Handling
    println!("\n9️⃣ Edge Cases and Error Handling");
    all_results.push(test_edge_cases()?);

    // Test 10: Resource Cleanup
    println!("\n🔟 Resource Cleanup Test");
    all_results.push(test_resource_cleanup()?);

    // Final Analysis
    println!("\n{}", "=".repeat(50));
    println!("📊 PRODUCTION READINESS ANALYSIS");
    println!("{}", "=".repeat(50));

    let total_tests = all_results.len();
    let passed_tests = all_results.iter().filter(|r| r.passed).count();
    let failed_tests = total_tests - passed_tests;

    println!("\n📈 Test Summary:");
    println!("  Total Tests: {}", total_tests);
    println!("  Passed: {} ✅", passed_tests);
    println!("  Failed: {} ❌", failed_tests);
    println!(
        "  Success Rate: {:.1}%",
        (passed_tests as f64 / total_tests as f64) * 100.0
    );

    println!("\n📋 Detailed Results:");
    for result in &all_results {
        let status = if result.passed {
            "✅ PASS"
        } else {
            "❌ FAIL"
        };
        println!(
            "  {} | {} ({:.2}s)",
            status,
            result.test_name,
            result.duration.as_secs_f64()
        );
        if !result.details.is_empty() {
            println!("      {}", result.details);
        }
        for error in &result.errors {
            println!("      ❌ ERROR: {}", error);
        }
    }

    // Final Verdict
    println!("\n{}", "=".repeat(50));
    if failed_tests == 0 {
        println!("🎉 VERDICT: PRODUCTION READY ✅");
        println!("Lightning DB passed all production readiness tests!");
    } else if failed_tests <= 2 {
        println!("⚠️  VERDICT: MOSTLY READY ⚠️");
        println!("Lightning DB has minor issues that should be addressed.");
    } else {
        println!("🚨 VERDICT: NOT PRODUCTION READY ❌");
        println!("Lightning DB has significant issues that must be fixed.");
    }
    println!("{}", "=".repeat(50));

    Ok(())
}

fn test_basic_functionality_stress() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Basic Functionality Under Load");

    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig {
        cache_size: 50 * 1024 * 1024,
        ..Default::default()
    }; // 50MB cache
    config.compression_enabled = true;

    let db = Database::create(temp_dir.path(), config)?;

    let operations = 100_000;
    let mut errors = 0;

    // Mixed workload: 70% reads, 20% writes, 10% deletes
    for i in 0..operations {
        let key = format!("stress_key_{:08}", i);
        let value = format!("stress_value_{}_{}", i, "x".repeat(100));

        match i % 10 {
            0..=1 => {
                // 20% writes
                if let Err(e) = db.put(key.as_bytes(), value.as_bytes()) {
                    errors += 1;
                    if errors > 100 {
                        // Too many errors
                        return Ok(result
                            .failure(start.elapsed(), &format!("Too many write errors: {}", e)));
                    }
                }
            }
            2..=8 => {
                // 70% reads
                if i > 1000 {
                    // Only read after some data exists
                    let read_key = format!("stress_key_{:08}", i % 1000);
                    if let Err(e) = db.get(read_key.as_bytes()) {
                        errors += 1;
                        if errors > 100 {
                            return Ok(result.failure(
                                start.elapsed(),
                                &format!("Too many read errors: {}", e),
                            ));
                        }
                    }
                }
            }
            9 => {
                // 10% deletes
                if i > 1000 {
                    let delete_key = format!("stress_key_{:08}", i % 1000);
                    if let Err(e) = db.delete(delete_key.as_bytes()) {
                        errors += 1;
                        if errors > 100 {
                            return Ok(result.failure(
                                start.elapsed(),
                                &format!("Too many delete errors: {}", e),
                            ));
                        }
                    }
                }
            }
            _ => unreachable!(),
        }

        if i % 10000 == 0 {
            print!(
                "  Progress: {}/{} ({:.1}%) - Errors: {}\r",
                i,
                operations,
                (i as f64 / operations as f64) * 100.0,
                errors
            );
        }
    }

    let duration = start.elapsed();
    let ops_per_sec = operations as f64 / duration.as_secs_f64();

    let details = format!(
        "{} ops in {:.2}s ({:.0} ops/sec), {} errors",
        operations,
        duration.as_secs_f64(),
        ops_per_sec,
        errors
    );

    if errors < 10 && ops_per_sec > 10000.0 {
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(
            duration,
            &format!("High error rate or low performance: {}", details),
        ))
    }
}

fn test_concurrent_operations() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Concurrent Operations");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(temp_dir.path(), config)?);

    let num_threads = 8;
    let ops_per_thread = 10_000;
    let barrier = Arc::new(Barrier::new(num_threads));
    let error_count = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        let error_count_clone = error_count.clone();

        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            barrier_clone.wait(); // Synchronize start

            for i in 0..ops_per_thread {
                let key = format!("concurrent_{}_{}", thread_id, i);
                let value = format!("value_{}_{}", thread_id, i);

                // Mix of operations
                match rng.random_range(0..10) {
                    0..=5 => {
                        // 60% writes
                        if db_clone.put(key.as_bytes(), value.as_bytes()).is_err() {
                            error_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    6..=8 => {
                        // 30% reads
                        if db_clone.get(key.as_bytes()).is_err() {
                            error_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    9 => {
                        // 10% deletes
                        if db_clone.delete(key.as_bytes()).is_err() {
                            error_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => unreachable!(),
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total_ops = num_threads * ops_per_thread;
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    let errors = error_count.load(Ordering::Relaxed);

    let details = format!(
        "{} threads, {} ops total, {:.0} ops/sec, {} errors",
        num_threads, total_ops, ops_per_sec, errors
    );

    if errors < 100 && ops_per_sec > 5000.0 {
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(duration, &format!("Concurrency issues: {}", details)))
    }
}

fn test_large_dataset() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Large Dataset (100MB+)");

    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig {
        cache_size: 20 * 1024 * 1024,
        ..Default::default()
    }; // 20MB cache (smaller than dataset)
    config.compression_enabled = true;

    let db = Database::create(temp_dir.path(), config)?;

    // Create 100MB+ of data
    let num_records = 100_000;
    let value_size = 1024; // 1KB values
    let large_value = "x".repeat(value_size);

    println!(
        "  Writing {} records ({} MB)...",
        num_records,
        (num_records * value_size) / (1024 * 1024)
    );

    // Write phase
    let write_start = Instant::now();
    for i in 0..num_records {
        let key = format!("large_dataset_key_{:08}", i);
        if let Err(e) = db.put(key.as_bytes(), large_value.as_bytes()) {
            return Ok(result.failure(start.elapsed(), &format!("Write failed at {}: {}", i, e)));
        }

        if i % 10000 == 0 {
            print!(
                "    Write progress: {}/{} ({:.1}%)\r",
                i,
                num_records,
                (i as f64 / num_records as f64) * 100.0
            );
        }
    }
    let write_duration = write_start.elapsed();

    // Read phase - random reads
    println!("  Reading random records...");
    let read_start = Instant::now();
    let mut rng = StdRng::seed_from_u64(42);
    let read_count = 10_000;
    let mut read_errors = 0;

    for _ in 0..read_count {
        let random_key = format!("large_dataset_key_{:08}", rng.random_range(0..num_records));
        match db.get(random_key.as_bytes()) {
            Ok(Some(value)) => {
                if value.len() != value_size {
                    read_errors += 1;
                }
            }
            Ok(None) => read_errors += 1,
            Err(_) => read_errors += 1,
        }
    }
    let read_duration = read_start.elapsed();

    let duration = start.elapsed();
    let write_ops_per_sec = num_records as f64 / write_duration.as_secs_f64();
    let read_ops_per_sec = read_count as f64 / read_duration.as_secs_f64();

    let details = format!(
        "{}MB written ({:.0} ops/sec), {} random reads ({:.0} ops/sec), {} read errors",
        (num_records * value_size) / (1024 * 1024),
        write_ops_per_sec,
        read_count,
        read_ops_per_sec,
        read_errors
    );

    if read_errors < read_count / 100 && write_ops_per_sec > 1000.0 {
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(duration, &format!("Large dataset issues: {}", details)))
    }
}

fn test_memory_pressure() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Memory Pressure");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024,
        ..Default::default()
    }; // Small cache

    let db = Database::create(temp_dir.path(), config)?;

    // Create memory pressure with large values and many keys
    let num_batches = 50;
    let keys_per_batch = 1000;
    let value_size = 8192; // 8KB values

    for batch in 0..num_batches {
        let large_value = format!("batch_{}_", batch) + &"x".repeat(value_size - 20);

        for i in 0..keys_per_batch {
            let key = format!("memory_pressure_{}_{}", batch, i);
            if let Err(e) = db.put(key.as_bytes(), large_value.as_bytes()) {
                return Ok(result.failure(
                    start.elapsed(),
                    &format!("Memory pressure write failed: {}", e),
                ));
            }
        }

        // Force some cleanup
        if batch % 10 == 0 {
            // Read some old data to test cache eviction
            for i in 0..100 {
                let old_key = format!("memory_pressure_0_{}", i);
                let _ = db.get(old_key.as_bytes());
            }
        }

        print!(
            "  Batch {}/{} ({:.1}%)\r",
            batch + 1,
            num_batches,
            ((batch + 1) as f64 / num_batches as f64) * 100.0
        );
    }

    let duration = start.elapsed();
    let total_data = num_batches * keys_per_batch * value_size;
    let total_keys = num_batches * keys_per_batch;

    let details = format!(
        "{} keys, {}MB total data, {:.2}s",
        total_keys,
        total_data / (1024 * 1024),
        duration.as_secs_f64()
    );

    Ok(result.success(duration, &details))
}

fn test_crash_recovery() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Crash Recovery");

    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig {
        use_improved_wal: true,
        ..Default::default()
    };
    config.wal_sync_mode = WalSyncMode::Sync; // Ensure durability

    // Phase 1: Write data
    {
        let db = Database::create(temp_dir.path(), config.clone())?;

        for i in 0..1000 {
            let key = format!("recovery_test_{}", i);
            let value = format!("recovery_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }

        // Start a transaction but don't commit (simulates crash)
        let tx_id = db.begin_transaction()?;
        for i in 1000..1100 {
            let key = format!("recovery_test_{}", i);
            let value = format!("uncommitted_value_{}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
        }
        // Database drops here without committing transaction
    }

    // Phase 2: Recover and verify
    let recovery_start = Instant::now();
    let db = Database::open(temp_dir.path(), config)?;
    let recovery_time = recovery_start.elapsed();

    // Verify committed data is there
    let mut missing_committed = 0;
    for i in 0..1000 {
        let key = format!("recovery_test_{}", i);
        match db.get(key.as_bytes())? {
            Some(value) => {
                let expected = format!("recovery_value_{}", i);
                if value != expected.as_bytes() {
                    missing_committed += 1;
                }
            }
            None => missing_committed += 1,
        }
    }

    // Verify uncommitted data is NOT there
    let mut found_uncommitted = 0;
    for i in 1000..1100 {
        let key = format!("recovery_test_{}", i);
        if db.get(key.as_bytes())?.is_some() {
            found_uncommitted += 1;
        }
    }

    let duration = start.elapsed();
    let details = format!(
        "Recovery: {:.2}ms, Missing committed: {}, Found uncommitted: {}",
        recovery_time.as_millis(),
        missing_committed,
        found_uncommitted
    );

    if missing_committed == 0 && found_uncommitted == 0 {
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(duration, &format!("Recovery issues: {}", details)))
    }
}

fn test_data_integrity_stress() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Data Integrity Under Stress");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(temp_dir.path(), config)?);

    // Write known data pattern
    let num_keys = 10_000;
    let checksum_map = Arc::new(std::sync::Mutex::new(HashMap::new()));

    // Phase 1: Write data with checksums
    for i in 0..num_keys {
        let key = format!("integrity_{:06}", i);
        let value = format!("checksum_{}_{}", i, calculate_simple_checksum(&key));

        db.put(key.as_bytes(), value.as_bytes())?;
        checksum_map.lock().unwrap().insert(key, value);
    }

    // Phase 2: Stress the database while verifying data
    let stop_flag = Arc::new(AtomicBool::new(false));
    let integrity_errors = Arc::new(AtomicU64::new(0));

    // Verification thread
    let db_clone = db.clone();
    let checksum_clone = checksum_map.clone();
    let stop_clone = stop_flag.clone();
    let errors_clone = integrity_errors.clone();

    let verify_handle = thread::spawn(move || {
        let mut rng = StdRng::seed_from_u64(12345);
        while !stop_clone.load(Ordering::Relaxed) {
            let random_key = format!("integrity_{:06}", rng.random_range(0..num_keys));

            match db_clone.get(random_key.as_bytes()) {
                Ok(Some(value)) => {
                    let expected = checksum_clone.lock().unwrap().get(&random_key).cloned();
                    if let Some(expected_value) = expected {
                        if value != expected_value.as_bytes() {
                            errors_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Ok(None) => {
                    errors_clone.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    errors_clone.fetch_add(1, Ordering::Relaxed);
                }
            }

            thread::sleep(Duration::from_millis(1));
        }
    });

    // Stress operations for 30 seconds
    let stress_duration = Duration::from_secs(30);
    let stress_start = Instant::now();
    let mut operations = 0;

    while stress_start.elapsed() < stress_duration {
        // Random operations
        let i = operations % num_keys;
        let key = format!("stress_{}", i);
        let value = format!("stress_value_{}", i);

        match operations % 5 {
            0..=2 => {
                let _ = db.put(key.as_bytes(), value.as_bytes());
            }
            3 => {
                let _ = db.get(key.as_bytes());
            }
            4 => {
                let _ = db.delete(key.as_bytes());
            }
            _ => unreachable!(),
        }

        operations += 1;
    }

    stop_flag.store(true, Ordering::Relaxed);
    verify_handle.join().unwrap();

    let duration = start.elapsed();
    let errors = integrity_errors.load(Ordering::Relaxed);
    let stress_ops = operations;

    let details = format!(
        "{} integrity checks, {} stress ops, {} errors in {}s",
        num_keys,
        stress_ops,
        errors,
        stress_duration.as_secs()
    );

    if errors == 0 {
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(
            duration,
            &format!("Data integrity compromised: {}", details),
        ))
    }
}

fn test_performance_sustainability() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Performance Sustainability (10min)");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    println!("  Running 10-minute sustained performance test...");

    let test_duration = Duration::from_secs(600); // 10 minutes
    let sample_interval = Duration::from_secs(30); // Sample every 30s
    let mut performance_samples = Vec::new();

    let mut total_operations = 0;
    let mut next_sample = Instant::now() + sample_interval;

    while start.elapsed() < test_duration {
        let sample_start = Instant::now();
        let mut sample_ops = 0;

        // Run operations until next sample time
        while Instant::now() < next_sample && start.elapsed() < test_duration {
            let key = format!("perf_sustain_{}", total_operations);
            let value = format!("value_{}", total_operations);

            // 80% reads, 20% writes
            if total_operations % 5 == 0 {
                let _ = db.put(key.as_bytes(), value.as_bytes());
            } else if total_operations > 1000 {
                let read_key = format!("perf_sustain_{}", total_operations % 1000);
                let _ = db.get(read_key.as_bytes());
            }

            total_operations += 1;
            sample_ops += 1;
        }

        let sample_duration = sample_start.elapsed();
        let ops_per_sec = sample_ops as f64 / sample_duration.as_secs_f64();
        performance_samples.push(ops_per_sec);

        print!(
            "    Time: {:.1}min, Ops/sec: {:.0}, Total ops: {}\r",
            start.elapsed().as_secs_f64() / 60.0,
            ops_per_sec,
            total_operations
        );

        next_sample += sample_interval;
    }

    let duration = start.elapsed();

    // Analyze performance degradation
    let avg_performance: f64 =
        performance_samples.iter().sum::<f64>() / performance_samples.len() as f64;
    let first_half_avg: f64 = performance_samples[..performance_samples.len() / 2]
        .iter()
        .sum::<f64>()
        / (performance_samples.len() / 2) as f64;
    let second_half_avg: f64 = performance_samples[performance_samples.len() / 2..]
        .iter()
        .sum::<f64>()
        / (performance_samples.len() / 2) as f64;

    let degradation = ((first_half_avg - second_half_avg) / first_half_avg) * 100.0;

    let details = format!(
        "{} ops total, {:.0} avg ops/sec, {:.1}% performance degradation",
        total_operations, avg_performance, degradation
    );

    if degradation < 20.0 && avg_performance > 5000.0 {
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(
            duration,
            &format!("Performance not sustainable: {}", details),
        ))
    }
}

fn test_transaction_consistency() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Transaction Consistency");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(temp_dir.path(), config)?);

    // Test concurrent transactions
    let num_accounts = 100;
    let initial_balance = 1000;
    let num_transfers = 1000;

    // Initialize accounts
    for i in 0..num_accounts {
        let key = format!("account_{}", i);
        let balance = initial_balance.to_string();
        db.put(key.as_bytes(), balance.as_bytes())?;
    }

    let error_count = Arc::new(AtomicU64::new(0));
    let completed_transfers = Arc::new(AtomicU64::new(0));

    // Run concurrent transfers
    let num_threads = 4;
    let transfers_per_thread = num_transfers / num_threads;
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let error_count_clone = error_count.clone();
        let completed_clone = completed_transfers.clone();

        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);

            for _ in 0..transfers_per_thread {
                let from_account = rng.random_range(0..num_accounts);
                let mut to_account = rng.random_range(0..num_accounts);
                while to_account == from_account {
                    to_account = rng.random_range(0..num_accounts);
                }

                let transfer_amount = rng.random_range(1..100);

                // Perform transfer transaction
                match transfer_money(&db_clone, from_account, to_account, transfer_amount) {
                    Ok(_) => {
                        completed_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify total balance is conserved
    let mut total_balance = 0;
    for i in 0..num_accounts {
        let key = format!("account_{}", i);
        if let Ok(Some(balance_bytes)) = db.get(key.as_bytes()) {
            if let Ok(balance_str) = std::str::from_utf8(&balance_bytes) {
                if let Ok(balance) = balance_str.parse::<i32>() {
                    total_balance += balance;
                }
            }
        }
    }

    let expected_total = (num_accounts * initial_balance) as i32;
    let balance_error = (total_balance - expected_total).abs();
    let errors = error_count.load(Ordering::Relaxed);
    let completed = completed_transfers.load(Ordering::Relaxed);

    let duration = start.elapsed();
    let details = format!(
        "{} transfers, {} errors, balance error: {}",
        completed, errors, balance_error
    );

    if balance_error == 0 && errors < completed / 10 {
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(
            duration,
            &format!("Transaction consistency issues: {}", details),
        ))
    }
}

fn test_edge_cases() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Edge Cases and Error Handling");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    let mut edge_case_errors = 0;

    // Test 1: Empty keys
    if db.put(b"", b"empty_key_value").is_err() {
        edge_case_errors += 1;
    }

    // Test 2: Very large keys
    let large_key = vec![b'x'; 10000];
    if db.put(&large_key, b"large_key_value").is_err() {
        edge_case_errors += 1;
    }

    // Test 3: Very large values
    let large_value = vec![b'y'; 1024 * 1024]; // 1MB
    if db.put(b"large_value_key", &large_value).is_err() {
        edge_case_errors += 1;
    }

    // Test 4: Delete non-existent key
    let _ = db.delete(b"non_existent_key");

    // Test 5: Read non-existent key
    match db.get(b"non_existent_key") {
        Ok(None) => {} // Expected
        _ => edge_case_errors += 1,
    }

    // Test 6: Transaction edge cases
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"tx_key", b"tx_value")?;
    db.abort_transaction(tx_id)?;

    // Verify aborted transaction data is not visible
    match db.get(b"tx_key") {
        Ok(None) => {} // Expected
        _ => edge_case_errors += 1,
    }

    // Test 7: Rapid transaction cycles
    for _ in 0..100 {
        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, b"rapid_tx", b"value")?;
        db.commit_transaction(tx_id)?;
    }

    let duration = start.elapsed();
    let details = format!("{} edge case tests, {} failures", 7, edge_case_errors);

    if edge_case_errors <= 1 {
        // Allow 1 failure for unsupported edge cases
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(duration, &details))
    }
}

fn test_resource_cleanup() -> Result<TestResults, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = TestResults::new("Resource Cleanup");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();

    // Create and destroy multiple database instances
    for i in 0..10 {
        let db = Database::create(temp_dir.path(), config.clone())?;

        // Do some operations
        for j in 0..1000 {
            let key = format!("cleanup_test_{}_{}", i, j);
            let value = format!("value_{}_{}", i, j);
            db.put(key.as_bytes(), value.as_bytes())?;
        }

        // Create some transactions
        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, b"tx_key", b"tx_value")?;
        db.commit_transaction(tx_id)?;

        // Database drops here
    }

    // Final database to verify data persists
    let final_db = Database::open(temp_dir.path(), config)?;

    // Verify some data is still there
    let mut found_data = 0;
    for i in 0..10 {
        for j in 0..100 {
            let key = format!("cleanup_test_{}_{}", i, j);
            if final_db.get(key.as_bytes())?.is_some() {
                found_data += 1;
            }
        }
    }

    let duration = start.elapsed();
    let details = format!(
        "10 DB cycles, {} data items found after cleanup",
        found_data
    );

    if found_data > 500 {
        // Should find most of the data
        Ok(result.success(duration, &details))
    } else {
        Ok(result.failure(duration, &format!("Resource cleanup issues: {}", details)))
    }
}

// Helper functions
fn calculate_simple_checksum(data: &str) -> u32 {
    data.bytes().map(|b| b as u32).sum()
}

fn transfer_money(
    db: &Database,
    from: usize,
    to: usize,
    amount: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let tx_id = db.begin_transaction()?;

    let from_key = format!("account_{}", from);
    let to_key = format!("account_{}", to);

    // Get current balances
    let from_balance: i32 = match db.get_tx(tx_id, from_key.as_bytes())? {
        Some(balance_bytes) => std::str::from_utf8(&balance_bytes)?.parse()?,
        None => return Err("From account not found".into()),
    };

    let to_balance: i32 = match db.get_tx(tx_id, to_key.as_bytes())? {
        Some(balance_bytes) => std::str::from_utf8(&balance_bytes)?.parse()?,
        None => return Err("To account not found".into()),
    };

    // Check sufficient funds
    if from_balance < amount {
        db.abort_transaction(tx_id)?;
        return Err("Insufficient funds".into());
    }

    // Perform transfer
    let new_from_balance = from_balance - amount;
    let new_to_balance = to_balance + amount;

    db.put_tx(
        tx_id,
        from_key.as_bytes(),
        new_from_balance.to_string().as_bytes(),
    )?;
    db.put_tx(
        tx_id,
        to_key.as_bytes(),
        new_to_balance.to_string().as_bytes(),
    )?;

    db.commit_transaction(tx_id)?;
    Ok(())
}
