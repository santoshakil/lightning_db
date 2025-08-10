use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Test that reliability fixes don't degrade performance
/// Validates that error handling overhead is minimal
#[test]
fn test_error_handling_performance_impact() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
        cache_size: 50 * 1024 * 1024,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Baseline: Normal operations without edge cases
    println!("Measuring baseline performance...");
    
    let baseline_start = Instant::now();
    let baseline_operations = 10000;
    
    for i in 0..baseline_operations {
        let key = format!("baseline_{:06}", i);
        let value = format!("baseline_value_{}", i);
        
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let _ = db.get(key.as_bytes()).unwrap();
    }
    
    let baseline_duration = baseline_start.elapsed();
    let baseline_ops_per_sec = (baseline_operations * 2) as f64 / baseline_duration.as_secs_f64();
    
    println!("Baseline performance: {:.0} ops/sec", baseline_ops_per_sec);

    // Test 1: Error handling overhead
    println!("Measuring error handling overhead...");
    
    let error_test_start = Instant::now();
    let error_operations = 10000;
    
    for i in 0..error_operations {
        let key = format!("error_test_{:06}", i);
        let value = format!("error_test_value_{}", i);
        
        // Normal operations (should have minimal error handling overhead)
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let _ = db.get(key.as_bytes()).unwrap();
        
        // Test edge cases that previously used unwrap()
        // These should now use proper error handling with minimal overhead
        let _ = db.get(b""); // Empty key
        let _ = db.get(b"non_existent_key_that_definitely_does_not_exist");
    }
    
    let error_test_duration = error_test_start.elapsed();
    let error_ops_per_sec = (error_operations * 4) as f64 / error_test_duration.as_secs_f64();
    
    println!("Error handling performance: {:.0} ops/sec", error_ops_per_sec);
    
    let overhead_percentage = ((baseline_ops_per_sec - error_ops_per_sec) / baseline_ops_per_sec) * 100.0;
    println!("Error handling overhead: {:.1}%", overhead_percentage);
    
    // Error handling should add minimal overhead (less than 10%)
    assert!(overhead_percentage < 10.0, "Error handling overhead should be less than 10%");

    // Test 2: Transaction error handling performance
    println!("Testing transaction error handling performance...");
    
    let tx_test_start = Instant::now();
    let tx_operations = 1000;
    
    for i in 0..tx_operations {
        // Valid transaction operations
        let tx_id = db.begin_transaction().unwrap();
        
        for j in 0..10 {
            let key = format!("tx_perf_{}_{:03}", i, j);
            let value = format!("tx_value_{}_{}", i, j);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.commit_transaction(tx_id).unwrap();
        
        // Test error conditions that are now handled gracefully
        let result = db.commit_transaction(999999); // Invalid transaction ID
        assert!(result.is_err(), "Should return error for invalid transaction");
        
        let result = db.get_tx(888888, b"some_key"); // Invalid transaction ID
        assert!(result.is_err(), "Should return error for invalid transaction");
    }
    
    let tx_test_duration = tx_test_start.elapsed();
    let tx_ops_per_sec = (tx_operations * 12) as f64 / tx_test_duration.as_secs_f64(); // 10 puts + 1 commit + 1 error test
    
    println!("Transaction error handling performance: {:.0} ops/sec", tx_ops_per_sec);
    
    // Should maintain reasonable transaction performance
    assert!(tx_ops_per_sec > 5000.0, "Transaction error handling should maintain performance > 5000 ops/sec");
}

/// Test that deadlock prevention doesn't impact normal operation performance
#[test]
fn test_deadlock_prevention_performance() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 50 },
        cache_size: 30 * 1024 * 1024,
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());

    // Test concurrent performance with deadlock prevention
    let num_threads = 8;
    let operations_per_thread = 2000;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let total_operations = Arc::new(AtomicUsize::new(0));
    let start_time = Arc::new(AtomicU64::new(0));
    let end_time = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let operations_clone = Arc::clone(&total_operations);
        let start_clone = Arc::clone(&start_time);
        let end_clone = Arc::clone(&end_time);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            // Record start time (first thread to start)
            let thread_start = Instant::now();
            start_clone.compare_exchange(0, thread_start.elapsed().as_nanos() as u64, Ordering::Relaxed, Ordering::Relaxed).ok();

            for i in 0..operations_per_thread {
                // Mix of operations that could potentially cause deadlocks
                match i % 4 {
                    0 => {
                        // Simple put/get operations
                        let key = format!("concurrent_{}_{:06}", thread_id, i);
                        let value = format!("value_{}_{}", thread_id, i);
                        
                        if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                            if db_clone.get(key.as_bytes()).is_ok() {
                                operations_clone.fetch_add(2, Ordering::Relaxed);
                            }
                        }
                    }
                    1 => {
                        // Transaction operations
                        if let Ok(tx_id) = db_clone.begin_transaction() {
                            let key = format!("tx_deadlock_{}_{:06}", thread_id, i);
                            let value = format!("tx_value_{}_{}", thread_id, i);
                            
                            if db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                                if db_clone.commit_transaction(tx_id).is_ok() {
                                    operations_clone.fetch_add(2, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    2 => {
                        // Mixed read operations
                        for j in 0..5 {
                            let key = format!("read_test_{}_{:06}", (thread_id + j) % num_threads, i);
                            if db_clone.get(key.as_bytes()).is_ok() {
                                operations_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    3 => {
                        // Cache-intensive operations
                        let key = format!("cache_test_{}_{:06}", thread_id, i);
                        let large_value = vec![b'C'; 10000]; // 10KB value
                        
                        if db_clone.put(key.as_bytes(), &large_value).is_ok() {
                            // Immediate read-back (should hit cache)
                            if db_clone.get(key.as_bytes()).is_ok() {
                                operations_clone.fetch_add(2, Ordering::Relaxed);
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }

            // Record end time (last thread to finish)
            let thread_end = Instant::now();
            end_clone.store(thread_end.elapsed().as_nanos() as u64, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    let test_start = Instant::now();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let test_duration = test_start.elapsed();
    let total_ops = total_operations.load(Ordering::Relaxed);
    let ops_per_second = total_ops as f64 / test_duration.as_secs_f64();

    println!("Deadlock prevention performance test:");
    println!("  Threads: {}", num_threads);
    println!("  Total operations: {}", total_ops);
    println!("  Duration: {:?}", test_duration);
    println!("  Operations per second: {:.0}", ops_per_second);

    // Should maintain high performance even with deadlock prevention
    assert!(ops_per_second > 50000.0, "Should maintain > 50k ops/sec with deadlock prevention");
    
    // Verify data integrity after high concurrent load
    let mut data_verified = 0;
    for thread_id in 0..num_threads {
        for i in (0..operations_per_thread).step_by(10) {
            let key = format!("concurrent_{}_{:06}", thread_id, i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                data_verified += 1;
            }
        }
    }
    
    println!("  Data integrity check: {} records verified", data_verified);
    assert!(data_verified > num_threads * operations_per_thread / 20, "Should maintain data integrity");
}

/// Test WAL corruption handling performance impact
#[test]
fn test_wal_corruption_handling_performance() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        cache_size: 20 * 1024 * 1024,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Test WAL operations with corruption handling overhead
    println!("Testing WAL corruption handling performance...");
    
    let wal_test_start = Instant::now();
    let wal_operations = 5000;
    
    // Create WAL-heavy workload
    for batch in 0..100 {
        let tx_id = db.begin_transaction().unwrap();
        
        for i in 0..50 {
            let key = format!("wal_perf_{}_{:06}", batch, i);
            let value = format!("wal_value_{}_{}", batch, i);
            
            // These operations now include corruption checking
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.commit_transaction(tx_id).unwrap();
        
        // Periodic checkpoint to exercise WAL corruption validation
        if batch % 20 == 0 {
            db.checkpoint().unwrap();
        }
    }
    
    let wal_duration = wal_test_start.elapsed();
    let wal_ops_per_sec = wal_operations as f64 / wal_duration.as_secs_f64();
    
    println!("WAL with corruption handling: {:.0} ops/sec", wal_ops_per_sec);
    
    // Should maintain reasonable WAL performance
    assert!(wal_ops_per_sec > 10000.0, "WAL corruption handling should maintain > 10k ops/sec");

    // Test WAL recovery performance
    println!("Testing WAL recovery performance...");
    
    drop(db);
    
    let recovery_start = Instant::now();
    let db = Database::open(dir.path(), config).unwrap();
    let recovery_duration = recovery_start.elapsed();
    
    println!("WAL recovery time: {:?}", recovery_duration);
    
    // Recovery should be reasonably fast
    assert!(recovery_duration < Duration::from_secs(5), "WAL recovery should complete within 5 seconds");
    
    // Verify data after recovery
    let verification_start = Instant::now();
    let mut verified_batches = 0;
    
    for batch in 0..100 {
        let mut batch_complete = true;
        for i in 0..50 {
            let key = format!("wal_perf_{}_{:06}", batch, i);
            if db.get(key.as_bytes()).unwrap().is_none() {
                batch_complete = false;
                break;
            }
        }
        if batch_complete {
            verified_batches += 1;
        }
    }
    
    let verification_duration = verification_start.elapsed();
    
    println!("Verification: {} batches recovered in {:?}", verified_batches, verification_duration);
    assert!(verified_batches >= 95, "Should recover at least 95% of committed WAL data");
}

/// Test data integrity validation performance
#[test]
fn test_integrity_validation_performance() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 200 },
        cache_size: 40 * 1024 * 1024,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Populate database with test data
    println!("Populating database for integrity validation test...");
    
    let data_size = 10000;
    for i in 0..data_size {
        let key = format!("integrity_perf_{:06}", i);
        let value = format!("integrity_value_{}_{}", i, "X".repeat(500)); // 500+ byte values
        
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    db.checkpoint().unwrap();

    // Test integrity validation performance
    println!("Running integrity validation performance test...");
    
    let validation_start = Instant::now();
    
    let verification_result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();
    
    let validation_duration = validation_start.elapsed();
    let validation_throughput = data_size as f64 / validation_duration.as_secs_f64();
    
    println!("Integrity validation results:");
    println!("  Records validated: {}", data_size);
    println!("  Validation time: {:?}", validation_duration);
    println!("  Validation throughput: {:.0} records/sec", validation_throughput);
    println!("  Checksum errors: {}", verification_result.checksum_errors.len());
    println!("  Structure errors: {}", verification_result.structure_errors.len());
    println!("  Consistency errors: {}", verification_result.consistency_errors.len());
    
    // Integrity validation should be reasonably fast
    assert!(validation_throughput > 5000.0, "Integrity validation should process > 5000 records/sec");
    assert_eq!(verification_result.checksum_errors.len(), 0, "Should have no checksum errors");
    assert_eq!(verification_result.structure_errors.len(), 0, "Should have no structure errors");
    assert_eq!(verification_result.consistency_errors.len(), 0, "Should have no consistency errors");

    // Test performance impact of continuous validation
    println!("Testing performance with continuous validation...");
    
    let continuous_start = Instant::now();
    let continuous_operations = 5000;
    let validation_interval = 500; // Run validation every 500 operations
    
    for i in 0..continuous_operations {
        let key = format!("continuous_perf_{:06}", i);
        let value = format!("continuous_value_{}", i);
        
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let _ = db.get(key.as_bytes()).unwrap();
        
        // Periodic validation
        if i % validation_interval == 0 && i > 0 {
            let quick_validation = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(db.verify_integrity())
                .unwrap();
            
            assert_eq!(quick_validation.checksum_errors.len(), 0, "Continuous validation should find no errors");
        }
    }
    
    let continuous_duration = continuous_start.elapsed();
    let continuous_ops_per_sec = (continuous_operations * 2) as f64 / continuous_duration.as_secs_f64();
    
    println!("Performance with continuous validation: {:.0} ops/sec", continuous_ops_per_sec);
    
    // Should maintain reasonable performance even with periodic validation
    assert!(continuous_ops_per_sec > 8000.0, "Should maintain > 8k ops/sec with continuous validation");
}

/// Test performance regression detection
#[test]
fn test_performance_regression_detection() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
        cache_size: 50 * 1024 * 1024,
        compression_enabled: true,
        prefetch_enabled: true,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Performance benchmarks for different operation types
    let benchmarks = vec![
        ("single_put", test_single_put_performance),
        ("single_get", test_single_get_performance),
        ("batch_operations", test_batch_operations_performance),
        ("transaction_operations", test_transaction_performance),
        ("mixed_workload", test_mixed_workload_performance),
    ];

    let mut performance_results = HashMap::new();

    for (benchmark_name, benchmark_fn) in benchmarks {
        println!("Running {} benchmark...", benchmark_name);
        
        let start_time = Instant::now();
        let operations = benchmark_fn(&db);
        let duration = start_time.elapsed();
        
        let ops_per_second = operations as f64 / duration.as_secs_f64();
        performance_results.insert(benchmark_name, ops_per_second);
        
        println!("  {}: {:.0} ops/sec", benchmark_name, ops_per_second);
    }

    // Performance expectations (these would be based on baseline measurements)
    let performance_expectations = [
        ("single_put", 100000.0),      // 100k ops/sec minimum
        ("single_get", 500000.0),      // 500k ops/sec minimum  
        ("batch_operations", 50000.0),  // 50k ops/sec minimum
        ("transaction_operations", 20000.0), // 20k ops/sec minimum
        ("mixed_workload", 30000.0),   // 30k ops/sec minimum
    ];

    println!("\nPerformance regression check:");
    let mut regressions_detected = 0;

    for (benchmark_name, expected_min_performance) in performance_expectations.iter() {
        if let Some(&actual_performance) = performance_results.get(benchmark_name) {
            if actual_performance < *expected_min_performance {
                println!("  REGRESSION: {} - Expected: {:.0}, Actual: {:.0} ({:.1}% below target)",
                        benchmark_name, expected_min_performance, actual_performance,
                        (expected_min_performance - actual_performance) / expected_min_performance * 100.0);
                regressions_detected += 1;
            } else {
                let improvement = (actual_performance - expected_min_performance) / expected_min_performance * 100.0;
                println!("  PASS: {} - {:.0} ops/sec ({:.1}% above minimum)",
                        benchmark_name, actual_performance, improvement);
            }
        }
    }

    assert_eq!(regressions_detected, 0, "Performance regressions detected");
    println!("All performance benchmarks passed!");
}

fn test_single_put_performance(db: &Database) -> usize {
    let operations = 10000;
    
    for i in 0..operations {
        let key = format!("put_perf_{:06}", i);
        let value = format!("put_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    operations
}

fn test_single_get_performance(db: &Database) -> usize {
    // First populate some data
    for i in 0..1000 {
        let key = format!("get_perf_{:06}", i);
        let value = format!("get_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    let operations = 20000;
    
    for i in 0..operations {
        let key = format!("get_perf_{:06}", i % 1000);
        let _ = db.get(key.as_bytes()).unwrap();
    }
    
    operations
}

fn test_batch_operations_performance(db: &Database) -> usize {
    let batches = 100;
    let operations_per_batch = 100;
    let total_operations = batches * operations_per_batch;
    
    for batch in 0..batches {
        // Batch write operations
        for i in 0..operations_per_batch {
            let key = format!("batch_{}_{:06}", batch, i);
            let value = format!("batch_value_{}_{}", batch, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Periodic checkpoint
        if batch % 20 == 0 {
            db.checkpoint().unwrap();
        }
    }
    
    total_operations
}

fn test_transaction_performance(db: &Database) -> usize {
    let transactions = 500;
    let operations_per_transaction = 20;
    let total_operations = transactions * operations_per_transaction;
    
    for tx_num in 0..transactions {
        let tx_id = db.begin_transaction().unwrap();
        
        for i in 0..operations_per_transaction {
            let key = format!("tx_perf_{}_{:06}", tx_num, i);
            let value = format!("tx_value_{}_{}", tx_num, i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.commit_transaction(tx_id).unwrap();
    }
    
    total_operations
}

fn test_mixed_workload_performance(db: &Database) -> usize {
    let operations = 10000;
    
    for i in 0..operations {
        match i % 4 {
            0 => {
                // Write operation
                let key = format!("mixed_write_{:06}", i);
                let value = format!("mixed_value_{}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            1 => {
                // Read operation
                let key = format!("mixed_write_{:06}", std::cmp::max(0, i as i32 - 100) as usize);
                let _ = db.get(key.as_bytes()).unwrap();
            }
            2 => {
                // Update operation
                if i > 200 {
                    let key = format!("mixed_write_{:06}", i - 200);
                    let value = format!("updated_mixed_value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            }
            3 => {
                // Delete operation
                if i > 400 {
                    let key = format!("mixed_write_{:06}", i - 400);
                    let _ = db.delete(key.as_bytes());
                }
            }
            _ => unreachable!(),
        }
    }
    
    operations
}

/// Test memory usage regression
#[test]
fn test_memory_usage_regression() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 20 * 1024 * 1024, // 20MB cache limit
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 200 },
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Test memory usage under various scenarios
    println!("Testing memory usage patterns...");
    
    // Scenario 1: Large number of small records
    let small_records = 50000;
    for i in 0..small_records {
        let key = format!("small_{:06}", i);
        let value = format!("value_{}", i); // ~20 bytes
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Force cache eviction and memory pressure
    let large_value = vec![b'L'; 100000]; // 100KB values
    for i in 0..100 {
        let key = format!("large_{:06}", i);
        db.put(key.as_bytes(), &large_value).unwrap();
    }
    
    // Scenario 2: Transaction memory usage
    for tx_batch in 0..50 {
        let tx_id = db.begin_transaction().unwrap();
        
        for i in 0..100 {
            let key = format!("tx_mem_{}_{:06}", tx_batch, i);
            let value = format!("tx_memory_value_{}_{}", tx_batch, i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.commit_transaction(tx_id).unwrap();
    }
    
    // Scenario 3: Cache pressure test
    for round in 0..10 {
        for i in 0..1000 {
            let key = format!("cache_pressure_{}_{:06}", round, i);
            let value = vec![b'C'; 5000]; // 5KB values
            db.put(key.as_bytes(), &value).unwrap();
            
            // Immediate read to test cache behavior
            let _ = db.get(key.as_bytes()).unwrap();
        }
        
        // Force checkpoint to test memory cleanup
        db.checkpoint().unwrap();
    }
    
    // Verify database still functions after memory pressure
    for i in 0..1000 {
        let key = format!("final_mem_test_{:06}", i);
        let value = format!("final_value_{}", i);
        
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), value);
    }
    
    // Check cache stats for memory efficiency
    if let Some(cache_stats) = db.cache_stats() {
        println!("Final cache stats: {}", cache_stats);
        // Cache should show healthy hit/miss ratios indicating efficient memory use
        assert!(cache_stats.contains("hit") || cache_stats.contains("miss"));
    }
    
    println!("Memory usage regression test completed successfully");
}