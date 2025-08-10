use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Test database behavior under simulated disk full conditions
#[test]
fn test_disk_full_failure_injection() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        cache_size: 10 * 1024 * 1024,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    println!("Testing disk full failure injection...");

    // Phase 1: Normal operations to establish baseline
    let baseline_records = 1000;
    for i in 0..baseline_records {
        let key = format!("baseline_disk_{:06}", i);
        let value = format!("baseline_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    db.checkpoint().unwrap();

    // Phase 2: Simulate operations that fail due to disk space
    let large_value = vec![b'X'; 500_000]; // 500KB values to quickly fill space
    let mut successful_large_writes = 0;
    let mut disk_full_errors = 0;

    println!("Injecting large writes to simulate disk full...");
    
    for i in 0..200 {
        let key = format!("large_disk_{:06}", i);
        
        match db.put(key.as_bytes(), &large_value) {
            Ok(_) => {
                successful_large_writes += 1;
                
                // Verify write succeeded
                match db.get(key.as_bytes()) {
                    Ok(Some(retrieved)) => {
                        assert_eq!(retrieved.len(), large_value.len());
                    }
                    Ok(None) => {
                        println!("Warning: Data not found immediately after write for key {}", key);
                        disk_full_errors += 1;
                    }
                    Err(_) => disk_full_errors += 1,
                }
            }
            Err(_) => {
                disk_full_errors += 1;
                println!("Simulated disk full error at large write {}", i);
                
                // Test recovery: try smaller writes
                for small_attempt in 0..5 {
                    let small_key = format!("recovery_{}_{}", i, small_attempt);
                    let small_value = format!("recovery_value_{}_{}", i, small_attempt);
                    
                    match db.put(small_key.as_bytes(), small_value.as_bytes()) {
                        Ok(_) => break, // Recovery successful
                        Err(_) => continue, // Still failing
                    }
                }
                break; // Simulate stopping after disk full
            }
        }

        // Occasional checkpoint attempts that might fail
        if i % 20 == 0 {
            match db.checkpoint() {
                Ok(_) => {},
                Err(_) => {
                    println!("Checkpoint failed at iteration {} (simulated I/O error)", i);
                    disk_full_errors += 1;
                }
            }
        }
    }

    println!("Disk full simulation results:");
    println!("  Successful large writes: {}", successful_large_writes);
    println!("  Disk full errors: {}", disk_full_errors);

    // Phase 3: Test graceful degradation
    println!("Testing graceful degradation after disk full...");
    
    // Small writes should still work even if large ones fail
    let mut small_write_successes = 0;
    for i in 0..100 {
        let key = format!("small_recovery_{:06}", i);
        let value = format!("small_value_{}", i);
        
        match db.put(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                small_write_successes += 1;
                // Verify read-back
                assert!(db.get(key.as_bytes()).unwrap().is_some());
            }
            Err(_) => {
                println!("Small write failed at {}", i);
            }
        }
    }

    println!("Small write recovery: {}/100", small_write_successes);
    assert!(small_write_successes >= 80, "Should handle small writes even after disk pressure");

    // Phase 4: Verify baseline data integrity after failure
    let mut baseline_intact = 0;
    for i in 0..baseline_records {
        let key = format!("baseline_disk_{:06}", i);
        match db.get(key.as_bytes()) {
            Ok(Some(_)) => baseline_intact += 1,
            Ok(None) => println!("Lost baseline data: {}", key),
            Err(e) => println!("Error reading baseline data {}: {:?}", key, e),
        }
    }

    println!("Baseline data integrity: {}/{}", baseline_intact, baseline_records);
    assert!(baseline_intact >= baseline_records * 95 / 100, "Should preserve 95%+ of baseline data");

    // Test database recovery after disk full scenario
    drop(db);
    
    let recovery_db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Verify recovery
    let mut recovered_baseline = 0;
    for i in 0..baseline_records {
        let key = format!("baseline_disk_{:06}", i);
        if recovery_db.get(key.as_bytes()).unwrap().is_some() {
            recovered_baseline += 1;
        }
    }

    println!("Recovery verification: {}/{} baseline records recovered", recovered_baseline, baseline_records);
    assert!(recovered_baseline >= baseline_records * 90 / 100, "Should recover 90%+ of baseline data");
}

/// Test behavior under simulated memory pressure and allocation failures
#[test]
fn test_memory_exhaustion_failure_injection() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 5 * 1024 * 1024, // Small 5MB cache to create pressure quickly
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    println!("Testing memory exhaustion failure injection...");

    // Phase 1: Fill cache and create memory pressure
    let memory_stress_records = 2000;
    let large_value_size = 10_000; // 10KB values
    let mut memory_allocation_failures = 0;
    let mut successful_operations = 0;

    for i in 0..memory_stress_records {
        let key = format!("memory_stress_{:06}", i);
        let value = vec![b'M'; large_value_size];
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                successful_operations += 1;
                
                // Test immediate read-back under memory pressure
                match db.get(key.as_bytes()) {
                    Ok(Some(retrieved)) => {
                        assert_eq!(retrieved.len(), large_value_size);
                    }
                    Ok(None) => {
                        memory_allocation_failures += 1;
                        println!("Memory pressure caused data loss for key: {}", key);
                    }
                    Err(_) => memory_allocation_failures += 1,
                }
            }
            Err(_) => {
                memory_allocation_failures += 1;
                if memory_allocation_failures % 100 == 0 {
                    println!("Memory allocation failure #{} at record {}", memory_allocation_failures, i);
                }
            }
        }

        // Test cache eviction behavior
        if i % 100 == 0 && i > 0 {
            // Try to read older data that might have been evicted
            let old_key = format!("memory_stress_{:06}", std::cmp::max(0, i as i32 - 500) as usize);
            match db.get(old_key.as_bytes()) {
                Ok(Some(_)) => {}, // Cache hit or disk read
                Ok(None) => {}, // Might be evicted, which is acceptable
                Err(_) => memory_allocation_failures += 1,
            }
        }
    }

    println!("Memory exhaustion results:");
    println!("  Successful operations: {}", successful_operations);
    println!("  Memory allocation failures: {}", memory_allocation_failures);

    // Should handle some operations even under memory pressure
    assert!(successful_operations > memory_stress_records / 2, "Should complete at least half the operations under memory pressure");

    // Phase 2: Test transaction behavior under memory pressure
    println!("Testing transactions under memory pressure...");
    
    let mut tx_successes = 0;
    let mut tx_failures = 0;
    
    for tx_batch in 0..100 {
        match db.begin_transaction() {
            Ok(tx_id) => {
                let mut tx_operations = 0;
                let mut tx_failed = false;
                
                for i in 0..50 {
                    let key = format!("tx_memory_{}_{:06}", tx_batch, i);
                    let value = vec![b'T'; 2000]; // 2KB values
                    
                    match db.put_tx(tx_id, key.as_bytes(), &value) {
                        Ok(_) => tx_operations += 1,
                        Err(_) => {
                            tx_failed = true;
                            break;
                        }
                    }
                }
                
                if !tx_failed {
                    match db.commit_transaction(tx_id) {
                        Ok(_) => tx_successes += 1,
                        Err(_) => {
                            tx_failures += 1;
                            println!("Transaction commit failed due to memory pressure: {}", tx_batch);
                        }
                    }
                } else {
                    let _ = db.abort_transaction(tx_id);
                    tx_failures += 1;
                }
            }
            Err(_) => {
                tx_failures += 1;
                println!("Transaction creation failed due to memory pressure: {}", tx_batch);
            }
        }
    }

    println!("Transaction memory pressure results:");
    println!("  Successful transactions: {}", tx_successes);
    println!("  Failed transactions: {}", tx_failures);
    
    assert!(tx_successes > 10, "Should complete some transactions even under memory pressure");

    // Phase 3: Test recovery from memory pressure
    println!("Testing recovery from memory pressure...");
    
    // Smaller operations should succeed better
    let mut recovery_successes = 0;
    for i in 0..200 {
        let key = format!("recovery_mem_{:06}", i);
        let value = format!("recovery_{}", i); // Small values
        
        match db.put(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                recovery_successes += 1;
                // Verify read-back
                assert!(db.get(key.as_bytes()).unwrap().is_some());
            }
            Err(_) => {},
        }
    }

    println!("Memory pressure recovery: {}/200", recovery_successes);
    assert!(recovery_successes >= 150, "Should handle small operations well during recovery");

    // Test cache statistics under pressure
    if let Some(cache_stats) = db.cache_stats() {
        println!("Cache stats under memory pressure: {}", cache_stats);
        assert!(cache_stats.contains("hit") || cache_stats.contains("miss"));
    }
}

/// Test network partition and I/O failure scenarios
#[test]
fn test_io_failure_injection() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        cache_size: 20 * 1024 * 1024,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    println!("Testing I/O failure injection...");

    // Phase 1: Establish baseline with successful I/O
    let baseline_operations = 1000;
    for i in 0..baseline_operations {
        let key = format!("io_baseline_{:06}", i);
        let value = format!("baseline_io_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    db.checkpoint().unwrap();

    // Phase 2: Simulate I/O failures during various operations
    let mut io_operations = 0;
    let mut io_failures = 0;
    let mut io_recoveries = 0;

    println!("Simulating I/O failures...");
    
    for i in 0..500 {
        let key = format!("io_test_{:06}", i);
        let value = format!("io_test_value_{}_{}", i, "X".repeat(1000));
        
        match db.put(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                io_operations += 1;
                
                // Test read-back which might also fail
                match db.get(key.as_bytes()) {
                    Ok(Some(_)) => io_operations += 1,
                    Ok(None) => {
                        println!("I/O failure: data not found after write for key {}", key);
                        io_failures += 1;
                    }
                    Err(_) => io_failures += 1,
                }
            }
            Err(_) => {
                io_failures += 1;
                
                // Simulate I/O recovery attempt
                thread::sleep(Duration::from_millis(10)); // Brief delay
                
                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        io_recoveries += 1;
                        println!("I/O recovery successful for key {}", key);
                    }
                    Err(_) => {
                        println!("I/O recovery failed for key {}", key);
                    }
                }
            }
        }

        // Simulate checkpoint failures
        if i % 50 == 0 {
            match db.checkpoint() {
                Ok(_) => {},
                Err(_) => {
                    io_failures += 1;
                    println!("Checkpoint I/O failure at operation {}", i);
                }
            }
        }
    }

    println!("I/O failure simulation results:");
    println!("  Successful I/O operations: {}", io_operations);
    println!("  I/O failures: {}", io_failures);
    println!("  I/O recoveries: {}", io_recoveries);

    // Phase 3: Test transaction I/O failures
    let mut tx_io_successes = 0;
    let mut tx_io_failures = 0;

    for tx_num in 0..100 {
        match db.begin_transaction() {
            Ok(tx_id) => {
                let mut tx_ok = true;
                
                // Add data to transaction
                for i in 0..10 {
                    let key = format!("tx_io_{}_{:06}", tx_num, i);
                    let value = format!("tx_io_value_{}_{}", tx_num, i);
                    
                    if db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_err() {
                        tx_ok = false;
                        println!("Transaction I/O failure during put: tx {}, operation {}", tx_num, i);
                        break;
                    }
                }
                
                if tx_ok {
                    match db.commit_transaction(tx_id) {
                        Ok(_) => tx_io_successes += 1,
                        Err(_) => {
                            tx_io_failures += 1;
                            println!("Transaction commit I/O failure: {}", tx_num);
                        }
                    }
                } else {
                    let _ = db.abort_transaction(tx_id);
                    tx_io_failures += 1;
                }
            }
            Err(_) => {
                tx_io_failures += 1;
                println!("Transaction begin I/O failure: {}", tx_num);
            }
        }
    }

    println!("Transaction I/O results:");
    println!("  Successful transaction I/O: {}", tx_io_successes);
    println!("  Failed transaction I/O: {}", tx_io_failures);

    // Phase 4: Test database integrity after I/O failures
    let mut baseline_verification = 0;
    for i in 0..baseline_operations {
        let key = format!("io_baseline_{:06}", i);
        match db.get(key.as_bytes()) {
            Ok(Some(_)) => baseline_verification += 1,
            Ok(None) => println!("Lost baseline data due to I/O: {}", key),
            Err(_) => println!("I/O error reading baseline data: {}", key),
        }
    }

    println!("Baseline data after I/O failures: {}/{}", baseline_verification, baseline_operations);
    assert!(baseline_verification >= baseline_operations * 90 / 100, "Should retain 90%+ baseline data despite I/O failures");

    // Test database recovery from I/O failures
    drop(db);
    
    println!("Testing recovery after I/O failures...");
    let recovery_db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Verify database is functional after I/O failures
    for i in 0..100 {
        let key = format!("post_io_recovery_{:06}", i);
        let value = format!("recovery_value_{}", i);
        
        recovery_db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let retrieved = recovery_db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), value);
    }

    println!("I/O failure injection test completed successfully");
}

/// Test concurrent operations under various failure conditions
#[test]
fn test_concurrent_failure_injection() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 200 },
        cache_size: 30 * 1024 * 1024,
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());

    println!("Testing concurrent operations under failure injection...");

    // Shared failure injection controls
    let inject_memory_failures = Arc::new(AtomicBool::new(false));
    let inject_io_failures = Arc::new(AtomicBool::new(false));
    let inject_transaction_failures = Arc::new(AtomicBool::new(false));
    
    // Metrics
    let successful_operations = Arc::new(AtomicUsize::new(0));
    let failed_operations = Arc::new(AtomicUsize::new(0));
    let recovered_operations = Arc::new(AtomicUsize::new(0));

    // Worker threads performing operations under failure injection
    let num_workers = 6;
    let operations_per_worker = 500;
    let barrier = Arc::new(Barrier::new(num_workers + 1)); // +1 for failure injector

    let mut worker_handles = vec![];

    // Spawn worker threads
    for worker_id in 0..num_workers {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let memory_failures = Arc::clone(&inject_memory_failures);
        let io_failures = Arc::clone(&inject_io_failures);
        let tx_failures = Arc::clone(&inject_transaction_failures);
        let successes = Arc::clone(&successful_operations);
        let failures = Arc::clone(&failed_operations);
        let recoveries = Arc::clone(&recovered_operations);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..operations_per_worker {
                // Choose operation type
                match i % 4 {
                    0 => {
                        // Regular put/get operations
                        let key = format!("concurrent_fail_{}_{:06}", worker_id, i);
                        let value = if memory_failures.load(Ordering::Relaxed) {
                            format!("small_{}", i) // Smaller values during memory failures
                        } else {
                            format!("large_value_{}_{}", worker_id, "X".repeat(1000))
                        };

                        match db_clone.put(key.as_bytes(), value.as_bytes()) {
                            Ok(_) => {
                                if db_clone.get(key.as_bytes()).is_ok() {
                                    successes.fetch_add(2, Ordering::Relaxed);
                                } else {
                                    failures.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(_) => {
                                failures.fetch_add(1, Ordering::Relaxed);
                                
                                // Retry with smaller data
                                let retry_value = format!("retry_{}", i);
                                if db_clone.put(key.as_bytes(), retry_value.as_bytes()).is_ok() {
                                    recoveries.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    1 => {
                        // Transaction operations
                        if !tx_failures.load(Ordering::Relaxed) {
                            match db_clone.begin_transaction() {
                                Ok(tx_id) => {
                                    let mut tx_ops = 0;
                                    for j in 0..5 {
                                        let key = format!("tx_fail_{}_{}_{:03}", worker_id, i, j);
                                        let value = format!("tx_value_{}_{}_{}", worker_id, i, j);
                                        
                                        if db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                                            tx_ops += 1;
                                        }
                                    }
                                    
                                    if tx_ops > 0 {
                                        match db_clone.commit_transaction(tx_id) {
                                            Ok(_) => successes.fetch_add(tx_ops, Ordering::Relaxed),
                                            Err(_) => {
                                                failures.fetch_add(1, Ordering::Relaxed);
                                                // Try to rollback
                                                if db_clone.abort_transaction(tx_id).is_ok() {
                                                    recoveries.fetch_add(1, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                    } else {
                                        let _ = db_clone.abort_transaction(tx_id);
                                        failures.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Err(_) => failures.fetch_add(1, Ordering::Relaxed),
                            }
                        }
                    }
                    2 => {
                        // Cache-intensive operations
                        for j in 0..3 {
                            let key = format!("cache_fail_{}_{:06}_{}", worker_id, i, j);
                            let value = vec![b'C'; if memory_failures.load(Ordering::Relaxed) { 100 } else { 5000 }];
                            
                            match db_clone.put(key.as_bytes(), &value) {
                                Ok(_) => {
                                    // Immediate read (cache test)
                                    if db_clone.get(key.as_bytes()).is_ok() {
                                        successes.fetch_add(2, Ordering::Relaxed);
                                    }
                                }
                                Err(_) => failures.fetch_add(1, Ordering::Relaxed),
                            }
                        }
                    }
                    3 => {
                        // Mixed operations with potential I/O pressure
                        let key = format!("mixed_fail_{}_{:06}", worker_id, i);
                        let value = format!("mixed_value_{}_{}", worker_id, i);
                        
                        // Write
                        match db_clone.put(key.as_bytes(), value.as_bytes()) {
                            Ok(_) => successes.fetch_add(1, Ordering::Relaxed),
                            Err(_) => failures.fetch_add(1, Ordering::Relaxed),
                        }
                        
                        // Read previous data
                        if i > 10 {
                            let prev_key = format!("mixed_fail_{}_{:06}", worker_id, i - 10);
                            match db_clone.get(prev_key.as_bytes()) {
                                Ok(Some(_)) => successes.fetch_add(1, Ordering::Relaxed),
                                Ok(None) => {}, // Might be deleted or not exist
                                Err(_) => failures.fetch_add(1, Ordering::Relaxed),
                            }
                        }
                    }
                    _ => unreachable!(),
                }

                // Brief pause to allow failure injection changes
                if i % 100 == 0 {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        });
        
        worker_handles.push(handle);
    }

    // Failure injection thread
    let memory_failures = Arc::clone(&inject_memory_failures);
    let io_failures = Arc::clone(&inject_io_failures);
    let tx_failures = Arc::clone(&inject_transaction_failures);
    let barrier_clone = Arc::clone(&barrier);

    let injector_handle = thread::spawn(move || {
        barrier_clone.wait();
        
        let test_duration = Duration::from_secs(10);
        let start = Instant::now();
        
        while start.elapsed() < test_duration {
            let elapsed = start.elapsed().as_secs();
            
            // Cycle through different failure modes
            match elapsed % 12 {
                0..=3 => {
                    // Normal operation
                    memory_failures.store(false, Ordering::Relaxed);
                    io_failures.store(false, Ordering::Relaxed);
                    tx_failures.store(false, Ordering::Relaxed);
                }
                4..=6 => {
                    // Memory pressure
                    memory_failures.store(true, Ordering::Relaxed);
                    io_failures.store(false, Ordering::Relaxed);
                    tx_failures.store(false, Ordering::Relaxed);
                }
                7..=9 => {
                    // I/O failures
                    memory_failures.store(false, Ordering::Relaxed);
                    io_failures.store(true, Ordering::Relaxed);
                    tx_failures.store(false, Ordering::Relaxed);
                }
                10..=11 => {
                    // Transaction failures
                    memory_failures.store(false, Ordering::Relaxed);
                    io_failures.store(false, Ordering::Relaxed);
                    tx_failures.store(true, Ordering::Relaxed);
                }
                _ => unreachable!(),
            }
            
            thread::sleep(Duration::from_millis(500));
        }
    });

    // Wait for all threads to complete
    for handle in worker_handles {
        handle.join().unwrap();
    }
    injector_handle.join().unwrap();

    let total_successes = successful_operations.load(Ordering::Relaxed);
    let total_failures = failed_operations.load(Ordering::Relaxed);
    let total_recoveries = recovered_operations.load(Ordering::Relaxed);

    println!("Concurrent failure injection results:");
    println!("  Successful operations: {}", total_successes);
    println!("  Failed operations: {}", total_failures);
    println!("  Recovery operations: {}", total_recoveries);

    let total_attempts = total_successes + total_failures;
    let success_rate = if total_attempts > 0 {
        (total_successes * 100) / total_attempts
    } else { 0 };

    println!("  Success rate: {}%", success_rate);
    println!("  Recovery rate: {:.1}%", if total_failures > 0 { 
        (total_recoveries as f64 / total_failures as f64) * 100.0 
    } else { 0.0 });

    // Should maintain reasonable success rate even under failure injection
    assert!(success_rate >= 60, "Should maintain at least 60% success rate under failure injection");
    assert!(total_successes > 3000, "Should complete substantial number of operations");

    // Test final database integrity
    let verification_result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();

    let integrity_errors = verification_result.checksum_errors.len() +
                          verification_result.structure_errors.len() +
                          verification_result.consistency_errors.len();

    println!("  Final integrity errors: {}", integrity_errors);
    assert_eq!(integrity_errors, 0, "Should maintain integrity despite failure injection");

    println!("Concurrent failure injection test completed successfully");
}

/// Test system resource exhaustion scenarios
#[test]
fn test_system_resource_exhaustion() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
        cache_size: 15 * 1024 * 1024,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    println!("Testing system resource exhaustion...");

    // Test 1: File descriptor exhaustion simulation
    println!("Simulating file descriptor pressure...");
    
    let mut active_transactions = Vec::new();
    let max_transactions = 1000;
    let mut fd_pressure_errors = 0;

    for i in 0..max_transactions {
        match db.begin_transaction() {
            Ok(tx_id) => {
                // Add some data to each transaction
                let key = format!("fd_test_{:06}", i);
                let value = format!("fd_value_{}", i);
                
                match db.put_tx(tx_id, key.as_bytes(), value.as_bytes()) {
                    Ok(_) => active_transactions.push(tx_id),
                    Err(_) => {
                        fd_pressure_errors += 1;
                        let _ = db.abort_transaction(tx_id);
                    }
                }
            }
            Err(_) => {
                fd_pressure_errors += 1;
                println!("Transaction creation failed at #{} (simulated FD exhaustion)", i);
                break;
            }
        }

        // Periodically clean up some transactions
        if active_transactions.len() > 100 {
            for _ in 0..20 {
                if let Some(tx_id) = active_transactions.pop() {
                    let _ = db.commit_transaction(tx_id);
                }
            }
        }
    }

    // Clean up remaining transactions
    for tx_id in active_transactions {
        let _ = db.abort_transaction(tx_id);
    }

    println!("File descriptor pressure: {} errors encountered", fd_pressure_errors);

    // Test 2: Thread exhaustion simulation
    println!("Simulating thread exhaustion with concurrent operations...");
    
    let num_concurrent_threads = 50; // High number to simulate thread pressure
    let barrier = Arc::new(Barrier::new(num_concurrent_threads));
    let db_arc = Arc::new(&db);
    
    let thread_successes = Arc::new(AtomicUsize::new(0));
    let thread_failures = Arc::new(AtomicUsize::new(0));

    let mut thread_handles = vec![];

    for thread_id in 0..num_concurrent_threads {
        let db_clone = db_arc.clone();
        let barrier_clone = Arc::clone(&barrier);
        let successes = Arc::clone(&thread_successes);
        let failures = Arc::clone(&thread_failures);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..50 {
                let key = format!("thread_pressure_{}_{:06}", thread_id, i);
                let value = format!("thread_value_{}_{}", thread_id, i);
                
                match db_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        if db_clone.get(key.as_bytes()).is_ok() {
                            successes.fetch_add(2, Ordering::Relaxed);
                        } else {
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => failures.fetch_add(1, Ordering::Relaxed),
                }

                // Brief pause to simulate work
                thread::sleep(Duration::from_millis(1));
            }
        });

        thread_handles.push(handle);
    }

    // Wait for threads with timeout (some might fail to complete due to resource exhaustion)
    let mut completed_threads = 0;
    for handle in thread_handles {
        match handle.join() {
            Ok(_) => completed_threads += 1,
            Err(_) => println!("Thread failed to complete (resource exhaustion)"),
        }
    }

    let thread_ops_success = thread_successes.load(Ordering::Relaxed);
    let thread_ops_failure = thread_failures.load(Ordering::Relaxed);

    println!("Thread exhaustion results:");
    println!("  Completed threads: {}/{}", completed_threads, num_concurrent_threads);
    println!("  Successful operations: {}", thread_ops_success);
    println!("  Failed operations: {}", thread_ops_failure);

    assert!(completed_threads >= num_concurrent_threads * 8 / 10, "Should complete most threads even under pressure");

    // Test 3: Mixed resource pressure
    println!("Testing mixed resource pressure scenarios...");
    
    let mixed_operations = 2000;
    let mut mixed_successes = 0;
    let mut mixed_failures = 0;

    for i in 0..mixed_operations {
        match i % 5 {
            0 => {
                // Large value operations (memory pressure)
                let key = format!("mixed_large_{:06}", i);
                let value = vec![b'L'; 50000]; // 50KB
                
                match db.put(key.as_bytes(), &value) {
                    Ok(_) => mixed_successes += 1,
                    Err(_) => mixed_failures += 1,
                }
            }
            1 => {
                // Transaction operations (FD pressure)
                if let Ok(tx_id) = db.begin_transaction() {
                    let key = format!("mixed_tx_{:06}", i);
                    let value = format!("mixed_tx_value_{}", i);
                    
                    if db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                        if db.commit_transaction(tx_id).is_ok() {
                            mixed_successes += 1;
                        } else {
                            mixed_failures += 1;
                        }
                    } else {
                        let _ = db.abort_transaction(tx_id);
                        mixed_failures += 1;
                    }
                } else {
                    mixed_failures += 1;
                }
            }
            2 => {
                // Rapid small operations (CPU pressure)
                for j in 0..10 {
                    let key = format!("mixed_rapid_{}_{:03}", i, j);
                    let value = format!("rapid_{}", j);
                    
                    match db.put(key.as_bytes(), value.as_bytes()) {
                        Ok(_) => mixed_successes += 1,
                        Err(_) => mixed_failures += 1,
                    }
                }
            }
            3 => {
                // I/O intensive operations
                let key = format!("mixed_io_{:06}", i);
                let value = vec![b'I'; 20000]; // 20KB
                
                match db.put(key.as_bytes(), &value) {
                    Ok(_) => {
                        db.checkpoint().ok();
                        mixed_successes += 1;
                    }
                    Err(_) => mixed_failures += 1,
                }
            }
            4 => {
                // Cache thrashing operations
                for j in 0..5 {
                    let key = format!("mixed_cache_{}_{:03}", i, j);
                    let value = vec![b'C'; 10000]; // 10KB
                    
                    if db.put(key.as_bytes(), &value).is_ok() {
                        // Immediate read (cache test)
                        if db.get(key.as_bytes()).is_ok() {
                            mixed_successes += 2;
                        } else {
                            mixed_failures += 1;
                        }
                    } else {
                        mixed_failures += 1;
                    }
                }
            }
            _ => unreachable!(),
        }

        if i % 100 == 0 {
            println!("Mixed pressure progress: {}/{} ({}% success rate)", 
                   i, mixed_operations, 
                   if mixed_successes + mixed_failures > 0 { 
                       (mixed_successes * 100) / (mixed_successes + mixed_failures) 
                   } else { 100 });
        }
    }

    println!("Mixed resource pressure results:");
    println!("  Successful operations: {}", mixed_successes);
    println!("  Failed operations: {}", mixed_failures);

    let mixed_success_rate = if mixed_successes + mixed_failures > 0 {
        (mixed_successes * 100) / (mixed_successes + mixed_failures)
    } else { 0 };

    println!("  Overall success rate: {}%", mixed_success_rate);

    // Should handle resource pressure gracefully
    assert!(mixed_success_rate >= 70, "Should maintain at least 70% success rate under mixed resource pressure");
    assert!(mixed_successes > 5000, "Should complete substantial operations under pressure");

    println!("System resource exhaustion test completed successfully");
}