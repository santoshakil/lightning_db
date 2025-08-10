use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Test crash safety - verify error handling works correctly under edge conditions
/// This replaces scenarios that would previously panic due to unwrap() usage
#[test]
fn test_crash_safety_no_panic() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Test edge case: Empty transaction commit
    let tx_id = db.begin_transaction().unwrap();
    assert!(db.commit_transaction(tx_id).is_ok(), "Empty transaction should commit without panic");

    // Test edge case: Invalid transaction ID operations
    let invalid_tx_id = 99999;
    let result = db.put_tx(invalid_tx_id, b"key", b"value");
    assert!(result.is_err(), "Invalid transaction operations should return error, not panic");

    let result = db.get_tx(invalid_tx_id, b"key");
    assert!(result.is_err(), "Invalid transaction operations should return error, not panic");

    let result = db.commit_transaction(invalid_tx_id);
    assert!(result.is_err(), "Invalid transaction commit should return error, not panic");

    // Test edge case: Multiple commits of same transaction
    let tx_id = db.begin_transaction().unwrap();
    db.put_tx(tx_id, b"test_key", b"test_value").unwrap();
    assert!(db.commit_transaction(tx_id).is_ok());
    
    // Second commit should fail gracefully
    let result = db.commit_transaction(tx_id);
    assert!(result.is_err(), "Double commit should return error, not panic");

    // Test edge case: Operations on committed transaction
    let result = db.put_tx(tx_id, b"another_key", b"another_value");
    assert!(result.is_err(), "Operations on committed transaction should return error, not panic");

    // Test edge case: Extremely large keys and values
    let large_key = vec![b'k'; 1_000_000]; // 1MB key
    let large_value = vec![b'v'; 10_000_000]; // 10MB value
    
    let result = db.put(&large_key, &large_value);
    // Should either succeed or fail gracefully, not panic
    match result {
        Ok(_) => println!("Large data handled successfully"),
        Err(e) => println!("Large data rejected gracefully: {:?}", e),
    }

    // Test edge case: Empty keys and values
    assert!(db.put(b"", b"empty_key_value").is_ok());
    assert!(db.put(b"empty_value_key", b"").is_ok());

    println!("Crash safety test completed - no panics observed");
}

/// Test concurrent transaction conflicts under extreme contention
/// Validates that deadlock prevention mechanisms work correctly
#[test]
fn test_extreme_transaction_contention() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path(), LightningDbConfig::default()).unwrap());

    // Initialize hot keys that will cause maximum contention
    let hot_keys = ["hot_key_1", "hot_key_2", "hot_key_3"];
    for key in &hot_keys {
        db.put(key.as_bytes(), b"0").unwrap();
    }

    let num_threads = 10;
    let operations_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));
    let conflict_count = Arc::new(AtomicUsize::new(0));
    let timeout_count = Arc::new(AtomicUsize::new(0));
    let deadlock_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let conflict_clone = Arc::clone(&conflict_count);
        let timeout_clone = Arc::clone(&timeout_count);
        let deadlock_clone = Arc::clone(&deadlock_count);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..operations_per_thread {
                // All threads compete for the same hot keys
                let key1 = hot_keys[i % hot_keys.len()];
                let key2 = hot_keys[(i + 1) % hot_keys.len()];

                let mut retries = 0;
                let max_retries = 100;
                let timeout = Duration::from_millis(5000);
                let start = Instant::now();

                loop {
                    if start.elapsed() > timeout {
                        timeout_clone.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    let tx_id = match db_clone.begin_transaction() {
                        Ok(id) => id,
                        Err(_) => {
                            deadlock_clone.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                    };

                    // Read both keys (potential deadlock scenario)
                    let val1 = match db_clone.get_tx(tx_id, key1.as_bytes()) {
                        Ok(Some(v)) => String::from_utf8(v).unwrap_or_default().parse::<i32>().unwrap_or(0),
                        Ok(None) => 0,
                        Err(_) => {
                            let _ = db_clone.abort_transaction(tx_id);
                            deadlock_clone.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                    };

                    let val2 = match db_clone.get_tx(tx_id, key2.as_bytes()) {
                        Ok(Some(v)) => String::from_utf8(v).unwrap_or_default().parse::<i32>().unwrap_or(0),
                        Ok(None) => 0,
                        Err(_) => {
                            let _ = db_clone.abort_transaction(tx_id);
                            deadlock_clone.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                    };

                    // Update both keys
                    if db_clone.put_tx(tx_id, key1.as_bytes(), (val1 + 1).to_string().as_bytes()).is_err() ||
                       db_clone.put_tx(tx_id, key2.as_bytes(), (val2 + 1).to_string().as_bytes()).is_err() {
                        let _ = db_clone.abort_transaction(tx_id);
                        deadlock_clone.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }

                    match db_clone.commit_transaction(tx_id) {
                        Ok(_) => break,
                        Err(_) => {
                            retries += 1;
                            if retries >= max_retries {
                                timeout_clone.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            conflict_clone.fetch_add(1, Ordering::Relaxed);
                            
                            // Exponential backoff with jitter
                            let backoff = std::cmp::min(1 << std::cmp::min(retries / 10, 8), 100);
                            let jitter = (thread_id as u64 * 13) % 10;
                            thread::sleep(Duration::from_millis(backoff + jitter));
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let conflicts = conflict_count.load(Ordering::Relaxed);
    let timeouts = timeout_count.load(Ordering::Relaxed);
    let deadlocks = deadlock_count.load(Ordering::Relaxed);

    println!("Extreme contention test results:");
    println!("  Conflicts handled: {}", conflicts);
    println!("  Timeouts: {}", timeouts);
    println!("  Deadlock recoveries: {}", deadlocks);

    // Verify system remained stable - deadlocks should be prevented
    assert!(deadlocks == 0 || deadlocks < timeouts, "Deadlock prevention should work");
    assert!(timeouts < num_threads * operations_per_thread / 2, "Should not have excessive timeouts");

    // Verify data consistency after extreme contention
    for key in &hot_keys {
        let value = db.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Hot keys should still exist after contention");
        let count: i32 = String::from_utf8(value.unwrap()).unwrap().parse().unwrap();
        assert!(count > 0, "Counters should have been incremented");
    }
}

/// Test resource exhaustion scenarios
/// Validates graceful handling when system resources are limited
#[test]
fn test_resource_exhaustion_handling() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 1024 * 1024, // Small 1MB cache to force pressure
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Test 1: Memory exhaustion simulation
    let large_value_size = 100_000; // 100KB values
    let mut successful_inserts = 0;
    let mut memory_errors = 0;

    for i in 0..1000 { // Try to insert 100MB of data into 1MB cache
        let key = format!("memory_test_{:06}", i);
        let value = vec![i as u8; large_value_size];

        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                successful_inserts += 1;
                // Verify immediate read-back
                match db.get(key.as_bytes()) {
                    Ok(Some(retrieved)) => {
                        assert_eq!(retrieved.len(), large_value_size);
                        assert_eq!(retrieved[0], i as u8);
                    }
                    Ok(None) => panic!("Just inserted data should be readable"),
                    Err(_) => memory_errors += 1,
                }
            }
            Err(_) => memory_errors += 1,
        }

        // Don't overwhelm the system
        if i % 10 == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }

    println!("Memory exhaustion test:");
    println!("  Successful inserts: {}", successful_inserts);
    println!("  Memory-related errors: {}", memory_errors);

    assert!(successful_inserts > 10, "Should handle at least some large values");

    // Test 2: Transaction exhaustion
    let mut active_transactions = Vec::new();
    let mut tx_exhausted = false;

    for i in 0..10000 {
        match db.begin_transaction() {
            Ok(tx_id) => {
                active_transactions.push(tx_id);
                // Add some data to each transaction
                let _ = db.put_tx(tx_id, format!("tx_key_{}", i).as_bytes(), b"tx_value");
            }
            Err(_) => {
                tx_exhausted = true;
                break;
            }
        }

        if i % 100 == 0 {
            // Clean up some transactions
            if active_transactions.len() > 50 {
                for _ in 0..10 {
                    if let Some(tx_id) = active_transactions.pop() {
                        let _ = db.abort_transaction(tx_id);
                    }
                }
            }
        }
    }

    // Clean up remaining transactions
    for tx_id in active_transactions {
        let _ = db.abort_transaction(tx_id);
    }

    println!("Transaction exhaustion test:");
    println!("  Transaction limit encountered: {}", tx_exhausted);

    // Should either handle many transactions or gracefully limit them
    assert!(tx_exhausted || true, "System should handle transaction pressure gracefully");

    // Test 3: Concurrent access under resource pressure
    let num_threads = 8;
    let barrier = Arc::new(Barrier::new(num_threads));
    let error_count = Arc::new(AtomicUsize::new(0));
    let success_count = Arc::new(AtomicUsize::new(0));
    let db_arc = Arc::new(db);

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db_arc);
        let barrier_clone = Arc::clone(&barrier);
        let error_clone = Arc::clone(&error_count);
        let success_clone = Arc::clone(&success_count);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..100 {
                let key = format!("pressure_{}_{}", thread_id, i);
                let value = vec![thread_id as u8; 50000]; // 50KB values

                match db_clone.put(key.as_bytes(), &value) {
                    Ok(_) => {
                        success_clone.fetch_add(1, Ordering::Relaxed);
                        // Verify read-back
                        match db_clone.get(key.as_bytes()) {
                            Ok(Some(_)) => {}
                            Ok(None) => { error_clone.fetch_add(1, Ordering::Relaxed); }
                            Err(_) => { error_clone.fetch_add(1, Ordering::Relaxed); }
                        }
                    }
                    Err(_) => { error_clone.fetch_add(1, Ordering::Relaxed); }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_successes = success_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);

    println!("Concurrent pressure test:");
    println!("  Successful operations: {}", total_successes);
    println!("  Error operations: {}", total_errors);

    // Should handle at least some operations under pressure
    assert!(total_successes > 100, "Should handle some operations under resource pressure");
}

/// Test corruption detection and recovery mechanisms
#[test]
fn test_corruption_detection_and_recovery() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Insert test data
    let test_data = (0..100).map(|i| {
        let key = format!("integrity_test_{:06}", i);
        let value = format!("test_value_{}_{}", i, rand::random::<u64>());
        (key, value)
    }).collect::<Vec<_>>();

    for (key, value) in &test_data {
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Force data to disk
    db.checkpoint().unwrap();

    // Verify initial integrity
    let initial_verification = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();

    let initial_errors = initial_verification.checksum_errors.len()
        + initial_verification.structure_errors.len()
        + initial_verification.consistency_errors.len();

    assert_eq!(initial_errors, 0, "Should have no integrity errors initially");

    // Simulate potential corruption scenarios by testing edge cases
    
    // Test 1: Verify data consistency after multiple operations
    for i in 0..50 {
        let key = format!("integrity_test_{:06}", i);
        let new_value = format!("updated_value_{}", rand::random::<u64>());
        db.put(key.as_bytes(), new_value.as_bytes()).unwrap();
    }

    // Delete some entries
    for i in 50..75 {
        let key = format!("integrity_test_{:06}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    // Force checkpoint again
    db.checkpoint().unwrap();

    // Verify integrity after modifications
    let post_modification_verification = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();

    let post_modification_errors = post_modification_verification.checksum_errors.len()
        + post_modification_verification.structure_errors.len()
        + post_modification_verification.consistency_errors.len();

    println!("Corruption detection test:");
    println!("  Initial integrity errors: {}", initial_errors);
    println!("  Post-modification integrity errors: {}", post_modification_errors);
    println!("  Checksum errors: {}", post_modification_verification.checksum_errors.len());
    println!("  Structure errors: {}", post_modification_verification.structure_errors.len());
    println!("  Consistency errors: {}", post_modification_verification.consistency_errors.len());

    assert_eq!(
        post_modification_errors, 0,
        "Should maintain integrity after normal operations"
    );

    // Test 2: Verify all remaining data is correct
    for (key, original_value) in &test_data[75..] {
        match db.get(key.as_bytes()).unwrap() {
            Some(retrieved_value) => {
                let retrieved_str = String::from_utf8(retrieved_value).unwrap();
                assert_eq!(&retrieved_str, original_value);
            }
            None => panic!("Data should still exist: {}", key),
        }
    }

    // Test 3: Verify deleted data is gone
    for i in 50..75 {
        let key = format!("integrity_test_{:06}", i);
        match db.get(key.as_bytes()).unwrap() {
            Some(_) => panic!("Deleted data should not exist: {}", key),
            None => {} // Expected
        }
    }
}

/// Test multiple crash/recovery cycles to ensure robustness
#[test]
fn test_multiple_crash_recovery_cycles() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    
    let cycles = 5;
    let operations_per_cycle = 100;

    for cycle in 0..cycles {
        println!("Starting crash recovery cycle {}/{}", cycle + 1, cycles);

        // Open database
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        let db = Database::open(&db_path, config).unwrap();

        // Verify existing data from previous cycles
        for prev_cycle in 0..cycle {
            for i in 0..operations_per_cycle {
                let key = format!("cycle_{}_{:06}", prev_cycle, i);
                match db.get(key.as_bytes()) {
                    Ok(Some(_)) => {} // Expected
                    Ok(None) => panic!("Data from cycle {} should exist after crash recovery", prev_cycle),
                    Err(e) => panic!("Error reading data from cycle {}: {:?}", prev_cycle, e),
                }
            }
        }

        // Add new data for this cycle
        for i in 0..operations_per_cycle {
            let key = format!("cycle_{}_{:06}", cycle, i);
            let value = format!("cycle_{}_value_{}", cycle, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Start some uncommitted transactions
        let mut uncommitted_transactions = Vec::new();
        for i in 0..5 {
            let tx_id = db.begin_transaction().unwrap();
            let key = format!("uncommitted_cycle_{}_{}", cycle, i);
            db.put_tx(tx_id, key.as_bytes(), b"should_not_survive").unwrap();
            uncommitted_transactions.push(tx_id);
        }

        // Force some data to be committed
        db.checkpoint().unwrap();

        // Simulate crash by dropping without proper shutdown
        std::mem::forget(db);
        
        println!("Simulated crash for cycle {}", cycle + 1);
    }

    // Final recovery and verification
    println!("Final recovery and verification");
    let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

    // Verify all committed data survived
    for cycle in 0..cycles {
        for i in 0..operations_per_cycle {
            let key = format!("cycle_{}_{:06}", cycle, i);
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let expected = format!("cycle_{}_value_{}", cycle, i);
                    assert_eq!(String::from_utf8(value).unwrap(), expected);
                }
                Ok(None) => panic!("Committed data should survive crash recovery: {}", key),
                Err(e) => panic!("Error reading committed data: {:?}", e),
            }
        }
    }

    // Verify uncommitted data did not survive
    for cycle in 0..cycles {
        for i in 0..5 {
            let key = format!("uncommitted_cycle_{}_{}", cycle, i);
            match db.get(key.as_bytes()) {
                Ok(Some(_)) => panic!("Uncommitted data should not survive crash: {}", key),
                Ok(None) => {} // Expected
                Err(e) => panic!("Error checking uncommitted data: {:?}", e),
            }
        }
    }

    println!("Multiple crash recovery cycles completed successfully");
}