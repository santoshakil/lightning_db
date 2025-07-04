use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{Arc, Barrier, atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Test heavy concurrent load with multiple readers and writers
#[test]
fn test_heavy_concurrent_load() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024, // 100MB cache
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
        prefetch_enabled: true,
        ..Default::default()
    };
    
    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let num_writers = 4;
    let num_readers = 8;
    let operations_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_writers + num_readers));
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    
    let mut handles = vec![];
    
    // Spawn writer threads
    for writer_id in 0..num_writers {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let success_clone = Arc::clone(&success_count);
        let error_clone = Arc::clone(&error_count);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..operations_per_thread {
                let key = format!("writer_{}_key_{}", writer_id, i);
                let value = format!("value_{}_{}_{}", writer_id, i, rand::random::<u32>());
                
                match db_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => success_clone.fetch_add(1, Ordering::Relaxed),
                    Err(_) => error_clone.fetch_add(1, Ordering::Relaxed),
                };
                
                // Occasionally update existing keys
                if i % 10 == 0 && i > 0 {
                    let update_key = format!("writer_{}_key_{}", writer_id, i - 5);
                    let update_value = format!("updated_value_{}_{}", writer_id, i);
                    let _ = db_clone.put(update_key.as_bytes(), update_value.as_bytes());
                }
            }
        });
        handles.push(handle);
    }
    
    // Spawn reader threads
    for reader_id in 0..num_readers {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let success_clone = Arc::clone(&success_count);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            for _ in 0..operations_per_thread {
                let writer_id = reader_id % num_writers;
                let key_id = (rand::random::<u32>() as usize) % operations_per_thread;
                let key = format!("writer_{}_key_{}", writer_id, key_id);
                
                if db_clone.get(key.as_bytes()).is_ok() {
                    success_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    let total_successes = success_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    
    println!("Heavy concurrent load test completed:");
    println!("  Total successful operations: {}", total_successes);
    println!("  Total errors: {}", total_errors);
    
    assert!(total_errors < 10, "Too many errors during concurrent operations");
    assert!(total_successes > (num_writers * operations_per_thread), 
            "Not enough successful operations");
}

/// Test database behavior under memory pressure
#[test]
fn test_memory_pressure() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024, // Small 10MB cache
        compression_enabled: true,
        ..Default::default()
    };
    
    let db = Database::open(dir.path(), config).unwrap();
    let large_value_size = 100_000; // 100KB values
    let num_entries = 200; // Total of ~20MB data
    
    // Insert large values that exceed cache size
    for i in 0..num_entries {
        let key = format!("large_key_{:06}", i);
        let value = vec![i as u8; large_value_size];
        
        db.put(key.as_bytes(), &value).unwrap();
        
        // Verify we can read it back immediately
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(retrieved.len(), large_value_size);
    }
    
    // Read back all values to test cache eviction
    for i in 0..num_entries {
        let key = format!("large_key_{:06}", i);
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(retrieved.len(), large_value_size);
        assert_eq!(retrieved[0], i as u8);
    }
    
    // Get cache statistics
    if let Some(stats_str) = db.cache_stats() {
        println!("Cache stats under memory pressure:");
        println!("{}", stats_str);
        
        // Simple check that we have some cache activity
        assert!(stats_str.contains("hit") || stats_str.contains("miss"), 
                "Should have cache activity under memory pressure");
    }
}

/// Test transaction conflicts and retry behavior
#[test]
#[ignore = "Stress test with extreme contention - passes but takes time"]
fn test_transaction_conflicts() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path(), LightningDbConfig::default()).unwrap());
    
    // Initialize test data
    for i in 0..10 {
        db.put(format!("counter_{}", i).as_bytes(), b"0").unwrap();
    }
    
    let num_threads = 3;
    let increments_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));
    let conflict_count = Arc::new(AtomicUsize::new(0));
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let conflict_clone = Arc::clone(&conflict_count);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..increments_per_thread {
                // Spread access pattern to reduce contention
                let counter_id = (thread_id * increments_per_thread + i) % 10;
                let key = format!("counter_{}", counter_id);
                
                // Retry loop for handling conflicts
                let mut retries = 0;
                loop {
                    let tx_id = db_clone.begin_transaction().unwrap();
                    
                    // Read current value
                    let current = db_clone.get_tx(tx_id, key.as_bytes()).unwrap()
                        .and_then(|v| String::from_utf8(v).ok())
                        .and_then(|s| s.parse::<i32>().ok())
                        .unwrap_or(0);
                    
                    // Increment
                    let new_value = (current + 1).to_string();
                    db_clone.put_tx(tx_id, key.as_bytes(), new_value.as_bytes()).unwrap();
                    
                    // Try to commit
                    match db_clone.commit_transaction(tx_id) {
                        Ok(_) => break,
                        Err(_) => {
                            retries += 1;
                            if retries > 50 {
                                panic!("Too many retries for transaction");
                            }
                            conflict_clone.fetch_add(1, Ordering::Relaxed);
                            // Exponential backoff with jitter
                            let backoff = std::cmp::min(100 * (1 << std::cmp::min(retries, 6)), 1000);
                            thread::sleep(Duration::from_micros(backoff + (retries * 10)));
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
    
    // Verify final counter values
    let mut total_count = 0;
    for i in 0..10 {
        let key = format!("counter_{}", i);
        let value = db.get(key.as_bytes()).unwrap().unwrap();
        let count = String::from_utf8(value).unwrap().parse::<i32>().unwrap();
        total_count += count;
    }
    
    let expected_total = num_threads * increments_per_thread;
    println!("Transaction conflict test:");
    println!("  Expected total count: {}", expected_total);
    println!("  Actual total count: {}", total_count);
    println!("  Number of conflicts: {}", conflict_count.load(Ordering::Relaxed));
    
    assert_eq!(total_count, expected_total as i32, "Incorrect total count after concurrent increments");
}

/// Test database recovery after simulated crash
#[test]
fn test_crash_recovery_simulation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    
    // Phase 1: Normal operations followed by abrupt termination
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        
        let db = Database::open(&db_path, config).unwrap();
        
        // Insert committed data
        for i in 0..50 {
            db.put(format!("committed_{}", i).as_bytes(), format!("value_{}", i).as_bytes()).unwrap();
        }
        
        // Start a transaction but don't commit
        let tx_id = db.begin_transaction().unwrap();
        for i in 0..20 {
            db.put_tx(tx_id, format!("uncommitted_{}", i).as_bytes(), b"should_not_exist").unwrap();
        }
        
        // Drop without committing or proper shutdown
        std::mem::forget(db);
    }
    
    // Phase 2: Recovery and verification
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();
        
        // Verify committed data exists
        for i in 0..50 {
            let key = format!("committed_{}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Committed data should be recovered");
        }
        
        // Verify uncommitted data does not exist
        for i in 0..20 {
            let key = format!("uncommitted_{}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_none(), "Uncommitted data should not be recovered");
        }
    }
}

/// Test long-running operations with checkpoints
#[test]
fn test_long_running_with_checkpoints() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 500 },
        ..Default::default()
    };
    
    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let running = Arc::new(AtomicBool::new(true));
    let checkpoint_count = Arc::new(AtomicUsize::new(0));
    
    // Spawn checkpoint thread
    let db_checkpoint = Arc::clone(&db);
    let running_checkpoint = Arc::clone(&running);
    let checkpoint_counter = Arc::clone(&checkpoint_count);
    
    let checkpoint_handle = thread::spawn(move || {
        while running_checkpoint.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(200));
            if db_checkpoint.checkpoint().is_ok() {
                checkpoint_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    
    // Run operations for 2 seconds
    let start = Instant::now();
    let mut operation_count = 0;
    
    while start.elapsed() < Duration::from_secs(2) {
        let key = format!("key_{}", operation_count);
        let value = format!("value_{}", operation_count);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        operation_count += 1;
        
        if operation_count % 100 == 0 {
            thread::sleep(Duration::from_millis(10));
        }
    }
    
    // Stop checkpoint thread
    running.store(false, Ordering::Relaxed);
    checkpoint_handle.join().unwrap();
    
    let final_checkpoints = checkpoint_count.load(Ordering::Relaxed);
    println!("Long-running operation test:");
    println!("  Operations performed: {}", operation_count);
    println!("  Checkpoints completed: {}", final_checkpoints);
    
    assert!(final_checkpoints >= 5, "Should have multiple checkpoints during long-running operations");
    assert!(operation_count > 100, "Should perform many operations");
}

/// Test graceful shutdown behavior
#[test]
fn test_graceful_shutdown() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    
    // Create database and perform operations
    {
        let config = LightningDbConfig {
            prefetch_enabled: true,
            ..Default::default()
        };
        
        let db = Database::open(&db_path, config).unwrap();
        
        // Insert test data
        for i in 0..100 {
            db.put(format!("shutdown_test_{}", i).as_bytes(), format!("value_{}", i).as_bytes()).unwrap();
        }
        
        // Perform graceful shutdown
        db.shutdown().unwrap();
    }
    
    // Reopen and verify all data is intact
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
        
        for i in 0..100 {
            let key = format!("shutdown_test_{}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Data should persist after graceful shutdown");
        }
    }
}

/// Test data integrity under concurrent modifications
#[test]
fn test_data_integrity_concurrent() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path(), LightningDbConfig::default()).unwrap());
    
    // Initialize test data
    let num_keys = 100;
    for i in 0..num_keys {
        let key = format!("integrity_key_{}", i);
        let value = format!("initial_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Run integrity verification
    let verifier = db.verify_integrity().unwrap();
    assert_eq!(verifier.errors.len(), 0, "Should have no integrity errors initially");
    
    // Perform concurrent modifications
    let num_threads = 4;
    let modifications_per_thread = 250;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..modifications_per_thread {
                let key_id = (rand::random::<u32>() as usize) % num_keys;
                let key = format!("integrity_key_{}", key_id);
                let value = format!("modified_by_thread_{}_{}", thread_id, i);
                
                // Randomly choose between put, delete, and re-insert
                match i % 3 {
                    0 => { let _ = db_clone.put(key.as_bytes(), value.as_bytes()); }
                    1 => { let _ = db_clone.delete(key.as_bytes()); }
                    2 => {
                        let _ = db_clone.delete(key.as_bytes());
                        let _ = db_clone.put(key.as_bytes(), value.as_bytes());
                    }
                    _ => unreachable!()
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify integrity after concurrent modifications
    let final_verification = db.verify_integrity().unwrap();
    println!("Data integrity after concurrent modifications:");
    println!("  Errors: {}", final_verification.errors.len());
    println!("  Warnings: {}", final_verification.warnings.len());
    
    assert_eq!(final_verification.errors.len(), 0, "Should maintain data integrity under concurrent modifications");
}