use lightning_db::{Database, LightningDbConfig, WriteBatch};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use rand::{Rng, thread_rng};

#[test]
fn test_mixed_workload_simulation() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("mixed_workload_db");

    let config = LightningDbConfig {
        cache_size: 8 * 1024 * 1024,
        compression_enabled: true,
        max_active_transactions: 100,
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));

    let total_ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = vec![];

    // Writer threads
    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let total_ops_clone = Arc::clone(&total_ops);
        let errors_clone = Arc::clone(&errors);

        let handle = thread::spawn(move || {
            let mut rng = thread_rng();
            for i in 0..1000 {
                let key = format!("writer_{}_{}", thread_id, i);
                let value = vec![rng.gen::<u8>(); rng.gen_range(100..1000)];

                if db_clone.put(key.as_bytes(), &value).is_err() {
                    errors_clone.fetch_add(1, Ordering::Relaxed);
                } else {
                    total_ops_clone.fetch_add(1, Ordering::Relaxed);
                }

                if i % 100 == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        });
        handles.push(handle);
    }

    // Reader threads
    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let total_ops_clone = Arc::clone(&total_ops);

        let handle = thread::spawn(move || {
            let mut rng = thread_rng();
            for _ in 0..2000 {
                let writer_id = rng.gen_range(0..4);
                let item_id = rng.gen_range(0..1000);
                let key = format!("writer_{}_{}", writer_id, item_id);

                if db_clone.get(key.as_bytes()).is_ok() {
                    total_ops_clone.fetch_add(1, Ordering::Relaxed);
                }

                thread::sleep(Duration::from_micros(5));
            }
        });
        handles.push(handle);
    }

    // Batch writer thread
    let db_clone = Arc::clone(&db);
    let total_ops_clone = Arc::clone(&total_ops);
    let handle = thread::spawn(move || {
        for batch_id in 0..100 {
            let mut batch = WriteBatch::new();
            for i in 0..10 {
                let key = format!("batch_{}_{}", batch_id, i);
                let value = format!("batch_value_{}_{}", batch_id, i);
                batch.put(key.into_bytes(), value.into_bytes()).expect("Batch put failed");
            }
            if db_clone.write_batch(&batch).is_ok() {
                total_ops_clone.fetch_add(10, Ordering::Relaxed);
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
    handles.push(handle);

    // Scanner thread
    let db_clone = Arc::clone(&db);
    let total_ops_clone = Arc::clone(&total_ops);
    let handle = thread::spawn(move || {
        for _ in 0..50 {
            let start_key = b"batch_";
            let end_key = b"batch_~";
            if let Ok(iter) = db_clone.scan(Some(start_key), Some(end_key)) {
                let count = iter.take(100).count();
                total_ops_clone.fetch_add(count as u64, Ordering::Relaxed);
            }
            thread::sleep(Duration::from_millis(20));
        }
    });
    handles.push(handle);

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let elapsed = start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();

    println!("Mixed workload test completed:");
    println!("  Total operations: {}", total);
    println!("  Errors: {}", error_count);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} ops/sec", ops_per_sec);

    assert!(total > 10000, "Should complete significant operations");
    assert!(error_count < 10, "Should have minimal errors");
}

#[test]
fn test_crash_recovery_simulation() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("crash_recovery_db");

    // Phase 1: Write data
    {
        let db = Database::open(&db_path, Default::default()).expect("Failed to open database");

        // Write test data
        for i in 0..100 {
            let key = format!("persistent_key_{}", i);
            let value = format!("persistent_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        }

        // Write batch data
        let mut batch = WriteBatch::new();
        for i in 100..150 {
            let key = format!("persistent_key_{}", i);
            let value = format!("persistent_value_{}", i);
            batch.put(key.into_bytes(), value.into_bytes()).expect("Batch put failed");
        }
        db.write_batch(&batch).expect("Batch write failed");

        // Simulate crash by dropping database without explicit close
    }

    // Phase 2: Recovery and verification
    {
        let db = Database::open(&db_path, Default::default()).expect("Failed to recover database");

        // Verify all data is present
        for i in 0..150 {
            let key = format!("persistent_key_{}", i);
            let expected_value = format!("persistent_value_{}", i);
            let actual_value = db.get(key.as_bytes()).expect("Get failed");
            assert_eq!(
                actual_value.map(|v| String::from_utf8_lossy(&v).to_string()),
                Some(expected_value),
                "Data mismatch for key {}",
                i
            );
        }

        // Add more data after recovery
        for i in 150..200 {
            let key = format!("persistent_key_{}", i);
            let value = format!("persistent_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Post-recovery put failed");
        }
    }

    // Phase 3: Final verification
    {
        let db = Database::open(&db_path, Default::default()).expect("Failed to reopen database");

        // Count all entries
        let iter = db.scan(None, None).expect("Scan failed");
        let count = iter.count();
        assert_eq!(count, 200, "Should have all 200 entries after recovery");
    }

    println!("Crash recovery test passed with 200 entries recovered");
}

#[test]
fn test_large_value_handling() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("large_value_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Test various value sizes
    let sizes = vec![
        1024,        // 1KB
        10 * 1024,   // 10KB
        100 * 1024,  // 100KB
        1024 * 1024, // 1MB
    ];

    for (idx, size) in sizes.iter().enumerate() {
        let key = format!("large_value_{}", idx);
        let value = vec![idx as u8; *size];

        // Write large value
        db.put(key.as_bytes(), &value).expect("Failed to put large value");

        // Read and verify
        let retrieved = db.get(key.as_bytes()).expect("Failed to get large value");
        assert_eq!(retrieved, Some(value), "Large value mismatch for size {}", size);
    }

    // Test batch with large values
    let mut batch = WriteBatch::new();
    for i in 0..5 {
        let key = format!("batch_large_{}", i);
        let value = vec![i as u8; 50 * 1024]; // 50KB each
        batch.put(key.into_bytes(), value).expect("Batch put failed");
    }
    db.write_batch(&batch).expect("Large batch write failed");

    println!("Large value handling test completed successfully");
}

#[test]
fn test_transaction_isolation() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("transaction_db");
    let db = Arc::new(Database::open(db_path, Default::default()).expect("Failed to open database"));

    // Setup initial data
    for i in 0..10 {
        let key = format!("tx_key_{}", i);
        let value = format!("initial_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Setup failed");
    }

    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);

    // Start two concurrent transactions
    let handle1 = thread::spawn(move || {
        let tx_id = db1.begin_transaction().expect("Failed to begin transaction 1");

        // Read initial value
        let value = db1.get_tx(tx_id, b"tx_key_5").expect("Get failed");
        assert_eq!(value.map(|v| String::from_utf8_lossy(&v).to_string()),
                   Some("initial_value_5".to_string()));

        // Modify value
        db1.put_tx(tx_id, b"tx_key_5", b"tx1_modified").expect("Put failed");

        thread::sleep(Duration::from_millis(50));

        db1.commit_transaction(tx_id).expect("Commit failed");
    });

    let handle2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(25)); // Start slightly after tx1

        let tx_id = db2.begin_transaction().expect("Failed to begin transaction 2");

        // Should still see initial value (tx1 not committed yet)
        let value = db2.get_tx(tx_id, b"tx_key_5").expect("Get failed");
        assert_eq!(value.map(|v| String::from_utf8_lossy(&v).to_string()),
                   Some("initial_value_5".to_string()));

        // Modify different key
        db2.put_tx(tx_id, b"tx_key_6", b"tx2_modified").expect("Put failed");

        db2.commit_transaction(tx_id).expect("Commit failed");
    });

    handle1.join().expect("Thread 1 panicked");
    handle2.join().expect("Thread 2 panicked");

    // Verify final state
    let final_value_5 = db.get(b"tx_key_5").expect("Final get failed");
    let final_value_6 = db.get(b"tx_key_6").expect("Final get failed");

    assert_eq!(final_value_5.map(|v| String::from_utf8_lossy(&v).to_string()),
               Some("tx1_modified".to_string()));
    assert_eq!(final_value_6.map(|v| String::from_utf8_lossy(&v).to_string()),
               Some("tx2_modified".to_string()));

    println!("Transaction isolation test passed");
}

#[test]
fn test_memory_pressure() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("memory_pressure_db");

    let config = LightningDbConfig {
        cache_size: 2 * 1024 * 1024, // Small 2MB cache
        compression_enabled: true,
        ..Default::default()
    };

    let db = Database::open(db_path, config).expect("Failed to open database");

    // Write data exceeding cache size
    let value_size = 10 * 1024; // 10KB per value
    let num_entries = 500; // Total ~5MB of data

    for i in 0..num_entries {
        let key = format!("mem_key_{:05}", i);
        let value = vec![(i % 256) as u8; value_size];
        db.put(key.as_bytes(), &value).expect("Put failed under memory pressure");
    }

    // Force cache eviction by reading different entries
    let mut rng = thread_rng();
    for _ in 0..100 {
        let idx = rng.gen_range(0..num_entries);
        let key = format!("mem_key_{:05}", idx);
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert!(value.is_some(), "Data should be retrievable under memory pressure");
    }

    // Verify all data still accessible
    let scan_iter = db.scan(Some(b"mem_key_"), Some(b"mem_key_~")).expect("Scan failed");
    let count = scan_iter.count();
    assert_eq!(count, num_entries, "All entries should be accessible");

    println!("Memory pressure test completed with {} entries", num_entries);
}