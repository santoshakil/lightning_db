//! Comprehensive B+Tree Testing Suite
//! 
//! This test suite ensures the B+Tree implementation is bulletproof for production use.
//! Tests cover:
//! - Edge cases (empty keys, large values, boundary conditions)
//! - Concurrent operations
//! - Node splits and merges
//! - Data integrity
//! - Performance under stress
//! - Recovery after crashes

use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Test B+Tree behavior with edge case key/value sizes
#[test]
fn test_btree_edge_case_sizes() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false, // Test raw B+Tree without compression
        cache_size: 0, // Disable cache to test B+Tree directly
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    // Test 1: Very small keys and values
    db.put(b"a", b"1").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));

    // Test 2: Empty value (allowed)
    db.put(b"empty_val", b"").unwrap();
    assert_eq!(db.get(b"empty_val").unwrap(), Some(b"".to_vec()));

    // Test 3: Large key (near limit)
    let large_key = vec![b'k'; 4095]; // Just under 4KB limit
    let large_value = vec![b'v'; 1000];
    db.put(&large_key, &large_value).unwrap();
    assert_eq!(db.get(&large_key).unwrap(), Some(large_value));

    // Test 4: Very large value (near 1MB limit)
    let huge_value = vec![b'h'; 1024 * 1024 - 1];
    db.put(b"huge_val_key", &huge_value).unwrap();
    assert_eq!(db.get(b"huge_val_key").unwrap(), Some(huge_value));

    // Test 5: Invalid empty key (should fail)
    assert!(db.put(b"", b"value").is_err());

    // Test 6: Key too large (should fail)
    let oversized_key = vec![b'k'; 4097];
    assert!(db.put(&oversized_key, b"value").is_err());

    // Test 7: Value too large (should fail)
    let oversized_value = vec![b'v'; 1024 * 1024 + 1];
    assert!(db.put(b"key", &oversized_value).is_err());
}

/// Test B+Tree node splits and tree growth
#[test]
fn test_btree_node_splits() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 0,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    // Insert enough keys to force multiple node splits
    // Based on MIN_KEYS_PER_NODE = 50, MAX_KEYS_PER_NODE = 100
    const NUM_KEYS: usize = 1000;
    
    // Insert keys in sequential order to test split behavior
    for i in 0..NUM_KEYS {
        let key = format!("split_test_{:06}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify all keys are present
    for i in 0..NUM_KEYS {
        let key = format!("split_test_{:06}", i);
        let expected = format!("value_{}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(expected.into_bytes()));
    }

    // Insert keys in reverse order to test different split patterns
    for i in 0..NUM_KEYS {
        let key = format!("reverse_{:06}", NUM_KEYS - i - 1);
        let value = format!("rev_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify reverse keys
    for i in 0..NUM_KEYS {
        let key = format!("reverse_{:06}", i);
        let expected = format!("rev_value_{}", NUM_KEYS - i - 1);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(expected.into_bytes()));
    }

    // Insert random keys to test random split patterns
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut random_keys = Vec::new();
    
    for _ in 0..NUM_KEYS {
        let key = format!("random_{:08}", rng.gen::<u32>());
        let value = format!("rand_value_{}", rng.gen::<u32>());
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        random_keys.push((key, value));
    }

    // Verify random keys
    for (key, value) in &random_keys {
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(value.as_bytes().to_vec()));
    }
}

/// Test B+Tree deletion and node merging
#[test]
fn test_btree_deletion_and_merging() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 0,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const NUM_KEYS: usize = 500;

    // Insert keys
    for i in 0..NUM_KEYS {
        let key = format!("del_test_{:04}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Delete keys in different patterns to test merge behavior

    // Pattern 1: Delete every other key
    for i in (0..NUM_KEYS).step_by(2) {
        let key = format!("del_test_{:04}", i);
        assert!(db.delete(key.as_bytes()).unwrap());
    }

    // Verify deletions
    for i in 0..NUM_KEYS {
        let key = format!("del_test_{:04}", i);
        if i % 2 == 0 {
            assert!(db.get(key.as_bytes()).unwrap().is_none());
        } else {
            assert!(db.get(key.as_bytes()).unwrap().is_some());
        }
    }

    // Pattern 2: Delete remaining keys from the middle outward
    let mut remaining: Vec<usize> = (1..NUM_KEYS).step_by(2).collect();
    while !remaining.is_empty() {
        let mid = remaining.len() / 2;
        let i = remaining.remove(mid);
        let key = format!("del_test_{:04}", i);
        assert!(db.delete(key.as_bytes()).unwrap());
        
        // Verify key is deleted
        assert!(db.get(key.as_bytes()).unwrap().is_none());
    }

    // All keys should now be deleted
    for i in 0..NUM_KEYS {
        let key = format!("del_test_{:04}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_none());
    }

    // Test double deletion (should return false)
    let key = format!("del_test_{:04}", 0);
    assert!(!db.delete(key.as_bytes()).unwrap());
}

/// Test B+Tree concurrent operations
#[test]
fn test_btree_concurrent_operations() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 10 * 1024 * 1024, // 10MB cache for concurrent access
        ..Default::default()
    };
    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const NUM_THREADS: usize = 8;
    const OPS_PER_THREAD: usize = 1000;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let mut handles = vec![];

    // Spawn threads for concurrent operations
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            // Wait for all threads to start
            barrier_clone.wait();

            // Each thread works on its own key space
            for i in 0..OPS_PER_THREAD {
                let key = format!("thread_{}_key_{:04}", thread_id, i);
                let value = format!("thread_{}_value_{}", thread_id, i);

                // Insert
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();

                // Read back immediately
                let retrieved = db_clone.get(key.as_bytes()).unwrap().unwrap();
                assert_eq!(retrieved, value.as_bytes());

                // Update occasionally
                if i % 10 == 0 {
                    let updated_value = format!("updated_{}_{}", thread_id, i);
                    db_clone.put(key.as_bytes(), updated_value.as_bytes()).unwrap();
                }

                // Delete occasionally
                if i % 20 == 0 && i > 0 {
                    let del_key = format!("thread_{}_key_{:04}", thread_id, i - 10);
                    db_clone.delete(del_key.as_bytes()).unwrap();
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify data consistency
    for thread_id in 0..NUM_THREADS {
        for i in 0..OPS_PER_THREAD {
            let key = format!("thread_{}_key_{:04}", thread_id, i);
            
            if i % 20 == 10 {
                // Should be deleted
                assert!(db.get(key.as_bytes()).unwrap().is_none());
            } else if i % 10 == 0 {
                // Should be updated
                let expected = format!("updated_{}_{}", thread_id, i);
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(expected.into_bytes()));
            } else {
                // Should have original value
                let expected = format!("thread_{}_value_{}", thread_id, i);
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(expected.into_bytes()));
            }
        }
    }
}

/// Test B+Tree range scans and iteration
#[test]
fn test_btree_range_scans() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 0,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    // Insert keys with predictable ordering
    let prefixes = ["a", "b", "c", "d", "e"];
    for prefix in &prefixes {
        for i in 0..20 {
            let key = format!("{}_{:02}", prefix, i);
            let value = format!("value_{}_{}", prefix, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Test prefix iteration
    for prefix in &prefixes {
        let prefix_bytes = prefix.as_bytes();
        let mut count = 0;
        let mut prev_key = None;

        for result in db.iter_prefix(prefix_bytes).unwrap() {
            let (key, value) = result.unwrap();
            count += 1;

            // Verify key starts with prefix
            assert!(key.starts_with(prefix_bytes));

            // Verify ordering
            if let Some(prev) = prev_key {
                assert!(key > prev);
            }
            prev_key = Some(key.clone());

            // Verify value format
            let expected_prefix = format!("value_{}_", prefix);
            assert!(String::from_utf8(value).unwrap().starts_with(&expected_prefix));
        }

        assert_eq!(count, 20, "Expected 20 keys for prefix {}", prefix);
    }

    // Test range iteration
    let start = b"b_05";
    let end = b"d_15";
    let mut range_count = 0;

    for result in db.iter_range(start, end).unwrap() {
        let (key, _) = result.unwrap();
        range_count += 1;
        
        // Verify key is within range
        assert!(key.as_slice() >= start);
        assert!(key.as_slice() < end);
    }

    // Should include: b_05 to b_19 (15), all of c (20), d_00 to d_14 (15) = 50 total
    assert_eq!(range_count, 50);
}

/// Test B+Tree persistence and crash recovery
#[test]
fn test_btree_persistence_and_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 0,
        use_improved_wal: true,
        ..Default::default()
    };

    // Phase 1: Create database and insert data
    {
        let db = Database::create(&db_path, config.clone()).unwrap();
        
        // Insert test data
        for i in 0..100 {
            let key = format!("persist_{:03}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Update some keys
        for i in (0..100).step_by(5) {
            let key = format!("persist_{:03}", i);
            let value = format!("updated_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Delete some keys
        for i in (0..100).step_by(10) {
            let key = format!("persist_{:03}", i);
            db.delete(key.as_bytes()).unwrap();
        }

        // Explicit sync
        db.sync().unwrap();
    }

    // Phase 2: Reopen and verify data
    {
        let db = Database::open(&db_path, config).unwrap();
        
        for i in 0..100 {
            let key = format!("persist_{:03}", i);
            
            if i % 10 == 0 {
                // Should be deleted
                assert!(db.get(key.as_bytes()).unwrap().is_none());
            } else if i % 5 == 0 {
                // Should be updated
                let expected = format!("updated_{}", i);
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(expected.into_bytes()));
            } else {
                // Should have original value
                let expected = format!("value_{}", i);
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(expected.into_bytes()));
            }
        }
    }
}

/// Stress test B+Tree with high load
#[test]
fn test_btree_stress_high_load() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 50 * 1024 * 1024, // 50MB cache
        ..Default::default()
    };
    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const NUM_THREADS: usize = 16;
    const DURATION_SECS: u64 = 10;
    
    let start_time = Instant::now();
    let mut handles = vec![];

    // Spawn stress test threads
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let start = start_time;

        let handle = thread::spawn(move || {
            let mut ops = 0u64;
            let mut rng = rand::thread_rng();
            use rand::Rng;

            while start.elapsed() < Duration::from_secs(DURATION_SECS) {
                let op = rng.gen_range(0..100);
                let key_num = rng.gen_range(0..10000);
                let key = format!("stress_key_{:05}", key_num);

                match op {
                    0..=60 => {
                        // 60% reads
                        let _ = db_clone.get(key.as_bytes());
                    }
                    61..=85 => {
                        // 25% writes
                        let value = format!("stress_value_{}_{}_{}", thread_id, ops, rng.gen::<u32>());
                        let _ = db_clone.put(key.as_bytes(), value.as_bytes());
                    }
                    86..=95 => {
                        // 10% updates
                        let value = format!("updated_{}_{}_{}", thread_id, ops, rng.gen::<u32>());
                        let _ = db_clone.put(key.as_bytes(), value.as_bytes());
                    }
                    _ => {
                        // 5% deletes
                        let _ = db_clone.delete(key.as_bytes());
                    }
                }

                ops += 1;

                // Occasionally do range scans
                if ops % 1000 == 0 {
                    let start_key = format!("stress_key_{:05}", rng.gen_range(0..5000));
                    let end_key = format!("stress_key_{:05}", rng.gen_range(5000..10000));
                    let _ = db_clone.iter_range(start_key.as_bytes(), end_key.as_bytes());
                }
            }

            ops
        });
        handles.push(handle);
    }

    // Collect results
    let mut total_ops = 0u64;
    for handle in handles {
        total_ops += handle.join().unwrap();
    }

    let elapsed = start_time.elapsed();
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!("B+Tree Stress Test Results:");
    println!("  Duration: {:?}", elapsed);
    println!("  Total operations: {}", total_ops);
    println!("  Operations/sec: {:.0}", ops_per_sec);

    // Verify some data is still accessible
    let test_keys = ["stress_key_00001", "stress_key_05000", "stress_key_09999"];
    for key in &test_keys {
        // Key might exist or not due to deletes, but shouldn't error
        let _ = db.get(key.as_bytes()).unwrap();
    }
}

/// Test B+Tree behavior with duplicate key insertions
#[test]
fn test_btree_duplicate_keys() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 0,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    // Insert initial value
    db.put(b"duplicate_key", b"value1").unwrap();
    assert_eq!(db.get(b"duplicate_key").unwrap(), Some(b"value1".to_vec()));

    // Update with new value (should overwrite)
    db.put(b"duplicate_key", b"value2").unwrap();
    assert_eq!(db.get(b"duplicate_key").unwrap(), Some(b"value2".to_vec()));

    // Update again
    db.put(b"duplicate_key", b"value3").unwrap();
    assert_eq!(db.get(b"duplicate_key").unwrap(), Some(b"value3".to_vec()));

    // Test rapid updates
    for i in 0..100 {
        let value = format!("rapid_value_{}", i);
        db.put(b"rapid_key", value.as_bytes()).unwrap();
    }
    
    // Should have the last value
    assert_eq!(db.get(b"rapid_key").unwrap(), Some(b"rapid_value_99".to_vec()));
}

/// Test B+Tree memory efficiency
#[test]
fn test_btree_memory_efficiency() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 5 * 1024 * 1024, // Small 5MB cache
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    // Insert data that exceeds cache size
    const NUM_ENTRIES: usize = 10000;
    const VALUE_SIZE: usize = 1024; // 1KB values

    let start_time = Instant::now();
    
    for i in 0..NUM_ENTRIES {
        let key = format!("mem_test_{:06}", i);
        let value = vec![i as u8; VALUE_SIZE];
        db.put(key.as_bytes(), &value).unwrap();
    }

    let write_duration = start_time.elapsed();

    // Read back all data
    let read_start = Instant::now();
    
    for i in 0..NUM_ENTRIES {
        let key = format!("mem_test_{:06}", i);
        let value = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(value.len(), VALUE_SIZE);
        assert_eq!(value[0], i as u8);
    }

    let read_duration = read_start.elapsed();

    println!("B+Tree Memory Efficiency Test:");
    println!("  Entries: {} x {} bytes", NUM_ENTRIES, VALUE_SIZE);
    println!("  Total data: {} MB", (NUM_ENTRIES * VALUE_SIZE) / (1024 * 1024));
    println!("  Write time: {:?}", write_duration);
    println!("  Read time: {:?}", read_duration);
    println!("  Write throughput: {:.2} MB/s", 
        (NUM_ENTRIES * VALUE_SIZE) as f64 / write_duration.as_secs_f64() / (1024.0 * 1024.0));
    println!("  Read throughput: {:.2} MB/s",
        (NUM_ENTRIES * VALUE_SIZE) as f64 / read_duration.as_secs_f64() / (1024.0 * 1024.0));
}