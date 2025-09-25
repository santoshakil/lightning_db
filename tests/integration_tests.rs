use lightning_db::{Database, LightningDbConfig, WriteBatch, WalSyncMode};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use rand::{Rng, thread_rng};

#[test]
fn test_basic_operations() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Test put and get
    db.put(b"key1", b"value1").expect("Put failed");
    let value = db.get(b"key1").expect("Get failed");
    assert_eq!(value, Some(b"value1".to_vec()));

    // Test update
    db.put(b"key1", b"updated_value").expect("Update failed");
    let value = db.get(b"key1").expect("Get after update failed");
    assert_eq!(value, Some(b"updated_value".to_vec()));

    // Test delete
    db.delete(b"key1").expect("Delete failed");
    let value = db.get(b"key1").expect("Get after delete failed");
    assert_eq!(value, None);
}

#[test]
fn test_batch_operations() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("batch_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    let mut batch = WriteBatch::new();
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        batch.put(key.into_bytes(), value.into_bytes()).expect("Batch put failed");
    }
    db.write_batch(&batch).expect("Batch write failed");

    // Verify all values
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let expected_value = format!("value_{:03}", i);
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert_eq!(value, Some(expected_value.into_bytes()));
    }
}

#[test]
fn test_transactions() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("tx_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Setup initial data
    db.put(b"counter", b"0").expect("Setup failed");

    // Start transaction
    let tx_id = db.begin_transaction().expect("Begin transaction failed");

    // Read in transaction
    let value = db.get_tx(tx_id, b"counter").expect("Get in tx failed");
    assert_eq!(value, Some(b"0".to_vec()));

    // Modify in transaction
    db.put_tx(tx_id, b"counter", b"1").expect("Put in tx failed");

    // Value should not be visible outside transaction
    let value = db.get(b"counter").expect("Get outside tx failed");
    assert_eq!(value, Some(b"0".to_vec()));

    // Commit transaction
    db.commit_transaction(tx_id).expect("Commit failed");

    // Now value should be updated
    let value = db.get(b"counter").expect("Get after commit failed");
    assert_eq!(value, Some(b"1".to_vec()));
}

#[test]
fn test_concurrent_operations() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("concurrent_db");
    let db = Arc::new(Database::open(db_path, Default::default()).expect("Failed to open database"));

    let num_threads = 4;
    let ops_per_thread = 100;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("thread_{}_value_{}", thread_id, i);
                db_clone.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all data
    for thread_id in 0..num_threads {
        for i in 0..ops_per_thread {
            let key = format!("thread_{}_key_{}", thread_id, i);
            let expected_value = format!("thread_{}_value_{}", thread_id, i);
            let value = db.get(key.as_bytes()).expect("Get failed");
            assert_eq!(value, Some(expected_value.into_bytes()));
        }
    }
}

#[test]
fn test_range_scan() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("scan_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Insert sorted data
    for i in 0..50 {
        let key = format!("key_{:02}", i);
        let value = format!("value_{:02}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }

    // Test full scan
    let iter = db.scan(None, None).expect("Full scan failed");
    let count = iter.count();
    assert_eq!(count, 50);

    // Test range scan
    let start = b"key_10";
    let end = b"key_20";
    let iter = db.scan(Some(start), Some(end)).expect("Range scan failed");
    let results: Vec<_> = iter.collect();
    assert!(results.len() >= 10 && results.len() <= 11);
}

#[test]
fn test_crash_recovery() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("recovery_db");

    // Phase 1: Write data
    {
        let db = Database::open(&db_path, Default::default()).expect("Failed to open database");

        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        }
        // Database drops here, simulating crash
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::open(&db_path, Default::default()).expect("Failed to recover database");

        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected_value = format!("value_{:03}", i);
            let value = db.get(key.as_bytes()).expect("Get failed");
            assert_eq!(value, Some(expected_value.into_bytes()));
        }
    }
}

#[test]
fn test_large_values() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("large_value_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Test various sizes
    let sizes = vec![1024, 10 * 1024, 100 * 1024, 1024 * 1024];

    for (idx, size) in sizes.iter().enumerate() {
        let key = format!("large_{}", idx);
        let value = vec![idx as u8; *size];

        db.put(key.as_bytes(), &value).expect("Put large value failed");
        let retrieved = db.get(key.as_bytes()).expect("Get large value failed");
        assert_eq!(retrieved, Some(value));
    }
}

#[test]
fn test_compression() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("compression_db");

    let config = LightningDbConfig {
        compression_enabled: true,
        compression_type: 1, // ZSTD
        ..Default::default()
    };

    let db = Database::open(db_path, config).expect("Failed to open database");

    // Write repetitive data that compresses well
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = "A".repeat(1000); // Highly compressible
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }

    // Verify data integrity
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert_eq!(value.map(|v| v.len()), Some(1000));
    }
}

#[test]
fn test_memory_pressure() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("memory_db");

    let config = LightningDbConfig {
        cache_size: 1024 * 1024, // Small 1MB cache
        ..Default::default()
    };

    let db = Database::open(db_path, config).expect("Failed to open database");

    // Write data exceeding cache
    for i in 0..500 {
        let key = format!("mem_key_{:05}", i);
        let value = vec![(i % 256) as u8; 10 * 1024]; // 10KB each
        db.put(key.as_bytes(), &value).expect("Put failed");
    }

    // Random reads to test cache eviction
    let mut rng = thread_rng();
    for _ in 0..100 {
        let idx = rng.gen_range(0..500);
        let key = format!("mem_key_{:05}", idx);
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert!(value.is_some());
    }
}

#[test]
fn test_consistency() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("consistency_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    let mut expected_data = std::collections::HashMap::new();

    // Write initial data
    for i in 0..200 {
        let key = format!("cons_key_{:04}", i);
        let value = format!("cons_value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        expected_data.insert(key, value);
    }

    // Perform updates
    for i in 0..50 {
        let key = format!("cons_key_{:04}", i);
        let value = format!("updated_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Update failed");
        expected_data.insert(key, value);
    }

    // Delete some keys
    for i in 100..120 {
        let key = format!("cons_key_{:04}", i);
        db.delete(key.as_bytes()).expect("Delete failed");
        expected_data.remove(&key);
    }

    // Verify all data
    for (key, expected_value) in &expected_data {
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert_eq!(value, Some(expected_value.clone().into_bytes()));
    }

    // Verify deleted keys
    for i in 100..120 {
        let key = format!("cons_key_{:04}", i);
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert!(value.is_none());
    }
}