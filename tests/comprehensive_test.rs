use lightning_db::{Database, LightningDbConfig, WalSyncMode, WriteBatch};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[test]
fn test_comprehensive_database_operations() {
    let config = LightningDbConfig {
        page_size: 4096,
        cache_size: 1024 * 1024 * 10, // 10MB
        compression_enabled: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Test 1: Basic CRUD operations
    println!("Test 1: Basic CRUD operations");
    db.put(b"key1", b"value1").unwrap();
    let val = db.get(b"key1").unwrap().unwrap();
    assert_eq!(val, b"value1");

    db.put(b"key2", b"value2").unwrap();
    db.put(b"key3", b"value3").unwrap();

    // Test 2: Update existing key
    println!("Test 2: Update operations");
    db.put(b"key1", b"updated_value1").unwrap();
    let val = db.get(b"key1").unwrap().unwrap();
    assert_eq!(val, b"updated_value1");

    // Test 3: Delete operations
    println!("Test 3: Delete operations");
    db.delete(b"key2").unwrap();
    assert!(db.get(b"key2").unwrap().is_none());

    // Test 4: Batch operations
    println!("Test 4: Batch operations");
    let mut batch = WriteBatch::new();
    batch.put(&b"batch_key1"[..], &b"batch_value1"[..]);
    batch.put(&b"batch_key2"[..], &b"batch_value2"[..]);
    batch.put(&b"batch_key3"[..], &b"batch_value3"[..]);
    batch.delete(&b"key3"[..]);
    db.write_batch(&batch).unwrap();

    assert_eq!(db.get(b"batch_key1").unwrap().unwrap(), b"batch_value1");
    assert_eq!(db.get(b"batch_key2").unwrap().unwrap(), b"batch_value2");
    assert!(db.get(b"key3").unwrap().is_none());

    // Test 5: Transaction operations
    println!("Test 5: Transaction operations");
    let tx = db.begin_transaction().unwrap();
    db.put_tx(tx, b"tx_key1", b"tx_value1").unwrap();
    db.put_tx(tx, b"tx_key2", b"tx_value2").unwrap();
    db.commit_transaction(tx).unwrap();

    assert_eq!(db.get(b"tx_key1").unwrap().unwrap(), b"tx_value1");
    assert_eq!(db.get(b"tx_key2").unwrap().unwrap(), b"tx_value2");

    // Test 6: Transaction rollback
    println!("Test 6: Transaction rollback");
    let tx = db.begin_transaction().unwrap();
    db.put_tx(tx, b"rollback_key", b"rollback_value").unwrap();
    db.abort_transaction(tx).unwrap();
    assert!(db.get(b"rollback_key").unwrap().is_none());

    // Test 7: Large value handling
    println!("Test 7: Large value handling");
    let large_value = vec![0u8; 3000];
    db.put(b"large_key", &large_value).unwrap();
    let retrieved = db.get(b"large_key").unwrap().unwrap();
    assert_eq!(retrieved.len(), 3000);

    // Test 8: Iterator/Scan operations
    println!("Test 8: Iterator operations");
    db.put(b"iter_1", b"value1").unwrap();
    db.put(b"iter_2", b"value2").unwrap();
    db.put(b"iter_3", b"value3").unwrap();

    let iterator = db.scan(Some(b"iter_"), Some(b"iter_9")).unwrap();
    let mut count = 0;
    for result in iterator {
        let (key, _value) = result.unwrap();
        assert!(key.starts_with(b"iter_"));
        count += 1;
    }
    assert_eq!(count, 3);

    // Test 9: Prefix scan
    println!("Test 9: Prefix scan");
    db.put(b"prefix_a_1", b"val1").unwrap();
    db.put(b"prefix_a_2", b"val2").unwrap();
    db.put(b"prefix_b_1", b"val3").unwrap();

    let prefix_iter = db.scan_prefix(b"prefix_a").unwrap();
    let mut prefix_count = 0;
    for result in prefix_iter {
        let (key, _) = result.unwrap();
        assert!(key.starts_with(b"prefix_a"));
        prefix_count += 1;
    }
    assert_eq!(prefix_count, 2);

    // Test 10: Concurrent operations
    println!("Test 10: Concurrent operations");
    let db_arc = Arc::new(db);
    let mut handles = vec![];

    for i in 0..5 {
        let db_clone = Arc::clone(&db_arc);
        let handle = thread::spawn(move || {
            for j in 0..20 {
                let key = format!("thread_{}_key_{}", i, j);
                let value = format!("thread_{}_value_{}", i, j);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all concurrent writes
    for i in 0..5 {
        for j in 0..20 {
            let key = format!("thread_{}_key_{}", i, j);
            let expected_value = format!("thread_{}_value_{}", i, j);
            let actual = db_arc.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(actual, expected_value.as_bytes());
        }
    }

    println!("All comprehensive tests passed!");
}

#[test]
fn test_performance_benchmarks() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("bench.db");
    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Benchmark 1: Sequential writes
    println!("Benchmark: Sequential writes");
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("seq_key_{:06}", i);
        let value = format!("seq_value_{:06}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let duration = start.elapsed();
    let ops_per_sec = 1000.0 / duration.as_secs_f64();
    println!("  Sequential writes: {:.0} ops/sec", ops_per_sec);

    // Benchmark 2: Random reads
    println!("Benchmark: Random reads");
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("seq_key_{:06}", i % 1000);
        let _ = db.get(key.as_bytes()).unwrap();
    }
    let duration = start.elapsed();
    let ops_per_sec = 1000.0 / duration.as_secs_f64();
    println!("  Random reads: {:.0} ops/sec", ops_per_sec);

    // Benchmark 3: Batch writes
    println!("Benchmark: Batch writes");
    let start = Instant::now();
    let mut batch = WriteBatch::new();
    for i in 0..1000 {
        let key = format!("batch_key_{:06}", i);
        let value = format!("batch_value_{:06}", i);
        batch.put(key.as_bytes(), value.as_bytes());
    }
    db.write_batch(&batch).unwrap();
    let duration = start.elapsed();
    let ops_per_sec = 1000.0 / duration.as_secs_f64();
    println!("  Batch writes: {:.0} ops/sec", ops_per_sec);

    // Benchmark 4: Mixed workload
    println!("Benchmark: Mixed workload");
    let start = Instant::now();
    for i in 0..1000 {
        if i % 3 == 0 {
            let key = format!("mixed_key_{:06}", i);
            let value = format!("mixed_value_{:06}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        } else if i % 3 == 1 {
            let key = format!("mixed_key_{:06}", i / 2);
            let _ = db.get(key.as_bytes());
        } else {
            let key = format!("mixed_key_{:06}", i / 4);
            db.delete(key.as_bytes()).ok();
        }
    }
    let duration = start.elapsed();
    let ops_per_sec = 1000.0 / duration.as_secs_f64();
    println!("  Mixed workload: {:.0} ops/sec", ops_per_sec);

    println!("All benchmarks completed!");
}

#[test]
fn test_edge_cases() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("edge.db");
    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Test empty key/value
    println!("Test: Empty key handling");
    assert!(db.put(b"", b"empty_key_value").is_ok());

    println!("Test: Empty value handling");
    assert!(db.put(b"empty_value_key", b"").is_ok());

    // Test special characters
    println!("Test: Special characters");
    db.put(b"special\x00\x01\xff", b"value\x00\x01\xff").unwrap();
    let val = db.get(b"special\x00\x01\xff").unwrap().unwrap();
    assert_eq!(val, b"value\x00\x01\xff");

    // Test very long key (within limits)
    println!("Test: Long keys");
    let long_key = vec![b'k'; 1000];
    let long_value = vec![b'v'; 1000];
    db.put(&long_key, &long_value).unwrap();
    assert_eq!(db.get(&long_key).unwrap().unwrap(), long_value);

    // Test non-existent key
    println!("Test: Non-existent keys");
    assert!(db.get(b"non_existent_key").unwrap().is_none());

    // Test double delete
    println!("Test: Double delete");
    db.put(b"delete_test", b"value").unwrap();
    db.delete(b"delete_test").unwrap();
    assert!(db.delete(b"delete_test").is_ok()); // Should not error

    // Test overwrite in same transaction
    println!("Test: Overwrite in transaction");
    let tx = db.begin_transaction().unwrap();
    db.put_tx(tx, b"tx_overwrite", b"value1").unwrap();
    db.put_tx(tx, b"tx_overwrite", b"value2").unwrap();
    db.commit_transaction(tx).unwrap();
    assert_eq!(db.get(b"tx_overwrite").unwrap().unwrap(), b"value2");

    println!("All edge case tests passed!");
}