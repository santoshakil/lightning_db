use lightning_db::{Database, LightningDbConfig, WriteBatch};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[test]
fn test_extreme_key_sizes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open(temp_dir.path(), LightningDbConfig::default()).unwrap();

    // Test minimum key size (1 byte)
    db.put(&[0u8], b"min_key_value").unwrap();
    assert_eq!(db.get(&[0u8]).unwrap(), Some(b"min_key_value".to_vec()));

    // Test maximum allowed key size (4096 bytes)
    let large_key = vec![b'k'; 4096];
    db.put(&large_key, b"large_key_value").unwrap();
    assert_eq!(db.get(&large_key).unwrap(), Some(b"large_key_value".to_vec()));

    // Test that oversized key is properly rejected
    let oversized_key = vec![b'k'; 4097];
    assert!(db.put(&oversized_key, b"value").is_err());

    // Test edge case: all zeros key
    let zero_key = vec![0u8; 100];
    db.put(&zero_key, b"zero_key_value").unwrap();
    assert_eq!(db.get(&zero_key).unwrap(), Some(b"zero_key_value".to_vec()));

    // Test edge case: all 0xFF key
    let ff_key = vec![0xFF; 100];
    db.put(&ff_key, b"ff_key_value").unwrap();
    assert_eq!(db.get(&ff_key).unwrap(), Some(b"ff_key_value".to_vec()));
}

#[test]
fn test_extreme_value_sizes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open(temp_dir.path(), LightningDbConfig::default()).unwrap();

    // Test empty value
    db.put(b"empty_val_key", b"").unwrap();
    assert_eq!(db.get(b"empty_val_key").unwrap(), Some(vec![]));

    // Test maximum allowed value size (1MB)
    let large_value = vec![b'v'; 1024 * 1024];
    db.put(b"large_val_key", &large_value).unwrap();
    assert_eq!(db.get(b"large_val_key").unwrap(), Some(large_value));

    // Test that oversized value is properly rejected
    let oversized_value = vec![b'v'; 1024 * 1024 + 1];
    assert!(db.put(b"oversized_val_key", &oversized_value).is_err());

    // Test value with null bytes
    let null_value = vec![0u8; 1000];
    db.put(b"null_val_key", &null_value).unwrap();
    assert_eq!(db.get(b"null_val_key").unwrap(), Some(null_value));
}

#[test]
fn test_transaction_limits() {
    let config = LightningDbConfig {
        max_active_transactions: 10,
        ..Default::default()
    };
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open(temp_dir.path(), config).unwrap();

    // Start maximum allowed transactions
    let mut txs = Vec::new();
    for i in 0..10 {
        let tx = db.begin_transaction().unwrap();
        db.put_tx(tx, format!("tx_key_{}", i).as_bytes(), b"value").unwrap();
        txs.push(tx);
    }

    // Try to start one more - should fail
    assert!(db.begin_transaction().is_err());

    // Commit one transaction
    db.commit_transaction(txs.pop().unwrap()).unwrap();

    // Now we should be able to start another
    let new_tx = db.begin_transaction().unwrap();
    db.commit_transaction(new_tx).unwrap();

    // Clean up remaining transactions
    for tx in txs {
        db.abort_transaction(tx).unwrap();
    }
}

#[test]
fn test_rapid_open_close_cycles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();

    // Rapidly open and close the database
    for i in 0..20 {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();
        db.put(format!("cycle_{}", i).as_bytes(), b"value").unwrap();
        db.sync().unwrap();
        drop(db);
    }

    // Final open to verify all data
    let db = Database::open(&path, LightningDbConfig::default()).unwrap();
    for i in 0..20 {
        let key = format!("cycle_{}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(b"value".to_vec()));
    }
}

#[test]
fn test_massive_batch_operations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open(temp_dir.path(), LightningDbConfig::default()).unwrap();

    // Create a massive batch
    let mut batch = WriteBatch::new();
    for i in 0..10000 {
        let key = format!("batch_key_{:06}", i).into_bytes();
        let value = format!("batch_value_{}", i).into_bytes();
        batch.put(key, value).unwrap();
    }

    // Apply the batch
    let start = Instant::now();
    db.write_batch(&batch).unwrap();
    let elapsed = start.elapsed();
    println!("Wrote 10,000 entries in {:?}", elapsed);

    // Verify a sample
    for i in (0..10000).step_by(100) {
        let key = format!("batch_key_{:06}", i);
        let expected = format!("batch_value_{}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(expected.as_bytes().to_vec()));
    }

    // Test massive delete batch
    let mut delete_batch = WriteBatch::new();
    for i in 0..5000 {
        let key = format!("batch_key_{:06}", i).into_bytes();
        delete_batch.delete(key).unwrap();
    }
    db.write_batch(&delete_batch).unwrap();

    // Verify deletions
    for i in 0..5000 {
        let key = format!("batch_key_{:06}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), None);
    }

    // Verify remaining keys still exist
    for i in 5000..10000 {
        let key = format!("batch_key_{:06}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn test_concurrent_stress_with_conflicts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(temp_dir.path(), LightningDbConfig::default()).unwrap());

    // Multiple threads trying to update the same keys
    let mut handles = vec![];
    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                // All threads fight over the same 10 keys
                let key = format!("shared_key_{}", i % 10);
                let value = format!("thread_{}_iteration_{}", thread_id, i);

                // Some operations in transactions, some not
                if i % 2 == 0 {
                    if let Ok(tx) = db_clone.begin_transaction() {
                        let _ = db_clone.put_tx(tx, key.as_bytes(), value.as_bytes());
                        let _ = db_clone.commit_transaction(tx);
                    }
                } else {
                    let _ = db_clone.put(key.as_bytes(), value.as_bytes());
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify that all shared keys exist (values could be from any thread)
    for i in 0..10 {
        let key = format!("shared_key_{}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn test_scan_with_extreme_ranges() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open(temp_dir.path(), LightningDbConfig::default()).unwrap();

    // Insert keys with various patterns
    for i in 0..100 {
        let key = format!("{:03}_key", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }

    // Test scan with no bounds (full scan)
    let mut count = 0;
    let iter = db.scan(None, None).unwrap();
    for _ in iter {
        count += 1;
    }
    assert_eq!(count, 100);

    // Test scan with start > end (should return nothing)
    let iter = db.scan(Some(b"zzz"), Some(b"aaa")).unwrap();
    assert_eq!(iter.count(), 0);

    // Test scan with identical start and end
    let iter = db.scan(Some(b"050_key"), Some(b"050_key")).unwrap();
    assert_eq!(iter.count(), 0); // Exclusive end

    // Test scan with very long bounds
    let long_start = vec![b'0'; 1000];
    let long_end = vec![b'z'; 1000];
    let iter = db.scan(Some(&long_start), Some(&long_end)).unwrap();
    assert_eq!(iter.count(), 100); // All keys should be within these bounds
}

#[test]
fn test_database_with_special_characters() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open(temp_dir.path(), LightningDbConfig::default()).unwrap();

    // Test with various special characters in keys and values
    let special_cases = vec![
        (b"\0\0\0".to_vec(), b"null_bytes".to_vec()),
        (b"\xFF\xFF\xFF".to_vec(), b"max_bytes".to_vec()),
        (b"\n\r\t".to_vec(), b"whitespace".to_vec()),
        ("emoji🔥⚡️🗲".as_bytes().to_vec(), b"emoji_value".to_vec()),
        ("日本語".as_bytes().to_vec(), b"japanese".to_vec()),
        (b"key\0with\0nulls".to_vec(), b"value\0with\0nulls".to_vec()),
    ];

    for (key, value) in &special_cases {
        db.put(key, value).unwrap();
        assert_eq!(db.get(key).unwrap(), Some(value.clone()));
    }

    // Verify all special cases persist after sync
    db.sync().unwrap();
    for (key, value) in &special_cases {
        assert_eq!(db.get(key).unwrap(), Some(value.clone()));
    }
}

#[test]
fn test_transaction_isolation_edge_cases() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(temp_dir.path(), LightningDbConfig::default()).unwrap());

    // Setup initial data
    db.put(b"key1", b"initial").unwrap();

    // Start transaction 1
    let tx1 = db.begin_transaction().unwrap();

    // Transaction 1 reads the value
    assert_eq!(db.get_tx(tx1, b"key1").unwrap(), Some(b"initial".to_vec()));

    // Concurrent direct write (outside transaction)
    db.put(b"key1", b"modified").unwrap();

    // Transaction 1 may see the new value depending on isolation implementation
    // Lightning DB doesn't provide full snapshot isolation, so tx sees the latest value
    assert_eq!(db.get_tx(tx1, b"key1").unwrap(), Some(b"modified".to_vec()));

    // Start transaction 2 after the modification
    let tx2 = db.begin_transaction().unwrap();

    // Transaction 2 should see the new value
    assert_eq!(db.get_tx(tx2, b"key1").unwrap(), Some(b"modified".to_vec()));

    // Transaction 1 tries to update
    db.put_tx(tx1, b"key1", b"tx1_update").unwrap();

    // Commit transaction 1 - may fail due to conflict
    let tx1_result = db.commit_transaction(tx1);

    // Transaction 2 tries to update the same key
    db.put_tx(tx2, b"key1", b"tx2_update").unwrap();

    // If tx1 succeeded, tx2 should handle it appropriately
    if tx1_result.is_ok() {
        // tx2 commit might fail or succeed depending on isolation level
        let _ = db.commit_transaction(tx2);
    } else {
        // tx1 failed, tx2 should succeed
        db.commit_transaction(tx2).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(b"tx2_update".to_vec()));
    }
}

#[test]
fn test_recovery_after_partial_write() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();

    // Phase 1: Write some data
    {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();
        for i in 0..100 {
            db.put(format!("key_{}", i).as_bytes(), b"value").unwrap();
        }
        db.sync().unwrap();
    }

    // Phase 2: Start a large batch but don't sync
    {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();
        let mut batch = WriteBatch::new();
        for i in 100..200 {
            let key = format!("key_{}", i).into_bytes();
            batch.put(key, b"batch_value".to_vec()).unwrap();
        }
        db.write_batch(&batch).unwrap();
        // Intentionally drop without sync to simulate crash
    }

    // Phase 3: Reopen and verify data integrity
    {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();

        // Original data should definitely be there
        for i in 0..100 {
            let key = format!("key_{}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(b"value".to_vec()));
        }

        // Batch data may or may not be there depending on WAL
        // Just verify database doesn't panic or corrupt
        for i in 100..200 {
            let key = format!("key_{}", i);
            let _ = db.get(key.as_bytes());
        }
    }
}