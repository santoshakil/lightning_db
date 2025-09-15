use lightning_db::{Database, Key, LightningDbConfig, WriteBatch};
use tempfile::tempdir;

mod common;
use common::*;

#[test]
fn test_complete_crud_operations() {
    let test_db = TestDatabase::new_fast();
    let db = &test_db.db;

    // Create
    for i in 0..FAST_TEST_SIZE { // Use common constant
        let key = format!("key_{:04}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Read
    for i in 0..FAST_TEST_SIZE { // Use common constant
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, Some(format!("value_{}", i).into_bytes()));
    }

    // Update
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = format!("updated_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify updates
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, Some(format!("updated_value_{}", i).into_bytes()));
    }

    // Delete
    for i in 25..75 {
        let key = format!("key_{:04}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    // Verify deletes
    for i in 0..FAST_TEST_SIZE { // Use common constant
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap();

        if i < 25 {
            assert_eq!(value, Some(format!("updated_value_{}", i).into_bytes()));
        } else if i < 75 {
            assert_eq!(value, None);
        } else {
            assert_eq!(value, Some(format!("value_{}", i).into_bytes()));
        }
    }
}

#[test]
fn test_transaction_isolation_levels() {
    let test_db = TestDatabase::new_fast();
    let db = &test_db.db;

    // Setup initial data
    db.put(b"key1", b"value1").unwrap();
    db.put(b"key2", b"value2").unwrap();

    // Test transaction isolation
    let tx1 = db.begin_transaction().unwrap();
    let tx2 = db.begin_transaction().unwrap();

    // tx1 modifies key1
    db.put_tx(tx1, b"key1", b"tx1_value").unwrap();

    // tx2 shouldn't see uncommitted changes
    let value = db.get_tx(tx2, b"key1").unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));

    // Commit tx1 and tx2
    let _ = db.commit_transaction(tx1);
    let _ = db.commit_transaction(tx2);

    // Test that transactions work with non-conflicting writes
    let tx3 = db.begin_transaction().unwrap();
    db.put_tx(tx3, b"key3", b"value3").unwrap();
    db.commit_transaction(tx3).unwrap();

    // Verify the non-conflicting write succeeded
    assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
}

#[test]
fn test_batch_operations() {
    let test_db = TestDatabase::new_fast();
    let db = &test_db.db;

    // Test batch put
    let mut batch = WriteBatch::new();
    for i in 0..1000 {
        let key = format!("batch_key_{:04}", i);
        let value = format!("batch_value_{}", i);
        batch.put(key.into_bytes(), value.into_bytes()).unwrap();
    }

    db.write_batch(&batch).unwrap();

    // Verify all batch writes
    for i in 0..1000 {
        let key = format!("batch_key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, Some(format!("batch_value_{}", i).into_bytes()));
    }

    // Test batch delete
    let mut batch = WriteBatch::new();
    for i in 500..1000 {
        let key = format!("batch_key_{:04}", i);
        batch.delete(key.into_bytes()).unwrap();
    }

    db.write_batch(&batch).unwrap();

    // Verify deletes
    for i in 0..1000 {
        let key = format!("batch_key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap();

        if i < 500 {
            assert_eq!(value, Some(format!("batch_value_{}", i).into_bytes()));
        } else {
            assert_eq!(value, None);
        }
    }

    // Test mixed batch operations
    let mut batch = WriteBatch::new();
    for i in 0..250 {
        let key = format!("batch_key_{:04}", i);
        batch.delete(key.into_bytes()).unwrap();
    }
    for i in 1000..1250 {
        let key = format!("batch_key_{:04}", i);
        let value = format!("new_batch_value_{}", i);
        batch.put(key.into_bytes(), value.into_bytes()).unwrap();
    }

    db.write_batch(&batch).unwrap();

    // Verify mixed operations
    assert_eq!(db.get(b"batch_key_0000").unwrap(), None);
    assert_eq!(
        db.get(b"batch_key_0250").unwrap(),
        Some(b"batch_value_250".to_vec())
    );
    assert_eq!(
        db.get(b"batch_key_1000").unwrap(),
        Some(b"new_batch_value_1000".to_vec())
    );
}

#[test]
fn test_range_and_prefix_scans() {
    let test_db = TestDatabase::new_fast();
    let db = &test_db.db;

    // Insert test data with different prefixes
    for prefix in &["alpha", "beta", "gamma"] {
        for i in 0..20 {
            let key = format!("{}_{:02}", prefix, i);
            let value = format!("value_{}_{}", prefix, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Test prefix scan
    let alpha_keys = db.prefix_scan_key(&Key::from(b"alpha".to_vec())).unwrap();
    assert_eq!(alpha_keys.len(), 20);

    let beta_keys = db.prefix_scan_key(&Key::from(b"beta".to_vec())).unwrap();
    assert_eq!(beta_keys.len(), 20);

    // Test range scan
    let range = db.range(Some(b"alpha_10"), Some(b"beta_10")).unwrap();
    assert!(range.len() > 10);
    assert!(range.len() < 40);

    // Verify range boundaries
    let first_key = String::from_utf8_lossy(&range[0].0);
    let last_key = String::from_utf8_lossy(&range[range.len() - 1].0);
    assert!(first_key.as_ref() >= "alpha_10");
    assert!(last_key.as_ref() < "beta_10");

    // Test open-ended range
    let all_data = db.range(None, None).unwrap();
    assert_eq!(all_data.len(), 60);
}

#[test]
fn test_persistence_and_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Write data and close
    {
        let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

        for i in 0..FAST_TEST_SIZE { // Use common constant
            let key = format!("persist_key_{:03}", i);
            let value = format!("persist_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Force checkpoint
        db.checkpoint().unwrap();
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

        for i in 0..FAST_TEST_SIZE { // Use common constant
            let key = format!("persist_key_{:03}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(format!("persist_value_{}", i).into_bytes()));
        }

        // Add more data
        for i in 100..150 {
            let key = format!("persist_key_{:03}", i);
            let value = format!("persist_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Phase 3: Final verification
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

        for i in 0..150 {
            let key = format!("persist_key_{:03}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(format!("persist_value_{}", i).into_bytes()));
        }
    }
}

#[test]
fn test_large_data_handling() {
    let test_db = TestDatabase::new_fast();
    let db = &test_db.db;

    // Test various sizes
    let sizes = vec![
        1024,       // 1 KB
        10 * 1024,  // 10 KB
        100 * 1024, // 100 KB
        512 * 1024, // 512 KB
    ];

    for (i, size) in sizes.iter().enumerate() {
        let key = format!("large_{}", i);
        let value = vec![b'X'; *size];

        // Write large value
        db.put(key.as_bytes(), &value).unwrap();

        // Read and verify
        let retrieved = db.get(key.as_bytes()).unwrap();
        assert_eq!(retrieved, Some(value));
    }

    // Test batch with large values
    let mut batch = WriteBatch::new();
    for i in 0..10 {
        let key = format!("batch_large_{}", i);
        let value = vec![b'Y'; 50 * 1024]; // 50 KB each
        batch.put(key.into_bytes(), value).unwrap();
    }

    db.write_batch(&batch).unwrap();

    // Verify batch writes
    for i in 0..10 {
        let key = format!("batch_large_{}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert_eq!(value.unwrap().len(), 50 * 1024);
    }
}

#[test]
fn test_database_full_lifecycle() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Create and populate database
    {
        let config = LightningDbConfig {
            cache_size: 10 * 1024 * 1024,
            compression_enabled: true,
            use_unified_wal: true,
            wal_sync_mode: lightning_db::WalSyncMode::Sync,
            ..Default::default()
        };

        let db = Database::create(&db_path, config).unwrap();

        // Insert various data patterns
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let value = vec![i as u8; 100];
            db.put(key.as_bytes(), &value).unwrap();
        }

        // Verify data
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().len(), 100);
        }
    }

    // Phase 2: Reopen and modify
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

        // Delete half the data
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            db.delete(key.as_bytes()).unwrap();
        }

        // Verify deletions
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_none());
        }

        // Verify remaining data
        for i in 500..1000 {
            let key = format!("key_{:04}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_some());
        }

        // Explicitly sync before closing
        db.sync().unwrap();
    }

    // Phase 3: Final verification
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

        let mut count = 0;
        let mut deleted_found = 0;
        let iter = db.scan(None, None).unwrap();
        for result in iter {
            if let Ok((key, _value)) = result {
                count += 1;
                // Check if this is a key that should have been deleted
                let key_str = String::from_utf8_lossy(&key);
                if let Some(num_str) = key_str.strip_prefix("key_") {
                    if let Ok(num) = num_str.parse::<usize>() {
                        if num < 500 {
                            deleted_found += 1;
                            println!("Found deleted key: {}", key_str);
                        }
                    }
                }
            }
        }
        println!(
            "Total count: {}, deleted keys found: {}",
            count, deleted_found
        );
        assert_eq!(
            deleted_found, 0,
            "Found {} keys that should have been deleted",
            deleted_found
        );
        assert_eq!(count, 500);
    }
}

#[test]
fn test_iterator_consistency() {
    let test_db = TestDatabase::new_fast();
    let db = &test_db.db;

    // Insert ordered data
    let mut expected = Vec::new();
    for i in 0..FAST_TEST_SIZE { // Use common constant
        let key = format!("iter_{:03}", i);
        let value = format!("value_{:03}", i); // Use consistent formatting
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        expected.push((key, value));
    }

    // Full scan
    let iter = db.scan(None, None).unwrap();
    let mut count = 0;
    for result in iter {
        let (key, value) = result.unwrap();
        if let Some(key_str) = std::str::from_utf8(&key).ok() {
            if key_str.starts_with("iter_") {
                count += 1;
                let expected_val = format!("value_{}", &key_str[5..]);
                assert_eq!(value, expected_val.as_bytes());
            }
        }
    }
    assert_eq!(count, 100);

    // Range scan
    let start = b"iter_020";
    let end = b"iter_030";
    let iter = db.scan(Some(start.to_vec()), Some(end.to_vec())).unwrap();
    let results: Vec<_> = iter.collect();
    assert!(results.len() >= 10);

    // Prefix scan
    let iter = db.scan_prefix(b"iter_0").unwrap();
    let results: Vec<_> = iter.collect();
    assert!(results.len() >= 10);
}

#[test]
fn test_edge_cases() {
    let test_db = TestDatabase::new_fast();
    let db = &test_db.db;

    // Empty value
    assert!(db.put(b"empty_val", b"").is_ok());
    assert_eq!(db.get(b"empty_val").unwrap(), Some(b"".to_vec()));

    // Very large key
    let large_key = vec![b'k'; 1000];
    assert!(db.put(&large_key, b"value").is_ok());
    assert_eq!(db.get(&large_key).unwrap(), Some(b"value".to_vec()));

    // Very large value
    let large_value = vec![b'v'; 100_000];
    assert!(db.put(b"large", &large_value).is_ok());
    assert_eq!(db.get(b"large").unwrap(), Some(large_value));

    // Binary data with all byte values
    let binary_key: Vec<u8> = (0..=255).collect();
    let binary_val: Vec<u8> = (0..=255).rev().collect();
    assert!(db.put(&binary_key, &binary_val).is_ok());
    assert_eq!(db.get(&binary_key).unwrap(), Some(binary_val));

    // Unicode strings
    let unicode_key = "🔑_कुंजी_ключ_鍵";
    let unicode_val = "📝_मूल्य_значение_値";
    assert!(db
        .put(unicode_key.as_bytes(), unicode_val.as_bytes())
        .is_ok());
    assert_eq!(
        db.get(unicode_key.as_bytes()).unwrap(),
        Some(unicode_val.as_bytes().to_vec())
    );
}

#[test]
fn test_batch_operations_atomicity() {
    let test_db = TestDatabase::new_fast();
    let db = &test_db.db;

    // Prepare batch
    let mut batch = Vec::new();
    for i in 0..FAST_TEST_SIZE { // Use common constant
        let key = format!("batch_{:03}", i);
        let value = format!("value_{}", i);
        batch.push((key.into_bytes(), value.into_bytes()));
    }

    // Insert batch
    db.batch_put(&batch).unwrap();

    // Verify all or nothing
    for i in 0..FAST_TEST_SIZE { // Use common constant
        let key = format!("batch_{:03}", i);
        let expected = format!("value_{}", i);
        let result = db.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(expected.into_bytes()));
    }

    // Prepare keys for batch delete
    let keys: Vec<_> = (0..100)
        .map(|i| format!("batch_{:03}", i).into_bytes())
        .collect();

    // Delete batch
    db.delete_batch(&keys).unwrap();

    // Verify all deleted
    for key in &keys {
        assert_eq!(db.get(key).unwrap(), None);
    }
}
