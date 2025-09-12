use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::collections::HashMap;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use tempfile::tempdir;

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
            wal_sync_mode: WalSyncMode::Sync,
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
    }
    
    // Phase 3: Final verification
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
        
        let mut count = 0;
        let iter = db.scan(None, None).unwrap();
        for result in iter {
            if result.is_ok() {
                count += 1;
            }
        }
        assert_eq!(count, 500);
    }
}

#[test]
fn test_transaction_isolation_levels() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(
        dir.path(),
        LightningDbConfig {
            max_active_transactions: 10,
            ..Default::default()
        },
    ).unwrap());
    
    // Prepare test data
    db.put(b"shared_key", b"initial_value").unwrap();
    
    let db1 = db.clone();
    let db2 = db.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();
    
    let handle1 = thread::spawn(move || {
        let tx1 = db1.begin_transaction().unwrap();
        
        // Read initial value
        let val = db1.get_tx(tx1, b"shared_key").unwrap();
        assert_eq!(val, Some(b"initial_value".to_vec()));
        
        // Signal ready
        b1.wait();
        
        // Try to read again after other transaction modified
        b1.wait();
        
        let val = db1.get_tx(tx1, b"shared_key").unwrap();
        // Should still see initial value (isolation)
        assert_eq!(val, Some(b"initial_value".to_vec()));
        
        db1.commit_transaction(tx1).unwrap();
    });
    
    let handle2 = thread::spawn(move || {
        // Wait for first transaction to start
        b2.wait();
        
        let tx2 = db2.begin_transaction().unwrap();
        db2.put_tx(tx2, b"shared_key", b"modified_value").unwrap();
        db2.commit_transaction(tx2).unwrap();
        
        // Signal modification complete
        b2.wait();
    });
    
    handle1.join().unwrap();
    handle2.join().unwrap();
    
    // Verify final state
    let final_val = db.get(b"shared_key").unwrap();
    assert_eq!(final_val, Some(b"modified_value".to_vec()));
}

#[test]
fn test_concurrent_transactions_stress() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(
        dir.path(),
        LightningDbConfig {
            max_active_transactions: 100,
            ..Default::default()
        },
    ).unwrap());
    
    let num_threads = 10;
    let ops_per_thread = 50;
    let results = Arc::new(Mutex::new(HashMap::new()));
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = db.clone();
            let results = results.clone();
            
            thread::spawn(move || {
                for op_id in 0..ops_per_thread {
                    let tx = db.begin_transaction().unwrap();
                    
                    let key = format!("thread_{}_op_{}", thread_id, op_id);
                    let value = format!("value_{}_{}", thread_id, op_id);
                    
                    db.put_tx(tx, key.as_bytes(), value.as_bytes()).unwrap();
                    
                    // Read within transaction to verify
                    let read_val = db.get_tx(tx, key.as_bytes()).unwrap();
                    assert_eq!(read_val, Some(value.as_bytes().to_vec()));
                    
                    db.commit_transaction(tx).unwrap();
                    
                    // Track successful commits
                    let mut res = results.lock().unwrap();
                    res.insert(key, value);
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all operations succeeded
    let results = results.lock().unwrap();
    assert_eq!(results.len(), num_threads * ops_per_thread);
    
    // Verify data persistence
    for (key, value) in results.iter() {
        let stored = db.get(key.as_bytes()).unwrap();
        assert_eq!(stored, Some(value.as_bytes().to_vec()));
    }
}

#[test]
fn test_edge_cases() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Empty key - may not be supported by all implementations
    let empty_key_result = db.put(b"", b"value");
    if empty_key_result.is_ok() {
        assert_eq!(db.get(b"").unwrap(), Some(b"value".to_vec()));
    }
    
    // Empty value
    assert!(db.put(b"empty_val", b"").is_ok());
    assert_eq!(db.get(b"empty_val").unwrap(), Some(b"".to_vec()));
    
    // Very large key
    let large_key = vec![b'k'; 10000];
    assert!(db.put(&large_key, b"value").is_ok());
    assert_eq!(db.get(&large_key).unwrap(), Some(b"value".to_vec()));
    
    // Very large value
    let large_value = vec![b'v'; 1_000_000];
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
    assert!(db.put(unicode_key.as_bytes(), unicode_val.as_bytes()).is_ok());
    assert_eq!(
        db.get(unicode_key.as_bytes()).unwrap(),
        Some(unicode_val.as_bytes().to_vec())
    );
}

#[test]
fn test_recovery_after_partial_write() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    
    // Create database and start transaction
    {
        let db = Database::create(
            &db_path,
            LightningDbConfig {
                use_unified_wal: true,
                wal_sync_mode: WalSyncMode::Sync,
                ..Default::default()
            },
        ).unwrap();
        
        // Write some committed data
        for i in 0..100 {
            let key = format!("committed_{}", i);
            db.put(key.as_bytes(), b"committed").unwrap();
        }
        
        // Start transaction but don't commit
        let tx = db.begin_transaction().unwrap();
        for i in 0..50 {
            let key = format!("uncommitted_{}", i);
            db.put_tx(tx, key.as_bytes(), b"uncommitted").unwrap();
        }
        // Transaction automatically aborted when db drops
    }
    
    // Reopen and verify
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
        
        // Committed data should exist
        for i in 0..100 {
            let key = format!("committed_{}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(b"committed".to_vec()));
        }
        
        // Uncommitted data should not exist
        for i in 0..50 {
            let key = format!("uncommitted_{}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), None);
        }
    }
}

#[test]
fn test_batch_operations_atomicity() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Prepare batch
    let mut batch = Vec::new();
    for i in 0..100 {
        let key = format!("batch_{:03}", i);
        let value = format!("value_{}", i);
        batch.push((key.into_bytes(), value.into_bytes()));
    }
    
    // Insert batch
    db.batch_put(&batch).unwrap();
    
    // Verify all or nothing
    for i in 0..100 {
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

#[test]
fn test_iterator_consistency() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Insert ordered data
    let mut expected = Vec::new();
    for i in 0..100 {
        let key = format!("iter_{:03}", i);
        let value = format!("value_{}", i);
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
    assert!(results.len() >= 10); // iter_000 through iter_099
}