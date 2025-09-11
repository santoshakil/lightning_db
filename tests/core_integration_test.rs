use lightning_db::{Database, LightningDbConfig, Key, WriteBatch};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_complete_crud_operations() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Create
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Read
    for i in 0..100 {
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
    for i in 0..100 {
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
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
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
fn test_concurrent_transactions() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    let barrier = Arc::new(Barrier::new(10));
    
    // Setup initial counter
    db.put(b"counter", b"0").unwrap();
    
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            
            thread::spawn(move || {
                barrier.wait();
                
                for _ in 0..10 {
                    let tx = db.begin_transaction().unwrap();
                    
                    // Read current value
                    let current = db.get_tx(tx, b"counter").unwrap()
                        .map(|v| String::from_utf8_lossy(&v).parse::<i32>().unwrap_or(0))
                        .unwrap_or(0);
                    
                    // Increment
                    let new_value = (current + 1).to_string();
                    db.put_tx(tx, b"counter", new_value.as_bytes()).unwrap();
                    
                    // Add thread-specific key
                    let thread_key = format!("thread_{}_op_{}", i, current);
                    db.put_tx(tx, thread_key.as_bytes(), b"done").unwrap();
                    
                    // Try to commit, ignore conflicts
                    let _ = db.commit_transaction(tx);
                }
            })
        })
        .collect();
    
    for handle in handles {
        let _ = handle.join();
    }
    
    // Verify all operations completed
    let mut count = 0;
    for i in 0..10 {
        for j in 0..100 {
            let key = format!("thread_{}_op_{}", i, j);
            if db.get(key.as_bytes()).unwrap().is_some() {
                count += 1;
            }
        }
    }
    
    // At least some operations should have succeeded
    assert!(count > 0);
}

#[test]
fn test_batch_operations() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
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
    assert_eq!(db.get(b"batch_key_0250").unwrap(), Some(b"batch_value_250".to_vec()));
    assert_eq!(db.get(b"batch_key_1000").unwrap(), Some(b"new_batch_value_1000".to_vec()));
}

#[test]
fn test_range_and_prefix_scans() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
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
        
        for i in 0..100 {
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
        
        for i in 0..100 {
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
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test various sizes
    let sizes = vec![
        1024,           // 1 KB
        10 * 1024,      // 10 KB
        100 * 1024,     // 100 KB
        512 * 1024,     // 512 KB
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
fn test_performance_under_load() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    // Concurrent write load
    let write_start = Instant::now();
    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..2500 {
                    let key = format!("thread_{}_key_{:04}", thread_id, i);
                    let value = format!("value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            })
        })
        .collect();
    
    for handle in handles {
        let _ = handle.join();
    }
    
    let write_duration = write_start.elapsed();
    let write_ops = 10000;
    let write_ops_per_sec = write_ops as f64 / write_duration.as_secs_f64();
    println!("Write performance: {:.0} ops/sec", write_ops_per_sec);
    
    // Concurrent read load
    let read_start = Instant::now();
    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..2500 {
                    let key = format!("thread_{}_key_{:04}", thread_id, i);
                    let _value = db.get(key.as_bytes()).unwrap();
                }
            })
        })
        .collect();
    
    for handle in handles {
        let _ = handle.join();
    }
    
    let read_duration = read_start.elapsed();
    let read_ops = 10000;
    let read_ops_per_sec = read_ops as f64 / read_duration.as_secs_f64();
    println!("Read performance: {:.0} ops/sec", read_ops_per_sec);
    
    // Performance should be reasonable
    assert!(write_ops_per_sec > 1000.0);
    assert!(read_ops_per_sec > 1000.0);
}

#[test]
fn test_error_recovery() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test transaction abort
    let tx = db.begin_transaction().unwrap();
    db.put_tx(tx, b"tx_key", b"tx_value").unwrap();
    db.abort_transaction(tx).unwrap();
    
    // Verify aborted transaction didn't persist
    assert_eq!(db.get(b"tx_key").unwrap(), None);
    
    // Test conflicting transactions
    db.put(b"conflict_key", b"initial").unwrap();
    
    let tx1 = db.begin_transaction().unwrap();
    let tx2 = db.begin_transaction().unwrap();
    
    // Both transactions try to modify the same key
    db.put_tx(tx1, b"conflict_key", b"tx1_value").unwrap();
    
    // tx2 attempting to write to same key may fail immediately or at commit
    let put_result = db.put_tx(tx2, b"conflict_key", b"tx2_value");
    
    if put_result.is_ok() {
        // If put succeeded, conflict will be detected at commit
        let result1 = db.commit_transaction(tx1);
        let result2 = db.commit_transaction(tx2);
        // At least one should fail
        assert!(result1.is_err() || result2.is_err());
    } else {
        // Conflict detected early, abort both transactions
        let _ = db.abort_transaction(tx1);
        let _ = db.abort_transaction(tx2);
    }
    
    // Test recovery from batch errors
    let mut batch = WriteBatch::new();
    for i in 0..100 {
        let key = format!("batch_error_{}", i);
        batch.put(key.into_bytes(), vec![b'Z'; 1024]).unwrap();
    }
    
    // Write should succeed
    assert!(db.write_batch(&batch).is_ok());
    
    // Verify partial writes didn't corrupt state
    let test_key = b"test_after_batch";
    db.put(test_key, b"value").unwrap();
    assert_eq!(db.get(test_key).unwrap(), Some(b"value".to_vec()));
}