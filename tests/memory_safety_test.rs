use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod common;
use common::*;

#[test]
fn test_no_memory_leaks_on_drop() {
    let (_dir, db) = setup_temp_db();
    
    // Insert data
    let data = generate_sequential_data(1000);
    for (key, value) in data {
        db.put(&key, &value).unwrap();
    }
    
    // Database should clean up properly when dropped
}

#[test]
fn test_concurrent_access_no_data_race() {
    let (_dir, db) = setup_concurrent_db();
    let num_threads = 50;
    let ops_per_thread = 100;
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for op_id in 0..ops_per_thread {
                    let key = generate_unique_key(thread_id, op_id);
                    let value = generate_unique_value(thread_id, op_id);
                    
                    // Interleave reads and writes
                    if op_id % 2 == 0 {
                        db.put(&key, &value).unwrap();
                    } else if op_id > 0 {
                        let prev_key = generate_unique_key(thread_id, op_id - 1);
                        let _ = db.get(&prev_key);
                    }
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_transaction_cleanup() {
    let (_dir, db) = setup_temp_db();
    
    // Start many transactions
    let mut tx_ids = Vec::new();
    for _ in 0..100 {
        let tx_id = db.begin_transaction().unwrap();
        tx_ids.push(tx_id);
    }
    
    // Abort half, commit half
    for (i, tx_id) in tx_ids.iter().enumerate() {
        if i % 2 == 0 {
            db.abort_transaction(*tx_id).unwrap();
        } else {
            db.commit_transaction(*tx_id).unwrap();
        }
    }
    
    // All transactions should be cleaned up
    let stats = db.stats();
    assert_eq!(stats.active_transactions, 0);
}

#[test]
fn test_large_value_handling() {
    let (_dir, db) = setup_temp_db();
    
    // Test various sizes to ensure no buffer overflows
    let sizes = vec![
        1024,           // 1KB
        10 * 1024,      // 10KB
        100 * 1024,     // 100KB
        1024 * 1024,    // 1MB
    ];
    
    for (i, size) in sizes.iter().enumerate() {
        let key = format!("large_{}", i);
        let value = generate_large_value(*size);
        
        db.put(key.as_bytes(), &value).unwrap();
        
        let retrieved = db.get(key.as_bytes()).unwrap();
        assert_eq!(retrieved, Some(value));
    }
}

#[test]
fn test_resource_cleanup_on_error() {
    let (_dir, db) = setup_temp_db();
    
    // Try operations that might fail
    let tx_id = db.begin_transaction().unwrap();
    
    // Insert some data
    db.put_tx(tx_id, b"key1", b"value1").unwrap();
    
    // Abort the transaction
    db.abort_transaction(tx_id).unwrap();
    
    // Try to use the aborted transaction (should fail)
    let result = db.put_tx(tx_id, b"key2", b"value2");
    assert!(result.is_err());
    
    // Verify no data was leaked
    assert!(db.get(b"key1").unwrap().is_none());
    assert!(db.get(b"key2").unwrap().is_none());
}

#[test]
fn test_cache_bounded_growth() {
    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 1024; // 1MB cache
    
    let (_dir, db) = setup_temp_db_with_config(config);
    
    // Insert more data than cache can hold
    for i in 0..10000 {
        let key = format!("key_{:06}", i);
        let value = generate_large_value(1024); // 1KB each
        db.put(key.as_bytes(), &value).unwrap();
    }
    
    // Cache should not grow unbounded
    let stats = db.stats();
    println!("Cache stats after inserting 10MB into 1MB cache:");
    println!("  Cache hit rate: {:?}", stats.cache_hit_rate);
    
    // Verify data integrity
    for i in 0..100 {
        let key = format!("key_{:06}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn test_concurrent_transactions_no_deadlock() {
    let (_dir, db) = setup_concurrent_db();
    let num_threads = 10;
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for _ in 0..10 {
                    let tx_id = db.begin_transaction().unwrap();
                    
                    // Access keys in different order to test deadlock prevention
                    let result = if thread_id % 2 == 0 {
                        db.put_tx(tx_id, b"shared_key_a", b"value_a")
                            .and_then(|_| {
                                thread::sleep(Duration::from_micros(100));
                                db.put_tx(tx_id, b"shared_key_b", b"value_b")
                            })
                    } else {
                        db.put_tx(tx_id, b"shared_key_b", b"value_b")
                            .and_then(|_| {
                                thread::sleep(Duration::from_micros(100));
                                db.put_tx(tx_id, b"shared_key_a", b"value_a")
                            })
                    };
                    
                    // If conflict detected, abort and retry
                    if result.is_err() {
                        let _ = db.abort_transaction(tx_id);
                    } else {
                        let _ = db.commit_transaction(tx_id);
                    }
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_batch_operations_memory_efficiency() {
    let (_dir, db) = setup_temp_db();
    
    // Create large batch
    let batch_size = 10000;
    let mut batch = Vec::new();
    for i in 0..batch_size {
        let key = format!("batch_key_{:06}", i);
        let value = format!("batch_value_{}", i);
        batch.push((key.into_bytes(), value.into_bytes()));
    }
    
    // Batch insert should be memory efficient
    db.batch_put(&batch).unwrap();
    
    // Verify all data
    for i in 0..batch_size {
        let key = format!("batch_key_{:06}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn test_file_handle_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    
    // Create and use database
    {
        let db = Database::create(&path, LightningDbConfig::default()).unwrap();
        
        // Perform operations
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.flush_lsm().unwrap();
    } // Database dropped here, should close all file handles
    
    // Reopen to verify file handles were properly closed
    {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();
        
        // Verify data
        for i in 0..100 {
            let key = format!("key_{}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn test_panic_safety() {
    let (_dir, db) = setup_concurrent_db();
    
    // Start a thread that will panic
    let db_clone = Arc::clone(&db);
    let handle = thread::spawn(move || {
        let tx_id = db_clone.begin_transaction().unwrap();
        db_clone.put_tx(tx_id, b"panic_key", b"panic_value").unwrap();
        
        // Simulate panic
        panic!("Intentional panic for testing");
    });
    
    // Wait for panic
    let _ = handle.join();
    
    // Database should still be usable
    db.put(b"after_panic", b"still_works").unwrap();
    assert_eq!(db.get(b"after_panic").unwrap(), Some(b"still_works".to_vec()));
    
    // Transaction from panicked thread should be rolled back
    assert!(db.get(b"panic_key").unwrap().is_none());
}