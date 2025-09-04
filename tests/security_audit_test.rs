use lightning_db::{Config, Database, TransactionMode};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use rand::{Rng, thread_rng};

#[test]
fn test_input_validation() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 64 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    
    // Test with extremely long keys
    let long_key = vec![0u8; 1024 * 1024]; // 1MB key
    let result = db.put(&long_key, b"value");
    // Should either succeed or return a proper error, not crash
    match result {
        Ok(_) => println!("Long key accepted"),
        Err(e) => println!("Long key rejected: {}", e),
    }
    
    // Test with empty keys
    let result = db.put(b"", b"value");
    match result {
        Ok(_) => println!("Empty key accepted"),
        Err(e) => println!("Empty key rejected: {}", e),
    }
    
    // Test with null bytes in keys
    let null_key = b"key\0with\0nulls";
    let result = db.put(null_key, b"value");
    match result {
        Ok(_) => {
            // Should be able to retrieve it
            let value = db.get(null_key).unwrap();
            assert_eq!(value, Some(b"value".to_vec()));
        }
        Err(e) => println!("Null byte key rejected: {}", e),
    }
    
    // Test with extremely large values
    let large_value = vec![0u8; 100 * 1024 * 1024]; // 100MB value
    let result = db.put(b"large_key", &large_value);
    match result {
        Ok(_) => println!("Large value accepted"),
        Err(e) => println!("Large value rejected: {}", e),
    }
}

#[test]
fn test_resource_exhaustion_protection() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 32 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(config).unwrap());
    
    // Test transaction limit
    let mut transactions = Vec::new();
    for i in 0..100 {
        match db.begin_transaction(TransactionMode::ReadWrite) {
            Ok(tx_id) => transactions.push(tx_id),
            Err(e) => {
                println!("Transaction limit reached at {}: {}", i, e);
                break;
            }
        }
    }
    
    // Clean up transactions
    for tx_id in transactions {
        let _ = db.abort_transaction(tx_id);
    }
    
    // Test memory exhaustion protection
    let stop_flag = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];
    
    for thread_id in 0..50 {
        let db_clone = Arc::clone(&db);
        let stop_clone = Arc::clone(&stop_flag);
        
        handles.push(thread::spawn(move || {
            let mut counter = 0;
            while !stop_clone.load(Ordering::Relaxed) {
                let key = format!("exhaust_key_{}_{}", thread_id, counter);
                let value = vec![0u8; 1024 * 1024]; // 1MB values
                
                match db_clone.put(key.as_bytes(), &value) {
                    Ok(_) => counter += 1,
                    Err(e) => {
                        println!("Resource exhaustion prevented: {}", e);
                        break;
                    }
                }
                
                if counter > 1000 {
                    break; // Prevent runaway test
                }
            }
            counter
        }));
    }
    
    thread::sleep(Duration::from_secs(5));
    stop_flag.store(true, Ordering::Relaxed);
    
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_access_safety() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 128 * 1024 * 1024,
        max_concurrent_transactions: 50,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(config).unwrap());
    let corruption_detected = Arc::new(AtomicBool::new(false));
    
    // Pre-populate with known values
    for i in 0..1000 {
        let key = format!("safety_key_{:04}", i);
        let value = format!("safety_value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    let mut handles = vec![];
    
    // Reader threads - verify data consistency
    for _ in 0..10 {
        let db_clone = Arc::clone(&db);
        let corruption_clone = Arc::clone(&corruption_detected);
        
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                let idx = thread_rng().gen_range(0..1000);
                let key = format!("safety_key_{:04}", idx);
                let expected_value = format!("safety_value_{:04}", idx);
                
                if let Ok(Some(value)) = db_clone.get(key.as_bytes()) {
                    if value != expected_value.as_bytes() {
                        corruption_clone.store(true, Ordering::Relaxed);
                        eprintln!("Data corruption detected: key={}, expected={}, got={}", 
                                 key, expected_value, String::from_utf8_lossy(&value));
                    }
                }
                
                thread::yield_now();
            }
        }));
    }
    
    // Writer threads - concurrent updates
    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("concurrent_key_{}_{}", thread_id, i);
                let value = format!("concurrent_value_{}_{}", thread_id, i);
                
                let _ = db_clone.put(key.as_bytes(), value.as_bytes());
                thread::yield_now();
            }
        }));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    assert!(!corruption_detected.load(Ordering::Relaxed), 
            "Data corruption detected during concurrent access");
}

#[test]
fn test_crash_consistency() {
    for iteration in 0..3 {
        println!("\n=== Crash Consistency Test Iteration {} ===", iteration + 1);
        
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        
        // Phase 1: Write with potential crash
        {
            let config = Config {
                path: path.clone(),
                cache_size: 32 * 1024 * 1024,
                max_concurrent_transactions: 5,
                enable_compression: false,
                compression_level: None,
                fsync_mode: lightning_db::FsyncMode::EveryTransaction,
                page_size: 4096,
                enable_encryption: false,
                encryption_key: None,
            };
            
            let db = Database::open(config).unwrap();
            
            // Start transaction
            let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
            
            // Write test data
            for i in 0..100 {
                let key = format!("crash_key_{:04}", i);
                let value = format!("crash_value_{:04}_iter_{}", i, iteration);
                tx.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            if iteration % 2 == 0 {
                // Commit and verify
                tx.commit().unwrap();
                println!("Transaction committed");
            } else {
                // Simulate crash without commit
                drop(tx);
                drop(db);
                println!("Simulated crash without commit");
            }
        }
        
        // Phase 2: Verify consistency
        {
            let config = Config {
                path: path.clone(),
                cache_size: 32 * 1024 * 1024,
                max_concurrent_transactions: 5,
                enable_compression: false,
                compression_level: None,
                fsync_mode: lightning_db::FsyncMode::EveryTransaction,
                page_size: 4096,
                enable_encryption: false,
                encryption_key: None,
            };
            
            let db = Database::open(config).unwrap();
            
            if iteration % 2 == 0 {
                // Data should be present
                for i in 0..100 {
                    let key = format!("crash_key_{:04}", i);
                    let expected_value = format!("crash_value_{:04}_iter_{}", i, iteration);
                    
                    let value = db.get(key.as_bytes()).unwrap();
                    assert_eq!(value, Some(expected_value.into_bytes()),
                              "Committed data not found after recovery");
                }
                println!("✓ All committed data recovered");
            } else {
                // Data should NOT be present (transaction wasn't committed)
                for i in 0..100 {
                    let key = format!("crash_key_{:04}", i);
                    let value = db.get(key.as_bytes()).unwrap();
                    assert_eq!(value, None,
                              "Uncommitted data found after recovery - atomicity violation!");
                }
                println!("✓ Uncommitted transaction properly rolled back");
            }
        }
    }
}

#[test]
fn test_permission_and_access_control() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 32 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::EveryTransaction,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    
    // Test read-only transaction restrictions
    let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
    
    // These operations should fail or be no-ops
    let result = tx.put(b"readonly_key", b"value");
    assert!(result.is_err() || tx.get(b"readonly_key").unwrap().is_none(),
            "Read-only transaction allowed write operation");
    
    let result = tx.delete(b"some_key");
    assert!(result.is_err(), "Read-only transaction allowed delete operation");
    
    drop(tx);
    
    // Test transaction isolation
    let mut tx1 = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
    let tx2 = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
    
    tx1.put(b"isolation_key", b"tx1_value").unwrap();
    
    // tx2 should not see uncommitted changes from tx1
    let value = tx2.get(b"isolation_key").unwrap();
    assert_eq!(value, None, "Transaction isolation violated - uncommitted data visible");
    
    tx1.commit().unwrap();
    
    // tx2 still shouldn't see the change (snapshot isolation)
    let value = tx2.get(b"isolation_key").unwrap();
    assert_eq!(value, None, "Snapshot isolation violated");
    
    drop(tx2);
    
    // New transaction should see committed data
    let tx3 = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
    let value = tx3.get(b"isolation_key").unwrap();
    assert_eq!(value, Some(b"tx1_value".to_vec()), "Committed data not visible");
}

#[test]
fn test_data_integrity_checksums() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 64 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::EveryTransaction,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    
    // Write test data
    for i in 0..1000 {
        let key = format!("integrity_key_{:06}", i);
        let value = format!("integrity_value_{:06}_with_some_extra_data", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Verify all data can be read back correctly
    for i in 0..1000 {
        let key = format!("integrity_key_{:06}", i);
        let expected_value = format!("integrity_value_{:06}_with_some_extra_data", i);
        
        let value = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, Some(expected_value.into_bytes()),
                  "Data integrity check failed");
    }
    
    // Verify database integrity if method exists
    if let Err(e) = db.verify_integrity() {
        panic!("Database integrity verification failed: {}", e);
    }
    
    println!("✓ Data integrity verified for 1000 entries");
}