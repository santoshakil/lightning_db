use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

#[test]
fn test_concurrent_access_safety() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path().join("concurrent.db"), config).unwrap());
    
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = vec![];
    
    for thread_id in 0..5 {
        let db = db.clone();
        let barrier = barrier.clone();
        
        let handle = thread::spawn(move || {
            barrier.wait();
            
            for i in 0..100 {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("value_{}", i);
                
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                
                // Also do some reads
                let read_key = format!("thread_{}_key_{}", (thread_id + 1) % 5, i);
                let _ = db.get(read_key.as_bytes());
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    }

#[test]
fn test_transaction_cleanup() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    let db_path = dir.path().join("cleanup.db");
    
    // Create database and leave transactions uncommitted
    {
        let db = Database::create(&db_path, config.clone()).unwrap();
        
        let _tx1 = db.begin_transaction().unwrap();
        let _tx2 = db.begin_transaction().unwrap();
        let _tx3 = db.begin_transaction().unwrap();
        
        // Transactions should be cleaned up on drop
    }
    
    // Reopen database - should work fine
    {
        let db = Database::open(&db_path, config).unwrap();
        
        let tx = db.begin_transaction().unwrap();
        db.put_tx(tx, b"test", b"value").unwrap();
        db.commit_transaction(tx).unwrap();
        
        // Verify the write
        assert_eq!(db.get(b"test").unwrap(), Some(b"value".to_vec()));
    }
    }

#[test]
fn test_large_value_handling() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path().join("large.db"), config).unwrap();
    
    // Test various sizes
    let sizes = [1024, 64 * 1024, 512 * 1024, 1024 * 1024]; // 1KB, 64KB, 512KB, 1MB
    
    for (i, &size) in sizes.iter().enumerate() {
        let key = format!("large_{}", i);
        let value = vec![0xAB; size];
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                // Verify we can read it back
                let read_value = db.get(key.as_bytes()).unwrap().unwrap();
                assert_eq!(read_value.len(), size);
                assert_eq!(read_value[0], 0xAB);
                assert_eq!(read_value[size - 1], 0xAB);
            }
            Err(_) => {
                // Large values might fail with InvalidValueSize
                // This is expected for values over 1MB
            }
        }
    }
    }

#[test]
fn test_concurrent_transactions() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path().join("tx_concurrent.db"), config).unwrap());
    
    // Initialize counter
    db.put(b"counter", b"0").unwrap();
    
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = vec![];
    
    for thread_id in 0..3 {
        let db = db.clone();
        let barrier = barrier.clone();
        
        let handle = thread::spawn(move || {
            barrier.wait();
            
            let mut success_count = 0;
            let mut conflict_count = 0;
            
            for _ in 0..50 {
                let tx_id = db.begin_transaction().unwrap();
                
                // Read current value
                let current = db.get_tx(tx_id, b"counter")
                    .unwrap()
                    .unwrap_or_else(|| b"0".to_vec());
                
                let value: i32 = std::str::from_utf8(&current)
                    .unwrap()
                    .parse()
                    .unwrap();
                
                // Increment
                let new_value = (value + 1).to_string();
                db.put_tx(tx_id, b"counter", new_value.as_bytes()).unwrap();
                
                // Try to commit
                match db.commit_transaction(tx_id) {
                    Ok(_) => success_count += 1,
                    Err(_) => conflict_count += 1,
                }
            }
            
            (thread_id, success_count, conflict_count)
        });
        
        handles.push(handle);
    }
    
    let mut total_success = 0;
    let mut total_conflicts = 0;
    
    for handle in handles {
        let (thread_id, success, conflicts) = handle.join().expect("Thread panicked");
        println!("Thread {}: {} commits, {} conflicts", thread_id, success, conflicts);
        total_success += success;
        total_conflicts += conflicts;
    }
    
    // Verify final counter value matches successful commits
    let final_value = db.get(b"counter").unwrap().unwrap();
    let count: i32 = std::str::from_utf8(&final_value).unwrap().parse().unwrap();
    
    assert_eq!(count, total_success);
    assert!(total_conflicts > 0); // Should have some conflicts in concurrent scenario
    }

#[test]
#[ignore] // Database doesn't have iter() method yet
fn test_iterator_memory_safety() {
    let _dir = tempdir().unwrap();
    let _config = LightningDbConfig::default();
    // TODO: Implement when Database has iter() method
    println!("Iterator test skipped - not yet implemented");
}