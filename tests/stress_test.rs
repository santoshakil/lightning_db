use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn stress_test_concurrent_writes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    const NUM_THREADS: usize = 10;
    const OPS_PER_THREAD: usize = 1000;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..OPS_PER_THREAD {
                let key = format!("t{}_k{:04}", thread_id, i);
                let value = format!("thread_{}_value_{}", thread_id, i);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let total_ops = NUM_THREADS * OPS_PER_THREAD;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    
    println!("Concurrent writes: {} ops in {:?} ({:.0} ops/sec)", 
             total_ops, elapsed, ops_per_sec);
    
    // Verify all data
    for thread_id in 0..NUM_THREADS {
        for i in 0..OPS_PER_THREAD {
            let key = format!("t{}_k{:04}", thread_id, i);
            let expected = format!("thread_{}_value_{}", thread_id, i);
            let result = db.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(expected.into_bytes()));
        }
    }
}

#[test]
fn stress_test_mixed_operations() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    const NUM_THREADS: usize = 8;
    const OPS_PER_THREAD: usize = 500;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    
    let mut handles = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            // Mix of operations
            for i in 0..OPS_PER_THREAD {
                let key = format!("mixed_{:04}", i);
                let value = format!("value_t{}_i{}", thread_id, i);
                
                match i % 4 {
                    0 => {
                        // Write
                        db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    1 => {
                        // Read
                        let _ = db_clone.get(key.as_bytes()).unwrap();
                    }
                    2 => {
                        // Update
                        db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    3 => {
                        // Delete
                        let _ = db_clone.delete(key.as_bytes()).unwrap();
                    }
                    _ => unreachable!(),
                }
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn stress_test_large_values() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test with increasingly large values
    let sizes = [1_024, 10_240, 102_400, 512_000];
    
    for (i, &size) in sizes.iter().enumerate() {
        let key = format!("large_{}", i);
        let value = vec![b'X'; size];
        
        db.put(key.as_bytes(), &value).unwrap();
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(retrieved.len(), size);
        assert_eq!(retrieved, value);
    }
}

#[test]
fn stress_test_transaction_conflicts() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    const NUM_THREADS: usize = 5;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    
    // Pre-populate some keys
    for i in 0..100 {
        let key = format!("conflict_{:03}", i);
        db.put(key.as_bytes(), b"initial").unwrap();
    }
    
    let mut handles = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            let mut success_count = 0;
            let mut conflict_count = 0;
            
            for i in 0..100 {
                let key = format!("conflict_{:03}", i);
                let value = format!("thread_{}", thread_id);
                
                let tx_id = db_clone.begin_transaction().unwrap();
                
                match db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        match db_clone.commit_transaction(tx_id) {
                            Ok(_) => success_count += 1,
                            Err(_) => {
                                conflict_count += 1;
                                let _ = db_clone.abort_transaction(tx_id);
                            }
                        }
                    }
                    Err(_) => {
                        conflict_count += 1;
                        let _ = db_clone.abort_transaction(tx_id);
                    }
                }
            }
            
            (success_count, conflict_count)
        });
        
        handles.push(handle);
    }
    
    let mut total_success = 0;
    let mut total_conflicts = 0;
    
    for handle in handles {
        let (success, conflicts) = handle.join().unwrap();
        total_success += success;
        total_conflicts += conflicts;
    }
    
    println!("Transaction stress test: {} successful, {} conflicts",
             total_success, total_conflicts);
    
    // Verify data consistency
    for i in 0..100 {
        let key = format!("conflict_{:03}", i);
        let result = db.get(key.as_bytes()).unwrap();
        assert!(result.is_some());
    }
}

#[test]
fn stress_test_range_scans() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Insert sorted data
    const NUM_KEYS: usize = 10_000;
    
    for i in 0..NUM_KEYS {
        let key = format!("scan_{:06}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Test various range scans
    let start = Instant::now();
    
    // Full scan
    let results = db.range(None, None).unwrap();
    assert_eq!(results.len(), NUM_KEYS);
    
    // Bounded scan
    let start_key = format!("scan_{:06}", 1000);
    let end_key = format!("scan_{:06}", 2000);
    let results = db.range(Some(start_key.as_bytes()), Some(end_key.as_bytes())).unwrap();
    assert!(results.len() >= 999);
    
    let elapsed = start.elapsed();
    println!("Range scan of {} keys: {:?}", NUM_KEYS, elapsed);
}

#[test]
fn stress_test_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    
    // Phase 1: Write data
    {
        let db = Database::create(&path, LightningDbConfig::default()).unwrap();
        
        for i in 0..1000 {
            let key = format!("persist_{:04}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Update some values
        for i in (0..1000).step_by(10) {
            let key = format!("persist_{:04}", i);
            let value = format!("updated_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Delete some values
        for i in (0..1000).step_by(20) {
            let key = format!("persist_{:04}", i);
            db.delete(key.as_bytes()).unwrap();
        }
        
        db.sync().unwrap();
    }
    
    // Phase 2: Reopen and verify
    {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();
        
        for i in 0..1000 {
            let key = format!("persist_{:04}", i);
            let result = db.get(key.as_bytes()).unwrap();
            
            if i % 20 == 0 {
                // Should be deleted
                assert_eq!(result, None);
            } else if i % 10 == 0 {
                // Should be updated
                let expected = format!("updated_{}", i);
                assert_eq!(result, Some(expected.into_bytes()));
            } else {
                // Should have original value
                let expected = format!("value_{}", i);
                assert_eq!(result, Some(expected.into_bytes()));
            }
        }
    }
}

#[test]
fn stress_test_memory_usage() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Insert and delete in cycles to test memory management
    for cycle in 0..10 {
        // Insert batch
        for i in 0..1000 {
            let key = format!("mem_c{}_k{:04}", cycle, i);
            let value = vec![b'V'; 1024]; // 1KB values
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        // Delete half
        for i in (0..1000).step_by(2) {
            let key = format!("mem_c{}_k{:04}", cycle, i);
            db.delete(key.as_bytes()).unwrap();
        }
        
        // Force compaction if available
        if cycle % 3 == 0 {
            let _ = db.sync();
        }
    }
    
    // Verify final state
    for cycle in 0..10 {
        for i in 0..1000 {
            let key = format!("mem_c{}_k{:04}", cycle, i);
            let result = db.get(key.as_bytes()).unwrap();
            
            if i % 2 == 0 {
                assert_eq!(result, None);
            } else {
                assert_eq!(result.unwrap().len(), 1024);
            }
        }
    }
}