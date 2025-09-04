//! Stress tests for Lightning DB (opt-in via `--features integration_tests`)
//! 
//! These tests push the database to its limits to ensure stability under load

// Imports are scoped within the gated module to avoid compiling when feature is disabled

#[cfg(all(test, feature = "integration_tests"))]
mod stress_tests {
    use lightning_db::*;
    use rand::{distributions::Alphanumeric, Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::sync::{Arc, Barrier, atomic::{AtomicBool, AtomicU64, Ordering}};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    
    // Note: imports moved inside the gated module to avoid compiling without feature
    
    /// Helper to generate random key-value pairs
    fn random_kv(rng: &mut impl Rng, key_size: usize, value_size: usize) -> (Vec<u8>, Vec<u8>) {
        let key: Vec<u8> = (0..key_size).map(|_| rng.random()).collect();
        let value: Vec<u8> = (0..value_size).map(|_| rng.random()).collect();
        (key, value)
    }
    
    #[test]
    fn stress_concurrent_readers_writers() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
        
        let num_writers = 5;
        let num_readers = 10;
        let ops_per_thread = 10000;
        let running = Arc::new(AtomicBool::new(true));
        let barrier = Arc::new(Barrier::new(num_writers + num_readers));
        
        // Metrics
        let writes = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        
        // Writer threads
        let writer_handles: Vec<_> = (0..num_writers).map(|writer_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            let running = running.clone();
            let writes = writes.clone();
            let errors = errors.clone();
            
            thread::spawn(move || {
                let mut rng = ChaCha8Rng::seed_from_u64(writer_id as u64);
                barrier.wait();
                
                for i in 0..ops_per_thread {
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                    
                    let key = format!("w{}_k{}", writer_id, i);
                    let value = vec![rng.random::<u8>(); 100 + rng.gen_range(0..1000)];
                    
                    match db.put(key.as_bytes(), &value) {
                        Ok(_) => writes.fetch_add(1, Ordering::Relaxed),
                        Err(_) => errors.fetch_add(1, Ordering::Relaxed),
                    };
                    
                    // Occasionally delete
                    if rng.gen_bool(0.1) {
                        let del_key = format!("w{}_k{}", writer_id, rng.gen_range(0..i.max(1)));
                        let _ = db.delete(del_key.as_bytes());
                    }
                }
            })
        }).collect();
        
        // Reader threads
        let reader_handles: Vec<_> = (0..num_readers).map(|reader_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            let running = running.clone();
            let reads = reads.clone();
            let errors = errors.clone();
            
            thread::spawn(move || {
                let mut rng = ChaCha8Rng::seed_from_u64(1000 + reader_id as u64);
                barrier.wait();
                
                for _ in 0..ops_per_thread {
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                    
                    let writer = rng.gen_range(0..num_writers);
                    let key_id = rng.gen_range(0..ops_per_thread);
                    let key = format!("w{}_k{}", writer, key_id);
                    
                    match db.get(key.as_bytes()) {
                        Ok(_) => reads.fetch_add(1, Ordering::Relaxed),
                        Err(_) => errors.fetch_add(1, Ordering::Relaxed),
                    };
                    
                    // Occasionally do range scan
                    if rng.gen_bool(0.05) {
                        let start_key = format!("w{}_k{}", writer, key_id);
                        let end_key = format!("w{}_k{}", writer, key_id + 10);
                        let _ = db.range(start_key.as_bytes()..end_key.as_bytes());
                    }
                }
            })
        }).collect();
        
        // Wait for completion
        for handle in writer_handles {
            handle.join().unwrap();
        }
        for handle in reader_handles {
            handle.join().unwrap();
        }
        
        // Report results
        let total_writes = writes.load(Ordering::Relaxed);
        let total_reads = reads.load(Ordering::Relaxed);
        let total_errors = errors.load(Ordering::Relaxed);
        
        println!("Stress test results:");
        println!("  Writes: {}", total_writes);
        println!("  Reads: {}", total_reads);
        println!("  Errors: {}", total_errors);
        
        assert!(total_writes > 0);
        assert!(total_reads > 0);
        assert_eq!(total_errors, 0, "No errors should occur during normal operation");
    }
    
    #[test]
    fn stress_large_values() {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            max_value_size: Some(10 * 1024 * 1024), // 10MB max value
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config).unwrap();
        let mut rng = rand::rng();
        
        // Test various value sizes
        let sizes = vec![
            1,                  // 1 byte
            1024,              // 1 KB
            10 * 1024,         // 10 KB
            100 * 1024,        // 100 KB
            1024 * 1024,       // 1 MB
            5 * 1024 * 1024,   // 5 MB
        ];
        
        for (i, &size) in sizes.iter().enumerate() {
            let key = format!("large_key_{}", i);
            let value: Vec<u8> = (0..size).map(|_| rng.random()).collect();
            
            db.put(key.as_bytes(), &value).unwrap();
            
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(retrieved.len(), value.len());
            assert_eq!(retrieved, value);
        }
    }
    
    #[test]
    fn stress_many_small_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
        
        let num_threads = 8;
        let txns_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));
        
        let handles: Vec<_> = (0..num_threads).map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                barrier.wait();
                
                for txn_id in 0..txns_per_thread {
                    let tx = db.begin_transaction().unwrap();
                    
                    // Small transaction with 1-5 operations
                    for op in 0..rand::rng().gen_range(1..=5) {
                        let key = format!("t{}_tx{}_k{}", thread_id, txn_id, op);
                        let value = format!("value_{}", op);
                        db.put_tx(tx, key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    
                    // 90% commit, 10% rollback
                    if rand::rng().gen_bool(0.9) {
                        db.commit_transaction(tx).unwrap();
                    } else {
                        db.rollback_transaction(tx).unwrap();
                    }
                }
            })
        }).collect();
        
        let start = Instant::now();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let total_txns = num_threads * txns_per_thread;
        let txns_per_sec = total_txns as f64 / duration.as_secs_f64();
        
        println!("Transaction throughput: {:.0} txns/sec", txns_per_sec);
        assert!(txns_per_sec > 100.0, "Should handle at least 100 txns/sec");
    }
    
    #[test]
    fn stress_rapid_open_close() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_path_buf();
        
        for iteration in 0..50 {
            // Open database
            let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
            
            // Quick operations
            for i in 0..10 {
                let key = format!("iter{}_key{}", iteration, i);
                let value = format!("iter{}_value{}", iteration, i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            // Close (drop) database
            drop(db);
            
            // Reopen and verify
            let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
            for i in 0..10 {
                let key = format!("iter{}_key{}", iteration, i);
                let value = format!("iter{}_value{}", iteration, i);
                assert_eq!(db.get(key.as_bytes()).unwrap().unwrap(), value.as_bytes());
            }
            
            drop(db);
        }
    }
    
    #[test]
    fn stress_memory_churn() {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            cache_size: 5 * 1024 * 1024, // 5MB cache
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config).unwrap();
        let mut rng = rand::rng();
        
        // Continuously allocate and free memory
        for round in 0..100 {
            // Insert batch of large values
            for i in 0..100 {
                let key = format!("churn_r{}_k{}", round, i);
                let value_size = rng.gen_range(1000..50000);
                let value: Vec<u8> = (0..value_size).map(|_| rng.random()).collect();
                db.put(key.as_bytes(), &value).unwrap();
            }
            
            // Delete half of them
            for i in (0..100).step_by(2) {
                let key = format!("churn_r{}_k{}", round, i);
                db.delete(key.as_bytes()).unwrap();
            }
            
            // Force cache eviction by reading old data
            if round > 10 {
                for i in 0..50 {
                    let old_round = round - 10;
                    let key = format!("churn_r{}_k{}", old_round, i * 2 + 1);
                    let _ = db.get(key.as_bytes());
                }
            }
        }
        
        // Verify memory didn't leak excessively
        let stats = db.get_memory_stats().unwrap();
        assert!(stats.allocated_bytes < 100 * 1024 * 1024, "Memory usage should stay under 100MB");
    }
    
    #[test]
    fn stress_concurrent_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
        
        let writer_db = db.clone();
        let compactor_db = db.clone();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_writer = stop_flag.clone();
        let stop_flag_compactor = stop_flag.clone();
        
        // Writer thread
        let writer = thread::spawn(move || {
            let mut rng = rand::rng();
            let mut count = 0;
            
            while !stop_flag_writer.load(Ordering::Relaxed) {
                let key = format!("compact_key_{}", count);
                let value: Vec<u8> = (0..rng.gen_range(100..1000)).map(|_| rng.random()).collect();
                writer_db.put(key.as_bytes(), &value).unwrap();
                
                // Delete some keys to create fragmentation
                if count > 100 && rng.gen_bool(0.3) {
                    let del_key = format!("compact_key_{}", rng.gen_range(0..count));
                    let _ = writer_db.delete(del_key.as_bytes());
                }
                
                count += 1;
            }
            count
        });
        
        // Compaction thread
        let compactor = thread::spawn(move || {
            let mut compact_count = 0;
            
            while !stop_flag_compactor.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
                let _ = compactor_db.compact();
                compact_count += 1;
            }
            compact_count
        });
        
        // Run for 5 seconds
        thread::sleep(Duration::from_secs(5));
        stop_flag.store(true, Ordering::Relaxed);
        
        let write_count = writer.join().unwrap();
        let compact_count = compactor.join().unwrap();
        
        println!("Writes during compaction: {}", write_count);
        println!("Compaction runs: {}", compact_count);
        
        // Verify data integrity
        let mut found = 0;
        for i in 0..write_count {
            let key = format!("compact_key_{}", i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                found += 1;
            }
        }
        
        println!("Keys found after stress: {}", found);
        assert!(found > 0, "Should have some keys remaining");
    }
    
    #[test]
    fn stress_transaction_conflicts() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
        
        // Initialize shared counters
        for i in 0..10 {
            db.put(format!("counter_{}", i).as_bytes(), b"0").unwrap();
        }
        
        let num_threads = 8;
        let ops_per_thread = 100;
        let barrier = Arc::new(Barrier::new(num_threads));
        let conflicts = Arc::new(AtomicU64::new(0));
        
        let handles: Vec<_> = (0..num_threads).map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            let conflicts = conflicts.clone();
            
            thread::spawn(move || {
                let mut rng = rand::rng();
                barrier.wait();
                
                for _ in 0..ops_per_thread {
                    let counter_id = rng.gen_range(0..10);
                    let counter_key = format!("counter_{}", counter_id);
                    
                    // Try to increment counter in transaction
                    loop {
                        let tx = db.begin_transaction().unwrap();
                        
                        let current = db.get_tx(tx, counter_key.as_bytes()).unwrap().unwrap();
                        let value: u64 = String::from_utf8_lossy(&current).parse().unwrap_or(0);
                        let new_value = (value + 1).to_string();
                        
                        db.put_tx(tx, counter_key.as_bytes(), new_value.as_bytes()).unwrap();
                        
                        match db.commit_transaction(tx) {
                            Ok(_) => break,
                            Err(_) => {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                                // Retry after conflict
                            }
                        }
                    }
                }
            })
        }).collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify final counter values
        let mut total = 0;
        for i in 0..10 {
            let counter_key = format!("counter_{}", i);
            let value = db.get(counter_key.as_bytes()).unwrap().unwrap();
            let count: u64 = String::from_utf8_lossy(&value).parse().unwrap();
            total += count;
        }
        
        let expected = (num_threads * ops_per_thread) as u64;
        assert_eq!(total, expected, "All increments should be accounted for");
        
        let conflict_count = conflicts.load(Ordering::Relaxed);
        println!("Transaction conflicts: {}", conflict_count);
        assert!(conflict_count > 0, "Should have some conflicts with concurrent updates");
    }
    
    #[test]
    fn stress_disk_space_exhaustion() {
        // This test simulates running out of disk space
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            max_db_size: Some(10 * 1024 * 1024), // Limit to 10MB
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config).unwrap();
        let mut rng = rand::rng();
        let mut write_count = 0;
        let mut space_exhausted = false;
        
        // Keep writing until space is exhausted
        for i in 0..100000 {
            let key = format!("exhaust_key_{}", i);
            let value: Vec<u8> = (0..1024).map(|_| rng.random()).collect(); // 1KB values
            
            match db.put(key.as_bytes(), &value) {
                Ok(_) => write_count += 1,
                Err(e) if e.is_resource_exhausted() => {
                    space_exhausted = true;
                    println!("Space exhausted after {} writes", write_count);
                    break;
                }
                Err(e) => panic!("Unexpected error: {}", e),
            }
        }
        
        assert!(space_exhausted, "Should hit space limit");
        assert!(write_count > 0, "Should write some data before exhaustion");
        
        // Database should still be readable
        let key = format!("exhaust_key_{}", 0);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
        
        // Compact to reclaim space
        let reclaimed = db.compact().unwrap();
        assert!(reclaimed > 0, "Should reclaim some space");
        
        // Should be able to write again after compaction
        db.put(b"after_compact", b"value").unwrap();
    }
}
