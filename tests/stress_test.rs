use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tempfile::tempdir;
use rand::Rng;

#[test]
fn stress_test_concurrent_writes() {
    let dir = tempdir().expect("Failed to create temp dir");
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).expect("Failed to create database"));
    
    const NUM_THREADS: usize = 20;
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
                let key = format!("t{:02}_k{:04}", thread_id, i);
                let value = format!("thread_{}_value_{}_data_{}", thread_id, i, "x".repeat(100));
                db_clone.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    
    let elapsed = start.elapsed();
    let total_ops = NUM_THREADS * OPS_PER_THREAD;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    
    println!("Concurrent writes: {} ops in {:?} ({:.0} ops/sec)", 
             total_ops, elapsed, ops_per_sec);
    
    // Verify all data with checksums
    let mut errors = 0;
    for thread_id in 0..NUM_THREADS {
        for i in 0..OPS_PER_THREAD {
            let key = format!("t{:02}_k{:04}", thread_id, i);
            let expected = format!("thread_{}_value_{}_data_{}", thread_id, i, "x".repeat(100));
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    if value != expected.as_bytes() {
                        errors += 1;
                        println!("Data mismatch for key {}", key);
                    }
                }
                Ok(None) => {
                    errors += 1;
                    println!("Missing key: {}", key);
                }
                Err(e) => {
                    errors += 1;
                    println!("Error reading key {}: {}", key, e);
                }
            }
        }
    }
    
    assert_eq!(errors, 0, "Found {} data integrity errors", errors);
    println!("✓ All {} entries verified successfully", total_ops);
}

#[test]
fn stress_test_data_integrity_under_load() {
    let dir = tempdir().expect("Failed to create temp dir");
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Sync;
    
    let db = Arc::new(Database::create(dir.path(), config).expect("Failed to create database"));
    
    const NUM_THREADS: usize = 10;
    const TEST_DURATION_SECS: u64 = 5;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let stop_flag = Arc::new(Mutex::new(false));
    let operation_counts = Arc::new(Mutex::new(HashMap::new()));
    
    let mut writer_handles = vec![];
    let mut reader_handles = vec![];
    
    // Writer threads
    for thread_id in 0..NUM_THREADS / 2 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let stop_flag_clone = Arc::clone(&stop_flag);
        let counts_clone = Arc::clone(&operation_counts);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let mut rng = rand::rng();
            let mut write_count = 0;
            
            while !*stop_flag_clone.lock().expect("Lock failed") {
                let key_id = rng.random_range(0..10000);
                let key = format!("integrity_key_{:05}", key_id);
                let value = format!("value_{}_thread_{}_seq_{}", key_id, thread_id, write_count);
                
                if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                    write_count += 1;
                    counts_clone.lock().expect("Lock failed").insert(key.clone(), value);
                }
                
                if write_count % 100 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            
            write_count
        });
        
        writer_handles.push(handle);
    }
    
    // Reader threads
    for _thread_id in NUM_THREADS / 2..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let stop_flag_clone = Arc::clone(&stop_flag);
        let _counts_clone = Arc::clone(&operation_counts);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let mut rng = rand::rng();
            let mut read_count = 0;
            let mut integrity_errors = 0;
            
            while !*stop_flag_clone.lock().expect("Lock failed") {
                let key_id = rng.random_range(0..10000);
                let key = format!("integrity_key_{:05}", key_id);
                
                if let Ok(Some(value)) = db_clone.get(key.as_bytes()) {
                    read_count += 1;
                    
                    // Verify value format
                    let value_str = String::from_utf8_lossy(&value);
                    if !value_str.starts_with(&format!("value_{}_", key_id)) {
                        integrity_errors += 1;
                        println!("Integrity error: key {} has unexpected value format", key);
                    }
                }
                
                if read_count % 100 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            
            (read_count, integrity_errors)
        });
        
        reader_handles.push(handle);
    }
    
    // Let test run for specified duration
    thread::sleep(Duration::from_secs(TEST_DURATION_SECS));
    *stop_flag.lock().expect("Lock failed") = true;
    
    let mut total_writes = 0;
    let mut total_reads = 0;
    let mut total_errors = 0;
    
    for handle in writer_handles {
        let writes = handle.join().expect("Thread panicked");
        total_writes += writes;
    }
    
    for handle in reader_handles {
        let (reads, errors) = handle.join().expect("Thread panicked");
        total_reads += reads;
        total_errors += errors;
    }
    
    println!("Data integrity test results:");
    println!("  Total writes: {}", total_writes);
    println!("  Total reads: {}", total_reads);
    println!("  Integrity errors: {}", total_errors);
    
    assert_eq!(total_errors, 0, "Found {} data integrity errors", total_errors);
    
    // Final verification
    let expected_data = operation_counts.lock().expect("Lock failed");
    let mut final_errors = 0;
    
    for (key, expected_value) in expected_data.iter() {
        if let Ok(Some(actual_value)) = db.get(key.as_bytes()) {
            if actual_value != expected_value.as_bytes() {
                final_errors += 1;
            }
        }
    }
    
    assert_eq!(final_errors, 0, "Final verification found {} mismatches", final_errors);
    println!("✓ All data integrity checks passed");
}

#[test]
fn stress_test_crash_recovery_simulation() {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().to_path_buf();
    
    // Simulate multiple crash/recovery cycles
    for cycle in 0..5 {
        println!("Crash recovery cycle {}", cycle + 1);
        
        {
            let mut config = LightningDbConfig::default();
            config.wal_sync_mode = WalSyncMode::Sync;
            let db = Database::create(&path, config).expect("Failed to create database");
            
            // Write data with specific patterns
            for i in 0..100 {
                let key = format!("crash_test_c{}_k{:03}", cycle, i);
                let value = format!("cycle_{}_value_{}_checksum_{:x}", cycle, i, i * cycle);
                db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
            }
            
            // Simulate transaction in progress
            let tx_id = db.begin_transaction().expect("Failed to begin transaction");
            for i in 100..150 {
                let key = format!("crash_test_c{}_k{:03}", cycle, i);
                let value = format!("tx_value_{}_pending", i);
                db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).expect("Put failed");
            }
            
            if cycle % 2 == 0 {
                // Commit some transactions
                db.commit_transaction(tx_id).expect("Commit failed");
            }
            // Otherwise, simulate crash by dropping db without commit
        }
        
        // Reopen and verify
        {
            let db = Database::open(&path, LightningDbConfig::default()).expect("Failed to reopen database");
            
            // Verify committed data from all cycles
            for c in 0..=cycle {
                for i in 0..100 {
                    let key = format!("crash_test_c{}_k{:03}", c, i);
                    let expected = format!("cycle_{}_value_{}_checksum_{:x}", c, i, i * c);
                    let result = db.get(key.as_bytes()).expect("Get failed");
                    assert_eq!(result, Some(expected.into_bytes()), 
                              "Data loss detected for key {} in cycle {}", key, c);
                }
                
                // Verify transactional data
                if c % 2 == 0 {
                    for i in 100..150 {
                        let key = format!("crash_test_c{}_k{:03}", c, i);
                        let result = db.get(key.as_bytes()).expect("Get failed");
                        assert!(result.is_some(), "Committed transaction data lost for key {}", key);
                    }
                } else {
                    // Uncommitted transactions should not be visible
                    for i in 100..150 {
                        let key = format!("crash_test_c{}_k{:03}", c, i);
                        let result = db.get(key.as_bytes()).expect("Get failed");
                        assert!(result.is_none(), "Uncommitted transaction data persisted for key {}", key);
                    }
                }
            }
        }
    }
    
    println!("✓ All crash recovery cycles completed successfully");
}

#[test]
fn stress_test_concurrent_transactions() {
    let dir = tempdir().expect("Failed to create temp dir");
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).expect("Failed to create database"));
    
    // Initialize shared counters
    for i in 0..100 {
        let key = format!("counter_{:03}", i);
        db.put(key.as_bytes(), b"0").expect("Put failed");
    }
    
    const NUM_THREADS: usize = 10;
    const TXS_PER_THREAD: usize = 50;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    
    let mut handles = vec![];
    
    for _thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let mut rng = rand::rng();
            let mut success_count = 0;
            let mut retry_count = 0;
            
            for _ in 0..TXS_PER_THREAD {
                let counter_id = rng.random_range(0..100);
                let key = format!("counter_{:03}", counter_id);
                
                let mut attempts = 0;
                const MAX_ATTEMPTS: u32 = 10;
                
                while attempts < MAX_ATTEMPTS {
                    attempts += 1;
                    
                    let tx_id = db_clone.begin_transaction().expect("Failed to begin transaction");
                    
                    match db_clone.get_tx(tx_id, key.as_bytes()) {
                        Ok(Some(value)) => {
                            let count: i32 = String::from_utf8_lossy(&value)
                                .parse()
                                .unwrap_or(0);
                            let new_count = count + 1;
                            
                            db_clone.put_tx(tx_id, key.as_bytes(), new_count.to_string().as_bytes())
                                .expect("Put failed");
                            
                            match db_clone.commit_transaction(tx_id) {
                                Ok(_) => {
                                    success_count += 1;
                                    break;
                                }
                                Err(_) => {
                                    retry_count += 1;
                                    thread::sleep(Duration::from_millis(attempts as u64));
                                }
                            }
                        }
                        _ => {
                            // Transaction will auto-rollback
                            retry_count += 1;
                        }
                    }
                }
            }
            
            (success_count, retry_count)
        });
        
        handles.push(handle);
    }
    
    let mut total_success = 0;
    let mut total_retries = 0;
    
    for handle in handles {
        let (success, retries) = handle.join().expect("Thread panicked");
        total_success += success;
        total_retries += retries;
    }
    
    println!("Concurrent transactions:");
    println!("  Successful: {}", total_success);
    println!("  Retries: {}", total_retries);
    println!("  Success rate: {:.1}%", total_success as f64 / (NUM_THREADS * TXS_PER_THREAD) as f64 * 100.0);
    
    // Verify final sum
    let mut total_count = 0;
    for i in 0..100 {
        let key = format!("counter_{:03}", i);
        if let Ok(Some(value)) = db.get(key.as_bytes()) {
            let count: i32 = String::from_utf8_lossy(&value).parse().unwrap_or(0);
            total_count += count;
        }
    }
    
    println!("  Total count: {} (expected: {})", total_count, total_success);
    assert_eq!(total_count, total_success as i32, "Transaction atomicity violation detected");
    println!("✓ Transaction atomicity verified");
}

#[test]
fn stress_test_memory_pressure() {
    let dir = tempdir().expect("Failed to create temp dir");
    let mut config = LightningDbConfig::default();
    config.cache_size = 128 * 1024; // Small cache to induce pressure
    config.page_size = 4096;
    
    let db = Database::create(dir.path(), config).expect("Failed to create database");
    
    const NUM_CYCLES: usize = 20;
    const KEYS_PER_CYCLE: usize = 500;
    const VALUE_SIZE: usize = 4096;
    
    let mut write_times = Vec::new();
    let mut read_times = Vec::new();
    
    for cycle in 0..NUM_CYCLES {
        // Write phase
        let start = Instant::now();
        for i in 0..KEYS_PER_CYCLE {
            let key = format!("mem_c{:02}_k{:04}", cycle, i);
            let value = vec![((cycle * i) % 256) as u8; VALUE_SIZE];
            db.put(key.as_bytes(), &value).expect("Put failed");
        }
        write_times.push(start.elapsed());
        
        // Read phase (with cache misses)
        let start = Instant::now();
        for prev_cycle in 0..=cycle {
            for i in (0..KEYS_PER_CYCLE).step_by(10) {
                let key = format!("mem_c{:02}_k{:04}", prev_cycle, i);
                if let Ok(Some(value)) = db.get(key.as_bytes()) {
                    // Verify data integrity
                    let expected_byte = ((prev_cycle * i) % 256) as u8;
                    assert_eq!(value[0], expected_byte, "Data corruption detected");
                    assert_eq!(value.len(), VALUE_SIZE, "Value size mismatch");
                }
            }
        }
        read_times.push(start.elapsed());
        
        // Delete some to create fragmentation
        if cycle % 3 == 0 {
            for i in (0..KEYS_PER_CYCLE).step_by(5) {
                let key = format!("mem_c{:02}_k{:04}", cycle, i);
                db.delete(key.as_bytes()).expect("Delete failed");
            }
        }
        
        // Force flush periodically
        if cycle % 5 == 0 {
            db.flush_lsm().expect("Flush failed");
        }
    }
    
    // Calculate statistics
    let avg_write: Duration = write_times.iter().sum::<Duration>() / write_times.len() as u32;
    let avg_read: Duration = read_times.iter().sum::<Duration>() / read_times.len() as u32;
    
    println!("Memory pressure test results:");
    println!("  Average write time per cycle: {:?}", avg_write);
    println!("  Average read time per cycle: {:?}", avg_read);
    
    let stats = db.stats();
    println!("  Final page count: {}", stats.page_count);
    println!("  Final free pages: {}", stats.free_page_count);
    println!("  Cache hit rate: {:.1}%", stats.cache_hit_rate.unwrap_or(0.0) * 100.0);
    
    // Final integrity check
    let mut verified = 0;
    let mut errors = 0;
    
    for cycle in 0..NUM_CYCLES {
        for i in 0..KEYS_PER_CYCLE {
            let key = format!("mem_c{:02}_k{:04}", cycle, i);
            
            // Check if it should exist (not deleted)
            let should_exist = !(cycle % 3 == 0 && i % 5 == 0);
            
            match db.get(key.as_bytes()) {
                Ok(Some(value)) if should_exist => {
                    let expected_byte = ((cycle * i) % 256) as u8;
                    if value[0] != expected_byte || value.len() != VALUE_SIZE {
                        errors += 1;
                    } else {
                        verified += 1;
                    }
                }
                Ok(None) if !should_exist => {
                    verified += 1;
                }
                _ => {
                    errors += 1;
                }
            }
        }
    }
    
    println!("  Verified entries: {}", verified);
    println!("  Errors: {}", errors);
    assert_eq!(errors, 0, "Found {} integrity errors under memory pressure", errors);
    println!("✓ All integrity checks passed under memory pressure");
}

#[test]
fn stress_test_large_dataset_integrity() {
    let dir = tempdir().expect("Failed to create temp dir");
    let db = Database::create(dir.path(), LightningDbConfig::default()).expect("Failed to create database");
    
    const BATCH_SIZE: usize = 1000;
    const NUM_BATCHES: usize = 10;
    
    println!("Testing large dataset integrity...");
    
    // Write phase with checksums
    for batch in 0..NUM_BATCHES {
        let start = Instant::now();
        
        for i in 0..BATCH_SIZE {
            let key = format!("large_{:03}_{:04}", batch, i);
            let checksum = calculate_checksum(batch, i);
            let value = format!("batch_{}_item_{}_checksum_{:08x}_data_{}", 
                              batch, i, checksum, "x".repeat(500));
            
            db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        }
        
        println!("  Batch {} written in {:?}", batch, start.elapsed());
        
        // Periodic flush
        if batch % 3 == 0 {
            db.flush_lsm().expect("Flush failed");
        }
    }
    
    // Verification phase
    println!("Verifying dataset integrity...");
    let mut errors = 0;
    
    for batch in 0..NUM_BATCHES {
        for i in 0..BATCH_SIZE {
            let key = format!("large_{:03}_{:04}", batch, i);
            
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let value_str = String::from_utf8_lossy(&value);
                    let expected_checksum = calculate_checksum(batch, i);
                    
                    if !value_str.contains(&format!("checksum_{:08x}", expected_checksum)) {
                        errors += 1;
                        println!("Checksum mismatch for key {}", key);
                    }
                    
                    if !value_str.starts_with(&format!("batch_{}_item_{}", batch, i)) {
                        errors += 1;
                        println!("Value format error for key {}", key);
                    }
                }
                _ => {
                    errors += 1;
                    println!("Missing or unreadable key: {}", key);
                }
            }
        }
    }
    
    let total_entries = NUM_BATCHES * BATCH_SIZE;
    println!("  Total entries: {}", total_entries);
    println!("  Errors found: {}", errors);
    
    assert_eq!(errors, 0, "Dataset integrity check failed with {} errors", errors);
    println!("✓ Large dataset integrity verified successfully");
}

fn calculate_checksum(batch: usize, item: usize) -> u32 {
    // Simple checksum calculation
    ((batch as u32) * 1000000 + (item as u32) * 1000) ^ 0xDEADBEEF
}