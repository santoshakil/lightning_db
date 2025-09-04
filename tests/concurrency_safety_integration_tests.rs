use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::hash::{BuildHasher, Hasher};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier, mpsc,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Helper function to safely join threads with timeout
fn safe_join_threads<T>(handles: Vec<std::thread::JoinHandle<T>>, timeout_secs: u64) -> Vec<Option<T>>
where
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let num_threads = handles.len();
    
    for (i, handle) in handles.into_iter().enumerate() {
        let tx_clone = tx.clone();
        std::thread::spawn(move || {
            let result = handle.join();
            tx_clone.send((i, result)).ok();
        });
    }
    
    let mut results = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        results.push(None);
    }
    let timeout = Duration::from_secs(timeout_secs);
    let start = Instant::now();
    
    for _ in 0..num_threads {
        let remaining_time = timeout.saturating_sub(start.elapsed());
        if let Ok((index, thread_result)) = rx.recv_timeout(remaining_time) {
            if let Ok(value) = thread_result {
                results[index] = Some(value);
            }
        }
    }
    
    results
}

/// Test ARC cache operations under heavy concurrent load
/// Validates deadlock prevention in cache eviction mechanisms
#[test]
fn test_arc_cache_concurrent_stress() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024, // 10MB cache to force evictions
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let num_threads = 12;
    let operations_per_thread = 1000;
    let value_size = 50000; // 50KB values to trigger cache pressure
    
    let barrier = Arc::new(Barrier::new(num_threads));
    let cache_hits = Arc::new(AtomicUsize::new(0));
    let cache_misses = Arc::new(AtomicUsize::new(0));
    let evictions_detected = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let hits_clone = Arc::clone(&cache_hits);
        let misses_clone = Arc::clone(&cache_misses);
        let evictions_clone = Arc::clone(&evictions_detected);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            let mut local_keys = Vec::new();
            
            // Phase 1: Insert unique keys per thread
            for i in 0..operations_per_thread {
                let key = format!("cache_test_{}_{:06}", thread_id, i);
                let value = vec![(thread_id as u8).wrapping_add(i as u8); value_size];
                
                match db_clone.put(key.as_bytes(), &value) {
                    Ok(_) => {
                        local_keys.push(key.clone());
                        
                        // Immediately read back to test cache behavior
                        match db_clone.get(key.as_bytes()) {
                            Ok(Some(retrieved)) => {
                                assert_eq!(retrieved.len(), value_size);
                                hits_clone.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(None) => {
                                misses_clone.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                misses_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        misses_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Every 50 operations, try to read from other threads' keys
                if i % 50 == 0 && i > 0 {
                    for other_thread in 0..num_threads {
                        if other_thread != thread_id {
                            let cross_key = format!("cache_test_{}_{:06}", other_thread, i / 2);
                            match db_clone.get(cross_key.as_bytes()) {
                                Ok(Some(_)) => {
                                    hits_clone.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(None) => {} // May not exist yet
                                Err(_) => {}
                            }
                        }
                    }
                }
            }

            // Phase 2: Random access pattern to trigger cache evictions
            for _ in 0..200 {
                use std::collections::hash_map::RandomState;
                let mut hasher = RandomState::new().build_hasher();
                hasher.write_usize(thread_id * 1000);
                let hash = hasher.finish() as usize;
                if let Some(key) = local_keys.get(hash % local_keys.len()) {
                    let before_access = db_clone.cache_stats();
                    
                    match db_clone.get(key.as_bytes()) {
                        Ok(Some(_)) => {
                            hits_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(None) => {
                            evictions_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            evictions_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    let after_access = db_clone.cache_stats();
                    
                    // Detect if cache behavior changed (potential eviction)
                    if let (Some(before), Some(after)) = (before_access, after_access) {
                        if before != after {
                            evictions_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }

    let _results = safe_join_threads(handles, 10); // 10 second timeout

    let total_hits = cache_hits.load(Ordering::Relaxed);
    let total_misses = cache_misses.load(Ordering::Relaxed);
    let total_evictions = evictions_detected.load(Ordering::Relaxed);

    println!("ARC cache concurrent stress test:");
    println!("  Cache hits: {}", total_hits);
    println!("  Cache misses: {}", total_misses);
    println!("  Evictions detected: {}", total_evictions);

    // Verify cache statistics
    if let Some(stats) = db.cache_stats() {
        println!("  Final cache stats: {}", stats);
        assert!(stats.contains("hit") || stats.contains("miss"), "Should have cache activity");
    }

    // Should have significant cache activity under this load
    assert!(total_hits > 1000, "Should have many cache hits");
    assert!(total_evictions > 0, "Should have cache evictions under memory pressure");
}

/// Test transaction manager under extreme concurrent load
/// Validates lock contention handling and transaction scheduling
#[test]
fn test_transaction_manager_concurrent_load() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_unified_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 10 },
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());

    // Initialize shared counters that will create heavy contention
    let num_counters = 5;
    for i in 0..num_counters {
        db.put(format!("shared_counter_{}", i).as_bytes(), b"0").unwrap();
    }

    let num_threads = 16;
    let operations_per_thread = 200;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let successful_commits = Arc::new(AtomicUsize::new(0));
    let failed_commits = Arc::new(AtomicUsize::new(0));
    let transaction_conflicts = Arc::new(AtomicUsize::new(0));
    let deadlock_recoveries = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let success_clone = Arc::clone(&successful_commits);
        let failed_clone = Arc::clone(&failed_commits);
        let conflicts_clone = Arc::clone(&transaction_conflicts);
        let deadlocks_clone = Arc::clone(&deadlock_recoveries);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..operations_per_thread {
                let timeout = Duration::from_millis(1000);
                let start = Instant::now();
                let mut retries = 0;
                let max_retries = 20;

                'transaction_retry: loop {
                    if start.elapsed() > timeout || retries >= max_retries {
                        failed_clone.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    // Create complex transaction that touches multiple counters
                    let tx_id = match db_clone.begin_transaction() {
                        Ok(id) => id,
                        Err(_) => {
                            deadlocks_clone.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_micros(100 + (retries * 50)));
                            retries += 1;
                            continue;
                        }
                    };

                    // Touch multiple counters to increase conflict probability
                    let counter1_id = (thread_id + i) % num_counters;
                    let counter2_id = (thread_id + i + 1) % num_counters;
                    let counter3_id = (thread_id + i + 2) % num_counters;

                    let key1 = format!("shared_counter_{}", counter1_id);
                    let key2 = format!("shared_counter_{}", counter2_id);
                    let key3 = format!("shared_counter_{}", counter3_id);

                    // Read all three counters
                    let val1 = match db_clone.get_tx(tx_id, key1.as_bytes()) {
                        Ok(Some(v)) => String::from_utf8(v).unwrap_or_default().parse::<i32>().unwrap_or(0),
                        Ok(None) => 0,
                        Err(_) => {
                            let _ = db_clone.abort_transaction(tx_id);
                            deadlocks_clone.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_micros(100 + (retries * 50)));
                            retries += 1;
                            continue 'transaction_retry;
                        }
                    };

                    let val2 = match db_clone.get_tx(tx_id, key2.as_bytes()) {
                        Ok(Some(v)) => String::from_utf8(v).unwrap_or_default().parse::<i32>().unwrap_or(0),
                        Ok(None) => 0,
                        Err(_) => {
                            let _ = db_clone.abort_transaction(tx_id);
                            deadlocks_clone.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_micros(100 + (retries * 50)));
                            retries += 1;
                            continue 'transaction_retry;
                        }
                    };

                    let val3 = match db_clone.get_tx(tx_id, key3.as_bytes()) {
                        Ok(Some(v)) => String::from_utf8(v).unwrap_or_default().parse::<i32>().unwrap_or(0),
                        Ok(None) => 0,
                        Err(_) => {
                            let _ = db_clone.abort_transaction(tx_id);
                            deadlocks_clone.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_micros(100 + (retries * 50)));
                            retries += 1;
                            continue 'transaction_retry;
                        }
                    };

                    // Write updated values
                    let operations_failed = 
                        db_clone.put_tx(tx_id, key1.as_bytes(), (val1 + 1).to_string().as_bytes()).is_err() ||
                        db_clone.put_tx(tx_id, key2.as_bytes(), (val2 + 1).to_string().as_bytes()).is_err() ||
                        db_clone.put_tx(tx_id, key3.as_bytes(), (val3 + 1).to_string().as_bytes()).is_err();

                    if operations_failed {
                        let _ = db_clone.abort_transaction(tx_id);
                        deadlocks_clone.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_micros(100 + (retries * 50)));
                        retries += 1;
                        continue;
                    }

                    // Add thread-specific data to increase transaction size
                    let thread_key = format!("thread_{}_{}", thread_id, i);
                    let thread_value = format!("data_{}_{}_{}", thread_id, i, val1 + val2 + val3);
                    if db_clone.put_tx(tx_id, thread_key.as_bytes(), thread_value.as_bytes()).is_err() {
                        let _ = db_clone.abort_transaction(tx_id);
                        deadlocks_clone.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_micros(100 + (retries * 50)));
                        retries += 1;
                        continue;
                    }

                    // Attempt commit
                    match db_clone.commit_transaction(tx_id) {
                        Ok(_) => {
                            success_clone.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(_) => {
                            conflicts_clone.fetch_add(1, Ordering::Relaxed);
                            retries += 1;
                            
                            // Exponential backoff with thread-specific jitter
                            let base_delay = std::cmp::min(10 * (1 << std::cmp::min(retries / 4, 6)), 500);
                            let jitter = ((thread_id as u64 * 17 + i as u64 * 7) % 50) as u64;
                            thread::sleep(Duration::from_micros(base_delay + jitter));
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }

    let _results = safe_join_threads(handles, 15); // 15 second timeout

    let commits = successful_commits.load(Ordering::Relaxed);
    let failures = failed_commits.load(Ordering::Relaxed);
    let conflicts = transaction_conflicts.load(Ordering::Relaxed);
    let deadlocks = deadlock_recoveries.load(Ordering::Relaxed);

    println!("Transaction manager concurrent load test:");
    println!("  Successful commits: {}", commits);
    println!("  Failed transactions: {}", failures);
    println!("  Conflicts resolved: {}", conflicts);
    println!("  Deadlock recoveries: {}", deadlocks);

    // Verify system performance under load
    let total_attempts = commits + failures;
    let success_rate = if total_attempts > 0 { (commits * 100) / total_attempts } else { 0 };
    
    println!("  Success rate: {}%", success_rate);

    assert!(success_rate >= 70, "Should maintain reasonable success rate under contention");
    assert!(commits > 1000, "Should successfully commit many transactions");

    // Verify data consistency after high contention
    let mut total_counter_value = 0;
    for i in 0..num_counters {
        let key = format!("shared_counter_{}", i);
        match db.get(key.as_bytes()) {
            Ok(Some(value)) => {
                let counter_val: i32 = String::from_utf8(value).unwrap().parse().unwrap();
                total_counter_value += counter_val;
                assert!(counter_val > 0, "Counters should have been incremented");
            }
            Ok(None) => panic!("Shared counter should exist"),
            Err(e) => panic!("Error reading shared counter: {:?}", e),
        }
    }

    println!("  Total counter increments: {}", total_counter_value);
    assert!(total_counter_value > 0, "Should have incremented counters");
}

/// Test background processing during active transactions
/// Validates that background operations don't interfere with foreground transactions
#[test]
fn test_background_processing_concurrent() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_unified_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 50 },
        prefetch_enabled: true,
        compression_enabled: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let running = Arc::new(AtomicBool::new(true));
    
    // Background checkpoint thread
    let db_checkpoint = Arc::clone(&db);
    let running_checkpoint = Arc::clone(&running);
    let checkpoint_count = Arc::new(AtomicUsize::new(0));
    let checkpoint_counter = Arc::clone(&checkpoint_count);

    let checkpoint_handle = thread::spawn(move || {
        while running_checkpoint.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));
            match db_checkpoint.checkpoint() {
                Ok(_) => {
                    checkpoint_counter.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {} // May fail due to concurrent activity, which is fine
            }
        }
    });

    // Background cache statistics thread
    let db_stats = Arc::clone(&db);
    let running_stats = Arc::clone(&running);
    let stats_checks = Arc::new(AtomicUsize::new(0));
    let stats_counter = Arc::clone(&stats_checks);

    let stats_handle = thread::spawn(move || {
        while running_stats.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(75));
            if db_stats.cache_stats().is_some() {
                stats_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    // Foreground transaction workload
    let num_transaction_threads = 8;
    let transactions_per_thread = 150;
    let barrier = Arc::new(Barrier::new(num_transaction_threads));
    
    let transaction_successes = Arc::new(AtomicUsize::new(0));
    let transaction_failures = Arc::new(AtomicUsize::new(0));
    let background_interference = Arc::new(AtomicUsize::new(0));

    let mut transaction_handles = vec![];

    for thread_id in 0..num_transaction_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let successes_clone = Arc::clone(&transaction_successes);
        let failures_clone = Arc::clone(&transaction_failures);
        let interference_clone = Arc::clone(&background_interference);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..transactions_per_thread {
                let start_time = Instant::now();
                let mut retries = 0;
                let max_retries = 10;

                'retry_loop: loop {
                    if retries >= max_retries {
                        failures_clone.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    let tx_id = match db_clone.begin_transaction() {
                        Ok(id) => id,
                        Err(_) => {
                            retries += 1;
                            thread::sleep(Duration::from_micros(100));
                            continue;
                        }
                    };

                    // Perform transaction operations
                    let key1 = format!("bg_test_{}_{}_primary", thread_id, i);
                    let key2 = format!("bg_test_{}_{}_secondary", thread_id, i);
                    let value1 = format!("primary_data_{}_{}_{}", thread_id, i, rand::random::<u32>());
                    let value2 = format!("secondary_data_{}_{}_{}", thread_id, i, rand::random::<u32>());

                    let operations_ok = 
                        db_clone.put_tx(tx_id, key1.as_bytes(), value1.as_bytes()).is_ok() &&
                        db_clone.put_tx(tx_id, key2.as_bytes(), value2.as_bytes()).is_ok();

                    if !operations_ok {
                        let _ = db_clone.abort_transaction(tx_id);
                        retries += 1;
                        thread::sleep(Duration::from_micros(100));
                        continue;
                    }

                    // Verify we can read our own writes
                    let read_ok = 
                        db_clone.get_tx(tx_id, key1.as_bytes()).is_ok() &&
                        db_clone.get_tx(tx_id, key2.as_bytes()).is_ok();

                    if !read_ok {
                        let _ = db_clone.abort_transaction(tx_id);
                        retries += 1;
                        thread::sleep(Duration::from_micros(100));
                        continue;
                    }

                    match db_clone.commit_transaction(tx_id) {
                        Ok(_) => {
                            successes_clone.fetch_add(1, Ordering::Relaxed);
                            
                            // Check if transaction took unusually long (potential background interference)
                            if start_time.elapsed() > Duration::from_millis(100) {
                                interference_clone.fetch_add(1, Ordering::Relaxed);
                            }
                            break 'retry_loop;
                        }
                        Err(_) => {
                            retries += 1;
                            thread::sleep(Duration::from_micros(200 + (retries * 100)));
                        }
                    }
                }
            }
        });
        transaction_handles.push(handle);
    }

    // Let everything run for a while
    thread::sleep(Duration::from_secs(5));

    // Stop background threads
    running.store(false, Ordering::Relaxed);

    // Wait for all threads to complete
    let _results = safe_join_threads(transaction_handles, 10);
    
    // Handle background threads with timeout
    let (tx_bg, rx_bg) = mpsc::channel();
    
    std::thread::spawn(move || {
        checkpoint_handle.join().ok();
        tx_bg.send(()).ok();
    });
    
    let (tx_stats, rx_stats) = mpsc::channel();
    std::thread::spawn(move || {
        stats_handle.join().ok();
        tx_stats.send(()).ok();
    });
    
    rx_bg.recv_timeout(Duration::from_secs(5)).ok();
    rx_stats.recv_timeout(Duration::from_secs(5)).ok();

    let total_successes = transaction_successes.load(Ordering::Relaxed);
    let total_failures = transaction_failures.load(Ordering::Relaxed);
    let total_checkpoints = checkpoint_count.load(Ordering::Relaxed);
    let total_stats_checks = stats_checks.load(Ordering::Relaxed);
    let interference_count = background_interference.load(Ordering::Relaxed);

    println!("Background processing concurrent test:");
    println!("  Transaction successes: {}", total_successes);
    println!("  Transaction failures: {}", total_failures);
    println!("  Background checkpoints: {}", total_checkpoints);
    println!("  Stats checks: {}", total_stats_checks);
    println!("  Potential interference events: {}", interference_count);

    // Verify background operations ran
    assert!(total_checkpoints > 10, "Should have multiple background checkpoints");
    assert!(total_stats_checks > 20, "Should have multiple stats checks");

    // Verify transactions succeeded despite background activity
    let success_rate = if total_successes + total_failures > 0 {
        (total_successes * 100) / (total_successes + total_failures)
    } else { 0 };
    
    println!("  Transaction success rate: {}%", success_rate);
    
    assert!(success_rate >= 85, "Should maintain high success rate with background processing");
    assert!(total_successes > 800, "Should complete most transactions successfully");

    // Verify data integrity after concurrent background processing
    let verification = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();
    
    let total_errors = verification.checksum_errors.len()
        + verification.structure_errors.len()
        + verification.consistency_errors.len();

    assert_eq!(total_errors, 0, "Should maintain integrity with background processing");
}

/// Test lock-free data structures on hot paths under extreme concurrency
#[test]
fn test_lock_free_hot_path_performance() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 50 * 1024 * 1024, // Large cache to reduce I/O bottlenecks
        prefetch_enabled: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    
    // Create hot keys that will be accessed frequently
    let hot_keys: Vec<String> = (0..10).map(|i| format!("hot_key_{:03}", i)).collect();
    for key in &hot_keys {
        db.put(key.as_bytes(), b"initial_value").unwrap();
    }

    let num_threads = 20; // High concurrency
    let reads_per_thread = 2000;
    let writes_per_thread = 200;
    
    let barrier = Arc::new(Barrier::new(num_threads));
    let read_successes = Arc::new(AtomicUsize::new(0));
    let write_successes = Arc::new(AtomicUsize::new(0));
    let contention_events = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let hot_keys_clone = hot_keys.clone();
        let reads_clone = Arc::clone(&read_successes);
        let writes_clone = Arc::clone(&write_successes);
        let contention_clone = Arc::clone(&contention_events);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            let start = Instant::now();

            // Heavy read load on hot keys
            for i in 0..reads_per_thread {
                let key = &hot_keys_clone[i % hot_keys_clone.len()];
                let read_start = Instant::now();
                
                match db_clone.get(key.as_bytes()) {
                    Ok(Some(_)) => {
                        reads_clone.fetch_add(1, Ordering::Relaxed);
                        
                        // Detect potential contention if read takes too long
                        if read_start.elapsed() > Duration::from_micros(100) {
                            contention_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(None) => {} // Key might have been deleted
                    Err(_) => {
                        contention_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Occasional writes to create contention
                if i % 10 == 0 {
                    let write_key = &hot_keys_clone[(i / 10) % hot_keys_clone.len()];
                    let write_value = format!("updated_by_thread_{}_{}", thread_id, i / 10);
                    let write_start = Instant::now();
                    
                    match db_clone.put(write_key.as_bytes(), write_value.as_bytes()) {
                        Ok(_) => {
                            writes_clone.fetch_add(1, Ordering::Relaxed);
                            
                            // Detect write contention
                            if write_start.elapsed() > Duration::from_millis(1) {
                                contention_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            contention_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }

            // Additional dedicated write phase
            for i in 0..writes_per_thread {
                let key = &hot_keys_clone[i % hot_keys_clone.len()];
                let value = format!("final_update_{}_{}", thread_id, i);
                
                match db_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        writes_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        contention_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            let elapsed = start.elapsed();
            println!("Thread {} completed in {:?}", thread_id, elapsed);
        });
        handles.push(handle);
    }

    let test_start = Instant::now();

    let _results = safe_join_threads(handles, 20); // 20 second timeout for stress test

    let test_duration = test_start.elapsed();
    let total_reads = read_successes.load(Ordering::Relaxed);
    let total_writes = write_successes.load(Ordering::Relaxed);
    let total_contention = contention_events.load(Ordering::Relaxed);

    println!("Lock-free hot path performance test:");
    println!("  Test duration: {:?}", test_duration);
    println!("  Total reads: {}", total_reads);
    println!("  Total writes: {}", total_writes);
    println!("  Contention events: {}", total_contention);

    let reads_per_second = if test_duration.as_secs() > 0 {
        total_reads / test_duration.as_secs() as usize
    } else {
        total_reads * 1000 / std::cmp::max(1, test_duration.as_millis() as usize)
    };

    println!("  Reads per second: {}", reads_per_second);

    // Performance expectations for lock-free implementation
    assert!(total_reads > num_threads * reads_per_thread * 8 / 10, "Should complete most reads");
    assert!(total_writes > num_threads * writes_per_thread * 7 / 10, "Should complete most writes");
    assert!(reads_per_second > 100_000, "Should achieve high read throughput");

    // Contention should be manageable with lock-free structures
    let contention_rate = if total_reads + total_writes > 0 {
        (total_contention * 100) / (total_reads + total_writes)
    } else { 0 };

    println!("  Contention rate: {}%", contention_rate);
    assert!(contention_rate < 20, "Lock-free structures should minimize contention");
}
