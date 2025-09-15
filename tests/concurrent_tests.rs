use lightning_db::{Database, LightningDbConfig};
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

mod common;
use common::{TestDatabase, TestDbConfig, FAST_TEST_SIZE, MEDIUM_TEST_SIZE};

#[test]
fn test_concurrent_reads_writes() {
    let test_db = TestDatabase::new(TestDbConfig {
        use_memory: true, // Use in-memory for fast concurrent tests
        cache_size: 20 * 1024 * 1024, // Reduced cache size
        max_active_transactions: 50, // Reduced transactions
    });
    let db = Arc::new(test_db.db);

    let num_writers = 2; // Reduced for faster tests
    let num_readers = 4; // Reduced for faster tests
    let ops_per_thread = FAST_TEST_SIZE; // Use common constant
    let barrier = Arc::new(Barrier::new(num_writers + num_readers));

    let write_counter = Arc::new(AtomicUsize::new(0));
    let read_counter = Arc::new(AtomicUsize::new(0));
    let error_flag = Arc::new(AtomicBool::new(false));

    let mut handles = vec![];

    // Writers
    for writer_id in 0..num_writers {
        let db = db.clone();
        let barrier = barrier.clone();
        let counter = write_counter.clone();
        let error_flag = error_flag.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            for i in 0..ops_per_thread {
                let key = format!("writer_{}_key_{}", writer_id, i);
                let value = format!("value_{}_{}", writer_id, i);

                if let Err(e) = db.put(key.as_bytes(), value.as_bytes()) {
                    eprintln!("Write error: {:?}", e);
                    error_flag.store(true, Ordering::SeqCst);
                    break;
                }

                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Readers
    for reader_id in 0..num_readers {
        let db = db.clone();
        let barrier = barrier.clone();
        let counter = read_counter.clone();
        let error_flag = error_flag.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            for _ in 0..ops_per_thread {
                let writer_id = reader_id % num_writers;
                let key_id = fastrand::usize(..ops_per_thread);
                let key = format!("writer_{}_key_{}", writer_id, key_id);

                match db.get(key.as_bytes()) {
                    Ok(_) => {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Read error: {:?}", e);
                        error_flag.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(
        !error_flag.load(Ordering::SeqCst),
        "Errors occurred during concurrent operations"
    );

    let total_writes = write_counter.load(Ordering::SeqCst);
    let total_reads = read_counter.load(Ordering::SeqCst);

    println!("Total writes: {}", total_writes);
    println!("Total reads: {}", total_reads);

    assert_eq!(total_writes, num_writers * ops_per_thread);
    assert_eq!(total_reads, num_readers * ops_per_thread);
}

#[test]
fn test_transaction_thread_safety() {
    let dir = tempdir().unwrap();
    let db = Arc::new(
        Database::create(
            dir.path(),
            LightningDbConfig {
                max_active_transactions: 50,
                ..Default::default()
            },
        )
        .unwrap(),
    );

    let num_threads = 20;
    let txns_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_counter = Arc::new(AtomicUsize::new(0));
    let abort_counter = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            let success_counter = success_counter.clone();
            let abort_counter = abort_counter.clone();

            thread::spawn(move || {
                barrier.wait();

                for txn_id in 0..txns_per_thread {
                    let tx = match db.begin_transaction() {
                        Ok(tx) => tx,
                        Err(_) => {
                            abort_counter.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let key = format!("thread_{}_txn_{}", thread_id, txn_id);
                    let value = format!("tx_value_{}_{}", thread_id, txn_id);

                    // Perform multiple operations in transaction
                    let mut all_ok = true;
                    for op in 0..5 {
                        let op_key = format!("{}_op_{}", key, op);
                        if db.put_tx(tx, op_key.as_bytes(), value.as_bytes()).is_err() {
                            all_ok = false;
                            break;
                        }
                    }

                    if all_ok {
                        if db.commit_transaction(tx).is_ok() {
                            success_counter.fetch_add(1, Ordering::Relaxed);
                        } else {
                            abort_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        let _ = db.abort_transaction(tx);
                        abort_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let total_success = success_counter.load(Ordering::SeqCst);
    let total_abort = abort_counter.load(Ordering::SeqCst);

    println!("Successful transactions: {}", total_success);
    println!("Aborted transactions: {}", total_abort);

    assert!(
        total_success > 0,
        "At least some transactions should succeed"
    );
    assert_eq!(
        total_success + total_abort,
        num_threads * txns_per_thread,
        "All transactions should be accounted for"
    );
}

#[test]
#[ignore] // TODO: Fix transaction conflict handling
fn test_safe_counter_with_transactions() {
    let dir = tempdir().unwrap();
    let db = Arc::new(
        Database::create(
            dir.path(),
            LightningDbConfig {
                max_active_transactions: 20,
                ..Default::default()
            },
        )
        .unwrap(),
    );

    // Shared counter key
    let counter_key = b"safe_counter";
    db.put(counter_key, b"0").unwrap();

    let num_threads = 10;
    let increments_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));
    let retry_counter = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            let retry_counter = retry_counter.clone();

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..increments_per_thread {
                    let mut attempts = 0;
                    const MAX_ATTEMPTS: u32 = 100;

                    while attempts < MAX_ATTEMPTS {
                        attempts += 1;
                        let tx = db.begin_transaction().unwrap();

                        // Read current value
                        let current = match db.get_tx(tx, counter_key) {
                            Ok(Some(val)) => val,
                            Ok(None) => {
                                let _ = db.abort_transaction(tx);
                                retry_counter.fetch_add(1, Ordering::Relaxed);
                                thread::sleep(Duration::from_millis(attempts as u64));
                                continue;
                            }
                            Err(_) => {
                                let _ = db.abort_transaction(tx);
                                retry_counter.fetch_add(1, Ordering::Relaxed);
                                thread::sleep(Duration::from_millis(attempts as u64));
                                continue;
                            }
                        };

                        let val = std::str::from_utf8(&current)
                            .unwrap()
                            .parse::<i32>()
                            .unwrap();

                        // Increment
                        let new_val = (val + 1).to_string();

                        // Try to update - handle write-write conflicts
                        if let Err(_) = db.put_tx(tx, counter_key, new_val.as_bytes()) {
                            let _ = db.abort_transaction(tx);
                            retry_counter.fetch_add(1, Ordering::Relaxed);
                            thread::sleep(Duration::from_millis(attempts as u64));
                            continue;
                        }

                        // Try to commit
                        match db.commit_transaction(tx) {
                            Ok(_) => break,
                            Err(_) => {
                                // Retry on conflict with backoff
                                retry_counter.fetch_add(1, Ordering::Relaxed);
                                thread::sleep(Duration::from_millis(attempts as u64));
                                continue;
                            }
                        }
                    }

                    if attempts >= MAX_ATTEMPTS {
                        eprintln!(
                            "Thread failed to complete increment after {} attempts",
                            MAX_ATTEMPTS
                        );
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Check final value - with transactions, should be exactly correct
    let final_value = db.get(counter_key).unwrap().unwrap();
    let final_count = std::str::from_utf8(&final_value)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    let total_retries = retry_counter.load(Ordering::SeqCst);

    println!("Expected count: {}", num_threads * increments_per_thread);
    println!("Actual count: {}", final_count);
    println!("Total retries: {}", total_retries);

    assert_eq!(
        final_count,
        (num_threads * increments_per_thread) as i32,
        "With transactions, counter should be exact"
    );
}

#[test]
fn test_stress_concurrent_writes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

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
                db_clone
                    .put(key.as_bytes(), value.as_bytes())
                    .expect("Put failed");
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

    println!(
        "Concurrent writes: {} ops in {:?} ({:.0} ops/sec)",
        total_ops, elapsed, ops_per_sec
    );

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
                    }
                }
                _ => errors += 1,
            }
        }
    }

    assert_eq!(errors, 0, "Found {} data integrity errors", errors);
    println!("✓ All {} entries verified successfully", total_ops);
}

#[test]
fn test_data_integrity_under_load() {
    let dir = tempdir().unwrap();
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = lightning_db::WalSyncMode::Sync;

    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const NUM_THREADS: usize = 10;
    const TEST_DURATION_SECS: u64 = 3;
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
            let mut rng = rand::thread_rng();
            let mut write_count = 0;

            while !*stop_flag_clone.lock().unwrap() {
                let key_id = rng.gen_range(0..1000);
                let key = format!("integrity_key_{:05}", key_id);
                let value = format!("value_{}_thread_{}_seq_{}", key_id, thread_id, write_count);

                if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                    write_count += 1;
                    counts_clone.lock().unwrap().insert(key.clone(), value);
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

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let mut rng = rand::thread_rng();
            let mut read_count = 0;
            let mut integrity_errors = 0;

            while !*stop_flag_clone.lock().unwrap() {
                let key_id = rng.gen_range(0..1000);
                let key = format!("integrity_key_{:05}", key_id);

                if let Ok(Some(value)) = db_clone.get(key.as_bytes()) {
                    read_count += 1;

                    // Verify value format
                    let value_str = String::from_utf8_lossy(&value);
                    if !value_str.starts_with(&format!("value_{}_", key_id)) {
                        integrity_errors += 1;
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
    *stop_flag.lock().unwrap() = true;

    let mut total_writes = 0;
    let mut total_reads = 0;
    let mut total_errors = 0;

    for handle in writer_handles {
        let writes = handle.join().unwrap();
        total_writes += writes;
    }

    for handle in reader_handles {
        let (reads, errors) = handle.join().unwrap();
        total_reads += reads;
        total_errors += errors;
    }

    println!("Data integrity test results:");
    println!("  Total writes: {}", total_writes);
    println!("  Total reads: {}", total_reads);
    println!("  Integrity errors: {}", total_errors);

    assert_eq!(
        total_errors, 0,
        "Found {} data integrity errors",
        total_errors
    );
}

#[test]
#[ignore] // TODO: Fix transaction conflict handling
fn test_concurrent_transactions_stress() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    // Initialize shared counters
    for i in 0..100 {
        let key = format!("counter_{:03}", i);
        db.put(key.as_bytes(), b"0").unwrap();
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
            let mut rng = rand::thread_rng();
            let mut success_count = 0;
            let mut retry_count = 0;

            for _ in 0..TXS_PER_THREAD {
                let counter_id = rng.gen_range(0..100);
                let key = format!("counter_{:03}", counter_id);

                let mut attempts = 0;
                const MAX_ATTEMPTS: u32 = 10;

                while attempts < MAX_ATTEMPTS {
                    attempts += 1;

                    let tx_id = db_clone.begin_transaction().unwrap();

                    match db_clone.get_tx(tx_id, key.as_bytes()) {
                        Ok(Some(value)) => {
                            let count: i32 = String::from_utf8_lossy(&value).parse().unwrap_or(0);
                            let new_count = count + 1;

                            // Handle write-write conflicts
                            if let Err(_) = db_clone.put_tx(
                                tx_id,
                                key.as_bytes(),
                                new_count.to_string().as_bytes(),
                            ) {
                                let _ = db_clone.abort_transaction(tx_id);
                                retry_count += 1;
                                thread::sleep(Duration::from_millis(attempts as u64));
                                continue;
                            }

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
        let (success, retries) = handle.join().unwrap();
        total_success += success;
        total_retries += retries;
    }

    println!("Concurrent transactions:");
    println!("  Successful: {}", total_success);
    println!("  Retries: {}", total_retries);
    println!(
        "  Success rate: {:.1}%",
        total_success as f64 / (NUM_THREADS * TXS_PER_THREAD) as f64 * 100.0
    );

    // Verify final sum
    let mut total_count = 0;
    for i in 0..100 {
        let key = format!("counter_{:03}", i);
        if let Ok(Some(value)) = db.get(key.as_bytes()) {
            let count: i32 = String::from_utf8_lossy(&value).parse().unwrap_or(0);
            total_count += count;
        }
    }

    println!(
        "  Total count: {} (expected: {})",
        total_count, total_success
    );
    assert_eq!(
        total_count, total_success as i32,
        "Transaction atomicity violation detected"
    );
}

#[test]
fn test_deadlock_prevention() {
    let dir = tempdir().unwrap();
    let db = Arc::new(
        Database::create(
            dir.path(),
            LightningDbConfig {
                max_active_transactions: 10,
                ..Default::default()
            },
        )
        .unwrap(),
    );

    // Create two keys that transactions will access in different orders
    db.put(b"lock_a", b"0").unwrap();
    db.put(b"lock_b", b"0").unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let deadlock_detected = Arc::new(AtomicBool::new(false));

    let db1 = db.clone();
    let b1 = barrier.clone();
    let dd1 = deadlock_detected.clone();

    let handle1 = thread::spawn(move || {
        for _ in 0..10 {
            let tx = db1.begin_transaction().unwrap();

            // Access lock_a first, then lock_b
            db1.put_tx(tx, b"lock_a", b"1").unwrap();
            b1.wait();
            thread::sleep(Duration::from_millis(10));

            if db1.put_tx(tx, b"lock_b", b"1").is_err() {
                dd1.store(true, Ordering::SeqCst);
                let _ = db1.abort_transaction(tx);
            } else {
                let _ = db1.commit_transaction(tx);
            }
        }
    });

    let db2 = db.clone();
    let b2 = barrier.clone();
    let dd2 = deadlock_detected.clone();

    let handle2 = thread::spawn(move || {
        for _ in 0..10 {
            let tx = db2.begin_transaction().unwrap();

            // Access lock_b first, then lock_a (opposite order)
            db2.put_tx(tx, b"lock_b", b"2").unwrap();
            b2.wait();
            thread::sleep(Duration::from_millis(10));

            if db2.put_tx(tx, b"lock_a", b"2").is_err() {
                dd2.store(true, Ordering::SeqCst);
                let _ = db2.abort_transaction(tx);
            } else {
                let _ = db2.commit_transaction(tx);
            }
        }
    });

    // Set a timeout to prevent actual deadlock in test
    let start = Instant::now();
    let timeout = Duration::from_secs(5);

    while !handle1.is_finished() && !handle2.is_finished() {
        if start.elapsed() > timeout {
            println!("Test timed out - possible deadlock");
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Clean up threads if they're still running
    let _ = handle1.join();
    let _ = handle2.join();

    // The system should either detect deadlock or complete without hanging
    println!(
        "Deadlock detected: {}",
        deadlock_detected.load(Ordering::SeqCst)
    );
}

#[test]
#[ignore] // TODO: Fix memory pressure test
fn test_memory_pressure() {
    let dir = tempdir().unwrap();
    let mut config = LightningDbConfig::default();
    config.cache_size = 128 * 1024; // Small cache to induce pressure
    config.page_size = 4096;

    let db = Database::create(dir.path(), config).unwrap();

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
            db.put(key.as_bytes(), &value).unwrap();
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
                db.delete(key.as_bytes()).unwrap();
            }
        }

        // Force flush periodically
        if cycle % 5 == 0 {
            db.flush_lsm().unwrap();
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
    println!(
        "  Cache hit rate: {:.1}%",
        stats.cache_hit_rate.unwrap_or(0.0) * 100.0
    );

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
    assert_eq!(
        errors, 0,
        "Found {} integrity errors under memory pressure",
        errors
    );
}
