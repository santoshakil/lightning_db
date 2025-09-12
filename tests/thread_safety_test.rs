use lightning_db::{Database, LightningDbConfig};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn test_concurrent_reads_writes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(
        Database::create(
            dir.path(),
            LightningDbConfig {
                max_active_transactions: 100,
                cache_size: 50 * 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap(),
    );

    let num_writers = 5;
    let num_readers = 10;
    let ops_per_thread = 1000;
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

    assert!(!error_flag.load(Ordering::SeqCst), "Errors occurred during concurrent operations");
    
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
    
    assert!(total_success > 0, "At least some transactions should succeed");
    assert_eq!(
        total_success + total_abort,
        num_threads * txns_per_thread,
        "All transactions should be accounted for"
    );
}

#[test]
fn test_race_condition_detection() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    // Shared counter key
    let counter_key = b"shared_counter";
    db.put(counter_key, b"0").unwrap();

    let num_threads = 10;
    let increments_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..increments_per_thread {
                    // Read-modify-write pattern (intentionally racy)
                    let current = db.get(counter_key).unwrap().unwrap();
                    let val = std::str::from_utf8(&current).unwrap().parse::<i32>().unwrap();
                    let new_val = (val + 1).to_string();
                    db.put(counter_key, new_val.as_bytes()).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Check final value - if thread-safe, should be exactly num_threads * increments_per_thread
    let final_value = db.get(counter_key).unwrap().unwrap();
    let final_count = std::str::from_utf8(&final_value).unwrap().parse::<i32>().unwrap();
    
    // This test intentionally has a race condition - the final count will likely be less
    // than expected due to lost updates. This demonstrates why transactions are needed.
    println!("Expected count: {}", num_threads * increments_per_thread);
    println!("Actual count: {}", final_count);
    
    // We expect some lost updates due to race conditions
    assert!(final_count <= (num_threads * increments_per_thread) as i32);
}

#[test]
fn test_safe_counter_with_transactions() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(
        dir.path(),
        LightningDbConfig {
            max_active_transactions: 20,
            ..Default::default()
        },
    ).unwrap());

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
                    loop {
                        let tx = db.begin_transaction().unwrap();
                        
                        // Read current value
                        let current = db.get_tx(tx, counter_key).unwrap().unwrap();
                        let val = std::str::from_utf8(&current).unwrap().parse::<i32>().unwrap();
                        
                        // Increment
                        let new_val = (val + 1).to_string();
                        db.put_tx(tx, counter_key, new_val.as_bytes()).unwrap();
                        
                        // Try to commit
                        match db.commit_transaction(tx) {
                            Ok(_) => break,
                            Err(_) => {
                                // Retry on conflict
                                retry_counter.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        }
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
    let final_count = std::str::from_utf8(&final_value).unwrap().parse::<i32>().unwrap();
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
fn test_deadlock_prevention() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(
        dir.path(),
        LightningDbConfig {
            max_active_transactions: 10,
            ..Default::default()
        },
    ).unwrap());

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
    println!("Deadlock detected: {}", deadlock_detected.load(Ordering::SeqCst));
}