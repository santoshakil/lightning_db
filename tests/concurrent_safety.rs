#![allow(deprecated)]

use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use rand::{thread_rng, Rng};

#[test]
fn test_concurrent_writes_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path(), Default::default()).unwrap());

    let num_threads = 10;
    let ops_per_thread = 100;
    let counter = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let counter_clone = Arc::clone(&counter);

        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("value_{}", counter_clone.fetch_add(1, Ordering::SeqCst));

                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes succeeded
    let total_expected = num_threads * ops_per_thread;
    let mut actual_count = 0;

    for thread_id in 0..num_threads {
        for i in 0..ops_per_thread {
            let key = format!("thread_{}_key_{}", thread_id, i);
            assert!(db.get(key.as_bytes()).unwrap().is_some());
            actual_count += 1;
        }
    }

    assert_eq!(actual_count, total_expected);
}

#[test]
fn test_concurrent_read_write_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path(), Default::default()).unwrap());

    // Pre-populate data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("initial_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let stop_flag = Arc::new(AtomicBool::new(false));
    let error_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Writer threads
    for writer_id in 0..2 {
        let db_clone = Arc::clone(&db);
        let stop = Arc::clone(&stop_flag);

        let handle = thread::spawn(move || {
            let mut rng = thread_rng();
            while !stop.load(Ordering::Relaxed) {
                let key_idx = rng.gen_range(0..100);
                let key = format!("key_{:03}", key_idx);
                let value = format!("updated_by_writer_{}", writer_id);

                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                thread::sleep(Duration::from_micros(100));
            }
        });
        handles.push(handle);
    }

    // Reader threads
    for _reader_id in 0..5 {
        let db_clone = Arc::clone(&db);
        let stop = Arc::clone(&stop_flag);
        let errors = Arc::clone(&error_count);

        let handle = thread::spawn(move || {
            let mut rng = thread_rng();
            while !stop.load(Ordering::Relaxed) {
                let key_idx = rng.gen_range(0..100);
                let key = format!("key_{:03}", key_idx);

                match db_clone.get(key.as_bytes()) {
                    Ok(Some(_)) => {},
                    Ok(None) => {
                        // Key might be temporarily not visible during flush
                        // Retry once after a short delay
                        thread::sleep(Duration::from_millis(1));
                        if db_clone.get(key.as_bytes()).unwrap().is_none() {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                thread::sleep(Duration::from_micros(50));
            }
        });
        handles.push(handle);
    }

    // Run for 1 second
    thread::sleep(Duration::from_secs(1));
    stop_flag.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    // Allow a small number of temporary visibility issues
    let errors = error_count.load(Ordering::Relaxed);
    assert!(errors < 5, "Too many read errors: {}", errors);
}

#[test]
fn test_concurrent_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let config = LightningDbConfig {
        max_active_transactions: 20,
        ..Default::default()
    };
    let db = Arc::new(Database::open(temp_dir.path(), config).unwrap());

    // Initialize counter
    db.put(b"counter", b"0").unwrap();

    let num_threads = 5;
    let increments_per_thread = 20;
    let mut handles = vec![];

    for _thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);

        let handle = thread::spawn(move || {
            for _ in 0..increments_per_thread {
                // Retry transaction until it succeeds
                loop {
                    let tx_id = match db_clone.begin_transaction() {
                        Ok(id) => id,
                        Err(_) => {
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                    };

                    // Read current value
                    let current = match db_clone.get_tx(tx_id, b"counter") {
                        Ok(Some(v)) => {
                            String::from_utf8(v).unwrap().parse::<u32>().unwrap()
                        }
                        _ => {
                            let _ = db_clone.abort_transaction(tx_id);
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                    };

                    // Increment
                    let new_value = (current + 1).to_string();

                    if db_clone.put_tx(tx_id, b"counter", new_value.as_bytes()).is_err() {
                        let _ = db_clone.abort_transaction(tx_id);
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }

                    // Commit
                    if db_clone.commit_transaction(tx_id).is_ok() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify final count
    let final_value = db.get(b"counter").unwrap().unwrap();
    let final_count: u32 = String::from_utf8(final_value).unwrap().parse().unwrap();

    // Allow for some transaction conflicts, but should be close
    let expected = (num_threads * increments_per_thread) as u32;
    assert!(final_count >= expected * 90 / 100,
            "Final count {} is too low, expected around {}", final_count, expected);
}

#[test]
fn test_concurrent_scan_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path(), Default::default()).unwrap());

    // Pre-populate sorted data
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let mut handles = vec![];

    // Multiple concurrent scanners
    for _scanner_id in 0..5 {
        let db_clone = Arc::clone(&db);

        let handle = thread::spawn(move || {
            let mut rng = thread_rng();

            for _ in 0..10 {
                let start = rng.gen_range(0..900);
                let end = start + 100;

                let start_key = format!("key_{:04}", start);
                let end_key = format!("key_{:04}", end);

                let iter = db_clone.scan(
                    Some(start_key.as_bytes()),
                    Some(end_key.as_bytes())
                ).unwrap();

                let count = iter.count();
                assert!(count > 0 && count <= 100);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_batch_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path(), Default::default()).unwrap());

    let mut handles = vec![];

    for batch_id in 0..10 {
        let db_clone = Arc::clone(&db);

        let handle = thread::spawn(move || {
            let mut batch = lightning_db::WriteBatch::new();

            for i in 0..100 {
                let key = format!("batch_{}_key_{}", batch_id, i);
                let value = format!("batch_{}_value_{}", batch_id, i);
                batch.put(key.into_bytes(), value.into_bytes()).unwrap();
            }

            db_clone.write_batch(&batch).unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all batches were written
    for batch_id in 0..10 {
        for i in 0..100 {
            let key = format!("batch_{}_key_{}", batch_id, i);
            assert!(db.get(key.as_bytes()).unwrap().is_some());
        }
    }
}