use lightning_db::{Database, LightningDbConfig, WriteBatch};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use std::time::Instant;
use tempfile::TempDir;
use rand::{Rng, thread_rng};

const STRESS_OPS: usize = 1000;
const STRESS_THREADS: usize = 4;

#[test]
fn test_concurrent_stress() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("stress_db");

    let config = LightningDbConfig {
        cache_size: 32 * 1024 * 1024,
        max_active_transactions: 100,
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));
    let total_ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for thread_id in 0..STRESS_THREADS {
        let db_clone = Arc::clone(&db);
        let ops_clone = Arc::clone(&total_ops);
        let err_clone = Arc::clone(&errors);

        let handle = thread::spawn(move || {
            let mut rng = thread_rng();

            for i in 0..STRESS_OPS {
                let op = rng.gen_range(0..5);

                match op {
                    0..=2 => {
                        // Write
                        let key = format!("key_{:08}", rng.gen_range(0..10000));
                        let value = vec![rng.gen::<u8>(); rng.gen_range(100..1000)];
                        if db_clone.put(key.as_bytes(), &value).is_ok() {
                            ops_clone.fetch_add(1, Ordering::Relaxed);
                        } else {
                            err_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    3 => {
                        // Read
                        let key = format!("key_{:08}", rng.gen_range(0..10000));
                        if db_clone.get(key.as_bytes()).is_ok() {
                            ops_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    4 => {
                        // Delete
                        let key = format!("key_{:08}", rng.gen_range(0..10000));
                        if db_clone.delete(key.as_bytes()).is_ok() {
                            ops_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => {}
                }

                if i % 100 == 0 {
                    thread::yield_now();
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let total = total_ops.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);

    assert!(total > (STRESS_THREADS * STRESS_OPS / 2) as u64);
    assert!(error_count < (total / 100)); // Less than 1% error rate
}

#[test]
fn test_extreme_key_sizes() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("extreme_key_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Test various key sizes
    let sizes = vec![1, 10, 100, 1000, 10000, 60000];

    for size in sizes {
        let key = vec![b'k'; size];
        let value = format!("value_for_{}_byte_key", size);

        if size <= 65536 {
            db.put(&key, value.as_bytes()).expect("Put failed");
            let retrieved = db.get(&key).expect("Get failed");
            assert_eq!(retrieved, Some(value.into_bytes()));
        }
    }
}

#[test]
fn test_extreme_value_sizes() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("extreme_value_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Test various value sizes
    let sizes = vec![0, 1, 1024, 100 * 1024, 1024 * 1024, 5 * 1024 * 1024];

    for (idx, size) in sizes.iter().enumerate() {
        let key = format!("value_size_{}", idx);
        let value = vec![(idx % 256) as u8; *size];

        db.put(key.as_bytes(), &value).expect("Put failed");
        let retrieved = db.get(key.as_bytes()).expect("Get failed");
        assert_eq!(retrieved, Some(value));
    }
}

#[test]
fn test_transaction_stress() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("tx_stress_db");

    let config = LightningDbConfig {
        max_active_transactions: 50,
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));

    // Initialize data
    for i in 0..100 {
        let key = format!("tx_key_{:03}", i);
        let value = format!("initial_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Setup failed");
    }

    let mut handles = vec![];

    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let mut rng = thread_rng();

            for _ in 0..25 {
                if let Ok(tx_id) = db_clone.begin_transaction() {
                    let num_ops = rng.gen_range(5..15);
                    let mut success = true;

                    for _ in 0..num_ops {
                        let key = format!("tx_key_{:03}", rng.gen_range(0..100));
                        let value = format!("thread_{}_update", thread_id);

                        if db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_err() {
                            success = false;
                            break;
                        }
                    }

                    if success {
                        let _ = db_clone.commit_transaction(tx_id);
                    } else {
                        let _ = db_clone.abort_transaction(tx_id);
                    }
                }

                thread::yield_now();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify database is still functional
    let iter = db.scan(Some(b"tx_key_"), Some(b"tx_key_~")).expect("Scan failed");
    let count = iter.count();
    assert_eq!(count, 100);
}

#[test]
fn test_massive_batch() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("massive_batch_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Create a very large batch
    let mut batch = WriteBatch::new();
    for i in 0..10000 {
        let key = format!("batch_key_{:05}", i);
        let value = format!("batch_value_{:05}", i);
        batch.put(key.into_bytes(), value.into_bytes()).expect("Batch put failed");
    }

    // Write the massive batch
    db.write_batch(&batch).expect("Massive batch write failed");

    // Verify random samples
    let mut rng = thread_rng();
    for _ in 0..100 {
        let idx = rng.gen_range(0..10000);
        let key = format!("batch_key_{:05}", idx);
        let expected = format!("batch_value_{:05}", idx);
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert_eq!(value, Some(expected.into_bytes()));
    }
}

#[test]
fn test_rapid_open_close() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("rapid_db");

    // Initial write
    {
        let db = Database::open(&db_path, Default::default()).expect("Failed to open");
        for i in 0..50 {
            let key = format!("persist_{:02}", i);
            let value = format!("value_{:02}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        }
    }

    // Rapid open/close cycles
    for cycle in 0..10 {
        let db = Database::open(&db_path, Default::default()).expect("Failed to reopen");

        // Verify data
        let key = format!("persist_{:02}", cycle);
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert!(value.is_some());

        // Add new data
        let new_key = format!("cycle_{:02}", cycle);
        let new_value = format!("added_in_cycle_{:02}", cycle);
        db.put(new_key.as_bytes(), new_value.as_bytes()).expect("Put failed");

        // Database closes here
    }

    // Final verification
    {
        let db = Database::open(&db_path, Default::default()).expect("Failed to open");

        // Check persistence
        for i in 0..10 {
            let key = format!("cycle_{:02}", i);
            let expected = format!("added_in_cycle_{:02}", i);
            let value = db.get(key.as_bytes()).expect("Get failed");
            assert_eq!(value, Some(expected.into_bytes()));
        }
    }
}

#[test]
fn test_scan_performance() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("scan_perf_db");
    let db = Database::open(db_path, Default::default()).expect("Failed to open database");

    // Insert ordered data
    for i in 0..1000 {
        let key = format!("scan_key_{:04}", i);
        let value = format!("scan_value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }

    let start = Instant::now();

    // Full scan
    let iter = db.scan(None, None).expect("Full scan failed");
    let count = iter.count();
    assert_eq!(count, 1000);

    let full_scan_time = start.elapsed();

    // Range scan
    let start = Instant::now();
    let iter = db.scan(Some(b"scan_key_0100"), Some(b"scan_key_0200")).expect("Range scan failed");
    let range_count = iter.count();
    assert!(range_count >= 100 && range_count <= 101);

    let range_scan_time = start.elapsed();

    println!("Full scan (1000 items): {:?}", full_scan_time);
    println!("Range scan (100 items): {:?}", range_scan_time);

    // Range scan should be significantly faster
    assert!(range_scan_time < full_scan_time);
}