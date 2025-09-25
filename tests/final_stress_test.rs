use lightning_db::{Database, LightningDbConfig, WriteBatch, WalSyncMode};
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use rand::{Rng, thread_rng, seq::SliceRandom};

const NUM_THREADS: usize = 8;
const OPS_PER_THREAD: usize = 10000;
const BATCH_SIZE: usize = 100;
const KEY_SPACE: usize = 100000;

#[test]
fn test_final_comprehensive_stress() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("final_stress_db");

    let config = LightningDbConfig {
        page_size: 8192,
        cache_size: 32 * 1024 * 1024,
        compression_enabled: true,
        max_active_transactions: 200,
        wal_sync_mode: WalSyncMode::Normal,
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));

    let total_ops = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));
    let stop_signal = Arc::new(AtomicBool::new(false));
    let start = Instant::now();

    let mut handles = vec![];

    // Mixed operation threads
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let total_ops_clone = Arc::clone(&total_ops);
        let total_errors_clone = Arc::clone(&total_errors);
        let stop_clone = Arc::clone(&stop_signal);

        let handle = thread::spawn(move || {
            let mut rng = thread_rng();
            let mut ops_completed = 0;

            while ops_completed < OPS_PER_THREAD && !stop_clone.load(Ordering::Relaxed) {
                let operation = rng.gen_range(0..10);

                match operation {
                    0..=3 => {
                        // Write operation
                        let key = format!("key_{:08}", rng.gen_range(0..KEY_SPACE));
                        let value = vec![rng.gen::<u8>(); rng.gen_range(100..10000)];

                        if db_clone.put(key.as_bytes(), &value).is_ok() {
                            total_ops_clone.fetch_add(1, Ordering::Relaxed);
                        } else {
                            total_errors_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    4..=6 => {
                        // Read operation
                        let key = format!("key_{:08}", rng.gen_range(0..KEY_SPACE));
                        if db_clone.get(key.as_bytes()).is_ok() {
                            total_ops_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    7 => {
                        // Batch write
                        let mut batch = WriteBatch::new();
                        for _ in 0..rng.gen_range(5..20) {
                            let key = format!("batch_{:08}_{}", thread_id, rng.gen_range(0..1000));
                            let value = vec![rng.gen::<u8>(); rng.gen_range(50..500)];
                            if batch.put(key.into_bytes(), value).is_err() {
                                break;
                            }
                        }
                        if db_clone.write_batch(&batch).is_ok() {
                            total_ops_clone.fetch_add(batch.len() as u64, Ordering::Relaxed);
                        } else {
                            total_errors_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    8 => {
                        // Range scan
                        let start_key = format!("key_{:08}", rng.gen_range(0..KEY_SPACE/2));
                        let end_key = format!("key_{:08}", rng.gen_range(KEY_SPACE/2..KEY_SPACE));
                        if let Ok(iter) = db_clone.scan(Some(start_key.as_bytes()), Some(end_key.as_bytes())) {
                            let count = iter.take(100).count();
                            total_ops_clone.fetch_add(count as u64, Ordering::Relaxed);
                        }
                    }
                    9 => {
                        // Delete operation
                        let key = format!("key_{:08}", rng.gen_range(0..KEY_SPACE));
                        if db_clone.delete(key.as_bytes()).is_ok() {
                            total_ops_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => {}
                }

                ops_completed += 1;
            }
        });
        handles.push(handle);
    }

    // Transaction stress thread
    let db_clone = Arc::clone(&db);
    let total_ops_clone = Arc::clone(&total_ops);
    let stop_clone = Arc::clone(&stop_signal);
    let handle = thread::spawn(move || {
        let mut rng = thread_rng();
        while !stop_clone.load(Ordering::Relaxed) {
            if let Ok(tx_id) = db_clone.begin_transaction() {
                let num_ops = rng.gen_range(5..20);
                let mut success = true;

                for _ in 0..num_ops {
                    let key = format!("tx_key_{:08}", rng.gen_range(0..1000));
                    let value = format!("tx_value_{}", rng.gen::<u64>());

                    if db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_err() {
                        success = false;
                        break;
                    }
                }

                if success {
                    if db_clone.commit_transaction(tx_id).is_ok() {
                        total_ops_clone.fetch_add(num_ops, Ordering::Relaxed);
                    }
                } else {
                    let _ = db_clone.abort_transaction(tx_id);
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
    handles.push(handle);

    // Monitor thread
    let total_ops_monitor = Arc::clone(&total_ops);
    let total_errors_monitor = Arc::clone(&total_errors);
    let stop_monitor = Arc::clone(&stop_signal);
    let monitor_handle = thread::spawn(move || {
        let mut last_ops = 0;
        loop {
            thread::sleep(Duration::from_secs(5));
            let current_ops = total_ops_monitor.load(Ordering::Relaxed);
            let current_errors = total_errors_monitor.load(Ordering::Relaxed);
            let throughput = (current_ops - last_ops) / 5;

            println!("Progress: {} ops, {} errors, {} ops/sec", current_ops, current_errors, throughput);
            last_ops = current_ops;

            if stop_monitor.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    // Wait for worker threads
    for handle in handles.drain(..NUM_THREADS) {
        handle.join().expect("Thread panicked");
    }

    // Stop remaining threads
    stop_signal.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    monitor_handle.join().expect("Monitor thread panicked");

    let elapsed = start.elapsed();
    let final_ops = total_ops.load(Ordering::Relaxed);
    let final_errors = total_errors.load(Ordering::Relaxed);
    let throughput = final_ops as f64 / elapsed.as_secs_f64();

    println!("\nFinal stress test results:");
    println!("  Total operations: {}", final_ops);
    println!("  Total errors: {}", final_errors);
    println!("  Duration: {:?}", elapsed);
    println!("  Average throughput: {:.2} ops/sec", throughput);
    println!("  Error rate: {:.2}%", (final_errors as f64 / final_ops as f64) * 100.0);

    assert!(final_ops > NUM_THREADS as u64 * OPS_PER_THREAD as u64 / 2,
            "Should complete at least half of planned operations");
    assert!(final_errors < final_ops / 100, "Error rate should be less than 1%");
}

#[test]
fn test_database_consistency_verification() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("consistency_db");
    let db = Database::open(&db_path, Default::default()).expect("Failed to open database");

    // Phase 1: Write known data
    let mut expected_data = std::collections::HashMap::new();
    for i in 0..1000 {
        let key = format!("verify_key_{:05}", i);
        let value = format!("verify_value_{:05}_data", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        expected_data.insert(key, value);
    }

    // Phase 2: Perform various operations
    let mut batch = WriteBatch::new();
    for i in 1000..1100 {
        let key = format!("verify_key_{:05}", i);
        let value = format!("verify_value_{:05}_batch", i);
        batch.put(key.clone().into_bytes(), value.clone().into_bytes()).expect("Batch put failed");
        expected_data.insert(key, value);
    }
    db.write_batch(&batch).expect("Batch write failed");

    // Delete some keys
    for i in (0..100).step_by(10) {
        let key = format!("verify_key_{:05}", i);
        db.delete(key.as_bytes()).expect("Delete failed");
        expected_data.remove(&key);
    }

    // Update some keys
    for i in (100..200).step_by(5) {
        let key = format!("verify_key_{:05}", i);
        let value = format!("updated_value_{:05}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Update failed");
        expected_data.insert(key, value);
    }

    // Phase 3: Verify all data
    for (key, expected_value) in &expected_data {
        let actual_value = db.get(key.as_bytes()).expect("Get failed");
        assert_eq!(
            actual_value.map(|v| String::from_utf8_lossy(&v).to_string()),
            Some(expected_value.clone()),
            "Data mismatch for key: {}",
            key
        );
    }

    // Verify deleted keys are gone
    for i in (0..100).step_by(10) {
        let key = format!("verify_key_{:05}", i);
        let value = db.get(key.as_bytes()).expect("Get failed");
        assert!(value.is_none(), "Deleted key {} should not exist", key);
    }

    // Verify scan consistency
    let iter = db.scan(Some(b"verify_key_"), Some(b"verify_key_~")).expect("Scan failed");
    let scan_count = iter.count();
    assert_eq!(scan_count, expected_data.len(), "Scan count mismatch");

    println!("Database consistency verification passed with {} entries", expected_data.len());
}

#[test]
fn test_extreme_conditions() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("extreme_db");

    let config = LightningDbConfig {
        cache_size: 1024 * 1024, // Very small cache
        max_active_transactions: 10, // Low transaction limit
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));

    // Test 1: Very large key
    let large_key = vec![b'k'; 60000]; // 60KB key
    let value = b"large_key_value";
    assert!(db.put(&large_key, value).is_ok(), "Should handle large keys");
    assert_eq!(db.get(&large_key).unwrap(), Some(value.to_vec()));

    // Test 2: Very large value
    let key = b"large_value_key";
    let large_value = vec![b'v'; 5 * 1024 * 1024]; // 5MB value
    assert!(db.put(key, &large_value).is_ok(), "Should handle large values");

    // Test 3: Rapid key creation
    for i in 0..10000 {
        let key = format!("rapid_{}", i);
        if db.put(key.as_bytes(), b"rapid").is_err() {
            break; // Expected under extreme conditions
        }
    }

    // Test 4: Deep nesting in transactions
    let mut tx_ids = vec![];
    for _ in 0..5 {
        if let Ok(tx_id) = db.begin_transaction() {
            tx_ids.push(tx_id);
        } else {
            break; // Expected to hit transaction limit
        }
    }
    for tx_id in tx_ids.iter().rev() {
        let _ = db.abort_transaction(*tx_id);
    }

    // Test 5: Concurrent stress on same key
    let key = b"contended_key";
    let mut handles = vec![];
    for _ in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let value = format!("value_{}", i);
                let _ = db_clone.put(key, value.as_bytes());
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    println!("Extreme conditions test completed successfully");
}