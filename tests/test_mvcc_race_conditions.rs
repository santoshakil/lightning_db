use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

/// Test for MVCC race condition fixes
/// Verifies that concurrent transactions cannot both commit conflicting writes

#[test]
fn test_write_write_conflict_detection() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());

    // Setup initial data
    db.put(b"test_key", b"initial_value").unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let db1 = db.clone();
    let db2 = db.clone();
    let barrier1 = barrier.clone();
    let barrier2 = barrier.clone();

    let mut results = Vec::new();

    // Spawn two threads that try to concurrently modify the same key
    let handle1 = thread::spawn(move || {
        let tx_id = db1.begin_transaction().unwrap();

        // Read the initial value to establish read timestamp
        let _value = db1.get_tx(tx_id, b"test_key").unwrap();

        // Wait for both threads to reach this point
        barrier1.wait();

        // Both threads will try to write to the same key
        let put_result = db1.put_tx(tx_id, b"test_key", b"thread1_value");
        if put_result.is_ok() {
            // Try to commit
            db1.commit_transaction(tx_id)
        } else {
            Err(put_result.unwrap_err())
        }
    });

    let handle2 = thread::spawn(move || {
        let tx_id = db2.begin_transaction().unwrap();

        // Read the initial value to establish read timestamp
        let _value = db2.get_tx(tx_id, b"test_key").unwrap();

        // Wait for both threads to reach this point
        barrier2.wait();

        // Both threads will try to write to the same key
        let put_result = db2.put_tx(tx_id, b"test_key", b"thread2_value");
        if put_result.is_ok() {
            // Try to commit
            db2.commit_transaction(tx_id)
        } else {
            Err(put_result.unwrap_err())
        }
    });

    results.push(handle1.join().unwrap());
    results.push(handle2.join().unwrap());

    // Exactly one transaction should succeed, one should fail
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();

    assert_eq!(successes, 1, "Exactly one transaction should succeed");
    assert_eq!(
        failures, 1,
        "Exactly one transaction should fail due to conflict"
    );

    // Verify that the database is in a consistent state
    let final_value = db.get(b"test_key").unwrap().unwrap();
    assert!(
        final_value == b"thread1_value" || final_value == b"thread2_value",
        "Final value should be from one of the transactions"
    );
}

#[test]
fn test_concurrent_transaction_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());

    // Setup initial data
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("initial_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let num_threads = 4;
    let operations_per_thread = 10;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let mut successful_commits = 0;
            let mut failed_commits = 0;

            for op_id in 0..operations_per_thread {
                let tx_id = db_clone.begin_transaction().unwrap();

                // Read several keys to establish read set
                let key1 = format!("key_{}", op_id % 5);
                let key2 = format!("key_{}", (op_id + 1) % 5);

                let _value1 = db_clone.get_tx(tx_id, key1.as_bytes());
                let _value2 = db_clone.get_tx(tx_id, key2.as_bytes());

                // Wait for all threads to start their transactions
                if op_id == 0 {
                    barrier_clone.wait();
                }

                // Write to one of the keys
                let new_value = format!("thread_{}_op_{}", thread_id, op_id);
                let put_result = db_clone.put_tx(tx_id, key1.as_bytes(), new_value.as_bytes());

                if put_result.is_ok() {
                    match db_clone.commit_transaction(tx_id) {
                        Ok(_) => successful_commits += 1,
                        Err(_) => failed_commits += 1,
                    }
                } else {
                    failed_commits += 1;
                    let _ = db_clone.abort_transaction(tx_id);
                }

                // Small delay to allow interleaving
                thread::sleep(Duration::from_millis(1));
            }

            (successful_commits, failed_commits)
        });

        handles.push(handle);
    }

    // Collect results
    let mut total_successes = 0;
    let mut total_failures = 0;

    for handle in handles {
        let (successes, failures) = handle.join().unwrap();
        total_successes += successes;
        total_failures += failures;
    }

    println!("Total successful commits: {}", total_successes);
    println!("Total failed commits: {}", total_failures);

    // Verify some basic properties
    assert!(total_successes > 0, "Some transactions should succeed");
    assert_eq!(
        total_successes + total_failures,
        num_threads * operations_per_thread,
        "All operations should either succeed or fail"
    );

    // Verify data integrity - all values should be from successful transactions
    for i in 0..5 {
        let key = format!("key_{}", i);
        if let Ok(Some(value)) = db.get(key.as_bytes()) {
            let value_str = String::from_utf8(value).unwrap();
            // Value should either be initial or from a successful transaction
            assert!(
                value_str.starts_with("initial_") || value_str.starts_with("thread_"),
                "Value should be from a committed transaction: {}",
                value_str
            );
        }
    }
}

#[test]
fn test_atomic_reservation_system() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());

    // Set up initial value to create a proper conflict scenario
    db.put(b"contested_key", b"initial_value").unwrap();

    // This test specifically targets the atomic reservation logic
    let num_threads = 8;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            // All threads try to write to the same key simultaneously
            let tx_id = db_clone.begin_transaction().unwrap();

            // Read the initial value to establish read timestamp
            let _initial = db_clone.get_tx(tx_id, b"contested_key").unwrap();

            // Synchronize all threads to start at the same time
            barrier_clone.wait();

            // All threads compete for the same key
            let value = format!("thread_{}_value", thread_id);
            let put_result = db_clone.put_tx(tx_id, b"contested_key", value.as_bytes());

            if put_result.is_ok() {
                db_clone.commit_transaction(tx_id)
            } else {
                Err(put_result.unwrap_err())
            }
        });

        handles.push(handle);
    }

    // Collect results
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.join().unwrap());
    }

    // Only one thread should succeed in this high-contention scenario
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();

    assert_eq!(
        successes, 1,
        "Exactly one thread should succeed in high contention"
    );
    assert_eq!(failures, num_threads - 1, "All other threads should fail");

    // Verify the winning value is present
    let final_value = db.get(b"contested_key").unwrap().unwrap();
    let final_value_str = String::from_utf8(final_value).unwrap();
    assert!(
        final_value_str.starts_with("thread_") && final_value_str.ends_with("_value"),
        "Final value should be from one of the competing threads: {}",
        final_value_str
    );
}
