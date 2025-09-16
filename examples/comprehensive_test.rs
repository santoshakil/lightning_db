use lightning_db::{Database, WriteBatch};
use std::time::Instant;
use std::sync::Arc;
use std::thread;

fn main() {
    println!("Lightning DB Comprehensive Real-World Test");
    println!("==========================================\n");

    // Test 1: Basic Operations
    test_basic_operations();

    // Test 2: Batch Operations
    test_batch_operations();

    // Test 3: Large Dataset
    test_large_dataset();

    // Test 4: Concurrent Access
    test_concurrent_access();

    // Test 5: Transaction Operations
    test_transactions();

    // Test 6: Performance Benchmark
    test_performance();

    println!("\n✅ All comprehensive tests passed!");
}

fn test_basic_operations() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let db = Database::create_temp().expect("Failed to create temp database");

    // Test put and get
    db.put(b"key1", b"value1").expect("Failed to put");
    let value = db.get(b"key1").expect("Failed to get");
    assert_eq!(value, Some(b"value1".to_vec()));

    // Test update
    db.put(b"key1", b"updated_value1").expect("Failed to update");
    let value = db.get(b"key1").expect("Failed to get updated");
    assert_eq!(value, Some(b"updated_value1".to_vec()));

    // Test delete
    db.delete(b"key1").expect("Failed to delete");
    let value = db.get(b"key1").expect("Failed to get deleted");
    assert_eq!(value, None);

    println!("✓ Basic operations test passed\n");
}

fn test_batch_operations() {
    println!("Test 2: Batch Operations");
    println!("------------------------");

    let db = Database::create_temp().expect("Failed to create temp database");

    // Test batch put
    let mut batch = WriteBatch::new();
    for i in 0..100 {
        let key = format!("batch_key_{}", i);
        let value = format!("batch_value_{}", i);
        batch.put(key.into_bytes(), value.into_bytes()).unwrap();
    }
    db.write_batch(&batch).expect("Failed to write batch");

    // Verify batch put
    for i in 0..100 {
        let key = format!("batch_key_{}", i);
        let expected_value = format!("batch_value_{}", i);
        let value = db.get(key.as_bytes()).expect("Failed to get batch key");
        assert_eq!(value, Some(expected_value.into_bytes()));
    }

    // Test batch delete
    let mut batch = WriteBatch::new();
    for i in 50..100 {
        let key = format!("batch_key_{}", i);
        batch.delete(key.into_bytes()).unwrap();
    }
    db.write_batch(&batch).expect("Failed to write delete batch");

    // Verify batch delete
    for i in 50..100 {
        let key = format!("batch_key_{}", i);
        let value = db.get(key.as_bytes()).expect("Failed to get deleted key");
        assert_eq!(value, None);
    }

    // Verify remaining keys
    for i in 0..50 {
        let key = format!("batch_key_{}", i);
        let expected_value = format!("batch_value_{}", i);
        let value = db.get(key.as_bytes()).expect("Failed to get remaining key");
        assert_eq!(value, Some(expected_value.into_bytes()));
    }

    println!("✓ Batch operations test passed\n");
}

fn test_large_dataset() {
    println!("Test 3: Large Dataset (10,000 keys)");
    println!("------------------------------------");

    let db = Database::create_temp().expect("Failed to create temp database");
    let start = Instant::now();

    // Insert 10,000 keys
    for i in 0..10_000 {
        let key = format!("large_key_{:06}", i);
        let value = format!("large_value_{:06}_with_some_extra_data_to_make_it_larger", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Failed to put large dataset");
    }

    let insert_time = start.elapsed();
    println!("  Inserted 10,000 keys in {:?}", insert_time);

    // Read random keys
    let start = Instant::now();
    for i in (0..10_000).step_by(100) {
        let key = format!("large_key_{:06}", i);
        let expected_value = format!("large_value_{:06}_with_some_extra_data_to_make_it_larger", i);
        let value = db.get(key.as_bytes()).expect("Failed to get large dataset key");
        assert_eq!(value, Some(expected_value.into_bytes()));
    }

    let read_time = start.elapsed();
    println!("  Read 100 random keys in {:?}", read_time);

    // Scan range
    let start = Instant::now();
    let mut count = 0;
    let iter = db.scan(Some(b"large_key_000000"), Some(b"large_key_000100"))
        .expect("Failed to scan");
    for _ in iter {
        count += 1;
        if count >= 100 {
            break;
        }
    }
    assert_eq!(count, 100);

    let scan_time = start.elapsed();
    println!("  Scanned 100 keys in {:?}", scan_time);

    println!("✓ Large dataset test passed\n");
}

fn test_concurrent_access() {
    println!("Test 4: Concurrent Access (4 threads)");
    println!("--------------------------------------");

    let db = Arc::new(Database::create_temp().expect("Failed to create temp database"));
    let mut handles = vec![];

    // Spawn 4 threads, each writing different keys
    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..250 {
                let key = format!("thread_{}_key_{:04}", thread_id, i);
                let value = format!("thread_{}_value_{:04}", thread_id, i);
                db_clone.put(key.as_bytes(), value.as_bytes())
                    .expect("Failed to put in thread");
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all keys from all threads
    for thread_id in 0..4 {
        for i in 0..250 {
            let key = format!("thread_{}_key_{:04}", thread_id, i);
            let expected_value = format!("thread_{}_value_{:04}", thread_id, i);
            let value = db.get(key.as_bytes()).expect("Failed to get concurrent key");
            assert_eq!(value, Some(expected_value.into_bytes()));
        }
    }

    println!("✓ Concurrent access test passed\n");
}

fn test_transactions() {
    println!("Test 5: Transaction Operations");
    println!("-------------------------------");

    let db = Database::create_temp().expect("Failed to create temp database");

    // Start transaction
    let tx_id = db.begin_transaction().expect("Failed to begin transaction");

    // Put within transaction
    db.put_tx(tx_id, b"tx_key1", b"tx_value1").expect("Failed to put in tx");
    db.put_tx(tx_id, b"tx_key2", b"tx_value2").expect("Failed to put in tx");

    // Read within transaction (should see uncommitted changes)
    let value = db.get_tx(tx_id, b"tx_key1").expect("Failed to get in tx");
    assert_eq!(value, Some(b"tx_value1".to_vec()));

    // Commit transaction
    db.commit_transaction(tx_id).expect("Failed to commit transaction");

    // Verify committed data
    let value = db.get(b"tx_key1").expect("Failed to get after commit");
    assert_eq!(value, Some(b"tx_value1".to_vec()));

    // Test transaction abort
    let tx_id2 = db.begin_transaction().expect("Failed to begin transaction 2");
    db.put_tx(tx_id2, b"tx_key3", b"tx_value3").expect("Failed to put in tx2");
    db.abort_transaction(tx_id2).expect("Failed to abort transaction");

    // Verify aborted data is not present
    let value = db.get(b"tx_key3").expect("Failed to get after abort");
    assert_eq!(value, None);

    println!("✓ Transaction operations test passed\n");
}

fn test_performance() {
    println!("Test 6: Performance Benchmark");
    println!("------------------------------");

    let db = Database::create_temp().expect("Failed to create temp database");
    let num_ops = 1000;

    // Benchmark sequential writes
    let start = Instant::now();
    for i in 0..num_ops {
        let key = format!("perf_key_{:06}", i);
        let value = format!("perf_value_{:06}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Failed to put perf");
    }
    let write_duration = start.elapsed();
    let writes_per_sec = num_ops as f64 / write_duration.as_secs_f64();
    println!("  Sequential writes: {:.0} ops/sec", writes_per_sec);

    // Benchmark random reads
    let start = Instant::now();
    for i in 0..num_ops {
        let key = format!("perf_key_{:06}", i);
        db.get(key.as_bytes()).expect("Failed to get perf");
    }
    let read_duration = start.elapsed();
    let reads_per_sec = num_ops as f64 / read_duration.as_secs_f64();
    println!("  Random reads: {:.0} ops/sec", reads_per_sec);

    // Benchmark batch writes
    let start = Instant::now();
    let mut batch = WriteBatch::new();
    for i in 0..num_ops {
        let key = format!("batch_perf_key_{:06}", i);
        let value = format!("batch_perf_value_{:06}", i);
        batch.put(key.into_bytes(), value.into_bytes()).unwrap();
    }
    db.write_batch(&batch).expect("Failed to write perf batch");
    let batch_duration = start.elapsed();
    let batch_writes_per_sec = num_ops as f64 / batch_duration.as_secs_f64();
    println!("  Batch writes: {:.0} ops/sec", batch_writes_per_sec);

    println!("✓ Performance benchmark completed\n");
}