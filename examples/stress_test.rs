use lightning_db::{Database, WriteBatch};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};

fn main() {
    println!("Lightning DB Comprehensive Stress Test");
    println!("======================================\n");

    // Test 1: Heavy concurrent writes
    test_concurrent_writes();

    // Test 2: Large batch operations
    test_large_batches();

    // Test 3: Mixed workload stress
    test_mixed_workload();

    // Test 4: Memory pressure test
    test_memory_pressure();

    // Test 5: Recovery stress test
    test_recovery_stress();

    println!("\n✅ All stress tests passed successfully!");
}

fn test_concurrent_writes() {
    println!("Test 1: Heavy Concurrent Writes");
    println!("--------------------------------");

    let db = Arc::new(Database::create_temp().expect("Failed to create database"));
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = vec![];

    // Spawn 10 threads, each writing 1000 unique keys
    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let success_clone = Arc::clone(&success_count);
        let error_clone = Arc::clone(&error_count);

        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("thread_{:02}_key_{:04}", thread_id, i);
                let value = format!("thread_{:02}_value_{:04}_with_padding_data", thread_id, i);

                match db_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => success_clone.fetch_add(1, Ordering::Relaxed),
                    Err(_) => error_clone.fetch_add(1, Ordering::Relaxed),
                };
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let duration = start.elapsed();
    let total_ops = 10_000;
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

    println!("  Threads: 10");
    println!("  Operations: {}", total_ops);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    println!("  Successes: {}", success_count.load(Ordering::Relaxed));
    println!("  Errors: {}", error_count.load(Ordering::Relaxed));

    // Verify all data
    let mut verified = 0;
    for thread_id in 0..10 {
        for i in 0..1000 {
            let key = format!("thread_{:02}_key_{:04}", thread_id, i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                verified += 1;
            }
        }
    }

    assert_eq!(verified, 10_000, "Not all keys were written");
    println!("  ✓ All {} keys verified\n", verified);
}

fn test_large_batches() {
    println!("Test 2: Large Batch Operations");
    println!("-------------------------------");

    let db = Database::create_temp().expect("Failed to create database");

    // Test increasingly large batches
    for batch_size in &[100, 500, 1000, 5000] {
        let start = Instant::now();
        let mut batch = WriteBatch::new();

        for i in 0..*batch_size {
            let key = format!("batch_{}_key_{:06}", batch_size, i);
            let value = format!("batch_{}_value_{:06}_with_extra_padding", batch_size, i);
            batch.put(key.into_bytes(), value.into_bytes()).unwrap();
        }

        db.write_batch(&batch).expect("Failed to write batch");

        let duration = start.elapsed();
        let ops_per_sec = *batch_size as f64 / duration.as_secs_f64();

        println!("  Batch size: {}", batch_size);
        println!("    Duration: {:?}", duration);
        println!("    Throughput: {:.0} ops/sec", ops_per_sec);

        // Verify batch
        let key = format!("batch_{}_key_{:06}", batch_size, 0);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }
    println!("  ✓ All batch sizes completed successfully\n");
}

fn test_mixed_workload() {
    println!("Test 3: Mixed Workload Stress");
    println!("------------------------------");

    let db = Arc::new(Database::create_temp().expect("Failed to create database"));

    // Pre-populate with data
    for i in 0..5000 {
        let key = format!("base_key_{:06}", i);
        let value = format!("base_value_{:06}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let start = Instant::now();
    let mut handles = vec![];

    // Reader threads
    for reader_id in 0..3 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let mut read_count = 0;
            for _ in 0..10000 {
                let key = format!("base_key_{:06}", reader_id * 1000 % 5000);
                if db_clone.get(key.as_bytes()).unwrap().is_some() {
                    read_count += 1;
                }
            }
            read_count
        });
        handles.push(handle);
    }

    // Writer threads
    for writer_id in 0..2 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let mut write_count = 0;
            for i in 0..2000 {
                let key = format!("writer_{}_key_{:04}", writer_id, i);
                let value = format!("writer_{}_value_{:04}", writer_id, i);
                if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                    write_count += 1;
                }
            }
            write_count
        });
        handles.push(handle);
    }

    // Delete thread
    let db_clone = Arc::clone(&db);
    let delete_handle = thread::spawn(move || {
        let mut delete_count = 0;
        for i in 0..1000 {
            let key = format!("base_key_{:06}", i);
            if db_clone.delete(key.as_bytes()).is_ok() {
                delete_count += 1;
            }
        }
        delete_count
    });
    handles.push(delete_handle);

    // Batch thread
    let db_clone = Arc::clone(&db);
    let batch_handle = thread::spawn(move || {
        let mut batch_count = 0;
        for batch_num in 0..10 {
            let mut batch = WriteBatch::new();
            for i in 0..100 {
                let key = format!("batch_thread_key_{:03}_{:03}", batch_num, i);
                let value = format!("batch_thread_value_{:03}_{:03}", batch_num, i);
                batch.put(key.into_bytes(), value.into_bytes()).unwrap();
            }
            if db_clone.write_batch(&batch).is_ok() {
                batch_count += 100;
            }
        }
        batch_count
    });
    handles.push(batch_handle);

    // Wait for all operations
    let mut total_ops = 0;
    for handle in handles {
        total_ops += handle.join().expect("Thread panicked");
    }

    let duration = start.elapsed();
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

    println!("  Readers: 3, Writers: 2, Delete: 1, Batch: 1");
    println!("  Total operations: {}", total_ops);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    println!("  ✓ Mixed workload completed successfully\n");
}

fn test_memory_pressure() {
    println!("Test 4: Memory Pressure Test");
    println!("-----------------------------");

    let db = Database::create_temp().expect("Failed to create database");

    // Write large values
    let large_value = vec![b'X'; 100_000]; // 100KB per value
    let start = Instant::now();

    for i in 0..100 {
        let key = format!("large_key_{:03}", i);
        db.put(key.as_bytes(), &large_value).expect("Failed to put large value");

        if i % 10 == 0 {
            // Force sync periodically to manage memory
            db.sync().unwrap();
        }
    }

    let duration = start.elapsed();
    let total_mb = (100 * 100) as f64 / 1024.0;
    let mb_per_sec = total_mb / duration.as_secs_f64();

    println!("  Values: 100 x 100KB");
    println!("  Total data: {:.1} MB", total_mb);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.1} MB/sec", mb_per_sec);

    // Verify data integrity
    let key = format!("large_key_{:03}", 50);
    let value = db.get(key.as_bytes()).expect("Failed to get large value");
    assert!(value.is_some());
    assert_eq!(value.unwrap().len(), 100_000);

    println!("  ✓ Large value operations successful\n");
}

fn test_recovery_stress() {
    println!("Test 5: Recovery Stress Test");
    println!("-----------------------------");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("recovery_test");

    // Phase 1: Write data
    {
        let db = Database::open(&db_path, Default::default())
            .expect("Failed to create database");

        for i in 0..1000 {
            let key = format!("recovery_key_{:04}", i);
            let value = format!("recovery_value_{:04}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Explicit sync before closing
        db.sync().unwrap();
        println!("  Phase 1: Wrote 1000 keys");
    }

    // Phase 2: Reopen and modify
    {
        let db = Database::open(&db_path, Default::default())
            .expect("Failed to reopen database");

        // Verify data persisted
        let mut found = 0;
        for i in 0..1000 {
            let key = format!("recovery_key_{:04}", i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                found += 1;
            }
        }
        assert_eq!(found, 1000, "Data not persisted correctly");

        // Delete half
        for i in 0..500 {
            let key = format!("recovery_key_{:04}", i);
            db.delete(key.as_bytes()).unwrap();
        }

        // Add new keys
        for i in 1000..1500 {
            let key = format!("recovery_key_{:04}", i);
            let value = format!("recovery_value_{:04}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        db.sync().unwrap();
        println!("  Phase 2: Modified data (deleted 500, added 500)");
    }

    // Phase 3: Final verification
    {
        let db = Database::open(&db_path, Default::default())
            .expect("Failed to reopen database");

        // Count keys
        let mut total = 0;
        let iter = db.scan(None, None).unwrap();
        for result in iter {
            if result.is_ok() {
                total += 1;
            }
            if total > 2000 {
                break; // Safety limit
            }
        }

        assert!(total >= 1000, "Expected at least 1000 keys, found {}", total);
        println!("  Phase 3: Verified {} keys after recovery", total);
    }

    println!("  ✓ Recovery stress test successful\n");
}