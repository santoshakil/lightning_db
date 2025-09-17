use lightning_db::{Database, WriteBatch};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};

fn main() {
    println!("Lightning DB Performance Test");
    println!("=============================\n");

    // Test 1: Sequential write performance
    test_sequential_writes();

    // Test 2: Random read performance
    test_random_reads();

    // Test 3: Concurrent read/write performance
    test_concurrent_operations();

    // Test 4: Batch operations performance
    test_batch_operations();

    // Test 5: Large value performance
    test_large_values();

    println!("\n✅ All performance tests completed!");
}

fn test_sequential_writes() {
    println!("Test 1: Sequential Write Performance");
    println!("------------------------------------");

    let db = Database::create_temp().expect("Failed to create database");
    let num_ops = 100_000;

    let start = Instant::now();
    for i in 0..num_ops {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let duration = start.elapsed();
    let ops_per_sec = num_ops as f64 / duration.as_secs_f64();

    println!("  Operations: {}", num_ops);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    println!("  Latency: {:.2} μs/op\n", duration.as_micros() as f64 / num_ops as f64);
}

fn test_random_reads() {
    println!("Test 2: Random Read Performance");
    println!("-------------------------------");

    let db = Database::create_temp().expect("Failed to create database");

    // Populate database
    let num_keys = 10_000;
    for i in 0..num_keys {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Random reads
    let num_reads = 100_000;
    let start = Instant::now();
    for _ in 0..num_reads {
        let key_idx = fastrand::u32(0..num_keys);
        let key = format!("key_{:08}", key_idx);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let duration = start.elapsed();
    let ops_per_sec = num_reads as f64 / duration.as_secs_f64();

    println!("  Keys in DB: {}", num_keys);
    println!("  Read operations: {}", num_reads);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} reads/sec", ops_per_sec);
    println!("  Latency: {:.2} μs/read\n", duration.as_micros() as f64 / num_reads as f64);
}

fn test_concurrent_operations() {
    println!("Test 3: Concurrent Read/Write Performance");
    println!("-----------------------------------------");

    let db = Arc::new(Database::create_temp().expect("Failed to create database"));
    let total_ops = Arc::new(AtomicU64::new(0));

    // Pre-populate
    for i in 0..1000 {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let start = Instant::now();
    let mut handles = vec![];

    // Writer threads
    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let ops_clone = Arc::clone(&total_ops);

        let handle = thread::spawn(move || {
            for i in 0..5000 {
                let key = format!("thread_{}_key_{:04}", thread_id, i);
                let value = format!("thread_{}_value_{:04}", thread_id, i);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                ops_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Reader threads
    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let ops_clone = Arc::clone(&total_ops);

        let handle = thread::spawn(move || {
            for _ in 0..10000 {
                let key_idx = thread_id * 250 + (fastrand::u32(0..250) as usize);
                let key = format!("key_{:08}", key_idx);
                let _ = db_clone.get(key.as_bytes());
                ops_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let ops_per_sec = total as f64 / duration.as_secs_f64();

    println!("  Writer threads: 4");
    println!("  Reader threads: 4");
    println!("  Total operations: {}", total);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} ops/sec\n", ops_per_sec);
}

fn test_batch_operations() {
    println!("Test 4: Batch Operations Performance");
    println!("------------------------------------");

    let db = Database::create_temp().expect("Failed to create database");

    let batch_sizes = vec![10, 100, 1000, 10000];

    for batch_size in batch_sizes {
        let start = Instant::now();
        let mut batch = WriteBatch::new();

        for i in 0..batch_size {
            let key = format!("batch_key_{:06}", i);
            let value = format!("batch_value_{:06}", i);
            batch.put(key.into_bytes(), value.into_bytes()).unwrap();
        }

        db.write_batch(&batch).unwrap();

        let duration = start.elapsed();
        let ops_per_sec = batch_size as f64 / duration.as_secs_f64();

        println!("  Batch size: {}", batch_size);
        println!("    Duration: {:?}", duration);
        println!("    Throughput: {:.0} ops/sec", ops_per_sec);
        println!("    Latency: {:.2} μs/op", duration.as_micros() as f64 / batch_size as f64);
    }
    println!();
}

fn test_large_values() {
    println!("Test 5: Large Value Performance");
    println!("-------------------------------");

    let db = Database::create_temp().expect("Failed to create database");

    let value_sizes = vec![1024, 10240, 102400, 1024000]; // 1KB, 10KB, 100KB, 1MB

    for value_size in value_sizes {
        let value = vec![b'X'; value_size];
        let num_ops = 100;

        let start = Instant::now();
        for i in 0..num_ops {
            let key = format!("large_key_{:03}", i);
            db.put(key.as_bytes(), &value).unwrap();
        }

        let duration = start.elapsed();
        let mb_written = (value_size * num_ops) as f64 / (1024.0 * 1024.0);
        let mb_per_sec = mb_written / duration.as_secs_f64();

        println!("  Value size: {} KB", value_size / 1024);
        println!("    Operations: {}", num_ops);
        println!("    Data written: {:.2} MB", mb_written);
        println!("    Duration: {:?}", duration);
        println!("    Throughput: {:.2} MB/sec", mb_per_sec);
    }
}