use lightning_db::{Database, WriteBatch};
use std::time::Instant;

fn main() {
    println!("Lightning DB Performance Benchmark");
    println!("===================================\n");

    let db = Database::create_temp().expect("Failed to create database");

    // Benchmark 1: Sequential writes
    println!("1. Sequential Writes (10,000 operations)");
    let start = Instant::now();
    for i in 0..10_000 {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}_with_some_padding_data", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Failed to put");
    }
    let duration = start.elapsed();
    let ops_per_sec = 10_000.0 / duration.as_secs_f64();
    println!("   Time: {:?}", duration);
    println!("   Speed: {:.0} ops/sec", ops_per_sec);
    println!("   Latency: {:.2} µs/op\n", duration.as_micros() as f64 / 10_000.0);

    // Benchmark 2: Random reads
    println!("2. Random Reads (10,000 operations)");
    let start = Instant::now();
    for i in 0..10_000 {
        let key = format!("key_{:08}", i);
        db.get(key.as_bytes()).expect("Failed to get");
    }
    let duration = start.elapsed();
    let ops_per_sec = 10_000.0 / duration.as_secs_f64();
    println!("   Time: {:?}", duration);
    println!("   Speed: {:.0} ops/sec", ops_per_sec);
    println!("   Latency: {:.2} µs/op\n", duration.as_micros() as f64 / 10_000.0);

    // Benchmark 3: Batch writes (100 batches of 100 ops)
    println!("3. Batch Writes (100 batches x 100 ops = 10,000 total)");
    let start = Instant::now();
    for batch_num in 0..100 {
        let mut batch = WriteBatch::new();
        for i in 0..100 {
            let key = format!("batch_key_{:04}_{:04}", batch_num, i);
            let value = format!("batch_value_{:04}_{:04}", batch_num, i);
            batch.put(key.into_bytes(), value.into_bytes()).unwrap();
        }
        db.write_batch(&batch).expect("Failed to write batch");
    }
    let duration = start.elapsed();
    let ops_per_sec = 10_000.0 / duration.as_secs_f64();
    println!("   Time: {:?}", duration);
    println!("   Speed: {:.0} ops/sec", ops_per_sec);
    println!("   Latency: {:.2} µs/op\n", duration.as_micros() as f64 / 10_000.0);

    // Benchmark 4: Mixed workload (70% reads, 20% writes, 10% deletes)
    println!("4. Mixed Workload (7000 reads, 2000 writes, 1000 deletes)");
    let start = Instant::now();

    // Writes
    for i in 0..2000 {
        let key = format!("mixed_key_{:06}", i);
        let value = format!("mixed_value_{:06}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Failed to put");
    }

    // Reads
    for i in 0..7000 {
        let key = format!("key_{:08}", i % 10_000);
        let _ = db.get(key.as_bytes()).expect("Failed to get");
    }

    // Deletes
    for i in 0..1000 {
        let key = format!("mixed_key_{:06}", i);
        db.delete(key.as_bytes()).expect("Failed to delete");
    }

    let duration = start.elapsed();
    let ops_per_sec = 10_000.0 / duration.as_secs_f64();
    println!("   Time: {:?}", duration);
    println!("   Speed: {:.0} ops/sec", ops_per_sec);
    println!("   Latency: {:.2} µs/op\n", duration.as_micros() as f64 / 10_000.0);

    // Benchmark 5: Large value writes (1000 x 10KB values)
    println!("5. Large Value Writes (1000 x 10KB values)");
    let large_value = vec![b'X'; 10_240]; // 10KB
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("large_key_{:06}", i);
        db.put(key.as_bytes(), &large_value).expect("Failed to put large");
    }
    let duration = start.elapsed();
    let ops_per_sec = 1000.0 / duration.as_secs_f64();
    let mb_per_sec = (1000.0 * 10.0) / duration.as_secs_f64() / 1024.0;
    println!("   Time: {:?}", duration);
    println!("   Speed: {:.0} ops/sec", ops_per_sec);
    println!("   Throughput: {:.1} MB/sec", mb_per_sec);
    println!("   Latency: {:.2} µs/op\n", duration.as_micros() as f64 / 1000.0);

    println!("Benchmark complete!");
}