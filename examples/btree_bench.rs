use lightning_db::Database;
use std::time::Instant;

fn main() {
    println!("B+Tree Performance Benchmark");
    println!("=============================\n");

    bench_btree_sequential_writes();
    bench_btree_random_reads();
    bench_btree_mixed_workload();
}

fn bench_btree_sequential_writes() {
    println!("Benchmark: B+Tree 100K Sequential Writes");
    println!("-----------------------------------------");

    let db = Database::create_temp().expect("Failed to create database");
    let num_ops = 100_000;

    let start = Instant::now();
    for i in 0..num_ops {
        let key = format!("btree_key_{:010}", i);
        let value = format!("btree_value_{:010}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let duration = start.elapsed();
    let ops_per_sec = num_ops as f64 / duration.as_secs_f64();

    println!("  Total: {} ops", num_ops);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    println!("  Latency: {:.2} μs/op\n", duration.as_micros() as f64 / num_ops as f64);
}

fn bench_btree_random_reads() {
    println!("Benchmark: B+Tree 100K Random Reads");
    println!("------------------------------------");

    let db = Database::create_temp().expect("Failed to create database");

    let num_keys = 100_000;
    println!("  Populating {} keys...", num_keys);
    for i in 0..num_keys {
        let key = format!("btree_key_{:010}", i);
        let value = format!("btree_value_{:010}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let num_reads = 100_000;
    let start = Instant::now();
    for _ in 0..num_reads {
        let key_idx = fastrand::u32(0..num_keys);
        let key = format!("btree_key_{:010}", key_idx);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let duration = start.elapsed();
    let ops_per_sec = num_reads as f64 / duration.as_secs_f64();

    println!("  Total reads: {}", num_reads);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} reads/sec", ops_per_sec);
    println!("  Latency: {:.2} μs/read\n", duration.as_micros() as f64 / num_reads as f64);
}

fn bench_btree_mixed_workload() {
    println!("Benchmark: B+Tree Mixed Workload (50% read, 50% write)");
    println!("-------------------------------------------------------");

    let db = Database::create_temp().expect("Failed to create database");

    let num_keys = 10_000;
    println!("  Initial population: {} keys...", num_keys);
    for i in 0..num_keys {
        let key = format!("btree_key_{:010}", i);
        let value = format!("btree_value_{:010}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let num_ops = 50_000;
    let start = Instant::now();
    for i in 0..num_ops {
        if i % 2 == 0 {
            let key_idx = fastrand::u32(0..num_keys);
            let key = format!("btree_key_{:010}", key_idx);
            let _ = db.get(key.as_bytes()).unwrap();
        } else {
            let key_idx = fastrand::u32(0..num_keys * 2);
            let key = format!("btree_key_{:010}", key_idx);
            let value = format!("btree_value_{:010}", key_idx);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    let duration = start.elapsed();
    let ops_per_sec = num_ops as f64 / duration.as_secs_f64();

    println!("  Total ops: {}", num_ops);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    println!("  Latency: {:.2} μs/op\n", duration.as_micros() as f64 / num_ops as f64);
}
