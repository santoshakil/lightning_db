use lightning_db::Database;
use std::time::Instant;

fn main() {
    println!("Lightning DB Large-Scale Performance Benchmark");
    println!("==============================================\n");

    bench_1m_sequential_writes();
    bench_1m_random_reads();
    bench_range_scan();
}

fn bench_1m_sequential_writes() {
    println!("Benchmark: 1M Sequential Writes");
    println!("---------------------------------");

    let db = Database::create_temp().expect("Failed to create database");
    let num_ops = 1_000_000;

    let start = Instant::now();
    for i in 0..num_ops {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_{:050}", i, i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();

        if i % 100_000 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let ops_per_sec = i as f64 / elapsed.as_secs_f64();
            println!("  Progress: {} ops ({:.0} ops/sec)", i, ops_per_sec);
        }
    }

    let duration = start.elapsed();
    let ops_per_sec = num_ops as f64 / duration.as_secs_f64();

    println!("  Total: {} ops", num_ops);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    println!("  Latency: {:.2} μs/op\n", duration.as_micros() as f64 / num_ops as f64);
}

fn bench_1m_random_reads() {
    println!("Benchmark: 1M Random Reads (from 1M dataset)");
    println!("---------------------------------------------");

    let db = Database::create_temp().expect("Failed to create database");

    let num_keys = 1_000_000;
    println!("  Populating {} keys...", num_keys);
    for i in 0..num_keys {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let num_reads = 1_000_000;
    let start = Instant::now();
    for i in 0..num_reads {
        let key_idx = fastrand::u32(0..num_keys);
        let key = format!("key_{:010}", key_idx);
        let _ = db.get(key.as_bytes()).unwrap();

        if i % 100_000 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let ops_per_sec = i as f64 / elapsed.as_secs_f64();
            println!("  Progress: {} reads ({:.0} reads/sec)", i, ops_per_sec);
        }
    }

    let duration = start.elapsed();
    let ops_per_sec = num_reads as f64 / duration.as_secs_f64();

    println!("  Total reads: {}", num_reads);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} reads/sec", ops_per_sec);
    println!("  Latency: {:.2} μs/read\n", duration.as_micros() as f64 / num_reads as f64);
}

fn bench_range_scan() {
    println!("Benchmark: Range Scans");
    println!("----------------------");

    let db = Database::create_temp().expect("Failed to create database");

    let num_keys = 100_000;
    for i in 0..num_keys {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let ranges = vec![100, 1_000, 10_000];
    for range_size in ranges {
        let start = Instant::now();
        let start_key = format!("key_{:08}", fastrand::u32(0..(num_keys - range_size)));
        let end_key = format!("key_{:08}", start_key.strip_prefix("key_").unwrap().parse::<u32>().unwrap() + range_size);

        let mut count = 0;
        for entry in db.range(Some(start_key.as_bytes()), Some(end_key.as_bytes())).unwrap() {
            count += 1;
            let _ = entry;
        }

        let duration = start.elapsed();
        let throughput = count as f64 / duration.as_secs_f64();

        println!("  Range size: {} keys", range_size);
        println!("    Scanned: {} keys", count);
        println!("    Duration: {:?}", duration);
        println!("    Throughput: {:.0} keys/sec", throughput);
    }
    println!();
}
