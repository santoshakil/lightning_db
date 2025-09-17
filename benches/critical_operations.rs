use lightning_db::{Database, WriteBatch};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

fn main() {
    println!("Lightning DB Critical Operations Benchmark");
    println!("==========================================\n");

    benchmark_single_operations();
    benchmark_transaction_operations();
    benchmark_recovery_operations();
    benchmark_compression_operations();
    benchmark_memory_efficiency();
    benchmark_concurrent_stress();

    println!("\n✅ All critical benchmarks completed!");
}

fn benchmark_single_operations() {
    println!("1. Single Operation Performance");
    println!("--------------------------------");

    let db = Database::create_temp().unwrap();

    let key_sizes = [8, 32, 128, 512, 4096];
    let value_sizes = [8, 64, 512, 4096, 65536];

    for &key_size in &key_sizes {
        for &value_size in &value_sizes {
            let key = vec![b'k'; key_size];
            let value = vec![b'v'; value_size];

            let write_start = Instant::now();
            db.put(&key, &value).unwrap();
            let write_time = write_start.elapsed();

            let read_start = Instant::now();
            let _ = db.get(&key).unwrap();
            let read_time = read_start.elapsed();

            let delete_start = Instant::now();
            db.delete(&key).unwrap();
            let delete_time = delete_start.elapsed();

            println!("  Key: {}B, Value: {}B", key_size, value_size);
            println!("    Write: {:.2} μs", write_time.as_nanos() as f64 / 1000.0);
            println!("    Read:  {:.2} μs", read_time.as_nanos() as f64 / 1000.0);
            println!("    Delete: {:.2} μs", delete_time.as_nanos() as f64 / 1000.0);
        }
    }
    println!();
}

fn benchmark_transaction_operations() {
    println!("2. Transaction Performance");
    println!("--------------------------");

    let db = Database::create_temp().unwrap();

    let tx_sizes = [1, 10, 100, 1000];

    for &size in &tx_sizes {
        let mut commit_times = Vec::new();
        let mut abort_times = Vec::new();

        for i in 0..10 {
            let tx = db.begin_transaction().unwrap();

            let start = Instant::now();
            for j in 0..size {
                let key = format!("tx_key_{}_{}", i, j);
                let value = format!("tx_value_{}_{}", i, j);
                db.put_tx(tx, key.as_bytes(), value.as_bytes()).unwrap();
            }

            if i % 2 == 0 {
                db.commit_transaction(tx).unwrap();
                commit_times.push(start.elapsed());
            } else {
                db.abort_transaction(tx).unwrap();
                abort_times.push(start.elapsed());
            }
        }

        let avg_commit = commit_times.iter()
            .map(|d| d.as_nanos() as f64)
            .sum::<f64>() / commit_times.len() as f64 / 1000.0;

        let avg_abort = abort_times.iter()
            .map(|d| d.as_nanos() as f64)
            .sum::<f64>() / abort_times.len() as f64 / 1000.0;

        println!("  Transaction size: {} ops", size);
        println!("    Avg commit: {:.2} μs", avg_commit);
        println!("    Avg abort: {:.2} μs", avg_abort);
    }
    println!();
}

fn benchmark_recovery_operations() {
    println!("3. Recovery & Consistency");
    println!("-------------------------");

    let num_entries = 10000;
    let checkpoint_interval = 1000;

    let db_path = tempfile::tempdir().unwrap();
    let db_path_str = db_path.path().to_str().unwrap();

    {
        let db = Database::open(db_path_str, Default::default()).unwrap();

        let start = Instant::now();
        for i in 0..num_entries {
            let key = format!("recovery_key_{:08}", i);
            let value = format!("recovery_value_{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();

            if i % checkpoint_interval == 0 {
                db.flush_lsm().unwrap();
            }
        }
        let write_time = start.elapsed();
        println!("  Initial write time: {:?}", write_time);
    }

    let recovery_start = Instant::now();
    {
        let db = Database::open(db_path_str, Default::default()).unwrap();
        let recovery_time = recovery_start.elapsed();
        println!("  Recovery time: {:?}", recovery_time);

        let verify_start = Instant::now();
        let mut verified = 0;
        for i in 0..num_entries {
            let key = format!("recovery_key_{:08}", i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                verified += 1;
            }
        }
        let verify_time = verify_start.elapsed();
        println!("  Verification time: {:?}", verify_time);
        println!("  Verified entries: {}/{}", verified, num_entries);
    }
    println!();
}

fn benchmark_compression_operations() {
    println!("4. Compression Performance");
    println!("--------------------------");

    let db = Database::create_temp().unwrap();

    let data_types = vec![
        ("random", generate_random_data(1024)),
        ("text", "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(20).into_bytes()),
        ("json", r#"{"name":"John","age":30,"city":"New York","tags":["a","b","c"]}"#.repeat(15).into_bytes()),
        ("zeros", vec![0u8; 1024]),
        ("pattern", (0..1024).map(|i| (i % 256) as u8).collect()),
    ];

    for (name, data) in &data_types {
        let key = format!("compress_{}", name);

        let write_start = Instant::now();
        db.put(key.as_bytes(), data).unwrap();
        let write_time = write_start.elapsed();

        let read_start = Instant::now();
        let read_data = db.get(key.as_bytes()).unwrap().unwrap();
        let read_time = read_start.elapsed();

        assert_eq!(read_data, *data);

        println!("  Data type: {} ({}B)", name, data.len());
        println!("    Write time: {:.2} μs", write_time.as_nanos() as f64 / 1000.0);
        println!("    Read time: {:.2} μs", read_time.as_nanos() as f64 / 1000.0);
    }
    println!();
}

fn benchmark_memory_efficiency() {
    println!("5. Memory Efficiency");
    println!("--------------------");

    let db = Database::create_temp().unwrap();

    let batch_sizes = [100, 1000, 5000];

    for &batch_size in &batch_sizes {
        let start = Instant::now();

        let mut batch = WriteBatch::new();
        for i in 0..batch_size {
            let key = format!("mem_key_{:08}", i);
            let value = vec![b'v'; 100];
            batch.put(key.into_bytes(), value).unwrap();
        }

        db.write_batch(&batch).unwrap();
        let batch_time = start.elapsed();

        let single_start = Instant::now();
        for i in 0..batch_size {
            let key = format!("single_key_{:08}", i);
            let value = vec![b'v'; 100];
            db.put(key.as_bytes(), value.as_slice()).unwrap();
        }
        let single_time = single_start.elapsed();

        println!("  Batch size: {} ops", batch_size);
        println!("    Batch write: {:?}", batch_time);
        println!("    Single writes: {:?}", single_time);
        println!("    Speedup: {:.2}x", single_time.as_nanos() as f64 / batch_time.as_nanos() as f64);
    }
    println!();
}

fn benchmark_concurrent_stress() {
    println!("6. Concurrent Stress Test");
    println!("-------------------------");

    let db = Arc::new(Database::create_temp().unwrap());
    let thread_counts = [2, 4, 8];

    for &thread_count in &thread_counts {
        let barrier = Arc::new(Barrier::new(thread_count));
        let mut handles = vec![];

        let ops_per_thread = 10000;

        let start = Instant::now();

        for thread_id in 0..thread_count {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                barrier.wait();

                for i in 0..ops_per_thread {
                    let key = format!("thread_{}_{}", thread_id, i);
                    let value = format!("value_{}_{}", thread_id, i);

                    match i % 3 {
                        0 => db.put(key.as_bytes(), value.as_bytes()).unwrap(),
                        1 => {
                            let _ = db.get(key.as_bytes());
                        }
                        _ => {db.delete(key.as_bytes()).unwrap();}
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = thread_count * ops_per_thread;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

        println!("  Threads: {}", thread_count);
        println!("    Total ops: {}", total_ops);
        println!("    Duration: {:?}", duration);
        println!("    Throughput: {:.0} ops/sec", ops_per_sec);
        println!("    Latency: {:.2} μs/op", duration.as_micros() as f64 / total_ops as f64);
    }
    println!();
}

fn generate_random_data(size: usize) -> Vec<u8> {
    (0..size).map(|_| fastrand::u8(..)).collect()
}