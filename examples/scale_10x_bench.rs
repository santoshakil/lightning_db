use lightning_db::Database;
use std::time::Instant;

fn main() {
    println!("10x Scale Test: 10M Keys Performance");
    println!("======================================\n");

    let db = Database::create_temp().expect("Failed to create database");

    println!("Writing 10M keys...");
    let num_keys = 10_000_000;
    let start = Instant::now();

    for i in 0..num_keys {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();

        if i % 1_000_000 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let ops_per_sec = i as f64 / elapsed.as_secs_f64();
            println!("  Progress: {} M keys ({:.0} ops/sec)", i / 1_000_000, ops_per_sec);
        }
    }

    let write_duration = start.elapsed();
    let write_ops = num_keys as f64 / write_duration.as_secs_f64();

    println!("  Total: {} M keys", num_keys / 1_000_000);
    println!("  Duration: {:.2}s", write_duration.as_secs_f64());
    println!("  Throughput: {:.0} ops/sec\n", write_ops);

    println!("Flushing LSM...");
    db.flush_lsm().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(3));

    if let Some(lsm_stats) = db.lsm_stats() {
        println!("\nLSM State:");
        let total_sstables: usize = lsm_stats.levels.iter().map(|l| l.num_files).sum();
        let total_size: usize = lsm_stats.levels.iter().map(|l| l.size_bytes).sum();

        println!("  Total SSTables: {}", total_sstables);
        println!("  Total size: {} MB", total_size / 1024 / 1024);

        for level in &lsm_stats.levels {
            if level.num_files > 0 {
                println!("    L{}: {} files, {} MB",
                    level.level,
                    level.num_files,
                    level.size_bytes / 1024 / 1024);
            }
        }
    }

    println!("\nRunning 100K random reads from 10M keys...");
    let num_reads = 100_000;
    let read_start = Instant::now();

    for _ in 0..num_reads {
        let key_idx = fastrand::u32(0..num_keys);
        let key = format!("key_{:010}", key_idx);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let read_duration = read_start.elapsed();
    let read_ops = num_reads as f64 / read_duration.as_secs_f64();

    println!("  Throughput: {:.0} reads/sec", read_ops);
    println!("  Latency: {:.2} μs/read", read_duration.as_micros() as f64 / num_reads as f64);

    if let Some(lsm_stats) = db.lsm_stats() {
        println!("\nCache Performance:");
        println!("  Hit rate: {:.2}%", lsm_stats.cache_hit_rate);
    }
}
