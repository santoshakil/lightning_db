use lightning_db::Database;
use std::time::Instant;

fn main() {
    println!("LSM Analysis: SSTable Count and Cache Performance");
    println!("===================================================\n");

    let db = Database::create_temp().expect("Failed to create database");

    let num_keys = 1_000_000;
    println!("Populating {} keys...", num_keys);

    for i in 0..num_keys {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    println!("Flushing LSM...");
    db.flush_lsm().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));

    let mut total_sstables = 0;

    if let Some(lsm_stats) = db.lsm_stats() {
        println!("\nLSM State After Write:");
        println!("  Memtable size: {} bytes", lsm_stats.memtable_size);
        println!("  Immutable memtables: {}", lsm_stats.immutable_count);

        total_sstables = lsm_stats.levels.iter().map(|l| l.num_files).sum();
        let total_size: usize = lsm_stats.levels.iter().map(|l| l.size_bytes).sum();

        println!("  Total SSTables: {}", total_sstables);
        println!("  Total LSM size: {} MB", total_size / 1024 / 1024);

        for level in &lsm_stats.levels {
            if level.num_files > 0 {
                println!("    L{}: {} files, {} MB",
                    level.level,
                    level.num_files,
                    level.size_bytes / 1024 / 1024);
            }
        }
    }

    println!("\nRunning 100K random reads...");
    let num_reads = 100_000;
    let start = Instant::now();

    for _ in 0..num_reads {
        let key_idx = fastrand::u32(0..num_keys);
        let key = format!("key_{:010}", key_idx);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let duration = start.elapsed();
    let ops_per_sec = num_reads as f64 / duration.as_secs_f64();

    println!("  Throughput: {:.0} reads/sec", ops_per_sec);
    println!("  Latency: {:.2} μs/read", duration.as_micros() as f64 / num_reads as f64);

    if let Some(lsm_stats) = db.lsm_stats() {
        println!("\nLSM Cache Performance:");
        println!("  Cache hits: {}", lsm_stats.cache_hits);
        println!("  Cache misses: {}", lsm_stats.cache_misses);
        println!("  Hit rate: {:.2}%", lsm_stats.cache_hit_rate);

        let total_reads = lsm_stats.cache_hits + lsm_stats.cache_misses;
        let avg_sstables_per_read = if total_sstables > 0 && total_reads > 0 {
            (lsm_stats.cache_misses as f64) / (total_reads as f64) * (total_sstables as f64)
        } else {
            0.0
        };

        println!("  Estimated SSTable checks per miss: {:.1}", avg_sstables_per_read);
    }
}
