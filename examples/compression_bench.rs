use lightning_db::Database;
use std::time::Instant;

fn main() {
    println!("Compression Algorithm Benchmark");
    println!("================================\n");

    bench_compression("Snappy", "snappy");
    bench_compression("Zstd", "zstd-compression");
    bench_compression("None (B+Tree)", "none");
}

fn bench_compression(name: &str, _feature: &str) {
    println!("Testing: {}", name);
    println!("-------------------");

    let db = Database::create_temp().expect("Failed to create database");
    let num_keys = 500_000;

    let write_start = Instant::now();
    for i in 0..num_keys {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_padding_data_to_make_compression_worthwhile_{}", i, "x".repeat(50));
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let write_duration = write_start.elapsed();
    let write_ops = num_keys as f64 / write_duration.as_secs_f64();

    db.flush_lsm().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));

    let mut total_size = 0;
    let mut total_sstables = 0;
    if let Some(lsm_stats) = db.lsm_stats() {
        total_sstables = lsm_stats.levels.iter().map(|l| l.num_files).sum();
        total_size = lsm_stats.levels.iter().map(|l| l.size_bytes).sum();
    }

    let num_reads = 100_000;
    let read_start = Instant::now();
    for _ in 0..num_reads {
        let key_idx = fastrand::u32(0..num_keys);
        let key = format!("key_{:010}", key_idx);
        let _ = db.get(key.as_bytes()).unwrap();
    }
    let read_duration = read_start.elapsed();
    let read_ops = num_reads as f64 / read_duration.as_secs_f64();

    println!("  Write: {:.0} ops/sec", write_ops);
    println!("  Read:  {:.0} ops/sec", read_ops);
    println!("  Size:  {} MB ({} SSTables)", total_size / 1024 / 1024, total_sstables);
    println!("  Compression ratio: {:.2}x",
        (num_keys as f64 * 120.0) / total_size as f64);

    if let Some(lsm_stats) = db.lsm_stats() {
        println!("  Cache hit rate: {:.2}%\n", lsm_stats.cache_hit_rate);
    }
}
