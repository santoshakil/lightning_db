use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn bench_config(name: &str, config: LightningDbConfig) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::create(temp_dir.path(), config)?;

    let count = 10_000;
    let value = vec![b'x'; 100];

    // Warmup
    for i in 0..100 {
        db.put(format!("warmup_{}", i).as_bytes(), &value)?;
    }

    // Benchmark
    let start = Instant::now();
    for i in 0..count {
        let key = format!("key_{:08}", i);
        db.put(key.as_bytes(), &value)?;
    }
    let elapsed = start.elapsed();

    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    let us_per_op = elapsed.as_micros() as f64 / count as f64;

    println!(
        "{:<35} {:>8.0} ops/sec  {:>6.1} μs/op",
        name, ops_per_sec, us_per_op
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Benchmarking Lightning DB components in RELEASE mode...\n");
    println!(
        "{:<35} {:>15}  {:>10}",
        "Configuration", "Throughput", "Latency"
    );
    println!("{}", "-".repeat(65));

    // Absolute minimal - direct B+Tree writes
    let mut config = LightningDbConfig {
        cache_size: 0,
        compression_enabled: false,
        prefetch_enabled: false,
        use_improved_wal: false,
        use_optimized_transactions: false,
        use_optimized_page_manager: false,
        mmap_config: None,
        ..Default::default()
    };
    bench_config("Minimal (B+Tree only)", config.clone())?;

    // Add components one by one
    config.use_improved_wal = true;
    config.wal_sync_mode = lightning_db::WalSyncMode::Async;
    bench_config("+ Async WAL", config.clone())?;

    config.cache_size = 100 * 1024 * 1024;
    bench_config("+ Cache (100MB)", config.clone())?;

    config.compression_enabled = true;
    bench_config("+ Compression", config.clone())?;

    config.prefetch_enabled = true;
    bench_config("+ Prefetch", config.clone())?;

    // Try with mmap
    config.mmap_config = Some(lightning_db::storage::MmapConfig::default());
    bench_config("+ Memory-mapped I/O", config.clone())?;

    // Default config
    bench_config("Default config", LightningDbConfig::default())?;

    // Optimized for writes
    let write_optimized = LightningDbConfig {
        compression_enabled: false,
        cache_size: 0,
        prefetch_enabled: false,
        ..Default::default()
    };
    bench_config("Write-optimized (no cache/comp)", write_optimized)?;

    Ok(())
}
