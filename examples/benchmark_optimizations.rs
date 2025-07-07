use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Optimization Benchmarks\n");

    let dir = tempdir()?;
    let value = vec![0u8; 100];

    // Test different optimization configurations
    benchmark_wal_optimizations(&dir, &value)?;
    benchmark_thread_local_writes(&dir, &value)?;
    benchmark_simd_operations(&dir)?;
    benchmark_memory_optimizations(&dir, &value)?;

    println!("\n✅ All benchmarks completed!");
    Ok(())
}

fn benchmark_wal_optimizations(
    dir: &tempfile::TempDir,
    value: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("1. WAL Optimization Benchmarks:");

    // Test with old WAL (sleep-based)
    {
        let mut config = LightningDbConfig::default();
        config.use_improved_wal = false;
        config.wal_sync_mode = WalSyncMode::Async;

        let db = Arc::new(Database::create(dir.path().join("wal_old.db"), config)?);
        let batcher = Database::create_auto_batcher(db.clone());

        let count = 10_000;
        let start = Instant::now();

        for i in 0..count {
            batcher.put(format!("key{:06}", i).into_bytes(), value.to_vec())?;
        }
        batcher.flush()?;
        batcher.wait_for_completion()?;

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("   Standard WAL: {:.0} ops/sec", ops_sec);
    }

    // Test with improved WAL (condition variable)
    {
        let mut config = LightningDbConfig::default();
        config.use_improved_wal = true;
        config.wal_sync_mode = WalSyncMode::Async;

        let db = Arc::new(Database::create(dir.path().join("wal_new.db"), config)?);
        let batcher = Database::create_auto_batcher(db.clone());

        let count = 10_000;
        let start = Instant::now();

        for i in 0..count {
            batcher.put(format!("key{:06}", i).into_bytes(), value.to_vec())?;
        }
        batcher.flush()?;
        batcher.wait_for_completion()?;

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("   Improved WAL: {:.0} ops/sec", ops_sec);
    }

    Ok(())
}

fn benchmark_thread_local_writes(
    dir: &tempfile::TempDir,
    value: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n2. Thread-Local Write Benchmarks:");

    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(
        dir.path().join("thread_local.db"),
        config,
    )?);

    // Test regular writes
    {
        let count = 10_000;
        let start = Instant::now();

        for i in 0..count {
            db.put(format!("key{:06}", i).as_bytes(), value)?;
        }

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("   Regular puts: {:.0} ops/sec", ops_sec);
    }

    // Test thread-local writes
    {
        let writer = db.clone().thread_local_writer();
        let count = 10_000;
        let start = Instant::now();

        for i in 0..count {
            writer.put(format!("tl_key{:06}", i).into_bytes(), value.to_vec())?;
        }
        writer.flush()?;

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("   Thread-local: {:.0} ops/sec", ops_sec);
    }

    Ok(())
}

fn benchmark_simd_operations(_dir: &tempfile::TempDir) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n3. SIMD Operation Benchmarks:");

    use lightning_db::simd;

    // Benchmark checksum calculation
    {
        let data = vec![42u8; 4096];
        let iterations = 100_000;

        // Regular CRC32
        let start = Instant::now();
        for _ in 0..iterations {
            use crc32fast::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(&data);
            let _ = hasher.finalize();
        }
        let regular_time = start.elapsed();

        // SIMD CRC32
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = simd::simd_crc32(&data, 0);
        }
        let simd_time = start.elapsed();

        let speedup = regular_time.as_secs_f64() / simd_time.as_secs_f64();
        println!("   Checksum speedup: {:.1}x", speedup);
    }

    // Benchmark zero detection
    {
        let zeros = vec![0u8; 4096];
        let iterations = 100_000;

        // Regular check
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = zeros.iter().all(|&b| b == 0);
        }
        let regular_time = start.elapsed();

        // SIMD check
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = simd::simd_is_zero(&zeros);
        }
        let simd_time = start.elapsed();

        let speedup = regular_time.as_secs_f64() / simd_time.as_secs_f64();
        println!("   Zero detection speedup: {:.1}x", speedup);
    }

    Ok(())
}

fn benchmark_memory_optimizations(
    dir: &tempfile::TempDir,
    value: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n4. Memory Optimization Benchmarks:");

    // Test with standard page manager
    {
        let mut config = LightningDbConfig::default();
        config.use_optimized_page_manager = false;

        let db = Arc::new(Database::create(dir.path().join("standard_pm.db"), config)?);
        let count = 1000;
        let start = Instant::now();

        // Write phase
        for i in 0..count {
            db.put(format!("key{:06}", i).as_bytes(), value)?;
        }

        // Read phase
        for i in 0..count {
            let _ = db.get(format!("key{:06}", i).as_bytes())?;
        }

        let duration = start.elapsed();
        println!(
            "   Standard page manager: {:.2} ms",
            duration.as_secs_f64() * 1000.0
        );
    }

    // Test with optimized page manager
    {
        let mut config = LightningDbConfig::default();
        config.use_optimized_page_manager = true;

        let db = Arc::new(Database::create(
            dir.path().join("optimized_pm.db"),
            config,
        )?);
        let count = 1000;
        let start = Instant::now();

        // Write phase
        for i in 0..count {
            db.put(format!("key{:06}", i).as_bytes(), value)?;
        }

        // Read phase
        for i in 0..count {
            let _ = db.get(format!("key{:06}", i).as_bytes())?;
        }

        let duration = start.elapsed();
        println!(
            "   Optimized page manager: {:.2} ms",
            duration.as_secs_f64() * 1000.0
        );
    }

    Ok(())
}
