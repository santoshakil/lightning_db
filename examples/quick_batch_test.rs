use lightning_db::{write_batch::WriteBatch, Database, LightningDbConfig};
use std::error::Error;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn format_throughput(ops: usize, duration: Duration) -> String {
    let ops_per_sec = ops as f64 / duration.as_secs_f64();
    let latency_us = duration.as_micros() as f64 / ops as f64;
    format!("{:.0} ops/sec, {:.2} μs/op", ops_per_sec, latency_us)
}

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Create database with basic WAL (no group commit) for fair comparison
    let mut config = LightningDbConfig {
        compression_enabled: false,
        ..Default::default()
    }; // Disable for fair comparison
    config.cache_size = 10 * 1024 * 1024; // 10MB cache
    config.wal_sync_mode = lightning_db::WalSyncMode::Sync; // Force sync mode
    config.use_improved_wal = false; // Use basic WAL without group commit

    println!("=== Quick Write Batch Test ===\n");
    let db = Database::create(db_path, config)?;

    // Test parameters
    let num_operations = 10_000;

    // Test 1: Individual writes
    println!("Test 1: Individual writes ({} operations)", num_operations);
    let start = Instant::now();

    for i in 0..num_operations {
        let key = format!("key_{:06}", i);
        let value = format!("value_{:06}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    let individual_duration = start.elapsed();
    let individual_throughput = format_throughput(num_operations, individual_duration);
    println!("  Result: {}", individual_throughput);

    // Test 2: Batch writes (batch size 100)
    let batch_size = 100;
    let num_batches = num_operations / batch_size;

    println!("\nTest 2: Batch writes (batch size {})", batch_size);
    let start = Instant::now();

    for batch_num in 0..num_batches {
        let mut batch = WriteBatch::new();

        for i in 0..batch_size {
            let idx = batch_num * batch_size + i;
            let key = format!("batch_{:06}", idx);
            let value = format!("value_{:06}", idx);
            batch.put(key.into_bytes(), value.into_bytes())?;
        }

        db.write_batch(&batch)?;
    }

    let batch_duration = start.elapsed();
    let batch_throughput = format_throughput(num_operations, batch_duration);
    println!("  Result: {}", batch_throughput);

    // Calculate improvement
    let individual_ops_per_sec = num_operations as f64 / individual_duration.as_secs_f64();
    let batch_ops_per_sec = num_operations as f64 / batch_duration.as_secs_f64();
    let improvement = batch_ops_per_sec / individual_ops_per_sec;

    println!("\nImprovement: {:.1}x faster", improvement);

    // Verify data integrity
    println!("\nVerifying data integrity...");

    // Check some individual writes
    for i in [0, 100, 1000, 5000, 9999] {
        let key = format!("key_{:06}", i);
        let expected = format!("value_{:06}", i);
        let value = db.get(key.as_bytes())?;
        assert_eq!(value.as_deref(), Some(expected.as_bytes()));
    }

    // Check some batch writes
    for i in [0, 100, 1000, 5000, 9999] {
        let key = format!("batch_{:06}", i);
        let expected = format!("value_{:06}", i);
        let value = db.get(key.as_bytes())?;
        assert_eq!(value.as_deref(), Some(expected.as_bytes()));
    }

    println!("Data integrity verified ✓");

    Ok(())
}
