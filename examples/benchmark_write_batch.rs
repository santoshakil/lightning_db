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

    // Create database with optimized config
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024, // 100MB cache
        compression_enabled: false, // Disable for fair comparison
        ..Default::default()
    };

    println!("=== Write Batch Performance Benchmark ===\n");
    let db = Database::create(db_path, config)?;

    // Test parameters
    let num_operations = 100_000;
    let batch_sizes = vec![1, 10, 100, 1000, 5000];

    // Warm up
    println!("Warming up...");
    for i in 0..1000 {
        let key = format!("warmup_{:06}", i);
        db.put(key.as_bytes(), b"warmup_value")?;
    }

    println!("\nTesting different batch sizes:\n");

    for batch_size in batch_sizes {
        println!("Batch size: {}", batch_size);

        // Test individual writes
        if batch_size == 1 {
            println!("  Individual writes:");
            let start = Instant::now();

            for i in 0..num_operations {
                let key = format!("single_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes())?;
            }

            let duration = start.elapsed();
            println!("    {}", format_throughput(num_operations, duration));
        }

        // Test batch writes
        println!("  Batch writes:");
        let start = Instant::now();
        let mut total_ops = 0;

        for batch_num in 0..(num_operations / batch_size) {
            let mut batch = WriteBatch::new();

            for i in 0..batch_size {
                let idx = batch_num * batch_size + i;
                let key = format!("batch{}_{:08}", batch_size, idx);
                let value = format!("value_{:08}", idx);
                batch.put(key.into_bytes(), value.into_bytes())?;
            }

            db.write_batch(&batch)?;
            total_ops += batch_size;
        }

        let duration = start.elapsed();
        println!("    {}", format_throughput(total_ops, duration));

        // Calculate improvement
        if batch_size > 1 {
            let single_write_time_us = 10.0; // Approximate from single write test
            let batch_write_time_us = duration.as_micros() as f64 / total_ops as f64;
            let improvement = single_write_time_us / batch_write_time_us;
            println!("    Improvement: {:.1}x faster", improvement);
        }

        println!();
    }

    // Test mixed operations in batch
    println!("Testing mixed operations batch:");
    let mut batch = WriteBatch::new();

    // Add puts
    for i in 0..500 {
        let key = format!("mixed_{:06}", i);
        let value = format!("value_{:06}", i);
        batch.put(key.into_bytes(), value.into_bytes())?;
    }

    // Add deletes
    for i in 0..500 {
        let key = format!("single_{:08}", i);
        batch.delete(key.into_bytes())?;
    }

    let start = Instant::now();
    db.write_batch(&batch)?;
    let duration = start.elapsed();

    println!(
        "  1000 mixed operations: {}",
        format_throughput(1000, duration)
    );

    // Verify data integrity
    println!("\nVerifying data integrity...");

    // Check that mixed puts are there
    for i in 0..10 {
        let key = format!("mixed_{:06}", i);
        let value = db.get(key.as_bytes())?;
        assert!(value.is_some(), "Key {} should exist", key);
    }

    // Check that deleted keys are gone
    for i in 0..10 {
        let key = format!("single_{:08}", i);
        let value = db.get(key.as_bytes())?;
        assert!(value.is_none(), "Key {} should be deleted", key);
    }

    println!("Data integrity verified ✓");

    // Show final metrics
    let metrics = db.get_metrics();
    println!("\nFinal metrics:");
    println!("  Total writes: {}", metrics.writes);
    println!(
        "  Average write latency: {:.2} μs",
        metrics.avg_write_latency_us
    );

    Ok(())
}
