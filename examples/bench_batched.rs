use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Batched Write Performance Test\n");

    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Test with fully optimized config
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config)?;

    // Test parameters
    let total_writes = 100_000;
    let batch_size = 1000;
    let value_size = 100;

    let test_value = vec![b'x'; value_size];

    // Single write performance (baseline)
    println!("Testing single writes (first 1000)...");
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("single_{:08}", i);
        db.put(key.as_bytes(), &test_value)?;
    }
    let single_duration = start.elapsed();
    let single_ops_per_sec = 1000.0 / single_duration.as_secs_f64();
    println!("  • Single writes: {:.0} ops/sec", single_ops_per_sec);

    // Batched write performance using transactions
    println!("\nTesting batched writes (100K total, 1K per batch)...");
    let start = Instant::now();

    for batch in 0..(total_writes / batch_size) {
        let tx_id = db.begin_transaction()?;

        for i in 0..batch_size {
            let key = format!("batch_{:08}_{:08}", batch, i);
            db.put_tx(tx_id, key.as_bytes(), &test_value)?;
        }

        db.commit_transaction(tx_id)?;
    }

    let batch_duration = start.elapsed();
    let batch_ops_per_sec = total_writes as f64 / batch_duration.as_secs_f64();
    let batch_us_per_op = batch_duration.as_micros() as f64 / total_writes as f64;

    println!("Batched Write Performance:");
    println!("  • {:.0} ops/sec", batch_ops_per_sec);
    println!("  • {:.2} μs/op", batch_us_per_op);
    println!("  • Target: 100K+ ops/sec, <10μs/op");
    println!(
        "  • Status: {}",
        if batch_ops_per_sec >= 100_000.0 {
            "✅ PASS"
        } else {
            "❌ FAIL"
        }
    );

    println!(
        "\n📊 Improvement from batching: {:.1}x",
        batch_ops_per_sec / single_ops_per_sec
    );

    // Test read performance to ensure data was written correctly
    println!("\nVerifying data integrity...");
    let sample_key = format!("batch_{:08}_{:08}", 0, 0);
    match db.get(sample_key.as_bytes())? {
        Some(val) => {
            if val == test_value {
                println!("✅ Data integrity verified");
            } else {
                println!("❌ Data corruption detected!");
            }
        }
        None => println!("❌ Data not found!"),
    }

    Ok(())
}
