use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Performance Test (No Sync)\n");

    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Test with fully optimized config but async writes
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = lightning_db::WalSyncMode::Async; // Disable sync on every write

    let db = Database::create(&db_path, config)?;

    // Test parameters
    let write_count = 10_000;
    let value_size = 100;

    let test_value = vec![b'x'; value_size];

    // Write benchmark
    println!("Testing writes (async mode)...");
    let start = Instant::now();
    for i in 0..write_count {
        let key = format!("key_{:08}", i);
        db.put(key.as_bytes(), &test_value)?;
    }
    // Final sync to ensure durability
    db.sync()?;

    let write_duration = start.elapsed();
    let write_ops_per_sec = write_count as f64 / write_duration.as_secs_f64();
    let write_us_per_op = write_duration.as_micros() as f64 / write_count as f64;

    println!("Write Performance (Async):");
    println!("  • {:.0} ops/sec", write_ops_per_sec);
    println!("  • {:.2} μs/op", write_us_per_op);
    println!("  • Target: 100K+ ops/sec, <10μs/op");
    println!(
        "  • Status: {}",
        if write_ops_per_sec >= 100_000.0 {
            "✅ PASS"
        } else {
            "❌ FAIL"
        }
    );

    Ok(())
}
