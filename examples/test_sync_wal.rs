use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing sync WAL performance...");

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();

    let config = LightningDbConfig {
        wal_sync_mode: WalSyncMode::Sync,
        use_optimized_transactions: false, // Make sure this is off
        ..Default::default()
    };

    let db = Database::create(db_path, config)?;

    println!("Writing 100 entries with sync WAL...");
    let start = Instant::now();

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);

        if i % 10 == 0 {
            println!("  Writing key {} at {:?}", i, start.elapsed());
        }

        db.put(key.as_bytes(), value.as_bytes())?;
    }

    let elapsed = start.elapsed();
    println!("Wrote 100 entries in {:?}", elapsed);
    println!("{:.0} ops/sec", 100.0 / elapsed.as_secs_f64());

    Ok(())
}
