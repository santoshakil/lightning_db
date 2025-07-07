use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing default config performance...");

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();

    // Use default config
    let config = LightningDbConfig::default();

    println!("Creating database with default config...");
    let db = Database::create(db_path, config)?;

    println!("Writing 100 entries...");
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
