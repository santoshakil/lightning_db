use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Minimal Config Test");
    println!("=====================\n");

    let dir = tempdir()?;

    // Test with most features disabled
    let config = LightningDbConfig {
        compression_enabled: false,        // Disable LSM
        cache_size: 0,                     // Disable cache
        use_improved_wal: false,           // Use old WAL
        prefetch_enabled: false,           // Disable prefetch
        use_optimized_transactions: false, // Disable optimized transactions
        ..Default::default()
    };

    println!("Creating database with minimal config...");
    let start = Instant::now();
    let db = Database::create(dir.path().join("minimal.db"), config)?;
    println!("  Created in: {:.3}s", start.elapsed().as_secs_f64());

    println!("Doing basic operations...");
    let start = Instant::now();
    db.put(b"key", b"value")?;
    let _result = db.get(b"key")?;
    println!("  Work in: {:.3}s", start.elapsed().as_secs_f64());

    println!("Dropping database...");
    let start = Instant::now();
    drop(db);
    let drop_time = start.elapsed().as_secs_f64();
    println!("  Dropped in: {:.3}s", drop_time);

    if drop_time < 0.1 {
        println!("  🚀 Very fast cleanup!");
    } else if drop_time < 0.5 {
        println!("  ✅ Fast cleanup!");
    } else {
        println!("  ⚠️  Still slow!");
    }

    Ok(())
}
