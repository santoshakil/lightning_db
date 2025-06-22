use lightning_db::{Database, LightningDbConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Quick Demo Test");

    // Create database
    let db_path = "data/demo_quick_db";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }

    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024, // 100MB
        compression_enabled: true,
        compression_type: 1, // Zstd
        ..Default::default()
    };

    let db = Database::create(db_path, config)?;

    // Initial cache stats
    println!("\nInitial cache stats:");
    if let Some(stats) = db.cache_stats() {
        println!("  {}", stats);
    }

    // Do some basic operations
    for i in 0..10 {
        let key = format!("basic_key_{:02}", i);
        let value = format!("Basic value {} with some additional data", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    println!("\nAfter 10 puts:");
    if let Some(stats) = db.cache_stats() {
        println!("  {}", stats);
    }

    // Read the data back
    for i in 0..10 {
        let key = format!("basic_key_{:02}", i);
        let _result = db.get(key.as_bytes())?;
    }

    println!("\nAfter 10 gets:");
    if let Some(stats) = db.cache_stats() {
        println!("  {}", stats);
    }

    // LSM stats
    if let Some(lsm_stats) = db.lsm_stats() {
        println!("\nLSM Tree Stats:");
        println!("  Cache hits: {}", lsm_stats.cache_hits);
        println!("  Cache misses: {}", lsm_stats.cache_misses);
        println!("  Cache hit rate: {:.2}%", lsm_stats.cache_hit_rate);
    }

    Ok(())
}
