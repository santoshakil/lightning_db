use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing minimal configuration...");

    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Create absolutely minimal config
    let mut config = LightningDbConfig::default();
    config.cache_size = 0; // No cache
    config.compression_enabled = false; // No LSM tree
    config.prefetch_enabled = false; // No prefetch
    config.use_optimized_transactions = false; // No optimized tx manager
    config.use_improved_wal = false; // Use standard WAL
    config.use_optimized_page_manager = false; // Use standard page manager
    config.mmap_config = None;

    println!("Creating database with minimal config...");
    let db = Database::create(&db_path, config)?;
    println!("Database created successfully!");

    println!("Testing write...");
    db.put(b"test", b"value")?;
    println!("Write successful!");

    println!("Testing read...");
    let value = db.get(b"test")?;
    println!("Read successful: {:?}", value);

    Ok(())
}
