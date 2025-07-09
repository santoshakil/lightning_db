use lightning_db::{Database, LightningDbConfig};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Testing optimized page manager...");

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();

    // Create config with optimized page manager
    let mut config = LightningDbConfig {
        prefetch_enabled: false,
        use_optimized_transactions: false,
        use_improved_wal: false,
        cache_size: 0,
        compression_enabled: false,
        use_optimized_page_manager: true,
        ..Default::default()
    };

    // Customize mmap config
    if let Some(ref mut mmap_config) = config.mmap_config {
        mmap_config.enable_async_msync = false; // Disable async msync
        mmap_config.flush_interval = std::time::Duration::from_secs(60); // Longer interval
        mmap_config.enable_huge_pages = false;
        mmap_config.enable_prefault = false;
    }

    println!("Creating database with optimized page manager (sync msync)...");
    let start = std::time::Instant::now();

    let db = Database::create(db_path, config)?;

    println!("Database created in {:?}", start.elapsed());

    // Simple test
    db.put(b"key", b"value")?;
    let val = db.get(b"key")?;
    println!("Got value: {:?}", val);

    Ok(())
}
