use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing AutoBatcher creation...");

    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Use minimal config to isolate AutoBatcher issue
    let config = LightningDbConfig {
        cache_size: 0,
        compression_enabled: false,
        prefetch_enabled: false,
        use_optimized_transactions: false,
        use_improved_wal: false,
        use_optimized_page_manager: false,
        mmap_config: None,
        ..Default::default()
    };

    println!("Creating database...");
    let db = Arc::new(Database::create(&db_path, config)?);
    println!("Database created!");

    println!("Creating AutoBatcher...");
    let batcher = Database::create_auto_batcher(db.clone());
    println!("AutoBatcher created!");

    println!("Shutting down AutoBatcher...");
    batcher.shutdown();
    println!("AutoBatcher shutdown complete!");

    println!("All tests passed!");
    Ok(())
}
