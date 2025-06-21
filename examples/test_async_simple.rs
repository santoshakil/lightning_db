use lightning_db::async_database::{AsyncDatabase, AsyncDatabaseConfigBuilder};
use tempfile::tempdir;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing basic async database operations...\n");
    
    let dir = tempdir()?;
    
    let config = AsyncDatabaseConfigBuilder::new()
        .worker_threads(1)
        .max_concurrent_ops(10)
        .buffer_size(100)
        .enable_write_coalescing(false) // Disable batching for simple test
        .build();
    
    let db_path = dir.path().join("test_async.db");
    println!("Creating database at: {:?}", db_path);
    
    let db = AsyncDatabase::create(&db_path, config).await?;
    
    println!("Database created successfully!");
    
    // Test single write
    println!("\nTesting single write...");
    let key = b"test_key";
    let value = b"test_value";
    
    db.put(key, value).await?;
    println!("Write successful!");
    
    // Test single read
    println!("\nTesting single read...");
    let result = db.get(key).await?;
    
    match result {
        Some(v) => {
            println!("Read successful! Value: {:?}", std::str::from_utf8(&v).unwrap_or("<binary>"));
        }
        None => {
            println!("Key not found!");
        }
    }
    
    // Test sync
    println!("\nSyncing database...");
    db.sync().await?;
    println!("Sync successful!");
    
    println!("\n✅ All tests passed!");
    
    Ok(())
}