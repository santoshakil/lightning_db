use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Let me just see if the AutoBatcher hangs on a simple case
    
    let db_path = "/tmp/simple_test.db";
    std::fs::remove_dir_all(db_path).ok(); // Clean up
    
    // Create database with async WAL to avoid sync issues
    let mut config = lightning_db::LightningDbConfig::default();
    config.wal_sync_mode = lightning_db::WalSyncMode::Async;
    let db = Arc::new(lightning_db::Database::create(&db_path, config)?);
    
    println!("Database created");
    
    // Test basic put/get first
    db.put(b"test", b"value")?;
    let result = db.get(b"test")?;
    println!("Basic put/get result: {:?}", result);
    
    // Test AutoBatcher
    println!("Creating AutoBatcher...");
    let batcher = lightning_db::Database::create_auto_batcher(db.clone());
    
    println!("Submitting writes...");
    for i in 0..3 {
        let key = format!("key{}", i);
        batcher.put(key.into_bytes(), b"value".to_vec())?;
        println!("Submitted write {}", i);
    }
    
    println!("Getting stats before wait...");
    let (submitted, completed, batches, errors) = batcher.get_stats();
    println!("Stats: submitted={}, completed={}, batches={}, errors={}", submitted, completed, batches, errors);
    
    println!("Waiting for completion...");
    match batcher.wait_for_completion() {
        Ok(_) => println!("✅ All writes completed"),
        Err(e) => println!("❌ Error: {}", e),
    }
    
    let (submitted, completed, batches, errors) = batcher.get_stats();
    println!("Final stats: submitted={}, completed={}, batches={}, errors={}", submitted, completed, batches, errors);
    
    batcher.shutdown();
    println!("✅ Test completed");
    
    Ok(())
}