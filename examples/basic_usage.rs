use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test.db");
    
    println!("Creating database at: {:?}", db_path);
    
    // Use default config with some customizations
    let config = LightningDbConfig {
        page_size: 4096,
        cache_size: 10 * 1024 * 1024,
        compression_enabled: true,
        enable_statistics: true,
        ..Default::default()
    };
    
    println!("Opening database...");
    let db = Database::open(&db_path, config)?;
    
    println!("Writing test data...");
    db.put(b"key1", b"value1")?;
    db.put(b"key2", b"value2")?;
    db.put(b"key3", b"value3")?;
    
    println!("Reading test data...");
    let val1 = db.get(b"key1")?;
    assert_eq!(val1, Some(b"value1".to_vec()));
    println!("key1 = {:?}", String::from_utf8_lossy(&val1.unwrap()));
    
    let val2 = db.get(b"key2")?;
    assert_eq!(val2, Some(b"value2".to_vec()));
    println!("key2 = {:?}", String::from_utf8_lossy(&val2.unwrap()));
    
    println!("Deleting key2...");
    db.delete(b"key2")?;
    
    let val2_after = db.get(b"key2")?;
    assert_eq!(val2_after, None);
    println!("key2 after delete = None");
    
    println!("Testing range query...");
    let mut count = 0;
    for (key, value) in db.range(Some(b"key1"), Some(b"key9"))? {
        println!("  {} = {}", 
            String::from_utf8_lossy(&key), 
            String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Found {} keys in range", count);
    
    println!("Getting database stats...");
    let stats = db.stats();
    println!("  Page count: {}", stats.page_count);
    println!("  Free pages: {}", stats.free_page_count);
    println!("  Tree height: {}", stats.tree_height);
    println!("  Active transactions: {}", stats.active_transactions);
    println!("  Memory usage: {} bytes", stats.memory_usage_bytes);
    
    println!("\n✅ All tests passed!");
    
    Ok(())
}