use lightning_db::{Database, LightningDbConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create database
    let db_path = "data/cache_test_db";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }

    let config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024, // 10MB
        compression_enabled: true,
        compression_type: 1, // Zstd
        ..Default::default()
    };

    let db = Database::create(&db_path, config)?;

    // Print initial cache stats
    println!("Initial cache stats:");
    if let Some(stats) = db.cache_stats() {
        println!("  {}", stats);
    } else {
        println!("  No cache statistics available");
    }

    // Do some operations
    println!("\nDoing 5 put operations...");
    for i in 0..5 {
        let key = format!("test_key_{}", i);
        let value = format!("test_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Print cache stats after puts
    println!("\nCache stats after puts:");
    if let Some(stats) = db.cache_stats() {
        println!("  {}", stats);
    } else {
        println!("  No cache statistics available");
    }

    // Do some get operations
    println!("\nDoing 5 get operations...");
    for i in 0..5 {
        let key = format!("test_key_{}", i);
        let value = db.get(key.as_bytes())?;
        println!(
            "  {} = {:?}",
            key,
            value.map(|v| String::from_utf8_lossy(&v).to_string())
        );
    }

    // Print cache stats after gets
    println!("\nCache stats after gets:");
    if let Some(stats) = db.cache_stats() {
        println!("  {}", stats);
    } else {
        println!("  No cache statistics available");
    }

    // Do more get operations (should hit cache)
    println!("\nDoing 5 more get operations (should hit cache)...");
    for i in 0..5 {
        let key = format!("test_key_{}", i);
        let value = db.get(key.as_bytes())?;
        println!(
            "  {} = {:?}",
            key,
            value.map(|v| String::from_utf8_lossy(&v).to_string())
        );
    }

    // Print final cache stats
    println!("\nFinal cache stats:");
    if let Some(stats) = db.cache_stats() {
        println!("  {}", stats);
    } else {
        println!("  No cache statistics available");
    }

    Ok(())
}
