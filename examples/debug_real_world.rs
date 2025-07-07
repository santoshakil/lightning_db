/// Debug version to find the issue
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 DEBUG REAL-WORLD TEST");
    println!("========================\n");

    println!("Creating temporary directory...");
    let temp_dir = TempDir::new()?;

    println!("Creating database config...");
    let config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024, // 10MB
        ..Default::default()
    };

    println!("Creating database...");
    let start = Instant::now();
    let db = Database::create(temp_dir.path(), config)?;
    println!("Database created in {:.2}ms", start.elapsed().as_millis());

    println!("\nWriting 10 keys...");
    for i in 0..10 {
        let key = format!("key:{}", i);
        let value = format!("value:{}", i);
        println!("  Writing {}...", key);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    println!("\nReading 10 keys...");
    for i in 0..10 {
        let key = format!("key:{}", i);
        println!("  Reading {}...", key);
        if let Some(value) = db.get(key.as_bytes())? {
            println!("    Found: {}", String::from_utf8_lossy(&value));
        } else {
            println!("    Not found!");
        }
    }

    println!("\n✅ Test completed successfully!");

    Ok(())
}
