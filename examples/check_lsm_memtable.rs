use lightning_db::{Database, LightningDbConfig};
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Test with compression enabled (LSM tree enabled)
    let mut config = LightningDbConfig {
        compression_enabled: true,
        ..Default::default()
    }; // Enable compression to enable LSM tree
    config.cache_size = 0; // Disable cache
    config.use_optimized_transactions = false;

    println!("=== Testing LSM memtable vs SSTable ===");
    let db = Database::create(db_path, config)?;

    // Insert test data
    println!("\nInserting test data...");
    for i in 1..=5 {
        let key = format!("key_{:02}", i);
        let value = format!("value_{:02}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("Inserted: {} => {}", key, value);
    }

    // Test scan immediately after insert (data should be in memtable)
    println!("\n=== Test 1: Scan immediately after insert ===");
    let mut count = 0;
    let iter = db.scan(None, None)?;
    for result in iter {
        let (key, value) = result?;
        println!(
            "Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("Total items found: {} (expected: 5)", count);

    // Force flush/sync
    println!("\n=== Forcing flush/sync ===");
    db.sync()?;

    // Insert more data to trigger potential memtable flush
    println!("\nInserting more data to potentially trigger flush...");
    for i in 6..=100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
        if i % 20 == 0 {
            println!("Inserted {} items...", i);
        }
    }

    // Test scan after potential flush
    println!("\n=== Test 2: Scan after inserting 100 items ===");
    count = 0;
    let iter = db.scan(None, None)?;
    for result in iter.take(10) {
        let (key, value) = result?;
        println!(
            "Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("... (showing first 10 items)");

    // Count total
    let iter = db.scan(None, None)?;
    count = iter.count();
    println!("Total items found: {} (expected: 100)", count);

    // Test direct get to confirm data is accessible
    println!("\n=== Test 3: Direct get operations ===");
    for i in [1, 50, 100] {
        let key = format!("key_{:03}", i);
        match db.get(key.as_bytes())? {
            Some(value) => {
                println!("Get '{}' => '{}'", key, String::from_utf8_lossy(&value));
            }
            None => {
                println!("Get '{}' => NOT FOUND", key);
            }
        }
    }

    Ok(())
}
