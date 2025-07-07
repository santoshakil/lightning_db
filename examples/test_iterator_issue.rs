use lightning_db::{Database, LightningDbConfig};
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;

    // Insert test data
    println!("Inserting test data...");
    for i in 0..10 {
        let key = format!("test_{:02}", i);
        let value = format!("value_{:02}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("  Inserted: {} => {}", key, value);
    }

    // Test 1: Full scan
    println!("\nTest 1: Full scan");
    let mut count = 0;
    let iter = db.scan(None, None)?;
    for result in iter {
        let (key, value) = result?;
        println!(
            "  Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("  Total items found: {}", count);

    // Test 2: Prefix scan
    println!("\nTest 2: Prefix scan for 'test_0'");
    count = 0;
    let iter = db.scan_prefix(b"test_0")?;
    for result in iter {
        let (key, value) = result?;
        println!(
            "  Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("  Total items found: {}", count);

    // Test 3: Range scan
    println!("\nTest 3: Range scan from test_03 to test_07");
    count = 0;
    let iter = db.scan(Some(b"test_03".to_vec()), Some(b"test_07".to_vec()))?;
    for result in iter {
        let (key, value) = result?;
        println!(
            "  Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("  Total items found: {}", count);

    // Test 4 and 5 commented out - these were testing internal B+Tree functionality
    // that's not exposed in the public API

    Ok(())
}
