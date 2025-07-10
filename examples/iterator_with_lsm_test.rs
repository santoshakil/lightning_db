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

    println!("=== Testing with LSM tree enabled ===");
    let db = Database::create(db_path, config)?;

    // Insert test data
    println!("\nInserting test data...");

    // Insert data with various prefixes
    let test_data = vec![
        ("scan_01", "value_01"),
        ("scan_02", "value_02"),
        ("scan_03", "value_03"),
        ("scan_04", "value_04"),
        ("scan_05", "value_05"),
        ("scan_06", "value_06"),
        ("scan_07", "value_07"),
        ("scan_08", "value_08"),
        ("scan_09", "value_09"),
        ("scan_10", "value_10"),
    ];

    for (key, value) in &test_data {
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("Inserted: {} => {}", key, value);
    }

    // Force sync to ensure data is written
    db.sync()?;

    // Test 1: Full scan
    println!("\n=== Test 1: Full scan ===");
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
    println!("Total items found: {} (expected: 10)", count);

    // Test 2: Prefix scan
    println!("\n=== Test 2: Prefix scan for 'scan_0' ===");
    count = 0;
    let iter = db.scan_prefix(b"scan_0")?;
    for result in iter {
        let (key, value) = result?;
        println!(
            "Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("Total items found: {} (expected: 9)", count);

    // Test 3: Range scan
    println!("\n=== Test 3: Range scan from scan_03 to scan_07 ===");
    count = 0;
    let iter = db.scan(Some(b"scan_03".to_vec()), Some(b"scan_07".to_vec()))?;
    for result in iter {
        let (key, value) = result?;
        println!(
            "Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("Total items found: {} (expected: 4)", count);

    // Test 4: Direct get operations to confirm data exists
    println!("\n=== Test 4: Direct get operations ===");
    for (key, expected_value) in &test_data {
        match db.get(key.as_bytes())? {
            Some(value) => {
                let value_str = String::from_utf8_lossy(&value);
                println!(
                    "Get '{}' => '{}' (match: {})",
                    key,
                    value_str,
                    value_str == *expected_value
                );
            }
            None => {
                println!("Get '{}' => NOT FOUND", key);
            }
        }
    }

    Ok(())
}
