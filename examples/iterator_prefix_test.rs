use lightning_db::{Database, LightningDbConfig};
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    
    // Create a minimal config for testing
    let mut config = LightningDbConfig::default();
    config.compression_enabled = false; // Disable compression to disable LSM tree
    config.cache_size = 0; // Disable cache
    config.use_optimized_transactions = false;
    
    let db = Database::create(db_path, config)?;

    // Insert test data with different prefixes
    println!("=== Inserting test data ===");
    
    // Insert data with various prefixes
    let test_data = vec![
        ("prefix_a_001", "value_a_001"),
        ("prefix_a_002", "value_a_002"),
        ("prefix_a_003", "value_a_003"),
        ("prefix_b_001", "value_b_001"),
        ("prefix_b_002", "value_b_002"),
        ("prefix_c_001", "value_c_001"),
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
        ("scan_11", "value_11"),
        ("test", "value_test"),
        ("test2", "value_test2"),
        ("user:1", "user_data_1"),
        ("user:2", "user_data_2"),
        ("user:10", "user_data_10"),
        ("user:11", "user_data_11"),
    ];
    
    for (key, value) in &test_data {
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("Inserted: {} => {}", key, value);
    }
    
    // Force sync to ensure data is written
    db.sync()?;

    // Test 1: Prefix scan for "prefix_a"
    println!("\n=== Test 1: Prefix scan for 'prefix_a' ===");
    let mut count = 0;
    let iter = db.scan_prefix(b"prefix_a")?;
    for result in iter {
        let (key, value) = result?;
        println!("Found: {} => {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Total items found: {} (expected: 3)", count);
    
    // Test 2: Prefix scan for "prefix_b"
    println!("\n=== Test 2: Prefix scan for 'prefix_b' ===");
    count = 0;
    let iter = db.scan_prefix(b"prefix_b")?;
    for result in iter {
        let (key, value) = result?;
        println!("Found: {} => {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Total items found: {} (expected: 2)", count);
    
    // Test 3: Prefix scan for "scan_0"
    println!("\n=== Test 3: Prefix scan for 'scan_0' ===");
    count = 0;
    let iter = db.scan_prefix(b"scan_0")?;
    for result in iter {
        let (key, value) = result?;
        println!("Found: {} => {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Total items found: {} (expected: 9)", count);
    
    // Test 4: Prefix scan for "scan_1"
    println!("\n=== Test 4: Prefix scan for 'scan_1' ===");
    count = 0;
    let iter = db.scan_prefix(b"scan_1")?;
    for result in iter {
        let (key, value) = result?;
        println!("Found: {} => {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Total items found: {} (expected: 2)", count);
    
    // Test 5: Prefix scan for "test"
    println!("\n=== Test 5: Prefix scan for 'test' ===");
    count = 0;
    let iter = db.scan_prefix(b"test")?;
    for result in iter {
        let (key, value) = result?;
        println!("Found: {} => {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Total items found: {} (expected: 2)", count);
    
    // Test 6: Prefix scan for "user:1"
    println!("\n=== Test 6: Prefix scan for 'user:1' ===");
    count = 0;
    let iter = db.scan_prefix(b"user:1")?;
    for result in iter {
        let (key, value) = result?;
        println!("Found: {} => {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Total items found: {} (expected: 3)", count);
    
    // Test 7: Debug scan_prefix with empty result
    println!("\n=== Test 7: Prefix scan for 'xyz' (should be empty) ===");
    count = 0;
    let iter = db.scan_prefix(b"xyz")?;
    for result in iter {
        let (key, value) = result?;
        println!("Found: {} => {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Total items found: {} (expected: 0)", count);

    Ok(())
}