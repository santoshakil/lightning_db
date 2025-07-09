use lightning_db::{Database, LightningDbConfig};
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Create a minimal config for testing
    let config = LightningDbConfig {
        compression_enabled: false, // Disable compression to disable LSM tree
        cache_size: 0, // Disable cache
        use_optimized_transactions: false,
        ..Default::default()
    };

    let db = Database::create(db_path, config)?;

    // Insert test data
    println!("=== Inserting test data ===");
    for i in 0..10 {
        let key = format!("test_{:02}", i);
        let value = format!("value_{:02}", i);
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
    println!("\n=== Test 2: Prefix scan for 'test_0' ===");
    count = 0;
    let iter = db.scan_prefix(b"test_0")?;
    for result in iter {
        let (key, value) = result?;
        println!(
            "Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("Total items found: {} (expected: 1-2)", count);

    // Test 3: Range scan
    println!("\n=== Test 3: Range scan from test_03 to test_07 ===");
    count = 0;
    let iter = db.scan(Some(b"test_03".to_vec()), Some(b"test_07".to_vec()))?;
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
    for i in 0..10 {
        let key = format!("test_{:02}", i);
        match db.get(key.as_bytes())? {
            Some(value) => {
                println!("Get '{}' => '{}'", key, String::from_utf8_lossy(&value));
            }
            None => {
                println!("Get '{}' => NOT FOUND", key);
            }
        }
    }

    // Test 5: Debug scan_prefix calculation
    println!("\n=== Test 5: Debug scan_prefix calculation ===");
    let prefix = b"test_0";
    let mut end_key = prefix.to_vec();

    println!("Prefix: {:?}", String::from_utf8_lossy(prefix));

    if let Some(last_byte) = end_key.last_mut() {
        if *last_byte < u8::MAX {
            *last_byte += 1;
            println!(
                "End key: {:?} (incremented last byte)",
                String::from_utf8_lossy(&end_key)
            );
        } else {
            end_key.push(0);
            println!(
                "End key: {:?} (appended 0)",
                String::from_utf8_lossy(&end_key)
            );
        }
    }

    // Test manual range with calculated end key
    println!("\n=== Test 6: Manual range scan with calculated end key ===");
    count = 0;
    let iter = db.scan(Some(prefix.to_vec()), Some(end_key))?;
    for result in iter {
        let (key, value) = result?;
        println!(
            "Found: {} => {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
        count += 1;
    }
    println!("Total items found: {}", count);

    Ok(())
}
