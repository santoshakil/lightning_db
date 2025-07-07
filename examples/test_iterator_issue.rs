use lightning_db::{Database, DatabaseConfig};
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    let config = DatabaseConfig::default();
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

    // Test 4: Debug BTreeLeafIterator directly
    println!("\nTest 4: Debug BTreeLeafIterator directly");
    use lightning_db::btree::{BPlusTree, BTreeLeafIterator};
    use parking_lot::RwLock;
    use std::sync::Arc;

    let btree = db.get_btree();
    let btree_guard = btree.read();

    let iter = BTreeLeafIterator::new(
        &btree_guard,
        Some(b"test_0".to_vec()),
        Some(b"test_1".to_vec()),
        true,
    )?;

    count = 0;
    for result in iter {
        match result {
            Ok((key, value)) => {
                println!(
                    "  Direct iterator found: {} => {}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value)
                );
                count += 1;
            }
            Err(e) => {
                println!("  Direct iterator error: {}", e);
                break;
            }
        }
    }
    println!("  Direct iterator total: {}", count);

    // Check if data is actually in the B+Tree
    println!("\nTest 5: Direct B+Tree lookups");
    for i in 0..10 {
        let key = format!("test_{:02}", i);
        match btree_guard.get(key.as_bytes()) {
            Ok(Some(value)) => {
                println!(
                    "  Direct get '{}' => '{}'",
                    key,
                    String::from_utf8_lossy(&value)
                );
            }
            Ok(None) => {
                println!("  Direct get '{}' => NOT FOUND", key);
            }
            Err(e) => {
                println!("  Direct get '{}' => ERROR: {}", key, e);
            }
        }
    }

    Ok(())
}
