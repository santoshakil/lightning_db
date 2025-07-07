use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

#[test]
fn test_simple_scan() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert a single key
    println!("Inserting key1...");
    db.put(b"key1", b"value1").unwrap();

    // Verify it was inserted
    println!("Getting key1...");
    let value = db.get(b"key1").unwrap();
    assert!(value.is_some());
    println!("Got value: {:?}", String::from_utf8_lossy(&value.unwrap()));

    // Try to scan
    println!("Starting scan...");
    let iter = db.scan(None, None).unwrap();

    let mut count = 0;
    for result in iter {
        match result {
            Ok((key, value)) => {
                println!(
                    "Scanned: {} = {}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value)
                );
                count += 1;
            }
            Err(e) => {
                println!("Error during scan: {}", e);
                panic!("Scan failed: {}", e);
            }
        }
    }

    println!("Total scanned: {}", count);
    assert_eq!(count, 1, "Expected to scan 1 entry");
}
