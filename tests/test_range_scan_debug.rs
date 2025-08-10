use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

#[test]
fn test_btree_range_scan_debug() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: false,
        cache_size: 0,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    // Insert a few simple keys
    println!("Inserting keys...");
    for i in 0..5 {
        let key = format!("key_{:02}", i);
        let value = format!("value_{}", i);
        println!("  Inserting: {} -> {}", key, value);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify keys can be read directly
    println!("\nDirect reads:");
    for i in 0..5 {
        let key = format!("key_{:02}", i);
        let result = db.get(key.as_bytes()).unwrap();
        println!(
            "  get({}) = {:?}",
            key,
            result.as_ref().map(|v| String::from_utf8_lossy(v))
        );
        assert!(result.is_some(), "Key {} should exist", key);
    }

    println!("\nDirect reads complete. Keys are confirmed to exist.");

    // Test range scan with no bounds
    println!("\nRange scan (no bounds):");
    println!("  About to call db.range()...");
    let all_entries = db.range(None, None).unwrap();
    println!("  db.range() returned, found {} entries", all_entries.len());
    for (key, value) in &all_entries {
        println!(
            "    {} -> {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }
    assert_eq!(all_entries.len(), 5, "Should find all 5 entries");

    // Test range scan with bounds
    println!("\nRange scan (key_01 to key_03):");
    let range_entries = db.range(Some(b"key_01"), Some(b"key_03")).unwrap();
    println!("  Found {} entries", range_entries.len());
    for (key, value) in &range_entries {
        println!(
            "    {} -> {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }
    assert_eq!(range_entries.len(), 2, "Should find key_01 and key_02");

    // Test prefix scan
    println!("\nPrefix scan (key_):");
    let prefix_iter = db.scan_prefix(b"key_").unwrap();
    let prefix_entries: Vec<_> = prefix_iter.collect();
    println!("  Found {} entries", prefix_entries.len());
    for result in &prefix_entries {
        if let Ok((key, value)) = result {
            println!(
                "    {} -> {}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value)
            );
        }
    }
    assert_eq!(
        prefix_entries.len(),
        5,
        "Should find all entries with prefix 'key_'"
    );
}
