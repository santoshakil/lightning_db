use lightning_db::btree::BPlusTree;
use lightning_db::storage::page::PageManager;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_btree_iterator_direct() {
    let dir = tempdir().unwrap();
    let page_manager = Arc::new(RwLock::new(
        PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap(),
    ));
    let mut btree = BPlusTree::new(page_manager).unwrap();

    // Insert a few keys
    println!("Inserting keys into B+Tree directly...");
    for i in 0..5 {
        let key = format!("key_{:02}", i);
        let value = format!("value_{}", i);
        println!("  Inserting: {} -> {}", key, value);
        btree.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify keys can be read
    println!("\nVerifying keys with get():");
    for i in 0..5 {
        let key = format!("key_{:02}", i);
        let result = btree.get(key.as_bytes()).unwrap();
        println!("  get({}) = {:?}", key, result.as_ref().map(|v| String::from_utf8_lossy(v)));
        assert!(result.is_some(), "Key {} should exist", key);
    }

    // Test range scan
    println!("\nTesting range() method:");
    println!("  About to call btree.range(None, None)...");
    let all_entries = btree.range(None, None).unwrap();
    println!("  range() returned {} entries", all_entries.len());
    
    for (key, value) in &all_entries {
        println!("    {} -> {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }
    
    assert_eq!(all_entries.len(), 5, "Should find all 5 entries");

    // Test range with bounds
    println!("\nTesting range with bounds:");
    let range_entries = btree.range(Some(b"key_01"), Some(b"key_03")).unwrap();
    println!("  Found {} entries", range_entries.len());
    assert_eq!(range_entries.len(), 2, "Should find key_01 and key_02");
}
