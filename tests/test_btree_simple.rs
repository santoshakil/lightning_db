use lightning_db::btree::BPlusTree;
use lightning_db::storage::page::PageManager;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_btree_simple_insert_get() {
    let dir = tempdir().unwrap();
    let page_manager = Arc::new(RwLock::new(
        PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap(),
    ));
    let mut btree = BPlusTree::new(page_manager).unwrap();

    // Test simple insert and get
    println!("Testing simple insert and get...");

    btree.insert(b"key1", b"value1").unwrap();
    println!("Inserted key1");

    let result = btree.get(b"key1").unwrap();
    println!("Got result: {:?}", result);
    assert_eq!(result, Some(b"value1".to_vec()));

    println!("Test passed!");
}

#[test]
fn test_btree_create_iterator() {
    use lightning_db::btree::BTreeLeafIterator;

    let dir = tempdir().unwrap();
    let page_manager = Arc::new(RwLock::new(
        PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap(),
    ));
    let mut btree = BPlusTree::new(page_manager).unwrap();

    // Insert one key
    btree.insert(b"key1", b"value1").unwrap();
    println!("Inserted key1");

    // Try to create an iterator
    println!("Creating iterator...");
    let iter = BTreeLeafIterator::new(&btree, None, None, true);
    assert!(iter.is_ok(), "Failed to create iterator: {:?}", iter.err());
    println!("Iterator created successfully");

    // Try to get one item from iterator
    println!("Getting first item from iterator...");
    let mut iter = iter.unwrap();
    let first = iter.next();
    println!("First item: {:?}", first);

    match first {
        Some(Ok((key, value))) => {
            assert_eq!(key, b"key1".to_vec());
            assert_eq!(value, b"value1".to_vec());
        }
        Some(Err(e)) => panic!("Iterator returned error: {:?}", e),
        None => panic!("Iterator returned None when should have item"),
    }

    println!("Test passed!");
}
