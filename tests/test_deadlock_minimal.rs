use lightning_db::btree::BPlusTree;
use lightning_db::storage::page::PageManager;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_btree_no_deadlock() {
    println!("Starting test...");
    
    let dir = tempdir().unwrap();
    println!("Created temp dir: {:?}", dir.path());
    
    let page_manager = Arc::new(RwLock::new(
        PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap(),
    ));
    println!("Created page manager");
    
    let mut btree = BPlusTree::new(page_manager).unwrap();
    println!("Created B+Tree");

    println!("About to insert key1...");
    btree.insert(b"key1", b"value1").unwrap();
    println!("Successfully inserted key1");
    
    println!("Test completed successfully!");
}
