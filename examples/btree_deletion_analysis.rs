use lightning_db::btree::BPlusTree;
use lightning_db::storage::PageManager;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== B+Tree Deletion Analysis ===");

    // Test 1: Basic deletion functionality
    println!("\n1. Testing basic deletion...");
    test_basic_deletion()?;

    // Test 2: Edge cases with minimum node sizes
    println!("\n2. Testing edge cases...");
    test_edge_cases()?;

    // Test 3: Test with large dataset for node splits and merges
    println!("\n3. Testing large dataset operations...");
    test_large_dataset()?;

    // Test 4: Test root node adjustments
    println!("\n4. Testing root adjustments...");
    test_root_adjustments()?;

    println!("\n=== Analysis Complete ===");
    Ok(())
}

fn test_basic_deletion() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let page_manager = Arc::new(RwLock::new(PageManager::create(
        &dir.path().join("test.db"),
        1024 * 1024,
    )?));
    let mut btree = BPlusTree::new(page_manager)?;

    // Insert some data
    for i in 0..10 {
        let key = format!("key{:03}", i);
        let value = format!("value{:03}", i);
        btree.insert(key.as_bytes(), value.as_bytes())?;
    }

    // Delete a key
    let deleted = btree.delete(b"key005")?;
    println!("   Deleted key005: {}", deleted);

    // Verify it's gone
    let result = btree.get(b"key005")?;
    println!("   key005 exists after deletion: {}", result.is_some());

    // Verify others still exist
    let result = btree.get(b"key004")?;
    println!("   key004 exists: {}", result.is_some());

    Ok(())
}

fn test_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let page_manager = Arc::new(RwLock::new(PageManager::create(
        &dir.path().join("test.db"),
        1024 * 1024,
    )?));
    let mut btree = BPlusTree::new(page_manager)?;

    // Test deletion from empty tree
    let deleted = btree.delete(b"nonexistent")?;
    println!("   Deleted from empty tree: {}", deleted);

    // Test deletion of non-existent key
    btree.insert(b"key1", b"value1")?;
    let deleted = btree.delete(b"key2")?;
    println!("   Deleted non-existent key: {}", deleted);

    // Test deletion of last key
    let deleted = btree.delete(b"key1")?;
    println!("   Deleted last key: {}", deleted);

    // Verify tree is empty
    let result = btree.get(b"key1")?;
    println!("   Tree empty after last deletion: {}", result.is_none());

    Ok(())
}

fn test_large_dataset() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let page_manager = Arc::new(RwLock::new(PageManager::create(
        &dir.path().join("test.db"),
        1024 * 1024,
    )?));
    let mut btree = BPlusTree::new(page_manager)?;

    // Insert enough data to create multi-level tree
    println!("   Inserting {} keys...", 500);
    for i in 0..500 {
        let key = format!("key{:05}", i);
        let value = format!("value{:05}", i);
        btree.insert(key.as_bytes(), value.as_bytes())?;
    }

    println!("   Tree height: {}", btree.height());

    // Delete many keys to trigger merging
    println!("   Deleting {} keys...", 400);
    let mut deleted_count = 0;
    for i in 0..400 {
        let key = format!("key{:05}", i);
        if btree.delete(key.as_bytes())? {
            deleted_count += 1;
        }
    }

    println!("   Successfully deleted: {}", deleted_count);
    println!("   Tree height after deletion: {}", btree.height());

    // Verify remaining keys
    let mut remaining = 0;
    for i in 400..500 {
        let key = format!("key{:05}", i);
        if btree.get(key.as_bytes())?.is_some() {
            remaining += 1;
        }
    }
    println!("   Remaining keys: {}", remaining);

    Ok(())
}

fn test_root_adjustments() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let page_manager = Arc::new(RwLock::new(PageManager::create(
        &dir.path().join("test.db"),
        1024 * 1024,
    )?));
    let mut btree = BPlusTree::new(page_manager)?;

    // Create a tree with multiple levels
    for i in 0..200 {
        let key = format!("key{:04}", i);
        let value = format!("value{:04}", i);
        btree.insert(key.as_bytes(), value.as_bytes())?;
    }

    let initial_height = btree.height();
    println!("   Initial height: {}", initial_height);

    // Delete most keys to potentially trigger root adjustments
    for i in 0..190 {
        let key = format!("key{:04}", i);
        btree.delete(key.as_bytes())?;
    }

    let final_height = btree.height();
    println!("   Final height: {}", final_height);
    println!("   Height reduced: {}", initial_height > final_height);

    // Verify tree still works
    let result = btree.get(b"key0195")?;
    println!("   Sample key still accessible: {}", result.is_some());

    Ok(())
}
