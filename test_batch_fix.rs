use lightning_db::{Database, WriteBatch};

fn main() {
    println!("Testing batch operations fix...");

    // Create a temporary database
    let db = Database::create_temp().expect("Failed to create temp database");

    // Test 1: Combined batch with puts and deletes
    println!("Test 1: Combined batch operations");
    let mut batch = WriteBatch::new();

    // Add some keys
    batch.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
    batch.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
    batch.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();

    // Delete one in the same batch
    batch.delete(b"key2".to_vec()).unwrap();

    // Apply batch
    db.write_batch(&batch).expect("Failed to write combined batch");

    // Verify results
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), None); // Should be deleted
    assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));

    println!("Combined batch test passed!");

    // Test 2: Separate batches causing conflict
    println!("\nTest 2: Separate batch operations");

    // First batch - add keys
    let mut batch1 = WriteBatch::new();
    batch1.put(b"test1".to_vec(), b"value1".to_vec()).unwrap();
    batch1.put(b"test2".to_vec(), b"value2".to_vec()).unwrap();
    db.write_batch(&batch1).expect("Failed to write first batch");

    // Second batch - try to delete (this was failing)
    let mut batch2 = WriteBatch::new();
    batch2.delete(b"test2".to_vec()).unwrap();

    // This should work now
    match db.write_batch(&batch2) {
        Ok(_) => println!("Second batch succeeded!"),
        Err(e) => {
            println!("Second batch failed with: {:?}", e);
            println!("This is the bug - separate batches cause conflicts");
        }
    }

    println!("\nTest complete!");
}