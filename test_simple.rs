use lightning_db::Database;

fn main() {
    println!("Testing simple operations...");

    // Create a temporary database
    let db = Database::create_temp().expect("Failed to create temp database");

    // Test 1: Simple put and get
    println!("Test 1: Simple put and get");
    db.put(b"key1", b"value1").expect("Failed to put key1");
    let v1 = db.get(b"key1").expect("Failed to get key1");
    println!("key1 = {:?}", v1.as_ref().map(|v| String::from_utf8_lossy(&v).to_string()));
    assert_eq!(v1, Some(b"value1".to_vec()));

    // Test 2: Delete
    println!("\nTest 2: Delete");
    db.delete(b"key1").expect("Failed to delete key1");
    let v1_after = db.get(b"key1").expect("Failed to get key1 after delete");
    println!("key1 after delete = {:?}", v1_after);
    assert_eq!(v1_after, None);

    // Test 3: Multiple puts using put_batch
    println!("\nTest 3: Batch puts");
    let batch_data = vec![
        (b"batch1".to_vec(), b"value1".to_vec()),
        (b"batch2".to_vec(), b"value2".to_vec()),
        (b"batch3".to_vec(), b"value3".to_vec()),
    ];
    db.put_batch(&batch_data).expect("Failed to put_batch");

    // Verify batch puts
    for (key, expected_value) in &batch_data {
        let value = db.get(key).expect(&format!("Failed to get {:?}", key));
        assert_eq!(value, Some(expected_value.clone()));
        println!("Verified {:?}", String::from_utf8_lossy(key));
    }

    // Test 4: Delete one from batch
    println!("\nTest 4: Delete from batch");
    db.delete(b"batch2").expect("Failed to delete batch2");
    let v2 = db.get(b"batch2").expect("Failed to get batch2");
    assert_eq!(v2, None);
    println!("batch2 deleted successfully");

    // Other keys should still exist
    assert!(db.get(b"batch1").unwrap().is_some());
    assert!(db.get(b"batch3").unwrap().is_some());

    println!("\nAll simple tests passed!");
}