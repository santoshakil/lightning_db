use lightning_db::{Database, WriteBatch};

fn main() {
    println!("Testing batch operations...");

    // Create a temporary database
    let db = Database::create_temp().expect("Failed to create temp database");

    // Test 1: Simple batch put
    println!("Test 1: Simple batch put");
    let mut batch = WriteBatch::new();
    batch.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
    batch.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
    batch.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();

    db.write_batch(&batch).expect("Failed to write batch");

    // Verify
    let v1 = db.get(b"key1").expect("Failed to get key1");
    let v2 = db.get(b"key2").expect("Failed to get key2");
    let v3 = db.get(b"key3").expect("Failed to get key3");

    println!("key1 = {:?}", v1.as_ref().map(|v| String::from_utf8_lossy(&v).to_string()));
    println!("key2 = {:?}", v2.as_ref().map(|v| String::from_utf8_lossy(&v).to_string()));
    println!("key3 = {:?}", v3.as_ref().map(|v| String::from_utf8_lossy(&v).to_string()));

    assert_eq!(v1, Some(b"value1".to_vec()), "key1 value mismatch");
    assert_eq!(v2, Some(b"value2".to_vec()), "key2 value mismatch");
    assert_eq!(v3, Some(b"value3".to_vec()), "key3 value mismatch");

    // Test 2: Batch delete
    println!("\nTest 2: Batch delete");
    let mut batch = WriteBatch::new();
    batch.delete(b"key2".to_vec()).unwrap();

    db.write_batch(&batch).expect("Failed to write delete batch");

    let v2_after = db.get(b"key2").expect("Failed to get key2 after delete");
    println!("key2 after delete = {:?}", v2_after);
    assert_eq!(v2_after, None, "key2 should be deleted");

    // Test 3: Large batch
    println!("\nTest 3: Large batch (500 keys)");
    let mut batch = WriteBatch::new();
    for i in 0..500 {
        let key = format!("batch_key_{:04}", i);
        let value = format!("batch_value_{}", i);
        batch.put(key.into_bytes(), value.into_bytes()).unwrap();
    }

    db.write_batch(&batch).expect("Failed to write large batch");

    // Spot check
    let check_keys = [0, 100, 250, 499];
    for i in check_keys {
        let key = format!("batch_key_{:04}", i);
        let expected_value = format!("batch_value_{}", i);
        let value = db.get(key.as_bytes()).expect(&format!("Failed to get {}", key));
        assert_eq!(value, Some(expected_value.into_bytes()), "Large batch key {} mismatch", i);
        println!("Verified key batch_key_{:04}", i);
    }

    println!("\nAll batch tests passed!");
}