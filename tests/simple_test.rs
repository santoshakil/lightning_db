use lightning_db::{Database, LightningDbConfig};

#[test]
fn test_basic_operations() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Basic put and get
    db.put(b"key1", b"value1").unwrap();
    let val = db.get(b"key1").unwrap().unwrap();
    assert_eq!(val, b"value1");

    // Update
    db.put(b"key1", b"updated").unwrap();
    let val = db.get(b"key1").unwrap().unwrap();
    assert_eq!(val, b"updated");

    // Delete
    db.delete(b"key1").unwrap();
    assert!(db.get(b"key1").unwrap().is_none());

    println!("Basic operations test passed!");
}