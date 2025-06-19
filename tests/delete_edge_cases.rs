use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

#[test]
fn test_delete_nonexistent_key() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_delete_nonexistent.db");
    let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

    // Try to delete a non-existent key
    let result = db.delete(b"nonexistent_key");
    assert!(result.is_ok());

    // Should return None when getting non-existent key
    let value = db.get(b"nonexistent_key").unwrap();
    assert_eq!(value, None);
}

#[test]
fn test_delete_and_reinsert() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_delete_reinsert.db");
    let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

    // Insert, delete, then reinsert
    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));

    db.delete(b"key1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), None);

    db.put(b"key1", b"value2").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value2".to_vec()));
}

#[test]
fn test_multiple_deletes_same_key() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_multiple_deletes.db");
    let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

    // Insert a key
    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));

    // Delete multiple times
    db.delete(b"key1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), None);

    db.delete(b"key1").unwrap(); // Delete again
    assert_eq!(db.get(b"key1").unwrap(), None);

    db.delete(b"key1").unwrap(); // Delete again
    assert_eq!(db.get(b"key1").unwrap(), None);
}

#[test]
fn test_delete_empty_value() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_delete_empty.db");
    let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

    // Insert a key with empty value
    db.put(b"empty_key", b"").unwrap();
    assert_eq!(db.get(b"empty_key").unwrap(), Some(b"".to_vec()));

    // Delete the key
    db.delete(b"empty_key").unwrap();
    assert_eq!(db.get(b"empty_key").unwrap(), None);
}

#[test]
fn test_transaction_delete_rollback() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_tx_rollback.db");
    let config = LightningDbConfig {
        compression_enabled: false, // Use B+tree to test transaction rollback
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert a key
    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));

    // Start transaction and delete
    let tx_id = db.begin_transaction().unwrap();
    db.delete_tx(tx_id, b"key1").unwrap();

    // Key should be deleted within transaction
    assert_eq!(db.get_tx(tx_id, b"key1").unwrap(), None);

    // But still exist outside transaction
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));

    // Abort transaction
    db.abort_transaction(tx_id).unwrap();

    // Key should still exist
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
}

#[test]
fn test_delete_with_lsm_flush() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_delete_flush.db");
    let config = LightningDbConfig {
        compression_enabled: true,
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert multiple keys
    for i in 0..10 {
        db.put(
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        )
        .unwrap();
    }

    // Delete half of them
    for i in 0..5 {
        db.delete(format!("key{}", i).as_bytes()).unwrap();
    }

    // Flush LSM
    db.flush_lsm().unwrap();

    // Verify deleted keys are gone and remaining keys exist
    for i in 0..5 {
        assert_eq!(db.get(format!("key{}", i).as_bytes()).unwrap(), None);
    }
    for i in 5..10 {
        assert_eq!(
            db.get(format!("key{}", i).as_bytes()).unwrap(),
            Some(format!("value{}", i).as_bytes().to_vec())
        );
    }
}
