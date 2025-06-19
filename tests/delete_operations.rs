use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

#[test]
fn test_direct_delete() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_direct_delete.db");
    let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

    // Insert a key-value pair
    db.put(b"key1", b"value1").unwrap();

    // Verify it exists
    let value = db.get(b"key1").unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));

    // Delete the key
    let deleted = db.delete(b"key1").unwrap();
    assert!(deleted);

    // Verify it's gone
    let value_after = db.get(b"key1").unwrap();
    assert_eq!(value_after, None, "Key should be deleted but still exists");
}

#[test]
fn test_transactional_delete() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_tx_delete.db");
    let config = LightningDbConfig {
        use_optimized_transactions: false,
        compression_enabled: false, // Disable LSM tree
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert a key-value pair
    db.put(b"tx_key", b"tx_value").unwrap();

    // Verify it exists
    let value = db.get(b"tx_key").unwrap();
    println!("Before delete: {:?}", value);
    assert_eq!(value, Some(b"tx_value".to_vec()));

    // Delete within a transaction
    let tx_id = db.begin_transaction().unwrap();
    db.delete_tx(tx_id, b"tx_key").unwrap();

    // Check within transaction (should be None)
    let value_in_tx = db.get_tx(tx_id, b"tx_key").unwrap();
    println!("In transaction after delete: {:?}", value_in_tx);
    assert_eq!(
        value_in_tx, None,
        "Key should be deleted within transaction"
    );

    // Commit the transaction
    db.commit_transaction(tx_id).unwrap();

    // Check after commit (should be None)
    let value_after_commit = db.get(b"tx_key").unwrap();
    println!("After transaction commit: {:?}", value_after_commit);
    assert_eq!(
        value_after_commit, None,
        "Key should be deleted after transaction commit"
    );
}

#[test]
fn test_lsm_delete() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_lsm_delete.db");

    // Enable LSM tree
    let config = LightningDbConfig {
        compression_enabled: true,
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert a key-value pair
    db.put(b"lsm_key", b"lsm_value").unwrap();

    // Verify it exists
    let value = db.get(b"lsm_key").unwrap();
    println!("Before delete: {:?}", value);
    assert_eq!(value, Some(b"lsm_value".to_vec()));

    // Delete the key (should create tombstone in LSM)
    let deleted = db.delete(b"lsm_key").unwrap();
    println!("Delete returned: {}", deleted);
    assert!(deleted);

    // Verify it's gone
    let value_after = db.get(b"lsm_key").unwrap();
    println!("After delete: {:?}", value_after);
    assert_eq!(value_after, None, "Key should be deleted from LSM tree");

    // Force flush to ensure tombstone is persisted
    db.flush_lsm().unwrap();

    // Check again after flush
    let value_after_flush = db.get(b"lsm_key").unwrap();
    println!("After flush: {:?}", value_after_flush);
    assert_eq!(
        value_after_flush, None,
        "Key should still be deleted after LSM flush"
    );
}

#[test]
fn test_btree_delete() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_btree_delete.db");

    // Disable LSM to force B+tree usage
    let config = LightningDbConfig {
        compression_enabled: false,
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert a key-value pair
    db.put(b"btree_key", b"btree_value").unwrap();

    // Verify it exists
    let value = db.get(b"btree_key").unwrap();
    assert_eq!(value, Some(b"btree_value".to_vec()));

    // Delete the key
    let deleted = db.delete(b"btree_key").unwrap();
    assert!(deleted);

    // Verify it's gone
    let value_after = db.get(b"btree_key").unwrap();
    assert_eq!(value_after, None, "Key should be deleted from B+tree");
}

#[test]
fn test_version_store_delete() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_version_delete.db");
    let config = LightningDbConfig {
        use_optimized_transactions: false,
        compression_enabled: false, // Disable LSM to test version store directly
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert multiple versions
    db.put(b"version_key", b"version1").unwrap();
    db.put(b"version_key", b"version2").unwrap();

    // Verify latest version
    let value = db.get(b"version_key").unwrap();
    assert_eq!(value, Some(b"version2".to_vec()));

    // Delete the key
    let deleted = db.delete(b"version_key").unwrap();
    assert!(deleted);

    // Verify it's gone
    let value_after = db.get(b"version_key").unwrap();
    assert_eq!(
        value_after, None,
        "Key should be deleted from version store"
    );
}
