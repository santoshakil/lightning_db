use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

#[test]
fn test_comprehensive_delete_functionality() {
    println!("🚀 Testing comprehensive delete functionality across all storage layers...\n");

    // Test 1: LSM Tree deletes with various scenarios
    test_lsm_comprehensive();

    // Test 2: B+Tree deletes
    test_btree_comprehensive();

    // Test 3: Transaction deletes with rollbacks
    test_transaction_comprehensive();

    // Test 4: Mixed operations
    test_mixed_operations();

    println!("✅ All comprehensive delete tests passed!");
}

fn test_lsm_comprehensive() {
    println!("📁 Testing LSM tree delete operations...");

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("lsm_comprehensive.db");
    let config = LightningDbConfig {
        compression_enabled: true,
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert multiple keys
    for i in 0..20 {
        db.put(
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        )
        .unwrap();
    }

    // Verify all keys exist
    for i in 0..20 {
        assert!(db.get(format!("key{}", i).as_bytes()).unwrap().is_some());
    }

    // Delete every other key
    for i in (0..20).step_by(2) {
        db.delete(format!("key{}", i).as_bytes()).unwrap();
    }

    // Verify deleted keys are gone and remaining keys exist
    for i in 0..20 {
        let result = db.get(format!("key{}", i).as_bytes()).unwrap();
        if i % 2 == 0 {
            assert!(result.is_none(), "Key{} should be deleted", i);
        } else {
            assert!(result.is_some(), "Key{} should exist", i);
        }
    }

    // Test empty value handling
    db.put(b"empty_key", b"").unwrap();
    assert_eq!(db.get(b"empty_key").unwrap(), Some(b"".to_vec()));
    db.delete(b"empty_key").unwrap();
    assert_eq!(db.get(b"empty_key").unwrap(), None);

    // Test delete and reinsert
    db.put(b"reinsert_key", b"original").unwrap();
    db.delete(b"reinsert_key").unwrap();
    db.put(b"reinsert_key", b"new_value").unwrap();
    assert_eq!(
        db.get(b"reinsert_key").unwrap(),
        Some(b"new_value".to_vec())
    );

    // Force flush and verify persistence
    db.flush_lsm().unwrap();
    for i in 0..20 {
        let result = db.get(format!("key{}", i).as_bytes()).unwrap();
        if i % 2 == 0 {
            assert!(
                result.is_none(),
                "Key{} should still be deleted after flush",
                i
            );
        } else {
            assert!(result.is_some(), "Key{} should still exist after flush", i);
        }
    }

    println!("  ✓ LSM tree deletes working correctly");
}

fn test_btree_comprehensive() {
    println!("🌳 Testing B+tree delete operations...");

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("btree_comprehensive.db");
    let config = LightningDbConfig {
        compression_enabled: false, // Force B+tree usage
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert and delete operations similar to LSM test
    for i in 0..10 {
        db.put(
            format!("btree_key{}", i).as_bytes(),
            format!("btree_value{}", i).as_bytes(),
        )
        .unwrap();
    }

    // Delete half
    for i in 0..5 {
        db.delete(format!("btree_key{}", i).as_bytes()).unwrap();
    }

    // Verify
    for i in 0..10 {
        let result = db.get(format!("btree_key{}", i).as_bytes()).unwrap();
        if i < 5 {
            assert!(result.is_none(), "BTree key{} should be deleted", i);
        } else {
            assert!(result.is_some(), "BTree key{} should exist", i);
        }
    }

    println!("  ✓ B+tree deletes working correctly");
}

fn test_transaction_comprehensive() {
    println!("🔄 Testing transactional delete operations...");

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("tx_comprehensive.db");
    let config = LightningDbConfig {
        compression_enabled: false, // Use version store for clearer transaction testing
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert initial data
    for i in 0..5 {
        db.put(
            format!("tx_key{}", i).as_bytes(),
            format!("tx_value{}", i).as_bytes(),
        )
        .unwrap();
    }

    // Test transaction commit
    let tx1 = db.begin_transaction().unwrap();
    db.delete_tx(tx1, b"tx_key0").unwrap();
    db.delete_tx(tx1, b"tx_key1").unwrap();
    db.commit_transaction(tx1).unwrap();

    // Verify committed deletes
    assert!(db.get(b"tx_key0").unwrap().is_none());
    assert!(db.get(b"tx_key1").unwrap().is_none());
    assert!(db.get(b"tx_key2").unwrap().is_some());

    // Test transaction rollback
    let tx2 = db.begin_transaction().unwrap();
    db.delete_tx(tx2, b"tx_key2").unwrap();
    db.delete_tx(tx2, b"tx_key3").unwrap();

    // Keys should be deleted within transaction
    assert!(db.get_tx(tx2, b"tx_key2").unwrap().is_none());
    assert!(db.get_tx(tx2, b"tx_key3").unwrap().is_none());

    // But still exist outside transaction
    assert!(db.get(b"tx_key2").unwrap().is_some());
    assert!(db.get(b"tx_key3").unwrap().is_some());

    // Abort transaction
    db.abort_transaction(tx2).unwrap();

    // Keys should still exist after abort
    assert!(db.get(b"tx_key2").unwrap().is_some());
    assert!(db.get(b"tx_key3").unwrap().is_some());

    println!("  ✓ Transactional deletes working correctly");
}

fn test_mixed_operations() {
    println!("🔄 Testing mixed operations across storage layers...");

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("mixed_comprehensive.db");
    let config = LightningDbConfig {
        compression_enabled: true, // Enable both LSM and transactions
        ..Default::default()
    };
    let db = Database::create(&db_path, config).unwrap();

    // Insert using direct puts (goes to LSM)
    db.put(b"mixed_key1", b"lsm_value1").unwrap();
    db.put(b"mixed_key2", b"lsm_value2").unwrap();

    // Insert using transactions (goes to version store)
    let tx1 = db.begin_transaction().unwrap();
    db.put_tx(tx1, b"mixed_key3", b"tx_value3").unwrap();
    db.put_tx(tx1, b"mixed_key4", b"tx_value4").unwrap();
    db.commit_transaction(tx1).unwrap();

    // Verify all keys exist
    assert_eq!(db.get(b"mixed_key1").unwrap(), Some(b"lsm_value1".to_vec()));
    assert_eq!(db.get(b"mixed_key2").unwrap(), Some(b"lsm_value2".to_vec()));
    assert_eq!(db.get(b"mixed_key3").unwrap(), Some(b"tx_value3".to_vec()));
    assert_eq!(db.get(b"mixed_key4").unwrap(), Some(b"tx_value4".to_vec()));

    // Delete using different methods
    db.delete(b"mixed_key1").unwrap(); // Direct delete (LSM)

    let tx2 = db.begin_transaction().unwrap();
    db.delete_tx(tx2, b"mixed_key3").unwrap(); // Transactional delete
    db.commit_transaction(tx2).unwrap();

    // Verify mixed deletes
    assert!(db.get(b"mixed_key1").unwrap().is_none());
    assert!(db.get(b"mixed_key2").unwrap().is_some());
    assert!(db.get(b"mixed_key3").unwrap().is_none());
    assert!(db.get(b"mixed_key4").unwrap().is_some());

    println!("  ✓ Mixed operations working correctly");
}
