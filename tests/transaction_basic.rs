use lightning_db::transaction::{TransactionManager, VersionStore};
use std::sync::Arc;

#[test]
fn test_simple_sequential_transactions() {
    let version_store = Arc::new(VersionStore::new());
    let tx_mgr = TransactionManager::new(100, version_store.clone());

    // First transaction
    let tx1_id = tx_mgr.begin().unwrap();
    let tx1_arc = tx_mgr.get_transaction(tx1_id).unwrap();
    tx1_arc
        .write()
        .add_write(b"key1".to_vec(), Some(b"value1".to_vec()), 0);

    // Commit first transaction
    let result1 = tx_mgr.commit(tx1_id);
    println!("First commit result: {:?}", result1);
    assert!(result1.is_ok());

    // Second transaction on different key
    let tx2_id = tx_mgr.begin().unwrap();
    let tx2_arc = tx_mgr.get_transaction(tx2_id).unwrap();
    tx2_arc
        .write()
        .add_write(b"key2".to_vec(), Some(b"value2".to_vec()), 0);

    // Commit second transaction
    let result2 = tx_mgr.commit(tx2_id);
    println!("Second commit result: {:?}", result2);
    assert!(result2.is_ok());
}

#[test]
fn test_simple_conflicting_transactions() {
    let version_store = Arc::new(VersionStore::new());
    let tx_mgr = TransactionManager::new(100, version_store.clone());

    // Start both transactions
    let tx1_id = tx_mgr.begin().unwrap();
    let tx2_id = tx_mgr.begin().unwrap();

    // Both write to same key
    let tx1_arc = tx_mgr.get_transaction(tx1_id).unwrap();
    tx1_arc
        .write()
        .add_write(b"key1".to_vec(), Some(b"value1".to_vec()), 0);

    let tx2_arc = tx_mgr.get_transaction(tx2_id).unwrap();
    tx2_arc
        .write()
        .add_write(b"key1".to_vec(), Some(b"value2".to_vec()), 0);

    // First commit should succeed
    let result1 = tx_mgr.commit(tx1_id);
    println!("First commit result: {:?}", result1);
    assert!(result1.is_ok());

    // Second commit should fail
    let result2 = tx_mgr.commit(tx2_id);
    println!("Second commit result: {:?}", result2);
    assert!(result2.is_err());
}
