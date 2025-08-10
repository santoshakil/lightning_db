use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

/// Simple validation test to verify integration test infrastructure works
#[test]
fn test_basic_database_operations() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    let db = Database::open(dir.path(), config).unwrap();

    // Test basic put/get
    db.put(b"test_key", b"test_value").unwrap();
    let retrieved = db.get(b"test_key").unwrap().unwrap();
    assert_eq!(retrieved, b"test_value");

    // Test transaction
    let tx_id = db.begin_transaction().unwrap();
    db.put_tx(tx_id, b"tx_key", b"tx_value").unwrap();
    db.commit_transaction(tx_id).unwrap();
    
    let tx_retrieved = db.get(b"tx_key").unwrap().unwrap();
    assert_eq!(tx_retrieved, b"tx_value");

    println!("Basic integration test infrastructure validated successfully!");
}