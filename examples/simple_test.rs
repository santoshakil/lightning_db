use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Simple Lightning DB Test\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    
    // Test both with and without LSM tree
    println!("=== Test 1: Without LSM tree (compression_enabled: false) ===");
    let config_no_lsm = LightningDbConfig {
        compression_enabled: false,
        use_optimized_transactions: false,
        use_optimized_page_manager: false,
        ..Default::default()
    };
    test_database(&db_path.join("no_lsm"), config_no_lsm)?;
    
    println!("\n=== Test 2: With LSM tree (compression_enabled: true) ===");
    let config_with_lsm = LightningDbConfig {
        compression_enabled: true,
        use_optimized_transactions: false,
        use_optimized_page_manager: false,
        ..Default::default()
    };
    test_database(&db_path.join("with_lsm"), config_with_lsm)?;
    
    println!("\n=== Test 3: Default config (optimized transactions) ===");
    let config_default = LightningDbConfig::default();
    test_database(&db_path.join("default"), config_default)?;
    
    Ok(())
}

fn test_database(path: &std::path::Path, config: LightningDbConfig) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create(path, config)?;
    
    println!("1. Testing regular operations:");
    
    // Test regular put/get
    db.put(b"key1", b"value1")?;
    let result = db.get(b"key1")?;
    println!("  Put/Get: {:?}", result);
    assert_eq!(result, Some(b"value1".to_vec()));
    
    // Test regular delete
    db.delete(b"key1")?;
    let result = db.get(b"key1")?;
    println!("  After delete: {:?}", result);
    assert_eq!(result, None);
    
    println!("\n2. Testing transactions:");
    
    // Test transaction put
    let tx_id = db.begin_transaction()?;
    println!("  Started transaction: {}", tx_id);
    
    db.put_tx(tx_id, b"tx_key", b"tx_value")?;
    println!("  Put data in transaction");
    
    // Check visibility before commit
    let before_commit = db.get(b"tx_key")?;
    println!("  Before commit: {:?}", before_commit);
    
    db.commit_transaction(tx_id)?;
    println!("  Committed transaction");
    
    // Check visibility after commit
    let after_commit = db.get(b"tx_key")?;
    println!("  After commit: {:?}", after_commit);
    
    if after_commit.is_none() {
        println!("\n  ⚠️  Issue: Transactional data not visible after commit!");
        
        // Try direct access to version store (for debugging)
        println!("\n  Debugging: Checking stats...");
        let stats = db.stats();
        println!("  Stats: {:?}", stats);
    } else {
        println!("  ✅ Transactions work correctly!");
    }
    
    Ok(())
}