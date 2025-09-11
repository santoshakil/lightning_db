use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_write_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    const NUM_OPS: usize = 10000;
    
    // Test write performance
    let start = Instant::now();
    for i in 0..NUM_OPS {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let write_duration = start.elapsed();
    
    let writes_per_sec = NUM_OPS as f64 / write_duration.as_secs_f64();
    println!("Write performance: {:.0} ops/sec", writes_per_sec);
    
    // Test read performance
    let start = Instant::now();
    for i in 0..NUM_OPS {
        let key = format!("key_{:08}", i);
        let _value = db.get(key.as_bytes()).unwrap();
    }
    let read_duration = start.elapsed();
    
    let reads_per_sec = NUM_OPS as f64 / read_duration.as_secs_f64();
    println!("Read performance: {:.0} ops/sec", reads_per_sec);
    
    // Test range scan performance
    let start = Instant::now();
    let count = db.range(None, None).unwrap().len();
    let scan_duration = start.elapsed();
    
    let items_per_sec = count as f64 / scan_duration.as_secs_f64();
    println!("Range scan performance: {:.0} items/sec", items_per_sec);
    
    // Verify performance thresholds
    assert!(writes_per_sec > 100_000.0, "Write performance too low: {:.0}", writes_per_sec);
    assert!(reads_per_sec > 150_000.0, "Read performance too low: {:.0}", reads_per_sec);
    assert!(items_per_sec > 1_000_000.0, "Scan performance too low: {:.0}", items_per_sec);
}

#[test]
fn test_transaction_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    const NUM_TXS: usize = 1000;
    const OPS_PER_TX: usize = 10;
    
    let start = Instant::now();
    for tx_id in 0..NUM_TXS {
        let tx = db.begin_transaction().unwrap();
        
        for op in 0..OPS_PER_TX {
            let key = format!("tx_{:04}_key_{:02}", tx_id, op);
            let value = format!("value_{:08}", tx_id * OPS_PER_TX + op);
            db.put_tx(tx, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.commit_transaction(tx).unwrap();
    }
    let duration = start.elapsed();
    
    let txs_per_sec = NUM_TXS as f64 / duration.as_secs_f64();
    let ops_per_sec = (NUM_TXS * OPS_PER_TX) as f64 / duration.as_secs_f64();
    
    println!("Transaction performance: {:.0} txs/sec", txs_per_sec);
    println!("Operations in transactions: {:.0} ops/sec", ops_per_sec);
    
    assert!(txs_per_sec > 5_000.0, "Transaction performance too low: {:.0}", txs_per_sec);
    assert!(ops_per_sec > 50_000.0, "Operations performance too low: {:.0}", ops_per_sec);
}