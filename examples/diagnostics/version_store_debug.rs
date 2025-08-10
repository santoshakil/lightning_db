use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Version Store Debug");
    println!("=================================\n");

    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.use_optimized_transactions = false;
    let db = Arc::new(Database::create(dir.path(), config)?);

    // Test 1: Regular put (non-transactional)
    println!("Test 1: Regular put");
    db.put(b"test_key", b"value_1")?;
    let val1 = db.get(b"test_key")?;
    println!("After regular put: {:?}", val1.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Test 2: Transactional put
    println!("\nTest 2: Transactional put");
    let tx1 = db.begin_transaction()?;
    db.put_tx(tx1, b"test_key", b"value_2")?;
    db.commit_transaction(tx1)?;
    let val2 = db.get(b"test_key")?;
    println!("After transaction: {:?}", val2.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Test 3: Another regular put
    println!("\nTest 3: Another regular put");
    db.put(b"test_key", b"value_3")?;
    let val3 = db.get(b"test_key")?;
    println!("After second regular put: {:?}", val3.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Test 4: Check if version store is interfering
    println!("\nTest 4: Mixed operations");
    
    // Start with fresh key
    db.put(b"counter", b"0")?;
    println!("Initial counter: 0");
    
    // Transaction 1
    let tx2 = db.begin_transaction()?;
    let curr = db.get_tx(tx2, b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    db.put_tx(tx2, b"counter", (curr + 1).to_string().as_bytes())?;
    db.commit_transaction(tx2)?;
    
    let after_tx = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("After transaction (should be 1): {}", after_tx);
    
    // Regular put
    db.put(b"counter", b"2")?;
    let after_put = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("After regular put (should be 2): {}", after_put);
    
    // Another transaction
    let tx3 = db.begin_transaction()?;
    let curr2 = db.get_tx(tx3, b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("Transaction read (should see 2): {}", curr2);
    db.put_tx(tx3, b"counter", (curr2 + 1).to_string().as_bytes())?;
    db.commit_transaction(tx3)?;
    
    let final_val = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    println!("Final value (should be 3): {}", final_val);
    
    if final_val != 3 {
        println!("\n❌ ERROR: Version store is interfering with regular puts!");
    } else {
        println!("\n✅ SUCCESS: All values correct");
    }
    
    Ok(())
}