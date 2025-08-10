use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Debug - Detailed");
    println!("==========================================\n");

    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.use_optimized_transactions = true;
    let db = Arc::new(Database::create(dir.path(), config)?);

    // Test: Simple write and read
    println!("Test: Simple Transaction Write and Read");
    println!("----------------------------------------");
    
    // Initialize value
    db.put(b"test_key", b"0")?;
    println!("Initial value: {:?}", db.get(b"test_key")?);
    
    // Transaction write
    let tx_id = db.begin_transaction()?;
    println!("Transaction {} started", tx_id);
    
    // Read in transaction
    let tx_value = db.get_tx(tx_id, b"test_key")?
        .and_then(|v| String::from_utf8(v).ok())
        .unwrap_or("NONE".to_string());
    println!("Transaction read: {}", tx_value);
    
    // Write new value
    db.put_tx(tx_id, b"test_key", b"100")?;
    println!("Transaction wrote: 100");
    
    // Commit
    match db.commit_transaction(tx_id) {
        Ok(_) => println!("Transaction {} committed successfully", tx_id),
        Err(e) => {
            println!("Transaction {} failed: {}", tx_id, e);
            db.abort_transaction(tx_id)?;
        }
    }
    
    // Read immediately after commit
    let value1 = db.get(b"test_key")?
        .and_then(|v| String::from_utf8(v).ok())
        .unwrap_or("NONE".to_string());
    println!("Read immediately after commit: {}", value1);
    
    // Try reading multiple times
    for i in 0..5 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let value = db.get(b"test_key")?
            .and_then(|v| String::from_utf8(v).ok())
            .unwrap_or("NONE".to_string());
        println!("Read after {}ms: {}", (i+1)*10, value);
    }
    
    // Test multiple sequential transactions
    println!("\nTest: Multiple Sequential Transactions");
    println!("---------------------------------------");
    
    for i in 1..=5 {
        let tx_id = db.begin_transaction()?;
        
        let current = db.get_tx(tx_id, b"test_key")?
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        
        let new_value = current + 10;
        db.put_tx(tx_id, b"test_key", new_value.to_string().as_bytes())?;
        
        match db.commit_transaction(tx_id) {
            Ok(_) => {
                println!("Transaction {}: {} -> {} (committed)", i, current, new_value);
            }
            Err(e) => {
                println!("Transaction {} failed: {}", i, e);
                db.abort_transaction(tx_id)?;
            }
        }
        
        // Read after each commit
        let read_value = db.get(b"test_key")?
            .and_then(|v| String::from_utf8(v).ok())
            .unwrap_or("NONE".to_string());
        println!("  Regular read sees: {}", read_value);
    }
    
    // Final check
    println!("\nFinal State:");
    let final_value = db.get(b"test_key")?
        .and_then(|v| String::from_utf8(v).ok())
        .unwrap_or("NONE".to_string());
    println!("Final value: {} (expected: 150)", final_value);
    
    if final_value == "150" {
        println!("✅ PASSED: All transactions persisted correctly");
    } else {
        println!("❌ FAILED: Expected 150, got {}", final_value);
    }
    
    Ok(())
}