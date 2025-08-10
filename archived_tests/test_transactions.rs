use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Test");
    println!("==============================\n");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    // Initialize two accounts with 1000 each
    db.put(b"account_a", b"1000")?;
    db.put(b"account_b", b"1000")?;
    
    println!("Initial state:");
    println!("  Account A: 1000");
    println!("  Account B: 1000");
    println!("  Total: 2000\n");
    
    let successful_transfers = Arc::new(AtomicU64::new(0));
    let failed_transfers = Arc::new(AtomicU64::new(0));
    
    // Run 4 threads doing concurrent transfers
    let mut handles = vec![];
    for thread_id in 0..4 {
        let db = db.clone();
        let success = successful_transfers.clone();
        let failed = failed_transfers.clone();
        
        let handle = thread::spawn(move || {
            for i in 0..25 {
                // Start transaction
                let tx_id = match db.begin_transaction() {
                    Ok(id) => id,
                    Err(e) => {
                        eprintln!("Thread {} failed to begin transaction: {}", thread_id, e);
                        failed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                // Read current balances WITHIN THE TRANSACTION
                let a_val = match db.get_tx(tx_id, b"account_a") {
                    Ok(Some(v)) => v,
                    Ok(None) => b"0".to_vec(),
                    Err(e) => {
                        eprintln!("Thread {} failed to read account_a: {}", thread_id, e);
                        db.abort_transaction(tx_id).ok();
                        failed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                let b_val = match db.get_tx(tx_id, b"account_b") {
                    Ok(Some(v)) => v,
                    Ok(None) => b"0".to_vec(),
                    Err(e) => {
                        eprintln!("Thread {} failed to read account_b: {}", thread_id, e);
                        db.abort_transaction(tx_id).ok();
                        failed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                let a_balance = String::from_utf8_lossy(&a_val).parse::<i32>().unwrap_or(0);
                let b_balance = String::from_utf8_lossy(&b_val).parse::<i32>().unwrap_or(0);
                
                // Try to transfer 10 from A to B
                if a_balance >= 10 {
                    let new_a = (a_balance - 10).to_string();
                    let new_b = (b_balance + 10).to_string();
                    
                    // Use TRANSACTIONAL puts to ensure isolation
                    if let Err(e) = db.put_tx(tx_id, b"account_a", new_a.as_bytes()) {
                        eprintln!("Thread {} failed to write account_a: {}", thread_id, e);
                        db.abort_transaction(tx_id).ok();
                        failed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    
                    if let Err(e) = db.put_tx(tx_id, b"account_b", new_b.as_bytes()) {
                        eprintln!("Thread {} failed to write account_b: {}", thread_id, e);
                        db.abort_transaction(tx_id).ok();
                        failed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    
                    // Commit the transaction
                    match db.commit_transaction(tx_id) {
                        Ok(_) => {
                            success.fetch_add(1, Ordering::Relaxed);
                            if i % 10 == 0 {
                                println!("Thread {} completed transfer {}", thread_id, i);
                            }
                        }
                        Err(e) => {
                            eprintln!("Thread {} failed to commit: {}", thread_id, e);
                            failed.fetch_add(1, Ordering::Relaxed);
                            db.abort_transaction(tx_id).ok();
                        }
                    }
                } else {
                    // Not enough balance, abort
                    db.abort_transaction(tx_id).ok();
                }
                
                // Small delay to reduce contention
                thread::sleep(std::time::Duration::from_micros(100));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Check final state
    println!("\nFinal state:");
    
    let final_a = db.get(b"account_a")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);
    
    let final_b = db.get(b"account_b")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);
    
    let successful = successful_transfers.load(Ordering::Relaxed);
    let failed = failed_transfers.load(Ordering::Relaxed);
    
    println!("  Account A: {}", final_a);
    println!("  Account B: {}", final_b);
    println!("  Total: {}", final_a + final_b);
    println!("\nTransfer statistics:");
    println!("  Successful: {}", successful);
    println!("  Failed: {}", failed);
    println!("  Expected transfers: {}", successful * 10);
    println!("  Actual transfers: {}", 1000 - final_a);
    
    // Verify ACID properties
    println!("\n=== ACID VERIFICATION ===");
    
    let total = final_a + final_b;
    if total == 2000 {
        println!("✅ Atomicity: PASSED - Total preserved (2000)");
    } else {
        println!("❌ Atomicity: FAILED - Total is {} (expected 2000)", total);
    }
    
    if final_a >= 0 && final_b >= 0 {
        println!("✅ Consistency: PASSED - No negative balances");
    } else {
        println!("❌ Consistency: FAILED - Negative balance detected");
    }
    
    if (successful as i32) * 10 == (1000 - final_a) {
        println!("✅ Isolation: PASSED - Transfer count matches");
    } else {
        println!("⚠️  Isolation: WARNING - Transfer count mismatch");
        println!("   This may indicate transaction isolation issues");
    }
    
    // Test durability by reopening
    drop(db);
    let db2 = Database::open(dir.path(), LightningDbConfig::default())?;
    
    let durable_a = db2.get(b"account_a")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);
    
    if durable_a == final_a {
        println!("✅ Durability: PASSED - Data persisted correctly");
    } else {
        println!("❌ Durability: FAILED - Data changed after reopen");
    }
    
    Ok(())
}