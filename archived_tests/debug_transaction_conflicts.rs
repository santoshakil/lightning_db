use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Conflict Debug");
    println!("========================================\n");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    // Simple test: Two accounts, concurrent transfers
    db.put(b"A", b"100")?;
    db.put(b"B", b"100")?;
    
    println!("Initial: A=100, B=100, Total=200\n");
    
    let commits = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    
    // Run two threads that will definitely conflict
    let mut handles = vec![];
    
    for thread_id in 0..2 {
        let db = db.clone();
        let comm = commits.clone();
        let conf = conflicts.clone();
        
        let handle = thread::spawn(move || {
            for i in 0..50 {
                println!("Thread {} starting transaction {}", thread_id, i);
                
                // Begin transaction
                let tx_id = db.begin_transaction().unwrap();
                
                // Read both values
                let a_val = db.get_tx(tx_id, b"A").unwrap().unwrap();
                let b_val = db.get_tx(tx_id, b"B").unwrap().unwrap();
                
                let a = String::from_utf8_lossy(&a_val).parse::<i32>().unwrap();
                let b = String::from_utf8_lossy(&b_val).parse::<i32>().unwrap();
                
                println!("Thread {} read: A={}, B={}", thread_id, a, b);
                
                // Transfer 1 from A to B
                if a >= 1 {
                    let new_a = (a - 1).to_string();
                    let new_b = (b + 1).to_string();
                    
                    db.put_tx(tx_id, b"A", new_a.as_bytes()).unwrap();
                    db.put_tx(tx_id, b"B", new_b.as_bytes()).unwrap();
                    
                    println!("Thread {} attempting commit: A={}, B={}", 
                            thread_id, new_a, new_b);
                    
                    match db.commit_transaction(tx_id) {
                        Ok(_) => {
                            println!("Thread {} COMMITTED transaction {}", thread_id, i);
                            comm.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            println!("Thread {} CONFLICT in transaction {}: {}", 
                                    thread_id, i, e);
                            conf.fetch_add(1, Ordering::Relaxed);
                            db.abort_transaction(tx_id).ok();
                        }
                    }
                } else {
                    println!("Thread {} insufficient funds, aborting", thread_id);
                    db.abort_transaction(tx_id).ok();
                }
                
                // Small delay to allow interleaving
                thread::sleep(std::time::Duration::from_micros(10));
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Check final state
    let final_a = db.get(b"A")?.unwrap();
    let final_b = db.get(b"B")?.unwrap();
    
    let a = String::from_utf8_lossy(&final_a).parse::<i32>().unwrap();
    let b = String::from_utf8_lossy(&final_b).parse::<i32>().unwrap();
    
    println!("\n=== RESULTS ===");
    println!("Final: A={}, B={}, Total={}", a, b, a + b);
    println!("Commits: {}", commits.load(Ordering::Relaxed));
    println!("Conflicts: {}", conflicts.load(Ordering::Relaxed));
    
    if a + b == 200 {
        println!("✅ ACID properties maintained");
    } else {
        println!("❌ ACID violation: Total is {} (expected 200)", a + b);
        println!("Lost {} units", 200 - (a + b));
    }
    
    // Detailed verification
    let expected_transfers = commits.load(Ordering::Relaxed) as i32;
    let actual_transfers = 100 - a;
    
    println!("\nDetailed Analysis:");
    println!("  Expected transfers: {}", expected_transfers);
    println!("  Actual transfers: {}", actual_transfers);
    
    if expected_transfers != actual_transfers {
        println!("  ⚠️  Mismatch: {} transfers committed but only {} applied", 
                expected_transfers, actual_transfers);
    }
    
    Ok(())
}