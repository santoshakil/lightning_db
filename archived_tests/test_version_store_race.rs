use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Version Store Race Condition Test");
    println!("===============================================\n");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    // Initialize counter
    db.put(b"counter", b"0")?;
    
    let race_detected = Arc::new(AtomicBool::new(false));
    let iterations = Arc::new(AtomicU64::new(0));
    
    // Thread 1: Continuously reads with regular get() and writes with transactions
    let db1 = db.clone();
    let race1 = race_detected.clone();
    let iter1 = iterations.clone();
    let handle1 = thread::spawn(move || {
        for i in 0..100 {
            // Read with REGULAR get (not transactional)
            let val = db1.get(b"counter").unwrap().unwrap();
            let counter = String::from_utf8_lossy(&val).parse::<i32>().unwrap();
            
            // Start transaction to increment
            let tx_id = db1.begin_transaction().unwrap();
            
            // Read again WITHIN transaction
            let tx_val = db1.get_tx(tx_id, b"counter").unwrap().unwrap();
            let tx_counter = String::from_utf8_lossy(&tx_val).parse::<i32>().unwrap();
            
            // Check if values differ
            if counter != tx_counter {
                println!("Thread 1 iteration {}: Race detected!", i);
                println!("  Regular get: {}", counter);
                println!("  Transaction get: {}", tx_counter);
                race1.store(true, Ordering::Relaxed);
            }
            
            // Increment and commit
            let new_val = (tx_counter + 1).to_string();
            db1.put_tx(tx_id, b"counter", new_val.as_bytes()).unwrap();
            
            if db1.commit_transaction(tx_id).is_ok() {
                iter1.fetch_add(1, Ordering::Relaxed);
            } else {
                db1.abort_transaction(tx_id).ok();
            }
            
            thread::sleep(Duration::from_micros(10));
        }
    });
    
    // Thread 2: Same pattern
    let db2 = db.clone();
    let race2 = race_detected.clone();
    let iter2 = iterations.clone();
    let handle2 = thread::spawn(move || {
        for i in 0..100 {
            // Read with REGULAR get
            let val = db2.get(b"counter").unwrap().unwrap();
            let counter = String::from_utf8_lossy(&val).parse::<i32>().unwrap();
            
            // Start transaction
            let tx_id = db2.begin_transaction().unwrap();
            
            // Read within transaction
            let tx_val = db2.get_tx(tx_id, b"counter").unwrap().unwrap();
            let tx_counter = String::from_utf8_lossy(&tx_val).parse::<i32>().unwrap();
            
            // Check for race
            if counter != tx_counter {
                println!("Thread 2 iteration {}: Race detected!", i);
                println!("  Regular get: {}", counter);
                println!("  Transaction get: {}", tx_counter);
                race2.store(true, Ordering::Relaxed);
            }
            
            // Increment and commit
            let new_val = (tx_counter + 1).to_string();
            db2.put_tx(tx_id, b"counter", new_val.as_bytes()).unwrap();
            
            if db2.commit_transaction(tx_id).is_ok() {
                iter2.fetch_add(1, Ordering::Relaxed);
            } else {
                db2.abort_transaction(tx_id).ok();
            }
            
            thread::sleep(Duration::from_micros(10));
        }
    });
    
    handle1.join().unwrap();
    handle2.join().unwrap();
    
    // Check final state
    let final_val = db.get(b"counter")?.unwrap();
    let final_counter = String::from_utf8_lossy(&final_val).parse::<i32>().unwrap();
    let total_commits = iterations.load(Ordering::Relaxed);
    
    println!("\n=== RESULTS ===");
    println!("Final counter: {}", final_counter);
    println!("Total commits: {}", total_commits);
    println!("Race condition detected: {}", race_detected.load(Ordering::Relaxed));
    
    if final_counter == total_commits as i32 {
        println!("✅ Counter matches commits");
    } else {
        println!("❌ Lost updates: {} commits but counter is {}", 
                total_commits, final_counter);
    }
    
    if race_detected.load(Ordering::Relaxed) {
        println!("\n⚠️  Race condition confirmed:");
        println!("Regular get() doesn't see committed transactions immediately");
        println!("This can cause lost updates when mixing regular and transactional operations");
    }
    
    Ok(())
}