use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Concurrent Transaction Loss Test");
    println!("==============================================\n");

    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.use_optimized_transactions = false; // Use regular manager
    let db = Arc::new(Database::create(dir.path(), config)?);

    // Initialize counter
    db.put(b"counter", b"0")?;
    println!("Initial value set to 0");

    // Create a barrier to ensure all threads start at the same time
    let barrier = Arc::new(Barrier::new(3)); // 2 worker threads + 1 main thread

    // Start two threads that will try to increment concurrently
    let mut handles = vec![];
    
    for thread_id in 0..2 {
        let db = db.clone();
        let barrier = barrier.clone();
        
        let handle = thread::spawn(move || -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
            // Wait for all threads to be ready
            barrier.wait();
            
            // Small delay to create different timing
            thread::sleep(std::time::Duration::from_micros(thread_id * 100));
            
            // Begin transaction
            let tx_id = db.begin_transaction()?;
            println!("[Thread {}] Started transaction {}", thread_id, tx_id);
            
            // Read current value
            let current = db.get_tx(tx_id, b"counter")?
                .and_then(|v| String::from_utf8(v).ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            println!("[Thread {}] Read value: {}", thread_id, current);
            
            // Small delay to increase chance of race condition
            thread::sleep(std::time::Duration::from_micros(50));
            
            // Write new value
            let new_value = current + 1;
            db.put_tx(tx_id, b"counter", new_value.to_string().as_bytes())?;
            println!("[Thread {}] Writing value: {}", thread_id, new_value);
            
            // Try to commit
            match db.commit_transaction(tx_id) {
                Ok(_) => {
                    println!("[Thread {}] ✅ Committed successfully with value {}", thread_id, new_value);
                    Ok(new_value)
                }
                Err(e) => {
                    println!("[Thread {}] ❌ Commit failed: {}", thread_id, e);
                    Err(Box::new(e))
                }
            }
        });
        handles.push(handle);
    }
    
    // Signal threads to start
    barrier.wait();
    
    // Wait for both threads to complete
    let mut successful_values = Vec::new();
    for handle in handles {
        if let Ok(Ok(value)) = handle.join() {
            successful_values.push(value);
        }
    }
    
    // Small delay to ensure everything is flushed
    thread::sleep(std::time::Duration::from_millis(100));
    
    // Check final value
    let final_value = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    
    println!("\n=== RESULTS ===");
    println!("Successful commits: {:?}", successful_values);
    println!("Final counter value: {}", final_value);
    println!("Expected final value: {}", successful_values.len());
    
    if final_value == successful_values.len() as u64 {
        println!("✅ SUCCESS: All committed transactions persisted");
    } else {
        println!("❌ FAILURE: Lost {} transactions", successful_values.len() as u64 - final_value);
        
        // Check version store directly to see if data is there
        println!("\n=== DEBUGGING INFO ===");
        
        // Close and reopen to ensure everything is flushed
        drop(db);
        let db2 = Database::open(dir.path(), LightningDbConfig::default())?;
        let after_reopen = db2.get(b"counter")?
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        println!("Value after reopen: {}", after_reopen);
    }
    
    Ok(())
}