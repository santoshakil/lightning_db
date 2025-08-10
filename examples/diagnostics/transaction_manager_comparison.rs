use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Manager Comparison");
    println!("============================================\n");

    // Test with optimized transaction manager
    println!("Test 1: With Optimized Transaction Manager");
    println!("-------------------------------------------");
    test_with_config(true)?;

    // Test without optimized transaction manager  
    println!("\nTest 2: Without Optimized Transaction Manager");
    println!("----------------------------------------------");
    test_with_config(false)?;

    Ok(())
}

fn test_with_config(use_optimized: bool) -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.use_optimized_transactions = use_optimized;
    let db = Arc::new(Database::create(dir.path(), config)?);

    // Initialize counter
    db.put(b"counter", b"0")?;
    
    let successful_commits = Arc::new(AtomicU64::new(0));
    let failed_commits = Arc::new(AtomicU64::new(0));
    
    // Launch 20 threads, each doing 50 increments
    let mut handles = vec![];
    for thread_id in 0..20 {
        let db = db.clone();
        let success = successful_commits.clone();
        let failed = failed_commits.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..50 {
                // Begin transaction
                let tx_id = match db.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => {
                        failed.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                };
                
                // Read current value
                let current = match db.get_tx(tx_id, b"counter") {
                    Ok(Some(v)) => String::from_utf8_lossy(&v).parse::<u64>().unwrap_or(0),
                    Ok(None) => 0,
                    Err(_) => {
                        let _ = db.abort_transaction(tx_id);
                        failed.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                };
                
                // Increment
                let new_value = current + 1;
                
                // Write new value
                if db.put_tx(tx_id, b"counter", new_value.to_string().as_bytes()).is_err() {
                    let _ = db.abort_transaction(tx_id);
                    failed.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                
                // Try to commit
                match db.commit_transaction(tx_id) {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        let _ = db.abort_transaction(tx_id);
                        failed.fetch_add(1, Ordering::SeqCst);
                    }
                }
                
                // Small delay to reduce contention slightly
                thread::sleep(std::time::Duration::from_micros(thread_id as u64));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        let _ = handle.join();
    }
    
    // Small delay to ensure everything is flushed
    thread::sleep(std::time::Duration::from_millis(100));
    
    // Check final value
    let final_value = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    
    let commits = successful_commits.load(Ordering::SeqCst);
    let failures = failed_commits.load(Ordering::SeqCst);
    
    println!("  Total attempts: {}", 20 * 50);
    println!("  Successful commits: {}", commits);
    println!("  Failed commits: {}", failures);
    println!("  Final counter value: {}", final_value);
    println!("  Expected value: {}", commits);
    
    let success_rate = commits as f64 / 1000.0 * 100.0;
    let accuracy = if commits > 0 {
        final_value as f64 / commits as f64 * 100.0
    } else {
        0.0
    };
    
    println!("  Success rate: {:.1}%", success_rate);
    println!("  Accuracy: {:.1}%", accuracy);
    
    if final_value == commits {
        println!("  ✅ PASSED: All commits persisted correctly");
    } else if final_value < commits {
        println!("  ❌ FAILED: Lost {} updates ({:.1}% loss)", 
                commits - final_value,
                (commits - final_value) as f64 / commits as f64 * 100.0);
    } else {
        println!("  ❌ FAILED: Extra updates detected! {} > {}", final_value, commits);
    }
    
    // Also test immediate re-open to verify persistence
    drop(db);
    let mut config2 = LightningDbConfig::default();
    config2.use_optimized_transactions = use_optimized;
    let db2 = Database::open(dir.path(), config2)?;
    
    let persisted_value = db2.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    
    if persisted_value == final_value {
        println!("  ✅ Persistence verified: Value survived restart");
    } else {
        println!("  ❌ Persistence issue: {} != {} after restart", persisted_value, final_value);
    }
    
    Ok(())
}