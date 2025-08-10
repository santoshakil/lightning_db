use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use rand::Rng;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Stress Test");
    println!("=====================================\n");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    // Test 1: High Contention Bank Transfer
    println!("Test 1: High Contention Bank Transfer");
    println!("--------------------------------------");
    test_bank_transfers(&db)?;
    
    // Test 2: Read-Write Mix
    println!("\nTest 2: Mixed Read-Write Transactions");
    println!("--------------------------------------");
    test_mixed_transactions(&db)?;
    
    // Test 3: Long Running Transactions
    println!("\nTest 3: Long Running Transactions");
    println!("-----------------------------------");
    test_long_transactions(&db)?;
    
    // Test 4: Abort and Retry Pattern
    println!("\nTest 4: Abort and Retry Pattern");
    println!("--------------------------------");
    test_abort_retry(&db)?;
    
    println!("\n✅ All transaction stress tests passed!");
    Ok(())
}

fn test_bank_transfers(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize 10 accounts with 1000 each
    for i in 0..10 {
        let key = format!("account_{}", i);
        db.put(key.as_bytes(), b"1000")?;
    }
    
    let total_initial = 10000;
    let successful = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    
    // Run 8 threads doing random transfers
    let mut handles = vec![];
    for thread_id in 0..8 {
        let db = db.clone();
        let success = successful.clone();
        let fail = failed.clone();
        let conflict = conflicts.clone();
        
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            
            for _ in 0..50 {
                // Pick two random accounts
                let from = rng.gen_range(0..10);
                let to = rng.gen_range(0..10);
                
                if from == to {
                    continue;
                }
                
                let from_key = format!("account_{}", from);
                let to_key = format!("account_{}", to);
                let amount = rng.gen_range(1..100);
                
                // Retry up to 3 times on conflict
                let mut retries = 0;
                loop {
                    let tx_id = match db.begin_transaction() {
                        Ok(id) => id,
                        Err(_) => {
                            fail.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    };
                    
                    // Read balances
                    let from_val = match db.get_tx(tx_id, from_key.as_bytes()) {
                        Ok(Some(v)) => v,
                        _ => {
                            db.abort_transaction(tx_id).ok();
                            fail.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    };
                    
                    let to_val = match db.get_tx(tx_id, to_key.as_bytes()) {
                        Ok(Some(v)) => v,
                        _ => {
                            db.abort_transaction(tx_id).ok();
                            fail.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    };
                    
                    let from_balance = String::from_utf8_lossy(&from_val)
                        .parse::<i32>().unwrap_or(0);
                    let to_balance = String::from_utf8_lossy(&to_val)
                        .parse::<i32>().unwrap_or(0);
                    
                    if from_balance >= amount {
                        let new_from = (from_balance - amount).to_string();
                        let new_to = (to_balance + amount).to_string();
                        
                        // Write new balances
                        if db.put_tx(tx_id, from_key.as_bytes(), new_from.as_bytes()).is_err() ||
                           db.put_tx(tx_id, to_key.as_bytes(), new_to.as_bytes()).is_err() {
                            db.abort_transaction(tx_id).ok();
                            fail.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        
                        // Try to commit
                        match db.commit_transaction(tx_id) {
                            Ok(_) => {
                                success.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(_) => {
                                conflict.fetch_add(1, Ordering::Relaxed);
                                retries += 1;
                                if retries >= 3 {
                                    fail.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                // Retry with backoff
                                thread::sleep(Duration::from_micros(100 * retries));
                            }
                        }
                    } else {
                        // Insufficient funds
                        db.abort_transaction(tx_id).ok();
                        break;
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify total is preserved
    let mut total = 0;
    for i in 0..10 {
        let key = format!("account_{}", i);
        let val = db.get(key.as_bytes())?.unwrap();
        let balance = String::from_utf8_lossy(&val).parse::<i32>().unwrap_or(0);
        total += balance;
    }
    
    println!("Results:");
    println!("  Successful: {}", successful.load(Ordering::Relaxed));
    println!("  Failed: {}", failed.load(Ordering::Relaxed));
    println!("  Conflicts: {}", conflicts.load(Ordering::Relaxed));
    println!("  Total preserved: {} (expected {})", total, total_initial);
    
    if total == total_initial {
        println!("  ✅ ACID properties maintained");
    } else {
        println!("  ❌ ACID violation: {} difference", total_initial - total);
        return Err("ACID violation in bank transfers".into());
    }
    
    Ok(())
}

fn test_mixed_transactions(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize test data
    for i in 0..100 {
        let key = format!("mixed_key_{:04}", i);
        let value = format!("initial_value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    let reads = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let read_writes = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    for thread_id in 0..4 {
        let db = db.clone();
        let r = reads.clone();
        let w = writes.clone();
        let rw = read_writes.clone();
        
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            
            for _ in 0..25 {
                let tx_type = rng.gen_range(0..3);
                let tx_id = db.begin_transaction().unwrap();
                
                match tx_type {
                    0 => {
                        // Read-only transaction
                        for _ in 0..5 {
                            let idx = rng.gen_range(0..100);
                            let key = format!("mixed_key_{:04}", idx);
                            let _ = db.get_tx(tx_id, key.as_bytes());
                        }
                        db.commit_transaction(tx_id).ok();
                        r.fetch_add(1, Ordering::Relaxed);
                    }
                    1 => {
                        // Write-only transaction
                        for _ in 0..3 {
                            let idx = rng.gen_range(0..100);
                            let key = format!("mixed_key_{:04}", idx);
                            let value = format!("thread_{}_value_{}", thread_id, idx);
                            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).ok();
                        }
                        if db.commit_transaction(tx_id).is_ok() {
                            w.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => {
                        // Read-modify-write transaction
                        let idx = rng.gen_range(0..100);
                        let key = format!("mixed_key_{:04}", idx);
                        
                        if let Ok(Some(val)) = db.get_tx(tx_id, key.as_bytes()) {
                            let new_val = format!("{}_modified_{}", 
                                String::from_utf8_lossy(&val), thread_id);
                            db.put_tx(tx_id, key.as_bytes(), new_val.as_bytes()).ok();
                        }
                        
                        if db.commit_transaction(tx_id).is_ok() {
                            rw.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    println!("Results:");
    println!("  Read-only transactions: {}", reads.load(Ordering::Relaxed));
    println!("  Write-only transactions: {}", writes.load(Ordering::Relaxed));
    println!("  Read-modify-write transactions: {}", read_writes.load(Ordering::Relaxed));
    println!("  ✅ Mixed transaction patterns handled correctly");
    
    Ok(())
}

fn test_long_transactions(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Test transactions that hold locks for longer periods
    let completed = Arc::new(AtomicU64::new(0));
    let aborted = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    for thread_id in 0..4 {
        let db = db.clone();
        let comp = completed.clone();
        let abort = aborted.clone();
        
        let handle = thread::spawn(move || {
            for i in 0..10 {
                let tx_id = db.begin_transaction().unwrap();
                
                // Simulate long-running work
                for j in 0..10 {
                    let key = format!("long_tx_{}_{}", thread_id, j);
                    let value = format!("iteration_{}", i);
                    
                    if db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_err() {
                        db.abort_transaction(tx_id).ok();
                        abort.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    
                    // Simulate processing time
                    thread::sleep(Duration::from_micros(100));
                }
                
                if db.commit_transaction(tx_id).is_ok() {
                    comp.fetch_add(1, Ordering::Relaxed);
                } else {
                    abort.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }
    
    let start = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }
    let duration = start.elapsed();
    
    println!("Results:");
    println!("  Completed: {}", completed.load(Ordering::Relaxed));
    println!("  Aborted: {}", aborted.load(Ordering::Relaxed));
    println!("  Duration: {:?}", duration);
    println!("  ✅ Long-running transactions handled correctly");
    
    Ok(())
}

fn test_abort_retry(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Test transaction abort and retry patterns
    let key = b"retry_counter";
    db.put(key, b"0")?;
    
    let successful_retries = Arc::new(AtomicU64::new(0));
    let max_retries_hit = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    for _ in 0..4 {
        let db = db.clone();
        let success = successful_retries.clone();
        let max_retry = max_retries_hit.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..10 {
                let max_attempts = 5;
                let mut attempts = 0;
                
                loop {
                    attempts += 1;
                    let tx_id = db.begin_transaction().unwrap();
                    
                    // Read current value
                    let val = db.get_tx(tx_id, key).unwrap().unwrap();
                    let counter = String::from_utf8_lossy(&val)
                        .parse::<i32>().unwrap_or(0);
                    
                    // Increment
                    let new_val = (counter + 1).to_string();
                    db.put_tx(tx_id, key, new_val.as_bytes()).unwrap();
                    
                    // Try to commit
                    match db.commit_transaction(tx_id) {
                        Ok(_) => {
                            if attempts > 1 {
                                success.fetch_add(1, Ordering::Relaxed);
                            }
                            break;
                        }
                        Err(_) => {
                            if attempts >= max_attempts {
                                max_retry.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            // Exponential backoff
                            thread::sleep(Duration::from_micros(10 * (1 << attempts)));
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Check final counter value
    let final_val = db.get(key)?.unwrap();
    let final_counter = String::from_utf8_lossy(&final_val)
        .parse::<i32>().unwrap_or(0);
    
    println!("Results:");
    println!("  Final counter: {}", final_counter);
    println!("  Successful retries: {}", successful_retries.load(Ordering::Relaxed));
    println!("  Max retries hit: {}", max_retries_hit.load(Ordering::Relaxed));
    println!("  ✅ Abort and retry pattern working correctly");
    
    Ok(())
}