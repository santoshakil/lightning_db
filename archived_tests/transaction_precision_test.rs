use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Precision Test");
    println!("========================================\n");

    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.max_active_transactions = 100;
    config.use_optimized_transactions = true;
    let db = Arc::new(Database::create(dir.path(), config)?);

    // Test 1: Sequential Transaction Verification
    println!("Test 1: Sequential Transaction Verification");
    println!("-------------------------------------------");
    test_sequential_transactions(db.clone())?;

    // Test 2: Precise Conflict Tracking
    println!("\nTest 2: Precise Conflict Tracking");
    println!("----------------------------------");
    test_precise_conflicts(db.clone())?;

    // Test 3: Edge Case - Rapid Fire Transactions
    println!("\nTest 3: Edge Case - Rapid Fire Transactions");
    println!("--------------------------------------------");
    test_rapid_fire_transactions(db.clone())?;

    // Test 4: Write-After-Read Consistency
    println!("\nTest 4: Write-After-Read Consistency");
    println!("-------------------------------------");
    test_write_after_read(db.clone())?;

    Ok(())
}

fn test_sequential_transactions(db: Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize account
    db.put(b"account", b"1000")?;
    
    let mut expected = 1000i64;
    let mut actual_operations = Vec::new();
    
    // Perform 100 sequential transactions
    for i in 0..100 {
        let tx_id = db.begin_transaction()?;
        
        // Read current balance
        let current = db.get_tx(tx_id, b"account")?
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        
        // Perform operation (alternating add/subtract)
        let delta = if i % 2 == 0 { 10 } else { -5 };
        let new_balance = current + delta;
        
        // Write new balance
        db.put_tx(tx_id, b"account", new_balance.to_string().as_bytes())?;
        
        // Try to commit
        match db.commit_transaction(tx_id) {
            Ok(_) => {
                expected += delta;
                actual_operations.push((i, delta, true));
                println!("  Transaction {}: {} → {} (delta: {})", i, current, new_balance, delta);
            }
            Err(_) => {
                db.abort_transaction(tx_id)?;
                actual_operations.push((i, delta, false));
                println!("  Transaction {}: ABORTED", i);
            }
        }
    }
    
    // Verify final balance
    let final_balance = db.get(b"account")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    
    println!("  Expected: {}, Actual: {}", expected, final_balance);
    if expected == final_balance {
        println!("  ✅ PASSED: Sequential transactions maintain consistency");
    } else {
        println!("  ❌ FAILED: Balance mismatch! {} != {}", final_balance, expected);
    }
    
    Ok(())
}

fn test_precise_conflicts(db: Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize two accounts
    db.put(b"acc1", b"1000")?;
    db.put(b"acc2", b"1000")?;
    
    let conflict_count = Arc::new(AtomicU64::new(0));
    let success_count = Arc::new(AtomicU64::new(0));
    
    // Create 10 threads doing conflicting operations
    let mut handles = vec![];
    for thread_id in 0..10 {
        let db = db.clone();
        let conflicts = conflict_count.clone();
        let successes = success_count.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..20 {
                // Start transaction
                let tx_id = match db.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                
                // Read both accounts
                let bal1 = db.get_tx(tx_id, b"acc1").ok()
                    .and_then(|v| v)
                    .and_then(|v| String::from_utf8(v).ok())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                    
                let bal2 = db.get_tx(tx_id, b"acc2").ok()
                    .and_then(|v| v)
                    .and_then(|v| String::from_utf8(v).ok())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                
                // Transfer 10 from acc1 to acc2
                let _ = db.put_tx(tx_id, b"acc1", (bal1 - 10).to_string().as_bytes());
                let _ = db.put_tx(tx_id, b"acc2", (bal2 + 10).to_string().as_bytes());
                
                // Try to commit
                match db.commit_transaction(tx_id) {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        let _ = db.abort_transaction(tx_id);
                        conflicts.fetch_add(1, Ordering::SeqCst);
                    }
                }
                
                // Small delay to increase conflict likelihood
                thread::sleep(Duration::from_micros(10));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Check final balances
    let bal1 = db.get(b"acc1")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
        
    let bal2 = db.get(b"acc2")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    
    let total = bal1 + bal2;
    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);
    
    println!("  Account 1: {}", bal1);
    println!("  Account 2: {}", bal2);
    println!("  Total: {} (expected: 2000)", total);
    println!("  Successful transactions: {}", successes);
    println!("  Conflicts detected: {}", conflicts);
    println!("  Expected acc1: {}", 1000 - (successes as i64 * 10));
    println!("  Expected acc2: {}", 1000 + (successes as i64 * 10));
    
    if total == 2000 && 
       bal1 == 1000 - (successes as i64 * 10) && 
       bal2 == 1000 + (successes as i64 * 10) {
        println!("  ✅ PASSED: Precise conflict tracking maintains consistency");
    } else {
        println!("  ❌ FAILED: Balance inconsistency detected!");
    }
    
    Ok(())
}

fn test_rapid_fire_transactions(db: Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize counter
    db.put(b"counter", b"0")?;
    
    let total_increments = Arc::new(AtomicU64::new(0));
    
    // Create 20 threads doing rapid increments
    let mut handles = vec![];
    for _ in 0..20 {
        let db = db.clone();
        let increments = total_increments.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..50 {
                // Rapid fire transactions without delay
                let tx_id = match db.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                
                let current = db.get_tx(tx_id, b"counter").ok()
                    .and_then(|v| v)
                    .and_then(|v| String::from_utf8(v).ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                
                let _ = db.put_tx(tx_id, b"counter", (current + 1).to_string().as_bytes());
                
                match db.commit_transaction(tx_id) {
                    Ok(_) => {
                        increments.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        let _ = db.abort_transaction(tx_id);
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_counter = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    
    let expected = total_increments.load(Ordering::SeqCst);
    
    println!("  Final counter: {}", final_counter);
    println!("  Expected (successful increments): {}", expected);
    
    if final_counter == expected {
        println!("  ✅ PASSED: Rapid fire transactions maintain consistency");
    } else {
        println!("  ❌ FAILED: Counter mismatch! {} != {}", final_counter, expected);
    }
    
    Ok(())
}

fn test_write_after_read(db: Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize test keys
    for i in 0..10 {
        let key = format!("war_key_{}", i);
        db.put(key.as_bytes(), b"0")?;
    }
    
    let inconsistencies = Arc::new(AtomicU64::new(0));
    
    // Create threads doing read-modify-write operations
    let mut handles = vec![];
    for thread_id in 0..5 {
        let db = db.clone();
        let inconsistent = inconsistencies.clone();
        
        let handle = thread::spawn(move || {
            for _round in 0..20 {
                let tx_id = match db.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                
                // Read all values
                let mut values = Vec::new();
                for i in 0..10 {
                    let key = format!("war_key_{}", i);
                    let val = db.get_tx(tx_id, key.as_bytes()).ok()
                        .and_then(|v| v)
                        .and_then(|v| String::from_utf8(v).ok())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    values.push(val);
                }
                
                // Simulate processing delay
                thread::sleep(Duration::from_micros(50));
                
                // Write modified values
                for (i, val) in values.iter().enumerate() {
                    let key = format!("war_key_{}", i);
                    let new_val = val + thread_id + 1;
                    let _ = db.put_tx(tx_id, key.as_bytes(), new_val.to_string().as_bytes());
                }
                
                // Commit and check for consistency
                match db.commit_transaction(tx_id) {
                    Ok(_) => {
                        // Verify writes were consistent
                        let mut sum = 0i64;
                        for i in 0..10 {
                            let key = format!("war_key_{}", i);
                            let val = db.get(key.as_bytes()).ok()
                                .and_then(|v| v)
                                .and_then(|v| String::from_utf8(v).ok())
                                .and_then(|s| s.parse::<i64>().ok())
                                .unwrap_or(0);
                            sum += val;
                        }
                        
                        // Check if sum is consistent with expectations
                        if sum % 10 != 0 {
                            inconsistent.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    Err(_) => {
                        let _ = db.abort_transaction(tx_id);
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let inconsistent_count = inconsistencies.load(Ordering::SeqCst);
    
    println!("  Inconsistent states detected: {}", inconsistent_count);
    
    if inconsistent_count == 0 {
        println!("  ✅ PASSED: Write-after-read maintains consistency");
    } else {
        println!("  ⚠️  WARNING: {} inconsistent states detected", inconsistent_count);
    }
    
    Ok(())
}