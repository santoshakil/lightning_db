use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Transaction Isolation Debug Test");
    println!("==============================================\n");
    
    // Test 1: Simple concurrent transfers
    test_simple_transfers()?;
    
    // Test 2: Single account stress
    test_single_account_stress()?;
    
    // Test 3: Transaction conflict detection
    test_conflict_detection()?;
    
    Ok(())
}

fn test_simple_transfers() -> Result<(), Box<dyn std::error::Error>> {
    println!("Test 1: Simple Concurrent Transfers");
    println!("------------------------------------");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    // Initialize two accounts
    let initial_balance: u64 = 1000;
    db.put(b"account_A", &initial_balance.to_le_bytes())?;
    db.put(b"account_B", &initial_balance.to_le_bytes())?;
    
    const NUM_THREADS: usize = 2;
    const TRANSFERS_PER_THREAD: usize = 100;
    const TRANSFER_AMOUNT: u64 = 10;
    
    let success_count = Arc::new(AtomicU64::new(0));
    let abort_count = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let success = Arc::clone(&success_count);
        let aborts = Arc::clone(&abort_count);
        
        let handle = thread::spawn(move || {
            for _ in 0..TRANSFERS_PER_THREAD {
                // Alternate transfer direction based on thread
                let (from_key, to_key) = if thread_id % 2 == 0 {
                    (b"account_A", b"account_B")
                } else {
                    (b"account_B", b"account_A")
                };
                
                // Start transaction
                let tx_id = match db_clone.begin_transaction() {
                    Ok(id) => id,
                    Err(e) => {
                        println!("Failed to begin transaction: {}", e);
                        continue;
                    }
                };
                
                // Read balances
                let from_balance = match db_clone.get_tx(tx_id, from_key) {
                    Ok(Some(data)) => {
                        let bytes: [u8; 8] = data.try_into().unwrap_or([0; 8]);
                        u64::from_le_bytes(bytes)
                    }
                    _ => {
                        db_clone.abort_transaction(tx_id).ok();
                        aborts.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                let to_balance = match db_clone.get_tx(tx_id, to_key) {
                    Ok(Some(data)) => {
                        let bytes: [u8; 8] = data.try_into().unwrap_or([0; 8]);
                        u64::from_le_bytes(bytes)
                    }
                    _ => {
                        db_clone.abort_transaction(tx_id).ok();
                        aborts.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                // Check sufficient balance
                if from_balance < TRANSFER_AMOUNT {
                    db_clone.abort_transaction(tx_id).ok();
                    continue;
                }
                
                // Update balances
                let new_from = from_balance - TRANSFER_AMOUNT;
                let new_to = to_balance + TRANSFER_AMOUNT;
                
                // Write new balances
                if db_clone.put_tx(tx_id, from_key, &new_from.to_le_bytes()).is_err() {
                    db_clone.abort_transaction(tx_id).ok();
                    aborts.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                
                if db_clone.put_tx(tx_id, to_key, &new_to.to_le_bytes()).is_err() {
                    db_clone.abort_transaction(tx_id).ok();
                    aborts.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                
                // Commit transaction
                match db_clone.commit_transaction(tx_id) {
                    Ok(_) => success.fetch_add(1, Ordering::Relaxed),
                    Err(_) => aborts.fetch_add(1, Ordering::Relaxed),
                };
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify final balances
    let balance_a = u64::from_le_bytes(
        db.get(b"account_A")?.unwrap_or(vec![0; 8]).try_into().unwrap_or([0; 8])
    );
    let balance_b = u64::from_le_bytes(
        db.get(b"account_B")?.unwrap_or(vec![0; 8]).try_into().unwrap_or([0; 8])
    );
    
    let total_balance = balance_a + balance_b;
    let expected_total = initial_balance * 2;
    
    println!("Account A: {}", balance_a);
    println!("Account B: {}", balance_b);
    println!("Total: {} (expected: {})", total_balance, expected_total);
    println!("Successful transfers: {}", success_count.load(Ordering::Relaxed));
    println!("Aborted transactions: {}", abort_count.load(Ordering::Relaxed));
    
    if total_balance != expected_total {
        println!("❌ FAILED: Balance mismatch! {} != {}", total_balance, expected_total);
        return Err("Transaction isolation violated".into());
    } else {
        println!("✅ PASSED: Balances correct");
    }
    
    Ok(())
}

fn test_single_account_stress() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 2: Single Account Stress");
    println!("------------------------------");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    // Initialize single account
    let initial_balance: u64 = 1000;
    db.put(b"account", &initial_balance.to_le_bytes())?;
    
    const NUM_THREADS: usize = 4;
    const OPS_PER_THREAD: usize = 50;
    
    let mut handles = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        
        let handle = thread::spawn(move || {
            for _ in 0..OPS_PER_THREAD {
                // Start transaction
                let tx_id = match db_clone.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                
                // Read balance
                let balance = match db_clone.get_tx(tx_id, b"account") {
                    Ok(Some(data)) => {
                        let bytes: [u8; 8] = data.try_into().unwrap_or([0; 8]);
                        u64::from_le_bytes(bytes)
                    }
                    _ => {
                        db_clone.abort_transaction(tx_id).ok();
                        continue;
                    }
                };
                
                // Modify balance (increment by thread_id to track contributions)
                let new_balance = balance + thread_id as u64 + 1;
                
                // Write new balance
                if db_clone.put_tx(tx_id, b"account", &new_balance.to_le_bytes()).is_err() {
                    db_clone.abort_transaction(tx_id).ok();
                    continue;
                }
                
                // Commit transaction
                db_clone.commit_transaction(tx_id).ok();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Check final balance
    let final_balance = u64::from_le_bytes(
        db.get(b"account")?.unwrap_or(vec![0; 8]).try_into().unwrap_or([0; 8])
    );
    
    // Calculate expected balance
    // Thread 0 adds 1 each time, thread 1 adds 2, etc.
    let mut expected = initial_balance;
    for thread_id in 0..NUM_THREADS {
        expected += (thread_id as u64 + 1) * OPS_PER_THREAD as u64;
    }
    
    println!("Final balance: {}", final_balance);
    println!("Expected balance: {}", expected);
    
    if final_balance < expected {
        println!("⚠️  Lost updates detected: {} < {}", final_balance, expected);
        println!("    Missing: {} units", expected - final_balance);
    } else if final_balance > expected {
        println!("❌ FAILED: Extra balance created: {} > {}", final_balance, expected);
        return Err("Transaction isolation violated - phantom money".into());
    } else {
        println!("✅ PASSED: Balance correct");
    }
    
    Ok(())
}

fn test_conflict_detection() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 3: Transaction Conflict Detection");
    println!("---------------------------------------");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path(), config)?;
    
    // Initialize data
    db.put(b"key1", b"value1")?;
    
    // Start two transactions
    let tx1 = db.begin_transaction()?;
    let tx2 = db.begin_transaction()?;
    
    println!("Transaction 1 ID: {}", tx1);
    println!("Transaction 2 ID: {}", tx2);
    
    // Both transactions read the same key
    let val1 = db.get_tx(tx1, b"key1")?;
    let val2 = db.get_tx(tx2, b"key1")?;
    
    println!("Tx1 read: {:?}", val1);
    println!("Tx2 read: {:?}", val2);
    
    // Both try to update the same key
    db.put_tx(tx1, b"key1", b"updated_by_tx1")?;
    db.put_tx(tx2, b"key1", b"updated_by_tx2")?;
    
    // Try to commit both
    let commit1 = db.commit_transaction(tx1);
    let commit2 = db.commit_transaction(tx2);
    
    println!("Tx1 commit result: {:?}", commit1);
    println!("Tx2 commit result: {:?}", commit2);
    
    // Check final value
    let final_value = db.get(b"key1")?;
    println!("Final value: {:?}", final_value.as_ref().map(|v| String::from_utf8_lossy(v)));
    
    // Verify only one succeeded
    if commit1.is_ok() && commit2.is_ok() {
        println!("❌ FAILED: Both transactions committed - no conflict detection!");
        return Err("No conflict detection".into());
    } else if commit1.is_ok() || commit2.is_ok() {
        println!("✅ PASSED: Conflict properly detected");
    } else {
        println!("⚠️  WARNING: Both transactions failed");
    }
    
    Ok(())
}