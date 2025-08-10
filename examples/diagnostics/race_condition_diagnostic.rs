use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Mutex, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct TransactionLog {
    tx_id: u64,
    operation: String,
    key: String,
    value_before: Option<String>,
    value_after: String,
    success: bool,
    timestamp: Instant,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Race Condition Diagnostic");
    println!("======================================\n");

    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.use_optimized_transactions = true;
    config.max_active_transactions = 100;
    let db = Arc::new(Database::create(dir.path(), config)?);

    // Test 1: Simple Concurrent Counter
    println!("Test 1: Simple Concurrent Counter");
    println!("----------------------------------");
    test_simple_counter(db.clone())?;

    // Test 2: Interleaved Read-Write Pattern
    println!("\nTest 2: Interleaved Read-Write Pattern");
    println!("---------------------------------------");
    test_interleaved_pattern(db.clone())?;

    // Test 3: Commit Synchronization Analysis
    println!("\nTest 3: Commit Synchronization Analysis");
    println!("----------------------------------------");
    test_commit_synchronization(db.clone())?;

    // Test 4: Version Store vs Main Store Consistency
    println!("\nTest 4: Version Store vs Main Store Consistency");
    println!("------------------------------------------------");
    test_store_consistency(db.clone())?;

    Ok(())
}

fn test_simple_counter(db: Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize counter
    db.put(b"counter", b"0")?;
    
    let successful_commits = Arc::new(AtomicU64::new(0));
    let transaction_log = Arc::new(Mutex::new(Vec::new()));
    
    // Launch 10 threads, each incrementing 100 times
    let mut handles = vec![];
    for thread_id in 0..10 {
        let db = db.clone();
        let commits = successful_commits.clone();
        let log = transaction_log.clone();
        
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let start = Instant::now();
                
                // Begin transaction
                let tx_id = match db.begin_transaction() {
                    Ok(id) => id,
                    Err(e) => {
                        println!("Thread {} failed to begin tx: {}", thread_id, e);
                        continue;
                    }
                };
                
                // Read current value
                let current = match db.get_tx(tx_id, b"counter") {
                    Ok(Some(v)) => String::from_utf8_lossy(&v).parse::<u64>().unwrap_or(0),
                    Ok(None) => 0,
                    Err(e) => {
                        println!("Thread {} read error: {}", thread_id, e);
                        let _ = db.abort_transaction(tx_id);
                        continue;
                    }
                };
                
                // Increment
                let new_value = current + 1;
                
                // Write new value
                if let Err(e) = db.put_tx(tx_id, b"counter", new_value.to_string().as_bytes()) {
                    println!("Thread {} write error: {}", thread_id, e);
                    let _ = db.abort_transaction(tx_id);
                    continue;
                }
                
                // Try to commit
                let success = match db.commit_transaction(tx_id) {
                    Ok(_) => {
                        commits.fetch_add(1, Ordering::SeqCst);
                        true
                    }
                    Err(_) => {
                        let _ = db.abort_transaction(tx_id);
                        false
                    }
                };
                
                // Log the transaction
                let entry = TransactionLog {
                    tx_id,
                    operation: format!("increment_t{}_i{}", thread_id, i),
                    key: "counter".to_string(),
                    value_before: Some(current.to_string()),
                    value_after: new_value.to_string(),
                    success,
                    timestamp: start,
                };
                
                if let Ok(mut log) = log.lock() {
                    log.push(entry);
                }
                
                // Small random delay to increase contention
                thread::sleep(Duration::from_micros(thread_id as u64 * 10));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        let _ = handle.join();
    }
    
    // Check final value
    thread::sleep(Duration::from_millis(100)); // Allow time for any pending operations
    
    let final_value = db.get(b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    
    let commits = successful_commits.load(Ordering::SeqCst);
    
    println!("  Successful commits: {}", commits);
    println!("  Final counter value: {}", final_value);
    println!("  Expected value: {}", commits);
    
    if final_value == commits {
        println!("  ✅ PASSED: All commits persisted correctly");
    } else {
        println!("  ❌ FAILED: Lost {} updates", commits - final_value);
        
        // Analyze the log
        if let Ok(log) = transaction_log.lock() {
            let successful: Vec<_> = log.iter().filter(|e| e.success).collect();
            println!("  Transaction log shows {} successful commits", successful.len());
        }
    }
    
    Ok(())
}

fn test_interleaved_pattern(db: Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Test pattern: Thread A writes, Thread B reads immediately
    let key = b"interleaved_test";
    db.put(key, b"0")?;
    
    let write_complete = Arc::new(AtomicBool::new(false));
    let read_values = Arc::new(Mutex::new(Vec::new()));
    
    // Writer thread
    let db_writer = db.clone();
    let write_flag = write_complete.clone();
    let writer = thread::spawn(move || {
        for i in 1..=10 {
            let tx_id = db_writer.begin_transaction().unwrap();
            db_writer.put_tx(tx_id, key, i.to_string().as_bytes()).unwrap();
            
            if db_writer.commit_transaction(tx_id).is_ok() {
                write_flag.store(true, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(10)); // Give reader time
                write_flag.store(false, Ordering::SeqCst);
            }
        }
    });
    
    // Reader thread
    let db_reader = db.clone();
    let write_flag = write_complete.clone();
    let values = read_values.clone();
    let reader = thread::spawn(move || {
        let mut last_value = 0;
        for _ in 0..100 {
            if write_flag.load(Ordering::SeqCst) {
                if let Ok(Some(v)) = db_reader.get(key) {
                    if let Ok(val) = String::from_utf8(v).unwrap().parse::<u64>() {
                        if val != last_value {
                            if let Ok(mut vals) = values.lock() {
                                vals.push(val);
                            }
                            last_value = val;
                        }
                    }
                }
            }
            thread::sleep(Duration::from_millis(2));
        }
    });
    
    let _ = writer.join();
    let _ = reader.join();
    
    // Check results
    if let Ok(vals) = read_values.lock() {
        println!("  Values observed by reader: {:?}", vals);
        if vals.len() == 10 {
            println!("  ✅ PASSED: All writes were visible to reader");
        } else {
            println!("  ⚠️  WARNING: Only {}/10 writes were visible", vals.len());
        }
    }
    
    Ok(())
}

fn test_commit_synchronization(db: Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Test if commits are truly synchronized
    let test_key = b"sync_test";
    db.put(test_key, b"0")?;
    
    let commit_times = Arc::new(Mutex::new(Vec::new()));
    let read_after_commit = Arc::new(Mutex::new(HashMap::new()));
    
    // Multiple threads doing commit and immediate read
    let mut handles = vec![];
    for thread_id in 0..5 {
        let db = db.clone();
        let times = commit_times.clone();
        let reads = read_after_commit.clone();
        
        let handle = thread::spawn(move || {
            for i in 0..20 {
                let value = format!("t{}_i{}", thread_id, i);
                
                // Transaction commit
                let tx_id = match db.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                let _ = db.get_tx(tx_id, test_key); // Read to track
                if db.put_tx(tx_id, test_key, value.as_bytes()).is_err() {
                    let _ = db.abort_transaction(tx_id);
                    continue;
                }
                
                let commit_start = Instant::now();
                let commit_result = db.commit_transaction(tx_id);
                let commit_duration = commit_start.elapsed();
                
                if commit_result.is_ok() {
                    // Immediately read back
                    let read_value = db.get(test_key)
                        .ok()
                        .and_then(|v| v)
                        .and_then(|v| String::from_utf8(v).ok());
                    
                    // Record timing
                    if let Ok(mut times) = times.lock() {
                        times.push(commit_duration);
                    }
                    
                    // Record what we read
                    if let Ok(mut reads) = reads.lock() {
                        reads.insert(value.clone(), read_value);
                    }
                } else {
                    let _ = db.abort_transaction(tx_id);
                }
                
                thread::sleep(Duration::from_micros(50));
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        let _ = handle.join();
    }
    
    // Analyze results
    if let Ok(times) = commit_times.lock() {
        let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
        println!("  Average commit time: {:?}", avg_time);
    }
    
    if let Ok(reads) = read_after_commit.lock() {
        let mut mismatches = 0;
        for (written, read) in reads.iter() {
            if read.as_ref() != Some(written) {
                mismatches += 1;
                println!("  Mismatch: wrote '{}', read '{:?}'", written, read);
            }
        }
        
        if mismatches == 0 {
            println!("  ✅ PASSED: All commits immediately visible");
        } else {
            println!("  ❌ FAILED: {} commits not immediately visible", mismatches);
        }
    }
    
    Ok(())
}

fn test_store_consistency(db: Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    // Test if version store and main store stay in sync
    let key = b"consistency_test";
    
    // Do a series of transactions
    let mut expected_values = Vec::new();
    
    for i in 0..10 {
        let value = format!("value_{}", i);
        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, key, value.as_bytes())?;
        
        if db.commit_transaction(tx_id).is_ok() {
            expected_values.push(value);
        } else {
            db.abort_transaction(tx_id)?;
        }
    }
    
    // Now read multiple times and check consistency
    let mut reads = Vec::new();
    for _ in 0..20 {
        if let Ok(Some(v)) = db.get(key) {
            reads.push(String::from_utf8_lossy(&v).to_string());
        }
        thread::sleep(Duration::from_millis(5));
    }
    
    // All reads should be the same (the last value)
    let all_same = reads.windows(2).all(|w| w[0] == w[1]);
    let last_expected = expected_values.last().unwrap();
    let all_correct = reads.iter().all(|r| r == last_expected);
    
    println!("  Expected final value: {}", last_expected);
    println!("  Reads: {:?}", &reads[..5.min(reads.len())]);
    
    if all_same && all_correct {
        println!("  ✅ PASSED: Consistent reads from stores");
    } else if !all_same {
        println!("  ❌ FAILED: Inconsistent reads detected");
    } else {
        println!("  ❌ FAILED: Wrong value read");
    }
    
    Ok(())
}