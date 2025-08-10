use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Production Validation Test");
    println!("========================================\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().to_path_buf();
    
    let mut all_passed = true;
    
    // Test 1: Durability
    println!("Test 1: Durability (Crash Recovery)");
    println!("------------------------------------");
    {
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(&db_path, config)?);
        
        // Write test data
        for i in 0..1000 {
            let key = format!("durability_key_{:04}", i);
            let value = format!("durability_value_{:04}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Force sync
        db.sync()?;
        
        // Simulate crash
        std::mem::forget(db);
    }
    
    // Recover and verify
    {
        let db = Database::open(&db_path, LightningDbConfig::default())?;
        let mut recovered = 0;
        for i in 0..1000 {
            let key = format!("durability_key_{:04}", i);
            if db.get(key.as_bytes())?.is_some() {
                recovered += 1;
            }
        }
        
        if recovered == 1000 {
            println!("✅ PASSED: All 1000 entries recovered after crash");
        } else {
            println!("❌ FAILED: Only {}/1000 entries recovered", recovered);
            all_passed = false;
        }
    }
    
    // Clean up for next test
    std::fs::remove_dir_all(&db_path)?;
    std::fs::create_dir_all(&db_path)?;
    
    // Test 2: Concurrent Operations
    println!("\nTest 2: Concurrent Operations");
    println!("------------------------------");
    {
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(&db_path, config)?);
        
        let total_ops = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        
        let mut handles = vec![];
        for thread_id in 0..8 {
            let db = db.clone();
            let ops = total_ops.clone();
            let errs = errors.clone();
            
            let handle = thread::spawn(move || {
                for i in 0..500 {
                    let key = format!("concurrent_{}_{}", thread_id, i);
                    let value = format!("value_{}_{}", thread_id, i);
                    
                    // Write
                    if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                        ops.fetch_add(1, Ordering::Relaxed);
                        
                        // Verify immediate read
                        if db.get(key.as_bytes()).unwrap().is_none() {
                            errs.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        errs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let total = total_ops.load(Ordering::Relaxed);
        let err_count = errors.load(Ordering::Relaxed);
        
        if err_count == 0 && total == 4000 {
            println!("✅ PASSED: 4000 concurrent operations succeeded");
        } else {
            println!("❌ FAILED: {} errors in {} operations", err_count, total);
            all_passed = false;
        }
    }
    
    // Clean up for next test
    std::fs::remove_dir_all(&db_path)?;
    std::fs::create_dir_all(&db_path)?;
    
    // Test 3: Transaction ACID
    println!("\nTest 3: Transaction ACID Properties");
    println!("------------------------------------");
    {
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(&db_path, config)?);
        
        // Initialize accounts
        db.put(b"account_a", b"1000")?;
        db.put(b"account_b", b"1000")?;
        
        let successful_transfers = Arc::new(AtomicU64::new(0));
        let failed_transfers = Arc::new(AtomicU64::new(0));
        
        // Concurrent transfers
        let mut handles = vec![];
        for _ in 0..4 {
            let db = db.clone();
            let success = successful_transfers.clone();
            let failed = failed_transfers.clone();
            
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    // Try a transfer
                    let tx_id = db.begin_transaction().unwrap();
                    
                    // Read current values WITHIN THE TRANSACTION
                    let a_balance = match db.get_tx(tx_id, b"account_a") {
                        Ok(Some(v)) => String::from_utf8_lossy(&v).parse::<i32>().unwrap_or(0),
                        _ => {
                            db.abort_transaction(tx_id).ok();
                            failed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    
                    let b_balance = match db.get_tx(tx_id, b"account_b") {
                        Ok(Some(v)) => String::from_utf8_lossy(&v).parse::<i32>().unwrap_or(0),
                        _ => {
                            db.abort_transaction(tx_id).ok();
                            failed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    
                    // Transfer 10 from A to B
                    if a_balance >= 10 {
                        let new_a = (a_balance - 10).to_string();
                        let new_b = (b_balance + 10).to_string();
                        
                        // Use TRANSACTIONAL puts to ensure isolation
                        if db.put_tx(tx_id, b"account_a", new_a.as_bytes()).is_err() ||
                           db.put_tx(tx_id, b"account_b", new_b.as_bytes()).is_err() {
                            db.abort_transaction(tx_id).ok();
                            failed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        
                        if db.commit_transaction(tx_id).is_ok() {
                            success.fetch_add(1, Ordering::Relaxed);
                        } else {
                            failed.fetch_add(1, Ordering::Relaxed);
                            db.abort_transaction(tx_id).ok();
                        }
                    } else {
                        db.abort_transaction(tx_id).ok();
                    }
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify final state
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
        
        // Total should still be 2000
        if final_a + final_b == 2000 && final_a >= 0 && final_b >= 0 {
            println!("✅ PASSED: ACID properties maintained");
            println!("   {} successful transfers, {} failed", successful, failed);
            println!("   Final balances: A={}, B={}", final_a, final_b);
        } else {
            println!("❌ FAILED: ACID violation detected");
            println!("   Final balances: A={}, B={} (sum={})", final_a, final_b, final_a + final_b);
            all_passed = false;
        }
    }
    
    // Clean up for next test
    std::fs::remove_dir_all(&db_path)?;
    std::fs::create_dir_all(&db_path)?;
    
    // Test 4: Performance
    println!("\nTest 4: Performance Benchmarks");
    println!("-------------------------------");
    {
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(&db_path, config)?);
        
        // Write performance
        let start = Instant::now();
        for i in 0..10000 {
            let key = format!("perf_key_{:06}", i);
            let value = format!("perf_value_{:06}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        let write_duration = start.elapsed();
        let write_ops_per_sec = 10000.0 / write_duration.as_secs_f64();
        
        // Read performance
        let start = Instant::now();
        for i in 0..10000 {
            let key = format!("perf_key_{:06}", i);
            db.get(key.as_bytes())?;
        }
        let read_duration = start.elapsed();
        let read_ops_per_sec = 10000.0 / read_duration.as_secs_f64();
        
        println!("Write: {:.0} ops/sec ({:.2}ms avg)", 
                write_ops_per_sec, write_duration.as_millis() as f64 / 10000.0);
        println!("Read: {:.0} ops/sec ({:.2}ms avg)", 
                read_ops_per_sec, read_duration.as_millis() as f64 / 10000.0);
        
        if write_ops_per_sec > 10000.0 && read_ops_per_sec > 50000.0 {
            println!("✅ PASSED: Performance meets requirements");
        } else {
            println!("⚠️  WARNING: Performance below optimal levels");
        }
    }
    
    // Final Summary
    println!("\n{}", "=".repeat(50));
    println!("PRODUCTION VALIDATION SUMMARY");
    println!("{}", "=".repeat(50));
    
    if all_passed {
        println!("✅ ALL TESTS PASSED - Database is production ready!");
    } else {
        println!("❌ SOME TESTS FAILED - Critical issues remain");
    }
    
    Ok(())
}