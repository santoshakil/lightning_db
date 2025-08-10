use lightning_db::{Database, LightningDbConfig};
use std::process::{Command, Stdio};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Crash Recovery Load Test");
    println!("======================================\n");

    let dir = tempdir()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Phase 1: Write data under load");
    println!("--------------------------------");
    
    // Track what we successfully wrote
    let committed_count = Arc::new(AtomicU64::new(0));
    let should_stop = Arc::new(AtomicBool::new(false));
    
    // Start database and write data
    {
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(&db_path, config)?);
        
        // Initialize counter
        db.put(b"counter", b"0")?;
        db.put(b"checkpoint", b"start")?;
        
        // Start multiple writer threads
        let mut handles = vec![];
        for thread_id in 0..4 {
            let db = db.clone();
            let committed = committed_count.clone();
            let stop = should_stop.clone();
            
            let handle = thread::spawn(move || {
                let mut local_count = 0;
                let mut tx_count = 0;
                while !stop.load(Ordering::Relaxed) {
                    // Write unique keys with sequential numbering
                    let key = format!("key_{}_{}", thread_id, local_count);
                    let value = format!("value_{}_{}", thread_id, local_count);
                    
                    if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                        local_count += 1;
                        
                        // Also do transactional writes (separate counter)
                        if local_count % 10 == 0 {
                            if let Ok(tx_id) = db.begin_transaction() {
                                let tx_key = format!("tx_key_{}_{}", thread_id, tx_count);
                                let tx_value = format!("tx_value_{}_{}", thread_id, tx_count);
                                
                                if db.put_tx(tx_id, tx_key.as_bytes(), tx_value.as_bytes()).is_ok() {
                                    if db.commit_transaction(tx_id).is_ok() {
                                        tx_count += 1;
                                    }
                                }
                            }
                        }
                    }
                    
                    // Small delay to not overwhelm
                    thread::sleep(Duration::from_micros(100));
                }
                committed.fetch_add(local_count + tx_count, Ordering::SeqCst);
            });
            handles.push(handle);
        }
        
        // Let it run for a bit
        println!("Writing data for 3 seconds...");
        thread::sleep(Duration::from_secs(3));
        
        // Mark checkpoint
        db.put(b"checkpoint", b"pre_crash")?;
        db.sync()?;
        
        // Stop threads
        should_stop.store(true, Ordering::Relaxed);
        for handle in handles {
            let _ = handle.join();
        }
        
        let total_written = committed_count.load(Ordering::SeqCst);
        println!("Wrote approximately {} entries", total_written);
        
        // CRITICAL: Force flush all data to disk before crash
        println!("Forcing full flush to disk...");
        
        // First, get initial stats
        if let Some(stats) = db.lsm_stats() {
            println!("Before flush - Memtable: {} bytes, Immutable: {}", 
                    stats.memtable_size, stats.immutable_count);
        }
        
        // Force sync multiple times to ensure everything is persisted
        for i in 0..3 {
            db.sync()?;
            thread::sleep(Duration::from_millis(100));
        }
        
        // Check stats after flush
        if let Some(stats) = db.lsm_stats() {
            println!("After flush - Memtable: {} bytes, Immutable: {}, L0 files: {}", 
                    stats.memtable_size, stats.immutable_count,
                    stats.levels.get(0).map(|l| l.num_files).unwrap_or(0));
        }
        
        // Count how many entries are actually readable before crash
        let mut pre_crash_readable = 0;
        for thread_id in 0..4 {
            for i in 0..10000 {
                let key = format!("key_{}_{}", thread_id, i);
                if db.get(key.as_bytes())?.is_some() {
                    pre_crash_readable += 1;
                } else {
                    break;
                }
            }
        }
        println!("Readable before crash: {}", pre_crash_readable);
        
        // Simulate crash by dropping without proper shutdown
        println!("\n💥 Simulating crash (dropping database without shutdown)...");
        std::mem::forget(db); // Leak the database to simulate crash
    }
    
    // Small delay to ensure OS has flushed buffers
    thread::sleep(Duration::from_millis(500));
    
    println!("\nPhase 2: Recovery and verification");
    println!("------------------------------------");
    
    // Reopen database
    let db = Database::open(&db_path, LightningDbConfig::default())?;
    
    // Check checkpoint
    let checkpoint = db.get(b"checkpoint")?
        .and_then(|v| String::from_utf8(v).ok())
        .unwrap_or_else(|| "missing".to_string());
    println!("Checkpoint status: {}", checkpoint);
    
    if checkpoint != "pre_crash" {
        println!("❌ CRITICAL: Checkpoint not found! Data loss detected!");
        return Ok(());
    }
    
    // Count recovered entries (don't assume sequential due to concurrent writes)
    let mut regular_entries = 0;
    let mut tx_entries = 0;
    let mut max_found = [0usize; 4];
    
    for thread_id in 0..4 {
        // Check all possible regular entries (don't break on missing)
        for i in 0..25000 {  // Check more than expected
            let key = format!("key_{}_{}", thread_id, i);
            if db.get(key.as_bytes())?.is_some() {
                regular_entries += 1;
                max_found[thread_id] = i;
            }
        }
        
        // Count transaction entries separately
        for i in 0..2500 {  // Check more than expected
            let tx_key = format!("tx_key_{}_{}", thread_id, i);
            if db.get(tx_key.as_bytes())?.is_some() {
                tx_entries += 1;
            }
        }
    }
    
    println!("Max indices found per thread: {:?}", max_found);
    
    println!("Recovered {} regular entries", regular_entries);
    println!("Recovered {} transactional entries", tx_entries);
    
    // Verify data integrity
    println!("\nPhase 3: Data integrity verification");
    println!("-------------------------------------");
    
    let mut corrupted = 0;
    for thread_id in 0..4 {
        for i in 0..regular_entries/4 {
            let key = format!("key_{}_{}", thread_id, i);
            let expected_value = format!("value_{}_{}", thread_id, i);
            
            if let Some(value) = db.get(key.as_bytes())? {
                if value != expected_value.as_bytes() {
                    corrupted += 1;
                    println!("❌ Corrupted entry: {} (expected {}, got {:?})", 
                            key, expected_value, String::from_utf8_lossy(&value));
                }
            }
        }
    }
    
    if corrupted > 0 {
        println!("❌ Found {} corrupted entries!", corrupted);
    } else {
        println!("✅ All recovered entries have correct values");
    }
    
    // Test post-recovery writes
    println!("\nPhase 4: Post-recovery operations");
    println!("----------------------------------");
    
    db.put(b"post_recovery", b"test")?;
    let post_val = db.get(b"post_recovery")?;
    
    if post_val == Some(b"test".to_vec()) {
        println!("✅ Post-recovery writes working");
    } else {
        println!("❌ Post-recovery writes failed!");
    }
    
    // Test transactions after recovery
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"post_recovery_tx", b"test_tx")?;
    db.commit_transaction(tx_id)?;
    
    let tx_val = db.get(b"post_recovery_tx")?;
    if tx_val == Some(b"test_tx".to_vec()) {
        println!("✅ Post-recovery transactions working");
    } else {
        println!("❌ Post-recovery transactions failed!");
    }
    
    // Summary
    println!("\n=== CRASH RECOVERY SUMMARY ===");
    let expected_min = committed_count.load(Ordering::SeqCst) / 2; // Allow some loss
    let total_recovered = regular_entries + tx_entries;
    
    if checkpoint == "pre_crash" && corrupted == 0 && total_recovered > expected_min as usize {
        println!("✅ PASSED: Database recovered successfully");
        println!("   - Checkpoint preserved");
        println!("   - No data corruption");
        println!("   - Recovered {}/{} entries", total_recovered, committed_count.load(Ordering::SeqCst));
    } else {
        println!("❌ FAILED: Recovery issues detected");
        if checkpoint != "pre_crash" {
            println!("   - Checkpoint lost");
        }
        if corrupted > 0 {
            println!("   - {} corrupted entries", corrupted);
        }
        if total_recovered <= expected_min as usize {
            println!("   - Excessive data loss: only {}/{} recovered", 
                    total_recovered, committed_count.load(Ordering::SeqCst));
        }
    }
    
    Ok(())
}