use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Concurrent Write Debug Test");
    println!("=========================================\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Phase 1: Test concurrent writes");
    println!("--------------------------------");
    
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(&db_path, config)?);
    
    let total_written = Arc::new(AtomicU64::new(0));
    let should_stop = Arc::new(AtomicBool::new(false));
    
    // Start 4 writer threads
    let mut handles = vec![];
    for thread_id in 0..4 {
        let db = db.clone();
        let written = total_written.clone();
        let stop = should_stop.clone();
        
        let handle = thread::spawn(move || {
            let mut count = 0;
            for i in 0..100 {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                
                let key = format!("key_{}_{:04}", thread_id, i);
                let value = format!("value_{}_{:04}", thread_id, i);
                
                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        count += 1;
                        // Immediately verify the write
                        if db.get(key.as_bytes()).unwrap().is_none() {
                            eprintln!("ERROR: Just wrote {} but can't read it back!", key);
                        }
                    }
                    Err(e) => {
                        eprintln!("ERROR: Failed to write {}: {}", key, e);
                    }
                }
            }
            written.fetch_add(count, Ordering::SeqCst);
            println!("Thread {} wrote {} entries", thread_id, count);
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let total = total_written.load(Ordering::SeqCst);
    println!("\nTotal written: {} entries", total);
    
    // Check how many are readable
    println!("\nPhase 2: Verify all writes are readable");
    println!("----------------------------------------");
    
    let mut readable = 0;
    let mut missing = Vec::new();
    
    for thread_id in 0..4 {
        for i in 0..100 {
            let key = format!("key_{}_{:04}", thread_id, i);
            if db.get(key.as_bytes())?.is_some() {
                readable += 1;
            } else {
                missing.push(key);
            }
        }
    }
    
    println!("Readable: {}/{} entries", readable, total);
    
    if !missing.is_empty() {
        println!("\nFirst 10 missing keys:");
        for key in missing.iter().take(10) {
            println!("  - {}", key);
        }
    }
    
    // Force sync and check again
    println!("\nPhase 3: Sync and re-check");
    println!("---------------------------");
    
    db.sync()?;
    
    let mut readable_after_sync = 0;
    for thread_id in 0..4 {
        for i in 0..100 {
            let key = format!("key_{}_{:04}", thread_id, i);
            if db.get(key.as_bytes())?.is_some() {
                readable_after_sync += 1;
            }
        }
    }
    
    println!("Readable after sync: {}/{} entries", readable_after_sync, total);
    
    // Check LSM stats
    if let Some(stats) = db.lsm_stats() {
        println!("\nLSM Stats:");
        println!("  Memtable size: {} bytes", stats.memtable_size);
        println!("  Immutable count: {}", stats.immutable_count);
        println!("  Level 0 files: {}", stats.levels.get(0).map(|l| l.num_files).unwrap_or(0));
    }
    
    // Summary
    println!("\n=== SUMMARY ===");
    if readable == total {
        println!("✅ SUCCESS: All concurrent writes are immediately readable");
    } else {
        println!("❌ FAILURE: {} writes are not readable", total - readable);
        println!("This explains the data loss - writes aren't making it to the LSM tree");
    }
    
    Ok(())
}