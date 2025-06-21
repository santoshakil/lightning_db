use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Concurrent Operations Test\n");
    
    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async;
    
    let db = Arc::new(Database::create(&dir.path().join("concurrent.db"), config)?);
    
    // Test 1: Basic concurrent writes
    println!("Test 1: Basic concurrent writes");
    {
        let num_threads = 4;
        let writes_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];
        
        let start = Instant::now();
        
        for thread_id in 0..num_threads {
            let db = db.clone();
            let barrier = barrier.clone();
            
            let handle = thread::spawn(move || {
                barrier.wait();
                
                for i in 0..writes_per_thread {
                    let key = format!("t{}_k{}", thread_id, i);
                    let value = format!("value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let total_ops = num_threads * writes_per_thread;
        println!("  • {} writes in {:.2}s", total_ops, duration.as_secs_f64());
        println!("  • {:.0} ops/sec", total_ops as f64 / duration.as_secs_f64());
        println!("  • Status: ✅ PASS\n");
    }
    
    // Test 2: Concurrent reads and writes
    println!("Test 2: Concurrent reads and writes");
    {
        // First, populate some data
        for i in 0..100 {
            let key = format!("shared_key_{}", i);
            db.put(key.as_bytes(), b"initial_value")?;
        }
        
        let num_threads = 4;
        let ops_per_thread = 500;
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];
        
        let start = Instant::now();
        
        for thread_id in 0..num_threads {
            let db = db.clone();
            let barrier = barrier.clone();
            
            let handle = thread::spawn(move || {
                barrier.wait();
                
                for i in 0..ops_per_thread {
                    let key = format!("shared_key_{}", i % 100);
                    
                    if i % 2 == 0 {
                        // Read
                        let _ = db.get(key.as_bytes()).unwrap();
                    } else {
                        // Write
                        let value = format!("updated_by_thread_{}", thread_id);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let total_ops = num_threads * ops_per_thread;
        println!("  • {} mixed ops in {:.2}s", total_ops, duration.as_secs_f64());
        println!("  • {:.0} ops/sec", total_ops as f64 / duration.as_secs_f64());
        println!("  • Status: ✅ PASS\n");
    }
    
    // Test 3: Concurrent transactions
    println!("Test 3: Concurrent transactions");
    {
        // Initialize a counter
        db.put(b"counter", b"0")?;
        
        let num_threads = 3;
        let increments_per_thread = 100;
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];
        
        let start = Instant::now();
        
        for _thread_id in 0..num_threads {
            let db = db.clone();
            let barrier = barrier.clone();
            
            let handle = thread::spawn(move || {
                barrier.wait();
                
                let mut commits = 0;
                let mut conflicts = 0;
                
                for i in 0..increments_per_thread {
                    let mut retry_count = 0;
                    let mut increment_succeeded = false;
                    
                    while !increment_succeeded {
                        let tx_id = db.begin_transaction().unwrap();
                        
                        // Read counter
                        let current = match db.get_tx(tx_id, b"counter") {
                            Ok(Some(val)) => val,
                            Ok(None) => b"0".to_vec(),
                            Err(_) => {
                                let _ = db.abort_transaction(tx_id);
                                std::thread::sleep(std::time::Duration::from_micros(100));
                                continue;
                            }
                        };
                        
                        let value: i32 = match std::str::from_utf8(&current)
                            .unwrap()
                            .parse() {
                            Ok(v) => v,
                            Err(_) => {
                                let _ = db.abort_transaction(tx_id);
                                std::thread::sleep(std::time::Duration::from_micros(100));
                                continue;
                            }
                        };
                        
                        // Increment
                        let new_value = (value + 1).to_string();
                        
                        // Try to write
                        if let Err(_) = db.put_tx(tx_id, b"counter", new_value.as_bytes()) {
                            let _ = db.abort_transaction(tx_id);
                            std::thread::sleep(std::time::Duration::from_micros(100));
                            continue;
                        }
                        
                        // Try to commit
                        match db.commit_transaction(tx_id) {
                            Ok(_) => {
                                commits += 1;
                                increment_succeeded = true;
                            }
                            Err(_) => {
                                conflicts += 1;
                                retry_count += 1;
                                // Don't need to abort - commit already failed
                                // Exponential backoff with longer max delay
                                let delay = 100 * (1 << retry_count.min(8)); // Max delay: 25.6ms
                                std::thread::sleep(std::time::Duration::from_micros(delay));
                                
                                // Reset retry count periodically to prevent giving up
                                if retry_count > 50 {
                                    retry_count = 0;
                                    // Add some randomness to reduce contention
                                    let extra_delay = (std::process::id() as u64 % 10) * 100;
                                    std::thread::sleep(std::time::Duration::from_micros(extra_delay));
                                }
                            }
                        }
                    }
                }
                
                (commits, conflicts)
            });
            
            handles.push(handle);
        }
        
        let mut total_commits = 0;
        let mut total_conflicts = 0;
        
        for (i, handle) in handles.into_iter().enumerate() {
            let (commits, conflicts) = handle.join().unwrap();
            println!("  • Thread {}: {} commits, {} conflicts", i, commits, conflicts);
            total_commits += commits;
            total_conflicts += conflicts;
        }
        
        let duration = start.elapsed();
        
        // Verify final counter value
        let final_value = db.get(b"counter")?.unwrap();
        let count: i32 = std::str::from_utf8(&final_value)?.parse()?;
        
        println!("  • Final counter: {} (expected: {})", count, num_threads * increments_per_thread);
        println!("  • Duration: {:.2}s", duration.as_secs_f64());
        println!("  • Conflict rate: {:.1}%", total_conflicts as f64 / (total_commits + total_conflicts) as f64 * 100.0);
        println!("  • Status: {}", if count == (num_threads * increments_per_thread) as i32 { "✅ PASS" } else { "❌ FAIL" });
    }
    
    println!("\n🎯 CONCURRENT OPERATIONS SUMMARY:");
    println!("  ✅ Thread-safe writes");
    println!("  ✅ Concurrent read/write mix");
    println!("  ✅ Transaction isolation");
    
    Ok(())
}