use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Production workload test to verify memory leak fixes
fn main() {
    println!("🧪 Memory Leak Production Workload Test");
    println!("Testing version cleanup under realistic production load");
    println!("{}", "=".repeat(60));

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    // Configure for production-like workload
    let mut config = LightningDbConfig::default();
    config.cache_size = 50 * 1024 * 1024; // 50MB cache
    config.compression_enabled = true;
    
    let db = Arc::new(Database::create(db_path, config).unwrap());
    
    println!("📊 Starting production workload simulation...");
    
    // Simulate production workload with high MVCC version turnover
    let duration = Duration::from_secs(60); // 1 minute test
    let start_time = Instant::now();
    let mut iteration = 0;
    
    let mut handles = Vec::new();
    
    // Spawn multiple worker threads for concurrent load
    for worker_id in 0..4 {
        let db_clone = db.clone();
        let worker_start = start_time;
        let worker_duration = duration;
        
        let handle = thread::spawn(move || {
            let mut local_ops = 0;
            let mut local_tx_ops = 0;
            
            while worker_start.elapsed() < worker_duration {
                // Mix of regular operations and transactions
                if local_ops % 10 == 0 {
                    // Transaction workload (creates many MVCC versions)
                    if let Ok(tx_id) = db_clone.begin_transaction() {
                        for i in 0..5 {
                            let key = format!("worker_{}_tx_key_{}", worker_id, i);
                            let value = format!("worker_{}_tx_value_{}_{}", worker_id, i, local_tx_ops);
                            let _ = db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes());
                        }
                        
                        // Commit or abort randomly to create version churn
                        if local_tx_ops % 3 == 0 {
                            let _ = db_clone.commit_transaction(tx_id);
                        } else {
                            let _ = db_clone.abort_transaction(tx_id);
                        }
                        local_tx_ops += 1;
                    }
                } else {
                    // Regular operations
                    let key = format!("worker_{}_key_{}", worker_id, local_ops);
                    let value = format!("worker_{}_value_{}_{}", worker_id, local_ops, 
                                      std::time::SystemTime::now()
                                          .duration_since(std::time::UNIX_EPOCH)
                                          .unwrap()
                                          .as_millis());
                    
                    let _ = db_clone.put(key.as_bytes(), value.as_bytes());
                    
                    // Mix in some reads and deletes
                    if local_ops % 5 == 0 {
                        let _ = db_clone.get(key.as_bytes());
                    }
                    if local_ops % 20 == 0 {
                        let _ = db_clone.delete(key.as_bytes());
                    }
                }
                
                local_ops += 1;
                
                // Brief pause to simulate realistic timing
                thread::sleep(Duration::from_millis(1));
            }
            
            (local_ops, local_tx_ops)
        });
        
        handles.push(handle);
    }
    
    // Monitor memory usage during the test
    let monitor_handle = {
        let monitor_start = start_time;
        let monitor_duration = duration;
        
        thread::spawn(move || {
            let mut samples = Vec::new();
            
            while monitor_start.elapsed() < monitor_duration {
                // Get memory info (approximation using process info)
                if let Ok(output) = std::process::Command::new("ps")
                    .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
                    .output()
                {
                    if let Ok(memory_str) = String::from_utf8(output.stdout) {
                        if let Ok(memory_kb) = memory_str.trim().parse::<u64>() {
                            samples.push((monitor_start.elapsed(), memory_kb));
                        }
                    }
                }
                
                thread::sleep(Duration::from_secs(5));
            }
            
            samples
        })
    };
    
    // Wait for all workers to complete
    let mut total_ops = 0;
    let mut total_tx_ops = 0;
    
    for handle in handles {
        let (ops, tx_ops) = handle.join().unwrap();
        total_ops += ops;
        total_tx_ops += tx_ops;
    }
    
    let memory_samples = monitor_handle.join().unwrap();
    
    // Force cleanup and checkpoint
    println!("🔄 Forcing cleanup and checkpoint...");
    let _ = db.checkpoint();
    thread::sleep(Duration::from_secs(2)); // Allow cleanup to run
    
    // Analyze results
    println!("\n📈 WORKLOAD RESULTS:");
    println!("  Duration: {:?}", duration);
    println!("  Total Operations: {}", total_ops);
    println!("  Total Transactions: {}", total_tx_ops);
    println!("  Ops/sec: {:.0}", total_ops as f64 / duration.as_secs_f64());
    
    if memory_samples.len() >= 2 {
        let initial_memory = memory_samples[0].1;
        let final_memory = memory_samples.last().unwrap().1;
        let peak_memory = memory_samples.iter().map(|(_, mem)| *mem).max().unwrap();
        
        println!("\n💾 MEMORY ANALYSIS:");
        println!("  Initial Memory: {} KB ({:.1} MB)", initial_memory, initial_memory as f64 / 1024.0);
        println!("  Final Memory: {} KB ({:.1} MB)", final_memory, final_memory as f64 / 1024.0);
        println!("  Peak Memory: {} KB ({:.1} MB)", peak_memory, peak_memory as f64 / 1024.0);
        
        let memory_growth = final_memory as i64 - initial_memory as i64;
        let growth_rate = memory_growth as f64 / duration.as_secs_f64() / 1024.0; // MB/sec
        
        println!("  Memory Growth: {} KB ({:.1} MB)", memory_growth, memory_growth as f64 / 1024.0);
        println!("  Growth Rate: {:.3} MB/sec", growth_rate);
        
        // Memory leak assessment
        let acceptable_growth = 10.0; // 10 MB total growth is acceptable
        let acceptable_rate = 0.1; // 0.1 MB/sec growth rate is acceptable
        
        let growth_mb = memory_growth as f64 / 1024.0;
        
        println!("\n🔍 LEAK ASSESSMENT:");
        if growth_mb <= acceptable_growth && growth_rate <= acceptable_rate {
            println!("  ✅ EXCELLENT - No significant memory leak detected");
            println!("  Memory growth is within acceptable bounds");
        } else if growth_mb <= acceptable_growth * 2.0 && growth_rate <= acceptable_rate * 2.0 {
            println!("  ⚠️  ACCEPTABLE - Minor memory growth observed");
            println!("  Growth is elevated but not critical");
        } else {
            println!("  ❌ MEMORY LEAK - Excessive memory growth detected!");
            println!("  Growth exceeds acceptable thresholds");
        }
        
        // Show memory timeline
        println!("\n📊 MEMORY TIMELINE:");
        for (time, memory) in &memory_samples {
            println!("  {:3.0}s: {} KB ({:.1} MB)", 
                     time.as_secs_f64(), memory, *memory as f64 / 1024.0);
        }
    }
    
    println!("\n🧪 FUNCTIONAL VERIFICATION:");
    
    // Verify database is still functional after intense workload
    let test_key = b"final_test_key";
    let test_value = b"final_test_value";
    
    match db.put(test_key, test_value) {
        Ok(_) => {
            match db.get(test_key) {
                Ok(Some(value)) if value == test_value => {
                    println!("  ✅ Database remains fully functional");
                }
                _ => {
                    println!("  ❌ Database read functionality compromised");
                }
            }
        }
        Err(e) => {
            println!("  ❌ Database write functionality compromised: {}", e);
        }
    }
    
    // Test transaction functionality
    match db.begin_transaction() {
        Ok(tx_id) => {
            let tx_result = db.put_tx(tx_id, b"tx_test", b"tx_value")
                .and_then(|_| db.commit_transaction(tx_id));
            
            match tx_result {
                Ok(_) => println!("  ✅ Transaction functionality intact"),
                Err(e) => println!("  ❌ Transaction functionality compromised: {}", e),
            }
        }
        Err(e) => {
            println!("  ❌ Transaction creation failed: {}", e);
        }
    }
    
    println!("\n{}", "=".repeat(60));
    println!("Memory leak production test completed!");
}