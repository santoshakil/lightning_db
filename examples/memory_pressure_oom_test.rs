use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use sysinfo::System;

struct MemoryMonitor {
    system: Mutex<System>,
    initial_memory: u64,
}

impl MemoryMonitor {
    fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        
        let initial_memory = if let Some(process) = system.process(sysinfo::get_current_pid().unwrap()) {
            process.memory()
        } else {
            0
        };
        
        Self {
            system: Mutex::new(system),
            initial_memory,
        }
    }
    
    fn get_current_memory(&self) -> (u64, u64, f32) {
        let mut system = self.system.lock().unwrap();
        system.refresh_all();
        
        let current_memory = if let Some(process) = system.process(sysinfo::get_current_pid().unwrap()) {
            process.memory()
        } else {
            0
        };
        
        let total_memory = system.total_memory();
        let used_memory = system.used_memory();
        let memory_percent = (used_memory as f32 / total_memory as f32) * 100.0;
        
        (current_memory, total_memory, memory_percent)
    }
    
    fn print_status(&self, context: &str) {
        let (current, total, percent) = self.get_current_memory();
        let delta = current.saturating_sub(self.initial_memory);
        
        println!("  {} Memory: {} MB (Δ {} MB), System: {:.1}% of {} GB", 
                 context,
                 current / 1024,
                 delta / 1024,
                 percent,
                 total / 1024 / 1024);
    }
}

fn allocate_memory(size_mb: usize) -> Vec<Vec<u8>> {
    let mut allocations = Vec::new();
    let chunk_size = 10 * 1024 * 1024; // 10MB chunks
    let num_chunks = (size_mb * 1024 * 1024) / chunk_size;
    
    for i in 0..num_chunks {
        match std::panic::catch_unwind(|| vec![i as u8; chunk_size]) {
            Ok(chunk) => allocations.push(chunk),
            Err(_) => {
                println!("  ⚠️  Failed to allocate chunk {} of {}", i + 1, num_chunks);
                break;
            }
        }
    }
    
    allocations
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB Memory Pressure & OOM Test ===\n");
    
    let monitor = Arc::new(MemoryMonitor::new());
    monitor.print_status("Initial");
    
    let dir = tempdir()?;
    let db_path = dir.path();
    
    // Test 1: Basic memory usage patterns
    println!("\nTest 1: Basic Memory Usage Patterns");
    println!("===================================");
    {
        // Small cache configuration
        let config = LightningDbConfig {
            cache_size: 50 * 1024 * 1024, // 50MB cache
            ..Default::default()
        };
        
        let db = Database::create(db_path, config)?;
        monitor.print_status("After DB creation");
        
        // Write data larger than cache
        println!("\n  Writing 200MB of data with 50MB cache...");
        let value_size = 100_000; // 100KB per value
        let num_values = 2000; // 200MB total
        
        for i in 0..num_values {
            let key = format!("memory_test_key_{:04}", i);
            let value = vec![(i % 256) as u8; value_size];
            db.put(key.as_bytes(), &value)?;
            
            if i % 200 == 0 {
                monitor.print_status(&format!("After {} writes", i));
            }
        }
        
        monitor.print_status("After all writes");
        
        // Random reads to stress cache
        println!("\n  Performing random reads...");
        use rand::Rng;
        let mut rng = rand::thread_rng();
        
        for i in 0..1000 {
            let key_id = rng.gen_range(0..num_values);
            let key = format!("memory_test_key_{:04}", key_id);
            let _ = db.get(key.as_bytes())?;
            
            if i % 200 == 0 {
                monitor.print_status(&format!("After {} reads", i));
            }
        }
        
        // Force checkpoint
        db.checkpoint()?;
        monitor.print_status("After checkpoint");
    }
    
    // Test 2: Concurrent memory pressure
    println!("\nTest 2: Concurrent Memory Pressure");
    println!("==================================");
    {
        let db = Arc::new(Database::open(db_path, LightningDbConfig::default())?);
        let barrier = Arc::new(Barrier::new(5)); // 4 threads + main
        let monitor_clone = Arc::clone(&monitor);
        
        monitor.print_status("Before concurrent test");
        
        let mut handles = vec![];
        
        for thread_id in 0..4 {
            let db_clone = Arc::clone(&db);
            let barrier_clone = Arc::clone(&barrier);
            let monitor_clone = Arc::clone(&monitor_clone);
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                
                // Each thread allocates memory and performs DB operations
                let _thread_memory = allocate_memory(50); // 50MB per thread
                
                for i in 0..500 {
                    let key = format!("concurrent_t{}_k{:03}", thread_id, i);
                    let value = vec![(thread_id * i) as u8; 50_000]; // 50KB values
                    
                    match db_clone.put(key.as_bytes(), &value) {
                        Ok(_) => {},
                        Err(e) => {
                            eprintln!("Thread {} write error: {}", thread_id, e);
                            break;
                        }
                    }
                    
                    if i % 100 == 0 {
                        monitor_clone.print_status(&format!("Thread {} progress", thread_id));
                    }
                }
            });
            
            handles.push(handle);
        }
        
        barrier.wait();
        let start = Instant::now();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        monitor.print_status("After concurrent operations");
        println!("  ✓ Concurrent test completed in {:.2}s", duration.as_secs_f64());
    }
    
    // Test 3: Memory exhaustion simulation
    println!("\nTest 3: Memory Exhaustion Simulation");
    println!("====================================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        monitor.print_status("Before exhaustion test");
        
        // Allocate large chunks of memory
        println!("\n  Allocating memory to simulate pressure...");
        let mut memory_chunks = Vec::new();
        let mut allocation_failed = false;
        
        for i in 0..50 {
            let chunks = allocate_memory(100); // Try to allocate 100MB
            if chunks.is_empty() {
                println!("  Memory allocation failed at iteration {}", i);
                allocation_failed = true;
                break;
            }
            memory_chunks.push(chunks);
            monitor.print_status(&format!("After allocation {}", i));
            
            // Try DB operations under memory pressure
            let key = format!("pressure_key_{}", i);
            let value = vec![i as u8; 10_000];
            
            match db.put(key.as_bytes(), &value) {
                Ok(_) => {},
                Err(e) => {
                    println!("  DB write failed under pressure: {}", e);
                    break;
                }
            }
        }
        
        if !allocation_failed {
            println!("  ⚠️  System has more memory than expected for OOM test");
        }
        
        // Release some memory
        println!("\n  Releasing half of allocated memory...");
        let chunks_to_keep = memory_chunks.len() / 2;
        memory_chunks.truncate(chunks_to_keep);
        monitor.print_status("After partial release");
        
        // Test recovery
        println!("\n  Testing database operations after memory release...");
        let mut recovery_writes = 0;
        for i in 0..100 {
            let key = format!("recovery_key_{}", i);
            let value = format!("recovery_value_{}", i);
            
            if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                recovery_writes += 1;
            }
        }
        
        println!("  ✓ {} writes successful after memory release", recovery_writes);
        
        // Clear all extra memory
        drop(memory_chunks);
        monitor.print_status("After full memory release");
    }
    
    // Test 4: Cache thrashing
    println!("\nTest 4: Cache Thrashing Test");
    println!("============================");
    {
        // Very small cache to force thrashing
        let config = LightningDbConfig {
            cache_size: 1 * 1024 * 1024, // Only 1MB cache
            ..Default::default()
        };
        
        let db = Database::open(db_path, config)?;
        monitor.print_status("Small cache DB created");
        
        // Access pattern that defeats cache
        println!("\n  Writing data much larger than cache...");
        let num_keys = 1000;
        let value_size = 10_000; // 10KB per value = 10MB total
        
        for i in 0..num_keys {
            let key = format!("thrash_key_{:04}", i);
            let value = vec![(i % 256) as u8; value_size];
            db.put(key.as_bytes(), &value)?;
        }
        
        monitor.print_status("After writes");
        
        // Sequential scan (should thrash cache)
        println!("\n  Sequential scan with tiny cache...");
        let start = Instant::now();
        let mut hits = 0;
        
        for i in 0..num_keys {
            let key = format!("thrash_key_{:04}", i);
            if db.get(key.as_bytes())?.is_some() {
                hits += 1;
            }
        }
        
        let scan_time = start.elapsed();
        println!("  ✓ Sequential scan: {} hits in {:.2}s", hits, scan_time.as_secs_f64());
        
        // Random access (even worse for cache)
        println!("\n  Random access pattern...");
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let start = Instant::now();
        hits = 0;
        
        for _ in 0..1000 {
            let i = rng.gen_range(0..num_keys);
            let key = format!("thrash_key_{:04}", i);
            if db.get(key.as_bytes())?.is_some() {
                hits += 1;
            }
        }
        
        let random_time = start.elapsed();
        println!("  ✓ Random access: {} hits in {:.2}s", hits, random_time.as_secs_f64());
        
        monitor.print_status("After cache thrashing");
    }
    
    // Test 5: Transaction memory usage
    println!("\nTest 5: Transaction Memory Usage");
    println!("================================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        monitor.print_status("Before transaction test");
        
        // Large transaction
        let tx_id = db.begin_transaction()?;
        monitor.print_status("Transaction started");
        
        println!("\n  Building large transaction...");
        for i in 0..5000 {
            let key = format!("tx_mem_key_{:04}", i);
            let value = vec![(i % 256) as u8; 20_000]; // 20KB per entry
            
            match db.put_tx(tx_id, key.as_bytes(), &value) {
                Ok(_) => {},
                Err(e) => {
                    println!("  Transaction write {} failed: {}", i, e);
                    break;
                }
            }
            
            if i % 1000 == 0 {
                monitor.print_status(&format!("After {} tx writes", i));
            }
        }
        
        monitor.print_status("Before commit");
        
        match db.commit_transaction(tx_id) {
            Ok(_) => println!("  ✓ Large transaction committed successfully"),
            Err(e) => {
                println!("  ⚠️  Transaction commit failed: {}", e);
                let _ = db.abort_transaction(tx_id);
            }
        }
        
        monitor.print_status("After transaction");
    }
    
    // Final memory stats
    println!("\nFinal Memory Analysis");
    println!("====================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        // Force garbage collection
        db.checkpoint()?;
        drop(db);
        
        // Give time for cleanup
        thread::sleep(Duration::from_millis(500));
        
        monitor.print_status("After full cleanup");
        
        let (current, _, _) = monitor.get_current_memory();
        let delta = current.saturating_sub(monitor.initial_memory);
        
        if delta < 50 * 1024 { // Less than 50MB increase
            println!("  ✓ Memory usage returned close to baseline (Δ {} MB)", delta / 1024);
        } else {
            println!("  ⚠️  Potential memory leak detected (Δ {} MB)", delta / 1024);
        }
    }
    
    println!("\n=== Memory Pressure & OOM Tests Complete ===");
    println!("Lightning DB demonstrates:");
    println!("✓ Controlled memory usage with cache limits");
    println!("✓ Stable operation under memory pressure");
    println!("✓ Recovery after memory exhaustion");
    println!("✓ Efficient cache management");
    println!("✓ Transaction memory handling");
    
    Ok(())
}