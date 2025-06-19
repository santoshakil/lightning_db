use lightning_db::{Database, LightningDbConfig};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    println!("🏋️ Lightning DB Stress Test");
    println!("===========================\n");

    let db_path = "stress_test_db";
    let _ = std::fs::remove_dir_all(db_path);

    let config = LightningDbConfig {
        cache_size: 256 * 1024 * 1024, // 256MB cache
        use_optimized_transactions: true,
        use_improved_wal: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));
    
    println!("Running stress tests...\n");
    
    // Test 1: High-concurrency writes
    test_concurrent_writes(Arc::clone(&db));
    
    // Test 2: Mixed read/write workload
    test_mixed_workload(Arc::clone(&db));
    
    // Test 3: Large transactions
    test_large_transactions(Arc::clone(&db));
    
    // Test 4: Memory pressure
    test_memory_pressure(Arc::clone(&db));
    
    // Test 5: Crash recovery
    test_crash_recovery(db_path);
    
    // Test 6: Long-running workload
    test_long_running(Arc::clone(&db));
    
    println!("\n✅ All stress tests completed!");
}

fn test_concurrent_writes(db: Arc<Database>) {
    println!("Test 1: High-concurrency writes");
    println!("-------------------------------");
    
    let num_threads = 16;
    let ops_per_thread = 10_000;
    let total_written = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let total_written_clone = Arc::clone(&total_written);
        
        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = vec![rng.random::<u8>(); 1024]; // 1KB values
                
                db_clone.put(key.as_bytes(), &value).expect("Put failed");
                total_written_clone.fetch_add(1, Ordering::Relaxed);
                
                if i % 1000 == 0 {
                    println!("  Thread {} progress: {}/{}", thread_id, i, ops_per_thread);
                }
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let total_ops = total_written.load(Ordering::Relaxed);
    let throughput = total_ops as f64 / elapsed.as_secs_f64();
    
    println!("  Total writes: {}", total_ops);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} ops/sec\n", throughput);
}

fn test_mixed_workload(db: Arc<Database>) {
    println!("Test 2: Mixed read/write workload");
    println!("---------------------------------");
    
    let num_threads = 8;
    let duration = Duration::from_secs(10);
    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let read_count_clone = Arc::clone(&read_count);
        let write_count_clone = Arc::clone(&write_count);
        
        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            let mut local_reads = 0u64;
            let mut local_writes = 0u64;
            
            while start.elapsed() < duration {
                if rng.random_bool(0.7) { // 70% reads
                    let key_id = rng.random_range(0..100_000);
                    let key = format!("key_{}", key_id);
                    
                    let _ = db_clone.get(key.as_bytes());
                    local_reads += 1;
                } else { // 30% writes
                    let key_id = rng.random_range(0..100_000);
                    let key = format!("key_{}", key_id);
                    let value = vec![rng.random::<u8>(); 256];
                    
                    db_clone.put(key.as_bytes(), &value).expect("Put failed");
                    local_writes += 1;
                }
            }
            
            read_count_clone.fetch_add(local_reads, Ordering::Relaxed);
            write_count_clone.fetch_add(local_writes, Ordering::Relaxed);
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let total_reads = read_count.load(Ordering::Relaxed);
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_ops = total_reads + total_writes;
    let elapsed = start.elapsed();
    
    println!("  Total operations: {}", total_ops);
    println!("  Reads: {} ({:.1}%)", total_reads, (total_reads as f64 / total_ops as f64) * 100.0);
    println!("  Writes: {} ({:.1}%)", total_writes, (total_writes as f64 / total_ops as f64) * 100.0);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} ops/sec\n", total_ops as f64 / elapsed.as_secs_f64());
}

fn test_large_transactions(db: Arc<Database>) {
    println!("Test 3: Large transactions");
    println!("--------------------------");
    
    let num_transactions = 10;
    let entries_per_transaction = 10_000;
    let start = Instant::now();
    
    for tx_id in 0..num_transactions {
        let tx = db.begin_transaction().expect("Failed to begin transaction");
        
        for i in 0..entries_per_transaction {
            let key = format!("tx_{}_entry_{}", tx_id, i);
            let value = vec![42u8; 512];
            
            db.put_tx(tx, key.as_bytes(), &value).expect("Put in transaction failed");
        }
        
        db.commit_transaction(tx).expect("Failed to commit transaction");
        println!("  Transaction {} committed ({} entries)", tx_id, entries_per_transaction);
    }
    
    let elapsed = start.elapsed();
    let total_entries = num_transactions * entries_per_transaction;
    
    println!("  Total entries: {}", total_entries);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} entries/sec\n", total_entries as f64 / elapsed.as_secs_f64());
}

fn test_memory_pressure(db: Arc<Database>) {
    println!("Test 4: Memory pressure");
    println!("-----------------------");
    
    let num_large_values = 1000;
    let value_size = 1024 * 1024; // 1MB per value
    let start = Instant::now();
    
    for i in 0..num_large_values {
        let key = format!("large_value_{}", i);
        let value = vec![i as u8; value_size];
        
        db.put(key.as_bytes(), &value).expect("Put failed");
        
        if i % 100 == 0 {
            println!("  Progress: {}/{} large values", i, num_large_values);
            
            // Force some cache eviction by using Database method through deref
            // For now, we'll just skip the flush since Arc<Database> doesn't expose it directly
        }
    }
    
    // Now read back with memory pressure
    let mut hit_count = 0;
    for i in 0..num_large_values {
        let key = format!("large_value_{}", i);
        if db.get(key.as_bytes()).expect("Get failed").is_some() {
            hit_count += 1;
        }
    }
    
    let elapsed = start.elapsed();
    println!("  Successfully stored and retrieved {} large values", hit_count);
    println!("  Duration: {:?}", elapsed);
    println!("  Cache stats: {:?}\n", db.cache_stats());
}

fn test_crash_recovery(db_path: &str) {
    println!("Test 5: Crash recovery simulation");
    println!("---------------------------------");
    
    // Create a new database
    let config = LightningDbConfig::default();
    let db = Database::open(db_path, config.clone()).expect("Failed to open database");
    
    // Write some data
    let num_entries = 1000;
    for i in 0..num_entries {
        let key = format!("recovery_key_{}", i);
        let value = format!("recovery_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    
    // Simulate crash by dropping without proper shutdown
    drop(db);
    
    // Reopen and verify data
    let db = Database::open(db_path, config).expect("Failed to reopen database");
    let mut recovered = 0;
    
    for i in 0..num_entries {
        let key = format!("recovery_key_{}", i);
        if let Some(value) = db.get(key.as_bytes()).expect("Get failed") {
            let expected = format!("recovery_value_{}", i);
            if value == expected.as_bytes() {
                recovered += 1;
            }
        }
    }
    
    println!("  Recovered {}/{} entries after crash", recovered, num_entries);
    println!("  Recovery rate: {:.1}%\n", (recovered as f64 / num_entries as f64) * 100.0);
}

fn test_long_running(db: Arc<Database>) {
    println!("Test 6: Long-running workload");
    println!("-----------------------------");
    
    let duration = Duration::from_secs(30);
    let num_threads = 4;
    let ops_count = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let ops_count_clone = Arc::clone(&ops_count);
        
        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            let mut local_ops = 0u64;
            
            while start.elapsed() < duration {
                // Mix of operations
                match rng.random_range(0..10) {
                    0..=5 => { // 60% reads
                        let key = format!("key_{}", rng.random_range(0..1_000_000));
                        let _ = db_clone.get(key.as_bytes());
                    }
                    6..=8 => { // 30% writes
                        let key = format!("key_{}", rng.random_range(0..1_000_000));
                        let value = vec![rng.random::<u8>(); 256];
                        db_clone.put(key.as_bytes(), &value).expect("Put failed");
                    }
                    9 => { // 10% deletes
                        let key = format!("key_{}", rng.random_range(0..1_000_000));
                        let _ = db_clone.delete(key.as_bytes());
                    }
                    _ => unreachable!(),
                }
                
                local_ops += 1;
                
                if local_ops % 10_000 == 0 {
                    ops_count_clone.fetch_add(local_ops, Ordering::Relaxed);
                    local_ops = 0;
                    println!("  Thread {} checkpoint: {} total ops", 
                             thread_id, ops_count_clone.load(Ordering::Relaxed));
                }
            }
            
            ops_count_clone.fetch_add(local_ops, Ordering::Relaxed);
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let total_ops = ops_count.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    
    println!("  Total operations: {}", total_ops);
    println!("  Duration: {:?}", elapsed);
    println!("  Sustained throughput: {:.2} ops/sec", total_ops as f64 / elapsed.as_secs_f64());
    
    // Final stats
    println!("\nFinal database statistics:");
    println!("  {:?}", db.stats());
}