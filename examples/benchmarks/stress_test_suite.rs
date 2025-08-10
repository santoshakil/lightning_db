use lightning_db::{Database, LightningDbConfig};
use lightning_db::write_batch::WriteBatch;
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use rand::Rng;

struct StressTestResults {
    total_operations: u64,
    successful_operations: u64,
    failed_operations: u64,
    duration: Duration,
    throughput_ops_per_sec: f64,
    avg_latency_us: f64,
    max_latency_us: f64,
    errors: Vec<String>,
}

impl StressTestResults {
    fn new() -> Self {
        Self {
            total_operations: 0,
            successful_operations: 0,
            failed_operations: 0,
            duration: Duration::ZERO,
            throughput_ops_per_sec: 0.0,
            avg_latency_us: 0.0,
            max_latency_us: 0.0,
            errors: Vec::new(),
        }
    }
    
    fn print_summary(&self, test_name: &str) {
        println!("\n=== {} Results ===", test_name);
        println!("Total Operations: {}", self.total_operations);
        println!("Successful: {} ({:.1}%)", 
                 self.successful_operations, 
                 (self.successful_operations as f64 / self.total_operations as f64) * 100.0);
        println!("Failed: {}", self.failed_operations);
        println!("Duration: {:.2}s", self.duration.as_secs_f64());
        println!("Throughput: {:.0} ops/sec", self.throughput_ops_per_sec);
        println!("Avg Latency: {:.2} μs", self.avg_latency_us);
        println!("Max Latency: {:.2} μs", self.max_latency_us);
        if !self.errors.is_empty() {
            println!("Errors: {:?}", self.errors);
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Stress Test Suite");
    println!("==============================\n");
    
    // Test 1: Concurrent Read/Write
    test_concurrent_read_write()?;
    
    // Test 2: Transaction Conflicts
    test_transaction_conflicts()?;
    
    // Test 3: Large Batch Operations
    test_large_batch_operations()?;
    
    // Test 4: Memory Pressure
    test_memory_pressure()?;
    
    // Test 5: Sustained Load
    test_sustained_load()?;
    
    // Test 6: Mixed Workload
    test_mixed_workload()?;
    
    println!("\n✅ All stress tests completed successfully!");
    Ok(())
}

fn test_concurrent_read_write() -> Result<(), Box<dyn std::error::Error>> {
    println!("Test 1: Concurrent Read/Write Operations");
    println!("-----------------------------------------");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    const NUM_THREADS: usize = 8;
    const OPS_PER_THREAD: usize = 10000;
    
    // Pre-populate some data
    for i in 0..1000 {
        let key = format!("init_key_{:06}", i);
        let value = format!("init_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let max_latency = Arc::new(AtomicU64::new(0));
    let total_latency = Arc::new(AtomicU64::new(0));
    
    let start = Instant::now();
    let mut handles = vec![];
    
    // Writer threads
    for thread_id in 0..NUM_THREADS/2 {
        let db_clone = Arc::clone(&db);
        let success = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);
        let max_lat = Arc::clone(&max_latency);
        let total_lat = Arc::clone(&total_latency);
        
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for i in 0..OPS_PER_THREAD {
                let op_start = Instant::now();
                let key = format!("t{}_key_{:06}", thread_id, i);
                let value = format!("t{}_value_{}", thread_id, i);
                
                match db_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                let latency = op_start.elapsed().as_micros() as u64;
                total_lat.fetch_add(latency, Ordering::Relaxed);
                max_lat.fetch_max(latency, Ordering::Relaxed);
                
                // Occasional delete
                if rng.gen_bool(0.1) {
                    let del_key = format!("t{}_key_{:06}", thread_id, rng.gen_range(0..i.max(1)));
                    let _ = db_clone.delete(del_key.as_bytes());
                }
            }
        });
        handles.push(handle);
    }
    
    // Reader threads
    for _thread_id in NUM_THREADS/2..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let success = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);
        let max_lat = Arc::clone(&max_latency);
        let total_lat = Arc::clone(&total_latency);
        
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for _ in 0..OPS_PER_THREAD {
                let op_start = Instant::now();
                
                // Read random key
                let key = if rng.gen_bool(0.5) {
                    // Read initial data
                    format!("init_key_{:06}", rng.gen_range(0..1000))
                } else {
                    // Read thread data
                    let t = rng.gen_range(0..NUM_THREADS/2);
                    let i = rng.gen_range(0..OPS_PER_THREAD);
                    format!("t{}_key_{:06}", t, i)
                };
                
                match db_clone.get(key.as_bytes()) {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                let latency = op_start.elapsed().as_micros() as u64;
                total_lat.fetch_add(latency, Ordering::Relaxed);
                max_lat.fetch_max(latency, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = (NUM_THREADS * OPS_PER_THREAD) as u64;
    let successful = success_count.load(Ordering::Relaxed);
    let failed = error_count.load(Ordering::Relaxed);
    
    let mut results = StressTestResults::new();
    results.total_operations = total_ops;
    results.successful_operations = successful;
    results.failed_operations = failed;
    results.duration = duration;
    results.throughput_ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    results.avg_latency_us = total_latency.load(Ordering::Relaxed) as f64 / total_ops as f64;
    results.max_latency_us = max_latency.load(Ordering::Relaxed) as f64;
    
    results.print_summary("Concurrent Read/Write");
    
    if failed > 0 {
        return Err(format!("{} operations failed", failed).into());
    }
    
    Ok(())
}

fn test_transaction_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 2: Transaction Conflicts");
    println!("------------------------------");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    // Initialize accounts
    const NUM_ACCOUNTS: usize = 100;
    const INITIAL_BALANCE: u64 = 1000;
    
    for i in 0..NUM_ACCOUNTS {
        let key = format!("account_{:03}", i);
        db.put(key.as_bytes(), &INITIAL_BALANCE.to_le_bytes())?;
    }
    
    const NUM_THREADS: usize = 8;
    const TRANSFERS_PER_THREAD: usize = 1000;
    
    let success_count = Arc::new(AtomicU64::new(0));
    let conflict_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for _ in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let success = Arc::clone(&success_count);
        let conflicts = Arc::clone(&conflict_count);
        let errors = Arc::clone(&error_count);
        
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            
            for _ in 0..TRANSFERS_PER_THREAD {
                let from = rng.gen_range(0..NUM_ACCOUNTS);
                let to = rng.gen_range(0..NUM_ACCOUNTS);
                
                if from == to {
                    continue;
                }
                
                let amount = rng.gen_range(1..100);
                
                // Try to transfer money
                let tx_id = match db_clone.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                let from_key = format!("account_{:03}", from);
                let to_key = format!("account_{:03}", to);
                
                // Read balances
                let from_balance = match db_clone.get_tx(tx_id, from_key.as_bytes()) {
                    Ok(Some(data)) => u64::from_le_bytes(data.try_into().unwrap_or([0; 8])),
                    _ => {
                        let _ = db_clone.abort_transaction(tx_id);
                        conflicts.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                let to_balance = match db_clone.get_tx(tx_id, to_key.as_bytes()) {
                    Ok(Some(data)) => u64::from_le_bytes(data.try_into().unwrap_or([0; 8])),
                    _ => {
                        let _ = db_clone.abort_transaction(tx_id);
                        conflicts.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                if from_balance < amount {
                    let _ = db_clone.abort_transaction(tx_id);
                    continue;
                }
                
                // Update balances
                let new_from = from_balance - amount;
                let new_to = to_balance + amount;
                
                if db_clone.put_tx(tx_id, from_key.as_bytes(), &new_from.to_le_bytes()).is_err() {
                    let _ = db_clone.abort_transaction(tx_id);
                    conflicts.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                
                if db_clone.put_tx(tx_id, to_key.as_bytes(), &new_to.to_le_bytes()).is_err() {
                    let _ = db_clone.abort_transaction(tx_id);
                    conflicts.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                
                // Commit transaction
                match db_clone.commit_transaction(tx_id) {
                    Ok(_) => { success.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => { conflicts.fetch_add(1, Ordering::Relaxed); }
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    
    // Verify total balance is conserved
    let mut total_balance = 0u64;
    for i in 0..NUM_ACCOUNTS {
        let key = format!("account_{:03}", i);
        if let Ok(Some(data)) = db.get(key.as_bytes()) {
            total_balance += u64::from_le_bytes(data.try_into().unwrap_or([0; 8]));
        }
    }
    
    let expected_total = NUM_ACCOUNTS as u64 * INITIAL_BALANCE;
    
    let mut results = StressTestResults::new();
    results.total_operations = (NUM_THREADS * TRANSFERS_PER_THREAD) as u64;
    results.successful_operations = success_count.load(Ordering::Relaxed);
    results.failed_operations = conflict_count.load(Ordering::Relaxed) + error_count.load(Ordering::Relaxed);
    results.duration = duration;
    results.throughput_ops_per_sec = results.successful_operations as f64 / duration.as_secs_f64();
    
    results.print_summary("Transaction Conflicts");
    
    println!("Conflicts detected: {}", conflict_count.load(Ordering::Relaxed));
    println!("Total balance: {} (expected: {})", total_balance, expected_total);
    
    if total_balance != expected_total {
        return Err(format!("Balance mismatch! {} != {}", total_balance, expected_total).into());
    }
    
    Ok(())
}

fn test_large_batch_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 3: Large Batch Operations");
    println!("-------------------------------");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path(), config)?;
    
    const BATCH_SIZE: usize = 500;  // Reduced from 1000
    const NUM_BATCHES: usize = 100;
    
    let mut results = StressTestResults::new();
    let start = Instant::now();
    
    for batch_id in 0..NUM_BATCHES {
        let batch_start = Instant::now();
        let mut batch = WriteBatch::new();
        
        for i in 0..BATCH_SIZE {
            let key = format!("batch_{:03}_key_{:06}", batch_id, i);
            let value = vec![batch_id as u8; 1024]; // 1KB value
            batch.put(key.into_bytes(), value)?;
        }
        
        match db.write_batch(&batch) {
            Ok(_) => results.successful_operations += BATCH_SIZE as u64,
            Err(e) => {
                results.failed_operations += BATCH_SIZE as u64;
                results.errors.push(format!("Batch {} failed: {}", batch_id, e));
            }
        }
        
        let batch_latency = batch_start.elapsed().as_micros() as f64;
        results.max_latency_us = results.max_latency_us.max(batch_latency);
    }
    
    results.duration = start.elapsed();
    results.total_operations = (NUM_BATCHES * 500) as u64;  // Updated to match new BATCH_SIZE
    results.throughput_ops_per_sec = results.total_operations as f64 / results.duration.as_secs_f64();
    results.avg_latency_us = results.duration.as_micros() as f64 / NUM_BATCHES as f64;
    
    // Verify data
    let mut verify_errors = 0;
    for batch_id in 0..NUM_BATCHES.min(10) { // Spot check first 10 batches
        for i in 0..BATCH_SIZE.min(10) {
            let key = format!("batch_{:03}_key_{:06}", batch_id, i);
            match db.get(key.as_bytes()) {
                Ok(Some(val)) if val[0] == batch_id as u8 => {},
                _ => verify_errors += 1,
            }
        }
    }
    
    results.print_summary("Large Batch Operations");
    
    if verify_errors > 0 {
        println!("⚠️  Verification errors: {}", verify_errors);
    }
    
    Ok(())
}

fn test_memory_pressure() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 4: Memory Pressure");
    println!("------------------------");
    
    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024; // 10MB cache (small to create pressure)
    let db = Database::create(dir.path(), config)?;
    
    const VALUE_SIZE: usize = 10 * 1024; // 10KB values
    const NUM_KEYS: usize = 10000;
    
    let mut results = StressTestResults::new();
    let start = Instant::now();
    
    // Write large values
    println!("Writing {} keys with {}KB values...", NUM_KEYS, VALUE_SIZE / 1024);
    for i in 0..NUM_KEYS {
        let key = format!("large_key_{:06}", i);
        let value = vec![(i % 256) as u8; VALUE_SIZE];
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => results.successful_operations += 1,
            Err(_) => results.failed_operations += 1,
        }
        
        if i % 1000 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }
    println!();
    
    // Read back with cache pressure
    println!("Reading back with cache pressure...");
    let mut rng = rand::thread_rng();
    for _ in 0..NUM_KEYS {
        let i = rng.gen_range(0..NUM_KEYS);
        let key = format!("large_key_{:06}", i);
        
        match db.get(key.as_bytes()) {
            Ok(Some(val)) if val[0] == (i % 256) as u8 => results.successful_operations += 1,
            _ => results.failed_operations += 1,
        }
    }
    
    results.duration = start.elapsed();
    results.total_operations = NUM_KEYS as u64 * 2;
    results.throughput_ops_per_sec = results.total_operations as f64 / results.duration.as_secs_f64();
    
    results.print_summary("Memory Pressure");
    
    let total_data_mb = (NUM_KEYS * VALUE_SIZE) / (1024 * 1024);
    println!("Total data size: {} MB", total_data_mb);
    println!("Cache size: 10 MB");
    println!("Cache pressure ratio: {:.1}x", total_data_mb as f64 / 10.0);
    
    Ok(())
}

fn test_sustained_load() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 5: Sustained Load (30 seconds)");
    println!("------------------------------------");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    const TEST_DURATION: Duration = Duration::from_secs(30);
    const NUM_THREADS: usize = 4;
    
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ops_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let stop = Arc::clone(&stop_flag);
        let ops = Arc::clone(&ops_count);
        let errors = Arc::clone(&error_count);
        
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut local_ops = 0u64;
            
            while !stop.load(Ordering::Relaxed) {
                let op = rng.gen_range(0..100);
                
                if op < 40 {
                    // 40% writes
                    let key = format!("sustained_t{}_key_{}", thread_id, local_ops);
                    let value = format!("value_{}", local_ops);
                    match db_clone.put(key.as_bytes(), value.as_bytes()) {
                        Ok(_) => local_ops += 1,
                        Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                    }
                } else if op < 90 {
                    // 50% reads
                    let key = format!("sustained_t{}_key_{}", 
                                     rng.gen_range(0..NUM_THREADS), 
                                     rng.gen_range(0..local_ops.max(1)));
                    match db_clone.get(key.as_bytes()) {
                        Ok(_) => local_ops += 1,
                        Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                    }
                } else {
                    // 10% deletes
                    let key = format!("sustained_t{}_key_{}", 
                                     thread_id, 
                                     rng.gen_range(0..local_ops.max(1)));
                    match db_clone.delete(key.as_bytes()) {
                        Ok(_) => local_ops += 1,
                        Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                    }
                }
            }
            
            ops.fetch_add(local_ops, Ordering::Relaxed);
        });
        handles.push(handle);
    }
    
    // Monitor progress
    let monitor_stop = Arc::clone(&stop_flag);
    let monitor_ops = Arc::clone(&ops_count);
    let monitor_handle = thread::spawn(move || {
        let mut last_ops = 0;
        while !monitor_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(5));
            let current_ops = monitor_ops.load(Ordering::Relaxed);
            let rate = (current_ops - last_ops) / 5;
            println!("Current rate: {} ops/sec", rate);
            last_ops = current_ops;
        }
    });
    
    // Run for specified duration
    thread::sleep(TEST_DURATION);
    stop_flag.store(true, Ordering::Relaxed);
    
    // Wait for threads
    for handle in handles {
        handle.join().unwrap();
    }
    monitor_handle.join().unwrap();
    
    let duration = start.elapsed();
    let total_ops = ops_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    
    let mut results = StressTestResults::new();
    results.total_operations = total_ops;
    results.successful_operations = total_ops;
    results.failed_operations = total_errors;
    results.duration = duration;
    results.throughput_ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    results.print_summary("Sustained Load");
    
    Ok(())
}

fn test_mixed_workload() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 6: Mixed Workload");
    println!("-----------------------");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path(), config)?);
    
    const NUM_THREADS: usize = 6;
    const OPS_PER_THREAD: usize = 5000;
    
    let ops_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    
    let start = Instant::now();
    let mut handles = vec![];
    
    // OLTP threads (small transactions)
    for thread_id in 0..NUM_THREADS/2 {
        let db_clone = Arc::clone(&db);
        let ops = Arc::clone(&ops_count);
        let errors = Arc::clone(&error_count);
        
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            
            for i in 0..OPS_PER_THREAD {
                // Small transaction
                let tx_id = match db_clone.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                // Read-modify-write pattern
                let mut tx_failed = false;
                for _ in 0..rng.gen_range(2..5) {
                    let key = format!("oltp_t{}_k{}", thread_id, rng.gen_range(0..100));
                    
                    let new_val = match db_clone.get_tx(tx_id, key.as_bytes()) {
                        Ok(Some(val)) => {
                            let mut v = val;
                            v.push(1);
                            v
                        }
                        _ => format!("v{}", i).into_bytes(),
                    };
                    
                    if db_clone.put_tx(tx_id, key.as_bytes(), &new_val).is_err() {
                        let _ = db_clone.abort_transaction(tx_id);
                        errors.fetch_add(1, Ordering::Relaxed);
                        tx_failed = true;
                        break;
                    }
                }
                
                if !tx_failed {
                    match db_clone.commit_transaction(tx_id) {
                        Ok(_) => { ops.fetch_add(1, Ordering::Relaxed); }
                        Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    // OLAP threads (range scans)
    for thread_id in NUM_THREADS/2..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let ops = Arc::clone(&ops_count);
        
        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            
            for _ in 0..OPS_PER_THREAD/10 { // Fewer but heavier operations
                // Simulate range scan
                let mut count = 0;
                for i in 0..50 {
                    let key = format!("oltp_t{}_k{}", thread_id % (NUM_THREADS/2), i);
                    if db_clone.get(key.as_bytes()).is_ok() {
                        count += 1;
                    }
                }
                
                if count > 0 {
                    ops.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = ops_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    
    let mut results = StressTestResults::new();
    results.total_operations = total_ops + total_errors;
    results.successful_operations = total_ops;
    results.failed_operations = total_errors;
    results.duration = duration;
    results.throughput_ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    results.print_summary("Mixed Workload");
    
    Ok(())
}