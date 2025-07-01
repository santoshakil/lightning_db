use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::tempdir;
use rand::Rng;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB Production Stress Test ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();
    
    // Test 1: High-volume concurrent writes
    println!("Test 1: Concurrent Write Stress Test");
    println!("=====================================");
    {
        let db = Arc::new(Database::create(db_path, LightningDbConfig::default())?);
        let num_threads = 8;
        let writes_per_thread = 10_000;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        
        let start = Instant::now();
        let mut handles = vec![];
        
        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let barrier_clone = Arc::clone(&barrier);
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                
                for i in 0..writes_per_thread {
                    let key = format!("thread_{}_key_{:06}", thread_id, i);
                    let value = format!("thread_{}_value_{:06}_data_{}", thread_id, i, "x".repeat(50));
                    
                    if let Err(e) = db_clone.put(key.as_bytes(), value.as_bytes()) {
                        eprintln!("Write error in thread {}: {}", thread_id, e);
                        return Err(e.to_string());
                    }
                }
                Ok(())
            });
            
            handles.push(handle);
        }
        
        barrier.wait(); // Start all threads simultaneously
        
        for (i, handle) in handles.into_iter().enumerate() {
            if let Err(e) = handle.join().unwrap() {
                panic!("Thread {} failed: {}", i, e);
            }
        }
        
        let duration = start.elapsed();
        let total_writes = num_threads * writes_per_thread;
        let writes_per_sec = total_writes as f64 / duration.as_secs_f64();
        
        println!("  ✓ Wrote {} entries in {:.2}s", total_writes, duration.as_secs_f64());
        println!("  ✓ Performance: {:.0} writes/sec", writes_per_sec);
        
        // Verify data
        print!("  Verifying data... ");
        let mut verified = 0;
        for thread_id in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("thread_{}_key_{:06}", thread_id, i);
                if db.get(key.as_bytes())?.is_some() {
                    verified += 1;
                }
            }
        }
        println!("{}/{} entries verified", verified, total_writes);
        assert_eq!(verified, total_writes);
    }
    
    // Test 2: Mixed read-write workload
    println!("\nTest 2: Mixed Read-Write Workload");
    println!("==================================");
    {
        let db = Arc::new(Database::open(db_path, LightningDbConfig::default())?);
        let num_threads = 8;
        let ops_per_thread = 5_000;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        
        let start = Instant::now();
        let mut handles = vec![];
        
        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let barrier_clone = Arc::clone(&barrier);
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                let mut rng = rand::thread_rng();
                let mut reads = 0;
                let mut writes = 0;
                
                for i in 0..ops_per_thread {
                    let op = rng.gen_range(0..10);
                    
                    if op < 7 { // 70% reads
                        let key_num = rng.gen_range(0..80_000);
                        let key = format!("thread_{}_key_{:06}", key_num % 8, key_num / 8);
                        
                        if let Err(e) = db_clone.get(key.as_bytes()) {
                            eprintln!("Read error in thread {}: {}", thread_id, e);
                            return Err(e.to_string());
                        }
                        reads += 1;
                    } else { // 30% writes
                        let key = format!("mixed_thread_{}_key_{:06}", thread_id, i);
                        let value = format!("mixed_value_{}", i);
                        
                        if let Err(e) = db_clone.put(key.as_bytes(), value.as_bytes()) {
                            eprintln!("Write error in thread {}: {}", thread_id, e);
                            return Err(e.to_string());
                        }
                        writes += 1;
                    }
                }
                
                Ok((reads, writes))
            });
            
            handles.push(handle);
        }
        
        barrier.wait(); // Start all threads simultaneously
        
        let mut total_reads = 0;
        let mut total_writes = 0;
        
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join().unwrap() {
                Ok((reads, writes)) => {
                    total_reads += reads;
                    total_writes += writes;
                }
                Err(e) => panic!("Thread {} failed: {}", i, e),
            }
        }
        
        let duration = start.elapsed();
        let total_ops = total_reads + total_writes;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        
        println!("  ✓ Completed {} operations in {:.2}s", total_ops, duration.as_secs_f64());
        println!("  ✓ Reads: {}, Writes: {}", total_reads, total_writes);
        println!("  ✓ Performance: {:.0} ops/sec", ops_per_sec);
    }
    
    // Test 3: Large value stress test
    println!("\nTest 3: Large Value Stress Test");
    println!("=================================");
    {
        let db = Arc::new(Database::open(db_path, LightningDbConfig::default())?);
        let num_large_values = 1_000;
        let value_size = 10_000; // 10KB values
        
        let start = Instant::now();
        let large_value = "x".repeat(value_size);
        
        for i in 0..num_large_values {
            let key = format!("large_key_{:06}", i);
            db.put(key.as_bytes(), large_value.as_bytes())?;
            
            if i % 100 == 0 {
                print!("\r  Writing large values: {}/{}...", i, num_large_values);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }
        println!("\r  ✓ Wrote {} large values ({} KB each)", num_large_values, value_size / 1024);
        
        let duration = start.elapsed();
        let total_data_mb = (num_large_values * value_size) as f64 / (1024.0 * 1024.0);
        let throughput_mb_s = total_data_mb / duration.as_secs_f64();
        
        println!("  ✓ Total data written: {:.2} MB", total_data_mb);
        println!("  ✓ Throughput: {:.2} MB/s", throughput_mb_s);
    }
    
    // Test 4: Transaction stress test
    println!("\nTest 4: Transaction Stress Test");
    println!("================================");
    {
        let db = Arc::new(Database::open(db_path, LightningDbConfig::default())?);
        let num_threads = 4;
        let txns_per_thread = 100;
        let ops_per_txn = 50;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        
        let start = Instant::now();
        let mut handles = vec![];
        
        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let barrier_clone = Arc::clone(&barrier);
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                let mut committed = 0;
                let mut aborted = 0;
                
                for txn in 0..txns_per_thread {
                    let tx_id = match db_clone.begin_transaction() {
                        Ok(id) => id,
                        Err(e) => {
                            eprintln!("Failed to begin transaction: {}", e);
                            continue;
                        }
                    };
                    
                    let mut success = true;
                    for op in 0..ops_per_txn {
                        let key = format!("tx_thread_{}_txn_{}_op_{}", thread_id, txn, op);
                        let value = format!("tx_value_{}", op);
                        
                        if let Err(e) = db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()) {
                            eprintln!("Transaction operation error: {}", e);
                            success = false;
                            break;
                        }
                    }
                    
                    if success {
                        if let Err(e) = db_clone.commit_transaction(tx_id) {
                            eprintln!("Failed to commit transaction: {}", e);
                            aborted += 1;
                        } else {
                            committed += 1;
                        }
                    } else {
                        if let Err(e) = db_clone.abort_transaction(tx_id) {
                            eprintln!("Failed to abort transaction: {}", e);
                        }
                        aborted += 1;
                    }
                }
                
                Ok((committed, aborted))
            });
            
            handles.push(handle);
        }
        
        barrier.wait(); // Start all threads simultaneously
        
        let mut total_committed = 0;
        let mut total_aborted = 0;
        
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join().unwrap() {
                Ok((committed, aborted)) => {
                    total_committed += committed;
                    total_aborted += aborted;
                }
                Err(e) => panic!("Thread {} failed: {}", i, e),
            }
        }
        
        let duration = start.elapsed();
        let txns_per_sec = total_committed as f64 / duration.as_secs_f64();
        
        println!("  ✓ Committed: {} transactions", total_committed);
        println!("  ✓ Aborted: {} transactions", total_aborted);
        println!("  ✓ Performance: {:.0} txns/sec", txns_per_sec);
    }
    
    // Test 5: Recovery stress test
    println!("\nTest 5: Database Recovery Test");
    println!("================================");
    {
        let test_key = b"recovery_test_key";
        let test_value = b"recovery_test_value";
        
        // Write data and checkpoint
        {
            let db = Database::open(db_path, LightningDbConfig::default())?;
            db.put(test_key, test_value)?;
            db.checkpoint()?;
            println!("  ✓ Data written and checkpointed");
        }
        
        // Reopen and verify
        {
            let db = Database::open(db_path, LightningDbConfig::default())?;
            match db.get(test_key)? {
                Some(value) => {
                    assert_eq!(value.as_ref(), test_value);
                    println!("  ✓ Data successfully recovered after reopening");
                }
                None => panic!("Data not found after recovery"),
            }
        }
    }
    
    println!("\n✅ All stress tests completed successfully!");
    
    Ok(())
}