/// Performance Deep Dive Analysis
/// 
/// Analyze Lightning DB performance characteristics and optimization opportunities

use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::TempDir;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 LIGHTNING DB PERFORMANCE DEEP DIVE");
    println!("=====================================\n");
    
    // Test 1: Write Amplification Analysis
    println!("1️⃣ Write Amplification Analysis");
    analyze_write_amplification()?;
    
    // Test 2: Read Latency Distribution
    println!("\n2️⃣ Read Latency Distribution");
    analyze_read_latency()?;
    
    // Test 3: Transaction Overhead
    println!("\n3️⃣ Transaction Overhead Analysis");
    analyze_transaction_overhead()?;
    
    // Test 4: Compaction Impact
    println!("\n4️⃣ Compaction Impact Analysis");
    analyze_compaction_impact()?;
    
    // Test 5: Memory Allocation Patterns
    println!("\n5️⃣ Memory Allocation Patterns");
    analyze_memory_patterns()?;
    
    // Test 6: Concurrent Performance Scaling
    println!("\n6️⃣ Concurrent Performance Scaling");
    analyze_concurrent_scaling()?;
    
    Ok(())
}

fn analyze_write_amplification() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;
    
    // Measure write amplification with different value sizes
    let value_sizes = vec![64, 256, 1024, 4096, 16384];
    let num_writes = 10_000;
    
    println!("   Testing write amplification with different value sizes:");
    
    for &value_size in &value_sizes {
        let value = vec![b'x'; value_size];
        let start = Instant::now();
        
        for i in 0..num_writes {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        let duration = start.elapsed();
        let throughput = num_writes as f64 / duration.as_secs_f64();
        let mb_written = (num_writes * value_size) as f64 / (1024.0 * 1024.0);
        let mb_per_sec = mb_written / duration.as_secs_f64();
        
        println!("     {} bytes: {:.0} ops/sec, {:.1} MB/s", 
                 value_size, throughput, mb_per_sec);
    }
    
    Ok(())
}

fn analyze_read_latency() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;
    
    // Populate database
    let num_keys = 100_000;
    for i in 0..num_keys {
        let key = format!("key_{:08}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Measure read latencies
    let mut rng = StdRng::seed_from_u64(42);
    let num_reads = 10_000;
    let mut latencies = Vec::with_capacity(num_reads);
    
    for _ in 0..num_reads {
        let key_id = rng.random_range(0..num_keys);
        let key = format!("key_{:08}", key_id);
        
        let start = Instant::now();
        let _ = db.get(key.as_bytes())?;
        let latency = start.elapsed();
        
        latencies.push(latency.as_micros() as f64);
    }
    
    // Calculate percentiles
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = latencies[latencies.len() / 2];
    let p90 = latencies[latencies.len() * 9 / 10];
    let p99 = latencies[latencies.len() * 99 / 100];
    let p999 = latencies[latencies.len() * 999 / 1000];
    
    println!("   Read latency percentiles (μs):");
    println!("     P50:  {:.2}", p50);
    println!("     P90:  {:.2}", p90);
    println!("     P99:  {:.2}", p99);
    println!("     P99.9: {:.2}", p999);
    
    Ok(())
}

fn analyze_transaction_overhead() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;
    
    let num_operations = 10_000;
    
    // Test 1: Direct writes
    let start = Instant::now();
    for i in 0..num_operations {
        let key = format!("direct_{}", i);
        db.put(key.as_bytes(), b"value")?;
    }
    let direct_duration = start.elapsed();
    let direct_throughput = num_operations as f64 / direct_duration.as_secs_f64();
    
    // Test 2: Single operation transactions
    let start = Instant::now();
    for i in 0..num_operations {
        let tx_id = db.begin_transaction()?;
        let key = format!("tx_single_{}", i);
        db.put_tx(tx_id, key.as_bytes(), b"value")?;
        db.commit_transaction(tx_id)?;
    }
    let single_tx_duration = start.elapsed();
    let single_tx_throughput = num_operations as f64 / single_tx_duration.as_secs_f64();
    
    // Test 3: Batched transactions
    let batch_size = 100;
    let start = Instant::now();
    for batch in 0..(num_operations / batch_size) {
        let tx_id = db.begin_transaction()?;
        for i in 0..batch_size {
            let key = format!("tx_batch_{}_{}", batch, i);
            db.put_tx(tx_id, key.as_bytes(), b"value")?;
        }
        db.commit_transaction(tx_id)?;
    }
    let batch_tx_duration = start.elapsed();
    let batch_tx_throughput = num_operations as f64 / batch_tx_duration.as_secs_f64();
    
    println!("   Direct writes: {:.0} ops/sec", direct_throughput);
    println!("   Single-op transactions: {:.0} ops/sec ({:.1}x slower)", 
             single_tx_throughput, direct_throughput / single_tx_throughput);
    println!("   Batched transactions ({}): {:.0} ops/sec ({:.1}x faster than single)", 
             batch_size, batch_tx_throughput, batch_tx_throughput / single_tx_throughput);
    
    Ok(())
}

fn analyze_compaction_impact() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default(); // LSM is used by default
    let db = Database::create(temp_dir.path(), config)?;
    
    // Write data to trigger compaction
    let num_writes = 50_000;
    let value = vec![b'x'; 1024]; // 1KB values
    
    let mut write_latencies = Vec::new();
    
    for i in 0..num_writes {
        let key = format!("compact_test_{:08}", i);
        
        let start = Instant::now();
        db.put(key.as_bytes(), &value)?;
        let latency = start.elapsed();
        
        write_latencies.push(latency.as_micros() as f64);
        
        if i % 10000 == 0 && i > 0 {
            println!("     After {} writes: avg latency {:.2}μs", 
                     i, 
                     write_latencies.iter().sum::<f64>() / write_latencies.len() as f64);
            write_latencies.clear();
        }
    }
    
    Ok(())
}

fn analyze_memory_patterns() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.cache_size = 50 * 1024 * 1024; // 50MB cache
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Test different access patterns
    let num_keys = 10_000;
    
    // Sequential writes
    let start = Instant::now();
    for i in 0..num_keys {
        let key = format!("seq_{:08}", i);
        db.put(key.as_bytes(), b"value")?;
    }
    let _seq_write_time = start.elapsed();
    
    // Random reads with different working set sizes
    let working_sets = vec![100, 1000, 5000, 10000];
    let reads_per_set = 10_000;
    
    println!("   Cache behavior with different working sets:");
    
    for &working_set in &working_sets {
        let mut rng = StdRng::seed_from_u64(42);
        let start = Instant::now();
        
        for _ in 0..reads_per_set {
            let key_id = rng.random_range(0..working_set.min(num_keys));
            let key = format!("seq_{:08}", key_id);
            let _ = db.get(key.as_bytes())?;
        }
        
        let duration = start.elapsed();
        let throughput = reads_per_set as f64 / duration.as_secs_f64();
        
        println!("     Working set {}: {:.0} ops/sec", working_set, throughput);
    }
    
    Ok(())
}

fn analyze_concurrent_scaling() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(temp_dir.path(), config)?);
    
    // Pre-populate database
    let num_keys = 100_000;
    for i in 0..num_keys {
        let key = format!("scale_{:08}", i);
        db.put(key.as_bytes(), b"initial")?;
    }
    
    let thread_counts = vec![1, 2, 4, 8, 16];
    let ops_per_thread = 10_000;
    
    println!("   Concurrent read scaling:");
    
    for &num_threads in &thread_counts {
        let db_clone = db.clone();
        let barrier = Arc::new(Barrier::new(num_threads));
        
        let start = Instant::now();
        let mut handles = vec![];
        
        for thread_id in 0..num_threads {
            let db_thread = db_clone.clone();
            let barrier_clone = barrier.clone();
            
            let handle = thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                barrier_clone.wait();
                
                for _ in 0..ops_per_thread {
                    let key_id = rng.random_range(0..num_keys);
                    let key = format!("scale_{:08}", key_id);
                    let _ = db_thread.get(key.as_bytes()).unwrap();
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let total_ops = num_threads * ops_per_thread;
        let throughput = total_ops as f64 / duration.as_secs_f64();
        
        println!("     {} threads: {:.0} ops/sec ({:.1}x)", 
                 num_threads, throughput, throughput / (ops_per_thread as f64));
    }
    
    Ok(())
}