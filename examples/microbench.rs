use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use std::hint::black_box;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Lightning DB Micro-benchmarks\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().join("bench.db");
    
    // Test with minimal configuration to isolate bottlenecks
    let config = LightningDbConfig {
        compression_enabled: false,
        use_optimized_transactions: false,
        use_optimized_page_manager: false,
        prefetch_enabled: false,
        cache_size: 0, // Disable cache to measure raw performance
        ..Default::default()
    };
    
    let db = Database::create(&db_path, config)?;
    
    // Prepare test data
    let key = b"test_key_12345";
    let value = b"test_value_with_some_data";
    let iterations = 100_000;
    
    // Warmup
    for _ in 0..1000 {
        db.put(key, value)?;
        let _ = db.get(key)?;
    }
    
    println!("🎯 Target: <1μs reads, <10μs writes\n");
    
    // Benchmark 1: Raw put operation
    println!("1. Put operation breakdown:");
    let start = Instant::now();
    for i in 0..iterations {
        let k = format!("key_{:08}", i);
        black_box(db.put(k.as_bytes(), value)?);
    }
    let duration = start.elapsed();
    let us_per_op = duration.as_micros() as f64 / iterations as f64;
    println!("   Total: {:.2} μs/op ({:.0} ops/sec)", us_per_op, 1_000_000.0 / us_per_op);
    
    // Benchmark 2: Raw get operation (sequential keys)
    println!("\n2. Get operation (sequential keys):");
    let start = Instant::now();
    for i in 0..iterations {
        let k = format!("key_{:08}", i);
        black_box(db.get(k.as_bytes())?);
    }
    let duration = start.elapsed();
    let us_per_op = duration.as_micros() as f64 / iterations as f64;
    println!("   Total: {:.2} μs/op ({:.0} ops/sec)", us_per_op, 1_000_000.0 / us_per_op);
    
    // Benchmark 3: Get operation (same key)
    println!("\n3. Get operation (same key - best case):");
    db.put(b"bench_key", value)?;
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(db.get(b"bench_key")?);
    }
    let duration = start.elapsed();
    let us_per_op = duration.as_micros() as f64 / iterations as f64;
    println!("   Total: {:.2} μs/op ({:.0} ops/sec)", us_per_op, 1_000_000.0 / us_per_op);
    
    // Now test with optimizations enabled
    println!("\n--- With all optimizations enabled ---");
    
    let config_opt = LightningDbConfig::default();
    let db_opt = Database::create(&db_path.join("opt"), config_opt)?;
    
    // Warmup
    for _ in 0..1000 {
        db_opt.put(key, value)?;
        let _ = db_opt.get(key)?;
    }
    
    // Benchmark 4: Optimized put
    println!("\n4. Put operation (optimized):");
    let start = Instant::now();
    for i in 0..iterations {
        let k = format!("opt_key_{:08}", i);
        black_box(db_opt.put(k.as_bytes(), value)?);
    }
    let duration = start.elapsed();
    let us_per_op = duration.as_micros() as f64 / iterations as f64;
    println!("   Total: {:.2} μs/op ({:.0} ops/sec)", us_per_op, 1_000_000.0 / us_per_op);
    
    // Benchmark 5: Optimized get
    println!("\n5. Get operation (optimized, same key):");
    db_opt.put(b"opt_bench_key", value)?;
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(db_opt.get(b"opt_bench_key")?);
    }
    let duration = start.elapsed();
    let us_per_op = duration.as_micros() as f64 / iterations as f64;
    println!("   Total: {:.2} μs/op ({:.0} ops/sec)", us_per_op, 1_000_000.0 / us_per_op);
    
    // Test transaction overhead
    println!("\n6. Transaction overhead:");
    let start = Instant::now();
    for _ in 0..10000 {
        let tx_id = db_opt.begin_transaction()?;
        db_opt.commit_transaction(tx_id)?;
    }
    let duration = start.elapsed();
    let us_per_tx = duration.as_micros() as f64 / 10000.0;
    println!("   Empty transaction: {:.2} μs/tx", us_per_tx);
    
    // Test batch performance
    println!("\n7. Batch operations:");
    let batch_size = 1000;
    let mut batch = Vec::with_capacity(batch_size);
    for i in 0..batch_size {
        batch.push((format!("batch_{}", i).into_bytes(), value.to_vec()));
    }
    
    let start = Instant::now();
    for _ in 0..100 {
        db_opt.put_batch(&batch)?;
    }
    let duration = start.elapsed();
    let total_ops = 100 * batch_size;
    let us_per_op = duration.as_micros() as f64 / total_ops as f64;
    println!("   Batch put: {:.2} μs/op ({:.0} ops/sec)", us_per_op, 1_000_000.0 / us_per_op);
    
    Ok(())
}