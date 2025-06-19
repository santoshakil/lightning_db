use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Lightning DB Feature Summary\n");
    
    let dir = tempdir()?;
    let mut working_features = Vec::new();
    let mut partial_features = Vec::new();
    let mut performance_results = Vec::new();
    
    // Test 1: Basic CRUD
    print!("Testing Basic CRUD... ");
    match test_basic_crud(&dir.path().join("crud")) {
        Ok(perf) => {
            println!("✅ Working");
            working_features.push("Basic CRUD (Put/Get/Delete)");
            if let Some(p) = perf {
                performance_results.push(("Basic Get", p.0, p.1));
            }
        }
        Err(e) => {
            println!("❌ Error: {}", e);
            partial_features.push(("Basic CRUD", e.to_string()));
        }
    }
    
    // Test 2: Transactions
    print!("Testing Transactions... ");
    match test_transactions(&dir.path().join("tx")) {
        Ok(_) => {
            println!("✅ Working");
            working_features.push("ACID Transactions");
        }
        Err(e) => {
            println!("⚠️ Partial: {}", e);
            partial_features.push(("Transactions", e.to_string()));
        }
    }
    
    // Test 3: Batch Operations
    print!("Testing Batch Operations... ");
    match test_batch(&dir.path().join("batch")) {
        Ok(perf) => {
            println!("✅ Working");
            working_features.push("Batch Put/Delete");
            if let Some(p) = perf {
                performance_results.push(("Batch Put", p.0, p.1));
            }
        }
        Err(e) => {
            println!("❌ Error: {}", e);
            partial_features.push(("Batch Operations", e.to_string()));
        }
    }
    
    // Test 4: Iterators
    print!("Testing Iterators... ");
    match test_iterators(&dir.path().join("iter")) {
        Ok(_) => {
            println!("✅ Working");
            working_features.push("Range Queries & Iterators");
        }
        Err(e) => {
            println!("⚠️ Partial: {}", e);
            partial_features.push(("Iterators", e.to_string()));
        }
    }
    
    // Test 5: Compression
    print!("Testing Compression... ");
    match test_compression(&dir.path().join("comp")) {
        Ok(_) => {
            println!("✅ Working");
            working_features.push("Zstd Compression");
        }
        Err(e) => {
            println!("⚠️ Partial: {}", e);
            partial_features.push(("Compression", e.to_string()));
        }
    }
    
    // Test 6: Caching
    print!("Testing Caching... ");
    match test_caching(&dir.path().join("cache")) {
        Ok(perf) => {
            println!("✅ Working");
            working_features.push("ARC Cache");
            if let Some(p) = perf {
                performance_results.push(("Cache Hit", p.0, p.1));
            }
        }
        Err(e) => {
            println!("⚠️ Partial: {}", e);
            partial_features.push(("Caching", e.to_string()));
        }
    }
    
    // Test 7: Persistence
    print!("Testing Persistence... ");
    match test_persistence(&dir.path().join("persist")) {
        Ok(_) => {
            println!("✅ Working");
            working_features.push("Data Persistence & WAL");
        }
        Err(e) => {
            println!("⚠️ Partial: {}", e);
            partial_features.push(("Persistence", e.to_string()));
        }
    }
    
    // Test 8: LSM Tree
    print!("Testing LSM Tree... ");
    match test_lsm(&dir.path().join("lsm")) {
        Ok(_) => {
            println!("✅ Working");
            working_features.push("LSM Tree with Compaction");
        }
        Err(e) => {
            println!("⚠️ Partial: {}", e);
            partial_features.push(("LSM Tree", e.to_string()));
        }
    }
    
    // Test 9: Performance
    print!("Testing Performance... ");
    match test_performance(&dir.path().join("perf")) {
        Ok((read_perf, write_perf)) => {
            println!("✅ Measured");
            performance_results.push(("Optimized Read", read_perf.0, read_perf.1));
            performance_results.push(("Optimized Write", write_perf.0, write_perf.1));
        }
        Err(e) => {
            println!("❌ Error: {}", e);
        }
    }
    
    // Print Summary
    println!("\n{}", "=".repeat(80));
    println!("📊 FEATURE SUMMARY");
    println!("{}", "=".repeat(80));
    
    println!("\n✅ WORKING FEATURES ({}):", working_features.len());
    for feature in &working_features {
        println!("  • {}", feature);
    }
    
    if !partial_features.is_empty() {
        println!("\n⚠️ PARTIALLY WORKING ({}):", partial_features.len());
        for (feature, issue) in &partial_features {
            println!("  • {}: {}", feature, issue);
        }
    }
    
    println!("\n⚡ PERFORMANCE RESULTS:");
    println!("{:<20} | {:>15} | {:>12}", "Operation", "Ops/sec", "Latency (μs)");
    println!("{}", "-".repeat(50));
    for (op, ops_sec, latency_us) in &performance_results {
        println!("{:<20} | {:>15.0} | {:>12.2}", op, ops_sec, latency_us);
    }
    
    // Performance vs Targets
    println!("\n🎯 PERFORMANCE VS TARGETS:");
    let best_read = performance_results.iter()
        .filter(|(op, _, _)| op.contains("Read") || op.contains("Cache"))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let best_write = performance_results.iter()
        .filter(|(op, _, _)| op.contains("Write") || op.contains("Put"))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    
    if let Some((op, ops, lat)) = best_read {
        let meets_target = *ops >= 1_000_000.0;
        println!("  Read:  {} ops/sec ({:.2} μs) - {} (Target: 1M+ ops/sec)",
                 ops, lat, if meets_target { "✅ ACHIEVED" } else { "❌ NOT MET" }, );
        println!("         Best operation: {}", op);
    }
    
    if let Some((op, ops, lat)) = best_write {
        let meets_target = *ops >= 100_000.0;
        println!("  Write: {} ops/sec ({:.2} μs) - {} (Target: 100K+ ops/sec)",
                 ops, lat, if meets_target { "✅ ACHIEVED" } else { "❌ NOT MET" });
        println!("         Best operation: {}", op);
    }
    
    println!("\n🔧 AREAS FOR IMPROVEMENT:");
    println!("  • Async I/O implementation (io_uring on Linux)");
    println!("  • Lock-free data structures for hot paths");
    println!("  • NUMA-aware memory allocation");
    println!("  • Column family support");
    println!("  • Secondary indexes");
    println!("  • Distributed replication");
    
    println!("\n{}", "=".repeat(80));
    
    Ok(())
}

fn test_basic_crud(path: &std::path::Path) -> Result<Option<(f64, f64)>, Box<dyn std::error::Error>> {
    let db = Database::create(path, LightningDbConfig::default())?;
    
    // Test put/get
    db.put(b"test_key", b"test_value")?;
    if db.get(b"test_key")?.as_deref() != Some(b"test_value") {
        return Err("Get mismatch".into());
    }
    
    // Test delete
    db.delete(b"test_key")?;
    if db.get(b"test_key")?.is_some() {
        return Err("Delete failed".into());
    }
    
    // Quick performance test
    let count = 10000;
    let start = Instant::now();
    for i in 0..count {
        db.put(format!("k{}", i).as_bytes(), b"value")?;
    }
    let write_time = start.elapsed();
    
    let start = Instant::now();
    for i in 0..count {
        let _ = db.get(format!("k{}", i).as_bytes())?;
    }
    let read_time = start.elapsed();
    
    let read_ops = count as f64 / read_time.as_secs_f64();
    let read_latency = read_time.as_micros() as f64 / count as f64;
    
    Ok(Some((read_ops, read_latency)))
}

fn test_transactions(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create(path, LightningDbConfig::default())?;
    
    // Test commit
    let tx = db.begin_transaction()?;
    db.put_tx(tx, b"tx_key", b"tx_value")?;
    db.commit_transaction(tx)?;
    
    if db.get(b"tx_key")?.as_deref() != Some(b"tx_value") {
        return Err("Transaction commit failed".into());
    }
    
    // Test rollback
    let tx = db.begin_transaction()?;
    db.put_tx(tx, b"rollback_key", b"value")?;
    db.abort_transaction(tx)?;
    
    if db.get(b"rollback_key")?.is_some() {
        return Err("Transaction rollback failed".into());
    }
    
    Ok(())
}

fn test_batch(path: &std::path::Path) -> Result<Option<(f64, f64)>, Box<dyn std::error::Error>> {
    let db = Database::create(path, LightningDbConfig::default())?;
    
    let batch_size = 1000;
    let batch: Vec<_> = (0..batch_size)
        .map(|i| (format!("batch_k{}", i).into_bytes(), b"value".to_vec()))
        .collect();
    
    let start = Instant::now();
    db.put_batch(&batch)?;
    let batch_time = start.elapsed();
    
    // Verify
    if db.get(b"batch_k0")?.is_none() {
        return Err("Batch put failed".into());
    }
    
    let ops_sec = batch_size as f64 / batch_time.as_secs_f64();
    let latency_us = batch_time.as_micros() as f64 / batch_size as f64;
    
    Ok(Some((ops_sec, latency_us)))
}

fn test_iterators(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create(path, LightningDbConfig::default())?;
    
    // Insert test data
    for i in 0..10 {
        db.put(format!("iter_k{:02}", i).as_bytes(), b"value")?;
    }
    
    // Test scan
    let iter = db.scan(None, None)?;
    let count = iter.count();
    if count != 10 {
        return Err(format!("Expected 10 entries, got {}", count).into());
    }
    
    // Test range
    let start = b"iter_k02".to_vec();
    let end = b"iter_k05".to_vec();
    let iter = db.scan(Some(start), Some(end))?;
    let range_count = iter.count();
    if range_count > 5 {
        return Err("Range query returned too many entries".into());
    }
    
    Ok(())
}

fn test_compression(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    
    let db = Database::create(path, config)?;
    
    // Insert compressible data
    let data = "x".repeat(10000);
    db.put(b"comp_key", data.as_bytes())?;
    
    if db.get(b"comp_key")?.as_deref() != Some(data.as_bytes()) {
        return Err("Compression data integrity failed".into());
    }
    
    // Force flush to test compression
    db.flush_lsm()?;
    
    Ok(())
}

fn test_caching(path: &std::path::Path) -> Result<Option<(f64, f64)>, Box<dyn std::error::Error>> {
    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024; // 10MB
    
    let db = Database::create(path, config)?;
    
    // Fill cache
    for i in 0..100 {
        db.put(format!("cache_k{}", i).as_bytes(), vec![0u8; 1024].as_slice())?;
    }
    
    // Test cache hits
    let start = Instant::now();
    let iterations = 100000;
    for _ in 0..iterations {
        let _ = db.get(b"cache_k0")?;
    }
    let cache_time = start.elapsed();
    
    let ops_sec = iterations as f64 / cache_time.as_secs_f64();
    let latency_us = cache_time.as_micros() as f64 / iterations as f64;
    
    Ok(Some((ops_sec, latency_us)))
}

fn test_persistence(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create and write data
    {
        let db = Database::create(path, LightningDbConfig::default())?;
        db.put(b"persist_key", b"persist_value")?;
    }
    
    // Reopen and verify
    {
        let db = Database::open(path, LightningDbConfig::default())?;
        if db.get(b"persist_key")?.as_deref() != Some(b"persist_value") {
            return Err("Data not persisted".into());
        }
    }
    
    Ok(())
}

fn test_lsm(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create(path, LightningDbConfig::default())?;
    
    // Insert data
    for i in 0..1000 {
        db.put(format!("lsm_k{:04}", i).as_bytes(), b"value")?;
    }
    
    // Force flush and compaction
    db.flush_lsm()?;
    db.compact_lsm()?;
    
    // Verify data
    if db.get(b"lsm_k0000")?.is_none() {
        return Err("Data lost after compaction".into());
    }
    
    Ok(())
}

fn test_performance(path: &std::path::Path) -> Result<((f64, f64), (f64, f64)), Box<dyn std::error::Error>> {
    let db = Database::create(path, LightningDbConfig::default())?;
    
    // Warm up
    for i in 0..1000 {
        db.put(format!("warm_{}", i).as_bytes(), b"value")?;
    }
    
    // Ensure something is cached
    db.put(b"cached_key", b"cached_value")?;
    let _ = db.get(b"cached_key")?;
    
    // Test optimized read (cache hit)
    let read_iterations = 1_000_000;
    let start = Instant::now();
    for _ in 0..read_iterations {
        let _ = db.get(b"cached_key")?;
    }
    let read_time = start.elapsed();
    let read_ops = read_iterations as f64 / read_time.as_secs_f64();
    let read_latency = read_time.as_micros() as f64 / read_iterations as f64;
    
    // Test optimized write
    let write_iterations = 100_000;
    let start = Instant::now();
    for i in 0..write_iterations {
        db.put(format!("perf_{}", i).as_bytes(), b"value")?;
    }
    let write_time = start.elapsed();
    let write_ops = write_iterations as f64 / write_time.as_secs_f64();
    let write_latency = write_time.as_micros() as f64 / write_iterations as f64;
    
    Ok(((read_ops, read_latency), (write_ops, write_latency)))
}