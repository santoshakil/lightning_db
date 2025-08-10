use lightning_db::{Database, LightningDbConfig};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use std::fs;

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.2} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} bytes", bytes)
    }
}

fn get_dir_size(path: &std::path::Path) -> u64 {
    let mut size = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries {
            if let Ok(entry) = entry {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        size += metadata.len();
                    } else if metadata.is_dir() {
                        size += get_dir_size(&entry.path());
                    }
                }
            }
        }
    }
    size
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Large Dataset Test");
    println!("================================\n");

    let dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    config.cache_size = 256 * 1024 * 1024; // 256MB cache
    
    println!("Creating database with 256MB cache...");
    let db = Database::create(dir.path(), config)?;
    
    // Test parameters
    const RECORD_SIZE: usize = 1024; // 1KB per record
    const TARGET_SIZE: u64 = 1_073_741_824; // 1GB
    const NUM_RECORDS: usize = (TARGET_SIZE / RECORD_SIZE as u64) as usize;
    const BATCH_SIZE: usize = 1000;
    
    println!("Target dataset size: {}", format_bytes(TARGET_SIZE));
    println!("Record size: {} bytes", RECORD_SIZE);
    println!("Number of records: {}", NUM_RECORDS);
    println!("Batch size: {}\n", BATCH_SIZE);
    
    // Generate test data
    let test_value = vec![b'X'; RECORD_SIZE];
    
    // Phase 1: Write test
    println!("Phase 1: Writing {} records...", NUM_RECORDS);
    let write_start = Instant::now();
    let mut written = 0;
    let mut last_report = Instant::now();
    
    for batch_num in 0..(NUM_RECORDS / BATCH_SIZE) {
        for i in 0..BATCH_SIZE {
            let key = format!("key_{:010}", batch_num * BATCH_SIZE + i);
            db.put(key.as_bytes(), &test_value)?;
            written += 1;
        }
        
        // Progress report every second
        if last_report.elapsed() > Duration::from_secs(1) {
            let progress = (written as f64 / NUM_RECORDS as f64) * 100.0;
            let elapsed = write_start.elapsed().as_secs_f64();
            let rate = written as f64 / elapsed;
            let eta = ((NUM_RECORDS - written) as f64 / rate) as u64;
            
            println!("  Progress: {:.1}% ({}/{}) - {:.0} records/sec - ETA: {}s",
                    progress, written, NUM_RECORDS, rate, eta);
            last_report = Instant::now();
        }
    }
    
    // Write remaining records
    let remaining = NUM_RECORDS % BATCH_SIZE;
    if remaining > 0 {
        for i in 0..remaining {
            let key = format!("key_{:010}", (NUM_RECORDS / BATCH_SIZE) * BATCH_SIZE + i);
            db.put(key.as_bytes(), &test_value)?;
            written += 1;
        }
    }
    
    let write_duration = write_start.elapsed();
    let write_throughput = NUM_RECORDS as f64 / write_duration.as_secs_f64();
    let write_mb_per_sec = (TARGET_SIZE as f64 / 1_048_576.0) / write_duration.as_secs_f64();
    
    println!("  Write completed in {:.2}s", write_duration.as_secs_f64());
    println!("  Throughput: {:.0} records/sec", write_throughput);
    println!("  Data rate: {:.2} MB/s\n", write_mb_per_sec);
    
    // Check disk usage
    let db_size = get_dir_size(dir.path());
    println!("Database size on disk: {}", format_bytes(db_size));
    let compression_ratio = if db_size > 0 {
        TARGET_SIZE as f64 / db_size as f64
    } else {
        0.0
    };
    println!("Compression ratio: {:.2}x\n", compression_ratio);
    
    // Phase 2: Random read test
    println!("Phase 2: Random read test (10,000 reads)...");
    let read_start = Instant::now();
    let mut successful_reads = 0;
    
    for i in 0..10000 {
        let key_num = (i * 100) % NUM_RECORDS; // Spread reads across dataset
        let key = format!("key_{:010}", key_num);
        
        if let Ok(Some(value)) = db.get(key.as_bytes()) {
            if value.len() == RECORD_SIZE {
                successful_reads += 1;
            }
        }
    }
    
    let read_duration = read_start.elapsed();
    let read_latency_us = read_duration.as_micros() as f64 / 10000.0;
    let read_throughput = 10000.0 / read_duration.as_secs_f64();
    
    println!("  Successful reads: {}/10000", successful_reads);
    println!("  Average latency: {:.2} μs", read_latency_us);
    println!("  Throughput: {:.0} reads/sec\n", read_throughput);
    
    // Phase 3: Sequential scan test
    println!("Phase 3: Sequential scan test (first 10,000 records)...");
    let scan_start = Instant::now();
    let mut scanned = 0;
    
    for i in 0..10000 {
        let key = format!("key_{:010}", i);
        if db.get(key.as_bytes())?.is_some() {
            scanned += 1;
        }
    }
    
    let scan_duration = scan_start.elapsed();
    let scan_rate = 10000.0 / scan_duration.as_secs_f64();
    
    println!("  Records found: {}/10000", scanned);
    println!("  Scan rate: {:.0} records/sec\n", scan_rate);
    
    // Phase 4: Update test
    println!("Phase 4: Update test (1,000 random updates)...");
    let update_value = vec![b'Y'; RECORD_SIZE];
    let update_start = Instant::now();
    let mut successful_updates = 0;
    
    for i in 0..1000 {
        let key_num = (i * 1000) % NUM_RECORDS;
        let key = format!("key_{:010}", key_num);
        
        db.put(key.as_bytes(), &update_value)?;
        successful_updates += 1;
    }
    
    let update_duration = update_start.elapsed();
    let update_throughput = 1000.0 / update_duration.as_secs_f64();
    
    println!("  Updates completed: {}/1000", successful_updates);
    println!("  Update throughput: {:.0} updates/sec\n", update_throughput);
    
    // Phase 5: Verify data integrity
    println!("Phase 5: Data integrity verification...");
    let mut verification_errors = 0;
    
    // Check a sample of records
    for i in (0..NUM_RECORDS).step_by(10000) {
        let key = format!("key_{:010}", i);
        match db.get(key.as_bytes())? {
            Some(value) => {
                if value.len() != RECORD_SIZE {
                    verification_errors += 1;
                    println!("  ERROR: Record {} has wrong size: {} bytes", i, value.len());
                }
            }
            None => {
                verification_errors += 1;
                println!("  ERROR: Record {} not found", i);
            }
        }
    }
    
    if verification_errors == 0 {
        println!("  ✅ All sampled records verified successfully");
    } else {
        println!("  ❌ Found {} verification errors", verification_errors);
    }
    
    // Final summary
    println!("\n========== Summary ==========");
    println!("Dataset size: {}", format_bytes(TARGET_SIZE));
    println!("Records written: {}", NUM_RECORDS);
    println!("Database size: {}", format_bytes(db_size));
    println!("Compression: {:.2}x", compression_ratio);
    println!("Write performance: {:.2} MB/s", write_mb_per_sec);
    println!("Read latency: {:.2} μs", read_latency_us);
    println!("Read throughput: {:.0} reads/sec", read_throughput);
    println!("Update throughput: {:.0} updates/sec", update_throughput);
    println!("Data integrity: {} errors", verification_errors);
    
    let passed = successful_reads == 10000 && 
                 scanned == 10000 && 
                 verification_errors == 0 &&
                 write_mb_per_sec > 10.0;
    
    if passed {
        println!("\n🎉 PASSED: Large dataset test successful!");
        println!("Lightning DB successfully handled 1GB+ dataset");
    } else {
        println!("\n⚠️  Some issues detected:");
        if successful_reads < 10000 {
            println!("  - Read errors: {} missing", 10000 - successful_reads);
        }
        if scanned < 10000 {
            println!("  - Scan errors: {} missing", 10000 - scanned);
        }
        if verification_errors > 0 {
            println!("  - Data integrity errors: {}", verification_errors);
        }
        if write_mb_per_sec <= 10.0 {
            println!("  - Write performance below threshold: {:.2} MB/s", write_mb_per_sec);
        }
    }
    
    Ok(())
}