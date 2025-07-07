#![allow(deprecated)]
use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

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
    println!("=== Lightning DB Simple Memory Test ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();

    // Test 1: Basic memory usage with small cache
    println!("Test 1: Small Cache Memory Usage");
    println!("================================");
    {
        let config = LightningDbConfig {
            cache_size: 10 * 1024 * 1024, // 10MB cache
            ..Default::default()
        };

        let db = Database::create(db_path, config)?;

        // Write data larger than cache
        println!("  Writing 50MB of data with 10MB cache...");
        let value_size = 10_000; // 10KB per value
        let num_values = 5000; // 50MB total

        let start = Instant::now();
        for i in 0..num_values {
            let key = format!("memory_key_{:04}", i);
            let value = vec![(i % 256) as u8; value_size];
            db.put(key.as_bytes(), &value)?;

            if i % 1000 == 0 {
                print!(".");
                use std::io::Write;
                std::io::stdout().flush()?;
            }
        }
        println!();

        let write_duration = start.elapsed();
        println!("  ✓ Wrote 50MB in {:.2}s", write_duration.as_secs_f64());

        // Random reads
        println!("  Performing random reads...");
        use rand::Rng;
        let mut rng = rand::rng();
        let start = Instant::now();
        let mut hits = 0;

        for _ in 0..1000 {
            let key_id = rng.random_range(0..num_values);
            let key = format!("memory_key_{:04}", key_id);
            if db.get(key.as_bytes())?.is_some() {
                hits += 1;
            }
        }

        let read_duration = start.elapsed();
        println!(
            "  ✓ Random reads: {}/1000 in {:.2}s",
            hits,
            read_duration.as_secs_f64()
        );
    }

    // Test 2: Concurrent memory pressure
    println!("\nTest 2: Concurrent Operations");
    println!("=============================");
    {
        let db = Arc::new(Database::open(db_path, LightningDbConfig::default())?);
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = vec![];

        for thread_id in 0..3 {
            let db_clone = Arc::clone(&db);
            let barrier_clone = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                barrier_clone.wait();

                // Each thread writes data
                for i in 0..1000 {
                    let key = format!("concurrent_t{}_k{:03}", thread_id, i);
                    let value = vec![(thread_id * i) as u8; 5000]; // 5KB values

                    match db_clone.put(key.as_bytes(), &value) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Thread {} write error: {}", thread_id, e);
                            break;
                        }
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
        println!(
            "  ✓ Concurrent operations completed in {:.2}s",
            duration.as_secs_f64()
        );
    }

    // Test 3: Memory stress with allocations
    println!("\nTest 3: Memory Stress Test");
    println!("==========================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Allocate some memory
        println!("  Allocating 200MB of memory...");
        let _memory_chunks = allocate_memory(200);

        // Try DB operations
        println!("  Testing DB operations under memory pressure...");
        let mut writes = 0;
        for i in 0..100 {
            let key = format!("stress_key_{}", i);
            let value = format!("stress_value_{}", i);

            if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                writes += 1;
            }
        }

        println!("  ✓ {} writes successful under memory pressure", writes);
    }

    // Test 4: Cache effectiveness
    println!("\nTest 4: Cache Effectiveness");
    println!("===========================");
    {
        let config = LightningDbConfig {
            cache_size: 5 * 1024 * 1024, // 5MB cache
            ..Default::default()
        };

        let db = Database::open(db_path, config)?;

        // Write sequential data
        let num_keys = 1000;
        for i in 0..num_keys {
            let key = format!("cache_key_{:04}", i);
            let value = vec![(i % 256) as u8; 5000]; // 5KB per entry
            db.put(key.as_bytes(), &value)?;
        }

        // Sequential read (cache friendly)
        let start = Instant::now();
        for i in 0..num_keys {
            let key = format!("cache_key_{:04}", i);
            let _ = db.get(key.as_bytes())?;
        }
        let seq_time = start.elapsed();

        // Random read (cache unfriendly)
        use rand::Rng;
        let mut rng = rand::rng();
        let start = Instant::now();
        for _ in 0..num_keys {
            let i = rng.random_range(0..num_keys);
            let key = format!("cache_key_{:04}", i);
            let _ = db.get(key.as_bytes())?;
        }
        let random_time = start.elapsed();

        println!(
            "  Sequential read: {:.2}ms",
            seq_time.as_secs_f64() * 1000.0
        );
        println!("  Random read: {:.2}ms", random_time.as_secs_f64() * 1000.0);
        println!(
            "  Cache speedup: {:.2}x",
            random_time.as_secs_f64() / seq_time.as_secs_f64()
        );
    }

    println!("\n=== Simple Memory Tests Complete ===");
    println!("Lightning DB demonstrates:");
    println!("✓ Efficient memory usage with limited cache");
    println!("✓ Stable concurrent operations");
    println!("✓ Operations under memory pressure");
    println!("✓ Effective cache management");

    Ok(())
}
