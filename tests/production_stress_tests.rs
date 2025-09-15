use lightning_db::{Database, LightningDbConfig};
use rand::{Rng, RngCore};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

mod common;

const MB: u64 = 1024 * 1024;
// GB constant removed as it's unused

// High Volume Stress Tests

#[test]
#[ignore] // Long-running test
fn test_sustained_high_write_throughput_1m() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 500 * MB,
        max_active_transactions: 1000,
        compression_enabled: false, // Disable for performance
        enable_statistics: false,   // Disable for performance
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const TOTAL_OPS: usize = 1_000_000;
    const BATCH_SIZE: usize = 1000;
    const VALUE_SIZE: usize = 100;

    let start = Instant::now();
    let mut ops_completed = 0;

    println!("Starting 1M write operations stress test...");

    for batch_idx in 0..(TOTAL_OPS / BATCH_SIZE) {
        let mut batch_ops = Vec::with_capacity(BATCH_SIZE);

        for i in 0..BATCH_SIZE {
            let key_id = batch_idx * BATCH_SIZE + i;
            let key = format!("stress_key_{:08}", key_id);
            let mut value = vec![0u8; VALUE_SIZE];
            rand::thread_rng().fill_bytes(&mut value);

            batch_ops.push((key.into_bytes(), value));
        }

        db.put_batch(&batch_ops).unwrap();
        ops_completed += BATCH_SIZE;

        if ops_completed % 50_000 == 0 {
            let elapsed = start.elapsed();
            let ops_per_sec = ops_completed as f64 / elapsed.as_secs_f64();
            println!(
                "Progress: {}/{} ops ({:.0} ops/sec)",
                ops_completed, TOTAL_OPS, ops_per_sec
            );
        }
    }

    let total_duration = start.elapsed();
    let final_ops_per_sec = TOTAL_OPS as f64 / total_duration.as_secs_f64();

    println!(
        "Completed 1M writes in {:.2}s ({:.0} ops/sec)",
        total_duration.as_secs_f64(),
        final_ops_per_sec
    );

    // Verify data integrity
    println!("Verifying data integrity...");
    let verify_start = Instant::now();
    let mut verified = 0;

    for i in 0..TOTAL_OPS {
        let key = format!("stress_key_{:08}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Missing key at index {}", i);
        assert_eq!(value.as_ref().unwrap().len(), VALUE_SIZE);
        verified += 1;

        if verified % 100_000 == 0 {
            println!("Verified {}/{} entries", verified, TOTAL_OPS);
        }
    }

    let verify_duration = verify_start.elapsed();
    let verify_ops_per_sec = TOTAL_OPS as f64 / verify_duration.as_secs_f64();

    println!(
        "Verified 1M entries in {:.2}s ({:.0} ops/sec)",
        verify_duration.as_secs_f64(),
        verify_ops_per_sec
    );

    assert!(
        final_ops_per_sec > 50_000.0,
        "Write throughput too low: {:.0}",
        final_ops_per_sec
    );
    assert!(
        verify_ops_per_sec > 100_000.0,
        "Read verification too slow: {:.0}",
        verify_ops_per_sec
    );
}

#[test]
#[ignore] // Long-running test
fn test_sustained_high_read_throughput_cache_pressure() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 50 * MB, // Small cache to create pressure
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const NUM_KEYS: usize = 500_000;
    const READ_ITERATIONS: usize = 2_000_000;
    const VALUE_SIZE: usize = 200;

    println!("Setting up {} keys for cache pressure test...", NUM_KEYS);

    // Setup: Create keys larger than cache
    for i in 0..NUM_KEYS {
        let key = format!("cache_test_key_{:08}", i);
        let mut value = vec![0u8; VALUE_SIZE];
        rand::thread_rng().fill_bytes(&mut value);
        db.put(key.as_bytes(), &value).unwrap();
    }

    println!(
        "Starting {} random reads with cache pressure...",
        READ_ITERATIONS
    );

    let start = Instant::now();
    let mut cache_hits = 0;
    let mut cache_misses = 0;

    for i in 0..READ_ITERATIONS {
        let key_idx = rand::thread_rng().gen_range(0..NUM_KEYS);
        let key = format!("cache_test_key_{:08}", key_idx);

        let read_start = Instant::now();
        let value = db.get(key.as_bytes()).unwrap();
        let read_duration = read_start.elapsed();

        assert!(value.is_some());
        assert_eq!(value.unwrap().len(), VALUE_SIZE);

        // Classify as cache hit/miss based on latency
        if read_duration.as_nanos() < 1_000 {
            // < 1μs = likely cache hit
            cache_hits += 1;
        } else {
            cache_misses += 1;
        }

        if i % 100_000 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let ops_per_sec = i as f64 / elapsed.as_secs_f64();
            let hit_ratio = cache_hits as f64 / (cache_hits + cache_misses) as f64;
            println!(
                "Progress: {}/{} reads ({:.0} ops/sec, {:.1}% hit ratio)",
                i,
                READ_ITERATIONS,
                ops_per_sec,
                hit_ratio * 100.0
            );
        }
    }

    let total_duration = start.elapsed();
    let final_ops_per_sec = READ_ITERATIONS as f64 / total_duration.as_secs_f64();
    let final_hit_ratio = cache_hits as f64 / (cache_hits + cache_misses) as f64;

    println!(
        "Completed {} reads in {:.2}s ({:.0} ops/sec, {:.1}% hit ratio)",
        READ_ITERATIONS,
        total_duration.as_secs_f64(),
        final_ops_per_sec,
        final_hit_ratio * 100.0
    );

    assert!(
        final_ops_per_sec > 100_000.0,
        "Read throughput under pressure too low: {:.0}",
        final_ops_per_sec
    );
}

#[test]
#[ignore] // Long-running test
fn test_mixed_workload_patterns() {
    let test_patterns = [
        (80, 20, "80/20 read/write"),
        (50, 50, "50/50 read/write"),
        (20, 80, "20/80 read/write"),
    ];

    for (read_pct, write_pct, pattern_name) in test_patterns {
        println!("Testing {} pattern...", pattern_name);

        let dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 100 * MB,
            max_active_transactions: 500,
            ..Default::default()
        };
        let db = Arc::new(Database::create(dir.path(), config).unwrap());

        const TOTAL_OPS: usize = 100_000;
        const NUM_THREADS: usize = 10;
        const INITIAL_KEYS: usize = 10_000;

        // Pre-populate with initial data
        for i in 0..INITIAL_KEYS {
            let key = format!("mixed_key_{:08}", i);
            let value = format!("initial_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let start = Instant::now();
        let completed_ops = Arc::new(AtomicUsize::new(0));
        let read_ops = Arc::new(AtomicUsize::new(0));
        let write_ops = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        for _ in 0..NUM_THREADS {
            let db = db.clone();
            let completed = completed_ops.clone();
            let reads = read_ops.clone();
            let writes = write_ops.clone();

            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();

                while completed.load(Ordering::Relaxed) < TOTAL_OPS {
                    let op_choice = rng.gen_range(0..100);

                    if op_choice < read_pct {
                        // Read operation
                        let key_idx = rng.gen_range(0..INITIAL_KEYS + 1000);
                        let key = format!("mixed_key_{:08}", key_idx);
                        let _ = db.get(key.as_bytes()).unwrap();
                        reads.fetch_add(1, Ordering::Relaxed);
                    } else {
                        // Write operation
                        let key_idx = rng.gen_range(0..INITIAL_KEYS + 10000);
                        let key = format!("mixed_key_{:08}", key_idx);
                        let value = format!(
                            "updated_value_{}_{}",
                            key_idx,
                            completed.load(Ordering::Relaxed)
                        );
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                        writes.fetch_add(1, Ordering::Relaxed);
                    }

                    completed.fetch_add(1, Ordering::Relaxed);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_completed = completed_ops.load(Ordering::Relaxed);
        let total_reads = read_ops.load(Ordering::Relaxed);
        let total_writes = write_ops.load(Ordering::Relaxed);
        let ops_per_sec = total_completed as f64 / duration.as_secs_f64();

        println!(
            "{}: {} ops in {:.2}s ({:.0} ops/sec, {} reads, {} writes)",
            pattern_name,
            total_completed,
            duration.as_secs_f64(),
            ops_per_sec,
            total_reads,
            total_writes
        );

        assert!(
            ops_per_sec > 10_000.0,
            "{} pattern too slow: {:.0} ops/sec",
            pattern_name,
            ops_per_sec
        );
    }
}

#[test]
#[ignore] // Long-running test
fn test_large_batch_operations() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 200 * MB,
        max_active_transactions: 100,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const BATCH_SIZES: &[usize] = &[1_000, 5_000, 10_000, 25_000];

    for &batch_size in BATCH_SIZES {
        println!("Testing batch size: {} operations", batch_size);

        let start = Instant::now();

        // Create large batch
        let mut batch_ops = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let key = format!("batch_{}_{:08}", batch_size, i);
            let value = format!("batch_value_{}_{}", batch_size, i);
            batch_ops.push((key.into_bytes(), value.into_bytes()));
        }

        let batch_creation_time = start.elapsed();

        // Execute batch
        let batch_start = Instant::now();
        db.put_batch(&batch_ops).unwrap();
        let batch_duration = batch_start.elapsed();

        // Verify batch
        let verify_start = Instant::now();
        for i in 0..batch_size {
            let key = format!("batch_{}_{:08}", batch_size, i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some());
        }
        let verify_duration = verify_start.elapsed();

        let batch_ops_per_sec = batch_size as f64 / batch_duration.as_secs_f64();
        let verify_ops_per_sec = batch_size as f64 / verify_duration.as_secs_f64();

        println!("Batch {}: creation={:.2}ms, write={:.2}ms ({:.0} ops/sec), verify={:.2}ms ({:.0} ops/sec)",
                 batch_size,
                 batch_creation_time.as_millis(),
                 batch_duration.as_millis(), batch_ops_per_sec,
                 verify_duration.as_millis(), verify_ops_per_sec);

        assert!(
            batch_ops_per_sec > 20_000.0,
            "Batch {} too slow: {:.0} ops/sec",
            batch_size,
            batch_ops_per_sec
        );
    }
}

// Concurrency Stress Tests

#[test]
#[ignore] // Long-running test
fn test_100_concurrent_readers() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * MB,
        ..Default::default()
    };
    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const NUM_KEYS: usize = 50_000;
    const NUM_READERS: usize = 100;
    const READS_PER_READER: usize = 1_000;

    println!("Setting up {} keys for concurrent reader test...", NUM_KEYS);

    // Setup data
    for i in 0..NUM_KEYS {
        let key = format!("concurrent_read_key_{:08}", i);
        let value = format!("concurrent_read_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    println!("Starting {} concurrent readers...", NUM_READERS);

    let barrier = Arc::new(Barrier::new(NUM_READERS));
    let start_time = Arc::new(Mutex::new(None));
    let completed_reads = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for reader_id in 0..NUM_READERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let start_time = start_time.clone();
        let completed_reads = completed_reads.clone();
        let error_count = error_count.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            // Record start time from first thread
            let mut start_guard = start_time.lock().unwrap();
            if start_guard.is_none() {
                *start_guard = Some(Instant::now());
            }
            drop(start_guard);

            let mut rng = rand::thread_rng();
            let mut local_reads = 0;

            for _ in 0..READS_PER_READER {
                let key_idx = rng.gen_range(0..NUM_KEYS);
                let key = format!("concurrent_read_key_{:08}", key_idx);

                match db.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        assert!(!value.is_empty());
                        local_reads += 1;
                    }
                    Ok(None) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            completed_reads.fetch_add(local_reads, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let start_time = start_time.lock().unwrap().unwrap();
    let duration = start_time.elapsed();
    let total_reads = completed_reads.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    let reads_per_sec = total_reads as f64 / duration.as_secs_f64();

    println!(
        "Completed {} concurrent readers: {} reads in {:.2}s ({:.0} reads/sec, {} errors)",
        NUM_READERS,
        total_reads,
        duration.as_secs_f64(),
        reads_per_sec,
        errors
    );

    assert_eq!(errors, 0, "Should have no read errors");
    assert!(
        reads_per_sec > 50_000.0,
        "Concurrent read performance too low: {:.0}",
        reads_per_sec
    );
    assert_eq!(total_reads, NUM_READERS * READS_PER_READER, "Missing reads");
}

#[test]
#[ignore] // Long-running test
fn test_50_concurrent_writers() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * MB,
        max_active_transactions: 100,
        ..Default::default()
    };
    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const NUM_WRITERS: usize = 50;
    const WRITES_PER_WRITER: usize = 2_000;

    println!("Starting {} concurrent writers...", NUM_WRITERS);

    let barrier = Arc::new(Barrier::new(NUM_WRITERS));
    let start_time = Arc::new(Mutex::new(None));
    let completed_writes = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for writer_id in 0..NUM_WRITERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let start_time = start_time.clone();
        let completed_writes = completed_writes.clone();
        let error_count = error_count.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            // Record start time from first thread
            let mut start_guard = start_time.lock().unwrap();
            if start_guard.is_none() {
                *start_guard = Some(Instant::now());
            }
            drop(start_guard);

            let mut local_writes = 0;

            for i in 0..WRITES_PER_WRITER {
                let key = format!("concurrent_write_{}_{:08}", writer_id, i);
                let value = format!("concurrent_value_{}_{}", writer_id, i);

                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        local_writes += 1;
                    }
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            completed_writes.fetch_add(local_writes, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let start_time = start_time.lock().unwrap().unwrap();
    let duration = start_time.elapsed();
    let total_writes = completed_writes.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    let writes_per_sec = total_writes as f64 / duration.as_secs_f64();

    println!(
        "Completed {} concurrent writers: {} writes in {:.2}s ({:.0} writes/sec, {} errors)",
        NUM_WRITERS,
        total_writes,
        duration.as_secs_f64(),
        writes_per_sec,
        errors
    );

    // Verify all data was written correctly
    println!("Verifying concurrent write data integrity...");
    let mut verified = 0;
    for writer_id in 0..NUM_WRITERS {
        for i in 0..WRITES_PER_WRITER {
            let key = format!("concurrent_write_{}_{:08}", writer_id, i);
            let expected_value = format!("concurrent_value_{}_{}", writer_id, i);

            match db.get(key.as_bytes()).unwrap() {
                Some(actual_value) => {
                    assert_eq!(actual_value, expected_value.as_bytes());
                    verified += 1;
                }
                None => {
                    panic!("Missing key: {}", key);
                }
            }
        }
    }

    println!("Verified {} written entries", verified);

    assert_eq!(errors, 0, "Should have no write errors");
    assert!(
        writes_per_sec > 10_000.0,
        "Concurrent write performance too low: {:.0}",
        writes_per_sec
    );
    assert_eq!(
        total_writes,
        NUM_WRITERS * WRITES_PER_WRITER,
        "Missing writes"
    );
    assert_eq!(
        verified,
        NUM_WRITERS * WRITES_PER_WRITER,
        "Data integrity check failed"
    );
}

#[test]
#[ignore] // Long-running test
fn test_reader_writer_contention() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 50 * MB,
        max_active_transactions: 200,
        ..Default::default()
    };
    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const NUM_READERS: usize = 75;
    const NUM_WRITERS: usize = 25;
    const INITIAL_KEYS: usize = 10_000;
    const OPS_PER_THREAD: usize = 1_000;
    const TEST_DURATION: Duration = Duration::from_secs(30);

    println!(
        "Setting up {} initial keys for contention test...",
        INITIAL_KEYS
    );

    // Setup initial data
    for i in 0..INITIAL_KEYS {
        let key = format!("contention_key_{:08}", i);
        let value = format!("initial_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    println!(
        "Starting reader-writer contention test: {} readers, {} writers for {:?}...",
        NUM_READERS, NUM_WRITERS, TEST_DURATION
    );

    let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let read_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));
    let read_errors = Arc::new(AtomicUsize::new(0));
    let write_errors = Arc::new(AtomicUsize::new(0));

    let start_time = Instant::now();
    let mut handles = vec![];

    // Spawn readers
    for reader_id in 0..NUM_READERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let stop_flag = stop_flag.clone();
        let read_count = read_count.clone();
        let read_errors = read_errors.clone();

        let handle = thread::spawn(move || {
            barrier.wait();
            let mut rng = rand::thread_rng();
            let mut local_reads = 0;
            let mut local_errors = 0;

            while !stop_flag.load(Ordering::Relaxed) {
                let key_idx = rng.gen_range(0..INITIAL_KEYS + 1000);
                let key = format!("contention_key_{:08}", key_idx);

                match db.get(key.as_bytes()) {
                    Ok(_) => local_reads += 1,
                    Err(_) => local_errors += 1,
                }

                if local_reads % 1000 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }

            read_count.fetch_add(local_reads, Ordering::Relaxed);
            read_errors.fetch_add(local_errors, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    // Spawn writers
    for writer_id in 0..NUM_WRITERS {
        let db = db.clone();
        let barrier = barrier.clone();
        let stop_flag = stop_flag.clone();
        let write_count = write_count.clone();
        let write_errors = write_errors.clone();

        let handle = thread::spawn(move || {
            barrier.wait();
            let mut rng = rand::thread_rng();
            let mut local_writes = 0;
            let mut local_errors = 0;

            while !stop_flag.load(Ordering::Relaxed) {
                let key_idx = rng.gen_range(0..INITIAL_KEYS + 1000);
                let key = format!("contention_key_{:08}", key_idx);
                let value = format!("updated_value_{}_{}", writer_id, local_writes);

                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => local_writes += 1,
                    Err(_) => local_errors += 1,
                }

                if local_writes % 100 == 0 {
                    thread::sleep(Duration::from_millis(5));
                }
            }

            write_count.fetch_add(local_writes, Ordering::Relaxed);
            write_errors.fetch_add(local_errors, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    // Let test run for specified duration
    thread::sleep(TEST_DURATION);
    stop_flag.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start_time.elapsed();
    let total_reads = read_count.load(Ordering::Relaxed);
    let total_writes = write_count.load(Ordering::Relaxed);
    let read_errs = read_errors.load(Ordering::Relaxed);
    let write_errs = write_errors.load(Ordering::Relaxed);

    let reads_per_sec = total_reads as f64 / duration.as_secs_f64();
    let writes_per_sec = total_writes as f64 / duration.as_secs_f64();

    println!(
        "Contention test completed in {:.2}s:",
        duration.as_secs_f64()
    );
    println!(
        "  Reads: {} ({:.0}/sec, {} errors)",
        total_reads, reads_per_sec, read_errs
    );
    println!(
        "  Writes: {} ({:.0}/sec, {} errors)",
        total_writes, writes_per_sec, write_errs
    );
    println!(
        "  Total ops: {} ({:.0}/sec)",
        total_reads + total_writes,
        (total_reads + total_writes) as f64 / duration.as_secs_f64()
    );

    assert!(
        reads_per_sec > 1_000.0,
        "Read performance under contention too low: {:.0}",
        reads_per_sec
    );
    assert!(
        writes_per_sec > 500.0,
        "Write performance under contention too low: {:.0}",
        writes_per_sec
    );
    assert!(
        (read_errs as f64 / total_reads as f64) < 0.01,
        "Too many read errors: {}/{}",
        read_errs,
        total_reads
    );
    assert!(
        (write_errs as f64 / total_writes as f64) < 0.01,
        "Too many write errors: {}/{}",
        write_errs,
        total_writes
    );
}

#[test]
#[ignore] // Long-running test
fn test_transaction_isolation_extreme_concurrency() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * MB,
        max_active_transactions: 500,
        ..Default::default()
    };
    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const NUM_CONCURRENT_TXS: usize = 100;
    const OPS_PER_TX: usize = 50;
    const SHARED_KEYS: usize = 100;

    println!(
        "Testing transaction isolation with {} concurrent transactions...",
        NUM_CONCURRENT_TXS
    );

    // Setup initial shared data
    for i in 0..SHARED_KEYS {
        let key = format!("shared_key_{:04}", i);
        let value = format!("initial_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let barrier = Arc::new(Barrier::new(NUM_CONCURRENT_TXS));
    let successful_commits = Arc::new(AtomicUsize::new(0));
    let failed_commits = Arc::new(AtomicUsize::new(0));
    let isolation_violations = Arc::new(AtomicUsize::new(0));

    let start_time = Instant::now();
    let mut handles = vec![];

    for tx_idx in 0..NUM_CONCURRENT_TXS {
        let db = db.clone();
        let barrier = barrier.clone();
        let successful_commits = successful_commits.clone();
        let failed_commits = failed_commits.clone();
        let isolation_violations = isolation_violations.clone();

        let handle = thread::spawn(move || {
            barrier.wait();
            let mut rng = rand::thread_rng();

            match db.begin_transaction() {
                Ok(tx_id) => {
                    let mut tx_successful = true;
                    let mut initial_values = HashMap::new();

                    // Phase 1: Read initial values within transaction
                    for _ in 0..OPS_PER_TX / 2 {
                        let key_idx = rng.gen_range(0..SHARED_KEYS);
                        let key = format!("shared_key_{:04}", key_idx);

                        match db.get(key.as_bytes()) {
                            Ok(Some(value)) => {
                                initial_values.insert(key.clone(), value.clone());
                            }
                            Ok(None) => {
                                isolation_violations.fetch_add(1, Ordering::Relaxed);
                                tx_successful = false;
                                break;
                            }
                            Err(_) => {
                                tx_successful = false;
                                break;
                            }
                        }
                    }

                    // Phase 2: Write operations within transaction
                    if tx_successful {
                        for _ in 0..OPS_PER_TX / 2 {
                            let key_idx = rng.gen_range(0..SHARED_KEYS);
                            let key = format!("shared_key_{:04}", key_idx);
                            let value = format!("tx_{}_{}_value", tx_idx, key_idx);

                            if let Err(_) = db.put_tx(tx_id, key.as_bytes(), value.as_bytes()) {
                                tx_successful = false;
                                break;
                            }
                        }
                    }

                    // Phase 3: Re-read to check consistency
                    if tx_successful {
                        for (key, initial_value) in initial_values {
                            match db.get(key.as_bytes()) {
                                Ok(Some(current_value)) => {
                                    // Value should either be unchanged or our transaction's value
                                    if current_value != initial_value {
                                        let our_value = format!("tx_{}_", tx_idx);
                                        if !String::from_utf8_lossy(&current_value)
                                            .starts_with(&our_value)
                                        {
                                            isolation_violations.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                                Ok(None) => {
                                    isolation_violations.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    tx_successful = false;
                                    break;
                                }
                            }
                        }
                    }

                    // Commit transaction
                    if tx_successful {
                        match db.commit_transaction(tx_id) {
                            Ok(_) => {
                                successful_commits.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                failed_commits.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        let _ = db.abort_transaction(tx_id);
                        failed_commits.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    failed_commits.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start_time.elapsed();
    let successful = successful_commits.load(Ordering::Relaxed);
    let failed = failed_commits.load(Ordering::Relaxed);
    let violations = isolation_violations.load(Ordering::Relaxed);
    let success_rate = successful as f64 / NUM_CONCURRENT_TXS as f64;

    println!(
        "Transaction isolation test completed in {:.2}s:",
        duration.as_secs_f64()
    );
    println!(
        "  Successful commits: {} ({:.1}%)",
        successful,
        success_rate * 100.0
    );
    println!("  Failed commits: {}", failed);
    println!("  Isolation violations: {}", violations);

    assert_eq!(
        successful + failed,
        NUM_CONCURRENT_TXS,
        "Missing transactions"
    );
    assert_eq!(violations, 0, "Transaction isolation violations detected");
    assert!(
        success_rate > 0.8,
        "Transaction success rate too low: {:.1}%",
        success_rate * 100.0
    );
}

// Resource Exhaustion Tests

#[test]
#[ignore] // Long-running test
fn test_memory_pressure_cache_evictions() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 10 * MB, // Small cache to force evictions
        ..Default::default()
    };
    let cache_size = config.cache_size;
    let db = Database::create(dir.path(), config).unwrap();

    const VALUE_SIZE: usize = 1024; // 1KB values
    const NUM_KEYS: usize = 50_000; // 50MB of data, 5x cache size

    println!(
        "Testing memory pressure with {}KB cache and {}MB data...",
        cache_size / 1024,
        (NUM_KEYS * VALUE_SIZE) as u64 / MB
    );

    // Phase 1: Fill cache and force evictions
    println!(
        "Phase 1: Writing {} keys to force cache evictions...",
        NUM_KEYS
    );
    let write_start = Instant::now();

    for i in 0..NUM_KEYS {
        let key = format!("memory_test_key_{:08}", i);
        let mut value = vec![0u8; VALUE_SIZE];
        // Fill with pattern for verification
        for (idx, byte) in value.iter_mut().enumerate() {
            *byte = ((i + idx) % 256) as u8;
        }

        db.put(key.as_bytes(), &value).unwrap();

        if i % 10_000 == 0 && i > 0 {
            let elapsed = write_start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!("  Written {} keys ({:.0} keys/sec)", i, rate);
        }
    }

    let write_duration = write_start.elapsed();
    let write_rate = NUM_KEYS as f64 / write_duration.as_secs_f64();
    println!(
        "Completed writes in {:.2}s ({:.0} keys/sec)",
        write_duration.as_secs_f64(),
        write_rate
    );

    // Phase 2: Random access pattern to test cache behavior
    println!("Phase 2: Random access pattern testing cache behavior...");
    let read_start = Instant::now();
    const READ_ITERATIONS: usize = 100_000;

    let mut cache_hits = 0;
    let mut cache_misses = 0;

    for i in 0..READ_ITERATIONS {
        let key_idx = rand::thread_rng().gen_range(0..NUM_KEYS);
        let key = format!("memory_test_key_{:08}", key_idx);

        let access_start = Instant::now();
        let value = db.get(key.as_bytes()).unwrap();
        let access_time = access_start.elapsed();

        assert!(value.is_some(), "Missing key at index {}", key_idx);
        let value = value.unwrap();
        assert_eq!(value.len(), VALUE_SIZE);

        // Verify data pattern
        for (idx, &byte) in value.iter().enumerate() {
            let expected = ((key_idx + idx) % 256) as u8;
            assert_eq!(
                byte, expected,
                "Data corruption at key {} index {}",
                key_idx, idx
            );
        }

        // Classify as cache hit/miss based on access time
        if access_time.as_nanos() < 10_000 {
            // < 10μs = likely cache hit
            cache_hits += 1;
        } else {
            cache_misses += 1;
        }

        if i % 20_000 == 0 && i > 0 {
            let elapsed = read_start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            let hit_ratio = cache_hits as f64 / (cache_hits + cache_misses) as f64;
            println!(
                "  Accessed {} keys ({:.0} keys/sec, {:.1}% hit ratio)",
                i,
                rate,
                hit_ratio * 100.0
            );
        }
    }

    let read_duration = read_start.elapsed();
    let read_rate = READ_ITERATIONS as f64 / read_duration.as_secs_f64();
    let final_hit_ratio = cache_hits as f64 / (cache_hits + cache_misses) as f64;

    println!(
        "Completed {} reads in {:.2}s ({:.0} keys/sec, {:.1}% hit ratio)",
        READ_ITERATIONS,
        read_duration.as_secs_f64(),
        read_rate,
        final_hit_ratio * 100.0
    );

    // Phase 3: Sequential access to test cache warming
    println!("Phase 3: Sequential access to test cache warming...");
    let seq_start = Instant::now();

    for i in (NUM_KEYS - 1000)..NUM_KEYS {
        let key = format!("memory_test_key_{:08}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert!(value.is_some());
    }

    let seq_duration = seq_start.elapsed();
    let seq_rate = 1000.0 / seq_duration.as_secs_f64();

    println!(
        "Sequential access: 1000 keys in {:.2}ms ({:.0} keys/sec)",
        seq_duration.as_millis(),
        seq_rate
    );

    // Assertions
    assert!(
        write_rate > 1_000.0,
        "Write performance under memory pressure too low: {:.0}",
        write_rate
    );
    assert!(
        read_rate > 10_000.0,
        "Read performance under memory pressure too low: {:.0}",
        read_rate
    );
    assert!(
        final_hit_ratio > 0.05,
        "Cache hit ratio too low, cache may not be working: {:.1}%",
        final_hit_ratio * 100.0
    );
    assert!(
        seq_rate > 10_000.0,
        "Sequential access too slow: {:.0}",
        seq_rate
    );
}

#[test]
#[ignore] // Requires significant disk space
fn test_disk_space_exhaustion_handling() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 50 * MB,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const VALUE_SIZE: usize = 10 * 1024; // 10KB values
    const MAX_ATTEMPTS: usize = 1_000_000; // Attempt to write 10GB

    println!(
        "Testing disk space exhaustion handling (max {} attempts)...",
        MAX_ATTEMPTS
    );

    let mut successful_writes = 0;
    let mut first_failure: Option<usize> = None;
    let start_time = Instant::now();

    for i in 0..MAX_ATTEMPTS {
        let key = format!("disk_test_key_{:08}", i);
        let mut value = vec![0u8; VALUE_SIZE];
        rand::thread_rng().fill_bytes(&mut value);

        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                successful_writes += 1;

                if i % 10_000 == 0 && i > 0 {
                    let elapsed = start_time.elapsed();
                    let rate = successful_writes as f64 / elapsed.as_secs_f64();
                    let mb_written = (successful_writes * VALUE_SIZE) as u64 / MB;
                    println!(
                        "  Written {} keys ({} MB, {:.0} keys/sec)",
                        successful_writes, mb_written, rate
                    );
                }
            }
            Err(e) => {
                if first_failure.is_none() {
                    first_failure = Some(i);
                    println!("First write failure at iteration {}: {:?}", i, e);

                    // Test database is still readable
                    let test_key = format!("disk_test_key_{:08}", successful_writes / 2);
                    match db.get(test_key.as_bytes()) {
                        Ok(Some(_)) => println!("Database remains readable after write failure"),
                        Ok(None) => println!("Warning: Key not found after write failure"),
                        Err(e) => println!("Error reading after write failure: {:?}", e),
                    }
                    break;
                }
            }
        }
    }

    let duration = start_time.elapsed();
    let final_rate = successful_writes as f64 / duration.as_secs_f64();
    let mb_written = (successful_writes * VALUE_SIZE) as u64 / MB;

    println!("Disk exhaustion test completed:");
    println!(
        "  Successful writes: {} ({} MB)",
        successful_writes, mb_written
    );
    println!(
        "  Duration: {:.2}s ({:.0} keys/sec)",
        duration.as_secs_f64(),
        final_rate
    );

    if let Some(failure_point) = first_failure {
        println!(
            "  First failure at: {} ({:.1}% through test)",
            failure_point,
            failure_point as f64 / MAX_ATTEMPTS as f64 * 100.0
        );
    } else {
        println!("  No failures encountered (test may need more aggressive limits)");
    }

    // Test database integrity after exhaustion
    println!("Testing database integrity after disk exhaustion...");
    let verify_count = std::cmp::min(successful_writes, 1000);
    let mut verified = 0;

    for i in 0..verify_count {
        let key = format!("disk_test_key_{:08}", i);
        match db.get(key.as_bytes()) {
            Ok(Some(value)) => {
                assert_eq!(value.len(), VALUE_SIZE);
                verified += 1;
            }
            Ok(None) => {
                panic!("Missing key after disk exhaustion: {}", key);
            }
            Err(e) => {
                panic!("Error reading key after disk exhaustion: {:?}", e);
            }
        }
    }

    println!("Verified {} keys after disk exhaustion test", verified);

    assert!(
        successful_writes > 100,
        "Should have succeeded some writes: {}",
        successful_writes
    );
    assert_eq!(
        verified, verify_count,
        "Database integrity compromised after disk exhaustion"
    );
}

#[test]
#[ignore] // System-dependent test
fn test_file_descriptor_limits() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 20 * MB,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const MAX_TRANSACTIONS: usize = 2000; // Attempt to exhaust FDs

    println!(
        "Testing file descriptor limits with {} concurrent transactions...",
        MAX_TRANSACTIONS
    );

    let mut active_transactions = Vec::new();
    let mut successful_begins = 0;
    let mut first_failure: Option<usize> = None;

    // Phase 1: Create many concurrent transactions
    for i in 0..MAX_TRANSACTIONS {
        match db.begin_transaction() {
            Ok(tx_id) => {
                active_transactions.push(tx_id);
                successful_begins += 1;

                // Do some work in transaction to keep it active
                let key = format!("fd_test_key_{}", i);
                let value = format!("fd_test_value_{}", i);
                if let Err(e) = db.put_tx(tx_id, key.as_bytes(), value.as_bytes()) {
                    println!("Error in transaction {}: {:?}", i, e);
                }

                if i % 200 == 0 && i > 0 {
                    println!("  Created {} transactions", successful_begins);
                }
            }
            Err(e) => {
                if first_failure.is_none() {
                    first_failure = Some(i);
                    println!("First transaction creation failure at {}: {:?}", i, e);
                    break;
                }
            }
        }
    }

    println!(
        "Successfully created {} concurrent transactions",
        successful_begins
    );

    // Phase 2: Test database is still functional
    println!(
        "Testing database functionality with {} active transactions...",
        active_transactions.len()
    );

    let test_start = Instant::now();
    for i in 0..100 {
        let key = format!("fd_stress_key_{}", i);
        let value = format!("fd_stress_value_{}", i);

        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let retrieved = db.get(key.as_bytes()).unwrap();
        assert_eq!(retrieved.as_deref(), Some(value.as_bytes()));
    }
    let test_duration = test_start.elapsed();

    println!(
        "Database remained functional: 100 ops in {:.2}ms",
        test_duration.as_millis()
    );

    // Phase 3: Clean up transactions
    println!("Cleaning up {} transactions...", active_transactions.len());
    let cleanup_start = Instant::now();

    let mut committed = 0;
    let mut rollbacks = 0;

    for (idx, tx_id) in active_transactions.into_iter().enumerate() {
        if idx % 2 == 0 {
            match db.commit_transaction(tx_id) {
                Ok(_) => committed += 1,
                Err(_) => {
                    let _ = db.abort_transaction(tx_id);
                    rollbacks += 1;
                }
            }
        } else {
            match db.abort_transaction(tx_id) {
                Ok(_) => rollbacks += 1,
                Err(e) => println!("Error rolling back transaction {}: {:?}", tx_id, e),
            }
        }

        if idx % 500 == 0 && idx > 0 {
            println!("  Cleaned up {} transactions", idx);
        }
    }

    let cleanup_duration = cleanup_start.elapsed();

    println!(
        "Transaction cleanup completed in {:.2}s: {} committed, {} rolled back",
        cleanup_duration.as_secs_f64(),
        committed,
        rollbacks
    );

    // Final verification
    println!("Final database verification...");
    for i in 0..100 {
        let key = format!("fd_stress_key_{}", i);
        let value = db.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Missing key after FD stress test");
    }

    println!("File descriptor limit test completed successfully");

    assert!(
        successful_begins > 100,
        "Should create many transactions: {}",
        successful_begins
    );
    assert!(
        committed + rollbacks == successful_begins,
        "Transaction count mismatch"
    );
}

#[test]
#[ignore] // Long-running test
fn test_thread_pool_saturation() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * MB,
        max_active_transactions: 1000,
        ..Default::default()
    };
    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const NUM_THREADS: usize = 200; // More threads than typical pool size
    const OPS_PER_THREAD: usize = 500;

    println!(
        "Testing thread pool saturation with {} threads...",
        NUM_THREADS
    );

    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let completed_ops = Arc::new(AtomicUsize::new(0));
    let thread_errors = Arc::new(AtomicUsize::new(0));
    let start_time = Arc::new(Mutex::new(None));

    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let db = db.clone();
        let barrier = barrier.clone();
        let completed_ops = completed_ops.clone();
        let thread_errors = thread_errors.clone();
        let start_time = start_time.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            // Record start time from first thread
            let mut start_guard = start_time.lock().unwrap();
            if start_guard.is_none() {
                *start_guard = Some(Instant::now());
            }
            drop(start_guard);

            let mut local_ops = 0;
            let mut local_errors = 0;
            let mut rng = rand::thread_rng();

            for i in 0..OPS_PER_THREAD {
                let operation_type = rng.gen_range(0..100);

                match operation_type {
                    0..=60 => {
                        // 60% reads
                        let key = format!("thread_{}_key_{}", thread_id, rng.gen_range(0..i + 1));
                        match db.get(key.as_bytes()) {
                            Ok(_) => local_ops += 1,
                            Err(_) => local_errors += 1,
                        }
                    }
                    61..=90 => {
                        // 30% writes
                        let key = format!("thread_{}_key_{}", thread_id, i);
                        let value = format!("thread_{}_value_{}", thread_id, i);
                        match db.put(key.as_bytes(), value.as_bytes()) {
                            Ok(_) => local_ops += 1,
                            Err(_) => local_errors += 1,
                        }
                    }
                    _ => {
                        // 10% transactions
                        match db.begin_transaction() {
                            Ok(tx_id) => {
                                let key = format!("thread_{}_tx_key_{}", thread_id, i);
                                let value = format!("thread_{}_tx_value_{}", thread_id, i);

                                let mut tx_success = true;
                                if let Err(_) = db.put_tx(tx_id, key.as_bytes(), value.as_bytes()) {
                                    tx_success = false;
                                }

                                if tx_success {
                                    match db.commit_transaction(tx_id) {
                                        Ok(_) => local_ops += 1,
                                        Err(_) => local_errors += 1,
                                    }
                                } else {
                                    let _ = db.abort_transaction(tx_id);
                                    local_errors += 1;
                                }
                            }
                            Err(_) => local_errors += 1,
                        }
                    }
                }

                // Add small delays to simulate real workload
                if i % 50 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }

            completed_ops.fetch_add(local_ops, Ordering::Relaxed);
            thread_errors.fetch_add(local_errors, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let start_time = start_time.lock().unwrap().unwrap();
    let duration = start_time.elapsed();
    let total_ops = completed_ops.load(Ordering::Relaxed);
    let errors = thread_errors.load(Ordering::Relaxed);
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    let error_rate = errors as f64 / (total_ops + errors) as f64;

    println!(
        "Thread pool saturation test completed in {:.2}s:",
        duration.as_secs_f64()
    );
    println!(
        "  Total operations: {} ({:.0} ops/sec)",
        total_ops, ops_per_sec
    );
    println!(
        "  Errors: {} ({:.2}% error rate)",
        errors,
        error_rate * 100.0
    );
    println!("  Expected operations: {}", NUM_THREADS * OPS_PER_THREAD);

    // Verify some operations completed successfully
    assert!(
        total_ops > NUM_THREADS * OPS_PER_THREAD / 2,
        "Too few operations completed: {} < {}",
        total_ops,
        NUM_THREADS * OPS_PER_THREAD / 2
    );
    assert!(
        error_rate < 0.1,
        "Error rate too high: {:.2}%",
        error_rate * 100.0
    );
    assert!(
        ops_per_sec > 1_000.0,
        "Throughput too low under thread saturation: {:.0}",
        ops_per_sec
    );
}

// Failure Recovery Stress Tests

#[test]
#[ignore] // Long-running test
fn test_continuous_crash_recovery_cycles() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    const NUM_CYCLES: usize = 50;
    const OPS_PER_CYCLE: usize = 1000;
    const TOTAL_EXPECTED_KEYS: usize = NUM_CYCLES * OPS_PER_CYCLE;

    println!(
        "Testing {} crash/recovery cycles with {} ops each...",
        NUM_CYCLES, OPS_PER_CYCLE
    );

    let mut total_written = 0;
    let mut recovery_times = Vec::new();

    for cycle in 0..NUM_CYCLES {
        println!(
            "Cycle {}/{}: Creating database and writing data...",
            cycle + 1,
            NUM_CYCLES
        );

        // Create new database instance (simulates restart after crash)
        let recovery_start = Instant::now();
        let config = LightningDbConfig {
            cache_size: 50 * MB,
            ..Default::default()
        };
        let db = Database::create(&db_path, config).unwrap();
        let recovery_time = recovery_start.elapsed();
        recovery_times.push(recovery_time);

        if cycle > 0 {
            println!("  Recovery time: {:.2}ms", recovery_time.as_millis());
        }

        // Write operations for this cycle
        let write_start = Instant::now();
        for i in 0..OPS_PER_CYCLE {
            let global_key_id = cycle * OPS_PER_CYCLE + i;
            let key = format!("crash_test_key_{:08}", global_key_id);
            let value = format!("crash_test_value_{}_cycle_{}", global_key_id, cycle);

            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            total_written += 1;
        }
        let write_time = write_start.elapsed();

        // Verify written data is immediately readable
        let verify_start = Instant::now();
        let mut verified_this_cycle = 0;
        for i in 0..OPS_PER_CYCLE {
            let global_key_id = cycle * OPS_PER_CYCLE + i;
            let key = format!("crash_test_key_{:08}", global_key_id);
            let value = db.get(key.as_bytes()).unwrap();

            if value.is_some() {
                verified_this_cycle += 1;
            }
        }
        let verify_time = verify_start.elapsed();

        // Verify all previously written data is still there
        if cycle > 0 {
            let full_verify_start = Instant::now();
            let mut verified_previous = 0;

            for prev_cycle in 0..cycle {
                for i in 0..OPS_PER_CYCLE {
                    let global_key_id = prev_cycle * OPS_PER_CYCLE + i;
                    let key = format!("crash_test_key_{:08}", global_key_id);

                    match db.get(key.as_bytes()) {
                        Ok(Some(_)) => verified_previous += 1,
                        Ok(None) => panic!("Missing key from previous cycle: {}", key),
                        Err(e) => panic!("Error reading key from previous cycle {}: {:?}", key, e),
                    }
                }
            }
            let full_verify_time = full_verify_start.elapsed();

            println!(
                "  Write: {:.2}ms, Verify current: {:.2}ms ({}), Verify previous: {:.2}ms ({})",
                write_time.as_millis(),
                verify_time.as_millis(),
                verified_this_cycle,
                full_verify_time.as_millis(),
                verified_previous
            );
        } else {
            println!(
                "  Write: {:.2}ms, Verify: {:.2}ms ({})",
                write_time.as_millis(),
                verify_time.as_millis(),
                verified_this_cycle
            );
        }

        // Explicitly drop database to simulate crash
        drop(db);

        if cycle % 10 == 9 {
            println!(
                "Completed {} cycles, {} total keys written",
                cycle + 1,
                total_written
            );
        }
    }

    // Final recovery and full verification
    println!("Performing final recovery and full verification...");
    let final_recovery_start = Instant::now();
    let final_db = Database::create(&db_path, LightningDbConfig::default()).unwrap();
    let final_recovery_time = final_recovery_start.elapsed();

    println!(
        "Final recovery time: {:.2}ms",
        final_recovery_time.as_millis()
    );

    let final_verify_start = Instant::now();
    let mut final_verified = 0;

    for cycle in 0..NUM_CYCLES {
        for i in 0..OPS_PER_CYCLE {
            let global_key_id = cycle * OPS_PER_CYCLE + i;
            let key = format!("crash_test_key_{:08}", global_key_id);
            let expected_value = format!("crash_test_value_{}_cycle_{}", global_key_id, cycle);

            match final_db.get(key.as_bytes()).unwrap() {
                Some(actual_value) => {
                    assert_eq!(
                        actual_value,
                        expected_value.as_bytes(),
                        "Data corruption in key {}",
                        key
                    );
                    final_verified += 1;
                }
                None => panic!("Missing key in final verification: {}", key),
            }
        }

        if cycle % 10 == 9 {
            println!(
                "  Verified through cycle {} ({} keys)",
                cycle + 1,
                final_verified
            );
        }
    }

    let final_verify_time = final_verify_start.elapsed();

    let avg_recovery_time =
        recovery_times.iter().skip(1).sum::<Duration>() / (recovery_times.len() - 1) as u32;
    let max_recovery_time = recovery_times.iter().skip(1).max().unwrap();

    println!("Crash/recovery test completed successfully:");
    println!("  Total cycles: {}", NUM_CYCLES);
    println!("  Total keys written: {}", total_written);
    println!(
        "  Final verification: {} keys in {:.2}ms",
        final_verified,
        final_verify_time.as_millis()
    );
    println!(
        "  Average recovery time: {:.2}ms",
        avg_recovery_time.as_millis()
    );
    println!(
        "  Maximum recovery time: {:.2}ms",
        max_recovery_time.as_millis()
    );

    assert_eq!(
        total_written, TOTAL_EXPECTED_KEYS,
        "Incorrect number of keys written"
    );
    assert_eq!(
        final_verified, TOTAL_EXPECTED_KEYS,
        "Data loss detected in crash recovery"
    );
    assert!(
        avg_recovery_time.as_millis() < 1000,
        "Average recovery time too slow: {}ms",
        avg_recovery_time.as_millis()
    );
    assert!(
        max_recovery_time.as_millis() < 5000,
        "Maximum recovery time too slow: {}ms",
        max_recovery_time.as_millis()
    );
}

#[test]
#[ignore] // Long-running test
fn test_recovery_under_load() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    const BACKGROUND_THREADS: usize = 10;
    const OPS_PER_THREAD: usize = 500;
    const RECOVERY_CYCLES: usize = 5;

    println!(
        "Testing recovery under load: {} background threads, {} recovery cycles...",
        BACKGROUND_THREADS, RECOVERY_CYCLES
    );

    for cycle in 0..RECOVERY_CYCLES {
        println!("Recovery cycle {}/{}", cycle + 1, RECOVERY_CYCLES);

        let recovery_start = Instant::now();
        let config = LightningDbConfig {
            cache_size: 100 * MB,
            max_active_transactions: 100,
            ..Default::default()
        };
        let db = Arc::new(Database::create(&db_path, config).unwrap());
        let recovery_time = recovery_start.elapsed();

        if cycle > 0 {
            println!("  Recovery time: {:.2}ms", recovery_time.as_millis());
        }

        // Start background load
        let stop_flag = Arc::new(AtomicBool::new(false));
        let completed_ops = Arc::new(AtomicUsize::new(0));
        let mut load_handles = vec![];

        for thread_id in 0..BACKGROUND_THREADS {
            let db = db.clone();
            let stop_flag = stop_flag.clone();
            let completed_ops = completed_ops.clone();

            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut local_ops = 0;

                while !stop_flag.load(Ordering::Relaxed) && local_ops < OPS_PER_THREAD {
                    let op_type = rng.gen_range(0..100);

                    match op_type {
                        0..=70 => {
                            // 70% writes
                            let key = format!("recovery_load_{}_{}", thread_id, local_ops);
                            let value = format!(
                                "recovery_value_{}_{}_cycle_{}",
                                thread_id, local_ops, cycle
                            );
                            if let Ok(_) = db.put(key.as_bytes(), value.as_bytes()) {
                                local_ops += 1;
                            }
                        }
                        71..=90 => {
                            // 20% reads
                            if local_ops > 0 {
                                let key_idx = rng.gen_range(0..local_ops);
                                let key = format!("recovery_load_{}_{}", thread_id, key_idx);
                                let _ = db.get(key.as_bytes());
                            }
                        }
                        _ => {
                            // 10% transactions
                            if let Ok(tx_id) = db.begin_transaction() {
                                let key = format!("recovery_tx_{}_{}", thread_id, local_ops);
                                let value =
                                    format!("recovery_tx_value_{}_{}", thread_id, local_ops);

                                if let Ok(_) = db.put_tx(tx_id, key.as_bytes(), value.as_bytes()) {
                                    if let Ok(_) = db.commit_transaction(tx_id) {
                                        local_ops += 1;
                                    } else {
                                        let _ = db.abort_transaction(tx_id);
                                    }
                                } else {
                                    let _ = db.abort_transaction(tx_id);
                                }
                            }
                        }
                    }

                    if local_ops % 100 == 0 {
                        thread::sleep(Duration::from_millis(1));
                    }
                }

                completed_ops.fetch_add(local_ops, Ordering::Relaxed);
            });

            load_handles.push(handle);
        }

        // Let load run for a while
        thread::sleep(Duration::from_millis(500));

        // Monitor progress
        let monitor_start = Instant::now();
        while monitor_start.elapsed() < Duration::from_secs(2) {
            let current_ops = completed_ops.load(Ordering::Relaxed);
            println!("  Load progress: {} ops", current_ops);
            thread::sleep(Duration::from_millis(500));
        }

        // Stop background load
        stop_flag.store(true, Ordering::Relaxed);

        for handle in load_handles {
            handle.join().unwrap();
        }

        let final_ops = completed_ops.load(Ordering::Relaxed);
        println!("  Completed {} operations under load", final_ops);

        // Verify some data before next cycle
        let mut verified = 0;
        for thread_id in 0..std::cmp::min(BACKGROUND_THREADS, 5) {
            for op_id in 0..std::cmp::min(final_ops / BACKGROUND_THREADS, 50) {
                let key = format!("recovery_load_{}_{}", thread_id, op_id);
                if let Ok(Some(_)) = db.get(key.as_bytes()) {
                    verified += 1;
                }
            }
        }

        println!("  Verified {} keys before next recovery cycle", verified);

        // Drop database to simulate crash
        drop(db);

        assert!(
            final_ops > 100,
            "Too few operations completed under load: {}",
            final_ops
        );
    }

    // Final recovery and verification
    println!("Performing final recovery...");
    let final_recovery_start = Instant::now();
    let final_db = Database::create(&db_path, LightningDbConfig::default()).unwrap();
    let final_recovery_time = final_recovery_start.elapsed();

    println!(
        "Final recovery completed in {:.2}ms",
        final_recovery_time.as_millis()
    );

    let final_verify_start = Instant::now();
    let range_result = final_db.range(None, None).unwrap();
    let total_keys = range_result.len();
    let final_verify_time = final_verify_start.elapsed();

    println!(
        "Final verification: {} total keys found in {:.2}ms",
        total_keys,
        final_verify_time.as_millis()
    );

    assert!(
        total_keys > 1000,
        "Too few keys survived recovery under load: {}",
        total_keys
    );
    assert!(
        final_recovery_time.as_millis() < 2000,
        "Final recovery too slow: {}ms",
        final_recovery_time.as_millis()
    );
}

// Long-Running Stability Tests

#[test]
#[ignore] // 24-hour test
fn test_24_hour_continuous_operation() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 200 * MB,
        max_active_transactions: 500,
        // wal_sync_interval removed - not available in config
        ..Default::default()
    };
    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    const TEST_DURATION: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours
    const NUM_WORKER_THREADS: usize = 20;
    const MEMORY_CHECK_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

    println!("Starting 24-hour continuous operation test...");
    println!("Test duration: {:?}", TEST_DURATION);
    println!("Worker threads: {}", NUM_WORKER_THREADS);

    let start_time = Instant::now();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let total_operations = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));

    // Memory monitoring thread
    let memory_db = db.clone();
    let memory_stop = stop_flag.clone();
    let memory_handle = thread::spawn(move || {
        let mut memory_samples = Vec::new();

        while !memory_stop.load(Ordering::Relaxed) {
            // Simple memory usage approximation
            let range_count = memory_db.range(None, None).unwrap_or_default().len();
            memory_samples.push((Instant::now(), range_count));

            if memory_samples.len() % 12 == 0 {
                // Every hour
                let hours = memory_samples.len() / 12;
                println!("Hour {}: {} keys in database", hours, range_count);
            }

            thread::sleep(MEMORY_CHECK_INTERVAL);
        }

        memory_samples
    });

    // Worker threads
    let mut worker_handles = vec![];

    for worker_id in 0..NUM_WORKER_THREADS {
        let db = db.clone();
        let stop_flag = stop_flag.clone();
        let total_ops = total_operations.clone();
        let total_errs = total_errors.clone();

        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut local_ops = 0u64;
            let mut local_errors = 0u64;
            let mut operation_counter = 0u64;

            while !stop_flag.load(Ordering::Relaxed) {
                let op_type = rng.gen_range(0..1000);
                operation_counter += 1;

                let result = match op_type {
                    0..=500 => {
                        // 50% reads
                        let key_id = rng.gen_range(0..operation_counter + 1);
                        let key = format!("stability_key_{}_{}", worker_id, key_id);
                        db.get(key.as_bytes()).map(|_| ())
                    }
                    501..=800 => {
                        // 30% writes
                        let key = format!("stability_key_{}_{}", worker_id, operation_counter);
                        let value = format!("stability_value_{}_{}", worker_id, operation_counter);
                        db.put(key.as_bytes(), value.as_bytes())
                    }
                    801..=900 => {
                        // 10% batch writes
                        let batch_size = rng.gen_range(10..100);
                        let mut batch = Vec::with_capacity(batch_size);

                        for i in 0..batch_size {
                            let key = format!(
                                "stability_batch_{}_{}_{}",
                                worker_id, operation_counter, i
                            );
                            let value =
                                format!("batch_value_{}_{}_{}", worker_id, operation_counter, i);
                            batch.push((key.into_bytes(), value.into_bytes()));
                        }

                        db.put_batch(&batch)
                    }
                    _ => {
                        // 10% transactions
                        db.begin_transaction().and_then(|tx_id| {
                            let key = format!("stability_tx_{}_{}", worker_id, operation_counter);
                            let value = format!("tx_value_{}_{}", worker_id, operation_counter);

                            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())
                                .and_then(|_| db.commit_transaction(tx_id))
                        })
                    }
                };

                match result {
                    Ok(_) => local_ops += 1,
                    Err(_) => local_errors += 1,
                }

                // Periodic reporting and small delays
                if operation_counter % 10000 == 0 {
                    total_ops.fetch_add(local_ops, Ordering::Relaxed);
                    total_errs.fetch_add(local_errors, Ordering::Relaxed);
                    local_ops = 0;
                    local_errors = 0;

                    thread::sleep(Duration::from_millis(10));
                }

                if operation_counter % 1000 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }

            // Final reporting
            total_ops.fetch_add(local_ops, Ordering::Relaxed);
            total_errs.fetch_add(local_errors, Ordering::Relaxed);
        });

        worker_handles.push(handle);
    }

    // Progress monitoring
    let monitoring_db = db.clone();
    let monitoring_stop = stop_flag.clone();
    let monitoring_ops = total_operations.clone();
    let monitoring_errors = total_errors.clone();

    let monitoring_handle = thread::spawn(move || {
        let mut last_ops = 0u64;
        let mut hour_counter = 0;

        while !monitoring_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(3600)); // 1 hour intervals

            if monitoring_stop.load(Ordering::Relaxed) {
                break;
            }

            hour_counter += 1;
            let current_ops = monitoring_ops.load(Ordering::Relaxed);
            let current_errors = monitoring_errors.load(Ordering::Relaxed);
            let ops_this_hour = current_ops - last_ops;
            last_ops = current_ops;

            let range_size = monitoring_db.range(None, None).unwrap_or_default().len();
            let error_rate = if current_ops > 0 {
                current_errors as f64 / current_ops as f64 * 100.0
            } else {
                0.0
            };

            println!(
                "Hour {}: {} ops total ({} this hour), {} errors ({:.3}%), {} keys",
                hour_counter, current_ops, ops_this_hour, current_errors, error_rate, range_size
            );
        }
    });

    // Run test for specified duration
    thread::sleep(TEST_DURATION);

    // Stop all threads
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads to complete
    for handle in worker_handles {
        handle.join().unwrap();
    }
    monitoring_handle.join().unwrap();
    let memory_samples = memory_handle.join().unwrap();

    let final_duration = start_time.elapsed();
    let final_ops = total_operations.load(Ordering::Relaxed);
    let final_errors = total_errors.load(Ordering::Relaxed);

    // Final verification and analysis
    println!("24-hour test completed!");
    println!(
        "Actual duration: {:.2} hours",
        final_duration.as_secs_f64() / 3600.0
    );
    println!("Total operations: {}", final_ops);
    println!(
        "Total errors: {} ({:.4}%)",
        final_errors,
        final_errors as f64 / final_ops as f64 * 100.0
    );
    println!(
        "Average ops/sec: {:.0}",
        final_ops as f64 / final_duration.as_secs_f64()
    );

    // Memory stability check
    if memory_samples.len() >= 2 {
        let initial_keys = memory_samples[0].1;
        let final_keys = memory_samples[memory_samples.len() - 1].1;
        let max_keys = memory_samples
            .iter()
            .map(|(_, keys)| *keys)
            .max()
            .unwrap_or(0);

        println!("Memory stability:");
        println!("  Initial keys: {}", initial_keys);
        println!("  Final keys: {}", final_keys);
        println!("  Peak keys: {}", max_keys);
        println!(
            "  Growth rate: {:.2} keys/hour",
            (final_keys - initial_keys) as f64 / (final_duration.as_secs_f64() / 3600.0)
        );
    }

    // Final database integrity check
    println!("Performing final integrity check...");
    let integrity_start = Instant::now();
    let final_range = db.range(None, None).unwrap();
    let integrity_time = integrity_start.elapsed();

    println!(
        "Final integrity check: {} keys verified in {:.2}ms",
        final_range.len(),
        integrity_time.as_millis()
    );

    // Assertions for test success
    assert!(
        final_duration >= TEST_DURATION - Duration::from_secs(60),
        "Test ended too early"
    );
    assert!(
        final_ops > 1_000_000,
        "Too few operations in 24 hours: {}",
        final_ops
    );
    assert!(
        (final_errors as f64 / final_ops as f64) < 0.001,
        "Error rate too high: {:.4}%",
        final_errors as f64 / final_ops as f64 * 100.0
    );
    assert!(!final_range.is_empty(), "Database empty after 24-hour test");
}

#[test]
#[ignore] // Long-running test
fn test_memory_leak_detection() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 50 * MB,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const TEST_CYCLES: usize = 1000;
    const OPS_PER_CYCLE: usize = 1000;
    const MEMORY_SAMPLES: usize = 100;

    println!(
        "Testing memory leak detection over {} cycles...",
        TEST_CYCLES
    );

    let mut memory_usage_samples = Vec::new();

    for cycle in 0..TEST_CYCLES {
        // Perform operations
        for i in 0..OPS_PER_CYCLE {
            let key = format!("leak_test_key_{}_{}", cycle, i);
            let value = format!("leak_test_value_{}_{}", cycle, i);

            db.put(key.as_bytes(), value.as_bytes()).unwrap();

            // Read back randomly
            if i % 10 == 0 {
                let read_key = format!(
                    "leak_test_key_{}_{}",
                    cycle,
                    rand::thread_rng().gen_range(0..i + 1)
                );
                let _ = db.get(read_key.as_bytes()).unwrap();
            }

            // Transaction operations
            if i % 50 == 0 {
                let tx_id = db.begin_transaction().unwrap();
                let tx_key = format!("leak_tx_key_{}_{}", cycle, i);
                let tx_value = format!("leak_tx_value_{}_{}", cycle, i);

                db.put_tx(tx_id, tx_key.as_bytes(), tx_value.as_bytes())
                    .unwrap();

                if i % 100 == 0 {
                    db.commit_transaction(tx_id).unwrap();
                } else {
                    db.abort_transaction(tx_id).unwrap();
                }
            }
        }

        // Sample memory usage periodically
        if cycle % (TEST_CYCLES / MEMORY_SAMPLES) == 0 {
            // Use range operation as a proxy for memory usage
            let range_count = db.range(None, None).unwrap().len();
            memory_usage_samples.push((cycle, range_count));

            if memory_usage_samples.len() % 10 == 0 {
                println!("Cycle {}: {} keys in database", cycle, range_count);
            }
        }

        // Periodic cleanup to test memory reclamation
        if cycle % 100 == 99 {
            // Delete some old keys
            for i in 0..50 {
                let old_key = format!("leak_test_key_{}_{}", cycle - 50, i);
                let _ = db.delete(old_key.as_bytes());
            }
        }
    }

    println!("Memory usage analysis:");

    // Analyze memory growth pattern
    let mut memory_differences = Vec::new();
    for i in 1..memory_usage_samples.len() {
        let diff = memory_usage_samples[i].1 as i64 - memory_usage_samples[i - 1].1 as i64;
        memory_differences.push(diff);
    }

    let initial_keys = memory_usage_samples[0].1;
    let final_keys = memory_usage_samples[memory_usage_samples.len() - 1].1;
    let max_keys = memory_usage_samples
        .iter()
        .map(|(_, keys)| *keys)
        .max()
        .unwrap_or(0);
    let min_keys = memory_usage_samples
        .iter()
        .map(|(_, keys)| *keys)
        .min()
        .unwrap_or(0);

    let avg_growth =
        memory_differences.iter().sum::<i64>() as f64 / memory_differences.len() as f64;
    let max_growth = memory_differences.iter().max().unwrap_or(&0);
    let min_growth = memory_differences.iter().min().unwrap_or(&0);

    println!("  Initial keys: {}", initial_keys);
    println!("  Final keys: {}", final_keys);
    println!("  Peak keys: {}", max_keys);
    println!("  Minimum keys: {}", min_keys);
    println!("  Net growth: {}", final_keys as i64 - initial_keys as i64);
    println!("  Average growth per sample: {:.1}", avg_growth);
    println!("  Max growth per sample: {}", max_growth);
    println!("  Min growth per sample: {}", min_growth);

    // Look for concerning patterns
    let growth_trend =
        (final_keys as f64 - initial_keys as f64) / memory_usage_samples.len() as f64;
    let excessive_growth_samples = memory_differences
        .iter()
        .filter(|&&diff| diff > 1000)
        .count();

    println!("  Growth trend per sample: {:.2}", growth_trend);
    println!(
        "  Samples with excessive growth (>1000): {}",
        excessive_growth_samples
    );

    // Final memory verification
    println!("Performing final memory verification...");

    // Force some cleanup operations
    for i in 0..100 {
        let key = format!("final_test_key_{}", i);
        let value = vec![0u8; 1024]; // 1KB values
        db.put(key.as_bytes(), &value).unwrap();
    }

    let pre_cleanup_count = db.range(None, None).unwrap().len();

    // Delete the test keys
    for i in 0..100 {
        let key = format!("final_test_key_{}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    let post_cleanup_count = db.range(None, None).unwrap().len();

    println!(
        "Memory cleanup test: {} -> {} keys (freed {})",
        pre_cleanup_count,
        post_cleanup_count,
        pre_cleanup_count - post_cleanup_count
    );

    // Assertions
    assert!(
        growth_trend < 10.0,
        "Memory growth trend too high: {:.2}",
        growth_trend
    );
    assert!(
        excessive_growth_samples < memory_usage_samples.len() / 10,
        "Too many samples with excessive growth: {}",
        excessive_growth_samples
    );
    assert!(
        post_cleanup_count <= pre_cleanup_count,
        "Memory not reclaimed after deletions: {} -> {}",
        pre_cleanup_count,
        post_cleanup_count
    );

    println!("Memory leak detection test completed successfully");
}

// Edge Case Stress Tests

#[test]
#[ignore] // Long-running test
fn test_maximum_key_value_sizes() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 500 * MB, // Large cache for big values
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    println!("Testing maximum key and value sizes...");

    // Test progressively larger key sizes
    let key_sizes = [100, 1_000, 10_000, 100_000];

    for &key_size in &key_sizes {
        println!("Testing key size: {} bytes", key_size);

        let large_key = "k".repeat(key_size);
        let value = "value_for_large_key";

        let start = Instant::now();
        let result = db.put(large_key.as_bytes(), value.as_bytes());
        let duration = start.elapsed();

        match result {
            Ok(_) => {
                println!(
                    "  Successfully stored {}-byte key in {:.2}ms",
                    key_size,
                    duration.as_millis()
                );

                // Verify retrieval
                let retrieve_start = Instant::now();
                let retrieved = db.get(large_key.as_bytes()).unwrap();
                let retrieve_duration = retrieve_start.elapsed();

                assert_eq!(retrieved.as_deref(), Some(value.as_bytes()));
                println!(
                    "  Successfully retrieved in {:.2}ms",
                    retrieve_duration.as_millis()
                );
            }
            Err(e) => {
                println!("  Failed to store {}-byte key: {:?}", key_size, e);
                // This may be expected for very large keys
            }
        }
    }

    // Test progressively larger value sizes
    let value_sizes = [1_024, 10_240, 102_400, 1_024_000, 10_240_000]; // 1KB to 10MB

    for &value_size in &value_sizes {
        println!(
            "Testing value size: {} bytes ({:.1} MB)",
            value_size,
            value_size as f64 / 1_048_576.0
        );

        let key = format!("large_value_key_{}", value_size);
        let mut large_value = vec![0u8; value_size];

        // Fill with pattern for verification
        for (i, byte) in large_value.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        let start = Instant::now();
        let result = db.put(key.as_bytes(), &large_value);
        let duration = start.elapsed();

        match result {
            Ok(_) => {
                let throughput = value_size as f64 / duration.as_secs_f64() / 1_048_576.0; // MB/s
                println!(
                    "  Successfully stored {:.1}MB value in {:.2}ms ({:.1} MB/s)",
                    value_size as f64 / 1_048_576.0,
                    duration.as_millis(),
                    throughput
                );

                // Verify retrieval
                let retrieve_start = Instant::now();
                let retrieved = db.get(key.as_bytes()).unwrap();
                let retrieve_duration = retrieve_start.elapsed();

                assert!(retrieved.is_some());
                let retrieved_value = retrieved.unwrap();
                assert_eq!(retrieved_value.len(), value_size);

                // Verify pattern
                for (i, &byte) in retrieved_value.iter().enumerate() {
                    assert_eq!(byte, (i % 256) as u8, "Data corruption at byte {}", i);
                }

                let retrieve_throughput =
                    value_size as f64 / retrieve_duration.as_secs_f64() / 1_048_576.0;
                println!(
                    "  Successfully retrieved in {:.2}ms ({:.1} MB/s)",
                    retrieve_duration.as_millis(),
                    retrieve_throughput
                );
            }
            Err(e) => {
                println!(
                    "  Failed to store {:.1}MB value: {:?}",
                    value_size as f64 / 1_048_576.0,
                    e
                );
                // May be expected for very large values
            }
        }
    }

    // Test combination of large keys and values
    println!("Testing combination of large keys and values...");

    let combo_key = "combo_key_".to_string() + &"x".repeat(1000);
    let combo_value = vec![42u8; 100_000]; // 100KB value

    let combo_start = Instant::now();
    let combo_result = db.put(combo_key.as_bytes(), &combo_value);
    let combo_duration = combo_start.elapsed();

    match combo_result {
        Ok(_) => {
            println!(
                "Successfully stored large key + large value in {:.2}ms",
                combo_duration.as_millis()
            );

            let retrieved = db.get(combo_key.as_bytes()).unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().len(), combo_value.len());
        }
        Err(e) => {
            println!("Failed to store large key + large value: {:?}", e);
        }
    }

    println!("Maximum key/value size test completed");
}

#[test]
#[ignore] // Long-running test
fn test_billions_of_tiny_keys() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * MB,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const TOTAL_KEYS: usize = 10_000_000; // 10 million keys (scaled down from billions for practicality)
    const BATCH_SIZE: usize = 10_000;
    const KEY_SIZE: usize = 8; // 8-byte keys
    const VALUE_SIZE: usize = 4; // 4-byte values

    println!(
        "Testing {} tiny keys ({}B key, {}B value)...",
        TOTAL_KEYS, KEY_SIZE, VALUE_SIZE
    );

    let start_time = Instant::now();
    let mut total_written = 0;

    for batch_idx in 0..(TOTAL_KEYS / BATCH_SIZE) {
        let batch_start = Instant::now();
        let mut batch_ops = Vec::with_capacity(BATCH_SIZE);

        for i in 0..BATCH_SIZE {
            let key_id = batch_idx * BATCH_SIZE + i;

            // Create 8-byte key from ID
            let key_bytes = key_id.to_be_bytes();

            // Create 4-byte value (just the lower 32 bits)
            let value_bytes = (key_id as u32).to_be_bytes();

            batch_ops.push((key_bytes.to_vec(), value_bytes.to_vec()));
        }

        let batch_write_start = Instant::now();
        db.put_batch(&batch_ops).unwrap();
        let batch_write_time = batch_write_start.elapsed();

        total_written += BATCH_SIZE;
        let batch_duration = batch_start.elapsed();

        if batch_idx % 100 == 0 {
            let elapsed = start_time.elapsed();
            let keys_per_sec = total_written as f64 / elapsed.as_secs_f64();
            let estimated_total_time =
                elapsed.as_secs_f64() * TOTAL_KEYS as f64 / total_written as f64;

            println!(
                "Batch {}/{}: {} keys total ({:.0} keys/sec, batch: {:.2}ms, ETA: {:.1}min)",
                batch_idx + 1,
                TOTAL_KEYS / BATCH_SIZE,
                total_written,
                keys_per_sec,
                batch_duration.as_millis(),
                (estimated_total_time - elapsed.as_secs_f64()) / 60.0
            );
        }
    }

    let write_duration = start_time.elapsed();
    let write_keys_per_sec = TOTAL_KEYS as f64 / write_duration.as_secs_f64();
    let total_data_mb = (TOTAL_KEYS * (KEY_SIZE + VALUE_SIZE)) as f64 / 1_048_576.0;
    let write_throughput_mb = total_data_mb / write_duration.as_secs_f64();

    println!("Write phase completed:");
    println!(
        "  {} keys written in {:.2}s",
        TOTAL_KEYS,
        write_duration.as_secs_f64()
    );
    println!("  {:.0} keys/sec", write_keys_per_sec);
    println!("  {:.1} MB total data", total_data_mb);
    println!("  {:.1} MB/s write throughput", write_throughput_mb);

    // Random read test
    println!("Testing random reads...");
    const READ_SAMPLES: usize = 100_000;

    let read_start = Instant::now();
    let mut successful_reads = 0;
    let mut read_errors = 0;

    for _ in 0..READ_SAMPLES {
        let random_key_id = rand::thread_rng().gen_range(0..TOTAL_KEYS);
        let key_bytes = random_key_id.to_be_bytes();

        match db.get(&key_bytes) {
            Ok(Some(value)) => {
                // Verify value is correct
                let expected_value = (random_key_id as u32).to_be_bytes();
                assert_eq!(value.as_slice(), &expected_value);
                successful_reads += 1;
            }
            Ok(None) => {
                read_errors += 1;
            }
            Err(_) => {
                read_errors += 1;
            }
        }
    }

    let read_duration = read_start.elapsed();
    let read_keys_per_sec = successful_reads as f64 / read_duration.as_secs_f64();

    println!("Random read test:");
    println!(
        "  {} successful reads out of {} attempts",
        successful_reads, READ_SAMPLES
    );
    println!("  {} errors", read_errors);
    println!("  {:.0} reads/sec", read_keys_per_sec);
    println!(
        "  Average latency: {:.2}μs",
        read_duration.as_nanos() as f64 / READ_SAMPLES as f64 / 1000.0
    );

    // Range scan test
    println!("Testing range scan performance...");

    let scan_start = Instant::now();
    let scan_result = db.range(None, None); // Get all entries
    let scan_result: Result<Vec<(Vec<u8>, Vec<u8>)>, lightning_db::core::error::Error> =
        Ok(scan_result
            .unwrap()
            .into_iter()
            .take(1000)
            .collect::<Vec<_>>());
    let scan_duration = scan_start.elapsed();

    match scan_result {
        Ok(entries) => {
            println!(
                "Range scan: {} entries in {:.2}ms ({:.0} entries/sec)",
                entries.len(),
                scan_duration.as_millis(),
                entries.len() as f64 / scan_duration.as_secs_f64()
            );

            // Verify first few entries
            for (i, (key, value)) in entries.iter().take(10).enumerate() {
                let expected_key = i.to_be_bytes();
                let expected_value = (i as u32).to_be_bytes();
                assert_eq!(key.as_slice(), &expected_key);
                assert_eq!(value.as_slice(), &expected_value);
            }
        }
        Err(e) => {
            println!("Range scan failed: {:?}", e);
        }
    }

    // Memory efficiency check
    let final_range = db.range(None, None).unwrap();
    let actual_keys = final_range.len();

    println!("Final verification:");
    println!("  Expected keys: {}", TOTAL_KEYS);
    println!("  Actual keys: {}", actual_keys);
    println!(
        "  Data integrity: {:.2}%",
        actual_keys as f64 / TOTAL_KEYS as f64 * 100.0
    );

    // Assertions
    assert_eq!(actual_keys, TOTAL_KEYS, "Key count mismatch");
    assert!(
        write_keys_per_sec > 50_000.0,
        "Write performance too slow: {:.0} keys/sec",
        write_keys_per_sec
    );
    assert!(
        read_keys_per_sec > 100_000.0,
        "Read performance too slow: {:.0} keys/sec",
        read_keys_per_sec
    );
    assert_eq!(
        read_errors, 0,
        "Should have no read errors for existing keys"
    );

    println!("Tiny keys stress test completed successfully");
}

#[test]
#[ignore] // Long-running test
fn test_deeply_nested_transactions() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * MB,
        max_active_transactions: 1000,
        ..Default::default()
    };
    let db = Database::create(dir.path(), config).unwrap();

    const MAX_NESTING_DEPTH: usize = 50;
    const OPERATIONS_PER_LEVEL: usize = 10;

    println!(
        "Testing deeply nested transactions (depth {})...",
        MAX_NESTING_DEPTH
    );

    // Test 1: Serial nested transactions (commit chain)
    println!("Test 1: Serial nested transaction commits...");

    let mut transaction_stack = Vec::new();
    let start_time = Instant::now();

    // Create nested transactions
    for depth in 0..MAX_NESTING_DEPTH {
        let tx_id = db.begin_transaction().unwrap();
        transaction_stack.push(tx_id);

        // Perform operations at this nesting level
        for op in 0..OPERATIONS_PER_LEVEL {
            let key = format!("nested_tx_depth_{}_op_{}", depth, op);
            let value = format!("nested_value_depth_{}_op_{}", depth, op);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }

        if depth % 10 == 9 {
            println!("  Created {} nested transactions", depth + 1);
        }
    }

    let creation_time = start_time.elapsed();

    // Commit transactions in reverse order (LIFO)
    let commit_start = Instant::now();
    let mut committed = 0;

    while let Some(tx_id) = transaction_stack.pop() {
        db.commit_transaction(tx_id).unwrap();
        committed += 1;

        if committed % 10 == 0 {
            println!("  Committed {} transactions", committed);
        }
    }

    let commit_time = commit_start.elapsed();
    let total_time = start_time.elapsed();

    println!("Serial nested transactions completed:");
    println!("  Creation: {:.2}ms", creation_time.as_millis());
    println!("  Commits: {:.2}ms", commit_time.as_millis());
    println!("  Total: {:.2}ms", total_time.as_millis());

    // Verify all data is committed
    let mut verified_keys = 0;
    for depth in 0..MAX_NESTING_DEPTH {
        for op in 0..OPERATIONS_PER_LEVEL {
            let key = format!("nested_tx_depth_{}_op_{}", depth, op);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Missing committed key: {}", key);
            verified_keys += 1;
        }
    }

    println!("  Verified {} committed keys", verified_keys);

    // Test 2: Concurrent nested transactions
    println!("Test 2: Concurrent nested transactions...");

    const CONCURRENT_CHAINS: usize = 10;
    const CHAIN_DEPTH: usize = 20;

    let concurrent_start = Instant::now();
    let barrier = Arc::new(Barrier::new(CONCURRENT_CHAINS));
    let completed_chains = Arc::new(AtomicUsize::new(0));
    let total_tx_ops = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for chain_id in 0..CONCURRENT_CHAINS {
        let db_clone = db.clone(); // Clone Arc for thread
        let barrier = barrier.clone();
        let completed_chains = completed_chains.clone();
        let total_tx_ops = total_tx_ops.clone();

        // Note: We can't directly move db into the closure as it would require Clone
        // For this test, we'll create a simpler version that tests the transaction depth

        let handle = thread::spawn(move || {
            barrier.wait();

            let mut chain_transactions = Vec::new();
            let mut local_ops = 0;

            // Create transaction chain
            for depth in 0..CHAIN_DEPTH {
                if let Ok(tx_id) = db_clone.begin_transaction() {
                    chain_transactions.push(tx_id);

                    // Perform operation in this transaction
                    let key = format!("concurrent_chain_{}_depth_{}", chain_id, depth);
                    let value = format!("concurrent_value_{}_depth_{}", chain_id, depth);

                    if let Ok(_) = db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()) {
                        local_ops += 1;
                    }
                } else {
                    break; // Stop if we can't create more transactions
                }
            }

            // Commit or rollback based on chain_id (mix of success/failure)
            if chain_id % 3 == 0 {
                // Rollback entire chain
                while let Some(tx_id) = chain_transactions.pop() {
                    let _ = db_clone.abort_transaction(tx_id);
                }
            } else {
                // Commit entire chain
                while let Some(tx_id) = chain_transactions.pop() {
                    if let Err(_) = db_clone.commit_transaction(tx_id) {
                        // If commit fails, rollback remaining
                        while let Some(remaining_tx) = chain_transactions.pop() {
                            let _ = db_clone.abort_transaction(remaining_tx);
                        }
                        break;
                    }
                }
            }

            completed_chains.fetch_add(1, Ordering::Relaxed);
            total_tx_ops.fetch_add(local_ops, Ordering::Relaxed);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let concurrent_duration = concurrent_start.elapsed();
    let chains_completed = completed_chains.load(Ordering::Relaxed);
    let tx_ops_completed = total_tx_ops.load(Ordering::Relaxed);

    println!("Concurrent nested transactions completed:");
    println!(
        "  {} chains completed in {:.2}ms",
        chains_completed,
        concurrent_duration.as_millis()
    );
    println!("  {} transaction operations total", tx_ops_completed);

    // Test 3: Transaction rollback at various depths
    println!("Test 3: Transaction rollbacks at various depths...");

    let rollback_start = Instant::now();

    for rollback_depth in [5, 15, 25, 35, 45] {
        let mut rollback_stack = Vec::new();

        // Create transactions up to rollback depth
        for depth in 0..rollback_depth {
            let tx_id = db.begin_transaction().unwrap();
            rollback_stack.push(tx_id);

            let key = format!("rollback_test_depth_{}", depth);
            let value = format!("rollback_value_depth_{}", depth);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Rollback all transactions
        let rollback_tx_start = Instant::now();
        while let Some(tx_id) = rollback_stack.pop() {
            db.abort_transaction(tx_id).unwrap();
        }
        let rollback_tx_time = rollback_tx_start.elapsed();

        // Verify data was rolled back
        for depth in 0..rollback_depth {
            let key = format!("rollback_test_depth_{}", depth);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_none(), "Data not rolled back at depth {}", depth);
        }

        println!(
            "  Rollback depth {}: {:.2}ms",
            rollback_depth,
            rollback_tx_time.as_millis()
        );
    }

    let rollback_duration = rollback_start.elapsed();
    println!(
        "Rollback tests completed in {:.2}ms",
        rollback_duration.as_millis()
    );

    println!("Deeply nested transaction test completed successfully");

    // Assertions
    assert_eq!(
        verified_keys,
        MAX_NESTING_DEPTH * OPERATIONS_PER_LEVEL,
        "Not all nested transaction data committed"
    );
    assert_eq!(
        chains_completed, CONCURRENT_CHAINS,
        "Not all concurrent chains completed"
    );
    assert!(
        tx_ops_completed > 0,
        "No transaction operations completed in concurrent test"
    );
    assert!(
        total_time.as_millis() < 10_000,
        "Nested transaction performance too slow: {}ms",
        total_time.as_millis()
    );
}

#[test]
#[ignore] // Long-running test
fn test_rapid_connection_cycles() {
    let dir = tempdir().unwrap();

    const NUM_CYCLES: usize = 1000;
    const OPS_PER_CYCLE: usize = 100;

    println!("Testing {} rapid database open/close cycles...", NUM_CYCLES);

    let start_time = Instant::now();
    let mut cycle_times = Vec::new();
    let mut total_operations = 0;

    for cycle in 0..NUM_CYCLES {
        let cycle_start = Instant::now();

        // Create database instance
        let config = LightningDbConfig {
            cache_size: 20 * MB,
            ..Default::default()
        };
        let db = Database::create(dir.path(), config).unwrap();

        // Perform rapid operations
        for op in 0..OPS_PER_CYCLE {
            let key = format!("rapid_cycle_{}_op_{}", cycle, op);
            let value = format!("rapid_value_{}_op_{}", cycle, op);

            db.put(key.as_bytes(), value.as_bytes()).unwrap();

            // Occasional reads and transactions
            if op % 10 == 0 {
                let _ = db.get(key.as_bytes()).unwrap();
            }

            if op % 20 == 0 {
                let tx_id = db.begin_transaction().unwrap();
                let tx_key = format!("rapid_tx_{}_op_{}", cycle, op);
                let tx_value = format!("rapid_tx_value_{}_op_{}", cycle, op);
                db.put_tx(tx_id, tx_key.as_bytes(), tx_value.as_bytes())
                    .unwrap();

                if op % 40 == 0 {
                    db.commit_transaction(tx_id).unwrap();
                } else {
                    db.abort_transaction(tx_id).unwrap();
                }
            }

            total_operations += 1;
        }

        // Explicitly drop database to close it
        drop(db);

        let cycle_duration = cycle_start.elapsed();
        cycle_times.push(cycle_duration);

        if cycle % 100 == 99 {
            let elapsed = start_time.elapsed();
            let cycles_per_sec = (cycle + 1) as f64 / elapsed.as_secs_f64();
            let avg_cycle_time = cycle_times.iter().sum::<Duration>() / cycle_times.len() as u32;

            println!(
                "Cycle {}: avg {:.2}ms/cycle ({:.0} cycles/sec)",
                cycle + 1,
                avg_cycle_time.as_millis(),
                cycles_per_sec
            );
        }
    }

    let total_duration = start_time.elapsed();
    let cycles_per_sec = NUM_CYCLES as f64 / total_duration.as_secs_f64();
    let ops_per_sec = total_operations as f64 / total_duration.as_secs_f64();

    let avg_cycle_time = cycle_times.iter().sum::<Duration>() / cycle_times.len() as u32;
    let min_cycle_time = cycle_times.iter().min().unwrap();
    let max_cycle_time = cycle_times.iter().max().unwrap();

    println!("Rapid connection cycles completed:");
    println!(
        "  {} cycles in {:.2}s ({:.0} cycles/sec)",
        NUM_CYCLES,
        total_duration.as_secs_f64(),
        cycles_per_sec
    );
    println!(
        "  {} operations total ({:.0} ops/sec)",
        total_operations, ops_per_sec
    );
    println!(
        "  Cycle times: avg {:.2}ms, min {:.2}ms, max {:.2}ms",
        avg_cycle_time.as_millis(),
        min_cycle_time.as_millis(),
        max_cycle_time.as_millis()
    );

    // Final verification - open database once more and check data persistence
    println!("Final verification of data persistence...");
    let final_db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    let final_range = final_db.range(None, None).unwrap();
    let final_key_count = final_range.len();

    // Verify some specific keys exist
    let mut verified_keys = 0;
    for cycle in (0..NUM_CYCLES).step_by(50) {
        // Check every 50th cycle
        for op in (0..OPS_PER_CYCLE).step_by(10) {
            // Check every 10th operation
            let key = format!("rapid_cycle_{}_op_{}", cycle, op);
            if let Ok(Some(_)) = final_db.get(key.as_bytes()) {
                verified_keys += 1;
            }
        }
    }

    println!("Final verification:");
    println!("  Total keys in database: {}", final_key_count);
    println!("  Spot-checked keys: {} verified", verified_keys);

    // Assertions
    assert!(
        cycles_per_sec > 10.0,
        "Connection cycle rate too slow: {:.0} cycles/sec",
        cycles_per_sec
    );
    assert!(
        ops_per_sec > 1000.0,
        "Operation rate too slow during rapid cycles: {:.0} ops/sec",
        ops_per_sec
    );
    assert!(
        avg_cycle_time.as_millis() < 500,
        "Average cycle time too slow: {}ms",
        avg_cycle_time.as_millis()
    );
    assert!(
        max_cycle_time.as_millis() < 2000,
        "Maximum cycle time too slow: {}ms",
        max_cycle_time.as_millis()
    );
    assert!(
        final_key_count > NUM_CYCLES * OPS_PER_CYCLE / 2,
        "Too much data loss during rapid cycles"
    );
    assert!(
        verified_keys > 0,
        "No spot-checked keys found after rapid cycles"
    );

    println!("Rapid connection cycles test completed successfully");
}
