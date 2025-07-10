/// Edge Case and Optimization Analysis
///
/// Test edge cases and identify optimization opportunities
use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 LIGHTNING DB EDGE CASE & OPTIMIZATION ANALYSIS");
    println!("=================================================\n");

    // Test 1: Empty and Single Key Operations
    println!("1️⃣ Empty Database and Single Key Performance");
    test_empty_database()?;

    // Test 2: Key Size Limits
    println!("\n2️⃣ Key Size Limits and Performance");
    test_key_size_limits()?;

    // Test 3: Value Size Edge Cases
    println!("\n3️⃣ Value Size Edge Cases");
    test_value_edge_cases()?;

    // Test 4: Transaction Edge Cases
    println!("\n4️⃣ Transaction Edge Cases");
    test_transaction_edge_cases()?;

    // Test 5: Concurrent Write Patterns
    println!("\n5️⃣ Concurrent Write Pattern Analysis");
    test_concurrent_write_patterns()?;

    // Test 6: Delete Performance
    println!("\n6️⃣ Delete Operation Performance");
    test_delete_performance()?;

    // Test 7: Database Reopening
    println!("\n7️⃣ Database Reopen Performance");
    test_database_reopen()?;

    // Test 8: Memory Limit Behavior
    println!("\n8️⃣ Memory Limit Behavior");
    test_memory_limits()?;

    Ok(())
}

fn test_empty_database() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    // Test operations on empty database
    let start = Instant::now();
    let result = db.get(b"nonexistent")?;
    let empty_get_time = start.elapsed();

    println!(
        "   Empty database get: {:?} in {:.2}μs",
        result.is_none(),
        empty_get_time.as_nanos() as f64 / 1000.0
    );

    // First write performance
    let start = Instant::now();
    db.put(b"first_key", b"first_value")?;
    let first_write_time = start.elapsed();

    println!(
        "   First write: {:.2}μs",
        first_write_time.as_nanos() as f64 / 1000.0
    );

    // Second write (non-empty database)
    let start = Instant::now();
    db.put(b"second_key", b"second_value")?;
    let second_write_time = start.elapsed();

    println!(
        "   Second write: {:.2}μs",
        second_write_time.as_nanos() as f64 / 1000.0
    );

    // Range scan on nearly empty database
    let start = Instant::now();
    let count = db.scan(None, None)?.count();
    let scan_time = start.elapsed();

    println!(
        "   Full scan ({} items): {:.2}μs",
        count,
        scan_time.as_nanos() as f64 / 1000.0
    );

    Ok(())
}

fn test_key_size_limits() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    let key_sizes = vec![1, 8, 16, 32, 64, 128, 256, 512, 1024];

    println!("   Testing different key sizes:");

    for &size in &key_sizes {
        let key = vec![b'k'; size];
        let value = b"test_value";

        let write_start = Instant::now();
        let write_result = db.put(&key, value);
        let write_time = write_start.elapsed();

        let read_start = Instant::now();
        let read_result = db.get(&key);
        let read_time = read_start.elapsed();

        println!(
            "     {} bytes: Write {:.2}μs ({}), Read {:.2}μs ({})",
            size,
            write_time.as_nanos() as f64 / 1000.0,
            if write_result.is_ok() { "OK" } else { "FAIL" },
            read_time.as_nanos() as f64 / 1000.0,
            if read_result.is_ok() { "OK" } else { "FAIL" }
        );
    }

    Ok(())
}

fn test_value_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    // Test empty value
    let start = Instant::now();
    db.put(b"empty_value", b"")?;
    let empty_write = start.elapsed();

    let start = Instant::now();
    let result = db.get(b"empty_value")?;
    let empty_read = start.elapsed();

    println!(
        "   Empty value: Write {:.2}μs, Read {:.2}μs, Retrieved: {}",
        empty_write.as_nanos() as f64 / 1000.0,
        empty_read.as_nanos() as f64 / 1000.0,
        result.map(|v| v.is_empty()).unwrap_or(false)
    );

    // Test very large value
    let large_value = vec![b'x'; 10 * 1024 * 1024]; // 10MB
    let start = Instant::now();
    let large_result = db.put(b"large_value", &large_value);
    let large_write = start.elapsed();

    println!(
        "   10MB value: Write {:.2}ms ({})",
        large_write.as_millis(),
        if large_result.is_ok() { "OK" } else { "FAIL" }
    );

    Ok(())
}

fn test_transaction_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    // Test empty transaction
    let start = Instant::now();
    let tx_id = db.begin_transaction()?;
    db.commit_transaction(tx_id)?;
    let empty_tx_time = start.elapsed();

    println!(
        "   Empty transaction: {:.2}μs",
        empty_tx_time.as_nanos() as f64 / 1000.0
    );

    // Test large transaction
    let tx_id = db.begin_transaction()?;
    let start = Instant::now();

    for i in 0..1000 {
        let key = format!("tx_key_{}", i);
        db.put_tx(tx_id, key.as_bytes(), b"value")?;
    }

    let prepare_time = start.elapsed();

    let commit_start = Instant::now();
    db.commit_transaction(tx_id)?;
    let commit_time = commit_start.elapsed();

    println!(
        "   Large transaction (1000 ops): Prepare {:.2}ms, Commit {:.2}ms",
        prepare_time.as_millis(),
        commit_time.as_millis()
    );

    // Test transaction abort
    let tx_id = db.begin_transaction()?;
    for i in 0..100 {
        let key = format!("abort_key_{}", i);
        db.put_tx(tx_id, key.as_bytes(), b"aborted")?;
    }

    let start = Instant::now();
    db.abort_transaction(tx_id)?;
    let abort_time = start.elapsed();

    // Verify aborted data doesn't exist
    let aborted_exists = db.get(b"abort_key_0")?.is_some();

    println!(
        "   Transaction abort (100 ops): {:.2}μs, Data exists: {}",
        abort_time.as_nanos() as f64 / 1000.0,
        aborted_exists
    );

    Ok(())
}

fn test_concurrent_write_patterns() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(temp_dir.path(), config)?);

    let patterns = vec![("Same Key", true), ("Different Keys", false)];

    for (pattern_name, same_key) in patterns {
        let num_threads = 4;
        let writes_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));
        let success_count = Arc::new(Mutex::new(0));

        let start = Instant::now();
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let db_clone = db.clone();
            let barrier_clone = barrier.clone();
            let success_clone = success_count.clone();

            let handle = thread::spawn(move || {
                barrier_clone.wait();

                for i in 0..writes_per_thread {
                    let key = if same_key {
                        "shared_key".to_string()
                    } else {
                        format!("thread_{}_key_{}", thread_id, i)
                    };

                    let value = format!("value_{}_{}", thread_id, i);

                    if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                        *success_clone.lock().unwrap() += 1;
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_writes = num_threads * writes_per_thread;
        let successful = *success_count.lock().unwrap();
        let throughput = successful as f64 / duration.as_secs_f64();

        println!(
            "   {} pattern: {}/{} successful, {:.0} ops/sec",
            pattern_name, successful, total_writes, throughput
        );
    }

    Ok(())
}

fn test_delete_performance() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    // Populate database
    let num_keys = 10_000;
    for i in 0..num_keys {
        let key = format!("delete_test_{:06}", i);
        db.put(key.as_bytes(), b"to_be_deleted")?;
    }

    // Test sequential deletes
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("delete_test_{:06}", i);
        db.delete(key.as_bytes())?;
    }
    let seq_delete_time = start.elapsed();
    let seq_throughput = 1000.0 / seq_delete_time.as_secs_f64();

    // Test random deletes
    let start = Instant::now();
    for i in (5000..6000).rev() {
        let key = format!("delete_test_{:06}", i);
        db.delete(key.as_bytes())?;
    }
    let random_delete_time = start.elapsed();
    let random_throughput = 1000.0 / random_delete_time.as_secs_f64();

    println!("   Sequential deletes: {:.0} ops/sec", seq_throughput);
    println!("   Random deletes: {:.0} ops/sec", random_throughput);

    // Verify deletes
    let deleted_key = db.get(b"delete_test_000000")?.is_some();
    let existing_key = db.get(b"delete_test_002000")?.is_some();

    println!(
        "   Verification: Deleted exists: {}, Undeleted exists: {}",
        deleted_key, existing_key
    );

    Ok(())
}

fn test_database_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();

    // Initial database creation and population
    let start = Instant::now();
    {
        let db = Database::create(temp_dir.path(), config.clone())?;

        for i in 0..10_000 {
            let key = format!("reopen_test_{:06}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
    }
    let create_time = start.elapsed();

    // Reopen database
    let start = Instant::now();
    let db = Database::open(temp_dir.path(), config)?;
    let open_time = start.elapsed();

    // Verify data
    let start = Instant::now();
    let mut verified = 0;
    for i in (0..10_000).step_by(100) {
        let key = format!("reopen_test_{:06}", i);
        if db.get(key.as_bytes())?.is_some() {
            verified += 1;
        }
    }
    let verify_time = start.elapsed();

    println!("   Initial creation: {:.2}s", create_time.as_secs_f64());
    println!("   Reopen time: {:.2}ms", open_time.as_millis());
    println!(
        "   Verification ({} samples): {:.2}ms",
        verified,
        verify_time.as_millis()
    );

    Ok(())
}

fn test_memory_limits() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig {
        cache_size: 1024 * 1024,
        ..Default::default()
    }; // 1MB cache only

    let db = Database::create(temp_dir.path(), config)?;

    // Write data much larger than cache
    let value_size = 1024; // 1KB values
    let num_values = 10_000; // 10MB total

    let start = Instant::now();
    for i in 0..num_values {
        let key = format!("mem_limit_{:06}", i);
        let value = vec![b'x'; value_size];
        db.put(key.as_bytes(), &value)?;
    }
    let write_time = start.elapsed();
    let write_throughput = num_values as f64 / write_time.as_secs_f64();

    // Read pattern that exceeds cache
    let start = Instant::now();
    let mut read_count = 0;
    for i in (0..num_values).step_by(100) {
        let key = format!("mem_limit_{:06}", i);
        if db.get(key.as_bytes())?.is_some() {
            read_count += 1;
        }
    }
    let read_time = start.elapsed();

    println!("   Small cache (1MB) with 10MB data:");
    println!("     Write: {:.0} ops/sec", write_throughput);
    println!(
        "     Read {} items: {:.2}ms",
        read_count,
        read_time.as_millis()
    );

    let stats = db.stats();
    println!(
        "     Final stats: {} pages, {} free pages",
        stats.page_count, stats.free_page_count
    );

    Ok(())
}
