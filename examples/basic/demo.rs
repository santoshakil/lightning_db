use lightning_db::{Database, LightningDbConfig};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

// Configuration constants
const CACHE_SIZE_MB: usize = 100;
const MAX_ACTIVE_TRANSACTIONS: usize = 100;

// Operation count constants
const BASIC_OPS_COUNT: usize = 10;
const SEQUENTIAL_WRITE_SMALL: usize = 1000;
const SEQUENTIAL_WRITE_LARGE: usize = 10000;
const RANDOM_READ_OPS: usize = 5000;
const MIXED_WORKLOAD_OPS: usize = 10000;
const CONCURRENT_OPS_PER_THREAD: usize = 1000;
const TRANSACTION_BATCH_COUNT: usize = 100;
const TRANSACTION_OPS_PER_BATCH: usize = 10;
const CACHE_TEST_OPS: usize = 1000;
const LARGE_VALUE_COUNT: usize = 100;

// Value size constants
const SMALL_VALUE_SIZE: usize = 100;
const LARGE_VALUE_SIZE: usize = 1000;
const MIXED_VALUE_SIZE: usize = 100;
const MEGA_VALUE_SIZE: usize = 1_000_000;

// Thread count configuration
const THREAD_COUNTS: &[usize] = &[2, 4, 8];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_separator("Lightning DB Comprehensive Demo");

    // Create database
    let db_path = "data/demo_db";
    if std::path::Path::new(db_path).exists() {
        if std::fs::metadata(db_path)?.is_file() {
            std::fs::remove_file(db_path)?;
        } else {
            std::fs::remove_dir_all(db_path)?;
        }
    }

    let config = LightningDbConfig {
        cache_size: (CACHE_SIZE_MB * 1024 * 1024) as u64,
        compression_enabled: true,
        compression_type: 1, // Zstd
        max_active_transactions: MAX_ACTIVE_TRANSACTIONS,
        ..Default::default()
    };

    println!("\nDatabase Configuration:");
    println!("  Path: {:?}", db_path);
    println!("  Cache Size: {} MB", CACHE_SIZE_MB);
    println!("  Compression: Zstd");
    println!("  LSM Tree: Enabled");
    println!("  Max Active Transactions: {}", MAX_ACTIVE_TRANSACTIONS);

    let start = Instant::now();
    let db = Database::create(db_path, config)?;
    let db_arc = Arc::new(db);
    println!("\nDatabase created in {}", format_duration(start.elapsed()));

    // 1. Basic Operations Demo
    basic_operations_demo(&db_arc)?;

    // 2. Transaction Demo
    transaction_demo(&db_arc)?;

    // 3. Performance Benchmarks
    performance_benchmarks(&db_arc)?;

    // 4. LSM Tree Demo
    lsm_tree_demo(&db_arc)?;

    // 5. Memory Management & Caching Demo
    memory_and_cache_demo(&db_arc)?;

    // Final Statistics
    print_section("Final Database Statistics");
    display_final_stats(&db_arc)?;

    print_separator("Demo Complete");

    Ok(())
}

fn basic_operations_demo(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    print_section("Basic Operations");

    // PUT operations
    println!("\n1. PUT Operations:");
    let mut put_times = Vec::new();

    for i in 0..BASIC_OPS_COUNT {
        let key = format!("basic_key_{:02}", i);
        let value = format!("Basic value {} with some additional data", i);

        let start = Instant::now();
        db.put(key.as_bytes(), value.as_bytes())?;
        let elapsed = start.elapsed();
        put_times.push(elapsed);

        println!("  PUT {} = {} ({})", key, value, format_duration(elapsed));
    }

    let avg_put = put_times.iter().sum::<Duration>() / put_times.len() as u32;
    println!("\n  Average PUT time: {}", format_duration(avg_put));

    // GET operations
    println!("\n2. GET Operations:");
    let mut get_times = Vec::new();

    for i in 0..BASIC_OPS_COUNT {
        let key = format!("basic_key_{:02}", i);

        let start = Instant::now();
        let result = db.get(key.as_bytes())?;
        let elapsed = start.elapsed();
        get_times.push(elapsed);

        match result {
            Some(value) => println!(
                "  GET {} = {} ({})",
                key,
                String::from_utf8_lossy(&value),
                format_duration(elapsed)
            ),
            None => println!("  GET {} = NOT FOUND", key),
        }
    }

    let avg_get = get_times.iter().sum::<Duration>() / get_times.len() as u32;
    println!("\n  Average GET time: {}", format_duration(avg_get));

    // DELETE operations
    println!("\n3. DELETE Operations:");
    let keys_to_delete = ["basic_key_03", "basic_key_07"];

    for key in &keys_to_delete {
        let start = Instant::now();
        db.delete(key.as_bytes())?;
        let elapsed = start.elapsed();
        println!("  DELETE {} ({})", key, format_duration(elapsed));
    }

    // Verify deletions
    println!("\n4. Verify Deletions:");
    for key in &keys_to_delete {
        match db.get(key.as_bytes())? {
            Some(_) => println!("  {} still exists (ERROR!)", key),
            None => println!("  {} successfully deleted", key),
        }
    }

    Ok(())
}

fn transaction_demo(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    print_section("Transaction Functionality");

    // 1. Basic Transaction
    println!("\n1. Basic Transaction:");

    // Initialize counter
    db.put(b"counter", b"0")?;

    let tx_id = db.begin_transaction()?;
    println!("  Started transaction: {}", tx_id);

    // Read current value
    let current = db
        .get_tx(tx_id, b"counter")?
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);
    println!("  Read counter: {}", current);

    // Update value
    db.put_tx(tx_id, b"counter", (current + 1).to_string().as_bytes())?;
    db.put_tx(tx_id, b"tx_data", b"transaction data")?;
    println!("  Updated counter to {} and added tx_data", current + 1);

    // Read uncommitted changes
    let tx_value = db.get_tx(tx_id, b"counter")?;
    let main_value = db.get(b"counter")?;
    println!(
        "  Value in transaction: {:?}",
        tx_value.map(|v| String::from_utf8_lossy(&v).to_string())
    );
    println!(
        "  Value outside transaction: {:?}",
        main_value.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // Commit
    db.commit_transaction(tx_id)?;
    println!("  Transaction committed");

    // Verify commit
    let final_value = db.get(b"counter")?;
    println!(
        "  Final counter value: {:?}",
        final_value.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // 2. Isolation Test
    println!("\n2. Transaction Isolation:");

    // Set initial value
    db.put(b"isolated_key", b"initial")?;

    let tx1 = db.begin_transaction()?;
    let tx2 = db.begin_transaction()?;
    println!("  Started TX1: {} and TX2: {}", tx1, tx2);

    // TX1 reads
    let tx1_read = db.get_tx(tx1, b"isolated_key")?;
    println!(
        "  TX1 reads: {:?}",
        tx1_read.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // TX2 modifies
    db.put_tx(tx2, b"isolated_key", b"modified_by_tx2")?;
    println!("  TX2 writes: 'modified_by_tx2'");

    // TX1 reads again (should see old value)
    let tx1_read2 = db.get_tx(tx1, b"isolated_key")?;
    println!(
        "  TX1 reads again: {:?} (snapshot isolation)",
        tx1_read2.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // Commit TX2
    db.commit_transaction(tx2)?;
    println!("  TX2 committed");

    // TX1 still sees old value
    let tx1_read3 = db.get_tx(tx1, b"isolated_key")?;
    println!(
        "  TX1 still sees: {:?}",
        tx1_read3.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    db.abort_transaction(tx1)?;
    println!("  TX1 aborted");

    // 3. Write-Write Conflict
    println!("\n3. Write-Write Conflict:");

    // Set initial value
    db.put(b"conflict_key", b"initial")?;

    let tx3 = db.begin_transaction()?;
    let tx4 = db.begin_transaction()?;

    // Both read first
    let _ = db.get_tx(tx3, b"conflict_key")?;
    let _ = db.get_tx(tx4, b"conflict_key")?;

    // Both write
    db.put_tx(tx3, b"conflict_key", b"tx3_value")?;
    db.put_tx(tx4, b"conflict_key", b"tx4_value")?;
    println!("  TX3 and TX4 both wrote to conflict_key");

    // Try to commit both
    let tx3_result = db.commit_transaction(tx3);
    let tx4_result = db.commit_transaction(tx4);

    match (&tx3_result, &tx4_result) {
        (Ok(()), Ok(())) => println!("  Both succeeded (ERROR!)"),
        (Ok(()), Err(e)) => {
            println!("  TX3: SUCCESS");
            println!("  TX4: FAILED - {} (expected)", e);
        }
        (Err(e), Ok(())) => {
            println!("  TX3: FAILED - {}", e);
            println!("  TX4: SUCCESS");
        }
        (Err(e1), Err(e2)) => {
            println!("  TX3: FAILED - {}", e1);
            println!("  TX4: FAILED - {}", e2);
        }
    }

    // 4. Rollback Test
    println!("\n4. Transaction Rollback:");

    db.put(b"rollback_test", b"original")?;

    let tx5 = db.begin_transaction()?;
    db.put_tx(tx5, b"rollback_test", b"modified")?;
    db.put_tx(tx5, b"new_key", b"new_value")?;
    println!("  Made changes in TX5");

    db.abort_transaction(tx5)?;
    println!("  TX5 aborted");

    // Verify rollback
    let val1 = db.get(b"rollback_test")?;
    let val2 = db.get(b"new_key")?;
    println!(
        "  rollback_test: {:?} (should be 'original')",
        val1.map(|v| String::from_utf8_lossy(&v).to_string())
    );
    println!(
        "  new_key: {:?} (should be None)",
        val2.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    Ok(())
}

fn performance_benchmarks(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    print_section("Performance Benchmarks");

    // 1. Sequential Write Benchmark
    println!("\n1. Sequential Write Performance:");
    let mut rng = StdRng::seed_from_u64(42);

    for (num_ops, value_size) in &[
        (SEQUENTIAL_WRITE_SMALL, SMALL_VALUE_SIZE),
        (SEQUENTIAL_WRITE_LARGE, SMALL_VALUE_SIZE),
        (SEQUENTIAL_WRITE_SMALL, LARGE_VALUE_SIZE),
    ] {
        let value: Vec<u8> = (0..*value_size).map(|_| rng.random()).collect();

        let start = Instant::now();
        for i in 0..*num_ops {
            let key = format!("seq_bench_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        let duration = start.elapsed();
        let ops_per_sec = *num_ops as f64 / duration.as_secs_f64();

        println!(
            "  {} ops x {} bytes: {} ({} ops/sec)",
            format_number(*num_ops as u64),
            value_size,
            format_duration(duration),
            format_number(ops_per_sec as u64)
        );
    }

    // 2. Random Read Benchmark
    println!("\n2. Random Read Performance:");
    let mut hits = 0;
    let num_reads = RANDOM_READ_OPS;

    let start = Instant::now();
    for _ in 0..num_reads {
        let key_num = rng.random_range(0..RANDOM_READ_OPS);
        let key = format!("seq_bench_{:08}", key_num);
        if db.get(key.as_bytes())?.is_some() {
            hits += 1;
        }
    }
    let duration = start.elapsed();
    let ops_per_sec = num_reads as f64 / duration.as_secs_f64();

    println!(
        "  {} random reads: {} ({} ops/sec, {} hits)",
        format_number(num_reads as u64),
        format_duration(duration),
        format_number(ops_per_sec as u64),
        format_number(hits)
    );

    // 3. Mixed Workload Benchmark
    println!("\n3. Mixed Workload (80/20 read/write):");
    let num_mixed = MIXED_WORKLOAD_OPS;
    let mut reads = 0;
    let mut writes = 0;

    let start = Instant::now();
    for i in 0..num_mixed {
        if rng.random::<f32>() < 0.8 {
            // Read
            let key_num = rng.random_range(0..i.max(1));
            let key = format!("mixed_key_{:08}", key_num);
            let _ = db.get(key.as_bytes())?;
            reads += 1;
        } else {
            // Write
            let key = format!("mixed_key_{:08}", i);
            let value: Vec<u8> = (0..MIXED_VALUE_SIZE).map(|_| rng.random()).collect();
            db.put(key.as_bytes(), &value)?;
            writes += 1;
        }
    }
    let duration = start.elapsed();
    let ops_per_sec = num_mixed as f64 / duration.as_secs_f64();

    println!(
        "  {} operations ({} reads, {} writes): {} ({} ops/sec)",
        format_number(num_mixed as u64),
        format_number(reads),
        format_number(writes),
        format_duration(duration),
        format_number(ops_per_sec as u64)
    );

    // 4. Concurrent Access Benchmark
    println!("\n4. Concurrent Access Performance:");

    for num_threads in THREAD_COUNTS {
        let barrier = Arc::new(Barrier::new(*num_threads + 1));
        let ops_per_thread = CONCURRENT_OPS_PER_THREAD;

        let handles: Vec<_> = (0..*num_threads)
            .map(|thread_id| {
                let db = Arc::clone(db);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                    let value: Vec<u8> = (0..MIXED_VALUE_SIZE).map(|_| rng.random()).collect();

                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = format!("thread_{}_key_{:06}", thread_id, i);
                        db.put(key.as_bytes(), &value).unwrap();
                    }
                })
            })
            .collect();

        barrier.wait();
        let start = Instant::now();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = num_threads * ops_per_thread;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

        println!(
            "  {} threads x {} ops: {} ({} ops/sec)",
            num_threads,
            ops_per_thread,
            format_duration(duration),
            format_number(ops_per_sec as u64)
        );
    }

    // 5. Transaction Performance
    println!("\n5. Transaction Performance:");

    let start = Instant::now();
    for i in 0..TRANSACTION_BATCH_COUNT {
        let tx_id = db.begin_transaction()?;
        for j in 0..TRANSACTION_OPS_PER_BATCH {
            let key = format!("tx_bench_{}_{}", i, j);
            let value = format!("transaction_value_{}", j);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
        }
        db.commit_transaction(tx_id)?;
    }
    let duration = start.elapsed();
    let tx_per_sec = TRANSACTION_BATCH_COUNT as f64 / duration.as_secs_f64();

    println!(
        "  {} transactions ({} ops each): {} ({:.0} tx/sec)",
        TRANSACTION_BATCH_COUNT,
        TRANSACTION_OPS_PER_BATCH,
        format_duration(duration),
        tx_per_sec
    );

    Ok(())
}

fn lsm_tree_demo(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    print_section("LSM Tree and Compression Features");

    // Note: In this implementation, LSM tree is used as a storage layer
    // but operations go through the transactional system

    // 1. Initial Database State
    println!("\n1. Initial Database State:");
    let stats = db.stats();
    println!("  Page count: {}", stats.page_count);
    println!("  Free pages: {}", stats.free_page_count);
    println!("  B+Tree height: {}", stats.tree_height);

    // 2. Insert Data to Build Up Storage
    println!("\n2. Inserting Data for Storage Test:");
    let start = Instant::now();

    for i in 0..1000 {
        let key = format!("storage_key_{:04}", i);
        let value = format!(
            "This is a test value for storage demonstration. Entry number: {}. \
                           Adding some padding to make the value larger and test compression.",
            i
        );
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    let duration = start.elapsed();
    println!("  Inserted 1,000 entries in {}", format_duration(duration));
    println!(
        "  Rate: {} ops/sec",
        format_number((1000.0 / duration.as_secs_f64()) as u64)
    );

    // 3. Check Storage After Insertions
    println!("\n3. Database State After Insertions:");
    let stats = db.stats();
    println!("  Page count: {}", stats.page_count);
    println!("  Free pages: {}", stats.free_page_count);
    println!("  B+Tree height: {}", stats.tree_height);

    // If LSM tree is available, show its stats
    if let Some(lsm_stats) = db.lsm_stats() {
        println!("\n  LSM Tree Stats:");
        println!(
            "    Memtable size: {} bytes",
            format_number(lsm_stats.memtable_size as u64)
        );
        println!("    Immutable memtables: {}", lsm_stats.immutable_count);
        println!(
            "    Cache hits: {}",
            format_number(lsm_stats.cache_hits as u64)
        );
        println!(
            "    Cache misses: {}",
            format_number(lsm_stats.cache_misses as u64)
        );
        println!("    Cache hit rate: {:.2}%", lsm_stats.cache_hit_rate);
        for level in lsm_stats.levels {
            if level.num_files > 0 || level.level == 0 {
                println!(
                    "    L{}: {} files, {} bytes",
                    level.level,
                    level.num_files,
                    format_number(level.size_bytes as u64)
                );
            }
        }
    }

    // 4. Test Compression
    println!("\n4. Compression Test:");
    let uncompressed = "The quick brown fox jumps over the lazy dog. ".repeat(100);
    let key = b"compression_test";

    println!(
        "  Original size: {} bytes",
        format_number(uncompressed.len() as u64)
    );

    let start = Instant::now();
    db.put(key, uncompressed.as_bytes())?;
    let write_time = start.elapsed();

    let start = Instant::now();
    let _read = db.get(key)?;
    let read_time = start.elapsed();

    println!("  Write time: {}", format_duration(write_time));
    println!("  Read time: {}", format_duration(read_time));

    // 5. Delete Operations
    println!("\n5. Delete Operations:");
    for i in (0..10).step_by(2) {
        let key = format!("storage_key_{:04}", i);
        db.delete(key.as_bytes())?;
        println!("  Deleted {}", key);
    }

    // Verify deletions
    println!("\n6. Verify Deletions:");
    for i in 0..10 {
        let key = format!("storage_key_{:04}", i);
        match db.get(key.as_bytes())? {
            Some(value) => {
                if value.is_empty() {
                    println!("  {} = [TOMBSTONE]", key);
                } else {
                    println!("  {} = {} bytes", key, value.len());
                }
            }
            None => println!("  {} = NOT FOUND", key),
        }
    }

    // 7. Database Sync
    println!("\n7. Syncing Database to Disk:");
    let start = Instant::now();
    db.sync()?;
    let duration = start.elapsed();
    println!("  Database synced in {}", format_duration(duration));

    Ok(())
}

fn memory_and_cache_demo(db: &Arc<Database>) -> Result<(), Box<dyn std::error::Error>> {
    print_section("Memory Management and Caching");

    // 1. Initial Cache State
    println!("\n1. Initial Cache Statistics:");
    display_cache_stats(db)?;

    // 2. Sequential Access (Cache-Friendly)
    println!("\n2. Sequential Access Pattern:");
    let start = Instant::now();

    for i in 0..100 {
        let key = format!("cache_test_{:03}", i);
        let value = format!("Cache test value {}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Read same keys sequentially
    for i in 0..100 {
        let key = format!("cache_test_{:03}", i);
        let _ = db.get(key.as_bytes())?;
    }

    let duration = start.elapsed();
    println!(
        "  Sequential access completed in {}",
        format_duration(duration)
    );

    println!("\n3. Cache Statistics After Sequential Access:");
    display_cache_stats(db)?;

    // 3. Random Access (Cache Test)
    println!("\n4. Random Access Pattern:");
    let mut rng = StdRng::seed_from_u64(42);
    let start = Instant::now();
    let mut cache_hits = 0;

    for _ in 0..CACHE_TEST_OPS {
        let key_num = rng.random_range(0..CACHE_TEST_OPS);
        let key = format!("cache_test_{:03}", key_num);
        if db.get(key.as_bytes())?.is_some() {
            cache_hits += 1;
        }
    }

    let duration = start.elapsed();
    println!(
        "  {} random reads completed in {}",
        CACHE_TEST_OPS,
        format_duration(duration)
    );
    println!(
        "  Hit rate: {:.1}%",
        (cache_hits as f64 / CACHE_TEST_OPS as f64) * 100.0
    );

    println!("\n5. Cache Statistics After Random Access:");
    display_cache_stats(db)?;

    // 4. Large Value Test (Cache Eviction)
    println!("\n6. Large Value Test (Cache Pressure):");
    let large_value: Vec<u8> = (0..MEGA_VALUE_SIZE).map(|i| (i % 256) as u8).collect();

    for i in 0..LARGE_VALUE_COUNT {
        let key = format!("large_{}", i);
        db.put(key.as_bytes(), &large_value)?;
    }

    println!("  Inserted {} x 1MB values", LARGE_VALUE_COUNT);

    println!("\n7. Cache Statistics After Large Values:");
    display_cache_stats(db)?;

    // 5. Database Sync
    println!("\n8. Database Sync:");
    let start = Instant::now();
    db.sync()?;
    let duration = start.elapsed();
    println!("  Database synced in {}", format_duration(duration));

    Ok(())
}

fn display_lsm_stats(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(stats) = db.lsm_stats() {
        println!(
            "  Memtable size: {} bytes",
            format_number(stats.memtable_size as u64)
        );
        println!("  Immutable memtables: {}", stats.immutable_count);
        println!("  Cache hits: {}", format_number(stats.cache_hits as u64));
        println!(
            "  Cache misses: {}",
            format_number(stats.cache_misses as u64)
        );
        println!("  Cache hit rate: {:.2}%", stats.cache_hit_rate);
        println!("  Levels:");
        for level in stats.levels {
            if level.num_files > 0 || level.level == 0 {
                println!(
                    "    L{}: {} files, {} bytes",
                    level.level,
                    level.num_files,
                    format_number(level.size_bytes as u64)
                );
            }
        }
    } else {
        println!("  No LSM statistics available");
    }
    Ok(())
}

fn display_cache_stats(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(cache_stats) = db.cache_stats() {
        println!("  {}", cache_stats);
    } else {
        println!("  No cache statistics available");
    }
    Ok(())
}

fn display_final_stats(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let stats = db.stats();

    println!("\nDatabase Core Statistics:");
    println!("  Page count: {}", format_number(stats.page_count as u64));
    println!(
        "  Free pages: {}",
        format_number(stats.free_page_count as u64)
    );
    println!("  B+Tree height: {}", stats.tree_height);
    println!("  Active transactions: {}", stats.active_transactions);

    println!("\nCache Performance:");
    display_cache_stats(db)?;

    println!("\nLSM Tree State:");
    display_lsm_stats(db)?;

    Ok(())
}

// Helper functions for formatting
fn format_duration(d: Duration) -> String {
    if d.as_secs() > 0 {
        format!("{:.2}s", d.as_secs_f64())
    } else if d.as_millis() > 0 {
        format!("{:.2}ms", d.as_secs_f64() * 1000.0)
    } else {
        format!("{:.2}μs", d.as_secs_f64() * 1_000_000.0)
    }
}

fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}

fn print_separator(title: &str) {
    println!("\n{}", "=".repeat(60));
    println!("{:^60}", title);
    println!("{}", "=".repeat(60));
}

fn print_section(title: &str) {
    println!("\n{}", "-".repeat(60));
    println!("{}", title);
    println!("{}", "-".repeat(60));
}
