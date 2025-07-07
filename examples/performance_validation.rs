use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

const NUM_THREADS: usize = 8;
const VALUE_SIZE: usize = 100;

fn main() {
    println!("Lightning DB Performance Validation");
    println!("===================================\n");

    // Test different configurations
    test_configuration("Default", LightningDbConfig::default());

    let mut async_config = LightningDbConfig::default();
    async_config.wal_sync_mode = WalSyncMode::Async;
    test_configuration("Async WAL", async_config);

    let mut sync_config = LightningDbConfig::default();
    sync_config.wal_sync_mode = WalSyncMode::Sync;
    test_configuration("Sync WAL", sync_config);

    let mut cache_config = LightningDbConfig::default();
    cache_config.cache_size = 100 * 1024 * 1024; // 100MB
    test_configuration("With Cache", cache_config);

    let mut compressed_config = LightningDbConfig::default();
    compressed_config.compression_enabled = true;
    compressed_config.compression_type = 1; // Zstd
    test_configuration("With Compression", compressed_config);
}

fn test_configuration(name: &str, config: LightningDbConfig) {
    println!("Testing configuration: {}", name);
    println!("{}", "-".repeat(50));

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path(), config).unwrap());

    // Sequential write test
    let write_latency = sequential_write_test(Arc::clone(&db));

    // Sequential read test
    let read_latency = sequential_read_test(Arc::clone(&db));

    // Concurrent write test
    let concurrent_write_throughput = concurrent_write_test(Arc::clone(&db));

    // Concurrent read test
    let concurrent_read_throughput = concurrent_read_test(Arc::clone(&db));

    // Mixed workload test
    let mixed_stats = mixed_workload_test(Arc::clone(&db));

    // Range scan test
    let scan_throughput = range_scan_test(Arc::clone(&db));

    // Print results
    println!(
        "Sequential Write: {:.2} μs/op ({:.0} ops/sec)",
        write_latency,
        1_000_000.0 / write_latency
    );
    println!(
        "Sequential Read: {:.2} μs/op ({:.0} ops/sec)",
        read_latency,
        1_000_000.0 / read_latency
    );
    println!(
        "Concurrent Write: {:.0} ops/sec",
        concurrent_write_throughput
    );
    println!("Concurrent Read: {:.0} ops/sec", concurrent_read_throughput);
    println!(
        "Mixed Workload: {:.0} ops/sec ({:.1}% reads)",
        mixed_stats.0, mixed_stats.1
    );
    println!("Range Scan: {:.0} entries/sec", scan_throughput);

    // Validate against targets
    validate_performance(
        name,
        write_latency,
        read_latency,
        concurrent_write_throughput,
        concurrent_read_throughput,
    );

    println!();
}

fn sequential_write_test(db: Arc<Database>) -> f64 {
    let start = Instant::now();

    for i in 0..10000 {
        let key = format!("key_{:016}", i);
        let value = vec![0u8; VALUE_SIZE];
        db.put(key.as_bytes(), &value).unwrap();
    }

    let duration = start.elapsed();
    duration.as_micros() as f64 / 10000.0
}

fn sequential_read_test(db: Arc<Database>) -> f64 {
    // Ensure data is present
    for i in 0..1000 {
        let key = format!("rkey_{:016}", i);
        let value = vec![1u8; VALUE_SIZE];
        db.put(key.as_bytes(), &value).unwrap();
    }

    let start = Instant::now();

    for i in 0..10000 {
        let key = format!("rkey_{:016}", i % 1000);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let duration = start.elapsed();
    duration.as_micros() as f64 / 10000.0
}

fn concurrent_write_test(db: Arc<Database>) -> f64 {
    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let tid = thread_id;
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("cw_{}_{:016}", tid, i);
                let value = vec![2u8; VALUE_SIZE];
                db_clone.put(key.as_bytes(), &value).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    (NUM_THREADS * 1000) as f64 / duration.as_secs_f64()
}

fn concurrent_read_test(db: Arc<Database>) -> f64 {
    // Ensure data is present
    for i in 0..1000 {
        let key = format!("crkey_{:016}", i);
        let value = vec![3u8; VALUE_SIZE];
        db.put(key.as_bytes(), &value).unwrap();
    }

    let start = Instant::now();
    let mut handles = vec![];

    for _ in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..10000 {
                let key = format!("crkey_{:016}", i % 1000);
                let _ = db_clone.get(key.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    (NUM_THREADS * 10000) as f64 / duration.as_secs_f64()
}

fn mixed_workload_test(db: Arc<Database>) -> (f64, f64) {
    use rand::Rng;

    let start = Instant::now();
    let mut handles = vec![];
    let total_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let read_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for _ in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let total_ops_clone = Arc::clone(&total_ops);
        let read_ops_clone = Arc::clone(&read_ops);

        let handle = thread::spawn(move || {
            let mut rng = rand::rng();
            for i in 0..1000 {
                if rng.random_bool(0.8) {
                    // 80% reads
                    let key = format!("mkey_{:016}", rng.random_range(0..1000));
                    let _ = db_clone.get(key.as_bytes());
                    read_ops_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    let key = format!("mkey_{:016}", i);
                    let value = vec![4u8; VALUE_SIZE];
                    db_clone.put(key.as_bytes(), &value).unwrap();
                }
                total_ops_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total = total_ops.load(std::sync::atomic::Ordering::Relaxed);
    let reads = read_ops.load(std::sync::atomic::Ordering::Relaxed);

    (
        total as f64 / duration.as_secs_f64(),
        (reads as f64 / total as f64) * 100.0,
    )
}

fn range_scan_test(db: Arc<Database>) -> f64 {
    // Insert ordered data
    for i in 0..10000 {
        let key = format!("scan_{:016}", i);
        let value = vec![5u8; VALUE_SIZE];
        db.put(key.as_bytes(), &value).unwrap();
    }

    let start = Instant::now();

    // Scan all entries that start with "scan_"
    let mut count = 0;
    for i in 0..10000 {
        let key = format!("scan_{:016}", i);
        if let Ok(Some(_)) = db.get(key.as_bytes()) {
            count += 1;
        }
    }

    let duration = start.elapsed();
    count as f64 / duration.as_secs_f64()
}

fn validate_performance(
    _config_name: &str,
    write_latency: f64,
    read_latency: f64,
    concurrent_write: f64,
    concurrent_read: f64,
) {
    println!("\nValidation Results:");

    // Target: 100K+ writes/sec = <10μs latency
    let write_target = 10.0;
    let write_pass = write_latency <= write_target;
    println!(
        "  Write latency: {} (target: <{:.0}μs)",
        if write_pass { "✓ PASS" } else { "✗ FAIL" },
        write_target
    );

    // Target: 1M+ reads/sec = <1μs latency
    let read_target = 1.0;
    let read_pass = read_latency <= read_target;
    println!(
        "  Read latency: {} (target: <{:.0}μs)",
        if read_pass { "✓ PASS" } else { "✗ FAIL" },
        read_target
    );

    // Target: 100K+ concurrent writes/sec
    let cwrite_target = 100_000.0;
    let cwrite_pass = concurrent_write >= cwrite_target;
    println!(
        "  Concurrent writes: {} (target: >{:.0} ops/sec)",
        if cwrite_pass { "✓ PASS" } else { "✗ FAIL" },
        cwrite_target
    );

    // Target: 1M+ concurrent reads/sec
    let cread_target = 1_000_000.0;
    let cread_pass = concurrent_read >= cread_target;
    println!(
        "  Concurrent reads: {} (target: >{:.0} ops/sec)",
        if cread_pass { "✓ PASS" } else { "✗ FAIL" },
        cread_target
    );

    let all_pass = write_pass && read_pass && cwrite_pass && cread_pass;
    println!(
        "  Overall: {}",
        if all_pass {
            "✓ ALL TESTS PASS"
        } else {
            "✗ SOME TESTS FAIL"
        }
    );
}
