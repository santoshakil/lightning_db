use lightning_db::{Config, Database, Error, Transaction, TransactionMode};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::{HashMap, HashSet};
use tempfile::tempdir;
use rand::{Rng, thread_rng, seq::SliceRandom};

#[derive(Debug, Clone)]
struct ConcurrentTestConfig {
    num_writers: usize,
    num_readers: usize,
    num_scanners: usize,
    num_deleters: usize,
    num_batch_writers: usize,
    operations_per_thread: usize,
    key_space_size: usize,
    value_size_range: (usize, usize),
    conflict_probability: f64,
    test_duration: Duration,
}

impl Default for ConcurrentTestConfig {
    fn default() -> Self {
        Self {
            num_writers: 20,
            num_readers: 30,
            num_scanners: 10,
            num_deleters: 10,
            num_batch_writers: 5,
            operations_per_thread: 1000,
            key_space_size: 10000,
            value_size_range: (100, 5000),
            conflict_probability: 0.3,
            test_duration: Duration::from_secs(60),
        }
    }
}

#[derive(Debug)]
struct ConcurrentTestStats {
    reads: AtomicU64,
    writes: AtomicU64,
    deletes: AtomicU64,
    scans: AtomicU64,
    batches: AtomicU64,
    conflicts: AtomicU64,
    retries: AtomicU64,
    errors: AtomicU64,
}

impl ConcurrentTestStats {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            scans: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
    
    fn print_summary(&self) {
        println!("\n=== Concurrent Test Statistics ===");
        println!("Reads: {}", self.reads.load(Ordering::Relaxed));
        println!("Writes: {}", self.writes.load(Ordering::Relaxed));
        println!("Deletes: {}", self.deletes.load(Ordering::Relaxed));
        println!("Scans: {}", self.scans.load(Ordering::Relaxed));
        println!("Batches: {}", self.batches.load(Ordering::Relaxed));
        println!("Conflicts: {}", self.conflicts.load(Ordering::Relaxed));
        println!("Retries: {}", self.retries.load(Ordering::Relaxed));
        println!("Errors: {}", self.errors.load(Ordering::Relaxed));
    }
}

fn generate_key(key_space: usize, conflict_prob: f64) -> Vec<u8> {
    let mut rng = thread_rng();
    let key_num = if rng.gen::<f64>() < conflict_prob {
        // Generate from a smaller hot set to increase conflicts
        rng.gen_range(0..key_space / 10)
    } else {
        rng.gen_range(0..key_space)
    };
    format!("key_{:010}", key_num).into_bytes()
}

fn generate_value(size_range: (usize, usize)) -> Vec<u8> {
    let mut rng = thread_rng();
    let size = rng.gen_range(size_range.0..size_range.1);
    (0..size).map(|_| rng.gen::<u8>()).collect()
}

fn writer_thread(
    db: Arc<Database>,
    config: ConcurrentTestConfig,
    stats: Arc<ConcurrentTestStats>,
    stop_flag: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) {
    barrier.wait();
    
    while !stop_flag.load(Ordering::Relaxed) {
        let key = generate_key(config.key_space_size, config.conflict_probability);
        let value = generate_value(config.value_size_range);
        
        let mut retries = 0;
        loop {
            match db.begin_transaction(TransactionMode::ReadWrite) {
                Ok(mut tx) => {
                    match tx.put(&key, &value) {
                        Ok(_) => {
                            match tx.commit() {
                                Ok(_) => {
                                    stats.writes.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Err(_) => {
                                    stats.conflicts.fetch_add(1, Ordering::Relaxed);
                                    retries += 1;
                                }
                            }
                        }
                        Err(_) => {
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                Err(_) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            
            if retries > 10 {
                stats.retries.fetch_add(retries, Ordering::Relaxed);
                break;
            }
        }
    }
}

fn reader_thread(
    db: Arc<Database>,
    config: ConcurrentTestConfig,
    stats: Arc<ConcurrentTestStats>,
    stop_flag: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) {
    barrier.wait();
    
    while !stop_flag.load(Ordering::Relaxed) {
        let key = generate_key(config.key_space_size, config.conflict_probability);
        
        match db.begin_transaction(TransactionMode::ReadOnly) {
            Ok(tx) => {
                match tx.get(&key) {
                    Ok(_) => {
                        stats.reads.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(_) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn scanner_thread(
    db: Arc<Database>,
    config: ConcurrentTestConfig,
    stats: Arc<ConcurrentTestStats>,
    stop_flag: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) {
    barrier.wait();
    let mut rng = thread_rng();
    
    while !stop_flag.load(Ordering::Relaxed) {
        let start_key = generate_key(config.key_space_size, 0.0);
        let end_key = generate_key(config.key_space_size, 0.0);
        
        let (start, end) = if start_key < end_key {
            (start_key, end_key)
        } else {
            (end_key, start_key)
        };
        
        match db.begin_transaction(TransactionMode::ReadOnly) {
            Ok(tx) => {
                let limit = rng.gen_range(10..100);
                let mut count = 0;
                let iter = tx.range(&start..&end);
                
                for result in iter.take(limit) {
                    match result {
                        Ok(_) => count += 1,
                        Err(_) => {
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                
                if count > 0 {
                    stats.scans.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn deleter_thread(
    db: Arc<Database>,
    config: ConcurrentTestConfig,
    stats: Arc<ConcurrentTestStats>,
    stop_flag: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) {
    barrier.wait();
    
    while !stop_flag.load(Ordering::Relaxed) {
        let key = generate_key(config.key_space_size, config.conflict_probability);
        
        let mut retries = 0;
        loop {
            match db.begin_transaction(TransactionMode::ReadWrite) {
                Ok(mut tx) => {
                    match tx.delete(&key) {
                        Ok(_) => {
                            match tx.commit() {
                                Ok(_) => {
                                    stats.deletes.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Err(_) => {
                                    stats.conflicts.fetch_add(1, Ordering::Relaxed);
                                    retries += 1;
                                }
                            }
                        }
                        Err(_) => {
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                Err(_) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            
            if retries > 10 {
                stats.retries.fetch_add(retries, Ordering::Relaxed);
                break;
            }
        }
    }
}

fn batch_writer_thread(
    db: Arc<Database>,
    config: ConcurrentTestConfig,
    stats: Arc<ConcurrentTestStats>,
    stop_flag: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) {
    barrier.wait();
    let mut rng = thread_rng();
    
    while !stop_flag.load(Ordering::Relaxed) {
        let batch_size = rng.gen_range(10..100);
        
        let mut retries = 0;
        loop {
            match db.begin_transaction(TransactionMode::ReadWrite) {
                Ok(mut tx) => {
                    let mut success = true;
                    for _ in 0..batch_size {
                        let key = generate_key(config.key_space_size, config.conflict_probability);
                        let value = generate_value(config.value_size_range);
                        
                        if tx.put(&key, &value).is_err() {
                            success = false;
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                    
                    if success {
                        match tx.commit() {
                            Ok(_) => {
                                stats.batches.fetch_add(1, Ordering::Relaxed);
                                stats.writes.fetch_add(batch_size, Ordering::Relaxed);
                                break;
                            }
                            Err(_) => {
                                stats.conflicts.fetch_add(1, Ordering::Relaxed);
                                retries += 1;
                            }
                        }
                    } else {
                        break;
                    }
                }
                Err(_) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            
            if retries > 10 {
                stats.retries.fetch_add(retries, Ordering::Relaxed);
                break;
            }
        }
    }
}

#[test]
fn test_exhaustive_concurrent_access() {
    let config = ConcurrentTestConfig::default();
    let dir = tempdir().unwrap();
    
    let db_config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 512 * 1024 * 1024, // 512MB
        max_concurrent_transactions: 200,
        enable_compression: true,
        compression_level: Some(3),
        fsync_mode: lightning_db::FsyncMode::Periodic(Duration::from_millis(100)),
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(db_config).unwrap());
    let stats = Arc::new(ConcurrentTestStats::new());
    let stop_flag = Arc::new(AtomicBool::new(false));
    
    let total_threads = config.num_writers + config.num_readers + 
                       config.num_scanners + config.num_deleters + 
                       config.num_batch_writers;
    let barrier = Arc::new(Barrier::new(total_threads));
    
    let mut handles = vec![];
    
    // Start writers
    for _ in 0..config.num_writers {
        let db_clone = Arc::clone(&db);
        let config_clone = config.clone();
        let stats_clone = Arc::clone(&stats);
        let stop_clone = Arc::clone(&stop_flag);
        let barrier_clone = Arc::clone(&barrier);
        
        handles.push(thread::spawn(move || {
            writer_thread(db_clone, config_clone, stats_clone, stop_clone, barrier_clone);
        }));
    }
    
    // Start readers
    for _ in 0..config.num_readers {
        let db_clone = Arc::clone(&db);
        let config_clone = config.clone();
        let stats_clone = Arc::clone(&stats);
        let stop_clone = Arc::clone(&stop_flag);
        let barrier_clone = Arc::clone(&barrier);
        
        handles.push(thread::spawn(move || {
            reader_thread(db_clone, config_clone, stats_clone, stop_clone, barrier_clone);
        }));
    }
    
    // Start scanners
    for _ in 0..config.num_scanners {
        let db_clone = Arc::clone(&db);
        let config_clone = config.clone();
        let stats_clone = Arc::clone(&stats);
        let stop_clone = Arc::clone(&stop_flag);
        let barrier_clone = Arc::clone(&barrier);
        
        handles.push(thread::spawn(move || {
            scanner_thread(db_clone, config_clone, stats_clone, stop_clone, barrier_clone);
        }));
    }
    
    // Start deleters
    for _ in 0..config.num_deleters {
        let db_clone = Arc::clone(&db);
        let config_clone = config.clone();
        let stats_clone = Arc::clone(&stats);
        let stop_clone = Arc::clone(&stop_flag);
        let barrier_clone = Arc::clone(&barrier);
        
        handles.push(thread::spawn(move || {
            deleter_thread(db_clone, config_clone, stats_clone, stop_clone, barrier_clone);
        }));
    }
    
    // Start batch writers
    for _ in 0..config.num_batch_writers {
        let db_clone = Arc::clone(&db);
        let config_clone = config.clone();
        let stats_clone = Arc::clone(&stats);
        let stop_clone = Arc::clone(&stop_flag);
        let barrier_clone = Arc::clone(&barrier);
        
        handles.push(thread::spawn(move || {
            batch_writer_thread(db_clone, config_clone, stats_clone, stop_clone, barrier_clone);
        }));
    }
    
    // Run test
    let start = Instant::now();
    thread::sleep(config.test_duration);
    stop_flag.store(true, Ordering::Relaxed);
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    println!("\nTest ran for {:.2} seconds", duration.as_secs_f64());
    stats.print_summary();
    
    // Verify integrity
    println!("\nVerifying database integrity...");
    match db.verify_integrity() {
        Ok(_) => println!("✓ Database integrity verified"),
        Err(e) => panic!("Database integrity check failed: {}", e),
    }
    
    // Check for excessive errors
    let error_count = stats.errors.load(Ordering::Relaxed);
    let total_ops = stats.reads.load(Ordering::Relaxed) + 
                   stats.writes.load(Ordering::Relaxed) + 
                   stats.deletes.load(Ordering::Relaxed) + 
                   stats.scans.load(Ordering::Relaxed);
    
    let error_rate = error_count as f64 / total_ops.max(1) as f64;
    assert!(error_rate < 0.01, "Error rate too high: {:.2}%", error_rate * 100.0);
}

#[test]
fn test_concurrent_isolation_levels() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 128 * 1024 * 1024,
        max_concurrent_transactions: 50,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::EveryTransaction,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(config).unwrap());
    
    // Test snapshot isolation
    let key = b"isolation_test_key";
    let initial_value = b"initial";
    
    // Set initial value
    let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
    tx.put(key, initial_value).unwrap();
    tx.commit().unwrap();
    
    // Start two concurrent transactions
    let tx1 = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
    let mut tx2 = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
    
    // tx1 should see initial value
    let value1 = tx1.get(key).unwrap().unwrap();
    assert_eq!(value1, initial_value);
    
    // tx2 modifies the value
    tx2.put(key, b"modified").unwrap();
    tx2.commit().unwrap();
    
    // tx1 should still see initial value (snapshot isolation)
    let value1_again = tx1.get(key).unwrap().unwrap();
    assert_eq!(value1_again, initial_value);
    
    // New transaction should see modified value
    let tx3 = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
    let value3 = tx3.get(key).unwrap().unwrap();
    assert_eq!(value3, b"modified");
}

#[test]
fn test_concurrent_deadlock_prevention() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 64 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::EveryTransaction,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(config).unwrap());
    let barrier = Arc::new(Barrier::new(2));
    let deadlock_detected = Arc::new(AtomicBool::new(false));
    
    // Create two keys
    let key1 = b"deadlock_key_1";
    let key2 = b"deadlock_key_2";
    
    // Initialize keys
    let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
    tx.put(key1, b"value1").unwrap();
    tx.put(key2, b"value2").unwrap();
    tx.commit().unwrap();
    
    // Thread 1: Acquires key1 then key2
    let db1 = Arc::clone(&db);
    let barrier1 = Arc::clone(&barrier);
    let deadlock1 = Arc::clone(&deadlock_detected);
    
    let handle1 = thread::spawn(move || {
        barrier1.wait();
        
        let mut tx = db1.begin_transaction(TransactionMode::ReadWrite).unwrap();
        tx.put(key1, b"thread1_value1").unwrap();
        
        thread::sleep(Duration::from_millis(100)); // Give thread 2 time to acquire key2
        
        match tx.put(key2, b"thread1_value2") {
            Ok(_) => {
                tx.commit().unwrap();
            }
            Err(_) => {
                deadlock1.store(true, Ordering::Relaxed);
            }
        }
    });
    
    // Thread 2: Acquires key2 then key1 (opposite order)
    let db2 = Arc::clone(&db);
    let barrier2 = Arc::clone(&barrier);
    let deadlock2 = Arc::clone(&deadlock_detected);
    
    let handle2 = thread::spawn(move || {
        barrier2.wait();
        
        let mut tx = db2.begin_transaction(TransactionMode::ReadWrite).unwrap();
        tx.put(key2, b"thread2_value2").unwrap();
        
        thread::sleep(Duration::from_millis(100)); // Give thread 1 time to acquire key1
        
        match tx.put(key1, b"thread2_value1") {
            Ok(_) => {
                tx.commit().unwrap();
            }
            Err(_) => {
                deadlock2.store(true, Ordering::Relaxed);
            }
        }
    });
    
    handle1.join().unwrap();
    handle2.join().unwrap();
    
    // The database should have prevented deadlock somehow
    // Either through timeout, deadlock detection, or lock ordering
    println!("Deadlock prevention test completed");
}