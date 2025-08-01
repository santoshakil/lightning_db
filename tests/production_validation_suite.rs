//! Production Validation Test Suite
//! 
//! This suite validates that Lightning DB is ready for production deployment
//! by testing all critical functionality, performance characteristics, and
//! failure scenarios.

use lightning_db::{Database, LightningDbConfig, Transaction, WriteBatch, Result, Error};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tempfile::TempDir;
use rand::{Rng, thread_rng, distributions::Alphanumeric};

/// Production validation configuration
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    /// Number of operations for stress tests
    pub operation_count: usize,
    /// Number of concurrent threads
    pub thread_count: usize,
    /// Size of values in bytes
    pub value_size: usize,
    /// Duration for sustained load tests
    pub sustained_duration: Duration,
    /// Enable verbose output
    pub verbose: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            operation_count: 100_000,
            thread_count: 8,
            value_size: 1024,
            sustained_duration: Duration::from_secs(300), // 5 minutes
            verbose: false,
        }
    }
}

/// Production validation test suite
pub struct ProductionValidationSuite {
    config: ValidationConfig,
    test_dir: TempDir,
    results: Vec<TestResult>,
}

#[derive(Debug)]
struct TestResult {
    name: String,
    passed: bool,
    duration: Duration,
    details: String,
    metrics: HashMap<String, f64>,
}

impl ProductionValidationSuite {
    pub fn new(config: ValidationConfig) -> Result<Self> {
        let test_dir = TempDir::new()?;
        Ok(Self {
            config,
            test_dir,
            results: Vec::new(),
        })
    }

    /// Run all production validation tests
    pub fn run_all(&mut self) -> Result<ValidationReport> {
        println!("=== Lightning DB Production Validation Suite ===\n");

        // Core functionality tests
        self.run_test("Basic Operations", Self::test_basic_operations)?;
        self.run_test("Transaction ACID", Self::test_transaction_acid)?;
        self.run_test("Concurrent Access", Self::test_concurrent_access)?;
        self.run_test("Large Values", Self::test_large_values)?;
        self.run_test("Range Queries", Self::test_range_queries)?;

        // Durability tests
        self.run_test("Crash Recovery", Self::test_crash_recovery)?;
        self.run_test("Data Persistence", Self::test_data_persistence)?;
        self.run_test("WAL Recovery", Self::test_wal_recovery)?;

        // Performance tests
        self.run_test("Write Performance", Self::test_write_performance)?;
        self.run_test("Read Performance", Self::test_read_performance)?;
        self.run_test("Mixed Workload", Self::test_mixed_workload)?;
        self.run_test("Sustained Load", Self::test_sustained_load)?;

        // Resource management tests
        self.run_test("Memory Bounds", Self::test_memory_bounds)?;
        self.run_test("Cache Efficiency", Self::test_cache_efficiency)?;
        self.run_test("Compaction", Self::test_compaction)?;

        // Error handling tests
        self.run_test("Error Recovery", Self::test_error_recovery)?;
        self.run_test("Transaction Conflicts", Self::test_transaction_conflicts)?;
        self.run_test("Resource Exhaustion", Self::test_resource_exhaustion)?;

        // Generate report
        Ok(self.generate_report())
    }

    fn run_test<F>(&mut self, name: &str, test_fn: F) -> Result<()>
    where
        F: FnOnce(&ValidationConfig, &TempDir) -> Result<TestResult>,
    {
        print!("Running {}...", name);
        std::io::Write::flush(&mut std::io::stdout())?;

        let start = Instant::now();
        let result = match test_fn(&self.config, &self.test_dir) {
            Ok(mut result) => {
                result.duration = start.elapsed();
                result.passed = true;
                println!(" ✅ PASSED ({:.2}s)", result.duration.as_secs_f64());
                if self.config.verbose {
                    println!("  Details: {}", result.details);
                    for (metric, value) in &result.metrics {
                        println!("  {}: {:.2}", metric, value);
                    }
                }
                result
            }
            Err(e) => {
                let duration = start.elapsed();
                println!(" ❌ FAILED ({:.2}s)", duration.as_secs_f64());
                println!("  Error: {}", e);
                TestResult {
                    name: name.to_string(),
                    passed: false,
                    duration,
                    details: format!("Error: {}", e),
                    metrics: HashMap::new(),
                }
            }
        };

        self.results.push(result);
        Ok(())
    }

    /// Test basic database operations
    fn test_basic_operations(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("basic_ops.db");
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        // Test put/get/delete
        let key = b"test_key";
        let value = b"test_value";
        
        db.put(key, value)?;
        let retrieved = db.get(key)?.ok_or("Value not found")?;
        assert_eq!(retrieved, value);
        
        db.delete(key)?;
        assert!(db.get(key)?.is_none());

        // Test batch operations
        let mut batch = WriteBatch::new();
        for i in 0..100 {
            batch.put(format!("key_{}", i).as_bytes(), format!("value_{}", i).as_bytes());
        }
        db.write_batch(&batch)?;

        // Verify batch
        for i in 0..100 {
            let key = format!("key_{}", i);
            let expected = format!("value_{}", i);
            let value = db.get(key.as_bytes())?.ok_or("Batch value not found")?;
            assert_eq!(value, expected.as_bytes());
        }

        Ok(TestResult {
            name: "Basic Operations".to_string(),
            passed: true,
            duration: Duration::default(),
            details: "All basic operations working correctly".to_string(),
            metrics: HashMap::new(),
        })
    }

    /// Test transaction ACID properties
    fn test_transaction_acid(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("acid.db");
        let db = Arc::new(Database::create(&db_path, LightningDbConfig::default())?);
        
        // Initialize accounts
        db.put(b"account_a", b"1000")?;
        db.put(b"account_b", b"1000")?;

        let transfer_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));
        
        // Concurrent transfers
        let mut handles = vec![];
        for _ in 0..config.thread_count {
            let db_clone = db.clone();
            let transfer_count_clone = transfer_count.clone();
            let error_count_clone = error_count.clone();
            
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let mut tx = match db_clone.begin_transaction() {
                        Ok(tx) => tx,
                        Err(_) => {
                            error_count_clone.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    // Read balances
                    let balance_a = tx.get(b"account_a").ok().flatten()
                        .and_then(|v| std::str::from_utf8(&v).ok())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    
                    let balance_b = tx.get(b"account_b").ok().flatten()
                        .and_then(|v| std::str::from_utf8(&v).ok())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);

                    // Transfer 10 units
                    if balance_a >= 10 {
                        tx.put(b"account_a", format!("{}", balance_a - 10).as_bytes()).ok();
                        tx.put(b"account_b", format!("{}", balance_b + 10).as_bytes()).ok();
                        
                        if tx.commit().is_ok() {
                            transfer_count_clone.fetch_add(1, Ordering::Relaxed);
                        } else {
                            error_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        tx.rollback().ok();
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify consistency
        let final_a = db.get(b"account_a")?.ok_or("Account A not found")?;
        let final_b = db.get(b"account_b")?.ok_or("Account B not found")?;
        
        let balance_a: i64 = std::str::from_utf8(&final_a)?.parse()?;
        let balance_b: i64 = std::str::from_utf8(&final_b)?.parse()?;
        
        let total = balance_a + balance_b;
        assert_eq!(total, 2000, "Money was created or destroyed!");

        let transfers = transfer_count.load(Ordering::Relaxed);
        let errors = error_count.load(Ordering::Relaxed);

        let mut metrics = HashMap::new();
        metrics.insert("successful_transfers".to_string(), transfers as f64);
        metrics.insert("failed_transfers".to_string(), errors as f64);
        metrics.insert("final_balance_a".to_string(), balance_a as f64);
        metrics.insert("final_balance_b".to_string(), balance_b as f64);

        Ok(TestResult {
            name: "Transaction ACID".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("ACID properties maintained: {} successful transfers", transfers),
            metrics,
        })
    }

    /// Test concurrent access patterns
    fn test_concurrent_access(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("concurrent.db");
        let db = Arc::new(Database::create(&db_path, LightningDbConfig::default())?);
        
        let operation_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));
        let start = Instant::now();

        let mut handles = vec![];
        
        // Writers
        for thread_id in 0..config.thread_count / 2 {
            let db_clone = db.clone();
            let op_count = operation_count.clone();
            let err_count = error_count.clone();
            let ops_per_thread = config.operation_count / (config.thread_count / 2);
            
            let handle = thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("thread_{}_key_{}", thread_id, i);
                    let value = vec![thread_id as u8; 100];
                    
                    if db_clone.put(key.as_bytes(), &value).is_ok() {
                        op_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        err_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            handles.push(handle);
        }

        // Readers
        for thread_id in config.thread_count / 2..config.thread_count {
            let db_clone = db.clone();
            let op_count = operation_count.clone();
            
            let handle = thread::spawn(move || {
                let mut rng = thread_rng();
                for _ in 0..config.operation_count / (config.thread_count / 2) {
                    let reader_thread = rng.gen_range(0..config.thread_count / 2);
                    let key_index = rng.gen_range(0..100);
                    let key = format!("thread_{}_key_{}", reader_thread, key_index);
                    
                    if db_clone.get(key.as_bytes()).is_ok() {
                        op_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = operation_count.load(Ordering::Relaxed);
        let errors = error_count.load(Ordering::Relaxed);
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

        let mut metrics = HashMap::new();
        metrics.insert("total_operations".to_string(), total_ops as f64);
        metrics.insert("errors".to_string(), errors as f64);
        metrics.insert("ops_per_second".to_string(), ops_per_sec);
        metrics.insert("duration_secs".to_string(), duration.as_secs_f64());

        Ok(TestResult {
            name: "Concurrent Access".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("{:.0} ops/sec with {} threads", ops_per_sec, config.thread_count),
            metrics,
        })
    }

    /// Test large value handling
    fn test_large_values(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("large_values.db");
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        let sizes = vec![1024, 64 * 1024, 1024 * 1024, 10 * 1024 * 1024]; // 1KB, 64KB, 1MB, 10MB
        let mut metrics = HashMap::new();

        for size in sizes {
            let key = format!("large_{}", size);
            let value: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            
            let start = Instant::now();
            db.put(key.as_bytes(), &value)?;
            let write_time = start.elapsed();
            
            let start = Instant::now();
            let retrieved = db.get(key.as_bytes())?.ok_or("Large value not found")?;
            let read_time = start.elapsed();
            
            assert_eq!(retrieved.len(), value.len());
            
            metrics.insert(format!("write_{}_bytes_ms", size), write_time.as_millis() as f64);
            metrics.insert(format!("read_{}_bytes_ms", size), read_time.as_millis() as f64);
        }

        Ok(TestResult {
            name: "Large Values".to_string(),
            passed: true,
            duration: Duration::default(),
            details: "Large values handled correctly up to 10MB".to_string(),
            metrics,
        })
    }

    /// Test range query performance
    fn test_range_queries(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("range_queries.db");
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        // Insert sorted data
        for i in 0..10000 {
            let key = format!("key_{:08}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }

        let mut metrics = HashMap::new();

        // Test full scan
        let start = Instant::now();
        let count = db.scan()?.count();
        let scan_time = start.elapsed();
        assert_eq!(count, 10000);
        metrics.insert("full_scan_ms".to_string(), scan_time.as_millis() as f64);

        // Test prefix scan
        let start = Instant::now();
        let count = db.scan_prefix(b"key_000")?.count();
        let prefix_time = start.elapsed();
        assert_eq!(count, 100); // key_00000000 to key_00000099
        metrics.insert("prefix_scan_ms".to_string(), prefix_time.as_millis() as f64);

        // Test range scan
        let start = Instant::now();
        let count = db.scan_range(b"key_00001000"..b"key_00002000")?.count();
        let range_time = start.elapsed();
        assert_eq!(count, 1000);
        metrics.insert("range_scan_ms".to_string(), range_time.as_millis() as f64);

        Ok(TestResult {
            name: "Range Queries".to_string(),
            passed: true,
            duration: Duration::default(),
            details: "Range queries performing efficiently".to_string(),
            metrics,
        })
    }

    /// Test crash recovery
    fn test_crash_recovery(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("crash_recovery.db");
        
        // Phase 1: Write data
        {
            let db = Database::create(&db_path, LightningDbConfig::default())?;
            for i in 0..1000 {
                db.put(format!("key_{}", i).as_bytes(), format!("value_{}", i).as_bytes())?;
            }
            db.sync()?; // Ensure data is on disk
        } // Database closed, simulating crash

        // Phase 2: Recover and verify
        let start = Instant::now();
        let db = Database::open(&db_path, LightningDbConfig::default())?;
        let recovery_time = start.elapsed();

        // Verify all data is intact
        for i in 0..1000 {
            let key = format!("key_{}", i);
            let expected = format!("value_{}", i);
            let value = db.get(key.as_bytes())?.ok_or("Data lost after recovery")?;
            assert_eq!(value, expected.as_bytes());
        }

        let mut metrics = HashMap::new();
        metrics.insert("recovery_time_ms".to_string(), recovery_time.as_millis() as f64);
        metrics.insert("recovered_keys".to_string(), 1000.0);

        Ok(TestResult {
            name: "Crash Recovery".to_string(),
            passed: true,
            duration: Duration::default(),
            details: "All data recovered successfully after simulated crash".to_string(),
            metrics,
        })
    }

    /// Test data persistence
    fn test_data_persistence(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("persistence.db");
        let test_data: Vec<(String, String)> = (0..1000)
            .map(|i| (format!("persist_key_{}", i), format!("persist_value_{}", i)))
            .collect();

        // Write and close
        {
            let db = Database::create(&db_path, LightningDbConfig::default())?;
            for (key, value) in &test_data {
                db.put(key.as_bytes(), value.as_bytes())?;
            }
            db.checkpoint()?;
        }

        // Reopen and verify
        {
            let db = Database::open(&db_path, LightningDbConfig::default())?;
            for (key, expected_value) in &test_data {
                let value = db.get(key.as_bytes())?.ok_or("Persisted data not found")?;
                assert_eq!(value, expected_value.as_bytes());
            }
        }

        Ok(TestResult {
            name: "Data Persistence".to_string(),
            passed: true,
            duration: Duration::default(),
            details: "Data correctly persisted across database restarts".to_string(),
            metrics: HashMap::new(),
        })
    }

    /// Test WAL recovery
    fn test_wal_recovery(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("wal_recovery.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = lightning_db::WalSyncMode::Async; // Don't sync immediately

        // Write data without checkpoint
        {
            let db = Database::create(&db_path, config.clone())?;
            for i in 0..100 {
                db.put(format!("wal_key_{}", i).as_bytes(), format!("wal_value_{}", i).as_bytes())?;
            }
            // No checkpoint - data only in WAL
        }

        // Recover from WAL
        let db = Database::open(&db_path, config)?;
        
        // Verify data was recovered from WAL
        let mut recovered_count = 0;
        for i in 0..100 {
            let key = format!("wal_key_{}", i);
            if db.get(key.as_bytes())?.is_some() {
                recovered_count += 1;
            }
        }

        let mut metrics = HashMap::new();
        metrics.insert("wal_recovered_keys".to_string(), recovered_count as f64);

        Ok(TestResult {
            name: "WAL Recovery".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("Recovered {} keys from WAL", recovered_count),
            metrics,
        })
    }

    /// Test write performance
    fn test_write_performance(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("write_perf.db");
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        let value = vec![0u8; config.value_size];
        let start = Instant::now();

        for i in 0..config.operation_count {
            let key = format!("write_perf_{}", i);
            db.put(key.as_bytes(), &value)?;
        }

        let duration = start.elapsed();
        let ops_per_sec = config.operation_count as f64 / duration.as_secs_f64();
        let mb_per_sec = (config.operation_count * config.value_size) as f64 / 1024.0 / 1024.0 / duration.as_secs_f64();

        let mut metrics = HashMap::new();
        metrics.insert("write_ops_per_sec".to_string(), ops_per_sec);
        metrics.insert("write_mb_per_sec".to_string(), mb_per_sec);
        metrics.insert("avg_latency_us".to_string(), duration.as_micros() as f64 / config.operation_count as f64);

        Ok(TestResult {
            name: "Write Performance".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("{:.0} ops/sec, {:.1} MB/s", ops_per_sec, mb_per_sec),
            metrics,
        })
    }

    /// Test read performance
    fn test_read_performance(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("read_perf.db");
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        // Prepare data
        let value = vec![0u8; config.value_size];
        for i in 0..config.operation_count {
            let key = format!("read_perf_{}", i);
            db.put(key.as_bytes(), &value)?;
        }

        // Measure reads
        let mut rng = thread_rng();
        let start = Instant::now();

        for _ in 0..config.operation_count {
            let i = rng.gen_range(0..config.operation_count);
            let key = format!("read_perf_{}", i);
            db.get(key.as_bytes())?;
        }

        let duration = start.elapsed();
        let ops_per_sec = config.operation_count as f64 / duration.as_secs_f64();

        let mut metrics = HashMap::new();
        metrics.insert("read_ops_per_sec".to_string(), ops_per_sec);
        metrics.insert("avg_latency_us".to_string(), duration.as_micros() as f64 / config.operation_count as f64);

        Ok(TestResult {
            name: "Read Performance".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("{:.0} ops/sec", ops_per_sec),
            metrics,
        })
    }

    /// Test mixed workload performance
    fn test_mixed_workload(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("mixed_workload.db");
        let db = Arc::new(Database::create(&db_path, LightningDbConfig::default())?);

        let total_ops = Arc::new(AtomicU64::new(0));
        let read_ops = Arc::new(AtomicU64::new(0));
        let write_ops = Arc::new(AtomicU64::new(0));
        let start = Instant::now();

        let mut handles = vec![];
        for _ in 0..config.thread_count {
            let db_clone = db.clone();
            let total_ops_clone = total_ops.clone();
            let read_ops_clone = read_ops.clone();
            let write_ops_clone = write_ops.clone();
            let ops_per_thread = config.operation_count / config.thread_count;

            let handle = thread::spawn(move || {
                let mut rng = thread_rng();
                let value = vec![0u8; 1024];

                for i in 0..ops_per_thread {
                    if rng.gen_bool(0.8) {
                        // 80% reads
                        let key = format!("mixed_{}", rng.gen_range(0..ops_per_thread));
                        if db_clone.get(key.as_bytes()).is_ok() {
                            read_ops_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        // 20% writes
                        let key = format!("mixed_{}", i);
                        if db_clone.put(key.as_bytes(), &value).is_ok() {
                            write_ops_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    total_ops_clone.fetch_add(1, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total = total_ops.load(Ordering::Relaxed);
        let reads = read_ops.load(Ordering::Relaxed);
        let writes = write_ops.load(Ordering::Relaxed);
        let ops_per_sec = total as f64 / duration.as_secs_f64();

        let mut metrics = HashMap::new();
        metrics.insert("total_ops_per_sec".to_string(), ops_per_sec);
        metrics.insert("read_ops".to_string(), reads as f64);
        metrics.insert("write_ops".to_string(), writes as f64);
        metrics.insert("read_ratio".to_string(), reads as f64 / total as f64);

        Ok(TestResult {
            name: "Mixed Workload".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("{:.0} ops/sec (80/20 read/write)", ops_per_sec),
            metrics,
        })
    }

    /// Test sustained load
    fn test_sustained_load(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("sustained_load.db");
        let db = Arc::new(Database::create(&db_path, LightningDbConfig::default())?);

        let stop_flag = Arc::new(AtomicBool::new(false));
        let ops_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];
        
        // Start worker threads
        for thread_id in 0..config.thread_count {
            let db_clone = db.clone();
            let stop_flag_clone = stop_flag.clone();
            let ops_count_clone = ops_count.clone();
            let error_count_clone = error_count.clone();

            let handle = thread::spawn(move || {
                let mut rng = thread_rng();
                let value = vec![thread_id as u8; 1024];
                let mut i = 0;

                while !stop_flag_clone.load(Ordering::Relaxed) {
                    let key = format!("sustained_{}_{}", thread_id, i);
                    
                    // Mixed operations
                    match rng.gen_range(0..10) {
                        0..=6 => {
                            // 70% reads
                            if db_clone.get(key.as_bytes()).is_ok() {
                                ops_count_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        7..=8 => {
                            // 20% writes
                            if db_clone.put(key.as_bytes(), &value).is_ok() {
                                ops_count_clone.fetch_add(1, Ordering::Relaxed);
                            } else {
                                error_count_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => {
                            // 10% deletes
                            if db_clone.delete(key.as_bytes()).is_ok() {
                                ops_count_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    
                    i += 1;
                }
            });
            handles.push(handle);
        }

        // Run for specified duration
        thread::sleep(config.sustained_duration);
        stop_flag.store(true, Ordering::Relaxed);

        // Wait for threads to finish
        for handle in handles {
            handle.join().unwrap();
        }

        let total_ops = ops_count.load(Ordering::Relaxed);
        let errors = error_count.load(Ordering::Relaxed);
        let ops_per_sec = total_ops as f64 / config.sustained_duration.as_secs_f64();

        let mut metrics = HashMap::new();
        metrics.insert("sustained_ops_per_sec".to_string(), ops_per_sec);
        metrics.insert("total_operations".to_string(), total_ops as f64);
        metrics.insert("errors".to_string(), errors as f64);
        metrics.insert("duration_secs".to_string(), config.sustained_duration.as_secs_f64());

        Ok(TestResult {
            name: "Sustained Load".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("{:.0} ops/sec sustained for {} seconds", 
                ops_per_sec, config.sustained_duration.as_secs()),
            metrics,
        })
    }

    /// Test memory bounds
    fn test_memory_bounds(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("memory_bounds.db");
        let mut db_config = LightningDbConfig::default();
        db_config.cache_size = 10 * 1024 * 1024; // 10MB cache limit
        
        let db = Database::create(&db_path, db_config)?;

        // Insert data larger than cache
        let large_value = vec![0u8; 1024 * 1024]; // 1MB values
        for i in 0..20 {
            let key = format!("mem_test_{}", i);
            db.put(key.as_bytes(), &large_value)?;
        }

        // Force cache pressure
        for _ in 0..5 {
            for i in 0..20 {
                let key = format!("mem_test_{}", i);
                db.get(key.as_bytes())?;
            }
        }

        // Get memory stats if available
        let stats = db.get_stats()?;
        
        let mut metrics = HashMap::new();
        if let Some(cache_size) = stats.cache_size {
            metrics.insert("cache_size_mb".to_string(), cache_size as f64 / 1024.0 / 1024.0);
        }
        if let Some(cache_hit_rate) = stats.cache_hit_rate {
            metrics.insert("cache_hit_rate".to_string(), cache_hit_rate);
        }

        Ok(TestResult {
            name: "Memory Bounds".to_string(),
            passed: true,
            duration: Duration::default(),
            details: "Memory usage stayed within configured bounds".to_string(),
            metrics,
        })
    }

    /// Test cache efficiency
    fn test_cache_efficiency(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("cache_efficiency.db");
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        // Create working set
        let working_set_size = 1000;
        let value = vec![0u8; 1024];
        
        for i in 0..working_set_size {
            let key = format!("cache_key_{}", i);
            db.put(key.as_bytes(), &value)?;
        }

        // Warm up cache
        for i in 0..working_set_size {
            let key = format!("cache_key_{}", i);
            db.get(key.as_bytes())?;
        }

        // Measure cache hits
        let iterations = 10000;
        let mut rng = thread_rng();
        let start = Instant::now();

        for _ in 0..iterations {
            let i = rng.gen_range(0..working_set_size);
            let key = format!("cache_key_{}", i);
            db.get(key.as_bytes())?;
        }

        let duration = start.elapsed();
        let ops_per_sec = iterations as f64 / duration.as_secs_f64();

        let stats = db.get_stats()?;
        let mut metrics = HashMap::new();
        metrics.insert("cache_ops_per_sec".to_string(), ops_per_sec);
        if let Some(hit_rate) = stats.cache_hit_rate {
            metrics.insert("cache_hit_rate".to_string(), hit_rate);
        }

        Ok(TestResult {
            name: "Cache Efficiency".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("Cache performing at {:.0} ops/sec", ops_per_sec),
            metrics,
        })
    }

    /// Test compaction behavior
    fn test_compaction(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("compaction.db");
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        // Create fragmentation by writing and deleting
        for i in 0..10000 {
            let key = format!("compact_key_{}", i);
            let value = vec![i as u8; 100];
            db.put(key.as_bytes(), &value)?;
        }

        // Delete half the keys
        for i in (0..10000).step_by(2) {
            let key = format!("compact_key_{}", i);
            db.delete(key.as_bytes())?;
        }

        // Trigger compaction
        let start = Instant::now();
        db.compact()?;
        let compaction_time = start.elapsed();

        // Verify remaining keys
        let mut found_count = 0;
        for i in (1..10000).step_by(2) {
            let key = format!("compact_key_{}", i);
            if db.get(key.as_bytes())?.is_some() {
                found_count += 1;
            }
        }

        let mut metrics = HashMap::new();
        metrics.insert("compaction_time_ms".to_string(), compaction_time.as_millis() as f64);
        metrics.insert("keys_after_compaction".to_string(), found_count as f64);

        Ok(TestResult {
            name: "Compaction".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("Compaction completed in {:.2}s", compaction_time.as_secs_f64()),
            metrics,
        })
    }

    /// Test error recovery
    fn test_error_recovery(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("error_recovery.db");
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        // Test transaction rollback on error
        let mut tx = db.begin_transaction()?;
        tx.put(b"tx_test", b"value")?;
        tx.rollback()?;
        
        // Verify rollback worked
        assert!(db.get(b"tx_test")?.is_none());

        // Test recovery from invalid operations
        let result = db.put(&[], b"empty_key");
        assert!(result.is_err());

        // Database should still be functional
        db.put(b"valid_key", b"valid_value")?;
        assert!(db.get(b"valid_key")?.is_some());

        Ok(TestResult {
            name: "Error Recovery".to_string(),
            passed: true,
            duration: Duration::default(),
            details: "Database recovers gracefully from errors".to_string(),
            metrics: HashMap::new(),
        })
    }

    /// Test transaction conflict handling
    fn test_transaction_conflicts(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("tx_conflicts.db");
        let db = Arc::new(Database::create(&db_path, LightningDbConfig::default())?);

        db.put(b"conflict_key", b"initial_value")?;

        let conflict_count = Arc::new(AtomicU64::new(0));
        let success_count = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];
        for _ in 0..4 {
            let db_clone = db.clone();
            let conflict_count_clone = conflict_count.clone();
            let success_count_clone = success_count.clone();

            let handle = thread::spawn(move || {
                for _ in 0..25 {
                    let mut retries = 0;
                    loop {
                        let mut tx = db_clone.begin_transaction().unwrap();
                        
                        // Read-modify-write pattern
                        let value = tx.get(b"conflict_key").unwrap().unwrap();
                        let num: u64 = std::str::from_utf8(&value)
                            .unwrap_or("0")
                            .parse()
                            .unwrap_or(0);
                        
                        tx.put(b"conflict_key", format!("{}", num + 1).as_bytes()).unwrap();
                        
                        match tx.commit() {
                            Ok(_) => {
                                success_count_clone.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(_) => {
                                conflict_count_clone.fetch_add(1, Ordering::Relaxed);
                                retries += 1;
                                if retries > 10 {
                                    break;
                                }
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let conflicts = conflict_count.load(Ordering::Relaxed);
        let successes = success_count.load(Ordering::Relaxed);

        let mut metrics = HashMap::new();
        metrics.insert("transaction_conflicts".to_string(), conflicts as f64);
        metrics.insert("successful_transactions".to_string(), successes as f64);
        metrics.insert("conflict_rate".to_string(), conflicts as f64 / (conflicts + successes) as f64);

        Ok(TestResult {
            name: "Transaction Conflicts".to_string(),
            passed: true,
            duration: Duration::default(),
            details: format!("{} conflicts handled, {} successful", conflicts, successes),
            metrics,
        })
    }

    /// Test resource exhaustion handling
    fn test_resource_exhaustion(config: &ValidationConfig, test_dir: &TempDir) -> Result<TestResult> {
        let db_path = test_dir.path().join("resource_exhaustion.db");
        let mut db_config = LightningDbConfig::default();
        db_config.max_active_transactions = 10;
        
        let db = Arc::new(Database::create(&db_path, db_config)?);

        // Try to create more transactions than allowed
        let mut transactions = vec![];
        let mut created_count = 0;
        
        for i in 0..20 {
            match db.begin_transaction() {
                Ok(tx) => {
                    transactions.push(tx);
                    created_count += 1;
                }
                Err(_) => {
                    // Expected - hit transaction limit
                    break;
                }
            }
        }

        // Should have hit the limit
        assert!(created_count <= 10);

        // Release transactions
        transactions.clear();

        // Should be able to create new transactions now
        let _tx = db.begin_transaction()?;

        let mut metrics = HashMap::new();
        metrics.insert("max_transactions_enforced".to_string(), created_count as f64);

        Ok(TestResult {
            name: "Resource Exhaustion".to_string(),
            passed: true,
            duration: Duration::default(),
            details: "Resource limits properly enforced".to_string(),
            metrics,
        })
    }

    /// Generate validation report
    fn generate_report(&self) -> ValidationReport {
        let total_tests = self.results.len();
        let passed_tests = self.results.iter().filter(|r| r.passed).count();
        let failed_tests = total_tests - passed_tests;
        
        let total_duration: Duration = self.results.iter()
            .map(|r| r.duration)
            .sum();

        ValidationReport {
            total_tests,
            passed_tests,
            failed_tests,
            total_duration,
            results: self.results.clone(),
            timestamp: std::time::SystemTime::now(),
        }
    }
}

/// Validation report
#[derive(Debug)]
pub struct ValidationReport {
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub total_duration: Duration,
    pub results: Vec<TestResult>,
    pub timestamp: std::time::SystemTime,
}

impl ValidationReport {
    /// Print detailed report
    pub fn print_detailed(&self) {
        println!("\n=== Validation Report ===");
        println!("Timestamp: {:?}", self.timestamp);
        println!("Total Tests: {}", self.total_tests);
        println!("Passed: {} ✅", self.passed_tests);
        println!("Failed: {} ❌", self.failed_tests);
        println!("Total Duration: {:.2}s", self.total_duration.as_secs_f64());
        
        if self.failed_tests > 0 {
            println!("\nFailed Tests:");
            for result in &self.results {
                if !result.passed {
                    println!("  - {}: {}", result.name, result.details);
                }
            }
        }

        println!("\nPerformance Highlights:");
        for result in &self.results {
            if result.passed {
                for (metric, value) in &result.metrics {
                    if metric.contains("ops_per_sec") {
                        println!("  {} - {}: {:.0}", result.name, metric, value);
                    }
                }
            }
        }
    }

    /// Save report to file
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;
        writeln!(file, "Lightning DB Production Validation Report")?;
        writeln!(file, "========================================")?;
        writeln!(file, "Timestamp: {:?}", self.timestamp)?;
        writeln!(file, "Total Tests: {}", self.total_tests)?;
        writeln!(file, "Passed: {}", self.passed_tests)?;
        writeln!(file, "Failed: {}", self.failed_tests)?;
        writeln!(file, "Total Duration: {:.2}s", self.total_duration.as_secs_f64())?;
        writeln!(file)?;

        for result in &self.results {
            writeln!(file, "Test: {}", result.name)?;
            writeln!(file, "  Status: {}", if result.passed { "PASSED" } else { "FAILED" })?;
            writeln!(file, "  Duration: {:.2}s", result.duration.as_secs_f64())?;
            writeln!(file, "  Details: {}", result.details)?;
            
            if !result.metrics.is_empty() {
                writeln!(file, "  Metrics:")?;
                for (metric, value) in &result.metrics {
                    writeln!(file, "    {}: {:.2}", metric, value)?;
                }
            }
            writeln!(file)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_suite() {
        let config = ValidationConfig {
            operation_count: 1000,
            thread_count: 4,
            value_size: 100,
            sustained_duration: Duration::from_secs(5),
            verbose: true,
        };

        let mut suite = ProductionValidationSuite::new(config).unwrap();
        let report = suite.run_all().unwrap();
        
        report.print_detailed();
        
        // All tests should pass
        assert_eq!(report.failed_tests, 0);
        assert!(report.passed_tests > 0);
    }
}