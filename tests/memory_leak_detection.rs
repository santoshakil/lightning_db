use lightning_db::{Config, Database, TransactionMode};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use rand::{Rng, thread_rng};

#[derive(Debug)]
struct MemoryStats {
    initial_memory: usize,
    peak_memory: AtomicU64,
    current_memory: AtomicU64,
    allocations: AtomicU64,
    deallocations: AtomicU64,
}

impl MemoryStats {
    fn new() -> Self {
        let initial = get_current_memory_usage();
        Self {
            initial_memory: initial,
            peak_memory: AtomicU64::new(initial as u64),
            current_memory: AtomicU64::new(initial as u64),
            allocations: AtomicU64::new(0),
            deallocations: AtomicU64::new(0),
        }
    }
    
    fn update(&self) {
        let current = get_current_memory_usage() as u64;
        self.current_memory.store(current, Ordering::Relaxed);
        
        let mut peak = self.peak_memory.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_memory.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => peak = x,
            }
        }
    }
    
    fn print_summary(&self) {
        let current = self.current_memory.load(Ordering::Relaxed) as usize;
        let peak = self.peak_memory.load(Ordering::Relaxed) as usize;
        
        println!("\n=== Memory Usage Summary ===");
        println!("Initial memory: {:.2} MB", self.initial_memory as f64 / 1_048_576.0);
        println!("Current memory: {:.2} MB", current as f64 / 1_048_576.0);
        println!("Peak memory: {:.2} MB", peak as f64 / 1_048_576.0);
        println!("Memory growth: {:.2} MB", (current as i64 - self.initial_memory as i64) as f64 / 1_048_576.0);
        println!("Allocations: {}", self.allocations.load(Ordering::Relaxed));
        println!("Deallocations: {}", self.deallocations.load(Ordering::Relaxed));
    }
    
    fn check_for_leak(&self, threshold_mb: f64) -> bool {
        let current = self.current_memory.load(Ordering::Relaxed) as usize;
        let growth = (current as i64 - self.initial_memory as i64) as f64 / 1_048_576.0;
        growth > threshold_mb
    }
}

fn get_current_memory_usage() -> usize {
    // Platform-specific memory usage detection
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<usize>() {
                            return kb * 1024;
                        }
                    }
                }
            }
        }
    }
    
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        if let Ok(output) = Command::new("ps")
            .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
        {
            if let Ok(s) = String::from_utf8(output.stdout) {
                if let Ok(kb) = s.trim().parse::<usize>() {
                    return kb * 1024;
                }
            }
        }
    }
    
    // Fallback: return 0 if we can't get memory info
    0
}

#[test]
fn test_transaction_memory_leak() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 64 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    let stats = MemoryStats::new();
    
    println!("Testing for transaction memory leaks...");
    
    // Warm up
    for _ in 0..100 {
        let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
        drop(tx);
    }
    
    stats.update();
    let baseline = stats.current_memory.load(Ordering::Relaxed);
    
    // Test repeated transaction creation/destruction
    for i in 0..10000 {
        let tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
        drop(tx);
        
        if i % 1000 == 0 {
            stats.update();
            let current = stats.current_memory.load(Ordering::Relaxed);
            let growth = (current as i64 - baseline as i64) as f64 / 1_048_576.0;
            println!("After {} transactions: memory growth = {:.2} MB", i, growth);
        }
    }
    
    // Force garbage collection if possible
    thread::sleep(Duration::from_millis(100));
    
    stats.update();
    stats.print_summary();
    
    assert!(!stats.check_for_leak(10.0), 
            "Memory leak detected: excessive growth in transaction test");
}

#[test]
fn test_large_value_memory_leak() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 128 * 1024 * 1024,
        max_concurrent_transactions: 5,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    let stats = MemoryStats::new();
    
    println!("Testing for large value memory leaks...");
    
    // Test with large values being inserted and deleted
    for cycle in 0..100 {
        // Insert phase
        let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
        for i in 0..10 {
            let key = format!("large_key_{:04}_{:04}", cycle, i);
            let value = vec![0u8; 1024 * 1024]; // 1MB values
            tx.put(key.as_bytes(), &value).unwrap();
            stats.allocations.fetch_add(1, Ordering::Relaxed);
        }
        tx.commit().unwrap();
        
        // Delete phase
        let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
        for i in 0..10 {
            let key = format!("large_key_{:04}_{:04}", cycle, i);
            tx.delete(key.as_bytes()).unwrap();
            stats.deallocations.fetch_add(1, Ordering::Relaxed);
        }
        tx.commit().unwrap();
        
        if cycle % 10 == 0 {
            stats.update();
            println!("Cycle {}: current memory = {:.2} MB", 
                     cycle, 
                     stats.current_memory.load(Ordering::Relaxed) as f64 / 1_048_576.0);
        }
    }
    
    stats.update();
    stats.print_summary();
    
    assert!(!stats.check_for_leak(50.0), 
            "Memory leak detected: excessive growth in large value test");
}

#[test]
fn test_concurrent_access_memory_leak() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 256 * 1024 * 1024,
        max_concurrent_transactions: 50,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(config).unwrap());
    let stats = Arc::new(MemoryStats::new());
    let stop_flag = Arc::new(AtomicBool::new(false));
    
    println!("Testing for concurrent access memory leaks...");
    
    // Start memory monitoring thread
    let stats_monitor = Arc::clone(&stats);
    let stop_monitor = Arc::clone(&stop_flag);
    let monitor_handle = thread::spawn(move || {
        while !stop_monitor.load(Ordering::Relaxed) {
            stats_monitor.update();
            thread::sleep(Duration::from_millis(100));
        }
    });
    
    // Start worker threads
    let mut handles = vec![];
    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let stop_clone = Arc::clone(&stop_flag);
        
        handles.push(thread::spawn(move || {
            let mut rng = thread_rng();
            
            while !stop_clone.load(Ordering::Relaxed) {
                let op = rng.gen_range(0..100);
                
                if op < 50 {
                    // Read operation
                    if let Ok(tx) = db_clone.begin_transaction(TransactionMode::ReadOnly) {
                        let key = format!("concurrent_key_{:06}", rng.gen_range(0..10000));
                        let _ = tx.get(key.as_bytes());
                    }
                } else if op < 80 {
                    // Write operation
                    if let Ok(mut tx) = db_clone.begin_transaction(TransactionMode::ReadWrite) {
                        let key = format!("concurrent_key_{:06}", rng.gen_range(0..10000));
                        let value = vec![rng.gen::<u8>(); rng.gen_range(100..10000)];
                        let _ = tx.put(key.as_bytes(), &value);
                        let _ = tx.commit();
                    }
                } else {
                    // Range scan
                    if let Ok(tx) = db_clone.begin_transaction(TransactionMode::ReadOnly) {
                        let start = format!("concurrent_key_{:06}", rng.gen_range(0..9000));
                        let end = format!("concurrent_key_{:06}", rng.gen_range(1000..10000));
                        
                        for result in tx.range(start.as_bytes()..end.as_bytes()).take(100) {
                            let _ = result;
                        }
                    }
                }
                
                if thread_id == 0 && rng.gen_bool(0.001) {
                    // Occasionally print status from one thread
                    println!("Thread 0: still running, memory = {:.2} MB",
                             get_current_memory_usage() as f64 / 1_048_576.0);
                }
            }
        }));
    }
    
    // Run for a fixed duration
    thread::sleep(Duration::from_secs(30));
    stop_flag.store(true, Ordering::Relaxed);
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    monitor_handle.join().unwrap();
    
    stats.update();
    stats.print_summary();
    
    assert!(!stats.check_for_leak(100.0), 
            "Memory leak detected: excessive growth in concurrent test");
}

#[test]
fn test_cache_eviction_memory_leak() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 8 * 1024 * 1024, // Small cache to force evictions
        max_concurrent_transactions: 5,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    let stats = MemoryStats::new();
    
    println!("Testing for cache eviction memory leaks...");
    
    // Write more data than cache can hold
    for batch in 0..100 {
        let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
        
        for i in 0..100 {
            let key = format!("eviction_test_{:06}_{:04}", batch * 100 + i, batch);
            let value = vec![0xAB; 4096]; // 4KB values
            tx.put(key.as_bytes(), &value).unwrap();
        }
        
        tx.commit().unwrap();
        
        // Read random keys to trigger cache evictions
        let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
        for _ in 0..50 {
            let key = format!("eviction_test_{:06}_{:04}", 
                            thread_rng().gen_range(0..batch * 100), 
                            thread_rng().gen_range(0..batch));
            let _ = tx.get(key.as_bytes());
        }
        
        if batch % 10 == 0 {
            stats.update();
            println!("Batch {}: memory = {:.2} MB", 
                     batch, 
                     stats.current_memory.load(Ordering::Relaxed) as f64 / 1_048_576.0);
        }
    }
    
    stats.update();
    stats.print_summary();
    
    assert!(!stats.check_for_leak(20.0), 
            "Memory leak detected: cache eviction not freeing memory properly");
}

#[test]
fn test_iterator_memory_leak() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 64 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    let stats = MemoryStats::new();
    
    println!("Testing for iterator memory leaks...");
    
    // Pre-populate database
    let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
    for i in 0..10000 {
        let key = format!("iterator_key_{:08}", i);
        let value = vec![i as u8; 1024];
        tx.put(key.as_bytes(), &value).unwrap();
        
        if i % 1000 == 999 {
            tx.commit().unwrap();
            tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
        }
    }
    tx.commit().unwrap();
    
    stats.update();
    let baseline = stats.current_memory.load(Ordering::Relaxed);
    
    // Test iterator creation and destruction
    for iteration in 0..1000 {
        let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
        
        // Create multiple iterators
        let iters: Vec<_> = (0..10).map(|i| {
            let start = format!("iterator_key_{:08}", i * 1000);
            let end = format!("iterator_key_{:08}", (i + 1) * 1000);
            tx.range(start.as_bytes()..end.as_bytes())
        }).collect();
        
        // Partially consume iterators
        for mut iter in iters {
            for _ in 0..10 {
                if iter.next().is_none() {
                    break;
                }
            }
            // Drop iterator with remaining items
        }
        
        if iteration % 100 == 0 {
            stats.update();
            let current = stats.current_memory.load(Ordering::Relaxed);
            let growth = (current as i64 - baseline as i64) as f64 / 1_048_576.0;
            println!("Iteration {}: memory growth = {:.2} MB", iteration, growth);
        }
    }
    
    stats.update();
    stats.print_summary();
    
    assert!(!stats.check_for_leak(15.0), 
            "Memory leak detected: iterators not properly cleaned up");
}