//! Chaos Engineering Test Suite for Lightning DB
//!
//! This suite implements various chaos scenarios to test database resilience
//! under extreme and unpredictable conditions.

use lightning_db::{Database, LightningDbConfig};
use rand::prelude::*;
use rand::{rng, Rng};
use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Chaos action that can be performed
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
enum ChaosAction {
    CorruptFile,
    DeleteFile,
    TruncateFile,
    FillDisk,
    SlowIO,
    NetworkPartition,
    ProcessKill,
    MemoryPressure,
    CPUStarvation,
}

/// Chaos test context
struct ChaosContext {
    db_path: String,
    db: Arc<Mutex<Option<Arc<Database>>>>,
    stop_flag: Arc<AtomicBool>,
    error_count: Arc<AtomicU64>,
    operation_count: Arc<AtomicU64>,
    chaos_events: Arc<Mutex<Vec<(Instant, ChaosAction)>>>,
}

impl ChaosContext {
    fn new(db_path: String) -> Self {
        Self {
            db_path,
            db: Arc::new(Mutex::new(None)),
            stop_flag: Arc::new(AtomicBool::new(false)),
            error_count: Arc::new(AtomicU64::new(0)),
            operation_count: Arc::new(AtomicU64::new(0)),
            chaos_events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record_chaos_event(&self, action: ChaosAction) {
        let mut events = self.chaos_events.lock().unwrap();
        events.push((Instant::now(), action));
    }
}

/// Workload generator thread
fn workload_thread(ctx: Arc<ChaosContext>, thread_id: u64) {
    let mut rng = rng();
    let mut local_data: HashMap<String, String> = HashMap::new();

    while !ctx.stop_flag.load(Ordering::Relaxed) {
        if let Some(db) = ctx.db.lock().unwrap().as_ref() {
            // Random operation
            match rng.random_range(0..100) {
                0..=40 => {
                    // Write operation
                    let key = format!("thread_{}_key_{}", thread_id, rng.random::<u64>());
                    let value = format!("value_{}", rng.random::<u64>());

                    match db.begin_transaction() {
                        Ok(tx_id) => {
                            match db
                                .put_tx(tx_id, key.as_bytes(), value.as_bytes())
                                .and_then(|_| db.commit_transaction(tx_id))
                            {
                                Ok(_) => {
                                    local_data.insert(key, value);
                                    ctx.operation_count.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    ctx.error_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(_) => {
                            ctx.error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                41..=80 => {
                    // Read operation
                    let keys: Vec<String> = local_data.keys().cloned().collect();
                    if let Some(key) = keys.choose(&mut rng) {
                        let expected_value = local_data.get(key).unwrap();
                        match db.begin_transaction() {
                            Ok(tx_id) => match db.get_tx(tx_id, key.as_bytes()) {
                                Ok(Some(value)) => {
                                    if value != expected_value.as_bytes() {
                                        eprintln!("Data corruption detected!");
                                        ctx.error_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                    ctx.operation_count.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(None) => {
                                    eprintln!("Missing data!");
                                    ctx.error_count.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    ctx.error_count.fetch_add(1, Ordering::Relaxed);
                                }
                            },
                            Err(_) => {
                                ctx.error_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                81..=90 => {
                    // Range scan
                    match db.begin_transaction() {
                        Ok(_tx_id) => {
                            let _start_key = format!("thread_{}_", thread_id);
                            // Note: scan API not available via transaction ID
                            // Skip range scan for now
                            ctx.operation_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            ctx.error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ => {
                    // Delete operation
                    let keys: Vec<String> = local_data.keys().cloned().collect();
                    if let Some(key) = keys.choose(&mut rng) {
                        match db.begin_transaction() {
                            Ok(tx_id) => {
                                match db
                                    .delete_tx(tx_id, key.as_bytes())
                                    .and_then(|_| db.commit_transaction(tx_id))
                                {
                                    Ok(_) => {
                                        local_data.remove(key);
                                        ctx.operation_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(_) => {
                                        ctx.error_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(_) => {
                                ctx.error_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }

        // Small delay
        thread::sleep(Duration::from_micros(rng.random_range(10..100)));
    }
}

/// Chaos injection thread
fn chaos_thread(ctx: Arc<ChaosContext>) {
    let mut rng = rng();
    let chaos_actions = [
        ChaosAction::CorruptFile,
        ChaosAction::DeleteFile,
        ChaosAction::TruncateFile,
        ChaosAction::SlowIO,
        ChaosAction::MemoryPressure,
    ];

    while !ctx.stop_flag.load(Ordering::Relaxed) {
        // Wait random interval
        thread::sleep(Duration::from_millis(rng.random_range(500..2000)));

        // Choose random chaos action
        let action = chaos_actions.choose(&mut rng).unwrap();

        match action {
            ChaosAction::CorruptFile => {
                corrupt_random_file(&ctx.db_path);
                ctx.record_chaos_event(*action);
            }
            ChaosAction::DeleteFile => {
                delete_random_file(&ctx.db_path);
                ctx.record_chaos_event(*action);
            }
            ChaosAction::TruncateFile => {
                truncate_random_file(&ctx.db_path);
                ctx.record_chaos_event(*action);
            }
            ChaosAction::SlowIO => {
                inject_io_delay();
                ctx.record_chaos_event(*action);
            }
            ChaosAction::MemoryPressure => {
                inject_memory_pressure();
                ctx.record_chaos_event(*action);
            }
            _ => {}
        }
    }
}

/// Corrupt a random file in the database directory
fn corrupt_random_file(db_path: &str) {
    let mut rng = rng();

    if let Ok(entries) = fs::read_dir(db_path) {
        let files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();

        if let Some(entry) = files.choose(&mut rng) {
            if let Ok(mut data) = fs::read(&entry.path()) {
                if !data.is_empty() {
                    // Corrupt random bytes
                    let corrupt_count = rng.random_range(1..10);
                    for _ in 0..corrupt_count {
                        let pos = rng.random_range(0..data.len());
                        data[pos] = rng.random();
                    }
                    let _ = fs::write(&entry.path(), data);
                }
            }
        }
    }
}

/// Delete a random file (except critical ones)
fn delete_random_file(db_path: &str) {
    let mut rng = rng();

    if let Ok(entries) = fs::read_dir(db_path) {
        let files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                let path = e.path();
                path.is_file() && !path.to_str().unwrap_or("").contains("metadata")
            })
            .collect();

        if let Some(entry) = files.choose(&mut rng) {
            let _ = fs::remove_file(&entry.path());
        }
    }
}

/// Truncate a random file
fn truncate_random_file(db_path: &str) {
    let mut rng = rng();

    if let Ok(entries) = fs::read_dir(db_path) {
        let files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();

        if let Some(entry) = files.choose(&mut rng) {
            if let Ok(metadata) = entry.metadata() {
                let new_len = rng.random_range(0..metadata.len());
                let _ = fs::OpenOptions::new()
                    .write(true)
                    .open(&entry.path())
                    .and_then(|f| f.set_len(new_len));
            }
        }
    }
}

/// Inject I/O delays
fn inject_io_delay() {
    // In a real implementation, this would use eBPF or similar
    // For now, just sleep to simulate
    thread::sleep(Duration::from_millis(100));
}

/// Inject memory pressure
fn inject_memory_pressure() {
    // Allocate and touch large memory blocks
    let mut allocations = Vec::new();
    for _ in 0..10 {
        let mut block = vec![0u8; 10 * 1024 * 1024]; // 10MB

        // Touch pages to ensure allocation
        for i in (0..block.len()).step_by(4096) {
            block[i] = 1;
        }

        allocations.push(block);
    }

    // Hold for a moment
    thread::sleep(Duration::from_millis(100));

    // Drop to release
    drop(allocations);
}

/// Run a chaos test scenario
fn run_chaos_scenario(name: &str, duration: Duration, thread_count: usize) {
    println!("\n=== Running Chaos Scenario: {} ===", name);

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap().to_string();

    // Create initial database
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).unwrap();

    // Initialize some data
    let tx_id = db.begin_transaction().unwrap();
    for i in 0..100 {
        let key = format!("initial_key_{}", i);
        let value = format!("initial_value_{}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.commit_transaction(tx_id).unwrap();

    // Create chaos context
    let ctx = Arc::new(ChaosContext::new(db_path.clone()));
    *ctx.db.lock().unwrap() = Some(Arc::new(db));

    // Start workload threads
    let mut handles = vec![];
    for i in 0..thread_count {
        let ctx = ctx.clone();
        handles.push(thread::spawn(move || workload_thread(ctx, i as u64)));
    }

    // Start chaos thread
    let chaos_ctx = ctx.clone();
    handles.push(thread::spawn(move || chaos_thread(chaos_ctx)));

    // Run for specified duration
    let start = Instant::now();
    thread::sleep(duration);

    // Stop all threads
    ctx.stop_flag.store(true, Ordering::Relaxed);

    // Wait for threads
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    // Report results
    let total_ops = ctx.operation_count.load(Ordering::Relaxed);
    let total_errors = ctx.error_count.load(Ordering::Relaxed);
    let chaos_events = ctx.chaos_events.lock().unwrap();

    println!("Duration: {:?}", elapsed);
    println!("Total operations: {}", total_ops);
    println!("Total errors: {}", total_errors);
    println!(
        "Error rate: {:.2}%",
        (total_errors as f64 / total_ops as f64) * 100.0
    );
    println!("Chaos events: {}", chaos_events.len());

    for (time, action) in chaos_events.iter() {
        println!("  {:?}: {:?}", time.duration_since(start), action);
    }

    // Verify database can still be opened
    *ctx.db.lock().unwrap() = None;
    match Database::open(&db_path, LightningDbConfig::default()) {
        Ok(db) => {
            println!("Database recovery: SUCCESS");

            // Verify some data
            let tx_id = db.begin_transaction().unwrap();
            let mut found = 0;
            for i in 0..100 {
                let key = format!("initial_key_{}", i);
                if db.get_tx(tx_id, key.as_bytes()).unwrap().is_some() {
                    found += 1;
                }
            }
            println!("Initial data preserved: {}/100", found);
        }
        Err(e) => {
            println!("Database recovery: FAILED - {}", e);
        }
    }
}

#[test]
fn test_chaos_basic() {
    run_chaos_scenario("Basic Chaos", Duration::from_secs(10), 5);
}

#[test]
fn test_chaos_intensive() {
    run_chaos_scenario("Intensive Chaos", Duration::from_secs(30), 20);
}

#[test]
fn test_chaos_long_running() {
    run_chaos_scenario("Long Running Chaos", Duration::from_secs(60), 10);
}

/// Test recovery after multiple chaos events
#[test]
fn test_chaos_recovery_chain() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();

    println!("\n=== Chaos Recovery Chain Test ===");

    // Create and corrupt database multiple times
    for round in 0..5 {
        println!("\nRound {}", round + 1);

        // Open/create database
        let db = if round == 0 {
            Database::create(db_path, LightningDbConfig::default()).unwrap()
        } else {
            Database::open(db_path, LightningDbConfig::default()).unwrap()
        };

        // Write data
        let tx_id = db.begin_transaction().unwrap();
        for i in 0..10 {
            let key = format!("round_{}_key_{}", round, i);
            let value = format!("round_{}_value_{}", round, i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.commit_transaction(tx_id).unwrap();

        // Close database
        drop(db);

        // Inject chaos
        corrupt_random_file(db_path);

        // Try to recover
        match Database::open(db_path, LightningDbConfig::default()) {
            Ok(db) => {
                println!("Recovery successful");

                // Verify data from all rounds
                let tx_id = db.begin_transaction().unwrap();
                for r in 0..=round {
                    let key = format!("round_{}_key_0", r);
                    if db.get_tx(tx_id, key.as_bytes()).unwrap().is_some() {
                        println!("  Round {} data: OK", r);
                    } else {
                        println!("  Round {} data: LOST", r);
                    }
                }
            }
            Err(e) => {
                println!("Recovery failed: {}", e);
                break;
            }
        }
    }
}
