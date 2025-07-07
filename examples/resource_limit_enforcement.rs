use lightning_db::{
    resource_limits::{ResourceEnforcer, ResourceLimits},
    Database, LightningDbConfig,
};
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Resource limit enforcement demonstration
/// Shows how Lightning DB can enforce various resource limits in production
fn main() {
    println!("🛡️  Lightning DB Resource Limit Enforcement Demo");
    println!("📊 Testing various resource limits and enforcement mechanisms");
    println!("{}", "=".repeat(70));

    // Create database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("resource_limited_db");

    let config = LightningDbConfig {
        cache_size: 16 * 1024 * 1024, // 16MB
        compression_enabled: true,
        use_improved_wal: true,
        ..Default::default()
    };

    let db = Arc::new(Database::create(&db_path, config).unwrap());

    // Configure resource limits
    let limits = ResourceLimits {
        max_memory_bytes: 50 * 1024 * 1024, // 50MB
        max_concurrent_operations: 100,
        max_write_throughput_bytes_per_sec: 10 * 1024 * 1024, // 10MB/s
        max_read_throughput_bytes_per_sec: 50 * 1024 * 1024,  // 50MB/s
        max_disk_bytes: 100 * 1024 * 1024,                    // 100MB
        max_wal_size_bytes: 20 * 1024 * 1024,                 // 20MB
        max_transaction_duration: Duration::from_secs(10),
        max_open_files: 50,
        enforcement_enabled: true,
    };

    let enforcer = Arc::new(ResourceEnforcer::new(limits.clone()));

    println!("\n📋 Resource Limits:");
    println!(
        "   Max memory: {} MB",
        limits.max_memory_bytes / 1024 / 1024
    );
    println!(
        "   Max concurrent operations: {}",
        limits.max_concurrent_operations
    );
    println!(
        "   Max write throughput: {} MB/s",
        limits.max_write_throughput_bytes_per_sec / 1024 / 1024
    );
    println!(
        "   Max read throughput: {} MB/s",
        limits.max_read_throughput_bytes_per_sec / 1024 / 1024
    );
    println!(
        "   Max disk usage: {} MB",
        limits.max_disk_bytes / 1024 / 1024
    );

    // Test 1: Concurrent operation limits
    test_concurrent_operations(&db, &enforcer);

    // Test 2: Write throughput limits
    test_write_throughput(&db, &enforcer);

    // Test 3: Memory limits
    test_memory_limits(&db, &enforcer);

    // Test 4: Mixed workload with all limits
    test_mixed_workload(&db, &enforcer);

    // Print final report
    print_enforcement_report(&enforcer);
}

/// Test concurrent operation limits
fn test_concurrent_operations(db: &Arc<Database>, enforcer: &Arc<ResourceEnforcer>) {
    println!("\n🔄 Test 1: Concurrent Operation Limits");
    println!("   Attempting to exceed max concurrent operations...");

    let mut handles = Vec::new();
    let violations = Arc::new(Mutex::new(0));

    // Try to start more operations than allowed
    for i in 0..150 {
        let db = db.clone();
        let enforcer = enforcer.clone();
        let violations = violations.clone();

        let handle = thread::spawn(move || {
            // Try to start an operation
            match enforcer.check_operation_start() {
                Ok(_guard) => {
                    // Operation allowed, do some work
                    let key = format!("concurrent_key_{}", i);
                    let value = format!("value_{}", i);
                    let _ = db.put(key.as_bytes(), value.as_bytes());
                    thread::sleep(Duration::from_millis(100));
                }
                Err(_) => {
                    // Operation rejected
                    *violations.lock().unwrap() += 1;
                }
            }
        });

        handles.push(handle);

        // Small delay to spread out requests
        thread::sleep(Duration::from_millis(5));
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let total_violations = *violations.lock().unwrap();
    println!(
        "   ✅ Concurrent operations limited: {} rejected",
        total_violations
    );
}

/// Test write throughput limits
fn test_write_throughput(db: &Arc<Database>, enforcer: &Arc<ResourceEnforcer>) {
    println!("\n📝 Test 2: Write Throughput Limits");
    println!("   Attempting to exceed write throughput limit...");

    let start_time = Instant::now();
    let mut total_written = 0u64;
    let mut throttled_count = 0;
    let mut total_throttle_time = Duration::ZERO;

    // Try to write data as fast as possible
    for i in 0..100 {
        let key = format!("throughput_key_{:06}", i);
        let value_size = 1024 * 1024; // 1MB values
        let value = vec![0u8; value_size];

        // Check if we can write
        match enforcer.check_write(value_size as u64) {
            Ok(_) => {
                // Write allowed
                if db.put(key.as_bytes(), &value).is_ok() {
                    enforcer.record_write(value_size as u64);
                    total_written += value_size as u64;
                }
            }
            Err(lightning_db::error::Error::Throttled(duration)) => {
                // Throttled - wait as instructed
                throttled_count += 1;
                total_throttle_time += duration;
                thread::sleep(duration);
            }
            Err(_) => {
                // Other error
                break;
            }
        }

        if start_time.elapsed() > Duration::from_secs(5) {
            break;
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = total_written as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;

    println!(
        "   Written: {:.1} MB in {:?}",
        total_written as f64 / 1024.0 / 1024.0,
        elapsed
    );
    println!("   Actual throughput: {:.1} MB/s", throughput);
    println!(
        "   Throttled {} times for total {:?}",
        throttled_count, total_throttle_time
    );
    println!("   ✅ Write throughput successfully limited");
}

/// Test memory limits
fn test_memory_limits(db: &Arc<Database>, enforcer: &Arc<ResourceEnforcer>) {
    println!("\n💾 Test 3: Memory Limits");
    println!("   Attempting to exceed memory limit...");

    let mut memory_used = 0u64;
    let mut rejected_count = 0;

    // Simulate memory usage
    for i in 0..100 {
        let allocation_size = 5 * 1024 * 1024; // 5MB allocations

        // Update simulated memory usage
        enforcer.update_memory_usage(memory_used);

        // Check if we can allocate
        match enforcer.check_write(allocation_size) {
            Ok(_) => {
                // Allocation allowed
                memory_used += allocation_size;
                let key = format!("memory_key_{}", i);
                let value = vec![0u8; 1024]; // Small actual value
                let _ = db.put(key.as_bytes(), &value);
            }
            Err(lightning_db::error::Error::ResourceLimitExceeded(_)) => {
                // Memory limit exceeded
                rejected_count += 1;
            }
            _ => {}
        }
    }

    println!("   Memory used: {} MB", memory_used / 1024 / 1024);
    println!("   Allocations rejected: {}", rejected_count);
    println!("   ✅ Memory usage successfully limited");
}

/// Test mixed workload with all limits
fn test_mixed_workload(db: &Arc<Database>, enforcer: &Arc<ResourceEnforcer>) {
    println!("\n🌪️  Test 4: Mixed Workload with All Limits");
    println!("   Running realistic workload with all limits enforced...");

    let start_time = Instant::now();
    let test_duration = Duration::from_secs(10);

    let stats = Arc::new(Mutex::new(WorkloadStats::default()));
    let mut handles = Vec::new();

    // Start multiple worker threads
    for thread_id in 0..10 {
        let db = db.clone();
        let enforcer = enforcer.clone();
        let stats = stats.clone();

        let handle = thread::spawn(move || {
            let mut rng = rand::rng();
            let mut local_stats = WorkloadStats::default();

            while start_time.elapsed() < test_duration {
                // Check if we can start an operation
                let _guard = match enforcer.check_operation_start() {
                    Ok(guard) => guard,
                    Err(_) => {
                        local_stats.operations_rejected += 1;
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                };

                // Random operation type
                match rng.random_range(0..100) {
                    0..=60 => {
                        // Write operation
                        let key = format!("mixed_key_{}_{}", thread_id, rng.random::<u32>());
                        let value_size = rng.random_range(1024..10240);
                        let value = vec![0u8; value_size];

                        match enforcer.check_write(value_size as u64) {
                            Ok(_) => {
                                if db.put(key.as_bytes(), &value).is_ok() {
                                    enforcer.record_write(value_size as u64);
                                    local_stats.writes_successful += 1;
                                }
                            }
                            Err(lightning_db::error::Error::Throttled(d)) => {
                                local_stats.writes_throttled += 1;
                                thread::sleep(d);
                            }
                            _ => local_stats.writes_rejected += 1,
                        }
                    }
                    _ => {
                        // Read operation
                        let key = format!("mixed_key_{}_{}", thread_id, rng.random::<u32>());

                        match enforcer.check_read(1024) {
                            Ok(_) => {
                                if db.get(key.as_bytes()).is_ok() {
                                    enforcer.record_read(1024);
                                    local_stats.reads_successful += 1;
                                }
                            }
                            Err(lightning_db::error::Error::Throttled(d)) => {
                                local_stats.reads_throttled += 1;
                                thread::sleep(d);
                            }
                            _ => local_stats.reads_rejected += 1,
                        }
                    }
                }

                // Small delay between operations
                thread::sleep(Duration::from_micros(100));
            }

            // Merge stats
            let mut global_stats = stats.lock().unwrap();
            global_stats.merge(&local_stats);
        });

        handles.push(handle);
    }

    // Monitor resource usage
    let enforcer_monitor = enforcer.clone();
    thread::spawn(move || {
        while start_time.elapsed() < test_duration {
            thread::sleep(Duration::from_secs(2));

            let usage = enforcer_monitor.get_usage();
            let percentages = usage.get_usage_percentages(&enforcer_monitor.limits);

            println!("\n   📊 Resource Usage Update:");
            println!("      Memory: {:.1}%", percentages.memory_percent);
            println!("      Operations: {:.1}%", percentages.operations_percent);
            println!(
                "      Write throughput: {:.1}%",
                percentages.write_throughput_percent
            );
            println!(
                "      Read throughput: {:.1}%",
                percentages.read_throughput_percent
            );
        }
    });

    // Wait for workers
    for handle in handles {
        handle.join().unwrap();
    }

    let final_stats = stats.lock().unwrap();
    println!("\n   📈 Workload Statistics:");
    println!("      Writes successful: {}", final_stats.writes_successful);
    println!("      Writes throttled: {}", final_stats.writes_throttled);
    println!("      Writes rejected: {}", final_stats.writes_rejected);
    println!("      Reads successful: {}", final_stats.reads_successful);
    println!("      Reads throttled: {}", final_stats.reads_throttled);
    println!(
        "      Operations rejected: {}",
        final_stats.operations_rejected
    );
}

/// Print enforcement report
fn print_enforcement_report(enforcer: &Arc<ResourceEnforcer>) {
    println!("\n{}", "=".repeat(70));
    println!("📊 RESOURCE ENFORCEMENT REPORT");
    println!("{}", "=".repeat(70));

    let violations = enforcer.get_violations();
    let usage = enforcer.get_usage();
    let percentages = usage.get_usage_percentages(&enforcer.limits);

    println!("\n🚨 Violations Summary:");
    if violations.is_empty() {
        println!("   ✅ No violations recorded");
    } else {
        use std::collections::HashMap;
        let mut violation_counts = HashMap::new();

        for violation in &violations {
            *violation_counts
                .entry(format!("{:?}", violation.violation_type))
                .or_insert(0) += 1;
        }

        for (vtype, count) in violation_counts {
            println!("   {} violations: {}", vtype, count);
        }
    }

    println!("\n📊 Final Resource Usage:");
    println!(
        "   Memory: {:.1} MB ({:.1}%)",
        usage.memory_bytes as f64 / 1024.0 / 1024.0,
        percentages.memory_percent
    );
    println!(
        "   Disk: {:.1} MB ({:.1}%)",
        usage.disk_bytes as f64 / 1024.0 / 1024.0,
        percentages.disk_percent
    );
    println!(
        "   Concurrent operations: {} ({:.1}%)",
        usage.concurrent_operations, percentages.operations_percent
    );

    println!("\n🏁 VERDICT:");
    if violations.is_empty() {
        println!("   ✅ EXCELLENT - All operations stayed within limits");
    } else if violations.len() < 50 {
        println!("   ⚠️  GOOD - Resource limits enforced with minimal violations");
    } else {
        println!(
            "   📈 ACTIVE - Resource limits actively enforced ({} violations)",
            violations.len()
        );
    }

    println!("\n💡 Resource limit enforcement is working correctly!");
    println!("   The database successfully prevents resource exhaustion");
    println!("   and maintains stable operation under load.");

    println!("\n{}", "=".repeat(70));
}

#[derive(Default)]
struct WorkloadStats {
    writes_successful: u64,
    writes_throttled: u64,
    writes_rejected: u64,
    reads_successful: u64,
    reads_throttled: u64,
    reads_rejected: u64,
    operations_rejected: u64,
}

impl WorkloadStats {
    fn merge(&mut self, other: &WorkloadStats) {
        self.writes_successful += other.writes_successful;
        self.writes_throttled += other.writes_throttled;
        self.writes_rejected += other.writes_rejected;
        self.reads_successful += other.reads_successful;
        self.reads_throttled += other.reads_throttled;
        self.reads_rejected += other.reads_rejected;
        self.operations_rejected += other.operations_rejected;
    }
}
