use lightning_db::{Database, LightningDbConfig};
use std::process;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};
use tempfile::TempDir;

/// Quick stability test - 30 second version
fn main() {
    println!("🏃 Lightning DB Quick Stability Test (30 seconds)");
    println!("{}", "=".repeat(60));

    let test_dir = TempDir::new().unwrap();
    let db_path = test_dir.path().join("stability_db");

    // Create database
    let config = LightningDbConfig {
        cache_size: 32 * 1024 * 1024, // 32MB cache
        compression_enabled: true,
        use_improved_wal: true,
        wal_sync_mode: lightning_db::WalSyncMode::Periodic { interval_ms: 1000 },
        ..Default::default()
    };

    let db = Arc::new(Database::create(&db_path, config).unwrap());

    // Capture initial metrics
    let start_time = Instant::now();
    let initial_memory = get_memory_usage();
    let initial_fd_count = get_fd_count();

    println!("📊 Initial Metrics:");
    println!(
        "   Memory: {:.2} MB",
        initial_memory as f64 / 1024.0 / 1024.0
    );
    println!("   File Descriptors: {}", initial_fd_count);

    // Track metrics
    let op_count = Arc::new(Mutex::new(0u64));
    let error_count = Arc::new(Mutex::new(0u64));
    let should_stop = Arc::new(Mutex::new(false));

    // Start workload threads
    let mut handles = Vec::new();
    for thread_id in 0..4 {
        let db = db.clone();
        let op_count = op_count.clone();
        let error_count = error_count.clone();
        let should_stop = should_stop.clone();

        let handle = thread::spawn(move || {
            let mut local_ops = 0u64;

            while !*should_stop.lock().unwrap() {
                // Simple write operation
                let key = format!("key_{:06}_{:06}", thread_id, local_ops);
                let value = format!("value_{}_data", thread_id);

                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => local_ops += 1,
                    Err(_) => *error_count.lock().unwrap() += 1,
                }

                // Simple read operation
                if local_ops > 0 {
                    let read_key = format!("key_{:06}_{:06}", thread_id, local_ops - 1);
                    let _ = db.get(read_key.as_bytes());
                }

                if local_ops % 1000 == 0 {
                    *op_count.lock().unwrap() += 1000;
                }
            }

            *op_count.lock().unwrap() += local_ops % 1000;
        });

        handles.push(handle);
    }

    // Monitor for 30 seconds
    let test_duration = Duration::from_secs(30);
    let mut peak_memory = initial_memory;
    let mut measurements = Vec::new();

    while start_time.elapsed() < test_duration {
        thread::sleep(Duration::from_secs(5));

        let current_memory = get_memory_usage();
        let current_fds = get_fd_count();
        let elapsed = start_time.elapsed();

        measurements.push((elapsed, current_memory, current_fds));
        peak_memory = peak_memory.max(current_memory);

        println!(
            "\n⏱️  {:?} - Memory: {:.2} MB, FDs: {}",
            elapsed,
            current_memory as f64 / 1024.0 / 1024.0,
            current_fds
        );
    }

    // Stop threads
    *should_stop.lock().unwrap() = true;
    for handle in handles {
        handle.join().unwrap();
    }

    // Final metrics
    let final_memory = get_memory_usage();
    let final_fd_count = get_fd_count();
    let total_ops = *op_count.lock().unwrap();
    let total_errors = *error_count.lock().unwrap();

    // Generate report
    println!("\n{}", "=".repeat(60));
    println!("📊 STABILITY TEST REPORT");
    println!("{}", "=".repeat(60));

    println!("\n🔢 Operations:");
    println!("   Total: {}", total_ops);
    println!("   Errors: {}", total_errors);
    println!(
        "   Error Rate: {:.4}%",
        total_errors as f64 / total_ops.max(1) as f64 * 100.0
    );

    println!("\n💾 Memory:");
    println!(
        "   Initial: {:.2} MB",
        initial_memory as f64 / 1024.0 / 1024.0
    );
    println!("   Final: {:.2} MB", final_memory as f64 / 1024.0 / 1024.0);
    println!("   Peak: {:.2} MB", peak_memory as f64 / 1024.0 / 1024.0);
    println!(
        "   Growth: {:.2} MB",
        (final_memory as i64 - initial_memory as i64) as f64 / 1024.0 / 1024.0
    );

    println!("\n📁 File Descriptors:");
    println!("   Initial: {}", initial_fd_count);
    println!("   Final: {}", final_fd_count);
    println!(
        "   Change: {}",
        final_fd_count as i32 - initial_fd_count as i32
    );

    // Detect issues
    let memory_growth = final_memory as i64 - initial_memory as i64;
    let fd_growth = final_fd_count as i32 - initial_fd_count as i32;

    println!("\n🔍 Analysis:");
    let mut issues = Vec::new();

    if memory_growth > 10 * 1024 * 1024 {
        // 10MB growth
        issues.push(format!(
            "High memory growth: {:.2} MB",
            memory_growth as f64 / 1024.0 / 1024.0
        ));
    }

    if fd_growth > 10 {
        issues.push(format!("File descriptor leak: {} new FDs", fd_growth));
    }

    if total_errors > 0 {
        issues.push(format!("{} errors encountered", total_errors));
    }

    if issues.is_empty() {
        println!("   ✅ No stability issues detected");
    } else {
        println!("   ⚠️  Issues detected:");
        for issue in &issues {
            println!("      - {}", issue);
        }
    }

    println!("\n🏁 VERDICT:");
    if issues.is_empty() {
        println!("   ✅ STABLE - No leaks or issues in 30 second test");
    } else {
        println!("   ⚠️  POTENTIAL ISSUES - Review findings above");
    }

    println!("\n{}", "=".repeat(60));
}

fn get_memory_usage() -> u64 {
    let mut system = System::new();
    let pid = Pid::from_u32(process::id());
    system.refresh_all();

    system.process(pid).map(|p| p.memory()).unwrap_or(0)
}

fn get_fd_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        let fd_dir = format!("/proc/{}/fd", process::id());
        fs::read_dir(&fd_dir)
            .map(|entries| entries.count())
            .unwrap_or(0)
    }

    #[cfg(target_os = "macos")]
    {
        // Simplified for quick test
        let output = process::Command::new("lsof")
            .args(["-p", &process::id().to_string()])
            .output()
            .unwrap_or_else(|_| process::Output {
                status: Default::default(),
                stdout: Vec::new(),
                stderr: Vec::new(),
            });

        String::from_utf8_lossy(&output.stdout)
            .lines()
            .count()
            .saturating_sub(1)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}
