use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Quick memory leak test (30 seconds)
fn main() {
    println!("🧪 Quick Memory Leak Test (30s)");
    println!("Testing version cleanup under load");
    println!("{}", "=".repeat(50));

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let mut config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024,
        ..Default::default()
    }; // 10MB cache
    config.compression_enabled = true;

    let db = Arc::new(Database::create(db_path, config).unwrap());

    println!("📊 Starting workload...");

    let duration = Duration::from_secs(30); // 30 second test
    let start_time = Instant::now();

    let mut handles = Vec::new();

    // Spawn 2 worker threads
    for worker_id in 0..2 {
        let db_clone = db.clone();
        let worker_start = start_time;
        let worker_duration = duration;

        let handle = thread::spawn(move || {
            let mut local_ops = 0;
            let mut local_tx_ops = 0;

            while worker_start.elapsed() < worker_duration {
                // Create transaction workload to generate MVCC versions
                if local_ops % 5 == 0 {
                    if let Ok(tx_id) = db_clone.begin_transaction() {
                        for i in 0..3 {
                            let key = format!("w{}_tx_k{}", worker_id, i);
                            let value = format!("w{}_tx_v{}_{}", worker_id, i, local_tx_ops);
                            let _ = db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes());
                        }

                        // Commit most, abort some to create version churn
                        if local_tx_ops % 4 == 0 {
                            let _ = db_clone.abort_transaction(tx_id);
                        } else {
                            let _ = db_clone.commit_transaction(tx_id);
                        }
                        local_tx_ops += 1;
                    }
                } else {
                    // Regular operations
                    let key = format!("w{}_k{}", worker_id, local_ops);
                    let value = format!(
                        "w{}_v{}_{}",
                        worker_id,
                        local_ops,
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                    );

                    let _ = db_clone.put(key.as_bytes(), value.as_bytes());

                    if local_ops % 10 == 0 {
                        let _ = db_clone.get(key.as_bytes());
                    }
                }

                local_ops += 1;
                thread::sleep(Duration::from_millis(2)); // Slight pause
            }

            (local_ops, local_tx_ops)
        });

        handles.push(handle);
    }

    // Wait for completion
    let mut total_ops = 0;
    let mut total_tx_ops = 0;

    for handle in handles {
        let (ops, tx_ops) = handle.join().unwrap();
        total_ops += ops;
        total_tx_ops += tx_ops;
    }

    // Force cleanup and checkpoint
    println!("🔄 Forcing cleanup...");
    let _ = db.checkpoint();
    thread::sleep(Duration::from_millis(500));

    println!("\n📈 RESULTS:");
    println!("  Duration: {:?}", duration);
    println!("  Total Operations: {}", total_ops);
    println!("  Total Transactions: {}", total_tx_ops);
    println!(
        "  Ops/sec: {:.0}",
        total_ops as f64 / duration.as_secs_f64()
    );

    // Basic memory assessment using process stats
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
    {
        if let Ok(memory_str) = String::from_utf8(output.stdout) {
            if let Ok(memory_kb) = memory_str.trim().parse::<u64>() {
                println!(
                    "  Current Memory: {} KB ({:.1} MB)",
                    memory_kb,
                    memory_kb as f64 / 1024.0
                );

                // For a quick test, just verify memory isn't excessive
                let memory_mb = memory_kb as f64 / 1024.0;
                if memory_mb < 200.0 {
                    println!("  ✅ Memory usage appears reasonable");
                } else if memory_mb < 500.0 {
                    println!("  ⚠️  Memory usage elevated but acceptable");
                } else {
                    println!("  ❌ Memory usage excessive!");
                }
            }
        }
    }

    // Verify functionality
    println!("\n🧪 FUNCTIONALITY CHECK:");
    let test_key = b"final_test";
    let test_value = b"final_value";

    match db.put(test_key, test_value) {
        Ok(_) => match db.get(test_key) {
            Ok(Some(value)) if value == test_value => {
                println!("  ✅ Database fully functional");
            }
            _ => {
                println!("  ❌ Database read issues");
            }
        },
        Err(e) => {
            println!("  ❌ Database write issues: {}", e);
        }
    }

    // Test transaction after intensive workload
    match db.begin_transaction() {
        Ok(tx_id) => {
            let tx_result = db
                .put_tx(tx_id, b"test_tx", b"test_val")
                .and_then(|_| db.commit_transaction(tx_id));

            match tx_result {
                Ok(_) => println!("  ✅ Transactions working correctly"),
                Err(e) => println!("  ❌ Transaction issues: {}", e),
            }
        }
        Err(e) => {
            println!("  ❌ Transaction creation failed: {}", e);
        }
    }

    println!("\n{}", "=".repeat(50));
    println!("✅ Quick memory test completed!");
}
