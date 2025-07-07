use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Group Commit Performance Test\n");

    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Create database with async WAL
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = lightning_db::WalSyncMode::Async;
    let db = Arc::new(Database::create(&db_path, config)?);

    // Test 1: Single-threaded writes (baseline)
    {
        println!("Test 1: Single-threaded writes");
        let start = Instant::now();
        let count = 1000;

        for i in 0..count {
            let key = format!("single_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
    }

    // Test 2: Multi-threaded writes (should trigger group commit)
    {
        println!("\nTest 2: Multi-threaded writes (4 threads)");
        let num_threads = 4;
        let writes_per_thread = 2500;
        let mut handles = Vec::new();

        let start = Instant::now();

        for t in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("thread_{}_{:08}", t, i);
                    db_clone.put(key.as_bytes(), b"value").unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_writes = num_threads * writes_per_thread;
        let ops_per_sec = total_writes as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!(
            "  • Status: {}",
            if ops_per_sec >= 100_000.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );
    }

    // Test 3: Simulate group commit with manual batching
    {
        println!("\nTest 3: Manual batch simulation");
        let batch_size = 100;
        let num_batches = 100;

        let start = Instant::now();

        // Create multiple threads that write concurrently
        let mut handles = Vec::new();
        for b in 0..num_batches {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                // Each thread does a small batch
                for i in 0..batch_size {
                    let key = format!("batch_{}_{:04}", b, i);
                    db_clone.put(key.as_bytes(), b"value").unwrap();
                }
            });
            handles.push(handle);

            // Start threads in waves to increase concurrency
            if handles.len() >= 10 {
                for h in handles.drain(..) {
                    h.join().unwrap();
                }
            }
        }

        // Wait for remaining threads
        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_writes = batch_size * num_batches;
        let ops_per_sec = total_writes as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!(
            "  • Status: {}",
            if ops_per_sec >= 100_000.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );
    }

    // Final sync and verification
    db.sync()?;

    // Verify some data
    let test_key = b"thread_0_00000000";
    match db.get(test_key)? {
        Some(val) if val == b"value" => println!("\n✅ Data verification passed"),
        _ => println!("\n❌ Data verification failed"),
    }

    Ok(())
}
