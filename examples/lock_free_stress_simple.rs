use lightning_db::lock_free::{LockFreeQueue, ShardedCache};
use lightning_db::{Database, LightningDbConfig};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() {
    println!("⚡ Lock-Free Components Stress Test (Simplified)");
    println!("==============================================\n");

    // Test lock-free queue
    test_queue_stress();

    // Test sharded cache
    test_sharded_cache_stress();

    // Test integrated database
    test_database_stress();

    println!("\n✅ All tests completed!");
}

fn test_queue_stress() {
    println!("Test 1: Lock-Free Queue Stress");
    println!("-----------------------------");

    let queue = Arc::new(LockFreeQueue::unbounded());
    let num_producers = 4;
    let num_consumers = 4;
    let items_per_producer = 10_000;

    let start = Instant::now();
    let mut handles = vec![];

    // Producers
    for producer_id in 0..num_producers {
        let queue_clone = Arc::clone(&queue);

        let handle = thread::spawn(move || {
            for i in 0..items_per_producer {
                let value = producer_id * items_per_producer + i;
                queue_clone.push(value).expect("Push failed");
            }
        });

        handles.push(handle);
    }

    // Consumers
    let consumed = Arc::new(AtomicU64::new(0));
    for _ in 0..num_consumers {
        let queue_clone = Arc::clone(&queue);
        let consumed_clone = Arc::clone(&consumed);

        let handle = thread::spawn(move || {
            while consumed_clone.load(Ordering::Relaxed)
                < (num_producers * items_per_producer) as u64
            {
                if queue_clone.pop().is_some() {
                    consumed_clone.fetch_add(1, Ordering::Relaxed);
                } else {
                    thread::yield_now();
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    println!("  Duration: {:?}", elapsed);
    println!("  Total items: {}", num_producers * items_per_producer);
    println!(
        "  Throughput: {:.2} items/sec\n",
        (num_producers * items_per_producer) as f64 / elapsed.as_secs_f64()
    );
}

fn test_sharded_cache_stress() {
    println!("Test 2: Sharded Cache Stress");
    println!("---------------------------");

    let cache = Arc::new(ShardedCache::<String, Vec<u8>>::new(16, 10_000));
    let num_threads = 8;
    let ops_per_thread = 50_000;

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let cache_clone = Arc::clone(&cache);

        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            let mut hits = 0;
            let mut misses = 0;

            for _ in 0..ops_per_thread {
                let key = format!("key_{}", rng.random_range(0..10_000));

                if rng.random_bool(0.7) {
                    // Read
                    if cache_clone.get(&key).is_some() {
                        hits += 1;
                    } else {
                        misses += 1;
                        // Insert on miss
                        let value = vec![rng.random::<u8>(); 100];
                        cache_clone.insert(key, value);
                    }
                } else {
                    // Write
                    let value = vec![rng.random::<u8>(); 100];
                    cache_clone.insert(key, value);
                }
            }

            (hits, misses)
        });

        handles.push(handle);
    }

    let mut total_hits = 0;
    let mut total_misses = 0;

    for handle in handles {
        let (hits, misses) = handle.join().unwrap();
        total_hits += hits;
        total_misses += misses;
    }

    let elapsed = start.elapsed();
    let total_ops = num_threads * ops_per_thread;

    println!("  Duration: {:?}", elapsed);
    println!("  Total operations: {}", total_ops);
    println!(
        "  Cache hit rate: {:.1}%",
        total_hits as f64 / (total_hits + total_misses) as f64 * 100.0
    );
    println!(
        "  Throughput: {:.2} ops/sec\n",
        total_ops as f64 / elapsed.as_secs_f64()
    );
}

fn test_database_stress() {
    println!("Test 3: Database with Lock-Free Components");
    println!("-----------------------------------------");

    let db_path = "lock_free_test_db";
    let _ = std::fs::remove_dir_all(db_path);

    let config = LightningDbConfig {
        cache_size: 64 * 1024 * 1024, // 64MB
        use_optimized_transactions: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));
    let num_threads = 8;
    let ops_per_thread = 10_000;

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);

        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);

            for i in 0..ops_per_thread {
                let key = format!("key_{}", rng.random_range(0..50_000));

                match rng.random_range(0..10) {
                    0..=6 => {
                        // Read (70%)
                        let _ = db_clone.get(key.as_bytes());
                    }
                    7..=8 => {
                        // Write (20%)
                        let value = vec![rng.random::<u8>(); 256];
                        db_clone.put(key.as_bytes(), &value).expect("Put failed");
                    }
                    9 => {
                        // Delete (10%)
                        let _ = db_clone.delete(key.as_bytes());
                    }
                    _ => unreachable!(),
                }

                if i % 1000 == 0 && i > 0 {
                    println!("  Thread {} progress: {}/{}", thread_id, i, ops_per_thread);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_operations = num_threads * ops_per_thread;

    println!("  Duration: {:?}", elapsed);
    println!("  Total operations: {}", total_operations);
    println!(
        "  Throughput: {:.2} ops/sec",
        total_operations as f64 / elapsed.as_secs_f64()
    );

    let stats = db.stats();
    println!("  Database stats: {:?}", stats);
}
