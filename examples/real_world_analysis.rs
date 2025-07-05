/// Real-World Usage Analysis
/// 
/// Comprehensive testing of Lightning DB with realistic workloads

use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::collections::HashMap;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 LIGHTNING DB REAL-WORLD ANALYSIS");
    println!("====================================\n");
    
    // Test 1: E-commerce Product Catalog
    println!("1️⃣ E-Commerce Product Catalog Simulation");
    test_ecommerce_workload()?;
    
    // Test 2: Time-Series Metrics Storage
    println!("\n2️⃣ Time-Series Metrics Storage");
    test_timeseries_workload()?;
    
    // Test 3: Session Store Pattern
    println!("\n3️⃣ Session Store Pattern");
    test_session_store()?;
    
    // Test 4: Counter/Analytics Workload
    println!("\n4️⃣ Counter/Analytics Workload");
    test_analytics_counters()?;
    
    // Test 5: Hot Key Detection
    println!("\n5️⃣ Hot Key Performance Analysis");
    test_hot_keys()?;
    
    // Test 6: Large Value Performance
    println!("\n6️⃣ Large Value Performance");
    test_large_values()?;
    
    // Test 7: Memory Pressure Test
    println!("\n7️⃣ Memory Pressure Test");
    test_memory_pressure()?;
    
    // Test 8: Concurrent Transaction Stress
    println!("\n8️⃣ Concurrent Transaction Stress");
    test_concurrent_transactions()?;
    
    Ok(())
}

fn test_ecommerce_workload() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.cache_size = 50 * 1024 * 1024; // 50MB cache for product data
    
    let db = Database::create(temp_dir.path(), config)?;
    let start = Instant::now();
    
    // Simulate product catalog
    let num_products = 10_000;
    let num_categories = 100;
    
    // Insert products
    for i in 0..num_products {
        let product_key = format!("product:{}", i);
        let category = i % num_categories;
        let product_data = format!(
            r#"{{"id":{},"name":"Product {}","price":{:.2},"category":{},"stock":{},"description":"A great product with many features"}}"#,
            i, i, (i as f64) * 10.99, category, i % 1000
        );
        
        db.put(product_key.as_bytes(), product_data.as_bytes())?;
        
        // Category index
        let category_key = format!("category:{}:product:{}", category, i);
        db.put(category_key.as_bytes(), &i.to_le_bytes())?;
    }
    
    let insert_time = start.elapsed();
    
    // Simulate reads (90% products, 10% category listings)
    let mut rng = StdRng::seed_from_u64(42);
    let read_start = Instant::now();
    let mut read_count = 0;
    
    for _ in 0..50_000 {
        if rng.gen_bool(0.9) {
            // Product lookup
            let product_id = rng.gen_range(0..num_products);
            let key = format!("product:{}", product_id);
            let _ = db.get(key.as_bytes())?;
        } else {
            // Category listing (range scan simulation)
            let category = rng.gen_range(0..num_categories);
            let prefix = format!("category:{}:", category);
            
            // Simulate scanning first 20 products in category
            for i in 0..20 {
                let key = format!("{}product:{}", prefix, i);
                let _ = db.get(key.as_bytes());
            }
        }
        read_count += 1;
    }
    
    let read_time = read_start.elapsed();
    let read_throughput = read_count as f64 / read_time.as_secs_f64();
    
    println!("   Products: {}, Categories: {}", num_products, num_categories);
    println!("   Insert time: {:.2}s ({:.0} products/sec)", 
             insert_time.as_secs_f64(), 
             num_products as f64 / insert_time.as_secs_f64());
    println!("   Read throughput: {:.0} ops/sec", read_throughput);
    
    // Check memory usage
    let stats = db.stats();
    println!("   Cache hit rate: {:.1}%", stats.cache_hit_rate * 100.0);
    
    Ok(())
}

fn test_timeseries_workload() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true; // Enable compression for time series
    config.enable_hot_key_optimization = true;
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Simulate metrics: metric_name:timestamp -> value
    let metrics = vec!["cpu_usage", "memory_usage", "disk_io", "network_rx", "network_tx"];
    let start_ts = 1_700_000_000_000u64; // Unix timestamp in ms
    let interval = 1000; // 1 second intervals
    let duration = 3600; // 1 hour of data
    
    let write_start = Instant::now();
    let mut write_count = 0;
    
    // Write time series data
    for i in 0..duration {
        let timestamp = start_ts + (i * interval);
        
        for metric in &metrics {
            let key = format!("{}:{}", metric, timestamp);
            let value = format!("{:.2}", rand::random::<f64>() * 100.0);
            
            db.put(key.as_bytes(), value.as_bytes())?;
            write_count += 1;
        }
    }
    
    let write_time = write_start.elapsed();
    let write_throughput = write_count as f64 / write_time.as_secs_f64();
    
    // Query patterns
    let query_start = Instant::now();
    
    // 1. Latest value queries
    for metric in &metrics {
        let latest_key = format!("{}:{}", metric, start_ts + ((duration - 1) * interval));
        let _ = db.get(latest_key.as_bytes())?;
    }
    
    // 2. Range queries (last 5 minutes)
    let range_start = start_ts + ((duration - 300) * interval);
    let mut range_count = 0;
    
    for metric in &metrics {
        for i in 0..300 {
            let key = format!("{}:{}", metric, range_start + (i * interval));
            if db.get(key.as_bytes())?.is_some() {
                range_count += 1;
            }
        }
    }
    
    let query_time = query_start.elapsed();
    
    println!("   Metrics: {}, Data points: {}", metrics.len(), write_count);
    println!("   Write throughput: {:.0} points/sec", write_throughput);
    println!("   Range query: {} points in {:.2}ms", range_count, query_time.as_millis());
    
    Ok(())
}

fn test_session_store() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.sync_mode = WalSyncMode::Async; // Sessions can tolerate some data loss
    
    let db = Arc::new(Database::create(temp_dir.path(), config)?);
    
    // Simulate session operations
    let num_threads = 8;
    let sessions_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));
    let errors = Arc::new(Mutex::new(0));
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        let errors_clone = errors.clone();
        
        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            barrier_clone.wait();
            
            for i in 0..sessions_per_thread {
                let session_id = format!("session:{}:{}", thread_id, i);
                let user_id = rng.gen_range(1..10000);
                
                // Create session
                let session_data = format!(
                    r#"{{"user_id":{},"created_at":{},"last_access":{},"data":{{"cart_items":{},"page_views":{}}}}}"#,
                    user_id,
                    Instant::now().elapsed().as_secs(),
                    Instant::now().elapsed().as_secs(),
                    rng.gen_range(0..10),
                    rng.gen_range(1..100)
                );
                
                if db_clone.put(session_id.as_bytes(), session_data.as_bytes()).is_err() {
                    *errors_clone.lock().unwrap() += 1;
                }
                
                // Random reads (session lookups)
                if rng.gen_bool(0.7) {
                    let random_session = format!("session:{}:{}", 
                                                 rng.gen_range(0..num_threads), 
                                                 rng.gen_range(0..i.max(1)));
                    let _ = db_clone.get(random_session.as_bytes());
                }
                
                // Occasional deletes (session expiry)
                if rng.gen_bool(0.1) {
                    let old_session = format!("session:{}:{}", thread_id, i.saturating_sub(100));
                    let _ = db_clone.delete(old_session.as_bytes());
                }
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = num_threads * sessions_per_thread;
    let throughput = total_ops as f64 / duration.as_secs_f64();
    let error_count = *errors.lock().unwrap();
    
    println!("   Threads: {}, Sessions: {}", num_threads, total_ops);
    println!("   Throughput: {:.0} sessions/sec", throughput);
    println!("   Errors: {}", error_count);
    println!("   Avg latency: {:.2}μs", duration.as_micros() as f64 / total_ops as f64);
    
    Ok(())
}

fn test_analytics_counters() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;
    
    // Simulate page view counters
    let pages = vec!["home", "products", "about", "contact", "blog"];
    let start = Instant::now();
    let num_updates = 100_000;
    
    for i in 0..num_updates {
        let page = pages[i % pages.len()];
        let counter_key = format!("pageviews:{}", page);
        
        // Atomic increment simulation
        let tx_id = db.begin_transaction()?;
        
        let current = match db.get_tx(tx_id, counter_key.as_bytes())? {
            Some(val) => {
                let s = std::str::from_utf8(&val)?;
                s.parse::<u64>().unwrap_or(0)
            }
            None => 0,
        };
        
        let new_value = (current + 1).to_string();
        db.put_tx(tx_id, counter_key.as_bytes(), new_value.as_bytes())?;
        db.commit_transaction(tx_id)?;
    }
    
    let duration = start.elapsed();
    let throughput = num_updates as f64 / duration.as_secs_f64();
    
    // Read final counts
    println!("   Counter updates: {}", num_updates);
    println!("   Throughput: {:.0} increments/sec", throughput);
    
    for page in &pages {
        let key = format!("pageviews:{}", page);
        if let Some(val) = db.get(key.as_bytes())? {
            let count: u64 = std::str::from_utf8(&val)?.parse()?;
            println!("   {}: {} views", page, count);
        }
    }
    
    Ok(())
}

fn test_hot_keys() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.enable_hot_key_optimization = true;
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Create workload with 90% requests to 10% of keys
    let total_keys = 1000;
    let hot_keys = 100;
    let operations = 100_000;
    
    // Populate all keys
    for i in 0..total_keys {
        let key = format!("key:{:06}", i);
        db.put(key.as_bytes(), b"initial_value")?;
    }
    
    let mut rng = StdRng::seed_from_u64(123);
    let start = Instant::now();
    
    for _ in 0..operations {
        let key_id = if rng.gen_bool(0.9) {
            // Hot key
            rng.gen_range(0..hot_keys)
        } else {
            // Cold key
            rng.gen_range(hot_keys..total_keys)
        };
        
        let key = format!("key:{:06}", key_id);
        let _ = db.get(key.as_bytes())?;
    }
    
    let duration = start.elapsed();
    let throughput = operations as f64 / duration.as_secs_f64();
    
    let stats = db.stats();
    
    println!("   Total keys: {}, Hot keys: {}", total_keys, hot_keys);
    println!("   Operations: {}, Throughput: {:.0} ops/sec", operations, throughput);
    println!("   Cache hit rate: {:.1}%", stats.cache_hit_rate * 100.0);
    println!("   Avg latency: {:.2}μs", duration.as_micros() as f64 / operations as f64);
    
    Ok(())
}

fn test_large_values() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Test various value sizes
    let value_sizes = vec![
        ("1KB", 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
        ("1MB", 1024 * 1024),
    ];
    
    println!("   Testing large value performance:");
    
    for (label, size) in value_sizes {
        let value = vec![b'x'; size];
        let key = format!("large_value_{}", label);
        
        // Write
        let write_start = Instant::now();
        db.put(key.as_bytes(), &value)?;
        let write_time = write_start.elapsed();
        
        // Read
        let read_start = Instant::now();
        let retrieved = db.get(key.as_bytes())?;
        let read_time = read_start.elapsed();
        
        println!("     {}: Write {:.2}ms, Read {:.2}ms, Verified: {}", 
                 label, 
                 write_time.as_secs_f64() * 1000.0,
                 read_time.as_secs_f64() * 1000.0,
                 retrieved.map(|v| v.len() == size).unwrap_or(false));
    }
    
    Ok(())
}

fn test_memory_pressure() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024; // Small 10MB cache
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Insert data larger than cache
    let num_entries = 50_000;
    let value_size = 1024; // 1KB values = 50MB total
    
    let start = Instant::now();
    
    for i in 0..num_entries {
        let key = format!("mem_test:{:08}", i);
        let value = format!("{:01024}", i); // 1KB value
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    let write_time = start.elapsed();
    
    // Random reads to test cache eviction
    let mut rng = StdRng::seed_from_u64(42);
    let read_start = Instant::now();
    let num_reads = 10_000;
    
    for _ in 0..num_reads {
        let key_id = rng.gen_range(0..num_entries);
        let key = format!("mem_test:{:08}", key_id);
        let _ = db.get(key.as_bytes())?;
    }
    
    let read_time = read_start.elapsed();
    let stats = db.stats();
    
    println!("   Data size: {}MB, Cache size: 10MB", 
             (num_entries * value_size) / (1024 * 1024));
    println!("   Write throughput: {:.0} entries/sec", 
             num_entries as f64 / write_time.as_secs_f64());
    println!("   Read throughput: {:.0} ops/sec", 
             num_reads as f64 / read_time.as_secs_f64());
    println!("   Cache hit rate: {:.1}%", stats.cache_hit_rate * 100.0);
    
    Ok(())
}

fn test_concurrent_transactions() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(temp_dir.path(), config)?);
    
    // Initialize shared counters
    let num_counters = 10;
    for i in 0..num_counters {
        let key = format!("counter:{}", i);
        db.put(key.as_bytes(), b"0")?;
    }
    
    let num_threads = 8;
    let ops_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));
    let conflicts = Arc::new(Mutex::new(0));
    let successes = Arc::new(Mutex::new(0));
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        let conflicts_clone = conflicts.clone();
        let successes_clone = successes.clone();
        
        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            barrier_clone.wait();
            
            for _ in 0..ops_per_thread {
                let counter_id = rng.gen_range(0..num_counters);
                let key = format!("counter:{}", counter_id);
                
                // Try to increment counter
                match increment_counter(&db_clone, &key) {
                    Ok(_) => *successes_clone.lock().unwrap() += 1,
                    Err(_) => *conflicts_clone.lock().unwrap() += 1,
                }
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_attempts = num_threads * ops_per_thread;
    let success_count = *successes.lock().unwrap();
    let conflict_count = *conflicts.lock().unwrap();
    
    // Verify final counter values
    let mut total_count = 0;
    for i in 0..num_counters {
        let key = format!("counter:{}", i);
        if let Some(val) = db.get(key.as_bytes())? {
            let count: u64 = std::str::from_utf8(&val)?.parse()?;
            total_count += count;
        }
    }
    
    println!("   Threads: {}, Operations: {}", num_threads, total_attempts);
    println!("   Successful: {}, Conflicts: {}", success_count, conflict_count);
    println!("   Success rate: {:.1}%", (success_count as f64 / total_attempts as f64) * 100.0);
    println!("   Total increments: {} (expected: {})", total_count, success_count);
    println!("   Throughput: {:.0} tx/sec", success_count as f64 / duration.as_secs_f64());
    
    Ok(())
}

fn increment_counter(db: &Database, key: &str) -> Result<(), Box<dyn std::error::Error>> {
    let tx_id = db.begin_transaction()?;
    
    let current = match db.get_tx(tx_id, key.as_bytes())? {
        Some(val) => std::str::from_utf8(&val)?.parse::<u64>()?,
        None => 0,
    };
    
    let new_value = (current + 1).to_string();
    db.put_tx(tx_id, key.as_bytes(), new_value.as_bytes())?;
    
    db.commit_transaction(tx_id)?;
    Ok(())
}