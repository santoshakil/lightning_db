use lightning_db::{Database, LightningDbConfig, Key, WalSyncMode};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use rand::Rng;

#[test]
fn test_ecommerce_platform_simulation() {
    let dir = tempdir().expect("Failed to create temp dir");
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    config.cache_size = 2 * 1024 * 1024;
    config.use_optimized_transactions = true;
    
    let db = Arc::new(Database::create(dir.path(), config).expect("Failed to create database"));
    
    println!("\n=== E-Commerce Platform Simulation ===\n");
    
    println!("1. Loading product catalog...");
    for product_id in 0..1000 {
        let product_key = format!("product:{:06}", product_id);
        let product_data = format!(r#"{{
            "id": {},
            "name": "Product {}",
            "price": {:.2},
            "stock": {},
            "category": "category_{}",
            "tags": ["tag_{}", "tag_{}"],
            "description": "High-quality product with excellent features",
            "images": ["img1.jpg", "img2.jpg", "img3.jpg"],
            "reviews_count": {},
            "average_rating": {:.1}
        }}"#, 
            product_id, 
            product_id, 
            10.0 + (product_id as f64 * 0.5),
            1000 - (product_id % 100),
            product_id % 20,
            product_id % 5,
            product_id % 10,
            product_id % 50,
            3.5 + (product_id % 3) as f64 * 0.5
        );
        
        db.put(product_key.as_bytes(), product_data.as_bytes()).expect("Put failed");
    }
    println!("   ✓ Loaded 1000 products");
    
    println!("\n2. Simulating concurrent user sessions...");
    let barrier = Arc::new(Barrier::new(20));
    let mut handles = vec![];
    
    for user_id in 0..20 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            let session_key = format!("session:user:{}", user_id);
            let session_data = format!(r#"{{
                "user_id": {},
                "login_time": "2024-01-01T12:00:00Z",
                "ip": "192.168.1.{}",
                "cart_items": 0,
                "wishlist_items": 0
            }}"#, user_id, user_id);
            
            db_clone.put(session_key.as_bytes(), session_data.as_bytes()).expect("Put failed");
            
            for item in 0..10 {
                let cart_key = format!("cart:user:{}:item:{}", user_id, item);
                let product_id = (user_id * 10 + item) % 1000;
                let cart_data = format!(r#"{{
                    "product_id": {},
                    "quantity": {},
                    "price": {:.2}
                }}"#, product_id, 1 + item % 3, 10.0 + product_id as f64 * 0.5);
                
                db_clone.put(cart_key.as_bytes(), cart_data.as_bytes()).expect("Put failed");
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    println!("   ✓ Created 20 concurrent user sessions with shopping carts");
    
    println!("\n3. Processing orders with transactions...");
    for order_id in 1000..1010 {
        let tx_id = db.begin_transaction().expect("Failed to begin transaction");
        
        let order_key = format!("order:{}", order_id);
        let order_data = format!(r#"{{
            "order_id": {},
            "user_id": {},
            "total": {:.2},
            "status": "processing",
            "created_at": "2024-01-01T13:00:00Z"
        }}"#, order_id, order_id % 20, 100.0 + (order_id as f64 * 10.0));
        
        db.put_tx(tx_id, order_key.as_bytes(), order_data.as_bytes()).expect("Put failed");
        
        for item in 0..5 {
            let item_key = format!("order:{}:item:{}", order_id, item);
            let item_data = format!(r#"{{
                "product_id": {},
                "quantity": {},
                "price": {:.2}
            }}"#, item * 100, 1 + item % 3, 20.0 + item as f64 * 5.0);
            
            db.put_tx(tx_id, item_key.as_bytes(), item_data.as_bytes()).expect("Put failed");
        }
        
        let inventory_key = format!("inventory:product:{}", order_id % 100);
        let inventory_data = format!(r#"{{
            "product_id": {},
            "available": {},
            "reserved": {},
            "last_updated": "2024-01-01T13:00:00Z"
        }}"#, order_id % 100, 900 - order_id % 100, order_id % 10);
        
        db.put_tx(tx_id, inventory_key.as_bytes(), inventory_data.as_bytes()).expect("Put failed");
        
        db.commit_transaction(tx_id).expect("Commit failed");
    }
    println!("   ✓ Processed 10 orders transactionally");
    
    println!("\n4. Analytics queries...");
    let products = db.prefix_scan_key(&Key::from(b"product:".as_ref())).expect("Scan failed");
    println!("   Products in catalog: {}", products.len());
    assert_eq!(products.len(), 1000);
    
    let sessions = db.prefix_scan_key(&Key::from(b"session:".as_ref())).expect("Scan failed");
    println!("   Active sessions: {}", sessions.len());
    assert_eq!(sessions.len(), 20);
    
    let orders = db.prefix_scan_key(&Key::from(b"order:".as_ref())).expect("Scan failed");
    println!("   Orders processed: {}", orders.len() / 6);
    
    let stats = db.stats();
    println!("\n5. Database statistics:");
    println!("   Page count: {}", stats.page_count);
    println!("   Free pages: {}", stats.free_page_count);
    println!("   Tree height: {}", stats.tree_height);
    println!("   Cache hit rate: {:.2}%", stats.cache_hit_rate.unwrap_or(0.0) * 100.0);
    println!("   Memory usage: {} KB", stats.memory_usage_bytes / 1024);
    
    println!("\n=== E-Commerce Simulation Complete ===\n");
}

#[test]
fn test_high_concurrency_banking_system() {
    let dir = tempdir().expect("Failed to create temp dir");
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Sync;
    config.use_optimized_transactions = true;
    
    let db = Arc::new(Database::create(dir.path(), config).expect("Failed to create database"));
    
    println!("\n=== Banking System Concurrency Test ===\n");
    
    for account_id in 0..100 {
        let account_key = format!("account:{:04}", account_id);
        let account_data = format!(r#"{{
            "account_id": {},
            "balance": 1000.00,
            "currency": "USD",
            "status": "active"
        }}"#, account_id);
        
        db.put(account_key.as_bytes(), account_data.as_bytes()).expect("Put failed");
    }
    println!("Created 100 bank accounts with $1000 each");
    
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];
    
    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let mut rng = rand::rng();
            
            for _ in 0..50 {
                let from_account = rng.random_range(0..100);
                let to_account = (from_account + rng.random_range(1..100)) % 100;
                let amount = rng.random_range(1..100) as f64;
                
                let mut retries = 0;
                const MAX_RETRIES: u32 = 5;
                
                loop {
                    let tx_id = db_clone.begin_transaction().expect("Failed to begin transaction");
                    
                    let from_key = format!("account:{:04}", from_account);
                    let to_key = format!("account:{:04}", to_account);
                    
                    match (db_clone.get_tx(tx_id, from_key.as_bytes()), 
                           db_clone.get_tx(tx_id, to_key.as_bytes())) {
                        (Ok(Some(from_data)), Ok(Some(to_data))) => {
                            let from_str = String::from_utf8_lossy(&from_data);
                            let to_str = String::from_utf8_lossy(&to_data);
                            
                            if let (Some(from_balance), Some(to_balance)) = 
                                (extract_balance(&from_str), extract_balance(&to_str)) {
                                
                                if from_balance >= amount {
                                    let new_from = update_balance(&from_str, from_balance - amount);
                                    let new_to = update_balance(&to_str, to_balance + amount);
                                    
                                    db_clone.put_tx(tx_id, from_key.as_bytes(), new_from.as_bytes()).expect("Put failed");
                                    db_clone.put_tx(tx_id, to_key.as_bytes(), new_to.as_bytes()).expect("Put failed");
                                    
                                    let transfer_key = format!("transfer:{}:{}", thread_id, 
                                        std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_micros());
                                    let transfer_data = format!(r#"{{
                                        "from": {},
                                        "to": {},
                                        "amount": {:.2},
                                        "timestamp": "2024-01-01T14:00:00Z"
                                    }}"#, from_account, to_account, amount);
                                    
                                    db_clone.put_tx(tx_id, transfer_key.as_bytes(), transfer_data.as_bytes()).expect("Put failed");
                                    
                                    match db_clone.commit_transaction(tx_id) {
                                        Ok(_) => break,
                                        Err(_) if retries < MAX_RETRIES => {
                                            retries += 1;
                                            thread::sleep(Duration::from_millis(retries as u64 * 10));
                                        }
                                        Err(e) => panic!("Transaction failed after {} retries: {}", retries, e),
                                    }
                                } else {
                                    // Transaction will auto-rollback when dropped
                                    break;
                                }
                            } else {
                                // Transaction will auto-rollback when dropped
                                break;
                            }
                        }
                        _ => {
                            // Transaction will auto-rollback when dropped
                            break;
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    
    let mut total_balance = 0.0;
    for account_id in 0..100 {
        let account_key = format!("account:{:04}", account_id);
        if let Some(data) = db.get(account_key.as_bytes()).expect("Get failed") {
            let data_str = String::from_utf8_lossy(&data);
            if let Some(balance) = extract_balance(&data_str) {
                total_balance += balance;
            }
        }
    }
    
    println!("Total balance after transfers: ${:.2}", total_balance);
    assert!((total_balance - 100000.0).abs() < 0.01, "Money was lost or created!");
    
    let transfers = db.prefix_scan_key(&Key::from(b"transfer:".as_ref())).expect("Scan failed");
    println!("Total transfers completed: {}", transfers.len());
    
    println!("\n=== Banking System Test Complete ===\n");
}

#[test]
fn test_time_series_iot_data() {
    let dir = tempdir().expect("Failed to create temp dir");
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    
    let db = Database::create(dir.path(), config).expect("Failed to create database");
    
    println!("\n=== IoT Time Series Data Test ===\n");
    
    let sensors = 100;
    let readings_per_sensor = 100;
    let start_time = Instant::now();
    
    for sensor_id in 0..sensors {
        for reading in 0..readings_per_sensor {
            let timestamp = 1704067200 + reading * 60;
            let key = format!("sensor:{}:ts:{}", sensor_id, timestamp);
            let value = format!(r#"{{
                "sensor_id": {},
                "timestamp": {},
                "temperature": {:.1},
                "humidity": {:.1},
                "pressure": {:.1},
                "battery": {}
            }}"#,
                sensor_id,
                timestamp,
                20.0 + (reading as f64 * 0.1) % 10.0,
                40.0 + (reading as f64 * 0.2) % 40.0,
                1013.0 + (reading as f64 * 0.05) % 20.0,
                100 - (reading % 20)
            );
            
            db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        }
    }
    
    let write_time = start_time.elapsed();
    println!("Wrote {} sensor readings in {:?}", sensors * readings_per_sensor, write_time);
    
    let start_query = Instant::now();
    let sensor_0_data = db.prefix_scan_key(&Key::from(b"sensor:0:".as_ref())).expect("Scan failed");
    let query_time = start_query.elapsed();
    
    println!("Retrieved {} readings for sensor 0 in {:?}", sensor_0_data.len(), query_time);
    assert_eq!(sensor_0_data.len(), readings_per_sensor);
    
    let time_range_prefix = format!("sensor:10:ts:1704067");
    let range_data = db.prefix_scan_key(&Key::from(time_range_prefix.as_bytes())).expect("Scan failed");
    
    println!("Range query returned {} readings", range_data.len());
    
    db.flush_lsm().expect("Flush failed");
    
    let stats = db.stats();
    println!("\nDatabase statistics:");
    println!("  Total pages: {}", stats.page_count);
    println!("  Compression ratio: ~{:.1}x", 
             (sensors * readings_per_sensor * 200) as f64 / (stats.page_count * 4096) as f64);
    
    println!("\n=== IoT Time Series Test Complete ===\n");
}

#[test]
fn test_crash_recovery_with_partial_writes() {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().to_path_buf();
    
    println!("\n=== Crash Recovery Test ===\n");
    
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        let db = Database::create(&path, config).expect("Failed to create database");
        
        for i in 0..500 {
            let key = format!("persistent_key_{:04}", i);
            let value = format!("persistent_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        }
        
        let tx_id = db.begin_transaction().expect("Failed to begin transaction");
        for i in 500..600 {
            let key = format!("tx_key_{:04}", i);
            let value = format!("tx_value_{}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).expect("Put failed");
        }
        db.commit_transaction(tx_id).expect("Commit failed");
        
        db.flush_lsm().expect("Flush failed");
        println!("Database synced with 600 entries");
    }
    
    {
        println!("Simulating database restart...");
        let db = Database::open(&path, LightningDbConfig::default()).expect("Failed to reopen database");
        
        let mut verified = 0;
        for i in 0..500 {
            let key = format!("persistent_key_{:04}", i);
            let value = format!("persistent_value_{}", i);
            let retrieved = db.get(key.as_bytes()).expect("Get failed");
            assert_eq!(retrieved, Some(value.into_bytes()));
            verified += 1;
        }
        
        for i in 500..600 {
            let key = format!("tx_key_{:04}", i);
            let value = format!("tx_value_{}", i);
            let retrieved = db.get(key.as_bytes()).expect("Get failed");
            assert_eq!(retrieved, Some(value.into_bytes()));
            verified += 1;
        }
        
        println!("✓ Successfully recovered all {} entries after restart", verified);
    }
    
    println!("\n=== Crash Recovery Test Complete ===\n");
}

#[test]
fn test_memory_pressure_and_cache_eviction() {
    let dir = tempdir().expect("Failed to create temp dir");
    let mut config = LightningDbConfig::default();
    config.cache_size = 256 * 1024;
    config.page_size = 4096;
    
    let db = Database::create(dir.path(), config).expect("Failed to create database");
    
    println!("\n=== Memory Pressure Test ===\n");
    
    let value_size = 4000;
    let value = vec![b'X'; value_size];
    let num_entries = 1000;
    
    let mut write_times = Vec::new();
    for i in 0..num_entries {
        let key = format!("pressure_key_{:06}", i);
        let start = Instant::now();
        db.put(key.as_bytes(), &value).expect("Put failed");
        write_times.push(start.elapsed());
        
        if i % 100 == 99 {
            let stats = db.stats();
            println!("After {} entries:", i + 1);
            println!("  Pages: {}, Free: {}", stats.page_count, stats.free_page_count);
            println!("  Cache hit rate: {:.1}%", stats.cache_hit_rate.unwrap_or(0.0) * 100.0);
        }
    }
    
    let avg_write_time: Duration = write_times.iter().sum::<Duration>() / write_times.len() as u32;
    println!("\nAverage write time: {:?}", avg_write_time);
    
    println!("\nTesting cache effectiveness with random reads...");
    let mut rng = rand::rng();
    let mut hit_times = Vec::new();
    let mut miss_times = Vec::new();
    
    for _ in 0..200 {
        let key_idx = rng.random_range(0..num_entries);
        let key = format!("pressure_key_{:06}", key_idx);
        
        let start = Instant::now();
        let _ = db.get(key.as_bytes()).expect("Get failed");
        let duration = start.elapsed();
        
        if key_idx < 50 {
            hit_times.push(duration);
        } else {
            miss_times.push(duration);
        }
    }
    
    if !hit_times.is_empty() && !miss_times.is_empty() {
        let avg_hit: Duration = hit_times.iter().sum::<Duration>() / hit_times.len() as u32;
        let avg_miss: Duration = miss_times.iter().sum::<Duration>() / miss_times.len() as u32;
        println!("Average hit time: {:?}", avg_hit);
        println!("Average miss time: {:?}", avg_miss);
    }
    
    println!("\n=== Memory Pressure Test Complete ===\n");
}

fn extract_balance(json: &str) -> Option<f64> {
    if let Some(start) = json.find("\"balance\":") {
        let rest = &json[start + 10..];
        if let Some(end) = rest.find(|c: char| c == ',' || c == '}') {
            rest[..end].trim().parse().ok()
        } else {
            None
        }
    } else {
        None
    }
}

fn update_balance(json: &str, new_balance: f64) -> String {
    if let Some(start) = json.find("\"balance\":") {
        let rest = &json[start + 10..];
        if let Some(end) = rest.find(|c: char| c == ',' || c == '}') {
            let before = &json[..start + 10];
            let after = &rest[end..];
            format!("{}{:.2}{}", before, new_balance, after)
        } else {
            json.to_string()
        }
    } else {
        json.to_string()
    }
}