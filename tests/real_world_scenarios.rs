use lightning_db::{Database, Key, LightningDbConfig, WalSyncMode, WriteBatch};
use rand::Rng;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

mod common;
use common::{TestDatabase, FAST_TEST_SIZE, MEDIUM_TEST_SIZE};

#[test]
fn test_ecommerce_platform_simulation() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: true,
        cache_size: 2 * 1024 * 1024,
        use_optimized_transactions: true,
        ..Default::default()
    };

    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    println!("\n=== E-Commerce Platform Simulation ===\n");

    println!("1. Loading product catalog...");
    for product_id in 0..1000 {
        let product_key = format!("product:{:06}", product_id);
        let product_data = format!(
            r#"{{
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

        db.put(product_key.as_bytes(), product_data.as_bytes())
            .unwrap();
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
            let session_data = format!(
                r#"{{
                "user_id": {},
                "login_time": "2024-01-01T12:00:00Z",
                "ip": "192.168.1.{}",
                "cart_items": 0,
                "wishlist_items": 0
            }}"#,
                user_id, user_id
            );

            db_clone
                .put(session_key.as_bytes(), session_data.as_bytes())
                .unwrap();

            for item in 0..10 {
                let cart_key = format!("cart:user:{}:item:{}", user_id, item);
                let product_id = (user_id * 10 + item) % 1000;
                let cart_data = format!(
                    r#"{{
                    "product_id": {},
                    "quantity": {},
                    "price": {:.2}
                }}"#,
                    product_id,
                    1 + item % 3,
                    10.0 + product_id as f64 * 0.5
                );

                db_clone
                    .put(cart_key.as_bytes(), cart_data.as_bytes())
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    println!("   ✓ Created 20 concurrent user sessions with shopping carts");

    println!("\n3. Processing orders with transactions...");
    for order_id in 1000..1010 {
        let tx_id = db.begin_transaction().unwrap();

        let order_key = format!("order:{}", order_id);
        let order_data = format!(
            r#"{{
            "order_id": {},
            "user_id": {},
            "total": {:.2},
            "status": "processing",
            "created_at": "2024-01-01T13:00:00Z"
        }}"#,
            order_id,
            order_id % 20,
            100.0 + (order_id as f64 * 10.0)
        );

        db.put_tx(tx_id, order_key.as_bytes(), order_data.as_bytes())
            .unwrap();

        for item in 0..5 {
            let item_key = format!("order:{}:item:{}", order_id, item);
            let item_data = format!(
                r#"{{
                "product_id": {},
                "quantity": {},
                "price": {:.2}
            }}"#,
                item * 100,
                1 + item % 3,
                20.0 + item as f64 * 5.0
            );

            db.put_tx(tx_id, item_key.as_bytes(), item_data.as_bytes())
                .unwrap();
        }

        let inventory_key = format!("inventory:product:{}", order_id % 100);
        let inventory_data = format!(
            r#"{{
            "product_id": {},
            "available": {},
            "reserved": {},
            "last_updated": "2024-01-01T13:00:00Z"
        }}"#,
            order_id % 100,
            900 - order_id % 100,
            order_id % 10
        );

        db.put_tx(tx_id, inventory_key.as_bytes(), inventory_data.as_bytes())
            .unwrap();

        db.commit_transaction(tx_id).unwrap();
    }
    println!("   ✓ Processed 10 orders transactionally");

    println!("\n4. Analytics queries...");
    let products = db
        .prefix_scan_key(&Key::from(b"product:".as_ref()))
        .unwrap();
    println!("   Products in catalog: {}", products.len());
    assert_eq!(products.len(), 1000);

    let sessions = db
        .prefix_scan_key(&Key::from(b"session:".as_ref()))
        .unwrap();
    println!("   Active sessions: {}", sessions.len());
    assert_eq!(sessions.len(), 20);

    let orders = db.prefix_scan_key(&Key::from(b"order:".as_ref())).unwrap();
    println!("   Orders processed: {}", orders.len() / 6);

    let stats = db.stats();
    println!("\n5. Database statistics:");
    println!("   Page count: {}", stats.page_count);
    println!("   Free pages: {}", stats.free_page_count);
    println!("   Tree height: {}", stats.tree_height);
    println!(
        "   Cache hit rate: {:.2}%",
        stats.cache_hit_rate.unwrap_or(0.0) * 100.0
    );
    println!("   Memory usage: {} KB", stats.memory_usage_bytes / 1024);

    println!("\n=== E-Commerce Simulation Complete ===\n");
}

#[test]
fn test_high_concurrency_banking_system() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        wal_sync_mode: WalSyncMode::Sync,
        use_optimized_transactions: true,
        ..Default::default()
    };

    let db = Arc::new(Database::create(dir.path(), config).unwrap());

    println!("\n=== Banking System Concurrency Test ===\n");

    for account_id in 0..100 {
        let account_key = format!("account:{:04}", account_id);
        let account_data = format!(
            r#"{{
            "account_id": {},
            "balance": 1000.00,
            "currency": "USD",
            "status": "active"
        }}"#,
            account_id
        );

        db.put(account_key.as_bytes(), account_data.as_bytes())
            .unwrap();
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
                    let tx_id = db_clone.begin_transaction().unwrap();

                    let from_key = format!("account:{:04}", from_account);
                    let to_key = format!("account:{:04}", to_account);

                    match (
                        db_clone.get_tx(tx_id, from_key.as_bytes()),
                        db_clone.get_tx(tx_id, to_key.as_bytes()),
                    ) {
                        (Ok(Some(from_data)), Ok(Some(to_data))) => {
                            let from_str = String::from_utf8_lossy(&from_data);
                            let to_str = String::from_utf8_lossy(&to_data);

                            if let (Some(from_balance), Some(to_balance)) =
                                (extract_balance(&from_str), extract_balance(&to_str))
                            {
                                if from_balance >= amount {
                                    let new_from = update_balance(&from_str, from_balance - amount);
                                    let new_to = update_balance(&to_str, to_balance + amount);

                                    db_clone
                                        .put_tx(tx_id, from_key.as_bytes(), new_from.as_bytes())
                                        .unwrap();
                                    db_clone
                                        .put_tx(tx_id, to_key.as_bytes(), new_to.as_bytes())
                                        .unwrap();

                                    let transfer_key = format!(
                                        "transfer:{}:{}",
                                        thread_id,
                                        std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_micros()
                                    );
                                    let transfer_data = format!(
                                        r#"{{
                                        "from": {},
                                        "to": {},
                                        "amount": {:.2},
                                        "timestamp": "2024-01-01T14:00:00Z"
                                    }}"#,
                                        from_account, to_account, amount
                                    );

                                    db_clone
                                        .put_tx(
                                            tx_id,
                                            transfer_key.as_bytes(),
                                            transfer_data.as_bytes(),
                                        )
                                        .unwrap();

                                    match db_clone.commit_transaction(tx_id) {
                                        Ok(_) => break,
                                        Err(_) if retries < MAX_RETRIES => {
                                            retries += 1;
                                            thread::sleep(Duration::from_millis(
                                                retries as u64 * 10,
                                            ));
                                        }
                                        Err(e) => panic!(
                                            "Transaction failed after {} retries: {}",
                                            retries, e
                                        ),
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
        handle.join().unwrap();
    }

    let mut total_balance = 0.0;
    for account_id in 0..100 {
        let account_key = format!("account:{:04}", account_id);
        if let Some(data) = db.get(account_key.as_bytes()).unwrap() {
            let data_str = String::from_utf8_lossy(&data);
            if let Some(balance) = extract_balance(&data_str) {
                total_balance += balance;
            }
        }
    }

    println!("Total balance after transfers: ${:.2}", total_balance);
    assert!(
        (total_balance - 100000.0).abs() < 0.01,
        "Money was lost or created!"
    );

    let transfers = db
        .prefix_scan_key(&Key::from(b"transfer:".as_ref()))
        .unwrap();
    println!("Total transfers completed: {}", transfers.len());

    println!("\n=== Banking System Test Complete ===\n");
}

#[test]
fn test_time_series_iot_data() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        compression_enabled: true,
        ..Default::default()
    };

    let db = Database::create(dir.path(), config).unwrap();

    println!("\n=== IoT Time Series Data Test ===\n");

    let sensors = 100;
    let readings_per_sensor = 100;
    let start_time = Instant::now();

    for sensor_id in 0..sensors {
        for reading in 0..readings_per_sensor {
            let timestamp = 1704067200 + reading * 60;
            let key = format!("sensor:{}:ts:{}", sensor_id, timestamp);
            let value = format!(
                r#"{{
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

            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    let write_time = start_time.elapsed();
    println!(
        "Wrote {} sensor readings in {:?}",
        sensors * readings_per_sensor,
        write_time
    );

    let start_query = Instant::now();
    let sensor_0_data = db
        .prefix_scan_key(&Key::from(b"sensor:0:".as_ref()))
        .unwrap();
    let query_time = start_query.elapsed();

    println!(
        "Retrieved {} readings for sensor 0 in {:?}",
        sensor_0_data.len(),
        query_time
    );
    assert_eq!(sensor_0_data.len(), readings_per_sensor);

    let time_range_prefix = format!("sensor:10:ts:1704067");
    let range_data = db
        .prefix_scan_key(&Key::from(time_range_prefix.as_bytes()))
        .unwrap();

    println!("Range query returned {} readings", range_data.len());

    db.flush_lsm().unwrap();

    let stats = db.stats();
    println!("\nDatabase statistics:");
    println!("  Total pages: {}", stats.page_count);
    println!(
        "  Compression ratio: ~{:.1}x",
        (sensors * readings_per_sensor * 200) as f64 / (stats.page_count * 4096) as f64
    );

    println!("\n=== IoT Time Series Test Complete ===\n");
}

#[test]
fn test_crash_recovery_simulation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Write data with WAL enabled
    {
        let db = Database::create(
            &db_path,
            LightningDbConfig {
                use_unified_wal: true,
                wal_sync_mode: WalSyncMode::Sync,
                ..Default::default()
            },
        )
        .unwrap();

        // Write critical data
        for i in 0..100 {
            let key = format!("critical:{}", i);
            let value = format!("important_data_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Start a transaction but don't commit (simulate crash)
        let tx_id = db.begin_transaction().unwrap();
        db.put_tx(tx_id, b"uncommitted", b"should_not_exist")
            .unwrap();
        // Simulate crash by dropping db without commit
    }

    // Phase 2: Reopen and verify recovery
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

        // Verify committed data survived
        for i in 0..100 {
            let key = format!("critical:{}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some());
            let expected = format!("important_data_{}", i);
            assert_eq!(value.unwrap(), expected.as_bytes());
        }

        // Verify uncommitted data was not recovered
        assert_eq!(db.get(b"uncommitted").unwrap(), None);
    }
}

#[test]
fn test_crash_recovery_with_partial_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    println!("\n=== Crash Recovery Test ===\n");

    {
        let config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        let db = Database::create(&path, config).unwrap();

        for i in 0..500 {
            let key = format!("persistent_key_{:04}", i);
            let value = format!("persistent_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let tx_id = db.begin_transaction().unwrap();
        for i in 500..600 {
            let key = format!("tx_key_{:04}", i);
            let value = format!("tx_value_{}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.commit_transaction(tx_id).unwrap();

        db.flush_lsm().unwrap();
        println!("Database synced with 600 entries");
    }

    {
        println!("Simulating database restart...");
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();

        let mut verified = 0;
        for i in 0..500 {
            let key = format!("persistent_key_{:04}", i);
            let value = format!("persistent_value_{}", i);
            let retrieved = db.get(key.as_bytes()).unwrap();
            assert_eq!(retrieved, Some(value.into_bytes()));
            verified += 1;
        }

        for i in 500..600 {
            let key = format!("tx_key_{:04}", i);
            let value = format!("tx_value_{}", i);
            let retrieved = db.get(key.as_bytes()).unwrap();
            assert_eq!(retrieved, Some(value.into_bytes()));
            verified += 1;
        }

        println!(
            "✓ Successfully recovered all {} entries after restart",
            verified
        );
    }

    println!("\n=== Crash Recovery Test Complete ===\n");
}

#[test]
fn test_stress_crash_recovery_cycles() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Simulate multiple crash/recovery cycles
    for cycle in 0..5 {
        println!("Crash recovery cycle {}", cycle + 1);

        {
            let mut config = LightningDbConfig::default();
            config.wal_sync_mode = WalSyncMode::Sync;
            let db = Database::create(&path, config).unwrap();

            // Write data with specific patterns
            for i in 0..100 {
                let key = format!("crash_test_c{}_k{:03}", cycle, i);
                let value = format!("cycle_{}_value_{}_checksum_{:x}", cycle, i, i * cycle);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }

            // Simulate transaction in progress
            let tx_id = db.begin_transaction().unwrap();
            for i in 100..150 {
                let key = format!("crash_test_c{}_k{:03}", cycle, i);
                let value = format!("tx_value_{}_pending", i);
                db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            }

            if cycle % 2 == 0 {
                // Commit some transactions
                db.commit_transaction(tx_id).unwrap();
            }
            // Otherwise, simulate crash by dropping db without commit
        }

        // Reopen and verify
        {
            let db = Database::open(&path, LightningDbConfig::default()).unwrap();

            // Verify committed data from all cycles
            for c in 0..=cycle {
                for i in 0..100 {
                    let key = format!("crash_test_c{}_k{:03}", c, i);
                    let expected = format!("cycle_{}_value_{}_checksum_{:x}", c, i, i * c);
                    let result = db.get(key.as_bytes()).unwrap();
                    assert_eq!(
                        result,
                        Some(expected.into_bytes()),
                        "Data loss detected for key {} in cycle {}",
                        key,
                        c
                    );
                }

                // Verify transactional data
                if c % 2 == 0 {
                    for i in 100..150 {
                        let key = format!("crash_test_c{}_k{:03}", c, i);
                        let result = db.get(key.as_bytes()).unwrap();
                        assert!(
                            result.is_some(),
                            "Committed transaction data lost for key {}",
                            key
                        );
                    }
                } else {
                    // Uncommitted transactions should not be visible
                    for i in 100..150 {
                        let key = format!("crash_test_c{}_k{:03}", c, i);
                        let result = db.get(key.as_bytes()).unwrap();
                        assert!(
                            result.is_none(),
                            "Uncommitted transaction data persisted for key {}",
                            key
                        );
                    }
                }
            }
        }
    }

    println!("✓ All crash recovery cycles completed successfully");
}

#[test]
fn test_large_dataset_simulation() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    const BATCH_SIZE: usize = 1000;
    const NUM_BATCHES: usize = 10;

    println!("Testing large dataset simulation...");

    // Write phase with checksums
    for batch in 0..NUM_BATCHES {
        let start = Instant::now();

        for i in 0..BATCH_SIZE {
            let key = format!("large_{:03}_{:04}", batch, i);
            let checksum = calculate_checksum(batch, i);
            let value = format!(
                "batch_{}_item_{}_checksum_{:08x}_data_{}",
                batch,
                i,
                checksum,
                "x".repeat(500)
            );

            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        println!("  Batch {} written in {:?}", batch, start.elapsed());

        // Periodic flush
        if batch % 3 == 0 {
            db.flush_lsm().unwrap();
        }
    }

    // Verification phase
    println!("Verifying dataset integrity...");
    let mut errors = 0;

    for batch in 0..NUM_BATCHES {
        for i in 0..BATCH_SIZE {
            let key = format!("large_{:03}_{:04}", batch, i);

            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let value_str = String::from_utf8_lossy(&value);
                    let expected_checksum = calculate_checksum(batch, i);

                    if !value_str.contains(&format!("checksum_{:08x}", expected_checksum)) {
                        errors += 1;
                    }

                    if !value_str.starts_with(&format!("batch_{}_item_{}", batch, i)) {
                        errors += 1;
                    }
                }
                _ => {
                    errors += 1;
                }
            }
        }
    }

    let total_entries = NUM_BATCHES * BATCH_SIZE;
    println!("  Total entries: {}", total_entries);
    println!("  Errors found: {}", errors);

    assert_eq!(
        errors, 0,
        "Dataset integrity check failed with {} errors",
        errors
    );
    println!("✓ Large dataset integrity verified successfully");
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

fn calculate_checksum(batch: usize, item: usize) -> u32 {
    // Simple checksum calculation
    ((batch as u32) * 1000000 + (item as u32) * 1000) ^ 0xDEADBEEF
}

#[test]
fn test_batch_operations() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    let mut batch = WriteBatch::new();
    for i in 0..1000 {
        let key = format!("batch_key_{:04}", i);
        let value = format!("batch_value_{}", i);
        batch.put(key.into_bytes(), value.into_bytes()).unwrap();
    }
    db.write_batch(&batch).unwrap();

    for i in 0..1000 {
        let key = format!("batch_key_{:04}", i);
        let result = db.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(format!("batch_value_{}", i).into_bytes()));
    }

    let mut batch = WriteBatch::new();
    for i in 500..1000 {
        let key = format!("batch_key_{:04}", i);
        batch.delete(key.into_bytes()).unwrap();
    }
    db.write_batch(&batch).unwrap();

    for i in 0..500 {
        let key = format!("batch_key_{:04}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }

    for i in 500..1000 {
        let key = format!("batch_key_{:04}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_none());
    }
}

#[test]
fn test_performance_characteristics() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    let num_operations = 10000;
    let start = Instant::now();

    for i in 0..num_operations {
        let key = format!("perf_key_{:06}", i);
        let value = vec![0u8; 100];
        db.put(key.as_bytes(), &value).unwrap();
    }

    let write_duration = start.elapsed();
    let writes_per_sec = num_operations as f64 / write_duration.as_secs_f64();
    println!("Write throughput: {:.0} ops/sec", writes_per_sec);
    assert!(writes_per_sec > 1000.0);

    let start = Instant::now();

    for i in 0..num_operations {
        let key = format!("perf_key_{:06}", i);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let read_duration = start.elapsed();
    let reads_per_sec = num_operations as f64 / read_duration.as_secs_f64();
    println!("Read throughput: {:.0} ops/sec", reads_per_sec);
    assert!(reads_per_sec > 5000.0);

    let num_range_keys = 100;
    let start_key = format!("perf_key_{:06}", 1000);
    let end_key = format!("perf_key_{:06}", 1000 + num_range_keys);

    let start = Instant::now();
    let results = db.range(Some(start_key.as_bytes()), Some(end_key.as_bytes())).unwrap();
    let range_duration = start.elapsed();

    assert_eq!(results.len(), num_range_keys);
    assert!(range_duration < Duration::from_millis(100));
}

#[test]
fn test_concurrent_stress() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    let num_threads = 10;
    let ops_per_thread = 1000;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for op_id in 0..ops_per_thread {
                let key = format!("t{}:k{:04}", thread_id, op_id);
                let value = format!("thread_{}_op_{}", thread_id, op_id);

                match op_id % 4 {
                    0 => {
                        db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    1 => {
                        let _ = db_clone.get(key.as_bytes());
                    }
                    2 => {
                        let tx_id = db_clone.begin_transaction().unwrap();
                        db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
                        db_clone.commit_transaction(tx_id).unwrap();
                    }
                    3 => {
                        let _ = db_clone.delete(key.as_bytes());
                    }
                    _ => unreachable!(),
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let all_keys: Vec<_> = db.range(None, None).unwrap();
    assert!(all_keys.len() > 0);
}
