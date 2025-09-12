use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_ecommerce_scenario() {
    let dir = tempdir().unwrap();
    let db = Database::create(
        dir.path(),
        LightningDbConfig {
            cache_size: 50 * 1024 * 1024,
            compression_enabled: true,
            ..Default::default()
        },
    )
    .unwrap();

    // Simulate product catalog
    for i in 0..1000 {
        let product_key = format!("product:{}", i);
        let product_data = format!(
            r#"{{"id":{},"name":"Product {}","price":{},"stock":{},"category":"electronics"}}"#,
            i,
            i,
            19.99 + (i as f64 * 0.1),
            100 - (i % 50)
        );
        db.put(product_key.as_bytes(), product_data.as_bytes())
            .unwrap();
    }

    // Simulate user sessions
    for user_id in 0..100 {
        let session_key = format!("session:user:{}", user_id);
        let session_data = format!(
            r#"{{"user_id":{},"cart":[],"last_activity":"2024-01-01T12:00:00Z"}}"#,
            user_id
        );
        db.put(session_key.as_bytes(), session_data.as_bytes())
            .unwrap();
    }

    // Simulate orders with transactions
    for order_id in 0..50 {
        let tx_id = db.begin_transaction().unwrap();

        // Order header
        let order_key = format!("order:{}", order_id);
        let order_data = format!(
            r#"{{"order_id":{},"user_id":{},"total":199.99,"status":"pending"}}"#,
            order_id,
            order_id % 100
        );
        db.put_tx(tx_id, order_key.as_bytes(), order_data.as_bytes())
            .unwrap();

        // Order items
        for item in 0..3 {
            let item_key = format!("order:{}:item:{}", order_id, item);
            let item_data = format!(
                r#"{{"product_id":{},"quantity":{},"price":49.99}}"#,
                item * 10,
                item + 1
            );
            db.put_tx(tx_id, item_key.as_bytes(), item_data.as_bytes())
                .unwrap();
        }

        // Update inventory
        let product_key = format!("product:{}", order_id % 10);
        if let Some(product_data) = db.get_tx(tx_id, product_key.as_bytes()).unwrap() {
            let updated_data = String::from_utf8(product_data)
                .unwrap()
                .replace("\"stock\":100", "\"stock\":95");
            db.put_tx(tx_id, product_key.as_bytes(), updated_data.as_bytes())
                .unwrap();
        }

        db.commit_transaction(tx_id).unwrap();
    }

    // Verify data integrity
    assert_eq!(db.get(b"product:0").unwrap().is_some(), true);
    assert_eq!(db.get(b"order:0").unwrap().is_some(), true);
    assert_eq!(db.get(b"session:user:0").unwrap().is_some(), true);

    // Test range queries
    let products_scan = db.scan_prefix(b"product:").unwrap();
    assert!(products_scan.count() >= 1000);

    let orders_scan = db.scan_prefix(b"order:").unwrap();
    assert!(orders_scan.count() >= 50);
}

#[test]
fn test_time_series_data() {
    let dir = tempdir().unwrap();
    let db = Database::create(
        dir.path(),
        LightningDbConfig {
            compression_enabled: true,
            compression_type: 2, // LZ4 for time series
            ..Default::default()
        },
    )
    .unwrap();

    let start = Instant::now();
    let mut total_written = 0;

    // Simulate IoT sensor data
    for device_id in 0..10 {
        for hour in 0..24 {
            for minute in 0..60 {
                let timestamp = format!("2024-01-01T{:02}:{:02}:00", hour, minute);
                let key = format!("sensor:{}:{}", device_id, timestamp);
                let data = format!(
                    r#"{{"device_id":{},"timestamp":"{}","temperature":{},"humidity":{},"pressure":{}}}"#,
                    device_id,
                    timestamp,
                    20.0 + (minute as f64 * 0.1),
                    50.0 + (hour as f64 * 0.5),
                    1013.25 + (device_id as f64 * 0.1)
                );
                db.put(key.as_bytes(), data.as_bytes()).unwrap();
                total_written += 1;
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = total_written as f64 / elapsed.as_secs_f64();
    println!(
        "Time series write throughput: {:.0} ops/sec ({} total in {:?})",
        throughput, total_written, elapsed
    );

    // Query recent data
    let recent_data = db.scan_prefix(b"sensor:0:2024-01-01T23:").unwrap();
    assert!(recent_data.count() >= 60);

    // Aggregate query simulation
    let device_data = db.scan_prefix(b"sensor:5:").unwrap();
    let count = device_data.count();
    assert_eq!(count, 24 * 60);
}

#[test]
fn test_concurrent_operations() {
    let dir = tempdir().unwrap();
    let db = Arc::new(
        Database::create(
            dir.path(),
            LightningDbConfig {
                max_active_transactions: 100,
                ..Default::default()
            },
        )
        .unwrap(),
    );

    let num_threads = 10;
    let ops_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                barrier.wait();

                for op in 0..ops_per_thread {
                    let key = format!("thread:{}:op:{}", thread_id, op);
                    let value = format!("data_{}_{}", thread_id, op);

                    // Mix of operations
                    match op % 4 {
                        0 => {
                            // Simple put
                            db.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                        1 => {
                            // Get
                            db.get(key.as_bytes()).unwrap();
                        }
                        2 => {
                            // Transaction
                            let tx_id = db.begin_transaction().unwrap();
                            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())
                                .unwrap();
                            db.commit_transaction(tx_id).unwrap();
                        }
                        3 => {
                            // Delete
                            db.delete(key.as_bytes()).unwrap();
                        }
                        _ => unreachable!(),
                    }
                }
            })
        })
        .collect();

    let start = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }
    let elapsed = start.elapsed();

    let total_ops = num_threads * ops_per_thread;
    let throughput = total_ops as f64 / elapsed.as_secs_f64();
    println!(
        "Concurrent ops throughput: {:.0} ops/sec ({} total in {:?})",
        throughput, total_ops, elapsed
    );
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
                wal_sync_mode: lightning_db::WalSyncMode::Sync,
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
        db.put_tx(tx_id, b"uncommitted", b"should_not_exist").unwrap();
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
fn test_large_value_handling() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Test various value sizes
    let sizes = vec![
        1,           // 1 byte
        1024,        // 1 KB
        10 * 1024,   // 10 KB
        100 * 1024,  // 100 KB
        1024 * 1024, // 1 MB
    ];

    for size in sizes {
        let key = format!("large:{}", size);
        let value = vec![b'X'; size];

        let start = Instant::now();
        db.put(key.as_bytes(), &value).unwrap();
        let write_time = start.elapsed();

        let start = Instant::now();
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        let read_time = start.elapsed();

        assert_eq!(retrieved.len(), size);
        println!(
            "Size: {} bytes - Write: {:?}, Read: {:?}",
            size, write_time, read_time
        );
    }
}

#[test]
fn test_batch_operations() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Batch insert
    let mut batch = Vec::new();
    for i in 0..1000 {
        let key = format!("batch:{:04}", i);
        let value = format!("value_{}", i);
        batch.push((key.into_bytes(), value.into_bytes()));
    }

    let start = Instant::now();
    db.batch_put(&batch).unwrap();
    let elapsed = start.elapsed();
    let throughput = 1000.0 / elapsed.as_secs_f64();
    println!("Batch insert throughput: {:.0} ops/sec", throughput);

    // Batch get
    let keys: Vec<_> = (0..1000)
        .map(|i| format!("batch:{:04}", i).into_bytes())
        .collect();

    let start = Instant::now();
    let results = db.get_batch(&keys).unwrap();
    let elapsed = start.elapsed();
    let throughput = 1000.0 / elapsed.as_secs_f64();
    println!("Batch get throughput: {:.0} ops/sec", throughput);

    assert_eq!(results.len(), 1000);
    assert!(results.iter().all(|r| r.is_some()));

    // Batch delete
    let start = Instant::now();
    db.delete_batch(&keys).unwrap();
    let elapsed = start.elapsed();
    let throughput = 1000.0 / elapsed.as_secs_f64();
    println!("Batch delete throughput: {:.0} ops/sec", throughput);

    // Verify deletion
    let results = db.get_batch(&keys).unwrap();
    assert!(results.iter().all(|r| r.is_none()));
}