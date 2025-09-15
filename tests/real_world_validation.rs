use lightning_db::{Database, LightningDbConfig, WriteBatch};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn test_ecommerce_workload() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    let mut handles = vec![];

    let db_clone = Arc::clone(&db);
    handles.push(thread::spawn(move || {
        for user_id in 0..100 {
            let key = format!("user:{}", user_id);
            let value = format!(r#"{{"id":{},"name":"User{}","email":"user{}@test.com"}}"#, user_id, user_id, user_id);
            db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }));

    let db_clone = Arc::clone(&db);
    handles.push(thread::spawn(move || {
        for product_id in 0..500 {
            let key = format!("product:{}", product_id);
            let value = format!(r#"{{"id":{},"name":"Product{}","price":{}}}"#, product_id, product_id, product_id * 10);
            db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }));

    let db_clone = Arc::clone(&db);
    handles.push(thread::spawn(move || {
        for order_id in 0..200 {
            let tx_id = db_clone.begin_transaction().unwrap();

            let order_key = format!("order:{}", order_id);
            let order_value = format!(r#"{{"id":{},"user_id":{},"total":{}}}"#, order_id, order_id % 100, order_id * 50);
            db_clone.put_tx(tx_id, order_key.as_bytes(), order_value.as_bytes()).unwrap();

            for item in 0..3 {
                let item_key = format!("order_item:{}:{}", order_id, item);
                let item_value = format!(r#"{{"order_id":{},"product_id":{},"quantity":{}}}"#, order_id, item * 10, item + 1);
                db_clone.put_tx(tx_id, item_key.as_bytes(), item_value.as_bytes()).unwrap();
            }

            db_clone.commit_transaction(tx_id).unwrap();
        }
    }));

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(db.get(b"user:0").unwrap().is_some(), true);
    assert_eq!(db.get(b"product:0").unwrap().is_some(), true);
    assert_eq!(db.get(b"order:0").unwrap().is_some(), true);

    let user_count = db.range(Some(b"user:"), Some(b"user:~")).unwrap().len();
    assert_eq!(user_count, 100);

    let product_count = db.range(Some(b"product:"), Some(b"product:~")).unwrap().len();
    assert_eq!(product_count, 500);
}

#[test]
fn test_time_series_workload() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    let base_time = 1700000000u64;
    for sensor_id in 0..10 {
        for hour in 0..24 {
            for minute in 0..60 {
                let timestamp = base_time + (hour * 3600) + (minute * 60);
                let key = format!("sensor:{}:{}", sensor_id, timestamp);
                let value = format!(r#"{{"sensor_id":{},"timestamp":{},"temperature":{},"humidity":{}}}"#,
                    sensor_id, timestamp, 20 + (minute % 10), 50 + (minute % 20));
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }
    }

    let sensor_0_start = format!("sensor:0:{}", base_time);
    let sensor_0_end = format!("sensor:0:{}", base_time + 86400);
    let readings = db.range(Some(sensor_0_start.as_bytes()), Some(sensor_0_end.as_bytes())).unwrap();
    assert_eq!(readings.len(), 1440);
}

#[test]
fn test_financial_transactions() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    for account_id in 0..100 {
        let key = format!("account:{}", account_id);
        let initial_balance = 10000;
        db.put(key.as_bytes(), initial_balance.to_string().as_bytes()).unwrap();
    }

    let mut handles = vec![];
    for thread_id in 0..5 {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..20 {
                let from_account = (thread_id * 20 + i) % 100;
                let to_account = (from_account + 1) % 100;
                let amount = 100;

                let tx_id = db_clone.begin_transaction().unwrap();

                let from_key = format!("account:{}", from_account);
                let from_balance_bytes = db_clone.get_tx(tx_id, from_key.as_bytes()).unwrap().unwrap();
                let from_balance: i32 = String::from_utf8(from_balance_bytes).unwrap().parse().unwrap();

                let to_key = format!("account:{}", to_account);
                let to_balance_bytes = db_clone.get_tx(tx_id, to_key.as_bytes()).unwrap().unwrap();
                let to_balance: i32 = String::from_utf8(to_balance_bytes).unwrap().parse().unwrap();

                if from_balance >= amount {
                    let new_from_balance = from_balance - amount;
                    let new_to_balance = to_balance + amount;

                    db_clone.put_tx(tx_id, from_key.as_bytes(), new_from_balance.to_string().as_bytes()).unwrap();
                    db_clone.put_tx(tx_id, to_key.as_bytes(), new_to_balance.to_string().as_bytes()).unwrap();

                    let transfer_key = format!("transfer:{}:{}:{}", from_account, to_account, i);
                    let transfer_value = format!(r#"{{"from":{},"to":{},"amount":{}}}"#, from_account, to_account, amount);
                    db_clone.put_tx(tx_id, transfer_key.as_bytes(), transfer_value.as_bytes()).unwrap();

                    db_clone.commit_transaction(tx_id).unwrap();
                } else {
                    db_clone.abort_transaction(tx_id).unwrap();
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let mut total_balance = 0i32;
    for account_id in 0..100 {
        let key = format!("account:{}", account_id);
        let balance_bytes = db.get(key.as_bytes()).unwrap().unwrap();
        let balance: i32 = String::from_utf8(balance_bytes).unwrap().parse().unwrap();
        total_balance += balance;
    }

    assert_eq!(total_balance, 1000000);
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
fn test_crash_recovery_simulation() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path, LightningDbConfig::default()).unwrap();

        for i in 0..100 {
            let key = format!("persistent_{:03}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        db.sync().unwrap();

        let tx_id = db.begin_transaction().unwrap();
        for i in 100..150 {
            let key = format!("transient_{:03}", i);
            let value = format!("value_{}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();

        for i in 0..100 {
            let key = format!("persistent_{:03}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(format!("value_{}", i).into_bytes()));
        }

        for i in 100..150 {
            let key = format!("transient_{:03}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert_eq!(result, None);
        }
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