use lightning_db::{Database, LightningDbConfig, WriteBatch};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_e_commerce_workload() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = LightningDbConfig {
        cache_size: 4 * 1024 * 1024, // 4MB
        compression_enabled: true,
        ..Default::default()
    };

    let db = Arc::new(Database::create(temp_dir.path(), config).expect("Failed to create database"));

    // Simulate product catalog
    for i in 0..100 {
        let key = format!("product:{}", i);
        let value = format!(r#"{{"id":{},"name":"Product {}","price":{}.99,"stock":{}}}"#,
                           i, i, i * 10, 100 - i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Failed to put product");
    }

    // Simulate user sessions
    for i in 0..50 {
        let key = format!("session:{}", i);
        let value = format!(r#"{{"user_id":{},"cart":[],"last_active":"2024-01-01T00:00:00Z"}}"#, i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Failed to put session");
    }

    // Test concurrent read/write operations
    let mut handles = vec![];

    // Reader threads
    for _thread_id in 0..3 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..50 {
                let key = format!("product:{}", i % 100);
                match db_clone.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        assert!(!value.is_empty());
                    }
                    Ok(None) => panic!("Product {} not found", i % 100),
                    Err(e) => panic!("Error reading product: {}", e),
                }
                thread::sleep(Duration::from_micros(100));
            }
        });
        handles.push(handle);
    }

    // Writer thread for cart updates
    let db_clone = Arc::clone(&db);
    let handle = thread::spawn(move || {
        for i in 0..20 {
            let mut batch = WriteBatch::new();
            let _session_key = format!("session:{}", i);
            let cart_key = format!("cart:user:{}", i);
            let cart_value = format!(r#"{{"items":[{{"product_id":{},"quantity":2}}],"total":{}.98}}"#,
                                    i, i * 20);

            batch.put(cart_key.into_bytes(), cart_value.into_bytes()).expect("Failed to add to batch");
            db_clone.write_batch(&batch).expect("Failed to write batch");
            thread::sleep(Duration::from_millis(5));
        }
    });
    handles.push(handle);

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify data integrity
    for i in 0..100 {
        let key = format!("product:{}", i);
        let value = db.get(key.as_bytes()).expect("Failed to get product");
        assert!(value.is_some(), "Product {} should exist", i);
    }

    // Test range scan for analytics
    let products_iter = db.scan(Some(b"product:"), Some(b"product:~")).expect("Failed to scan");
    let product_count = products_iter.count();
    assert_eq!(product_count, 100, "Should have 100 products");

    println!("✓ E-commerce workload test passed");
}

#[test]
fn test_time_series_workload() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = LightningDbConfig {
        cache_size: 2 * 1024 * 1024, // 2MB
        compression_enabled: true,
        ..Default::default()
    };

    let db = Database::create(temp_dir.path(), config).expect("Failed to create database");

    // Insert time series data
    let start_time = Instant::now();
    let mut batch = WriteBatch::new();

    for hour in 0..24 {
        for minute in 0..60 {
            for sensor_id in 0..5 {
                let timestamp = format!("2024-01-01T{:02}:{:02}:00", hour, minute);
                let key = format!("sensor:{}:{}", sensor_id, timestamp);
                let value = format!(r#"{{"temperature":{}.{},"humidity":{}}}"#,
                                  20 + (sensor_id * 2), minute % 10, 40 + (minute % 20));
                batch.put(key.into_bytes(), value.into_bytes()).expect("Failed to add to batch");

                // Flush batch periodically
                if batch.len() >= 100 {
                    db.write_batch(&batch).expect("Failed to write batch");
                    batch = WriteBatch::new();
                }
            }
        }
    }

    // Write remaining batch
    if batch.len() > 0 {
        db.write_batch(&batch).expect("Failed to write batch");
    }

    let insert_duration = start_time.elapsed();
    println!("Inserted {} time series records in {:?}", 24 * 60 * 5, insert_duration);

    // Query time range for sensor 0
    let start_key = b"sensor:0:2024-01-01T00:00:00";
    let end_key = b"sensor:0:2024-01-01T12:00:00";

    let range_iter = db.scan(Some(start_key), Some(end_key)).expect("Failed to scan range");
    let range_count = range_iter.count();
    assert_eq!(range_count, 12 * 60, "Should have 12 hours of data for sensor 0");

    // Test aggregation query pattern
    let mut total_records = 0;
    for sensor_id in 0..5 {
        let prefix = format!("sensor:{}:", sensor_id);
        let iter = db.scan(Some(prefix.as_bytes()), None).expect("Failed to scan");
        let count = iter.take_while(|result| {
            match result {
                Ok((k, _)) => k.starts_with(prefix.as_bytes()),
                Err(_) => false,
            }
        }).count();
        total_records += count;
        assert_eq!(count, 24 * 60, "Each sensor should have 24*60 records");
    }
    assert_eq!(total_records, 24 * 60 * 5, "Total records mismatch");

    println!("✓ Time series workload test passed");
}

#[test]
fn test_social_media_workload() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();

    let db = Arc::new(Database::create(temp_dir.path(), config).expect("Failed to create database"));

    // Create users
    for user_id in 0..100 {
        let user_key = format!("user:{}", user_id);
        let user_value = format!(r#"{{"id":{},"username":"user{}","followers":[],"following":[]}}"#,
                               user_id, user_id);
        db.put(user_key.as_bytes(), user_value.as_bytes()).expect("Failed to create user");
    }

    // Create posts with timestamps
    for user_id in 0..100 {
        for post_num in 0..5 {
            let timestamp = format!("2024-01-{:02}T12:00:00", post_num + 1);
            let post_key = format!("post:{}:{}:{}", user_id, timestamp, post_num);
            let post_value = format!(r#"{{"author":{},"content":"Post {} from user {}","likes":0,"timestamp":"{}"}}"#,
                                   user_id, post_num, user_id, timestamp);
            db.put(post_key.as_bytes(), post_value.as_bytes()).expect("Failed to create post");

            // Add to user's timeline
            let timeline_key = format!("timeline:{}:{}", user_id, timestamp);
            db.put(timeline_key.as_bytes(), post_key.as_bytes()).expect("Failed to update timeline");
        }
    }

    // Simulate concurrent likes
    let mut handles = vec![];
    for thread_id in 0..5 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..20 {
                let user_id = thread_id * 20 + i;
                let post_id = (user_id + 1) % 100;
                let like_key = format!("like:{}:{}:{}", post_id, 0, user_id);
                let like_value = format!(r#"{{"user":{},"post":"post:{}:2024-01-01T12:00:00:0","timestamp":"2024-01-01T13:00:00"}}"#,
                                       user_id, post_id);
                db_clone.put(like_key.as_bytes(), like_value.as_bytes()).expect("Failed to add like");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify timeline queries work
    for user_id in 0..10 {
        let timeline_prefix = format!("timeline:{}:", user_id);
        let iter = db.scan(Some(timeline_prefix.as_bytes()), None).expect("Failed to scan timeline");
        let posts: Vec<_> = iter
            .take_while(|result| {
                match result {
                    Ok((k, _)) => k.starts_with(timeline_prefix.as_bytes()),
                    Err(_) => false,
                }
            })
            .collect();
        assert_eq!(posts.len(), 5, "User {} should have 5 posts in timeline", user_id);
    }

    // Count total likes
    let likes_iter = db.scan(Some(b"like:"), Some(b"like:~")).expect("Failed to scan likes");
    let total_likes = likes_iter.count();
    assert_eq!(total_likes, 100, "Should have 100 likes");

    println!("✓ Social media workload test passed");
}

#[test]
fn test_crash_recovery() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().to_path_buf();

    // Phase 1: Write data and close
    {
        let db = Database::create(&db_path, LightningDbConfig::default())
            .expect("Failed to create database");

        // Write test data
        for i in 0..100 {
            let key = format!("key:{:04}", i);
            let value = format!("value:{}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Failed to put");
        }

        // Database drops here, simulating crash
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::open(&db_path, LightningDbConfig::default())
            .expect("Failed to reopen database");

        // Verify all data is present
        for i in 0..100 {
            let key = format!("key:{:04}", i);
            let value = db.get(key.as_bytes()).expect("Failed to get");
            assert!(value.is_some(), "Key {} should exist after recovery", i);
            let expected = format!("value:{}", i);
            assert_eq!(value.unwrap(), expected.as_bytes());
        }

        // Add more data after recovery
        for i in 100..150 {
            let key = format!("key:{:04}", i);
            let value = format!("value:{}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Failed to put after recovery");
        }
    }

    // Phase 3: Final verification
    {
        let db = Database::open(&db_path, LightningDbConfig::default())
            .expect("Failed to reopen database again");

        // Count all entries
        let iter = db.scan(None, None).expect("Failed to scan all");
        let total_count = iter.count();
        assert_eq!(total_count, 150, "Should have 150 entries after recovery");
    }

    println!("✓ Crash recovery test passed");
}