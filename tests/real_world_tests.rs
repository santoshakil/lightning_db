use lightning_db::{Database, LightningDbConfig, WriteBatch};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use rand::{Rng, thread_rng};

#[test]
fn test_e_commerce_workload() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("ecommerce_db");
    let db = Arc::new(Database::open(db_path, Default::default()).expect("Failed to open database"));

    // Product catalog
    for product_id in 0..100 {
        let key = format!("product:{}", product_id);
        let value = format!(r#"{{"id":{},"name":"Product {}","price":{}.99,"stock":{}}}"#,
                          product_id, product_id, product_id * 10, 100 - product_id);
        db.put(key.as_bytes(), value.as_bytes()).expect("Failed to add product");
    }

    // User sessions and carts
    let mut handles = vec![];

    for user_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            // Create session
            let session_key = format!("session:user:{}", user_id);
            let session_value = format!(r#"{{"user_id":{},"created_at":"2024-01-01T00:00:00Z"}}"#, user_id);
            db_clone.put(session_key.as_bytes(), session_value.as_bytes()).expect("Session creation failed");

            // Add to cart
            let mut batch = WriteBatch::new();
            for item in 0..5 {
                let cart_key = format!("cart:user:{}:item:{}", user_id, item);
                let cart_value = format!(r#"{{"product_id":{},"quantity":2}}"#, item * 10 + user_id);
                batch.put(cart_key.into_bytes(), cart_value.into_bytes()).expect("Cart batch failed");
            }
            db_clone.write_batch(&batch).expect("Cart update failed");

            // Simulate checkout
            let order_key = format!("order:user:{}", user_id);
            let order_value = format!(r#"{{"user_id":{},"total":299.99,"status":"completed"}}"#, user_id);
            db_clone.put(order_key.as_bytes(), order_value.as_bytes()).expect("Order creation failed");
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify data
    let products = db.scan(Some(b"product:"), Some(b"product:~")).expect("Product scan failed");
    assert_eq!(products.count(), 100);

    let orders = db.scan(Some(b"order:"), Some(b"order:~")).expect("Order scan failed");
    assert_eq!(orders.count(), 10);
}

#[test]
fn test_time_series_workload() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("timeseries_db");

    let config = LightningDbConfig {
        compression_enabled: true,
        write_batch_size: 1000,
        ..Default::default()
    };

    let db = Database::open(db_path, config).expect("Failed to open database");

    // Simulate sensor data collection
    let mut batch = WriteBatch::new();

    for hour in 0..24 {
        for minute in 0..60 {
            for sensor_id in 0..5 {
                let timestamp = format!("2024-01-01T{:02}:{:02}:00", hour, minute);
                let key = format!("sensor:{}:{}", sensor_id, timestamp);
                let temp = 20.0 + (sensor_id as f64 * 2.0) + (minute as f64 * 0.1);
                let humidity = 40.0 + (minute as f64 * 0.5);
                let value = format!(r#"{{"temperature":{},"humidity":{}}}"#, temp, humidity);

                batch.put(key.into_bytes(), value.into_bytes()).expect("Batch put failed");

                if batch.len() >= 100 {
                    db.write_batch(&batch).expect("Batch write failed");
                    batch = WriteBatch::new();
                }
            }
        }
    }

    if batch.len() > 0 {
        db.write_batch(&batch).expect("Final batch write failed");
    }

    // Query specific sensor data
    let sensor_0_data = db.scan(Some(b"sensor:0:"), Some(b"sensor:1:")).expect("Sensor scan failed");
    assert_eq!(sensor_0_data.count(), 24 * 60);

    // Query specific time range
    let morning_data = db.scan(
        Some(b"sensor:0:2024-01-01T06:"),
        Some(b"sensor:0:2024-01-01T12:")
    ).expect("Time range scan failed");
    assert!(morning_data.count() > 0);
}

#[test]
fn test_social_media_workload() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("social_db");
    let db = Arc::new(Database::open(db_path, Default::default()).expect("Failed to open database"));

    // Create users
    for user_id in 0..20 {
        let user_key = format!("user:{}", user_id);
        let user_value = format!(r#"{{"id":{},"username":"user{}","followers":{},"following":{}}}"#,
                                user_id, user_id, user_id * 10, user_id * 5);
        db.put(user_key.as_bytes(), user_value.as_bytes()).expect("User creation failed");
    }

    // Create posts
    for user_id in 0..20 {
        for post_id in 0..5 {
            let post_key = format!("post:user:{}:id:{}", user_id, post_id);
            let post_value = format!(r#"{{"user_id":{},"content":"Post {} by user {}","likes":0}}"#,
                                   user_id, post_id, user_id);
            db.put(post_key.as_bytes(), post_value.as_bytes()).expect("Post creation failed");

            // Add to timeline
            let timeline_key = format!("timeline:{}:{:03}:{}", user_id, user_id * 10 + post_id, post_id);
            db.put(timeline_key.as_bytes(), post_key.as_bytes()).expect("Timeline update failed");
        }
    }

    // Simulate likes
    let mut handles = vec![];
    for _ in 0..5 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let mut rng = thread_rng();
            for _ in 0..20 {
                let user_id = rng.gen_range(0..20);
                let post_id = rng.gen_range(0..5);
                let like_key = format!("like:user:{}:post:{}:{}",
                                      rng.gen_range(0..20), user_id, post_id);
                let like_value = format!(r#"{{"timestamp":"2024-01-01T00:00:00Z"}}"#);
                db_clone.put(like_key.as_bytes(), like_value.as_bytes()).expect("Like failed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify data
    let users = db.scan(Some(b"user:"), Some(b"user:~")).expect("User scan failed");
    assert_eq!(users.count(), 20);

    let posts = db.scan(Some(b"post:"), Some(b"post:~")).expect("Post scan failed");
    assert_eq!(posts.count(), 100);
}

#[test]
fn test_document_storage() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("document_db");

    let config = LightningDbConfig {
        compression_enabled: true,
        cache_size: 16 * 1024 * 1024,
        ..Default::default()
    };

    let db = Database::open(db_path, config).expect("Failed to open database");

    // Store various document types
    for doc_id in 0..50 {
        let doc_key = format!("doc:{:04}", doc_id);

        // Create a JSON document with varying complexity
        let doc_value = if doc_id % 3 == 0 {
            // Simple document
            format!(r#"{{"id":{},"type":"simple","title":"Document {}"}}"#, doc_id, doc_id)
        } else if doc_id % 3 == 1 {
            // Medium complexity
            format!(r#"{{
                "id": {},
                "type": "medium",
                "title": "Document {}",
                "metadata": {{
                    "author": "User {}",
                    "created": "2024-01-01",
                    "tags": ["tag1", "tag2", "tag3"]
                }},
                "content": "This is the content of document {}"
            }}"#, doc_id, doc_id, doc_id % 10, doc_id)
        } else {
            // Complex nested document
            format!(r#"{{
                "id": {},
                "type": "complex",
                "title": "Document {}",
                "metadata": {{
                    "author": {{"id": {}, "name": "Author {}"}},
                    "created": "2024-01-01",
                    "modified": "2024-01-02",
                    "tags": ["tag1", "tag2"],
                    "categories": ["cat1", "cat2"]
                }},
                "content": {{
                    "sections": [
                        {{"title": "Section 1", "text": "Content 1"}},
                        {{"title": "Section 2", "text": "Content 2"}}
                    ],
                    "references": [{}, {}]
                }}
            }}"#, doc_id, doc_id, doc_id % 5, doc_id % 5, doc_id + 1, doc_id + 2)
        };

        db.put(doc_key.as_bytes(), doc_value.as_bytes()).expect("Document storage failed");
    }

    // Create indices
    for doc_id in 0..50 {
        let doc_type = if doc_id % 3 == 0 { "simple" } else if doc_id % 3 == 1 { "medium" } else { "complex" };
        let index_key = format!("index:type:{}:doc:{:04}", doc_type, doc_id);
        let index_value = format!("doc:{:04}", doc_id);
        db.put(index_key.as_bytes(), index_value.as_bytes()).expect("Index creation failed");
    }

    // Query by type
    let simple_docs = db.scan(Some(b"index:type:simple:"), Some(b"index:type:simple:~"))
                        .expect("Index scan failed");
    assert!(simple_docs.count() > 0);

    // Verify all documents
    let all_docs = db.scan(Some(b"doc:"), Some(b"doc:~")).expect("Document scan failed");
    assert_eq!(all_docs.count(), 50);
}

#[test]
fn test_cache_workload() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("cache_db");

    let config = LightningDbConfig {
        cache_size: 8 * 1024 * 1024,
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));

    // Simulate cache-like usage patterns
    let mut handles = vec![];

    // Writer thread
    let db_writer = Arc::clone(&db);
    let writer = thread::spawn(move || {
        for i in 0..100 {
            let key = format!("cache:key:{:03}", i);
            let value = format!("cached_value_{}_timestamp_{}", i, i * 1000);
            db_writer.put(key.as_bytes(), value.as_bytes()).expect("Cache write failed");
            thread::sleep(Duration::from_millis(10));
        }
    });
    handles.push(writer);

    // Multiple reader threads
    for reader_id in 0..3 {
        let db_reader = Arc::clone(&db);
        let reader = thread::spawn(move || {
            let mut rng = thread_rng();
            for _ in 0..50 {
                let key = format!("cache:key:{:03}", rng.gen_range(0..100));
                let _ = db_reader.get(key.as_bytes());
                thread::sleep(Duration::from_millis(5));
            }
        });
        handles.push(reader);
    }

    // Invalidation thread
    let db_invalidator = Arc::clone(&db);
    let invalidator = thread::spawn(move || {
        let mut rng = thread_rng();
        thread::sleep(Duration::from_millis(500)); // Let some data accumulate

        for _ in 0..20 {
            let key = format!("cache:key:{:03}", rng.gen_range(0..50));
            db_invalidator.delete(key.as_bytes()).ok();
            thread::sleep(Duration::from_millis(25));
        }
    });
    handles.push(invalidator);

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify cache state
    let remaining = db.scan(Some(b"cache:"), Some(b"cache:~")).expect("Cache scan failed");
    let count = remaining.count();
    assert!(count > 0 && count <= 100);
}