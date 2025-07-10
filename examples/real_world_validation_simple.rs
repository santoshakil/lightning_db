/// Simplified Real-World Usage Pattern Validation
///
/// Validates Lightning DB against common production use cases
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌍 REAL-WORLD USAGE PATTERN VALIDATION");
    println!("======================================\n");

    // Test 1: Key-Value Cache Pattern
    println!("1️⃣ Key-Value Cache Pattern (Redis-like)");
    validate_cache_pattern()?;

    // Test 2: Time-Series Database Pattern
    println!("\n2️⃣ Time-Series Database Pattern");
    validate_timeseries_pattern()?;

    // Test 3: Document Store Pattern
    println!("\n3️⃣ Document Store Pattern");
    validate_document_pattern()?;

    // Test 4: Session Management Pattern
    println!("\n4️⃣ Session Management Pattern");
    validate_session_pattern()?;

    // Test 5: Queue/Stream Pattern
    println!("\n5️⃣ Queue/Stream Pattern");
    validate_queue_pattern()?;

    println!("\n✅ ALL REAL-WORLD VALIDATION TESTS COMPLETED!");

    Ok(())
}

fn validate_cache_pattern() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024,
        ..Default::default()
    }; // 10MB cache

    let db = Arc::new(Database::create(temp_dir.path(), config)?);

    // Pre-populate some data
    for i in 0..100 {
        let key = format!("cache:key:{}", i);
        let value = format!("value:{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Simulate cache workload: 90% reads, 10% writes
    let num_threads = 4;
    let operations_per_thread = 1_000;

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = db.clone();

        let handle = thread::spawn(move || {
            let mut hits = 0;
            let mut misses = 0;

            for i in 0..operations_per_thread {
                let operation_type = i % 10; // 9 reads for every 1 write
                let key_id = (thread_id * 1000 + i) % 200; // Use a rotating key space
                let key = format!("cache:key:{}", key_id);

                if operation_type < 9 {
                    // Read operation
                    match db_clone.get(key.as_bytes()) {
                        Ok(Some(_)) => hits += 1,
                        Ok(None) => misses += 1,
                        Err(_) => {}
                    }
                } else {
                    // Write operation
                    let value = format!("value:{}:updated", key_id);
                    let _ = db_clone.put(key.as_bytes(), value.as_bytes());
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

    let duration = start.elapsed();
    let total_ops = num_threads * operations_per_thread;
    let throughput = total_ops as f64 / duration.as_secs_f64();
    let hit_rate = if total_hits + total_misses > 0 {
        total_hits as f64 / (total_hits + total_misses) as f64
    } else {
        0.0
    };

    println!("   Throughput: {:.0} ops/sec", throughput);
    println!("   Hit rate: {:.1}%", hit_rate * 100.0);
    println!(
        "   Avg latency: {:.2}μs",
        duration.as_micros() as f64 / total_ops as f64
    );

    Ok(())
}

fn validate_timeseries_pattern() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig {
        compression_enabled: true,
        ..Default::default()
    }; // Important for time series

    let db = Database::create(temp_dir.path(), config)?;

    // Simulate metrics ingestion
    let metrics = vec!["cpu", "memory", "disk", "network"];
    let hosts = 10;
    let duration_secs = 60; // 1 minute of data

    let start = Instant::now();
    let mut write_count = 0;

    // Write phase - sequential timestamps
    for second in 0..duration_secs {
        let timestamp = 1700000000 + second;

        for host in 0..hosts {
            for metric in &metrics {
                let key = format!("ts:{}:host{}:{}", metric, host, timestamp);
                let value = format!("{:.2}", second as f64 + host as f64);

                db.put(key.as_bytes(), value.as_bytes())?;
                write_count += 1;
            }
        }
    }

    let write_duration = start.elapsed();
    let write_throughput = write_count as f64 / write_duration.as_secs_f64();

    // Query phase - range scans
    let query_start = Instant::now();
    let mut scan_count = 0;

    // Scan last 10 seconds of data for specific metric/host
    for host in 0..5 {
        let start_key = format!("ts:cpu:host{}:1700000050", host);
        let end_key = format!("ts:cpu:host{}:1700000060", host);

        let count = db
            .scan(Some(start_key.into_bytes()), Some(end_key.into_bytes()))?
            .count();

        scan_count += count;
    }

    let query_duration = query_start.elapsed();

    println!("   Data points: {}", write_count);
    println!("   Write throughput: {:.0} points/sec", write_throughput);
    println!(
        "   Range query: {} points in {:.2}ms",
        scan_count,
        query_duration.as_millis()
    );

    Ok(())
}

fn validate_document_pattern() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    // Simulate document store with JSON-like data
    let num_documents = 1_000;
    let start = Instant::now();

    // Insert documents
    for i in 0..num_documents {
        let doc_key = format!("doc:{:08}", i);
        let doc_value = format!(
            r#"{{"id":{},"type":"user","name":"User{}","email":"user{}@example.com","created_at":{},"metadata":{{"tags":["tag1","tag2"],"score":{}}}}}"#,
            i,
            i,
            i,
            1700000000 + i,
            i % 100
        );

        db.put(doc_key.as_bytes(), doc_value.as_bytes())?;
    }

    let insert_duration = start.elapsed();

    // Update documents (simulating partial updates)
    let update_start = Instant::now();
    for i in (0..100).step_by(10) {
        let doc_key = format!("doc:{:08}", i);

        // In real implementation, this would be read-modify-write
        if let Ok(Some(existing)) = db.get(doc_key.as_bytes()) {
            let mut updated = String::from_utf8_lossy(&existing).to_string();
            updated = updated.replace(
                "\"score\":",
                &format!("\"score\":{},\"updated\":", i + 1000),
            );
            db.put(doc_key.as_bytes(), updated.as_bytes())?;
        }
    }
    let update_duration = update_start.elapsed();

    // Complex queries (prefix scan)
    let query_start = Instant::now();
    let count = db
        .scan(Some(b"doc:00000".to_vec()), Some(b"doc:00001".to_vec()))?
        .count();
    let query_duration = query_start.elapsed();

    println!("   Documents: {}", num_documents);
    println!(
        "   Insert: {:.2}ms ({:.0} docs/sec)",
        insert_duration.as_millis(),
        num_documents as f64 / insert_duration.as_secs_f64()
    );
    println!("   Updates: {:.2}ms", update_duration.as_millis());
    println!(
        "   Prefix scan: {} docs in {:.2}ms",
        count,
        query_duration.as_millis()
    );

    Ok(())
}

fn validate_session_pattern() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(temp_dir.path(), config)?);

    // Simulate web session management
    let num_threads = 4;
    let sessions_per_thread = 250;

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = db.clone();

        let handle = thread::spawn(move || {
            for i in 0..sessions_per_thread {
                let session_id = format!("session:{}:{}", thread_id, i);
                let user_id = thread_id * 1000 + i;

                // Create session
                let session_data = format!(
                    r#"{{"user_id":{},"created":{},"last_access":{},"data":{{"page":"/home","cart_items":0}}}}"#,
                    user_id, i, i
                );

                db_clone
                    .put(session_id.as_bytes(), session_data.as_bytes())
                    .unwrap();

                // Simulate session access pattern (3 accesses per session)
                for j in 0..3 {
                    if let Ok(Some(data)) = db_clone.get(session_id.as_bytes()) {
                        // Update last_access
                        let updated = String::from_utf8_lossy(&data).replace(
                            "\"last_access\":",
                            &format!("\"last_access\":{},\"old\":", i + j),
                        );
                        let _ = db_clone.put(session_id.as_bytes(), updated.as_bytes());
                    }
                }

                // Simulate session expiry (10% of sessions)
                if i % 10 == 0 {
                    let _ = db_clone.delete(session_id.as_bytes());
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total_sessions = num_threads * sessions_per_thread;
    let throughput = total_sessions as f64 / duration.as_secs_f64();

    println!("   Sessions: {}", total_sessions);
    println!("   Throughput: {:.0} sessions/sec", throughput);
    println!(
        "   Avg session lifetime: {:.2}ms",
        duration.as_millis() as f64 / total_sessions as f64
    );

    Ok(())
}

fn validate_queue_pattern() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(temp_dir.path(), config)?);

    // Simulate message queue pattern
    let messages_per_producer = 500;
    let start = Instant::now();

    // Producer
    for i in 0..messages_per_producer {
        let key = format!("queue:msg:{:016}", i);
        let value = format!(
            r#"{{"id":{},"timestamp":{},"payload":"Message data here"}}"#,
            i, i
        );

        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Consumer
    let mut consumed = 0;
    let mut last_key = format!("queue:msg:{:016}", 0);

    loop {
        // Scan for new messages
        let mut found = false;
        let scan_result = db.scan(
            Some(last_key.clone().into_bytes()),
            Some(b"queue:msg:~".to_vec()),
        );

        if let Ok(iter) = scan_result {
            for (key, _value) in iter.take(10).flatten() {
                // Process up to 10 messages at a time
                // Process message
                if db.delete(&key).is_ok() {
                    consumed += 1;
                    last_key = String::from_utf8_lossy(&key).to_string();
                    found = true;
                }
            }
        }

        if !found {
            break;
        }
    }

    let duration = start.elapsed();
    let throughput = messages_per_producer as f64 / duration.as_secs_f64();

    println!("   Messages: {} produced", messages_per_producer);
    println!("   Throughput: {:.0} msg/sec", throughput);
    println!(
        "   Consumer efficiency: {:.1}%",
        (consumed as f64 / messages_per_producer as f64) * 100.0
    );

    Ok(())
}
