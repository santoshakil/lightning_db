use lightning_db::{Database, LightningDbConfig};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_metrics_collection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("metrics_db");

    let config = LightningDbConfig {
        use_optimized_page_manager: true,
        prefetch_enabled: false,
        use_optimized_transactions: false,
        ..Default::default()
    };

    let db = Database::create(&db_path, config).unwrap();

    // Perform some operations
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Read operations
    for i in 0..50 {
        let key = format!("key_{:03}", i);
        let _value = db.get(key.as_bytes()).unwrap();
    }

    // Delete operations
    for i in 0..10 {
        let key = format!("key_{:03}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    // Get metrics
    let metrics = db.get_metrics();

    assert_eq!(metrics.writes, 100);
    assert_eq!(metrics.reads, 50);
    assert_eq!(metrics.deletes, 10);

    println!("Metrics collected:");
    println!("  Writes: {}", metrics.writes);
    println!("  Reads: {}", metrics.reads);
    println!("  Deletes: {}", metrics.deletes);
    println!("  Avg write latency: {} μs", metrics.avg_write_latency_us);
    println!("  Avg read latency: {} μs", metrics.avg_read_latency_us);
}

#[test]
fn test_metrics_reporter() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("reporter_db");

    let config = LightningDbConfig {
        use_optimized_page_manager: true,
        prefetch_enabled: false,
        use_optimized_transactions: false,
        ..Default::default()
    };

    let db = Database::create(&db_path, config).unwrap();

    // Perform mixed operations
    for i in 0..50 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();

        if i % 2 == 0 {
            db.get(key.as_bytes()).unwrap();
        }
    }

    // Get reporter and generate reports
    let reporter = db.get_metrics_reporter();

    // Text report
    let text_report = reporter.text_report();
    println!("\n{}", text_report);

    // JSON report
    let json_report = reporter.json_report();
    println!("\nJSON Report:");
    println!("{}", serde_json::to_string_pretty(&json_report).unwrap());

    // Summary
    let summary = reporter.summary_report();
    println!("\nSummary:");
    println!("  Total operations: {}", summary.total_operations);
    println!("  Avg latency: {} μs", summary.avg_latency_us);
    println!("  Database size: {:.2} MB", summary.database_size_mb);
}

#[test]
fn test_cache_metrics() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("cache_metrics_db");

    let config = LightningDbConfig {
        use_optimized_page_manager: true,
        prefetch_enabled: false,
        use_optimized_transactions: false,
        cache_size: 1024 * 1024, // 1MB cache
        ..Default::default()
    };

    let db = Database::create(&db_path, config).unwrap();

    // Write data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Read same data multiple times to test cache
    for _ in 0..3 {
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            db.get(key.as_bytes()).unwrap();
        }
    }

    let metrics = db.get_metrics();

    println!("\nCache metrics:");
    println!("  Cache hits: {}", metrics.cache_hits);
    println!("  Cache misses: {}", metrics.cache_misses);
    println!("  Cache hit rate: {:.1}%", metrics.cache_hit_rate * 100.0);

    // First reads should be misses, subsequent reads should be hits
    assert!(metrics.cache_hits > 0);
    assert!(metrics.cache_misses > 0);
}

#[test]
fn test_window_metrics() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("window_metrics_db");

    let config = LightningDbConfig {
        use_optimized_page_manager: true,
        prefetch_enabled: false,
        use_optimized_transactions: false,
        ..Default::default()
    };

    let db = Database::create(&db_path, config).unwrap();
    let collector = db.metrics_collector();

    // Perform operations over time
    for batch in 0..3 {
        for i in 0..20 {
            let key = format!("key_{}_{}", batch, i);
            let value = format!("value_{}_{}", batch, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Sleep to create time windows
        thread::sleep(Duration::from_millis(100));
    }

    // Get window metrics
    if let Some(window_metrics) = collector.get_window_metrics(Duration::from_secs(1)) {
        println!("\nWindow metrics (1 second):");
        println!("  Reads/sec: {:.2}", window_metrics.reads_per_sec);
        println!("  Writes/sec: {:.2}", window_metrics.writes_per_sec);
        println!("  Error rate: {:.4}%", window_metrics.error_rate * 100.0);
    }
}

#[test]
fn test_component_metrics() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("component_metrics_db");

    let config = LightningDbConfig {
        use_optimized_page_manager: true,
        prefetch_enabled: false,
        use_optimized_transactions: false,
        compression_enabled: true, // Enable LSM tree
        ..Default::default()
    };

    let db = Database::create(&db_path, config).unwrap();
    let collector = db.metrics_collector();

    // Register and update component metrics
    if let Some(lsm_metrics) = collector.get_component("lsm_tree") {
        lsm_metrics.update_metric("memtable_size", 1024.0 * 1024.0); // 1MB
        lsm_metrics.update_metric("sstable_count", 5.0);
        lsm_metrics.update_metric("compression_ratio", 0.65);

        let metrics = lsm_metrics.get_metrics();
        println!("\nLSM Tree component metrics:");
        for (key, value) in metrics.iter() {
            println!("  {}: {:.2}", key, value);
        }
    }
}
