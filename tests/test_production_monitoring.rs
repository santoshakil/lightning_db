use lightning_db::monitoring::production_hooks::{
    HealthStatus, MonitoringEvent, MonitoringHook, OperationType,
};
use lightning_db::{Database, LightningDbConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;

/// Custom monitoring hook for testing
struct TestMonitoringHook {
    events: Arc<Mutex<Vec<MonitoringEvent>>>,
    metrics: Arc<Mutex<HashMap<String, f64>>>,
}

impl TestMonitoringHook {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            metrics: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_events(&self) -> Vec<MonitoringEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl MonitoringHook for TestMonitoringHook {
    fn on_event(&self, event: &MonitoringEvent) {
        self.events.lock().unwrap().push(event.clone());
    }

    fn collect_metrics(&self) -> HashMap<String, f64> {
        self.metrics.lock().unwrap().clone()
    }
}

/// Test basic monitoring hook functionality
#[test]
fn test_monitoring_hooks() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Register custom monitoring hook
    let test_hook = Arc::new(TestMonitoringHook::new());
    db.register_monitoring_hook(test_hook.clone());

    // Set operation thresholds
    db.set_operation_threshold(OperationType::Read, Duration::from_micros(100));
    db.set_operation_threshold(OperationType::Write, Duration::from_millis(1));

    // Perform operations
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    for i in 0..10 {
        let key = format!("key_{}", i);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    // Force a slow operation by using a large value
    let large_value = vec![0u8; 1_000_000];
    db.put(b"large_key", &large_value).unwrap();

    // Check events were recorded
    let events = test_hook.get_events();
    println!("Recorded {} monitoring events", events.len());

    // Verify we got some events
    assert!(events.len() > 0, "Should have recorded monitoring events");

    // Check for slow operation events
    let slow_ops: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, MonitoringEvent::SlowOperation { .. }))
        .collect();

    println!("Found {} slow operations", slow_ops.len());
}

/// Test production metrics collection
#[test]
fn test_production_metrics() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Perform various operations
    for i in 0..100 {
        let key = format!("metric_key_{}", i);
        let value = format!("metric_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    for i in 0..50 {
        let key = format!("metric_key_{}", i);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    for i in 0..25 {
        let key = format!("metric_key_{}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    // Collect metrics
    let metrics = db.get_production_metrics();

    println!("Production metrics:");
    for (key, value) in &metrics {
        println!("  {}: {:.2}", key, value);
    }

    // Verify some metrics exist
    assert!(!metrics.is_empty(), "Should have collected some metrics");
}

/// Test health check functionality
#[test]
fn test_health_check() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Perform health check
    let health_status = db.production_health_check();

    println!("Health check result: {:?}", health_status);

    // Database should be healthy initially
    assert_eq!(health_status, HealthStatus::Healthy);
}

/// Test custom monitoring events
#[test]
fn test_custom_monitoring_events() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Register test hook
    let test_hook = Arc::new(TestMonitoringHook::new());
    db.register_monitoring_hook(test_hook.clone());

    // Emit custom events
    db.emit_monitoring_event(MonitoringEvent::HealthCheck {
        status: HealthStatus::Healthy,
        details: HashMap::from([
            ("component".to_string(), "test".to_string()),
            ("version".to_string(), "1.0.0".to_string()),
        ]),
    });

    db.emit_monitoring_event(MonitoringEvent::MemoryUsage {
        total_bytes: 1024 * 1024 * 100, // 100MB
        cache_bytes: 1024 * 1024 * 50,  // 50MB
        index_bytes: 1024 * 1024 * 20,  // 20MB
        wal_bytes: 1024 * 1024 * 10,    // 10MB
    });

    db.emit_monitoring_event(MonitoringEvent::CacheEviction {
        evicted_count: 100,
        cache_size: 1000,
        reason: "Memory pressure".to_string(),
    });

    // Verify events were recorded
    let events = test_hook.get_events();
    assert_eq!(events.len(), 3, "Should have recorded 3 custom events");

    // Check event types
    let has_health_check = events
        .iter()
        .any(|e| matches!(e, MonitoringEvent::HealthCheck { .. }));
    let has_memory_usage = events
        .iter()
        .any(|e| matches!(e, MonitoringEvent::MemoryUsage { .. }));
    let has_cache_eviction = events
        .iter()
        .any(|e| matches!(e, MonitoringEvent::CacheEviction { .. }));

    assert!(has_health_check, "Should have health check event");
    assert!(has_memory_usage, "Should have memory usage event");
    assert!(has_cache_eviction, "Should have cache eviction event");
}

/// Test monitoring during concurrent operations
#[test]
fn test_concurrent_monitoring() {
    use std::sync::Barrier;
    use std::thread;

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path(), LightningDbConfig::default()).unwrap());

    // Register monitoring hook
    let test_hook = Arc::new(TestMonitoringHook::new());
    db.register_monitoring_hook(test_hook.clone());

    // Set aggressive thresholds to catch more events
    db.set_operation_threshold(OperationType::Read, Duration::from_micros(1));
    db.set_operation_threshold(OperationType::Write, Duration::from_micros(10));

    let num_threads = 4;
    let ops_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("thread_{}_value_{}", thread_id, i);

                // Mix of operations
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                let _ = db_clone.get(key.as_bytes());

                if i % 10 == 0 {
                    db_clone.delete(key.as_bytes()).unwrap();
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Check events
    let events = test_hook.get_events();
    println!(
        "Recorded {} events during concurrent operations",
        events.len()
    );

    // Should have recorded many events
    assert!(
        events.len() > 0,
        "Should have recorded events during concurrent operations"
    );

    // Count slow operations
    let slow_ops = events
        .iter()
        .filter(|e| matches!(e, MonitoringEvent::SlowOperation { .. }))
        .count();

    println!(
        "Found {} slow operations during concurrent execution",
        slow_ops
    );
}
