use lightning_db::{Database, LightningDbConfig};
use lightning_db::features::logging::{
    LoggingConfig, LogLevel, LogFormat, LogOutput, FileOutput, FileRotation,
    TelemetryConfig, SamplingConfig, RedactionConfig, PerformanceConfig,
    JaegerConfig, PrometheusConfig, get_logging_system, export_metrics_to_file,
    get_performance_stats, get_metrics_snapshot, TraceContext, 
    with_trace_context, traced_span, with_perf_monitoring
};
use std::time::Duration;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure comprehensive logging system
    let logging_config = LoggingConfig {
        level: LogLevel::Info,
        format: LogFormat::Json,
        output: LogOutput {
            console: true,
            file: Some(FileOutput {
                path: "logs".to_string(),
                rotation: FileRotation::Daily,
                max_files: 7,
            }),
            jaeger: Some(JaegerConfig {
                endpoint: "http://localhost:14268/api/traces".to_string(),
                service_name: "lightning_db_demo".to_string(),
                batch_timeout: Duration::from_secs(1),
                max_batch_size: 100,
            }),
            prometheus: Some(PrometheusConfig {
                endpoint: "http://localhost:9090".to_string(),
                push_interval: Duration::from_secs(15),
                registry_name: "lightning_db".to_string(),
            }),
        },
        sampling: SamplingConfig {
            enabled: true,
            trace_sample_rate: 0.1,
            debug_sample_rate: 0.05,
            high_frequency_operations: vec![
                "get".to_string(),
                "cache_lookup".to_string(),
            ],
            high_frequency_sample_rate: 0.01,
            ..Default::default()
        },
        telemetry: TelemetryConfig {
            enabled: true,
            service_name: "lightning_db_demo".to_string(),
            service_version: "1.0.0".to_string(),
            environment: "development".to_string(),
            resource_attributes: {
                let mut attrs = HashMap::new();
                attrs.insert("datacenter".to_string(), "us-west-1".to_string());
                attrs.insert("instance_id".to_string(), "demo-001".to_string());
                attrs
            },
        },
        correlation_id_header: "x-correlation-id".to_string(),
        redaction: RedactionConfig {
            enabled: true,
            patterns: vec![
                r"password=\S+".to_string(),
                r"token=\S+".to_string(),
            ],
            replacement: "[REDACTED]".to_string(),
            redact_keys: false,
            redact_values: true,
        },
        performance: PerformanceConfig {
            slow_operation_threshold: Duration::from_millis(50),
            enable_operation_timing: true,
            enable_memory_tracking: true,
            enable_resource_monitoring: true,
            metrics_buffer_size: 10000,
        },
    };

    // 2. Initialize the logging system
    lightning_db::features::logging::init_logging_with_config(logging_config)?;
    
    println!("🚀 Lightning DB Comprehensive Logging Demo");
    
    // 3. Create database with default config
    let db = Database::create("./demo_db", LightningDbConfig::default())?;
    
    // 4. Demonstrate trace context propagation
    println!("\n📍 Demonstrating trace context propagation...");
    
    let main_context = TraceContext::new()
        .with_user_id("demo_user".to_string())
        .with_request_id("req_12345".to_string())
        .with_session_id("session_abcde".to_string())
        .with_custom_attribute("demo_type".to_string(), "comprehensive".to_string());
    
    with_trace_context!(main_context.clone(), {
        // 5. Perform basic operations with tracing
        demo_basic_operations(&db).await?;
        
        // 6. Demonstrate transaction logging
        demo_transaction_operations(&db).await?;
        
        // 7. Demonstrate performance monitoring
        demo_performance_monitoring(&db).await?;
    });
    
    // 8. Demonstrate high-frequency operations with sampling
    println!("\n⚡ Demonstrating high-frequency operations with sampling...");
    demo_high_frequency_operations(&db).await?;
    
    // 9. Export metrics and performance stats
    println!("\n📊 Exporting metrics and performance statistics...");
    
    // Export metrics to file
    export_metrics_to_file("demo_metrics.json")?;
    println!("   ✓ Metrics exported to demo_metrics.json");
    
    // Get performance stats
    let perf_stats = get_performance_stats();
    println!("   ✓ Performance stats for {} operations", perf_stats.len());
    
    for (operation, stats) in perf_stats {
        println!("     {} -> {} ops, avg: {:?}, p99: {:?}", 
                operation, stats.total_operations, stats.avg_duration, stats.p99_duration);
    }
    
    // Get metrics snapshot
    let snapshot = get_metrics_snapshot();
    println!("   ✓ Metrics snapshot captured at {:?}", snapshot.timestamp);
    println!("     Uptime: {:?}", snapshot.uptime);
    
    // 10. Demonstrate error handling with logging
    println!("\n❌ Demonstrating error handling with comprehensive logging...");
    demo_error_handling(&db).await;
    
    // 11. Show logging system statistics
    println!("\n📈 Logging System Statistics:");
    let system = get_logging_system();
    
    if let Some(sampler_stats) = system.sampler.get_sampling_stats().operation_stats.get("get") {
        println!("   Sampling for 'get' operations:");
        println!("     Total requests: {}", sampler_stats.total_requests);
        println!("     Sampled requests: {}", sampler_stats.sampled_requests);
        println!("     Current rate: {:.4}", sampler_stats.current_rate);
        println!("     Actual ratio: {:.4}", sampler_stats.sampling_ratio);
    }
    
    // 12. Demonstrate custom spans and events
    println!("\n🎯 Demonstrating custom spans and events...");
    demo_custom_spans_and_events(&db).await?;
    
    println!("\n✅ Comprehensive logging demo completed successfully!");
    println!("   Check the logs/ directory for detailed log files");
    println!("   Check demo_metrics.json for exported metrics");
    println!("   If Jaeger is running, view traces at http://localhost:16686");
    println!("   If Prometheus is running, metrics are available for scraping");
    
    Ok(())
}

async fn demo_basic_operations(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    println!("   Performing basic database operations...");
    
    // These operations will be automatically logged with trace context
    for i in 0..5 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        
        db.put(key.as_bytes(), value.as_bytes())?;
        let retrieved = db.get(key.as_bytes())?;
        
        assert_eq!(retrieved.as_deref(), Some(value.as_bytes()));
    }
    
    println!("   ✓ Basic operations completed with full tracing");
    Ok(())
}

async fn demo_transaction_operations(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    println!("   Performing transaction operations...");
    
    // Transaction operations are automatically logged with correlation
    let tx_id = db.begin_transaction()?;
    
    db.put_tx(tx_id, b"tx_key_1", b"tx_value_1")?;
    db.put_tx(tx_id, b"tx_key_2", b"tx_value_2")?;
    
    // Check transaction state
    let value = db.get_tx(tx_id, b"tx_key_1")?;
    assert_eq!(value.as_deref(), Some(b"tx_value_1".as_ref()));
    
    // Commit transaction
    db.commit_transaction(tx_id)?;
    
    println!("   ✓ Transaction operations completed with full logging");
    Ok(())
}

async fn demo_performance_monitoring(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    println!("   Demonstrating performance monitoring...");
    
    // Use performance monitoring macro
    let result = with_perf_monitoring!("batch_operations", {
        for i in 0..100 {
            let key = format!("perf_key_{}", i);
            let value = format!("perf_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        "completed"
    });
    
    assert_eq!(result, "completed");
    println!("   ✓ Performance monitoring demo completed");
    Ok(())
}

async fn demo_high_frequency_operations(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    // This will demonstrate sampling in action
    for i in 0..1000 {
        let key = format!("freq_key_{}", i % 10); // Reuse keys to test caching
        let value = format!("freq_value_{}", i);
        
        if i % 2 == 0 {
            db.put(key.as_bytes(), value.as_bytes())?;
        } else {
            let _retrieved = db.get(key.as_bytes())?;
        }
    }
    
    println!("   ✓ High-frequency operations completed (with sampling)");
    Ok(())
}

async fn demo_error_handling(db: &Database) {
    // Demonstrate error logging
    println!("   Testing error conditions...");
    
    // Try to get a very long key (should trigger validation error)
    let long_key = vec![b'x'; 5000]; // Exceeds MAX_KEY_SIZE
    match db.put(&long_key, b"value") {
        Err(e) => {
            println!("   ✓ Expected error logged: {}", e);
        }
        Ok(_) => {
            println!("   ⚠ Unexpected success with oversized key");
        }
    }
    
    // Try to put a very large value
    let large_value = vec![b'x'; 2 * 1024 * 1024]; // 2MB, exceeds MAX_VALUE_SIZE
    match db.put(b"test_key", &large_value) {
        Err(e) => {
            println!("   ✓ Expected error logged: {}", e);
        }
        Ok(_) => {
            println!("   ⚠ Unexpected success with oversized value");
        }
    }
}

async fn demo_custom_spans_and_events(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom traced span
    let custom_span = traced_span!("custom_batch_operation");
    let _guard = custom_span.enter();
    
    // Add events to the span
    custom_span.add_event("batch_started", vec![
        ("batch_size", "50"),
        ("operation_type", "mixed"),
    ]);
    
    // Perform operations within the span
    for i in 0..50 {
        let key = format!("custom_key_{}", i);
        let value = format!("custom_value_{}", i);
        
        if i % 3 == 0 {
            db.put(key.as_bytes(), value.as_bytes())?;
        } else {
            let _retrieved = db.get(key.as_bytes())?;
        }
    }
    
    custom_span.add_event("batch_completed", vec![
        ("operations_completed", "50"),
        ("success", "true"),
    ]);
    
    println!("   ✓ Custom spans and events demo completed");
    Ok(())
}