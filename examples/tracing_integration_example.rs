//! Advanced Distributed Tracing Integration Example
//! 
//! Demonstrates the TracedDatabase wrapper and real-world integration patterns
//! for distributed tracing in Lightning DB applications.

use lightning_db::distributed_tracing::*;
use lightning_db::distributed_tracing::context::*;
use lightning_db::distributed_tracing::span::*;
use lightning_db::distributed_tracing::sampler::*;
use lightning_db::distributed_tracing::exporter::{MultiExporter, FileExporter, FileFormat, BatchExporter, MetricsExporter, LoggingExporter, HttpExporter, HttpFormat, LogLevel as ExporterLogLevel};
use lightning_db::distributed_tracing::integration::*;
use lightning_db::{Database, Result};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    println!("🚀 Lightning DB Advanced Tracing Integration Example");
    
    // Initialize distributed tracing
    setup_production_tracing()?;
    
    // Create database and traced wrapper
    let database = Database::create("integration_example.db", lightning_db::LightningDbConfig::default())?;
    let traced_db = TracedDatabase::new(database);
    
    println!("\n🔧 Testing basic instrumented operations...");
    test_basic_instrumented_operations(&traced_db)?;
    
    println!("\n🌐 Testing HTTP request simulation...");
    test_http_request_simulation(&traced_db)?;
    
    println!("\n🔄 Testing concurrent operations...");
    test_concurrent_operations(&traced_db)?;
    
    println!("\n📊 Testing batch operations with tracing...");
    test_batch_operations_with_metrics(&traced_db)?;
    
    println!("\n⚡ Testing performance monitoring...");
    test_performance_monitoring(&traced_db)?;
    
    println!("\n🔍 Testing error handling and recovery...");
    test_error_handling_with_tracing(&traced_db)?;
    
    println!("\n✅ All advanced tracing integration tests completed!");
    Ok(())
}

/// Set up production-grade distributed tracing
fn setup_production_tracing() -> Result<()> {
    // Create advanced sampler configuration
    let sampler = ProbabilitySampler::new(0.05); // 5% sampling for production
    
    // Create production exporter chain
    let multi_exporter = MultiExporter::new()
        // Console for development
        .add_exporter(Box::new(LoggingExporter::new(ExporterLogLevel::Info)
            .with_tags(true)
            .with_events(false)))
        // File export for analysis
        .add_exporter(Box::new(FileExporter::new(
            "production_traces.jsonl".to_string(),
            FileFormat::JsonLines
        )?))
        // Metrics for monitoring
        .add_exporter(Box::new(MetricsExporter::new()))
        // HTTP export to OpenTelemetry collector (mock endpoint)
        .add_exporter(Box::new(HttpExporter::new(
            "http://localhost:4318/v1/traces".to_string(),
            HttpFormat::OpenTelemetryJson
        ).with_header("X-API-Key".to_string(), "your-api-key".to_string())))
        .with_fail_fast(false);
    
    let batch_exporter = BatchExporter::new(
        Box::new(multi_exporter),
        100, // Larger batch size for production
        10000, // 10 second timeout
    );
    
    let tracer = Tracer::new("lightning_db_service".to_string())
        .with_sampler(Box::new(sampler))
        .with_exporter(Box::new(batch_exporter));
    
    init_tracer(tracer);
    println!("🎯 Production-grade tracing system initialized");
    Ok(())
}

/// Test basic instrumented operations
fn test_basic_instrumented_operations(traced_db: &TracedDatabase) -> Result<()> {
    // Create a service context
    let mut context = TraceContext::new_root();
    context.set_baggage("service.name".to_string(), "user_service".to_string());
    context.set_baggage("service.version".to_string(), "1.2.3".to_string());
    
    set_current_context(context.clone());
    
    // Test automatic context propagation
    traced_db.put(b"user:12345", br#"{"id": 12345, "name": "Alice", "email": "alice@example.com"}"#)?;
    
    let user_data = traced_db.get(b"user:12345")?;
    println!("   Retrieved user: {}", String::from_utf8_lossy(&user_data.unwrap()));
    
    let existed = traced_db.delete(b"user:12345")?;
    println!("   User deletion result: {}", existed);
    
    // Test with explicit context
    let mut explicit_context = TraceContext::new_root();
    explicit_context.set_baggage("operation.type".to_string(), "admin_action".to_string());
    
    traced_db.put_with_context(&explicit_context, b"admin:action", b"user_deleted")?;
    
    println!("   ✓ Basic instrumented operations completed");
    Ok(())
}

/// Test HTTP request simulation with header propagation
fn test_http_request_simulation(traced_db: &TracedDatabase) -> Result<()> {
    // Simulate incoming HTTP headers
    let mut request_headers = HashMap::new();
    request_headers.insert(
        "traceparent".to_string(),
        "00-12345678901234567890123456789012-1234567890123456-01".to_string()
    );
    
    // Create baggage for the HTTP request
    let baggage = create_http_baggage("POST", "/api/users", Some("user123"), Some("req-789"));
    let baggage_header = baggage.iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",");
    request_headers.insert("baggage".to_string(), baggage_header);
    
    // Extract context from headers
    let context = extract_from_headers(&request_headers)?.unwrap_or_else(TraceContext::new_root);
    
    // Simulate API endpoint processing
    with_context(context.clone(), || -> Result<()> {
        // Create an API operation span
        let mut api_span = DatabaseSpanBuilder::new(&context, "api.user.create")
            .with_operation("API_CALL")
            .build();
        
        api_span.set_tag("http.method".to_string(), "POST".to_string());
        api_span.set_tag("http.path".to_string(), "/api/users".to_string());
        api_span.set_tag("http.status_code".to_string(), "201".to_string());
        
        // Database operations within the API call
        traced_db.put_with_context(&context, b"user:new123", 
            br#"{"id": "new123", "name": "Bob", "created_via": "api"}"#)?;
        
        // Additional operations
        traced_db.put_with_context(&context, b"user:new123:profile", 
            br#"{"preferences": {"theme": "dark"}, "settings": {"notifications": true}}"#)?;
        
        api_span.record_success();
        api_span.finish();
        
        // Prepare response headers
        let mut response_headers = HashMap::new();
        inject_into_headers(&context, &mut response_headers);
        
        println!("   ✓ HTTP request processed - Trace ID: {}", context.trace_id);
        println!("   Response headers: {:?}", response_headers.keys().collect::<Vec<_>>());
        
        Ok(())
    })
}

/// Test concurrent operations with independent trace contexts
fn test_concurrent_operations(_traced_db: &TracedDatabase) -> Result<()> {
    // For simplicity in this example, we'll create separate database instances for each thread
    let mut handles = vec![];
    
    // Spawn multiple threads with different trace contexts
    for thread_id in 0..4 {
        let handle = thread::spawn(move || -> Result<()> {
            // Create a database instance for this thread
            let thread_db_path = format!("thread_{}_test.db", thread_id);
            let database = lightning_db::Database::create(&thread_db_path, lightning_db::LightningDbConfig::default())?;
            let traced_db = TracedDatabase::new(database);
            
            // Each thread has its own trace context
            let mut context = TraceContext::new_root();
            context.set_baggage("thread.id".to_string(), thread_id.to_string());
            context.set_baggage("operation.type".to_string(), "concurrent_test".to_string());
            
            set_current_context(context.clone());
            
            // Perform operations in this thread's context
            for i in 0..10 {
                let key = format!("thread_{}:item_{}", thread_id, i);
                let value = format!(r#"{{"thread": {}, "item": {}, "timestamp": "2024-01-15T10:30:00Z"}}"#, 
                                  thread_id, i);
                
                traced_db.put(key.as_bytes(), value.as_bytes())?;
                
                // Simulate some processing time
                thread::sleep(Duration::from_millis(10));
                
                // Read back the data
                let _retrieved = traced_db.get(key.as_bytes())?;
            }
            
            println!("   Thread {} completed operations", thread_id);
            Ok(())
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap()?;
    }
    
    println!("   ✓ Concurrent operations with independent tracing completed");
    Ok(())
}

/// Test batch operations with detailed metrics
fn test_batch_operations_with_metrics(traced_db: &TracedDatabase) -> Result<()> {
    let mut context = TraceContext::new_root();
    context.set_baggage("operation.type".to_string(), "batch_import".to_string());
    context.set_baggage("batch.source".to_string(), "csv_import".to_string());
    
    // Create a large batch of operations
    let batch_size = 500;
    let operations: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_size)
        .map(|i| {
            let key = format!("batch_item:{:05}", i);
            let value = format!(r#"{{"id": {}, "data": "batch test data", "sequence": {}}}"#, i, i);
            (key.into_bytes(), value.into_bytes())
        })
        .collect();
    
    // Convert to slice references for the API
    let operation_refs: Vec<(&[u8], &[u8])> = operations.iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    
    // Execute batch operation
    traced_db.batch_put_with_context(&context, &operation_refs)?;
    
    // Verify with scan operation
    let mut scanned_count = 0;
    traced_db.scan_with_context(&context, Some(b"batch_item:"), Some(b"batch_item;"), |_key, _value| {
        scanned_count += 1;
        Ok(true) // Continue scanning
    })?;
    
    println!("   ✓ Batch operations completed - {} items inserted, {} scanned", 
             batch_size, scanned_count);
    Ok(())
}

/// Test performance monitoring and thresholds
fn test_performance_monitoring(traced_db: &TracedDatabase) -> Result<()> {
    let mut context = TraceContext::new_root();
    context.set_baggage("test.type".to_string(), "performance_monitoring".to_string());
    
    set_current_context(context.clone());
    
    // Test different operation types and check performance
    let medium_value = vec![b'x'; 1024]; // 1KB value
    let large_value = vec![b'y'; 10240]; // 10KB value
    let test_cases = vec![
        ("fast_get", b"fast_key" as &[u8], b"small_value" as &[u8]),
        ("medium_put", b"medium_key" as &[u8], medium_value.as_slice()),
        ("large_put", b"large_key" as &[u8], large_value.as_slice()),
    ];
    
    for (test_name, key, value) in test_cases {
        println!("   Running performance test: {}", test_name);
        
        // Create a span with performance thresholds
        let mut span = DatabaseSpanBuilder::new(&context, &format!("perf_test.{}", test_name))
            .with_performance_thresholds(100, 1000) // 100μs warn, 1ms error
            .build();
        
        let start = std::time::Instant::now();
        
        // Perform operation
        traced_db.put_with_context(&context, key, value)?;
        let _retrieved = traced_db.get_with_context(&context, key)?;
        
        let duration = start.elapsed();
        let duration_micros = duration.as_micros() as u64;
        
        // Check performance thresholds
        if let Some(warning) = PerformanceMonitor::check_performance("db.put", duration_micros) {
            span.set_tag("performance.warning".to_string(), warning);
            println!("     ⚠️  {}", PerformanceMonitor::check_performance("db.put", duration_micros).unwrap());
        }
        
        span.record_success();
        span.finish();
        
        println!("     Duration: {}μs, Value size: {} bytes", duration_micros, value.len());
    }
    
    println!("   ✓ Performance monitoring tests completed");
    Ok(())
}

/// Test error handling and recovery with tracing
fn test_error_handling_with_tracing(traced_db: &TracedDatabase) -> Result<()> {
    let mut context = TraceContext::new_root();
    context.set_baggage("test.type".to_string(), "error_handling".to_string());
    
    // Test 1: Handle non-existent key gracefully
    with_context(context.clone(), || -> Result<()> {
        let mut span = DatabaseSpanBuilder::new(&context, "error_test.missing_key")
            .with_operation("GET")
            .build();
        
        match traced_db.get_with_context(&context, b"definitely_does_not_exist") {
            Ok(None) => {
                span.log(LogLevel::Info, "Key not found (expected)".to_string());
                span.record_success();
            }
            Ok(Some(_)) => {
                span.record_error("Unexpected data found", Some("DATA_CONSISTENCY_ERROR"));
            }
            Err(e) => {
                span.record_error(&format!("Unexpected error: {}", e), Some("UNEXPECTED_ERROR"));
            }
        }
        
        span.finish();
        Ok(())
    })?;
    
    // Test 2: Recovery scenario
    with_context(context.clone(), || -> Result<()> {
        let mut recovery_span = DatabaseSpans::recovery(&context, "test_recovery").build();
        
        recovery_span.log(LogLevel::Info, "Starting recovery simulation".to_string());
        
        // Simulate recovery by re-creating some test data
        for i in 0..5 {
            let key = format!("recovered_item:{}", i);
            let value = format!(r#"{{"id": {}, "recovered": true}}"#, i);
            
            traced_db.put_with_context(&context, key.as_bytes(), value.as_bytes())?;
        }
        
        recovery_span.log(LogLevel::Info, "Recovery completed successfully".to_string());
        recovery_span.record_success();
        recovery_span.finish();
        
        Ok(())
    })?;
    
    // Test 3: Simulate and handle a timeout scenario
    with_context(context.clone(), || -> Result<()> {
        let mut timeout_span = DatabaseSpanBuilder::new(&context, "error_test.timeout_simulation")
            .with_operation("SLOW_OPERATION")
            .build();
        
        timeout_span.log(LogLevel::Warn, "Simulating slow operation".to_string());
        
        // Simulate slow operation
        thread::sleep(Duration::from_millis(100));
        
        // Check if operation would have timed out in production
        let elapsed = timeout_span.metrics().successes; // Using existing field for demonstration
        if elapsed > 0 { // Simulate timeout condition
            timeout_span.log(LogLevel::Error, "Operation would have timed out".to_string());
            timeout_span.set_tag("timeout.simulated".to_string(), "true".to_string());
        }
        
        timeout_span.record_success(); // Recovery was successful
        timeout_span.finish();
        
        Ok(())
    })?;
    
    println!("   ✓ Error handling and recovery tests completed");
    Ok(())
}

/// Macro usage example
#[allow(unused_macros)]
macro_rules! custom_traced_operation {
    ($context:expr, $operation:expr, $body:block) => {{
        use lightning_db::trace_db_operation;
        trace_db_operation!($operation, $context, $body)
    }};
}