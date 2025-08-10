//! Distributed Tracing Integration Example
//!
//! Demonstrates how to use Lightning DB's distributed tracing system
//! to trace database operations across different components.

use lightning_db::distributed_tracing::context::*;
use lightning_db::distributed_tracing::exporter::{
    BatchExporter, FileExporter, FileFormat, LogLevel as ExporterLogLevel, LoggingExporter,
    MetricsExporter, MultiExporter,
};
use lightning_db::distributed_tracing::sampler::*;
use lightning_db::distributed_tracing::span::*;
use lightning_db::distributed_tracing::*;
use lightning_db::{Database, Result};
use std::collections::HashMap;

/// Example showing basic distributed tracing usage
fn main() -> Result<()> {
    println!("🔍 Lightning DB Distributed Tracing Example");

    // Initialize the tracing system
    setup_distributed_tracing()?;

    // Create a database instance
    let database = Database::create(
        "example_traced.db",
        lightning_db::LightningDbConfig::default(),
    )?;

    // Example 1: Single operation tracing
    println!("\n📊 Example 1: Basic Operation Tracing");
    trace_basic_operations(&database)?;

    // Example 2: Nested operation tracing
    println!("\n🔗 Example 2: Nested Operations with Context Propagation");
    trace_nested_operations(&database)?;

    // Example 3: Cross-service tracing
    println!("\n🌐 Example 3: Cross-Service Request Tracing");
    trace_cross_service_request(&database)?;

    // Example 4: Transaction tracing
    println!("\n💰 Example 4: Transaction Tracing");
    trace_transaction_operations(&database)?;

    // Example 5: Error tracing
    println!("\n❌ Example 5: Error Tracing and Recovery");
    trace_error_scenarios(&database)?;

    // Example 6: Performance monitoring
    println!("\n⚡ Example 6: Performance Monitoring");
    trace_performance_monitoring(&database)?;

    println!("\n✅ All distributed tracing examples completed!");
    Ok(())
}

/// Set up the distributed tracing system with exporters and sampling
fn setup_distributed_tracing() -> Result<()> {
    // Create a simple probability sampler for this example
    let sampler = ProbabilitySampler::new(0.1); // 10% sampling

    // Create multiple exporters
    let multi_exporter = MultiExporter::new()
        // Console logging for development
        .add_exporter(Box::new(LoggingExporter::new(ExporterLogLevel::Info)))
        // File export for persistence
        .add_exporter(Box::new(FileExporter::new(
            "traces.jsonl".to_string(),
            FileFormat::JsonLines,
        )?))
        // Metrics collection
        .add_exporter(Box::new(MetricsExporter::new()))
        // Batch processing for efficiency
        .with_fail_fast(false);

    let batch_exporter = BatchExporter::new(
        Box::new(multi_exporter),
        50,   // Batch size
        5000, // 5 second timeout
    );

    // Initialize the global tracer
    let tracer = Tracer::new("lightning_db_service".to_string())
        .with_sampler(Box::new(sampler))
        .with_exporter(Box::new(batch_exporter));

    init_tracer(tracer);
    println!("🚀 Distributed tracing system initialized");
    Ok(())
}

/// Example 1: Basic operation tracing
fn trace_basic_operations(database: &Database) -> Result<()> {
    // Create a root trace context for this operation
    let context = TraceContext::new_root();

    // Set the context for the current thread
    set_current_context(context.clone());

    // Create a database span for a PUT operation
    let mut span = DatabaseSpans::put(&context, "user:123", 256).build();

    // Record operation details
    span.set_tag("operation.type".to_string(), "user_creation".to_string());
    span.set_tag("user.id".to_string(), "123".to_string());
    span.log(LogLevel::Info, "Starting user creation".to_string());

    // Simulate database operation
    let key = b"user:123";
    let value = br#"{"id": 123, "name": "Alice", "email": "alice@example.com"}"#;

    // Record I/O operation
    span.record_io_operation("write", value.len());

    // Perform the actual database operation
    match database.put(key, value) {
        Ok(_) => {
            span.record_success();
            span.log(LogLevel::Info, "User created successfully".to_string());
        }
        Err(e) => {
            span.record_error(
                &format!("Failed to create user: {}", e),
                Some("DB_WRITE_ERROR"),
            );
        }
    }

    // Finish the span (automatically done when span goes out of scope)
    let completed_span = span.finish();
    println!(
        "   ✓ Basic operation traced - Duration: {}μs",
        completed_span.duration_micros()
    );

    Ok(())
}

/// Example 2: Nested operations with context propagation
fn trace_nested_operations(database: &Database) -> Result<()> {
    // Create a parent context for the user service operation
    let mut parent_context = TraceContext::new_root();
    parent_context.set_baggage("service".to_string(), "user_service".to_string());
    parent_context.set_baggage("request_id".to_string(), "req_456".to_string());

    // Execute the parent operation
    with_context(parent_context.clone(), || -> Result<()> {
        let mut parent_span = DatabaseSpanBuilder::new(&parent_context, "user_service.get_profile")
            .with_operation("COMPOSITE")
            .build();

        parent_span.log(
            LogLevel::Info,
            "Starting user profile retrieval".to_string(),
        );

        // Child operation 1: Get user data
        let _user_data = get_user_data(database, "user:123")?;
        parent_span.log(LogLevel::Debug, "Retrieved user data".to_string());

        // Child operation 2: Get user preferences
        let _preferences = get_user_preferences(database, "preferences:123")?;
        parent_span.log(LogLevel::Debug, "Retrieved user preferences".to_string());

        // Child operation 3: Get user activity
        let _activity = get_user_activity(database, "activity:123")?;
        parent_span.log(LogLevel::Debug, "Retrieved user activity".to_string());

        parent_span.record_success();
        parent_span.log(
            LogLevel::Info,
            "User profile retrieved successfully".to_string(),
        );

        let completed = parent_span.finish();
        println!(
            "   ✓ Nested operations traced - Total duration: {}μs",
            completed.duration_micros()
        );

        Ok(())
    })
}

/// Child operation: Get user data
fn get_user_data(database: &Database, key: &str) -> Result<Vec<u8>> {
    let current_context = current_context().unwrap_or_else(TraceContext::new_root);
    let child_context = current_context.create_child();

    let mut span = DatabaseSpans::get(&child_context, key).build();
    span.set_tag("data_type".to_string(), "user_profile".to_string());

    match database.get(key.as_bytes()) {
        Ok(Some(data)) => {
            span.record_cache_hit(); // Simulate cache behavior
            span.record_success();
            let _completed = span.finish();
            Ok(data)
        }
        Ok(None) => {
            span.record_cache_miss();
            span.record_error("User not found", Some("NOT_FOUND"));
            let _completed = span.finish();
            Err(lightning_db::Error::Generic("Key not found".to_string()))
        }
        Err(e) => {
            span.record_error(&format!("Database error: {}", e), Some("DB_ERROR"));
            let _completed = span.finish();
            Err(e)
        }
    }
}

/// Child operation: Get user preferences
fn get_user_preferences(database: &Database, key: &str) -> Result<Vec<u8>> {
    let current_context = current_context().unwrap_or_else(TraceContext::new_root);
    let child_context = current_context.create_child();

    let mut span = DatabaseSpans::get(&child_context, key)
        .with_table("preferences")
        .build();

    // Simulate getting preferences (with default if not found)
    match database.get(key.as_bytes()) {
        Ok(Some(data)) => {
            span.record_success();
            let _completed = span.finish();
            Ok(data)
        }
        Ok(None) => {
            // Return default preferences
            span.log(LogLevel::Info, "Using default preferences".to_string());
            span.record_success();
            let _completed = span.finish();
            Ok(br#"{"theme": "dark", "notifications": true}"#.to_vec())
        }
        Err(e) => {
            span.record_error(&format!("Database error: {}", e), Some("DB_ERROR"));
            let _completed = span.finish();
            Err(e)
        }
    }
}

/// Child operation: Get user activity
fn get_user_activity(_database: &Database, key: &str) -> Result<Vec<u8>> {
    let current_context = current_context().unwrap_or_else(TraceContext::new_root);
    let child_context = current_context.create_child();

    let mut span = DatabaseSpans::get(&child_context, key)
        .with_table("activity")
        .build();

    // Simulate a scan operation for recent activity
    span.set_tag("scan.type".to_string(), "recent_activity".to_string());
    span.record_io_operation("scan", 1024); // Simulate scanning 1KB of data

    span.record_success();
    let _completed = span.finish();
    Ok(br#"{"last_login": "2024-01-15T10:30:00Z", "actions": 42}"#.to_vec())
}

/// Example 3: Cross-service request tracing
fn trace_cross_service_request(database: &Database) -> Result<()> {
    // Simulate receiving a request with trace headers
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
    );
    headers.insert(
        "baggage".to_string(),
        "userId=alice,serverNode=db-1,environment=production".to_string(),
    );

    // Extract trace context from headers
    let context = extract_from_headers(&headers)?.unwrap_or_else(TraceContext::new_root);

    with_context(context.clone(), || -> Result<()> {
        let mut span = DatabaseSpanBuilder::new(&context, "api.process_request")
            .with_operation("API_CALL")
            .build();

        // Log the request context
        span.set_tag("http.method".to_string(), "POST".to_string());
        span.set_tag("http.url".to_string(), "/api/users/123/update".to_string());

        // Extract baggage information
        if let Some(user_id) = context.get_baggage("userId") {
            span.set_tag("user.id".to_string(), user_id.clone());
        }
        if let Some(server_node) = context.get_baggage("serverNode") {
            span.set_tag("server.node".to_string(), server_node.clone());
        }

        span.log(
            LogLevel::Info,
            "Processing cross-service request".to_string(),
        );

        // Simulate updating user data
        let key = b"user:123";
        let value =
            br#"{"id": 123, "name": "Alice Updated", "last_modified": "2024-01-15T10:30:00Z"}"#;

        span.record_io_operation("write", value.len());

        match database.put(key, value) {
            Ok(_) => {
                span.record_success();
                span.log(LogLevel::Info, "User data updated successfully".to_string());
            }
            Err(e) => {
                span.record_error(
                    &format!("Failed to update user: {}", e),
                    Some("UPDATE_ERROR"),
                );
            }
        }

        // Prepare response headers with trace context
        let mut response_headers = HashMap::new();
        inject_into_headers(&context, &mut response_headers);

        span.set_tag("response.trace_id".to_string(), context.trace_id.clone());

        let completed = span.finish();
        println!(
            "   ✓ Cross-service request traced - Trace ID: {}",
            context.trace_id
        );
        println!("     Duration: {}μs", completed.duration_micros());

        Ok(())
    })
}

/// Example 4: Transaction tracing
fn trace_transaction_operations(database: &Database) -> Result<()> {
    let mut context = TraceContext::new_root();
    context.set_baggage("operation_type".to_string(), "money_transfer".to_string());

    with_context(context.clone(), || -> Result<()> {
        let mut tx_span = DatabaseSpans::transaction(&context, 12345, "transfer")
            .with_record_count(2) // Two account updates
            .build();

        tx_span.set_tag("transfer.from".to_string(), "account:100".to_string());
        tx_span.set_tag("transfer.to".to_string(), "account:200".to_string());
        tx_span.set_tag("transfer.amount".to_string(), "500.00".to_string());

        tx_span.log(
            LogLevel::Info,
            "Starting money transfer transaction".to_string(),
        );

        // Simulate transaction operations
        let operations = vec![
            ("account:100", br#"{"balance": 1500.00}"#),
            ("account:200", br#"{"balance": 2500.00}"#),
        ];

        for (key, value) in operations {
            // Create child span for each operation
            let child_context = context.create_child();
            let mut op_span = DatabaseSpans::put(&child_context, key, value.len())
                .with_transaction_id(12345)
                .build();

            op_span.record_io_operation("write", value.len());

            match database.put(key.as_bytes(), value) {
                Ok(_) => {
                    op_span.record_success();
                    op_span.log(LogLevel::Debug, format!("Updated {}", key));
                }
                Err(e) => {
                    op_span.record_error(
                        &format!("Failed to update {}: {}", key, e),
                        Some("TX_ERROR"),
                    );
                    tx_span.record_error(
                        &format!("Transaction failed at {}", key),
                        Some("TX_ROLLBACK"),
                    );

                    let _op_completed = op_span.finish();
                    let _tx_completed = tx_span.finish();
                    return Err(e);
                }
            }

            let _op_completed = op_span.finish();
        }

        // Commit transaction
        tx_span.log(LogLevel::Info, "Committing transaction".to_string());
        tx_span.record_success();

        let completed = tx_span.finish();
        println!(
            "   ✓ Transaction traced - TX ID: 12345, Duration: {}μs",
            completed.duration_micros()
        );

        Ok(())
    })
}

/// Example 5: Error tracing and recovery
fn trace_error_scenarios(database: &Database) -> Result<()> {
    let context = TraceContext::new_root();

    with_context(context.clone(), || -> Result<()> {
        let mut span = DatabaseSpanBuilder::new(&context, "error_handling_example")
            .with_operation("ERROR_SIMULATION")
            .build();

        span.log(LogLevel::Info, "Simulating error scenarios".to_string());

        // Scenario 1: Trying to get non-existent key
        let child_context = context.create_child();
        let mut get_span = DatabaseSpans::get(&child_context, "nonexistent:key").build();

        match database.get(b"nonexistent:key") {
            Ok(Some(_)) => {
                get_span.record_success();
            }
            Ok(None) => {
                get_span.record_cache_miss();
                get_span.log(LogLevel::Warn, "Key not found".to_string());
                // This is expected, so not really an error
                get_span.record_success();
            }
            Err(e) => {
                get_span.record_error(&format!("Database error: {}", e), Some("GET_ERROR"));
            }
        }
        let _get_completed = get_span.finish();

        // Scenario 2: Recovery operation
        span.log(LogLevel::Info, "Performing recovery operation".to_string());
        let recovery_context = context.create_child();
        let mut recovery_span = DatabaseSpans::recovery(&recovery_context, "key_recovery").build();

        // Simulate successful recovery
        recovery_span.log(
            LogLevel::Info,
            "Recovery completed successfully".to_string(),
        );
        recovery_span.record_success();
        let _recovery_completed = recovery_span.finish();

        span.record_success();
        let completed = span.finish();
        println!(
            "   ✓ Error scenarios traced - Duration: {}μs",
            completed.duration_micros()
        );

        Ok(())
    })
}

/// Example 6: Performance monitoring
fn trace_performance_monitoring(database: &Database) -> Result<()> {
    let mut context = TraceContext::new_root();
    context.set_baggage(
        "performance_test".to_string(),
        "batch_operations".to_string(),
    );

    with_context(context.clone(), || -> Result<()> {
        let mut batch_span = DatabaseSpans::batch(&context, "PUT", 100).build();

        batch_span.set_tag("batch.size".to_string(), "100".to_string());
        batch_span.set_tag(
            "test.type".to_string(),
            "performance_monitoring".to_string(),
        );

        batch_span.log(
            LogLevel::Info,
            "Starting batch performance test".to_string(),
        );

        let mut total_bytes = 0;

        // Simulate batch operations
        for i in 0..100 {
            let key = format!("perf_test:{:03}", i);
            let value = format!(r#"{{"id": {}, "data": "performance test data"}}"#, i);
            total_bytes += value.len();

            // Every 10th operation, create a child span for detailed tracing
            if i % 10 == 0 {
                let child_context = context.create_child();
                let mut op_span = DatabaseSpans::put(&child_context, &key, value.len()).build();
                op_span.set_tag("batch.position".to_string(), i.to_string());
                op_span.record_io_operation("write", value.len());

                match database.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => op_span.record_success(),
                    Err(e) => op_span.record_error(
                        &format!("Batch operation failed: {}", e),
                        Some("BATCH_ERROR"),
                    ),
                }

                let _op_completed = op_span.finish();
            } else {
                // For other operations, just do the database call without individual tracing
                let _ = database.put(key.as_bytes(), value.as_bytes());
            }
        }

        batch_span.record_io_operation("batch_write", total_bytes);
        batch_span.record_memory_allocation(total_bytes * 2); // Estimate
        batch_span.record_success();

        let completed = batch_span.finish();
        println!(
            "   ✓ Performance monitoring traced - {} operations in {}μs",
            100,
            completed.duration_micros()
        );
        println!(
            "     Throughput: {:.2} MB/s",
            (total_bytes as f64 / 1024.0 / 1024.0)
                / (completed.duration_micros() as f64 / 1_000_000.0)
        );

        Ok(())
    })
}
