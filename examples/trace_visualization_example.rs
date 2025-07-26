//! Distributed Tracing Visualization Example
//! 
//! Demonstrates how to use Lightning DB's distributed tracing visualization tools
//! to analyze trace data, generate HTML viewers, and create service maps.

use lightning_db::distributed_tracing::*;
use lightning_db::distributed_tracing::context::*;
use lightning_db::distributed_tracing::span::*;
use lightning_db::distributed_tracing::sampler::*;
use lightning_db::distributed_tracing::exporter::*;
use lightning_db::distributed_tracing::integration::*;
use lightning_db::distributed_tracing::visualization::*;
use lightning_db::{Database, Result};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use std::fs;

fn main() -> Result<()> {
    println!("🔍 Lightning DB Trace Visualization Example");
    
    // Initialize distributed tracing
    setup_tracing_system()?;
    
    // Create database and traced wrapper
    let database = Database::create("visualization_example.db", lightning_db::LightningDbConfig::default())?;
    let traced_db = TracedDatabase::new(database);
    
    // Create trace viewer
    let viewer = TraceViewer::new(ViewerConfig::default());
    
    println!("\n🚀 Generating sample traces...");
    generate_sample_traces(&traced_db, &viewer)?;
    
    println!("\n📊 Analyzing traces...");
    analyze_traces(&viewer)?;
    
    println!("\n🌐 Generating service map...");
    generate_service_map(&viewer)?;
    
    println!("\n🔍 Searching traces...");
    search_traces(&viewer)?;
    
    println!("\n📈 Performance analysis...");
    performance_analysis(&viewer)?;
    
    println!("\n📄 Generating HTML trace viewer...");
    generate_html_viewer(&viewer)?;
    
    println!("\n✅ Trace visualization example completed!");
    Ok(())
}

/// Set up the tracing system with exporters
fn setup_tracing_system() -> Result<()> {
    let sampler = ProbabilitySampler::new(1.0); // 100% sampling for demo
    
    let multi_exporter = MultiExporter::new()
        .add_exporter(Box::new(LoggingExporter::new(LogLevel::Info)))
        .add_exporter(Box::new(MetricsExporter::new()));
    
    let tracer = Tracer::new("visualization_demo".to_string())
        .with_sampler(Box::new(sampler))
        .with_exporter(Box::new(multi_exporter));
    
    init_tracer(tracer);
    println!("🎯 Tracing system initialized");
    Ok(())
}

/// Generate sample traces for visualization
fn generate_sample_traces(traced_db: &TracedDatabase, viewer: &TraceViewer) -> Result<()> {
    let mut all_spans = Vec::new();
    
    // Generate traces for different scenarios
    for scenario in 0..5 {
        let scenario_name = match scenario {
            0 => "user_registration",
            1 => "data_processing",
            2 => "api_request",
            3 => "batch_operation",
            _ => "error_scenario",
        };
        
        println!("   Generating traces for scenario: {}", scenario_name);
        let spans = generate_scenario_traces(traced_db, scenario_name, scenario)?;
        
        // Add spans to viewer
        viewer.add_spans(&spans)?;
        all_spans.extend(spans);
        
        // Small delay between scenarios
        thread::sleep(Duration::from_millis(100));
    }
    
    println!("   Generated {} spans across {} traces", all_spans.len(), 5);
    Ok(())
}

/// Generate traces for a specific scenario
fn generate_scenario_traces(
    traced_db: &TracedDatabase, 
    scenario: &str, 
    scenario_id: usize
) -> Result<Vec<Span>> {
    let mut context = TraceContext::new_root();
    context.set_baggage("scenario".to_string(), scenario.to_string());
    context.set_baggage("scenario_id".to_string(), scenario_id.to_string());
    
    set_current_context(context.clone());
    
    let mut spans = Vec::new();
    
    match scenario {
        "user_registration" => {
            spans.extend(simulate_user_registration(traced_db, &context)?);
        }
        "data_processing" => {
            spans.extend(simulate_data_processing(traced_db, &context)?);
        }
        "api_request" => {
            spans.extend(simulate_api_request(traced_db, &context)?);
        }
        "batch_operation" => {
            spans.extend(simulate_batch_operation(traced_db, &context)?);
        }
        "error_scenario" => {
            spans.extend(simulate_error_scenario(traced_db, &context)?);
        }
        _ => {}
    }
    
    Ok(spans)
}

/// Simulate user registration trace
fn simulate_user_registration(traced_db: &TracedDatabase, context: &TraceContext) -> Result<Vec<Span>> {
    let mut spans = Vec::new();
    
    // Root span for user registration
    let mut root_span = DatabaseSpanBuilder::new(context, "user_service.register")
        .with_operation("REGISTRATION")
        .build();
    
    root_span.set_tag("service.name".to_string(), "user_service".to_string());
    root_span.set_tag("user.email".to_string(), "alice@example.com".to_string());
    
    // Database operations
    traced_db.put_with_context(context, b"user:alice", b"alice@example.com")?;
    traced_db.put_with_context(context, b"email:alice@example.com", b"user:alice")?;
    
    // Child span for validation
    let child_context = context.create_child();
    let mut validation_span = DatabaseSpanBuilder::new(&child_context, "user_service.validate_email")
        .with_operation("VALIDATION")
        .build();
    validation_span.set_tag("validation.type".to_string(), "email".to_string());
    validation_span.record_success();
    let validation_completed = validation_span.finish();
    spans.push(validation_completed);
    
    // Child span for notification
    let notify_context = context.create_child();
    let mut notify_span = DatabaseSpanBuilder::new(&notify_context, "notification_service.send_welcome")
        .with_operation("NOTIFICATION")
        .build();
    notify_span.set_tag("service.name".to_string(), "notification_service".to_string());
    notify_span.set_tag("notification.type".to_string(), "welcome_email".to_string());
    notify_span.record_success();
    let notify_completed = notify_span.finish();
    spans.push(notify_completed);
    
    root_span.record_success();
    let root_completed = root_span.finish();
    spans.push(root_completed);
    
    Ok(spans)
}

/// Simulate data processing trace
fn simulate_data_processing(traced_db: &TracedDatabase, context: &TraceContext) -> Result<Vec<Span>> {
    let mut spans = Vec::new();
    
    let mut root_span = DatabaseSpanBuilder::new(context, "data_processor.process_batch")
        .with_operation("BATCH_PROCESSING")
        .with_record_count(1000)
        .build();
    
    root_span.set_tag("service.name".to_string(), "data_processor".to_string());
    root_span.set_tag("batch.size".to_string(), "1000".to_string());
    
    // Simulate processing steps
    for step in &["extract", "transform", "load"] {
        let step_context = context.create_child();
        let mut step_span = DatabaseSpanBuilder::new(&step_context, &format!("data_processor.{}", step))
            .with_operation("ETL_STEP")
            .build();
        
        step_span.set_tag("etl.step".to_string(), step.to_string());
        
        // Simulate some database operations
        for i in 0..5 {
            let key = format!("{}:item:{}", step, i);
            let value = format!("processed_data_{}", i);
            traced_db.put_with_context(&step_context, key.as_bytes(), value.as_bytes())?;
        }
        
        // Simulate processing time
        thread::sleep(Duration::from_millis(50));
        
        step_span.record_success();
        let step_completed = step_span.finish();
        spans.push(step_completed);
    }
    
    root_span.record_success();
    let root_completed = root_span.finish();
    spans.push(root_completed);
    
    Ok(spans)
}

/// Simulate API request trace
fn simulate_api_request(traced_db: &TracedDatabase, context: &TraceContext) -> Result<Vec<Span>> {
    let mut spans = Vec::new();
    
    let mut api_span = DatabaseSpanBuilder::new(context, "api.get_user_profile")
        .with_operation("API_CALL")
        .build();
    
    api_span.set_tag("service.name".to_string(), "api_gateway".to_string());
    api_span.set_tag("http.method".to_string(), "GET".to_string());
    api_span.set_tag("http.path".to_string(), "/api/users/123/profile".to_string());
    api_span.set_tag("http.status_code".to_string(), "200".to_string());
    
    // Database lookup
    let db_context = context.create_child();
    let mut db_span = DatabaseSpans::get(&db_context, "user:123").build();
    db_span.record_cache_hit();
    db_span.record_success();
    let db_completed = db_span.finish();
    spans.push(db_completed);
    
    // External service call (simulated)
    let external_context = context.create_child();
    let mut external_span = DatabaseSpanBuilder::new(&external_context, "external_service.get_preferences")
        .with_operation("EXTERNAL_CALL")
        .build();
    external_span.set_tag("service.name".to_string(), "preferences_service".to_string());
    external_span.set_tag("external.endpoint".to_string(), "https://prefs.example.com/api".to_string());
    external_span.record_success();
    let external_completed = external_span.finish();
    spans.push(external_completed);
    
    api_span.record_success();
    let api_completed = api_span.finish();
    spans.push(api_completed);
    
    Ok(spans)
}

/// Simulate batch operation trace
fn simulate_batch_operation(traced_db: &TracedDatabase, context: &TraceContext) -> Result<Vec<Span>> {
    let mut spans = Vec::new();
    
    let mut batch_span = DatabaseSpans::batch(context, "PUT", 50).build();
    batch_span.set_tag("service.name".to_string(), "batch_processor".to_string());
    batch_span.set_tag("operation.type".to_string(), "bulk_insert".to_string());
    
    // Simulate batch operations
    let operations: Vec<(Vec<u8>, Vec<u8>)> = (0..50)
        .map(|i| {
            let key = format!("batch_item:{}", i);
            let value = format!("{{\"id\": {}, \"processed\": true}}", i);
            (key.into_bytes(), value.into_bytes())
        })
        .collect();
    
    let operation_refs: Vec<(&[u8], &[u8])> = operations.iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    
    traced_db.batch_put_with_context(context, &operation_refs)?;
    
    batch_span.record_success();
    let batch_completed = batch_span.finish();
    spans.push(batch_completed);
    
    Ok(spans)
}

/// Simulate error scenario trace
fn simulate_error_scenario(traced_db: &TracedDatabase, context: &TraceContext) -> Result<Vec<Span>> {
    let mut spans = Vec::new();
    
    let mut error_span = DatabaseSpanBuilder::new(context, "error_service.process_request")
        .with_operation("ERROR_HANDLING")
        .build();
    
    error_span.set_tag("service.name".to_string(), "error_service".to_string());
    
    // Simulate successful operation first
    traced_db.put_with_context(context, b"temp:data", b"some data")?;
    
    // Simulate error in child span
    let error_context = context.create_child();
    let mut child_span = DatabaseSpanBuilder::new(&error_context, "error_service.validate_data")
        .with_operation("VALIDATION")
        .build();
    
    child_span.record_error("Validation failed: invalid format", Some("VALIDATION_ERROR"));
    let child_completed = child_span.finish();
    spans.push(child_completed);
    
    // Parent span handles the error
    error_span.record_error("Request processing failed", Some("PROCESSING_ERROR"));
    let error_completed = error_span.finish();
    spans.push(error_completed);
    
    Ok(spans)
}

/// Analyze traces using the viewer
fn analyze_traces(viewer: &TraceViewer) -> Result<()> {
    let trace_ids = viewer.list_traces();
    println!("   Found {} traces to analyze", trace_ids.len());
    
    for (i, trace_id) in trace_ids.iter().enumerate() {
        if let Some(trace) = viewer.get_trace(trace_id) {
            println!("   Trace {}: {} spans, {} errors, {:.2}ms duration", 
                i + 1,
                trace.span_count,
                trace.error_count,
                trace.total_duration_micros as f64 / 1000.0
            );
            
            // Show critical path
            if !trace.critical_path.is_empty() {
                println!("     Critical path: {} spans", trace.critical_path.len());
            }
            
            // Show service breakdown
            for (service, info) in &trace.service_map {
                println!("     Service {}: {} spans, {:.2}ms avg", 
                    service, 
                    info.span_count,
                    info.avg_duration_micros / 1000.0
                );
            }
        }
    }
    
    Ok(())
}

/// Generate and display service map
fn generate_service_map(viewer: &TraceViewer) -> Result<()> {
    let service_map = viewer.generate_service_map();
    
    println!("   Service Map:");
    println!("   Services: {}, Connections: {}", 
        service_map.services.len(), 
        service_map.connections.len()
    );
    
    for (name, node) in &service_map.services {
        println!("   📊 Service: {}", name);
        println!("      Request Rate: {:.1}/min", node.request_rate);
        println!("      Error Rate: {:.2}%", node.error_rate * 100.0);
        println!("      Avg Duration: {:.2}ms", node.avg_duration_micros / 1000.0);
        
        if !node.dependencies.is_empty() {
            println!("      Dependencies: {:?}", node.dependencies);
        }
    }
    
    for connection in &service_map.connections {
        println!("   🔗 {} → {}: {} requests, {:.1}% errors", 
            connection.from_service,
            connection.to_service,
            connection.request_count,
            (connection.error_count as f64 / connection.request_count as f64) * 100.0
        );
    }
    
    Ok(())
}

/// Search traces with different criteria
fn search_traces(viewer: &TraceViewer) -> Result<()> {
    // Search for traces with errors
    let error_criteria = SearchCriteria {
        service_name: None,
        operation_name: None,
        min_duration_micros: None,
        max_duration_micros: None,
        has_errors: Some(true),
        tag_filters: HashMap::new(),
        time_range: None,
    };
    
    let error_traces = viewer.search_traces(&error_criteria);
    println!("   Found {} traces with errors", error_traces.len());
    
    // Search for API traces
    let mut api_filters = HashMap::new();
    api_filters.insert("http.method".to_string(), "GET".to_string());
    
    let api_criteria = SearchCriteria {
        service_name: Some("api_gateway".to_string()),
        operation_name: None,
        min_duration_micros: None,
        max_duration_micros: None,
        has_errors: None,
        tag_filters: api_filters,
        time_range: None,
    };
    
    let api_traces = viewer.search_traces(&api_criteria);
    println!("   Found {} API traces", api_traces.len());
    
    // Search for slow traces (> 100ms)
    let slow_criteria = SearchCriteria {
        service_name: None,
        operation_name: None,
        min_duration_micros: Some(100_000), // 100ms
        max_duration_micros: None,
        has_errors: None,
        tag_filters: HashMap::new(),
        time_range: None,
    };
    
    let slow_traces = viewer.search_traces(&slow_criteria);
    println!("   Found {} slow traces (>100ms)", slow_traces.len());
    
    Ok(())
}

/// Perform performance analysis
fn performance_analysis(viewer: &TraceViewer) -> Result<()> {
    let stats = viewer.get_performance_stats();
    
    println!("   📈 Performance Statistics:");
    println!("      Total Traces: {}", stats.total_traces);
    println!("      Total Spans: {}", stats.total_spans);
    println!("      Average Trace Duration: {:.2}ms", stats.avg_trace_duration_micros / 1000.0);
    println!("      Error Rate: {:.2}%", stats.error_rate * 100.0);
    
    if !stats.slowest_traces.is_empty() {
        println!("      Slowest Traces:");
        for (i, trace_id) in stats.slowest_traces.iter().take(3).enumerate() {
            if let Some(trace) = viewer.get_trace(trace_id) {
                println!("      {}. {} ({:.2}ms)", 
                    i + 1, 
                    trace_id[..8].to_string() + "...",
                    trace.total_duration_micros as f64 / 1000.0
                );
            }
        }
    }
    
    Ok(())
}

/// Generate HTML trace viewer
fn generate_html_viewer(viewer: &TraceViewer) -> Result<()> {
    let trace_ids = viewer.list_traces();
    
    if let Some(trace_id) = trace_ids.first() {
        let html = viewer.generate_html_viewer(trace_id)?;
        
        let filename = format!("trace_viewer_{}.html", &trace_id[..8]);
        fs::write(&filename, html)?;
        
        println!("   📄 Generated HTML trace viewer: {}", filename);
        println!("      Open this file in a web browser to view the interactive trace");
        
        // Also export JSON for external tools
        let json = viewer.export_json(Some(vec![trace_id.clone()]))?;
        let json_filename = format!("trace_data_{}.json", &trace_id[..8]);
        fs::write(&json_filename, json)?;
        
        println!("   📊 Exported trace data: {}", json_filename);
    } else {
        println!("   No traces available for HTML generation");
    }
    
    Ok(())
}