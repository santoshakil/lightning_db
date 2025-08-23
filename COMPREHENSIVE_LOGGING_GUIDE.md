# Lightning DB Comprehensive Logging and Telemetry Guide

## Overview

Lightning DB includes a state-of-the-art logging and telemetry system designed for production environments. The system provides structured logging, distributed tracing, performance monitoring, metrics collection, and sensitive data protection.

## Features

### 🔍 Structured Logging
- **Multiple Formats**: JSON, compact, pretty, and full formats
- **Configurable Outputs**: Console, file with rotation, and remote systems
- **Log Levels**: TRACE, DEBUG, INFO, WARN, ERROR with sampling
- **Contextual Information**: Automatic correlation IDs, trace IDs, and custom attributes

### 📊 Metrics Collection
- **Performance Metrics**: Operation latencies, throughput, resource utilization
- **Database Metrics**: Cache hit rates, transaction metrics, error rates
- **Histograms**: HDR histograms for accurate latency percentiles
- **Prometheus Integration**: Export metrics for monitoring systems

### 🔗 Distributed Tracing
- **OpenTelemetry Compatible**: Standard tracing with Jaeger integration
- **Context Propagation**: Automatic trace context across operations
- **Span Relationships**: Parent-child span relationships with baggage
- **Custom Attributes**: Add business-specific context to traces

### 🎯 Intelligent Sampling
- **Adaptive Sampling**: Automatic rate adjustment based on volume
- **Operation-Specific**: Different rates for different operations
- **Burst Protection**: Rate limiting during traffic spikes
- **Configuration**: Fine-tuned sampling strategies

### 🔒 Security and Privacy
- **Data Redaction**: Automatic PII and sensitive data masking
- **Configurable Patterns**: Custom regex patterns for redaction
- **Key/Value Filtering**: Selective redaction of keys or values
- **Audit Logging**: Comprehensive audit trails

### ⚡ Performance Monitoring
- **Low Overhead**: Minimal performance impact on database operations
- **Real-time Metrics**: Live performance statistics
- **Alert System**: Configurable thresholds and notifications
- **Resource Tracking**: Memory, CPU, and I/O monitoring

## Quick Start

### Basic Setup

```rust
use lightning_db::features::logging::{
    LoggingConfig, LogLevel, LogFormat, init_logging_with_config
};

// Initialize with default configuration
lightning_db::features::logging::init_logging(
    tracing::Level::INFO,
    true // JSON format
)?;

// Or use custom configuration
let config = LoggingConfig {
    level: LogLevel::Info,
    format: LogFormat::Json,
    // ... other options
    ..Default::default()
};

init_logging_with_config(config)?;
```

### Database Operations with Automatic Logging

```rust
use lightning_db::{Database, LightningDbConfig};

let db = Database::create("./mydb", LightningDbConfig::default())?;

// All operations are automatically logged with context
db.put(b"key1", b"value1")?;
let value = db.get(b"key1")?;

// Transactions are logged with correlation IDs
let tx_id = db.begin_transaction()?;
db.put_tx(tx_id, b"tx_key", b"tx_value")?;
db.commit_transaction(tx_id)?;
```

## Configuration Options

### Logging Configuration

```rust
use lightning_db::features::logging::*;
use std::time::Duration;

let config = LoggingConfig {
    level: LogLevel::Debug,
    format: LogFormat::Json,
    
    output: LogOutput {
        console: true,
        file: Some(FileOutput {
            path: "logs/lightning_db.log".to_string(),
            rotation: FileRotation::Daily,
            max_files: 30,
        }),
        jaeger: Some(JaegerConfig {
            endpoint: "http://localhost:14268/api/traces".to_string(),
            service_name: "my_app".to_string(),
            batch_timeout: Duration::from_secs(1),
            max_batch_size: 100,
        }),
        prometheus: Some(PrometheusConfig {
            endpoint: "http://localhost:9090".to_string(),
            push_interval: Duration::from_secs(30),
            registry_name: "lightning_db".to_string(),
        }),
    },
    
    // Sampling configuration
    sampling: SamplingConfig {
        enabled: true,
        trace_sample_rate: 0.1,  // Sample 10% of traces
        debug_sample_rate: 0.05, // Sample 5% of debug logs
        high_frequency_operations: vec!["get".to_string(), "cache_lookup".to_string()],
        high_frequency_sample_rate: 0.01, // Sample 1% of high-freq ops
        adaptive_sampling: AdaptiveSamplingConfig {
            enabled: true,
            target_samples_per_second: 100,
            adjustment_interval: Duration::from_secs(60),
            min_rate: 0.001,
            max_rate: 1.0,
        },
    },
    
    // Telemetry configuration
    telemetry: TelemetryConfig {
        enabled: true,
        service_name: "my_lightning_db_app".to_string(),
        service_version: "1.0.0".to_string(),
        environment: "production".to_string(),
        resource_attributes: {
            let mut attrs = std::collections::HashMap::new();
            attrs.insert("datacenter".to_string(), "us-west-1".to_string());
            attrs.insert("instance_id".to_string(), "app-001".to_string());
            attrs
        },
    },
    
    // Data redaction
    redaction: RedactionConfig {
        enabled: true,
        patterns: vec![
            r"\\b\\d{4}[-\\s]?\\d{4}[-\\s]?\\d{4}[-\\s]?\\d{4}\\b".to_string(), // Credit cards
            r"\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b".to_string(), // Emails
            r"password=\\S+".to_string(), // Passwords in URLs
        ],
        replacement: "[REDACTED]".to_string(),
        redact_keys: false,
        redact_values: true,
    },
    
    // Performance monitoring
    performance: PerformanceConfig {
        slow_operation_threshold: Duration::from_millis(100),
        enable_operation_timing: true,
        enable_memory_tracking: true,
        enable_resource_monitoring: true,
        metrics_buffer_size: 10000,
    },
    
    correlation_id_header: "x-correlation-id".to_string(),
};
```

## Advanced Usage

### Custom Trace Context

```rust
use lightning_db::features::logging::{TraceContext, with_trace_context};

// Create custom trace context
let context = TraceContext::new()
    .with_user_id("user123".to_string())
    .with_request_id("req_456".to_string())
    .with_session_id("session_789".to_string())
    .with_custom_attribute("feature_flag".to_string(), "new_algorithm".to_string());

// Execute operations within context
with_trace_context!(context, {
    db.put(b"contextual_key", b"contextual_value")?;
    let value = db.get(b"contextual_key")?;
});
```

### Custom Spans and Events

```rust
use lightning_db::features::logging::{traced_span, current_trace_context};

// Create custom span
let custom_span = traced_span!("batch_operation");
let _guard = custom_span.enter();

// Add events to span
custom_span.add_event("batch_started", vec![
    ("batch_size", "100"),
    ("operation_type", "bulk_insert"),
]);

// Perform operations
for i in 0..100 {
    db.put(format!("key_{}", i).as_bytes(), b"value")?;
}

custom_span.add_event("batch_completed", vec![
    ("success", "true"),
    ("operations_completed", "100"),
]);
```

### Performance Monitoring

```rust
use lightning_db::features::logging::{with_perf_monitoring, get_performance_stats};

// Monitor operation performance
let result = with_perf_monitoring!("complex_operation", {
    // Your complex database operations here
    for i in 0..1000 {
        db.put(format!("perf_key_{}", i).as_bytes(), b"data")?;
    }
    "completed"
});

// Get performance statistics
let stats = get_performance_stats();
for (operation, stat) in stats {
    println!("{}: {} ops, avg: {:?}, p99: {:?}", 
             operation, stat.total_operations, stat.avg_duration, stat.p99_duration);
}
```

### Metrics Export

```rust
use lightning_db::features::logging::{export_metrics_to_file, get_metrics_snapshot};

// Export metrics to JSON file
export_metrics_to_file("metrics.json")?;

// Get current metrics snapshot
let snapshot = get_metrics_snapshot();
println!("Uptime: {:?}", snapshot.uptime);
println!("Operations: {}", snapshot.operations.len());
```

## Environment Variables

The logging system can be configured via environment variables:

```bash
# Set log level
export LIGHTNING_LOG_LEVEL=debug

# Set log format
export LIGHTNING_LOG_FORMAT=json

# Set environment for telemetry
export LIGHTNING_ENV=production

# Enable/disable specific features
export LIGHTNING_SAMPLING_ENABLED=true
export LIGHTNING_TELEMETRY_ENABLED=true
export LIGHTNING_REDACTION_ENABLED=true
```

## Log Output Examples

### JSON Format (Production)
```json
{
  "timestamp": "2025-01-21T10:30:45.123Z",
  "level": "INFO",
  "target": "lightning_db",
  "fields": {
    "operation": "put",
    "key_size": 10,
    "value_size": 256,
    "duration_us": 45,
    "trace_id": "1234567890abcdef",
    "correlation_id": "req_12345",
    "user_id": "user_789"
  },
  "message": "Database operation completed"
}
```

### Pretty Format (Development)
```
2025-01-21T10:30:45.123Z  INFO lightning_db: Database operation completed
    at src/lib.rs:1085
    operation: put
    key_size: 10
    value_size: 256
    duration_us: 45
    trace_id: 1234567890abcdef
    correlation_id: req_12345
```

## Integration with Monitoring Systems

### Jaeger Tracing

1. Start Jaeger:
```bash
docker run -d --name jaeger \\
  -p 16686:16686 \\
  -p 14268:14268 \\
  jaegertracing/all-in-one:latest
```

2. Configure Lightning DB:
```rust
let config = LoggingConfig {
    output: LogOutput {
        jaeger: Some(JaegerConfig {
            endpoint: "http://localhost:14268/api/traces".to_string(),
            service_name: "my_app".to_string(),
            // ...
        }),
        // ...
    },
    // ...
};
```

3. View traces at http://localhost:16686

### Prometheus Metrics

1. Configure metrics export:
```rust
let config = LoggingConfig {
    output: LogOutput {
        prometheus: Some(PrometheusConfig {
            endpoint: "http://localhost:9090".to_string(),
            push_interval: Duration::from_secs(30),
            registry_name: "lightning_db".to_string(),
        }),
        // ...
    },
    // ...
};
```

2. Metrics are automatically exported and available for Prometheus scraping

### Available Metrics

- `lightning_db_operations_total` - Total database operations
- `lightning_db_operations_duration_seconds` - Operation duration histogram
- `lightning_db_cache_hits_total` - Cache hit count
- `lightning_db_cache_misses_total` - Cache miss count
- `lightning_db_transactions_active` - Active transaction count
- `lightning_db_memory_usage_bytes` - Current memory usage
- `lightning_db_slow_operations_total` - Slow operation count

## Best Practices

### 1. Log Level Strategy
- **TRACE**: Extremely detailed debugging (disabled in production)
- **DEBUG**: Detailed debugging information (sampled in production)
- **INFO**: General operational messages
- **WARN**: Warning conditions that should be investigated
- **ERROR**: Error conditions that require immediate attention

### 2. Sampling Configuration
- Use high sample rates (10-100%) for errors and warnings
- Use low sample rates (0.1-1%) for high-frequency operations
- Enable adaptive sampling for automatic adjustment
- Monitor sampling statistics to ensure adequate coverage

### 3. Context Propagation
- Always set correlation IDs for request tracking
- Use user IDs for user-centric analysis
- Add custom attributes for business context
- Propagate context across service boundaries

### 4. Performance Considerations
- Enable sampling to reduce log volume
- Use appropriate log levels for production
- Monitor overhead using performance metrics
- Configure reasonable buffer sizes

### 5. Security
- Enable data redaction for sensitive information
- Use custom patterns for application-specific data
- Audit log access and retention policies
- Implement proper log rotation and archival

## Troubleshooting

### High Log Volume
```rust
// Increase sampling for high-frequency operations
let mut config = LoggingConfig::default();
config.sampling.high_frequency_operations.push("get".to_string());
config.sampling.high_frequency_sample_rate = 0.001; // 0.1%
```

### Missing Traces
```rust
// Check if sampling is too aggressive
config.sampling.trace_sample_rate = 0.1; // 10%

// Ensure telemetry is enabled
config.telemetry.enabled = true;
```

### Performance Impact
```rust
// Reduce log level and increase sampling
config.level = LogLevel::Warn;
config.sampling.enabled = true;
config.performance.enable_operation_timing = false; // Disable if not needed
```

## Performance Impact

The logging system is designed for minimal performance impact:

- **Sampling**: Reduces log volume by 90-99% while maintaining observability
- **Async Processing**: Non-blocking log processing
- **Optimized Serialization**: Fast JSON serialization for structured logs
- **Lock-Free Operations**: Minimal contention on hot paths
- **Configurable Overhead**: Disable features not needed for your use case

Typical overhead: < 1% CPU, < 10MB memory for moderate traffic applications.

## Example: Complete Production Setup

```rust
use lightning_db::features::logging::*;
use std::time::Duration;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Production logging configuration
    let config = LoggingConfig {
        level: LogLevel::Info,
        format: LogFormat::Json,
        
        output: LogOutput {
            console: false, // Disable console in production
            file: Some(FileOutput {
                path: "/var/log/lightning_db".to_string(),
                rotation: FileRotation::Daily,
                max_files: 30,
            }),
            jaeger: Some(JaegerConfig {
                endpoint: "http://jaeger-collector:14268/api/traces".to_string(),
                service_name: "lightning_db_prod".to_string(),
                batch_timeout: Duration::from_secs(1),
                max_batch_size: 500,
            }),
            prometheus: Some(PrometheusConfig {
                endpoint: "http://prometheus:9090".to_string(),
                push_interval: Duration::from_secs(15),
                registry_name: "lightning_db".to_string(),
            }),
        },
        
        sampling: SamplingConfig {
            enabled: true,
            trace_sample_rate: 0.05,
            debug_sample_rate: 0.01,
            high_frequency_operations: vec![
                "get".to_string(),
                "cache_lookup".to_string(),
                "lock_acquire".to_string(),
            ],
            high_frequency_sample_rate: 0.001,
            adaptive_sampling: AdaptiveSamplingConfig {
                enabled: true,
                target_samples_per_second: 50,
                adjustment_interval: Duration::from_secs(60),
                min_rate: 0.0001,
                max_rate: 0.1,
            },
        },
        
        redaction: RedactionConfig {
            enabled: true,
            patterns: vec![
                r"\\b\\d{4}[-\\s]?\\d{4}[-\\s]?\\d{4}[-\\s]?\\d{4}\\b".to_string(),
                r"\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b".to_string(),
                r"password=\\S+".to_string(),
                r"token=\\S+".to_string(),
                r"api[_-]?key=\\S+".to_string(),
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
            metrics_buffer_size: 50000,
        },
        
        telemetry: TelemetryConfig {
            enabled: true,
            service_name: "lightning_db_prod".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            environment: "production".to_string(),
            resource_attributes: {
                let mut attrs = HashMap::new();
                attrs.insert("datacenter".to_string(), "us-west-1".to_string());
                attrs.insert("instance_type".to_string(), "c5.xlarge".to_string());
                attrs.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());
                attrs
            },
        },
        
        correlation_id_header: "x-correlation-id".to_string(),
    };
    
    // Initialize logging
    init_logging_with_config(config)?;
    
    // Your application code here
    let db = lightning_db::Database::create("./prod_db", 
                                           lightning_db::LightningDbConfig::default())?;
    
    // Operations are automatically logged with full context
    db.put(b"production_key", b"production_value")?;
    
    Ok(())
}
```

This comprehensive logging system provides production-ready observability for Lightning DB applications with minimal performance overhead and maximum flexibility.