//! Database Operations Integration
//!
//! Provides instrumented wrappers for database operations with automatic
//! distributed tracing support.

use super::{TraceContext};
use super::context::{current_context, with_context, DatabaseContextPropagation};
use super::span::{DatabaseSpans, DatabaseSpanGuard};
use crate::{Database, Result};
use std::collections::HashMap;

/// Instrumented database wrapper that automatically traces all operations
pub struct TracedDatabase {
    inner: Database,
}

impl TracedDatabase {
    /// Create a new traced database instance
    pub fn new(database: Database) -> Self {
        Self { inner: database }
    }
    
    /// Get a value with automatic tracing
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let context = current_context().unwrap_or_else(|| {
            DatabaseContextPropagation::create_db_context("GET", None)
        });
        
        self.get_with_context(&context, key)
    }
    
    /// Get a value with explicit trace context
    pub fn get_with_context(&self, context: &TraceContext, key: &[u8]) -> Result<Option<Vec<u8>>> {
        with_context(context.clone(), || {
            let key_str = String::from_utf8_lossy(key);
            let mut span_guard = DatabaseSpanGuard::new(
                DatabaseSpans::get(context, &key_str).build()
            );
            
            span_guard.record_io_operation("read", key.len());
            
            match self.inner.get(key) {
                Ok(Some(value)) => {
                    span_guard.record_cache_hit();
                    span_guard.record_success();
                    
                    if let Some(completed) = span_guard.finish() {
                        if completed.duration_micros() > 1000 { // Log slow operations
                            println!("SLOW GET: {} took {}μs", key_str, completed.duration_micros());
                        }
                    }
                    
                    Ok(Some(value))
                }
                Ok(None) => {
                    span_guard.record_cache_miss();
                    span_guard.record_success();
                    Ok(None)
                }
                Err(e) => {
                    span_guard.record_error(&format!("GET failed: {}", e), Some("GET_ERROR"));
                    Err(e)
                }
            }
        })
    }
    
    /// Put a value with automatic tracing
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let context = current_context().unwrap_or_else(|| {
            DatabaseContextPropagation::create_db_context("PUT", None)
        });
        
        self.put_with_context(&context, key, value)
    }
    
    /// Put a value with explicit trace context
    pub fn put_with_context(&self, context: &TraceContext, key: &[u8], value: &[u8]) -> Result<()> {
        with_context(context.clone(), || {
            let key_str = String::from_utf8_lossy(key);
            let mut span_guard = DatabaseSpanGuard::new(
                DatabaseSpans::put(context, &key_str, value.len()).build()
            );
            
            // Record operation metrics
            span_guard.record_io_operation("write", key.len() + value.len());
            // Memory allocation would be recorded automatically by the span
            
            match self.inner.put(key, value) {
                Ok(_) => {
                    span_guard.record_success();
                    
                    if let Some(completed) = span_guard.finish() {
                        if completed.duration_micros() > 5000 { // Log slow writes
                            println!("SLOW PUT: {} took {}μs", key_str, completed.duration_micros());
                        }
                    }
                    
                    Ok(())
                }
                Err(e) => {
                    span_guard.record_error(&format!("PUT failed: {}", e), Some("PUT_ERROR"));
                    Err(e)
                }
            }
        })
    }
    
    /// Delete a value with automatic tracing
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let context = current_context().unwrap_or_else(|| {
            DatabaseContextPropagation::create_db_context("DELETE", None)
        });
        
        self.delete_with_context(&context, key)
    }
    
    /// Delete a value with explicit trace context
    pub fn delete_with_context(&self, context: &TraceContext, key: &[u8]) -> Result<bool> {
        with_context(context.clone(), || {
            let key_str = String::from_utf8_lossy(key);
            let mut span_guard = DatabaseSpanGuard::new(
                DatabaseSpans::delete(context, &key_str).build()
            );
            
            span_guard.record_io_operation("delete", key.len());
            
            match self.inner.delete(key) {
                Ok(existed) => {
                    span_guard.record_success();
                    
                    if existed {
                        span_guard.record_cache_hit(); // Key existed
                    } else {
                        span_guard.record_cache_miss(); // Key didn't exist
                    }
                    
                    Ok(existed)
                }
                Err(e) => {
                    span_guard.record_error(&format!("DELETE failed: {}", e), Some("DELETE_ERROR"));
                    Err(e)
                }
            }
        })
    }
    
    /// Scan operations with automatic tracing
    pub fn scan<F>(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>, callback: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let context = current_context().unwrap_or_else(|| {
            DatabaseContextPropagation::create_query_context("SCAN", 
                start_key.map(|k| String::from_utf8_lossy(k).into_owned()).as_deref())
        });
        
        self.scan_with_context(&context, start_key, end_key, callback)
    }
    
    /// Scan operations with explicit trace context
    pub fn scan_with_context<F>(&self, context: &TraceContext, start_key: Option<&[u8]>, 
                                end_key: Option<&[u8]>, mut callback: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        with_context(context.clone(), || {
            let start_key_str = start_key.map(|k| String::from_utf8_lossy(k).to_string());
            let end_key_str = end_key.map(|k| String::from_utf8_lossy(k).to_string());
            
            let mut span_guard = DatabaseSpanGuard::new(
                DatabaseSpans::scan(context, 
                    start_key_str.as_deref(), 
                    end_key_str.as_deref()
                ).build()
            );
            
            let mut scanned_count = 0;
            let mut total_bytes = 0;
            
            // Convert Option<&[u8]> to Option<Vec<u8>> for the scan API
            let start_key_vec = start_key.map(|k| k.to_vec());
            let end_key_vec = end_key.map(|k| k.to_vec());
            
            let result = self.inner.scan(start_key_vec, end_key_vec).and_then(|iterator| {
                for result in iterator {
                    let (key, value) = result?;
                    scanned_count += 1;
                    total_bytes += key.len() + value.len();
                    
                    // Create a child span for each record processed
                    if scanned_count % 1000 == 0 { // Trace every 1000th record to avoid overhead
                        let child_context = context.create_child();
                        let mut record_span = DatabaseSpans::get(&child_context, 
                            &String::from_utf8_lossy(&key))
                            .with_record_count(scanned_count)
                            .build();
                        
                        record_span.record_io_operation("scan_read", key.len() + value.len());
                        record_span.record_success();
                        record_span.finish();
                    }
                    
                    // Call the user callback
                    if !callback(&key, &value)? {
                        break;
                    }
                }
                Ok(())
            });
            
            // Record scan metrics
            span_guard.record_io_operation("scan", total_bytes);
            
            match result {
                Ok(_) => {
                    span_guard.record_success();
                    
                    if let Some(completed) = span_guard.finish() {
                        println!("SCAN completed: {} records, {} bytes in {}μs", 
                               scanned_count, total_bytes, completed.duration_micros());
                    }
                    
                    Ok(())
                }
                Err(e) => {
                    span_guard.record_error(&format!("SCAN failed: {}", e), Some("SCAN_ERROR"));
                    Err(e)
                }
            }
        })
    }
    
    /// Batch operations with automatic tracing
    pub fn batch_put(&self, operations: &[(&[u8], &[u8])]) -> Result<()> {
        let context = current_context().unwrap_or_else(|| {
            DatabaseContextPropagation::create_db_context("BATCH_PUT", None)
        });
        
        self.batch_put_with_context(&context, operations)
    }
    
    /// Batch operations with explicit trace context
    pub fn batch_put_with_context(&self, context: &TraceContext, operations: &[(&[u8], &[u8])]) -> Result<()> {
        with_context(context.clone(), || {
            let mut span_guard = DatabaseSpanGuard::new(
                DatabaseSpans::batch(context, "PUT", operations.len()).build()
            );
            
            let total_bytes: usize = operations.iter()
                .map(|(k, v)| k.len() + v.len())
                .sum();
            
            span_guard.record_io_operation("batch_write", total_bytes);
            // Memory allocation would be recorded automatically by the span
            
            let mut successful_ops = 0;
            let mut failed_ops = 0;
            
            for (i, (key, value)) in operations.iter().enumerate() {
                // Create child spans for large batches (every 100th operation)
                if operations.len() > 100 && i % 100 == 0 {
                    let child_context = context.create_child();
                    let mut op_span = DatabaseSpans::put(&child_context, 
                        &String::from_utf8_lossy(key), value.len())
                        .with_record_count(i + 1)
                        .build();
                    
                    match self.inner.put(key, value) {
                        Ok(_) => {
                            op_span.record_success();
                            successful_ops += 1;
                        }
                        Err(e) => {
                            op_span.record_error(&format!("Batch operation {} failed: {}", i, e), 
                                                Some("BATCH_OP_ERROR"));
                            failed_ops += 1;
                        }
                    }
                    
                    op_span.finish();
                } else {
                    match self.inner.put(key, value) {
                        Ok(_) => successful_ops += 1,
                        Err(_) => failed_ops += 1,
                    }
                }
            }
            
            if failed_ops == 0 {
                span_guard.record_success();
            } else {
                span_guard.record_error(&format!("Batch operation had {} failures out of {} operations", 
                                                failed_ops, operations.len()), Some("BATCH_PARTIAL_ERROR"));
            }
            
            if let Some(completed) = span_guard.finish() {
                println!("BATCH PUT: {}/{} operations successful in {}μs", 
                       successful_ops, operations.len(), completed.duration_micros());
            }
            
            if failed_ops > 0 {
                Err(crate::Error::Generic(format!("Batch operation had {} failures", failed_ops)))
            } else {
                Ok(())
            }
        })
    }
    
    /// Access the underlying database
    pub fn inner(&self) -> &Database {
        &self.inner
    }
}

/// Macro for automatically instrumenting database operations
#[macro_export]
macro_rules! trace_db_operation {
    ($operation:expr, $context:expr, $body:block) => {{
        use $crate::distributed_tracing::context::with_context;
        use $crate::distributed_tracing::span::{DatabaseSpanBuilder, DatabaseSpanGuard, LogLevel};
        
        with_context($context.clone(), || {
            let mut span_guard = DatabaseSpanGuard::new(
                DatabaseSpanBuilder::new($context, $operation).build()
            );
            
            let result = $body;
            
            match &result {
                Ok(_) => span_guard.record_success(),
                Err(e) => span_guard.record_error(&format!("Operation failed: {}", e), None),
            }
            
            result
        })
    }};
}

/// Helper for creating baggage for HTTP requests
pub fn create_http_baggage(
    method: &str,
    path: &str,
    user_id: Option<&str>,
    request_id: Option<&str>,
) -> HashMap<String, String> {
    let mut baggage = HashMap::new();
    
    baggage.insert("http.method".to_string(), method.to_string());
    baggage.insert("http.path".to_string(), path.to_string());
    
    if let Some(uid) = user_id {
        baggage.insert("user.id".to_string(), uid.to_string());
    }
    
    if let Some(rid) = request_id {
        baggage.insert("request.id".to_string(), rid.to_string());
    }
    
    baggage.insert("service.name".to_string(), "lightning_db".to_string());
    baggage.insert("service.version".to_string(), env!("CARGO_PKG_VERSION").to_string());
    
    baggage
}

/// Helper for adding performance thresholds to spans
pub trait SpanPerformanceExt {
    fn with_performance_thresholds(self, warn_micros: u64, error_micros: u64) -> Self;
}

impl SpanPerformanceExt for super::span::DatabaseSpanBuilder {
    fn with_performance_thresholds(mut self, warn_micros: u64, error_micros: u64) -> Self {
        self = self.with_key_pattern(&format!("perf_warn:{}", warn_micros));
        self = self.with_data_size(error_micros as usize);
        self
    }
}

/// Performance monitoring utilities
pub struct PerformanceMonitor;

impl PerformanceMonitor {
    /// Check if an operation exceeded performance thresholds
    pub fn check_performance(operation: &str, duration_micros: u64) -> Option<String> {
        let thresholds = match operation {
            "db.get" => (100, 1000),           // 100μs warn, 1ms error
            "db.put" => (500, 5000),           // 500μs warn, 5ms error
            "db.delete" => (200, 2000),        // 200μs warn, 2ms error
            "db.scan" => (10000, 100000),      // 10ms warn, 100ms error
            "db.batch" => (50000, 500000),     // 50ms warn, 500ms error
            _ => return None,
        };
        
        if duration_micros > thresholds.1 {
            Some(format!("PERFORMANCE ERROR: {} took {}μs (threshold: {}μs)", 
                        operation, duration_micros, thresholds.1))
        } else if duration_micros > thresholds.0 {
            Some(format!("PERFORMANCE WARNING: {} took {}μs (threshold: {}μs)", 
                        operation, duration_micros, thresholds.0))
        } else {
            None
        }
    }
    
    /// Get recommended batch size based on operation type
    pub fn recommended_batch_size(operation: &str) -> usize {
        match operation {
            "db.put" => 100,
            "db.delete" => 50,
            "db.get" => 200,
            _ => 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_tracing::TraceContext;
    use tempfile::NamedTempFile;

    #[test]
    fn test_traced_database_operations() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let database = Database::create(temp_file.path(), crate::LightningDbConfig::default())?;
        let traced_db = TracedDatabase::new(database);
        
        let context = TraceContext::new_root();
        
        // Test PUT operation
        traced_db.put_with_context(&context, b"test_key", b"test_value")?;
        
        // Test GET operation
        let result = traced_db.get_with_context(&context, b"test_key")?;
        assert_eq!(result, Some(b"test_value".to_vec()));
        
        // Test DELETE operation
        let existed = traced_db.delete_with_context(&context, b"test_key")?;
        assert!(existed);
        
        // Test GET after delete
        let result = traced_db.get_with_context(&context, b"test_key")?;
        assert_eq!(result, None);
        
        Ok(())
    }
    
    #[test]
    fn test_batch_operations() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let database = Database::create(temp_file.path(), crate::LightningDbConfig::default())?;
        let traced_db = TracedDatabase::new(database);
        
        let context = TraceContext::new_root();
        
        let operations = vec![
            (b"key1".as_slice(), b"value1".as_slice()),
            (b"key2".as_slice(), b"value2".as_slice()),
            (b"key3".as_slice(), b"value3".as_slice()),
        ];
        
        traced_db.batch_put_with_context(&context, &operations)?;
        
        // Verify all keys were inserted
        for (key, expected_value) in operations {
            let result = traced_db.get_with_context(&context, key)?;
            assert_eq!(result, Some(expected_value.to_vec()));
        }
        
        Ok(())
    }
    
    #[test]
    fn test_performance_monitor() {
        assert!(PerformanceMonitor::check_performance("db.get", 50).is_none());
        assert!(PerformanceMonitor::check_performance("db.get", 150).is_some());
        assert!(PerformanceMonitor::check_performance("db.get", 1500).is_some());
        
        assert_eq!(PerformanceMonitor::recommended_batch_size("db.put"), 100);
        assert_eq!(PerformanceMonitor::recommended_batch_size("db.delete"), 50);
    }
    
    #[test]
    fn test_http_baggage_creation() {
        let baggage = create_http_baggage("POST", "/api/users", Some("123"), Some("req-456"));
        
        assert_eq!(baggage.get("http.method"), Some(&"POST".to_string()));
        assert_eq!(baggage.get("http.path"), Some(&"/api/users".to_string()));
        assert_eq!(baggage.get("user.id"), Some(&"123".to_string()));
        assert_eq!(baggage.get("request.id"), Some(&"req-456".to_string()));
    }
}