//! Span Management and Database-Specific Spans
//!
//! Provides database-specific span implementations for common operations
//! with automatic metrics collection and error handling.

use super::{Span, SpanKind, SpanStatus, LogLevel, TraceContext};
use std::collections::HashMap;
use std::time::{Instant, Duration};

/// Database-specific span builder with predefined tags and semantics
pub struct DatabaseSpanBuilder {
    span: Span,
}

impl DatabaseSpanBuilder {
    /// Create a new database span
    pub fn new(context: &TraceContext, operation: &str) -> Self {
        let mut span = Span::new(context, operation.to_string());
        
        // Set standard database tags
        span.set_tag("component".to_string(), "lightning_db".to_string());
        span.set_tag("db.type".to_string(), "embedded".to_string());
        span.set_tag("db.system".to_string(), "lightning_db".to_string());
        
        Self { span }
    }
    
    /// Set database operation type
    pub fn with_operation(mut self, operation: &str) -> Self {
        self.span.set_tag("db.operation".to_string(), operation.to_string());
        self
    }
    
    /// Set table/collection name
    pub fn with_table(mut self, table: &str) -> Self {
        self.span.set_tag("db.table".to_string(), table.to_string());
        self
    }
    
    /// Set key pattern for queries
    pub fn with_key_pattern(mut self, pattern: &str) -> Self {
        self.span.set_tag("db.key_pattern".to_string(), pattern.to_string());
        self
    }
    
    /// Set transaction ID
    pub fn with_transaction_id(mut self, tx_id: u64) -> Self {
        self.span.set_tag("db.transaction_id".to_string(), tx_id.to_string());
        self
    }
    
    /// Set record count (for bulk operations)
    pub fn with_record_count(mut self, count: usize) -> Self {
        self.span.set_tag("db.record_count".to_string(), count.to_string());
        self
    }
    
    /// Set data size in bytes
    pub fn with_data_size(mut self, size_bytes: usize) -> Self {
        self.span.set_tag("db.data_size_bytes".to_string(), size_bytes.to_string());
        self
    }
    
    /// Set span kind
    pub fn with_kind(mut self, kind: SpanKind) -> Self {
        self.span.set_kind(kind);
        self
    }
    
    /// Build the span
    pub fn build(self) -> DatabaseSpan {
        DatabaseSpan::new(self.span)
    }
}

/// Enhanced span for database operations with automatic metrics
pub struct DatabaseSpan {
    span: Span,
    start_time: Instant,
    metrics: OperationMetrics,
}

impl DatabaseSpan {
    fn new(span: Span) -> Self {
        Self {
            span,
            start_time: Instant::now(),
            metrics: OperationMetrics::new(),
        }
    }
    
    /// Record cache hit
    pub fn record_cache_hit(&mut self) {
        self.metrics.cache_hits += 1;
        self.span.set_tag("db.cache_hit".to_string(), "true".to_string());
    }
    
    /// Record cache miss
    pub fn record_cache_miss(&mut self) {
        self.metrics.cache_misses += 1;
        self.span.set_tag("db.cache_hit".to_string(), "false".to_string());
    }
    
    /// Record I/O operation
    pub fn record_io_operation(&mut self, operation_type: &str, bytes: usize) {
        self.metrics.io_operations += 1;
        self.metrics.bytes_processed += bytes;
        
        let mut fields = HashMap::new();
        fields.insert("io_type".to_string(), operation_type.to_string());
        fields.insert("bytes".to_string(), bytes.to_string());
        
        self.span.log(LogLevel::Debug, 
            format!("I/O operation: {} ({} bytes)", operation_type, bytes),
            fields
        );
    }
    
    /// Record memory allocation
    pub fn record_memory_allocation(&mut self, bytes: usize) {
        self.metrics.memory_allocated += bytes;
        self.span.set_tag("db.memory_allocated".to_string(), bytes.to_string());
    }
    
    /// Record error with automatic status setting
    pub fn record_error(&mut self, error: &str, error_code: Option<&str>) {
        self.span.log_error(error);
        
        if let Some(code) = error_code {
            self.span.set_tag("error.code".to_string(), code.to_string());
        }
        
        self.metrics.errors += 1;
    }
    
    /// Record successful operation
    pub fn record_success(&mut self) {
        self.span.status = SpanStatus::Ok;
        self.metrics.successes += 1;
    }
    
    /// Set custom tag
    pub fn set_tag(&mut self, key: String, value: String) {
        self.span.set_tag(key, value);
    }
    
    /// Add log entry
    pub fn log(&mut self, level: LogLevel, message: String) {
        self.span.log(level, message, HashMap::new());
    }
    
    /// Add log entry with fields
    pub fn log_with_fields(&mut self, level: LogLevel, message: String, fields: HashMap<String, String>) {
        self.span.log(level, message, fields);
    }
    
    /// Finish the span with metrics
    pub fn finish(mut self) -> CompletedDatabaseSpan {
        let duration = self.start_time.elapsed();
        
        // Set final metrics as tags
        self.span.set_tag("db.cache_hits".to_string(), self.metrics.cache_hits.to_string());
        self.span.set_tag("db.cache_misses".to_string(), self.metrics.cache_misses.to_string());
        self.span.set_tag("db.io_operations".to_string(), self.metrics.io_operations.to_string());
        self.span.set_tag("db.bytes_processed".to_string(), self.metrics.bytes_processed.to_string());
        self.span.set_tag("db.memory_allocated".to_string(), self.metrics.memory_allocated.to_string());
        self.span.set_tag("db.errors".to_string(), self.metrics.errors.to_string());
        self.span.set_tag("db.successes".to_string(), self.metrics.successes.to_string());
        
        // Calculate cache hit rate
        if self.metrics.cache_hits + self.metrics.cache_misses > 0 {
            let hit_rate = self.metrics.cache_hits as f64 / 
                (self.metrics.cache_hits + self.metrics.cache_misses) as f64;
            self.span.set_tag("db.cache_hit_rate".to_string(), format!("{:.3}", hit_rate));
        }
        
        // Calculate throughput
        if duration.as_secs_f64() > 0.0 {
            let throughput = self.metrics.bytes_processed as f64 / duration.as_secs_f64();
            self.span.set_tag("db.throughput_bytes_per_sec".to_string(), format!("{:.0}", throughput));
        }
        
        self.span.finish();
        
        CompletedDatabaseSpan {
            span: self.span,
            metrics: self.metrics,
            duration,
        }
    }
    
    /// Get current metrics
    pub fn metrics(&self) -> &OperationMetrics {
        &self.metrics
    }
    
    /// Get span ID
    pub fn span_id(&self) -> &str {
        &self.span.span_id
    }
    
    /// Get trace ID
    pub fn trace_id(&self) -> &str {
        &self.span.trace_id
    }
}

/// Completed database span with final metrics
pub struct CompletedDatabaseSpan {
    pub span: Span,
    pub metrics: OperationMetrics,
    pub duration: Duration,
}

impl CompletedDatabaseSpan {
    /// Get operation duration in microseconds
    pub fn duration_micros(&self) -> u64 {
        self.duration.as_micros() as u64
    }
    
    /// Get operation duration in milliseconds
    pub fn duration_millis(&self) -> u64 {
        self.duration.as_millis() as u64
    }
    
    /// Check if operation was successful
    pub fn is_success(&self) -> bool {
        matches!(self.span.status, SpanStatus::Ok)
    }
    
    /// Get error message if operation failed
    pub fn error_message(&self) -> Option<&str> {
        match &self.span.status {
            SpanStatus::Error(msg) => Some(msg),
            _ => None,
        }
    }
}

/// Metrics collected during database operations
#[derive(Debug, Clone, Default)]
pub struct OperationMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub io_operations: u64,
    pub bytes_processed: usize,
    pub memory_allocated: usize,
    pub errors: u64,
    pub successes: u64,
}

impl OperationMetrics {
    fn new() -> Self {
        Self::default()
    }
    
    /// Calculate cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        if self.cache_hits + self.cache_misses == 0 {
            0.0
        } else {
            self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
        }
    }
    
    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.errors + self.successes == 0 {
            0.0
        } else {
            self.successes as f64 / (self.errors + self.successes) as f64
        }
    }
}

/// Predefined span builders for common database operations
pub struct DatabaseSpans;

impl DatabaseSpans {
    /// Create span for GET operation
    pub fn get(context: &TraceContext, key: &str) -> DatabaseSpanBuilder {
        DatabaseSpanBuilder::new(context, "db.get")
            .with_operation("SELECT")
            .with_key_pattern(key)
            .with_kind(SpanKind::Internal)
    }
    
    /// Create span for PUT operation
    pub fn put(context: &TraceContext, key: &str, value_size: usize) -> DatabaseSpanBuilder {
        DatabaseSpanBuilder::new(context, "db.put")
            .with_operation("INSERT")
            .with_key_pattern(key)
            .with_data_size(value_size)
            .with_kind(SpanKind::Internal)
    }
    
    /// Create span for DELETE operation
    pub fn delete(context: &TraceContext, key: &str) -> DatabaseSpanBuilder {
        DatabaseSpanBuilder::new(context, "db.delete")
            .with_operation("DELETE")
            .with_key_pattern(key)
            .with_kind(SpanKind::Internal)
    }
    
    /// Create span for SCAN operation
    pub fn scan(context: &TraceContext, start_key: Option<&str>, _end_key: Option<&str>) -> DatabaseSpanBuilder {
        let mut builder = DatabaseSpanBuilder::new(context, "db.scan")
            .with_operation("SCAN")
            .with_kind(SpanKind::Internal);
        
        if let Some(start) = start_key {
            builder = builder.with_key_pattern(start);
        }
        
        builder
    }
    
    /// Create span for transaction operations
    pub fn transaction(context: &TraceContext, tx_id: u64, operation: &str) -> DatabaseSpanBuilder {
        DatabaseSpanBuilder::new(context, &format!("db.transaction.{}", operation))
            .with_operation("TRANSACTION")
            .with_transaction_id(tx_id)
            .with_kind(SpanKind::Internal)
    }
    
    /// Create span for batch operations
    pub fn batch(context: &TraceContext, operation: &str, count: usize) -> DatabaseSpanBuilder {
        DatabaseSpanBuilder::new(context, &format!("db.batch.{}", operation))
            .with_operation(operation)
            .with_record_count(count)
            .with_kind(SpanKind::Internal)
    }
    
    /// Create span for compaction operations
    pub fn compaction(context: &TraceContext, compaction_type: &str) -> DatabaseSpanBuilder {
        DatabaseSpanBuilder::new(context, "db.compaction")
            .with_operation("MAINTENANCE")
            .with_table(compaction_type)
            .with_kind(SpanKind::Internal)
    }
    
    /// Create span for backup operations
    pub fn backup(context: &TraceContext, backup_type: &str) -> DatabaseSpanBuilder {
        DatabaseSpanBuilder::new(context, "db.backup")
            .with_operation("BACKUP")
            .with_table(backup_type)
            .with_kind(SpanKind::Internal)
    }
    
    /// Create span for recovery operations
    pub fn recovery(context: &TraceContext, recovery_type: &str) -> DatabaseSpanBuilder {
        DatabaseSpanBuilder::new(context, "db.recovery")
            .with_operation("RECOVERY")
            .with_table(recovery_type)
            .with_kind(SpanKind::Internal)
    }
}

/// Macro for creating database spans with automatic finishing
#[macro_export]
macro_rules! db_span {
    ($context:expr, $operation:expr) => {{
        use $crate::distributed_tracing::span::DatabaseSpanBuilder;
        DatabaseSpanBuilder::new($context, $operation).build()
    }};
    
    ($context:expr, $operation:expr, $($key:ident = $value:expr),*) => {{
        use $crate::distributed_tracing::span::DatabaseSpanBuilder;
        let mut builder = DatabaseSpanBuilder::new($context, $operation);
        $(
            builder = match stringify!($key) {
                "table" => builder.with_table($value),
                "key_pattern" => builder.with_key_pattern($value),
                "transaction_id" => builder.with_transaction_id($value),
                "record_count" => builder.with_record_count($value),
                "data_size" => builder.with_data_size($value),
                _ => builder,
            };
        )*
        builder.build()
    }};
}

/// Span guard that automatically finishes database spans
pub struct DatabaseSpanGuard {
    span: Option<DatabaseSpan>,
}

impl DatabaseSpanGuard {
    pub fn new(span: DatabaseSpan) -> Self {
        Self { span: Some(span) }
    }
    
    /// Record cache hit
    pub fn record_cache_hit(&mut self) {
        if let Some(span) = &mut self.span {
            span.record_cache_hit();
        }
    }
    
    /// Record cache miss
    pub fn record_cache_miss(&mut self) {
        if let Some(span) = &mut self.span {
            span.record_cache_miss();
        }
    }
    
    /// Record I/O operation
    pub fn record_io_operation(&mut self, operation_type: &str, bytes: usize) {
        if let Some(span) = &mut self.span {
            span.record_io_operation(operation_type, bytes);
        }
    }
    
    /// Record error
    pub fn record_error(&mut self, error: &str, error_code: Option<&str>) {
        if let Some(span) = &mut self.span {
            span.record_error(error, error_code);
        }
    }
    
    /// Record success
    pub fn record_success(&mut self) {
        if let Some(span) = &mut self.span {
            span.record_success();
        }
    }
    
    /// Finish span manually (otherwise done in Drop)
    pub fn finish(mut self) -> Option<CompletedDatabaseSpan> {
        self.span.take().map(|span| span.finish())
    }
}

impl Drop for DatabaseSpanGuard {
    fn drop(&mut self) {
        if let Some(span) = self.span.take() {
            span.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_tracing::TraceContext;

    #[test]
    fn test_database_span_builder() {
        let context = TraceContext::new_root();
        let span = DatabaseSpanBuilder::new(&context, "test_operation")
            .with_operation("SELECT")
            .with_table("users")
            .with_key_pattern("user:*")
            .with_record_count(100)
            .build();
        
        assert_eq!(span.span.operation_name, "test_operation");
        assert_eq!(span.span.tags.get("db.operation"), Some(&"SELECT".to_string()));
        assert_eq!(span.span.tags.get("db.table"), Some(&"users".to_string()));
    }

    #[test]
    fn test_database_span_metrics() {
        let context = TraceContext::new_root();
        let mut span = DatabaseSpanBuilder::new(&context, "test_op").build();
        
        span.record_cache_hit();
        span.record_cache_miss();
        span.record_io_operation("read", 1024);
        span.record_success();
        
        let metrics = span.metrics();
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.cache_misses, 1);
        assert_eq!(metrics.io_operations, 1);
        assert_eq!(metrics.bytes_processed, 1024);
        assert_eq!(metrics.successes, 1);
        assert_eq!(metrics.cache_hit_rate(), 0.5);
    }

    #[test]
    fn test_completed_span() {
        let context = TraceContext::new_root();
        let mut span = DatabaseSpanBuilder::new(&context, "test_op").build();
        
        span.record_success();
        let completed = span.finish();
        
        assert!(completed.is_success());
        assert!(completed.duration_micros() > 0);
        assert_eq!(completed.metrics.successes, 1);
    }

    #[test]
    fn test_database_spans_builders() {
        let context = TraceContext::new_root();
        
        let get_span = DatabaseSpans::get(&context, "user:123").build();
        assert_eq!(get_span.span.operation_name, "db.get");
        
        let put_span = DatabaseSpans::put(&context, "user:123", 256).build();
        assert_eq!(put_span.span.operation_name, "db.put");
        
        let tx_span = DatabaseSpans::transaction(&context, 42, "commit").build();
        assert_eq!(tx_span.span.operation_name, "db.transaction.commit");
    }

    #[test]
    fn test_span_guard() {
        let context = TraceContext::new_root();
        let span = DatabaseSpanBuilder::new(&context, "test_op").build();
        let mut guard = DatabaseSpanGuard::new(span);
        
        guard.record_cache_hit();
        guard.record_success();
        
        let completed = guard.finish().unwrap();
        assert!(completed.is_success());
        assert_eq!(completed.metrics.cache_hits, 1);
    }
}