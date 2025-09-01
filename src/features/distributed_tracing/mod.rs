//! Distributed Tracing System
//!
//! Provides comprehensive distributed tracing capabilities for Lightning DB operations
//! including trace context propagation, OpenTelemetry integration, and performance monitoring.

use crate::core::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

pub mod baggage;
pub mod context;
pub mod exporter;
pub mod integration;
pub mod sampler;
pub mod span;
pub mod visualization;

/// Trace context that propagates across operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub trace_flags: u8,
    pub trace_state: String,
    pub baggage: HashMap<String, String>,
}

impl TraceContext {
    /// Create a new root trace context
    pub fn new_root() -> Self {
        Self {
            trace_id: generate_trace_id(),
            span_id: generate_span_id(),
            parent_span_id: None,
            trace_flags: 1, // Sampled
            trace_state: String::new(),
            baggage: HashMap::new(),
        }
    }

    /// Create a child trace context
    pub fn create_child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: generate_span_id(),
            parent_span_id: Some(self.span_id.clone()),
            trace_flags: self.trace_flags,
            trace_state: self.trace_state.clone(),
            baggage: self.baggage.clone(),
        }
    }

    /// Check if trace is sampled
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 1 != 0
    }

    /// Add baggage item
    pub fn set_baggage(&mut self, key: String, value: String) {
        self.baggage.insert(key, value);
    }

    /// Get baggage item
    pub fn get_baggage(&self, key: &str) -> Option<&String> {
        self.baggage.get(key)
    }
}

/// Span represents a single operation with timing and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub operation_name: String,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub duration: Option<Duration>,
    pub tags: HashMap<String, String>,
    pub logs: Vec<SpanLog>,
    pub status: SpanStatus,
    pub span_kind: SpanKind,
}

/// Span status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error(String),
}

/// Span kind
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

/// Span log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLog {
    pub timestamp: SystemTime,
    pub level: LogLevel,
    pub message: String,
    pub fields: HashMap<String, String>,
}

/// Log level for span logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl Span {
    /// Create a new span
    pub fn new(context: &TraceContext, operation_name: String) -> Self {
        Self {
            trace_id: context.trace_id.clone(),
            span_id: context.span_id.clone(),
            parent_span_id: context.parent_span_id.clone(),
            operation_name,
            start_time: SystemTime::now(),
            end_time: None,
            duration: None,
            tags: HashMap::new(),
            logs: Vec::new(),
            status: SpanStatus::Unset,
            span_kind: SpanKind::Internal,
        }
    }

    /// Set span tag
    pub fn set_tag(&mut self, key: String, value: String) {
        self.tags.insert(key, value);
    }

    /// Set span kind
    pub fn set_kind(&mut self, kind: SpanKind) {
        self.span_kind = kind;
    }

    /// Add log entry
    pub fn log(&mut self, level: LogLevel, message: String, fields: HashMap<String, String>) {
        self.logs.push(SpanLog {
            timestamp: SystemTime::now(),
            level,
            message,
            fields,
        });
    }

    /// Log error
    pub fn log_error(&mut self, error: &str) {
        self.status = SpanStatus::Error(error.to_string());
        self.log(LogLevel::Error, error.to_string(), HashMap::new());
    }

    /// Finish the span
    pub fn finish(&mut self) {
        let end_time = SystemTime::now();
        self.end_time = Some(end_time);

        if let Ok(duration) = end_time.duration_since(self.start_time) {
            self.duration = Some(duration);
        }

        if matches!(self.status, SpanStatus::Unset) {
            self.status = SpanStatus::Ok;
        }
    }

    /// Check if span is finished
    pub fn is_finished(&self) -> bool {
        self.end_time.is_some()
    }

    /// Get span duration in microseconds
    pub fn duration_micros(&self) -> Option<u64> {
        self.duration.map(|d| d.as_micros() as u64)
    }
}

/// Tracer manages span creation and collection
pub struct Tracer {
    service_name: String,
    spans: Arc<Mutex<Vec<Span>>>,
    active_spans: Arc<Mutex<HashMap<String, Span>>>,
    sampler: Box<dyn TraceSampler + Send + Sync>,
    exporters: Vec<Box<dyn TraceExporter + Send + Sync>>,
}

impl Tracer {
    /// Create a new tracer
    pub fn new(service_name: String) -> Self {
        Self {
            service_name,
            spans: Arc::new(Mutex::new(Vec::new())),
            active_spans: Arc::new(Mutex::new(HashMap::new())),
            sampler: Box::new(AlwaysSampler),
            exporters: Vec::new(),
        }
    }

    /// Set sampler
    pub fn with_sampler(mut self, sampler: Box<dyn TraceSampler + Send + Sync>) -> Self {
        self.sampler = sampler;
        self
    }

    /// Add exporter
    pub fn with_exporter(mut self, exporter: Box<dyn TraceExporter + Send + Sync>) -> Self {
        self.exporters.push(exporter);
        self
    }

    /// Start a new span
    pub fn start_span(&self, context: &TraceContext, operation_name: String) -> Result<String> {
        // Check if trace should be sampled
        if !self.sampler.should_sample(context, &operation_name) {
            return Ok(context.span_id.clone());
        }

        let mut span = Span::new(context, operation_name);
        span.set_tag("service.name".to_string(), self.service_name.clone());

        let span_id = span.span_id.clone();

        // Store active span
        if let Ok(mut active_spans) = self.active_spans.lock() {
            active_spans.insert(span_id.clone(), span);
        }

        Ok(span_id)
    }

    /// Get active span
    pub fn get_span(&self, span_id: &str) -> Option<Span> {
        if let Ok(active_spans) = self.active_spans.lock() {
            active_spans.get(span_id).cloned()
        } else {
            None
        }
    }

    /// Update span with tags or logs
    pub fn update_span<F>(&self, span_id: &str, updater: F) -> Result<()>
    where
        F: FnOnce(&mut Span),
    {
        if let Ok(mut active_spans) = self.active_spans.lock() {
            if let Some(span) = active_spans.get_mut(span_id) {
                updater(span);
            }
        }
        Ok(())
    }

    /// Finish a span
    pub fn finish_span(&self, span_id: &str) -> Result<()> {
        if let Ok(mut active_spans) = self.active_spans.lock() {
            if let Some(mut span) = active_spans.remove(span_id) {
                span.finish();

                // Add to completed spans
                if let Ok(mut spans) = self.spans.lock() {
                    spans.push(span.clone());
                }

                // Export span
                for exporter in &self.exporters {
                    if let Err(e) = exporter.export_span(&span) {
                        eprintln!("Failed to export span: {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Get all completed spans
    pub fn get_spans(&self) -> Vec<Span> {
        if let Ok(spans) = self.spans.lock() {
            spans.clone()
        } else {
            Vec::new()
        }
    }

    /// Clear completed spans
    pub fn clear_spans(&self) {
        if let Ok(mut spans) = self.spans.lock() {
            spans.clear();
        }
    }

    /// Flush all spans to exporters
    pub fn flush(&self) -> Result<()> {
        let spans = self.get_spans();

        for exporter in &self.exporters {
            for span in &spans {
                exporter.export_span(span)?;
            }
        }

        self.clear_spans();
        Ok(())
    }
}

/// Trait for trace sampling strategies
pub trait TraceSampler {
    fn should_sample(&self, context: &TraceContext, operation_name: &str) -> bool;
}

/// Always sample traces
pub struct AlwaysSampler;

impl TraceSampler for AlwaysSampler {
    fn should_sample(&self, _context: &TraceContext, _operation_name: &str) -> bool {
        true
    }
}

/// Never sample traces
pub struct NeverSampler;

impl TraceSampler for NeverSampler {
    fn should_sample(&self, _context: &TraceContext, _operation_name: &str) -> bool {
        false
    }
}

/// Sample traces at a fixed rate
pub struct RateLimitingSampler {
    rate: f64,
    counter: Arc<Mutex<u64>>,
}

impl RateLimitingSampler {
    pub fn new(rate: f64) -> Self {
        Self {
            rate: rate.clamp(0.0, 1.0),
            counter: Arc::new(Mutex::new(0)),
        }
    }
}

impl TraceSampler for RateLimitingSampler {
    fn should_sample(&self, _context: &TraceContext, _operation_name: &str) -> bool {
        if let Ok(mut counter) = self.counter.lock() {
            *counter += 1;
            (*counter as f64 * self.rate) % 1.0 < self.rate
        } else {
            false
        }
    }
}

/// Trait for trace exporters
pub trait TraceExporter {
    fn export_span(&self, span: &Span) -> Result<()>;
    fn export_spans(&self, spans: &[Span]) -> Result<()> {
        for span in spans {
            self.export_span(span)?;
        }
        Ok(())
    }
}

/// Console exporter for debugging
pub struct ConsoleExporter;

impl TraceExporter for ConsoleExporter {
    fn export_span(&self, span: &Span) -> Result<()> {
        println!(
            "TRACE: {} [{}] {} ({:?})",
            span.trace_id,
            span.span_id,
            span.operation_name,
            span.duration_micros()
        );
        Ok(())
    }
}

/// Generate a new trace ID
fn generate_trace_id() -> String {
    format!("{:032x}", Uuid::new_v4().as_u128())
}

/// Generate a new span ID
fn generate_span_id() -> String {
    format!("{:016x}", rand::random::<u64>())
}

/// Global tracer instance (thread-safe, one-time init)
static GLOBAL_TRACER: std::sync::OnceLock<Arc<Tracer>> = std::sync::OnceLock::new();

/// Initialize global tracer
pub fn init_tracer(tracer: Tracer) {
    let _ = GLOBAL_TRACER.set(Arc::new(tracer));
}

/// Get global tracer
pub fn global_tracer() -> Option<Arc<Tracer>> {
    GLOBAL_TRACER.get().cloned()
}

/// Convenience macro for tracing function calls
#[macro_export]
macro_rules! trace_span {
    ($operation:expr) => {{
        use $crate::distributed_tracing::{global_tracer, TraceContext};

        let context = TraceContext::new_root();
        let span_id = if let Some(tracer) = global_tracer() {
            tracer.start_span(&context, $operation.to_string()).ok()
        } else {
            None
        };

        $crate::distributed_tracing::SpanGuard::new(span_id)
    }};

    ($context:expr, $operation:expr) => {{
        use $crate::distributed_tracing::global_tracer;

        let child_context = $context.create_child();
        let span_id = if let Some(tracer) = global_tracer() {
            tracer
                .start_span(&child_context, $operation.to_string())
                .ok()
        } else {
            None
        };

        $crate::distributed_tracing::SpanGuard::new(span_id)
    }};
}

/// RAII guard for automatically finishing spans
pub struct SpanGuard {
    span_id: Option<String>,
}

impl SpanGuard {
    pub fn new(span_id: Option<String>) -> Self {
        Self { span_id }
    }

    /// Set span tag
    pub fn set_tag(&self, key: String, value: String) {
        if let (Some(span_id), Some(tracer)) = (&self.span_id, global_tracer()) {
            let _ = tracer.update_span(span_id, |span| {
                span.set_tag(key, value);
            });
        }
    }

    /// Log error
    pub fn log_error(&self, error: &str) {
        if let (Some(span_id), Some(tracer)) = (&self.span_id, global_tracer()) {
            let _ = tracer.update_span(span_id, |span| {
                span.log_error(error);
            });
        }
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        if let (Some(span_id), Some(tracer)) = (&self.span_id, global_tracer()) {
            let _ = tracer.finish_span(span_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_context_creation() {
        let context = TraceContext::new_root();
        assert!(!context.trace_id.is_empty());
        assert!(!context.span_id.is_empty());
        assert!(context.parent_span_id.is_none());
        assert!(context.is_sampled());
    }

    #[test]
    fn test_child_context() {
        let parent = TraceContext::new_root();
        let child = parent.create_child();

        assert_eq!(parent.trace_id, child.trace_id);
        assert_ne!(parent.span_id, child.span_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
    }

    #[test]
    fn test_span_lifecycle() {
        let context = TraceContext::new_root();
        let mut span = Span::new(&context, "test_operation".to_string());

        assert_eq!(span.operation_name, "test_operation");
        assert!(!span.is_finished());

        span.set_tag("key".to_string(), "value".to_string());
        span.finish();

        assert!(span.is_finished());
        assert!(span.duration.is_some());
        assert_eq!(span.tags.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_tracer() {
        let tracer = Tracer::new("test_service".to_string());
        let context = TraceContext::new_root();

        let span_id = tracer.start_span(&context, "test_op".to_string()).unwrap();

        tracer
            .update_span(&span_id, |span| {
                span.set_tag("test".to_string(), "value".to_string());
            })
            .unwrap();

        tracer.finish_span(&span_id).unwrap();

        let spans = tracer.get_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].operation_name, "test_op");
    }

    #[test]
    fn test_rate_limiting_sampler() {
        let sampler = RateLimitingSampler::new(0.5);
        let context = TraceContext::new_root();

        let mut sampled = 0;
        for _ in 0..100 {
            if sampler.should_sample(&context, "test") {
                sampled += 1;
            }
        }

        // Should be approximately 50% with some variance
        assert!((40..=60).contains(&sampled));
    }
}
