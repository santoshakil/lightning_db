//! Trace Exporters
//!
//! Provides various exporters for sending trace data to different backends
//! including OpenTelemetry collectors, logging systems, and metrics stores.

use super::{Span, TraceExporter};
use crate::Result;
use serde_json;
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

/// Batch exporter that collects spans and exports them in batches
pub struct BatchExporter {
    inner_exporter: Box<dyn TraceExporter + Send + Sync>,
    batch_size: usize,
    batch_timeout_ms: u64,
    pending_spans: Arc<Mutex<Vec<Span>>>,
}

impl BatchExporter {
    pub fn new(
        exporter: Box<dyn TraceExporter + Send + Sync>,
        batch_size: usize,
        batch_timeout_ms: u64,
    ) -> Self {
        Self {
            inner_exporter: exporter,
            batch_size,
            batch_timeout_ms,
            pending_spans: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Force flush all pending spans
    pub fn flush(&self) -> Result<()> {
        if let Ok(mut spans) = self.pending_spans.lock() {
            if !spans.is_empty() {
                self.inner_exporter.export_spans(&spans)?;
                spans.clear();
            }
        }
        Ok(())
    }

    /// Get number of pending spans
    pub fn pending_count(&self) -> usize {
        if let Ok(spans) = self.pending_spans.lock() {
            spans.len()
        } else {
            0
        }
    }
}

impl TraceExporter for BatchExporter {
    fn export_span(&self, span: &Span) -> Result<()> {
        if let Ok(mut spans) = self.pending_spans.lock() {
            spans.push(span.clone());

            // Export if batch is full
            if spans.len() >= self.batch_size {
                self.inner_exporter.export_spans(&spans)?;
                spans.clear();
            }
        }
        Ok(())
    }

    fn export_spans(&self, spans: &[Span]) -> Result<()> {
        for span in spans {
            self.export_span(span)?;
        }
        Ok(())
    }
}

/// File-based exporter that writes spans to a file
pub struct FileExporter {
    file_path: String,
    writer: Arc<Mutex<Option<std::fs::File>>>,
    format: FileFormat,
}

#[derive(Debug, Clone)]
pub enum FileFormat {
    Json,
    JsonLines,
    Csv,
}

impl FileExporter {
    pub fn new(file_path: String, format: FileFormat) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        Ok(Self {
            file_path,
            writer: Arc::new(Mutex::new(Some(file))),
            format,
        })
    }

    fn write_span(&self, span: &Span) -> Result<()> {
        if let Ok(mut writer_opt) = self.writer.lock() {
            if let Some(ref mut writer) = *writer_opt {
                match self.format {
                    FileFormat::Json => {
                        writeln!(writer, "{}", serde_json::to_string_pretty(span)?)?;
                    }
                    FileFormat::JsonLines => {
                        writeln!(writer, "{}", serde_json::to_string(span)?)?;
                    }
                    FileFormat::Csv => {
                        self.write_csv_span(writer, span)?;
                    }
                }
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn write_csv_span(&self, writer: &mut std::fs::File, span: &Span) -> Result<()> {
        let start_time = span
            .start_time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let duration_micros = span.duration_micros().unwrap_or(0);

        let status = match &span.status {
            super::SpanStatus::Ok => "OK",
            super::SpanStatus::Error(_) => "ERROR",
            super::SpanStatus::Unset => "UNSET",
        };

        let tags_json = serde_json::to_string(&span.tags).unwrap_or_default();

        writeln!(
            writer,
            "{},{},{},{},{},{},{},{}",
            span.trace_id,
            span.span_id,
            span.parent_span_id.as_deref().unwrap_or(""),
            span.operation_name,
            start_time,
            duration_micros,
            status,
            tags_json
        )?;

        Ok(())
    }
}

impl TraceExporter for FileExporter {
    fn export_span(&self, span: &Span) -> Result<()> {
        self.write_span(span)
    }
}

/// HTTP exporter for sending spans to OpenTelemetry collectors
#[cfg(feature = "http-client")]
pub struct HttpExporter {
    endpoint: String,
    headers: HashMap<String, String>,
    client: reqwest::blocking::Client,
    format: HttpFormat,
}

#[cfg(not(feature = "http-client"))]
pub struct HttpExporter {
    endpoint: String,
    headers: HashMap<String, String>,
    format: HttpFormat,
}

#[derive(Debug, Clone)]
pub enum HttpFormat {
    OpenTelemetryJson,
    Jaeger,
    Zipkin,
}

impl HttpExporter {
    pub fn new(endpoint: String, format: HttpFormat) -> Self {
        #[cfg(feature = "http-client")]
        {
            let client = reqwest::blocking::Client::new();
            Self {
                endpoint,
                headers: HashMap::new(),
                client,
                format,
            }
        }
        #[cfg(not(feature = "http-client"))]
        {
            Self {
                endpoint,
                headers: HashMap::new(),
                format,
            }
        }
    }

    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    pub fn with_auth_header(self, token: String) -> Self {
        self.with_header("Authorization".to_string(), format!("Bearer {}", token))
    }

    fn convert_to_otel_format(&self, spans: &[Span]) -> serde_json::Value {
        let resource_spans = spans
            .iter()
            .map(|span| {
                serde_json::json!({
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {"stringValue": "lightning_db"}
                            }
                        ]
                    },
                    "instrumentationLibrarySpans": [{
                        "instrumentationLibrary": {
                            "name": "lightning_db_tracer",
                            "version": "1.0.0"
                        },
                        "spans": [self.span_to_otel_format(span)]
                    }]
                })
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "resourceSpans": resource_spans
        })
    }

    fn span_to_otel_format(&self, span: &Span) -> serde_json::Value {
        let start_time_nanos = span
            .start_time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let end_time_nanos = span
            .end_time
            .map(|end| {
                end.duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            })
            .unwrap_or(start_time_nanos);

        let attributes = span
            .tags
            .iter()
            .map(|(k, v)| {
                serde_json::json!({
                    "key": k,
                    "value": {"stringValue": v}
                })
            })
            .collect::<Vec<_>>();

        let events = span
            .logs
            .iter()
            .map(|log| {
                let timestamp_nanos = log
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();

                serde_json::json!({
                    "timeUnixNano": timestamp_nanos.to_string(),
                    "name": log.message,
                    "attributes": log.fields.iter().map(|(k, v)| {
                        serde_json::json!({
                            "key": k,
                            "value": {"stringValue": v}
                        })
                    }).collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();

        let status_code = match &span.status {
            super::SpanStatus::Ok => 1,
            super::SpanStatus::Error(_) => 2,
            super::SpanStatus::Unset => 0,
        };

        serde_json::json!({
            "traceId": hex::decode(&span.trace_id).unwrap_or_default(),
            "spanId": hex::decode(&span.span_id).unwrap_or_default(),
            "parentSpanId": span.parent_span_id.as_ref()
                .and_then(|id| hex::decode(id).ok())
                .unwrap_or_default(),
            "name": span.operation_name,
            "kind": match span.span_kind {
                super::SpanKind::Internal => 1,
                super::SpanKind::Server => 2,
                super::SpanKind::Client => 3,
                super::SpanKind::Producer => 4,
                super::SpanKind::Consumer => 5,
            },
            "startTimeUnixNano": start_time_nanos.to_string(),
            "endTimeUnixNano": end_time_nanos.to_string(),
            "attributes": attributes,
            "events": events,
            "status": {
                "code": status_code,
                "message": match &span.status {
                    super::SpanStatus::Error(msg) => msg.clone(),
                    _ => String::new(),
                }
            }
        })
    }

    fn send_http_request(&self, payload: serde_json::Value) -> Result<()> {
        #[cfg(feature = "http-client")]
        {
            let mut request = self
                .client
                .post(&self.endpoint)
                .header("Content-Type", "application/json");

            // Add custom headers
            for (key, value) in &self.headers {
                request = request.header(key, value);
            }

            let response = request
                .json(&payload)
                .send()
                .map_err(|e| crate::Error::IoError(e.to_string()))?;

            if !response.status().is_success() {
                return Err(crate::Error::IoError(format!(
                    "HTTP export failed with status: {}",
                    response.status()
                )));
            }

            Ok(())
        }
        #[cfg(not(feature = "http-client"))]
        {
            Err(crate::Error::IoError("HTTP export requires 'http-client' feature".to_string()))
        }
    }
}

impl TraceExporter for HttpExporter {
    fn export_span(&self, span: &Span) -> Result<()> {
        self.export_spans(&[span.clone()])
    }

    fn export_spans(&self, spans: &[Span]) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let payload = match self.format {
            HttpFormat::OpenTelemetryJson => self.convert_to_otel_format(spans),
            HttpFormat::Jaeger => {
                // Simplified Jaeger format
                serde_json::json!({
                    "data": spans.iter().map(|s| {
                        serde_json::json!({
                            "traceID": s.trace_id,
                            "spanID": s.span_id,
                            "operationName": s.operation_name,
                            "startTime": s.start_time.duration_since(UNIX_EPOCH)
                                .unwrap_or_default().as_micros(),
                            "duration": s.duration_micros().unwrap_or(0),
                            "tags": s.tags,
                            "process": {
                                "serviceName": "lightning_db",
                                "tags": {}
                            }
                        })
                    }).collect::<Vec<_>>()
                })
            }
            HttpFormat::Zipkin => {
                // Simplified Zipkin format
                serde_json::json!(spans
                    .iter()
                    .map(|s| {
                        serde_json::json!({
                            "traceId": s.trace_id,
                            "id": s.span_id,
                            "parentId": s.parent_span_id,
                            "name": s.operation_name,
                            "timestamp": s.start_time.duration_since(UNIX_EPOCH)
                                .unwrap_or_default().as_micros(),
                            "duration": s.duration_micros().unwrap_or(0),
                            "localEndpoint": {
                                "serviceName": "lightning_db"
                            },
                            "tags": s.tags
                        })
                    })
                    .collect::<Vec<_>>())
            }
        };

        self.send_http_request(payload)
    }
}

/// Metrics exporter that converts spans to metrics
pub struct MetricsExporter {
    metrics_store: Arc<Mutex<HashMap<String, OperationMetrics>>>,
}

#[derive(Debug, Clone, Default)]
pub struct OperationMetrics {
    count: u64,
    total_duration_micros: u64,
    error_count: u64,
    success_count: u64,
}

impl OperationMetrics {
    /// Get total count of operations
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Get total duration in microseconds
    pub fn total_duration_micros(&self) -> u64 {
        self.total_duration_micros
    }

    /// Get average duration in microseconds
    pub fn average_duration_micros(&self) -> f64 {
        if self.count > 0 {
            self.total_duration_micros as f64 / self.count as f64
        } else {
            0.0
        }
    }

    /// Get error count
    pub fn error_count(&self) -> u64 {
        self.error_count
    }

    /// Get success count
    pub fn success_count(&self) -> u64 {
        self.success_count
    }

    /// Get error rate (0.0 to 1.0)
    pub fn error_rate(&self) -> f64 {
        if self.count > 0 {
            self.error_count as f64 / self.count as f64
        } else {
            0.0
        }
    }

    /// Get success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.count > 0 {
            self.success_count as f64 / self.count as f64
        } else {
            0.0
        }
    }
}

impl MetricsExporter {
    pub fn new() -> Self {
        Self {
            metrics_store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get metrics for all operations
    pub fn get_metrics(&self) -> HashMap<String, OperationMetrics> {
        if let Ok(metrics) = self.metrics_store.lock() {
            metrics.clone()
        } else {
            HashMap::new()
        }
    }

    /// Get metrics for specific operation
    pub fn get_operation_metrics(&self, operation: &str) -> Option<OperationMetrics> {
        if let Ok(metrics) = self.metrics_store.lock() {
            metrics.get(operation).cloned()
        } else {
            None
        }
    }

    fn update_metrics(&self, span: &Span) {
        if let Ok(mut metrics) = self.metrics_store.lock() {
            let op_metrics = metrics.entry(span.operation_name.clone()).or_default();

            op_metrics.count += 1;

            if let Some(duration) = span.duration_micros() {
                op_metrics.total_duration_micros += duration;
            }

            match &span.status {
                super::SpanStatus::Ok => op_metrics.success_count += 1,
                super::SpanStatus::Error(_) => op_metrics.error_count += 1,
                super::SpanStatus::Unset => {} // Don't count unfinished spans
            }
        }
    }
}

impl TraceExporter for MetricsExporter {
    fn export_span(&self, span: &Span) -> Result<()> {
        self.update_metrics(span);
        Ok(())
    }
}

/// Multi-exporter that sends spans to multiple exporters
pub struct MultiExporter {
    exporters: Vec<Box<dyn TraceExporter + Send + Sync>>,
    fail_fast: bool,
}

impl MultiExporter {
    pub fn new() -> Self {
        Self {
            exporters: Vec::new(),
            fail_fast: false,
        }
    }

    /// Add an exporter
    pub fn add_exporter(mut self, exporter: Box<dyn TraceExporter + Send + Sync>) -> Self {
        self.exporters.push(exporter);
        self
    }

    /// Set whether to fail fast on first error or continue with other exporters
    pub fn with_fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }
}

impl TraceExporter for MultiExporter {
    fn export_span(&self, span: &Span) -> Result<()> {
        let mut errors = Vec::new();

        for exporter in &self.exporters {
            match exporter.export_span(span) {
                Ok(_) => {}
                Err(e) => {
                    if self.fail_fast {
                        return Err(e);
                    } else {
                        errors.push(e);
                    }
                }
            }
        }

        if !errors.is_empty() && !self.fail_fast {
            return Err(crate::Error::Generic(format!(
                "Multiple export errors: {:?}",
                errors
            )));
        }

        Ok(())
    }

    fn export_spans(&self, spans: &[Span]) -> Result<()> {
        for span in spans {
            self.export_span(span)?;
        }
        Ok(())
    }
}

/// Logging exporter that outputs spans to structured logs
pub struct LoggingExporter {
    level: LogLevel,
    include_tags: bool,
    include_events: bool,
}

#[derive(Debug, Clone)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LoggingExporter {
    pub fn new(level: LogLevel) -> Self {
        Self {
            level,
            include_tags: true,
            include_events: true,
        }
    }

    pub fn with_tags(mut self, include: bool) -> Self {
        self.include_tags = include;
        self
    }

    pub fn with_events(mut self, include: bool) -> Self {
        self.include_events = include;
        self
    }

    fn log_span(&self, span: &Span) {
        let duration = span.duration_micros().unwrap_or(0);
        let status = match &span.status {
            super::SpanStatus::Ok => "OK",
            super::SpanStatus::Error(msg) => msg,
            super::SpanStatus::Unset => "UNSET",
        };

        let mut log_msg = format!(
            "TRACE: {} [{}] {} ({}μs) - {}",
            span.trace_id, span.span_id, span.operation_name, duration, status
        );

        if self.include_tags && !span.tags.is_empty() {
            log_msg.push_str(&format!(" | Tags: {:?}", span.tags));
        }

        if self.include_events && !span.logs.is_empty() {
            log_msg.push_str(&format!(" | Events: {}", span.logs.len()));
        }

        match self.level {
            LogLevel::Error => eprintln!("ERROR: {}", log_msg),
            LogLevel::Warn => eprintln!("WARN: {}", log_msg),
            LogLevel::Info => println!("INFO: {}", log_msg),
            LogLevel::Debug => println!("DEBUG: {}", log_msg),
            LogLevel::Trace => println!("TRACE: {}", log_msg),
        }
    }
}

impl TraceExporter for LoggingExporter {
    fn export_span(&self, span: &Span) -> Result<()> {
        self.log_span(span);
        Ok(())
    }
}

impl Default for LoggingExporter {
    fn default() -> Self {
        Self::new(LogLevel::Info)
    }
}

/// Null exporter that discards all spans (useful for testing)
pub struct NullExporter;

impl TraceExporter for NullExporter {
    fn export_span(&self, _span: &Span) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_tracing::{Span, TraceContext};
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_exporter_json() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let exporter = FileExporter::new(
            temp_file.path().to_string_lossy().to_string(),
            FileFormat::JsonLines,
        )?;

        let context = TraceContext::new_root();
        let mut span = Span::new(&context, "test_operation".to_string());
        span.finish();

        exporter.export_span(&span)?;

        // Verify file was written
        let content = std::fs::read_to_string(temp_file.path())?;
        assert!(!content.is_empty());
        assert!(content.contains("test_operation"));

        Ok(())
    }

    #[test]
    fn test_batch_exporter() {
        let inner_exporter = Box::new(NullExporter);
        let batch_exporter = BatchExporter::new(inner_exporter, 3, 1000);

        let context = TraceContext::new_root();
        let span = Span::new(&context, "test_op".to_string());

        // Add spans to batch
        batch_exporter.export_span(&span).unwrap();
        assert_eq!(batch_exporter.pending_count(), 1);

        batch_exporter.export_span(&span).unwrap();
        assert_eq!(batch_exporter.pending_count(), 2);

        // Should trigger batch export when reaching batch size
        batch_exporter.export_span(&span).unwrap();
        assert_eq!(batch_exporter.pending_count(), 0);
    }

    #[test]
    fn test_metrics_exporter() {
        let exporter = MetricsExporter::new();
        let context = TraceContext::new_root();

        let mut span1 = Span::new(&context, "test_op".to_string());
        span1.status = crate::distributed_tracing::SpanStatus::Ok;
        span1.finish();

        let mut span2 = Span::new(&context, "test_op".to_string());
        span2.status = crate::distributed_tracing::SpanStatus::Error("test error".to_string());
        span2.finish();

        exporter.export_span(&span1).unwrap();
        exporter.export_span(&span2).unwrap();

        let metrics = exporter.get_operation_metrics("test_op").unwrap();
        assert_eq!(metrics.count, 2);
        assert_eq!(metrics.success_count, 1);
        assert_eq!(metrics.error_count, 1);
    }

    #[test]
    fn test_multi_exporter() {
        let multi_exporter = MultiExporter::new()
            .add_exporter(Box::new(NullExporter))
            .add_exporter(Box::new(MetricsExporter::new()))
            .with_fail_fast(false);

        let context = TraceContext::new_root();
        let span = Span::new(&context, "test_op".to_string());

        // Should export to both exporters without error
        multi_exporter.export_span(&span).unwrap();
    }

    #[test]
    fn test_logging_exporter() {
        let exporter = LoggingExporter::new(LogLevel::Debug)
            .with_tags(true)
            .with_events(false);

        let context = TraceContext::new_root();
        let mut span = Span::new(&context, "test_operation".to_string());
        span.set_tag("test_key".to_string(), "test_value".to_string());
        span.finish();

        // Should not panic when logging
        exporter.export_span(&span).unwrap();
    }
}
