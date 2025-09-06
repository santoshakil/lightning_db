#[cfg(feature = "telemetry")]
use opentelemetry::{
    trace::{TraceError, Tracer, TracerProvider, Span as OtelSpan},
    Context, KeyValue,
};
#[cfg(feature = "telemetry")]
use opentelemetry_sdk::{
    trace::{self, TracerProvider as SdkTracerProvider},
    Resource,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::Span;
#[cfg(feature = "telemetry")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub correlation_id: String,
    pub user_id: Option<String>,
    pub request_id: Option<String>,
    pub session_id: Option<String>,
    pub custom_attributes: HashMap<String, String>,
}

impl TelemetryContext {
    pub fn new() -> Self {
        Self {
            trace_id: Uuid::new_v4().to_string(),
            span_id: Uuid::new_v4().to_string(),
            parent_span_id: None,
            correlation_id: Uuid::new_v4().to_string(),
            user_id: None,
            request_id: None,
            session_id: None,
            custom_attributes: HashMap::new(),
        }
    }
    
    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.correlation_id = correlation_id;
        self
    }
    
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }
    
    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.request_id = Some(request_id);
        self
    }
    
    pub fn with_session_id(mut self, session_id: String) -> Self {
        self.session_id = Some(session_id);
        self
    }
    
    pub fn with_custom_attribute(mut self, key: String, value: String) -> Self {
        self.custom_attributes.insert(key, value);
        self
    }
    
    pub fn child_context(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: Uuid::new_v4().to_string(),
            parent_span_id: Some(self.span_id.clone()),
            correlation_id: self.correlation_id.clone(),
            user_id: self.user_id.clone(),
            request_id: self.request_id.clone(),
            session_id: self.session_id.clone(),
            custom_attributes: self.custom_attributes.clone(),
        }
    }
    
    #[cfg(feature = "telemetry")]
    pub fn to_span_attributes(&self) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("trace_id", self.trace_id.clone()),
            KeyValue::new("span_id", self.span_id.clone()),
            KeyValue::new("correlation_id", self.correlation_id.clone()),
        ];
        
        if let Some(parent_id) = &self.parent_span_id {
            attributes.push(KeyValue::new("parent_span_id", parent_id.clone()));
        }
        
        if let Some(user_id) = &self.user_id {
            attributes.push(KeyValue::new("user_id", user_id.clone()));
        }
        
        if let Some(request_id) = &self.request_id {
            attributes.push(KeyValue::new("request_id", request_id.clone()));
        }
        
        if let Some(session_id) = &self.session_id {
            attributes.push(KeyValue::new("session_id", session_id.clone()));
        }
        
        for (key, value) in &self.custom_attributes {
            attributes.push(KeyValue::new(key.clone(), value.clone()));
        }
        
        attributes
    }
    
    #[cfg(not(feature = "telemetry"))]
    pub fn to_span_attributes(&self) -> Vec<(&'static str, String)> {
        vec![]
    }
}

#[cfg(feature = "telemetry")]
pub struct TelemetryManager {
    tracer_provider: Arc<SdkTracerProvider>,
    tracer: opentelemetry_sdk::trace::Tracer,
    span_processor: Arc<dyn SpanProcessor + Send + Sync>,
    context_storage: Arc<parking_lot::RwLock<HashMap<String, TelemetryContext>>>,
}

#[cfg(not(feature = "telemetry"))]
pub struct TelemetryManager {
    span_processor: Arc<dyn SpanProcessor + Send + Sync>,
    context_storage: Arc<parking_lot::RwLock<HashMap<String, TelemetryContext>>>,
}

#[cfg(feature = "telemetry")]
impl TelemetryManager {
    pub fn new(
        service_name: &str,
        service_version: &str,
        endpoint: Option<&str>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let resource = Resource::new(vec![
            KeyValue::new("service.name", service_name.to_string()),
            KeyValue::new("service.version", service_version.to_string()),
            KeyValue::new("service.namespace", "lightning_db".to_string()),
        ]);
        
        let tracer_provider = if let Some(endpoint) = endpoint {
            opentelemetry_jaeger::new_collector_pipeline()
                .with_service_name(service_name)
                .with_endpoint(endpoint)
                .with_trace_config(trace::config().with_resource(resource))
                .install_batch(opentelemetry_sdk::runtime::Tokio)?
                .provider()
                .unwrap()
        } else {
            trace::TracerProvider::builder()
                .with_config(trace::config().with_resource(resource))
                .build()
        };
        
        let tracer = tracer_provider.tracer("lightning_db");
        let span_processor = Arc::new(DatabaseSpanProcessor::new());
        
        Ok(Self {
            tracer_provider: Arc::new(tracer_provider),
            tracer,
            span_processor,
            context_storage: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        })
    }
}

#[cfg(not(feature = "telemetry"))]
impl TelemetryManager {
    pub fn new(
        _service_name: &str,
        _service_version: &str,
        _endpoint: Option<&str>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            span_processor: Arc::new(DatabaseSpanProcessor::new()),
            context_storage: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        })
    }
}

impl TelemetryManager {
    #[cfg(feature = "telemetry")]
    pub fn create_span(&self, name: &str, context: &TelemetryContext) -> DatabaseSpan {
        let mut builder = self.tracer.span_builder(name.to_string());
        
        // Add context attributes to the span
        for attribute in context.to_span_attributes() {
            builder = builder.with_attributes(vec![attribute]);
        }
        
        let span = builder.start(&self.tracer);
        
        DatabaseSpan::new(span, context.clone(), self.span_processor.clone())
    }
    
    #[cfg(not(feature = "telemetry"))]
    pub fn create_span(&self, _name: &str, context: &TelemetryContext) -> DatabaseSpan {
        DatabaseSpan::new_mock(context.clone(), self.span_processor.clone())
    }
    
    pub fn create_child_span(&self, name: &str, parent_context: &TelemetryContext) -> DatabaseSpan {
        let child_context = parent_context.child_context();
        self.create_span(name, &child_context)
    }
    
    pub fn store_context(&self, key: String, context: TelemetryContext) {
        self.context_storage.write().insert(key, context);
    }
    
    pub fn get_context(&self, key: &str) -> Option<TelemetryContext> {
        self.context_storage.read().get(key).cloned()
    }
    
    pub fn remove_context(&self, key: &str) -> Option<TelemetryContext> {
        self.context_storage.write().remove(key)
    }
    
    pub fn propagate_context_to_tracing_span(&self, span: &Span, context: &TelemetryContext) {
        span.record("trace_id", &context.trace_id);
        span.record("span_id", &context.span_id);
        span.record("correlation_id", &context.correlation_id);
        
        if let Some(user_id) = &context.user_id {
            span.record("user_id", user_id);
        }
        
        if let Some(request_id) = &context.request_id {
            span.record("request_id", request_id);
        }
        
        // Set OpenTelemetry context
        #[cfg(feature = "telemetry")]
        {
            let otel_context = Context::current();
            span.set_parent(otel_context);
        }
    }
    
    #[cfg(feature = "telemetry")]
    pub fn shutdown(self) -> Result<(), TraceError> {
        let provider = Arc::try_unwrap(self.tracer_provider)
            .map_err(|_| TraceError::Other("Failed to unwrap tracer provider".into()))?;
        let _ = provider.force_flush();
        Ok(())
    }
    
    #[cfg(not(feature = "telemetry"))]
    pub fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

#[cfg(feature = "telemetry")]
pub struct DatabaseSpan {
    span: opentelemetry_sdk::trace::Span,
    context: TelemetryContext,
    processor: Arc<dyn SpanProcessor + Send + Sync>,
    start_time: SystemTime,
}

#[cfg(not(feature = "telemetry"))]
pub struct DatabaseSpan {
    context: TelemetryContext,
    processor: Arc<dyn SpanProcessor + Send + Sync>,
    start_time: SystemTime,
}

impl DatabaseSpan {
    #[cfg(feature = "telemetry")]
    pub fn new(
        span: opentelemetry_sdk::trace::Span,
        context: TelemetryContext,
        processor: Arc<dyn SpanProcessor + Send + Sync>,
    ) -> Self {
        let span_data = DatabaseSpanData {
            span_id: context.span_id.clone(),
            trace_id: context.trace_id.clone(),
            parent_span_id: context.parent_span_id.clone(),
            operation_name: "unknown".to_string(),
            start_time: SystemTime::now(),
            end_time: None,
            duration: None,
            status: SpanStatus::Ok,
            attributes: context.custom_attributes.clone(),
            events: Vec::new(),
        };
        
        processor.on_start(&span_data);
        
        Self {
            span,
            context,
            processor,
            start_time: SystemTime::now(),
        }
    }
    
    #[cfg(not(feature = "telemetry"))]
    pub fn new_mock(
        context: TelemetryContext,
        processor: Arc<dyn SpanProcessor + Send + Sync>,
    ) -> Self {
        let span_data = DatabaseSpanData {
            span_id: context.span_id.clone(),
            trace_id: context.trace_id.clone(),
            parent_span_id: context.parent_span_id.clone(),
            operation_name: "unknown".to_string(),
            start_time: SystemTime::now(),
            end_time: None,
            duration: None,
            status: SpanStatus::Ok,
            attributes: context.custom_attributes.clone(),
            events: Vec::new(),
        };
        
        processor.on_start(&span_data);
        
        Self {
            context,
            processor,
            start_time: SystemTime::now(),
        }
    }
    
    #[cfg(feature = "telemetry")]
    pub fn set_attribute(&mut self, key: &str, value: &str) {
        self.span.set_attribute(KeyValue::new(key.to_string(), value.to_string()));
    }
    
    #[cfg(not(feature = "telemetry"))]
    pub fn set_attribute(&mut self, _key: &str, _value: &str) {
        // No-op for non-telemetry builds
    }
    
    #[cfg(feature = "telemetry")]
    pub fn add_event(&mut self, name: &str, attributes: Vec<(String, String)>) {
        let otel_attributes: Vec<KeyValue> = attributes
            .into_iter()
            .map(|(k, v)| KeyValue::new(k, v))
            .collect();
        
        self.span.add_event(name.to_string(), otel_attributes);
    }
    
    #[cfg(not(feature = "telemetry"))]
    pub fn add_event(&mut self, _name: &str, _attributes: Vec<(String, String)>) {
        // No-op for non-telemetry builds
    }
    
    #[cfg(feature = "telemetry")]
    pub fn set_status(&mut self, status: SpanStatus) {
        match status {
            SpanStatus::Ok => self.span.set_status(opentelemetry::trace::Status::Ok),
            SpanStatus::Error(message) => {
                self.span.set_status(opentelemetry::trace::Status::error(message))
            }
        }
    }
    
    #[cfg(not(feature = "telemetry"))]
    pub fn set_status(&mut self, _status: SpanStatus) {
        // No-op for non-telemetry builds
    }
    
    #[cfg(feature = "telemetry")]
    pub fn record_exception(&mut self, error: &dyn std::error::Error) {
        self.span.record_error(error);
        self.set_status(SpanStatus::Error(error.to_string()));
    }
    
    #[cfg(not(feature = "telemetry"))]
    pub fn record_exception(&mut self, error: &dyn std::error::Error) {
        self.set_status(SpanStatus::Error(error.to_string()));
    }
    
    pub fn finish(self) {
        let duration = self.start_time.elapsed().unwrap_or(Duration::ZERO);
        
        let span_data = DatabaseSpanData {
            span_id: self.context.span_id,
            trace_id: self.context.trace_id,
            parent_span_id: self.context.parent_span_id,
            operation_name: "finished".to_string(),
            start_time: self.start_time,
            end_time: Some(SystemTime::now()),
            duration: Some(duration),
            status: SpanStatus::Ok,
            attributes: self.context.custom_attributes,
            events: Vec::new(),
        };
        
        self.processor.on_end(&span_data);
        #[cfg(feature = "telemetry")]
        {
            self.span.end();
        }
    }
    
    pub fn context(&self) -> &TelemetryContext {
        &self.context
    }
}

#[derive(Debug, Clone)]
pub enum SpanStatus {
    Ok,
    Error(String),
}

#[derive(Debug, Clone)]
pub struct DatabaseSpanData {
    pub span_id: String,
    pub trace_id: String,
    pub parent_span_id: Option<String>,
    pub operation_name: String,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub duration: Option<Duration>,
    pub status: SpanStatus,
    pub attributes: HashMap<String, String>,
    pub events: Vec<SpanEvent>,
}

#[derive(Debug, Clone)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: SystemTime,
    pub attributes: HashMap<String, String>,
}

pub trait SpanProcessor: Send + Sync {
    fn on_start(&self, span: &DatabaseSpanData);
    fn on_end(&self, span: &DatabaseSpanData);
    fn shutdown(&self);
}

pub struct DatabaseSpanProcessor {
    spans: Arc<parking_lot::RwLock<Vec<DatabaseSpanData>>>,
    metrics_collector: Arc<crate::features::logging::metrics::HistogramCollector>,
}

impl DatabaseSpanProcessor {
    pub fn new() -> Self {
        Self {
            spans: Arc::new(parking_lot::RwLock::new(Vec::new())),
            metrics_collector: Arc::new(crate::features::logging::metrics::HistogramCollector::new()),
        }
    }
    
    pub fn get_recent_spans(&self, limit: usize) -> Vec<DatabaseSpanData> {
        let spans = self.spans.read();
        spans.iter().rev().take(limit).cloned().collect()
    }
    
    pub fn clear_spans(&self) {
        self.spans.write().clear();
    }
}

impl SpanProcessor for DatabaseSpanProcessor {
    fn on_start(&self, span: &DatabaseSpanData) {
        tracing::debug!(
            span_id = %span.span_id,
            trace_id = %span.trace_id,
            operation = %span.operation_name,
            "Span started"
        );
    }
    
    fn on_end(&self, span: &DatabaseSpanData) {
        // Record metrics
        if let Some(duration) = span.duration {
            self.metrics_collector.record(&span.operation_name, duration);
        }
        
        // Store span data
        self.spans.write().push(span.clone());
        
        // Log span completion
        tracing::debug!(
            span_id = %span.span_id,
            trace_id = %span.trace_id,
            operation = %span.operation_name,
            duration_ms = span.duration.map(|d| d.as_millis()).unwrap_or(0),
            status = match &span.status {
                SpanStatus::Ok => "ok",
                SpanStatus::Error(_) => "error",
            },
            "Span ended"
        );
    }
    
    fn shutdown(&self) {
        self.clear_spans();
    }
}

// Helper macros for telemetry
#[macro_export]
macro_rules! telemetry_span {
    ($telemetry_manager:expr, $name:expr, $context:expr) => {
        $telemetry_manager.create_span($name, $context)
    };
}

#[macro_export]
macro_rules! telemetry_child_span {
    ($telemetry_manager:expr, $name:expr, $parent_context:expr) => {
        $telemetry_manager.create_child_span($name, $parent_context)
    };
}

pub struct DistributedTracing {
    manager: Arc<TelemetryManager>,
    correlation_header: String,
}

impl DistributedTracing {
    pub fn new(
        manager: Arc<TelemetryManager>,
        correlation_header: String,
    ) -> Self {
        Self {
            manager,
            correlation_header,
        }
    }
    
    pub fn extract_context_from_headers(
        &self,
        headers: &HashMap<String, String>,
    ) -> Option<TelemetryContext> {
        if let Some(correlation_id) = headers.get(&self.correlation_header) {
            let mut context = TelemetryContext::new()
                .with_correlation_id(correlation_id.clone());
            
            // Extract other standard headers
            if let Some(trace_id) = headers.get("x-trace-id") {
                context.trace_id = trace_id.clone();
            }
            
            if let Some(user_id) = headers.get("x-user-id") {
                context = context.with_user_id(user_id.clone());
            }
            
            if let Some(request_id) = headers.get("x-request-id") {
                context = context.with_request_id(request_id.clone());
            }
            
            Some(context)
        } else {
            None
        }
    }
    
    pub fn inject_context_to_headers(
        &self,
        context: &TelemetryContext,
        headers: &mut HashMap<String, String>,
    ) {
        headers.insert(self.correlation_header.clone(), context.correlation_id.clone());
        headers.insert("x-trace-id".to_string(), context.trace_id.clone());
        
        if let Some(user_id) = &context.user_id {
            headers.insert("x-user-id".to_string(), user_id.clone());
        }
        
        if let Some(request_id) = &context.request_id {
            headers.insert("x-request-id".to_string(), request_id.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_telemetry_context_creation() {
        let context = TelemetryContext::new()
            .with_correlation_id("test-correlation".to_string())
            .with_user_id("user123".to_string());
        
        assert_eq!(context.correlation_id, "test-correlation");
        assert_eq!(context.user_id.unwrap(), "user123");
    }
    
    #[test]
    fn test_child_context() {
        let parent = TelemetryContext::new();
        let child = parent.child_context();
        
        assert_eq!(child.trace_id, parent.trace_id);
        assert_eq!(child.parent_span_id.unwrap(), parent.span_id);
        assert_ne!(child.span_id, parent.span_id);
    }
    
    #[test]
    fn test_context_headers() {
        let context = TelemetryContext::new()
            .with_correlation_id("test-correlation".to_string())
            .with_user_id("user123".to_string());
        
        let distributed = DistributedTracing::new(
            Arc::new(TelemetryManager::new("test", "1.0", None).unwrap()),
            "x-correlation-id".to_string(),
        );
        
        let mut headers = HashMap::new();
        distributed.inject_context_to_headers(&context, &mut headers);
        
        assert_eq!(headers.get("x-correlation-id").unwrap(), "test-correlation");
        assert_eq!(headers.get("x-user-id").unwrap(), "user123");
        
        let extracted = distributed.extract_context_from_headers(&headers).unwrap();
        assert_eq!(extracted.correlation_id, "test-correlation");
        assert_eq!(extracted.user_id.unwrap(), "user123");
    }
}