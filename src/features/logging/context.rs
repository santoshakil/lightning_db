#[cfg(feature = "telemetry")]
use opentelemetry::{Context as OtelContext, KeyValue};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{span, Span};
#[cfg(feature = "telemetry")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

thread_local! {
    static CURRENT_CONTEXT: std::cell::RefCell<Option<TraceContext>> = const { std::cell::RefCell::new(None) };
}

#[derive(Debug, Clone)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub correlation_id: String,
    pub user_id: Option<String>,
    pub request_id: Option<String>,
    pub session_id: Option<String>,
    pub operation_id: String,
    pub baggage: HashMap<String, String>,
    pub sampling_decision: SamplingDecision,
}

#[derive(Debug, Clone)]
pub struct SamplingDecision {
    pub sampled: bool,
    pub rate: f64,
    pub reason: String,
}

impl Default for SamplingDecision {
    fn default() -> Self {
        Self {
            sampled: true,
            rate: 1.0,
            reason: "default".to_string(),
        }
    }
}

impl TraceContext {
    pub fn new() -> Self {
        Self {
            trace_id: generate_trace_id(),
            span_id: generate_span_id(),
            parent_span_id: None,
            correlation_id: Uuid::new_v4().to_string(),
            user_id: None,
            request_id: None,
            session_id: None,
            operation_id: Uuid::new_v4().to_string(),
            baggage: HashMap::new(),
            sampling_decision: SamplingDecision::default(),
        }
    }

    pub fn from_headers(headers: &HashMap<String, String>) -> Self {
        let trace_id = headers
            .get("x-trace-id")
            .or_else(|| headers.get("traceparent"))
            .cloned()
            .unwrap_or_else(generate_trace_id);

        let span_id = headers
            .get("x-span-id")
            .cloned()
            .unwrap_or_else(generate_span_id);

        let parent_span_id = headers.get("x-parent-span-id").cloned();

        let correlation_id = headers
            .get("x-correlation-id")
            .or_else(|| headers.get("x-request-id"))
            .cloned()
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let mut baggage = HashMap::new();
        if let Some(baggage_header) = headers.get("baggage") {
            baggage = parse_baggage(baggage_header);
        }

        Self {
            trace_id,
            span_id,
            parent_span_id,
            correlation_id,
            user_id: headers.get("x-user-id").cloned(),
            request_id: headers.get("x-request-id").cloned(),
            session_id: headers.get("x-session-id").cloned(),
            operation_id: Uuid::new_v4().to_string(),
            baggage,
            sampling_decision: SamplingDecision::default(),
        }
    }

    pub fn child(&self, operation_name: &str) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: generate_span_id(),
            parent_span_id: Some(self.span_id.clone()),
            correlation_id: self.correlation_id.clone(),
            user_id: self.user_id.clone(),
            request_id: self.request_id.clone(),
            session_id: self.session_id.clone(),
            operation_id: format!("{}::{}", self.operation_id, operation_name),
            baggage: self.baggage.clone(),
            sampling_decision: self.sampling_decision.clone(),
        }
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

    pub fn with_baggage_item(mut self, key: String, value: String) -> Self {
        self.baggage.insert(key, value);
        self
    }

    pub fn with_sampling_decision(mut self, decision: SamplingDecision) -> Self {
        self.sampling_decision = decision;
        self
    }

    pub fn to_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();

        headers.insert("x-trace-id".to_string(), self.trace_id.clone());
        headers.insert("x-span-id".to_string(), self.span_id.clone());
        headers.insert("x-correlation-id".to_string(), self.correlation_id.clone());

        if let Some(parent_id) = &self.parent_span_id {
            headers.insert("x-parent-span-id".to_string(), parent_id.clone());
        }

        if let Some(user_id) = &self.user_id {
            headers.insert("x-user-id".to_string(), user_id.clone());
        }

        if let Some(request_id) = &self.request_id {
            headers.insert("x-request-id".to_string(), request_id.clone());
        }

        if let Some(session_id) = &self.session_id {
            headers.insert("x-session-id".to_string(), session_id.clone());
        }

        if !self.baggage.is_empty() {
            headers.insert("baggage".to_string(), format_baggage(&self.baggage));
        }

        headers
    }

    #[cfg(feature = "telemetry")]
    pub fn to_span_attributes(&self) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("trace.trace_id", self.trace_id.clone()),
            KeyValue::new("trace.span_id", self.span_id.clone()),
            KeyValue::new("correlation.id", self.correlation_id.clone()),
            KeyValue::new("operation.id", self.operation_id.clone()),
        ];

        if let Some(parent_id) = &self.parent_span_id {
            attributes.push(KeyValue::new("trace.parent_span_id", parent_id.clone()));
        }

        if let Some(user_id) = &self.user_id {
            attributes.push(KeyValue::new("user.id", user_id.clone()));
        }

        if let Some(request_id) = &self.request_id {
            attributes.push(KeyValue::new("request.id", request_id.clone()));
        }

        if let Some(session_id) = &self.session_id {
            attributes.push(KeyValue::new("session.id", session_id.clone()));
        }

        // Add baggage as attributes
        for (key, value) in &self.baggage {
            attributes.push(KeyValue::new(format!("baggage.{}", key), value.clone()));
        }

        attributes
    }

    #[cfg(not(feature = "telemetry"))]
    pub fn to_span_attributes(&self) -> Vec<(&'static str, String)> {
        vec![]
    }

    pub fn is_sampled(&self) -> bool {
        self.sampling_decision.sampled
    }
}

pub struct ContextManager {
    contexts: Arc<RwLock<HashMap<String, TraceContext>>>,
    context_hierarchy: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl ContextManager {
    pub fn new() -> Self {
        Self {
            contexts: Arc::new(RwLock::new(HashMap::new())),
            context_hierarchy: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn store_context(&self, key: String, context: TraceContext) {
        // Store parent-child relationship
        if let Some(parent_id) = &context.parent_span_id {
            let mut hierarchy = self.context_hierarchy.write();
            hierarchy
                .entry(parent_id.clone())
                .or_insert_with(Vec::new)
                .push(context.span_id.clone());
        }

        self.contexts.write().insert(key, context);
    }

    pub fn get_context(&self, key: &str) -> Option<TraceContext> {
        self.contexts.read().get(key).cloned()
    }

    pub fn remove_context(&self, key: &str) -> Option<TraceContext> {
        if let Some(context) = self.contexts.write().remove(key) {
            // Clean up hierarchy
            let mut hierarchy = self.context_hierarchy.write();
            if let Some(parent_id) = &context.parent_span_id {
                if let Some(children) = hierarchy.get_mut(parent_id) {
                    children.retain(|child_id| child_id != &context.span_id);
                    if children.is_empty() {
                        hierarchy.remove(parent_id);
                    }
                }
            }
            Some(context)
        } else {
            None
        }
    }

    pub fn get_children(&self, parent_span_id: &str) -> Vec<String> {
        self.context_hierarchy
            .read()
            .get(parent_span_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn cleanup_expired_contexts(&self, _max_age: std::time::Duration) {
        // In a real implementation, you'd track creation times and clean up old contexts
        // For simplicity, this is a placeholder
    }
}

// Thread-local context management
pub fn set_current_context(context: TraceContext) {
    CURRENT_CONTEXT.with(|c| {
        *c.borrow_mut() = Some(context);
    });
}

pub fn get_current_context() -> Option<TraceContext> {
    CURRENT_CONTEXT.with(|c| c.borrow().clone())
}

pub fn with_context<F, R>(context: TraceContext, f: F) -> R
where
    F: FnOnce() -> R,
{
    let previous = get_current_context();
    set_current_context(context);

    let result = f();

    // Restore previous context
    if let Some(prev) = previous {
        set_current_context(prev);
    } else {
        CURRENT_CONTEXT.with(|c| {
            *c.borrow_mut() = None;
        });
    }

    result
}

pub struct TracedSpan {
    span: Span,
    context: TraceContext,
}

impl TracedSpan {
    pub fn new(name: &str, context: TraceContext) -> Self {
        let span = span!(tracing::Level::INFO, "operation", operation_name = name);

        // Add context attributes to the span
        #[cfg(feature = "telemetry")]
        for attribute in context.to_span_attributes() {
            span.record(
                attribute.key.as_str(),
                &tracing::field::display(&attribute.value),
            );
        }

        #[cfg(not(feature = "telemetry"))]
        for (key, value) in context.to_span_attributes() {
            span.record(key, &tracing::field::display(&value));
        }

        // Set OpenTelemetry context
        #[cfg(feature = "telemetry")]
        {
            let otel_context = OtelContext::current();
            span.set_parent(otel_context);
        }

        Self { span, context }
    }

    pub fn enter(&self) -> tracing::span::Entered<'_> {
        self.span.enter()
    }

    pub fn context(&self) -> &TraceContext {
        &self.context
    }

    pub fn add_event(&self, name: &str, attributes: Vec<(&str, &str)>) {
        self.span.record("event_name", &name);
        for (key, value) in attributes {
            self.span.record(key, &value);
        }
    }

    pub fn set_error(&self, error: &dyn std::error::Error) {
        self.span.record("error", &tracing::field::display(error));
        self.span.record("error.type", &error.to_string());
    }
}

// Context propagation for async operations
pub struct AsyncContextPropagator {
    manager: Arc<ContextManager>,
}

impl AsyncContextPropagator {
    pub fn new(manager: Arc<ContextManager>) -> Self {
        Self { manager }
    }

    pub async fn with_context<F, R>(&self, context: TraceContext, future: F) -> R
    where
        F: std::future::Future<Output = R> + Send,
    {
        let context_key = format!("async_{}", context.span_id);
        self.manager
            .store_context(context_key.clone(), context.clone());

        // Set thread-local context for the async block
        set_current_context(context);

        let result = future.await;

        self.manager.remove_context(&context_key);
        result
    }
}

// Utility functions
fn generate_trace_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

fn generate_span_id() -> String {
    format!("{:016x}", rand::random::<u64>())
}

fn parse_baggage(baggage_header: &str) -> HashMap<String, String> {
    let mut baggage = HashMap::new();

    for item in baggage_header.split(',') {
        let item = item.trim();
        if let Some(eq_pos) = item.find('=') {
            let key = item[..eq_pos].trim().to_string();
            let value = item[eq_pos + 1..].trim().to_string();
            baggage.insert(key, value);
        }
    }

    baggage
}

fn format_baggage(baggage: &HashMap<String, String>) -> String {
    baggage
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

// Macros for easier context usage
#[macro_export]
macro_rules! with_trace_context {
    ($context:expr, $code:block) => {
        $crate::features::logging::context::with_context($context, || $code)
    };
}

#[macro_export]
macro_rules! current_trace_context {
    () => {
        $crate::features::logging::context::get_current_context()
    };
}

#[macro_export]
macro_rules! traced_span {
    ($name:expr) => {
        if let Some(context) = $crate::features::logging::context::get_current_context() {
            let child_context = context.child($name);
            $crate::features::logging::context::TracedSpan::new($name, child_context)
        } else {
            let context = $crate::features::logging::context::TraceContext::new();
            $crate::features::logging::context::TracedSpan::new($name, context)
        }
    };

    ($name:expr, $context:expr) => {
        $crate::features::logging::context::TracedSpan::new($name, $context)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_context_creation() {
        let context = TraceContext::new();
        assert!(!context.trace_id.is_empty());
        assert!(!context.span_id.is_empty());
        assert!(!context.correlation_id.is_empty());
        assert!(context.parent_span_id.is_none());
    }

    #[test]
    fn test_child_context() {
        let parent = TraceContext::new();
        let child = parent.child("test_operation");

        assert_eq!(child.trace_id, parent.trace_id);
        assert_eq!(child.correlation_id, parent.correlation_id);
        assert_eq!(child.parent_span_id.unwrap(), parent.span_id);
        assert_ne!(child.span_id, parent.span_id);
        assert!(child.operation_id.contains("test_operation"));
    }

    #[test]
    fn test_context_from_headers() {
        let mut headers = HashMap::new();
        headers.insert("x-trace-id".to_string(), "test-trace-id".to_string());
        headers.insert("x-span-id".to_string(), "test-span-id".to_string());
        headers.insert(
            "x-correlation-id".to_string(),
            "test-correlation-id".to_string(),
        );
        headers.insert("x-user-id".to_string(), "user123".to_string());
        headers.insert(
            "baggage".to_string(),
            "key1=value1, key2=value2".to_string(),
        );

        let context = TraceContext::from_headers(&headers);

        assert_eq!(context.trace_id, "test-trace-id");
        assert_eq!(context.span_id, "test-span-id");
        assert_eq!(context.correlation_id, "test-correlation-id");
        assert_eq!(context.user_id.unwrap(), "user123");
        assert_eq!(context.baggage.get("key1").unwrap(), "value1");
        assert_eq!(context.baggage.get("key2").unwrap(), "value2");
    }

    #[test]
    fn test_context_to_headers() {
        let context = TraceContext::new()
            .with_user_id("user123".to_string())
            .with_baggage_item("key1".to_string(), "value1".to_string());

        let headers = context.to_headers();

        assert_eq!(headers.get("x-trace-id").unwrap(), &context.trace_id);
        assert_eq!(headers.get("x-span-id").unwrap(), &context.span_id);
        assert_eq!(headers.get("x-user-id").unwrap(), "user123");
        assert!(headers.get("baggage").unwrap().contains("key1=value1"));
    }

    #[test]
    fn test_context_manager() {
        let manager = ContextManager::new();
        let context = TraceContext::new();
        let key = "test-key".to_string();

        manager.store_context(key.clone(), context.clone());
        let retrieved = manager.get_context(&key).unwrap();

        assert_eq!(retrieved.trace_id, context.trace_id);
        assert_eq!(retrieved.span_id, context.span_id);
    }

    #[test]
    fn test_thread_local_context() {
        let context = TraceContext::new();
        let trace_id = context.trace_id.clone();

        with_context(context, || {
            let current = get_current_context().unwrap();
            assert_eq!(current.trace_id, trace_id);
        });

        // Context should be cleared after the closure
        assert!(get_current_context().is_none());
    }

    #[test]
    fn test_baggage_parsing() {
        let baggage_header = "key1=value1, key2=value2, key3=value3";
        let baggage = parse_baggage(baggage_header);

        assert_eq!(baggage.len(), 3);
        assert_eq!(baggage.get("key1").unwrap(), "value1");
        assert_eq!(baggage.get("key2").unwrap(), "value2");
        assert_eq!(baggage.get("key3").unwrap(), "value3");
    }

    #[test]
    fn test_baggage_formatting() {
        let mut baggage = HashMap::new();
        baggage.insert("key1".to_string(), "value1".to_string());
        baggage.insert("key2".to_string(), "value2".to_string());

        let formatted = format_baggage(&baggage);
        assert!(formatted.contains("key1=value1"));
        assert!(formatted.contains("key2=value2"));
    }
}
