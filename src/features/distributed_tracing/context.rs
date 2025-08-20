//! Trace Context Propagation
//!
//! Handles trace context propagation across operations and service boundaries
//! following W3C Trace Context specification.

use super::TraceContext;
use crate::core::error::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;

thread_local! {
    static CURRENT_CONTEXT: std::cell::RefCell<Option<TraceContext>> = std::cell::RefCell::new(None);
}

/// Context manager for handling trace context propagation
#[derive(Debug, Clone)]
pub struct ContextManager {
    contexts: Arc<RwLock<HashMap<String, TraceContext>>>,
}

impl ContextManager {
    /// Create a new context manager
    pub fn new() -> Self {
        Self {
            contexts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set context for a thread/operation
    pub fn set_context(&self, key: String, context: TraceContext) {
        if let Ok(mut contexts) = self.contexts.write() {
            contexts.insert(key, context);
        }
    }

    /// Get context for a thread/operation
    pub fn get_context(&self, key: &str) -> Option<TraceContext> {
        if let Ok(contexts) = self.contexts.read() {
            contexts.get(key).cloned()
        } else {
            None
        }
    }

    /// Remove context
    pub fn remove_context(&self, key: &str) {
        if let Ok(mut contexts) = self.contexts.write() {
            contexts.remove(key);
        }
    }

    /// Get current thread's context
    pub fn current_context(&self) -> Option<TraceContext> {
        let thread_id = format!("{:?}", thread::current().id());
        self.get_context(&thread_id)
    }

    /// Set current thread's context
    pub fn set_current_context(&self, context: TraceContext) {
        let thread_id = format!("{:?}", thread::current().id());
        self.set_context(thread_id, context);
    }
}

impl Default for ContextManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current trace context from thread-local storage
pub fn current_context() -> Option<TraceContext> {
    CURRENT_CONTEXT.with(|ctx| ctx.borrow().clone())
}

/// Set current trace context in thread-local storage
pub fn set_current_context(context: TraceContext) {
    CURRENT_CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = Some(context);
    });
}

/// Clear current trace context
pub fn clear_current_context() {
    CURRENT_CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = None;
    });
}

/// Execute a function with a specific trace context
pub fn with_context<F, R>(context: TraceContext, f: F) -> R
where
    F: FnOnce() -> R,
{
    let previous = current_context();
    set_current_context(context);

    let result = f();

    match previous {
        Some(prev) => set_current_context(prev),
        None => clear_current_context(),
    }

    result
}

/// W3C Trace Context header parser/serializer
pub struct W3CTraceContext;

impl W3CTraceContext {
    /// Parse trace context from W3C traceparent header
    /// Format: 00-{trace_id}-{span_id}-{flags}
    pub fn parse_traceparent(header: &str) -> Result<TraceContext> {
        let parts: Vec<&str> = header.split('-').collect();

        if parts.len() != 4 {
            return Err(crate::core::error::Error::InvalidInput(
                "Invalid traceparent header format".to_string(),
            ));
        }

        let version = parts[0];
        if version != "00" {
            return Err(crate::core::error::Error::InvalidInput(
                "Unsupported trace context version".to_string(),
            ));
        }

        let trace_id = parts[1].to_string();
        let span_id = parts[2].to_string();
        let flags = u8::from_str_radix(parts[3], 16)
            .map_err(|_| crate::core::error::Error::InvalidInput("Invalid trace flags".to_string()))?;

        // Validate trace_id and span_id lengths
        if trace_id.len() != 32 || span_id.len() != 16 {
            return Err(crate::core::error::Error::InvalidInput(
                "Invalid trace_id or span_id length".to_string(),
            ));
        }

        Ok(TraceContext {
            trace_id,
            span_id,
            parent_span_id: None,
            trace_flags: flags,
            trace_state: String::new(),
            baggage: HashMap::new(),
        })
    }

    /// Format trace context as W3C traceparent header
    pub fn format_traceparent(context: &TraceContext) -> String {
        format!(
            "00-{}-{}-{:02x}",
            context.trace_id, context.span_id, context.trace_flags
        )
    }

    /// Parse trace state from W3C tracestate header
    /// Format: key1=value1,key2=value2
    pub fn parse_tracestate(header: &str) -> Result<String> {
        // Basic validation - in production, this would be more thorough
        if header.len() > 512 {
            return Err(crate::core::error::Error::InvalidInput(
                "Tracestate header too long".to_string(),
            ));
        }

        Ok(header.to_string())
    }

    /// Format trace state as W3C tracestate header
    pub fn format_tracestate(trace_state: &str) -> String {
        trace_state.to_string()
    }

    /// Parse baggage from W3C baggage header
    /// Format: key1=value1,key2=value2;metadata
    pub fn parse_baggage(header: &str) -> Result<HashMap<String, String>> {
        let mut baggage = HashMap::new();

        for item in header.split(',') {
            let item = item.trim();
            if let Some(eq_pos) = item.find('=') {
                let key = item[..eq_pos].trim();
                let value_with_metadata = &item[eq_pos + 1..];

                // Remove metadata (everything after ';')
                let value = if let Some(semicolon_pos) = value_with_metadata.find(';') {
                    &value_with_metadata[..semicolon_pos]
                } else {
                    value_with_metadata
                }
                .trim();

                baggage.insert(key.to_string(), value.to_string());
            }
        }

        Ok(baggage)
    }

    /// Format baggage as W3C baggage header
    pub fn format_baggage(baggage: &HashMap<String, String>) -> String {
        baggage
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// HTTP header names for trace context propagation
pub struct TraceHeaders;

impl TraceHeaders {
    pub const TRACEPARENT: &'static str = "traceparent";
    pub const TRACESTATE: &'static str = "tracestate";
    pub const BAGGAGE: &'static str = "baggage";
}

/// Extract trace context from HTTP headers
pub fn extract_from_headers(headers: &HashMap<String, String>) -> Result<Option<TraceContext>> {
    if let Some(traceparent) = headers.get(TraceHeaders::TRACEPARENT) {
        let mut context = W3CTraceContext::parse_traceparent(traceparent)?;

        // Parse tracestate if present
        if let Some(tracestate) = headers.get(TraceHeaders::TRACESTATE) {
            context.trace_state = W3CTraceContext::parse_tracestate(tracestate)?;
        }

        // Parse baggage if present
        if let Some(baggage) = headers.get(TraceHeaders::BAGGAGE) {
            context.baggage = W3CTraceContext::parse_baggage(baggage)?;
        }

        Ok(Some(context))
    } else {
        Ok(None)
    }
}

/// Inject trace context into HTTP headers
pub fn inject_into_headers(context: &TraceContext, headers: &mut HashMap<String, String>) {
    headers.insert(
        TraceHeaders::TRACEPARENT.to_string(),
        W3CTraceContext::format_traceparent(context),
    );

    if !context.trace_state.is_empty() {
        headers.insert(
            TraceHeaders::TRACESTATE.to_string(),
            W3CTraceContext::format_tracestate(&context.trace_state),
        );
    }

    if !context.baggage.is_empty() {
        headers.insert(
            TraceHeaders::BAGGAGE.to_string(),
            W3CTraceContext::format_baggage(&context.baggage),
        );
    }
}

/// Context propagation utilities for database operations
pub struct DatabaseContextPropagation;

impl DatabaseContextPropagation {
    /// Create context for database operation
    pub fn create_db_context(operation: &str, table: Option<&str>) -> TraceContext {
        let mut context = if let Some(current) = current_context() {
            current.create_child()
        } else {
            TraceContext::new_root()
        };

        // Add database-specific baggage
        context.set_baggage("db.operation".to_string(), operation.to_string());

        if let Some(table_name) = table {
            context.set_baggage("db.table".to_string(), table_name.to_string());
        }

        context.set_baggage("db.system".to_string(), "lightning_db".to_string());

        context
    }

    /// Create context for transaction
    pub fn create_transaction_context(transaction_id: u64) -> TraceContext {
        let mut context = if let Some(current) = current_context() {
            current.create_child()
        } else {
            TraceContext::new_root()
        };

        context.set_baggage("db.transaction_id".to_string(), transaction_id.to_string());
        context.set_baggage("db.operation_type".to_string(), "transaction".to_string());

        context
    }

    /// Create context for query operations
    pub fn create_query_context(query_type: &str, key_pattern: Option<&str>) -> TraceContext {
        let mut context = if let Some(current) = current_context() {
            current.create_child()
        } else {
            TraceContext::new_root()
        };

        context.set_baggage("db.query_type".to_string(), query_type.to_string());

        if let Some(pattern) = key_pattern {
            context.set_baggage("db.key_pattern".to_string(), pattern.to_string());
        }

        context
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_manager() {
        let manager = ContextManager::new();
        let context = TraceContext::new_root();
        let key = "test_key".to_string();

        manager.set_context(key.clone(), context.clone());
        let retrieved = manager.get_context(&key);

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().trace_id, context.trace_id);
    }

    #[test]
    fn test_thread_local_context() {
        let context = TraceContext::new_root();

        set_current_context(context.clone());
        let retrieved = current_context();

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().trace_id, context.trace_id);

        clear_current_context();
        assert!(current_context().is_none());
    }

    #[test]
    fn test_with_context() {
        let context1 = TraceContext::new_root();
        let context2 = TraceContext::new_root();

        set_current_context(context1.clone());

        let result = with_context(context2.clone(), || current_context().unwrap().trace_id);

        assert_eq!(result, context2.trace_id);
        assert_eq!(current_context().unwrap().trace_id, context1.trace_id);
    }

    #[test]
    fn test_w3c_traceparent_parsing() {
        let header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let context = W3CTraceContext::parse_traceparent(header).unwrap();

        assert_eq!(context.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(context.span_id, "b7ad6b7169203331");
        assert_eq!(context.trace_flags, 0x01);
        assert!(context.is_sampled());
    }

    #[test]
    fn test_w3c_traceparent_formatting() {
        let mut context = TraceContext::new_root();
        context.trace_id = "0af7651916cd43dd8448eb211c80319c".to_string();
        context.span_id = "b7ad6b7169203331".to_string();
        context.trace_flags = 0x01;

        let header = W3CTraceContext::format_traceparent(&context);
        assert_eq!(
            header,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        );
    }

    #[test]
    fn test_baggage_parsing() {
        let header = "userId=alice,serverNode=DF:28,isProduction=false";
        let baggage = W3CTraceContext::parse_baggage(header).unwrap();

        assert_eq!(baggage.get("userId"), Some(&"alice".to_string()));
        assert_eq!(baggage.get("serverNode"), Some(&"DF:28".to_string()));
        assert_eq!(baggage.get("isProduction"), Some(&"false".to_string()));
    }

    #[test]
    fn test_header_extraction_injection() {
        let original_context = TraceContext::new_root();
        let mut headers = HashMap::new();

        inject_into_headers(&original_context, &mut headers);
        let extracted_context = extract_from_headers(&headers).unwrap().unwrap();

        assert_eq!(extracted_context.trace_id, original_context.trace_id);
        assert_eq!(extracted_context.span_id, original_context.span_id);
        assert_eq!(extracted_context.trace_flags, original_context.trace_flags);
    }

    #[test]
    fn test_database_context_creation() {
        let context = DatabaseContextPropagation::create_db_context("SELECT", Some("users"));

        assert_eq!(
            context.get_baggage("db.operation"),
            Some(&"SELECT".to_string())
        );
        assert_eq!(context.get_baggage("db.table"), Some(&"users".to_string()));
        assert_eq!(
            context.get_baggage("db.system"),
            Some(&"lightning_db".to_string())
        );
    }
}
