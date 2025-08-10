//! OpenTelemetry Integration
//!
//! Enhanced OpenTelemetry integration for Lightning DB with full OTLP support,
//! resource detection, and production-grade configuration.

use super::{Span, TraceContext, TraceExporter, SpanStatus, SpanKind};
use crate::Result;
use base64::{Engine as _, engine::general_purpose};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use serde_json::{json, Value};

/// OpenTelemetry OTLP trace exporter with full specification compliance
#[cfg(feature = "http-client")]
pub struct OtlpTraceExporter {
    client: reqwest::blocking::Client,
    endpoint: String,
    headers: HashMap<String, String>,
    resource: OtlpResource,
    instrumentation_library: InstrumentationLibrary,
    batch_size: usize,
    timeout_ms: u64,
}

#[cfg(not(feature = "http-client"))]
pub struct OtlpTraceExporter {
    endpoint: String,
    headers: HashMap<String, String>,
    resource: OtlpResource,
    instrumentation_library: InstrumentationLibrary,
    batch_size: usize,
    timeout_ms: u64,
}

impl OtlpTraceExporter {
    /// Create a new OTLP trace exporter
    pub fn new(endpoint: String) -> Self {
        #[cfg(feature = "http-client")]
        {
            let client = reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client");
            
            Self {
                client,
                endpoint,
                headers: HashMap::new(),
                resource: OtlpResource::auto_detect(),
                instrumentation_library: InstrumentationLibrary::default(),
                batch_size: 100,
                timeout_ms: 30000,
            }
        }
        #[cfg(not(feature = "http-client"))]
        {
            Self {
                endpoint,
                headers: HashMap::new(),
                resource: OtlpResource::auto_detect(),
                instrumentation_library: InstrumentationLibrary::default(),
                batch_size: 100,
                timeout_ms: 30000,
            }
        }
    }
    
    /// Set authentication header for the OTLP endpoint
    pub fn with_auth_header(mut self, header_value: String) -> Self {
        self.headers.insert("Authorization".to_string(), header_value);
        self
    }
    
    /// Set API key for the OTLP endpoint
    pub fn with_api_key(mut self, api_key: String) -> Self {
        self.headers.insert("X-API-Key".to_string(), api_key);
        self
    }
    
    /// Set custom headers
    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers.extend(headers);
        self
    }
    
    /// Set custom resource information
    pub fn with_resource(mut self, resource: OtlpResource) -> Self {
        self.resource = resource;
        self
    }
    
    /// Set instrumentation library information
    pub fn with_instrumentation_library(mut self, library: InstrumentationLibrary) -> Self {
        self.instrumentation_library = library;
        self
    }
    
    /// Set batch size for trace exports
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
    
    /// Set request timeout
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
    
    /// Convert Lightning DB spans to OTLP format
    fn convert_spans_to_otlp(&self, spans: &[Span]) -> Value {
        let otlp_spans: Vec<Value> = spans.iter()
            .map(|span| self.span_to_otlp(span))
            .collect();
        
        json!({
            "resourceSpans": [{
                "resource": self.resource.to_json(),
                "scopeSpans": [{
                    "scope": self.instrumentation_library.to_json(),
                    "spans": otlp_spans
                }]
            }]
        })
    }
    
    /// Convert a single span to OTLP format
    fn span_to_otlp(&self, span: &Span) -> Value {
        let start_time_nanos = span.start_time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let end_time_nanos = span.end_time
            .map(|end| end.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64)
            .unwrap_or(start_time_nanos);
        
        // Convert tags to OTLP attributes
        let attributes: Vec<Value> = span.tags.iter()
            .map(|(k, v)| json!({
                "key": k,
                "value": {
                    "stringValue": v
                }
            }))
            .collect();
        
        // Convert logs to OTLP events
        let events: Vec<Value> = span.logs.iter()
            .map(|log| {
                let event_time_nanos = log.timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                
                let event_attributes: Vec<Value> = log.fields.iter()
                    .map(|(k, v)| json!({
                        "key": k,
                        "value": {
                            "stringValue": v
                        }
                    }))
                    .collect();
                
                json!({
                    "timeUnixNano": event_time_nanos.to_string(),
                    "name": log.message,
                    "attributes": event_attributes
                })
            })
            .collect();
        
        // Convert span status
        let (status_code, status_message) = match &span.status {
            SpanStatus::Ok => (1, "".to_string()),
            SpanStatus::Error(msg) => (2, msg.clone()),
            SpanStatus::Unset => (0, "".to_string()),
        };
        
        // Convert span kind
        let span_kind_code = match span.span_kind {
            SpanKind::Internal => 1,
            SpanKind::Server => 2,
            SpanKind::Client => 3,
            SpanKind::Producer => 4,
            SpanKind::Consumer => 5,
        };
        
        json!({
            "traceId": self.hex_to_bytes(&span.trace_id),
            "spanId": self.hex_to_bytes(&span.span_id),
            "parentSpanId": span.parent_span_id.as_ref()
                .map(|id| self.hex_to_bytes(id))
                .unwrap_or_else(|| Value::String("".to_string())),
            "name": span.operation_name,
            "kind": span_kind_code,
            "startTimeUnixNano": start_time_nanos.to_string(),
            "endTimeUnixNano": end_time_nanos.to_string(),
            "attributes": attributes,
            "events": events,
            "status": {
                "code": status_code,
                "message": status_message
            }
        })
    }
    
    /// Convert hex string to base64 bytes for OTLP
    fn hex_to_bytes(&self, hex_str: &str) -> Value {
        if let Ok(bytes) = hex::decode(hex_str) {
            json!(general_purpose::STANDARD.encode(bytes))
        } else {
            json!("")
        }
    }
    
    /// Send traces to OTLP endpoint
    fn send_otlp_request(&self, payload: Value) -> Result<()> {
        let mut request = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .timeout(std::time::Duration::from_millis(self.timeout_ms));
        
        // Add custom headers
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }
        
        let response = request
            .json(&payload)
            .send()
            .map_err(|e| crate::Error::IoError(format!("OTLP request failed: {}", e)))?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(crate::Error::IoError(format!(
                "OTLP export failed with status {}: {}", 
                status, body
            )));
        }
        
        Ok(())
    }
}

impl TraceExporter for OtlpTraceExporter {
    fn export_span(&self, span: &Span) -> Result<()> {
        self.export_spans(&[span.clone()])
    }
    
    fn export_spans(&self, spans: &[Span]) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }
        
        let payload = self.convert_spans_to_otlp(spans);
        self.send_otlp_request(payload)
    }
}

/// OpenTelemetry resource information
#[derive(Debug, Clone)]
pub struct OtlpResource {
    pub service_name: String,
    pub service_version: String,
    pub service_namespace: Option<String>,
    pub service_instance_id: Option<String>,
    pub deployment_environment: Option<String>,
    pub host_name: Option<String>,
    pub host_arch: Option<String>,
    pub os_type: Option<String>,
    pub os_description: Option<String>,
    pub process_pid: Option<u32>,
    pub process_executable_name: Option<String>,
    pub process_command_line: Option<String>,
    pub custom_attributes: HashMap<String, String>,
}

impl OtlpResource {
    /// Create a new resource with minimal information
    pub fn new(service_name: String, service_version: String) -> Self {
        Self {
            service_name,
            service_version,
            service_namespace: None,
            service_instance_id: None,
            deployment_environment: None,
            host_name: None,
            host_arch: None,
            os_type: None,
            os_description: None,
            process_pid: None,
            process_executable_name: None,
            process_command_line: None,
            custom_attributes: HashMap::new(),
        }
    }
    
    /// Auto-detect system resource information
    pub fn auto_detect() -> Self {
        let mut resource = Self::new(
            "lightning_db".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        );
        
        // Detect hostname
        if let Ok(hostname) = hostname::get() {
            resource.host_name = hostname.to_string_lossy().into_owned().into();
        }
        
        // Detect OS information
        resource.os_type = std::env::consts::OS.to_string().into();
        resource.host_arch = std::env::consts::ARCH.to_string().into();
        
        // Detect process information
        resource.process_pid = std::process::id().into();
        
        if let Ok(current_exe) = std::env::current_exe() {
            resource.process_executable_name = current_exe
                .file_name()
                .and_then(|name| name.to_str())
                .map(|s| s.to_string());
        }
        
        // Detect environment from environment variables
        if let Ok(env) = std::env::var("DEPLOYMENT_ENVIRONMENT") {
            resource.deployment_environment = Some(env);
        } else if let Ok(_) = std::env::var("KUBERNETES_SERVICE_HOST") {
            resource.deployment_environment = Some("kubernetes".to_string());
        }
        
        // Add custom attributes from environment
        for (key, value) in std::env::vars() {
            if key.starts_with("OTEL_RESOURCE_ATTRIBUTES_") {
                let attr_key = key.strip_prefix("OTEL_RESOURCE_ATTRIBUTES_").unwrap();
                resource.custom_attributes.insert(attr_key.to_lowercase(), value);
            }
        }
        
        resource
    }
    
    /// Add custom attribute
    pub fn with_attribute(mut self, key: String, value: String) -> Self {
        self.custom_attributes.insert(key, value);
        self
    }
    
    /// Set service namespace
    pub fn with_namespace(mut self, namespace: String) -> Self {
        self.service_namespace = Some(namespace);
        self
    }
    
    /// Set deployment environment
    pub fn with_environment(mut self, environment: String) -> Self {
        self.deployment_environment = Some(environment);
        self
    }
    
    /// Convert to OTLP JSON format
    fn to_json(&self) -> Value {
        let mut attributes = vec![
            json!({
                "key": "service.name",
                "value": {"stringValue": self.service_name}
            }),
            json!({
                "key": "service.version", 
                "value": {"stringValue": self.service_version}
            }),
        ];
        
        if let Some(ref namespace) = self.service_namespace {
            attributes.push(json!({
                "key": "service.namespace",
                "value": {"stringValue": namespace}
            }));
        }
        
        if let Some(ref instance_id) = self.service_instance_id {
            attributes.push(json!({
                "key": "service.instance.id",
                "value": {"stringValue": instance_id}
            }));
        }
        
        if let Some(ref environment) = self.deployment_environment {
            attributes.push(json!({
                "key": "deployment.environment",
                "value": {"stringValue": environment}
            }));
        }
        
        if let Some(ref hostname) = self.host_name {
            attributes.push(json!({
                "key": "host.name",
                "value": {"stringValue": hostname}
            }));
        }
        
        if let Some(ref arch) = self.host_arch {
            attributes.push(json!({
                "key": "host.arch",
                "value": {"stringValue": arch}
            }));
        }
        
        if let Some(ref os_type) = self.os_type {
            attributes.push(json!({
                "key": "os.type",
                "value": {"stringValue": os_type}
            }));
        }
        
        if let Some(ref os_desc) = self.os_description {
            attributes.push(json!({
                "key": "os.description",
                "value": {"stringValue": os_desc}
            }));
        }
        
        if let Some(pid) = self.process_pid {
            attributes.push(json!({
                "key": "process.pid",
                "value": {"intValue": pid.to_string()}
            }));
        }
        
        if let Some(ref exe_name) = self.process_executable_name {
            attributes.push(json!({
                "key": "process.executable.name",
                "value": {"stringValue": exe_name}
            }));
        }
        
        if let Some(ref command_line) = self.process_command_line {
            attributes.push(json!({
                "key": "process.command_line",
                "value": {"stringValue": command_line}
            }));
        }
        
        // Add custom attributes
        for (key, value) in &self.custom_attributes {
            attributes.push(json!({
                "key": key,
                "value": {"stringValue": value}
            }));
        }
        
        json!({
            "attributes": attributes
        })
    }
}

/// OpenTelemetry instrumentation library information
#[derive(Debug, Clone)]
pub struct InstrumentationLibrary {
    pub name: String,
    pub version: String,
    pub schema_url: Option<String>,
    pub attributes: HashMap<String, String>,
}

impl InstrumentationLibrary {
    /// Create a new instrumentation library
    pub fn new(name: String, version: String) -> Self {
        Self {
            name,
            version,
            schema_url: None,
            attributes: HashMap::new(),
        }
    }
    
    /// Set schema URL
    pub fn with_schema_url(mut self, schema_url: String) -> Self {
        self.schema_url = Some(schema_url);
        self
    }
    
    /// Add attribute
    pub fn with_attribute(mut self, key: String, value: String) -> Self {
        self.attributes.insert(key, value);
        self
    }
    
    /// Convert to OTLP JSON format
    fn to_json(&self) -> Value {
        let mut result = json!({
            "name": self.name,
            "version": self.version
        });
        
        if let Some(ref schema_url) = self.schema_url {
            result["schemaUrl"] = json!(schema_url);
        }
        
        if !self.attributes.is_empty() {
            let attributes: Vec<Value> = self.attributes.iter()
                .map(|(k, v)| json!({
                    "key": k,
                    "value": {"stringValue": v}
                }))
                .collect();
            result["attributes"] = json!(attributes);
        }
        
        result
    }
}

impl Default for InstrumentationLibrary {
    fn default() -> Self {
        Self {
            name: "lightning_db_tracer".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            schema_url: Some("https://opentelemetry.io/schemas/1.24.0".to_string()),
            attributes: HashMap::new(),
        }
    }
}

/// OTLP configuration builder
pub struct OtlpConfigBuilder {
    endpoint: String,
    resource: OtlpResource,
    instrumentation_library: InstrumentationLibrary,
    headers: HashMap<String, String>,
    batch_size: usize,
    timeout_ms: u64,
}

impl OtlpConfigBuilder {
    /// Create a new OTLP configuration builder
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            resource: OtlpResource::auto_detect(),
            instrumentation_library: InstrumentationLibrary::default(),
            headers: HashMap::new(),
            batch_size: 100,
            timeout_ms: 30000,
        }
    }
    
    /// Configure for Jaeger
    pub fn jaeger(jaeger_endpoint: String) -> Self {
        Self::new(format!("{}/v1/traces", jaeger_endpoint))
    }
    
    /// Configure for OTEL Collector
    pub fn collector(collector_endpoint: String) -> Self {
        Self::new(format!("{}/v1/traces", collector_endpoint))
    }
    
    /// Configure for Honeycomb
    pub fn honeycomb(api_key: String) -> Self {
        let mut builder = Self::new("https://api.honeycomb.io/v1/traces".to_string());
        builder.headers.insert("X-Honeycomb-Team".to_string(), api_key);
        builder
    }
    
    /// Configure for DataDog
    pub fn datadog(api_key: String) -> Self {
        let mut builder = Self::new("https://trace.agent.datadoghq.com/v1/traces".to_string());
        builder.headers.insert("DD-API-KEY".to_string(), api_key);
        builder
    }
    
    /// Set resource information
    pub fn with_resource(mut self, resource: OtlpResource) -> Self {
        self.resource = resource;
        self
    }
    
    /// Set instrumentation library
    pub fn with_instrumentation_library(mut self, library: InstrumentationLibrary) -> Self {
        self.instrumentation_library = library;
        self
    }
    
    /// Add header
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }
    
    /// Set batch size 
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
    
    /// Set timeout
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
    
    /// Build the OTLP exporter
    pub fn build(self) -> OtlpTraceExporter {
        OtlpTraceExporter::new(self.endpoint)
            .with_resource(self.resource)
            .with_instrumentation_library(self.instrumentation_library)
            .with_headers(self.headers)
            .with_batch_size(self.batch_size)
            .with_timeout_ms(self.timeout_ms)
    }
}

/// Trace configuration from environment variables
pub struct EnvConfig;

impl EnvConfig {
    /// Create OTLP exporter from environment variables
    pub fn from_env() -> Result<OtlpTraceExporter> {
        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .map_err(|_| crate::Error::InvalidInput(
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT required".to_string()
            ))?;
        
        let mut builder = OtlpConfigBuilder::new(endpoint);
        
        // Parse headers from environment
        if let Ok(headers_str) = std::env::var("OTEL_EXPORTER_OTLP_HEADERS") {
            for header_pair in headers_str.split(',') {
                if let Some((key, value)) = header_pair.split_once('=') {
                    builder = builder.with_header(key.trim().to_string(), value.trim().to_string());
                }
            }
        }
        
        // Parse timeout from environment
        if let Ok(timeout_str) = std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT") {
            if let Ok(timeout_ms) = timeout_str.parse::<u64>() {
                builder = builder.with_timeout_ms(timeout_ms);
            }
        }
        
        Ok(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_tracing::TraceContext;

    #[test]
    fn test_otlp_resource_creation() {
        let resource = OtlpResource::new("test_service".to_string(), "1.0.0".to_string())
            .with_namespace("test_namespace".to_string())
            .with_environment("test".to_string())
            .with_attribute("custom.key".to_string(), "custom.value".to_string());
        
        assert_eq!(resource.service_name, "test_service");
        assert_eq!(resource.service_version, "1.0.0");
        assert_eq!(resource.service_namespace, Some("test_namespace".to_string()));
        assert_eq!(resource.deployment_environment, Some("test".to_string()));
        assert_eq!(resource.custom_attributes.get("custom.key"), Some(&"custom.value".to_string()));
    }
    
    #[test]
    fn test_auto_detect_resource() {
        let resource = OtlpResource::auto_detect();
        
        assert_eq!(resource.service_name, "lightning_db");
        assert!(!resource.service_version.is_empty());
        assert!(resource.os_type.is_some());
        assert!(resource.host_arch.is_some());
    }
    
    #[test]
    fn test_instrumentation_library() {
        let library = InstrumentationLibrary::new("test_lib".to_string(), "2.0.0".to_string())
            .with_schema_url("https://example.com/schema".to_string())
            .with_attribute("lib.type".to_string(), "database".to_string());
        
        assert_eq!(library.name, "test_lib");
        assert_eq!(library.version, "2.0.0");
        assert_eq!(library.schema_url, Some("https://example.com/schema".to_string()));
        assert_eq!(library.attributes.get("lib.type"), Some(&"database".to_string()));
    }
    
    #[test]
    fn test_otlp_config_builder() {
        let exporter = OtlpConfigBuilder::new("http://localhost:4318/v1/traces".to_string())
            .with_header("Authorization".to_string(), "Bearer token".to_string())
            .with_batch_size(50)
            .with_timeout_ms(15000)
            .build();
        
        assert_eq!(exporter.endpoint, "http://localhost:4318/v1/traces");
        assert_eq!(exporter.headers.get("Authorization"), Some(&"Bearer token".to_string()));
        assert_eq!(exporter.batch_size, 50);
        assert_eq!(exporter.timeout_ms, 15000);
    }
    
    #[test]
    fn test_span_to_otlp_conversion() {
        let context = TraceContext::new_root();
        let span = super::super::Span::new(&context, "test_operation".to_string());
        
        let exporter = OtlpTraceExporter::new("http://localhost:4318/v1/traces".to_string());
        let otlp_span = exporter.span_to_otlp(&span);
        
        assert_eq!(otlp_span["name"], "test_operation");
        assert!(otlp_span["traceId"].is_string());
        assert!(otlp_span["spanId"].is_string());
        assert_eq!(otlp_span["kind"], 1); // Internal
    }
}