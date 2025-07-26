//! Distributed Tracing Visualization Tools
//!
//! Provides web-based visualization and analysis tools for Lightning DB traces,
//! including trace viewers, performance analysis, and debugging utilities.

use super::{Span, SpanStatus, SpanKind};
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use serde::{Serialize, Deserialize};
// serde_json imports will be added when needed for HTML generation

/// Web-based trace viewer for Lightning DB traces
pub struct TraceViewer {
    traces: Arc<Mutex<HashMap<String, TraceView>>>,
    config: ViewerConfig,
}

/// Configuration for the trace viewer
#[derive(Debug, Clone)]
pub struct ViewerConfig {
    pub max_traces: usize,
    pub max_spans_per_trace: usize,
    pub retention_hours: u64,
    pub enable_real_time: bool,
    pub sampling_rate: f64,
}

impl Default for ViewerConfig {
    fn default() -> Self {
        Self {
            max_traces: 1000,
            max_spans_per_trace: 10000,
            retention_hours: 24,
            enable_real_time: true,
            sampling_rate: 1.0,
        }
    }
}

/// A complete trace view with all spans organized hierarchically
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceView {
    pub trace_id: String,
    pub root_span: SpanView,
    pub all_spans: Vec<SpanView>,
    pub total_duration_micros: u64,
    pub span_count: usize,
    pub error_count: usize,
    pub service_map: HashMap<String, ServiceInfo>,
    pub critical_path: Vec<String>, // Span IDs in critical path
    pub created_at: SystemTime,
}

/// Enhanced span view with additional visualization data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanView {
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub operation_name: String,
    pub service_name: String,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub duration_micros: Option<u64>,
    pub status: SpanStatus,
    pub span_kind: SpanKind,
    pub tags: HashMap<String, String>,
    pub logs: Vec<SpanLogView>,
    pub children: Vec<SpanView>,
    pub depth: usize,
    pub percentage_of_trace: f64,
    pub is_critical_path: bool,
}

/// Enhanced log view for visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLogView {
    pub timestamp: SystemTime,
    pub level: String,
    pub message: String,
    pub fields: HashMap<String, String>,
    pub relative_time_micros: u64, // Time relative to span start
}

/// Service information for service map visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub span_count: usize,
    pub error_count: usize,
    pub avg_duration_micros: f64,
    pub operations: HashMap<String, OperationStats>,
}

/// Statistics for individual operations within a service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStats {
    pub operation_name: String,
    pub count: usize,
    pub avg_duration_micros: f64,
    pub error_rate: f64,
    pub p95_duration_micros: u64,
    pub p99_duration_micros: u64,
}

impl TraceViewer {
    /// Create a new trace viewer
    pub fn new(config: ViewerConfig) -> Self {
        Self {
            traces: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Add spans to the trace viewer
    pub fn add_spans(&self, spans: &[Span]) -> Result<()> {
        if let Ok(mut traces) = self.traces.lock() {
            for span in spans {
                // Check sampling
                if rand::random::<f64>() > self.config.sampling_rate {
                    continue;
                }

                let trace_view = traces.entry(span.trace_id.clone())
                    .or_insert_with(|| TraceView::new(span.trace_id.clone()));

                trace_view.add_span(span)?;
            }

            // Clean up old traces
            self.cleanup_old_traces(&mut traces);
        }

        Ok(())
    }

    /// Get a trace view by trace ID
    pub fn get_trace(&self, trace_id: &str) -> Option<TraceView> {
        if let Ok(traces) = self.traces.lock() {
            traces.get(trace_id).cloned()
        } else {
            None
        }
    }

    /// Get all trace IDs
    pub fn list_traces(&self) -> Vec<String> {
        if let Ok(traces) = self.traces.lock() {
            traces.keys().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// Search traces by criteria
    pub fn search_traces(&self, criteria: &SearchCriteria) -> Vec<TraceView> {
        if let Ok(traces) = self.traces.lock() {
            traces.values()
                .filter(|trace| criteria.matches(trace))
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Generate a service map from all traces
    pub fn generate_service_map(&self) -> ServiceMap {
        if let Ok(traces) = self.traces.lock() {
            let mut service_map = ServiceMap::new();
            
            for trace in traces.values() {
                service_map.add_trace(trace);
            }
            
            service_map
        } else {
            ServiceMap::new()
        }
    }

    /// Get performance statistics
    pub fn get_performance_stats(&self) -> PerformanceStats {
        if let Ok(traces) = self.traces.lock() {
            PerformanceStats::from_traces(traces.values().collect())
        } else {
            PerformanceStats::default()
        }
    }

    /// Export traces to JSON for external tools
    pub fn export_json(&self, trace_ids: Option<Vec<String>>) -> Result<String> {
        if let Ok(traces) = self.traces.lock() {
            let export_traces: Vec<&TraceView> = if let Some(ids) = trace_ids {
                ids.iter()
                    .filter_map(|id| traces.get(id))
                    .collect()
            } else {
                traces.values().collect()
            };

            Ok(serde_json::to_string_pretty(&export_traces)?)
        } else {
            Ok("[]".to_string())
        }
    }

    /// Generate HTML trace viewer
    pub fn generate_html_viewer(&self, trace_id: &str) -> Result<String> {
        let trace = self.get_trace(trace_id)
            .ok_or_else(|| crate::Error::Generic(format!("Trace {} not found", trace_id)))?;

        let html = format!(r#"
<!DOCTYPE html>
<html>
<head>
    <title>Lightning DB Trace Viewer - {}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .trace-header {{ background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .span {{ margin-left: 20px; border-left: 2px solid #ddd; padding: 10px; margin-bottom: 5px; }}
        .span.error {{ border-left-color: #ff4444; background: #fff5f5; }}
        .span.success {{ border-left-color: #44ff44; }}
        .span-header {{ font-weight: bold; cursor: pointer; }}
        .span-details {{ margin-top: 10px; font-size: 0.9em; color: #666; }}
        .tag {{ background: #e7f3ff; padding: 2px 6px; border-radius: 3px; margin: 2px; display: inline-block; }}
        .duration {{ color: #666; float: right; }}
        .critical-path {{ background: #fff3e0; border-left-color: #ff9800; }}
        .service-map {{ margin-top: 30px; }}
        .timeline {{ height: 20px; background: #f0f0f0; margin: 10px 0; position: relative; }}
        .timeline-bar {{ height: 100%; background: #4CAF50; position: absolute; }}
        .timeline-bar.error {{ background: #f44336; }}
    </style>
    <script>
        function toggleSpan(spanId) {{
            const details = document.getElementById('details-' + spanId);
            details.style.display = details.style.display === 'none' ? 'block' : 'none';
        }}
    </script>
</head>
<body>
    <div class="trace-header">
        <h1>Trace: {}</h1>
        <p><strong>Duration:</strong> {:.2}ms | <strong>Spans:</strong> {} | <strong>Errors:</strong> {}</p>
        <div class="timeline">
            <div class="timeline-bar" style="width: 100%;"></div>
        </div>
    </div>
    
    <div class="spans">
        {}
    </div>
    
    <div class="service-map">
        <h2>Service Map</h2>
        {}
    </div>
</body>
</html>"#,
            trace_id,
            trace_id,
            trace.total_duration_micros as f64 / 1000.0,
            trace.span_count,
            trace.error_count,
            self.generate_span_html(&trace.root_span, &trace)?,
            self.generate_service_map_html(&trace.service_map)?
        );

        Ok(html)
    }

    /// Clean up old traces based on retention policy
    fn cleanup_old_traces(&self, traces: &mut HashMap<String, TraceView>) {
        let retention_duration = Duration::from_secs(self.config.retention_hours * 3600);
        let cutoff_time = SystemTime::now() - retention_duration;

        traces.retain(|_, trace| trace.created_at > cutoff_time);

        // Also enforce max traces limit
        if traces.len() > self.config.max_traces {
            let mut trace_pairs: Vec<_> = traces.iter()
                .map(|(id, trace)| (id.clone(), trace.created_at))
                .collect();
            trace_pairs.sort_by_key(|(_, created_at)| *created_at);
            
            let to_remove = traces.len() - self.config.max_traces;
            let ids_to_remove: Vec<String> = trace_pairs.iter()
                .take(to_remove)
                .map(|(id, _)| id.clone())
                .collect();
            
            for trace_id in ids_to_remove {
                traces.remove(&trace_id);
            }
        }
    }

    /// Generate HTML for a span and its children
    fn generate_span_html(&self, span: &SpanView, trace: &TraceView) -> Result<String> {
        let status_class = match span.status {
            SpanStatus::Error(_) => "error",
            SpanStatus::Ok => "success",
            _ => "",
        };

        let critical_class = if span.is_critical_path { " critical-path" } else { "" };

        let duration_str = span.duration_micros
            .map(|d| format!("{:.2}ms", d as f64 / 1000.0))
            .unwrap_or_else(|| "ongoing".to_string());

        let mut html = format!(r#"
<div class="span {}{}" style="margin-left: {}px;">
    <div class="span-header" onclick="toggleSpan('{}')">
        {} <span class="duration">{}</span>
    </div>
    <div id="details-{}" class="span-details" style="display: none;">
        <p><strong>Service:</strong> {}</p>
        <p><strong>Span ID:</strong> {}</p>
        <p><strong>Kind:</strong> {:?}</p>
        "#,
            status_class, critical_class,
            span.depth * 20,
            span.span_id,
            span.operation_name,
            duration_str,
            span.span_id,
            span.service_name,
            span.span_id,
            span.span_kind
        );

        // Add tags
        if !span.tags.is_empty() {
            html.push_str("<div><strong>Tags:</strong><br>");
            for (key, value) in &span.tags {
                html.push_str(&format!("<span class='tag'>{}={}</span>", key, value));
            }
            html.push_str("</div>");
        }

        // Add logs
        if !span.logs.is_empty() {
            html.push_str("<div><strong>Logs:</strong><ul>");
            for log in &span.logs {
                html.push_str(&format!(
                    "<li>[{}] {}: {}</li>", 
                    log.level, 
                    log.relative_time_micros as f64 / 1000.0,
                    log.message
                ));
            }
            html.push_str("</ul></div>");
        }

        html.push_str("</div>");

        // Add children
        for child in &span.children {
            html.push_str(&self.generate_span_html(child, trace)?);
        }

        html.push_str("</div>");
        Ok(html)
    }

    /// Generate HTML for service map
    fn generate_service_map_html(&self, service_map: &HashMap<String, ServiceInfo>) -> Result<String> {
        let mut html = String::from("<div class='services'>");

        for (service_name, info) in service_map {
            html.push_str(&format!(r#"
<div class="service">
    <h3>{}</h3>
    <p>Spans: {} | Errors: {} | Avg Duration: {:.2}ms</p>
    <div class="operations">
"#,
                service_name,
                info.span_count,
                info.error_count,
                info.avg_duration_micros / 1000.0
            ));

            for (op_name, stats) in &info.operations {
                html.push_str(&format!(
                    "<div class='operation'>{}: {} calls, {:.2}ms avg, {:.1}% errors</div>",
                    op_name,
                    stats.count,
                    stats.avg_duration_micros / 1000.0,
                    stats.error_rate * 100.0
                ));
            }

            html.push_str("</div></div>");
        }

        html.push_str("</div>");
        Ok(html)
    }
}

impl TraceView {
    /// Create a new trace view
    pub fn new(trace_id: String) -> Self {
        Self {
            trace_id,
            root_span: SpanView::placeholder(),
            all_spans: Vec::new(),
            total_duration_micros: 0,
            span_count: 0,
            error_count: 0,
            service_map: HashMap::new(),
            critical_path: Vec::new(),
            created_at: SystemTime::now(),
        }
    }

    /// Add a span to the trace view
    pub fn add_span(&mut self, span: &Span) -> Result<()> {
        let span_view = SpanView::from_span(span);

        // Update trace statistics
        self.span_count += 1;
        if matches!(span.status, SpanStatus::Error(_)) {
            self.error_count += 1;
        }

        // Update service map
        let service_name = span.tags.get("service.name")
            .unwrap_or(&"unknown".to_string())
            .clone();

        let service_info = self.service_map.entry(service_name.clone())
            .or_insert_with(|| ServiceInfo {
                name: service_name.clone(),
                span_count: 0,
                error_count: 0,
                avg_duration_micros: 0.0,
                operations: HashMap::new(),
            });

        service_info.span_count += 1;
        if matches!(span.status, SpanStatus::Error(_)) {
            service_info.error_count += 1;
        }

        if let Some(duration) = span.duration_micros() {
            service_info.avg_duration_micros = 
                (service_info.avg_duration_micros * (service_info.span_count - 1) as f64 + duration as f64) 
                / service_info.span_count as f64;
        }

        // Update operation stats
        let op_stats = service_info.operations.entry(span.operation_name.clone())
            .or_insert_with(|| OperationStats {
                operation_name: span.operation_name.clone(),
                count: 0,
                avg_duration_micros: 0.0,
                error_rate: 0.0,
                p95_duration_micros: 0,
                p99_duration_micros: 0,
            });

        op_stats.count += 1;
        if let Some(duration) = span.duration_micros() {
            op_stats.avg_duration_micros = 
                (op_stats.avg_duration_micros * (op_stats.count - 1) as f64 + duration as f64) 
                / op_stats.count as f64;
        }
        op_stats.error_rate = service_info.error_count as f64 / service_info.span_count as f64;

        self.all_spans.push(span_view);

        // Rebuild hierarchy
        self.build_hierarchy()?;
        self.calculate_critical_path()?;

        Ok(())
    }

    /// Build span hierarchy
    fn build_hierarchy(&mut self) -> Result<()> {
        // Find root span (no parent)
        if let Some(root_idx) = self.all_spans.iter()
            .position(|s| s.parent_span_id.is_none()) {
            
            let root_span = self.all_spans[root_idx].clone();
            self.root_span = self.build_span_tree(&root_span, 0)?;
            
            if let Some(duration) = self.root_span.duration_micros {
                self.total_duration_micros = duration;
            }
        }

        Ok(())
    }

    /// Build span tree recursively
    fn build_span_tree(&self, parent: &SpanView, depth: usize) -> Result<SpanView> {
        let mut span = parent.clone();
        span.depth = depth;

        // Calculate percentage of trace
        if self.total_duration_micros > 0 {
            if let Some(span_duration) = span.duration_micros {
                span.percentage_of_trace = (span_duration as f64 / self.total_duration_micros as f64) * 100.0;
            }
        }

        // Find children
        let children: Vec<SpanView> = self.all_spans.iter()
            .filter(|s| s.parent_span_id.as_ref() == Some(&parent.span_id))
            .map(|child| self.build_span_tree(child, depth + 1))
            .collect::<Result<Vec<_>>>()?;

        span.children = children;
        Ok(span)
    }

    /// Calculate critical path through the trace
    fn calculate_critical_path(&mut self) -> Result<()> {
        // Simple critical path: longest duration chain
        let mut critical_path = Vec::new();
        let mut current_span = &self.root_span;

        loop {
            critical_path.push(current_span.span_id.clone());

            // Find child with longest duration
            if let Some(longest_child) = current_span.children.iter()
                .max_by_key(|s| s.duration_micros.unwrap_or(0)) {
                current_span = longest_child;
            } else {
                break;
            }
        }

        self.critical_path = critical_path.clone();

        // Mark spans on critical path
        TraceView::mark_critical_path_recursive(&mut self.root_span, &critical_path);

        Ok(())
    }

    /// Mark spans on critical path recursively
    fn mark_critical_path_recursive(span: &mut SpanView, critical_path: &[String]) {
        span.is_critical_path = critical_path.contains(&span.span_id);
        
        for child in &mut span.children {
            TraceView::mark_critical_path_recursive(child, critical_path);
        }
    }
}

impl SpanView {
    /// Create a placeholder span view
    fn placeholder() -> Self {
        Self {
            span_id: "placeholder".to_string(),
            parent_span_id: None,
            operation_name: "placeholder".to_string(),
            service_name: "unknown".to_string(),
            start_time: SystemTime::now(),
            end_time: None,
            duration_micros: None,
            status: SpanStatus::Unset,
            span_kind: SpanKind::Internal,
            tags: HashMap::new(),
            logs: Vec::new(),
            children: Vec::new(),
            depth: 0,
            percentage_of_trace: 0.0,
            is_critical_path: false,
        }
    }

    /// Create span view from span
    fn from_span(span: &Span) -> Self {
        let logs = span.logs.iter()
            .map(|log| SpanLogView {
                timestamp: log.timestamp,
                level: format!("{:?}", log.level),
                message: log.message.clone(),
                fields: log.fields.clone(),
                relative_time_micros: log.timestamp
                    .duration_since(span.start_time)
                    .unwrap_or_default()
                    .as_micros() as u64,
            })
            .collect();

        Self {
            span_id: span.span_id.clone(),
            parent_span_id: span.parent_span_id.clone(),
            operation_name: span.operation_name.clone(),
            service_name: span.tags.get("service.name")
                .unwrap_or(&"unknown".to_string())
                .clone(),
            start_time: span.start_time,
            end_time: span.end_time,
            duration_micros: span.duration_micros(),
            status: span.status.clone(),
            span_kind: span.span_kind.clone(),
            tags: span.tags.clone(),
            logs,
            children: Vec::new(),
            depth: 0,
            percentage_of_trace: 0.0,
            is_critical_path: false,
        }
    }
}

/// Search criteria for finding traces
#[derive(Debug, Clone)]
pub struct SearchCriteria {
    pub service_name: Option<String>,
    pub operation_name: Option<String>,
    pub min_duration_micros: Option<u64>,
    pub max_duration_micros: Option<u64>,
    pub has_errors: Option<bool>,
    pub tag_filters: HashMap<String, String>,
    pub time_range: Option<(SystemTime, SystemTime)>,
}

impl SearchCriteria {
    /// Check if a trace matches the search criteria
    pub fn matches(&self, trace: &TraceView) -> bool {
        // Check service name
        if let Some(ref service) = self.service_name {
            if !trace.service_map.contains_key(service) {
                return false;
            }
        }

        // Check operation name
        if let Some(ref operation) = self.operation_name {
            if !trace.all_spans.iter().any(|s| s.operation_name == *operation) {
                return false;
            }
        }

        // Check duration
        if let Some(min_duration) = self.min_duration_micros {
            if trace.total_duration_micros < min_duration {
                return false;
            }
        }

        if let Some(max_duration) = self.max_duration_micros {
            if trace.total_duration_micros > max_duration {
                return false;
            }
        }

        // Check errors
        if let Some(has_errors) = self.has_errors {
            if has_errors && trace.error_count == 0 {
                return false;
            }
            if !has_errors && trace.error_count > 0 {
                return false;
            }
        }

        // Check tag filters
        for (key, value) in &self.tag_filters {
            if !trace.all_spans.iter().any(|s| 
                s.tags.get(key).map(|v| v == value).unwrap_or(false)
            ) {
                return false;
            }
        }

        // Check time range
        if let Some((start, end)) = self.time_range {
            if trace.created_at < start || trace.created_at > end {
                return false;
            }
        }

        true
    }
}

/// Service map for visualizing service dependencies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMap {
    pub services: HashMap<String, ServiceMapNode>,
    pub connections: Vec<ServiceConnection>,
}

/// Node in the service map
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMapNode {
    pub name: String,
    pub request_rate: f64,
    pub error_rate: f64,
    pub avg_duration_micros: f64,
    pub dependencies: Vec<String>,
}

/// Connection between services
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConnection {
    pub from_service: String,
    pub to_service: String,
    pub request_count: usize,
    pub error_count: usize,
    pub avg_duration_micros: f64,
}

impl ServiceMap {
    /// Create a new service map
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
            connections: Vec::new(),
        }
    }

    /// Add a trace to the service map
    pub fn add_trace(&mut self, trace: &TraceView) {
        // Add services
        for (name, info) in &trace.service_map {
            let node = self.services.entry(name.clone())
                .or_insert_with(|| ServiceMapNode {
                    name: name.clone(),
                    request_rate: 0.0,
                    error_rate: 0.0,
                    avg_duration_micros: 0.0,
                    dependencies: Vec::new(),
                });

            // Update statistics (simplified)
            node.request_rate += info.span_count as f64;
            node.error_rate = info.error_count as f64 / info.span_count as f64;
            node.avg_duration_micros = info.avg_duration_micros;
        }

        // Add connections (simplified - based on parent-child relationships)
        for span in &trace.all_spans {
            if let Some(parent_id) = &span.parent_span_id {
                if let Some(parent_span) = trace.all_spans.iter()
                    .find(|s| s.span_id == *parent_id) {
                    
                    if parent_span.service_name != span.service_name {
                        // This is a cross-service call
                        self.add_connection(
                            &parent_span.service_name,
                            &span.service_name,
                            span.duration_micros.unwrap_or(0),
                            matches!(span.status, SpanStatus::Error(_))
                        );
                    }
                }
            }
        }
    }

    /// Add a connection between services
    fn add_connection(&mut self, from: &str, to: &str, duration_micros: u64, is_error: bool) {
        if let Some(connection) = self.connections.iter_mut()
            .find(|c| c.from_service == from && c.to_service == to) {
            
            connection.request_count += 1;
            if is_error {
                connection.error_count += 1;
            }
            
            // Update average duration
            connection.avg_duration_micros = 
                (connection.avg_duration_micros * (connection.request_count - 1) as f64 + duration_micros as f64) 
                / connection.request_count as f64;
        } else {
            self.connections.push(ServiceConnection {
                from_service: from.to_string(),
                to_service: to.to_string(),
                request_count: 1,
                error_count: if is_error { 1 } else { 0 },
                avg_duration_micros: duration_micros as f64,
            });
        }
    }
}

/// Overall performance statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub total_traces: usize,
    pub total_spans: usize,
    pub avg_trace_duration_micros: f64,
    pub error_rate: f64,
    pub throughput_traces_per_minute: f64,
    pub top_operations: Vec<OperationStats>,
    pub slowest_traces: Vec<String>, // Trace IDs
}

impl Default for PerformanceStats {
    fn default() -> Self {
        Self {
            total_traces: 0,
            total_spans: 0,
            avg_trace_duration_micros: 0.0,
            error_rate: 0.0,
            throughput_traces_per_minute: 0.0,
            top_operations: Vec::new(),
            slowest_traces: Vec::new(),
        }
    }
}

impl PerformanceStats {
    /// Generate performance statistics from traces
    pub fn from_traces(traces: Vec<&TraceView>) -> Self {
        if traces.is_empty() {
            return Self::default();
        }

        let total_traces = traces.len();
        let total_spans: usize = traces.iter().map(|t| t.span_count).sum();
        let total_errors: usize = traces.iter().map(|t| t.error_count).sum();
        
        let avg_trace_duration_micros = traces.iter()
            .map(|t| t.total_duration_micros as f64)
            .sum::<f64>() / total_traces as f64;

        let error_rate = total_errors as f64 / total_spans as f64;

        // Find slowest traces
        let mut trace_durations: Vec<_> = traces.iter()
            .map(|t| (t.trace_id.clone(), t.total_duration_micros))
            .collect();
        trace_durations.sort_by_key(|(_, duration)| *duration);
        trace_durations.reverse();
        
        let slowest_traces = trace_durations.into_iter()
            .take(10)
            .map(|(id, _)| id)
            .collect();

        Self {
            total_traces,
            total_spans,
            avg_trace_duration_micros,
            error_rate,
            throughput_traces_per_minute: 0.0, // Would need time window calculation
            top_operations: Vec::new(), // Would need aggregation across traces
            slowest_traces,
        }
    }
}

/// Real-time trace streaming for live visualization
pub struct TraceStreamer {
    subscribers: Arc<Mutex<Vec<Box<dyn TraceSubscriber + Send>>>>,
    buffer: Arc<Mutex<VecDeque<Span>>>,
    config: StreamerConfig,
}

/// Configuration for trace streaming
#[derive(Debug, Clone)]
pub struct StreamerConfig {
    pub buffer_size: usize,
    pub flush_interval_ms: u64,
    pub max_subscribers: usize,
}

impl Default for StreamerConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            flush_interval_ms: 100,
            max_subscribers: 10,
        }
    }
}

/// Trait for trace subscribers
pub trait TraceSubscriber {
    fn on_spans(&mut self, spans: &[Span]) -> Result<()>;
    fn on_trace_complete(&mut self, trace_id: &str) -> Result<()>;
}

impl TraceStreamer {
    /// Create a new trace streamer
    pub fn new(config: StreamerConfig) -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(Vec::new())),
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            config,
        }
    }

    /// Add a span to the stream
    pub fn add_span(&self, span: Span) -> Result<()> {
        if let Ok(mut buffer) = self.buffer.lock() {
            buffer.push_back(span);
            
            // Maintain buffer size limit
            while buffer.len() > self.config.buffer_size {
                buffer.pop_front();
            }
        }
        
        Ok(())
    }

    /// Subscribe to trace updates
    pub fn subscribe(&self, subscriber: Box<dyn TraceSubscriber + Send>) -> Result<()> {
        if let Ok(mut subscribers) = self.subscribers.lock() {
            if subscribers.len() < self.config.max_subscribers {
                subscribers.push(subscriber);
                Ok(())
            } else {
                Err(crate::Error::Generic("Max subscribers reached".to_string()))
            }
        } else {
            Err(crate::Error::Generic("Failed to acquire lock".to_string()))
        }
    }

    /// Flush buffered spans to subscribers
    pub fn flush(&self) -> Result<()> {
        let spans = if let Ok(mut buffer) = self.buffer.lock() {
            let spans: Vec<Span> = buffer.drain(..).collect();
            spans
        } else {
            return Ok(());
        };

        if let Ok(mut subscribers) = self.subscribers.lock() {
            for subscriber in subscribers.iter_mut() {
                if let Err(e) = subscriber.on_spans(&spans) {
                    eprintln!("Subscriber error: {}", e);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_tracing::TraceContext;

    #[test]
    fn test_trace_viewer_creation() {
        let viewer = TraceViewer::new(ViewerConfig::default());
        assert_eq!(viewer.list_traces().len(), 0);
    }

    #[test]
    fn test_span_view_creation() {
        let context = TraceContext::new_root();
        let mut span = super::super::Span::new(&context, "test_operation".to_string());
        span.set_tag("service.name".to_string(), "test_service".to_string());
        span.finish();

        let span_view = SpanView::from_span(&span);
        assert_eq!(span_view.operation_name, "test_operation");
        assert_eq!(span_view.service_name, "test_service");
        assert!(span_view.duration_micros.is_some());
    }

    #[test]
    fn test_search_criteria() {
        let trace = TraceView::new("test_trace".to_string());
        
        let criteria = SearchCriteria {
            service_name: Some("test_service".to_string()),
            operation_name: None,
            min_duration_micros: None,
            max_duration_micros: None,
            has_errors: None,
            tag_filters: HashMap::new(),
            time_range: None,
        };

        // Should not match as trace has no service map entries
        assert!(!criteria.matches(&trace));
    }

    #[test]
    fn test_service_map() {
        let service_map = ServiceMap::new();
        assert_eq!(service_map.services.len(), 0);
        assert_eq!(service_map.connections.len(), 0);
    }

    #[test]
    fn test_performance_stats() {
        let stats = PerformanceStats::from_traces(vec![]);
        assert_eq!(stats.total_traces, 0);
        assert_eq!(stats.total_spans, 0);
    }
}