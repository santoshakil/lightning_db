use crate::core::error::{Error, Result};
use parking_lot::{RwLock, Mutex};
use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn, error};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    pub timestamp: u64,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeries {
    pub name: String,
    pub points: VecDeque<MetricPoint>,
    pub max_points: usize,
    pub aggregation: AggregationType,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum AggregationType {
    Sum,
    Average,
    Min,
    Max,
    Count,
    Percentile(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub title: String,
    pub description: String,
    pub panels: Vec<Panel>,
    pub refresh_interval: Duration,
    pub time_range: TimeRange,
    pub variables: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Panel {
    pub id: String,
    pub title: String,
    pub panel_type: PanelType,
    pub metrics: Vec<String>,
    pub layout: PanelLayout,
    pub thresholds: Vec<Threshold>,
    pub options: PanelOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PanelType {
    Graph,
    Gauge,
    Counter,
    Heatmap,
    Table,
    Stat,
    BarGauge,
    PieChart,
    Alert,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PanelLayout {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Threshold {
    pub value: f64,
    pub color: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PanelOptions {
    pub show_legend: bool,
    pub show_tooltip: bool,
    pub unit: Option<String>,
    pub decimals: Option<u8>,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: u64,
    pub end: u64,
}

pub struct MetricsCollector {
    series: Arc<RwLock<HashMap<String, TimeSeries>>>,
    counters: Arc<RwLock<HashMap<String, AtomicU64>>>,
    gauges: Arc<RwLock<HashMap<String, AtomicU64>>>,
    histograms: Arc<RwLock<HashMap<String, Histogram>>>,
    retention: Duration,
    flush_interval: Duration,
    last_flush: Arc<Mutex<Instant>>,
}

struct Histogram {
    buckets: Vec<f64>,
    counts: Vec<AtomicU64>,
    sum: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn new(buckets: Vec<f64>) -> Self {
        let counts = (0..=buckets.len())
            .map(|_| AtomicU64::new(0))
            .collect();
        
        Self {
            buckets,
            counts,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
    
    fn observe(&self, value: f64) {
        let mut bucket_idx = self.buckets.len();
        for (i, &bucket) in self.buckets.iter().enumerate() {
            if value <= bucket {
                bucket_idx = i;
                break;
            }
        }
        
        self.counts[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value.to_bits(), Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
    
    fn percentile(&self, p: f64) -> f64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        
        let target = ((total as f64) * p / 100.0) as u64;
        let mut accumulated = 0u64;
        
        for (i, count) in self.counts.iter().enumerate() {
            accumulated += count.load(Ordering::Relaxed);
            if accumulated >= target {
                if i < self.buckets.len() {
                    return self.buckets[i];
                } else {
                    return self.buckets.last().copied().unwrap_or(0.0);
                }
            }
        }
        
        self.buckets.last().copied().unwrap_or(0.0)
    }
}

impl MetricsCollector {
    pub fn new(retention: Duration, flush_interval: Duration) -> Self {
        Self {
            series: Arc::new(RwLock::new(HashMap::new())),
            counters: Arc::new(RwLock::new(HashMap::new())),
            gauges: Arc::new(RwLock::new(HashMap::new())),
            histograms: Arc::new(RwLock::new(HashMap::new())),
            retention,
            flush_interval,
            last_flush: Arc::new(Mutex::new(Instant::now())),
        }
    }
    
    pub fn record_counter(&self, name: &str, value: u64, labels: HashMap<String, String>) {
        let mut counters = self.counters.write();
        let counter = counters.entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(value, Ordering::Relaxed);
        
        self.record_series(name, counter.load(Ordering::Relaxed) as f64, labels);
    }
    
    pub fn record_gauge(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let mut gauges = self.gauges.write();
        let gauge = gauges.entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        gauge.store(value.to_bits(), Ordering::Relaxed);
        
        self.record_series(name, value, labels);
    }
    
    pub fn record_histogram(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let mut histograms = self.histograms.write();
        let histogram = histograms.entry(name.to_string())
            .or_insert_with(|| Histogram::new(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
            ]));
        histogram.observe(value);
        
        self.record_series(name, value, labels);
    }
    
    fn record_series(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let point = MetricPoint {
            timestamp,
            value,
            labels,
        };
        
        let mut series_map = self.series.write();
        let series = series_map.entry(name.to_string())
            .or_insert_with(|| TimeSeries {
                name: name.to_string(),
                points: VecDeque::new(),
                max_points: 10000,
                aggregation: AggregationType::Average,
            });
        
        series.points.push_back(point);
        
        if series.points.len() > series.max_points {
            series.points.pop_front();
        }
        
        let cutoff = timestamp - self.retention.as_secs();
        while let Some(front) = series.points.front() {
            if front.timestamp < cutoff {
                series.points.pop_front();
            } else {
                break;
            }
        }
    }
    
    pub fn get_series(&self, name: &str, time_range: TimeRange) -> Option<Vec<MetricPoint>> {
        let series_map = self.series.read();
        series_map.get(name).map(|series| {
            series.points
                .iter()
                .filter(|p| p.timestamp >= time_range.start && p.timestamp <= time_range.end)
                .cloned()
                .collect()
        })
    }
    
    pub fn get_aggregated(
        &self,
        name: &str,
        time_range: TimeRange,
        aggregation: AggregationType,
        interval: Duration,
    ) -> Vec<MetricPoint> {
        let series_map = self.series.read();
        
        if let Some(series) = series_map.get(name) {
            let mut aggregated = Vec::new();
            let interval_secs = interval.as_secs();
            let mut current_bucket = time_range.start;
            
            while current_bucket <= time_range.end {
                let bucket_end = current_bucket + interval_secs;
                let bucket_points: Vec<_> = series.points
                    .iter()
                    .filter(|p| p.timestamp >= current_bucket && p.timestamp < bucket_end)
                    .collect();
                
                if !bucket_points.is_empty() {
                    let aggregated_value = match aggregation {
                        AggregationType::Sum => {
                            bucket_points.iter().map(|p| p.value).sum()
                        }
                        AggregationType::Average => {
                            let sum: f64 = bucket_points.iter().map(|p| p.value).sum();
                            sum / bucket_points.len() as f64
                        }
                        AggregationType::Min => {
                            bucket_points.iter()
                                .map(|p| p.value)
                                .fold(f64::INFINITY, f64::min)
                        }
                        AggregationType::Max => {
                            bucket_points.iter()
                                .map(|p| p.value)
                                .fold(f64::NEG_INFINITY, f64::max)
                        }
                        AggregationType::Count => bucket_points.len() as f64,
                        AggregationType::Percentile(p) => {
                            let mut values: Vec<_> = bucket_points.iter()
                                .map(|point| point.value)
                                .collect();
                            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                            let idx = ((values.len() as f64) * p / 100.0) as usize;
                            values.get(idx).copied().unwrap_or(0.0)
                        }
                    };
                    
                    aggregated.push(MetricPoint {
                        timestamp: current_bucket,
                        value: aggregated_value,
                        labels: HashMap::new(),
                    });
                }
                
                current_bucket = bucket_end;
            }
            
            aggregated
        } else {
            Vec::new()
        }
    }
    
    pub fn get_histogram_percentile(&self, name: &str, percentile: f64) -> Option<f64> {
        let histograms = self.histograms.read();
        histograms.get(name).map(|h| h.percentile(percentile))
    }
    
    pub fn flush(&self) {
        let mut last_flush = self.last_flush.lock();
        *last_flush = Instant::now();
        
        info!("Metrics flushed at {:?}", *last_flush);
    }
}

pub struct DashboardManager {
    dashboards: Arc<RwLock<HashMap<String, Dashboard>>>,
    metrics_collector: Arc<MetricsCollector>,
    alert_manager: Arc<AlertManager>,
    render_cache: Arc<RwLock<HashMap<String, RenderedDashboard>>>,
    update_channel: broadcast::Sender<DashboardUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderedDashboard {
    pub dashboard_id: String,
    pub rendered_at: u64,
    pub panels: Vec<RenderedPanel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderedPanel {
    pub panel_id: String,
    pub data: PanelData,
    pub status: PanelStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PanelData {
    TimeSeries(Vec<MetricPoint>),
    SingleValue(f64),
    Table(Vec<Vec<String>>),
    Heatmap(Vec<Vec<f64>>),
    PieChart(Vec<(String, f64)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PanelStatus {
    Ok,
    Warning,
    Critical,
    NoData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardUpdate {
    pub dashboard_id: String,
    pub update_type: UpdateType,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateType {
    DataUpdate,
    ConfigChange,
    AlertTriggered(String),
}

impl DashboardManager {
    pub fn new(metrics_collector: Arc<MetricsCollector>) -> Self {
        let (tx, _) = broadcast::channel(1000);
        
        Self {
            dashboards: Arc::new(RwLock::new(HashMap::new())),
            metrics_collector,
            alert_manager: Arc::new(AlertManager::new()),
            render_cache: Arc::new(RwLock::new(HashMap::new())),
            update_channel: tx,
        }
    }
    
    pub fn create_dashboard(&self, dashboard: Dashboard) -> Result<String> {
        let id = dashboard.id.clone();
        let mut dashboards = self.dashboards.write();
        
        if dashboards.contains_key(&id) {
            return Err(Error::Generic(format!("Dashboard {} already exists", id)));
        }
        
        dashboards.insert(id.clone(), dashboard);
        
        info!("Created dashboard: {}", id);
        Ok(id)
    }
    
    pub fn update_dashboard(&self, id: &str, dashboard: Dashboard) -> Result<()> {
        let mut dashboards = self.dashboards.write();
        
        if !dashboards.contains_key(id) {
            return Err(Error::Generic(format!("Dashboard {} not found", id)));
        }
        
        dashboards.insert(id.to_string(), dashboard);
        
        self.notify_update(id, UpdateType::ConfigChange);
        
        info!("Updated dashboard: {}", id);
        Ok(())
    }
    
    pub fn delete_dashboard(&self, id: &str) -> Result<()> {
        let mut dashboards = self.dashboards.write();
        
        if dashboards.remove(id).is_none() {
            return Err(Error::Generic(format!("Dashboard {} not found", id)));
        }
        
        self.render_cache.write().remove(id);
        
        info!("Deleted dashboard: {}", id);
        Ok(())
    }
    
    pub fn render_dashboard(&self, id: &str) -> Result<RenderedDashboard> {
        let dashboards = self.dashboards.read();
        
        let dashboard = dashboards.get(id)
            .ok_or_else(|| Error::Generic(format!("Dashboard {} not found", id)))?;
        
        let mut rendered_panels = Vec::new();
        
        for panel in &dashboard.panels {
            let rendered_panel = self.render_panel(panel, &dashboard.time_range)?;
            rendered_panels.push(rendered_panel);
        }
        
        let rendered = RenderedDashboard {
            dashboard_id: id.to_string(),
            rendered_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            panels: rendered_panels,
        };
        
        self.render_cache.write().insert(id.to_string(), rendered.clone());
        
        Ok(rendered)
    }
    
    fn render_panel(&self, panel: &Panel, time_range: &TimeRange) -> Result<RenderedPanel> {
        let data = match panel.panel_type {
            PanelType::Graph => {
                let mut all_points = Vec::new();
                for metric in &panel.metrics {
                    if let Some(points) = self.metrics_collector.get_series(metric, *time_range) {
                        all_points.extend(points);
                    }
                }
                PanelData::TimeSeries(all_points)
            }
            PanelType::Gauge | PanelType::Counter | PanelType::Stat => {
                let value = panel.metrics.first()
                    .and_then(|metric| {
                        self.metrics_collector.get_series(metric, *time_range)
                            .and_then(|points| points.last().map(|p| p.value))
                    })
                    .unwrap_or(0.0);
                PanelData::SingleValue(value)
            }
            PanelType::Table => {
                let mut rows = Vec::new();
                for metric in &panel.metrics {
                    if let Some(points) = self.metrics_collector.get_series(metric, *time_range) {
                        for point in points {
                            rows.push(vec![
                                metric.clone(),
                                point.timestamp.to_string(),
                                point.value.to_string(),
                            ]);
                        }
                    }
                }
                PanelData::Table(rows)
            }
            _ => PanelData::SingleValue(0.0),
        };
        
        let status = self.calculate_panel_status(&data, &panel.thresholds);
        
        Ok(RenderedPanel {
            panel_id: panel.id.clone(),
            data,
            status,
        })
    }
    
    fn calculate_panel_status(&self, data: &PanelData, thresholds: &[Threshold]) -> PanelStatus {
        let value = match data {
            PanelData::SingleValue(v) => *v,
            PanelData::TimeSeries(points) => {
                points.last().map(|p| p.value).unwrap_or(0.0)
            }
            _ => return PanelStatus::Ok,
        };
        
        for threshold in thresholds.iter().rev() {
            if value >= threshold.value {
                return match threshold.color.as_str() {
                    "red" => PanelStatus::Critical,
                    "yellow" | "orange" => PanelStatus::Warning,
                    _ => PanelStatus::Ok,
                };
            }
        }
        
        PanelStatus::Ok
    }
    
    fn notify_update(&self, dashboard_id: &str, update_type: UpdateType) {
        let update = DashboardUpdate {
            dashboard_id: dashboard_id.to_string(),
            update_type,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        let _ = self.update_channel.send(update);
    }
    
    pub fn subscribe_updates(&self) -> broadcast::Receiver<DashboardUpdate> {
        self.update_channel.subscribe()
    }
    
    pub fn list_dashboards(&self) -> Vec<(String, String)> {
        let dashboards = self.dashboards.read();
        dashboards.iter()
            .map(|(id, dash)| (id.clone(), dash.title.clone()))
            .collect()
    }
    
    pub fn export_dashboard(&self, id: &str) -> Result<String> {
        let dashboards = self.dashboards.read();
        let dashboard = dashboards.get(id)
            .ok_or_else(|| Error::Generic(format!("Dashboard {} not found", id)))?;
        
        serde_json::to_string_pretty(dashboard)
            .map_err(|e| Error::Generic(format!("Failed to serialize dashboard: {}", e)))
    }
    
    pub fn import_dashboard(&self, json: &str) -> Result<String> {
        let dashboard: Dashboard = serde_json::from_str(json)
            .map_err(|e| Error::Generic(format!("Failed to deserialize dashboard: {}", e)))?;
        
        self.create_dashboard(dashboard)
    }
}

pub struct AlertManager {
    rules: Arc<RwLock<Vec<AlertRule>>>,
    active_alerts: Arc<RwLock<HashMap<String, Alert>>>,
    alert_history: Arc<RwLock<VecDeque<Alert>>>,
    max_history: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub for_duration: Duration,
    pub cooldown: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCondition {
    ThresholdAbove { metric: String, value: f64 },
    ThresholdBelow { metric: String, value: f64 },
    RateOfChange { metric: String, threshold: f64 },
    Absence { metric: String, duration: Duration },
    Custom { expression: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub rule_id: String,
    pub triggered_at: u64,
    pub resolved_at: Option<u64>,
    pub severity: AlertSeverity,
    pub value: f64,
    pub message: String,
    pub labels: HashMap<String, String>,
}

impl AlertManager {
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(Vec::new())),
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
            alert_history: Arc::new(RwLock::new(VecDeque::new())),
            max_history: 1000,
        }
    }
    
    pub fn add_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write();
        rules.push(rule);
    }
    
    pub fn evaluate(&self, metrics: &HashMap<String, f64>) {
        let rules = self.rules.read().clone();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        for rule in rules {
            let triggered = match &rule.condition {
                AlertCondition::ThresholdAbove { metric, value } => {
                    metrics.get(metric).map_or(false, |v| v > value)
                }
                AlertCondition::ThresholdBelow { metric, value } => {
                    metrics.get(metric).map_or(false, |v| v < value)
                }
                _ => false,
            };
            
            if triggered {
                self.trigger_alert(&rule, metrics.get(&rule.name).copied().unwrap_or(0.0), now);
            } else {
                self.resolve_alert(&rule.id, now);
            }
        }
    }
    
    fn trigger_alert(&self, rule: &AlertRule, value: f64, timestamp: u64) {
        let alert_id = format!("{}_{}", rule.id, timestamp);
        
        let alert = Alert {
            id: alert_id.clone(),
            rule_id: rule.id.clone(),
            triggered_at: timestamp,
            resolved_at: None,
            severity: rule.severity,
            value,
            message: format!("{} triggered: value={}", rule.name, value),
            labels: rule.labels.clone(),
        };
        
        let mut active = self.active_alerts.write();
        active.insert(alert_id, alert.clone());
        
        let mut history = self.alert_history.write();
        history.push_back(alert);
        
        if history.len() > self.max_history {
            history.pop_front();
        }
        
        match rule.severity {
            AlertSeverity::Emergency => error!("EMERGENCY ALERT: {}", rule.name),
            AlertSeverity::Critical => error!("CRITICAL ALERT: {}", rule.name),
            AlertSeverity::Warning => warn!("WARNING ALERT: {}", rule.name),
            AlertSeverity::Info => info!("INFO ALERT: {}", rule.name),
        }
    }
    
    fn resolve_alert(&self, rule_id: &str, timestamp: u64) {
        let mut active = self.active_alerts.write();
        
        let alerts_to_resolve: Vec<_> = active
            .iter()
            .filter(|(_, alert)| alert.rule_id == rule_id && alert.resolved_at.is_none())
            .map(|(id, _)| id.clone())
            .collect();
        
        for alert_id in alerts_to_resolve {
            if let Some(mut alert) = active.remove(&alert_id) {
                alert.resolved_at = Some(timestamp);
                
                let mut history = self.alert_history.write();
                history.push_back(alert);
                
                if history.len() > self.max_history {
                    history.pop_front();
                }
            }
        }
    }
    
    pub fn get_active_alerts(&self) -> Vec<Alert> {
        self.active_alerts.read().values().cloned().collect()
    }
    
    pub fn get_alert_history(&self, limit: usize) -> Vec<Alert> {
        let history = self.alert_history.read();
        history.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }
}

pub fn create_default_dashboards() -> Vec<Dashboard> {
    vec![
        Dashboard {
            id: "system_overview".to_string(),
            title: "System Overview".to_string(),
            description: "Overall system health and performance".to_string(),
            panels: vec![
                Panel {
                    id: "cpu_usage".to_string(),
                    title: "CPU Usage".to_string(),
                    panel_type: PanelType::Graph,
                    metrics: vec!["system.cpu.usage".to_string()],
                    layout: PanelLayout { x: 0, y: 0, width: 6, height: 4 },
                    thresholds: vec![
                        Threshold { value: 80.0, color: "yellow".to_string(), label: "High".to_string() },
                        Threshold { value: 95.0, color: "red".to_string(), label: "Critical".to_string() },
                    ],
                    options: PanelOptions {
                        show_legend: true,
                        show_tooltip: true,
                        unit: Some("%".to_string()),
                        decimals: Some(1),
                        min_value: Some(0.0),
                        max_value: Some(100.0),
                    },
                },
                Panel {
                    id: "memory_usage".to_string(),
                    title: "Memory Usage".to_string(),
                    panel_type: PanelType::Gauge,
                    metrics: vec!["system.memory.usage".to_string()],
                    layout: PanelLayout { x: 6, y: 0, width: 6, height: 4 },
                    thresholds: vec![
                        Threshold { value: 70.0, color: "yellow".to_string(), label: "High".to_string() },
                        Threshold { value: 90.0, color: "red".to_string(), label: "Critical".to_string() },
                    ],
                    options: PanelOptions {
                        show_legend: false,
                        show_tooltip: true,
                        unit: Some("GB".to_string()),
                        decimals: Some(2),
                        min_value: Some(0.0),
                        max_value: None,
                    },
                },
            ],
            refresh_interval: Duration::from_secs(5),
            time_range: TimeRange {
                start: 0,
                end: u64::MAX,
            },
            variables: HashMap::new(),
        },
        Dashboard {
            id: "database_performance".to_string(),
            title: "Database Performance".to_string(),
            description: "Database operations and performance metrics".to_string(),
            panels: vec![
                Panel {
                    id: "ops_per_second".to_string(),
                    title: "Operations Per Second".to_string(),
                    panel_type: PanelType::Graph,
                    metrics: vec![
                        "db.ops.reads".to_string(),
                        "db.ops.writes".to_string(),
                    ],
                    layout: PanelLayout { x: 0, y: 0, width: 12, height: 6 },
                    thresholds: vec![],
                    options: PanelOptions {
                        show_legend: true,
                        show_tooltip: true,
                        unit: Some("ops/s".to_string()),
                        decimals: Some(0),
                        min_value: Some(0.0),
                        max_value: None,
                    },
                },
                Panel {
                    id: "query_latency".to_string(),
                    title: "Query Latency".to_string(),
                    panel_type: PanelType::Heatmap,
                    metrics: vec!["db.query.latency".to_string()],
                    layout: PanelLayout { x: 0, y: 6, width: 12, height: 6 },
                    thresholds: vec![],
                    options: PanelOptions {
                        show_legend: true,
                        show_tooltip: true,
                        unit: Some("ms".to_string()),
                        decimals: Some(2),
                        min_value: Some(0.0),
                        max_value: None,
                    },
                },
            ],
            refresh_interval: Duration::from_secs(10),
            time_range: TimeRange {
                start: 0,
                end: u64::MAX,
            },
            variables: HashMap::new(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        );
        
        let mut labels = HashMap::new();
        labels.insert("host".to_string(), "server1".to_string());
        
        collector.record_counter("requests", 1, labels.clone());
        collector.record_gauge("temperature", 25.5, labels.clone());
        collector.record_histogram("latency", 0.125, labels);
        
        let time_range = TimeRange {
            start: 0,
            end: u64::MAX,
        };
        
        let series = collector.get_series("requests", time_range);
        assert!(series.is_some());
    }
    
    #[test]
    fn test_histogram() {
        let histogram = Histogram::new(vec![1.0, 5.0, 10.0, 50.0, 100.0]);
        
        histogram.observe(0.5);
        histogram.observe(3.0);
        histogram.observe(7.0);
        histogram.observe(25.0);
        histogram.observe(75.0);
        histogram.observe(150.0);
        
        let p50 = histogram.percentile(50.0);
        let p95 = histogram.percentile(95.0);
        
        assert!(p50 <= p95);
    }
    
    #[test]
    fn test_dashboard_creation() {
        let collector = Arc::new(MetricsCollector::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        
        let manager = DashboardManager::new(collector);
        
        let dashboard = Dashboard {
            id: "test".to_string(),
            title: "Test Dashboard".to_string(),
            description: "Test".to_string(),
            panels: vec![],
            refresh_interval: Duration::from_secs(5),
            time_range: TimeRange { start: 0, end: u64::MAX },
            variables: HashMap::new(),
        };
        
        let id = manager.create_dashboard(dashboard).unwrap();
        assert_eq!(id, "test");
        
        let list = manager.list_dashboards();
        assert_eq!(list.len(), 1);
    }
    
    #[test]
    fn test_alert_manager() {
        let alert_manager = AlertManager::new();
        
        let rule = AlertRule {
            id: "high_cpu".to_string(),
            name: "High CPU Usage".to_string(),
            condition: AlertCondition::ThresholdAbove {
                metric: "cpu_usage".to_string(),
                value: 80.0,
            },
            severity: AlertSeverity::Warning,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            for_duration: Duration::from_secs(60),
            cooldown: Duration::from_secs(300),
        };
        
        alert_manager.add_rule(rule);
        
        let mut metrics = HashMap::new();
        metrics.insert("cpu_usage".to_string(), 85.0);
        
        alert_manager.evaluate(&metrics);
        
        let active = alert_manager.get_active_alerts();
        assert!(!active.is_empty());
    }
}