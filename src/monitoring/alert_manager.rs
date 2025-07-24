//! Alert Management System for Lightning DB
//!
//! Comprehensive alerting system with configurable conditions, severity levels,
//! notification channels, and alert lifecycle management.

use crate::{Database, Result, Error};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use tracing::{info, warn, error, debug};

use super::health_checker::HealthStatus;
use super::metrics_collector::DatabaseMetrics;
use super::performance_monitor::PerformanceData;
use super::resource_tracker::ResourceUsage;

/// Alert manager for Lightning DB
pub struct AlertManager {
    /// Alert rules and conditions
    alert_rules: Arc<RwLock<Vec<AlertRule>>>,
    /// Active alerts
    active_alerts: Arc<RwLock<HashMap<String, Alert>>>,
    /// Alert history
    alert_history: Arc<RwLock<VecDeque<AlertEvent>>>,
    /// Notification channels
    notification_channels: Arc<RwLock<Vec<Box<dyn NotificationChannel + Send + Sync>>>>,
    /// Configuration
    config: AlertConfig,
    /// Alert ID counter
    alert_id_counter: AtomicU64,
}

/// Configuration for alert management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Maximum number of active alerts
    pub max_active_alerts: usize,
    /// Maximum alert history entries
    pub max_history_entries: usize,
    /// Default alert timeout
    pub default_alert_timeout: Duration,
    /// Evaluation interval
    pub evaluation_interval: Duration,
    /// Enable alert aggregation
    pub enable_aggregation: bool,
    /// Aggregation window
    pub aggregation_window: Duration,
    /// Enable alert suppression
    pub enable_suppression: bool,
    /// Suppression window
    pub suppression_window: Duration,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            max_active_alerts: 1000,
            max_history_entries: 10000,
            default_alert_timeout: Duration::from_secs(24 * 3600),
            evaluation_interval: Duration::from_secs(30),
            enable_aggregation: true,
            aggregation_window: Duration::from_secs(300), // 5 minutes
            enable_suppression: true,
            suppression_window: Duration::from_secs(600), // 10 minutes
        }
    }
}

/// Alert with metadata and lifecycle information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique alert identifier
    pub id: String,
    /// Alert name/title
    pub name: String,
    /// Detailed alert message
    pub message: String,
    /// Alert severity level
    pub severity: AlertSeverity,
    /// Alert labels/tags
    pub labels: HashMap<String, String>,
    /// When the alert was created
    pub timestamp: SystemTime,
    /// When the alert was last updated
    pub last_updated: SystemTime,
    /// Alert state
    pub state: AlertState,
    /// Source rule that generated this alert
    pub rule_name: String,
    /// Number of times this alert has fired
    pub fire_count: u64,
    /// Alert expiration time
    pub expires_at: Option<SystemTime>,
    /// Resolved timestamp
    pub resolved_at: Option<SystemTime>,
    /// Resolution message
    pub resolution_message: Option<String>,
}

/// Alert severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Alert states
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AlertState {
    Pending,
    Firing,
    Suppressed,
    Resolved,
    Expired,
}

/// Alert rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Rule name
    pub name: String,
    /// Rule description
    pub description: String,
    /// Alert condition
    pub condition: AlertCondition,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Labels to add to alerts
    pub labels: HashMap<String, String>,
    /// Rule enabled
    pub enabled: bool,
    /// Evaluation interval
    pub evaluation_interval: Duration,
    /// For duration (how long condition must be true)
    pub for_duration: Duration,
    /// Alert message template
    pub message_template: String,
    /// Auto-resolve condition
    pub resolve_condition: Option<AlertCondition>,
}

/// Alert conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCondition {
    /// Metric threshold condition
    MetricThreshold {
        metric: String,
        operator: ComparisonOperator,
        threshold: f64,
    },
    /// Performance degradation
    PerformanceDegradation {
        metric: String,
        degradation_percent: f64,
        window: Duration,
    },
    /// Health status condition
    HealthStatus {
        status: HealthStatusCondition,
    },
    /// Resource usage condition
    ResourceUsage {
        resource: String,
        operator: ComparisonOperator,
        threshold: f64,
    },
    /// Composite condition (AND/OR of multiple conditions)
    Composite {
        operator: LogicalOperator,
        conditions: Vec<AlertCondition>,
    },
    /// Rate condition (change over time)
    Rate {
        metric: String,
        operator: ComparisonOperator,
        rate_threshold: f64,
        window: Duration,
    },
}

/// Comparison operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonOperator {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
}

/// Logical operators for composite conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalOperator {
    And,
    Or,
}

/// Health status conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatusCondition {
    Unhealthy,
    Degraded,
    Unknown,
}

/// Alert event for audit trail
#[derive(Debug, Clone, Serialize)]
pub struct AlertEvent {
    pub event_id: String,
    pub alert_id: String,
    pub event_type: AlertEventType,
    pub timestamp: SystemTime,
    pub message: String,
    pub metadata: HashMap<String, String>,
}

/// Types of alert events
#[derive(Debug, Clone, Serialize)]
pub enum AlertEventType {
    Created,
    Fired,
    Updated,
    Suppressed,
    Resolved,
    Expired,
    Acknowledged,
    Escalated,
}

/// Notification channel trait
pub trait NotificationChannel {
    fn name(&self) -> &str;
    fn send_alert(&self, alert: &Alert) -> Result<()>;
    fn send_alert_resolution(&self, alert: &Alert) -> Result<()>;
    fn is_enabled(&self) -> bool { true }
}

/// Console notification channel (for development)
pub struct ConsoleNotificationChannel {
    name: String,
    enabled: bool,
}

impl ConsoleNotificationChannel {
    pub fn new(name: String) -> Self {
        Self {
            name,
            enabled: true,
        }
    }
}

impl NotificationChannel for ConsoleNotificationChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn send_alert(&self, alert: &Alert) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        println!("🚨 ALERT [{}]: {}", alert.severity.to_string().to_uppercase(), alert.name);
        println!("   Message: {}", alert.message);
        println!("   Severity: {:?}", alert.severity);
        println!("   Time: {:?}", alert.timestamp);
        if !alert.labels.is_empty() {
            println!("   Labels: {:?}", alert.labels);
        }
        println!();

        Ok(())
    }

    fn send_alert_resolution(&self, alert: &Alert) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        println!("✅ RESOLVED [{}]: {}", alert.severity.to_string().to_uppercase(), alert.name);
        println!("   Resolution: {}", alert.resolution_message.as_deref().unwrap_or("Alert resolved"));
        println!("   Duration: {:?}", 
            alert.resolved_at.and_then(|resolved| 
                resolved.duration_since(alert.timestamp).ok()
            ).unwrap_or(Duration::from_secs(0))
        );
        println!();

        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Webhook notification channel
pub struct WebhookNotificationChannel {
    name: String,
    webhook_url: String,
    enabled: bool,
    headers: HashMap<String, String>,
}

impl WebhookNotificationChannel {
    pub fn new(name: String, webhook_url: String) -> Self {
        Self {
            name,
            webhook_url,
            enabled: true,
            headers: HashMap::new(),
        }
    }

    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }
}

impl NotificationChannel for WebhookNotificationChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn send_alert(&self, alert: &Alert) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let payload = serde_json::json!({
            "alert_id": alert.id,
            "name": alert.name,
            "message": alert.message,
            "severity": alert.severity,
            "labels": alert.labels,
            "timestamp": alert.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            "state": alert.state,
            "fire_count": alert.fire_count
        });

        // In a real implementation, this would make an HTTP request
        debug!("Webhook alert sent to {}: {}", self.webhook_url, payload);
        Ok(())
    }

    fn send_alert_resolution(&self, alert: &Alert) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let payload = serde_json::json!({
            "alert_id": alert.id,
            "name": alert.name,
            "message": alert.message,
            "severity": alert.severity,
            "labels": alert.labels,
            "timestamp": alert.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            "resolved_at": alert.resolved_at.map(|t| t.duration_since(UNIX_EPOCH).unwrap().as_secs()),
            "resolution_message": alert.resolution_message,
            "state": "resolved"
        });

        debug!("Webhook resolution sent to {}: {}", self.webhook_url, payload);
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new() -> Self {
        Self::with_config(AlertConfig::default())
    }

    /// Create alert manager with custom configuration
    pub fn with_config(config: AlertConfig) -> Self {
        let mut manager = Self {
            alert_rules: Arc::new(RwLock::new(Vec::new())),
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
            alert_history: Arc::new(RwLock::new(VecDeque::new())),
            notification_channels: Arc::new(RwLock::new(Vec::new())),
            config,
            alert_id_counter: AtomicU64::new(1),
        };

        // Add default notification channel
        manager.add_notification_channel(Box::new(
            ConsoleNotificationChannel::new("console".to_string())
        ));

        // Register default alert rules
        manager.register_default_rules();

        manager
    }

    /// Register default alert rules
    fn register_default_rules(&self) {
        let default_rules = vec![
            AlertRule {
                name: "high_error_rate".to_string(), 
                description: "High error rate detected".to_string(),
                condition: AlertCondition::MetricThreshold {
                    metric: "error_rate".to_string(),
                    operator: ComparisonOperator::GreaterThan,
                    threshold: 0.05, // 5% error rate
                },
                severity: AlertSeverity::Critical,
                labels: HashMap::new(),
                enabled: true,
                evaluation_interval: Duration::from_secs(60),
                for_duration: Duration::from_secs(300), // 5 minutes
                message_template: "Error rate is {{value}}%, exceeding threshold of 5%".to_string(),
                resolve_condition: Some(AlertCondition::MetricThreshold {
                    metric: "error_rate".to_string(),
                    operator: ComparisonOperator::LessThan,
                    threshold: 0.02, // 2% error rate
                }),
            },
            AlertRule {
                name: "high_latency".to_string(),
                description: "High operation latency detected".to_string(),
                condition: AlertCondition::MetricThreshold {
                    metric: "average_latency_ms".to_string(),
                    operator: ComparisonOperator::GreaterThan,
                    threshold: 1000.0, // 1 second
                },
                severity: AlertSeverity::Warning,
                labels: HashMap::new(),
                enabled: true,
                evaluation_interval: Duration::from_secs(60),
                for_duration: Duration::from_secs(180), // 3 minutes
                message_template: "Average latency is {{value}}ms, exceeding threshold of 1000ms".to_string(),
                resolve_condition: Some(AlertCondition::MetricThreshold {
                    metric: "average_latency_ms".to_string(),
                    operator: ComparisonOperator::LessThan,
                    threshold: 500.0, // 500ms
                }),
            },
            AlertRule {
                name: "low_cache_hit_rate".to_string(),
                description: "Cache hit rate is too low".to_string(),
                condition: AlertCondition::MetricThreshold {
                    metric: "cache_hit_rate".to_string(),
                    operator: ComparisonOperator::LessThan,
                    threshold: 0.8, // 80% hit rate
                },
                severity: AlertSeverity::Warning,
                labels: HashMap::new(),
                enabled: true,
                evaluation_interval: Duration::from_secs(120),
                for_duration: Duration::from_secs(600), // 10 minutes
                message_template: "Cache hit rate is {{value}}%, below threshold of 80%".to_string(),
                resolve_condition: Some(AlertCondition::MetricThreshold {
                    metric: "cache_hit_rate".to_string(),
                    operator: ComparisonOperator::GreaterThan,
                    threshold: 0.85, // 85% hit rate
                }),
            },
            AlertRule {
                name: "system_unhealthy".to_string(),
                description: "System health is unhealthy".to_string(),
                condition: AlertCondition::HealthStatus {
                    status: HealthStatusCondition::Unhealthy,
                },
                severity: AlertSeverity::Critical,
                labels: HashMap::new(),
                enabled: true,
                evaluation_interval: Duration::from_secs(30),
                for_duration: Duration::from_secs(60), // 1 minute
                message_template: "System health status is UNHEALTHY".to_string(),
                resolve_condition: None, // Will auto-resolve when health improves
            },
        ];

        let mut rules = self.alert_rules.write().unwrap();
        rules.extend(default_rules);
    }

    /// Add a custom alert rule
    pub fn add_alert_rule(&self, rule: AlertRule) {
        let mut rules = self.alert_rules.write().unwrap();
        rules.push(rule);
    }

    /// Add a notification channel
    pub fn add_notification_channel(&self, channel: Box<dyn NotificationChannel + Send + Sync>) {
        let mut channels = self.notification_channels.write().unwrap();
        channels.push(channel);
    }

    /// Evaluate alert conditions
    pub fn evaluate_conditions(
        &self,
        _database: &Database,
        metrics: &DatabaseMetrics,
        health: &HealthStatus,
        performance: &PerformanceData,
        resources: &ResourceUsage,
    ) -> Result<()> {
        let rules = self.alert_rules.read().unwrap().clone();
        
        for rule in rules.iter().filter(|r| r.enabled) {
            let condition_met = self.evaluate_condition(
                &rule.condition,
                metrics,
                health,
                performance,
                resources,
            )?;

            if condition_met {
                self.handle_condition_met(rule, metrics)?;
            } else {
                self.handle_condition_not_met(rule)?;
            }
        }

        // Clean up expired alerts
        self.cleanup_expired_alerts()?;

        Ok(())
    }

    /// Evaluate a single alert condition
    fn evaluate_condition(
        &self,
        condition: &AlertCondition,
        metrics: &DatabaseMetrics,
        health: &HealthStatus,
        performance: &PerformanceData,
        resources: &ResourceUsage,
    ) -> Result<bool> {
        match condition {
            AlertCondition::MetricThreshold { metric, operator, threshold } => {
                let value = self.get_metric_value(metric, metrics, performance, resources)?;
                Ok(self.compare_values(value, *threshold, operator))
            }
            AlertCondition::PerformanceDegradation { metric, degradation_percent, window: _ } => {
                let current_value = self.get_metric_value(metric, metrics, performance, resources)?;
                // Simplified: would need historical values for proper degradation detection
                let baseline_value = current_value * 1.2; // Simulate 20% worse baseline
                let degradation = ((baseline_value - current_value) / baseline_value * 100.0).abs();
                Ok(degradation > *degradation_percent)
            }
            AlertCondition::HealthStatus { status } => {
                match (health, status) {
                    (HealthStatus::Unhealthy(_), HealthStatusCondition::Unhealthy) => Ok(true),
                    (HealthStatus::Degraded(_), HealthStatusCondition::Degraded) => Ok(true),
                    (HealthStatus::Unknown, HealthStatusCondition::Unknown) => Ok(true),
                    _ => Ok(false),
                }
            }
            AlertCondition::ResourceUsage { resource, operator, threshold } => {
                let value = match resource.as_str() {
                    "cpu_usage_percent" => resources.cpu_usage_percent,
                    "memory_usage_percent" => resources.memory_usage_bytes as f64,
                    "disk_usage_bytes" => resources.disk_usage_bytes as f64,
                    _ => return Err(Error::Generic(format!("Unknown resource metric: {}", resource))),
                };
                Ok(self.compare_values(value, *threshold, operator))
            }
            AlertCondition::Composite { operator, conditions } => {
                let results: Result<Vec<bool>> = conditions.iter()
                    .map(|c| self.evaluate_condition(c, metrics, health, performance, resources))
                    .collect();
                
                match operator {
                    LogicalOperator::And => Ok(results?.iter().all(|&x| x)),
                    LogicalOperator::Or => Ok(results?.iter().any(|&x| x)),
                }
            }
            AlertCondition::Rate { metric, operator, rate_threshold, window: _ } => {
                // Simplified rate calculation - would need time series data
                let current_value = self.get_metric_value(metric, metrics, performance, resources)?;
                let rate = current_value; // Placeholder - would calculate actual rate
                Ok(self.compare_values(rate, *rate_threshold, operator))
            }
        }
    }

    /// Get metric value by name
    fn get_metric_value(
        &self,
        metric: &str,
        metrics: &DatabaseMetrics,
        performance: &PerformanceData,
        resources: &ResourceUsage,
    ) -> Result<f64> {
        match metric {
            "error_rate" => Ok(metrics.error_rate),
            "average_latency_ms" => Ok(metrics.average_latency.as_millis() as f64),
            "cache_hit_rate" => Ok(metrics.cache_hit_rate),
            "operations_per_second" => Ok(metrics.operations_per_second),
            "total_operations" => Ok(metrics.total_operations as f64),
            "storage_size_bytes" => Ok(metrics.storage_size_bytes as f64),
            "active_transactions" => Ok(metrics.active_transactions as f64),
            "read_throughput" => Ok(performance.read_throughput),
            "write_throughput" => Ok(performance.write_throughput),
            "transaction_success_rate" => Ok(performance.transaction_success_rate),
            "cpu_usage_percent" => Ok(resources.cpu_usage_percent),
            "memory_usage_bytes" => Ok(resources.memory_usage_bytes as f64),
            "disk_usage_bytes" => Ok(resources.disk_usage_bytes as f64),
            _ => Err(Error::Generic(format!("Unknown metric: {}", metric))),
        }
    }

    /// Compare two values using the given operator
    fn compare_values(&self, left: f64, right: f64, operator: &ComparisonOperator) -> bool {
        match operator {
            ComparisonOperator::GreaterThan => left > right,
            ComparisonOperator::GreaterThanOrEqual => left >= right,
            ComparisonOperator::LessThan => left < right,
            ComparisonOperator::LessThanOrEqual => left <= right,
            ComparisonOperator::Equal => (left - right).abs() < f64::EPSILON,
            ComparisonOperator::NotEqual => (left - right).abs() >= f64::EPSILON,
        }
    }

    /// Handle when a condition is met
    fn handle_condition_met(&self, rule: &AlertRule, metrics: &DatabaseMetrics) -> Result<()> {
        let alert_key = format!("{}_{}", rule.name, "default");
        
        let mut active_alerts = self.active_alerts.write().unwrap();
        
        if let Some(existing_alert) = active_alerts.get_mut(&alert_key) {
            // Update existing alert
            existing_alert.fire_count += 1;
            existing_alert.last_updated = SystemTime::now();
            existing_alert.state = AlertState::Firing;
            
            // Send notification if needed (e.g., on escalation)
            if existing_alert.fire_count % 10 == 0 { // Every 10th fire
                self.send_alert_notification(existing_alert)?;
            }
        } else {
            // Create new alert
            let alert_id = self.alert_id_counter.fetch_add(1, Ordering::Relaxed);
            let alert = Alert {
                id: format!("alert_{}", alert_id),
                name: rule.name.clone(),
                message: self.format_alert_message(&rule.message_template, metrics),
                severity: rule.severity.clone(),
                labels: rule.labels.clone(),
                timestamp: SystemTime::now(),
                last_updated: SystemTime::now(),
                state: AlertState::Firing,
                rule_name: rule.name.clone(),
                fire_count: 1,
                expires_at: Some(SystemTime::now() + self.config.default_alert_timeout),
                resolved_at: None,
                resolution_message: None,
            };

            // Send notification
            self.send_alert_notification(&alert)?;
            
            // Record event
            self.record_alert_event(&alert, AlertEventType::Created)?;
            
            active_alerts.insert(alert_key, alert);
        }

        Ok(())
    }

    /// Handle when a condition is not met
    fn handle_condition_not_met(&self, rule: &AlertRule) -> Result<()> {
        if let Some(resolve_condition) = &rule.resolve_condition {
            let alert_key = format!("{}_{}", rule.name, "default");
            
            let mut active_alerts = self.active_alerts.write().unwrap();
            
            if let Some(alert) = active_alerts.get_mut(&alert_key) {
                if matches!(alert.state, AlertState::Firing) {
                    // Mark as resolved
                    alert.state = AlertState::Resolved;
                    alert.resolved_at = Some(SystemTime::now());
                    alert.resolution_message = Some("Alert condition no longer met".to_string());
                    alert.last_updated = SystemTime::now();
                    
                    // Send resolution notification
                    self.send_alert_resolution(alert)?;
                    
                    // Record event
                    self.record_alert_event(alert, AlertEventType::Resolved)?;
                    
                    // Remove from active alerts
                    active_alerts.remove(&alert_key);
                }
            }
        }

        Ok(())
    }

    /// Send alert notification to all channels
    fn send_alert_notification(&self, alert: &Alert) -> Result<()> {
        let channels = self.notification_channels.read().unwrap();
        
        for channel in channels.iter() {
            if channel.is_enabled() {
                if let Err(e) = channel.send_alert(alert) {
                    error!("Failed to send alert via {}: {}", channel.name(), e);
                }
            }
        }

        Ok(())
    }

    /// Send alert resolution notification
    fn send_alert_resolution(&self, alert: &Alert) -> Result<()> {
        let channels = self.notification_channels.read().unwrap();
        
        for channel in channels.iter() {
            if channel.is_enabled() {
                if let Err(e) = channel.send_alert_resolution(alert) {
                    error!("Failed to send alert resolution via {}: {}", channel.name(), e);
                }
            }
        }

        Ok(())
    }

    /// Format alert message template
    fn format_alert_message(&self, template: &str, metrics: &DatabaseMetrics) -> String {
        // Simple template substitution - would use a proper template engine in production
        template
            .replace("{{error_rate}}", &format!("{:.2}", metrics.error_rate * 100.0))
            .replace("{{average_latency_ms}}", &format!("{:.0}", metrics.average_latency.as_millis()))
            .replace("{{cache_hit_rate}}", &format!("{:.2}", metrics.cache_hit_rate * 100.0))
            .replace("{{operations_per_second}}", &format!("{:.0}", metrics.operations_per_second))
    }

    /// Record alert event
    fn record_alert_event(&self, alert: &Alert, event_type: AlertEventType) -> Result<()> {
        let event = AlertEvent {
            event_id: format!("event_{}_{}", alert.id, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()),
            alert_id: alert.id.clone(),
            event_type: event_type.clone(),
            timestamp: SystemTime::now(),
            message: format!("Alert {} {:?}", alert.name, event_type),
            metadata: HashMap::new(),
        };

        let mut history = self.alert_history.write().unwrap();
        history.push_back(event);

        // Trim history
        while history.len() > self.config.max_history_entries {
            history.pop_front();
        }

        Ok(())
    }

    /// Clean up expired alerts
    fn cleanup_expired_alerts(&self) -> Result<()> {
        let now = SystemTime::now();
        let mut active_alerts = self.active_alerts.write().unwrap();
        
        let expired_keys: Vec<String> = active_alerts
            .iter()
            .filter(|(_, alert)| {
                if let Some(expires_at) = alert.expires_at {
                    now > expires_at
                } else {
                    false
                }
            })
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired_keys {
            if let Some(mut alert) = active_alerts.remove(&key) {
                alert.state = AlertState::Expired;
                alert.last_updated = now;
                
                self.record_alert_event(&alert, AlertEventType::Expired)?;
                info!("Alert {} expired", alert.name);
            }
        }

        Ok(())
    }

    /// Get active alerts
    pub fn get_active_alerts(&self) -> Vec<Alert> {
        self.active_alerts.read().unwrap().values().cloned().collect()
    }

    /// Get alert history
    pub fn get_alert_history(&self) -> Vec<AlertEvent> {
        self.alert_history.read().unwrap().iter().cloned().collect()
    }

    /// Get alert statistics
    pub fn get_alert_stats(&self) -> AlertStats {
        let active_alerts = self.active_alerts.read().unwrap();
        let history = self.alert_history.read().unwrap();
        
        let mut stats_by_severity = HashMap::new();
        for alert in active_alerts.values() {
            *stats_by_severity.entry(alert.severity.clone()).or_insert(0) += 1;
        }

        AlertStats {
            total_active_alerts: active_alerts.len(),
            alerts_by_severity: stats_by_severity,
            total_historical_alerts: history.len(),
            most_frequent_alert: self.get_most_frequent_alert(&history),
        }
    }

    /// Get most frequent alert from history
    fn get_most_frequent_alert(&self, history: &VecDeque<AlertEvent>) -> Option<String> {
        let mut alert_counts = HashMap::new();
        
        for event in history {
            if matches!(event.event_type, AlertEventType::Created) {
                *alert_counts.entry(event.alert_id.clone()).or_insert(0) += 1;
            }
        }
        
        alert_counts.into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(alert_id, _)| alert_id)
    }
}

/// Alert statistics
#[derive(Debug, Serialize)]
pub struct AlertStats {
    pub total_active_alerts: usize,
    pub alerts_by_severity: HashMap<AlertSeverity, usize>,
    pub total_historical_alerts: usize,
    pub most_frequent_alert: Option<String>,
}

impl ToString for AlertSeverity {
    fn to_string(&self) -> String {
        match self {
            AlertSeverity::Info => "info".to_string(),
            AlertSeverity::Warning => "warning".to_string(),
            AlertSeverity::Critical => "critical".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::LightningDbConfig;

    #[test]
    fn test_alert_manager_creation() {
        let manager = AlertManager::new();
        let rules = manager.alert_rules.read().unwrap();
        assert!(!rules.is_empty());
    }

    #[test]
    fn test_alert_condition_evaluation() {
        let manager = AlertManager::new();
        let metrics = DatabaseMetrics::default();
        let health = HealthStatus::Healthy;
        let performance = PerformanceData::default();
        let resources = ResourceUsage::default();

        let condition = AlertCondition::MetricThreshold {
            metric: "cpu_usage_percent".to_string(),
            operator: ComparisonOperator::GreaterThan,
            threshold: 80.0,
        };

        let result = manager.evaluate_condition(&condition, &metrics, &health, &performance, &resources).unwrap();
        assert!(!result); // 50% < 80%
    }

    #[test]
    fn test_comparison_operators() {
        let manager = AlertManager::new();
        
        assert!(manager.compare_values(10.0, 5.0, &ComparisonOperator::GreaterThan));
        assert!(!manager.compare_values(5.0, 10.0, &ComparisonOperator::GreaterThan));
        assert!(manager.compare_values(5.0, 10.0, &ComparisonOperator::LessThan));
        assert!(manager.compare_values(10.0, 10.0, &ComparisonOperator::Equal));
    }

    #[test]
    fn test_notification_channels() {
        let channel = ConsoleNotificationChannel::new("test".to_string());
        assert_eq!(channel.name(), "test");
        assert!(channel.is_enabled());
        
        let alert = Alert {
            id: "test_alert".to_string(),
            name: "Test Alert".to_string(),
            message: "Test message".to_string(),
            severity: AlertSeverity::Warning,
            labels: HashMap::new(),
            timestamp: SystemTime::now(),
            last_updated: SystemTime::now(),
            state: AlertState::Firing,
            rule_name: "test_rule".to_string(),
            fire_count: 1,
            expires_at: None,
            resolved_at: None,
            resolution_message: None,
        };

        // Should not panic
        let result = channel.send_alert(&alert);
        assert!(result.is_ok());
    }
}