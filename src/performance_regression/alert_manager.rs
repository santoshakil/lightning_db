//! Alert Manager for Performance Regression Detection
//!
//! Manages alerting and notification systems for performance regressions,
//! including rate limiting, escalation, and multi-channel notifications.

use super::{RegressionDetectionResult, RegressionSeverity, RegressionDetectorConfig};
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, Duration};
use serde::{Serialize, Deserialize};

/// Alert manager for regression notifications
pub struct AlertManager {
    config: RegressionDetectorConfig,
    alert_channels: Vec<Box<dyn AlertChannel + Send + Sync>>,
    alert_history: Arc<Mutex<AlertHistory>>,
    escalation_rules: Vec<EscalationRule>,
    rate_limiter: Arc<Mutex<AlertRateLimiter>>,
}

/// Alert channel trait for different notification methods
pub trait AlertChannel {
    fn send_alert(&self, alert: &Alert) -> Result<()>;
    fn test_connection(&self) -> Result<bool>;
    fn get_channel_type(&self) -> AlertChannelType;
}

/// Types of alert channels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertChannelType {
    Email,
    Slack,
    Webhook,
    PagerDuty,
    Console,
    Log,
    Metrics,
}

/// Alert message structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub timestamp: SystemTime,
    pub severity: RegressionSeverity,
    pub title: String,
    pub description: String,
    pub operation_type: String,
    pub degradation_percentage: f64,
    pub statistical_confidence: f64,
    pub recommended_actions: Vec<String>,
    pub metadata: HashMap<String, String>,
    pub escalation_level: u32,
}

/// Alert history management
struct AlertHistory {
    alerts: VecDeque<Alert>,
    last_alert_by_operation: HashMap<String, SystemTime>,
    suppressed_alerts: HashMap<String, SuppressedAlert>,
}

/// Suppressed alert information
#[derive(Debug, Clone)]
struct SuppressedAlert {
    first_occurrence: SystemTime,
    last_occurrence: SystemTime,
    count: usize,
    original_alert: Alert,
}

/// Escalation rule configuration
#[derive(Debug, Clone)]
pub struct EscalationRule {
    pub severity_threshold: RegressionSeverity,
    pub time_to_escalate: Duration,
    pub escalation_channels: Vec<AlertChannelType>,
    pub max_escalation_level: u32,
}

/// Rate limiter for alerts
struct AlertRateLimiter {
    operation_limits: HashMap<String, OperationRateLimit>,
    global_limit: GlobalRateLimit,
}

/// Rate limit per operation type
#[derive(Debug, Clone)]
struct OperationRateLimit {
    last_alert: SystemTime,
    alert_count: usize,
    cooldown_until: SystemTime,
}

/// Global rate limit
#[derive(Debug, Clone)]
struct GlobalRateLimit {
    alerts_this_hour: usize,
    hour_start: SystemTime,
    max_alerts_per_hour: usize,
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new(config: RegressionDetectorConfig) -> Self {
        let alert_history = Arc::new(Mutex::new(AlertHistory {
            alerts: VecDeque::new(),
            last_alert_by_operation: HashMap::new(),
            suppressed_alerts: HashMap::new(),
        }));

        let rate_limiter = Arc::new(Mutex::new(AlertRateLimiter {
            operation_limits: HashMap::new(),
            global_limit: GlobalRateLimit {
                alerts_this_hour: 0,
                hour_start: SystemTime::now(),
                max_alerts_per_hour: 50, // Configurable limit
            },
        }));

        let escalation_rules = Self::create_default_escalation_rules();

        Self {
            config,
            alert_channels: Vec::new(),
            alert_history,
            escalation_rules,
            rate_limiter,
        }
    }

    /// Add an alert channel
    pub fn add_channel(&mut self, channel: Box<dyn AlertChannel + Send + Sync>) {
        self.alert_channels.push(channel);
    }

    /// Send regression alert
    pub fn send_regression_alert(&self, regression: &RegressionDetectionResult) -> Result<()> {
        // Check rate limiting
        if !self.should_send_alert(&regression.operation_type, regression.severity)? {
            self.suppress_alert(regression)?;
            return Ok(());
        }

        // Create alert
        let alert = self.create_alert_from_regression(regression)?;

        // Send alert through appropriate channels
        self.send_alert_to_channels(&alert)?;

        // Update alert history
        self.update_alert_history(&alert)?;

        // Update rate limiter
        self.update_rate_limiter(&regression.operation_type)?;

        // Schedule escalation if needed
        self.schedule_escalation(&alert)?;

        Ok(())
    }

    /// Create alert from regression detection result
    fn create_alert_from_regression(&self, regression: &RegressionDetectionResult) -> Result<Alert> {
        let alert_id = format!("perf_regression_{}_{}",
            regression.operation_type,
            regression.detection_timestamp.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default().as_secs()
        );

        let title = format!(
            "{:?} Performance Regression: {} ({:.1}% degradation)",
            regression.severity,
            regression.operation_type,
            regression.degradation_percentage * 100.0
        );

        let description = self.create_alert_description(regression);

        let mut metadata = HashMap::new();
        metadata.insert("trace_id".to_string(), 
            regression.current_performance.trace_id.clone().unwrap_or_default());
        metadata.insert("span_id".to_string(), 
            regression.current_performance.span_id.clone().unwrap_or_default());
        metadata.insert("baseline_created".to_string(),
            format!("{:?}", regression.baseline_performance.created_at));
        metadata.insert("baseline_samples".to_string(),
            regression.baseline_performance.sample_count.to_string());

        Ok(Alert {
            id: alert_id,
            timestamp: SystemTime::now(),
            severity: regression.severity,
            title,
            description,
            operation_type: regression.operation_type.clone(),
            degradation_percentage: regression.degradation_percentage,
            statistical_confidence: regression.statistical_confidence,
            recommended_actions: regression.recommended_actions.clone(),
            metadata,
            escalation_level: 0,
        })
    }

    /// Create detailed alert description
    fn create_alert_description(&self, regression: &RegressionDetectionResult) -> String {
        let mut description = String::new();
        
        description.push_str(&format!(
            "Performance regression detected for operation: {}\n\n",
            regression.operation_type
        ));

        description.push_str(&format!(
            "📊 Performance Impact:\n\
             • Degradation: {:.1}%\n\
             • Current Duration: {:.2}ms\n\
             • Baseline Duration: {:.2}ms\n\
             • Statistical Confidence: {:.1}%\n\n",
            regression.degradation_percentage * 100.0,
            regression.current_performance.duration_micros as f64 / 1000.0,
            regression.baseline_performance.mean_duration_micros / 1000.0,
            regression.statistical_confidence * 100.0
        ));

        if regression.current_performance.memory_usage_bytes > 0 {
            description.push_str(&format!(
                "💾 Memory Usage:\n\
                 • Current: {} MB\n\
                 • Baseline: {} MB\n\n",
                regression.current_performance.memory_usage_bytes / (1024 * 1024),
                regression.baseline_performance.memory_baseline / (1024 * 1024)
            ));
        }

        if regression.current_performance.cpu_usage_percent > 0.0 {
            description.push_str(&format!(
                "⚡ CPU Usage:\n\
                 • Current: {:.1}%\n\
                 • Baseline: {:.1}%\n\n",
                regression.current_performance.cpu_usage_percent,
                regression.baseline_performance.cpu_baseline
            ));
        }

        if !regression.recommended_actions.is_empty() {
            description.push_str("🔧 Recommended Actions:\n");
            for (i, action) in regression.recommended_actions.iter().enumerate() {
                description.push_str(&format!("{}. {}\n", i + 1, action));
            }
            description.push('\n');
        }

        if let Some(trace_id) = &regression.current_performance.trace_id {
            description.push_str(&format!("🔍 Trace ID: {}\n", trace_id));
        }

        description.push_str(&format!(
            "⏰ Detection Time: {:?}\n",
            regression.detection_timestamp
        ));

        description
    }

    /// Send alert to appropriate channels based on severity
    fn send_alert_to_channels(&self, alert: &Alert) -> Result<()> {
        let channels_to_use = self.select_channels_for_severity(alert.severity);
        
        for channel in &self.alert_channels {
            if channels_to_use.contains(&channel.get_channel_type()) {
                if let Err(e) = channel.send_alert(alert) {
                    eprintln!("Failed to send alert via {:?}: {}", channel.get_channel_type(), e);
                    // Continue with other channels
                }
            }
        }

        Ok(())
    }

    /// Select appropriate channels based on severity
    fn select_channels_for_severity(&self, severity: RegressionSeverity) -> Vec<AlertChannelType> {
        match severity {
            RegressionSeverity::Critical => vec![
                AlertChannelType::PagerDuty,
                AlertChannelType::Slack,
                AlertChannelType::Email,
                AlertChannelType::Log,
            ],
            RegressionSeverity::Major => vec![
                AlertChannelType::Slack,
                AlertChannelType::Email,
                AlertChannelType::Log,
            ],
            RegressionSeverity::Moderate => vec![
                AlertChannelType::Slack,
                AlertChannelType::Log,
            ],
            RegressionSeverity::Minor => vec![
                AlertChannelType::Log,
                AlertChannelType::Metrics,
            ],
        }
    }

    /// Check if alert should be sent based on rate limiting
    fn should_send_alert(&self, operation_type: &str, severity: RegressionSeverity) -> Result<bool> {
        if let Ok(mut rate_limiter) = self.rate_limiter.lock() {
            // Check global rate limit
            let now = SystemTime::now();
            if now.duration_since(rate_limiter.global_limit.hour_start).unwrap_or_default().as_secs() >= 3600 {
                // Reset hourly counter
                rate_limiter.global_limit.hour_start = now;
                rate_limiter.global_limit.alerts_this_hour = 0;
            }

            if rate_limiter.global_limit.alerts_this_hour >= rate_limiter.global_limit.max_alerts_per_hour {
                return Ok(false); // Global rate limit exceeded
            }

            // Check operation-specific rate limit
            let cooldown_duration = Duration::from_secs(self.config.alert_cooldown_minutes * 60);
            
            if let Some(op_limit) = rate_limiter.operation_limits.get(operation_type) {
                if now < op_limit.cooldown_until {
                    return Ok(false); // Still in cooldown
                }
                
                // Critical alerts bypass some rate limiting
                if severity != RegressionSeverity::Critical {
                    let time_since_last = now.duration_since(op_limit.last_alert).unwrap_or_default();
                    if time_since_last < cooldown_duration {
                        return Ok(false); // Too soon since last alert
                    }
                }
            }

            Ok(true)
        } else {
            Ok(true) // Default to allowing alerts if lock fails
        }
    }

    /// Update rate limiter after sending alert
    fn update_rate_limiter(&self, operation_type: &str) -> Result<()> {
        if let Ok(mut rate_limiter) = self.rate_limiter.lock() {
            let now = SystemTime::now();
            
            // Update global count
            rate_limiter.global_limit.alerts_this_hour += 1;
            
            // Update operation-specific limit
            let cooldown_duration = Duration::from_secs(self.config.alert_cooldown_minutes * 60);
            let op_limit = rate_limiter.operation_limits.entry(operation_type.to_string())
                .or_insert_with(|| OperationRateLimit {
                    last_alert: SystemTime::UNIX_EPOCH,
                    alert_count: 0,
                    cooldown_until: SystemTime::UNIX_EPOCH,
                });
            
            op_limit.last_alert = now;
            op_limit.alert_count += 1;
            op_limit.cooldown_until = now + cooldown_duration;
            
            // Exponential backoff for repeated alerts
            if op_limit.alert_count > 3 {
                let backoff_multiplier = (op_limit.alert_count - 3).min(8); // Cap at 8x
                op_limit.cooldown_until = now + cooldown_duration * (2_u32.pow(backoff_multiplier as u32));
            }
        }
        
        Ok(())
    }

    /// Suppress alert and track it
    fn suppress_alert(&self, regression: &RegressionDetectionResult) -> Result<()> {
        if let Ok(mut history) = self.alert_history.lock() {
            let key = format!("{}_{:?}", regression.operation_type, regression.severity);
            
            if let Some(suppressed) = history.suppressed_alerts.get_mut(&key) {
                suppressed.last_occurrence = SystemTime::now();
                suppressed.count += 1;
            } else {
                let alert = self.create_alert_from_regression(regression)?;
                history.suppressed_alerts.insert(key, SuppressedAlert {
                    first_occurrence: SystemTime::now(),
                    last_occurrence: SystemTime::now(),
                    count: 1,
                    original_alert: alert,
                });
            }
        }
        
        Ok(())
    }

    /// Update alert history
    fn update_alert_history(&self, alert: &Alert) -> Result<()> {
        if let Ok(mut history) = self.alert_history.lock() {
            history.alerts.push_back(alert.clone());
            history.last_alert_by_operation.insert(alert.operation_type.clone(), alert.timestamp);
            
            // Keep only recent alerts
            while history.alerts.len() > 1000 {
                history.alerts.pop_front();
            }
            
            // Clean up old suppressed alerts
            let cutoff = SystemTime::now() - Duration::from_secs(24 * 3600); // 24 hours
            history.suppressed_alerts.retain(|_, suppressed| suppressed.last_occurrence > cutoff);
        }
        
        Ok(())
    }

    /// Schedule escalation for critical alerts
    fn schedule_escalation(&self, alert: &Alert) -> Result<()> {
        // This would typically integrate with a task scheduler
        // For now, we'll just log the escalation requirement
        if matches!(alert.severity, RegressionSeverity::Critical | RegressionSeverity::Major) {
            println!("⚠️  Escalation scheduled for alert: {} in 15 minutes", alert.id);
        }
        
        Ok(())
    }

    /// Create default escalation rules
    fn create_default_escalation_rules() -> Vec<EscalationRule> {
        vec![
            EscalationRule {
                severity_threshold: RegressionSeverity::Critical,
                time_to_escalate: Duration::from_secs(15 * 60), // 15 minutes
                escalation_channels: vec![AlertChannelType::PagerDuty, AlertChannelType::Email],
                max_escalation_level: 3,
            },
            EscalationRule {
                severity_threshold: RegressionSeverity::Major,
                time_to_escalate: Duration::from_secs(60 * 60), // 1 hour
                escalation_channels: vec![AlertChannelType::Email],
                max_escalation_level: 2,
            },
        ]
    }

    /// Get alert statistics
    pub fn get_alert_statistics(&self, hours: u64) -> Result<AlertStatistics> {
        if let Ok(history) = self.alert_history.lock() {
            let cutoff = SystemTime::now() - Duration::from_secs(hours * 3600);
            
            let recent_alerts: Vec<_> = history.alerts.iter()
                .filter(|alert| alert.timestamp > cutoff)
                .collect();
            
            let mut by_severity = HashMap::new();
            let mut by_operation = HashMap::new();
            
            for alert in &recent_alerts {
                *by_severity.entry(alert.severity).or_insert(0) += 1;
                *by_operation.entry(alert.operation_type.clone()).or_insert(0) += 1;
            }
            
            let suppressed_count = history.suppressed_alerts.values()
                .filter(|s| s.last_occurrence > cutoff)
                .map(|s| s.count)
                .sum();
            
            Ok(AlertStatistics {
                total_alerts: recent_alerts.len(),
                suppressed_alerts: suppressed_count,
                alerts_by_severity: by_severity,
                alerts_by_operation: by_operation,
                time_window_hours: hours,
            })
        } else {
            Ok(AlertStatistics::default())
        }
    }

    /// Test all alert channels
    pub fn test_all_channels(&self) -> HashMap<AlertChannelType, bool> {
        let mut results = HashMap::new();
        
        for channel in &self.alert_channels {
            let test_result = channel.test_connection().unwrap_or(false);
            results.insert(channel.get_channel_type(), test_result);
        }
        
        results
    }
}

/// Alert statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertStatistics {
    pub total_alerts: usize,
    pub suppressed_alerts: usize,
    pub alerts_by_severity: HashMap<RegressionSeverity, usize>,
    pub alerts_by_operation: HashMap<String, usize>,
    pub time_window_hours: u64,
}

impl Default for AlertStatistics {
    fn default() -> Self {
        Self {
            total_alerts: 0,
            suppressed_alerts: 0,
            alerts_by_severity: HashMap::new(),
            alerts_by_operation: HashMap::new(),
            time_window_hours: 0,
        }
    }
}

/// Console alert channel for testing
pub struct ConsoleAlertChannel;

impl AlertChannel for ConsoleAlertChannel {
    fn send_alert(&self, alert: &Alert) -> Result<()> {
        println!("\n🚨 PERFORMANCE REGRESSION ALERT 🚨");
        println!("ID: {}", alert.id);
        println!("Severity: {:?}", alert.severity);
        println!("Title: {}", alert.title);
        println!("Operation: {}", alert.operation_type);
        println!("Degradation: {:.1}%", alert.degradation_percentage * 100.0);
        println!("Confidence: {:.1}%", alert.statistical_confidence * 100.0);
        println!("Time: {:?}", alert.timestamp);
        
        if !alert.recommended_actions.is_empty() {
            println!("\nRecommended Actions:");
            for (i, action) in alert.recommended_actions.iter().enumerate() {
                println!("  {}. {}", i + 1, action);
            }
        }
        
        println!("{}", "=".repeat(60));
        
        Ok(())
    }

    fn test_connection(&self) -> Result<bool> {
        println!("✅ Console alert channel test successful");
        Ok(true)
    }

    fn get_channel_type(&self) -> AlertChannelType {
        AlertChannelType::Console
    }
}

/// Log-based alert channel
pub struct LogAlertChannel {
    log_level: LogLevel,
}

#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
}

impl LogAlertChannel {
    pub fn new(log_level: LogLevel) -> Self {
        Self { log_level }
    }
}

impl AlertChannel for LogAlertChannel {
    fn send_alert(&self, alert: &Alert) -> Result<()> {
        let log_msg = format!(
            "Performance regression alert: {} - {} - {:.1}% degradation (confidence: {:.1}%)",
            alert.operation_type,
            alert.title,
            alert.degradation_percentage * 100.0,
            alert.statistical_confidence * 100.0
        );

        match self.log_level {
            LogLevel::Error => eprintln!("ERROR: {}", log_msg),
            LogLevel::Warn => eprintln!("WARN: {}", log_msg),
            LogLevel::Info => println!("INFO: {}", log_msg),
        }

        Ok(())
    }

    fn test_connection(&self) -> Result<bool> {
        println!("✅ Log alert channel test successful");
        Ok(true)
    }

    fn get_channel_type(&self) -> AlertChannelType {
        AlertChannelType::Log
    }
}

/// Webhook alert channel
pub struct WebhookAlertChannel {
    webhook_url: String,
    headers: HashMap<String, String>,
}

impl WebhookAlertChannel {
    pub fn new(webhook_url: String) -> Self {
        Self {
            webhook_url,
            headers: HashMap::new(),
        }
    }

    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }
}

impl AlertChannel for WebhookAlertChannel {
    fn send_alert(&self, alert: &Alert) -> Result<()> {
        // In a real implementation, this would make an HTTP request
        println!("🔗 Webhook alert sent to: {}", self.webhook_url);
        println!("Alert payload: {}", serde_json::to_string_pretty(alert).unwrap_or_default());
        Ok(())
    }

    fn test_connection(&self) -> Result<bool> {
        // In a real implementation, this would test the webhook endpoint
        println!("✅ Webhook alert channel test successful (URL: {})", self.webhook_url);
        Ok(true)
    }

    fn get_channel_type(&self) -> AlertChannelType {
        AlertChannelType::Webhook
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_regression() -> RegressionDetectionResult {
        use super::super::{PerformanceMetric, PerformanceBaseline};
        
        let current_performance = PerformanceMetric {
            timestamp: SystemTime::now(),
            operation_type: "test_op".to_string(),
            duration_micros: 1500,
            throughput_ops_per_sec: 80.0,
            memory_usage_bytes: 2048,
            cpu_usage_percent: 7.5,
            error_rate: 0.0,
            additional_metrics: HashMap::new(),
            trace_id: Some("test-trace-123".to_string()),
            span_id: Some("test-span-456".to_string()),
        };

        let baseline_performance = PerformanceBaseline {
            operation_type: "test_op".to_string(),
            created_at: SystemTime::now() - Duration::from_secs(3600),
            sample_count: 100,
            mean_duration_micros: 1000.0,
            std_deviation_micros: 100.0,
            p50_duration_micros: 1000,
            p95_duration_micros: 1200,
            p99_duration_micros: 1300,
            mean_throughput: 100.0,
            memory_baseline: 1024,
            cpu_baseline: 5.0,
            confidence_interval: (950.0, 1050.0),
        };

        RegressionDetectionResult {
            detected: true,
            severity: RegressionSeverity::Major,
            operation_type: "test_op".to_string(),
            current_performance,
            baseline_performance,
            degradation_percentage: 0.5, // 50% degradation
            statistical_confidence: 0.95,
            recommended_actions: vec![
                "Investigate recent code changes".to_string(),
                "Check system resources".to_string(),
            ],
            detection_timestamp: SystemTime::now(),
        }
    }

    #[test]
    fn test_alert_manager_creation() {
        let config = RegressionDetectorConfig::default();
        let mut manager = AlertManager::new(config);
        
        // Add test channels
        manager.add_channel(Box::new(ConsoleAlertChannel));
        manager.add_channel(Box::new(LogAlertChannel::new(LogLevel::Warn)));
        
        assert_eq!(manager.alert_channels.len(), 2);
    }

    #[test]
    fn test_alert_creation() {
        let config = RegressionDetectorConfig::default();
        let manager = AlertManager::new(config);
        
        let regression = create_test_regression();
        let alert = manager.create_alert_from_regression(&regression).unwrap();
        
        assert_eq!(alert.operation_type, "test_op");
        assert_eq!(alert.severity, RegressionSeverity::Major);
        assert!(alert.degradation_percentage > 0.0);
        assert!(!alert.recommended_actions.is_empty());
    }

    #[test]
    fn test_channel_selection() {
        let config = RegressionDetectorConfig::default();
        let manager = AlertManager::new(config);
        
        let critical_channels = manager.select_channels_for_severity(RegressionSeverity::Critical);
        let minor_channels = manager.select_channels_for_severity(RegressionSeverity::Minor);
        
        assert!(critical_channels.len() > minor_channels.len());
        assert!(critical_channels.contains(&AlertChannelType::PagerDuty));
        assert!(!minor_channels.contains(&AlertChannelType::PagerDuty));
    }

    #[test]
    fn test_rate_limiting() {
        let mut config = RegressionDetectorConfig::default();
        config.alert_cooldown_minutes = 0; // No cooldown for test
        let manager = AlertManager::new(config);
        
        // First alert should be allowed
        let should_send = manager.should_send_alert("test_op", RegressionSeverity::Major).unwrap();
        assert!(should_send);
        
        // Update rate limiter
        manager.update_rate_limiter("test_op").unwrap();
        
        // Second alert should still be allowed (no cooldown set)
        let should_send = manager.should_send_alert("test_op", RegressionSeverity::Major).unwrap();
        assert!(should_send);
    }

    #[test]
    fn test_console_alert_channel() {
        let channel = ConsoleAlertChannel;
        let config = RegressionDetectorConfig::default();
        let manager = AlertManager::new(config);
        
        let regression = create_test_regression();
        let alert = manager.create_alert_from_regression(&regression).unwrap();
        
        // This will print to console - visual test
        assert!(channel.send_alert(&alert).is_ok());
        assert!(channel.test_connection().unwrap());
        assert_eq!(channel.get_channel_type(), AlertChannelType::Console);
    }

    #[test]
    fn test_webhook_alert_channel() {
        let channel = WebhookAlertChannel::new("https://hooks.slack.com/test".to_string())
            .with_header("Content-Type".to_string(), "application/json".to_string());
        
        let config = RegressionDetectorConfig::default();
        let manager = AlertManager::new(config);
        
        let regression = create_test_regression();
        let alert = manager.create_alert_from_regression(&regression).unwrap();
        
        assert!(channel.send_alert(&alert).is_ok());
        assert!(channel.test_connection().unwrap());
        assert_eq!(channel.get_channel_type(), AlertChannelType::Webhook);
    }
}