use super::detector::DetectionResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    High,
    Critical,
    Emergency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: u64,
    pub severity: AlertSeverity,
    pub title: String,
    pub message: String,
    pub corruption_data: Option<DetectionResult>,
    pub timestamp: SystemTime,
    pub acknowledged: bool,
    pub resolved: bool,
    pub tags: Vec<String>,
}

impl Alert {
    pub fn new(severity: AlertSeverity, message: String, corruption: DetectionResult) -> Self {
        Self {
            id: 0, // Will be set by AlertManager
            severity,
            title: format!("Data Corruption Detected: {:?}", corruption.corruption_type),
            message,
            corruption_data: Some(corruption),
            timestamp: SystemTime::now(),
            acknowledged: false,
            resolved: false,
            tags: Vec::new(),
        }
    }

    pub fn info(message: String) -> Self {
        Self {
            id: 0,
            severity: AlertSeverity::Info,
            title: "Information".to_string(),
            message,
            corruption_data: None,
            timestamp: SystemTime::now(),
            acknowledged: false,
            resolved: false,
            tags: Vec::new(),
        }
    }

    pub fn warning(message: String) -> Self {
        Self {
            id: 0,
            severity: AlertSeverity::Warning,
            title: "Warning".to_string(),
            message,
            corruption_data: None,
            timestamp: SystemTime::now(),
            acknowledged: false,
            resolved: false,
            tags: Vec::new(),
        }
    }

    pub fn critical(message: String) -> Self {
        Self {
            id: 0,
            severity: AlertSeverity::Critical,
            title: "Critical Alert".to_string(),
            message,
            corruption_data: None,
            timestamp: SystemTime::now(),
            acknowledged: false,
            resolved: false,
            tags: Vec::new(),
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}

#[derive(Debug, Clone)]
pub struct AlertThresholds {
    pub checksum_errors: usize,
    pub structure_errors: usize,
    pub corruption_percentage: f64,
    pub recovery_failures: usize,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            checksum_errors: 10,
            structure_errors: 5,
            corruption_percentage: 1.0,
            recovery_failures: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub enum AlertChannel {
    Log,
    Email(String),
    Webhook(String),
    Slack(String),
    Discord(String),
    Console,
}

pub struct AlertManager {
    alerts: Arc<RwLock<HashMap<u64, Alert>>>,
    next_id: Arc<RwLock<u64>>,
    thresholds: AlertThresholds,
    channels: Vec<AlertChannel>,
    alert_sender: Option<mpsc::UnboundedSender<Alert>>,
    counters: Arc<RwLock<AlertCounters>>,
}

#[derive(Debug, Default)]
struct AlertCounters {
    checksum_errors: usize,
    structure_errors: usize,
    recovery_failures: usize,
    total_pages_scanned: usize,
}

impl AlertManager {
    pub fn new(thresholds: AlertThresholds) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Spawn background task to handle alerts
        tokio::spawn(async move {
            while let Some(alert) = receiver.recv().await {
                Self::handle_alert_background(alert).await;
            }
        });

        Self {
            alerts: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(1)),
            thresholds,
            channels: vec![AlertChannel::Console, AlertChannel::Log],
            alert_sender: Some(sender),
            counters: Arc::new(RwLock::new(AlertCounters::default())),
        }
    }

    pub fn add_channel(&mut self, channel: AlertChannel) {
        self.channels.push(channel);
    }

    pub async fn send_alert(&self, mut alert: Alert) -> crate::Result<u64> {
        // Assign ID
        let alert_id = {
            let mut next_id = self.next_id.write().await;
            let id = *next_id;
            *next_id += 1;
            id
        };
        alert.id = alert_id;

        // Update counters based on alert type
        if let Some(corruption) = &alert.corruption_data {
            let mut counters = self.counters.write().await;
            match corruption.corruption_type {
                super::detector::CorruptionType::ChecksumMismatch => {
                    counters.checksum_errors += 1;
                }
                super::detector::CorruptionType::InvalidPageHeader
                | super::detector::CorruptionType::BTreeStructureViolation => {
                    counters.structure_errors += 1;
                }
                _ => {}
            }
        }

        // Check if we need to escalate based on thresholds
        self.check_and_escalate(&alert).await?;

        // Store alert
        self.alerts.write().await.insert(alert_id, alert.clone());

        // Send to background processor
        if let Some(sender) = &self.alert_sender {
            let _ = sender.send(alert);
        }

        Ok(alert_id)
    }

    async fn check_and_escalate(&self, _alert: &Alert) -> crate::Result<()> {
        let counters = self.counters.read().await;

        // Check checksum error threshold
        if counters.checksum_errors >= self.thresholds.checksum_errors {
            let escalated_alert = Alert::critical(format!(
                "Checksum error threshold exceeded: {} errors detected",
                counters.checksum_errors
            ))
            .with_tags(vec![
                "threshold_exceeded".to_string(),
                "checksum_errors".to_string(),
            ]);

            // Send escalated alert (but don't count it to avoid recursion)
            if let Some(sender) = &self.alert_sender {
                let _ = sender.send(escalated_alert);
            }
        }

        // Check structure error threshold
        if counters.structure_errors >= self.thresholds.structure_errors {
            let escalated_alert = Alert::critical(format!(
                "Structure error threshold exceeded: {} errors detected",
                counters.structure_errors
            ))
            .with_tags(vec![
                "threshold_exceeded".to_string(),
                "structure_errors".to_string(),
            ]);

            if let Some(sender) = &self.alert_sender {
                let _ = sender.send(escalated_alert);
            }
        }

        // Check corruption percentage
        if counters.total_pages_scanned > 0 {
            let corruption_percentage = ((counters.checksum_errors + counters.structure_errors)
                as f64
                / counters.total_pages_scanned as f64)
                * 100.0;

            if corruption_percentage >= self.thresholds.corruption_percentage {
                let escalated_alert = Alert::emergency(format!(
                    "Corruption percentage threshold exceeded: {:.2}% of pages corrupted",
                    corruption_percentage
                ))
                .with_tags(vec![
                    "threshold_exceeded".to_string(),
                    "corruption_percentage".to_string(),
                ]);

                if let Some(sender) = &self.alert_sender {
                    let _ = sender.send(escalated_alert);
                }
            }
        }

        Ok(())
    }

    pub async fn acknowledge_alert(&self, alert_id: u64) -> crate::Result<()> {
        let mut alerts = self.alerts.write().await;
        if let Some(alert) = alerts.get_mut(&alert_id) {
            alert.acknowledged = true;
            Ok(())
        } else {
            Err(crate::Error::InvalidOperation {
                reason: format!("Alert {} not found", alert_id),
            })
        }
    }

    pub async fn resolve_alert(&self, alert_id: u64) -> crate::Result<()> {
        let mut alerts = self.alerts.write().await;
        if let Some(alert) = alerts.get_mut(&alert_id) {
            alert.resolved = true;
            Ok(())
        } else {
            Err(crate::Error::InvalidOperation {
                reason: format!("Alert {} not found", alert_id),
            })
        }
    }

    pub async fn get_alert(&self, alert_id: u64) -> Option<Alert> {
        self.alerts.read().await.get(&alert_id).cloned()
    }

    pub async fn list_alerts(&self) -> Vec<Alert> {
        self.alerts.read().await.values().cloned().collect()
    }

    pub async fn list_unacknowledged_alerts(&self) -> Vec<Alert> {
        self.alerts
            .read()
            .await
            .values()
            .filter(|alert| !alert.acknowledged)
            .cloned()
            .collect()
    }

    pub async fn list_unresolved_alerts(&self) -> Vec<Alert> {
        self.alerts
            .read()
            .await
            .values()
            .filter(|alert| !alert.resolved)
            .cloned()
            .collect()
    }

    pub async fn list_alerts_by_severity(&self, severity: AlertSeverity) -> Vec<Alert> {
        self.alerts
            .read()
            .await
            .values()
            .filter(|alert| alert.severity == severity)
            .cloned()
            .collect()
    }

    pub async fn clear_old_alerts(&self, max_age: std::time::Duration) -> crate::Result<usize> {
        let cutoff_time = SystemTime::now()
            .checked_sub(max_age)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let mut alerts = self.alerts.write().await;
        let initial_count = alerts.len();

        // Remove old resolved alerts
        alerts.retain(|_, alert| !alert.resolved || alert.timestamp > cutoff_time);

        Ok(initial_count - alerts.len())
    }

    pub async fn get_alert_summary(&self) -> AlertSummary {
        let alerts = self.alerts.read().await;
        let counters = self.counters.read().await;

        let mut summary = AlertSummary::default();
        summary.total_alerts = alerts.len();
        summary.checksum_errors = counters.checksum_errors;
        summary.structure_errors = counters.structure_errors;
        summary.recovery_failures = counters.recovery_failures;

        for alert in alerts.values() {
            match alert.severity {
                AlertSeverity::Info => summary.info_alerts += 1,
                AlertSeverity::Warning => summary.warning_alerts += 1,
                AlertSeverity::High => summary.high_alerts += 1,
                AlertSeverity::Critical => summary.critical_alerts += 1,
                AlertSeverity::Emergency => summary.emergency_alerts += 1,
            }

            if !alert.acknowledged {
                summary.unacknowledged_alerts += 1;
            }

            if !alert.resolved {
                summary.unresolved_alerts += 1;
            }
        }

        summary
    }

    async fn handle_alert_background(alert: Alert) {
        // Log the alert
        match alert.severity {
            AlertSeverity::Info => {
                println!("[INFO] {}: {}", alert.title, alert.message);
            }
            AlertSeverity::Warning => {
                println!("[WARNING] {}: {}", alert.title, alert.message);
            }
            AlertSeverity::High => {
                eprintln!("[HIGH] {}: {}", alert.title, alert.message);
            }
            AlertSeverity::Critical => {
                eprintln!("[CRITICAL] {}: {}", alert.title, alert.message);
            }
            AlertSeverity::Emergency => {
                eprintln!("[EMERGENCY] {}: {}", alert.title, alert.message);
            }
        }

        // Additional handling for different channels would go here
        // For example: send emails, webhook notifications, etc.
    }

    pub async fn update_scan_progress(&self, pages_scanned: usize) {
        let mut counters = self.counters.write().await;
        counters.total_pages_scanned = pages_scanned;
    }

    pub async fn record_recovery_failure(&self) {
        let mut counters = self.counters.write().await;
        counters.recovery_failures += 1;

        // Check if we need to send an alert
        if counters.recovery_failures >= self.thresholds.recovery_failures {
            let alert = Alert::critical(format!(
                "Recovery failure threshold exceeded: {} failures",
                counters.recovery_failures
            ))
            .with_tags(vec![
                "threshold_exceeded".to_string(),
                "recovery_failures".to_string(),
            ]);

            if let Some(sender) = &self.alert_sender {
                let _ = sender.send(alert);
            }
        }
    }

    pub async fn reset_counters(&self) {
        *self.counters.write().await = AlertCounters::default();
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AlertSummary {
    pub total_alerts: usize,
    pub unacknowledged_alerts: usize,
    pub unresolved_alerts: usize,
    pub info_alerts: usize,
    pub warning_alerts: usize,
    pub high_alerts: usize,
    pub critical_alerts: usize,
    pub emergency_alerts: usize,
    pub checksum_errors: usize,
    pub structure_errors: usize,
    pub recovery_failures: usize,
}

// Extension trait for Alert severity
impl AlertSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertSeverity::Info => "INFO",
            AlertSeverity::Warning => "WARNING",
            AlertSeverity::High => "HIGH",
            AlertSeverity::Critical => "CRITICAL",
            AlertSeverity::Emergency => "EMERGENCY",
        }
    }

    pub fn color_code(&self) -> &'static str {
        match self {
            AlertSeverity::Info => "\x1b[36m",      // Cyan
            AlertSeverity::Warning => "\x1b[33m",   // Yellow
            AlertSeverity::High => "\x1b[35m",      // Magenta
            AlertSeverity::Critical => "\x1b[31m",  // Red
            AlertSeverity::Emergency => "\x1b[91m", // Bright Red
        }
    }
}

impl Alert {
    pub fn emergency(message: String) -> Self {
        Self {
            id: 0,
            severity: AlertSeverity::Emergency,
            title: "Emergency Alert".to_string(),
            message,
            corruption_data: None,
            timestamp: SystemTime::now(),
            acknowledged: false,
            resolved: false,
            tags: Vec::new(),
        }
    }

    pub fn formatted_message(&self) -> String {
        format!(
            "{}[{}]\x1b[0m {}: {}",
            self.severity.color_code(),
            self.severity.as_str(),
            self.title,
            self.message
        )
    }
}
