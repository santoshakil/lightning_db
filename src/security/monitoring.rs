use crate::security::{SecurityError, SecurityResult};
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct SecurityEvent {
    pub timestamp: Instant,
    pub event_type: SecurityEventType,
    pub source_ip: Option<IpAddr>,
    pub user_id: Option<String>,
    pub severity: EventSeverity,
    pub details: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum SecurityEventType {
    FailedAuthentication,
    BruteForceDetected,
    UnauthorizedAccess,
    AnomalousActivity,
    SuspiciousPattern,
    RateLimitExceeded,
    MaliciousPayload,
    DataExfiltration,
    PrivilegeEscalation,
    SystemIntrusion,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
pub struct SecurityAlert {
    pub id: String,
    pub timestamp: SystemTime,
    pub alert_type: AlertType,
    pub severity: EventSeverity,
    pub description: String,
    pub affected_resource: String,
    pub source_ip: Option<IpAddr>,
    pub user_id: Option<String>,
    pub recommended_actions: Vec<String>,
    pub acknowledged: bool,
}

#[derive(Debug, Clone)]
pub enum AlertType {
    BruteForceAttack,
    AnomalousTraffic,
    DataBreachAttempt,
    SystemCompromise,
    PolicyViolation,
    ResourceExhaustion,
    IntrusionDetected,
}

pub struct SecurityMonitor {
    event_sender: Sender<SecurityEvent>,
    alert_sender: Sender<SecurityAlert>,
    brute_force_detector: Arc<RwLock<BruteForceDetector>>,
    anomaly_detector: Arc<RwLock<AnomalyDetector>>,
    intrusion_detector: Arc<RwLock<IntrusionDetector>>,
    active_alerts: Arc<RwLock<HashMap<String, SecurityAlert>>>,
    config: MonitoringConfig,
}

#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    pub enabled: bool,
    pub brute_force_threshold: usize,
    pub brute_force_window: Duration,
    pub anomaly_detection_enabled: bool,
    pub intrusion_detection_enabled: bool,
    pub alert_retention_hours: u64,
    pub max_alerts: usize,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            brute_force_threshold: 5,
            brute_force_window: Duration::from_secs(300),
            anomaly_detection_enabled: true,
            intrusion_detection_enabled: true,
            alert_retention_hours: 24,
            max_alerts: 1000,
        }
    }
}

struct BruteForceDetector {
    failed_attempts: HashMap<IpAddr, VecDeque<Instant>>,
    threshold: usize,
    window: Duration,
}

struct AnomalyDetector {
    baselines: HashMap<String, TrafficBaseline>,
    detection_window: Duration,
    sensitivity: f64,
}

struct TrafficBaseline {
    normal_request_rate: f64,
    normal_data_size: f64,
    last_updated: Instant,
    samples: VecDeque<f64>,
}

struct IntrusionDetector {
    suspicious_patterns: Vec<SuspiciousPattern>,
    payload_scanners: Vec<PayloadScanner>,
}

struct SuspiciousPattern {
    pattern: String,
    severity: EventSeverity,
    description: String,
}

struct PayloadScanner {
    name: String,
    signatures: Vec<String>,
    severity: EventSeverity,
}

impl SecurityMonitor {
    pub fn new(config: MonitoringConfig) -> SecurityResult<Self> {
        let (event_sender, event_receiver) = mpsc::channel(1000);
        let (alert_sender, alert_receiver) = mpsc::channel(100);

        let brute_force_detector = BruteForceDetector {
            failed_attempts: HashMap::new(),
            threshold: config.brute_force_threshold,
            window: config.brute_force_window,
        };

        let anomaly_detector = AnomalyDetector {
            baselines: HashMap::new(),
            detection_window: Duration::from_secs(300),
            sensitivity: 2.0,
        };

        let intrusion_detector = IntrusionDetector {
            suspicious_patterns: Self::create_suspicious_patterns(),
            payload_scanners: Self::create_payload_scanners(),
        };

        let monitor = Self {
            event_sender,
            alert_sender,
            brute_force_detector: Arc::new(RwLock::new(brute_force_detector)),
            anomaly_detector: Arc::new(RwLock::new(anomaly_detector)),
            intrusion_detector: Arc::new(RwLock::new(intrusion_detector)),
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
            config: config.clone(),
        };

        if config.enabled {
            Self::start_event_processor(event_receiver, monitor.clone())?;
            Self::start_alert_processor(alert_receiver)?;
        }

        Ok(monitor)
    }

    pub async fn report_event(&self, event: SecurityEvent) -> SecurityResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.event_sender.send(event).await.map_err(|e| {
            SecurityError::PolicyViolation(format!("Failed to report security event: {}", e))
        })?;

        Ok(())
    }

    pub async fn report_failed_authentication(
        &self,
        ip: Option<IpAddr>,
        user_id: Option<String>,
    ) -> SecurityResult<()> {
        let event = SecurityEvent {
            timestamp: Instant::now(),
            event_type: SecurityEventType::FailedAuthentication,
            source_ip: ip,
            user_id,
            severity: EventSeverity::Medium,
            details: HashMap::new(),
        };

        self.report_event(event).await
    }

    pub async fn report_unauthorized_access(
        &self,
        ip: Option<IpAddr>,
        user_id: Option<String>,
        resource: String,
    ) -> SecurityResult<()> {
        let mut details = HashMap::new();
        details.insert("resource".to_string(), resource);

        let event = SecurityEvent {
            timestamp: Instant::now(),
            event_type: SecurityEventType::UnauthorizedAccess,
            source_ip: ip,
            user_id,
            severity: EventSeverity::High,
            details,
        };

        self.report_event(event).await
    }

    pub async fn report_rate_limit_exceeded(
        &self,
        ip: Option<IpAddr>,
        user_id: Option<String>,
    ) -> SecurityResult<()> {
        let event = SecurityEvent {
            timestamp: Instant::now(),
            event_type: SecurityEventType::RateLimitExceeded,
            source_ip: ip,
            user_id,
            severity: EventSeverity::Medium,
            details: HashMap::new(),
        };

        self.report_event(event).await
    }

    pub async fn scan_payload(
        &self,
        payload: &str,
        context: &str,
    ) -> SecurityResult<Vec<SecurityAlert>> {
        if !self.config.intrusion_detection_enabled {
            return Ok(Vec::new());
        }

        let detector = self.intrusion_detector.read().unwrap();
        let mut alerts = Vec::new();

        for scanner in &detector.payload_scanners {
            for signature in &scanner.signatures {
                if payload.contains(signature) {
                    let alert = SecurityAlert {
                        id: uuid::Uuid::new_v4().to_string(),
                        timestamp: SystemTime::now(),
                        alert_type: AlertType::IntrusionDetected,
                        severity: scanner.severity.clone(),
                        description: format!(
                            "Malicious payload detected: {} in {}",
                            signature, context
                        ),
                        affected_resource: context.to_string(),
                        source_ip: None,
                        user_id: None,
                        recommended_actions: vec![
                            "Block the source IP".to_string(),
                            "Investigate the payload source".to_string(),
                            "Review security logs".to_string(),
                        ],
                        acknowledged: false,
                    };
                    alerts.push(alert);
                }
            }
        }

        Ok(alerts)
    }

    pub fn get_active_alerts(&self) -> Vec<SecurityAlert> {
        let alerts = self.active_alerts.read().unwrap();
        alerts.values().cloned().collect()
    }

    pub fn acknowledge_alert(&self, alert_id: &str) -> SecurityResult<()> {
        let mut alerts = self.active_alerts.write().unwrap();
        if let Some(alert) = alerts.get_mut(alert_id) {
            alert.acknowledged = true;
            Ok(())
        } else {
            Err(SecurityError::PolicyViolation(
                "Alert not found".to_string(),
            ))
        }
    }

    pub fn get_security_metrics(&self) -> SecurityMetrics {
        let alerts = self.active_alerts.read().unwrap();
        let mut metrics = SecurityMetrics {
            total_alerts: alerts.len(),
            critical_alerts: 0,
            high_alerts: 0,
            medium_alerts: 0,
            low_alerts: 0,
            acknowledged_alerts: 0,
            brute_force_attempts: 0,
            anomalous_activities: 0,
            intrusion_attempts: 0,
        };

        for alert in alerts.values() {
            match alert.severity {
                EventSeverity::Critical => metrics.critical_alerts += 1,
                EventSeverity::High => metrics.high_alerts += 1,
                EventSeverity::Medium => metrics.medium_alerts += 1,
                EventSeverity::Low => metrics.low_alerts += 1,
            }

            if alert.acknowledged {
                metrics.acknowledged_alerts += 1;
            }

            match alert.alert_type {
                AlertType::BruteForceAttack => metrics.brute_force_attempts += 1,
                AlertType::AnomalousTraffic => metrics.anomalous_activities += 1,
                AlertType::IntrusionDetected => metrics.intrusion_attempts += 1,
                _ => {}
            }
        }

        metrics
    }

    fn start_event_processor(
        mut receiver: Receiver<SecurityEvent>,
        monitor: SecurityMonitor,
    ) -> SecurityResult<()> {
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                if let Err(e) = monitor.process_security_event(event).await {
                    error!("Failed to process security event: {}", e);
                }
            }
        });
        Ok(())
    }

    fn start_alert_processor(mut receiver: Receiver<SecurityAlert>) -> SecurityResult<()> {
        tokio::spawn(async move {
            while let Some(alert) = receiver.recv().await {
                match alert.severity {
                    EventSeverity::Critical => {
                        error!("CRITICAL SECURITY ALERT: {}", alert.description)
                    }
                    EventSeverity::High => warn!("HIGH SECURITY ALERT: {}", alert.description),
                    EventSeverity::Medium => warn!("MEDIUM SECURITY ALERT: {}", alert.description),
                    EventSeverity::Low => info!("LOW SECURITY ALERT: {}", alert.description),
                }
            }
        });
        Ok(())
    }

    async fn process_security_event(&self, event: SecurityEvent) -> SecurityResult<()> {
        match event.event_type {
            SecurityEventType::FailedAuthentication => {
                if let Some(ip) = event.source_ip {
                    self.check_brute_force(ip).await?;
                }
            }
            SecurityEventType::AnomalousActivity => {
                if self.config.anomaly_detection_enabled {
                    self.analyze_anomaly(&event).await?;
                }
            }
            SecurityEventType::MaliciousPayload => {
                self.handle_malicious_payload(&event).await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn check_brute_force(&self, ip: IpAddr) -> SecurityResult<()> {
        let should_alert = {
            let mut detector = self.brute_force_detector.write().unwrap();
            let now = Instant::now();
            let window = detector.window;
            let threshold = detector.threshold;

            let attempts = detector.failed_attempts.entry(ip).or_default();
            attempts.push_back(now);

            attempts.retain(|&time| now.duration_since(time) <= window);

            attempts.len() >= threshold
        };

        if should_alert {
            let alert = SecurityAlert {
                id: uuid::Uuid::new_v4().to_string(),
                timestamp: SystemTime::now(),
                alert_type: AlertType::BruteForceAttack,
                severity: EventSeverity::High,
                description: format!("Brute force attack detected from IP: {}", ip),
                affected_resource: "authentication".to_string(),
                source_ip: Some(ip),
                user_id: None,
                recommended_actions: vec![
                    "Block the source IP immediately".to_string(),
                    "Investigate the attack pattern".to_string(),
                    "Review authentication logs".to_string(),
                ],
                acknowledged: false,
            };

            self.add_alert(alert).await?;
        }

        Ok(())
    }

    async fn analyze_anomaly(&self, _event: &SecurityEvent) -> SecurityResult<()> {
        Ok(())
    }

    async fn handle_malicious_payload(&self, event: &SecurityEvent) -> SecurityResult<()> {
        let alert = SecurityAlert {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: SystemTime::now(),
            alert_type: AlertType::IntrusionDetected,
            severity: event.severity.clone(),
            description: "Malicious payload detected in request".to_string(),
            affected_resource: "payload_scanner".to_string(),
            source_ip: event.source_ip,
            user_id: event.user_id.clone(),
            recommended_actions: vec![
                "Block the source".to_string(),
                "Analyze the payload".to_string(),
                "Check for similar patterns".to_string(),
            ],
            acknowledged: false,
        };

        self.add_alert(alert).await
    }

    async fn add_alert(&self, alert: SecurityAlert) -> SecurityResult<()> {
        let alert_id = alert.id.clone();

        {
            let mut alerts = self.active_alerts.write().unwrap();
            alerts.insert(alert_id, alert.clone());

            if alerts.len() > self.config.max_alerts {
                let oldest_id = alerts.keys().next().cloned();
                if let Some(id) = oldest_id {
                    alerts.remove(&id);
                }
            }
        }

        self.alert_sender
            .send(alert)
            .await
            .map_err(|e| SecurityError::PolicyViolation(format!("Failed to send alert: {}", e)))?;

        Ok(())
    }

    fn create_suspicious_patterns() -> Vec<SuspiciousPattern> {
        vec![
            SuspiciousPattern {
                pattern: "'; DROP TABLE".to_string(),
                severity: EventSeverity::Critical,
                description: "SQL injection attempt".to_string(),
            },
            SuspiciousPattern {
                pattern: "<script>".to_string(),
                severity: EventSeverity::High,
                description: "XSS attempt".to_string(),
            },
            SuspiciousPattern {
                pattern: "../".to_string(),
                severity: EventSeverity::Medium,
                description: "Path traversal attempt".to_string(),
            },
        ]
    }

    fn create_payload_scanners() -> Vec<PayloadScanner> {
        vec![
            PayloadScanner {
                name: "SQL Injection Scanner".to_string(),
                signatures: vec![
                    "' OR '1'='1".to_string(),
                    "UNION SELECT".to_string(),
                    "'; DROP TABLE".to_string(),
                    "'; DELETE FROM".to_string(),
                ],
                severity: EventSeverity::Critical,
            },
            PayloadScanner {
                name: "Command Injection Scanner".to_string(),
                signatures: vec![
                    "; rm -rf".to_string(),
                    "|whoami".to_string(),
                    "&& cat /etc/passwd".to_string(),
                    "`id`".to_string(),
                ],
                severity: EventSeverity::Critical,
            },
            PayloadScanner {
                name: "XSS Scanner".to_string(),
                signatures: vec![
                    "<script>alert(".to_string(),
                    "javascript:".to_string(),
                    "onerror=".to_string(),
                    "onload=".to_string(),
                ],
                severity: EventSeverity::High,
            },
        ]
    }
}

#[derive(Debug)]
pub struct SecurityMetrics {
    pub total_alerts: usize,
    pub critical_alerts: usize,
    pub high_alerts: usize,
    pub medium_alerts: usize,
    pub low_alerts: usize,
    pub acknowledged_alerts: usize,
    pub brute_force_attempts: usize,
    pub anomalous_activities: usize,
    pub intrusion_attempts: usize,
}

impl Clone for SecurityMonitor {
    fn clone(&self) -> Self {
        Self {
            event_sender: self.event_sender.clone(),
            alert_sender: self.alert_sender.clone(),
            brute_force_detector: Arc::clone(&self.brute_force_detector),
            anomaly_detector: Arc::clone(&self.anomaly_detector),
            intrusion_detector: Arc::clone(&self.intrusion_detector),
            active_alerts: Arc::clone(&self.active_alerts),
            config: self.config.clone(),
        }
    }
}
