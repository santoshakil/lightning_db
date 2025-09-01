use crate::security::{SecurityError, SecurityResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditEventType {
    Authentication,
    Authorization,
    DataAccess,
    DataModification,
    SecurityViolation,
    SystemAccess,
    ConfigurationChange,
    KeyManagement,
    BackupRestore,
    UserManagement,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: String,
    pub timestamp: u64,
    pub event_type: AuditEventType,
    pub severity: AuditSeverity,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub source_ip: Option<IpAddr>,
    pub resource: String,
    pub action: String,
    pub result: AuditResult,
    pub details: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditResult {
    Success,
    Failure,
    Error,
    Blocked,
}

#[derive(Debug)]
pub struct AuditLogger {
    sender: Sender<AuditEvent>,
    config: AuditConfig,
}

#[derive(Debug, Clone)]
pub struct AuditConfig {
    pub enabled: bool,
    pub log_file_path: String,
    pub max_file_size: u64,
    pub retention_days: u32,
    pub buffer_size: usize,
    pub sync_interval_seconds: u64,
    pub encrypt_logs: bool,
    pub minimum_severity: AuditSeverity,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_file_path: "./audit.log".to_string(),
            max_file_size: 100 * 1024 * 1024, // 100MB
            retention_days: 365,
            buffer_size: 1000,
            sync_interval_seconds: 30,
            encrypt_logs: true,
            minimum_severity: AuditSeverity::Low,
        }
    }
}

impl AuditLogger {
    pub fn new(config: AuditConfig) -> SecurityResult<Self> {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        
        if config.enabled {
            Self::start_background_writer(receiver, config.clone())?;
        }

        Ok(Self { sender, config })
    }

    pub async fn log_event(&self, event: AuditEvent) -> SecurityResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if !self.should_log_severity(&event.severity) {
            return Ok(());
        }

        self.sender.send(event).await
            .map_err(|e| SecurityError::AuditFailure(format!("Failed to queue audit event: {}", e)))?;

        Ok(())
    }

    pub async fn log_authentication(&self, user_id: Option<String>, session_id: Option<String>, 
                                   source_ip: Option<IpAddr>, success: bool, details: HashMap<String, String>) -> SecurityResult<()> {
        let event = AuditEvent {
            event_id: self.generate_event_id(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            event_type: AuditEventType::Authentication,
            severity: if success { AuditSeverity::Low } else { AuditSeverity::Medium },
            user_id,
            session_id,
            source_ip,
            resource: "authentication".to_string(),
            action: "login".to_string(),
            result: if success { AuditResult::Success } else { AuditResult::Failure },
            details,
            metadata: HashMap::new(),
        };

        self.log_event(event).await
    }

    pub async fn log_authorization(&self, user_id: Option<String>, session_id: Option<String>,
                                  resource: String, action: String, allowed: bool, 
                                  details: HashMap<String, String>) -> SecurityResult<()> {
        let event = AuditEvent {
            event_id: self.generate_event_id(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            event_type: AuditEventType::Authorization,
            severity: if allowed { AuditSeverity::Low } else { AuditSeverity::Medium },
            user_id,
            session_id,
            source_ip: None,
            resource,
            action,
            result: if allowed { AuditResult::Success } else { AuditResult::Failure },
            details,
            metadata: HashMap::new(),
        };

        self.log_event(event).await
    }

    pub async fn log_data_access(&self, user_id: Option<String>, session_id: Option<String>,
                                key: String, success: bool, details: HashMap<String, String>) -> SecurityResult<()> {
        let event = AuditEvent {
            event_id: self.generate_event_id(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            event_type: AuditEventType::DataAccess,
            severity: AuditSeverity::Low,
            user_id,
            session_id,
            source_ip: None,
            resource: key,
            action: "read".to_string(),
            result: if success { AuditResult::Success } else { AuditResult::Failure },
            details,
            metadata: HashMap::new(),
        };

        self.log_event(event).await
    }

    pub async fn log_data_modification(&self, user_id: Option<String>, session_id: Option<String>,
                                     key: String, action: String, success: bool, 
                                     details: HashMap<String, String>) -> SecurityResult<()> {
        let event = AuditEvent {
            event_id: self.generate_event_id(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            event_type: AuditEventType::DataModification,
            severity: AuditSeverity::Medium,
            user_id,
            session_id,
            source_ip: None,
            resource: key,
            action,
            result: if success { AuditResult::Success } else { AuditResult::Failure },
            details,
            metadata: HashMap::new(),
        };

        self.log_event(event).await
    }

    pub async fn log_security_violation(&self, user_id: Option<String>, session_id: Option<String>,
                                       source_ip: Option<IpAddr>, violation_type: String,
                                       details: HashMap<String, String>) -> SecurityResult<()> {
        let event = AuditEvent {
            event_id: self.generate_event_id(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            event_type: AuditEventType::SecurityViolation,
            severity: AuditSeverity::High,
            user_id,
            session_id,
            source_ip,
            resource: "security".to_string(),
            action: violation_type,
            result: AuditResult::Blocked,
            details,
            metadata: HashMap::new(),
        };

        self.log_event(event).await
    }

    pub async fn log_system_access(&self, user_id: Option<String>, session_id: Option<String>,
                                  source_ip: Option<IpAddr>, system_component: String,
                                  action: String, success: bool, details: HashMap<String, String>) -> SecurityResult<()> {
        let event = AuditEvent {
            event_id: self.generate_event_id(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            event_type: AuditEventType::SystemAccess,
            severity: AuditSeverity::Medium,
            user_id,
            session_id,
            source_ip,
            resource: system_component,
            action,
            result: if success { AuditResult::Success } else { AuditResult::Failure },
            details,
            metadata: HashMap::new(),
        };

        self.log_event(event).await
    }

    pub async fn log_key_management(&self, user_id: Option<String>, key_id: String,
                                   action: String, success: bool, details: HashMap<String, String>) -> SecurityResult<()> {
        let event = AuditEvent {
            event_id: self.generate_event_id(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            event_type: AuditEventType::KeyManagement,
            severity: AuditSeverity::High,
            user_id,
            session_id: None,
            source_ip: None,
            resource: key_id,
            action,
            result: if success { AuditResult::Success } else { AuditResult::Failure },
            details,
            metadata: HashMap::new(),
        };

        self.log_event(event).await
    }

    fn should_log_severity(&self, severity: &AuditSeverity) -> bool {
        let severity_level = match severity {
            AuditSeverity::Low => 0,
            AuditSeverity::Medium => 1,
            AuditSeverity::High => 2,
            AuditSeverity::Critical => 3,
        };

        let min_level = match self.config.minimum_severity {
            AuditSeverity::Low => 0,
            AuditSeverity::Medium => 1,
            AuditSeverity::High => 2,
            AuditSeverity::Critical => 3,
        };

        severity_level >= min_level
    }

    fn generate_event_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn start_background_writer(mut receiver: Receiver<AuditEvent>, config: AuditConfig) -> SecurityResult<()> {
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut last_sync = std::time::Instant::now();

            while let Some(event) = receiver.recv().await {
                buffer.push(event);

                if buffer.len() >= config.buffer_size || 
                   last_sync.elapsed().as_secs() >= config.sync_interval_seconds {
                    if let Err(e) = Self::flush_buffer(&mut buffer, &config).await {
                        error!("Failed to flush audit buffer: {}", e);
                    }
                    last_sync = std::time::Instant::now();
                }
            }

            if !buffer.is_empty() {
                if let Err(e) = Self::flush_buffer(&mut buffer, &config).await {
                    error!("Failed to flush final audit buffer: {}", e);
                }
            }
        });

        Ok(())
    }

    async fn flush_buffer(buffer: &mut Vec<AuditEvent>, config: &AuditConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if buffer.is_empty() {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.log_file_path)
            .await?;

        for event in buffer.drain(..) {
            let log_line = serde_json::to_string(&event)?;
            file.write_all(format!("{}\n", log_line).as_bytes()).await?;
        }

        file.flush().await?;
        Ok(())
    }
}

pub struct SecurityMetrics {
    authentication_attempts: Arc<Mutex<u64>>,
    authentication_failures: Arc<Mutex<u64>>,
    authorization_failures: Arc<Mutex<u64>>,
    security_violations: Arc<Mutex<u64>>,
    blocked_ips: Arc<Mutex<u64>>,
    rate_limit_violations: Arc<Mutex<u64>>,
}

impl Default for SecurityMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SecurityMetrics {
    pub fn new() -> Self {
        Self {
            authentication_attempts: Arc::new(Mutex::new(0)),
            authentication_failures: Arc::new(Mutex::new(0)),
            authorization_failures: Arc::new(Mutex::new(0)),
            security_violations: Arc::new(Mutex::new(0)),
            blocked_ips: Arc::new(Mutex::new(0)),
            rate_limit_violations: Arc::new(Mutex::new(0)),
        }
    }

    pub fn increment_authentication_attempts(&self) {
        *self.authentication_attempts.lock().unwrap() += 1;
    }

    pub fn increment_authentication_failures(&self) {
        *self.authentication_failures.lock().unwrap() += 1;
    }

    pub fn increment_authorization_failures(&self) {
        *self.authorization_failures.lock().unwrap() += 1;
    }

    pub fn increment_security_violations(&self) {
        *self.security_violations.lock().unwrap() += 1;
    }

    pub fn increment_blocked_ips(&self) {
        *self.blocked_ips.lock().unwrap() += 1;
    }

    pub fn increment_rate_limit_violations(&self) {
        *self.rate_limit_violations.lock().unwrap() += 1;
    }

    pub fn get_metrics(&self) -> SecurityMetricsSnapshot {
        SecurityMetricsSnapshot {
            authentication_attempts: *self.authentication_attempts.lock().unwrap(),
            authentication_failures: *self.authentication_failures.lock().unwrap(),
            authorization_failures: *self.authorization_failures.lock().unwrap(),
            security_violations: *self.security_violations.lock().unwrap(),
            blocked_ips: *self.blocked_ips.lock().unwrap(),
            rate_limit_violations: *self.rate_limit_violations.lock().unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SecurityMetricsSnapshot {
    pub authentication_attempts: u64,
    pub authentication_failures: u64,
    pub authorization_failures: u64,
    pub security_violations: u64,
    pub blocked_ips: u64,
    pub rate_limit_violations: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio;

    #[tokio::test]
    async fn test_audit_logger_creation() {
        let config = AuditConfig::default();
        let logger = AuditLogger::new(config);
        assert!(logger.is_ok());
    }

    #[tokio::test]
    async fn test_log_authentication() {
        let config = AuditConfig {
            enabled: false,
            ..AuditConfig::default()
        };
        let logger = AuditLogger::new(config).unwrap();
        
        let result = logger.log_authentication(
            Some("user123".to_string()),
            Some("session456".to_string()),
            None,
            true,
            HashMap::new()
        ).await;
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_security_metrics() {
        let metrics = SecurityMetrics::new();
        
        metrics.increment_authentication_attempts();
        metrics.increment_authentication_failures();
        
        let snapshot = metrics.get_metrics();
        assert_eq!(snapshot.authentication_attempts, 1);
        assert_eq!(snapshot.authentication_failures, 1);
    }

    #[test]
    fn test_severity_filtering() {
        let config = AuditConfig {
            minimum_severity: AuditSeverity::Medium,
            ..AuditConfig::default()
        };
        let logger = AuditLogger::new(config).unwrap();
        
        assert!(!logger.should_log_severity(&AuditSeverity::Low));
        assert!(logger.should_log_severity(&AuditSeverity::Medium));
        assert!(logger.should_log_severity(&AuditSeverity::High));
        assert!(logger.should_log_severity(&AuditSeverity::Critical));
    }
}
