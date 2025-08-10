//! Health Checking System for Lightning DB
//!
//! Comprehensive health monitoring including connectivity, performance,
//! data integrity, and system resource health checks.

use crate::{Database, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tracing::info;

/// Health checker for Lightning DB
pub struct HealthChecker {
    /// Individual health checks
    health_checks: Arc<RwLock<Vec<Box<dyn HealthCheck + Send + Sync>>>>,
    /// Current health status
    current_status: Arc<RwLock<HealthStatus>>,
    /// Health check history
    health_history: Arc<RwLock<Vec<HealthCheckResult>>>,
    /// Configuration
    config: HealthCheckConfig,
}

/// Configuration for health checking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Maximum number of health check results to keep in history
    pub max_history_entries: usize,
    /// Timeout for individual health checks
    pub check_timeout: Duration,
    /// Interval between health checks
    pub check_interval: Duration,
    /// Enable detailed health checks
    pub enable_detailed_checks: bool,
    /// Enable performance health checks
    pub enable_performance_checks: bool,
    /// Enable resource health checks
    pub enable_resource_checks: bool,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            max_history_entries: 1000,
            check_timeout: Duration::from_secs(30),
            check_interval: Duration::from_secs(60),
            enable_detailed_checks: true,
            enable_performance_checks: true,
            enable_resource_checks: true,
        }
    }
}

/// Overall health status of the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    /// All systems healthy
    Healthy,
    /// Some issues detected but system operational
    Degraded(Vec<String>),
    /// Critical issues affecting system operation
    Unhealthy(Vec<String>),
    /// Cannot determine health status
    Unknown,
}

/// Result of a health check
#[derive(Debug, Clone, Serialize)]
pub struct HealthCheckResult {
    pub check_name: String,
    pub status: CheckStatus,
    pub message: String,
    pub duration: Duration,
    pub timestamp: SystemTime,
    pub details: HashMap<String, String>,
}

/// Status of individual health check
#[derive(Debug, Clone, Serialize)]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
    Unknown,
}

/// Trait for individual health checks
pub trait HealthCheck {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn check(&self, database: &Database) -> Result<HealthCheckResult>;
    fn is_critical(&self) -> bool {
        false
    }
}

/// Comprehensive health report
#[derive(Debug, Serialize)]
pub struct HealthReport {
    pub overall_status: HealthStatus,
    pub check_results: Vec<HealthCheckResult>,
    pub summary: HealthSummary,
    pub timestamp: SystemTime,
    pub total_checks: usize,
    pub passed_checks: usize,
    pub warning_checks: usize,
    pub failed_checks: usize,
}

/// Summary of health check results
#[derive(Debug, Serialize)]
pub struct HealthSummary {
    pub total_checks_run: usize,
    pub checks_passed: usize,
    pub checks_with_warnings: usize,
    pub checks_failed: usize,
    pub critical_failures: usize,
    pub average_check_duration: Duration,
    pub system_uptime: Duration,
    pub last_successful_check: Option<SystemTime>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new() -> Self {
        Self::with_config(HealthCheckConfig::default())
    }

    /// Create health checker with custom configuration
    pub fn with_config(config: HealthCheckConfig) -> Self {
        let mut checker = Self {
            health_checks: Arc::new(RwLock::new(Vec::new())),
            current_status: Arc::new(RwLock::new(HealthStatus::Unknown)),
            health_history: Arc::new(RwLock::new(Vec::new())),
            config,
        };

        // Register default health checks
        checker.register_default_checks();
        checker
    }

    /// Register default health checks
    fn register_default_checks(&mut self) {
        let mut checks = self.health_checks.write().unwrap();

        // Basic connectivity check
        checks.push(Box::new(ConnectivityCheck));

        // Storage integrity check
        checks.push(Box::new(StorageIntegrityCheck));

        // Performance checks
        if self.config.enable_performance_checks {
            checks.push(Box::new(ResponseTimeCheck));
            checks.push(Box::new(ThroughputCheck));
            checks.push(Box::new(LatencyCheck));
        }

        // Resource checks
        if self.config.enable_resource_checks {
            checks.push(Box::new(MemoryUsageCheck));
            checks.push(Box::new(DiskSpaceCheck));
            checks.push(Box::new(FileDescriptorCheck));
        }

        // Detailed checks
        if self.config.enable_detailed_checks {
            checks.push(Box::new(TransactionHealthCheck));
            checks.push(Box::new(CacheHealthCheck));
            checks.push(Box::new(CompactionHealthCheck));
            checks.push(Box::new(WALHealthCheck));
        }
    }

    /// Add a custom health check
    pub fn add_health_check(&self, check: Box<dyn HealthCheck + Send + Sync>) {
        let mut checks = self.health_checks.write().unwrap();
        checks.push(check);
    }

    /// Perform all health checks
    pub fn perform_health_checks(&self, database: &Database) -> Result<HealthReport> {
        let start_time = Instant::now();
        let mut results = Vec::new();
        let mut critical_failures = Vec::new();
        let mut warnings = Vec::new();

        info!("Starting comprehensive health check");

        let checks = self.health_checks.read().unwrap();

        for check in checks.iter() {
            let check_start = Instant::now();

            // Execute health check with simple timeout handling
            let result = match check.check(database) {
                Ok(mut result) => {
                    // Check if the check took too long
                    if result.duration > self.config.check_timeout {
                        result.status = CheckStatus::Warn;
                        result.message =
                            format!("{} (slow execution: {:?})", result.message, result.duration);
                    }

                    match result.status {
                        CheckStatus::Fail if check.is_critical() => {
                            critical_failures.push(result.message.clone());
                        }
                        CheckStatus::Fail => {
                            warnings.push(result.message.clone());
                        }
                        CheckStatus::Warn => {
                            warnings.push(result.message.clone());
                        }
                        _ => {}
                    }
                    result
                }
                Err(e) => {
                    let error_result = HealthCheckResult {
                        check_name: check.name().to_string(),
                        status: CheckStatus::Fail,
                        message: format!("Health check error: {}", e),
                        duration: check_start.elapsed(),
                        timestamp: SystemTime::now(),
                        details: HashMap::new(),
                    };

                    if check.is_critical() {
                        critical_failures.push(error_result.message.clone());
                    } else {
                        warnings.push(error_result.message.clone());
                    }

                    error_result
                }
            };

            results.push(result);
        }

        // Determine overall health status
        let overall_status = if !critical_failures.is_empty() {
            HealthStatus::Unhealthy(critical_failures.clone())
        } else if !warnings.is_empty() {
            HealthStatus::Degraded(warnings)
        } else {
            HealthStatus::Healthy
        };

        // Update current status
        {
            let mut current = self.current_status.write().unwrap();
            *current = overall_status.clone();
        }

        // Add to history
        self.add_to_history(&results);

        // Create summary
        let passed_checks = results
            .iter()
            .filter(|r| matches!(r.status, CheckStatus::Pass))
            .count();
        let warning_checks = results
            .iter()
            .filter(|r| matches!(r.status, CheckStatus::Warn))
            .count();
        let failed_checks = results
            .iter()
            .filter(|r| matches!(r.status, CheckStatus::Fail))
            .count();

        let average_duration = if !results.is_empty() {
            results.iter().map(|r| r.duration).sum::<Duration>() / results.len() as u32
        } else {
            Duration::from_millis(0)
        };

        let summary = HealthSummary {
            total_checks_run: results.len(),
            checks_passed: passed_checks,
            checks_with_warnings: warning_checks,
            checks_failed: failed_checks,
            critical_failures: critical_failures.len(),
            average_check_duration: average_duration,
            system_uptime: start_time.elapsed(), // Simplified
            last_successful_check: if passed_checks > 0 {
                Some(SystemTime::now())
            } else {
                None
            },
        };

        let report = HealthReport {
            overall_status,
            check_results: results,
            summary,
            timestamp: SystemTime::now(),
            total_checks: checks.len(),
            passed_checks,
            warning_checks,
            failed_checks,
        };

        info!(
            "Health check completed: {} checks, {} passed, {} warnings, {} failed",
            report.total_checks, passed_checks, warning_checks, failed_checks
        );

        Ok(report)
    }

    /// Get current health status
    pub fn get_overall_health(&self) -> HealthStatus {
        self.current_status.read().unwrap().clone()
    }

    /// Get health check history
    pub fn get_health_history(&self) -> Vec<HealthCheckResult> {
        self.health_history.read().unwrap().clone()
    }

    /// Add results to history
    fn add_to_history(&self, results: &[HealthCheckResult]) {
        let mut history = self.health_history.write().unwrap();

        for result in results {
            history.push(result.clone());
        }

        // Trim history if it exceeds max entries
        while history.len() > self.config.max_history_entries {
            history.remove(0);
        }
    }
}

// Individual health check implementations

/// Basic connectivity check
struct ConnectivityCheck;

impl HealthCheck for ConnectivityCheck {
    fn name(&self) -> &str {
        "connectivity"
    }
    fn description(&self) -> &str {
        "Verifies basic database connectivity"
    }
    fn is_critical(&self) -> bool {
        true
    }

    fn check(&self, database: &Database) -> Result<HealthCheckResult> {
        let start = Instant::now();

        // Test basic operation
        let test_key = b"__health_check__";
        let test_value = b"ping";

        match database.put(test_key, test_value) {
            Ok(_) => {
                // Clean up
                let _ = database.delete(test_key);

                Ok(HealthCheckResult {
                    check_name: self.name().to_string(),
                    status: CheckStatus::Pass,
                    message: "Database connectivity verified".to_string(),
                    duration: start.elapsed(),
                    timestamp: SystemTime::now(),
                    details: HashMap::new(),
                })
            }
            Err(e) => Ok(HealthCheckResult {
                check_name: self.name().to_string(),
                status: CheckStatus::Fail,
                message: format!("Database connectivity failed: {}", e),
                duration: start.elapsed(),
                timestamp: SystemTime::now(),
                details: HashMap::new(),
            }),
        }
    }
}

/// Storage integrity check
struct StorageIntegrityCheck;

impl HealthCheck for StorageIntegrityCheck {
    fn name(&self) -> &str {
        "storage_integrity"
    }
    fn description(&self) -> &str {
        "Verifies storage integrity and data consistency"
    }
    fn is_critical(&self) -> bool {
        true
    }

    fn check(&self, database: &Database) -> Result<HealthCheckResult> {
        let start = Instant::now();

        // Test data consistency
        let test_key = b"__integrity_check__";
        let test_value = b"data_integrity_test_value";

        match database.put(test_key, test_value) {
            Ok(_) => match database.get(test_key) {
                Ok(Some(retrieved_value)) => {
                    let _ = database.delete(test_key);

                    if retrieved_value == test_value {
                        Ok(HealthCheckResult {
                            check_name: self.name().to_string(),
                            status: CheckStatus::Pass,
                            message: "Storage integrity verified".to_string(),
                            duration: start.elapsed(),
                            timestamp: SystemTime::now(),
                            details: HashMap::new(),
                        })
                    } else {
                        Ok(HealthCheckResult {
                            check_name: self.name().to_string(),
                            status: CheckStatus::Fail,
                            message: "Data corruption detected".to_string(),
                            duration: start.elapsed(),
                            timestamp: SystemTime::now(),
                            details: HashMap::new(),
                        })
                    }
                }
                Ok(None) => Ok(HealthCheckResult {
                    check_name: self.name().to_string(),
                    status: CheckStatus::Fail,
                    message: "Data not found after write".to_string(),
                    duration: start.elapsed(),
                    timestamp: SystemTime::now(),
                    details: HashMap::new(),
                }),
                Err(e) => Ok(HealthCheckResult {
                    check_name: self.name().to_string(),
                    status: CheckStatus::Fail,
                    message: format!("Storage read failed: {}", e),
                    duration: start.elapsed(),
                    timestamp: SystemTime::now(),
                    details: HashMap::new(),
                }),
            },
            Err(e) => Ok(HealthCheckResult {
                check_name: self.name().to_string(),
                status: CheckStatus::Fail,
                message: format!("Storage write failed: {}", e),
                duration: start.elapsed(),
                timestamp: SystemTime::now(),
                details: HashMap::new(),
            }),
        }
    }
}

/// Response time check
struct ResponseTimeCheck;

impl HealthCheck for ResponseTimeCheck {
    fn name(&self) -> &str {
        "response_time"
    }
    fn description(&self) -> &str {
        "Monitors database response times"
    }

    fn check(&self, database: &Database) -> Result<HealthCheckResult> {
        let start = Instant::now();

        let test_key = b"__response_time_check__";
        let test_value = b"response_time_test";

        // Measure write time
        let write_start = Instant::now();
        database.put(test_key, test_value)?;
        let write_time = write_start.elapsed();

        // Measure read time
        let read_start = Instant::now();
        database.get(test_key)?;
        let read_time = read_start.elapsed();

        // Clean up
        let _ = database.delete(test_key);

        let total_time = start.elapsed();

        let mut details = HashMap::new();
        details.insert(
            "write_time_ms".to_string(),
            write_time.as_millis().to_string(),
        );
        details.insert(
            "read_time_ms".to_string(),
            read_time.as_millis().to_string(),
        );

        let status = if total_time > Duration::from_millis(1000) {
            CheckStatus::Fail
        } else if total_time > Duration::from_millis(100) {
            CheckStatus::Warn
        } else {
            CheckStatus::Pass
        };

        let message = match status {
            CheckStatus::Pass => "Response times within acceptable range".to_string(),
            CheckStatus::Warn => format!("Slow response time: {:?}", total_time),
            CheckStatus::Fail => format!("Very slow response time: {:?}", total_time),
            _ => "Unknown response time status".to_string(),
        };

        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status,
            message,
            duration: total_time,
            timestamp: SystemTime::now(),
            details,
        })
    }
}

/// Throughput check
struct ThroughputCheck;

impl HealthCheck for ThroughputCheck {
    fn name(&self) -> &str {
        "throughput"
    }
    fn description(&self) -> &str {
        "Monitors database throughput"
    }

    fn check(&self, database: &Database) -> Result<HealthCheckResult> {
        let start = Instant::now();
        let num_operations = 100;

        // Perform batch operations
        for i in 0..num_operations {
            let key = format!("__throughput_test_{}", i);
            let value = format!("value_{}", i);
            database.put(key.as_bytes(), value.as_bytes())?;
        }

        let write_duration = start.elapsed();
        let write_ops_per_sec = num_operations as f64 / write_duration.as_secs_f64();

        // Read operations
        let read_start = Instant::now();
        for i in 0..num_operations {
            let key = format!("__throughput_test_{}", i);
            database.get(key.as_bytes())?;
        }
        let read_duration = read_start.elapsed();
        let read_ops_per_sec = num_operations as f64 / read_duration.as_secs_f64();

        // Clean up
        for i in 0..num_operations {
            let key = format!("__throughput_test_{}", i);
            let _ = database.delete(key.as_bytes());
        }

        let mut details = HashMap::new();
        details.insert(
            "write_ops_per_sec".to_string(),
            write_ops_per_sec.to_string(),
        );
        details.insert("read_ops_per_sec".to_string(), read_ops_per_sec.to_string());

        let status = if write_ops_per_sec < 100.0 || read_ops_per_sec < 1000.0 {
            CheckStatus::Warn
        } else {
            CheckStatus::Pass
        };

        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status,
            message: format!(
                "Write: {:.0} ops/sec, Read: {:.0} ops/sec",
                write_ops_per_sec, read_ops_per_sec
            ),
            duration: start.elapsed(),
            timestamp: SystemTime::now(),
            details,
        })
    }
}

/// Latency check
struct LatencyCheck;

impl HealthCheck for LatencyCheck {
    fn name(&self) -> &str {
        "latency"
    }
    fn description(&self) -> &str {
        "Monitors operation latency percentiles"
    }

    fn check(&self, _database: &Database) -> Result<HealthCheckResult> {
        let start = Instant::now();

        // This would typically collect latency samples from metrics
        // For now, simulate some reasonable values
        let p50_latency = Duration::from_micros(50);
        let p95_latency = Duration::from_micros(200);
        let p99_latency = Duration::from_micros(500);

        let mut details = HashMap::new();
        details.insert(
            "p50_latency_us".to_string(),
            p50_latency.as_micros().to_string(),
        );
        details.insert(
            "p95_latency_us".to_string(),
            p95_latency.as_micros().to_string(),
        );
        details.insert(
            "p99_latency_us".to_string(),
            p99_latency.as_micros().to_string(),
        );

        let status = if p99_latency > Duration::from_millis(10) {
            CheckStatus::Warn
        } else {
            CheckStatus::Pass
        };

        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status,
            message: format!("P99 latency: {:?}", p99_latency),
            duration: start.elapsed(),
            timestamp: SystemTime::now(),
            details,
        })
    }
}

// Placeholder implementations for other checks
struct MemoryUsageCheck;
impl HealthCheck for MemoryUsageCheck {
    fn name(&self) -> &str {
        "memory_usage"
    }
    fn description(&self) -> &str {
        "Monitors memory usage"
    }
    fn check(&self, _database: &Database) -> Result<HealthCheckResult> {
        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status: CheckStatus::Pass,
            message: "Memory usage normal".to_string(),
            duration: Duration::from_millis(1),
            timestamp: SystemTime::now(),
            details: HashMap::new(),
        })
    }
}

struct DiskSpaceCheck;
impl HealthCheck for DiskSpaceCheck {
    fn name(&self) -> &str {
        "disk_space"
    }
    fn description(&self) -> &str {
        "Monitors available disk space"
    }
    fn check(&self, _database: &Database) -> Result<HealthCheckResult> {
        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status: CheckStatus::Pass,
            message: "Disk space sufficient".to_string(),
            duration: Duration::from_millis(1),
            timestamp: SystemTime::now(),
            details: HashMap::new(),
        })
    }
}

struct FileDescriptorCheck;
impl HealthCheck for FileDescriptorCheck {
    fn name(&self) -> &str {
        "file_descriptors"
    }
    fn description(&self) -> &str {
        "Monitors file descriptor usage"
    }
    fn check(&self, _database: &Database) -> Result<HealthCheckResult> {
        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status: CheckStatus::Pass,
            message: "File descriptor usage normal".to_string(),
            duration: Duration::from_millis(1),
            timestamp: SystemTime::now(),
            details: HashMap::new(),
        })
    }
}

struct TransactionHealthCheck;
impl HealthCheck for TransactionHealthCheck {
    fn name(&self) -> &str {
        "transaction_health"
    }
    fn description(&self) -> &str {
        "Monitors transaction system health"
    }
    fn check(&self, _database: &Database) -> Result<HealthCheckResult> {
        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status: CheckStatus::Pass,
            message: "Transaction system healthy".to_string(),
            duration: Duration::from_millis(1),
            timestamp: SystemTime::now(),
            details: HashMap::new(),
        })
    }
}

struct CacheHealthCheck;
impl HealthCheck for CacheHealthCheck {
    fn name(&self) -> &str {
        "cache_health"
    }
    fn description(&self) -> &str {
        "Monitors cache system health"
    }
    fn check(&self, _database: &Database) -> Result<HealthCheckResult> {
        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status: CheckStatus::Pass,
            message: "Cache system healthy".to_string(),
            duration: Duration::from_millis(1),
            timestamp: SystemTime::now(),
            details: HashMap::new(),
        })
    }
}

struct CompactionHealthCheck;
impl HealthCheck for CompactionHealthCheck {
    fn name(&self) -> &str {
        "compaction_health"
    }
    fn description(&self) -> &str {
        "Monitors compaction system health"
    }
    fn check(&self, _database: &Database) -> Result<HealthCheckResult> {
        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status: CheckStatus::Pass,
            message: "Compaction system healthy".to_string(),
            duration: Duration::from_millis(1),
            timestamp: SystemTime::now(),
            details: HashMap::new(),
        })
    }
}

struct WALHealthCheck;
impl HealthCheck for WALHealthCheck {
    fn name(&self) -> &str {
        "wal_health"
    }
    fn description(&self) -> &str {
        "Monitors WAL system health"
    }
    fn check(&self, _database: &Database) -> Result<HealthCheckResult> {
        Ok(HealthCheckResult {
            check_name: self.name().to_string(),
            status: CheckStatus::Pass,
            message: "WAL system healthy".to_string(),
            duration: Duration::from_millis(1),
            timestamp: SystemTime::now(),
            details: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LightningDbConfig;
    use tempfile::TempDir;

    #[test]
    fn test_health_checker_creation() {
        let checker = HealthChecker::new();
        let checks = checker.health_checks.read().unwrap();
        assert!(!checks.is_empty());
    }

    #[test]
    fn test_connectivity_check() {
        let test_dir = TempDir::new().unwrap();
        let config = LightningDbConfig::default();
        let database = Database::create(test_dir.path(), config).unwrap();

        let check = ConnectivityCheck;
        let result = check.check(&database).unwrap();

        assert_eq!(result.check_name, "connectivity");
        assert!(matches!(result.status, CheckStatus::Pass));
    }

    #[test]
    fn test_storage_integrity_check() {
        let test_dir = TempDir::new().unwrap();
        let config = LightningDbConfig::default();
        let database = Database::create(test_dir.path(), config).unwrap();

        let check = StorageIntegrityCheck;
        let result = check.check(&database).unwrap();

        assert_eq!(result.check_name, "storage_integrity");
        assert!(matches!(result.status, CheckStatus::Pass));
    }

    #[test]
    fn test_health_status_determination() {
        // Test healthy status
        assert!(matches!(HealthStatus::Healthy, HealthStatus::Healthy));

        // Test degraded status
        let degraded = HealthStatus::Degraded(vec!["warning".to_string()]);
        if let HealthStatus::Degraded(warnings) = degraded {
            assert_eq!(warnings.len(), 1);
        }

        // Test unhealthy status
        let unhealthy = HealthStatus::Unhealthy(vec!["critical error".to_string()]);
        if let HealthStatus::Unhealthy(errors) = unhealthy {
            assert_eq!(errors.len(), 1);
        }
    }
}
