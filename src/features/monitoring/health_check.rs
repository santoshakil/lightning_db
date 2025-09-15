//! Production Health Check System for Lightning DB
//!
//! This module provides comprehensive health checking capabilities
//! for monitoring Lightning DB in production environments.

use crate::Database;
use crate::core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;

/// Health status levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Everything is working perfectly
    Healthy,
    /// Some issues but service is operational
    Degraded,
    /// Critical issues, service may be impacted
    Unhealthy,
    /// Service is not operational
    Critical,
}

impl HealthStatus {
    /// Convert to HTTP status code
    pub fn to_http_status(&self) -> u16 {
        match self {
            HealthStatus::Healthy => 200,
            HealthStatus::Degraded => 200, // Still return 200 for degraded
            HealthStatus::Unhealthy => 503,
            HealthStatus::Critical => 503,
        }
    }
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    /// Overall status
    pub status: HealthStatus,
    /// Timestamp of the check
    pub timestamp: SystemTime,
    /// Individual component checks
    pub components: HashMap<String, ComponentHealth>,
    /// Performance metrics
    pub metrics: HealthMetrics,
    /// Any issues found
    pub issues: Vec<HealthIssue>,
    /// Version information
    pub version: VersionInfo,
}

/// Individual component health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Component status
    pub status: HealthStatus,
    /// Last check time
    pub last_check: SystemTime,
    /// Component-specific details
    pub details: HashMap<String, serde_json::Value>,
}

/// Health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMetrics {
    /// Current operations per second
    pub ops_per_second: f64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// P99 latency in microseconds
    pub p99_latency_us: f64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Disk usage in bytes
    pub disk_usage_bytes: u64,
    /// Active connections
    pub active_connections: u64,
    /// Error rate (errors per second)
    pub error_rate: f64,
}

/// Health issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthIssue {
    /// Issue severity
    pub severity: HealthStatus,
    /// Component affected
    pub component: String,
    /// Issue description
    pub description: String,
    /// When the issue was detected
    pub detected_at: SystemTime,
    /// Suggested action
    pub action: Option<String>,
}

/// Version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    /// Lightning DB version
    pub lightning_db: String,
    /// Rust version
    pub rust: String,
    /// Build timestamp
    pub build_time: String,
    /// Git commit hash
    pub git_commit: String,
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Enable detailed checks
    pub detailed: bool,
    /// Check interval
    pub interval: Duration,
    /// Timeout for individual checks
    pub timeout: Duration,
    /// Thresholds for status determination
    pub thresholds: HealthThresholds,
}

/// Health thresholds
#[derive(Debug, Clone)]
pub struct HealthThresholds {
    /// Max latency for healthy status (microseconds)
    pub healthy_latency_us: u64,
    /// Max latency for degraded status (microseconds)
    pub degraded_latency_us: u64,
    /// Min cache hit rate for healthy status
    pub healthy_cache_hit_rate: f64,
    /// Max error rate for healthy status
    pub healthy_error_rate: f64,
    /// Max memory usage percentage
    pub max_memory_usage_percent: f64,
    /// Max disk usage percentage
    pub max_disk_usage_percent: f64,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            detailed: true,
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            thresholds: HealthThresholds::default(),
        }
    }
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            healthy_latency_us: 1000,      // 1ms
            degraded_latency_us: 10000,    // 10ms
            healthy_cache_hit_rate: 0.8,   // 80%
            healthy_error_rate: 0.01,      // 1%
            max_memory_usage_percent: 0.9, // 90%
            max_disk_usage_percent: 0.85,  // 85%
        }
    }
}

/// Health check system
pub struct HealthCheckSystem {
    db: Arc<Database>,
    config: HealthCheckConfig,
    running: Arc<AtomicBool>,
    last_result: Arc<RwLock<Option<HealthCheckResult>>>,
    check_count: Arc<AtomicU64>,
}

impl HealthCheckSystem {
    /// Create new health check system
    pub fn new(db: Arc<Database>, config: HealthCheckConfig) -> Self {
        Self {
            db,
            config,
            running: Arc::new(AtomicBool::new(false)),
            last_result: Arc::new(RwLock::new(None)),
            check_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start health check system
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(Error::Generic("Health check system already running".into()));
        }

        let running = self.running.clone();
        let db = self.db.clone();
        let config = self.config.clone();
        let last_result = self.last_result.clone();
        let check_count = self.check_count.clone();

        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                let start = Instant::now();

                // Perform health check
                let result = Self::perform_health_check(&db, &config).await;

                // Update last result
                if let Ok(result) = result {
                    let mut last = last_result.write().await;
                    *last = Some(result);
                }

                check_count.fetch_add(1, Ordering::Relaxed);

                // Wait for next interval
                let elapsed = start.elapsed();
                if elapsed < config.interval {
                    tokio::time::sleep(config.interval - elapsed).await;
                }
            }
        });

        Ok(())
    }

    /// Stop health check system
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Get latest health check result
    pub async fn get_latest_result(&self) -> Option<HealthCheckResult> {
        self.last_result.read().await.clone()
    }

    /// Perform single health check
    pub async fn check_health(&self) -> Result<HealthCheckResult> {
        Self::perform_health_check(&self.db, &self.config).await
    }

    /// Perform health check
    async fn perform_health_check(
        db: &Arc<Database>,
        config: &HealthCheckConfig,
    ) -> Result<HealthCheckResult> {
        let mut components = HashMap::new();
        let mut issues = Vec::new();
        let _start = Instant::now();

        // Check storage health
        let storage_health = Self::check_storage_health(db, config).await?;
        if storage_health.status != HealthStatus::Healthy {
            issues.push(HealthIssue {
                severity: storage_health.status,
                component: "storage".to_string(),
                description: "Storage system issues detected".to_string(),
                detected_at: SystemTime::now(),
                action: Some("Check disk space and I/O performance".to_string()),
            });
        }
        components.insert("storage".to_string(), storage_health);

        // Check cache health
        let cache_health = Self::check_cache_health(db, config).await?;
        components.insert("cache".to_string(), cache_health);

        // Check transaction health
        let transaction_health = Self::check_transaction_health(db, config).await?;
        components.insert("transactions".to_string(), transaction_health);

        // Check performance health
        let performance_health = Self::check_performance_health(db, config).await?;
        components.insert("performance".to_string(), performance_health);

        // Collect metrics
        let metrics = Self::collect_metrics(db).await?;

        // Determine overall status
        let overall_status =
            Self::determine_overall_status(&components, &metrics, &config.thresholds);

        // Version info
        let version = VersionInfo {
            lightning_db: env!("CARGO_PKG_VERSION").to_string(),
            rust: env!("RUSTC_VERSION").to_string(),
            build_time: env!("BUILD_TIME").to_string(),
            git_commit: env!("GIT_COMMIT").to_string(),
        };

        Ok(HealthCheckResult {
            status: overall_status,
            timestamp: SystemTime::now(),
            components,
            metrics,
            issues,
            version,
        })
    }

    /// Check storage health
    async fn check_storage_health(
        db: &Arc<Database>,
        config: &HealthCheckConfig,
    ) -> Result<ComponentHealth> {
        let _start = Instant::now();
        let mut details = HashMap::new();
        let mut status = HealthStatus::Healthy;

        // Check disk space
        let stats = db.get_storage_stats()?;
        let disk_usage_percent = stats.used_bytes as f64 / stats.total_bytes as f64;

        details.insert(
            "disk_usage_percent".to_string(),
            serde_json::json!(disk_usage_percent * 100.0),
        );
        details.insert(
            "free_bytes".to_string(),
            serde_json::json!(stats.total_bytes - stats.used_bytes),
        );

        if disk_usage_percent > config.thresholds.max_disk_usage_percent {
            status = HealthStatus::Unhealthy;
        } else if disk_usage_percent > config.thresholds.max_disk_usage_percent * 0.9 {
            status = HealthStatus::Degraded;
        }

        // Test write capability
        if config.detailed {
            let test_key = b"__health_check_test__";
            let test_value = b"test";

            match db.put(test_key, test_value) {
                Ok(_) => {
                    // Clean up test key
                    let _ = db.delete(test_key);
                    details.insert("write_test".to_string(), serde_json::json!("passed"));
                }
                Err(e) => {
                    status = HealthStatus::Critical;
                    details.insert(
                        "write_test".to_string(),
                        serde_json::json!(format!("failed: {}", e)),
                    );
                }
            }
        }

        Ok(ComponentHealth {
            name: "storage".to_string(),
            status,
            last_check: SystemTime::now(),
            details,
        })
    }

    /// Check cache health
    async fn check_cache_health(
        db: &Arc<Database>,
        config: &HealthCheckConfig,
    ) -> Result<ComponentHealth> {
        let mut details = HashMap::new();
        let mut status = HealthStatus::Healthy;

        use std::sync::atomic::Ordering;
        let cache_stats = db.cache_stats();
        let hits = cache_stats.hits.load(Ordering::Relaxed);
        let misses = cache_stats.misses.load(Ordering::Relaxed);
        let hit_rate = if hits + misses > 0 { hits as f64 / (hits + misses) as f64 } else { 0.0 };

        details.insert("hit_rate".to_string(), serde_json::json!(hit_rate));
        // Note: size_bytes and entry_count not available in current CacheStats
        details.insert(
            "size_bytes".to_string(),
            serde_json::json!(0), // Placeholder
        );
        details.insert(
            "entry_count".to_string(),
            serde_json::json!(0), // Placeholder
        );

        if hit_rate < config.thresholds.healthy_cache_hit_rate {
            status = HealthStatus::Degraded;
        }

        Ok(ComponentHealth {
            name: "cache".to_string(),
            status,
            last_check: SystemTime::now(),
            details,
        })
    }

    /// Check transaction health
    async fn check_transaction_health(
        db: &Arc<Database>,
        config: &HealthCheckConfig,
    ) -> Result<ComponentHealth> {
        let mut details = HashMap::new();
        let status = HealthStatus::Healthy;

        let tx_stats = db.get_transaction_stats()?;

        details.insert(
            "active_transactions".to_string(),
            serde_json::json!(tx_stats.active),
        );
        details.insert(
            "total_commits".to_string(),
            serde_json::json!(tx_stats.commits),
        );
        details.insert(
            "total_rollbacks".to_string(),
            serde_json::json!(tx_stats.rollbacks),
        );
        details.insert(
            "conflicts".to_string(),
            serde_json::json!(tx_stats.conflicts),
        );

        // Test transaction capability
        if config.detailed {
            let test_result = db.test_transaction().await;
            details.insert(
                "transaction_test".to_string(),
                serde_json::json!(if test_result.is_ok() {
                    "passed"
                } else {
                    "failed"
                }),
            );
        }

        Ok(ComponentHealth {
            name: "transactions".to_string(),
            status,
            last_check: SystemTime::now(),
            details,
        })
    }

    /// Check performance health
    async fn check_performance_health(
        db: &Arc<Database>,
        config: &HealthCheckConfig,
    ) -> Result<ComponentHealth> {
        let mut details = HashMap::new();
        let mut status = HealthStatus::Healthy;

        // Measure operation latency
        let start = Instant::now();
        let test_key = b"__perf_check__";
        db.get(test_key)?;
        let latency_us = start.elapsed().as_micros() as u64;

        details.insert("read_latency_us".to_string(), serde_json::json!(latency_us));

        if latency_us > config.thresholds.degraded_latency_us {
            status = HealthStatus::Unhealthy;
        } else if latency_us > config.thresholds.healthy_latency_us {
            status = HealthStatus::Degraded;
        }

        Ok(ComponentHealth {
            name: "performance".to_string(),
            status,
            last_check: SystemTime::now(),
            details,
        })
    }

    /// Collect metrics
    async fn collect_metrics(db: &Arc<Database>) -> Result<HealthMetrics> {
        let stats = db.get_stats()?;
        let perf_stats = db.get_performance_stats()?;

        Ok(HealthMetrics {
            ops_per_second: perf_stats.operations_per_second,
            avg_latency_us: perf_stats.average_latency_us,
            p99_latency_us: perf_stats.p99_latency_us,
            cache_hit_rate: stats.cache_hit_rate.unwrap_or(0.0),
            memory_usage_bytes: stats.memory_usage_bytes,
            disk_usage_bytes: stats.disk_usage_bytes,
            active_connections: stats.active_connections,
            error_rate: perf_stats.error_rate,
        })
    }

    /// Determine overall status
    fn determine_overall_status(
        components: &HashMap<String, ComponentHealth>,
        metrics: &HealthMetrics,
        thresholds: &HealthThresholds,
    ) -> HealthStatus {
        let mut worst_status = HealthStatus::Healthy;

        // Check component statuses
        for component in components.values() {
            match component.status {
                HealthStatus::Critical => return HealthStatus::Critical,
                HealthStatus::Unhealthy => worst_status = HealthStatus::Unhealthy,
                HealthStatus::Degraded => {
                    if worst_status == HealthStatus::Healthy {
                        worst_status = HealthStatus::Degraded;
                    }
                }
                HealthStatus::Healthy => {}
            }
        }

        // Check metrics
        if metrics.p99_latency_us > thresholds.degraded_latency_us as f64 {
            worst_status = HealthStatus::Unhealthy;
        } else if metrics.p99_latency_us > thresholds.healthy_latency_us as f64 && worst_status == HealthStatus::Healthy {
            worst_status = HealthStatus::Degraded;
        }

        if metrics.error_rate > thresholds.healthy_error_rate {
            worst_status = HealthStatus::Unhealthy;
        }

        worst_status
    }
}

/// HTTP health check endpoint handler
pub mod http {
    use super::*;
    use serde_json::json;

    /// Health check response
    #[derive(Debug, Serialize)]
    pub struct HealthResponse {
        pub status: String,
        pub checks: HashMap<String, ComponentStatus>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub details: Option<HealthCheckResult>,
    }

    /// Component status for HTTP response
    #[derive(Debug, Serialize)]
    pub struct ComponentStatus {
        pub status: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub details: Option<HashMap<String, serde_json::Value>>,
    }

    /// Convert health check result to HTTP response
    pub fn to_http_response(result: &HealthCheckResult, include_details: bool) -> HealthResponse {
        let mut checks = HashMap::new();

        for (name, component) in &result.components {
            checks.insert(
                name.clone(),
                ComponentStatus {
                    status: format!("{:?}", component.status).to_lowercase(),
                    details: if include_details {
                        Some(component.details.clone())
                    } else {
                        None
                    },
                },
            );
        }

        HealthResponse {
            status: format!("{:?}", result.status).to_lowercase(),
            checks,
            details: if include_details {
                Some(result.clone())
            } else {
                None
            },
        }
    }

    /// Liveness probe handler (simple check)
    pub async fn liveness_handler(health_system: &HealthCheckSystem) -> (u16, String) {
        // Simple check - is the service responsive?
        match health_system.get_latest_result().await {
            Some(result) if result.status != HealthStatus::Critical => {
                (200, json!({"status": "ok"}).to_string())
            }
            _ => (503, json!({"status": "error"}).to_string()),
        }
    }

    /// Readiness probe handler (detailed check)
    pub async fn readiness_handler(health_system: &HealthCheckSystem) -> (u16, String) {
        match health_system.check_health().await {
            Ok(result) => {
                let status_code = result.status.to_http_status();
                let response = to_http_response(&result, false);
                (status_code, serde_json::to_string(&response).unwrap())
            }
            Err(e) => (
                503,
                json!({"status": "error", "message": e.to_string()}).to_string(),
            ),
        }
    }

    /// Detailed health check handler
    pub async fn health_handler(
        health_system: &HealthCheckSystem,
        include_details: bool,
    ) -> (u16, String) {
        match health_system.check_health().await {
            Ok(result) => {
                let status_code = result.status.to_http_status();
                let response = to_http_response(&result, include_details);
                (
                    status_code,
                    serde_json::to_string_pretty(&response).unwrap(),
                )
            }
            Err(e) => (
                503,
                json!({"status": "error", "message": e.to_string()}).to_string(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check_system() {
        let db = Arc::new(Database::create_temp().unwrap());
        let config = HealthCheckConfig::default();
        let health_system = HealthCheckSystem::new(db, config);

        // Start health check system
        health_system.start().await.unwrap();

        // Wait for first check
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Get result
        let result = health_system.get_latest_result().await;
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(!result.components.is_empty());

        // Stop system
        health_system.stop();
    }

    #[test]
    fn test_health_status_to_http() {
        assert_eq!(HealthStatus::Healthy.to_http_status(), 200);
        assert_eq!(HealthStatus::Degraded.to_http_status(), 200);
        assert_eq!(HealthStatus::Unhealthy.to_http_status(), 503);
        assert_eq!(HealthStatus::Critical.to_http_status(), 503);
    }
}
