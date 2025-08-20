use crate::core::error::{Error, Result};
use crate::core::recovery::*;
use crate::{Database, LightningDbConfig};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Comprehensive recovery manager that coordinates all recovery operations
/// 
/// This is the main entry point for production-ready database recovery operations.
/// It integrates crash recovery, transaction recovery, I/O recovery, 
/// memory recovery, and corruption recovery into a unified system.
pub struct ComprehensiveRecoveryManager {
    db_path: String,
    config: LightningDbConfig,
    progress: Arc<RecoveryProgress>,
    
    // Individual recovery managers
    crash_recovery_manager: Option<CrashRecoveryManager>,
    io_recovery_manager: Arc<IoRecoveryManager>,
    memory_recovery_manager: Arc<MemoryRecoveryManager>,
    transaction_recovery_manager: Arc<TransactionRecoveryManager>,
    corruption_recovery_manager: Arc<CorruptionRecoveryManager>,
    
    // Recovery state
    recovery_config: RecoveryConfiguration,
    health_monitor: Arc<RwLock<RecoveryHealthMonitor>>,
    shutdown_tx: Arc<RwLock<Option<oneshot::Sender<()>>>>,
    monitoring_task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

#[derive(Debug, Clone)]
pub struct RecoveryConfiguration {
    pub enable_crash_recovery: bool,
    pub enable_io_recovery: bool,
    pub enable_memory_recovery: bool,
    pub enable_transaction_recovery: bool,
    pub enable_corruption_recovery: bool,
    pub auto_recovery: bool,
    pub recovery_timeout: Duration,
    pub health_check_interval: Duration,
}

impl Default for RecoveryConfiguration {
    fn default() -> Self {
        Self {
            enable_crash_recovery: true,
            enable_io_recovery: true,
            enable_memory_recovery: true,
            enable_transaction_recovery: true,
            enable_corruption_recovery: true,
            auto_recovery: true,
            recovery_timeout: Duration::from_secs(600),
            health_check_interval: Duration::from_secs(30),
        }
    }
}

impl ComprehensiveRecoveryManager {
    pub fn new(db_path: impl AsRef<Path>, config: LightningDbConfig) -> Result<Self> {
        let db_path_ref = db_path.as_ref();
        let recovery_config = RecoveryConfiguration::default();
        
        // Initialize individual recovery managers
        let io_recovery_manager = Arc::new(IoRecoveryManager::new(IoRecoveryConfig::default()));
        let memory_recovery_manager = Arc::new(MemoryRecoveryManager::new(MemoryRecoveryConfig::default()));
        let transaction_recovery_manager = Arc::new(TransactionRecoveryManager::new(TransactionRecoveryConfig::default()));
        let corruption_recovery_manager = Arc::new(CorruptionRecoveryManager::new(
            CorruptionRecoveryConfig::default(),
            db_path_ref,
        )?);

        // Initialize crash recovery if enabled
        let crash_recovery_manager = if recovery_config.enable_crash_recovery {
            Some(CrashRecoveryManager::new(
                db_path_ref,
                config.clone(),
                Some(recovery_config.recovery_timeout),
            )?)
        } else {
            None
        };

        let health_monitor = Arc::new(RwLock::new(RecoveryHealthMonitor::new()));

        Ok(Self {
            db_path: db_path_ref.to_string_lossy().to_string(),
            config,
            progress: Arc::new(RecoveryProgress::new()),
            crash_recovery_manager,
            io_recovery_manager,
            memory_recovery_manager,
            transaction_recovery_manager,
            corruption_recovery_manager,
            recovery_config,
            health_monitor,
            shutdown_tx: Arc::new(RwLock::new(None)),
            monitoring_task: Arc::new(RwLock::new(None)),
        })
    }

    /// Create a new recovery manager with custom configuration
    pub fn with_config(
        db_path: impl AsRef<Path>, 
        config: LightningDbConfig,
        recovery_config: RecoveryConfiguration,
    ) -> Result<Self> {
        let db_path_ref = db_path.as_ref();
        
        // Initialize individual recovery managers based on configuration
        let io_recovery_manager = if recovery_config.enable_io_recovery {
            Arc::new(IoRecoveryManager::new(IoRecoveryConfig::default()))
        } else {
            Arc::new(IoRecoveryManager::new(IoRecoveryConfig {
                checksum_validation: false,
                auto_repair: false,
                fallback_enabled: false,
                ..Default::default()
            }))
        };

        let memory_recovery_manager = if recovery_config.enable_memory_recovery {
            Arc::new(MemoryRecoveryManager::new(MemoryRecoveryConfig::default()))
        } else {
            Arc::new(MemoryRecoveryManager::new(MemoryRecoveryConfig {
                oom_recovery_enabled: false,
                memory_leak_detection: false,
                ..Default::default()
            }))
        };

        let transaction_recovery_manager = if recovery_config.enable_transaction_recovery {
            Arc::new(TransactionRecoveryManager::new(TransactionRecoveryConfig::default()))
        } else {
            Arc::new(TransactionRecoveryManager::new(TransactionRecoveryConfig {
                deadlock_detection_enabled: false,
                auto_rollback_incomplete: false,
                ..Default::default()
            }))
        };

        let corruption_recovery_manager = if recovery_config.enable_corruption_recovery {
            Arc::new(CorruptionRecoveryManager::new(
                CorruptionRecoveryConfig::default(),
                db_path_ref,
            )?)
        } else {
            Arc::new(CorruptionRecoveryManager::new(
                CorruptionRecoveryConfig {
                    auto_repair_enabled: false,
                    data_validation_level: ValidationLevel::None,
                    ..Default::default()
                },
                db_path_ref,
            )?)
        };

        let crash_recovery_manager = if recovery_config.enable_crash_recovery {
            Some(CrashRecoveryManager::new(
                db_path_ref,
                config.clone(),
                Some(recovery_config.recovery_timeout),
            )?)
        } else {
            None
        };

        let health_monitor = Arc::new(RwLock::new(RecoveryHealthMonitor::new()));

        Ok(Self {
            db_path: db_path_ref.to_string_lossy().to_string(),
            config,
            progress: Arc::new(RecoveryProgress::new()),
            crash_recovery_manager,
            io_recovery_manager,
            memory_recovery_manager,
            transaction_recovery_manager,
            corruption_recovery_manager,
            recovery_config,
            health_monitor,
            shutdown_tx: Arc::new(RwLock::new(None)),
            monitoring_task: Arc::new(RwLock::new(None)),
        })
    }

    /// Get recovery progress tracker
    pub fn progress(&self) -> Arc<RecoveryProgress> {
        self.progress.clone()
    }

    /// Perform comprehensive database recovery
    pub async fn recover(&self) -> Result<Database> {
        info!("Starting comprehensive database recovery for: {}", self.db_path);
        let start_time = Instant::now();

        self.progress.set_phase("Initializing recovery systems");

        // Phase 1: Pre-recovery checks and preparation
        self.pre_recovery_checks().await?;

        // Phase 2: Memory recovery preparation
        if self.recovery_config.enable_memory_recovery {
            self.progress.set_phase("Preparing memory recovery");
            self.prepare_memory_recovery().await?;
        }

        // Phase 3: I/O system recovery
        if self.recovery_config.enable_io_recovery {
            self.progress.set_phase("Recovering I/O systems");
            self.recover_io_systems().await?;
        }

        // Phase 4: Corruption detection and repair
        if self.recovery_config.enable_corruption_recovery {
            self.progress.set_phase("Detecting and repairing corruption");
            self.detect_and_repair_corruption().await?;
        }

        // Phase 5: Transaction recovery
        if self.recovery_config.enable_transaction_recovery {
            self.progress.set_phase("Recovering incomplete transactions");
            self.recover_transactions().await?;
        }

        // Phase 6: Crash recovery (if needed)
        let database = if let Some(ref crash_manager) = self.crash_recovery_manager {
            self.progress.set_phase("Performing crash recovery");
            crash_manager.recover()?
        } else {
            self.progress.set_phase("Opening database normally");
            Database::open(&self.db_path, self.config.clone())?
        };

        // Phase 7: Post-recovery validation
        self.progress.set_phase("Validating recovery");
        self.post_recovery_validation(&database).await?;

        // Phase 8: Start health monitoring
        self.start_health_monitoring().await;

        let recovery_duration = start_time.elapsed();
        info!("Comprehensive recovery completed in {:?}", recovery_duration);
        
        self.progress.set_phase("Recovery complete");
        Ok(database)
    }

    async fn pre_recovery_checks(&self) -> Result<()> {
        info!("Performing pre-recovery checks");

        // Check if recovery is actually needed
        if !self.needs_recovery().await? {
            info!("No recovery needed");
            return Ok(());
        }

        // Check system resources
        self.check_system_resources().await?;

        // Initialize backup locations if corruption recovery is enabled
        if self.recovery_config.enable_corruption_recovery {
            self.setup_backup_locations().await?;
        }

        Ok(())
    }

    async fn prepare_memory_recovery(&self) -> Result<()> {
        info!("Preparing memory recovery systems");

        // Register memory pools and cache managers with the memory recovery system
        // This would be done by actual cache implementations in practice

        Ok(())
    }

    async fn recover_io_systems(&self) -> Result<()> {
        info!("Recovering I/O systems");

        // Check disk health
        let disk_health = self.io_recovery_manager.monitor_disk_health().await?;
        if disk_health.status == DiskHealthStatus::Critical {
            warn!("Critical disk health detected: {:?}", disk_health);
        }

        // Set up fallback paths if needed
        // This would be configured based on the database configuration
        
        Ok(())
    }

    async fn detect_and_repair_corruption(&self) -> Result<()> {
        info!("Detecting and repairing corruption");

        // Perform database-wide corruption scan
        let scan_report = self.corruption_recovery_manager
            .scan_database_corruption(&self.db_path).await?;

        info!(
            "Corruption scan results: {} files scanned, {} corruptions found",
            scan_report.files_scanned,
            scan_report.corruptions_found.len()
        );

        // Attempt to repair detected corruptions
        for corruption in &scan_report.corruptions_found {
            if corruption.auto_repairable && self.recovery_config.auto_recovery {
                match self.corruption_recovery_manager.repair_corruption(corruption).await? {
                    RepairResult::Success { bytes_recovered } => {
                        info!("Successfully repaired corruption, recovered {} bytes", bytes_recovered);
                    }
                    RepairResult::Partial { bytes_recovered, bytes_lost } => {
                        warn!("Partially repaired corruption: recovered {} bytes, lost {} bytes", 
                              bytes_recovered, bytes_lost);
                    }
                    RepairResult::Failed(reason) => {
                        error!("Failed to repair corruption: {}", reason);
                    }
                    RepairResult::Skipped(reason) => {
                        info!("Skipped corruption repair: {}", reason);
                    }
                }
            }
        }

        Ok(())
    }

    async fn recover_transactions(&self) -> Result<()> {
        info!("Recovering incomplete transactions");

        let recovered_transactions = self.transaction_recovery_manager
            .recover_incomplete_transactions().await?;

        if !recovered_transactions.is_empty() {
            info!("Recovered {} incomplete transactions", recovered_transactions.len());
        }

        Ok(())
    }

    async fn post_recovery_validation(&self, _database: &Database) -> Result<()> {
        info!("Validating recovery results");

        // Validate that all systems are functioning correctly
        if self.recovery_config.enable_memory_recovery {
            let memory_health = self.memory_recovery_manager.get_memory_health_report().await;
            if memory_health.status == MemoryHealthStatus::Critical {
                return Err(Error::RecoveryVerificationFailed {
                    check_name: "Memory Health".to_string(),
                    details: "Memory system is in critical state after recovery".to_string(),
                    critical: true,
                });
            }
        }

        if self.recovery_config.enable_transaction_recovery {
            let transaction_health = self.transaction_recovery_manager.get_transaction_health_report().await;
            if transaction_health.status == TransactionHealthStatus::Critical {
                return Err(Error::RecoveryVerificationFailed {
                    check_name: "Transaction Health".to_string(),
                    details: "Transaction system is in critical state after recovery".to_string(),
                    critical: true,
                });
            }
        }

        if self.recovery_config.enable_corruption_recovery {
            let corruption_health = self.corruption_recovery_manager.get_corruption_health_report().await;
            if corruption_health.status == CorruptionHealthStatus::Critical {
                return Err(Error::RecoveryVerificationFailed {
                    check_name: "Corruption Health".to_string(),
                    details: "Corruption system detected critical issues after recovery".to_string(),
                    critical: true,
                });
            }
        }

        Ok(())
    }

    async fn start_health_monitoring(&self) {
        info!("Starting continuous health monitoring");

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        *self.shutdown_tx.write().await = Some(shutdown_tx);

        let health_monitor = self.health_monitor.clone();
        let io_recovery = self.io_recovery_manager.clone();
        let memory_recovery = self.memory_recovery_manager.clone();
        let transaction_recovery = self.transaction_recovery_manager.clone();
        let corruption_recovery = self.corruption_recovery_manager.clone();
        let interval = self.recovery_config.health_check_interval;

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        let mut monitor = health_monitor.write().await;
                        
                        // Update health metrics
                        if let Ok(disk_health) = io_recovery.monitor_disk_health().await {
                            monitor.update_io_health(disk_health);
                        }

                        let memory_health = memory_recovery.get_memory_health_report().await;
                        monitor.update_memory_health(memory_health);

                        let transaction_health = transaction_recovery.get_transaction_health_report().await;
                        monitor.update_transaction_health(transaction_health);

                        let corruption_health = corruption_recovery.get_corruption_health_report().await;
                        monitor.update_corruption_health(corruption_health);

                        // Log critical issues
                        if monitor.has_critical_issues() {
                            error!("Critical health issues detected: {:?}", monitor.get_critical_issues());
                        }
                    }
                    _ = &mut shutdown_rx => {
                        info!("Stopping health monitoring");
                        break;
                    }
                }
            }
        });
        
        *self.monitoring_task.write().await = Some(handle);
    }
    
    /// Stop health monitoring gracefully
    pub async fn stop_health_monitoring(&self) {
        if let Some(tx) = self.shutdown_tx.write().await.take() {
            let _ = tx.send(());
        }
        
        if let Some(handle) = self.monitoring_task.write().await.take() {
            let _ = handle.await;
        }
    }

    async fn check_system_resources(&self) -> Result<()> {
        // Check available disk space
        // Check available memory
        // Check file handle limits
        // This would use platform-specific APIs
        
        Ok(())
    }

    async fn setup_backup_locations(&self) -> Result<()> {
        // Set up backup locations from configuration
        // This would be based on the database configuration
        
        Ok(())
    }

    /// Check if recovery is needed
    pub async fn needs_recovery(&self) -> Result<bool> {
        // Check all recovery systems to see if any indicate recovery is needed
        
        // Check for crash recovery indicators
        if let Some(ref crash_manager) = self.crash_recovery_manager {
            // Use crash manager's detection logic if available
            let _ = crash_manager;
        }

        // Basic recovery indicators (from original implementation)
        let db_path = Path::new(&self.db_path);

        // Check for explicit recovery marker
        let recovery_marker = db_path.join(".recovery_needed");
        if recovery_marker.exists() {
            return Ok(true);
        }
        
        // Check if this is a fresh database (no data files yet)
        let data_file = db_path.join("data.db");
        let pages_dir = db_path.join("pages");
        let has_data = data_file.exists() || pages_dir.exists();
        
        if !has_data {
            // Fresh database, no recovery needed
            return Ok(false);
        }

        // For existing databases, check for incomplete shutdown marker
        let shutdown_marker = db_path.join(".clean_shutdown");
        if !shutdown_marker.exists() {
            return Ok(true);
        }

        // Check for WAL files indicating uncommitted changes
        let wal_dir = db_path.join("wal");
        if wal_dir.exists() {
            let mut wal_entries = tokio::fs::read_dir(&wal_dir).await
                .map_err(|e| Error::RecoveryPermissionError {
                    path: wal_dir.to_string_lossy().to_string(),
                    required_permissions: format!("read access: {}", e),
                })?;

            while let Some(entry) = wal_entries.next_entry().await
                .map_err(|e| Error::Io(e.to_string()))? {
                
                if let Some(ext) = entry.path().extension() {
                    if ext == "wal" {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Get comprehensive recovery health report
    pub async fn get_health_report(&self) -> ComprehensiveHealthReport {
        let monitor = self.health_monitor.read().await;
        
        ComprehensiveHealthReport {
            io_health: monitor.io_health.clone(),
            memory_health: monitor.memory_health.clone(),
            transaction_health: monitor.transaction_health.clone(),
            corruption_health: monitor.corruption_health.clone(),
            overall_status: monitor.get_overall_status(),
            last_updated: monitor.last_updated,
        }
    }

    /// Access individual recovery managers
    pub fn io_recovery(&self) -> &Arc<IoRecoveryManager> {
        &self.io_recovery_manager
    }

    pub fn memory_recovery(&self) -> &Arc<MemoryRecoveryManager> {
        &self.memory_recovery_manager
    }

    pub fn transaction_recovery(&self) -> &Arc<TransactionRecoveryManager> {
        &self.transaction_recovery_manager
    }

    pub fn corruption_recovery(&self) -> &Arc<CorruptionRecoveryManager> {
        &self.corruption_recovery_manager
    }

    pub fn crash_recovery(&self) -> Option<&CrashRecoveryManager> {
        self.crash_recovery_manager.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct ComprehensiveHealthReport {
    pub io_health: Option<DiskHealthReport>,
    pub memory_health: Option<MemoryHealthReport>,
    pub transaction_health: Option<TransactionHealthReport>,
    pub corruption_health: Option<CorruptionHealthReport>,
    pub overall_status: OverallHealthStatus,
    pub last_updated: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OverallHealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

struct RecoveryHealthMonitor {
    io_health: Option<DiskHealthReport>,
    memory_health: Option<MemoryHealthReport>,
    transaction_health: Option<TransactionHealthReport>,
    corruption_health: Option<CorruptionHealthReport>,
    last_updated: Instant,
}

impl RecoveryHealthMonitor {
    fn new() -> Self {
        Self {
            io_health: None,
            memory_health: None,
            transaction_health: None,
            corruption_health: None,
            last_updated: Instant::now(),
        }
    }

    fn update_io_health(&mut self, health: DiskHealthReport) {
        self.io_health = Some(health);
        self.last_updated = Instant::now();
    }

    fn update_memory_health(&mut self, health: MemoryHealthReport) {
        self.memory_health = Some(health);
        self.last_updated = Instant::now();
    }

    fn update_transaction_health(&mut self, health: TransactionHealthReport) {
        self.transaction_health = Some(health);
        self.last_updated = Instant::now();
    }

    fn update_corruption_health(&mut self, health: CorruptionHealthReport) {
        self.corruption_health = Some(health);
        self.last_updated = Instant::now();
    }

    fn get_overall_status(&self) -> OverallHealthStatus {
        let mut has_critical = false;
        let mut has_warning = false;
        let mut has_healthy = false;

        if let Some(ref io_health) = self.io_health {
            match io_health.status {
                DiskHealthStatus::Critical => has_critical = true,
                DiskHealthStatus::Warning => has_warning = true,
                DiskHealthStatus::Healthy => has_healthy = true,
            }
        }

        if let Some(ref memory_health) = self.memory_health {
            match memory_health.status {
                MemoryHealthStatus::Critical => has_critical = true,
                MemoryHealthStatus::Warning => has_warning = true,
                MemoryHealthStatus::Healthy => has_healthy = true,
            }
        }

        if let Some(ref transaction_health) = self.transaction_health {
            match transaction_health.status {
                TransactionHealthStatus::Critical => has_critical = true,
                TransactionHealthStatus::Warning => has_warning = true,
                TransactionHealthStatus::Healthy => has_healthy = true,
            }
        }

        if let Some(ref corruption_health) = self.corruption_health {
            match corruption_health.status {
                CorruptionHealthStatus::Critical => has_critical = true,
                CorruptionHealthStatus::Warning => has_warning = true,
                CorruptionHealthStatus::Healthy => has_healthy = true,
            }
        }

        if has_critical {
            OverallHealthStatus::Critical
        } else if has_warning {
            OverallHealthStatus::Warning
        } else if has_healthy {
            OverallHealthStatus::Healthy
        } else {
            OverallHealthStatus::Unknown
        }
    }

    fn has_critical_issues(&self) -> bool {
        self.get_overall_status() == OverallHealthStatus::Critical
    }

    fn get_critical_issues(&self) -> Vec<String> {
        let mut issues = Vec::new();

        if let Some(ref io_health) = self.io_health {
            if io_health.status == DiskHealthStatus::Critical {
                issues.push(format!("I/O: Critical disk health (success rate: {:.2}%)", 
                    io_health.success_rate * 100.0));
            }
        }

        if let Some(ref memory_health) = self.memory_health {
            if memory_health.status == MemoryHealthStatus::Critical {
                issues.push(format!("Memory: Critical memory state (OOM events: {})", 
                    memory_health.oom_events));
            }
        }

        if let Some(ref transaction_health) = self.transaction_health {
            if transaction_health.status == TransactionHealthStatus::Critical {
                issues.push(format!("Transactions: Critical transaction state (deadlocks: {})", 
                    transaction_health.deadlocks_detected));
            }
        }

        if let Some(ref corruption_health) = self.corruption_health {
            if corruption_health.status == CorruptionHealthStatus::Critical {
                issues.push(format!("Corruption: Critical corruption state (active: {})", 
                    corruption_health.critical_corruptions));
            }
        }

        issues
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_comprehensive_recovery_manager() {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig::default();
        
        let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config).unwrap();
        
        // Test needs recovery check
        let needs_recovery = recovery_manager.needs_recovery().await.unwrap();
        assert!(!needs_recovery); // Fresh directory shouldn't need recovery
        
        // Test health report
        let health_report = recovery_manager.get_health_report().await;
        assert_eq!(health_report.overall_status, OverallHealthStatus::Unknown);
        
        // Clean up any background tasks
        recovery_manager.stop_health_monitoring().await;
    }

    #[tokio::test]
    async fn test_recovery_configuration() {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig::default();
        
        let recovery_config = RecoveryConfiguration {
            enable_crash_recovery: false,
            enable_io_recovery: true,
            enable_memory_recovery: true,
            enable_transaction_recovery: true,
            enable_corruption_recovery: true,
            auto_recovery: false,
            ..Default::default()
        };
        
        let recovery_manager = ComprehensiveRecoveryManager::with_config(
            temp_dir.path(), 
            config, 
            recovery_config
        ).unwrap();
        
        assert!(recovery_manager.crash_recovery().is_none());
        
        // Clean up any background tasks
        recovery_manager.stop_health_monitoring().await;
    }
}