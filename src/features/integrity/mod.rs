use crate::{Database, Error, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};

pub mod alerts;
pub mod checksum;
pub mod detector;
pub mod quarantine;
pub mod recovery;
pub mod repair;
pub mod report;
pub mod scanner;
pub mod validator;

pub use alerts::{Alert, AlertManager, AlertSeverity};
pub use checksum::{ChecksumError, ChecksumManager, ChecksumType};
pub use detector::{CorruptionDetector, CorruptionType, DetectionResult};
pub use quarantine::{QuarantineEntry, QuarantineManager, QuarantineStatus};
pub use recovery::{RecoveryManager, RecoveryResult, RecoveryStrategy};
pub use repair::{AutoRepair, RepairResult, RepairStrategy};
pub use report::{CorruptionReport, ReportFormat, ReportGenerator};
pub use scanner::{IntegrityScanner, ScanConfig, ScanResult};
pub use validator::{DataValidator, ValidationError, ValidationResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityConfig {
    pub auto_repair_enabled: bool,
    pub checksum_algorithm: ChecksumType,
    pub scan_interval: Duration,
    pub max_quarantine_size: usize,
    pub background_scanning: bool,
    pub alert_thresholds: AlertThresholds,
    pub repair_strategies: Vec<RepairStrategy>,
    pub backup_before_repair: bool,
    pub parallel_scanning: bool,
    pub scan_batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    pub checksum_errors: usize,
    pub structure_errors: usize,
    pub corruption_percentage: f64,
    pub recovery_failures: usize,
}

impl Default for IntegrityConfig {
    fn default() -> Self {
        Self {
            auto_repair_enabled: true,
            checksum_algorithm: ChecksumType::XXHash64,
            scan_interval: Duration::from_secs(3600), // 1 hour
            max_quarantine_size: 1000,
            background_scanning: true,
            alert_thresholds: AlertThresholds {
                checksum_errors: 10,
                structure_errors: 5,
                corruption_percentage: 1.0,
                recovery_failures: 3,
            },
            repair_strategies: vec![
                RepairStrategy::RecoverFromWal,
                RepairStrategy::RecoverFromBackup,
                RepairStrategy::PartialReconstruction,
            ],
            backup_before_repair: true,
            parallel_scanning: true,
            scan_batch_size: 100,
        }
    }
}

pub struct IntegrityManager {
    database: Arc<Database>,
    config: IntegrityConfig,
    checksum_manager: Arc<ChecksumManager>,
    detector: Arc<CorruptionDetector>,
    validator: Arc<DataValidator>,
    scanner: Arc<RwLock<Option<IntegrityScanner>>>,
    quarantine: Arc<QuarantineManager>,
    auto_repair: Arc<AutoRepair>,
    recovery: Arc<RecoveryManager>,
    alert_manager: Arc<AlertManager>,
    report_generator: Arc<ReportGenerator>,
    stats: Arc<Mutex<IntegrityStats>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct IntegrityStats {
    pub total_scans: u64,
    pub corrupted_pages: u64,
    pub repaired_pages: u64,
    pub quarantined_pages: u64,
    pub failed_repairs: u64,
    pub last_scan: Option<SystemTime>,
    pub scan_duration: Option<Duration>,
}

impl IntegrityManager {
    pub fn new(database: Arc<Database>, config: IntegrityConfig) -> Result<Self> {
        let checksum_manager = Arc::new(ChecksumManager::new(config.checksum_algorithm));
        let detector = Arc::new(CorruptionDetector::new(
            database.clone(),
            checksum_manager.clone(),
        ));
        let validator = Arc::new(DataValidator::new(database.clone()));
        let quarantine = Arc::new(QuarantineManager::new(config.max_quarantine_size));
        let recovery = Arc::new(RecoveryManager::new(database.clone()));
        let auto_repair = Arc::new(AutoRepair::new(
            database.clone(),
            recovery.clone(),
            config.repair_strategies.clone(),
        ));
        let alert_manager = Arc::new(AlertManager::new(alerts::AlertThresholds::default()));
        let report_generator = Arc::new(ReportGenerator::new());

        Ok(Self {
            database,
            config,
            checksum_manager,
            detector,
            validator,
            scanner: Arc::new(RwLock::new(None)),
            quarantine,
            auto_repair,
            recovery,
            alert_manager,
            report_generator,
            stats: Arc::new(Mutex::new(IntegrityStats::default())),
        })
    }

    pub async fn start_background_scanning(&self) -> Result<()> {
        if !self.config.background_scanning {
            return Ok(());
        }

        let scan_config = ScanConfig {
            batch_size: self.config.scan_batch_size,
            parallel: self.config.parallel_scanning,
            deep_validation: true,
        };

        let scanner = IntegrityScanner::new(
            self.database.clone(),
            self.detector.clone(),
            self.validator.clone(),
            scan_config,
        );

        scanner
            .start_background_scan(self.config.scan_interval)
            .await?;

        *self.scanner.write().await = Some(scanner);
        Ok(())
    }

    pub async fn stop_background_scanning(&self) -> Result<()> {
        if let Some(scanner) = self.scanner.write().await.take() {
            scanner.stop_background_scan().await?;
        }
        Ok(())
    }

    pub async fn verify_integrity(&self) -> Result<CorruptionReport> {
        let start_time = SystemTime::now();

        let scan_config = ScanConfig {
            batch_size: self.config.scan_batch_size,
            parallel: self.config.parallel_scanning,
            deep_validation: true,
        };

        let scanner = IntegrityScanner::new(
            self.database.clone(),
            self.detector.clone(),
            self.validator.clone(),
            scan_config,
        );

        let scan_result = scanner.full_scan().await?;

        let mut stats = self.stats.lock().await;
        stats.total_scans += 1;
        stats.last_scan = Some(start_time);
        stats.scan_duration = SystemTime::now().duration_since(start_time).ok();

        let report = self
            .report_generator
            .generate_report(&scan_result, &*stats)
            .await?;

        if scan_result.has_corruption() {
            self.handle_corruption_detected(&scan_result).await?;
        }

        Ok(report)
    }

    pub async fn scan_for_corruption(&self) -> Result<ScanResult> {
        let scan_config = ScanConfig {
            batch_size: self.config.scan_batch_size,
            parallel: self.config.parallel_scanning,
            deep_validation: false, // Quick scan
        };

        let scanner = IntegrityScanner::new(
            self.database.clone(),
            self.detector.clone(),
            self.validator.clone(),
            scan_config,
        );

        scanner.quick_scan().await
    }

    pub async fn repair_corruption(&self, auto_mode: bool) -> Result<RepairResult> {
        if auto_mode && !self.config.auto_repair_enabled {
            return Err(Error::InvalidOperation {
                reason: "Auto repair is disabled".to_string(),
            });
        }

        let scan_result = self.scan_for_corruption().await?;

        if !scan_result.has_corruption() {
            return Ok(RepairResult::NoCorruptionFound);
        }

        if self.config.backup_before_repair {
            self.create_backup_before_repair().await?;
        }

        let repair_result = if auto_mode {
            self.auto_repair.repair_automatically(&scan_result).await?
        } else {
            self.auto_repair.repair_manually(&scan_result).await?
        };

        self.update_repair_stats(&repair_result).await;

        Ok(repair_result)
    }

    pub async fn get_corruption_report(&self) -> Result<CorruptionReport> {
        let stats = self.stats.lock().await;
        let quarantine_stats = self.quarantine.get_stats().await?;

        self.report_generator
            .generate_status_report(&*stats, &quarantine_stats)
            .await
    }

    pub async fn set_integrity_check_interval(&self, interval: Duration) -> Result<()> {
        let scanner_opt = self.scanner.write().await;
        if let Some(scanner) = scanner_opt.as_ref() {
            scanner.update_scan_interval(interval).await?;
        }

        Ok(())
    }

    pub async fn enable_auto_repair(&self, enabled: bool) -> Result<()> {
        self.auto_repair.set_enabled(enabled).await;
        Ok(())
    }

    pub async fn get_quarantined_data(&self) -> Result<Vec<QuarantineEntry>> {
        self.quarantine.list_entries().await
    }

    pub async fn restore_quarantined_data(&self, entry_id: u64) -> Result<()> {
        let entry = self.quarantine.get_entry(entry_id).await?;

        let recovery_result = self.recovery.recover_from_quarantine(&entry).await?;

        if recovery_result.success {
            self.quarantine.remove_entry(entry_id).await?;
        }

        Ok(())
    }

    async fn handle_corruption_detected(&self, scan_result: &ScanResult) -> Result<()> {
        for corruption in &scan_result.corruptions {
            self.quarantine.quarantine_data(corruption).await?;

            let alert = Alert::new(
                AlertSeverity::High,
                format!("Corruption detected: {:?}", corruption.corruption_type),
                corruption.clone(),
            );

            self.alert_manager.send_alert(alert).await?;
        }

        if self.config.auto_repair_enabled {
            tokio::spawn({
                let auto_repair = self.auto_repair.clone();
                let scan_result = scan_result.clone();
                async move {
                    if let Err(e) = auto_repair.repair_automatically(&scan_result).await {
                        eprintln!("Auto-repair failed: {}", e);
                    }
                }
            });
        }

        Ok(())
    }

    async fn create_backup_before_repair(&self) -> Result<()> {
        // Implementation would create a backup
        Ok(())
    }

    async fn update_repair_stats(&self, repair_result: &RepairResult) {
        let mut stats = self.stats.lock().await;
        match repair_result {
            RepairResult::Success { repaired_count, .. } => {
                stats.repaired_pages += *repaired_count as u64;
            }
            RepairResult::PartialSuccess {
                repaired_count,
                failed_count,
                ..
            } => {
                stats.repaired_pages += *repaired_count as u64;
                stats.failed_repairs += *failed_count as u64;
            }
            RepairResult::Failed { .. } => {
                stats.failed_repairs += 1;
            }
            _ => {}
        }
    }

    pub async fn get_stats(&self) -> IntegrityStats {
        self.stats.lock().await.clone()
    }
}
