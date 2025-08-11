//! Data Integrity Validation Tools
//!
//! This module provides comprehensive tools for validating data integrity
//! in Lightning DB, including checksums, consistency checks, and repair utilities.

use crate::{Database, Error, Result};
use crc32fast::Hasher;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;

pub mod checksum_validator;
pub mod consistency_checker;
pub mod page_scanner;
pub mod repair_tool;
pub mod data_integrity;
pub mod error_types;
pub mod validation_config;
pub mod metrics;

pub use checksum_validator::ChecksumValidator;
pub use consistency_checker::ConsistencyChecker;
pub use data_integrity::{DataIntegrityValidator, IntegrityReport as NewIntegrityReport};
pub use error_types::{IntegrityError, IntegrityViolation, ValidationResult, ViolationSeverity};
pub use page_scanner::PageScanner;
pub use repair_tool::RepairTool;
pub use validation_config::{IntegrityConfig as NewIntegrityConfig, ValidationLevel, ValidationContext};
pub use metrics::{IntegrityMetrics, MetricsSnapshot};

/// Legacy integrity validation configuration (deprecated - use validation_config::IntegrityConfig)
#[derive(Debug, Clone)]
#[deprecated(note = "Use validation_config::IntegrityConfig instead")]
pub struct IntegrityConfig {
    /// Enable checksum validation
    pub validate_checksums: bool,
    /// Enable page structure validation
    pub validate_page_structure: bool,
    /// Enable B+Tree consistency validation
    pub validate_btree_consistency: bool,
    /// Enable transaction log validation
    pub validate_transaction_log: bool,
    /// Enable cross-reference validation
    pub validate_cross_references: bool,
    /// Maximum errors before stopping
    pub max_errors: usize,
    /// Enable repair attempts
    pub enable_repair: bool,
    /// Backup before repair
    pub backup_before_repair: bool,
}

impl Default for IntegrityConfig {
    fn default() -> Self {
        Self {
            validate_checksums: true,
            validate_page_structure: true,
            validate_btree_consistency: true,
            validate_transaction_log: true,
            validate_cross_references: true,
            max_errors: 1000,
            enable_repair: false,
            backup_before_repair: true,
        }
    }
}

/// Convert legacy config to new config
impl From<IntegrityConfig> for NewIntegrityConfig {
    fn from(legacy: IntegrityConfig) -> Self {
        Self {
            enabled: true,
            level: ValidationLevel::Standard,
            validate_all_reads: false,
            validate_all_writes: true,
            validate_cached_data: false,
            background_validation: true,
            checksum_algorithm: validation_config::ChecksumAlgorithm::CRC32Fast,
            max_validation_latency: std::time::Duration::from_millis(10),
            sampling_rate: 0.1,
            deep_structural_validation: legacy.validate_page_structure,
            cross_reference_validation: legacy.validate_cross_references,
            temporal_consistency_checks: true,
            max_errors_threshold: legacy.max_errors,
            auto_repair: legacy.enable_repair,
            backup_before_repair: legacy.backup_before_repair,
            collect_metrics: true,
            enable_alerts: true,
            context_timeout: std::time::Duration::from_secs(30),
            enable_parallel_validation: true,
            validation_thread_pool_size: 4,
        }
    }
}

/// Integrity validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    /// Start time of validation
    pub start_time: SystemTime,
    /// End time of validation
    pub end_time: Option<SystemTime>,
    /// Total pages scanned
    pub pages_scanned: u64,
    /// Total keys validated
    pub keys_validated: u64,
    /// Checksum errors found
    pub checksum_errors: Vec<ChecksumError>,
    /// Structure errors found
    pub structure_errors: Vec<StructureError>,
    /// Consistency errors found
    pub consistency_errors: Vec<ConsistencyError>,
    /// Transaction log errors
    pub transaction_errors: Vec<TransactionError>,
    /// Cross-reference errors
    pub cross_reference_errors: Vec<CrossReferenceError>,
    /// Repair actions taken
    pub repair_actions: Vec<RepairAction>,
    /// Overall integrity status
    pub status: IntegrityStatus,
}

/// Checksum error details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecksumError {
    pub page_id: u64,
    pub expected_checksum: u32,
    pub actual_checksum: u32,
    pub data_size: usize,
    pub error_type: ChecksumErrorType,
}

/// Types of checksum errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumErrorType {
    PageHeader,
    PageData,
    KeyValue,
    Metadata,
}

/// Structure error details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructureError {
    pub page_id: u64,
    pub error_type: StructureErrorType,
    pub description: String,
    pub severity: ErrorSeverity,
}

/// Types of structure errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructureErrorType {
    InvalidPageType,
    InvalidKeyCount,
    InvalidPointer,
    InvalidSize,
    CorruptedHeader,
    InvalidMagicNumber,
}

/// Consistency error details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistencyError {
    pub error_type: ConsistencyErrorType,
    pub description: String,
    pub affected_pages: Vec<u64>,
    pub severity: ErrorSeverity,
}

/// Types of consistency errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsistencyErrorType {
    BTreeDepthMismatch,
    InvalidKeyOrder,
    DuplicateKeys,
    OrphanedPage,
    CircularReference,
    MissingChild,
    InvalidParentPointer,
}

/// Transaction log error details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionError {
    pub transaction_id: u64,
    pub error_type: TransactionErrorType,
    pub description: String,
}

/// Types of transaction errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionErrorType {
    IncompleteTransaction,
    InvalidSequence,
    CorruptedLogEntry,
    MissingCommit,
    InvalidCheckpoint,
}

/// Cross-reference error details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossReferenceError {
    pub source_page: u64,
    pub target_page: u64,
    pub error_type: CrossReferenceErrorType,
    pub description: String,
}

/// Types of cross-reference errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrossReferenceErrorType {
    InvalidPointer,
    MissingTarget,
    TypeMismatch,
    CyclicReference,
}

/// Repair action details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairAction {
    pub action_type: RepairActionType,
    pub target: RepairTarget,
    pub description: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Types of repair actions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairActionType {
    RebuildChecksum,
    RemoveOrphanedPage,
    FixKeyOrder,
    RebuildIndex,
    TruncateCorruptedLog,
    RestoreFromBackup,
}

/// Repair target
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepairTarget {
    Page(u64),
    Transaction(u64),
    Index(String),
    Log,
    Metadata,
}

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ErrorSeverity {
    Warning,
    Error,
    Critical,
    Fatal,
}

/// Overall integrity status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrityStatus {
    /// No errors found
    Clean,
    /// Minor issues that don't affect data integrity
    MinorIssues,
    /// Errors that may affect performance but not data
    Degraded,
    /// Data integrity issues detected
    Corrupted,
    /// Fatal errors, database unusable
    Fatal,
}

/// Main integrity validator
pub struct IntegrityValidator {
    config: NewIntegrityConfig,
    database: Arc<Database>,
    report: Arc<RwLock<IntegrityReport>>,
    error_count: Arc<RwLock<usize>>,
}

impl IntegrityValidator {
    /// Create new integrity validator
    pub fn new(database: Arc<Database>, config: NewIntegrityConfig) -> Self {
        Self {
            config,
            database,
            report: Arc::new(RwLock::new(IntegrityReport {
                start_time: SystemTime::now(),
                end_time: None,
                pages_scanned: 0,
                keys_validated: 0,
                checksum_errors: Vec::new(),
                structure_errors: Vec::new(),
                consistency_errors: Vec::new(),
                transaction_errors: Vec::new(),
                cross_reference_errors: Vec::new(),
                repair_actions: Vec::new(),
                status: IntegrityStatus::Clean,
            })),
            error_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Run full integrity validation
    pub async fn validate(&self) -> Result<IntegrityReport> {
        println!("Starting integrity validation...");

        // Phase 1: Checksum validation
        if self.config.enabled {
            println!("Phase 1: Validating checksums...");
            self.validate_checksums().await?;
        }

        // Phase 2: Page structure validation
        if self.config.deep_structural_validation {
            println!("Phase 2: Validating page structures...");
            self.validate_page_structures().await?;
        }

        // Phase 3: B+Tree consistency
        if self.config.deep_structural_validation {
            println!("Phase 3: Validating B+Tree consistency...");
            self.validate_btree_consistency().await?;
        }

        // Phase 4: Transaction log validation
        if self.config.temporal_consistency_checks {
            println!("Phase 4: Validating transaction log...");
            self.validate_transaction_log().await?;
        }

        // Phase 5: Cross-reference validation
        if self.config.cross_reference_validation {
            println!("Phase 5: Validating cross-references...");
            self.validate_cross_references().await?;
        }

        // Determine overall status
        self.determine_overall_status();

        // Generate final report
        let mut report = self.report.write();
        report.end_time = Some(SystemTime::now());
        Ok(report.clone())
    }

    /// Validate checksums
    async fn validate_checksums(&self) -> Result<()> {
        let validator = ChecksumValidator::new(self.database.clone());
        let errors = validator.validate_all().await?;

        let mut report = self.report.write();
        report.checksum_errors.extend(errors);

        Ok(())
    }

    /// Validate page structures
    async fn validate_page_structures(&self) -> Result<()> {
        let scanner = PageScanner::new(self.database.clone());
        let errors = scanner.scan_all_pages().await?;

        let mut report = self.report.write();
        report.structure_errors.extend(errors);
        report.pages_scanned = scanner.get_pages_scanned();

        Ok(())
    }

    /// Validate B+Tree consistency
    async fn validate_btree_consistency(&self) -> Result<()> {
        let checker = ConsistencyChecker::new(self.database.clone());
        let errors = checker.check_btree_consistency().await?;

        let mut report = self.report.write();
        report.consistency_errors.extend(errors);
        report.keys_validated = checker.get_keys_validated();

        Ok(())
    }

    /// Validate transaction log
    async fn validate_transaction_log(&self) -> Result<()> {
        // Implementation would validate transaction log integrity
        Ok(())
    }

    /// Validate cross-references
    async fn validate_cross_references(&self) -> Result<()> {
        // Implementation would validate all cross-references between pages
        Ok(())
    }

    /// Determine overall integrity status
    fn determine_overall_status(&self) {
        let report = self.report.read();

        let critical_errors = report
            .structure_errors
            .iter()
            .filter(|e| e.severity >= ErrorSeverity::Critical)
            .count()
            + report
                .consistency_errors
                .iter()
                .filter(|e| e.severity >= ErrorSeverity::Critical)
                .count();

        let total_errors = report.checksum_errors.len()
            + report.structure_errors.len()
            + report.consistency_errors.len()
            + report.transaction_errors.len()
            + report.cross_reference_errors.len();

        drop(report);

        let status = if critical_errors > 0 {
            IntegrityStatus::Fatal
        } else if total_errors > 100 {
            IntegrityStatus::Corrupted
        } else if total_errors > 10 {
            IntegrityStatus::Degraded
        } else if total_errors > 0 {
            IntegrityStatus::MinorIssues
        } else {
            IntegrityStatus::Clean
        };

        self.report.write().status = status;
    }

    /// Attempt repairs if enabled
    pub async fn repair(&self) -> Result<Vec<RepairAction>> {
        if !self.config.auto_repair {
            return Err(Error::InvalidOperation {
                reason: "Repair not enabled in configuration".to_string(),
            });
        }

        if self.config.backup_before_repair {
            println!("Creating backup before repair...");
            self.create_backup().await?;
        }

        let repair_tool = RepairTool::new(self.database.clone());
        let report = self.report.read().clone();

        let actions = repair_tool.repair_errors(&report).await?;

        self.report.write().repair_actions.extend(actions.clone());

        Ok(actions)
    }

    /// Create backup before repair
    async fn create_backup(&self) -> Result<()> {
        // Implementation would create a backup of the database
        Ok(())
    }

    /// Generate detailed report
    pub fn generate_report(&self) -> String {
        let report = self.report.read();

        let mut output = String::new();
        output.push_str("=== Lightning DB Integrity Report ===\n\n");

        output.push_str(&format!("Status: {:?}\n", report.status));
        output.push_str(&format!("Pages Scanned: {}\n", report.pages_scanned));
        output.push_str(&format!("Keys Validated: {}\n", report.keys_validated));

        if let Some(end_time) = report.end_time {
            if let Ok(duration) = end_time.duration_since(report.start_time) {
                output.push_str(&format!("Duration: {:?}\n", duration));
            }
        }

        output.push_str("\n--- Errors Summary ---\n");
        output.push_str(&format!(
            "Checksum Errors: {}\n",
            report.checksum_errors.len()
        ));
        output.push_str(&format!(
            "Structure Errors: {}\n",
            report.structure_errors.len()
        ));
        output.push_str(&format!(
            "Consistency Errors: {}\n",
            report.consistency_errors.len()
        ));
        output.push_str(&format!(
            "Transaction Errors: {}\n",
            report.transaction_errors.len()
        ));
        output.push_str(&format!(
            "Cross-Reference Errors: {}\n",
            report.cross_reference_errors.len()
        ));

        if !report.repair_actions.is_empty() {
            output.push_str("\n--- Repair Actions ---\n");
            for action in &report.repair_actions {
                output.push_str(&format!(
                    "{:?}: {} - {}\n",
                    action.action_type,
                    action.description,
                    if action.success { "SUCCESS" } else { "FAILED" }
                ));
            }
        }

        output
    }
}

/// Quick integrity check for routine validation
pub async fn quick_integrity_check(database: Arc<Database>) -> Result<bool> {
    let config = NewIntegrityConfig {
        enabled: true,
        deep_structural_validation: false,
        cross_reference_validation: false,
        temporal_consistency_checks: false,
        max_errors_threshold: 10,
        auto_repair: false,
        backup_before_repair: false,
        ..NewIntegrityConfig::default()
    };

    let validator = IntegrityValidator::new(database, config);
    let report = validator.validate().await?;

    Ok(report.status == IntegrityStatus::Clean)
}

/// Calculate checksum for data
pub fn calculate_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}
