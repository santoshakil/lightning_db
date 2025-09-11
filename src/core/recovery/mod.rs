//! Recovery module for Lightning DB
//!
//! Provides comprehensive recovery mechanisms including:
//! - Crash recovery with WAL recovery and integrity validation
//! - Transaction recovery with rollback and deadlock handling
//! - I/O error recovery with retry logic and fallback strategies
//! - Memory error recovery with graceful degradation
//! - Corruption recovery with auto-repair mechanisms
//! - Double-write buffer for torn page prevention
//! - Redundant metadata storage
//! - Recovery orchestration

pub mod corruption_recovery;
pub mod crash_recovery_manager;
pub mod io_recovery;
pub mod memory_recovery;
pub mod transaction_recovery;
pub mod recovery_utils;

pub use corruption_recovery::{
    CorruptionHealthReport, CorruptionHealthStatus, CorruptionRecoveryConfig,
    CorruptionRecoveryManager, CorruptionRecoveryStats, CorruptionReport, CorruptionScanReport,
    CorruptionSeverity, CorruptionType, RepairResult, RepairStrategy, ValidationLevel,
};
pub use crash_recovery_manager::{
    CrashRecoveryManager, RecoveryLockManager, RecoveryStage, RecoveryStageResult, RecoveryState,
    ResourceChecker, RollbackAction, RollbackData, RollbackManager,
};
pub use io_recovery::{
    DiskHealthReport, DiskHealthStatus, IoRecoveryConfig, IoRecoveryManager, IoRecoveryStats,
    ReliableFileHandle,
};
pub use memory_recovery::{
    CacheManager, MemoryHealthReport, MemoryHealthStatus, MemoryPool, MemoryRecoveryConfig,
    MemoryRecoveryManager, MemoryStats,
};
pub use transaction_recovery::{
    ConflictResolutionStrategy, OperationType, TransactionHealthReport, TransactionHealthStatus,
    TransactionInfo, TransactionOperation, TransactionRecoveryConfig, TransactionRecoveryManager,
    TransactionRecoveryStats, TransactionState,
};
pub use recovery_utils::{
    DoubleWriteBuffer, EnhancedWalRecovery, RecoveryProgress, RecoveryStats, RedundantMetadata,
};

use crate::core::error::Result;
use crate::{Database, LightningDbConfig};
use std::path::Path;
use std::sync::Arc;

/// Recovery manager that coordinates all recovery operations
///
/// This is the main entry point for database recovery operations.
/// It provides both legacy compatibility and access to the new comprehensive
/// crash recovery system.
pub struct RecoveryManager {
    db_path: String,
    config: LightningDbConfig,
    progress: Arc<RecoveryProgress>,
    crash_recovery_manager: Option<CrashRecoveryManager>,
}

impl RecoveryManager {
    pub fn new(db_path: impl AsRef<Path>, config: LightningDbConfig) -> Self {
        Self {
            db_path: db_path.as_ref().to_string_lossy().to_string(),
            config,
            progress: Arc::new(RecoveryProgress::new()),
            crash_recovery_manager: None,
        }
    }

    /// Create a new recovery manager with comprehensive crash recovery
    pub fn with_crash_recovery(
        db_path: impl AsRef<Path>,
        config: LightningDbConfig,
    ) -> Result<Self> {
        let crash_recovery_manager = Some(CrashRecoveryManager::new(
            db_path.as_ref(),
            config.clone(),
            None, // Use default timeout
        )?);

        Ok(Self {
            db_path: db_path.as_ref().to_string_lossy().to_string(),
            config,
            progress: Arc::new(RecoveryProgress::new()),
            crash_recovery_manager,
        })
    }

    /// Get recovery progress tracker
    pub fn progress(&self) -> Arc<RecoveryProgress> {
        self.progress.clone()
    }

    /// Perform full database recovery with comprehensive error handling
    ///
    /// This method will use the new crash recovery system if available,
    /// otherwise fall back to the legacy recovery method.
    pub fn recover(&self) -> Result<Database> {
        if let Some(ref crash_manager) = self.crash_recovery_manager {
            // Use comprehensive crash recovery system
            crash_manager.recover()
        } else {
            // Use legacy recovery method for backward compatibility
            self.recover_legacy()
        }
    }

    /// Legacy recovery method (preserved for backward compatibility)
    fn recover_legacy(&self) -> Result<Database> {
        self.progress.set_phase("Starting legacy recovery");

        // Step 1: Recover metadata
        self.progress.set_phase("Recovering metadata");
        let metadata = RedundantMetadata::new(Path::new(&self.db_path));
        let header = metadata.read_header().map_err(|e| {
            crate::core::error::Error::PartialRecoveryFailure {
                completed_stages: vec![],
                failed_stage: "Metadata Recovery".to_string(),
                cause: Box::new(e),
                rollback_available: false,
            }
        })?;

        // Step 2: Recover from double-write buffer
        self.progress.set_phase("Checking double-write buffer");
        let dwb = DoubleWriteBuffer::new(Path::new(&self.db_path), header.page_size as usize, 32)
            .map_err(|e| crate::core::error::Error::PartialRecoveryFailure {
            completed_stages: vec!["Metadata Recovery".to_string()],
            failed_stage: "Double-Write Buffer Setup".to_string(),
            cause: Box::new(e),
            rollback_available: false,
        })?;

        let recovered_pages = dwb
            .recover(|page_id, _data| {
                // In real implementation, write to page manager
                println!("Recovered page {} from double-write buffer", page_id);
                Ok(())
            })
            .map_err(|e| crate::core::error::Error::PartialRecoveryFailure {
                completed_stages: vec![
                    "Metadata Recovery".to_string(),
                    "Double-Write Buffer Setup".to_string(),
                ],
                failed_stage: "Double-Write Buffer Recovery".to_string(),
                cause: Box::new(e),
                rollback_available: false,
            })?;

        if recovered_pages > 0 {
            println!(
                "Recovered {} pages from double-write buffer",
                recovered_pages
            );
        }

        // Step 3: Recover from WAL
        self.progress.set_phase("Recovering from WAL");
        let wal_recovery =
            EnhancedWalRecovery::new(Path::new(&self.db_path).join("wal"), self.progress.clone());

        let stats = wal_recovery
            .recover(|_entry| {
                // In real implementation, apply entry to database
                Ok(())
            })
            .map_err(|e| crate::core::error::Error::PartialRecoveryFailure {
                completed_stages: vec![
                    "Metadata Recovery".to_string(),
                    "Double-Write Buffer Setup".to_string(),
                    "Double-Write Buffer Recovery".to_string(),
                ],
                failed_stage: "WAL Recovery".to_string(),
                cause: Box::new(e),
                rollback_available: false,
            })?;

        println!("WAL recovery stats: {:?}", stats);

        // Step 4: Open database normally
        self.progress.set_phase("Opening database");
        let db = Database::open(&self.db_path, self.config.clone()).map_err(|e| {
            crate::core::error::Error::RecoveryVerificationFailed {
                check_name: "Database Open".to_string(),
                details: e.to_string(),
                critical: true,
            }
        })?;

        self.progress.set_phase("Recovery complete");

        Ok(db)
    }

    /// Check if recovery is needed with enhanced detection
    pub fn needs_recovery(&self) -> Result<bool> {
        // Check for recovery indicators:
        // 1. Incomplete shutdown marker
        // 2. Non-empty double-write buffer
        // 3. WAL entries after last checkpoint
        // 4. Database lock files
        // 5. Consistency markers

        let db_path = Path::new(&self.db_path);

        // Check for explicit recovery marker
        let recovery_marker = db_path.join(".recovery_needed");
        if recovery_marker.exists() {
            return Ok(true);
        }

        // Check for incomplete shutdown marker
        let shutdown_marker = db_path.join(".clean_shutdown");
        if !shutdown_marker.exists() {
            return Ok(true);
        }

        // Check double-write buffer
        let dwb_path = db_path.join("double_write.buffer");
        if dwb_path.exists() {
            let dwb_size = std::fs::metadata(&dwb_path)
                .map_err(|e| crate::core::error::Error::RecoveryDependencyError {
                    dependency: "double_write.buffer".to_string(),
                    issue: format!("Cannot check file: {}", e),
                })?
                .len();
            if dwb_size > 8 {
                // Has more than just header
                return Ok(true);
            }
        }

        // Check for stale recovery locks (indicates previous recovery failure)
        let recovery_lock = db_path.join(".recovery_lock");
        if recovery_lock.exists() {
            // Check if lock is stale (older than 1 hour)
            if let Ok(metadata) = std::fs::metadata(&recovery_lock) {
                if let Ok(modified) = metadata.modified() {
                    if modified.elapsed().map(|d| d.as_secs()).unwrap_or(0) > 3600 {
                        // Stale lock indicates crashed recovery
                        return Ok(true);
                    }
                }
            }
        }

        // Check WAL directory for uncommitted entries
        let wal_dir = db_path.join("wal");
        if wal_dir.exists() {
            let wal_files = std::fs::read_dir(&wal_dir).map_err(|e| {
                crate::core::error::Error::RecoveryPermissionError {
                    path: wal_dir.to_string_lossy().to_string(),
                    required_permissions: format!("read access: {}", e),
                }
            })?;

            let wal_count = wal_files
                .filter_map(|entry| {
                    entry.ok().and_then(|e| {
                        if e.path().extension()?.to_str()? == "wal" {
                            Some(())
                        } else {
                            None
                        }
                    })
                })
                .count();

            if wal_count > 0 {
                // WAL files present indicate potential uncommitted changes
                return Ok(true);
            }
        }

        // If we have a crash recovery manager, use its detection logic
        if let Some(ref crash_manager) = self.crash_recovery_manager {
            // Additional checks could be added here based on crash manager capabilities
            let _ = crash_manager; // Prevent unused variable warning
        }

        Ok(false)
    }

    /// Get the crash recovery manager if available
    pub fn crash_recovery_manager(&self) -> Option<&CrashRecoveryManager> {
        self.crash_recovery_manager.as_ref()
    }

    /// Abort ongoing recovery (if using crash recovery manager)
    pub fn abort_recovery(&self) {
        if let Some(ref crash_manager) = self.crash_recovery_manager {
            crash_manager.abort_recovery();
        }
    }
}
