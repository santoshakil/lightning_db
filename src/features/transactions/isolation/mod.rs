//! Transaction isolation system for Lightning DB
//!
//! This module implements comprehensive transaction isolation following SQL standards:
//! - Read Uncommitted: Allows dirty reads, non-repeatable reads, and phantom reads
//! - Read Committed: Prevents dirty reads, allows non-repeatable reads and phantom reads  
//! - Repeatable Read: Prevents dirty reads and non-repeatable reads, allows phantom reads
//! - Serializable: Prevents all phenomena including phantom reads
//! - Snapshot: Snapshot isolation with first-committer-wins for write conflicts
//!
//! ## Architecture
//!
//! The isolation system consists of several coordinated components:
//!
//! ### Core Components
//! - **Isolation Levels**: SQL standard isolation level definitions and properties
//! - **Lock Manager**: Multi-granularity locking with deadlock detection
//! - **Visibility Engine**: MVCC visibility rules for different isolation levels
//! - **Snapshot Manager**: Point-in-time snapshots for repeatable reads
//! - **Conflict Resolver**: Detection and resolution of various conflict types
//! - **Predicate Locks**: Range and gap locking for phantom prevention
//! - **Serialization Validator**: Optimistic concurrency control validation
//! - **Deadlock Detector**: Advanced deadlock detection with multiple resolution strategies
//!
//! ### Locking Mechanisms
//! - Row-level locking with shared/exclusive modes
//! - Intent locks for hierarchical locking
//! - Range locks for phantom read prevention
//! - Gap locks between existing keys
//! - Next-key locks (record + gap)
//! - Predicate locks for complex conditions
//!
//! ### MVCC Integration
//! - Version-based visibility rules
//! - Snapshot-based isolation for repeatable read and serializable
//! - Write-write conflict detection
//! - Phantom read detection for range queries
//! - Serialization anomaly detection
//!
//! ## Usage Examples
//!
//! ### Basic Isolation Level Usage
//! ```rust,no_run
//! use lightning_db::{Database, LightningDbConfig};
//! use lightning_db::features::transactions::isolation::{IsolationLevel, IsolationConfig};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Database::create("./db", LightningDbConfig::default())?;
//!
//! // Set isolation level for a transaction
//! let tx_id = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
//! db.put_tx(tx_id, b"key", b"value")?;
//! db.commit_transaction(tx_id)?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Configuring Isolation Settings  
//! ```rust,no_run
//! use lightning_db::features::transactions::isolation::{IsolationConfig, IsolationLevel};
//! use std::time::Duration;
//!
//! let isolation_config = IsolationConfig {
//!     level: IsolationLevel::RepeatableRead,
//!     enable_deadlock_detection: true,
//!     deadlock_timeout: Duration::from_secs(5),
//!     lock_timeout: Duration::from_secs(10),
//!     max_concurrent_transactions: 1000,
//!     snapshot_cleanup_interval: Duration::from_secs(60),
//! };
//! ```
//!
//! ### Manual Deadlock Detection
//! ```rust,no_run
//! # use lightning_db::{Database, LightningDbConfig};
//! # let db = Database::create("./db", LightningDbConfig::default()).unwrap();
//! // Enable automatic deadlock detection
//! db.enable_deadlock_detection(true)?;
//!
//! // Manual deadlock check
//! let deadlocked_txs = db.detect_deadlocks()?;
//! for tx_id in deadlocked_txs {
//!     println!("Transaction {} is deadlocked", tx_id);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod conflicts;
pub mod deadlock_types;
pub mod levels;
pub mod locks;
pub mod snapshot;
pub mod validation;
pub mod visibility;

// Re-export main types for public API
pub use conflicts::{Conflict, ConflictResolution, ConflictResolver, ConflictStats, ConflictType};
pub use deadlock_types::{
    DeadlockDetector, DeadlockResolutionStrategy, DeadlockResult, DeadlockStats, NextKeyLocker,
    Predicate, PredicateLock, PredicateLockManager, PredicateLockStats,
};
pub use levels::{IsolationConfig, IsolationLevel, LockDurationPolicy, LockRequirements};
pub use locks::{LockGranularity, LockManager, LockMode, TxId};
pub use snapshot::{Snapshot, SnapshotId, SnapshotManager, SnapshotStats};
pub use validation::{SerializationValidator, ValidationInfo, ValidationResult, ValidationStats};
pub use visibility::{TxInfo, TxState, VersionInfo, VisibilityEngine, VisibilityStats};

use crate::core::error::{Error, Result};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

/// Unified transaction isolation manager
#[derive(Debug)]
pub struct IsolationManager {
    /// Lock manager for pessimistic concurrency control
    pub lock_manager: Arc<LockManager>,
    /// Deadlock detector
    pub deadlock_detector: Arc<DeadlockDetector>,
    /// MVCC visibility engine
    pub visibility_engine: Arc<VisibilityEngine>,
    /// Snapshot manager
    pub snapshot_manager: Arc<SnapshotManager>,
    /// Conflict resolver
    pub conflict_resolver: Arc<ConflictResolver>,
    /// Predicate lock manager
    pub predicate_manager: Arc<PredicateLockManager>,
    /// Serialization validator for optimistic CC
    pub serialization_validator: Arc<SerializationValidator>,
    /// Configuration
    config: Arc<RwLock<IsolationConfig>>,
}

impl IsolationManager {
    /// Create a new isolation manager with default configuration
    pub fn new() -> Self {
        Self::with_config(IsolationConfig::default())
    }

    /// Create a new isolation manager with custom configuration
    pub fn with_config(config: IsolationConfig) -> Self {
        let lock_manager = Arc::new(LockManager::new(
            config.lock_timeout,
            config.enable_deadlock_detection,
            config.deadlock_timeout,
        ));

        let deadlock_detector = Arc::new(DeadlockDetector::new(
            DeadlockResolutionStrategy::AbortYoungest,
            Duration::from_millis(500), // Check every 500ms
            config.deadlock_timeout,
        ));

        let visibility_engine = Arc::new(VisibilityEngine::new());

        let snapshot_manager = Arc::new(SnapshotManager::new(
            Duration::from_secs(300), // 5 minute max age
            config.snapshot_cleanup_interval,
        ));

        let conflict_resolver = Arc::new(ConflictResolver::new());

        let predicate_manager = Arc::new(PredicateLockManager::new(lock_manager.clone()));

        let serialization_validator = Arc::new(SerializationValidator::new());

        Self {
            lock_manager,
            deadlock_detector,
            visibility_engine,
            snapshot_manager,
            conflict_resolver,
            predicate_manager,
            serialization_validator,
            config: Arc::new(RwLock::new(config)),
        }
    }

    /// Begin a new transaction with specified isolation level
    pub fn begin_transaction(&self, tx_id: TxId, isolation_level: IsolationLevel) -> Result<()> {
        debug!(
            "Beginning transaction {} with isolation level {}",
            tx_id, isolation_level
        );

        // Register with all relevant components
        self.deadlock_detector.register_transaction(tx_id);
        self.conflict_resolver.register_transaction(tx_id);
        self.visibility_engine
            .begin_transaction(tx_id, isolation_level)?;
        self.serialization_validator
            .begin_transaction(tx_id, isolation_level)?;

        info!(
            "Started transaction {} with {} isolation",
            tx_id, isolation_level
        );
        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, tx_id: TxId) -> Result<()> {
        debug!("Committing transaction {}", tx_id);

        // Validate transaction if using optimistic concurrency control
        let validation_result = self.serialization_validator.validate_transaction(tx_id)?;
        match validation_result {
            ValidationResult::Valid => {
                // Proceed with commit
                self.visibility_engine.commit_transaction(tx_id)?;
                self.serialization_validator.commit_transaction(tx_id)?;
            }
            ValidationResult::Invalid { reason, conflicts } => {
                return Err(Error::InvalidArgument(format!(
                    "Transaction {} validation failed: {} (conflicts with {:?})",
                    tx_id, reason, conflicts
                )));
            }
            ValidationResult::Retry { reason } => {
                return Err(Error::Transaction(format!(
                    "Transaction {} should retry: {}",
                    tx_id, reason
                )));
            }
        }

        // Clean up resources
        self.cleanup_transaction(tx_id)?;

        info!("Committed transaction {}", tx_id);
        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, tx_id: TxId) -> Result<()> {
        debug!("Aborting transaction {}", tx_id);

        self.visibility_engine.abort_transaction(tx_id)?;
        self.serialization_validator.abort_transaction(tx_id)?;
        self.cleanup_transaction(tx_id)?;

        info!("Aborted transaction {}", tx_id);
        Ok(())
    }

    /// Acquire a lock for a transaction
    pub fn acquire_lock(
        &self,
        tx_id: TxId,
        granularity: LockGranularity,
        mode: LockMode,
    ) -> Result<()> {
        self.lock_manager.acquire_lock(tx_id, granularity, mode)
    }

    /// Check for deadlocks
    pub fn detect_deadlocks(&self) -> Result<Vec<DeadlockResult>> {
        let deadlocked_txs = self.deadlock_detector.detect_deadlocks()?;
        Ok(deadlocked_txs
            .into_iter()
            .map(|tx_id| DeadlockResult {
                deadlocked_txs: vec![tx_id],
                victim_tx: Some(tx_id),
                victim: Some(tx_id),
            })
            .collect())
    }

    /// Get current isolation level for a transaction
    pub fn get_isolation_level(&self, _tx_id: TxId) -> Result<IsolationLevel> {
        // Implementation would look up the transaction's isolation level
        Ok(IsolationLevel::default()) // Placeholder
    }

    /// Set global isolation configuration
    pub fn set_config(&self, config: IsolationConfig) {
        *self.config.write() = config;
    }

    /// Get current configuration
    pub fn get_config(&self) -> IsolationConfig {
        self.config.read().clone()
    }

    /// Get comprehensive statistics
    pub fn get_stats(&self) -> IsolationStats {
        IsolationStats {
            lock_stats: self.lock_manager.get_metrics(),
            deadlock_stats: self.deadlock_detector.get_stats(),
            visibility_stats: self.visibility_engine.get_stats(),
            snapshot_stats: self.snapshot_manager.get_stats(),
            conflict_stats: self.conflict_resolver.get_stats(),
            predicate_stats: self.predicate_manager.get_stats(),
            validation_stats: self.serialization_validator.get_stats(),
        }
    }

    /// Run maintenance tasks (cleanup, deadlock detection, etc.)
    pub fn run_maintenance(&self) -> Result<MaintenanceResults> {
        let deadlocks = self.deadlock_detector.detect_deadlocks()?.len();
        let snapshots_cleaned = self.snapshot_manager.cleanup_snapshots()?;
        let versions_cleaned = self.visibility_engine.cleanup_versions(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .saturating_sub(3600), // Clean versions older than 1 hour
        );
        let old_txs_cleaned = self.serialization_validator.cleanup_old_transactions(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .saturating_sub(1800), // Clean transactions older than 30 minutes
        );

        Ok(MaintenanceResults {
            deadlocks_detected: deadlocks,
            snapshots_cleaned,
            versions_cleaned,
            transactions_cleaned: old_txs_cleaned,
        })
    }

    /// Private helper to clean up transaction resources
    fn cleanup_transaction(&self, tx_id: TxId) -> Result<()> {
        self.lock_manager.release_locks(tx_id);
        self.deadlock_detector.unregister_transaction(tx_id);
        self.conflict_resolver.unregister_transaction(tx_id);
        self.predicate_manager.release_predicate_locks(tx_id);
        self.conflict_resolver
            .clear_conflicts_for_transaction(tx_id);
        Ok(())
    }
}

impl Default for IsolationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined statistics from all isolation components
#[derive(Debug, Clone)]
pub struct IsolationStats {
    pub lock_stats: locks::LockManagerMetrics,
    pub deadlock_stats: DeadlockStats,
    pub visibility_stats: VisibilityStats,
    pub snapshot_stats: SnapshotStats,
    pub conflict_stats: ConflictStats,
    pub predicate_stats: PredicateLockStats,
    pub validation_stats: ValidationStats,
}

/// Results from maintenance operations
#[derive(Debug, Clone)]
pub struct MaintenanceResults {
    pub deadlocks_detected: usize,
    pub snapshots_cleaned: usize,
    pub versions_cleaned: usize,
    pub transactions_cleaned: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_isolation_manager_basic() {
        let manager = IsolationManager::new();

        // Begin transaction
        manager
            .begin_transaction(1, IsolationLevel::ReadCommitted)
            .unwrap();

        // Acquire lock
        let lock_granularity = LockGranularity::Row(Bytes::from("test_key"));
        manager
            .acquire_lock(1, lock_granularity, LockMode::Shared)
            .unwrap();

        // Commit transaction
        manager.commit_transaction(1).unwrap();
    }

    #[test]
    fn test_deadlock_detection() {
        let manager = IsolationManager::new();

        manager
            .begin_transaction(1, IsolationLevel::Serializable)
            .unwrap();
        manager
            .begin_transaction(2, IsolationLevel::Serializable)
            .unwrap();

        // Create potential deadlock scenario
        let key1 = LockGranularity::Row(Bytes::from("key1"));
        let key2 = LockGranularity::Row(Bytes::from("key2"));

        manager
            .acquire_lock(1, key1.clone(), LockMode::Exclusive)
            .unwrap();
        manager
            .acquire_lock(2, key2.clone(), LockMode::Exclusive)
            .unwrap();

        // This should detect no deadlock yet (no circular dependency)
        let deadlocks = manager.detect_deadlocks().unwrap();
        assert!(deadlocks.is_empty());

        // Cleanup
        manager.abort_transaction(1).unwrap();
        manager.abort_transaction(2).unwrap();
    }

    #[test]
    fn test_isolation_levels() {
        let manager = IsolationManager::new();

        // Test different isolation levels
        let levels = [
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
            IsolationLevel::Snapshot,
        ];

        for (i, &level) in levels.iter().enumerate() {
            let tx_id = (i + 1) as u64;
            manager.begin_transaction(tx_id, level).unwrap();
            manager.commit_transaction(tx_id).unwrap();
        }
    }

    #[test]
    fn test_maintenance() {
        let manager = IsolationManager::new();

        // Run maintenance - should not fail
        let results = manager.run_maintenance().unwrap();

        // Should have some results (even if zero)
        assert!(results.deadlocks_detected == 0); // No transactions running
    }
}
