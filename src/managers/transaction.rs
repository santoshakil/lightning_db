use std::sync::Arc;
use std::collections::HashMap;
use crate::{Database, Result, TransactionStats};
use crate::features::transactions::isolation::{
    IsolationLevel, IsolationStats, DeadlockStats, SnapshotStats,
    ValidationStats, ConflictStats, IsolationConfig, MaintenanceResults,
    locks::{LockMode, LockManagerMetrics}, PredicateLockStats
};

pub struct TransactionManager {
    db: Arc<Database>,
}

impl TransactionManager {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
    
    pub fn begin_with_isolation(&self, isolation: IsolationLevel) -> Result<u64> {
        self.db.begin_transaction_with_isolation(isolation)
    }
    
    pub fn commit_with_validation(&self, tx_id: u64) -> Result<()> {
        self.db.commit_transaction_with_validation(tx_id)
    }
    
    pub fn abort_with_cleanup(&self, tx_id: u64) -> Result<()> {
        self.db.abort_transaction_with_cleanup(tx_id)
    }
    
    pub fn set_isolation(&self, level: IsolationLevel) -> Result<()> {
        self.db.set_transaction_isolation(level)
    }
    
    pub fn get_isolation(&self) -> IsolationLevel {
        self.db.get_transaction_isolation()
    }
    
    pub fn get_isolation_level(&self, tx_id: u64) -> Result<IsolationLevel> {
        self.db.get_transaction_isolation_level(tx_id)
    }
    
    pub fn enable_deadlock_detection(&self, enabled: bool) -> Result<()> {
        self.db.enable_deadlock_detection(enabled)
    }
    
    pub fn detect_deadlocks(&self) -> Result<Vec<u64>> {
        self.db.detect_deadlocks()
    }
    
    pub fn get_lock_info(&self) -> HashMap<String, Vec<(u64, String, bool)>> {
        self.db.get_lock_info()
    }
    
    pub fn get_isolation_stats(&self) -> IsolationStats {
        self.db.get_isolation_stats()
    }
    
    pub fn run_isolation_maintenance(&self) -> Result<MaintenanceResults> {
        self.db.run_isolation_maintenance()
    }
    
    pub fn acquire_lock(&self, tx_id: u64, key: &[u8], exclusive: bool) -> Result<()> {
        let lock_mode = if exclusive { LockMode::Exclusive } else { LockMode::Shared };
        self.db.acquire_lock(tx_id, key, lock_mode)
    }
    
    pub fn acquire_range_lock(
        &self,
        tx_id: u64,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<()> {
        self.db.acquire_range_lock(tx_id, start_key, end_key)
    }
    
    pub fn get_active_transactions(&self) -> Vec<u64> {
        self.db.get_active_transactions()
    }
    
    pub fn get_deadlock_stats(&self) -> DeadlockStats {
        self.db.get_deadlock_stats()
    }
    
    pub fn get_snapshot_stats(&self) -> SnapshotStats {
        self.db.get_snapshot_stats()
    }
    
    pub fn get_lock_manager_metrics(&self) -> LockManagerMetrics {
        self.db.get_lock_manager_metrics()
    }
    
    pub fn get_predicate_lock_stats(&self) -> PredicateLockStats {
        self.db.get_predicate_lock_stats()
    }
    
    pub fn cleanup_isolation_data(&self, max_age_seconds: u64) -> Result<(usize, usize)> {
        self.db.cleanup_isolation_data(max_age_seconds)
    }
    
    pub fn get_validation_stats(&self) -> ValidationStats {
        self.db.get_validation_stats()
    }
    
    pub fn get_conflict_stats(&self) -> ConflictStats {
        self.db.get_conflict_stats()
    }
    
    pub fn configure_isolation(&self, config: IsolationConfig) -> Result<()> {
        self.db.configure_isolation(config)
    }
    
    pub fn get_isolation_config(&self) -> IsolationConfig {
        self.db.get_isolation_config()
    }
    
    pub fn get_transaction_stats(&self) -> Option<TransactionStats> {
        self.db.get_transaction_statistics()
    }
    
    pub fn cleanup_old_transactions(&self, max_age_ms: u64) {
        self.db.cleanup_old_transactions(max_age_ms)
    }
}