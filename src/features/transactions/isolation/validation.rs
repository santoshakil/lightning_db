use crate::core::error::{Error, Result};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::debug;

use super::levels::IsolationLevel;
use super::locks::TxId;

/// Validation result for serialization
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    /// Transaction can commit
    Valid,
    /// Transaction must abort due to conflicts
    Invalid { reason: String, conflicts: Vec<TxId> },
    /// Transaction must retry
    Retry { reason: String },
}

/// Read/write set for optimistic concurrency control
#[derive(Debug, Clone)]
pub struct TransactionReadSet {
    pub keys: HashSet<Bytes>,
    pub versions: HashMap<Bytes, u64>,
    pub ranges: Vec<(Option<Bytes>, Option<Bytes>)>,
    pub first_read_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct TransactionWriteSet {
    pub keys: HashSet<Bytes>,
    pub values: HashMap<Bytes, Option<Bytes>>, // None for deletions
    pub versions: HashMap<Bytes, u64>,
    pub first_write_timestamp: u64,
}

/// Transaction validation information
#[derive(Debug, Clone)]
pub struct ValidationInfo {
    pub tx_id: TxId,
    pub isolation_level: IsolationLevel,
    pub start_timestamp: u64,
    pub read_set: TransactionReadSet,
    pub write_set: TransactionWriteSet,
    pub validation_timestamp: Option<u64>,
    pub committed: bool,
}

/// Serialization validator for optimistic concurrency control
#[derive(Debug)]
pub struct SerializationValidator {
    /// Transaction validation info
    tx_info: Arc<RwLock<HashMap<TxId, ValidationInfo>>>,
    /// Global timestamp for ordering
    global_timestamp: Arc<AtomicU64>,
    /// Committed transactions for validation
    committed_transactions: Arc<RwLock<BTreeSet<(u64, TxId)>>>, // (commit_timestamp, tx_id)
    /// Active validation timestamp ranges
    active_validations: Arc<RwLock<HashMap<TxId, (u64, u64)>>>, // tx_id -> (start, end)
    /// Statistics
    stats: Arc<RwLock<ValidationStats>>,
}

#[derive(Debug, Default, Clone)]
pub struct ValidationStats {
    pub validations_performed: u64,
    pub validations_passed: u64,
    pub validations_failed: u64,
    pub retries_requested: u64,
    pub active_transactions: usize,
}

impl SerializationValidator {
    pub fn new() -> Self {
        Self {
            tx_info: Arc::new(RwLock::new(HashMap::new())),
            global_timestamp: Arc::new(AtomicU64::new(1)),
            committed_transactions: Arc::new(RwLock::new(BTreeSet::new())),
            active_validations: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ValidationStats::default())),
        }
    }

    /// Begin tracking a transaction for validation
    pub fn begin_transaction(
        &self,
        tx_id: TxId,
        isolation_level: IsolationLevel,
    ) -> Result<()> {
        let start_timestamp = self.get_next_timestamp();
        
        let validation_info = ValidationInfo {
            tx_id,
            isolation_level,
            start_timestamp,
            read_set: TransactionReadSet {
                keys: HashSet::new(),
                versions: HashMap::new(),
                ranges: Vec::new(),
                first_read_timestamp: start_timestamp,
            },
            write_set: TransactionWriteSet {
                keys: HashSet::new(),
                values: HashMap::new(),
                versions: HashMap::new(),
                first_write_timestamp: start_timestamp,
            },
            validation_timestamp: None,
            committed: false,
        };

        self.tx_info.write().insert(tx_id, validation_info);
        debug!("Started validation tracking for transaction {}", tx_id);

        Ok(())
    }

    /// Record a read operation
    pub fn record_read(
        &self,
        tx_id: TxId,
        key: Bytes,
        version: u64,
    ) -> Result<()> {
        let mut tx_info = self.tx_info.write();
        if let Some(info) = tx_info.get_mut(&tx_id) {
            info.read_set.keys.insert(key.clone());
            info.read_set.versions.insert(key, version);
        }
        Ok(())
    }

    /// Record a write operation
    pub fn record_write(
        &self,
        tx_id: TxId,
        key: Bytes,
        value: Option<Bytes>,
        version: u64,
    ) -> Result<()> {
        let mut tx_info = self.tx_info.write();
        if let Some(info) = tx_info.get_mut(&tx_id) {
            info.write_set.keys.insert(key.clone());
            info.write_set.values.insert(key.clone(), value);
            info.write_set.versions.insert(key, version);
        }
        Ok(())
    }

    /// Record a range read operation
    pub fn record_range_read(
        &self,
        tx_id: TxId,
        start_key: Option<Bytes>,
        end_key: Option<Bytes>,
    ) -> Result<()> {
        let mut tx_info = self.tx_info.write();
        if let Some(info) = tx_info.get_mut(&tx_id) {
            info.read_set.ranges.push((start_key, end_key));
        }
        Ok(())
    }

    /// Validate a transaction for commit (optimistic concurrency control)
    pub fn validate_transaction(&self, tx_id: TxId) -> Result<ValidationResult> {
        let validation_timestamp = self.get_next_timestamp();
        
        // Update validation timestamp
        {
            let mut tx_info = self.tx_info.write();
            if let Some(info) = tx_info.get_mut(&tx_id) {
                info.validation_timestamp = Some(validation_timestamp);
            } else {
                return Err(Error::TransactionNotFound { id: tx_id });
            }
        }

        self.active_validations.write().insert(tx_id, 
            (self.get_transaction_start_time(tx_id)?, validation_timestamp));

        let result = self.perform_validation(tx_id, validation_timestamp)?;
        
        self.active_validations.write().remove(&tx_id);
        
        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.validations_performed += 1;
            match &result {
                ValidationResult::Valid => stats.validations_passed += 1,
                ValidationResult::Invalid { .. } => stats.validations_failed += 1,
                ValidationResult::Retry { .. } => stats.retries_requested += 1,
            }
        }

        debug!("Validation result for transaction {}: {:?}", tx_id, result);
        Ok(result)
    }

    /// Commit a transaction after successful validation
    pub fn commit_transaction(&self, tx_id: TxId) -> Result<()> {
        let commit_timestamp = self.get_next_timestamp();
        
        {
            let mut tx_info = self.tx_info.write();
            if let Some(info) = tx_info.get_mut(&tx_id) {
                info.committed = true;
            } else {
                return Err(Error::TransactionNotFound { id: tx_id });
            }
        }

        // Add to committed transactions
        self.committed_transactions.write().insert((commit_timestamp, tx_id));
        
        debug!("Committed transaction {} at timestamp {}", tx_id, commit_timestamp);
        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, tx_id: TxId) -> Result<()> {
        self.tx_info.write().remove(&tx_id);
        self.active_validations.write().remove(&tx_id);
        debug!("Aborted transaction {}", tx_id);
        Ok(())
    }

    /// Check for write-write conflicts (backward validation)
    pub fn check_write_write_conflicts(&self, tx_id: TxId) -> Result<Vec<TxId>> {
        let tx_info = self.tx_info.read();
        let validation_info = tx_info.get(&tx_id)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?;

        let mut conflicts = Vec::new();
        let committed_transactions = self.committed_transactions.read();

        // Check against all transactions that committed after our start time
        for &(commit_ts, committed_tx_id) in committed_transactions.iter() {
            if commit_ts > validation_info.start_timestamp {
                if let Some(committed_info) = tx_info.get(&committed_tx_id) {
                    // Check for overlapping write sets
                    for write_key in &validation_info.write_set.keys {
                        if committed_info.write_set.keys.contains(write_key) {
                            conflicts.push(committed_tx_id);
                            break;
                        }
                    }
                }
            }
        }

        Ok(conflicts)
    }

    /// Check for read-write conflicts (forward validation)
    pub fn check_read_write_conflicts(&self, tx_id: TxId) -> Result<Vec<TxId>> {
        let tx_info = self.tx_info.read();
        let validation_info = tx_info.get(&tx_id)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?;

        let mut conflicts = Vec::new();

        // Check against all active transactions that started after us
        for (other_tx_id, other_info) in tx_info.iter() {
            if *other_tx_id != tx_id 
                && other_info.start_timestamp > validation_info.start_timestamp
                && !other_info.committed
            {
                // Check if our writes conflict with their reads
                for write_key in &validation_info.write_set.keys {
                    if other_info.read_set.keys.contains(write_key) {
                        conflicts.push(*other_tx_id);
                        break;
                    }
                }
            }
        }

        Ok(conflicts)
    }

    /// Check for phantom read conflicts
    pub fn check_phantom_conflicts(&self, tx_id: TxId) -> Result<Vec<TxId>> {
        let tx_info = self.tx_info.read();
        let validation_info = tx_info.get(&tx_id)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?;

        // Only check for serializable isolation
        if validation_info.isolation_level != IsolationLevel::Serializable {
            return Ok(Vec::new());
        }

        let mut conflicts = Vec::new();
        let committed_transactions = self.committed_transactions.read();

        // Check if any committed transaction inserted keys in our read ranges
        for &(commit_ts, committed_tx_id) in committed_transactions.iter() {
            if commit_ts > validation_info.start_timestamp {
                if let Some(committed_info) = tx_info.get(&committed_tx_id) {
                    for (start_key, end_key) in &validation_info.read_set.ranges {
                        for write_key in &committed_info.write_set.keys {
                            if self.key_in_range(write_key, start_key.as_ref(), end_key.as_ref()) {
                                conflicts.push(committed_tx_id);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(conflicts)
    }

    /// Perform validation based on isolation level
    fn perform_validation(&self, tx_id: TxId, _validation_timestamp: u64) -> Result<ValidationResult> {
        let isolation_level = {
            let tx_info = self.tx_info.read();
            tx_info.get(&tx_id)
                .map(|info| info.isolation_level)
                .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?
        };

        match isolation_level {
            IsolationLevel::ReadUncommitted => {
                // No validation needed
                Ok(ValidationResult::Valid)
            }
            IsolationLevel::ReadCommitted => {
                // Check write-write conflicts only
                let conflicts = self.check_write_write_conflicts(tx_id)?;
                if conflicts.is_empty() {
                    Ok(ValidationResult::Valid)
                } else {
                    Ok(ValidationResult::Invalid {
                        reason: "Write-write conflict detected".to_string(),
                        conflicts,
                    })
                }
            }
            IsolationLevel::RepeatableRead => {
                // Check write-write and read-write conflicts
                let ww_conflicts = self.check_write_write_conflicts(tx_id)?;
                let rw_conflicts = self.check_read_write_conflicts(tx_id)?;
                
                let all_conflicts: Vec<TxId> = ww_conflicts.into_iter()
                    .chain(rw_conflicts.into_iter())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();

                if all_conflicts.is_empty() {
                    Ok(ValidationResult::Valid)
                } else {
                    Ok(ValidationResult::Invalid {
                        reason: "Read-write or write-write conflict detected".to_string(),
                        conflicts: all_conflicts,
                    })
                }
            }
            IsolationLevel::Serializable => {
                // Full validation including phantom detection
                let ww_conflicts = self.check_write_write_conflicts(tx_id)?;
                let rw_conflicts = self.check_read_write_conflicts(tx_id)?;
                let phantom_conflicts = self.check_phantom_conflicts(tx_id)?;
                
                let all_conflicts: Vec<TxId> = ww_conflicts.into_iter()
                    .chain(rw_conflicts.into_iter())
                    .chain(phantom_conflicts.into_iter())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();

                if all_conflicts.is_empty() {
                    Ok(ValidationResult::Valid)
                } else {
                    Ok(ValidationResult::Invalid {
                        reason: "Serialization conflict detected".to_string(),
                        conflicts: all_conflicts,
                    })
                }
            }
            IsolationLevel::Snapshot => {
                // Snapshot isolation validation
                self.validate_snapshot_isolation(tx_id)
            }
        }
    }

    /// Validate snapshot isolation (check for write-write conflicts only)
    fn validate_snapshot_isolation(&self, tx_id: TxId) -> Result<ValidationResult> {
        let conflicts = self.check_write_write_conflicts(tx_id)?;
        
        if conflicts.is_empty() {
            Ok(ValidationResult::Valid)
        } else {
            // For snapshot isolation, write-write conflicts should retry
            Ok(ValidationResult::Retry {
                reason: "Write-write conflict in snapshot isolation - retry recommended".to_string(),
            })
        }
    }

    /// Get transaction start time
    fn get_transaction_start_time(&self, tx_id: TxId) -> Result<u64> {
        let tx_info = self.tx_info.read();
        tx_info.get(&tx_id)
            .map(|info| info.start_timestamp)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })
    }

    /// Check if a key is in a range
    fn key_in_range(&self, key: &Bytes, start: Option<&Bytes>, end: Option<&Bytes>) -> bool {
        match (start, end) {
            (None, None) => true,
            (Some(s), None) => key >= s,
            (None, Some(e)) => key < e,
            (Some(s), Some(e)) => key >= s && key < e,
        }
    }

    /// Get next timestamp
    fn get_next_timestamp(&self) -> u64 {
        self.global_timestamp.fetch_add(1, Ordering::Relaxed)
    }

    /// Get current statistics
    pub fn get_stats(&self) -> ValidationStats {
        let mut stats = self.stats.write();
        stats.active_transactions = self.tx_info.read().len();
        stats.clone()
    }

    /// Clean up old committed transactions
    pub fn cleanup_old_transactions(&self, before_timestamp: u64) -> usize {
        let mut committed = self.committed_transactions.write();
        let original_len = committed.len();
        
        // Keep transactions that committed after the cutoff
        *committed = committed.split_off(&(before_timestamp, 0));
        
        let cleaned = original_len - committed.len();
        if cleaned > 0 {
            debug!("Cleaned up {} old committed transactions", cleaned);
        }
        
        cleaned
    }

    /// Get validation info for debugging
    pub fn get_validation_info(&self, tx_id: TxId) -> Option<ValidationInfo> {
        self.tx_info.read().get(&tx_id).cloned()
    }

    /// Get all active transaction IDs
    pub fn get_active_transactions(&self) -> Vec<TxId> {
        self.tx_info.read().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_basic() {
        let validator = SerializationValidator::new();
        
        // Begin transaction
        validator.begin_transaction(1, IsolationLevel::ReadCommitted).unwrap();
        
        // Record operations
        validator.record_read(1, Bytes::from("key1"), 1).unwrap();
        validator.record_write(1, Bytes::from("key2"), Some(Bytes::from("value2")), 1).unwrap();
        
        // Validate
        let result = validator.validate_transaction(1).unwrap();
        assert_eq!(result, ValidationResult::Valid);
        
        // Commit
        validator.commit_transaction(1).unwrap();
    }

    #[test]
    fn test_write_write_conflict() {
        let validator = SerializationValidator::new();
        
        // First transaction
        validator.begin_transaction(1, IsolationLevel::ReadCommitted).unwrap();
        validator.record_write(1, Bytes::from("key1"), Some(Bytes::from("value1")), 1).unwrap();
        validator.validate_transaction(1).unwrap();
        validator.commit_transaction(1).unwrap();
        
        // Second transaction writing to same key
        validator.begin_transaction(2, IsolationLevel::ReadCommitted).unwrap();
        validator.record_write(2, Bytes::from("key1"), Some(Bytes::from("value2")), 2).unwrap();
        
        let result = validator.validate_transaction(2).unwrap();
        match result {
            ValidationResult::Invalid { conflicts, .. } => {
                assert!(conflicts.contains(&1));
            }
            _ => panic!("Expected validation failure"),
        }
    }

    #[test]
    fn test_serializable_validation() {
        let validator = SerializationValidator::new();
        
        // Transaction with range read
        validator.begin_transaction(1, IsolationLevel::Serializable).unwrap();
        validator.record_range_read(1, Some(Bytes::from("a")), Some(Bytes::from("z"))).unwrap();
        
        // Another transaction that commits an insert in the range
        validator.begin_transaction(2, IsolationLevel::Serializable).unwrap();
        validator.record_write(2, Bytes::from("m"), Some(Bytes::from("new_value")), 1).unwrap();
        validator.validate_transaction(2).unwrap();
        validator.commit_transaction(2).unwrap();
        
        // First transaction should fail validation due to phantom
        let result = validator.validate_transaction(1).unwrap();
        match result {
            ValidationResult::Invalid { reason, .. } => {
                assert!(reason.contains("conflict"));
            }
            _ => panic!("Expected validation failure due to phantom read"),
        }
    }

    #[test]
    fn test_snapshot_isolation_retry() {
        let validator = SerializationValidator::new();
        
        // Two transactions with write-write conflict in snapshot isolation
        validator.begin_transaction(1, IsolationLevel::Snapshot).unwrap();
        validator.begin_transaction(2, IsolationLevel::Snapshot).unwrap();
        
        validator.record_write(1, Bytes::from("key1"), Some(Bytes::from("value1")), 1).unwrap();
        validator.record_write(2, Bytes::from("key1"), Some(Bytes::from("value2")), 2).unwrap();
        
        // First to validate should succeed
        let result1 = validator.validate_transaction(1).unwrap();
        assert_eq!(result1, ValidationResult::Valid);
        validator.commit_transaction(1).unwrap();
        
        // Second should get retry recommendation
        let result2 = validator.validate_transaction(2).unwrap();
        match result2 {
            ValidationResult::Retry { .. } => {
                // Expected behavior for snapshot isolation
            }
            _ => panic!("Expected retry for snapshot isolation conflict"),
        }
    }
}