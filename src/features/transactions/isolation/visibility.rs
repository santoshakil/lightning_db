use crate::core::error::{Error, Result};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, trace};

use super::levels::IsolationLevel;
use super::snapshot::{Snapshot, SnapshotId};

/// Transaction identifier
pub type TxId = u64;

/// Version identifier for MVCC
pub type Version = u64;

/// Transaction state for visibility decisions
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TxState {
    Active,
    Committed,
    Aborted,
}

/// Transaction information for visibility
#[derive(Debug, Clone)]
pub struct TxInfo {
    pub tx_id: TxId,
    pub state: TxState,
    pub commit_timestamp: Option<u64>,
    pub start_timestamp: u64,
    pub isolation_level: IsolationLevel,
    pub snapshot_id: Option<SnapshotId>,
}

/// Version information for a data item
#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub version: Version,
    pub tx_id: TxId,
    pub timestamp: u64,
    pub value: Option<Bytes>, // None for deletions
    pub is_committed: bool,
}

/// Visibility rules engine for MVCC transactions
#[derive(Debug)]
pub struct VisibilityEngine {
    /// Active and committed transactions
    transactions: Arc<RwLock<HashMap<TxId, TxInfo>>>,
    /// Version history for keys
    versions: Arc<RwLock<HashMap<Bytes, BTreeMap<Version, VersionInfo>>>>,
    /// Global timestamp counter
    timestamp_counter: Arc<AtomicU64>,
    /// Snapshots for repeatable read and serializable isolation
    snapshots: Arc<RwLock<HashMap<SnapshotId, Snapshot>>>,
    /// Next snapshot ID
    next_snapshot_id: Arc<AtomicU64>,
}

impl VisibilityEngine {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            versions: Arc::new(RwLock::new(HashMap::new())),
            timestamp_counter: Arc::new(AtomicU64::new(1)),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            next_snapshot_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self, tx_id: TxId, isolation_level: IsolationLevel) -> Result<()> {
        let start_timestamp = self.get_next_timestamp();

        let snapshot_id = if isolation_level.uses_snapshot_isolation() {
            let snapshot = self.create_snapshot(tx_id, start_timestamp)?;
            Some(snapshot.id)
        } else {
            None
        };

        let tx_info = TxInfo {
            tx_id,
            state: TxState::Active,
            commit_timestamp: None,
            start_timestamp,
            isolation_level,
            snapshot_id,
        };

        let mut transactions = self.transactions.write();
        transactions.insert(tx_id, tx_info);

        debug!(
            "Started transaction {} with isolation level {:?}",
            tx_id, isolation_level
        );
        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, tx_id: TxId) -> Result<()> {
        let commit_timestamp = self.get_next_timestamp();

        let mut transactions = self.transactions.write();
        if let Some(tx_info) = transactions.get_mut(&tx_id) {
            tx_info.state = TxState::Committed;
            tx_info.commit_timestamp = Some(commit_timestamp);

            // Mark all versions created by this transaction as committed
            let mut versions = self.versions.write();
            for version_map in versions.values_mut() {
                for version_info in version_map.values_mut() {
                    if version_info.tx_id == tx_id {
                        version_info.is_committed = true;
                        version_info.timestamp = commit_timestamp;
                    }
                }
            }

            debug!(
                "Committed transaction {} at timestamp {}",
                tx_id, commit_timestamp
            );
        } else {
            return Err(Error::TransactionNotFound { id: tx_id });
        }

        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, tx_id: TxId) -> Result<()> {
        let mut transactions = self.transactions.write();
        if let Some(tx_info) = transactions.get_mut(&tx_id) {
            tx_info.state = TxState::Aborted;

            // Remove all versions created by this transaction
            let mut versions = self.versions.write();
            for version_map in versions.values_mut() {
                version_map.retain(|_, version_info| version_info.tx_id != tx_id);
            }

            // Clean up snapshot if any
            if let Some(snapshot_id) = tx_info.snapshot_id {
                self.snapshots.write().remove(&snapshot_id);
            }

            debug!("Aborted transaction {}", tx_id);
        } else {
            return Err(Error::TransactionNotFound { id: tx_id });
        }

        Ok(())
    }

    /// Add a new version for a key
    pub fn add_version(
        &self,
        key: Bytes,
        value: Option<Bytes>,
        tx_id: TxId,
        version: Version,
    ) -> Result<()> {
        let timestamp = self.get_next_timestamp();

        let version_info = VersionInfo {
            version,
            tx_id,
            timestamp,
            value,
            is_committed: false, // Will be set to true on commit
        };

        let mut versions = self.versions.write();
        versions
            .entry(key.clone())
            .or_insert_with(BTreeMap::new)
            .insert(version, version_info);

        trace!(
            "Added version {} for key {:?} by transaction {}",
            version,
            key,
            tx_id
        );
        Ok(())
    }

    /// Check if a version is visible to a transaction
    pub fn is_visible(&self, tx_id: TxId, key: &Bytes, version: Version) -> Result<bool> {
        let transactions = self.transactions.read();
        let tx_info = transactions
            .get(&tx_id)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?;

        let versions = self.versions.read();
        let version_map = versions.get(key);
        if let Some(version_map) = version_map {
            if let Some(version_info) = version_map.get(&version) {
                return Ok(self.is_version_visible(tx_info, version_info));
            }
        }

        Ok(false)
    }

    /// Get the visible version for a key from a transaction's perspective
    pub fn get_visible_version(&self, tx_id: TxId, key: &Bytes) -> Result<Option<VersionInfo>> {
        let transactions = self.transactions.read();
        let tx_info = transactions
            .get(&tx_id)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?;

        let versions = self.versions.read();
        if let Some(version_map) = versions.get(key) {
            // Find the latest visible version
            for (_, version_info) in version_map.iter().rev() {
                if self.is_version_visible(tx_info, version_info) {
                    return Ok(Some(version_info.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Get all visible versions for a key range (for range queries)
    pub fn get_visible_versions_in_range(
        &self,
        tx_id: TxId,
        start_key: Option<&Bytes>,
        end_key: Option<&Bytes>,
    ) -> Result<Vec<(Bytes, VersionInfo)>> {
        let transactions = self.transactions.read();
        let tx_info = transactions
            .get(&tx_id)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?;

        let versions = self.versions.read();
        let mut result = Vec::new();

        for (key, version_map) in versions.iter() {
            // Check if key is in range
            if let Some(start) = start_key {
                if key < start {
                    continue;
                }
            }
            if let Some(end) = end_key {
                if key >= end {
                    continue;
                }
            }

            // Find latest visible version for this key
            for (_, version_info) in version_map.iter().rev() {
                if self.is_version_visible(tx_info, version_info) {
                    result.push((key.clone(), version_info.clone()));
                    break;
                }
            }
        }

        Ok(result)
    }

    /// Check for write-write conflicts
    pub fn check_write_conflict(&self, tx_id: TxId, key: &Bytes) -> Result<bool> {
        let transactions = self.transactions.read();
        let tx_info = transactions
            .get(&tx_id)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?;

        let versions = self.versions.read();
        if let Some(version_map) = versions.get(key) {
            for version_info in version_map.values() {
                // Check if another transaction has written to this key after our start time
                if version_info.tx_id != tx_id
                    && version_info.timestamp > tx_info.start_timestamp
                    && (version_info.is_committed
                        || self.is_transaction_committed(version_info.tx_id))
                {
                    return Ok(true); // Conflict detected
                }
            }
        }

        Ok(false)
    }

    /// Check for phantom reads (range conflicts)
    pub fn check_phantom_read(
        &self,
        tx_id: TxId,
        start_key: Option<&Bytes>,
        end_key: Option<&Bytes>,
    ) -> Result<bool> {
        let transactions = self.transactions.read();
        let tx_info = transactions
            .get(&tx_id)
            .ok_or_else(|| Error::TransactionNotFound { id: tx_id })?;

        // Only check for serializable isolation
        if tx_info.isolation_level != IsolationLevel::Serializable {
            return Ok(false);
        }

        let versions = self.versions.read();

        for (key, version_map) in versions.iter() {
            // Check if key is in range
            if let Some(start) = start_key {
                if key < start {
                    continue;
                }
            }
            if let Some(end) = end_key {
                if key >= end {
                    continue;
                }
            }

            // Check if any new versions were added after our transaction started
            for version_info in version_map.values() {
                if version_info.tx_id != tx_id
                    && version_info.timestamp > tx_info.start_timestamp
                    && (version_info.is_committed
                        || self.is_transaction_committed(version_info.tx_id))
                {
                    return Ok(true); // Phantom read detected
                }
            }
        }

        Ok(false)
    }

    /// Clean up committed versions (garbage collection)
    pub fn cleanup_versions(&self, before_timestamp: u64) -> usize {
        let mut versions = self.versions.write();
        let mut cleaned = 0;

        for version_map in versions.values_mut() {
            let original_len = version_map.len();

            // Keep only the latest committed version and any uncommitted versions
            let mut to_keep = BTreeMap::new();
            let mut latest_committed = None;

            for (&version, version_info) in version_map.iter().rev() {
                if version_info.is_committed && version_info.timestamp < before_timestamp {
                    if latest_committed.is_none() {
                        latest_committed = Some((version, version_info.clone()));
                    }
                } else {
                    to_keep.insert(version, version_info.clone());
                }
            }

            if let Some((version, version_info)) = latest_committed {
                to_keep.insert(version, version_info);
            }

            cleaned += original_len - to_keep.len();
            *version_map = to_keep;
        }

        // Remove empty version maps
        versions.retain(|_, version_map| !version_map.is_empty());

        if cleaned > 0 {
            debug!("Cleaned up {} old versions", cleaned);
        }

        cleaned
    }

    /// Get statistics
    pub fn get_stats(&self) -> VisibilityStats {
        let transactions = self.transactions.read();
        let versions = self.versions.read();
        let snapshots = self.snapshots.read();

        let total_versions: usize = versions.values().map(|vm| vm.len()).sum();

        VisibilityStats {
            active_transactions: transactions.len(),
            total_versions,
            active_snapshots: snapshots.len(),
            current_timestamp: self.timestamp_counter.load(Ordering::Relaxed),
        }
    }

    /// Private helper: Check if a version is visible to a transaction
    fn is_version_visible(&self, tx_info: &TxInfo, version_info: &VersionInfo) -> bool {
        // Own writes are always visible
        if version_info.tx_id == tx_info.tx_id {
            return true;
        }

        match tx_info.isolation_level {
            IsolationLevel::ReadUncommitted => {
                // Can see uncommitted changes from other transactions
                true
            }
            IsolationLevel::ReadCommitted => {
                // Can only see committed changes
                version_info.is_committed || self.is_transaction_committed(version_info.tx_id)
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Use snapshot isolation
                if let Some(snapshot_id) = tx_info.snapshot_id {
                    self.is_version_in_snapshot(version_info, snapshot_id)
                } else {
                    // Fallback to timestamp-based visibility
                    version_info.is_committed && version_info.timestamp <= tx_info.start_timestamp
                }
            }
            IsolationLevel::Snapshot => {
                // Pure snapshot isolation
                if let Some(snapshot_id) = tx_info.snapshot_id {
                    self.is_version_in_snapshot(version_info, snapshot_id)
                } else {
                    false
                }
            }
        }
    }

    /// Check if a transaction is committed
    fn is_transaction_committed(&self, tx_id: TxId) -> bool {
        if let Some(tx_info) = self.transactions.read().get(&tx_id) {
            tx_info.state == TxState::Committed
        } else {
            false
        }
    }

    /// Create a snapshot for a transaction
    fn create_snapshot(&self, tx_id: TxId, start_timestamp: u64) -> Result<Snapshot> {
        let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);

        // Get list of committed transactions at this point
        let committed_txs: HashSet<TxId> = self
            .transactions
            .read()
            .values()
            .filter(|tx| {
                tx.state == TxState::Committed
                    && tx.commit_timestamp.unwrap_or(0) <= start_timestamp
            })
            .map(|tx| tx.tx_id)
            .collect();

        let snapshot = Snapshot {
            id: snapshot_id,
            tx_id,
            start_timestamp,
            committed_txs,
        };

        self.snapshots.write().insert(snapshot_id, snapshot.clone());

        debug!(
            "Created snapshot {} for transaction {} with {} committed transactions",
            snapshot_id,
            tx_id,
            snapshot.committed_txs.len()
        );

        Ok(snapshot)
    }

    /// Check if a version is visible in a snapshot
    fn is_version_in_snapshot(&self, version_info: &VersionInfo, snapshot_id: SnapshotId) -> bool {
        if let Some(snapshot) = self.snapshots.read().get(&snapshot_id) {
            // Version is visible if it was committed before the snapshot
            version_info.is_committed && snapshot.committed_txs.contains(&version_info.tx_id)
        } else {
            false
        }
    }

    /// Get next timestamp
    fn get_next_timestamp(&self) -> u64 {
        self.timestamp_counter.fetch_add(1, Ordering::Relaxed)
    }
}

/// Visibility engine statistics
#[derive(Debug, Clone)]
pub struct VisibilityStats {
    pub active_transactions: usize,
    pub total_versions: usize,
    pub active_snapshots: usize,
    pub current_timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_committed_visibility() {
        let engine = VisibilityEngine::new();

        // Start two transactions
        engine
            .begin_transaction(1, IsolationLevel::ReadCommitted)
            .unwrap();
        engine
            .begin_transaction(2, IsolationLevel::ReadCommitted)
            .unwrap();

        let key = Bytes::from("test_key");

        // Transaction 1 writes a value
        engine
            .add_version(key.clone(), Some(Bytes::from("value1")), 1, 1)
            .unwrap();

        // Transaction 2 should not see uncommitted write
        let visible = engine.get_visible_version(2, &key).unwrap();
        assert!(visible.is_none());

        // Commit transaction 1
        engine.commit_transaction(1).unwrap();

        // Now transaction 2 should see the committed value
        let visible = engine.get_visible_version(2, &key).unwrap();
        assert!(visible.is_some());
        assert_eq!(visible.unwrap().value, Some(Bytes::from("value1")));
    }

    #[test]
    fn test_repeatable_read_isolation() {
        let engine = VisibilityEngine::new();

        // Start transaction with repeatable read
        engine
            .begin_transaction(1, IsolationLevel::RepeatableRead)
            .unwrap();
        engine
            .begin_transaction(2, IsolationLevel::ReadCommitted)
            .unwrap();

        let key = Bytes::from("test_key");

        // Initial value
        engine
            .add_version(key.clone(), Some(Bytes::from("initial")), 2, 1)
            .unwrap();
        engine.commit_transaction(2).unwrap();

        // Transaction 1 reads the value
        let visible1 = engine.get_visible_version(1, &key).unwrap();
        assert!(visible1.is_some());

        // Another transaction modifies the value
        engine
            .begin_transaction(3, IsolationLevel::ReadCommitted)
            .unwrap();
        engine
            .add_version(key.clone(), Some(Bytes::from("modified")), 3, 2)
            .unwrap();
        engine.commit_transaction(3).unwrap();

        // Transaction 1 should still see the original value (repeatable read)
        let visible2 = engine.get_visible_version(1, &key).unwrap();
        assert!(visible2.is_some());
        assert_eq!(visible2.unwrap().value, Some(Bytes::from("initial")));
    }

    #[test]
    fn test_write_conflict_detection() {
        let engine = VisibilityEngine::new();

        engine
            .begin_transaction(1, IsolationLevel::RepeatableRead)
            .unwrap();
        engine
            .begin_transaction(2, IsolationLevel::RepeatableRead)
            .unwrap();

        let key = Bytes::from("conflict_key");

        // Both transactions try to write to the same key
        engine
            .add_version(key.clone(), Some(Bytes::from("value1")), 1, 1)
            .unwrap();
        engine
            .add_version(key.clone(), Some(Bytes::from("value2")), 2, 2)
            .unwrap();

        // Commit transaction 1
        engine.commit_transaction(1).unwrap();

        // Check for write conflict for transaction 2
        let conflict = engine.check_write_conflict(2, &key).unwrap();
        assert!(conflict);
    }
}
