pub mod optimized_manager;
pub mod mvcc;

use crate::error::{Error, Result};
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use optimized_manager::{OptimizedTransactionManager, TransactionStatistics};
pub use mvcc::{MVCCTransactionManager, MVCCTransaction, MVCCVersionStore};

#[derive(Debug, Clone, PartialEq)]
pub enum TxState {
    Active,
    Preparing, // New state for validation phase
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct WriteOp {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // None = delete
    pub prev_version: u64,
}

#[derive(Debug, Clone)]
pub struct ReadOp {
    pub key: Vec<u8>,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: u64,
    pub read_timestamp: u64,
    pub commit_timestamp: Option<u64>,
    pub write_set: Vec<WriteOp>,
    pub read_set: Vec<ReadOp>,
    pub state: TxState,
    pub start_time: Instant,
}

impl Transaction {
    pub fn new(id: u64, read_timestamp: u64) -> Self {
        Self {
            id,
            read_timestamp,
            commit_timestamp: None,
            write_set: Vec::new(),
            read_set: Vec::new(),
            state: TxState::Active,
            start_time: Instant::now(),
        }
    }

    pub fn add_write(&mut self, key: Vec<u8>, value: Option<Vec<u8>>, prev_version: u64) {
        // Check if we already have a write for this key
        if let Some(pos) = self.write_set.iter().position(|w| w.key == key) {
            self.write_set[pos] = WriteOp {
                key,
                value,
                prev_version,
            };
        } else {
            self.write_set.push(WriteOp {
                key,
                value,
                prev_version,
            });
        }
    }

    pub fn add_read(&mut self, key: Vec<u8>, version: u64) {
        // Check if we already have a read for this key
        if !self.read_set.iter().any(|r| r.key == key) {
            self.read_set.push(ReadOp { key, version });
        }
    }

    pub fn is_active(&self) -> bool {
        self.state == TxState::Active
    }

    pub fn prepare(&mut self) {
        self.state = TxState::Preparing;
    }

    pub fn commit(&mut self, timestamp: u64) {
        self.state = TxState::Committed;
        self.commit_timestamp = Some(timestamp);
    }

    pub fn abort(&mut self) {
        self.state = TxState::Aborted;
    }

    pub fn get_write(&self, key: &[u8]) -> Option<&WriteOp> {
        self.write_set.iter().find(|w| w.key == key)
    }
}

pub struct TransactionManager {
    next_tx_id: AtomicU64,
    active_transactions: Arc<SkipMap<u64, Arc<RwLock<Transaction>>>>,
    commit_timestamp: AtomicU64,
    max_active_transactions: usize,
    version_store: Arc<VersionStore>,
}

impl TransactionManager {
    pub fn new(max_active_transactions: usize, version_store: Arc<VersionStore>) -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            active_transactions: Arc::new(SkipMap::new()),
            commit_timestamp: AtomicU64::new(1),
            max_active_transactions,
            version_store,
        }
    }

    pub fn begin(&self) -> Result<u64> {
        // Check transaction limit
        if self.active_transactions.len() >= self.max_active_transactions {
            return Err(Error::TransactionLimitReached {
                limit: self.max_active_transactions,
            });
        }

        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let read_timestamp = self.commit_timestamp.load(Ordering::SeqCst);

        let tx = Arc::new(RwLock::new(Transaction::new(tx_id, read_timestamp)));
        self.active_transactions.insert(tx_id, tx);

        Ok(tx_id)
    }

    pub fn get_transaction(&self, tx_id: u64) -> Result<Arc<RwLock<Transaction>>> {
        self.active_transactions
            .get(&tx_id)
            .map(|entry| entry.value().clone())
            .ok_or(Error::TransactionNotFound { id: tx_id })
    }

    pub fn commit(&self, tx_id: u64) -> Result<()> {
        // Phase 1: Prepare and validate
        let tx_arc = self.get_transaction(tx_id)?;

        // Clone transaction data to minimize lock time
        let tx_data = {
            let mut tx = tx_arc.write();
            if !tx.is_active() {
                return Err(Error::TransactionInvalidState {
                    id: tx_id,
                    state: format!("{:?}", tx.state),
                });
            }
            tx.prepare(); // Mark as preparing
            tx.clone()
        };

        // Phase 2: Validate without holding locks
        self.validate_transaction(&tx_data)?;

        // Phase 3: Get commit timestamp and apply writes
        let commit_ts = self.commit_timestamp.fetch_add(1, Ordering::SeqCst) + 1;

        // Apply writes to version store
        for write_op in &tx_data.write_set {
            self.version_store.put(
                write_op.key.clone(),
                write_op.value.clone(),
                commit_ts,
                tx_id,
            );
        }

        // Phase 4: Mark transaction as committed
        {
            let mut tx = tx_arc.write();
            tx.commit(commit_ts);
        }

        // Phase 5: Remove from active transactions
        self.active_transactions.remove(&tx_id);

        Ok(())
    }

    pub fn abort(&self, tx_id: u64) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;

        {
            let mut tx = tx_arc.write();
            if !tx.is_active() && tx.state != TxState::Preparing {
                return Err(Error::TransactionInvalidState {
                    id: tx_id,
                    state: format!("{:?}", tx.state),
                });
            }
            tx.abort();
        }

        self.active_transactions.remove(&tx_id);
        Ok(())
    }

    fn validate_transaction(&self, tx: &Transaction) -> Result<()> {
        // For basic TransactionManager, we use optimistic concurrency control
        // Write-write conflicts are resolved by first-committer-wins
        // We don't check against other active transactions here, only committed state
        
        // Check for write-write conflicts against committed data only
        for write_op in &tx.write_set {
            // Check if someone else has committed a write to this key after our read timestamp
            if let Some(latest_version) = self.version_store.get_latest_version(&write_op.key) {
                if latest_version > tx.read_timestamp {
                    return Err(Error::Transaction(
                        "Write-write conflict detected".to_string(),
                    ));
                }
            }
        }

        // Check for read-write conflicts (snapshot isolation)
        for read_op in &tx.read_set {
            // Check if the version we read is still the latest
            let current_version = self
                .version_store
                .get_latest_version(&read_op.key)
                .unwrap_or(0);

            if current_version > read_op.version {
                // Someone committed a write to this key after we read it
                return Err(Error::Transaction(
                    "Read-write conflict detected".to_string(),
                ));
            }
        }

        Ok(())
    }

    pub fn cleanup_old_transactions(&self, max_age: Duration) {
        let mut to_remove = Vec::new();

        for entry in self.active_transactions.iter() {
            if let Some(tx) = entry.value().try_read() {
                if tx.start_time.elapsed() > max_age {
                    to_remove.push(tx.id);
                }
            }
        }

        for tx_id in to_remove {
            let _ = self.abort(tx_id);
        }
    }

    pub fn active_transaction_count(&self) -> usize {
        self.active_transactions.len()
    }

    pub fn get_read_timestamp(&self) -> u64 {
        self.commit_timestamp.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub value: Option<Vec<u8>>, // None represents deletion
    pub timestamp: u64,
    pub tx_id: u64,
}

impl VersionedValue {
    pub fn new(value: Option<Vec<u8>>, timestamp: u64, tx_id: u64) -> Self {
        Self {
            value,
            timestamp,
            tx_id,
        }
    }
}

pub struct VersionStore {
    // key -> (timestamp -> value)
    versions: SkipMap<Vec<u8>, Arc<SkipMap<u64, VersionedValue>>>,
}

impl VersionStore {
    pub fn new() -> Self {
        Self {
            versions: SkipMap::new(),
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>, timestamp: u64, tx_id: u64) {
        let versioned_value = VersionedValue::new(value, timestamp, tx_id);

        if let Some(key_versions) = self.versions.get(&key) {
            key_versions.value().insert(timestamp, versioned_value);
        } else {
            let key_versions = Arc::new(SkipMap::new());
            key_versions.insert(timestamp, versioned_value);
            self.versions.insert(key, key_versions);
        }
    }

    pub fn get(&self, key: &[u8], read_timestamp: u64) -> Option<Vec<u8>> {
        self.get_versioned(key, read_timestamp)
            .and_then(|versioned| versioned.value)
    }
    
    pub fn get_versioned(&self, key: &[u8], read_timestamp: u64) -> Option<VersionedValue> {
        if let Some(key_versions) = self.versions.get(key) {
            // Find the latest version that is <= read_timestamp
            let mut latest_version = None;

            for entry in key_versions.value().iter() {
                if *entry.key() <= read_timestamp {
                    latest_version = Some(entry.value().clone());
                } else {
                    break;
                }
            }

            latest_version
        } else {
            None
        }
    }

    pub fn get_latest_version(&self, key: &[u8]) -> Option<u64> {
        self.versions.get(key).and_then(|key_versions| {
            key_versions
                .value()
                .iter()
                .next_back()
                .map(|entry| *entry.key())
        })
    }

    pub fn cleanup_old_versions(&self, before_timestamp: u64, keep_min_versions: usize) {
        for entry in self.versions.iter() {
            let key_versions = entry.value();

            // Count total versions
            let total_versions = key_versions.len();
            if total_versions <= keep_min_versions {
                continue; // Keep minimum number of versions
            }

            // Find versions to remove
            let mut to_remove = Vec::new();
            let mut kept_count = 0;

            // Iterate from newest to oldest
            for version_entry in key_versions.iter().rev() {
                kept_count += 1;
                if kept_count > keep_min_versions && version_entry.key() < &before_timestamp {
                    to_remove.push(*version_entry.key());
                }
            }

            // Remove old versions
            for version in to_remove {
                key_versions.remove(&version);
            }
        }
    }

    pub fn get_all_versions(&self, key: &[u8]) -> Vec<(u64, Option<Vec<u8>>)> {
        if let Some(key_versions) = self.versions.get(key) {
            key_versions
                .value()
                .iter()
                .map(|entry| (*entry.key(), entry.value().value.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }
}

impl Default for VersionStore {
    fn default() -> Self {
        Self::new()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_basic() {
        let version_store = Arc::new(VersionStore::new());
        let tx_mgr = TransactionManager::new(100, version_store.clone());

        // Begin transaction
        let tx_id = tx_mgr.begin().unwrap();
        assert_eq!(tx_mgr.active_transaction_count(), 1);

        // Get transaction
        let tx_arc = tx_mgr.get_transaction(tx_id).unwrap();
        {
            let mut tx = tx_arc.write();
            tx.add_write(b"key1".to_vec(), Some(b"value1".to_vec()), 0);
            tx.add_read(b"key2".to_vec(), 0);
        }

        // Commit transaction
        tx_mgr.commit(tx_id).unwrap();
        assert_eq!(tx_mgr.active_transaction_count(), 0);

        // Check version store
        let value = version_store.get(b"key1", u64::MAX);
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_write_write_conflict() {
        let version_store = Arc::new(VersionStore::new());
        let tx_mgr = TransactionManager::new(100, version_store);

        // Start two transactions
        let tx1_id = tx_mgr.begin().unwrap();
        let tx2_id = tx_mgr.begin().unwrap();

        // Both write to same key
        let tx1_arc = tx_mgr.get_transaction(tx1_id).unwrap();
        tx1_arc
            .write()
            .add_write(b"key1".to_vec(), Some(b"value1".to_vec()), 0);

        let tx2_arc = tx_mgr.get_transaction(tx2_id).unwrap();
        tx2_arc
            .write()
            .add_write(b"key1".to_vec(), Some(b"value2".to_vec()), 0);

        // First commit should succeed
        tx_mgr.commit(tx1_id).unwrap();

        // Second commit should fail due to write-write conflict
        assert!(tx_mgr.commit(tx2_id).is_err());
    }

    #[test]
    fn test_snapshot_isolation() {
        let version_store = Arc::new(VersionStore::new());
        let tx_mgr = TransactionManager::new(100, version_store.clone());

        // Add initial data
        version_store.put(b"key1".to_vec(), Some(b"initial".to_vec()), 1, 0);

        // Start transaction 1
        let tx1_id = tx_mgr.begin().unwrap();
        let tx1_arc = tx_mgr.get_transaction(tx1_id).unwrap();

        // Transaction 1 reads key1
        tx1_arc.write().add_read(b"key1".to_vec(), 1);

        // Start transaction 2 and modify key1
        let tx2_id = tx_mgr.begin().unwrap();
        let tx2_arc = tx_mgr.get_transaction(tx2_id).unwrap();
        tx2_arc
            .write()
            .add_write(b"key1".to_vec(), Some(b"modified".to_vec()), 1);
        tx_mgr.commit(tx2_id).unwrap();

        // Transaction 1 tries to write based on its read
        tx1_arc
            .write()
            .add_write(b"key1".to_vec(), Some(b"conflict".to_vec()), 1);

        // This should fail due to read-write conflict
        assert!(tx_mgr.commit(tx1_id).is_err());
    }
}
