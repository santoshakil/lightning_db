use crate::error::{Error, Result};
use crate::transaction::{TxState, WriteOp, ReadOp};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashSet, BTreeMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// MVCC transaction manager with proper snapshot isolation
pub struct MVCCTransactionManager {
    next_tx_id: AtomicU64,
    next_timestamp: AtomicU64,
    
    // Active transactions and their read timestamps
    active_transactions: Arc<DashMap<u64, Arc<RwLock<MVCCTransaction>>>>,
    
    // Version store for multi-version data
    version_store: Arc<MVCCVersionStore>,
    
    // Transaction visibility tracking
    visibility_tracker: Arc<RwLock<VisibilityTracker>>,
    
    // Configuration
    max_active_transactions: usize,
    gc_interval: Duration,
    
    shutdown: Arc<AtomicU64>,
}

/// Enhanced transaction with MVCC support
#[derive(Debug, Clone)]
pub struct MVCCTransaction {
    pub id: u64,
    pub read_timestamp: u64,
    pub commit_timestamp: Option<u64>,
    pub snapshot: TransactionSnapshot,
    pub write_set: Vec<WriteOp>,
    pub read_set: Vec<ReadOp>,
    pub state: TxState,
    pub start_time: Instant,
}

/// Snapshot of the database at transaction start
#[derive(Debug, Clone)]
pub struct TransactionSnapshot {
    // Timestamp when snapshot was taken
    pub timestamp: u64,
    
    // Set of active transaction IDs at snapshot time
    pub active_tx_ids: HashSet<u64>,
    
    // Minimum active transaction ID (for GC)
    pub min_active_tx_id: u64,
}

/// Tracks transaction visibility for MVCC
#[derive(Debug)]
struct VisibilityTracker {
    // Map of transaction ID to commit timestamp
    committed_transactions: BTreeMap<u64, u64>,
    
    // Currently active transactions
    active_transactions: HashSet<u64>,
    
    // Minimum active transaction timestamp (for GC)
    min_active_timestamp: u64,
}

/// Enhanced version store with MVCC visibility
pub struct MVCCVersionStore {
    // Key -> List of versions (sorted by timestamp, newest first)
    versions: Arc<DashMap<Vec<u8>, Vec<VersionEntry>>>,
    
    // Statistics
    total_versions: AtomicU64,
    garbage_collected: AtomicU64,
}

#[derive(Debug, Clone)]
struct VersionEntry {
    value: Option<Vec<u8>>,  // None = tombstone
    timestamp: u64,
    tx_id: u64,
}

impl MVCCTransactionManager {
    pub fn new(max_active_transactions: usize) -> Arc<Self> {
        let shutdown = Arc::new(AtomicU64::new(0));
        let _shutdown_clone = Arc::clone(&shutdown);
        
        let manager = Arc::new(Self {
            next_tx_id: AtomicU64::new(1),
            next_timestamp: AtomicU64::new(1),
            active_transactions: Arc::new(DashMap::new()),
            version_store: Arc::new(MVCCVersionStore::new()),
            visibility_tracker: Arc::new(RwLock::new(VisibilityTracker::new())),
            max_active_transactions,
            gc_interval: Duration::from_secs(60),
            shutdown,
        });
        
        // Start background GC thread
        let manager_clone = Arc::clone(&manager);
        std::thread::spawn(move || {
            manager_clone.gc_loop();
        });
        
        manager
    }
    
    pub fn begin(&self) -> Result<u64> {
        if self.active_transactions.len() >= self.max_active_transactions {
            return Err(Error::Transaction("Too many active transactions".to_string()));
        }
        
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let read_timestamp = self.next_timestamp.load(Ordering::SeqCst);
        
        // Create transaction snapshot
        let snapshot = {
            let visibility = self.visibility_tracker.read();
            TransactionSnapshot {
                timestamp: read_timestamp,
                active_tx_ids: visibility.active_transactions.clone(),
                min_active_tx_id: visibility.active_transactions.iter()
                    .min()
                    .copied()
                    .unwrap_or(tx_id),
            }
        };
        
        // Register transaction
        {
            let mut visibility = self.visibility_tracker.write();
            visibility.active_transactions.insert(tx_id);
            visibility.update_min_active_timestamp();
        }
        
        let tx = Arc::new(RwLock::new(MVCCTransaction {
            id: tx_id,
            read_timestamp,
            commit_timestamp: None,
            snapshot,
            write_set: Vec::new(),
            read_set: Vec::new(),
            state: TxState::Active,
            start_time: Instant::now(),
        }));
        
        self.active_transactions.insert(tx_id, tx);
        Ok(tx_id)
    }
    
    pub fn get(&self, tx_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tx_arc = self.get_transaction(tx_id)?;
        let mut tx = tx_arc.write();
        
        // Check write set first
        if let Some(write_op) = tx.write_set.iter().find(|w| w.key == key) {
            return Ok(write_op.value.clone());
        }
        
        // Read from version store with visibility check
        let visible_value = self.version_store.get_visible(
            key,
            &tx.snapshot,
            &self.visibility_tracker.read(),
        )?;
        
        // Record read in read set
        let version = self.version_store.get_version_at(key, tx.read_timestamp);
        tx.read_set.push(ReadOp {
            key: key.to_vec(),
            version,
        });
        
        Ok(visible_value)
    }
    
    pub fn put(&self, tx_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;
        let mut tx = tx_arc.write();
        
        if !tx.is_active() {
            return Err(Error::Transaction("Transaction is not active".to_string()));
        }
        
        let prev_version = self.version_store.get_version_at(key, tx.read_timestamp);
        tx.add_write(key.to_vec(), Some(value.to_vec()), prev_version);
        
        Ok(())
    }
    
    pub fn delete(&self, tx_id: u64, key: &[u8]) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;
        let mut tx = tx_arc.write();
        
        if !tx.is_active() {
            return Err(Error::Transaction("Transaction is not active".to_string()));
        }
        
        let prev_version = self.version_store.get_version_at(key, tx.read_timestamp);
        tx.add_write(key.to_vec(), None, prev_version);
        
        Ok(())
    }
    
    pub fn commit(&self, tx_id: u64) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;
        
        // Phase 1: Prepare
        let tx_data = {
            let mut tx = tx_arc.write();
            if !tx.is_active() {
                return Err(Error::Transaction("Transaction is not active".to_string()));
            }
            tx.state = TxState::Preparing;
            tx.clone()
        };
        
        // Phase 2: Validate
        self.validate_snapshot_isolation(&tx_data)?;
        
        // Phase 3: Commit
        let commit_timestamp = self.next_timestamp.fetch_add(1, Ordering::SeqCst) + 1;
        
        // Apply writes to version store
        for write_op in &tx_data.write_set {
            self.version_store.put_version(
                write_op.key.clone(),
                write_op.value.clone(),
                commit_timestamp,
                tx_id,
            );
        }
        
        // Update transaction state
        {
            let mut tx = tx_arc.write();
            tx.commit(commit_timestamp);
        }
        
        // Update visibility tracker
        {
            let mut visibility = self.visibility_tracker.write();
            visibility.active_transactions.remove(&tx_id);
            visibility.committed_transactions.insert(tx_id, commit_timestamp);
            visibility.update_min_active_timestamp();
        }
        
        // Remove from active transactions
        self.active_transactions.remove(&tx_id);
        
        Ok(())
    }
    
    pub fn abort(&self, tx_id: u64) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;
        
        {
            let mut tx = tx_arc.write();
            tx.abort();
        }
        
        // Update visibility tracker
        {
            let mut visibility = self.visibility_tracker.write();
            visibility.active_transactions.remove(&tx_id);
            visibility.update_min_active_timestamp();
        }
        
        self.active_transactions.remove(&tx_id);
        Ok(())
    }
    
    fn get_transaction(&self, tx_id: u64) -> Result<Arc<RwLock<MVCCTransaction>>> {
        self.active_transactions
            .get(&tx_id)
            .map(|entry| entry.value().clone())
            .ok_or(Error::Transaction("Transaction not found".to_string()))
    }
    
    fn validate_snapshot_isolation(&self, tx: &MVCCTransaction) -> Result<()> {
        // Check write-write conflicts
        for write_op in &tx.write_set {
            let versions = self.version_store.get_versions(&write_op.key);
            
            // Check if any version was written after our snapshot
            for version in versions {
                if version.timestamp > tx.read_timestamp && version.tx_id != tx.id {
                    // A newer version exists from a different transaction
                    // This is a write-write conflict
                    return Err(Error::Transaction(
                        "Write-write conflict: concurrent modification".to_string()
                    ));
                }
            }
        }
        
        // For snapshot isolation, we don't need to check read-write conflicts
        // (that would be serializable isolation)
        
        Ok(())
    }
    
    fn gc_loop(&self) {
        while self.shutdown.load(Ordering::Relaxed) == 0 {
            std::thread::sleep(self.gc_interval);
            
            let min_timestamp = {
                let visibility = self.visibility_tracker.read();
                visibility.min_active_timestamp
            };
            
            // Garbage collect old versions
            let collected = self.version_store.garbage_collect(min_timestamp);
            if collected > 0 {
                tracing::info!("Garbage collected {} old versions", collected);
            }
        }
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(1, Ordering::Relaxed);
    }
}

impl MVCCTransaction {
    pub fn is_active(&self) -> bool {
        self.state == TxState::Active
    }
    
    pub fn add_write(&mut self, key: Vec<u8>, value: Option<Vec<u8>>, prev_version: u64) {
        if let Some(pos) = self.write_set.iter().position(|w| w.key == key) {
            self.write_set[pos] = WriteOp { key, value, prev_version };
        } else {
            self.write_set.push(WriteOp { key, value, prev_version });
        }
    }
    
    pub fn commit(&mut self, timestamp: u64) {
        self.state = TxState::Committed;
        self.commit_timestamp = Some(timestamp);
    }
    
    pub fn abort(&mut self) {
        self.state = TxState::Aborted;
    }
}

impl VisibilityTracker {
    fn new() -> Self {
        Self {
            committed_transactions: BTreeMap::new(),
            active_transactions: HashSet::new(),
            min_active_timestamp: 0,
        }
    }
    
    fn update_min_active_timestamp(&mut self) {
        self.min_active_timestamp = self.active_transactions
            .iter()
            .min()
            .copied()
            .unwrap_or(u64::MAX);
    }
    
    pub fn is_visible(&self, version: &VersionEntry, snapshot: &TransactionSnapshot) -> bool {
        // Version is visible if:
        // 1. It was committed before our snapshot, AND
        // 2. The committing transaction was not active in our snapshot
        
        if version.timestamp <= snapshot.timestamp {
            // Check if the transaction was active in our snapshot
            !snapshot.active_tx_ids.contains(&version.tx_id)
        } else {
            false
        }
    }
}

impl MVCCVersionStore {
    fn new() -> Self {
        Self {
            versions: Arc::new(DashMap::new()),
            total_versions: AtomicU64::new(0),
            garbage_collected: AtomicU64::new(0),
        }
    }
    
    fn put_version(&self, key: Vec<u8>, value: Option<Vec<u8>>, timestamp: u64, tx_id: u64) {
        let version = VersionEntry {
            value,
            timestamp,
            tx_id,
        };
        
        self.versions
            .entry(key)
            .and_modify(|versions| {
                // Insert in timestamp order (newest first)
                let pos = versions.iter().position(|v| v.timestamp < timestamp).unwrap_or(versions.len());
                versions.insert(pos, version.clone());
            })
            .or_insert_with(|| vec![version]);
        
        self.total_versions.fetch_add(1, Ordering::Relaxed);
    }
    
    fn get_visible(
        &self,
        key: &[u8],
        snapshot: &TransactionSnapshot,
        visibility: &VisibilityTracker,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(versions) = self.versions.get(key) {
            // Find the first visible version
            for version in versions.value() {
                if visibility.is_visible(version, snapshot) {
                    return Ok(version.value.clone());
                }
            }
        }
        Ok(None)
    }
    
    fn get_versions(&self, key: &[u8]) -> Vec<VersionEntry> {
        self.versions
            .get(key)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }
    
    fn get_version_at(&self, key: &[u8], timestamp: u64) -> u64 {
        if let Some(versions) = self.versions.get(key) {
            for version in versions.value() {
                if version.timestamp <= timestamp {
                    return version.timestamp;
                }
            }
        }
        0
    }
    
    fn garbage_collect(&self, min_timestamp: u64) -> usize {
        let mut collected = 0;
        
        for mut entry in self.versions.iter_mut() {
            let original_len = entry.value().len();
            
            // Keep only the newest version older than min_timestamp
            // and all versions newer than min_timestamp
            let mut keep_next_old = true;
            entry.value_mut().retain(|v| {
                if v.timestamp >= min_timestamp {
                    true
                } else if keep_next_old {
                    keep_next_old = false;
                    true
                } else {
                    false
                }
            });
            
            collected += original_len - entry.value().len();
        }
        
        self.garbage_collected.fetch_add(collected as u64, Ordering::Relaxed);
        collected
    }
}

impl Drop for MVCCTransactionManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_mvcc_snapshot_isolation() {
        let manager = MVCCTransactionManager::new(100);
        
        // Transaction 1: Read and write
        let tx1 = manager.begin().unwrap();
        manager.put(tx1, b"key1", b"value1").unwrap();
        
        // Transaction 2: Start after tx1 write but before commit
        let tx2 = manager.begin().unwrap();
        
        // tx2 should not see tx1's write
        assert_eq!(manager.get(tx2, b"key1").unwrap(), None);
        
        // Commit tx1
        manager.commit(tx1).unwrap();
        
        // tx2 still shouldn't see tx1's write (snapshot isolation)
        assert_eq!(manager.get(tx2, b"key1").unwrap(), None);
        
        // New transaction should see committed data
        let tx3 = manager.begin().unwrap();
        assert_eq!(manager.get(tx3, b"key1").unwrap(), Some(b"value1".to_vec()));
    }
    
    #[test]
    fn test_mvcc_write_conflict() {
        let manager = MVCCTransactionManager::new(100);
        
        // Setup: commit initial value
        let tx0 = manager.begin().unwrap();
        manager.put(tx0, b"key1", b"value0").unwrap();
        manager.commit(tx0).unwrap();
        
        // Two concurrent transactions
        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();
        
        // Both read the same value
        assert_eq!(manager.get(tx1, b"key1").unwrap(), Some(b"value0".to_vec()));
        assert_eq!(manager.get(tx2, b"key1").unwrap(), Some(b"value0".to_vec()));
        
        // Both try to update
        manager.put(tx1, b"key1", b"value1").unwrap();
        manager.put(tx2, b"key1", b"value2").unwrap();
        
        // First commit succeeds
        assert!(manager.commit(tx1).is_ok());
        
        // Second commit fails (write-write conflict)
        assert!(manager.commit(tx2).is_err());
    }
}