use crate::core::error::{Error, Result};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::locks::TxId;

/// Snapshot identifier
pub type SnapshotId = u64;

/// A point-in-time snapshot for transaction isolation
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub tx_id: TxId,
    pub start_timestamp: u64,
    pub committed_txs: HashSet<TxId>,
}

/// Snapshot metadata for management
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    pub id: SnapshotId,
    pub tx_id: TxId,
    pub created_at: Instant,
    pub reference_count: usize,
    pub last_accessed: Instant,
}

/// Snapshot manager for MVCC isolation levels
#[derive(Debug)]
pub struct SnapshotManager {
    /// Active snapshots
    snapshots: Arc<RwLock<HashMap<SnapshotId, Snapshot>>>,
    /// Snapshot metadata for cleanup and management
    metadata: Arc<RwLock<HashMap<SnapshotId, SnapshotMetadata>>>,
    /// Reference counting for shared snapshots
    references: Arc<RwLock<HashMap<SnapshotId, HashSet<TxId>>>>,
    /// Next snapshot ID
    next_snapshot_id: Arc<AtomicU64>,
    /// Cleanup configuration
    max_snapshot_age: Duration,
    cleanup_interval: Duration,
    last_cleanup: Arc<RwLock<Instant>>,
    /// Statistics
    stats: Arc<RwLock<SnapshotStats>>,
}

#[derive(Debug, Default, Clone)]
pub struct SnapshotStats {
    pub snapshots_created: u64,
    pub snapshots_destroyed: u64,
    pub active_snapshots: usize,
    pub total_references: usize,
    pub cleanup_runs: u64,
    pub snapshots_cleaned: u64,
}

impl SnapshotManager {
    pub fn new(max_snapshot_age: Duration, cleanup_interval: Duration) -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            references: Arc::new(RwLock::new(HashMap::new())),
            next_snapshot_id: Arc::new(AtomicU64::new(1)),
            max_snapshot_age,
            cleanup_interval,
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
            stats: Arc::new(RwLock::new(SnapshotStats::default())),
        }
    }

    /// Create a new snapshot
    pub fn create_snapshot(
        &self,
        tx_id: TxId,
        start_timestamp: u64,
        committed_txs: HashSet<TxId>,
    ) -> Result<SnapshotId> {
        let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();

        let snapshot = Snapshot {
            id: snapshot_id,
            tx_id,
            start_timestamp,
            committed_txs: committed_txs.clone(),
        };

        let metadata = SnapshotMetadata {
            id: snapshot_id,
            tx_id,
            created_at: now,
            reference_count: 1,
            last_accessed: now,
        };

        // Store snapshot and metadata
        self.snapshots.write().insert(snapshot_id, snapshot);
        self.metadata.write().insert(snapshot_id, metadata);

        // Add initial reference
        self.references
            .write()
            .insert(snapshot_id, [tx_id].iter().cloned().collect());

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.snapshots_created += 1;
            stats.active_snapshots += 1;
            stats.total_references += 1;
        }

        debug!(
            "Created snapshot {} for transaction {} with {} committed transactions",
            snapshot_id,
            tx_id,
            committed_txs.len()
        );

        Ok(snapshot_id)
    }

    /// Get a snapshot by ID
    pub fn get_snapshot(&self, snapshot_id: SnapshotId) -> Option<Snapshot> {
        let snapshots = self.snapshots.read();
        let snapshot = snapshots.get(&snapshot_id).cloned();

        if snapshot.is_some() {
            // Update access time
            if let Some(metadata) = self.metadata.write().get_mut(&snapshot_id) {
                metadata.last_accessed = Instant::now();
            }
        }

        snapshot
    }

    /// Add a reference to a snapshot from another transaction
    pub fn add_reference(&self, snapshot_id: SnapshotId, tx_id: TxId) -> Result<()> {
        let mut references = self.references.write();
        let mut metadata = self.metadata.write();

        if let Some(ref_set) = references.get_mut(&snapshot_id) {
            if ref_set.insert(tx_id) {
                // New reference added
                if let Some(meta) = metadata.get_mut(&snapshot_id) {
                    meta.reference_count += 1;
                    meta.last_accessed = Instant::now();
                }

                self.stats.write().total_references += 1;
                debug!(
                    "Added reference to snapshot {} from transaction {}",
                    snapshot_id, tx_id
                );
            }
            Ok(())
        } else {
            Err(Error::SnapshotNotFound(snapshot_id))
        }
    }

    /// Remove a reference to a snapshot
    pub fn remove_reference(&self, snapshot_id: SnapshotId, tx_id: TxId) -> Result<bool> {
        let mut references = self.references.write();
        let mut metadata = self.metadata.write();

        if let Some(ref_set) = references.get_mut(&snapshot_id) {
            if ref_set.remove(&tx_id) {
                // Reference removed
                if let Some(meta) = metadata.get_mut(&snapshot_id) {
                    meta.reference_count = meta.reference_count.saturating_sub(1);
                }

                self.stats.write().total_references =
                    self.stats.read().total_references.saturating_sub(1);

                // If no more references, mark for cleanup
                let should_cleanup = ref_set.is_empty();
                if should_cleanup {
                    references.remove(&snapshot_id);
                    debug!(
                        "Snapshot {} has no more references, marked for cleanup",
                        snapshot_id
                    );
                }

                Ok(should_cleanup)
            } else {
                Ok(false) // Reference didn't exist
            }
        } else {
            Err(Error::SnapshotNotFound(snapshot_id))
        }
    }

    /// Destroy a snapshot
    pub fn destroy_snapshot(&self, snapshot_id: SnapshotId) -> Result<()> {
        let mut snapshots = self.snapshots.write();
        let mut metadata = self.metadata.write();
        let mut references = self.references.write();

        if snapshots.remove(&snapshot_id).is_some() {
            metadata.remove(&snapshot_id);
            references.remove(&snapshot_id);

            let mut stats = self.stats.write();
            stats.snapshots_destroyed += 1;
            stats.active_snapshots = stats.active_snapshots.saturating_sub(1);

            debug!("Destroyed snapshot {}", snapshot_id);
            Ok(())
        } else {
            Err(Error::SnapshotNotFound(snapshot_id))
        }
    }

    /// Check if a transaction is committed in a snapshot
    pub fn is_committed_in_snapshot(&self, snapshot_id: SnapshotId, tx_id: TxId) -> Result<bool> {
        let snapshots = self.snapshots.read();
        if let Some(snapshot) = snapshots.get(&snapshot_id) {
            Ok(snapshot.committed_txs.contains(&tx_id))
        } else {
            Err(Error::SnapshotNotFound(snapshot_id))
        }
    }

    /// Get all transactions committed in a snapshot
    pub fn get_committed_transactions(&self, snapshot_id: SnapshotId) -> Result<HashSet<TxId>> {
        let snapshots = self.snapshots.read();
        if let Some(snapshot) = snapshots.get(&snapshot_id) {
            Ok(snapshot.committed_txs.clone())
        } else {
            Err(Error::SnapshotNotFound(snapshot_id))
        }
    }

    /// Run cleanup to remove old and unused snapshots
    pub fn cleanup_snapshots(&self) -> Result<usize> {
        let now = Instant::now();

        // Check if it's time to cleanup
        {
            let mut last_cleanup = self.last_cleanup.write();
            if now - *last_cleanup < self.cleanup_interval {
                return Ok(0);
            }
            *last_cleanup = now;
        }

        let mut to_destroy = Vec::new();

        // Find snapshots to clean up
        {
            let metadata = self.metadata.read();
            let references = self.references.read();

            for (snapshot_id, meta) in metadata.iter() {
                let should_cleanup =
                    // No references
                    !references.contains_key(snapshot_id) ||
                    references.get(snapshot_id).is_none_or(|refs| refs.is_empty()) ||
                    // Too old
                    now - meta.created_at > self.max_snapshot_age;

                if should_cleanup {
                    to_destroy.push(*snapshot_id);
                }
            }
        }

        // Destroy identified snapshots
        let mut cleaned = 0;
        for snapshot_id in to_destroy {
            if self.destroy_snapshot(snapshot_id).is_ok() {
                cleaned += 1;
            }
        }

        if cleaned > 0 {
            self.stats.write().cleanup_runs += 1;
            self.stats.write().snapshots_cleaned += cleaned as u64;
            info!("Cleaned up {} old snapshots", cleaned);
        }

        Ok(cleaned)
    }

    /// Get snapshots that reference a specific transaction
    pub fn get_snapshots_for_transaction(&self, tx_id: TxId) -> Vec<SnapshotId> {
        let references = self.references.read();
        references
            .iter()
            .filter(|(_, ref_set)| ref_set.contains(&tx_id))
            .map(|(snapshot_id, _)| *snapshot_id)
            .collect()
    }

    /// Get oldest active snapshot timestamp
    pub fn get_oldest_snapshot_timestamp(&self) -> Option<u64> {
        let snapshots = self.snapshots.read();
        snapshots
            .values()
            .map(|snapshot| snapshot.start_timestamp)
            .min()
    }

    /// Get snapshot information for debugging
    pub fn get_snapshot_info(&self) -> HashMap<SnapshotId, (TxId, usize, Instant)> {
        let metadata = self.metadata.read();
        metadata
            .iter()
            .map(|(id, meta)| (*id, (meta.tx_id, meta.reference_count, meta.created_at)))
            .collect()
    }

    /// Get current statistics
    pub fn get_stats(&self) -> SnapshotStats {
        let mut stats = self.stats.write();
        stats.active_snapshots = self.snapshots.read().len();
        stats.total_references = self.references.read().values().map(|s| s.len()).sum();
        stats.clone()
    }

    /// Force cleanup of all snapshots older than the specified age
    pub fn force_cleanup_older_than(&self, max_age: Duration) -> Result<usize> {
        let now = Instant::now();
        let mut to_destroy = Vec::new();

        {
            let metadata = self.metadata.read();
            for (snapshot_id, meta) in metadata.iter() {
                if now - meta.created_at > max_age {
                    to_destroy.push(*snapshot_id);
                }
            }
        }

        let mut cleaned = 0;
        for snapshot_id in to_destroy {
            if self.destroy_snapshot(snapshot_id).is_ok() {
                cleaned += 1;
            }
        }

        if cleaned > 0 {
            warn!(
                "Force cleaned {} snapshots older than {:?}",
                cleaned, max_age
            );
        }

        Ok(cleaned)
    }

    /// Create a shared snapshot that can be used by multiple transactions
    pub fn create_shared_snapshot(
        &self,
        tx_ids: &[TxId],
        start_timestamp: u64,
        committed_txs: HashSet<TxId>,
    ) -> Result<SnapshotId> {
        if tx_ids.is_empty() {
            return Err(Error::InvalidArgument(
                "No transaction IDs provided".to_string(),
            ));
        }

        let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();

        let snapshot = Snapshot {
            id: snapshot_id,
            tx_id: tx_ids[0], // Primary transaction
            start_timestamp,
            committed_txs: committed_txs.clone(),
        };

        let metadata = SnapshotMetadata {
            id: snapshot_id,
            tx_id: tx_ids[0],
            created_at: now,
            reference_count: tx_ids.len(),
            last_accessed: now,
        };

        // Store snapshot and metadata
        self.snapshots.write().insert(snapshot_id, snapshot);
        self.metadata.write().insert(snapshot_id, metadata);

        // Add references for all transactions
        self.references
            .write()
            .insert(snapshot_id, tx_ids.iter().cloned().collect());

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.snapshots_created += 1;
            stats.active_snapshots += 1;
            stats.total_references += tx_ids.len();
        }

        debug!(
            "Created shared snapshot {} for {} transactions with {} committed transactions",
            snapshot_id,
            tx_ids.len(),
            committed_txs.len()
        );

        Ok(snapshot_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_creation_and_retrieval() {
        let manager = SnapshotManager::new(Duration::from_secs(60), Duration::from_secs(10));

        let committed_txs: HashSet<TxId> = [1, 2, 3].iter().cloned().collect();
        let snapshot_id = manager
            .create_snapshot(100, 1000, committed_txs.clone())
            .unwrap();

        let snapshot = manager.get_snapshot(snapshot_id).unwrap();
        assert_eq!(snapshot.tx_id, 100);
        assert_eq!(snapshot.start_timestamp, 1000);
        assert_eq!(snapshot.committed_txs, committed_txs);
    }

    #[test]
    fn test_reference_management() {
        let manager = SnapshotManager::new(Duration::from_secs(60), Duration::from_secs(10));

        let committed_txs = HashSet::new();
        let snapshot_id = manager.create_snapshot(100, 1000, committed_txs).unwrap();

        // Add reference
        manager.add_reference(snapshot_id, 200).unwrap();

        // Remove references
        let should_cleanup = manager.remove_reference(snapshot_id, 100).unwrap();
        assert!(!should_cleanup); // Still has reference from tx 200

        let should_cleanup = manager.remove_reference(snapshot_id, 200).unwrap();
        assert!(should_cleanup); // No more references
    }

    #[test]
    fn test_shared_snapshot() {
        let manager = SnapshotManager::new(Duration::from_secs(60), Duration::from_secs(10));

        let tx_ids = vec![100, 200, 300];
        let committed_txs = HashSet::new();

        let snapshot_id = manager
            .create_shared_snapshot(&tx_ids, 1000, committed_txs)
            .unwrap();

        let snapshot = manager.get_snapshot(snapshot_id).unwrap();
        assert_eq!(snapshot.tx_id, 100); // Primary transaction

        // All transactions should be referenced
        for &tx_id in &tx_ids {
            let snapshots = manager.get_snapshots_for_transaction(tx_id);
            assert!(snapshots.contains(&snapshot_id));
        }
    }

    #[test]
    fn test_cleanup() {
        let manager = SnapshotManager::new(
            Duration::from_millis(1), // Very short age for testing
            Duration::from_millis(1),
        );

        let committed_txs = HashSet::new();
        let snapshot_id = manager.create_snapshot(100, 1000, committed_txs).unwrap();

        // Remove reference to make it eligible for cleanup
        manager.remove_reference(snapshot_id, 100).unwrap();

        // Wait a bit and run cleanup
        std::thread::sleep(Duration::from_millis(10));
        let cleaned = manager.cleanup_snapshots().unwrap();

        assert!(cleaned > 0);
        assert!(manager.get_snapshot(snapshot_id).is_none());
    }
}
