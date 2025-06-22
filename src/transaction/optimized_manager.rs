use crate::error::{Error, Result};
use crate::transaction::{Transaction, TxState, VersionStore};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Optimized transaction manager with reduced lock contention
pub struct OptimizedTransactionManager {
    // Core transaction management
    next_tx_id: Arc<AtomicU64>,
    active_transactions: Arc<DashMap<u64, Arc<RwLock<Transaction>>>>,
    commit_timestamp: Arc<AtomicU64>,

    // Lock-free conflict detection
    write_locks: Arc<DashMap<Vec<u8>, WriteLockInfo>>,
    read_locks: Arc<DashMap<Vec<u8>, ReadLockSet>>,

    // Configuration and stats
    max_active_transactions: usize,
    version_store: Arc<VersionStore>,
    lock_timeout: Duration,

    // Performance optimizations
    tx_pool: Arc<Mutex<Vec<Transaction>>>, // Transaction object pool
    batch_commit_size: usize,              // Batch multiple commits
    pending_commits: Arc<DashMap<u64, Transaction>>, // Queue for batch commits

    // Statistics
    commit_count: Arc<AtomicU64>,
    abort_count: Arc<AtomicU64>,
    conflict_count: Arc<AtomicU64>,
    deadlock_count: Arc<AtomicU64>,

    // Background processing
    background_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    shutdown: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct WriteLockInfo {
    tx_id: u64,
    timestamp: Instant,
    priority: LockPriority,
}

#[derive(Debug, Clone)]
struct ReadLockSet {
    readers: Vec<ReadLockInfo>,
    last_cleanup: Instant,
}

#[derive(Debug, Clone)]
struct ReadLockInfo {
    tx_id: u64,
    timestamp: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum LockPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3, // For deadlock resolution
}

impl OptimizedTransactionManager {
    pub fn new(max_active_transactions: usize, version_store: Arc<VersionStore>) -> Self {
        

        Self {
            next_tx_id: Arc::new(AtomicU64::new(1)),
            active_transactions: Arc::new(DashMap::with_capacity(max_active_transactions)),
            commit_timestamp: Arc::new(AtomicU64::new(1)),
            write_locks: Arc::new(DashMap::new()),
            read_locks: Arc::new(DashMap::new()),
            max_active_transactions,
            version_store,
            lock_timeout: Duration::from_millis(50), // Reduced timeout for faster detection
            tx_pool: Arc::new(Mutex::new(Vec::with_capacity(64))),
            batch_commit_size: 16,
            pending_commits: Arc::new(DashMap::new()),
            commit_count: Arc::new(AtomicU64::new(0)),
            abort_count: Arc::new(AtomicU64::new(0)),
            conflict_count: Arc::new(AtomicU64::new(0)),
            deadlock_count: Arc::new(AtomicU64::new(0)),
            background_thread: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn start_background_processing(&mut self) {
        let pending_commits = Arc::clone(&self.pending_commits);
        let version_store = Arc::clone(&self.version_store);
        let commit_timestamp = Arc::clone(&self.commit_timestamp);
        let active_transactions = Arc::clone(&self.active_transactions);
        let write_locks = Arc::clone(&self.write_locks);
        let read_locks = Arc::clone(&self.read_locks);
        let shutdown = Arc::clone(&self.shutdown);
        let batch_size = self.batch_commit_size;

        *self.background_thread.lock() = Some(thread::spawn(move || {
            debug!("Starting optimized transaction manager background thread");

            while shutdown.load(Ordering::Relaxed) == 0 {
                // Process batch commits
                Self::process_batch_commits(
                    &pending_commits,
                    &version_store,
                    &commit_timestamp,
                    &active_transactions,
                    batch_size,
                );

                // Cleanup expired locks
                Self::cleanup_expired_locks(&write_locks, &read_locks);

                // Sleep longer to reduce overhead
                thread::sleep(Duration::from_millis(100));
            }

            debug!("Optimized transaction manager background thread stopped");
        }));
    }

    pub fn stop(&self) {
        self.shutdown.store(1, Ordering::Relaxed);

        if let Some(handle) = self.background_thread.lock().take() {
            let _ = handle.join();
        }
    }

    pub fn begin(&self) -> Result<u64> {
        // Fast path: check transaction limit without expensive operations
        if self.active_transactions.len() >= self.max_active_transactions {
            return Err(Error::Transaction(
                "Too many active transactions".to_string(),
            ));
        }

        let tx_id = self.next_tx_id.fetch_add(1, Ordering::Relaxed); // Relaxed ordering for better performance
        let read_timestamp = self.commit_timestamp.load(Ordering::Acquire);

        // Try to reuse transaction object from pool
        let tx = {
            let mut pool = self.tx_pool.lock();
            if let Some(mut recycled_tx) = pool.pop() {
                // Reset the recycled transaction
                recycled_tx.id = tx_id;
                recycled_tx.read_timestamp = read_timestamp;
                recycled_tx.commit_timestamp = None;
                recycled_tx.write_set.clear();
                recycled_tx.read_set.clear();
                recycled_tx.state = TxState::Active;
                recycled_tx.start_time = Instant::now();
                recycled_tx
            } else {
                Transaction::new(tx_id, read_timestamp)
            }
        };

        let tx_arc = Arc::new(RwLock::new(tx));
        self.active_transactions.insert(tx_id, tx_arc);

        Ok(tx_id)
    }

    pub fn get_transaction(&self, tx_id: u64) -> Result<Arc<RwLock<Transaction>>> {
        self.active_transactions
            .get(&tx_id)
            .map(|entry| entry.value().clone())
            .ok_or(Error::Transaction("Transaction not found".to_string()))
    }

    pub fn acquire_write_lock(&self, tx_id: u64, key: &[u8]) -> Result<()> {
        let key_vec = key.to_vec();
        // Determine lock priority based on transaction characteristics
        let priority = if tx_id < 100 {
            LockPriority::Critical // System transactions
        } else if tx_id < 1000 {
            LockPriority::High // Important user transactions
        } else if tx_id > 10000 {
            LockPriority::Low // Background operations
        } else {
            LockPriority::Normal // Regular transactions
        };

        let lock_info = WriteLockInfo {
            tx_id,
            timestamp: Instant::now(),
            priority,
        };

        // Try to acquire write lock
        match self.write_locks.entry(key_vec.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                // No existing write lock, check for read locks
                if let Some(read_set) = self.read_locks.get(&key_vec) {
                    if read_set.readers.iter().any(|r| r.tx_id != tx_id) {
                        // Other transactions have read locks
                        return Err(Error::Transaction("Read-write conflict".to_string()));
                    }
                }
                debug!(
                    "Acquired write lock for key {:?} by tx {} with priority {:?}",
                    key, tx_id, lock_info.priority
                );
                entry.insert(lock_info);
                Ok(())
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let existing = entry.get();
                if existing.tx_id == tx_id {
                    // We already own this lock
                    Ok(())
                } else {
                    // Check if lock has timed out
                    let lock_age = existing.timestamp.elapsed();
                    if lock_age > self.lock_timeout {
                        // Force expire the old lock
                        debug!(
                            "Force expiring lock on key {:?} held by tx {} for {:?}",
                            key, existing.tx_id, lock_age
                        );
                        drop(entry.remove());
                        // Retry acquiring lock
                        self.acquire_write_lock(tx_id, key)
                    } else {
                        // Deadlock detection: compare transaction IDs for ordering
                        if tx_id < existing.tx_id {
                            // Lower ID wins (wound-wait)
                            self.deadlock_count.fetch_add(1, Ordering::Relaxed);
                            Err(Error::Transaction(
                                "Deadlock detected - abort higher ID transaction".to_string(),
                            ))
                        } else {
                            // Higher ID waits
                            Err(Error::Transaction("Write-write conflict".to_string()))
                        }
                    }
                }
            }
        }
    }

    pub fn acquire_read_lock(&self, tx_id: u64, key: &[u8]) -> Result<()> {
        let key_vec = key.to_vec();

        // Check for write locks first
        if let Some(write_lock) = self.write_locks.get(&key_vec) {
            if write_lock.tx_id != tx_id {
                return Err(Error::Transaction("Write-read conflict".to_string()));
            }
            // We own the write lock, so read is allowed
            return Ok(());
        }

        // Add to read lock set
        let read_info = ReadLockInfo {
            tx_id,
            timestamp: Instant::now(),
        };

        self.read_locks
            .entry(key_vec)
            .and_modify(|read_set| {
                // Check if we're already in the read set
                if !read_set.readers.iter().any(|r| r.tx_id == tx_id) {
                    read_set.readers.push(read_info.clone());
                }
            })
            .or_insert_with(|| ReadLockSet {
                readers: vec![read_info],
                last_cleanup: Instant::now(),
            });

        Ok(())
    }

    pub fn commit(&self, tx_id: u64) -> Result<()> {
        // Get transaction and clone its data for validation
        let tx_arc = self.get_transaction(tx_id)?;
        let tx_data = {
            let mut tx = tx_arc.write();
            if !tx.is_active() {
                return Err(Error::Transaction("Transaction is not active".to_string()));
            }
            tx.prepare();
            tx.clone()
        };

        // Fast path validation without holding locks
        self.validate_transaction_optimized(&tx_data)?;

        // Add to pending commits for batch processing
        self.pending_commits.insert(tx_id, tx_data);

        Ok(())
    }

    pub fn commit_sync(&self, tx_id: u64) -> Result<()> {
        // Synchronous commit for critical operations
        let tx_arc = self.get_transaction(tx_id)?;
        let tx_data = {
            let mut tx = tx_arc.write();
            if !tx.is_active() {
                return Err(Error::Transaction("Transaction is not active".to_string()));
            }
            tx.prepare();
            tx.clone()
        };

        // Validate
        self.validate_transaction_optimized(&tx_data)?;

        // Get commit timestamp
        let commit_ts = self.commit_timestamp.fetch_add(1, Ordering::SeqCst) + 1;

        // Apply writes immediately
        for write_op in &tx_data.write_set {
            self.version_store.put(
                write_op.key.clone(),
                write_op.value.clone(),
                commit_ts,
                tx_id,
            );
        }

        // Mark as committed and release locks
        {
            let mut tx = tx_arc.write();
            tx.commit(commit_ts);
        }

        self.release_locks(tx_id, &tx_data);
        self.active_transactions.remove(&tx_id);
        self.commit_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn abort(&self, tx_id: u64) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;
        let tx_data = {
            let mut tx = tx_arc.write();
            if !tx.is_active() && tx.state != TxState::Preparing {
                return Err(Error::Transaction("Transaction is not active".to_string()));
            }
            tx.abort();
            tx.clone()
        };

        self.release_locks(tx_id, &tx_data);
        self.active_transactions.remove(&tx_id);
        self.abort_count.fetch_add(1, Ordering::Relaxed);

        // Return transaction to pool for reuse
        {
            let mut pool = self.tx_pool.lock();
            if pool.len() < pool.capacity() {
                pool.push(tx_data);
            }
        }

        Ok(())
    }

    fn validate_transaction_optimized(&self, tx: &Transaction) -> Result<()> {
        // Optimized validation with reduced lock contention

        // Check write-write conflicts using lock-free data structures
        for write_op in &tx.write_set {
            if let Some(existing_lock) = self.write_locks.get(&write_op.key) {
                if existing_lock.tx_id != tx.id {
                    self.conflict_count.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::Transaction(
                        "Write-write conflict detected".to_string(),
                    ));
                }
            }
        }

        // Fast read-write conflict detection
        for read_op in &tx.read_set {
            // Check if there's a newer version in the version store
            let current_version = self
                .version_store
                .get_latest_version(&read_op.key)
                .unwrap_or(0);

            if current_version > read_op.version {
                self.conflict_count.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Transaction(
                    "Read-write conflict detected".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn release_locks(&self, tx_id: u64, tx: &Transaction) {
        // Release write locks
        for write_op in &tx.write_set {
            if let Some((_, _existing_lock)) = self
                .write_locks
                .remove_if(&write_op.key, |_, v| v.tx_id == tx_id)
            {
                debug!(
                    "Released write lock for key {:?} by tx {}",
                    write_op.key, tx_id
                );
            }
        }

        // Release read locks
        for read_op in &tx.read_set {
            self.read_locks
                .entry(read_op.key.clone())
                .and_modify(|read_set| {
                    read_set.readers.retain(|r| r.tx_id != tx_id);
                });

            // Remove empty read lock sets
            self.read_locks
                .remove_if(&read_op.key, |_, v| v.readers.is_empty());
        }
    }

    fn process_batch_commits(
        pending_commits: &DashMap<u64, Transaction>,
        version_store: &VersionStore,
        commit_timestamp: &AtomicU64,
        active_transactions: &DashMap<u64, Arc<RwLock<Transaction>>>,
        batch_size: usize,
    ) {
        if pending_commits.is_empty() {
            return;
        }

        let mut batch = Vec::with_capacity(batch_size);

        // Collect batch
        for entry in pending_commits.iter().take(batch_size) {
            batch.push((*entry.key(), entry.value().clone()));
        }

        if batch.is_empty() {
            return;
        }

        // Get single commit timestamp for the entire batch
        let base_commit_ts = commit_timestamp.fetch_add(batch.len() as u64, Ordering::SeqCst) + 1;

        // Apply all writes in the batch
        for (i, (tx_id, tx_data)) in batch.iter().enumerate() {
            let commit_ts = base_commit_ts + i as u64;

            for write_op in &tx_data.write_set {
                version_store.put(
                    write_op.key.clone(),
                    write_op.value.clone(),
                    commit_ts,
                    *tx_id,
                );
            }

            // Mark transaction as committed
            if let Some(tx_arc) = active_transactions.get(tx_id) {
                if let Some(mut tx) = tx_arc.try_write() {
                    tx.commit(commit_ts);
                }
            }

            // Remove from active transactions
            active_transactions.remove(tx_id);
            pending_commits.remove(tx_id);
        }

        debug!("Processed batch commit of {} transactions", batch.len());
    }

    fn cleanup_expired_locks(
        write_locks: &DashMap<Vec<u8>, WriteLockInfo>,
        read_locks: &DashMap<Vec<u8>, ReadLockSet>,
    ) {
        let timeout = Duration::from_secs(30); // 30 second timeout for abandoned locks
        let now = Instant::now();

        // Cleanup expired write locks
        write_locks.retain(|_, lock_info| {
            if now.duration_since(lock_info.timestamp) > timeout {
                warn!("Cleaning up expired write lock for tx {}", lock_info.tx_id);
                false
            } else {
                true
            }
        });

        // Cleanup expired read locks
        read_locks.retain(|_, read_set| {
            if now.duration_since(read_set.last_cleanup) > Duration::from_secs(10) {
                read_set
                    .readers
                    .retain(|reader| now.duration_since(reader.timestamp) <= timeout);
                true
            } else {
                !read_set.readers.is_empty()
            }
        });
    }

    pub fn get_statistics(&self) -> TransactionStatistics {
        TransactionStatistics {
            active_transactions: self.active_transactions.len(),
            commit_count: self.commit_count.load(Ordering::Relaxed),
            abort_count: self.abort_count.load(Ordering::Relaxed),
            conflict_count: self.conflict_count.load(Ordering::Relaxed),
            deadlock_count: self.deadlock_count.load(Ordering::Relaxed),
            pending_commits: self.pending_commits.len(),
            write_locks_held: self.write_locks.len(),
            read_locks_held: self.read_locks.len(),
        }
    }

    pub fn get_read_timestamp(&self) -> u64 {
        self.commit_timestamp.load(Ordering::Acquire)
    }

    pub fn active_transaction_count(&self) -> usize {
        self.active_transactions.len()
    }
}

#[derive(Debug, Clone)]
pub struct TransactionStatistics {
    pub active_transactions: usize,
    pub commit_count: u64,
    pub abort_count: u64,
    pub conflict_count: u64,
    pub deadlock_count: u64,
    pub pending_commits: usize,
    pub write_locks_held: usize,
    pub read_locks_held: usize,
}

impl Drop for OptimizedTransactionManager {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::VersionStore;

    #[test]
    fn test_optimized_transaction_manager_basic() {
        let version_store = Arc::new(VersionStore::new());
        let mut manager = OptimizedTransactionManager::new(100, version_store);
        manager.start_background_processing();

        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();

        assert_ne!(tx1, tx2);
        assert_eq!(manager.active_transaction_count(), 2);

        manager.commit_sync(tx1).unwrap();
        assert_eq!(manager.active_transaction_count(), 1);

        manager.abort(tx2).unwrap();
        assert_eq!(manager.active_transaction_count(), 0);
    }

    #[test]
    fn test_write_lock_conflict() {
        let version_store = Arc::new(VersionStore::new());
        let manager = OptimizedTransactionManager::new(100, version_store);

        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();

        // tx1 acquires write lock
        assert!(manager.acquire_write_lock(tx1, b"key1").is_ok());

        // tx2 should fail to acquire same write lock
        assert!(manager.acquire_write_lock(tx2, b"key1").is_err());

        manager.abort(tx1).unwrap();
        manager.abort(tx2).unwrap();
    }

    #[test]
    fn test_read_write_lock_conflict() {
        let version_store = Arc::new(VersionStore::new());
        let manager = OptimizedTransactionManager::new(100, version_store);

        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();

        // tx1 acquires read lock
        assert!(manager.acquire_read_lock(tx1, b"key1").is_ok());

        // tx2 should fail to acquire write lock
        assert!(manager.acquire_write_lock(tx2, b"key1").is_err());

        manager.abort(tx1).unwrap();
        manager.abort(tx2).unwrap();
    }
}
