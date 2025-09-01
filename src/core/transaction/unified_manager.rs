use crate::core::error::{Error, Result};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info};

// Local transaction type definitions
#[derive(Debug, Clone, PartialEq)]
pub enum TxState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct ReadOp {
    pub key: Bytes,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub struct WriteOp {
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub prev_version: u64,
}

#[derive(Debug, Default)]
pub struct TransactionMetrics {
    pub active_transactions: usize,
    pub successful_commits: usize,
    pub failed_commits: usize,
    pub conflicts: usize,
}

/// Unified production-ready transaction manager combining MVCC, optimizations, and safety guarantees
#[derive(Debug)]
pub struct UnifiedTransactionManager {
    // Core MVCC infrastructure
    next_tx_id: Arc<AtomicU64>,
    pub next_timestamp: Arc<AtomicU64>,
    
    // Transaction tracking with high-performance concurrent collections
    active_transactions: Arc<DashMap<u64, Arc<RwLock<UnifiedTransaction>>>>,
    
    // MVCC version store and visibility tracking
    version_store: Arc<UnifiedVersionStore>,
    visibility_tracker: Arc<RwLock<VisibilityTracker>>,
    
    // Optimized lock-free conflict detection
    write_locks: Arc<DashMap<Bytes, WriteLockInfo>>,
    read_locks: Arc<DashMap<Bytes, ReadLockSet>>,
    
    // Configuration
    max_active_transactions: usize,
    lock_timeout: Duration,
    gc_interval: Duration,
    
    // Performance optimizations
    tx_pool: Arc<Mutex<Vec<UnifiedTransaction>>>,
    batch_commit_size: usize,
    pending_commits: Arc<DashMap<u64, UnifiedTransaction>>,
    
    // Statistics and monitoring
    stats: Arc<TransactionStats>,
    
    // Background processing
    background_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    shutdown: Arc<AtomicUsize>,
}

/// Enhanced transaction with MVCC support and optimizations
#[derive(Debug, Clone)]
pub struct UnifiedTransaction {
    pub id: u64,
    pub read_timestamp: u64,
    pub commit_timestamp: Option<u64>,
    pub snapshot: TransactionSnapshot,
    pub write_set: FxHashMap<Bytes, WriteOp>,
    pub read_set: SmallVec<[ReadOp; 8]>,
    pub state: TxState,
    pub start_time: Instant,
    pub priority: TransactionPriority,
}

/// MVCC snapshot for transaction isolation
#[derive(Debug, Clone)]
pub struct TransactionSnapshot {
    pub timestamp: u64,
    pub active_tx_ids: HashSet<u64>,
    pub min_active_tx_id: u64,
}

/// Transaction priority for deadlock prevention
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransactionPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Lock information for optimized conflict detection
#[derive(Debug, Clone)]
struct WriteLockInfo {
    tx_id: u64,
    timestamp: Instant,
    priority: TransactionPriority,
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

/// MVCC visibility tracker for snapshot isolation
#[derive(Debug)]
struct VisibilityTracker {
    committed_transactions: BTreeMap<u64, u64>,
    active_transactions: HashSet<u64>,
    min_active_timestamp: u64,
}

/// Unified version store with MVCC and optimization support
#[derive(Debug)]
pub struct UnifiedVersionStore {
    versions: Arc<DashMap<Vec<u8>, Vec<VersionEntry>>>,
    total_versions: AtomicU64,
    garbage_collected: AtomicU64,
    reserved_slots: Arc<DashMap<(Vec<u8>, u64), u64>>, // (key, timestamp) -> tx_id
}

#[derive(Debug, Clone)]
struct VersionEntry {
    value: Option<Vec<u8>>,
    timestamp: u64,
    tx_id: u64,
    is_reserved: bool,
}

#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub value: Option<Vec<u8>>,
    pub timestamp: u64,
}

/// Comprehensive transaction statistics
#[derive(Debug)]
pub struct TransactionStats {
    pub active_transactions: AtomicU64,
    pub commit_count: AtomicU64,
    pub abort_count: AtomicU64,
    pub conflict_count: AtomicU64,
    pub deadlock_count: AtomicU64,
    pub gc_runs: AtomicU64,
    pub versions_cleaned: AtomicU64,
}

impl UnifiedTransactionManager {
    pub fn new(max_active_transactions: usize) -> Arc<Self> {
        let shutdown = Arc::new(AtomicUsize::new(0));
        
        let manager = Arc::new(Self {
            next_tx_id: Arc::new(AtomicU64::new(1)),
            next_timestamp: Arc::new(AtomicU64::new(1)),
            active_transactions: Arc::new(DashMap::with_capacity(max_active_transactions)),
            version_store: Arc::new(UnifiedVersionStore::new()),
            visibility_tracker: Arc::new(RwLock::new(VisibilityTracker::new())),
            write_locks: Arc::new(DashMap::new()),
            read_locks: Arc::new(DashMap::new()),
            max_active_transactions,
            lock_timeout: Duration::from_millis(50),
            gc_interval: Duration::from_secs(30),
            tx_pool: Arc::new(Mutex::new(Vec::with_capacity(64))),
            batch_commit_size: 16,
            pending_commits: Arc::new(DashMap::new()),
            stats: Arc::new(TransactionStats::new()),
            background_thread: Arc::new(Mutex::new(None)),
            shutdown,
        });
        
        // Start background processing
        let manager_clone = Arc::clone(&manager);
        *manager.background_thread.lock() = Some(thread::spawn(move || {
            manager_clone.background_loop();
        }));
        
        manager
    }
    
    pub fn begin(&self) -> Result<u64> {
        self.begin_with_priority(TransactionPriority::Normal)
    }
    
    pub fn begin_with_priority(&self, priority: TransactionPriority) -> Result<u64> {
        // Fast path: check transaction limit
        if self.active_transactions.len() >= self.max_active_transactions {
            return Err(Error::TransactionLimitReached {
                limit: self.max_active_transactions,
            });
        }
        
        // CRITICAL FIX: Check for timestamp overflow BEFORE allocation
        let current_ts = self.next_timestamp.load(Ordering::Acquire);
        if current_ts >= u64::MAX - 1000 { // Reserve 1000 for safety
            return Err(Error::TimestampOverflow);
        }
        
        // Atomically get transaction ID and read timestamp
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let read_timestamp = self.next_timestamp.fetch_add(1, Ordering::SeqCst);
        
        // Create MVCC snapshot
        let snapshot = {
            let visibility = self.visibility_tracker.read();
            TransactionSnapshot {
                timestamp: read_timestamp,
                active_tx_ids: visibility.active_transactions.clone(),
                min_active_tx_id: visibility.active_transactions.iter().min().copied().unwrap_or(tx_id),
            }
        };
        
        // Register transaction in visibility tracker
        {
            let mut visibility = self.visibility_tracker.write();
            visibility.active_transactions.insert(tx_id);
            visibility.update_min_active_timestamp();
        }
        
        // Try to reuse transaction object from pool
        let tx = {
            let mut pool = self.tx_pool.lock();
            if let Some(mut recycled_tx) = pool.pop() {
                recycled_tx.reset(tx_id, read_timestamp, snapshot, priority);
                recycled_tx
            } else {
                UnifiedTransaction::new(tx_id, read_timestamp, snapshot, priority)
            }
        };
        
        let tx_arc = Arc::new(RwLock::new(tx));
        self.active_transactions.insert(tx_id, tx_arc);
        self.stats.active_transactions.fetch_add(1, Ordering::Release);
        
        Ok(tx_id)
    }
    
    pub fn get(&self, tx_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tx_arc = self.get_transaction(tx_id)?;
        
        // Check write set first (local writes)
        let write_set_value = {
            let tx = tx_arc.read();
            tx.write_set.get(key).map(|w| w.value.clone())
        };
        
        if let Some(value) = write_set_value {
            return Ok(value.map(|v| v.to_vec()));
        }
        
        // Acquire read lock for consistency
        self.acquire_read_lock(tx_id, key)?;
        
        // Get snapshot and read from version store
        let (snapshot, read_timestamp) = {
            let tx = tx_arc.read();
            (tx.snapshot.clone(), tx.read_timestamp)
        };
        
        let visible_value = {
            let visibility = self.visibility_tracker.read();
            self.version_store.get_visible(key, &snapshot, &visibility)?
        };
        
        // Record read operation
        {
            let version = self.version_store.get_version_at(key, read_timestamp);
            let mut tx = tx_arc.write();
            tx.read_set.push(ReadOp {
                key: key.to_vec().into(),
                version,
            });
        }
        
        Ok(visible_value)
    }
    
    pub fn put(&self, tx_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        // Acquire write lock first
        self.acquire_write_lock(tx_id, key)?;
        
        let tx_arc = self.get_transaction(tx_id)?;
        let mut tx = tx_arc.write();
        
        if !tx.is_active() {
            return Err(Error::TransactionInvalidState {
                id: tx_id,
                state: format!("{:?}", tx.state),
            });
        }
        
        let prev_version = self.version_store.get_version_at(key, tx.read_timestamp);
        tx.add_write(key.to_vec(), Some(value.to_vec()), prev_version);
        
        Ok(())
    }
    
    pub fn delete(&self, tx_id: u64, key: &[u8]) -> Result<()> {
        self.acquire_write_lock(tx_id, key)?;
        
        let tx_arc = self.get_transaction(tx_id)?;
        let mut tx = tx_arc.write();
        
        if !tx.is_active() {
            return Err(Error::TransactionInvalidState {
                id: tx_id,
                state: format!("{:?}", tx.state),
            });
        }
        
        let prev_version = self.version_store.get_version_at(key, tx.read_timestamp);
        tx.add_write(key.to_vec(), None::<Vec<u8>>, prev_version);
        
        Ok(())
    }
    
    pub fn commit(&self, tx_id: u64) -> Result<()> {
        // Async commit for performance
        let tx_arc = self.get_transaction(tx_id)?;
        
        let tx_data = {
            let mut tx = tx_arc.write();
            if !tx.is_active() {
                return Err(Error::TransactionInvalidState {
                    id: tx_id,
                    state: format!("{:?}", tx.state),
                });
            }
            tx.state = TxState::Preparing;
            tx.clone()
        };
        
        // Validate MVCC constraints
        self.validate_mvcc_constraints(&tx_data)?;
        
        // Add to pending commits for batch processing
        self.pending_commits.insert(tx_id, tx_data);
        
        Ok(())
    }
    
    pub fn commit_sync(&self, tx_id: u64) -> Result<()> {
        // Synchronous commit for when immediate consistency is required
        let commit_ts = self.next_timestamp.fetch_add(1, Ordering::SeqCst) + 1;
        
        let tx_arc = self.get_transaction(tx_id)?;
        
        let tx_data = {
            let mut tx = tx_arc.write();
            if !tx.is_active() {
                return Err(Error::TransactionInvalidState {
                    id: tx_id,
                    state: format!("{:?}", tx.state),
                });
            }
            tx.state = TxState::Preparing;
            let data = tx.clone();
            tx.commit(commit_ts);
            data
        };
        
        // Validate with atomic reservation
        self.atomic_validate_and_reserve(&tx_data, commit_ts)?;
        
        // Apply writes
        for write_op in tx_data.write_set.values() {
            self.version_store.complete_reserved_write(
                &write_op.key,
                commit_ts,
                write_op.value.as_ref(),
                tx_id,
            );
        }
        
        // Update visibility
        {
            let mut visibility = self.visibility_tracker.write();
            visibility.active_transactions.remove(&tx_id);
            visibility.committed_transactions.insert(tx_id, commit_ts);
            visibility.update_min_active_timestamp();
        }
        
        // Release locks and cleanup
        self.release_locks_safe(tx_id, &tx_data);
        self.active_transactions.remove(&tx_id);
        self.stats.active_transactions.fetch_sub(1, Ordering::Release);
        self.stats.commit_count.fetch_add(1, Ordering::Release);
        
        Ok(())
    }
    
    pub fn abort(&self, tx_id: u64) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;
        
        let tx_data = {
            let mut tx = tx_arc.write();
            if !tx.is_active() && tx.state != TxState::Preparing {
                return Err(Error::TransactionInvalidState {
                    id: tx_id,
                    state: format!("{:?}", tx.state),
                });
            }
            tx.abort();
            tx.clone()
        };
        
        // Update visibility
        {
            let mut visibility = self.visibility_tracker.write();
            visibility.active_transactions.remove(&tx_id);
            visibility.update_min_active_timestamp();
        }
        
        // Release locks and cleanup
        self.release_locks_safe(tx_id, &tx_data);
        self.active_transactions.remove(&tx_id);
        self.pending_commits.remove(&tx_id);
        
        // Return to pool
        {
            let mut pool = self.tx_pool.lock();
            if pool.len() < pool.capacity() {
                pool.push(tx_data);
            }
        }
        
        self.stats.active_transactions.fetch_sub(1, Ordering::Release);
        self.stats.abort_count.fetch_add(1, Ordering::Release);
        
        Ok(())
    }
    
    pub fn get_transaction(&self, tx_id: u64) -> Result<Arc<RwLock<UnifiedTransaction>>> {
        self.active_transactions
            .get(&tx_id)
            .map(|entry| entry.value().clone())
            .ok_or(Error::TransactionNotFound { id: tx_id })
    }
    
    pub fn acquire_write_lock(&self, tx_id: u64, key: &[u8]) -> Result<()> {
        let priority = {
            let tx_arc = self.get_transaction(tx_id)?;
            let tx = tx_arc.read();
            tx.priority.clone()
        };
        
        let lock_info = WriteLockInfo {
            tx_id,
            timestamp: Instant::now(),
            priority,
        };
        
        match self.write_locks.entry(key.to_vec().into()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                // Check for conflicting read locks
                if let Some(read_set) = self.read_locks.get(&Bytes::from(key.to_vec())) {
                    if read_set.readers.iter().any(|r| r.tx_id != tx_id) {
                        self.stats.conflict_count.fetch_add(1, Ordering::Relaxed);
                        return Err(Error::Transaction("Read-write conflict".to_string()));
                    }
                }
                entry.insert(lock_info);
                Ok(())
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let existing = entry.get();
                if existing.tx_id == tx_id {
                    Ok(())
                } else if existing.timestamp.elapsed() > self.lock_timeout {
                    entry.remove();
                    self.acquire_write_lock(tx_id, key)
                } else {
                    // Deadlock prevention using wound-wait
                    if lock_info.priority > existing.priority || 
                       (lock_info.priority == existing.priority && tx_id < existing.tx_id) {
                        self.stats.deadlock_count.fetch_add(1, Ordering::Relaxed);
                        Err(Error::Transaction("Deadlock detected".to_string()))
                    } else {
                        self.stats.conflict_count.fetch_add(1, Ordering::Relaxed);
                        Err(Error::Transaction("Write-write conflict".to_string()))
                    }
                }
            }
        }
    }
    
    pub fn acquire_read_lock(&self, tx_id: u64, key: &[u8]) -> Result<()> {
        // Check for write locks
        if let Some(write_lock) = self.write_locks.get(key) {
            if write_lock.tx_id != tx_id {
                self.stats.conflict_count.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Transaction("Write-read conflict".to_string()));
            }
            return Ok(());
        }
        
        // Add to read lock set
        let read_info = ReadLockInfo {
            tx_id,
            timestamp: Instant::now(),
        };
        
        self.read_locks
            .entry(key.to_vec().into())
            .and_modify(|read_set| {
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
    
    fn validate_mvcc_constraints(&self, tx: &UnifiedTransaction) -> Result<()> {
        // Check for write-write conflicts using MVCC
        for write_op in tx.write_set.values() {
            let versions = self.version_store.get_versions(&write_op.key);
            
            for version in versions {
                if version.timestamp > tx.read_timestamp && 
                   version.tx_id != tx.id && 
                   !version.is_reserved {
                    self.stats.conflict_count.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::Transaction(
                        "MVCC write-write conflict detected".to_string(),
                    ));
                }
            }
        }
        
        Ok(())
    }
    
    fn atomic_validate_and_reserve(&self, tx: &UnifiedTransaction, commit_ts: u64) -> Result<()> {
        // Reserve all write slots atomically
        let mut reserved_keys = SmallVec::<[&Bytes; 16]>::new();
        
        for write_op in tx.write_set.values() {
            if !self.version_store.try_reserve_write(&write_op.key, tx.read_timestamp, commit_ts) {
                // Rollback all reservations
                for key in &reserved_keys {
                    self.version_store.cancel_reserved_write(key, commit_ts);
                }
                self.stats.conflict_count.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Transaction("Atomic validation failed".to_string()));
            }
            reserved_keys.push(&write_op.key);
        }
        
        Ok(())
    }
    
    fn release_locks_safe(&self, tx_id: u64, tx: &UnifiedTransaction) {
        // Release write locks first
        for write_op in tx.write_set.values() {
            self.write_locks.remove_if(&write_op.key, |_, v| v.tx_id == tx_id);
        }
        
        // Release read locks
        for read_op in &tx.read_set {
            let should_remove = self.read_locks
                .entry(read_op.key.clone())
                .and_modify(|read_set| {
                    read_set.readers.retain(|r| r.tx_id != tx_id);
                })
                .or_default()
                .readers.is_empty();
                
            if should_remove {
                self.read_locks.remove_if(&read_op.key, |_, v| v.readers.is_empty());
            }
        }
    }
    
    fn background_loop(&self) {
        debug!("Starting unified transaction manager background loop");
        
        while self.shutdown.load(Ordering::Acquire) == 0 {
            // Process batch commits
            self.process_batch_commits();
            
            // Cleanup expired locks
            self.cleanup_expired_locks();
            
            // Garbage collection
            if self.stats.gc_runs.load(Ordering::Acquire) % 10 == 0 {
                self.run_garbage_collection();
            }
            
            thread::sleep(Duration::from_millis(100));
        }
        
        debug!("Unified transaction manager background loop stopped");
    }
    
    fn process_batch_commits(&self) {
        if self.pending_commits.is_empty() {
            return;
        }
        
        let mut batch = Vec::with_capacity(self.batch_commit_size);
        let mut tx_ids_to_remove = Vec::with_capacity(self.batch_commit_size);
        
        // Collect batch
        for entry in self.pending_commits.iter().take(self.batch_commit_size) {
            let tx_id = *entry.key();
            let tx_data = entry.value().clone();
            batch.push((tx_id, tx_data));
            tx_ids_to_remove.push(tx_id);
        }
        
        if batch.is_empty() {
            return;
        }
        
        // CRITICAL FIX: Get consecutive timestamps without gaps
        let base_commit_ts = self.next_timestamp.fetch_add(1, Ordering::SeqCst) + 1;
        let mut next_ts = base_commit_ts;
        
        // Apply writes
        for (tx_id, tx_data) in batch.iter() {
            let commit_ts = next_ts;
            next_ts = self.next_timestamp.fetch_add(1, Ordering::SeqCst) + 1;
            
            for write_op in tx_data.write_set.values() {
                self.version_store.put_version(
                    write_op.key.to_vec(),
                    write_op.value.as_ref().map(|v| v.to_vec()),
                    commit_ts,
                    *tx_id,
                );
            }
            
            // Update transaction state
            if let Some(tx_arc) = self.active_transactions.get(tx_id) {
                if let Some(mut tx) = tx_arc.try_write() {
                    tx.commit(commit_ts);
                }
            }
            
            // Release locks
            self.release_locks_safe(*tx_id, tx_data);
        }
        
        // Update visibility tracker
        {
            let mut visibility = self.visibility_tracker.write();
            let mut batch_commit_ts = base_commit_ts;
            for (tx_id, _) in batch.iter() {
                visibility.active_transactions.remove(tx_id);
                visibility.committed_transactions.insert(*tx_id, batch_commit_ts);
                batch_commit_ts += 1;
            }
            visibility.update_min_active_timestamp();
        }
        
        // Cleanup
        for tx_id in tx_ids_to_remove {
            self.active_transactions.remove(&tx_id);
            self.pending_commits.remove(&tx_id);
            self.stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
            self.stats.commit_count.fetch_add(1, Ordering::Relaxed);
        }
        
        debug!("Processed batch commit of {} transactions", batch.len());
    }
    
    fn cleanup_expired_locks(&self) {
        let timeout = Duration::from_secs(30);
        let now = Instant::now();
        
        // Cleanup write locks
        self.write_locks.retain(|_, lock_info| {
            now.duration_since(lock_info.timestamp) <= timeout
        });
        
        // Cleanup read locks
        self.read_locks.retain(|_, read_set| {
            if now.duration_since(read_set.last_cleanup) > Duration::from_secs(10) {
                read_set.readers.retain(|reader| {
                    now.duration_since(reader.timestamp) <= timeout
                });
            }
            !read_set.readers.is_empty()
        });
    }
    
    pub fn run_garbage_collection(&self) {
        let min_timestamp = {
            let visibility = self.visibility_tracker.read();
            visibility.min_active_timestamp
        };
        
        let collected = self.version_store.garbage_collect(min_timestamp);
        if collected > 0 {
            self.stats.versions_cleaned.fetch_add(collected as u64, Ordering::Relaxed);
            info!("Garbage collected {} old versions", collected);
        }
        
        self.stats.gc_runs.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(1, Ordering::Release);
        
        if let Some(handle) = self.background_thread.lock().take() {
            let _ = handle.join();
        }
    }
    
    pub fn get_metrics(&self) -> TransactionMetrics {
        TransactionMetrics {
            active_transactions: self.stats.active_transactions.load(Ordering::Relaxed) as usize,
            successful_commits: self.stats.commit_count.load(Ordering::Relaxed) as usize,
            failed_commits: self.stats.abort_count.load(Ordering::Relaxed) as usize,
            conflicts: self.stats.conflict_count.load(Ordering::Relaxed) as usize,
        }
    }
    
    pub fn get_detailed_stats(&self) -> TransactionStats {
        TransactionStats {
            active_transactions: AtomicU64::new(self.stats.active_transactions.load(Ordering::Relaxed)),
            commit_count: AtomicU64::new(self.stats.commit_count.load(Ordering::Relaxed)),
            abort_count: AtomicU64::new(self.stats.abort_count.load(Ordering::Relaxed)),
            conflict_count: AtomicU64::new(self.stats.conflict_count.load(Ordering::Relaxed)),
            deadlock_count: AtomicU64::new(self.stats.deadlock_count.load(Ordering::Relaxed)),
            gc_runs: AtomicU64::new(self.stats.gc_runs.load(Ordering::Relaxed)),
            versions_cleaned: AtomicU64::new(self.stats.versions_cleaned.load(Ordering::Relaxed)),
        }
    }
    
    pub fn active_transaction_count(&self) -> usize {
        self.active_transactions.len()
    }
    
    pub fn get_read_timestamp(&self) -> u64 {
        self.next_timestamp.load(Ordering::Acquire)
    }
    
    pub fn stop(&self) {
        self.shutdown();
    }
    
    pub fn cleanup_old_transactions(&self, _max_age: std::time::Duration) {
        self.cleanup_expired_locks();
    }
    
    pub fn get_statistics(&self) -> TransactionStats {
        self.get_detailed_stats()
    }
}

impl UnifiedTransaction {
    fn new(id: u64, read_timestamp: u64, snapshot: TransactionSnapshot, priority: TransactionPriority) -> Self {
        Self {
            id,
            read_timestamp,
            commit_timestamp: None,
            snapshot,
            write_set: FxHashMap::default(),
            read_set: SmallVec::new(),
            state: TxState::Active,
            start_time: Instant::now(),
            priority,
        }
    }
    
    fn reset(&mut self, id: u64, read_timestamp: u64, snapshot: TransactionSnapshot, priority: TransactionPriority) {
        self.id = id;
        self.read_timestamp = read_timestamp;
        self.commit_timestamp = None;
        self.snapshot = snapshot;
        self.write_set.clear();
        self.read_set.clear();
        self.state = TxState::Active;
        self.start_time = Instant::now();
        self.priority = priority;
    }
    
    pub fn is_active(&self) -> bool {
        self.state == TxState::Active
    }
    
    pub fn add_write(&mut self, key: impl Into<Bytes>, value: Option<impl Into<Bytes>>, prev_version: u64) {
        let key = key.into();
        let value = value.map(|v| v.into());
        self.write_set.insert(key.clone(), WriteOp {
            key,
            value,
            prev_version,
        });
    }
    
    pub fn commit(&mut self, timestamp: u64) {
        self.state = TxState::Committed;
        self.commit_timestamp = Some(timestamp);
    }
    
    pub fn abort(&mut self) {
        self.state = TxState::Aborted;
    }
    
    pub fn get_write(&self, key: &[u8]) -> Option<&WriteOp> {
        self.write_set.get(key)
    }
    
    pub fn add_read(&mut self, key: Vec<u8>, version: u64) {
        self.read_set.push(ReadOp {
            key: key.into(),
            version,
        });
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
        if version.is_reserved {
            return false;
        }
        
        if version.timestamp <= snapshot.timestamp {
            !snapshot.active_tx_ids.contains(&version.tx_id)
        } else {
            false
        }
    }
}

impl UnifiedVersionStore {
    pub fn new() -> Self {
        Self {
            versions: Arc::new(DashMap::new()),
            total_versions: AtomicU64::new(0),
            garbage_collected: AtomicU64::new(0),
            reserved_slots: Arc::new(DashMap::new()),
        }
    }
    
    fn put_version(&self, key: Vec<u8>, value: Option<Vec<u8>>, timestamp: u64, tx_id: u64) {
        let version = VersionEntry {
            value,
            timestamp,
            tx_id,
            is_reserved: false,
        };
        
        self.versions
            .entry(key)
            .and_modify(|versions| {
                let pos = versions.iter().position(|v| v.timestamp < timestamp).unwrap_or(versions.len());
                versions.insert(pos, version.clone());
                std::sync::atomic::fence(Ordering::Release);
            })
            .or_insert_with(|| vec![version]);
        
        self.total_versions.fetch_add(1, Ordering::Relaxed);
    }
    
    fn get_visible(&self, key: &[u8], snapshot: &TransactionSnapshot, visibility: &VisibilityTracker) -> Result<Option<Vec<u8>>> {
        if let Some(versions) = self.versions.get(key) {
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
                if version.timestamp <= timestamp && !version.is_reserved {
                    return version.timestamp;
                }
            }
        }
        0
    }
    
    fn try_reserve_write(&self, key: &[u8], _read_timestamp: u64, commit_timestamp: u64) -> bool {
        let key_tuple = (key.to_vec(), commit_timestamp);
        
        // Try to reserve the slot
        match self.reserved_slots.entry(key_tuple) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(0); // Reserved by this transaction
                true
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                false // Already reserved
            }
        }
    }
    
    fn complete_reserved_write(&self, key: &[u8], timestamp: u64, value: Option<&Bytes>, tx_id: u64) {
        // Remove reservation
        self.reserved_slots.remove(&(key.to_vec(), timestamp));
        
        // Add actual version
        self.put_version(
            key.to_vec(),
            value.map(|v| v.to_vec()),
            timestamp,
            tx_id,
        );
    }
    
    fn cancel_reserved_write(&self, key: &[u8], timestamp: u64) {
        self.reserved_slots.remove(&(key.to_vec(), timestamp));
    }
    
    fn garbage_collect(&self, min_timestamp: u64) -> usize {
        let mut collected = 0;
        
        for mut entry in self.versions.iter_mut() {
            let original_len = entry.value().len();
            
            let mut keep_next_old = true;
            entry.value_mut().retain(|v| {
                if v.timestamp >= min_timestamp {
                    true
                } else if keep_next_old && !v.is_reserved {
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
    
    /// Cleanup old versions for version cleanup thread compatibility
    pub fn cleanup_old_versions(&self, before_timestamp: u64, min_versions_to_keep: usize) {
        let mut total_cleaned = 0;
        
        for mut entry in self.versions.iter_mut() {
            let versions = entry.value_mut();
            let original_len = versions.len();
            
            if original_len <= min_versions_to_keep {
                continue;
            }
            
            // Keep the most recent versions and remove old ones
            versions.retain(|v| {
                v.timestamp >= before_timestamp || !v.is_reserved
            });
            
            // Ensure we keep at least min_versions_to_keep
            while versions.len() > min_versions_to_keep && 
                  versions.last().is_some_and(|v| v.timestamp < before_timestamp) {
                versions.pop();
            }
            
            total_cleaned += original_len - versions.len();
        }
        
        if total_cleaned > 0 {
            self.garbage_collected.fetch_add(total_cleaned as u64, Ordering::Relaxed);
        }
    }
    
    /// Put method for compatibility with other version stores
    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>, timestamp: u64, tx_id: u64) {
        self.put_version(key, value, timestamp, tx_id);
    }
    
    /// Get method for compatibility
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(versions) = self.versions.get(key) {
            for version in versions.value() {
                if !version.is_reserved {
                    return version.value.clone();
                }
            }
        }
        None
    }
    
    /// Get versioned value for compatibility
    pub fn get_versioned(&self, key: &[u8], max_timestamp: u64) -> Option<VersionedValue> {
        if let Some(versions) = self.versions.get(key) {
            for version in versions.value() {
                if version.timestamp <= max_timestamp && !version.is_reserved {
                    return Some(VersionedValue {
                        value: version.value.clone(),
                        timestamp: version.timestamp,
                    });
                }
            }
        }
        None
    }
    
    /// Get latest version for compatibility
    pub fn get_latest_version(&self, key: &[u8]) -> Option<(Vec<u8>, u64)> {
        if let Some(versions) = self.versions.get(key) {
            for version in versions.value() {
                if !version.is_reserved {
                    return version.value.as_ref().map(|v| (v.clone(), version.timestamp));
                }
            }
        }
        None
    }
    
}

impl TransactionStats {
    fn new() -> Self {
        Self {
            active_transactions: AtomicU64::new(0),
            commit_count: AtomicU64::new(0),
            abort_count: AtomicU64::new(0),
            conflict_count: AtomicU64::new(0),
            deadlock_count: AtomicU64::new(0),
            gc_runs: AtomicU64::new(0),
            versions_cleaned: AtomicU64::new(0),
        }
    }
}

impl Default for ReadLockSet {
    fn default() -> Self {
        Self {
            readers: Vec::new(),
            last_cleanup: Instant::now(),
        }
    }
}

impl Drop for UnifiedTransactionManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_unified_transaction_basic() {
        let manager = UnifiedTransactionManager::new(100);
        
        let tx_id = manager.begin().unwrap();
        assert_eq!(manager.active_transaction_count(), 1);
        
        manager.put(tx_id, b"key1", b"value1").unwrap();
        
        let value = manager.get(tx_id, b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
        
        manager.commit_sync(tx_id).unwrap();
        assert_eq!(manager.active_transaction_count(), 0);
    }
    
    #[test]
    fn test_mvcc_isolation() {
        let manager = UnifiedTransactionManager::new(100);
        
        let tx1 = manager.begin().unwrap();
        manager.put(tx1, b"key1", b"value1").unwrap();
        
        let tx2 = manager.begin().unwrap();
        assert_eq!(manager.get(tx2, b"key1").unwrap(), None);
        
        manager.commit_sync(tx1).unwrap();
        
        // tx2 still shouldn't see committed data (snapshot isolation)
        assert_eq!(manager.get(tx2, b"key1").unwrap(), None);
        
        let tx3 = manager.begin().unwrap();
        assert_eq!(manager.get(tx3, b"key1").unwrap(), Some(b"value1".to_vec()));
    }
    
    #[test]
    fn test_write_conflict() {
        let manager = UnifiedTransactionManager::new(100);
        
        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();
        
        manager.put(tx1, b"key1", b"value1").unwrap();
        
        // Should fail due to write lock conflict
        assert!(manager.put(tx2, b"key1", b"value2").is_err());
    }
    
    #[test]
    fn test_priority_deadlock_prevention() {
        let manager = UnifiedTransactionManager::new(100);
        
        let tx1 = manager.begin_with_priority(TransactionPriority::High).unwrap();
        let tx2 = manager.begin_with_priority(TransactionPriority::Low).unwrap();
        
        manager.put(tx1, b"key1", b"value1").unwrap();
        
        // Lower priority transaction should be aborted
        assert!(manager.put(tx2, b"key1", b"value2").is_err());
    }
}
