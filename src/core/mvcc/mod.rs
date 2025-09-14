use crate::core::error::{Error, Result};
use dashmap::{DashMap, DashSet};
use parking_lot::{RwLock, Mutex};
use std::collections::{BTreeMap, VecDeque, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::cmp::Ordering as CmpOrdering;
use std::ops::Bound;
use crossbeam_skiplist::SkipMap;
use bytes::Bytes;

pub type TransactionId = u64;
pub type Timestamp = u64;
pub type VersionId = u64;

const VACUUM_INTERVAL: Duration = Duration::from_secs(60);
const MAX_SNAPSHOT_AGE: Duration = Duration::from_secs(300);
const GC_BATCH_SIZE: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MVCCVersion {
    pub transaction_id: TransactionId,
    pub timestamp: Timestamp,
    pub version_id: VersionId,
}

impl MVCCVersion {
    pub fn new(transaction_id: TransactionId, timestamp: Timestamp) -> Self {
        static VERSION_COUNTER: AtomicU64 = AtomicU64::new(1);
        
        Self {
            transaction_id,
            timestamp,
            version_id: VERSION_COUNTER.fetch_add(1, Ordering::SeqCst),
        }
    }
}

#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub version: MVCCVersion,
    pub value: Option<Bytes>,
    pub deleted: bool,
    pub prev_version: Option<Arc<VersionedValue>>,
}

impl VersionedValue {
    pub fn new(version: MVCCVersion, value: Option<Bytes>) -> Self {
        Self {
            version,
            value: value.clone(),
            deleted: value.is_none(),
            prev_version: None,
        }
    }
    
    pub fn with_prev(version: MVCCVersion, value: Option<Bytes>, prev: Arc<VersionedValue>) -> Self {
        Self {
            version,
            value,
            deleted: value.is_none(),
            prev_version: Some(prev),
        }
    }
    
    pub fn is_visible_to(&self, snapshot: &TransactionSnapshot) -> bool {
        snapshot.can_see(self.version.transaction_id)
    }
    
    pub fn find_visible_version(&self, snapshot: &TransactionSnapshot) -> Option<&VersionedValue> {
        if self.is_visible_to(snapshot) {
            Some(self)
        } else if let Some(prev) = &self.prev_version {
            prev.find_visible_version(snapshot)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionSnapshot {
    pub transaction_id: TransactionId,
    pub timestamp: Timestamp,
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub active_transactions: HashSet<TransactionId>,
    pub committed_transactions: Arc<DashSet<TransactionId>>,
}

impl TransactionSnapshot {
    pub fn new(
        transaction_id: TransactionId,
        active_transactions: HashSet<TransactionId>,
        committed_transactions: Arc<DashSet<TransactionId>>,
    ) -> Self {
        let xmin = active_transactions.iter().min().copied().unwrap_or(transaction_id);
        let xmax = active_transactions.iter().max().copied().unwrap_or(transaction_id);
        
        Self {
            transaction_id,
            timestamp: current_timestamp(),
            xmin,
            xmax,
            active_transactions,
            committed_transactions,
        }
    }
    
    pub fn can_see(&self, other_txn_id: TransactionId) -> bool {
        if other_txn_id == self.transaction_id {
            return true;
        }
        
        if other_txn_id >= self.xmax {
            return false;
        }
        
        if other_txn_id < self.xmin {
            return self.committed_transactions.contains(&other_txn_id);
        }

        !self.active_transactions.contains(&other_txn_id) &&
        self.committed_transactions.contains(&other_txn_id)
    }
}

pub struct MVCCEngine {
    data: Arc<SkipMap<Bytes, Arc<RwLock<VersionedValue>>>>,
    transaction_manager: Arc<TransactionManager>,
    vacuum_manager: Arc<VacuumManager>,
    conflict_manager: Arc<ConflictManager>,
    metrics: Arc<MVCCMetrics>,
}

impl MVCCEngine {
    pub fn new() -> Self {
        let transaction_manager = Arc::new(TransactionManager::new());
        let vacuum_manager = Arc::new(VacuumManager::new());
        
        Self {
            data: Arc::new(SkipMap::new()),
            transaction_manager: transaction_manager.clone(),
            vacuum_manager: vacuum_manager.clone(),
            conflict_manager: Arc::new(ConflictManager::new()),
            metrics: Arc::new(MVCCMetrics::new()),
        }
    }
    
    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Result<Transaction> {
        self.transaction_manager.begin(isolation_level)
    }
    
    pub fn get(&self, key: impl Into<Bytes>, txn: &Transaction) -> Result<Option<Bytes>> {
        let key_bytes = key.into();
        
        if let Some(entry) = self.data.get(&key_bytes) {
            let versioned = entry.value().read();
            
            if let Some(visible) = versioned.find_visible_version(&txn.snapshot) {
                if !visible.deleted {
                    self.metrics.reads.fetch_add(1, Ordering::Relaxed);
                    return Ok(visible.value.clone());
                }
            }
        }
        
        Ok(None)
    }
    
    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>, txn: &mut Transaction) -> Result<()> {
        let key_bytes = key.into();
        let value_bytes = value.into();
        
        let key_clone = key_bytes.clone();
        let value_clone = value_bytes.clone();
        txn.add_write(key_clone, Some(value_clone));
        
        if txn.isolation_level == IsolationLevel::Serializable {
            self.conflict_manager.check_write_conflict(&key_bytes, txn)?;
        }
        
        let version = MVCCVersion::new(txn.id, current_timestamp());
        
        match self.data.get(&key_bytes) {
            Some(entry) => {
                let mut versioned = entry.value().write();
                let prev = Arc::new(versioned.clone());
                *versioned = VersionedValue::with_prev(version, Some(value_bytes), prev);
            }
            None => {
                let versioned = VersionedValue::new(version, Some(value_bytes));
                self.data.insert(key_bytes, Arc::new(RwLock::new(versioned)));
            }
        }
        
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    pub fn delete(&self, key: impl Into<Bytes>, txn: &mut Transaction) -> Result<()> {
        let key_bytes = key.into();
        
        let key_clone = key_bytes.clone();
        txn.add_write(key_clone, None);
        
        if txn.isolation_level == IsolationLevel::Serializable {
            self.conflict_manager.check_write_conflict(&key_bytes, txn)?;
        }
        
        let version = MVCCVersion::new(txn.id, current_timestamp());
        
        match self.data.get(&key_bytes) {
            Some(entry) => {
                let mut versioned = entry.value().write();
                let prev = Arc::new(versioned.clone());
                *versioned = VersionedValue::with_prev(version, None, prev);
            }
            None => {
                let versioned = VersionedValue::new(version, None);
                self.data.insert(key_bytes, Arc::new(RwLock::new(versioned)));
            }
        }
        
        self.metrics.deletes.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    pub fn scan(
        &self,
        start: impl Into<Bytes>,
        end: impl Into<Bytes>,
        txn: &Transaction,
        limit: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let start_key = start.into();
        let end_key = end.into();
        
        let mut results = Vec::new();
        let mut count = 0;
        
        for entry in self.data.range((Bound::Included(start_key), Bound::Excluded(end_key))) {
            if let Some(limit) = limit {
                if count >= limit {
                    break;
                }
            }
            
            let versioned = entry.value().read();
            
            if let Some(visible) = versioned.find_visible_version(&txn.snapshot) {
                if !visible.deleted {
                    if let Some(value) = &visible.value {
                        results.push((entry.key().clone(), value.clone()));
                        count += 1;
                    }
                }
            }
        }
        
        self.metrics.scans.fetch_add(1, Ordering::Relaxed);
        
        Ok(results)
    }
    
    pub fn commit(&self, txn: Transaction) -> Result<()> {
        self.transaction_manager.commit(txn)
    }
    
    pub fn abort(&self, txn: Transaction) -> Result<()> {
        self.transaction_manager.abort(txn)
    }
    
    pub fn vacuum(&self) -> Result<()> {
        self.vacuum_manager.vacuum(&self.data, &self.transaction_manager)
    }
    
    pub fn get_metrics(&self) -> MVCCMetricsSnapshot {
        self.metrics.snapshot()
    }
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    active_transactions: Arc<DashMap<TransactionId, TransactionState>>,
    committed_transactions: Arc<DashSet<TransactionId>>,
    aborted_transactions: Arc<DashSet<TransactionId>>,
    commit_timestamps: Arc<DashMap<TransactionId, Timestamp>>,
}

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub id: TransactionId,
    pub state: TxnState,
    pub start_time: Instant,
    pub isolation_level: IsolationLevel,
    pub read_set: HashSet<Bytes>,
    pub write_set: HashMap<Bytes, Option<Bytes>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TxnState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_transactions: Arc::new(DashMap::new()),
            committed_transactions: Arc::new(DashSet::new()),
            aborted_transactions: Arc::new(DashSet::new()),
            commit_timestamps: Arc::new(DashMap::new()),
        }
    }
    
    pub fn begin(&self, isolation_level: IsolationLevel) -> Result<Transaction> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        
        let active_txns: HashSet<TransactionId> = self.active_transactions
            .iter()
            .map(|entry| *entry.key())
            .collect();
        
        let snapshot = TransactionSnapshot::new(
            txn_id,
            active_txns,
            self.committed_transactions.clone(),
        );
        
        let state = TransactionState {
            id: txn_id,
            state: TxnState::Active,
            start_time: Instant::now(),
            isolation_level,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
        };
        
        self.active_transactions.insert(txn_id, state);
        
        Ok(Transaction {
            id: txn_id,
            snapshot,
            isolation_level,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            state: TxnState::Active,
        })
    }
    
    pub fn commit(&self, mut txn: Transaction) -> Result<()> {
        if let Some(mut state) = self.active_transactions.get_mut(&txn.id) {
            state.state = TxnState::Preparing;
        }
        
        if txn.isolation_level == IsolationLevel::Serializable {
            self.validate_serializable(&txn)?;
        }
        
        if let Some((_, mut state)) = self.active_transactions.remove(&txn.id) {
            state.state = TxnState::Committed;
            self.committed_transactions.insert(txn.id);
            self.commit_timestamps.insert(txn.id, current_timestamp());
        }
        
        txn.state = TxnState::Committed;
        
        Ok(())
    }
    
    pub fn abort(&self, mut txn: Transaction) -> Result<()> {
        if let Some((_, mut state)) = self.active_transactions.remove(&txn.id) {
            state.state = TxnState::Aborted;
            self.aborted_transactions.insert(txn.id);
        }
        
        txn.state = TxnState::Aborted;
        
        Ok(())
    }
    
    fn validate_serializable(&self, txn: &Transaction) -> Result<()> {
        for entry in self.active_transactions.iter() {
            let (other_id, other_state) = entry.pair();
            if *other_id == txn.id {
                continue;
            }
            
            let read_write_conflict = txn.read_set.iter()
                .any(|key| other_state.write_set.contains_key(key));
            
            let write_read_conflict = txn.write_set.keys()
                .any(|key| other_state.read_set.contains(key));
            
            let write_write_conflict = txn.write_set.keys()
                .any(|key| other_state.write_set.contains_key(key));
            
            if read_write_conflict || write_read_conflict || write_write_conflict {
                return Err(Error::SerializationConflict(
                    format!("Conflict with transaction {}", other_id)
                ));
            }
        }
        
        Ok(())
    }
    
    pub fn get_oldest_active(&self) -> Option<TransactionId> {
        self.active_transactions.iter().map(|entry| *entry.key()).min()
    }
}

pub struct Transaction {
    pub id: TransactionId,
    pub snapshot: TransactionSnapshot,
    pub isolation_level: IsolationLevel,
    pub read_set: HashSet<Bytes>,
    pub write_set: HashMap<Bytes, Option<Bytes>>,
    pub state: TxnState,
}

impl Transaction {
    pub fn add_read(&mut self, key: Bytes) {
        self.read_set.insert(key);
    }
    
    pub fn add_write(&mut self, key: Bytes, value: Option<Bytes>) {
        self.write_set.insert(key, value);
    }
    
    pub fn is_active(&self) -> bool {
        self.state == TxnState::Active
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

pub struct ConflictManager {
    lock_table: Arc<DashMap<Bytes, LockInfo>>,
    deadlock_detector: Arc<DeadlockDetector>,
}

#[derive(Debug, Clone)]
struct LockInfo {
    owner: TransactionId,
    lock_type: LockType,
    acquired_at: Instant,
    waiters: Vec<TransactionId>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum LockType {
    Shared,
    Exclusive,
}

impl ConflictManager {
    pub fn new() -> Self {
        Self {
            lock_table: Arc::new(DashMap::new()),
            deadlock_detector: Arc::new(DeadlockDetector::new()),
        }
    }
    
    pub fn check_write_conflict(&self, key: &Bytes, txn: &Transaction) -> Result<()> {
        if let Some(lock_info) = self.lock_table.get(key) {
            if lock_info.owner != txn.id {
                if self.deadlock_detector.would_cause_deadlock(txn.id, lock_info.owner) {
                    return Err(Error::DeadlockDetected);
                }
                
                return Err(Error::WriteConflict(
                    format!("Key locked by transaction {}", lock_info.owner)
                ));
            }
        }
        
        Ok(())
    }
    
    pub fn acquire_lock(&self, key: Bytes, txn_id: TransactionId, lock_type: LockType) -> Result<()> {
        if let Some(mut lock_info) = self.lock_table.get_mut(&key) {
            if lock_info.owner == txn_id {
                if lock_type == LockType::Exclusive && lock_info.lock_type == LockType::Shared {
                    lock_info.lock_type = LockType::Exclusive;
                }
                return Ok(());
            }
            
            if lock_info.lock_type == LockType::Exclusive || lock_type == LockType::Exclusive {
                return Err(Error::LockConflict);
            }
        } else {
            self.lock_table.insert(key, LockInfo {
                owner: txn_id,
                lock_type,
                acquired_at: Instant::now(),
                waiters: Vec::new(),
            });
        }
        
        Ok(())
    }
    
    pub fn release_locks(&self, txn_id: TransactionId) {
        self.lock_table.retain(|_, lock_info| lock_info.owner != txn_id);
    }
}

struct DeadlockDetector {
    wait_for_graph: Arc<DashMap<TransactionId, HashSet<TransactionId>>>,
}

impl DeadlockDetector {
    fn new() -> Self {
        Self {
            wait_for_graph: Arc::new(DashMap::new()),
        }
    }
    
    fn would_cause_deadlock(&self, waiter: TransactionId, holder: TransactionId) -> bool {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(holder);

        while let Some(current) = queue.pop_front() {
            if current == waiter {
                return true;
            }

            if visited.contains(&current) {
                continue;
            }

            visited.insert(current);

            if let Some(dependencies) = self.wait_for_graph.get(&current) {
                for &dep in dependencies {
                    queue.push_back(dep);
                }
            }
        }
        
        false
    }
    
    fn add_edge(&self, from: TransactionId, to: TransactionId) {
        self.wait_for_graph.entry(from).or_insert_with(HashSet::new).insert(to);
    }
    
    fn remove_edges(&self, txn_id: TransactionId) {
        self.wait_for_graph.remove(&txn_id);

        for mut edges in self.wait_for_graph.iter_mut() {
            edges.value_mut().remove(&txn_id);
        }
    }
}

pub struct VacuumManager {
    last_vacuum: Arc<Mutex<Instant>>,
    vacuum_threshold: usize,
    min_transaction_age: Duration,
}

impl VacuumManager {
    pub fn new() -> Self {
        Self {
            last_vacuum: Arc::new(Mutex::new(Instant::now())),
            vacuum_threshold: 1000,
            min_transaction_age: Duration::from_secs(60),
        }
    }
    
    pub fn vacuum(
        &self,
        data: &SkipMap<Bytes, Arc<RwLock<VersionedValue>>>,
        txn_manager: &TransactionManager,
    ) -> Result<()> {
        let mut last_vacuum = self.last_vacuum.lock();
        
        if last_vacuum.elapsed() < VACUUM_INTERVAL {
            return Ok(());
        }
        
        let oldest_active = txn_manager.get_oldest_active();
        let vacuum_horizon = oldest_active.unwrap_or(u64::MAX);
        
        let mut garbage_collected = 0;
        
        for entry in data.iter() {
            let mut versioned = entry.value().write();
            
            if let Some(prev) = &versioned.prev_version {
                if prev.version.transaction_id < vacuum_horizon {
                    let cleaned = Self::clean_version_chain(
                        &versioned,
                        vacuum_horizon,
                        self.vacuum_threshold,
                    );
                    
                    if let Some(cleaned_version) = cleaned {
                        *versioned = cleaned_version;
                        garbage_collected += 1;
                    }
                }
            }
            
            if garbage_collected >= GC_BATCH_SIZE {
                break;
            }
        }
        
        *last_vacuum = Instant::now();
        
        Ok(())
    }
    
    fn clean_version_chain(
        current: &VersionedValue,
        vacuum_horizon: TransactionId,
        max_versions: usize,
    ) -> Option<VersionedValue> {
        let mut versions = Vec::new();
        let mut curr = Some(current);
        
        while let Some(version) = curr {
            if version.version.transaction_id >= vacuum_horizon || versions.len() >= max_versions {
                break;
            }
            
            versions.push(version.clone());
            curr = version.prev_version.as_ref().map(|v| v.as_ref());
        }
        
        if versions.len() <= 1 {
            return None;
        }
        
        let mut result = versions[0].clone();
        result.prev_version = None;
        
        Some(result)
    }
}

struct MVCCMetrics {
    reads: AtomicU64,
    writes: AtomicU64,
    deletes: AtomicU64,
    scans: AtomicU64,
    conflicts: AtomicU64,
    aborts: AtomicU64,
    commits: AtomicU64,
}

impl MVCCMetrics {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            scans: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
            aborts: AtomicU64::new(0),
            commits: AtomicU64::new(0),
        }
    }
    
    fn snapshot(&self) -> MVCCMetricsSnapshot {
        MVCCMetricsSnapshot {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            scans: self.scans.load(Ordering::Relaxed),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            aborts: self.aborts.load(Ordering::Relaxed),
            commits: self.commits.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MVCCMetricsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub scans: u64,
    pub conflicts: u64,
    pub aborts: u64,
    pub commits: u64,
}

fn current_timestamp() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_mvcc_basic_operations() {
        let mvcc = MVCCEngine::new();
        
        let mut txn1 = mvcc.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
        mvcc.put(b"key1", b"value1", &mut txn1).unwrap();
        mvcc.commit(txn1).unwrap();
        
        let txn2 = mvcc.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
        let value = mvcc.get(b"key1", &txn2).unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value1")));
    }
    
    #[test]
    fn test_mvcc_isolation() {
        let mvcc = MVCCEngine::new();
        
        let mut txn1 = mvcc.begin_transaction(IsolationLevel::RepeatableRead).unwrap();
        mvcc.put(b"key1", b"value1", &mut txn1).unwrap();
        
        let txn2 = mvcc.begin_transaction(IsolationLevel::RepeatableRead).unwrap();
        let value = mvcc.get(b"key1", &txn2).unwrap();
        assert_eq!(value, None);
        
        mvcc.commit(txn1).unwrap();
        
        let value = mvcc.get(b"key1", &txn2).unwrap();
        assert_eq!(value, None);
    }
}