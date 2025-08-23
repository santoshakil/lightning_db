use crate::core::error::{Error, Result};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Transaction identifier
pub type TxId = u64;

/// Lock modes following standard database locking
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockMode {
    /// Intent Shared lock (for hierarchical locking)
    IntentionShared,
    /// Intent Exclusive lock (for hierarchical locking)
    IntentionExclusive,
    /// Shared lock - allows concurrent readers
    Shared,
    /// Exclusive lock - exclusive access
    Exclusive,
    /// Shared with Intent Exclusive - for updates
    SharedIntentionExclusive,
    /// Update lock - prevents deadlocks during updates
    Update,
}

impl LockMode {
    /// Check if two lock modes are compatible
    pub fn is_compatible_with(self, other: LockMode) -> bool {
        use LockMode::*;
        match (self, other) {
            // IS is compatible with everything except X
            (IntentionShared, Exclusive) | (Exclusive, IntentionShared) => false,
            (IntentionShared, _) | (_, IntentionShared) => true,
            
            // IE is compatible with IS and IE only
            (IntentionExclusive, Shared) | (Shared, IntentionExclusive) => false,
            (IntentionExclusive, Exclusive) | (Exclusive, IntentionExclusive) => false,
            (IntentionExclusive, Update) | (Update, IntentionExclusive) => false,
            (IntentionExclusive, SharedIntentionExclusive) | (SharedIntentionExclusive, IntentionExclusive) => false,
            (IntentionExclusive, IntentionExclusive) => true,
            
            // S is compatible with S, IS, and SIX
            (Shared, Shared) | (Shared, SharedIntentionExclusive) | (SharedIntentionExclusive, Shared) => true,
            (Shared, _) | (_, Shared) => false,
            
            // SIX is compatible with IS and S only
            (SharedIntentionExclusive, SharedIntentionExclusive) => false,
            (SharedIntentionExclusive, _) | (_, SharedIntentionExclusive) => false,
            
            // U is compatible with IS and S only
            (Update, Update) => false,
            (Update, Exclusive) | (Exclusive, Update) => false,
            (Update, _) | (_, Update) => true,
            
            // X is compatible with nothing
            (Exclusive, _) | (_, Exclusive) => false,
        }
    }

    /// Check if this lock mode provides read access
    pub fn provides_read_access(self) -> bool {
        match self {
            LockMode::Shared | LockMode::Exclusive | LockMode::SharedIntentionExclusive | LockMode::Update => true,
            LockMode::IntentionShared | LockMode::IntentionExclusive => false,
        }
    }

    /// Check if this lock mode provides write access
    pub fn provides_write_access(self) -> bool {
        match self {
            LockMode::Exclusive | LockMode::SharedIntentionExclusive | LockMode::Update => true,
            _ => false,
        }
    }
}

/// A lock request in the queue
#[derive(Debug, Clone)]
pub struct LockRequest {
    pub tx_id: TxId,
    pub mode: LockMode,
    pub granted: bool,
    pub timestamp: Instant,
    pub timeout: Option<Instant>,
}

/// Lock granularity levels
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockGranularity {
    /// Database level lock
    Database,
    /// Table level lock
    Table(String),
    /// Page level lock
    Page(u64),
    /// Row level lock
    Row(Bytes),
    /// Key range lock for phantom prevention
    Range { start: Option<Bytes>, end: Option<Bytes> },
    /// Predicate lock for complex conditions
    Predicate(String),
}

/// A lock held by the system
#[derive(Debug)]
pub struct Lock {
    pub granularity: LockGranularity,
    pub requests: VecDeque<LockRequest>,
    pub granted_modes: HashSet<LockMode>,
}

impl Lock {
    pub fn new(granularity: LockGranularity) -> Self {
        Self {
            granularity,
            requests: VecDeque::new(),
            granted_modes: HashSet::new(),
        }
    }

    /// Check if a lock mode can be granted immediately
    pub fn can_grant(&self, mode: LockMode, tx_id: TxId) -> bool {
        // If we already have this lock for this transaction, it's fine
        if self.requests.iter().any(|req| req.tx_id == tx_id && req.granted && req.mode == mode) {
            return true;
        }

        // Check compatibility with all granted locks
        for granted_mode in &self.granted_modes {
            if !mode.is_compatible_with(*granted_mode) {
                return false;
            }
        }

        // Check if there are waiting requests that would conflict
        for req in &self.requests {
            if !req.granted && !mode.is_compatible_with(req.mode) {
                return false;
            }
        }

        true
    }

    /// Try to grant pending lock requests
    pub fn try_grant_pending(&mut self) -> Vec<(TxId, LockMode)> {
        let mut granted = Vec::new();
        let mut indices_to_grant = Vec::new();

        // First pass: identify which requests can be granted
        for (idx, req) in self.requests.iter().enumerate() {
            if !req.granted {
                // Simple logic: grant if no conflicting modes exist
                let can_grant = match req.mode {
                    LockMode::Shared => !self.granted_modes.contains(&LockMode::Exclusive),
                    LockMode::Exclusive => self.granted_modes.is_empty(),
                    LockMode::Update => !self.granted_modes.contains(&LockMode::Exclusive) && 
                                      !self.granted_modes.contains(&LockMode::Update),
                    LockMode::IntentionShared => true, // Intent locks are generally compatible
                    LockMode::IntentionExclusive => !self.granted_modes.contains(&LockMode::Exclusive),
                    LockMode::SharedIntentionExclusive => !self.granted_modes.contains(&LockMode::Exclusive) &&
                                                          !self.granted_modes.contains(&LockMode::SharedIntentionExclusive),
                };
                
                if can_grant {
                    indices_to_grant.push(idx);
                }
            }
        }

        // Second pass: actually grant the locks
        for idx in indices_to_grant {
            if let Some(req) = self.requests.get_mut(idx) {
                req.granted = true;
                self.granted_modes.insert(req.mode);
                granted.push((req.tx_id, req.mode));
            }
        }

        granted
    }

    /// Remove all locks for a transaction
    pub fn release_locks(&mut self, tx_id: TxId) -> Vec<LockMode> {
        let mut released = Vec::new();

        // Remove all requests for this transaction
        let _original_len = self.requests.len();
        self.requests.retain(|req| {
            if req.tx_id == tx_id {
                if req.granted {
                    released.push(req.mode);
                }
                false
            } else {
                true
            }
        });

        // Rebuild granted_modes set
        self.granted_modes.clear();
        for req in &self.requests {
            if req.granted {
                self.granted_modes.insert(req.mode);
            }
        }

        released
    }
}

/// Lock manager for transaction isolation
#[derive(Debug)]
pub struct LockManager {
    locks: Arc<DashMap<LockGranularity, Arc<Mutex<Lock>>>>,
    tx_locks: Arc<DashMap<TxId, HashSet<LockGranularity>>>,
    wait_graph: Arc<Mutex<HashMap<TxId, HashSet<TxId>>>>,
    lock_timeout: Duration,
    enable_deadlock_detection: bool,
    next_deadlock_check: Arc<Mutex<Instant>>,
    deadlock_check_interval: Duration,
    metrics: Arc<Mutex<LockManagerMetrics>>,
}

#[derive(Debug, Clone, Default)]
pub struct LockManagerMetrics {
    pub locks_acquired: u64,
    pub locks_released: u64,
    pub deadlocks_detected: u64,
    pub lock_timeouts: u64,
    pub lock_conflicts: u64,
}

impl LockManager {
    pub fn new(
        lock_timeout: Duration,
        enable_deadlock_detection: bool,
        deadlock_check_interval: Duration,
    ) -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
            tx_locks: Arc::new(DashMap::new()),
            wait_graph: Arc::new(Mutex::new(HashMap::new())),
            lock_timeout,
            enable_deadlock_detection,
            next_deadlock_check: Arc::new(Mutex::new(Instant::now() + deadlock_check_interval)),
            deadlock_check_interval,
            metrics: Arc::new(Mutex::new(LockManagerMetrics::default())),
        }
    }

    /// Acquire a lock
    pub fn acquire_lock(
        &self,
        tx_id: TxId,
        granularity: LockGranularity,
        mode: LockMode,
    ) -> Result<()> {
        let timeout = Some(Instant::now() + self.lock_timeout);
        
        // Get or create the lock
        let lock_ref = self.locks
            .entry(granularity.clone())
            .or_insert_with(|| Arc::new(Mutex::new(Lock::new(granularity.clone()))))
            .clone();

        let granted = {
            let mut lock = lock_ref.lock();
            
            // Check if we can grant immediately
            if lock.can_grant(mode, tx_id) {
                let request = LockRequest {
                    tx_id,
                    mode,
                    granted: true,
                    timestamp: Instant::now(),
                    timeout,
                };
                lock.requests.push_back(request);
                lock.granted_modes.insert(mode);
                true
            } else {
                // Add to wait queue
                let request = LockRequest {
                    tx_id,
                    mode,
                    granted: false,
                    timestamp: Instant::now(),
                    timeout,
                };
                lock.requests.push_back(request);

                // Update wait graph for deadlock detection
                if self.enable_deadlock_detection {
                    self.update_wait_graph(tx_id, &lock);
                }
                false
            }
        };

        if granted {
            // Track the lock for this transaction
            self.tx_locks
                .entry(tx_id)
                .or_insert_with(HashSet::new)
                .insert(granularity.clone());

            self.metrics.lock().locks_acquired += 1;
            debug!("Lock acquired: tx={}, granularity={:?}, mode={:?}", tx_id, granularity, mode);
            Ok(())
        } else {
            // Wait for lock or timeout
            self.wait_for_lock(tx_id, granularity, mode, timeout)
        }
    }

    /// Release all locks for a transaction
    pub fn release_locks(&self, tx_id: TxId) {
        let tx_locks = self.tx_locks.remove(&tx_id);
        if let Some((_, granularities)) = tx_locks {
            for granularity in granularities {
                if let Some(lock_ref) = self.locks.get(&granularity) {
                    let mut lock = lock_ref.lock();
                    let released = lock.release_locks(tx_id);
                    
                    if !released.is_empty() {
                        debug!("Released locks: tx={}, granularity={:?}, modes={:?}", 
                              tx_id, granularity, released);
                        self.metrics.lock().locks_released += released.len() as u64;
                    }

                    // Try to grant pending requests
                    let granted = lock.try_grant_pending();
                    for (granted_tx, granted_mode) in granted {
                        debug!("Granted pending lock: tx={}, mode={:?}", granted_tx, granted_mode);
                    }
                }
            }
        }

        // Remove from wait graph
        if self.enable_deadlock_detection {
            let mut wait_graph = self.wait_graph.lock();
            wait_graph.remove(&tx_id);
            // Remove this transaction from all wait lists
            for waiters in wait_graph.values_mut() {
                waiters.remove(&tx_id);
            }
        }
    }

    /// Check if a transaction holds a specific lock
    pub fn holds_lock(&self, tx_id: TxId, granularity: &LockGranularity, mode: LockMode) -> bool {
        if let Some(lock_ref) = self.locks.get(granularity) {
            let lock = lock_ref.lock();
            lock.requests.iter().any(|req| {
                req.tx_id == tx_id && req.granted && req.mode == mode
            })
        } else {
            false
        }
    }

    /// Get lock information for debugging
    pub fn get_lock_info(&self) -> HashMap<String, Vec<(TxId, String, bool)>> {
        let mut info = HashMap::new();
        
        for entry in self.locks.iter() {
            let granularity = entry.key();
            let lock = entry.value().lock();
            
            let key = format!("{:?}", granularity);
            let mut requests = Vec::new();
            
            for req in &lock.requests {
                requests.push((req.tx_id, format!("{:?}", req.mode), req.granted));
            }
            
            info.insert(key, requests);
        }
        
        info
    }

    /// Detect deadlocks using cycle detection in wait graph
    pub fn detect_deadlocks(&self) -> Vec<TxId> {
        if !self.enable_deadlock_detection {
            return Vec::new();
        }

        let now = Instant::now();
        let mut next_check = self.next_deadlock_check.lock();
        if now < *next_check {
            return Vec::new();
        }
        *next_check = now + self.deadlock_check_interval;
        drop(next_check);

        let wait_graph = self.wait_graph.lock();
        let deadlocked = self.find_cycles_in_wait_graph(&wait_graph);
        
        if !deadlocked.is_empty() {
            warn!("Detected deadlocks involving transactions: {:?}", deadlocked);
            self.metrics.lock().deadlocks_detected += deadlocked.len() as u64;
        }

        deadlocked
    }

    /// Get current metrics
    pub fn get_metrics(&self) -> LockManagerMetrics {
        self.metrics.lock().clone()
    }

    /// Private helper to wait for a lock
    fn wait_for_lock(
        &self,
        tx_id: TxId,
        granularity: LockGranularity,
        mode: LockMode,
        timeout: Option<Instant>,
    ) -> Result<()> {
        let start = Instant::now();
        
        loop {
            // Check timeout
            if let Some(timeout_instant) = timeout {
                if Instant::now() > timeout_instant {
                    self.metrics.lock().lock_timeouts += 1;
                    return Err(Error::LockTimeout(format!(
                        "Lock timeout for tx={}, granularity={:?}, mode={:?}",
                        tx_id, granularity, mode
                    )));
                }
            }

            // Check if lock is now available
            if let Some(lock_ref) = self.locks.get(&granularity) {
                let mut lock = lock_ref.lock();
                
                // Find our request and check if it can be granted
                let can_grant = match mode {
                    LockMode::Shared => !lock.granted_modes.contains(&LockMode::Exclusive),
                    LockMode::Exclusive => lock.granted_modes.is_empty(),
                    LockMode::Update => !lock.granted_modes.contains(&LockMode::Exclusive) && 
                                      !lock.granted_modes.contains(&LockMode::Update),
                    LockMode::IntentionShared => true, // Intent locks are generally compatible
                    LockMode::IntentionExclusive => !lock.granted_modes.contains(&LockMode::Exclusive),
                    LockMode::SharedIntentionExclusive => !lock.granted_modes.contains(&LockMode::Exclusive) &&
                                                          !lock.granted_modes.contains(&LockMode::SharedIntentionExclusive),
                };
                
                for req in &mut lock.requests {
                    if req.tx_id == tx_id && req.mode == mode && !req.granted {
                        if can_grant {
                            req.granted = true;
                            lock.granted_modes.insert(mode);
                            
                            // Track the lock for this transaction
                            self.tx_locks
                                .entry(tx_id)
                                .or_insert_with(HashSet::new)
                                .insert(granularity.clone());

                            self.metrics.lock().locks_acquired += 1;
                            debug!("Lock acquired after wait: tx={}, granularity={:?}, mode={:?}, wait_time={:?}", 
                                  tx_id, granularity, mode, start.elapsed());
                            return Ok(());
                        }
                        break;
                    }
                }
            }

            // Check for deadlocks
            if self.enable_deadlock_detection {
                let deadlocked = self.detect_deadlocks();
                if deadlocked.contains(&tx_id) {
                    return Err(Error::Deadlock(format!("Deadlock detected for tx={}", tx_id)));
                }
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Update wait graph for deadlock detection
    fn update_wait_graph(&self, waiting_tx: TxId, lock: &Lock) {
        let mut wait_graph = self.wait_graph.lock();
        
        let waiting_for: HashSet<TxId> = lock.requests
            .iter()
            .filter(|req| req.granted)
            .map(|req| req.tx_id)
            .collect();

        if !waiting_for.is_empty() {
            wait_graph.insert(waiting_tx, waiting_for);
        }
    }

    /// Find cycles in wait graph using DFS
    fn find_cycles_in_wait_graph(&self, wait_graph: &HashMap<TxId, HashSet<TxId>>) -> Vec<TxId> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut deadlocked = Vec::new();

        for &tx_id in wait_graph.keys() {
            if !visited.contains(&tx_id) {
                if self.dfs_cycle_detection(tx_id, wait_graph, &mut visited, &mut rec_stack) {
                    deadlocked.push(tx_id);
                }
            }
        }

        deadlocked
    }

    /// DFS helper for cycle detection
    fn dfs_cycle_detection(
        &self,
        tx_id: TxId,
        wait_graph: &HashMap<TxId, HashSet<TxId>>,
        visited: &mut HashSet<TxId>,
        rec_stack: &mut HashSet<TxId>,
    ) -> bool {
        visited.insert(tx_id);
        rec_stack.insert(tx_id);

        if let Some(waiting_for) = wait_graph.get(&tx_id) {
            for &dep_tx in waiting_for {
                if !visited.contains(&dep_tx) {
                    if self.dfs_cycle_detection(dep_tx, wait_graph, visited, rec_stack) {
                        return true;
                    }
                } else if rec_stack.contains(&dep_tx) {
                    return true; // Cycle detected
                }
            }
        }

        rec_stack.remove(&tx_id);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_compatibility() {
        use LockMode::*;
        
        // Test shared lock compatibility
        assert!(Shared.is_compatible_with(Shared));
        assert!(!Shared.is_compatible_with(Exclusive));
        assert!(Shared.is_compatible_with(IntentionShared));

        // Test exclusive lock compatibility
        assert!(!Exclusive.is_compatible_with(Shared));
        assert!(!Exclusive.is_compatible_with(Exclusive));
        assert!(!Exclusive.is_compatible_with(IntentionShared));
    }

    #[test]
    fn test_lock_manager() {
        let lm = LockManager::new(
            Duration::from_secs(1),
            true,
            Duration::from_millis(100),
        );

        let granularity = LockGranularity::Row(Bytes::from("key1"));

        // Acquire shared lock
        lm.acquire_lock(1, granularity.clone(), LockMode::Shared).unwrap();
        assert!(lm.holds_lock(1, &granularity, LockMode::Shared));

        // Acquire another shared lock - should succeed
        lm.acquire_lock(2, granularity.clone(), LockMode::Shared).unwrap();
        assert!(lm.holds_lock(2, &granularity, LockMode::Shared));

        // Release locks
        lm.release_locks(1);
        lm.release_locks(2);

        assert!(!lm.holds_lock(1, &granularity, LockMode::Shared));
        assert!(!lm.holds_lock(2, &granularity, LockMode::Shared));
    }
}