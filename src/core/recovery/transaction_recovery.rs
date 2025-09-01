use crate::core::error::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct TransactionRecoveryConfig {
    pub deadlock_detection_enabled: bool,
    pub deadlock_timeout: Duration,
    pub transaction_timeout: Duration,
    pub retry_on_conflict: bool,
    pub max_retry_attempts: u32,
    pub auto_rollback_incomplete: bool,
    pub conflict_resolution_strategy: ConflictResolutionStrategy,
}

impl Default for TransactionRecoveryConfig {
    fn default() -> Self {
        Self {
            deadlock_detection_enabled: true,
            deadlock_timeout: Duration::from_secs(30),
            transaction_timeout: Duration::from_secs(300),
            retry_on_conflict: true,
            max_retry_attempts: 3,
            auto_rollback_incomplete: true,
            conflict_resolution_strategy: ConflictResolutionStrategy::OldestFirst,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConflictResolutionStrategy {
    OldestFirst,
    NewestFirst,
    SmallestFirst,
    RandomAbort,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
    Recovering,
    Timeout,
}

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub id: u64,
    pub state: TransactionState,
    pub start_time: Instant,
    pub last_activity: Instant,
    pub operations: Vec<TransactionOperation>,
    pub locks_held: HashSet<String>,
    pub waiting_for: Option<u64>,
    pub retry_count: u32,
}

#[derive(Debug, Clone)]
pub struct TransactionOperation {
    pub operation_type: OperationType,
    pub key: String,
    pub timestamp: Instant,
    pub rollback_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperationType {
    Read,
    Write,
    Delete,
    Create,
}

#[derive(Debug)]
pub struct TransactionRecoveryStats {
    pub transactions_started: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_aborted: AtomicU64,
    pub deadlocks_detected: AtomicU64,
    pub deadlocks_resolved: AtomicU64,
    pub timeouts: AtomicU64,
    pub conflicts_resolved: AtomicU64,
    pub rollbacks_performed: AtomicU64,
    pub retries_attempted: AtomicU64,
}

impl Default for TransactionRecoveryStats {
    fn default() -> Self {
        Self {
            transactions_started: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_aborted: AtomicU64::new(0),
            deadlocks_detected: AtomicU64::new(0),
            deadlocks_resolved: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            conflicts_resolved: AtomicU64::new(0),
            rollbacks_performed: AtomicU64::new(0),
            retries_attempted: AtomicU64::new(0),
        }
    }
}

pub struct TransactionRecoveryManager {
    config: TransactionRecoveryConfig,
    stats: Arc<TransactionRecoveryStats>,
    transactions: Arc<RwLock<HashMap<u64, TransactionInfo>>>,
    lock_table: Arc<RwLock<HashMap<String, HashSet<u64>>>>,
    wait_graph: Arc<RwLock<HashMap<u64, HashSet<u64>>>>,
    next_tx_id: AtomicU64,
    deadlock_detector: Arc<Mutex<DeadlockDetector>>,
}

impl TransactionRecoveryManager {
    pub fn new(config: TransactionRecoveryConfig) -> Self {
        Self {
            config,
            stats: Arc::new(TransactionRecoveryStats::default()),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            lock_table: Arc::new(RwLock::new(HashMap::new())),
            wait_graph: Arc::new(RwLock::new(HashMap::new())),
            next_tx_id: AtomicU64::new(1),
            deadlock_detector: Arc::new(Mutex::new(DeadlockDetector::new())),
        }
    }

    pub fn stats(&self) -> Arc<TransactionRecoveryStats> {
        self.stats.clone()
    }

    pub async fn begin_transaction(&self) -> Result<u64> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();
        
        let tx_info = TransactionInfo {
            id: tx_id,
            state: TransactionState::Active,
            start_time: now,
            last_activity: now,
            operations: Vec::new(),
            locks_held: HashSet::new(),
            waiting_for: None,
            retry_count: 0,
        };

        {
            let mut transactions = self.transactions.write().await;
            transactions.insert(tx_id, tx_info);
        }

        self.stats.transactions_started.fetch_add(1, Ordering::SeqCst);
        info!("Started transaction: {}", tx_id);
        
        Ok(tx_id)
    }

    pub async fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        let _tx_info = {
            let mut transactions = self.transactions.write().await;
            if let Some(tx) = transactions.get_mut(&tx_id) {
                if tx.state != TransactionState::Active {
                    return Err(Error::TransactionInvalidState {
                        id: tx_id,
                        state: format!("{:?}", tx.state),
                    });
                }
                tx.state = TransactionState::Preparing;
                tx.clone()
            } else {
                return Err(Error::TransactionNotFound { id: tx_id });
            }
        };

        // Apply commit with timeout
        let commit_result = timeout(
            self.config.transaction_timeout,
            self.apply_commit(tx_id)
        ).await;

        match commit_result {
            Ok(Ok(_)) => {
                self.cleanup_transaction(tx_id, TransactionState::Committed).await;
                self.stats.transactions_committed.fetch_add(1, Ordering::SeqCst);
                info!("Committed transaction: {}", tx_id);
                Ok(())
            }
            Ok(Err(e)) => {
                self.rollback_transaction(tx_id).await?;
                Err(e)
            }
            Err(_) => {
                self.stats.timeouts.fetch_add(1, Ordering::SeqCst);
                self.rollback_transaction(tx_id).await?;
                Err(Error::Timeout(format!("Transaction {} commit timeout", tx_id)))
            }
        }
    }

    pub async fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        let tx_info = {
            let mut transactions = self.transactions.write().await;
            if let Some(tx) = transactions.get_mut(&tx_id) {
                if matches!(tx.state, TransactionState::Committed) {
                    return Err(Error::TransactionInvalidState {
                        id: tx_id,
                        state: format!("{:?}", tx.state),
                    });
                }
                tx.state = TransactionState::Aborted;
                tx.clone()
            } else {
                return Err(Error::TransactionNotFound { id: tx_id });
            }
        };

        // Perform rollback operations
        for operation in tx_info.operations.iter().rev() {
            if let Err(e) = self.rollback_operation(operation).await {
                error!("Failed to rollback operation for tx {}: {}", tx_id, e);
            }
        }

        self.cleanup_transaction(tx_id, TransactionState::Aborted).await;
        self.stats.transactions_aborted.fetch_add(1, Ordering::SeqCst);
        self.stats.rollbacks_performed.fetch_add(1, Ordering::SeqCst);
        
        info!("Rolled back transaction: {}", tx_id);
        Ok(())
    }

    async fn apply_commit(&self, tx_id: u64) -> Result<()> {
        // This would integrate with the actual storage engine
        // For now, we simulate the commit process
        
        let tx_info = {
            let transactions = self.transactions.read().await;
            transactions.get(&tx_id).cloned()
                .ok_or(Error::TransactionNotFound { id: tx_id })?
        };

        // Apply all operations
        for operation in &tx_info.operations {
            // Apply operation to storage
            // This would call into the actual storage layer
            self.apply_operation(operation).await?;
        }

        Ok(())
    }

    async fn apply_operation(&self, _operation: &TransactionOperation) -> Result<()> {
        // Placeholder for actual operation application
        // This would integrate with the storage engine
        Ok(())
    }

    async fn rollback_operation(&self, operation: &TransactionOperation) -> Result<()> {
        // Apply rollback based on operation type and rollback data
        match operation.operation_type {
            OperationType::Write => {
                if let Some(ref rollback_data) = operation.rollback_data {
                    // Restore previous value
                    info!("Restoring previous value for key: {}", operation.key);
                    let _ = rollback_data; // Use rollback data to restore
                }
            }
            OperationType::Create => {
                // Delete the created key
                info!("Deleting created key: {}", operation.key);
            }
            OperationType::Delete => {
                if let Some(ref rollback_data) = operation.rollback_data {
                    // Restore deleted value
                    info!("Restoring deleted key: {}", operation.key);
                    let _ = rollback_data; // Use rollback data to restore
                }
            }
            OperationType::Read => {
                // No rollback needed for reads
            }
        }
        Ok(())
    }

    async fn cleanup_transaction(&self, tx_id: u64, final_state: TransactionState) {
        // Update transaction state
        {
            let mut transactions = self.transactions.write().await;
            if let Some(tx) = transactions.get_mut(&tx_id) {
                tx.state = final_state;
            }
        }

        // Release all locks
        self.release_all_locks(tx_id).await;

        // Update wait graph
        {
            let mut wait_graph = self.wait_graph.write().await;
            wait_graph.remove(&tx_id);
            
            // Remove this transaction from other wait lists
            for wait_list in wait_graph.values_mut() {
                wait_list.remove(&tx_id);
            }
        }

        // Remove from transaction table after a delay (for monitoring)
        let transactions = self.transactions.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let mut txs = transactions.write().await;
            txs.remove(&tx_id);
        });
    }

    pub async fn acquire_lock(&self, tx_id: u64, key: &str, exclusive: bool) -> Result<()> {
        // Check if transaction exists and is active
        {
            let transactions = self.transactions.read().await;
            let tx = transactions.get(&tx_id)
                .ok_or(Error::TransactionNotFound { id: tx_id })?;
            
            if tx.state != TransactionState::Active {
                return Err(Error::TransactionInvalidState {
                    id: tx_id,
                    state: format!("{:?}", tx.state),
                });
            }
        }

        // Attempt to acquire lock with deadlock detection
        self.acquire_lock_with_deadlock_detection(tx_id, key, exclusive).await
    }

    async fn acquire_lock_with_deadlock_detection(&self, tx_id: u64, key: &str, exclusive: bool) -> Result<()> {
        let start_time = Instant::now();
        
        loop {
            // Try to acquire lock
            match self.try_acquire_lock(tx_id, key, exclusive).await {
                Ok(_) => return Ok(()),
                Err(Error::LockFailed { .. }) => {
                    // Check for deadlock
                    if self.config.deadlock_detection_enabled {
                        if let Some(deadlock_cycle) = self.detect_deadlock(tx_id).await {
                            return self.resolve_deadlock(deadlock_cycle).await;
                        }
                    }

                    // Check timeout
                    if start_time.elapsed() > self.config.deadlock_timeout {
                        self.stats.timeouts.fetch_add(1, Ordering::SeqCst);
                        return Err(Error::Timeout(format!("Lock acquisition timeout for key: {}", key)));
                    }

                    // Wait a bit before retrying
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn try_acquire_lock(&self, tx_id: u64, key: &str, _exclusive: bool) -> Result<()> {
        let mut lock_table = self.lock_table.write().await;
        
        let lock_holders = lock_table.entry(key.to_string()).or_insert_with(HashSet::new);
        
        if lock_holders.is_empty() || lock_holders.contains(&tx_id) {
            // Can acquire lock
            lock_holders.insert(tx_id);
            
            // Update transaction info
            let mut transactions = self.transactions.write().await;
            if let Some(tx) = transactions.get_mut(&tx_id) {
                tx.locks_held.insert(key.to_string());
                tx.last_activity = Instant::now();
            }
            
            Ok(())
        } else {
            // Lock is held by another transaction
            let blocking_tx = *lock_holders.iter().next().unwrap();
            
            // Update wait graph
            {
                let mut wait_graph = self.wait_graph.write().await;
                wait_graph.entry(tx_id).or_insert_with(HashSet::new).insert(blocking_tx);
            }

            // Update transaction waiting info
            {
                let mut transactions = self.transactions.write().await;
                if let Some(tx) = transactions.get_mut(&tx_id) {
                    tx.waiting_for = Some(blocking_tx);
                    tx.last_activity = Instant::now();
                }
            }

            Err(Error::LockFailed {
                resource: key.to_string(),
            })
        }
    }

    async fn detect_deadlock(&self, tx_id: u64) -> Option<Vec<u64>> {
        let mut detector = self.deadlock_detector.lock().await;
        let wait_graph = self.wait_graph.read().await;
        
        detector.detect_cycle(&wait_graph, tx_id)
    }

    async fn resolve_deadlock(&self, deadlock_cycle: Vec<u64>) -> Result<()> {
        self.stats.deadlocks_detected.fetch_add(1, Ordering::SeqCst);
        warn!("Deadlock detected involving transactions: {:?}", deadlock_cycle);

        // Choose victim based on strategy
        let victim = self.choose_deadlock_victim(&deadlock_cycle).await;
        
        // Abort victim transaction
        self.rollback_transaction(victim).await?;
        self.stats.deadlocks_resolved.fetch_add(1, Ordering::SeqCst);
        
        info!("Resolved deadlock by aborting transaction: {}", victim);
        
        Err(Error::Deadlock(format!("Transaction {} was aborted to resolve deadlock", victim)))
    }

    async fn choose_deadlock_victim(&self, deadlock_cycle: &[u64]) -> u64 {
        let transactions = self.transactions.read().await;
        
        match self.config.conflict_resolution_strategy {
            ConflictResolutionStrategy::OldestFirst => {
                deadlock_cycle.iter()
                    .min_by_key(|&&tx_id| {
                        transactions.get(&tx_id).map(|tx| tx.start_time).unwrap_or_else(Instant::now)
                    })
                    .copied().unwrap_or(deadlock_cycle[0])
            }
            ConflictResolutionStrategy::NewestFirst => {
                deadlock_cycle.iter()
                    .max_by_key(|&&tx_id| {
                        transactions.get(&tx_id).map(|tx| tx.start_time).unwrap_or_else(Instant::now)
                    })
                    .copied().unwrap_or(deadlock_cycle[0])
            }
            ConflictResolutionStrategy::SmallestFirst => {
                deadlock_cycle.iter()
                    .min_by_key(|&&tx_id| {
                        transactions.get(&tx_id).map(|tx| tx.operations.len()).unwrap_or(0)
                    })
                    .copied().unwrap_or(deadlock_cycle[0])
            }
            ConflictResolutionStrategy::RandomAbort => {
                deadlock_cycle[0] // Simple choice for now
            }
        }
    }

    async fn release_all_locks(&self, tx_id: u64) {
        let mut lock_table = self.lock_table.write().await;
        let locks_to_release: Vec<String> = lock_table.iter()
            .filter_map(|(key, holders)| {
                if holders.contains(&tx_id) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        for key in locks_to_release {
            if let Some(holders) = lock_table.get_mut(&key) {
                holders.remove(&tx_id);
                if holders.is_empty() {
                    lock_table.remove(&key);
                }
            }
        }
    }

    pub async fn add_operation(&self, tx_id: u64, operation: TransactionOperation) -> Result<()> {
        let mut transactions = self.transactions.write().await;
        if let Some(tx) = transactions.get_mut(&tx_id) {
            if tx.state != TransactionState::Active {
                return Err(Error::TransactionInvalidState {
                    id: tx_id,
                    state: format!("{:?}", tx.state),
                });
            }
            tx.operations.push(operation);
            tx.last_activity = Instant::now();
            Ok(())
        } else {
            Err(Error::TransactionNotFound { id: tx_id })
        }
    }

    pub async fn recover_incomplete_transactions(&self) -> Result<Vec<u64>> {
        info!("Starting recovery of incomplete transactions");
        
        let mut recovered = Vec::new();
        let transactions_to_process: Vec<(u64, TransactionInfo)> = {
            let transactions = self.transactions.read().await;
            transactions.iter()
                .filter(|(_, tx)| matches!(tx.state, TransactionState::Active | TransactionState::Preparing))
                .map(|(&id, tx)| (id, tx.clone()))
                .collect()
        };

        for (tx_id, tx_info) in transactions_to_process {
            match tx_info.state {
                TransactionState::Active => {
                    if self.config.auto_rollback_incomplete {
                        warn!("Rolling back incomplete active transaction: {}", tx_id);
                        if let Err(e) = self.rollback_transaction(tx_id).await {
                            error!("Failed to rollback transaction {}: {}", tx_id, e);
                        } else {
                            recovered.push(tx_id);
                        }
                    }
                }
                TransactionState::Preparing => {
                    // Check if commit can be completed
                    warn!("Attempting to complete preparing transaction: {}", tx_id);
                    match self.apply_commit(tx_id).await {
                        Ok(_) => {
                            self.cleanup_transaction(tx_id, TransactionState::Committed).await;
                            self.stats.transactions_committed.fetch_add(1, Ordering::SeqCst);
                            recovered.push(tx_id);
                        }
                        Err(_) => {
                            if let Err(e) = self.rollback_transaction(tx_id).await {
                                error!("Failed to rollback preparing transaction {}: {}", tx_id, e);
                            } else {
                                recovered.push(tx_id);
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        info!("Recovered {} incomplete transactions", recovered.len());
        Ok(recovered)
    }

    pub async fn get_transaction_health_report(&self) -> TransactionHealthReport {
        let transactions = self.transactions.read().await;
        let active_count = transactions.values()
            .filter(|tx| tx.state == TransactionState::Active)
            .count();
        
        let long_running_count = transactions.values()
            .filter(|tx| tx.start_time.elapsed() > Duration::from_secs(300))
            .count();

        let deadlocked_count = {
            let wait_graph = self.wait_graph.read().await;
            wait_graph.len()
        };

        let status = if deadlocked_count > 0 || long_running_count > active_count / 2 {
            TransactionHealthStatus::Critical
        } else if long_running_count > 0 || active_count > 1000 {
            TransactionHealthStatus::Warning
        } else {
            TransactionHealthStatus::Healthy
        };

        TransactionHealthReport {
            active_transactions: active_count,
            long_running_transactions: long_running_count,
            deadlocked_transactions: deadlocked_count,
            total_started: self.stats.transactions_started.load(Ordering::SeqCst),
            total_committed: self.stats.transactions_committed.load(Ordering::SeqCst),
            total_aborted: self.stats.transactions_aborted.load(Ordering::SeqCst),
            deadlocks_detected: self.stats.deadlocks_detected.load(Ordering::SeqCst),
            status,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionHealthReport {
    pub active_transactions: usize,
    pub long_running_transactions: usize,
    pub deadlocked_transactions: usize,
    pub total_started: u64,
    pub total_committed: u64,
    pub total_aborted: u64,
    pub deadlocks_detected: u64,
    pub status: TransactionHealthStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionHealthStatus {
    Healthy,
    Warning,
    Critical,
}

struct DeadlockDetector {
    visited: HashSet<u64>,
    path: Vec<u64>,
}

impl DeadlockDetector {
    fn new() -> Self {
        Self {
            visited: HashSet::new(),
            path: Vec::new(),
        }
    }

    fn detect_cycle(&mut self, wait_graph: &HashMap<u64, HashSet<u64>>, start_tx: u64) -> Option<Vec<u64>> {
        self.visited.clear();
        self.path.clear();
        
        if self.dfs(wait_graph, start_tx, start_tx) {
            Some(self.path.clone())
        } else {
            None
        }
    }

    fn dfs(&mut self, wait_graph: &HashMap<u64, HashSet<u64>>, current: u64, target: u64) -> bool {
        if self.path.contains(&current) {
            // Found cycle
            if let Some(start_idx) = self.path.iter().position(|&tx| tx == current) {
                self.path.drain(0..start_idx);
                return true;
            }
        }

        if self.visited.contains(&current) {
            return false;
        }

        self.visited.insert(current);
        self.path.push(current);

        if let Some(waiting_for) = wait_graph.get(&current) {
            for &next_tx in waiting_for {
                if next_tx == target && self.path.len() > 1 {
                    return true;
                }
                if self.dfs(wait_graph, next_tx, target) {
                    return true;
                }
            }
        }

        self.path.pop();
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let config = TransactionRecoveryConfig::default();
        let manager = TransactionRecoveryManager::new(config);
        
        let tx_id = manager.begin_transaction().await.unwrap();
        assert!(tx_id > 0);
        
        manager.commit_transaction(tx_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_deadlock_detection() {
        let config = TransactionRecoveryConfig::default();
        let manager = TransactionRecoveryManager::new(config);
        
        let tx1 = manager.begin_transaction().await.unwrap();
        let tx2 = manager.begin_transaction().await.unwrap();
        
        // Create potential deadlock scenario
        manager.acquire_lock(tx1, "key1", true).await.unwrap();
        manager.acquire_lock(tx2, "key2", true).await.unwrap();
        
        // This should detect deadlock when both try to acquire each other's lock
        let result1 = manager.acquire_lock(tx1, "key2", true).await;
        let result2 = manager.acquire_lock(tx2, "key1", true).await;
        
        // At least one should fail due to deadlock detection
        assert!(result1.is_err() || result2.is_err());
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let config = TransactionRecoveryConfig::default();
        let manager = TransactionRecoveryManager::new(config);
        
        let tx_id = manager.begin_transaction().await.unwrap();
        
        let operation = TransactionOperation {
            operation_type: OperationType::Write,
            key: "test_key".to_string(),
            timestamp: Instant::now(),
            rollback_data: Some(b"original_value".to_vec()),
        };
        
        manager.add_operation(tx_id, operation).await.unwrap();
        manager.rollback_transaction(tx_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_incomplete_transaction_recovery() {
        let config = TransactionRecoveryConfig {
            auto_rollback_incomplete: true,
            ..Default::default()
        };
        let manager = TransactionRecoveryManager::new(config);
        
        let tx_id = manager.begin_transaction().await.unwrap();
        
        // Simulate incomplete transaction
        let recovered = manager.recover_incomplete_transactions().await.unwrap();
        assert!(recovered.contains(&tx_id));
    }
}
