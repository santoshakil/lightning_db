use super::*;
use futures::future;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{Duration, Instant};
use uuid::Uuid;

/// Two-Phase Commit coordinator for cross-shard transactions
pub struct TwoPhaseCommitCoordinator {
    /// Active transactions
    transactions: Arc<RwLock<HashMap<String, CrossShardTransaction>>>,

    /// Transaction ID generator
    tx_counter: AtomicU64,

    /// Node connections for communication
    nodes: Arc<RwLock<HashMap<NodeId, Arc<RaftNode>>>>,

    /// Configuration
    config: CoordinatorConfig,

    /// Background task handles
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,

    /// Metrics
    metrics: Arc<CoordinatorMetrics>,
}

/// Configuration for transaction coordinator
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Default transaction timeout
    pub default_timeout: Duration,

    /// Maximum concurrent transactions
    pub max_concurrent_txs: u32,

    /// Retry attempts for failed operations
    pub retry_attempts: u32,

    /// Retry delay
    pub retry_delay: Duration,

    /// Enable distributed deadlock detection
    pub enable_deadlock_detection: bool,

    /// Deadlock detection interval
    pub deadlock_check_interval: Duration,

    /// Enable transaction logging
    pub enable_tx_log: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            max_concurrent_txs: 1000,
            retry_attempts: 3,
            retry_delay: Duration::from_millis(100),
            enable_deadlock_detection: true,
            deadlock_check_interval: Duration::from_secs(5),
            enable_tx_log: true,
        }
    }
}

/// Transaction coordinator metrics
#[derive(Debug, Default)]
pub struct CoordinatorMetrics {
    /// Total transactions started
    pub transactions_started: AtomicU64,

    /// Successful commits
    pub successful_commits: AtomicU64,

    /// Failed transactions (aborted)
    pub failed_transactions: AtomicU64,

    /// Timeout transactions
    pub timeout_transactions: AtomicU64,

    /// Average transaction duration
    pub avg_tx_duration: parking_lot::RwLock<f64>,

    /// Deadlocks detected
    pub deadlocks_detected: AtomicU64,

    /// Active transaction count
    pub active_transactions: AtomicU64,
}

impl TwoPhaseCommitCoordinator {
    /// Create new coordinator
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            tx_counter: AtomicU64::new(0),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            config: CoordinatorConfig::default(),
            tasks: Mutex::new(Vec::new()),
            metrics: Arc::new(CoordinatorMetrics::default()),
        }
    }

    /// Add node for coordination
    pub async fn add_node(&self, node_id: NodeId, node: Arc<RaftNode>) {
        self.nodes.write().insert(node_id, node);
    }

    /// Remove node
    pub async fn remove_node(&self, node_id: NodeId) {
        self.nodes.write().remove(&node_id);
    }

    /// Start background tasks
    pub async fn start(&self) -> Result<()> {
        // Transaction timeout task
        let transactions = self.transactions.clone();
        let metrics = self.metrics.clone();
        let timeout_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                let now = Instant::now();
                let mut expired_txs = Vec::new();

                // Find expired transactions
                {
                    let txs = transactions.read();
                    for (tx_id, tx) in txs.iter() {
                        if now > tx.timeout {
                            expired_txs.push(tx_id.clone());
                        }
                    }
                }

                // Abort expired transactions
                for tx_id in expired_txs {
                    if let Some(mut tx) = transactions.write().remove(&tx_id) {
                        tx.state = TransactionState::Aborted;
                        metrics.timeout_transactions.fetch_add(1, Ordering::Relaxed);
                        println!("Transaction {} timed out and was aborted", tx_id);

                        // Send timeout response
                        if let Some(response_tx) = tx.response_tx.take() {
                            let _ = response_tx
                                .send(Err(Error::Timeout("Transaction timeout".to_string())));
                        }
                    }
                }
            }
        });

        // Deadlock detection task
        if self.config.enable_deadlock_detection {
            let transactions = self.transactions.clone();
            let metrics = self.metrics.clone();
            let interval = self.config.deadlock_check_interval;

            let deadlock_task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(interval);
                loop {
                    interval.tick().await;

                    // Simple deadlock detection based on transaction wait times
                    let mut long_running_txs = Vec::new();
                    let now = Instant::now();

                    {
                        let txs = transactions.read();
                        for (tx_id, tx) in txs.iter() {
                            if matches!(tx.state, TransactionState::Preparing)
                                && now.duration_since(tx.timeout - Duration::from_secs(30))
                                    > Duration::from_secs(10)
                            {
                                long_running_txs.push(tx_id.clone());
                            }
                        }
                    }

                    if !long_running_txs.is_empty() {
                        println!(
                            "Potential deadlock detected: {} long-running transactions",
                            long_running_txs.len()
                        );
                        metrics.deadlocks_detected.fetch_add(1, Ordering::Relaxed);

                        // For simplicity, abort the oldest transaction
                        if let Some(tx_id) = long_running_txs.first() {
                            if let Some(mut tx) = transactions.write().remove(tx_id) {
                                tx.state = TransactionState::Aborted;
                                println!(
                                    "Aborted transaction {} to resolve potential deadlock",
                                    tx_id
                                );

                                if let Some(response_tx) = tx.response_tx.take() {
                                    let _ = response_tx.send(Err(Error::Deadlock(
                                        "Transaction aborted due to deadlock".to_string(),
                                    )));
                                }
                            }
                        }
                    }
                }
            });

            self.tasks.lock().await.push(deadlock_task);
        }

        self.tasks.lock().await.push(timeout_task);
        Ok(())
    }

    /// Prepare phase - ask all participants to prepare
    async fn prepare_transaction(&self, tx_id: &str) -> Result<bool> {
        let tx = {
            let txs = self.transactions.read();
            txs.get(tx_id)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("Transaction {} not found", tx_id)))?
        };

        println!(
            "Preparing transaction {} across {} shards",
            tx_id,
            tx.shards.len()
        );

        let mut prepare_results = Vec::new();

        // Send prepare requests to all shards
        for shard_id in &tx.shards {
            let operations = tx.operations.get(shard_id).cloned().unwrap_or_default();
            let result = self
                .send_prepare_request(*shard_id, tx_id, operations)
                .await;
            prepare_results.push((*shard_id, result));
        }

        // Check if all participants voted to commit
        let mut all_prepared = true;
        for (shard_id, result) in prepare_results {
            match result {
                Ok(prepared) => {
                    if !prepared {
                        println!("Shard {} voted to abort transaction {}", shard_id, tx_id);
                        all_prepared = false;
                        break;
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Failed to prepare shard {} for transaction {}: {}",
                        shard_id, tx_id, e
                    );
                    all_prepared = false;
                    break;
                }
            }
        }

        // Update transaction state
        {
            let mut txs = self.transactions.write();
            if let Some(tx) = txs.get_mut(tx_id) {
                if all_prepared {
                    tx.state = TransactionState::Prepared;
                    println!("Transaction {} prepared successfully", tx_id);
                } else {
                    tx.state = TransactionState::Aborting;
                    println!("Transaction {} failed to prepare", tx_id);
                }
            }
        }

        Ok(all_prepared)
    }

    /// Send prepare request to a shard
    async fn send_prepare_request(
        &self,
        shard_id: ShardId,
        tx_id: &str,
        operations: Vec<TransactionOp>,
    ) -> Result<bool> {
        // Get the leader node for this shard
        let _leader_node = self.find_shard_leader(shard_id).await?;

        // For now, simulate prepare request
        // In a real implementation, this would be an RPC call
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Simulate validation - check if operations can be performed
        for op in &operations {
            match op {
                TransactionOp::Read { key: _key } => {
                    // Read operations don't need preparation
                }
                TransactionOp::Write { key, value: _ } => {
                    // Check if key can be locked
                    if key.len() > 1024 * 1024 {
                        // Reject very large keys
                        return Ok(false);
                    }
                }
                TransactionOp::Delete { key } => {
                    // Check if key exists and can be deleted
                    if key.is_empty() {
                        return Ok(false);
                    }
                }
                TransactionOp::ConditionalWrite { key, .. } => {
                    // Check if conditional write is valid
                    if key.len() > 1024 * 1024 {
                        return Ok(false);
                    }
                }
            }
        }

        println!(
            "Shard {} prepared successfully for transaction {}",
            shard_id, tx_id
        );
        Ok(true)
    }

    /// Commit phase - tell all participants to commit
    async fn commit_transaction_phase(&self, tx_id: &str) -> Result<()> {
        let tx = {
            let txs = self.transactions.read();
            txs.get(tx_id)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("Transaction {} not found", tx_id)))?
        };

        println!(
            "Committing transaction {} across {} shards",
            tx_id,
            tx.shards.len()
        );

        // Send commit requests to all shards
        let mut commit_tasks = Vec::new();

        for shard_id in &tx.shards {
            let operations = tx.operations.get(shard_id).cloned().unwrap_or_default();
            let task = self.send_commit_request(*shard_id, tx_id, operations);
            commit_tasks.push(task);
        }

        // Wait for all commits to complete
        let results = future::join_all(commit_tasks).await;

        let mut commit_success = true;
        for (i, result) in results.iter().enumerate() {
            let shard_id = tx.shards[i];
            match result {
                Ok(()) => {
                    println!(
                        "Shard {} committed transaction {} successfully",
                        shard_id, tx_id
                    );
                }
                Err(e) => {
                    eprintln!(
                        "Failed to commit transaction {} on shard {}: {}",
                        tx_id, shard_id, e
                    );
                    commit_success = false;
                }
            }
        }

        // Update transaction state
        {
            let mut txs = self.transactions.write();
            if let Some(tx) = txs.get_mut(tx_id) {
                if commit_success {
                    tx.state = TransactionState::Committed;
                    println!("Transaction {} committed successfully", tx_id);
                } else {
                    tx.state = TransactionState::Failed;
                    println!("Transaction {} failed during commit", tx_id);
                }
            }
        }

        if !commit_success {
            return Err(Error::TransactionFailed("Commit phase failed".to_string()));
        }

        Ok(())
    }

    /// Send commit request to a shard
    async fn send_commit_request(
        &self,
        shard_id: ShardId,
        tx_id: &str,
        operations: Vec<TransactionOp>,
    ) -> Result<()> {
        let leader_node = self.find_shard_leader(shard_id).await?;

        // Execute operations on the shard
        for op in operations {
            let command = match op {
                TransactionOp::Write { key, value } => crate::raft::Command::Write { key, value },
                TransactionOp::Delete { key } => crate::raft::Command::Delete { key },
                TransactionOp::ConditionalWrite { key, value, .. } => {
                    // TODO: Implement proper conditional write logic
                    crate::raft::Command::Write { key, value }
                }
                TransactionOp::Read { .. } => {
                    // Reads don't need to be committed
                    continue;
                }
            };

            // Execute command on leader
            let result = leader_node.execute_command_direct(command).await;
            if let Err(e) = result {
                return Err(Error::TransactionFailed(format!(
                    "Failed to execute operation: {}",
                    e
                )));
            }
        }

        Ok(())
    }

    /// Abort phase - tell all participants to abort
    async fn abort_transaction_phase(&self, tx_id: &str) -> Result<()> {
        let tx = {
            let txs = self.transactions.read();
            txs.get(tx_id)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("Transaction {} not found", tx_id)))?
        };

        println!(
            "Aborting transaction {} across {} shards",
            tx_id,
            tx.shards.len()
        );

        // Send abort requests to all shards
        for shard_id in &tx.shards {
            if let Err(e) = self.send_abort_request(*shard_id, tx_id).await {
                eprintln!(
                    "Failed to abort transaction {} on shard {}: {}",
                    tx_id, shard_id, e
                );
            }
        }

        // Update transaction state
        {
            let mut txs = self.transactions.write();
            if let Some(tx) = txs.get_mut(tx_id) {
                tx.state = TransactionState::Aborted;
            }
        }

        Ok(())
    }

    /// Send abort request to a shard
    async fn send_abort_request(&self, shard_id: ShardId, tx_id: &str) -> Result<()> {
        // In a real implementation, this would clean up any locks or prepared state
        println!("Shard {} aborted transaction {}", shard_id, tx_id);
        Ok(())
    }

    /// Find the leader node for a shard
    async fn find_shard_leader(&self, shard_id: ShardId) -> Result<Arc<RaftNode>> {
        let nodes = self.nodes.read();

        // For simplicity, use the first available node
        // In a real implementation, you'd query the shard metadata to find the current leader
        if let Some((_, node)) = nodes.iter().next() {
            Ok(node.clone())
        } else {
            Err(Error::NotFound("No nodes available".to_string()))
        }
    }

    /// Generate unique transaction ID
    fn generate_tx_id(&self) -> String {
        let counter = self.tx_counter.fetch_add(1, Ordering::SeqCst);
        format!(
            "tx_{}_{}",
            counter,
            Uuid::new_v4().to_string()[..8].to_string()
        )
    }

    /// Validate transaction operations
    fn validate_operations(&self, operations: &HashMap<ShardId, Vec<TransactionOp>>) -> Result<()> {
        if operations.is_empty() {
            return Err(Error::InvalidInput(
                "Transaction has no operations".to_string(),
            ));
        }

        let total_ops: usize = operations.values().map(|ops| ops.len()).sum();
        if total_ops > 1000 {
            return Err(Error::InvalidInput(
                "Too many operations in transaction".to_string(),
            ));
        }

        // Check for conflicting operations
        for (shard_id, ops) in operations {
            let mut keys = std::collections::HashSet::new();
            for op in ops {
                let key = match op {
                    TransactionOp::Read { key } => key,
                    TransactionOp::Write { key, .. } => key,
                    TransactionOp::Delete { key } => key,
                    TransactionOp::ConditionalWrite { key, .. } => key,
                };

                if !keys.insert(key.clone()) {
                    return Err(Error::InvalidInput(format!(
                        "Duplicate key in shard {}",
                        shard_id
                    )));
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl TransactionCoordinator for TwoPhaseCommitCoordinator {
    async fn begin_transaction(
        &self,
        operations: HashMap<ShardId, Vec<TransactionOp>>,
    ) -> Result<String> {
        // Validate operations
        self.validate_operations(&operations)?;

        // Check concurrent transaction limit
        let active_count = self.metrics.active_transactions.load(Ordering::Acquire);
        if active_count >= self.config.max_concurrent_txs as u64 {
            return Err(Error::ResourceExhausted {
                resource: "Too many concurrent transactions".to_string(),
            });
        }

        let tx_id = self.generate_tx_id();
        let shards: Vec<ShardId> = operations.keys().cloned().collect();
        let timeout = Instant::now() + self.config.default_timeout;

        // Create response channel
        let (response_tx, _response_rx) = mpsc::unbounded_channel();

        let tx = CrossShardTransaction {
            tx_id: tx_id.clone(),
            shards,
            state: TransactionState::Preparing,
            operations,
            timeout,
            response_tx: Some(response_tx),
        };

        // Store number of shards before moving tx
        let num_shards = tx.shards.len();

        // Store transaction
        self.transactions.write().insert(tx_id.clone(), tx);

        // Update metrics
        self.metrics
            .transactions_started
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .active_transactions
            .fetch_add(1, Ordering::Relaxed);

        println!("Started transaction {} with {} shards", tx_id, num_shards);

        Ok(tx_id)
    }

    async fn commit_transaction(&self, tx_id: &str) -> Result<()> {
        let start_time = Instant::now();

        // Phase 1: Prepare
        let prepared = self.prepare_transaction(tx_id).await?;

        if !prepared {
            // Some participant voted to abort
            self.abort_transaction(tx_id).await?;
            self.metrics
                .failed_transactions
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::TransactionFailed(
                "Transaction was aborted during prepare phase".to_string(),
            ));
        }

        // Phase 2: Commit
        match self.commit_transaction_phase(tx_id).await {
            Ok(()) => {
                // Clean up transaction
                self.transactions.write().remove(tx_id);

                // Update metrics
                self.metrics
                    .successful_commits
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .active_transactions
                    .fetch_sub(1, Ordering::Relaxed);

                let duration = start_time.elapsed().as_millis() as f64;
                let mut avg_duration = self.metrics.avg_tx_duration.write();
                let current_count = self.metrics.successful_commits.load(Ordering::Relaxed) as f64;
                *avg_duration = (*avg_duration * (current_count - 1.0) + duration) / current_count;

                println!(
                    "Transaction {} committed successfully in {:.2}ms",
                    tx_id, duration
                );
                Ok(())
            }
            Err(e) => {
                // Commit failed, try to abort
                let _ = self.abort_transaction(tx_id).await;
                self.metrics
                    .failed_transactions
                    .fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    async fn abort_transaction(&self, tx_id: &str) -> Result<()> {
        self.abort_transaction_phase(tx_id).await?;

        // Clean up transaction
        self.transactions.write().remove(tx_id);
        self.metrics
            .active_transactions
            .fetch_sub(1, Ordering::Relaxed);

        println!("Transaction {} aborted", tx_id);
        Ok(())
    }

    fn get_transaction_status(&self, tx_id: &str) -> Option<TransactionState> {
        self.transactions.read().get(tx_id).map(|tx| tx.state)
    }
}

/// Simple transaction coordinator for single-shard transactions
pub struct SimpleCoordinator {
    /// Node connections
    nodes: Arc<RwLock<HashMap<NodeId, Arc<RaftNode>>>>,

    /// Metrics
    metrics: Arc<CoordinatorMetrics>,
}

impl SimpleCoordinator {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(CoordinatorMetrics::default()),
        }
    }

    pub async fn add_node(&self, node_id: NodeId, node: Arc<RaftNode>) {
        self.nodes.write().insert(node_id, node);
    }
}

#[async_trait::async_trait]
impl TransactionCoordinator for SimpleCoordinator {
    async fn begin_transaction(
        &self,
        operations: HashMap<ShardId, Vec<TransactionOp>>,
    ) -> Result<String> {
        if operations.len() != 1 {
            return Err(Error::InvalidInput(
                "SimpleCoordinator only supports single-shard transactions".to_string(),
            ));
        }

        let tx_id = format!("simple_tx_{}", rand::random::<u64>());

        // Execute operations immediately since it's single-shard
        let (shard_id, ops) = operations.into_iter().next().unwrap();

        // Find leader node for shard
        let leader_node = {
            let nodes = self.nodes.read();
            nodes
                .values()
                .next()
                .ok_or_else(|| Error::NotFound("No nodes available".to_string()))?
                .clone()
        };

        // Execute operations
        for op in ops {
            let command = match op {
                TransactionOp::Write { key, value } => crate::raft::Command::Write { key, value },
                TransactionOp::Delete { key } => crate::raft::Command::Delete { key },
                TransactionOp::Read { .. } => continue,
                TransactionOp::ConditionalWrite { key, value, .. } => {
                    // For now, treat conditional write as regular write
                    // TODO: Implement proper conditional logic
                    crate::raft::Command::Write { key, value }
                }
            };

            leader_node.execute_command_direct(command).await?;
        }

        self.metrics
            .transactions_started
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .successful_commits
            .fetch_add(1, Ordering::Relaxed);

        Ok(tx_id)
    }

    async fn commit_transaction(&self, _tx_id: &str) -> Result<()> {
        // Already committed in begin_transaction
        Ok(())
    }

    async fn abort_transaction(&self, _tx_id: &str) -> Result<()> {
        // Not supported for simple coordinator
        Ok(())
    }

    fn get_transaction_status(&self, _tx_id: &str) -> Option<TransactionState> {
        Some(TransactionState::Committed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_creation() {
        let coordinator = TwoPhaseCommitCoordinator::new();
        assert_eq!(
            coordinator
                .metrics
                .active_transactions
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn test_transaction_id_generation() {
        let coordinator = TwoPhaseCommitCoordinator::new();
        let tx_id1 = coordinator.generate_tx_id();
        let tx_id2 = coordinator.generate_tx_id();

        assert_ne!(tx_id1, tx_id2);
        assert!(tx_id1.starts_with("tx_"));
        assert!(tx_id2.starts_with("tx_"));
    }

    #[test]
    fn test_operation_validation() {
        let coordinator = TwoPhaseCommitCoordinator::new();

        // Empty operations should fail
        let empty_ops = HashMap::new();
        assert!(coordinator.validate_operations(&empty_ops).is_err());

        // Valid operations should pass
        let mut valid_ops = HashMap::new();
        valid_ops.insert(
            1,
            vec![TransactionOp::Write {
                key: b"test".to_vec(),
                value: b"value".to_vec(),
            }],
        );
        assert!(coordinator.validate_operations(&valid_ops).is_ok());
    }
}
