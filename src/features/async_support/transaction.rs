// TODO: Fix async storage imports
// use crate::async_storage::{AsyncIOConfig, AsyncTransaction};
use crate::core::error::{Error, Result};
use crate::core::transaction::{UnifiedTransaction, TxState};
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Semaphore, OwnedSemaphorePermit};
use tokio::time::timeout;

/// Wrapper for transaction with its semaphore permit
struct TransactionWithPermit {
    transaction: Arc<RwLock<Transaction>>,
    _permit: OwnedSemaphorePermit,
}

/// Async transaction manager for non-blocking transaction processing
pub struct AsyncTransactionManager {
    active_transactions: Arc<DashMap<u64, TransactionWithPermit>>,
    next_tx_id: AtomicU64,
    commit_timestamp: AtomicU64,
    config: AsyncIOConfig,
    commit_processor: Option<mpsc::Sender<CommitRequest>>,
    concurrency_limiter: Arc<Semaphore>,
    // Handle to background task for proper cleanup
    background_task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
struct CommitRequest {
    _tx_id: u64,
    transaction: Transaction,
    response: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
struct _CommitBatch {
    requests: Vec<CommitRequest>,
    batch_timestamp: u64,
}

impl AsyncTransactionManager {
    /// Create a new async transaction manager
    pub fn new(max_transactions: usize, config: AsyncIOConfig) -> Self {
        let mut manager = Self {
            active_transactions: Arc::new(DashMap::new()),
            next_tx_id: AtomicU64::new(1),
            commit_timestamp: AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
            ),
            config: config.clone(),
            commit_processor: None,
            concurrency_limiter: Arc::new(Semaphore::new(max_transactions)),
            background_task: None,
        };

        // Start commit processing if batching is enabled
        if config.enable_write_coalescing {
            manager.start_commit_processing();
        }

        manager
    }

    /// Start the commit processing background task
    fn start_commit_processing(&mut self) {
        let (tx, mut rx) = mpsc::channel::<CommitRequest>(self.config.buffer_size);
        self.commit_processor = Some(tx);

        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            let mut pending_commits = Vec::new();
            let batch_window = Duration::from_millis(config.write_coalescing_window_ms);

            loop {
                let deadline = Instant::now() + batch_window;

                // Collect commits for batching
                while Instant::now() < deadline && pending_commits.len() < config.buffer_size {
                    match timeout(Duration::from_millis(1), rx.recv()).await {
                        Ok(Some(commit_req)) => pending_commits.push(commit_req),
                        Ok(None) => {
                            // Channel closed
                            if !pending_commits.is_empty() {
                                Self::process_commit_batch(pending_commits).await;
                            }
                            return;
                        }
                        Err(_) => break, // Timeout
                    }
                }

                if !pending_commits.is_empty() {
                    Self::process_commit_batch(pending_commits).await;
                    pending_commits = Vec::new();
                }
            }
        });
        
        // Store the handle for cleanup
        self.background_task = Some(handle);
    }

    /// Process a batch of commits together
    async fn process_commit_batch(requests: Vec<CommitRequest>) {
        let _batch_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Validate all transactions in the batch
        let mut valid_requests = Vec::new();
        let mut invalid_requests = Vec::new();

        for request in requests {
            // Simplified validation - in practice you'd check for conflicts
            if request.transaction.state == TxState::Active {
                valid_requests.push(request);
            } else {
                invalid_requests.push((
                    request,
                    Error::Transaction("Transaction not active".to_string()),
                ));
            }
        }

        // Commit valid transactions
        for request in valid_requests {
            // In a real implementation, you'd apply writes to storage here
            let _ = request.response.send(Ok(()));
        }

        // Reject invalid transactions
        for (request, error) in invalid_requests {
            let _ = request.response.send(Err(error));
        }
    }

    /// Get transaction statistics
    pub fn get_stats(&self) -> AsyncTransactionStats {
        AsyncTransactionStats {
            active_transactions: self.active_transactions.len() as u64,
            total_commits: 0, // Would track this in a real implementation
            total_aborts: 0,
            avg_commit_time_us: 0,
            concurrent_transactions_peak: 0,
        }
    }

    /// Clean up old transactions
    pub async fn cleanup_old_transactions(&self, max_age: Duration) {
        let cutoff = SystemTime::now() - max_age;
        let cutoff_millis = cutoff
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Use retain for atomic removal to prevent race conditions
        self.active_transactions.retain(|_tx_id, tx_with_permit| {
            let tx = tx_with_permit.transaction.read();
            tx.read_timestamp >= cutoff_millis
        });
    }
}

impl Drop for AsyncTransactionManager {
    fn drop(&mut self) {
        // Signal shutdown by dropping the channel sender
        self.commit_processor.take();
        
        // If there's a background task, attempt to clean it up
        // Note: We can't block_on in Drop, so we just abort the task
        if let Some(handle) = self.background_task.take() {
            // Abort the background task to ensure it stops
            handle.abort();
        }
    }
}

#[async_trait]
impl AsyncTransaction for AsyncTransactionManager {
    async fn begin(&self) -> Result<u64> {
        // Acquire concurrency limit - store permit with transaction to prevent leaks
        let permit = self.concurrency_limiter.clone().acquire_owned().await
            .map_err(|_| Error::Generic("Failed to acquire transaction permit".to_string()))?;

        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let read_timestamp = self.commit_timestamp.load(Ordering::SeqCst);

        let transaction = Arc::new(RwLock::new(UnifiedTransaction::new(tx_id, read_timestamp)));
        let tx_with_permit = TransactionWithPermit {
            transaction,
            _permit: permit,
        };
        
        self.active_transactions.insert(tx_id, tx_with_permit);

        Ok(tx_id)
    }

    async fn put(&self, tx_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let tx_arc = self
            .active_transactions
            .get(&tx_id)
            .ok_or(Error::Transaction("Transaction not found".to_string()))?
            .transaction.clone();

        let mut tx = tx_arc.write();
        if !tx.is_active() {
            return Err(Error::Transaction("Transaction is not active".to_string()));
        }

        // Add write to transaction
        tx.add_write(key.to_vec(), Some(value.to_vec()), 0);
        Ok(())
    }

    async fn get(&self, tx_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tx_arc = self
            .active_transactions
            .get(&tx_id)
            .ok_or(Error::Transaction("Transaction not found".to_string()))?
            .transaction.clone();

        let mut tx = tx_arc.write();
        if !tx.is_active() {
            return Err(Error::Transaction("Transaction is not active".to_string()));
        }

        // Check write set first (O(1) lookup with HashMap)
        if let Some(write_op) = tx.write_set.get(key) {
            return Ok(write_op.value.as_ref().map(|v| v.to_vec()));
        }

        // Record read
        tx.add_read(key.to_vec(), 0);

        // TODO: Read from storage
        Ok(None)
    }

    async fn delete(&self, tx_id: u64, key: &[u8]) -> Result<()> {
        let tx_arc = self
            .active_transactions
            .get(&tx_id)
            .ok_or(Error::Transaction("Transaction not found".to_string()))?
            .transaction.clone();

        let mut tx = tx_arc.write();
        if !tx.is_active() {
            return Err(Error::Transaction("Transaction is not active".to_string()));
        }

        // Add delete to transaction
        tx.add_write(key.to_vec(), None::<Vec<u8>>, 0);
        Ok(())
    }

    async fn commit(&self, tx_id: u64) -> Result<()> {
        // Get transaction WITHOUT removing it first - prevents losing transaction on commit failure
        let tx_with_permit = self
            .active_transactions
            .get(&tx_id)
            .ok_or(Error::Transaction("Transaction not found".to_string()))?;
            
        let tx_arc = tx_with_permit.transaction.clone();
        
        let transaction = {
            let mut tx = tx_arc.write();
            if !tx.is_active() {
                return Err(Error::Transaction("Transaction is not active".to_string()));
            }
            tx.prepare();
            tx.clone()
        };

        // Perform the actual commit
        let commit_result = if let Some(ref processor) = self.commit_processor {
            let (tx, rx) = oneshot::channel();
            let request = CommitRequest {
                _tx_id: tx_id,
                transaction,
                response: tx,
            };

            let send_result = processor
                .send(request)
                .await
                .map_err(|_| Error::Generic("Commit processor channel closed".to_string()));
                
            match send_result {
                Ok(_) => rx.await
                    .map_err(|_| Error::Generic("Commit response channel closed".to_string()))?,
                Err(e) => return Err(e),
            }
        } else {
            // Direct commit
            // TODO: Apply writes to storage
            Ok(())
        };

        // Only remove transaction after successful commit
        match commit_result {
            Ok(_) => {
                self.active_transactions.remove(&tx_id);
                Ok(())
            }
            Err(e) => {
                // Revert transaction state on failure
                let mut tx = tx_arc.write();
                tx.abort();
                Err(e)
            }
        }
    }

    async fn abort(&self, tx_id: u64) -> Result<()> {
        if let Some((_, tx_with_permit)) = self.active_transactions.remove(&tx_id) {
            let mut tx = tx_with_permit.transaction.write();
            tx.abort();
            // Permit is automatically dropped when tx_with_permit is dropped
        }
        Ok(())
    }
}

/// Statistics for async transaction processing
#[derive(Debug, Clone, Default)]
pub struct AsyncTransactionStats {
    pub active_transactions: u64,
    pub total_commits: u64,
    pub total_aborts: u64,
    pub avg_commit_time_us: u64,
    pub concurrent_transactions_peak: u64,
}

/// Async batch transaction processor
pub struct AsyncBatchTransactionProcessor {
    transaction_manager: Arc<AsyncTransactionManager>,
    config: AsyncIOConfig,
}

impl AsyncBatchTransactionProcessor {
    pub fn new(transaction_manager: Arc<AsyncTransactionManager>, config: AsyncIOConfig) -> Self {
        Self {
            transaction_manager,
            config,
        }
    }

    /// Process multiple transactions in parallel
    pub async fn process_parallel_transactions(
        &self,
        operations: Vec<Vec<(Vec<u8>, Vec<u8>)>>, // Vec of transaction operations
    ) -> Result<Vec<Result<()>>> {
        let mut handles = Vec::new();

        for ops in operations {
            let tx_manager = self.transaction_manager.clone();
            let handle = tokio::spawn(async move {
                let tx_id = tx_manager.begin().await?;

                for (key, value) in ops {
                    tx_manager.put(tx_id, &key, &value).await?;
                }

                tx_manager.commit(tx_id).await
            });

            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(_) => results.push(Err(Error::Generic("Task join error".to_string()))),
            }
        }

        Ok(results)
    }

    /// Process a large transaction by splitting it into smaller batches
    pub async fn process_large_transaction(
        &self,
        operations: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let batch_size = self.config.buffer_size;

        for chunk in operations.chunks(batch_size) {
            let tx_id = self.transaction_manager.begin().await?;

            for (key, value) in chunk {
                self.transaction_manager.put(tx_id, key, value).await?;
            }

            self.transaction_manager.commit(tx_id).await?;
        }

        Ok(())
    }
}
