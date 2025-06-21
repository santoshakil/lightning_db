use crate::error::Result;
use crate::storage::Page;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;

/// Async storage trait for non-blocking I/O operations
#[async_trait]
pub trait AsyncStorage: Send + Sync {
    /// Allocate a new page asynchronously
    async fn allocate_page(&self) -> Result<u32>;
    
    /// Free a page asynchronously
    async fn free_page(&self, page_id: u32) -> Result<()>;
    
    /// Read a page asynchronously
    async fn read_page(&self, page_id: u32) -> Result<Page>;
    
    /// Write a page asynchronously
    async fn write_page(&self, page: &Page) -> Result<()>;
    
    /// Sync all pending writes to disk
    async fn sync(&self) -> Result<()>;
    
    /// Get total page count
    fn page_count(&self) -> u32;
    
    /// Get free page count
    fn free_page_count(&self) -> usize;
}

/// Async WAL trait for non-blocking log operations
#[async_trait]
pub trait AsyncWAL: Send + Sync {
    /// Append an operation to the log asynchronously
    async fn append(&self, operation: crate::wal::WALOperation) -> Result<()>;
    
    /// Sync the WAL to disk
    async fn sync(&self) -> Result<()>;
    
    /// Recover operations from the WAL
    async fn recover(&self) -> Result<Vec<crate::wal::WALOperation>>;
    
    /// Checkpoint the WAL (mark operations as persisted)
    async fn checkpoint(&self, operations_count: usize) -> Result<()>;
}

/// Async transaction trait for non-blocking transaction processing
#[async_trait]
pub trait AsyncTransaction: Send + Sync {
    /// Begin a transaction asynchronously
    async fn begin(&self) -> Result<u64>;
    
    /// Put a key-value pair in the transaction
    async fn put(&self, tx_id: u64, key: &[u8], value: &[u8]) -> Result<()>;
    
    /// Get a value from the transaction
    async fn get(&self, tx_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>>;
    
    /// Delete a key from the transaction
    async fn delete(&self, tx_id: u64, key: &[u8]) -> Result<()>;
    
    /// Commit a transaction asynchronously
    async fn commit(&self, tx_id: u64) -> Result<()>;
    
    /// Abort a transaction asynchronously
    async fn abort(&self, tx_id: u64) -> Result<()>;
}

/// Future type for batch operations
pub type BatchFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

/// Async batch processor for high-throughput operations
#[async_trait]
pub trait AsyncBatchProcessor: Send + Sync {
    /// Process a batch of write operations
    async fn process_write_batch(&self, writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()>;
    
    /// Process a batch of read operations
    async fn process_read_batch(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>>;
    
    /// Get the optimal batch size for this processor
    fn optimal_batch_size(&self) -> usize;
}

/// Configuration for async I/O operations
#[derive(Debug, Clone)]
pub struct AsyncIOConfig {
    /// Number of async I/O worker threads
    pub worker_threads: usize,
    
    /// Maximum number of concurrent operations
    pub max_concurrent_ops: usize,
    
    /// Buffer size for async operations
    pub buffer_size: usize,
    
    /// Enable write coalescing (combine multiple writes)
    pub enable_write_coalescing: bool,
    
    /// Write coalescing window in milliseconds
    pub write_coalescing_window_ms: u64,
    
    /// Enable read-ahead for sequential access
    pub enable_read_ahead: bool,
    
    /// Read-ahead distance in pages
    pub read_ahead_pages: usize,
}

impl Default for AsyncIOConfig {
    fn default() -> Self {
        Self {
            worker_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            max_concurrent_ops: 1000,
            buffer_size: 64 * 1024, // 64KB
            enable_write_coalescing: true,
            write_coalescing_window_ms: 5,
            enable_read_ahead: true,
            read_ahead_pages: 8,
        }
    }
}

/// Async I/O statistics
#[derive(Debug, Clone, Default)]
pub struct AsyncIOStats {
    pub total_reads: u64,
    pub total_writes: u64,
    pub total_syncs: u64,
    pub read_latency_avg_us: u64,
    pub write_latency_avg_us: u64,
    pub sync_latency_avg_us: u64,
    pub concurrent_ops_peak: u64,
    pub write_coalescing_hits: u64,
    pub read_ahead_hits: u64,
}

/// Result type for async batch operations
#[derive(Debug)]
pub struct AsyncBatchResult<T> {
    pub results: Vec<Result<T>>,
    pub completed_count: usize,
    pub failed_count: usize,
    pub total_time_ms: u64,
}

impl<T> AsyncBatchResult<T> {
    pub fn new(results: Vec<Result<T>>, total_time_ms: u64) -> Self {
        let completed_count = results.iter().filter(|r| r.is_ok()).count();
        let failed_count = results.len() - completed_count;
        
        Self {
            results,
            completed_count,
            failed_count,
            total_time_ms,
        }
    }
    
    pub fn success_rate(&self) -> f64 {
        if self.results.is_empty() {
            0.0
        } else {
            self.completed_count as f64 / self.results.len() as f64
        }
    }
    
    pub fn throughput_ops_per_sec(&self) -> f64 {
        if self.total_time_ms == 0 {
            0.0
        } else {
            (self.completed_count as f64 * 1000.0) / self.total_time_ms as f64
        }
    }
}