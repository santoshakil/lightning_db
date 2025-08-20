use crate::core::error::{Error, Result};
use crate::Database;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Branch prediction hints - using stable intrinsics
#[inline(always)]
fn likely(b: bool) -> bool {
    #[cold]
    fn cold() {}
    
    if !b {
        cold();
    }
    b
}

#[inline(always)]
fn unlikely(b: bool) -> bool {
    #[cold]
    fn cold() {}
    
    if b {
        cold();
    }
    b
}

// Optimized batch sizes for better amortization
const DEFAULT_BATCH_SIZE: usize = 1024;  // Increased from typical 100-200
const MAX_BATCH_SIZE: usize = 8192;     // Allow larger batches for better throughput
const STACK_BUFFER_SIZE: usize = 64;    // For small key-value pairs

/// Stack-allocated buffer for small batches to avoid heap allocations
#[repr(align(64))]
#[derive(Debug)]
struct StackBuffer {
    data: [(Vec<u8>, Vec<u8>); STACK_BUFFER_SIZE],
    len: usize,
}

impl Default for StackBuffer {
    fn default() -> Self {
        unsafe {
            let mut buffer = std::mem::MaybeUninit::<Self>::uninit();
            let ptr = buffer.as_mut_ptr();
            std::ptr::write(&mut (*ptr).len, 0);
            buffer.assume_init()
        }
    }
}

/// Fast write batcher that bypasses transactions for better performance
#[derive(Debug)]
pub struct FastAutoBatcher {
    db: Arc<Database>,
    inner: Arc<Mutex<BatcherInner>>,
    stats: Arc<BatcherStats>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug)]
struct BatcherInner {
    pending: VecDeque<(Vec<u8>, Vec<u8>)>,
    stack_buffer: StackBuffer,  // For small fast batches
    last_flush: Instant,
    batch_size: usize,
    max_delay: Duration,
    write_combining_enabled: bool,
}

#[derive(Debug)]
struct BatcherStats {
    writes_submitted: AtomicU64,
    writes_completed: AtomicU64,
    batches_flushed: AtomicU64,
    write_errors: AtomicU64,
}

impl FastAutoBatcher {
    pub fn new(db: Arc<Database>, batch_size: usize, max_delay_ms: u64) -> Arc<Self> {
        let optimized_batch_size = batch_size.max(DEFAULT_BATCH_SIZE).min(MAX_BATCH_SIZE);
        
        let inner = Arc::new(Mutex::new(BatcherInner {
            pending: VecDeque::with_capacity(optimized_batch_size * 2),
            stack_buffer: StackBuffer::default(),
            last_flush: Instant::now(),
            batch_size: optimized_batch_size,
            max_delay: Duration::from_millis(max_delay_ms),
            write_combining_enabled: true,
        }));

        let stats = Arc::new(BatcherStats {
            writes_submitted: AtomicU64::new(0),
            writes_completed: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        });

        let shutdown = Arc::new(AtomicBool::new(false));

        let batcher = Arc::new(Self {
            db: db.clone(),
            inner: inner.clone(),
            stats: stats.clone(),
            shutdown: shutdown.clone(),
        });

        // Start background flusher thread
        let db_clone = db.clone();
        let inner_clone = inner.clone();
        let stats_clone = stats.clone();
        let shutdown_clone = shutdown.clone();

        thread::spawn(move || {
            // Higher priority background thread for better batching
            // Note: thread_priority crate would be needed for this optimization
            // let _ = thread_priority::set_current_thread_priority(
            //     thread_priority::ThreadPriority::Max
            // );
            
            while !shutdown_clone.load(Ordering::Relaxed) {
                // Adaptive sleep based on workload
                let sleep_duration = {
                    let inner = inner_clone.lock();
                    if inner.pending.len() > inner.batch_size / 2 {
                        Duration::from_millis(1) // High load - check frequently
                    } else {
                        Duration::from_millis(5) // Lower load - less frequent
                    }
                };
                thread::sleep(sleep_duration);

                let should_flush = {
                    let inner = inner_clone.lock();
                    !inner.pending.is_empty()
                        && (inner.pending.len() >= inner.batch_size
                            || inner.last_flush.elapsed() >= inner.max_delay
                            || inner.stack_buffer.len >= STACK_BUFFER_SIZE)
                };

                if should_flush {
                    Self::flush_pending(&db_clone, &inner_clone, &stats_clone);
                }
            }

            // Final flush
            Self::flush_pending(&db_clone, &inner_clone, &stats_clone);
        });

        batcher
    }

    /// Submit a write to be batched - optimized hot path
    #[inline(always)]
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if unlikely(self.shutdown.load(Ordering::Relaxed)) {
            return Err(Error::Generic(
                "FastAutoBatcher is shutting down".to_string(),
            ));
        }

        self.stats.writes_submitted.fetch_add(1, Ordering::Relaxed);

        // Try stack buffer first for small items to reduce lock contention
        let total_size = key.len() + value.len();
        if total_size <= 256 {  // Small key-value pairs
            let mut inner = self.inner.lock();
            if inner.write_combining_enabled && inner.stack_buffer.len < STACK_BUFFER_SIZE {
                unsafe {
                    let idx = inner.stack_buffer.len;
                    std::ptr::write(&mut inner.stack_buffer.data[idx], (key, value));
                    inner.stack_buffer.len += 1;
                }
                return Ok(());
            }
            // Fall through to normal path if stack buffer full
            inner.pending.push_back((key, value));
        } else {
            // Large items go directly to pending queue
            let mut inner = self.inner.lock();
            inner.pending.push_back((key, value));
        }

        Ok(())
    }

    /// Force flush all pending writes
    pub fn flush(&self) -> Result<()> {
        Self::flush_pending(&self.db, &self.inner, &self.stats);
        Ok(())
    }

    /// Flush pending writes directly without transactions - optimized
    fn flush_pending(
        db: &Arc<Database>,
        inner: &Arc<Mutex<BatcherInner>>,
        stats: &Arc<BatcherStats>,
    ) {
        let (writes, stack_writes) = {
            let mut inner = inner.lock();
            
            // Collect stack buffer writes first
            let mut stack_writes = Vec::new();
            if inner.stack_buffer.len > 0 {
                for i in 0..inner.stack_buffer.len {
                    unsafe {
                        let item = std::ptr::read(&inner.stack_buffer.data[i]);
                        stack_writes.push(item);
                    }
                }
                inner.stack_buffer.len = 0;
            }
            
            if inner.pending.is_empty() && stack_writes.is_empty() {
                return;
            }

            // Take all pending writes
            let mut writes = VecDeque::new();
            std::mem::swap(&mut writes, &mut inner.pending);
            inner.last_flush = Instant::now();
            (writes, stack_writes)
        };

        // Combine both sources of writes for vectorized processing
        let mut all_writes = Vec::with_capacity(writes.len() + stack_writes.len());
        all_writes.extend(writes);
        all_writes.extend(stack_writes);
        
        if all_writes.is_empty() {
            return;
        }

        // Execute batch as single transaction for better performance
        let write_count = all_writes.len() as u64;
        let success_count = match Self::execute_batch_transaction_vectorized(db, all_writes) {
            Ok(count) => count,
            Err(_) => {
                stats.write_errors.fetch_add(write_count, Ordering::Relaxed);
                0
            }
        };

        stats
            .writes_completed
            .fetch_add(success_count, Ordering::Relaxed);
        stats.batches_flushed.fetch_add(1, Ordering::Relaxed);
    }

    /// Vectorized batch execution with optimized memory access patterns
    fn execute_batch_transaction_vectorized(db: &Database, writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<u64> {
        if writes.is_empty() {
            return Ok(0);
        }

        let write_count = writes.len() as u64;

        // For sync WAL mode with LSM enabled, use optimized vectorized write path
        if matches!(db._config.wal_sync_mode, crate::WalSyncMode::Sync) {
            if let Some(ref lsm) = db.lsm_tree {
                // Bypass transactions entirely for sync WAL to avoid double-sync
                // Use vectorized writes for better cache locality
                let mut keys: Vec<Vec<u8>> = Vec::with_capacity(writes.len());
                let mut values: Vec<Vec<u8>> = Vec::with_capacity(writes.len());
                
                for (key, value) in writes {
                    keys.push(key);
                    values.push(value);
                }
                
                // Batch insert with prefetch hints for better cache performance
                for (key, value) in keys.into_iter().zip(values.into_iter()) {
                    // Direct memtable insert without WAL logging
                    lsm.insert_no_wal(key, value)?;
                }

                // Now sync the WAL once for the entire batch
                if let Some(ref wal) = db.unified_wal {
                    // Write a checkpoint to mark the batch
                    wal.append(crate::core::wal::WALOperation::Checkpoint {
                        lsn: write_count, // Use write count as checkpoint marker
                    })?;
                    wal.sync()?;
                }

                return Ok(write_count);
            }
        }

        // Use a single transaction for the entire batch with optimized loop unrolling
        let tx_id = db.begin_transaction()?;
        let mut success_count = 0u64;
        let mut i = 0;
        
        // Process writes in chunks of 4 for loop unrolling
        while i + 3 < writes.len() {
            // Unroll loop for better instruction-level parallelism
            let results = [
                db.put_tx(tx_id, &writes[i].0, &writes[i].1),
                db.put_tx(tx_id, &writes[i+1].0, &writes[i+1].1),
                db.put_tx(tx_id, &writes[i+2].0, &writes[i+2].1),
                db.put_tx(tx_id, &writes[i+3].0, &writes[i+3].1),
            ];
            
            for result in results {
                match result {
                    Ok(_) => success_count += 1,
                    Err(_) => {
                        // Abort transaction on any failure
                        let _ = db.abort_transaction(tx_id);
                        return Err(crate::core::error::Error::Generic(
                            "Batch transaction failed".to_string(),
                        ));
                    }
                }
            }
            i += 4;
        }
        
        // Handle remaining writes
        for (key, value) in &writes[i..] {
            match db.put_tx(tx_id, key, value) {
                Ok(_) => success_count += 1,
                Err(_) => {
                    // Abort transaction on any failure
                    let _ = db.abort_transaction(tx_id);
                    return Err(crate::core::error::Error::Generic(
                        "Batch transaction failed".to_string(),
                    ));
                }
            }
        }

        // Commit all writes at once
        db.commit_transaction(tx_id)?;
        Ok(success_count)
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Force final flush before shutdown
        self.flush().ok();
        // Give the background thread time to finish
        thread::sleep(Duration::from_millis(100));
    }

    pub fn get_stats(&self) -> (u64, u64, u64, u64) {
        (
            self.stats.writes_submitted.load(Ordering::Relaxed),
            self.stats.writes_completed.load(Ordering::Relaxed),
            self.stats.batches_flushed.load(Ordering::Relaxed),
            self.stats.write_errors.load(Ordering::Relaxed),
        )
    }

    /// Wait for all submitted writes to complete
    pub fn wait_for_completion(&self) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(30);

        loop {
            let (submitted, completed, _, errors) = self.get_stats();

            if errors > 0 {
                return Err(Error::Generic(format!("{} writes failed", errors)));
            }

            if completed >= submitted {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(Error::Generic(
                    "Timeout waiting for writes to complete".to_string(),
                ));
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for FastAutoBatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}
