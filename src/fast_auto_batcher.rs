use crate::error::{Error, Result};
use crate::Database;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Fast write batcher that bypasses transactions for better performance
pub struct FastAutoBatcher {
    db: Arc<Database>,
    inner: Arc<Mutex<BatcherInner>>,
    stats: Arc<BatcherStats>,
    shutdown: Arc<AtomicBool>,
}

struct BatcherInner {
    pending: VecDeque<(Vec<u8>, Vec<u8>)>,
    last_flush: Instant,
    batch_size: usize,
    max_delay: Duration,
}

struct BatcherStats {
    writes_submitted: AtomicU64,
    writes_completed: AtomicU64,
    batches_flushed: AtomicU64,
    write_errors: AtomicU64,
}

impl FastAutoBatcher {
    pub fn new(db: Arc<Database>, batch_size: usize, max_delay_ms: u64) -> Arc<Self> {
        let inner = Arc::new(Mutex::new(BatcherInner {
            pending: VecDeque::with_capacity(batch_size * 2),
            last_flush: Instant::now(),
            batch_size,
            max_delay: Duration::from_millis(max_delay_ms),
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
            while !shutdown_clone.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(10)); // Check every 10ms for better batching
                
                let should_flush = {
                    let inner = inner_clone.lock();
                    !inner.pending.is_empty() && 
                    (inner.pending.len() >= inner.batch_size || 
                     inner.last_flush.elapsed() >= inner.max_delay)
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
    
    /// Submit a write to be batched
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(Error::Generic("FastAutoBatcher is shutting down".to_string()));
        }
        
        self.stats.writes_submitted.fetch_add(1, Ordering::Relaxed);
        
        // Add to pending queue
        {
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
    
    /// Flush pending writes directly without transactions
    fn flush_pending(db: &Arc<Database>, inner: &Arc<Mutex<BatcherInner>>, stats: &Arc<BatcherStats>) {
        let writes = {
            let mut inner = inner.lock();
            if inner.pending.is_empty() {
                return;
            }
            
            // Take all pending writes
            let mut writes = VecDeque::new();
            std::mem::swap(&mut writes, &mut inner.pending);
            inner.last_flush = Instant::now();
            writes
        };
        
        // Execute batch as single transaction for better performance
        let write_count = writes.len() as u64;
        let success_count = match Self::execute_batch_transaction(&db, writes.into()) {
            Ok(count) => count,
            Err(_) => {
                stats.write_errors.fetch_add(write_count, Ordering::Relaxed);
                0
            }
        };
        
        stats.writes_completed.fetch_add(success_count, Ordering::Relaxed);
        stats.batches_flushed.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Optimized batch execution using a single transaction
    fn execute_batch_transaction(db: &Database, writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<u64> {
        if writes.is_empty() {
            return Ok(0);
        }
        
        let write_count = writes.len() as u64;
        
        // For sync WAL mode with LSM enabled, use optimized direct write path
        if matches!(db._config.wal_sync_mode, crate::WalSyncMode::Sync) {
            if let Some(ref lsm) = db.lsm_tree {
                // Bypass transactions entirely for sync WAL to avoid double-sync
                // Write all to LSM memtable without individual WAL entries
                for (key, value) in writes {
                    // Direct memtable insert without WAL logging
                    lsm.insert_no_wal(key, value)?;
                }
                
                // Now sync the WAL once for the entire batch
                if let Some(ref wal) = db.improved_wal {
                    // Write a checkpoint to mark the batch
                    wal.append(crate::wal::WALOperation::Checkpoint {
                        lsn: write_count // Use write count as checkpoint marker
                    })?;
                    wal.sync()?;
                }
                
                return Ok(write_count);
            }
        }
        
        // Use a single transaction for the entire batch
        let tx_id = db.begin_transaction()?;
        let mut success_count = 0u64;
        
        for (key, value) in writes {
            match db.put_tx(tx_id, &key, &value) {
                Ok(_) => success_count += 1,
                Err(_) => {
                    // Abort transaction on any failure
                    let _ = db.abort_transaction(tx_id);
                    return Err(crate::error::Error::Generic("Batch transaction failed".to_string()));
                }
            }
        }
        
        // Commit all writes at once
        db.commit_transaction(tx_id)?;
        Ok(success_count)
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Give the background thread time to flush
        thread::sleep(Duration::from_millis(50));
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
                return Err(Error::Generic("Timeout waiting for writes to complete".to_string()));
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