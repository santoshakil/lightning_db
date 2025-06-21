use crate::error::{Error, Result};
use crate::Database;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Automatic write batcher that improves single write performance
/// by automatically batching writes into transactions
pub struct AutoBatcher {
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

impl AutoBatcher {
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
            let debug = std::env::var("DEBUG_BATCHER").is_ok();
            while !shutdown_clone.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_micros(100)); // Check every 100μs for better responsiveness
                
                let should_flush = {
                    let inner = inner_clone.lock();
                    let pending_len = inner.pending.len();
                    let time_elapsed = inner.last_flush.elapsed();
                    let batch_size = inner.batch_size;
                    let max_delay = inner.max_delay;
                    
                    if pending_len > 0 && debug {
                        println!("DEBUG: Pending: {}, Batch size: {}, Time elapsed: {:?}, Max delay: {:?}", 
                               pending_len, batch_size, time_elapsed, max_delay);
                    }
                    
                    !inner.pending.is_empty() && 
                    (inner.pending.len() >= inner.batch_size || 
                     inner.last_flush.elapsed() >= inner.max_delay)
                };
                
                if should_flush {
                    if debug {
                        println!("DEBUG: Flushing batch");
                    }
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
            return Err(Error::Generic("AutoBatcher is shutting down".to_string()));
        }
        
        self.stats.writes_submitted.fetch_add(1, Ordering::Relaxed);
        
        // Add to pending queue and check if we need to flush
        let should_trigger_flush = {
            let mut inner = self.inner.lock();
            inner.pending.push_back((key, value));
            
            // If we hit the batch size, we need to signal for immediate flush
            inner.pending.len() >= inner.batch_size
        };
        
        // If we hit batch size, trigger immediate flush in background
        // We don't do it synchronously to maintain async behavior
        if should_trigger_flush {
            // The background thread will pick this up in the next iteration (5ms max)
            // This is better than synchronous flushing which would block the caller
        }
        
        // Note: This returns immediately without waiting for the write to complete
        // This is key to achieving high throughput
        Ok(())
    }
    
    /// Force flush all pending writes and wait for completion
    pub fn flush(&self) -> Result<()> {
        Self::flush_pending(&self.db, &self.inner, &self.stats);
        Ok(())
    }
    
    /// Flush pending writes in a transaction
    fn flush_pending(db: &Arc<Database>, inner: &Arc<Mutex<BatcherInner>>, stats: &Arc<BatcherStats>) {
        let debug = std::env::var("DEBUG_BATCHER").is_ok();
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
        
        // Execute batch transaction
        let write_count = writes.len() as u64;
        if debug {
            println!("[AutoBatcher] Flushing {} writes", write_count);
        }
        match db.begin_transaction() {
            Ok(tx_id) => {
                let mut success = true;
                
                // Execute all writes in the transaction
                for (key, value) in writes {
                    if let Err(e) = db.put_tx(tx_id, &key, &value) {
                        success = false;
                        stats.write_errors.fetch_add(1, Ordering::Relaxed);
                        if debug {
                            eprintln!("[AutoBatcher] Write failed: {:?}", e);
                        }
                        break;
                    }
                }
                
                // Commit or abort
                if success {
                    match db.commit_transaction(tx_id) {
                        Ok(_) => {
                            stats.writes_completed.fetch_add(write_count, Ordering::Relaxed);
                            stats.batches_flushed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            if debug {
                                eprintln!("[AutoBatcher] Commit failed: {}", e);
                            }
                            let _ = db.abort_transaction(tx_id);
                            stats.write_errors.fetch_add(write_count, Ordering::Relaxed);
                        }
                    }
                } else {
                    let _ = db.abort_transaction(tx_id);
                }
            }
            Err(e) => {
                if debug {
                    eprintln!("[AutoBatcher] Failed to begin transaction: {:?}", e);
                }
                stats.write_errors.fetch_add(write_count, Ordering::Relaxed);
            }
        }
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

impl Drop for AutoBatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}