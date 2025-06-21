use crate::error::Result;
use crate::Database;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Automatic write batcher for improving single put performance
pub struct WriteBatcher {
    db: Arc<Database>,
    batch_size: usize,
    max_delay_ms: u64,
    pending: Arc<Mutex<VecDeque<PendingWrite>>>,
    shutdown: Arc<AtomicBool>,
    writes_queued: Arc<AtomicU64>,
    writes_completed: Arc<AtomicU64>,
}

struct PendingWrite {
    key: Vec<u8>,
    value: Vec<u8>,
    result_sender: tokio::sync::oneshot::Sender<Result<()>>,
    queued_at: Instant,
}

impl WriteBatcher {
    pub fn new(db: Arc<Database>, batch_size: usize, max_delay_ms: u64) -> Arc<Self> {
        let batcher = Arc::new(Self {
            db,
            batch_size,
            max_delay_ms,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            writes_queued: Arc::new(AtomicU64::new(0)),
            writes_completed: Arc::new(AtomicU64::new(0)),
        });
        
        // Start background flush thread
        let batcher_clone = Arc::clone(&batcher);
        thread::spawn(move || {
            batcher_clone.flush_loop();
        });
        
        batcher
    }
    
    /// Queue a write operation
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        {
            let mut pending = self.pending.lock();
            pending.push_back(PendingWrite {
                key,
                value,
                result_sender: tx,
                queued_at: Instant::now(),
            });
            
            self.writes_queued.fetch_add(1, Ordering::Relaxed);
            
            // If batch is full, notify immediately
            if pending.len() >= self.batch_size {
                drop(pending);
                self.flush_batch();
            }
        }
        
        // Wait for result
        rx.await.map_err(|_| crate::error::Error::Generic("Write batcher shutdown".to_string()))?
    }
    
    /// Synchronous put for non-async contexts
    pub fn put_sync(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // For sync contexts, just use a transaction directly
        let tx_id = self.db.begin_transaction()?;
        self.db.put_tx(tx_id, &key, &value)?;
        self.db.commit_transaction(tx_id)
    }
    
    fn flush_loop(&self) {
        let max_delay = Duration::from_millis(self.max_delay_ms);
        
        while !self.shutdown.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(10)); // Check every 10ms
            
            let should_flush = {
                let pending = self.pending.lock();
                if pending.is_empty() {
                    false
                } else if pending.len() >= self.batch_size {
                    true
                } else {
                    // Check if oldest write has exceeded max delay
                    pending.front()
                        .map(|w| w.queued_at.elapsed() >= max_delay)
                        .unwrap_or(false)
                }
            };
            
            if should_flush {
                self.flush_batch();
            }
        }
    }
    
    fn flush_batch(&self) {
        let mut batch = Vec::new();
        let mut senders = Vec::new();
        
        {
            let mut pending = self.pending.lock();
            while !pending.is_empty() && batch.len() < self.batch_size {
                if let Some(write) = pending.pop_front() {
                    batch.push((write.key, write.value));
                    senders.push(write.result_sender);
                }
            }
        }
        
        if batch.is_empty() {
            return;
        }
        
        // Execute batch write
        let result = self.execute_batch(&batch);
        
        // Send results
        for sender in senders {
            let _ = sender.send(result.clone());
        }
        
        self.writes_completed.fetch_add(batch.len() as u64, Ordering::Relaxed);
    }
    
    fn execute_batch(&self, batch: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let tx_id = self.db.begin_transaction()?;
        
        for (key, value) in batch {
            self.db.put_tx(tx_id, key, value)?;
        }
        
        self.db.commit_transaction(tx_id)
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        
        // Flush any remaining writes
        self.flush_batch();
    }
    
    pub fn stats(&self) -> BatcherStats {
        BatcherStats {
            writes_queued: self.writes_queued.load(Ordering::Relaxed),
            writes_completed: self.writes_completed.load(Ordering::Relaxed),
            pending_count: self.pending.lock().len(),
        }
    }
}

pub struct BatcherStats {
    pub writes_queued: u64,
    pub writes_completed: u64,
    pub pending_count: usize,
}

impl Drop for WriteBatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}