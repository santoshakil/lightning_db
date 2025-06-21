use crate::error::{Error, Result};
use crate::Database;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use parking_lot::Mutex;

/// Lock-free write batcher for maximum performance
pub struct LockFreeBatcher {
    _db: Arc<Database>,
    queue: Arc<ArrayQueue<(Vec<u8>, Vec<u8>)>>,
    stats: Arc<BatcherStats>,
    shutdown: Arc<AtomicBool>,
    batch_size: usize,
    _max_delay: Duration,
    worker_thread: Arc<Mutex<Option<JoinHandle<()>>>>,
}

struct BatcherStats {
    writes_submitted: AtomicU64,
    writes_completed: AtomicU64,
    batches_flushed: AtomicU64,
    write_errors: AtomicU64,
}

impl LockFreeBatcher {
    pub fn new(db: Arc<Database>, batch_size: usize, max_delay_ms: u64) -> Arc<Self> {
        let queue = Arc::new(ArrayQueue::new(batch_size * 10)); // 10x capacity
        
        let stats = Arc::new(BatcherStats {
            writes_submitted: AtomicU64::new(0),
            writes_completed: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        });
        
        let shutdown = Arc::new(AtomicBool::new(false));
        let max_delay = Duration::from_millis(max_delay_ms);
        
        let batcher = Self {
            _db: db.clone(),
            queue: queue.clone(),
            stats: stats.clone(),
            shutdown: shutdown.clone(),
            batch_size,
            _max_delay: max_delay,
            worker_thread: Arc::new(Mutex::new(None)),
        };
        
        // Start background flusher thread
        let db_clone = db.clone();
        let queue_clone = queue.clone();
        let stats_clone = stats.clone();
        let shutdown_clone = shutdown.clone();
        let batch_size_clone = batch_size;
        let max_delay_clone = max_delay;
        
        let worker_thread = thread::spawn(move || {
            let mut batch = Vec::with_capacity(batch_size_clone);
            let mut last_flush = Instant::now();
            
            while !shutdown_clone.load(Ordering::Acquire) {
                // Collect items from queue
                let mut collected = 0;
                while collected < batch_size_clone {
                    match queue_clone.pop() {
                        Some(item) => {
                            batch.push(item);
                            collected += 1;
                        }
                        None => break,
                    }
                }
                
                // Check if we should flush
                let should_flush = !batch.is_empty() && 
                    (batch.len() >= batch_size_clone || last_flush.elapsed() >= max_delay_clone);
                
                if should_flush {
                    Self::flush_batch(&db_clone, &mut batch, &stats_clone);
                    last_flush = Instant::now();
                } else if batch.is_empty() {
                    // No work, sleep briefly
                    thread::sleep(Duration::from_micros(50));
                }
            }
            
            // Final flush
            if !batch.is_empty() {
                Self::flush_batch(&db_clone, &mut batch, &stats_clone);
            }
        });
        
        *batcher.worker_thread.lock() = Some(worker_thread);
        Arc::new(batcher)
    }
    
    /// Submit a write to be batched
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::Generic("LockFreeBatcher is shutting down".to_string()));
        }
        
        self.stats.writes_submitted.fetch_add(1, Ordering::AcqRel);
        
        // Try to push to queue
        let mut retries = 0;
        loop {
            match self.queue.push((key.clone(), value.clone())) {
                Ok(()) => return Ok(()),
                Err(_) => {
                    retries += 1;
                    if retries > 100 {
                        return Err(Error::Generic("Queue is full".to_string()));
                    }
                    // Queue is full, spin briefly
                    thread::yield_now();
                }
            }
        }
    }
    
    /// Force flush by filling queue with sentinel values
    pub fn flush(&self) -> Result<()> {
        // Push sentinel values to trigger flush
        for _ in 0..self.batch_size {
            let _ = self.queue.push((vec![], vec![]));
        }
        Ok(())
    }
    
    /// Flush a batch of writes
    fn flush_batch(db: &Arc<Database>, batch: &mut Vec<(Vec<u8>, Vec<u8>)>, stats: &Arc<BatcherStats>) {
        if batch.is_empty() {
            return;
        }
        
        // Filter out sentinel values
        batch.retain(|(k, _)| !k.is_empty());
        
        if batch.is_empty() {
            return;
        }
        
        // Use put_batch for efficiency
        let batch_data: Vec<_> = batch.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        match db.put_batch(&batch_data) {
            Ok(_) => {
                stats.writes_completed.fetch_add(batch.len() as u64, Ordering::AcqRel);
                stats.batches_flushed.fetch_add(1, Ordering::AcqRel);
            }
            Err(_) => {
                stats.write_errors.fetch_add(batch.len() as u64, Ordering::AcqRel);
            }
        }
        
        batch.clear();
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        // Wake up worker thread
        for _ in 0..10 {
            let _ = self.queue.push((vec![], vec![]));
        }
    }
    
    pub fn get_stats(&self) -> (u64, u64, u64, u64) {
        (
            self.stats.writes_submitted.load(Ordering::Acquire),
            self.stats.writes_completed.load(Ordering::Acquire),
            self.stats.batches_flushed.load(Ordering::Acquire),
            self.stats.write_errors.load(Ordering::Acquire),
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
            
            if completed >= submitted && self.queue.is_empty() {
                return Ok(());
            }
            
            if start.elapsed() > timeout {
                return Err(Error::Generic("Timeout waiting for writes to complete".to_string()));
            }
            
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for LockFreeBatcher {
    fn drop(&mut self) {
        self.shutdown();
        
        // Wait for worker thread to finish
        if let Some(handle) = self.worker_thread.lock().take() {
            let _ = handle.join();
        }
    }
}