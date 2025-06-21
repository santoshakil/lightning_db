use crate::error::{Error, Result};
use crate::{Database, LightningDbConfig};
use crossbeam::channel::{bounded, Sender};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Fast writer that batches writes automatically for high throughput
pub struct FastWriter {
    db: Arc<Database>,
    write_tx: Sender<WriteRequest>,
    pending_writes: Arc<Mutex<HashMap<u64, Sender<Result<()>>>>>,
    next_request_id: AtomicU64,
    shutdown: Arc<AtomicBool>,
}

struct WriteRequest {
    id: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl FastWriter {
    pub fn new(db: Arc<Database>) -> Arc<Self> {
        let (write_tx, write_rx) = bounded(10000);
        let pending_writes = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        
        let writer = Arc::new(Self {
            db: Arc::clone(&db),
            write_tx,
            pending_writes: Arc::clone(&pending_writes),
            next_request_id: AtomicU64::new(0),
            shutdown: Arc::clone(&shutdown),
        });
        
        // Start background writer thread
        let db_clone = Arc::clone(&db);
        let pending_clone = Arc::clone(&pending_writes);
        let shutdown_clone = Arc::clone(&shutdown);
        
        thread::spawn(move || {
            FastWriterWorker::new(db_clone, write_rx, pending_clone, shutdown_clone).run();
        });
        
        writer
    }
    
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(Error::Generic("FastWriter is shutting down".to_string()));
        }
        
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (result_tx, result_rx) = bounded(1);
        
        // Register pending write
        {
            let mut pending = self.pending_writes.lock();
            pending.insert(id, result_tx);
        }
        
        // Send write request
        self.write_tx.send(WriteRequest { id, key, value })
            .map_err(|_| Error::Generic("Write channel closed".to_string()))?;
        
        // Wait for result
        match result_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(result) => result,
            Err(_) => {
                // Timeout - remove from pending
                let mut pending = self.pending_writes.lock();
                pending.remove(&id);
                Err(Error::Generic("Write timeout".to_string()))
            }
        }
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

struct FastWriterWorker {
    db: Arc<Database>,
    write_rx: crossbeam::channel::Receiver<WriteRequest>,
    pending_writes: Arc<Mutex<HashMap<u64, Sender<Result<()>>>>>,
    shutdown: Arc<AtomicBool>,
    batch_size: usize,
    max_delay: Duration,
}

impl FastWriterWorker {
    fn new(
        db: Arc<Database>,
        write_rx: crossbeam::channel::Receiver<WriteRequest>,
        pending_writes: Arc<Mutex<HashMap<u64, Sender<Result<()>>>>>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            db,
            write_rx,
            pending_writes,
            shutdown,
            batch_size: 1000,
            max_delay: Duration::from_millis(10),
        }
    }
    
    fn run(self) {
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut last_flush = Instant::now();
        
        while !self.shutdown.load(Ordering::Relaxed) {
            // Collect writes for up to max_delay or until batch is full
            let deadline = last_flush + self.max_delay;
            
            while batch.len() < self.batch_size && Instant::now() < deadline {
                match self.write_rx.recv_timeout(Duration::from_millis(1)) {
                    Ok(write) => batch.push(write),
                    Err(_) => {
                        if !batch.is_empty() && Instant::now() >= deadline {
                            break;
                        }
                    }
                }
            }
            
            // Process batch if we have writes
            if !batch.is_empty() {
                self.process_batch(&mut batch);
                last_flush = Instant::now();
            }
        }
        
        // Process any remaining writes
        while let Ok(write) = self.write_rx.try_recv() {
            batch.push(write);
        }
        if !batch.is_empty() {
            self.process_batch(&mut batch);
        }
    }
    
    fn process_batch(&self, batch: &mut Vec<WriteRequest>) {
        // Start transaction
        let tx_result = self.db.begin_transaction();
        
        match tx_result {
            Ok(tx_id) => {
                let mut write_results = Vec::with_capacity(batch.len());
                let mut success = true;
                
                // Execute all writes in the transaction
                for write in batch.iter() {
                    match self.db.put_tx(tx_id, &write.key, &write.value) {
                        Ok(_) => write_results.push((write.id, Ok(()))),
                        Err(e) => {
                            write_results.push((write.id, Err(e)));
                            success = false;
                            break;
                        }
                    }
                }
                
                // Commit or abort based on success
                let final_result = if success {
                    self.db.commit_transaction(tx_id)
                } else {
                    let _ = self.db.abort_transaction(tx_id);
                    Err(Error::Generic("Batch write failed".to_string()))
                };
                
                // Send results to waiters
                let mut pending = self.pending_writes.lock();
                for (id, result) in write_results {
                    if let Some(sender) = pending.remove(&id) {
                        let _ = sender.send(result);
                    }
                }
                
                // If commit failed, notify remaining waiters
                if final_result.is_err() {
                    for write in batch.iter() {
                        if let Some(sender) = pending.remove(&write.id) {
                            let _ = sender.send(Err(Error::Generic("Transaction commit failed".to_string())));
                        }
                    }
                }
            }
            Err(e) => {
                // Failed to start transaction - notify all waiters
                let mut pending = self.pending_writes.lock();
                for write in batch.iter() {
                    if let Some(sender) = pending.remove(&write.id) {
                        let _ = sender.send(Err(Error::Generic(format!("Failed to start transaction: {}", e))));
                    }
                }
            }
        }
        
        batch.clear();
    }
}

impl Drop for FastWriter {
    fn drop(&mut self) {
        self.shutdown();
    }
}