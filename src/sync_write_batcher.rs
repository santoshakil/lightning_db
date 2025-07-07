use crate::error::{Error, Result};
use crate::Database;
use crossbeam::channel::{bounded, Receiver, Sender};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Synchronous write batcher for improving single put performance
pub struct SyncWriteBatcher {
    sender: Sender<BatchCommand>,
    shutdown: Arc<AtomicBool>,
    stats: Arc<BatcherStats>,
}

enum BatchCommand {
    Write {
        key: Vec<u8>,
        value: Vec<u8>,
        result_sender: Sender<Result<()>>,
    },
    Flush,
    Shutdown,
}

struct BatcherStats {
    writes_queued: AtomicU64,
    writes_completed: AtomicU64,
    batches_flushed: AtomicU64,
}

impl SyncWriteBatcher {
    pub fn new(db: Arc<Database>, batch_size: usize, max_delay_ms: u64) -> Arc<Self> {
        let (sender, receiver) = bounded(batch_size * 10); // Buffer up to 10 batches
        let shutdown = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(BatcherStats {
            writes_queued: AtomicU64::new(0),
            writes_completed: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
        });

        // Start worker thread
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_stats = Arc::clone(&stats);
        thread::spawn(move || {
            BatchWorker::new(
                db,
                receiver,
                batch_size,
                max_delay_ms,
                worker_shutdown,
                worker_stats,
            )
            .run();
        });

        Arc::new(Self {
            sender,
            shutdown,
            stats,
        })
    }

    /// Queue a write operation
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(Error::Generic("Write batcher is shutting down".to_string()));
        }

        let (result_sender, result_receiver) = bounded(1);

        self.sender
            .send(BatchCommand::Write {
                key,
                value,
                result_sender,
            })
            .map_err(|_| Error::Generic("Write batcher channel closed".to_string()))?;

        self.stats.writes_queued.fetch_add(1, Ordering::Relaxed);

        // Wait for result
        result_receiver
            .recv()
            .map_err(|_| Error::Generic("Failed to receive write result".to_string()))?
    }

    /// Force flush pending writes
    pub fn flush(&self) -> Result<()> {
        self.sender
            .send(BatchCommand::Flush)
            .map_err(|_| Error::Generic("Write batcher channel closed".to_string()))
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.sender.send(BatchCommand::Shutdown);
    }

    pub fn get_stats(&self) -> (u64, u64, u64) {
        (
            self.stats.writes_queued.load(Ordering::Relaxed),
            self.stats.writes_completed.load(Ordering::Relaxed),
            self.stats.batches_flushed.load(Ordering::Relaxed),
        )
    }
}

struct BatchWorker {
    db: Arc<Database>,
    receiver: Receiver<BatchCommand>,
    batch_size: usize,
    max_delay: Duration,
    pending: Vec<(Vec<u8>, Vec<u8>, Sender<Result<()>>)>,
    last_flush: Instant,
    shutdown: Arc<AtomicBool>,
    stats: Arc<BatcherStats>,
}

impl BatchWorker {
    fn new(
        db: Arc<Database>,
        receiver: Receiver<BatchCommand>,
        batch_size: usize,
        max_delay_ms: u64,
        shutdown: Arc<AtomicBool>,
        stats: Arc<BatcherStats>,
    ) -> Self {
        Self {
            db,
            receiver,
            batch_size,
            max_delay: Duration::from_millis(max_delay_ms),
            pending: Vec::with_capacity(batch_size),
            last_flush: Instant::now(),
            shutdown,
            stats,
        }
    }

    fn run(mut self) {
        while !self.shutdown.load(Ordering::Relaxed) {
            // Check if we should flush based on time
            if !self.pending.is_empty() && self.last_flush.elapsed() >= self.max_delay {
                self.flush_batch();
            }

            // Try to receive with timeout
            match self.receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(BatchCommand::Write {
                    key,
                    value,
                    result_sender,
                }) => {
                    self.pending.push((key, value, result_sender));

                    // Flush if batch is full
                    if self.pending.len() >= self.batch_size {
                        self.flush_batch();
                    }
                }
                Ok(BatchCommand::Flush) => {
                    self.flush_batch();
                }
                Ok(BatchCommand::Shutdown) => {
                    self.flush_batch();
                    break;
                }
                Err(_) => {
                    // Timeout - check if we need to flush based on time
                    continue;
                }
            }
        }

        // Final flush
        self.flush_batch();
    }

    fn flush_batch(&mut self) {
        if self.pending.is_empty() {
            return;
        }

        // Execute batch transaction
        let result = self.execute_batch();

        // Send results to all waiters
        for (_, _, sender) in self.pending.drain(..) {
            // Clone the error if needed, otherwise send Ok
            let send_result = match &result {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Generic(format!("Batch write failed: {}", e))),
            };
            let _ = sender.send(send_result);
        }

        self.last_flush = Instant::now();
        self.stats.batches_flushed.fetch_add(1, Ordering::Relaxed);
    }

    fn execute_batch(&mut self) -> Result<()> {
        let tx_id = self.db.begin_transaction()?;

        for (key, value, _) in &self.pending {
            self.db.put_tx(tx_id, key, value)?;
        }

        let count = self.pending.len() as u64;
        self.db.commit_transaction(tx_id)?;

        self.stats
            .writes_completed
            .fetch_add(count, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for SyncWriteBatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}
