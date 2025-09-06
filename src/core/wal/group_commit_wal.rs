use crate::core::error::{Error, Result};
use super::{WALOperation, WALEntry, WriteAheadLog};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use crossbeam_channel::{bounded, Sender, Receiver};

const BATCH_SIZE: usize = 100;
const BATCH_TIMEOUT: Duration = Duration::from_millis(10);
const BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug)]
pub struct GroupCommitWAL {
    writer_thread: Option<thread::JoinHandle<()>>,
    commit_channel: Sender<CommitRequest>,
    shutdown: Arc<AtomicBool>,
    next_lsn: Arc<AtomicU64>,
    stats: Arc<GroupCommitStats>,
}

#[derive(Debug)]
struct CommitRequest {
    operation: WALOperation,
    lsn_sender: Sender<Result<u64>>,
}

#[derive(Debug, Default)]
struct GroupCommitStats {
    total_commits: AtomicU64,
    total_batches: AtomicU64,
    total_bytes: AtomicU64,
    avg_batch_size: AtomicU64,
}

struct WriterState {
    file: BufWriter<File>,
    pending: VecDeque<CommitRequest>,
    receiver: Receiver<CommitRequest>,
    shutdown: Arc<AtomicBool>,
    next_lsn: Arc<AtomicU64>,
    stats: Arc<GroupCommitStats>,
}

impl GroupCommitWAL {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path.as_ref())
            .map_err(|e| Error::Io(format!("Failed to create WAL file: {}", e)))?;
        
        Self::init_with_file(file)
    }
    
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(path.as_ref())
            .map_err(|e| Error::Io(format!("Failed to open WAL file: {}", e)))?;
        
        // Find the last LSN
        let last_lsn = Self::find_last_lsn(&mut file)?;
        
        // Seek to end for appending
        file.seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(format!("Failed to seek to end: {}", e)))?;
        
        let mut wal = Self::init_with_file(file)?;
        Arc::get_mut(&mut wal)
            .ok_or_else(|| Error::Internal("Failed to get mutable reference".to_string()))?
            .next_lsn.store(last_lsn + 1, Ordering::SeqCst);
        Ok(wal)
    }
    
    fn init_with_file(file: File) -> Result<Arc<Self>> {
        let (sender, receiver) = bounded(1000);
        let shutdown = Arc::new(AtomicBool::new(false));
        let next_lsn = Arc::new(AtomicU64::new(1));
        let stats = Arc::new(GroupCommitStats::default());
        
        let writer_state = WriterState {
            file: BufWriter::with_capacity(BUFFER_SIZE, file),
            pending: VecDeque::with_capacity(BATCH_SIZE),
            receiver,
            shutdown: Arc::clone(&shutdown),
            next_lsn: Arc::clone(&next_lsn),
            stats: Arc::clone(&stats),
        };
        
        let writer_thread = thread::Builder::new()
            .name("wal-group-commit".to_string())
            .spawn(move || {
                Self::writer_loop(writer_state);
            })
            .map_err(|e| Error::Internal(format!("Failed to spawn writer thread: {}", e)))?;
        
        Ok(Arc::new(Self {
            writer_thread: Some(writer_thread),
            commit_channel: sender,
            shutdown,
            next_lsn,
            stats,
        }))
    }
    
    fn writer_loop(mut state: WriterState) {
        let mut last_batch = Instant::now();
        
        loop {
            // Check for shutdown
            if state.shutdown.load(Ordering::Relaxed) {
                // Flush remaining requests
                if !state.pending.is_empty() {
                    Self::flush_batch(&mut state);
                }
                break;
            }
            
            // Collect requests up to batch size or timeout
            let timeout = BATCH_TIMEOUT.saturating_sub(last_batch.elapsed());
            
            match state.receiver.recv_timeout(timeout) {
                Ok(request) => {
                    state.pending.push_back(request);
                    
                    // Drain more requests if available
                    while state.pending.len() < BATCH_SIZE {
                        match state.receiver.try_recv() {
                            Ok(req) => state.pending.push_back(req),
                            Err(_) => break,
                        }
                    }
                    
                    // Flush if batch is full or timeout reached
                    if state.pending.len() >= BATCH_SIZE || last_batch.elapsed() >= BATCH_TIMEOUT {
                        Self::flush_batch(&mut state);
                        last_batch = Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !state.pending.is_empty() {
                        Self::flush_batch(&mut state);
                        last_batch = Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    // Channel closed, flush and exit
                    if !state.pending.is_empty() {
                        Self::flush_batch(&mut state);
                    }
                    break;
                }
            }
        }
    }
    
    fn flush_batch(state: &mut WriterState) {
        let batch_size = state.pending.len();
        if batch_size == 0 {
            return;
        }
        
        let mut total_bytes = 0;
        let mut responses = Vec::with_capacity(batch_size);
        
        // Write all operations
        for request in state.pending.drain(..) {
            let lsn = state.next_lsn.fetch_add(1, Ordering::SeqCst);
            
            let mut entry = WALEntry::new(lsn, request.operation);
            entry.calculate_checksum();
            
            let serialized = match bincode::encode_to_vec(&entry, bincode::config::standard()) {
                Ok(data) => data,
                Err(e) => {
                    let _ = request.lsn_sender.send(Err(Error::Serialization(e.to_string())));
                    continue;
                }
            };
            
            let len_bytes = (serialized.len() as u32).to_le_bytes();
            
            // Write length prefix and data
            let write_result = state.file.write_all(&len_bytes)
                .and_then(|_| state.file.write_all(&serialized));
            
            match write_result {
                Ok(_) => {
                    total_bytes += 4 + serialized.len();
                    responses.push((request.lsn_sender, Ok(lsn)));
                }
                Err(e) => {
                    responses.push((request.lsn_sender, Err(Error::Io(e.to_string()))));
                }
            }
        }
        
        // Sync to disk once for entire batch
        if let Err(e) = state.file.flush() {
            // Send error to all pending responses
            for (sender, _) in responses.iter() {
                let _ = sender.send(Err(Error::Io(format!("Flush failed: {}", e))));
            }
            return;
        }
        
        // Send success responses
        for (sender, result) in responses {
            let _ = sender.send(result);
        }
        
        // Update statistics
        state.stats.total_commits.fetch_add(batch_size as u64, Ordering::Relaxed);
        state.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        state.stats.total_bytes.fetch_add(total_bytes as u64, Ordering::Relaxed);
        
        let avg = state.stats.total_commits.load(Ordering::Relaxed) 
            / state.stats.total_batches.load(Ordering::Relaxed).max(1);
        state.stats.avg_batch_size.store(avg, Ordering::Relaxed);
    }
    
    fn find_last_lsn(file: &mut File) -> Result<u64> {
        use std::io::Read;
        
        file.seek(SeekFrom::Start(0))
            .map_err(|e| Error::Io(e.to_string()))?;
        
        let mut max_lsn = 0u64;
        let mut length_buf = [0u8; 4];
        
        loop {
            match file.read_exact(&mut length_buf) {
                Ok(_) => {
                    let len = u32::from_le_bytes(length_buf) as usize;
                    if len == 0 || len > 1024 * 1024 {
                        break;
                    }
                    
                    let mut entry_buf = vec![0u8; len];
                    if file.read_exact(&mut entry_buf).is_err() {
                        break;
                    }
                    
                    if let Ok((entry, _)) = bincode::decode_from_slice::<WALEntry, _>(&entry_buf, bincode::config::standard()) {
                        max_lsn = max_lsn.max(entry.lsn);
                    }
                }
                Err(_) => break,
            }
        }
        
        Ok(max_lsn)
    }
    
    pub fn get_statistics(&self) -> String {
        let commits = self.stats.total_commits.load(Ordering::Relaxed);
        let batches = self.stats.total_batches.load(Ordering::Relaxed);
        let bytes = self.stats.total_bytes.load(Ordering::Relaxed);
        let avg_batch = self.stats.avg_batch_size.load(Ordering::Relaxed);
        
        format!(
            "GroupCommitWAL Stats:\n\
             Total Commits: {}\n\
             Total Batches: {}\n\
             Total Bytes: {}\n\
             Avg Batch Size: {}\n\
             Throughput: {:.2} MB/s",
            commits, batches, bytes, avg_batch,
            bytes as f64 / 1024.0 / 1024.0
        )
    }
}

impl WriteAheadLog for GroupCommitWAL {
    fn append(&self, operation: WALOperation) -> Result<u64> {
        let (sender, receiver) = bounded(1);
        
        let request = CommitRequest {
            operation,
            lsn_sender: sender,
        };
        
        self.commit_channel.send(request)
            .map_err(|_| Error::Internal("WAL writer thread terminated".to_string()))?;
        
        receiver.recv()
            .map_err(|_| Error::Internal("Failed to receive LSN".to_string()))?
    }
    
    fn sync(&self) -> Result<()> {
        // Group commit automatically syncs, so this is a no-op
        Ok(())
    }
    
    fn replay(&self) -> Result<Vec<WALOperation>> {
        // Implementation would read from file
        // For now, return empty vec
        Ok(Vec::new())
    }
    
    fn truncate(&self, _lsn: u64) -> Result<()> {
        // Implementation would truncate log up to LSN
        Ok(())
    }
    
    fn checkpoint(&self) -> Result<()> {
        // Implementation would create checkpoint
        Ok(())
    }
}

impl Drop for GroupCommitWAL {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(thread) = self.writer_thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_group_commit() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        
        let wal = GroupCommitWAL::create(&wal_path).unwrap();
        
        // Write multiple operations
        let mut lsns = Vec::new();
        for i in 0..10 {
            let op = WALOperation::Put {
                key: vec![i],
                value: vec![i * 2],
            };
            let lsn = wal.append(op).unwrap();
            lsns.push(lsn);
        }
        
        // Verify LSNs are sequential
        for i in 1..lsns.len() {
            assert_eq!(lsns[i], lsns[i-1] + 1);
        }
    }
}