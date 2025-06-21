use crate::async_storage::{AsyncWAL, AsyncIOConfig};
use crate::error::{Error, Result};
use crate::wal::WALOperation;
use async_trait::async_trait;
use tokio::sync::RwLock;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;

/// Async Write-Ahead Log implementation
pub struct AsyncWriteAheadLog {
    file: Arc<RwLock<Option<File>>>,
    _file_path: std::path::PathBuf,
    sequence_number: AtomicU64,
    config: AsyncIOConfig,
    batch_sender: Option<mpsc::Sender<WALRequest>>,
}

#[derive(Debug)]
struct WALRequest {
    operation: WALOperation,
    sequence: u64,
    response: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
struct _WALBatch {
    operations: Vec<(WALOperation, u64)>,
    responses: Vec<oneshot::Sender<Result<()>>>,
}

impl AsyncWriteAheadLog {
    /// Create a new async WAL
    pub async fn create<P: AsRef<Path>>(path: P, config: AsyncIOConfig) -> Result<Arc<Self>> {
        let file_path = path.as_ref().to_path_buf();
        
        let file = File::create(&file_path).await?;
        
        let mut wal = Self {
            file: Arc::new(RwLock::new(Some(file))),
            _file_path: file_path,
            sequence_number: AtomicU64::new(0),
            config: config.clone(),
            batch_sender: None,
        };
        
        // Start batching if enabled
        if config.enable_write_coalescing {
            wal.start_batching().await;
        }
        
        Ok(Arc::new(wal))
    }
    
    /// Open an existing async WAL
    pub async fn open<P: AsRef<Path>>(path: P, config: AsyncIOConfig) -> Result<Arc<Self>> {
        let file_path = path.as_ref().to_path_buf();
        
        let file = File::options()
            .read(true)
            .write(true)
            .append(true)
            .open(&file_path)
            .await
            ?;
        
        // TODO: Read the last sequence number from the file
        let sequence_number = 0;
        
        let mut wal = Self {
            file: Arc::new(RwLock::new(Some(file))),
            _file_path: file_path,
            sequence_number: AtomicU64::new(sequence_number),
            config: config.clone(),
            batch_sender: None,
        };
        
        // Start batching if enabled
        if config.enable_write_coalescing {
            wal.start_batching().await;
        }
        
        Ok(Arc::new(wal))
    }
    
    /// Start the WAL batching background task
    async fn start_batching(&mut self) {
        let (tx, mut rx) = mpsc::channel::<WALRequest>(self.config.buffer_size);
        self.batch_sender = Some(tx);
        
        let file = self.file.clone();
        let config = self.config.clone();
        
        tokio::spawn(async move {
            let mut pending_requests = Vec::new();
            let mut interval = interval(Duration::from_millis(config.write_coalescing_window_ms));
            
            loop {
                tokio::select! {
                    // Collect requests
                    request = rx.recv() => {
                        match request {
                            Some(req) => {
                                pending_requests.push(req);
                                
                                // Flush if batch is full
                                if pending_requests.len() >= config.buffer_size {
                                    Self::flush_batch(&file, &mut pending_requests).await;
                                }
                            }
                            None => {
                                // Channel closed, flush remaining and exit
                                if !pending_requests.is_empty() {
                                    Self::flush_batch(&file, &mut pending_requests).await;
                                }
                                break;
                            }
                        }
                    }
                    
                    // Periodic flush
                    _ = interval.tick() => {
                        if !pending_requests.is_empty() {
                            Self::flush_batch(&file, &mut pending_requests).await;
                        }
                    }
                }
            }
        });
    }
    
    /// Flush a batch of WAL requests
    async fn flush_batch(
        file: &Arc<RwLock<Option<File>>>,
        requests: &mut Vec<WALRequest>,
    ) {
        if requests.is_empty() {
            return;
        }
        
        let result = {
            let mut file_guard = file.write().await;
            if let Some(ref mut file) = *file_guard {
                let mut success = true;
                
                // Write all operations in sequence
                for request in requests.iter() {
                    let serialized = Self::serialize_operation(&request.operation, request.sequence);
                    if let Err(_) = file.write_all(&serialized).await {
                        success = false;
                        break;
                    }
                }
                
                // Sync to disk
                if success {
                    if let Err(_) = file.sync_data().await {
                        success = false;
                    }
                }
                
                if success { 
                    Ok(()) 
                } else { 
                    Err(Error::Io("Write failed".to_string())) 
                }
            } else {
                Err(Error::Generic("WAL file not open".to_string()))
            }
        };
        
        // Send responses
        for request in requests.drain(..) {
            let _ = request.response.send(result.clone());
        }
    }
    
    /// Serialize a WAL operation with sequence number
    fn serialize_operation(operation: &WALOperation, sequence: u64) -> Vec<u8> {
        let mut data = Vec::new();
        
        // Write sequence number (8 bytes)
        data.extend_from_slice(&sequence.to_le_bytes());
        
        // Write operation type and data
        match operation {
            WALOperation::Put { key, value } => {
                data.push(1); // Put operation type
                data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                data.extend_from_slice(key);
                data.extend_from_slice(&(value.len() as u32).to_le_bytes());
                data.extend_from_slice(value);
            }
            WALOperation::Delete { key } => {
                data.push(2); // Delete operation type
                data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                data.extend_from_slice(key);
            }
            WALOperation::TransactionBegin { tx_id } => {
                data.push(3); // Begin transaction type
                data.extend_from_slice(&tx_id.to_le_bytes());
            }
            WALOperation::TransactionCommit { tx_id } => {
                data.push(4); // Commit transaction type
                data.extend_from_slice(&tx_id.to_le_bytes());
            }
            WALOperation::TransactionAbort { tx_id } => {
                data.push(5); // Abort transaction type
                data.extend_from_slice(&tx_id.to_le_bytes());
            }
            WALOperation::Checkpoint { lsn } => {
                data.push(6); // Checkpoint type
                data.extend_from_slice(&lsn.to_le_bytes());
            }
            WALOperation::BeginTransaction { tx_id } => {
                data.push(7); // Begin transaction type (alternate)
                data.extend_from_slice(&tx_id.to_le_bytes());
            }
            WALOperation::CommitTransaction { tx_id } => {
                data.push(8); // Commit transaction type (alternate)
                data.extend_from_slice(&tx_id.to_le_bytes());
            }
            WALOperation::AbortTransaction { tx_id } => {
                data.push(9); // Abort transaction type (alternate)
                data.extend_from_slice(&tx_id.to_le_bytes());
            }
        }
        
        // Add checksum (simple CRC32)
        let checksum = crc32fast::hash(&data);
        data.extend_from_slice(&checksum.to_le_bytes());
        
        data
    }
    
    /// Deserialize a WAL operation
    fn deserialize_operation(data: &[u8]) -> Result<(WALOperation, u64)> {
        if data.len() < 13 { // Minimum: 8 bytes seq + 1 byte type + 4 bytes checksum
            return Err(Error::Generic(format!("Invalid WAL entry: too short ({})", data.len())));
        }
        
        // Verify checksum
        let payload_len = data.len() - 4;
        let payload = &data[..payload_len];
        let stored_checksum = u32::from_le_bytes([
            data[payload_len], data[payload_len + 1], 
            data[payload_len + 2], data[payload_len + 3]
        ]);
        let computed_checksum = crc32fast::hash(payload);
        
        if stored_checksum != computed_checksum {
            return Err(Error::Generic("WAL entry checksum mismatch".to_string()));
        }
        
        // Parse sequence number
        let sequence = u64::from_le_bytes([
            data[0], data[1], data[2], data[3],
            data[4], data[5], data[6], data[7],
        ]);
        
        // Parse operation
        let op_type = data[8];
        let mut offset = 9;
        
        let operation = match op_type {
            1 => { // Put
                if offset + 4 > data.len() {
                    return Err(Error::Generic("Invalid Put: insufficient key length bytes".to_string()));
                }
                let key_len = u32::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
                ]) as usize;
                offset += 4;
                
                if offset + key_len > data.len() {
                    return Err(Error::Generic("Invalid Put: insufficient key bytes".to_string()));
                }
                let key = data[offset..offset + key_len].to_vec();
                offset += key_len;
                
                if offset + 4 > data.len() {
                    return Err(Error::Generic("Invalid Put: insufficient value length bytes".to_string()));
                }
                let value_len = u32::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
                ]) as usize;
                offset += 4;
                
                if offset + value_len > data.len() {
                    return Err(Error::Generic("Invalid Put: insufficient value bytes".to_string()));
                }
                let value = data[offset..offset + value_len].to_vec();
                
                WALOperation::Put { key, value }
            }
            2 => { // Delete
                if offset + 4 > data.len() {
                    return Err(Error::Generic("Invalid Delete: insufficient key length bytes".to_string()));
                }
                let key_len = u32::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
                ]) as usize;
                offset += 4;
                
                if offset + key_len > data.len() {
                    return Err(Error::Generic("Invalid Delete: insufficient key bytes".to_string()));
                }
                let key = data[offset..offset + key_len].to_vec();
                
                WALOperation::Delete { key }
            }
            3 => { // Begin transaction
                let tx_id = u64::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                    data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
                ]);
                
                WALOperation::TransactionBegin { tx_id }
            }
            4 => { // Commit transaction
                let tx_id = u64::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                    data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
                ]);
                
                WALOperation::TransactionCommit { tx_id }
            }
            5 => { // Abort transaction
                let tx_id = u64::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                    data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
                ]);
                
                WALOperation::TransactionAbort { tx_id }
            }
            6 => { // Checkpoint
                let lsn = u64::from_le_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                    data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
                ]);
                
                WALOperation::Checkpoint { lsn }
            }
            _ => return Err(Error::Generic("Unknown WAL operation type".to_string())),
        };
        
        Ok((operation, sequence))
    }
    
    /// Deserialize a WAL operation and return its size
    fn deserialize_operation_with_size(data: &[u8]) -> Result<(WALOperation, u64, usize)> {
        let (operation, sequence) = Self::deserialize_operation(data)?;
        
        // Calculate the actual size of this entry
        let mut size = 8 + 1 + 4; // sequence + type + checksum
        
        match &operation {
            WALOperation::Put { key, value } => {
                size += 4 + key.len() + 4 + value.len();
            }
            WALOperation::Delete { key } => {
                size += 4 + key.len();
            }
            WALOperation::TransactionBegin { .. } |
            WALOperation::TransactionCommit { .. } |
            WALOperation::TransactionAbort { .. } |
            WALOperation::Checkpoint { .. } |
            WALOperation::BeginTransaction { .. } |
            WALOperation::CommitTransaction { .. } |
            WALOperation::AbortTransaction { .. } => {
                size += 8; // tx_id or lsn
            }
        }
        
        Ok((operation, sequence, size))
    }
}

#[async_trait]
impl AsyncWAL for AsyncWriteAheadLog {
    async fn append(&self, operation: WALOperation) -> Result<()> {
        let sequence = self.sequence_number.fetch_add(1, Ordering::SeqCst);
        
        // Use batching if available
        if let Some(ref sender) = self.batch_sender {
            let (tx, rx) = oneshot::channel();
            let request = WALRequest {
                operation,
                sequence,
                response: tx,
            };
            
            sender.send(request).await.map_err(|_| 
                Error::Generic("WAL batch channel closed".to_string()))?;
            
            rx.await.map_err(|_| 
                Error::Generic("WAL response channel closed".to_string()))?
        } else {
            // Direct write
            let serialized = Self::serialize_operation(&operation, sequence);
            
            let mut file_guard = self.file.write().await;
            if let Some(ref mut file) = *file_guard {
                file.write_all(&serialized).await?;
                file.sync_data().await?;
            } else {
                return Err(Error::Generic("WAL file not open".to_string()));
            }
            
            Ok(())
        }
    }
    
    async fn sync(&self) -> Result<()> {
        let mut file_guard = self.file.write().await;
        if let Some(ref mut file) = *file_guard {
            file.sync_all().await?;
            Ok(())
        } else {
            Err(Error::Generic("WAL file not open".to_string()))
        }
    }
    
    async fn recover(&self) -> Result<Vec<WALOperation>> {
        let mut operations = Vec::new();
        
        let mut file_guard = self.file.write().await;
        if let Some(ref mut file) = *file_guard {
            file.seek(SeekFrom::Start(0)).await?;
            
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).await?;
            
            // If the file is empty or too small, return empty operations
            if buffer.len() < 13 {
                return Ok(operations);
            }
            
            // Parse operations from buffer
            let mut offset = 0;
            while offset < buffer.len() {
                // Need at least 13 bytes for minimum entry
                if offset + 13 > buffer.len() {
                    break;
                }
                
                // Try to parse the operation and get its actual size
                match Self::deserialize_operation_with_size(&buffer[offset..]) {
                    Ok((operation, _sequence, size)) => {
                        operations.push(operation);
                        offset += size;
                    }
                    Err(_) => {
                        // Skip to next potential entry
                        offset += 1;
                    }
                }
            }
        }
        
        Ok(operations)
    }
    
    async fn checkpoint(&self, _operations_count: usize) -> Result<()> {
        // Mark operations as persisted - simplified implementation
        // In practice, you'd track which operations have been persisted to main storage
        Ok(())
    }
}