use crate::error::{Error, Result};
use crate::wal::WALOperation;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A batch of write operations to be applied atomically
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
    size_bytes: usize,
    max_size: usize,
}

#[derive(Clone, Debug)]
pub enum BatchOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl BatchOperation {
    fn size_bytes(&self) -> usize {
        match self {
            BatchOperation::Put { key, value } => key.len() + value.len() + 16, // overhead
            BatchOperation::Delete { key } => key.len() + 16,
        }
    }

    fn to_wal_operation(&self) -> WALOperation {
        match self {
            BatchOperation::Put { key, value } => WALOperation::Put {
                key: key.clone(),
                value: value.clone(),
            },
            BatchOperation::Delete { key } => WALOperation::Delete { key: key.clone() },
        }
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBatch {
    /// Create a new write batch with default size limit (1MB)
    pub fn new() -> Self {
        Self::with_capacity(1024 * 1024) // 1MB default
    }

    /// Create a new write batch with specified size limit
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            operations: Vec::new(),
            size_bytes: 0,
            max_size,
        }
    }

    /// Add a put operation to the batch
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let op = BatchOperation::Put {
            key: key.clone(),
            value: value.clone(),
        };

        let op_size = op.size_bytes();
        if self.size_bytes + op_size > self.max_size {
            return Err(Error::Generic(format!(
                "Write batch size limit exceeded: {} + {} > {}",
                self.size_bytes, op_size, self.max_size
            )));
        }

        self.operations.push(op);
        self.size_bytes += op_size;
        Ok(())
    }

    /// Add a delete operation to the batch
    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        let op = BatchOperation::Delete { key: key.clone() };

        let op_size = op.size_bytes();
        if self.size_bytes + op_size > self.max_size {
            return Err(Error::Generic(format!(
                "Write batch size limit exceeded: {} + {} > {}",
                self.size_bytes, op_size, self.max_size
            )));
        }

        self.operations.push(op);
        self.size_bytes += op_size;
        Ok(())
    }

    /// Clear all operations from the batch
    pub fn clear(&mut self) {
        self.operations.clear();
        self.size_bytes = 0;
    }

    /// Get the number of operations in the batch
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Get the total size of the batch in bytes
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Get the operations in the batch
    pub fn operations(&self) -> &[BatchOperation] {
        &self.operations
    }

    /// Convert all operations to WAL operations
    pub fn to_wal_operations(&self) -> Vec<WALOperation> {
        self.operations
            .iter()
            .map(|op| op.to_wal_operation())
            .collect()
    }
}

/// Automatic write batcher that groups writes together
pub struct AutoBatcher {
    pending: Arc<Mutex<WriteBatch>>,
    flush_interval: Duration,
    flush_size: usize,
    last_flush: Arc<Mutex<Instant>>,
}

impl AutoBatcher {
    pub fn new(flush_interval: Duration, flush_size: usize) -> Self {
        Self {
            pending: Arc::new(Mutex::new(WriteBatch::with_capacity(flush_size * 2))),
            flush_interval,
            flush_size,
            last_flush: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Add an operation to the batch
    pub fn add_operation(&self, op: BatchOperation) -> Result<bool> {
        let mut batch = self.pending.lock();

        match &op {
            BatchOperation::Put { key, value } => batch.put(key.clone(), value.clone())?,
            BatchOperation::Delete { key } => batch.delete(key.clone())?,
        }

        // Check if we should flush
        let should_flush = batch.size_bytes() >= self.flush_size
            || self.last_flush.lock().elapsed() >= self.flush_interval;

        Ok(should_flush)
    }

    /// Take the current batch and reset
    pub fn take_batch(&self) -> WriteBatch {
        let mut batch = self.pending.lock();
        let mut new_batch = WriteBatch::with_capacity(self.flush_size * 2);
        std::mem::swap(&mut *batch, &mut new_batch);
        *self.last_flush.lock() = Instant::now();
        new_batch
    }

    /// Get the current batch size
    pub fn current_size(&self) -> usize {
        self.pending.lock().size_bytes()
    }

    /// Check if it's time to flush based on time
    pub fn should_flush_by_time(&self) -> bool {
        self.last_flush.lock().elapsed() >= self.flush_interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batch_basic() {
        let mut batch = WriteBatch::new();

        batch.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        batch.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        batch.delete(b"key3".to_vec()).unwrap();

        assert_eq!(batch.len(), 3);
        assert!(batch.size_bytes() > 0);

        let ops = batch.operations();
        assert_eq!(ops.len(), 3);

        match &ops[0] {
            BatchOperation::Put { key, value } => {
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
            }
            _ => panic!("Expected Put operation"),
        }

        match &ops[2] {
            BatchOperation::Delete { key } => {
                assert_eq!(key, b"key3");
            }
            _ => panic!("Expected Delete operation"),
        }
    }

    #[test]
    fn test_write_batch_size_limit() {
        let mut batch = WriteBatch::with_capacity(100);

        // This should succeed
        batch.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();

        // This should fail due to size limit
        let large_value = vec![0u8; 200];
        let result = batch.put(b"key2".to_vec(), large_value);
        assert!(result.is_err());
    }

    #[test]
    fn test_auto_batcher() {
        let batcher = AutoBatcher::new(Duration::from_millis(100), 1000);

        // Add operations
        let op1 = BatchOperation::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        };
        let should_flush = batcher.add_operation(op1).unwrap();
        assert!(!should_flush); // Shouldn't flush yet

        // Take batch
        let batch = batcher.take_batch();
        assert_eq!(batch.len(), 1);

        // Batch should be empty now
        assert_eq!(batcher.current_size(), 0);
    }
}
