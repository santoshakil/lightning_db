use crate::core::error::{Error, Result};
use crate::performance::memory::thread_local_pools::{with_vec_buffer, with_large_buffer};
use bytes::Bytes;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::collections::VecDeque;

/// Zero-copy write batch that eliminates all unnecessary allocations
/// Uses memory pools and direct memory access for maximum performance
pub struct ZeroCopyWriteBatch {
    // Ring buffer of operation entries for zero-copy access
    operations: Vec<OperationEntry>,
    // Pre-allocated buffer for serialization
    buffer: Vec<u8>,
    // Current operation count
    count: usize,
    // Total size tracking
    total_size: usize,
    // Maximum capacity
    capacity: usize,
}

/// Operation entry that avoids cloning data
#[derive(Debug)]
struct OperationEntry {
    op_type: OperationType,
    key_ptr: NonNull<u8>,
    key_len: usize,
    value_ptr: Option<NonNull<u8>>,
    value_len: usize,
    // Safety: We ensure these pointers remain valid during batch lifetime
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationType {
    Put = 1,
    Delete = 2,
}

unsafe impl Send for OperationEntry {}
unsafe impl Sync for OperationEntry {}

impl ZeroCopyWriteBatch {
    /// Create a new zero-copy write batch
    pub fn new(capacity: usize) -> Self {
        Self {
            operations: Vec::with_capacity(capacity),
            buffer: Vec::with_capacity(64 * 1024), // 64KB initial buffer
            count: 0,
            total_size: 0,
            capacity,
        }
    }

    /// Add a put operation using zero-copy semantics
    /// The key and value must remain valid for the lifetime of the batch
    pub unsafe fn put_zero_copy(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.count >= self.capacity {
            return Err(Error::Generic("Batch capacity exceeded".into()));
        }

        let entry = OperationEntry {
            op_type: OperationType::Put,
            key_ptr: NonNull::new_unchecked(key.as_ptr() as *mut u8),
            key_len: key.len(),
            value_ptr: Some(NonNull::new_unchecked(value.as_ptr() as *mut u8)),
            value_len: value.len(),
        };

        self.operations.push(entry);
        self.count += 1;
        self.total_size += key.len() + value.len() + 16; // Add overhead
        Ok(())
    }

    /// Add a delete operation using zero-copy semantics
    pub unsafe fn delete_zero_copy(&mut self, key: &[u8]) -> Result<()> {
        if self.count >= self.capacity {
            return Err(Error::Generic("Batch capacity exceeded".into()));
        }

        let entry = OperationEntry {
            op_type: OperationType::Delete,
            key_ptr: NonNull::new_unchecked(key.as_ptr() as *mut u8),
            key_len: key.len(),
            value_ptr: None,
            value_len: 0,
        };

        self.operations.push(entry);
        self.count += 1;
        self.total_size += key.len() + 16;
        Ok(())
    }

    /// Serialize the batch to a buffer without allocations
    pub fn serialize_to_buffer(&mut self) -> &[u8] {
        self.buffer.clear();
        
        // Reserve space for header
        self.buffer.extend_from_slice(&(self.count as u32).to_le_bytes());
        self.buffer.extend_from_slice(&(self.total_size as u32).to_le_bytes());

        // Serialize operations
        for entry in &self.operations {
            // Operation type
            self.buffer.push(entry.op_type as u8);
            
            // Key length and data
            self.buffer.extend_from_slice(&(entry.key_len as u32).to_le_bytes());
            // SAFETY: Creating slice from zero-copy key pointer
            // Invariants:
            // 1. key_ptr is valid NonNull pointer from caller
            // 2. key_len matches actual key size
            // 3. Key data remains valid during serialization
            // 4. Caller ensures no concurrent modification
            // Guarantees:
            // - Key data copied to buffer
            // - No use-after-free as data copied immediately
            unsafe {
                let key_slice = std::slice::from_raw_parts(entry.key_ptr.as_ptr(), entry.key_len);
                self.buffer.extend_from_slice(key_slice);
            }

            // Value (if present)
            if let Some(value_ptr) = entry.value_ptr {
                self.buffer.extend_from_slice(&(entry.value_len as u32).to_le_bytes());
                // SAFETY: Creating slice from zero-copy value pointer
                // Invariants:
                // 1. value_ptr is valid NonNull pointer from caller
                // 2. value_len matches actual value size
                // 3. Value data remains valid during serialization
                // 4. Caller ensures no concurrent modification
                // Guarantees:
                // - Value data copied to buffer
                // - Safe serialization without lifetime issues
                unsafe {
                    let value_slice = std::slice::from_raw_parts(value_ptr.as_ptr(), entry.value_len);
                    self.buffer.extend_from_slice(value_slice);
                }
            } else {
                self.buffer.extend_from_slice(&0u32.to_le_bytes());
            }
        }

        &self.buffer
    }

    /// Execute operations with zero-copy access
    pub fn execute_zero_copy<F>(&self, mut executor: F) -> Result<()>
    where
        F: FnMut(OperationType, &[u8], Option<&[u8]>) -> Result<()>,
    {
        for entry in &self.operations {
            // SAFETY: Creating slices for zero-copy execution
            // Invariants:
            // 1. All pointers in operations are valid
            // 2. Lengths match actual data sizes
            // 3. Data remains valid during execution
            // 4. Executor receives borrowed slices only
            // Guarantees:
            // - Safe read-only access to data
            // - No ownership transfer
            // - Lifetime limited to executor call
            unsafe {
                let key = std::slice::from_raw_parts(entry.key_ptr.as_ptr(), entry.key_len);
                let value = entry.value_ptr.map(|ptr| {
                    std::slice::from_raw_parts(ptr.as_ptr(), entry.value_len)
                });
                
                executor(entry.op_type, key, value)?;
            }
        }
        Ok(())
    }

    /// Clear the batch for reuse
    pub fn clear(&mut self) {
        self.operations.clear();
        self.buffer.clear();
        self.count = 0;
        self.total_size = 0;
    }

    /// Get the number of operations
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the total size in bytes
    pub fn size_bytes(&self) -> usize {
        self.total_size
    }
}

/// Pooled zero-copy write batcher that reuses batches to eliminate allocations
pub struct PooledZeroCopyBatcher {
    // Pool of reusable batches
    batch_pool: VecDeque<ZeroCopyWriteBatch>,
    // Current active batch
    active_batch: Option<ZeroCopyWriteBatch>,
    // Pool configuration
    pool_size: usize,
    batch_capacity: usize,
    flush_threshold: usize,
}

impl PooledZeroCopyBatcher {
    pub fn new(pool_size: usize, batch_capacity: usize, flush_threshold: usize) -> Self {
        let mut batch_pool = VecDeque::with_capacity(pool_size);
        
        // Pre-populate pool with batches
        for _ in 0..pool_size {
            batch_pool.push_back(ZeroCopyWriteBatch::new(batch_capacity));
        }

        Self {
            batch_pool,
            active_batch: None,
            pool_size,
            batch_capacity,
            flush_threshold,
        }
    }

    /// Get or create an active batch
    fn get_active_batch(&mut self) -> &mut ZeroCopyWriteBatch {
        if self.active_batch.is_none() {
            self.active_batch = Some(
                self.batch_pool.pop_front()
                    .unwrap_or_else(|| ZeroCopyWriteBatch::new(self.batch_capacity))
            );
        }
        // SAFETY: We just ensured active_batch is Some above
        self.active_batch.as_mut().unwrap()
    }

    /// Add a put operation to the active batch
    pub unsafe fn put(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let batch = self.get_active_batch();
        batch.put_zero_copy(key, value)?;

        Ok(batch.size_bytes() >= self.flush_threshold)
    }

    /// Add a delete operation to the active batch
    pub unsafe fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let batch = self.get_active_batch();
        batch.delete_zero_copy(key)?;

        Ok(batch.size_bytes() >= self.flush_threshold)
    }

    /// Take the current batch and return it to the pool
    pub fn take_batch(&mut self) -> Option<ZeroCopyWriteBatch> {
        self.active_batch.take()
    }

    /// Return a batch to the pool after use
    pub fn return_batch(&mut self, mut batch: ZeroCopyWriteBatch) {
        batch.clear();
        if self.batch_pool.len() < self.pool_size {
            self.batch_pool.push_back(batch);
        }
        // If pool is full, just drop the batch
    }

    /// Get current batch size
    pub fn current_size(&self) -> usize {
        self.active_batch.as_ref().map_or(0, |b| b.size_bytes())
    }
}

/// Arena-based zero-copy batcher that uses a single large allocation
/// For maximum performance when batch lifetime is predictable
pub struct ArenaZeroCopyBatcher {
    // Large pre-allocated arena
    arena: Vec<u8>,
    // Current offset in arena
    offset: AtomicUsize,
    // Operations metadata
    operations: Vec<ArenaOperation>,
    // Arena size
    arena_size: usize,
}

#[derive(Debug)]
struct ArenaOperation {
    op_type: OperationType,
    key_offset: usize,
    key_len: usize,
    value_offset: usize,
    value_len: usize,
}

impl ArenaZeroCopyBatcher {
    /// Create a new arena-based batcher
    pub fn new(arena_size: usize, max_operations: usize) -> Self {
        Self {
            arena: vec![0; arena_size],
            offset: AtomicUsize::new(0),
            operations: Vec::with_capacity(max_operations),
            arena_size,
        }
    }

    /// Add a put operation by copying data to arena
    pub fn put_arena(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let current_offset = self.offset.load(Ordering::Relaxed);
        let required_space = key.len() + value.len();
        
        if current_offset + required_space > self.arena_size {
            return Err(Error::Generic("Arena space exhausted".into()));
        }

        // Copy key to arena
        let key_offset = current_offset;
        let key_end = key_offset + key.len();
        self.arena[key_offset..key_end].copy_from_slice(key);

        // Copy value to arena
        let value_offset = key_end;
        let value_end = value_offset + value.len();
        self.arena[value_offset..value_end].copy_from_slice(value);

        // Update offset
        self.offset.store(value_end, Ordering::Relaxed);

        // Record operation
        self.operations.push(ArenaOperation {
            op_type: OperationType::Put,
            key_offset,
            key_len: key.len(),
            value_offset,
            value_len: value.len(),
        });

        Ok(())
    }

    /// Add a delete operation
    pub fn delete_arena(&mut self, key: &[u8]) -> Result<()> {
        let current_offset = self.offset.load(Ordering::Relaxed);
        
        if current_offset + key.len() > self.arena_size {
            return Err(Error::Generic("Arena space exhausted".into()));
        }

        // Copy key to arena
        let key_offset = current_offset;
        let key_end = key_offset + key.len();
        self.arena[key_offset..key_end].copy_from_slice(key);

        // Update offset
        self.offset.store(key_end, Ordering::Relaxed);

        // Record operation
        self.operations.push(ArenaOperation {
            op_type: OperationType::Delete,
            key_offset,
            key_len: key.len(),
            value_offset: 0,
            value_len: 0,
        });

        Ok(())
    }

    /// Execute all operations with zero-copy access to arena data
    pub fn execute_arena<F>(&self, mut executor: F) -> Result<()>
    where
        F: FnMut(OperationType, &[u8], Option<&[u8]>) -> Result<()>,
    {
        for op in &self.operations {
            let key = &self.arena[op.key_offset..op.key_offset + op.key_len];
            
            let value = if op.value_len > 0 {
                Some(&self.arena[op.value_offset..op.value_offset + op.value_len])
            } else {
                None
            };

            executor(op.op_type, key, value)?;
        }
        Ok(())
    }

    /// Reset the arena for reuse
    pub fn reset(&mut self) {
        self.offset.store(0, Ordering::Relaxed);
        self.operations.clear();
    }

    /// Get the number of operations
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Get used arena space
    pub fn used_space(&self) -> usize {
        self.offset.load(Ordering::Relaxed)
    }
}

/// Thread-local zero-copy batch manager for maximum performance
thread_local! {
    static THREAD_LOCAL_BATCHER: std::cell::RefCell<PooledZeroCopyBatcher> = 
        std::cell::RefCell::new(PooledZeroCopyBatcher::new(4, 1000, 64 * 1024));
}

/// High-level interface for thread-local zero-copy batching
pub fn with_thread_local_batcher<F, R>(f: F) -> R
where
    F: FnOnce(&mut PooledZeroCopyBatcher) -> R,
{
    THREAD_LOCAL_BATCHER.with(|batcher| {
        let mut batcher = batcher.borrow_mut();
        f(&mut batcher)
    })
}

/// Helper function for zero-copy put operation
pub unsafe fn thread_local_put(key: &[u8], value: &[u8]) -> Result<bool> {
    with_thread_local_batcher(|batcher| batcher.put(key, value))
}

/// Helper function for zero-copy delete operation
pub unsafe fn thread_local_delete(key: &[u8]) -> Result<bool> {
    with_thread_local_batcher(|batcher| batcher.delete(key))
}

/// Take current thread-local batch
pub fn take_thread_local_batch() -> Option<ZeroCopyWriteBatch> {
    with_thread_local_batcher(|batcher| batcher.take_batch())
}

/// Return batch to thread-local pool
pub fn return_thread_local_batch(batch: ZeroCopyWriteBatch) {
    with_thread_local_batcher(|batcher| batcher.return_batch(batch));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_write_batch() {
        let mut batch = ZeroCopyWriteBatch::new(100);

        let key1 = b"key1";
        let value1 = b"value1";
        let key2 = b"key2";

        // SAFETY: Test with static byte slices
        // Invariants:
        // 1. Static strings have 'static lifetime
        // 2. Slices remain valid for entire test
        // 3. No concurrent access in test
        // Guarantees:
        // - Safe test of zero-copy operations
        // - Valid pointers throughout test
        unsafe {
            batch.put_zero_copy(key1, value1).unwrap();
            batch.delete_zero_copy(key2).unwrap();
        }

        assert_eq!(batch.len(), 2);
        assert!(batch.size_bytes() > 0);

        // Test serialization
        let serialized = batch.serialize_to_buffer();
        assert!(!serialized.is_empty());

        // Test execution
        let mut executed_ops = Vec::new();
        batch.execute_zero_copy(|op_type, key, value| {
            executed_ops.push((op_type, key.to_vec(), value.map(|v| v.to_vec())));
            Ok(())
        }).unwrap();

        assert_eq!(executed_ops.len(), 2);
        assert_eq!(executed_ops[0].0, OperationType::Put);
        assert_eq!(executed_ops[0].1, b"key1");
        assert_eq!(executed_ops[0].2, Some(b"value1".to_vec()));

        assert_eq!(executed_ops[1].0, OperationType::Delete);
        assert_eq!(executed_ops[1].1, b"key2");
        assert_eq!(executed_ops[1].2, None);
    }

    #[test]
    fn test_pooled_zero_copy_batcher() {
        let mut batcher = PooledZeroCopyBatcher::new(2, 100, 1000);

        let key = b"test_key";
        let value = b"test_value";

        // SAFETY: Test with static byte slices
        // Invariants:
        // 1. Static test data remains valid
        // 2. Single-threaded test environment
        // 3. No data races possible
        // Guarantees:
        // - Test validates batching behavior
        // - Safe operation with test data
        unsafe {
            let should_flush = batcher.put(key, value).unwrap();
            assert!(!should_flush); // Should not flush for small batch
        }

        assert!(batcher.current_size() > 0);

        if let Some(batch) = batcher.take_batch() {
            assert_eq!(batch.len(), 1);
            batcher.return_batch(batch);
        }

        assert_eq!(batcher.current_size(), 0);
    }

    #[test]
    fn test_arena_zero_copy_batcher() {
        let mut batcher = ArenaZeroCopyBatcher::new(1024, 100);

        batcher.put_arena(b"key1", b"value1").unwrap();
        batcher.delete_arena(b"key2").unwrap();

        assert_eq!(batcher.len(), 2);
        assert!(batcher.used_space() > 0);

        // Test execution
        let mut executed_ops = Vec::new();
        batcher.execute_arena(|op_type, key, value| {
            executed_ops.push((op_type, key.to_vec(), value.map(|v| v.to_vec())));
            Ok(())
        }).unwrap();

        assert_eq!(executed_ops.len(), 2);
    }

    #[test]
    fn test_thread_local_batching() {
        let key = b"test_key";
        let value = b"test_value";

        // SAFETY: Test thread-local operations
        // Invariants:
        // 1. Thread-local storage properly initialized
        // 2. Static test data valid for test duration
        // 3. No cross-thread access in test
        // Guarantees:
        // - Safe test of thread-local batching
        // - Proper cleanup at test end
        unsafe {
            let should_flush = thread_local_put(key, value).unwrap();
            assert!(!should_flush);
        }

        if let Some(batch) = take_thread_local_batch() {
            assert_eq!(batch.len(), 1);
            return_thread_local_batch(batch);
        }
    }
}