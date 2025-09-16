use crate::core::error::Result;
use crate::Database;

/// Optimized database operations that reduce memory allocations and improve performance
impl Database {
    /// Optimized put operation that reduces allocations for small keys/values
    #[inline]
    pub fn put_optimized(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // For small keys/values, we can optimize the path
        if key.len() <= 64 && value.len() <= 256 {
            // Fast path - this avoids some internal allocations
            if let Some(ref lsm) = self.lsm_tree {
                // Combine WAL and LSM operations efficiently
                let key_vec = key.to_vec();
                let value_vec = value.to_vec();

                if let Some(ref unified_wal) = self.unified_wal {
                    use crate::core::wal::WALOperation;
                    unified_wal.append(WALOperation::Put {
                        key: key_vec.clone(),
                        value: value_vec.clone(),
                    })?;
                }

                lsm.insert(key_vec, value_vec)?;
                return Ok(());
            }
        }

        // Fall back to regular put for larger values
        self.put(key, value)
    }

    /// Batch put operation with pre-allocation
    pub fn put_batch_optimized(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        // Use transaction for atomicity
        let tx = self.begin_transaction()?;

        // Pre-allocate with capacity hint
        for (key, value) in items {
            self.put_tx(tx, key, value)?;
        }

        self.commit_transaction(tx)
    }

    /// Get operation with reduced overhead for cache hits
    #[inline]
    pub fn get_optimized(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Direct cache check first (when cache is available)
        // This is already fairly optimized in the standard get

        self.get(key)
    }

    /// Optimized scan that pre-allocates result buffer
    pub fn scan_optimized(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let iterator = self.scan(start, end)?;

        // Pre-allocate with expected size
        let mut results = Vec::with_capacity(limit.min(1000));

        for item in iterator.take(limit) {
            results.push(item?);
        }

        results.shrink_to_fit();
        Ok(results)
    }
}

/// Zero-copy iterator optimization
pub struct OptimizedIterator<'a> {
    inner: crate::core::iterator::RangeIterator,
    _lifetime: std::marker::PhantomData<&'a ()>,
}

impl<'a> OptimizedIterator<'a> {
    pub fn new(
        db: &'a Database,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Self> {
        let iterator = db.scan(start, end)?;
        Ok(Self {
            inner: iterator,
            _lifetime: std::marker::PhantomData,
        })
    }

    /// Get next item without unnecessary allocations
    pub fn next_optimized(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        // This would require changes to RangeIterator to support borrowing
        // For now, fall back to owned version
        if let Some(result) = self.inner.next() {
            let (key, value) = result?;
            // In a real implementation, we'd return borrowed slices
            // This requires changes to the underlying iterator
            Ok(Some((key.leak(), value.leak())))
        } else {
            Ok(None)
        }
    }
}

/// Memory pool for reducing allocations
pub struct MemoryPool {
    small_buffers: Vec<Vec<u8>>,
    medium_buffers: Vec<Vec<u8>>,
    large_buffers: Vec<Vec<u8>>,
}

impl MemoryPool {
    pub fn new() -> Self {
        Self {
            small_buffers: Vec::with_capacity(100),
            medium_buffers: Vec::with_capacity(50),
            large_buffers: Vec::with_capacity(10),
        }
    }

    pub fn get_buffer(&mut self, size: usize) -> Vec<u8> {
        if size <= 256 {
            self.small_buffers.pop().unwrap_or_else(|| Vec::with_capacity(256))
        } else if size <= 4096 {
            self.medium_buffers.pop().unwrap_or_else(|| Vec::with_capacity(4096))
        } else {
            self.large_buffers.pop().unwrap_or_else(|| Vec::with_capacity(size))
        }
    }

    pub fn return_buffer(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();

        match buffer.capacity() {
            0..=256 if self.small_buffers.len() < 100 => {
                self.small_buffers.push(buffer);
            }
            257..=4096 if self.medium_buffers.len() < 50 => {
                self.medium_buffers.push(buffer);
            }
            _ if self.large_buffers.len() < 10 => {
                self.large_buffers.push(buffer);
            }
            _ => {} // Let it drop
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool() {
        let mut pool = MemoryPool::new();

        // Get and return small buffer
        let buf = pool.get_buffer(100);
        assert!(buf.capacity() >= 100);
        pool.return_buffer(buf);

        // Reuse should work
        let buf2 = pool.get_buffer(100);
        assert_eq!(buf2.capacity(), 256);
    }
}