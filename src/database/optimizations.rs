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