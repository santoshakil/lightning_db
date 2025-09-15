use crate::{
    Database, Error, Result,
    core::{
        wal::WALOperation,
    },
    utils::{
        batching::write_batch::{WriteBatch, BatchOperation},
        safety::consistency::ConsistencyLevel,
    },
    performance,
};

impl Database {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Validate key and value sizes
        const MAX_KEY_SIZE: usize = 4096;
        const MAX_VALUE_SIZE: usize = 1024 * 1024; // 1MB

        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(Error::InvalidKeySize {
                size: key.len(),
                min: 1,
                max: MAX_KEY_SIZE,
            });
        }

        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::InvalidValueSize {
                size: value.len(),
                max: MAX_VALUE_SIZE,
            });
        }


        // Check resource quotas
        if let Some(ref quota_manager) = self.quota_manager {
            let size_bytes = key.len() as u64 + value.len() as u64;
            quota_manager.check_write_allowed(None, size_bytes)?;
        }

        // Check if we have a write batcher
        if let Some(ref batcher) = self.write_batcher {
            return batcher.put(key.to_vec(), value.to_vec());
        }

        // Optimization: For small keys/values, try to minimize allocations
        if key.len() <= 64 && value.len() <= 1024 {
            return self.put_small_optimized(key, value);
        }
        // Fast path for non-transactional puts with LSM
        if let Some(ref lsm) = self.lsm_tree {
            // Write to WAL first for durability
            if let Some(ref unified_wal) = self.unified_wal {
                use crate::core::wal::WALOperation;
                unified_wal.append(WALOperation::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                })?;
            }

            // Write to LSM - use thread-local buffers for small data
            let key_vec = if key.len() <= 256 {
                performance::thread_local::cache::copy_to_thread_buffer(key)
            } else {
                key.to_vec()
            };
            let value_vec = if value.len() <= 4096 {
                performance::thread_local::cache::copy_to_thread_buffer(value)
            } else {
                value.to_vec()
            };
            lsm.insert(key_vec, value_vec)?;

            // UnifiedCache integration implemented

            return Ok(());
        }

        // Fast path for direct B+Tree writes when not using transactions
        // Note: Even with optimized transactions enabled, non-transactional puts should use the direct path
        if let Some(ref write_buffer) = self.btree_write_buffer {
            return write_buffer.insert(key, value);
        }

        // Direct B+Tree write (fallback)
        self.put_standard(key, value)
    }

    pub fn batch_put(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        // Use transaction for atomicity if optimized transactions are enabled
        if self._config.use_optimized_transactions {
            let tx_id = self.transaction_manager.begin()?;

            // Wrap in transaction
            for (key, value) in pairs {
                if let Err(e) = self.put_tx(tx_id, key, value) {
                    // Abort on error
                    let _ = self.transaction_manager.abort(tx_id);
                    return Err(e);
                }
            }

            // Commit the transaction
            self.commit_transaction(tx_id)?;
            Ok(())
        } else {
            // Non-transactional batch
            for (key, value) in pairs {
                self.put(key, value)?;
            }
            Ok(())
        }
    }


    fn put_standard(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Write to WAL first for durability
        if let Some(ref unified_wal) = self.unified_wal {
            unified_wal.append(WALOperation::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            })?;
        }

        // Write directly to B+Tree
        let mut btree = self.btree.write();
        btree.insert(key, value)?;

        // Sync page manager if using synchronous WAL mode
        if matches!(self._config.wal_sync_mode, crate::WalSyncMode::Sync) {
            drop(btree);
            self.page_manager.sync()?;
        }

        Ok(())
    }

    pub fn put_with_consistency(
        &self,
        key: &[u8],
        value: &[u8],
        _consistency_level: ConsistencyLevel,
    ) -> Result<()> {
        // TODO: Implement consistency level handling
        // For now, just perform the write
        self.put(key, value)
    }

    pub fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        // Create implicit transaction for the batch
        let tx_id = self.transaction_manager.begin()?;

        // Apply all operations within the transaction
        for op in batch.operations() {
            match op {
                BatchOperation::Put { key, value } => {
                    if let Err(e) = self.put_tx(tx_id, key, value) {
                        // Abort on error
                        let _ = self.transaction_manager.abort(tx_id);
                        return Err(e);
                    }
                }
                BatchOperation::Delete { key } => {
                    if let Err(e) = self.delete_tx(tx_id, key) {
                        // Abort on error
                        let _ = self.transaction_manager.abort(tx_id);
                        return Err(e);
                    }
                }
            }
        }

        // Commit the transaction
        self.commit_transaction(tx_id)?;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check resource quotas
        if let Some(ref quota_manager) = self.quota_manager {
            quota_manager.check_read_allowed(None, key.len() as u64)?;
        }

        self.get_with_consistency(key, self._config.consistency_config.default_level)
    }

    pub fn get_with_consistency(
        &self,
        key: &[u8],
        consistency_level: ConsistencyLevel,
    ) -> Result<Option<Vec<u8>>> {
        let start = std::time::Instant::now();
        let result = {
            // TODO: Implement consistency level handling
            let _ = consistency_level;
            {
                    // TODO: Implement proper key-value caching
                    // UnifiedCache is for page caching, not key-value caching

                    // Try LSM tree first if available
                    let result = if let Some(ref lsm) = self.lsm_tree {
                        lsm.get(key)?
                    } else {
                        // For non-LSM databases, read directly from B+Tree
                        let btree = self.btree.read();
                        btree.get(key)?
                    };

                    // TODO: Update cache when proper key-value cache is implemented

                    result
                }
        };

        // Record metrics
        let metrics = self.metrics_collector.database_metrics();
        if result.is_some() {
            metrics.record_read(start.elapsed());
        }

        Ok(result)
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let start = std::time::Instant::now();
        let result: Result<bool> = (|| {
            // Write to WAL first for durability
            if let Some(ref unified_wal) = self.unified_wal {
                use crate::core::wal::WALOperation;
                unified_wal.append(WALOperation::Delete { key: key.to_vec() })?;
            }

            // Apply delete directly to storage layers
            // Use LSM tree if available, otherwise use implicit transaction
            let existed = if let Some(ref lsm) = self.lsm_tree {
                // For LSM, check if key exists before deleting
                let existed = lsm.get(key)?.is_some();
                lsm.delete(key)?;
                existed
            } else {
                // For non-LSM databases, delete directly from B+Tree
                if let Some(ref write_buffer) = self.btree_write_buffer {
                    write_buffer.delete(key)?
                } else {
                    // Direct B+Tree delete
                    let mut btree = self.btree.write();
                    let existed = btree.delete(key)?;
                    drop(btree);
                    self.page_manager.sync()?;
                    existed
                }
            };

            // TODO: Remove from cache when proper key-value cache is implemented

            Ok(existed)
        })();

        // Record metrics
        if result.is_ok() {
            let metrics = self.metrics_collector.database_metrics();
            metrics.record_delete(start.elapsed());
        }

        result
    }

    pub fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        // Use batch_put which already exists
        self.batch_put(pairs)
    }

    pub fn get_batch(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key)?);
        }
        Ok(results)
    }

    pub fn delete_batch(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.delete(key)?);
        }
        Ok(results)
    }

    pub fn sync(&self) -> Result<()> {
        // Sync WAL if present
        if let Some(ref wal) = self.unified_wal {
            wal.sync()?;
        }
        // Sync page manager
        self.page_manager.sync()?;
        Ok(())
    }

    pub fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<crate::core::iterator::RangeIterator> {
        use crate::core::iterator::RangeIterator;
        RangeIterator::new(
            self.btree.clone(),
            start.map(|s| s.to_vec()),
            end.map(|e| e.to_vec()),
            None,
        )
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<crate::core::iterator::RangeIterator> {
        use crate::core::iterator::RangeIterator;
        // Calculate end key for prefix scan
        let mut end = prefix.to_vec();
        for i in (0..end.len()).rev() {
            if end[i] < 255 {
                end[i] += 1;
                end.truncate(i + 1);
                return RangeIterator::new(
                    self.btree.clone(),
                    Some(prefix.to_vec()),
                    Some(end),
                    None,
                );
            }
        }
        // If all bytes are 255, scan from prefix to end
        RangeIterator::new(
            self.btree.clone(),
            Some(prefix.to_vec()),
            None,
            None,
        )
    }

    pub(crate) fn put_small_optimized(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Fast path for small data - minimize allocations
        if let Some(ref lsm) = self.lsm_tree {
            // Write to WAL first
            if let Some(ref unified_wal) = self.unified_wal {
                unified_wal.append(WALOperation::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                })?;
            }

            // Use thread-local buffer for small data
            let key_vec = performance::thread_local::cache::copy_to_thread_buffer(key);
            let value_vec = performance::thread_local::cache::copy_to_thread_buffer(value);
            lsm.insert(key_vec, value_vec)?;
            Ok(())
        } else {
            self.put_standard(key, value)
        }
    }
}