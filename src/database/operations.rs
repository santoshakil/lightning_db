use crate::{
    Database, Error, Result, OperationType,
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
            return write_buffer.put(key, value);
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

    fn put_small_optimized(&self, key: &[u8], value: &[u8]) -> Result<()> {
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
        consistency_level: ConsistencyLevel,
    ) -> Result<()> {
        // Check consistency requirements
        self.consistency_manager
            .check_write(consistency_level, || {
                // Perform the actual write
                self.put(key, value)
            })
    }

    pub fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        // Create implicit transaction for the batch
        let tx_id = self.transaction_manager.begin()?;

        // Apply all operations within the transaction
        for op in &batch.operations {
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
        let metrics = self.metrics_collector.database_metrics();

        metrics.record_operation(OperationType::Get, || {
            // Check consistency requirements
            self.consistency_manager
                .check_read(consistency_level, || {
                    // Check unified cache first if available
                    if let Some(ref cache) = self.unified_cache {
                        if let Some(value) = cache.get(key) {
                            metrics.record_cache_hit();
                            return Ok(Some(value));
                        }
                        metrics.record_cache_miss();
                    }

                    // Try LSM tree first if available
                    let result = if let Some(ref lsm) = self.lsm_tree {
                        lsm.get(key)?
                    } else {
                        // For non-LSM databases, read directly from B+Tree
                        let btree = self.btree.read();
                        btree.get(key)?
                    };

                    // Update cache if found
                    if let (Some(ref value), Some(ref cache)) = (&result, &self.unified_cache) {
                        cache.put(key.to_vec(), value.clone());
                    }

                    Ok(result)
                })
        })
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let metrics = self.metrics_collector.database_metrics();

        metrics.record_operation(OperationType::Delete, || {
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

            // Remove from cache if present
            if let Some(ref cache) = self.unified_cache {
                cache.remove(key);
            }

            Ok(existed)
        })
    }
}