use crate::{
    Database, Error, Result,
    core::{
        wal::WALOperation,
    },
    utils::{
        batching::write_batch::{WriteBatch, BatchOperation},
        safety::consistency::ConsistencyLevel,
    },
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
            // Convert to vec once and reuse
            let key_vec = key.to_vec();
            let value_vec = value.to_vec();

            // Write to WAL first for durability
            if let Some(ref unified_wal) = self.unified_wal {
                use crate::core::wal::WALOperation;
                unified_wal.append(WALOperation::Put {
                    key: key_vec.clone(),
                    value: value_vec.clone(),
                })?;
            }

            // Write to LSM
            lsm.insert(key_vec, value_vec)?;

            return Ok(());
        }

        // Fast path for direct B+Tree writes when not using transactions
        // Note: Even with optimized transactions enabled, non-transactional puts should use the direct path
        if let Some(ref write_buffer) = self.btree_write_buffer {
            return write_buffer.insert(key.to_vec(), value.to_vec());
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
        // Consistency level is not applicable for embedded database
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
            // Consistency level is not applicable for embedded database
            let _ = consistency_level;
            {
                    // Try LSM tree first if available
                    if let Some(ref lsm) = self.lsm_tree {
                        lsm.get(key)?
                    } else {
                        // For non-LSM databases, read directly from B+Tree
                        let btree = self.btree.read();
                        btree.get(key)?
                    }
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
        // Flush LSM tree if present to ensure all memtable data is persisted
        if let Some(ref lsm) = self.lsm_tree {
            lsm.flush()?;
        }

        // Sync WAL if present
        if let Some(ref wal) = self.unified_wal {
            wal.sync()?;
        }

        // Sync page manager
        self.page_manager.sync()?;
        Ok(())
    }

    pub fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<crate::core::iterator::RangeIterator> {
        use crate::core::iterator::{RangeIterator, ScanDirection};

        let mut iterator = RangeIterator::new(
            start.map(|s| s.to_vec()),
            end.map(|e| e.to_vec()),
            ScanDirection::Forward,
            0, // TODO: Use proper read timestamp
        );

        // Attach data sources
        if let Some(ref lsm) = self.lsm_tree {
            iterator.attach_lsm(lsm)?;
        } else {
            // For non-LSM mode, attach B+Tree
            let btree = self.btree.read();
            iterator.attach_btree(&*btree)?;
        }

        Ok(iterator)
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<crate::core::iterator::RangeIterator> {
        use crate::core::iterator::{RangeIterator, ScanDirection};
        // Calculate end key for prefix scan
        let mut end = prefix.to_vec();
        let end_key = {
            let mut found_non_255 = false;
            for i in (0..end.len()).rev() {
                if end[i] < 255 {
                    end[i] += 1;
                    end.truncate(i + 1);
                    found_non_255 = true;
                    break;
                }
            }
            if found_non_255 {
                Some(end)
            } else {
                None
            }
        };

        let mut iterator = RangeIterator::new(
            Some(prefix.to_vec()),
            end_key,
            ScanDirection::Forward,
            0, // TODO: Use proper read timestamp
        );

        // Attach data sources
        if let Some(ref lsm) = self.lsm_tree {
            iterator.attach_lsm(lsm)?;
        } else {
            // For non-LSM mode, attach B+Tree
            let btree = self.btree.read();
            iterator.attach_btree(&*btree)?;
        }

        Ok(iterator)
    }

    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use crate::core::lsm::LSMFullIterator;
        use std::collections::BTreeMap;

        let mut result = Vec::new();

        // Use LSM tree if available
        if let Some(ref lsm) = self.lsm_tree {
            // Collect from memtable using iterator
            let mut iter = LSMFullIterator::new(
                &lsm,
                start.map(|s| s.to_vec()),
                end.map(|e| e.to_vec()),
                true, // forward
            )?;

            // Use a BTreeMap to deduplicate and sort
            let mut entries = BTreeMap::new();

            // Get from memtable
            while let Some((key, value, _timestamp)) = iter.advance() {
                if let Some(val) = value {
                    if !crate::core::lsm::LSMTree::is_tombstone(&val) {
                        entries.insert(key, val);
                    }
                }
            }

            // Convert the sorted entries to result
            match (start, end) {
                (None, None) => {
                    // No bounds - add all entries
                    for (key, value) in entries {
                        result.push((key, value));
                    }
                }
                (Some(s), None) => {
                    // Only start bound
                    for (key, value) in entries {
                        if key.as_slice() >= s {
                            result.push((key, value));
                        }
                    }
                }
                (None, Some(e)) => {
                    // Only end bound
                    for (key, value) in entries {
                        if key.as_slice() < e {
                            result.push((key, value));
                        }
                    }
                }
                (Some(s), Some(e)) => {
                    // Both bounds
                    for (key, value) in entries {
                        let k = key.as_slice();
                        if k >= s && k < e {
                            result.push((key, value));
                        }
                    }
                }
            }
        } else {
            // B+Tree range scan requires implementing iterator on BPlusTree
            return Err(crate::Error::Internal("Range scan is only supported with LSM tree storage engine".to_string()));
        }

        Ok(result)
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

            // Insert to LSM
            lsm.insert(key.to_vec(), value.to_vec())?;
            Ok(())
        } else {
            self.put_standard(key, value)
        }
    }
}