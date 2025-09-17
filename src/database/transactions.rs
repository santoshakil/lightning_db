use crate::{
    Database, Error, Result,
    features::transactions::isolation::IsolationLevel,
};

impl Database {
    pub fn begin_transaction(&self) -> Result<u64> {
        // Check resource quotas for transaction
        if let Some(ref quota_manager) = self.quota_manager {
            quota_manager.check_connection_allowed()?; // Treat transaction as a connection for quota purposes
        }

        self.transaction_manager.begin()
    }

    pub fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        // Get the transaction and write set before committing
        let write_set = {
            let tx_arc = self.transaction_manager.get_transaction(tx_id)?;

            let tx = tx_arc.read();
            tx.write_set.clone()
        };

        // For sync WAL mode, we need to optimize transaction commits
        let is_sync_wal = matches!(self._config.wal_sync_mode, crate::WalSyncMode::Sync);

        // Write to WAL first for durability (before committing to version store)
        if let Some(ref unified_wal) = self.unified_wal {
            use crate::core::wal::WALOperation;

            // Write transaction begin marker
            unified_wal.append(WALOperation::TransactionBegin { tx_id })?;

            // Write all operations
            for write_op in write_set.values() {
                if let Some(ref value) = write_op.value {
                    unified_wal.append(WALOperation::Put {
                        key: write_op.key.to_vec(),
                        value: value.to_vec(),
                    })?;
                } else {
                    unified_wal.append(WALOperation::Delete {
                        key: write_op.key.to_vec(),
                    })?;
                }
            }

            // Write commit marker
            unified_wal.append(WALOperation::TransactionCommit { tx_id })?;
        }

        // Commit through transaction manager (use sync for immediate lock release)
        self.transaction_manager.commit_sync(tx_id)?;

        // Apply committed changes to storage
        if let Some(ref lsm) = self.lsm_tree {
            // Apply to LSM tree
            for write_op in write_set.values() {
                if let Some(ref value) = write_op.value {
                    lsm.insert(write_op.key.to_vec(), value.to_vec())?;
                } else {
                    lsm.delete(&write_op.key)?;
                }
            }
        } else if is_sync_wal {
            // For non-LSM with sync WAL, we need to be more careful
            // Apply operations directly to B+Tree but batch them
            let mut btree = self.btree.write();
            for write_op in write_set.values() {
                if let Some(ref value) = write_op.value {
                    btree.insert(&write_op.key, value)?;
                } else {
                    btree.delete(&write_op.key)?;
                }
            }
            // Ensure changes are flushed to disk
            drop(btree);
            self.page_manager.sync()?;
        }

        Ok(())
    }

    pub fn abort_transaction(&self, tx_id: u64) -> Result<()> {
        self.transaction_manager.abort(tx_id)
    }

    pub fn put_tx(&self, tx_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        // Validate key is not empty
        if key.is_empty() {
            return Err(Error::InvalidKeySize {
                size: 0,
                min: 1,
                max: usize::MAX,
            });
        }

        // Add to transaction write set
        self.transaction_manager.put(tx_id, key, value)?;

        Ok(())
    }

    pub fn get_tx(&self, tx_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check transaction write set first
        let tx_arc = self.transaction_manager.get_transaction(tx_id)?;
        let tx = tx_arc.read();

        if let Some(write_op) = tx.write_set.get(key) {
            return Ok(write_op.value.as_ref().map(|b| b.to_vec()));
        }

        // Read from storage (could be from committed versions)
        if let Some(ref lsm) = self.lsm_tree {
            lsm.get(key)
        } else {
            let btree = self.btree.read();
            btree.get(key)
        }
    }

    pub fn delete_tx(&self, tx_id: u64, key: &[u8]) -> Result<()> {
        // Add tombstone to transaction write set (deletion)
        self.transaction_manager.delete(tx_id, key)?;

        Ok(())
    }

    pub fn begin_transaction_with_isolation(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<u64> {
        // Check resource quotas for transaction
        if let Some(ref quota_manager) = self.quota_manager {
            quota_manager.check_connection_allowed()?; // Treat transaction as a connection for quota purposes
        }

        // Begin transaction with the existing transaction manager
        let tx_id = if let Some(opt_manager) = Some(&self.transaction_manager) {
            opt_manager.begin()?
        } else {
            self.transaction_manager.begin()?
        };

        // Register with isolation manager
        self.isolation_manager
            .begin_transaction(tx_id, isolation_level)?;

        Ok(tx_id)
    }

    pub fn commit_transaction_with_isolation(&self, tx_id: u64) -> Result<()> {
        // Perform the actual commit
        self.commit_transaction(tx_id)?;

        // Clean up isolation manager state
        self.isolation_manager.commit_transaction(tx_id)?;

        Ok(())
    }

    pub fn abort_transaction_with_cleanup(&self, tx_id: u64) -> Result<()> {
        // Abort through normal transaction manager
        self.abort_transaction(tx_id)?;

        // Clean up isolation manager state
        self.isolation_manager.abort_transaction(tx_id)?;

        Ok(())
    }

    /// Get all active transactions (for debugging)
    pub fn get_active_transactions(&self) -> Vec<u64> {
        // Returns empty for now - transaction manager handles this internally
        Vec::new()
    }

    /// Clean up old transactions
    pub fn cleanup_old_transactions(&self, max_age_ms: u64) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let cutoff_time = current_time.saturating_sub(max_age_ms);

        // Cleanup is handled by transaction manager internally
        let _ = cutoff_time;

        // Cleanup logic disabled until get_active_transactions is implemented
        // for tx_id in active_txs {
        //     if let Ok(tx_arc) = self.transaction_manager.get_transaction(tx_id) {
        //         let tx = tx_arc.read();
        //         if tx.start_timestamp < cutoff_time {
        //             drop(tx); // Release lock before aborting
        //             let _ = self.transaction_manager.abort(tx_id);
        //         }
        //     }
        // }
    }

    /// Clean up old versions in the version store
    pub fn cleanup_old_versions(&self, before_timestamp: u64) {
        // Version cleanup is handled automatically by the version store
        let _ = before_timestamp;
    }
}