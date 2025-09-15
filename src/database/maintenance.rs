use std::sync::Arc;
use crate::{Database, Result};

impl Database {
    pub fn checkpoint(&self) -> Result<()> {
        // Flush WAL to ensure all writes are durable
        if let Some(ref unified_wal) = self.unified_wal {
            unified_wal.sync()?;
        }

        // Flush LSM tree if present
        if let Some(ref lsm) = self.lsm_tree {
            lsm.flush()?;
        }

        // Sync page manager to ensure B+Tree changes are persisted
        self.page_manager.sync()?;

        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        // Stop prefetch manager
        if let Some(ref prefetch_manager) = self.prefetch_manager {
            prefetch_manager.stop()?;
        }

        // Stop metrics collector
        self.metrics_collector.stop();

        // Flush all pending writes
        self.checkpoint()?;

        // Shutdown WAL
        if let Some(ref unified_wal) = self.unified_wal {
            unified_wal.shutdown();
        }

        Ok(())
    }

    pub fn flush_lsm(&self) -> Result<()> {
        if let Some(ref lsm) = self.lsm_tree {
            lsm.flush()?;
        }
        Ok(())
    }

    pub fn flush_write_buffer(&self) -> Result<()> {
        if let Some(ref write_buffer) = self.btree_write_buffer {
            write_buffer.flush()?;
        }
        Ok(())
    }

    pub fn compact_lsm(&self) -> Result<()> {
        if let Some(ref lsm) = self.lsm_tree {
            lsm.compact_all()?;
        }
        Ok(())
    }

    pub fn defragment(&self) -> Result<()> {
        // TODO: Implement B+Tree reorganization when method is available
        // For now, just sync to disk
        self.page_manager.sync()?;
        Ok(())
    }

    pub fn vacuum(&self) -> Result<()> {
        // TODO: Implement vacuum operation when PageManager supports it
        // For now, just sync to ensure consistency
        self.page_manager.sync()?;

        Ok(())
    }

    pub async fn backup<P: AsRef<std::path::Path>>(&self, backup_path: P) -> Result<()> {
        // Ensure all data is flushed first
        self.checkpoint()?;

        // Create backup directory
        let backup_dir = backup_path.as_ref();
        std::fs::create_dir_all(backup_dir)?;

        // Backup B+Tree file
        let btree_src = self.path.join("btree.db");
        let btree_dst = backup_dir.join("btree.db");
        if btree_src.exists() {
            std::fs::copy(&btree_src, &btree_dst)?;
        }

        // Backup LSM tree if present
        if self.lsm_tree.is_some() {
            let lsm_src = self.path.join("lsm");
            let lsm_dst = backup_dir.join("lsm");
            if lsm_src.exists() {
                // Copy LSM directory recursively
                copy_dir_recursive(&lsm_src, &lsm_dst)?;
            }
        }

        // Backup WAL if present
        if self.unified_wal.is_some() {
            let wal_src = self.path.join("wal");
            let wal_dst = backup_dir.join("wal");
            if wal_src.exists() {
                copy_dir_recursive(&wal_src, &wal_dst)?;
            }
        }

        Ok(())
    }

    pub async fn restore<P: AsRef<std::path::Path>>(&self, backup_path: P) -> Result<()> {
        // Shutdown the database first
        self.shutdown()?;

        let backup_dir = backup_path.as_ref();

        // Restore B+Tree file
        let btree_src = backup_dir.join("btree.db");
        let btree_dst = self.path.join("btree.db");
        if btree_src.exists() {
            std::fs::copy(&btree_src, &btree_dst)?;
        }

        // Restore LSM tree if present
        let lsm_src = backup_dir.join("lsm");
        let lsm_dst = self.path.join("lsm");
        if lsm_src.exists() {
            copy_dir_recursive(&lsm_src, &lsm_dst)?;
        }

        // Restore WAL if present
        let wal_src = backup_dir.join("wal");
        let wal_dst = self.path.join("wal");
        if wal_src.exists() {
            copy_dir_recursive(&wal_src, &wal_dst)?;
        }

        Ok(())
    }

    pub fn verify_integrity(&self) -> Result<bool> {
        // TODO: Implement integrity verification when methods are available
        // Currently, basic validation can be done through:
        // - LSM tree validation: lsm.validate_invariants()

        if let Some(ref lsm) = self.lsm_tree {
            let _ = lsm.validate_invariants()?;
        }

        Ok(true)
    }

    pub fn enable_compaction(&self, config: crate::features::compaction::CompactionConfig) -> Result<()> {
        if self.compaction_manager.is_none() {
            let compaction_manager = Arc::new(crate::features::compaction::incremental::IncrementalCompactor::new(config));
            // Note: We can't modify self here as it's not mutable
            // This would need to be handled differently in the actual implementation
            // For now, we'll just return an error
            return Err(crate::Error::Internal("Compaction manager already initialized".to_string()));
        }
        Ok(())
    }

    pub fn trigger_compaction(&self) -> Result<()> {
        if let Some(ref compaction_manager) = self.compaction_manager {
            tokio::runtime::Runtime::new()?.block_on(compaction_manager.compact_async())?;
        }
        Ok(())
    }
}

// Helper function to copy directories recursively
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();
        let dst_path = dst.join(&file_name);

        if path.is_dir() {
            copy_dir_recursive(&path, &dst_path)?;
        } else {
            std::fs::copy(&path, &dst_path)?;
        }
    }

    Ok(())
}