use std::sync::Arc;
use crate::{Database, DatabaseStats};

impl Database {
    pub fn stats(&self) -> DatabaseStats {
        let btree_stats = {
            let btree = self.btree.read();
            btree.stats()
        };

        let lsm_stats = self.lsm_tree.as_ref().map(|lsm| lsm.stats());

        let cache_stats = self
            .unified_cache
            .as_ref()
            .map(|cache| cache.stats())
            .unwrap_or_default();

        let wal_stats = self.unified_wal.as_ref().map(|wal| wal.stats());

        DatabaseStats {
            btree_stats,
            lsm_stats,
            cache_stats,
            wal_stats,
            transaction_count: self.transaction_manager.get_active_transactions().len(),
        }
    }

    pub fn get_metrics(&self) -> crate::features::statistics::MetricsSnapshot {
        self.metrics_collector.get_snapshot()
    }

    pub fn get_metrics_reporter(&self) -> crate::features::statistics::MetricsReporter {
        crate::features::statistics::MetricsReporter::new(self.metrics_collector.clone())
    }

    pub fn metrics_collector(&self) -> Arc<crate::features::statistics::MetricsCollector> {
        self.metrics_collector.clone()
    }

    pub fn get_cache_hit_ratio(&self) -> f64 {
        if let Some(ref cache) = self.unified_cache {
            let stats = cache.stats();
            let total = stats.hits + stats.misses;
            if total > 0 {
                stats.hits as f64 / total as f64
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        let mut total = 0;

        // B+Tree memory
        {
            let btree = self.btree.read();
            total += btree.estimated_memory_usage();
        }

        // LSM tree memory
        if let Some(ref lsm) = self.lsm_tree {
            total += lsm.memory_usage();
        }

        // Cache memory
        if let Some(ref cache) = self.unified_cache {
            total += cache.memory_usage();
        }

        // Transaction manager memory
        total += self.transaction_manager.memory_usage();

        // Version store memory
        total += self.version_store.memory_usage();

        total
    }

    pub fn get_disk_usage(&self) -> Result<u64, crate::Error> {
        let mut total = 0u64;

        // B+Tree file size
        let btree_path = self.path.join("btree.db");
        if btree_path.exists() {
            total += std::fs::metadata(&btree_path)?.len();
        }

        // LSM tree disk usage
        let lsm_path = self.path.join("lsm");
        if lsm_path.exists() {
            total += get_dir_size(&lsm_path)?;
        }

        // WAL disk usage
        let wal_path = self.path.join("wal");
        if wal_path.exists() {
            total += get_dir_size(&wal_path)?;
        }

        Ok(total)
    }

    pub fn get_transaction_stats(&self) -> TransactionStats {
        TransactionStats {
            active: self.transaction_manager.get_active_transactions().len(),
            committed: self.transaction_manager.get_committed_count(),
            aborted: self.transaction_manager.get_aborted_count(),
            oldest_active: self.transaction_manager.get_oldest_active_transaction(),
        }
    }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let metrics = self.metrics_collector.database_metrics();

        PerformanceMetrics {
            ops_per_second: metrics.get_ops_per_second(),
            avg_latency_ms: metrics.get_avg_latency_ms(),
            p99_latency_ms: metrics.get_p99_latency_ms(),
            cache_hit_ratio: self.get_cache_hit_ratio(),
            write_amplification: self.get_write_amplification(),
        }
    }

    fn get_write_amplification(&self) -> f64 {
        if let Some(ref lsm) = self.lsm_tree {
            lsm.get_write_amplification()
        } else {
            1.0 // No amplification for B+Tree only
        }
    }

    pub fn cache_stats(&self) -> crate::performance::cache::CacheStats {
        if let Some(ref cache) = self.unified_cache {
            cache.stats().clone()
        } else {
            Default::default()
        }
    }

    pub fn lsm_stats(&self) -> Option<crate::core::lsm::LSMStats> {
        self.lsm_tree.as_ref().map(|lsm| lsm.stats())
    }

    pub fn get_root_page_id(&self) -> Result<u64, crate::Error> {
        let btree = self.btree.read();
        Ok(btree.root_page_id())
    }

    pub fn get_page_manager(&self) -> crate::core::storage::page_wrappers::PageManagerWrapper {
        self.page_manager.clone()
    }
}

// Helper function to calculate directory size
fn get_dir_size(dir: &std::path::Path) -> Result<u64, crate::Error> {
    let mut size = 0;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            size += get_dir_size(&path)?;
        } else {
            size += std::fs::metadata(&path)?.len();
        }
    }

    Ok(size)
}

#[derive(Debug, Clone)]
pub struct TransactionStats {
    pub active: usize,
    pub committed: u64,
    pub aborted: u64,
    pub oldest_active: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub ops_per_second: f64,
    pub avg_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub cache_hit_ratio: f64,
    pub write_amplification: f64,
}