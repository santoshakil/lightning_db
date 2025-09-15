use std::sync::Arc;
use crate::{Database, DatabaseStats};

impl Database {
    pub fn stats(&self) -> DatabaseStats {
        let page_count = self.page_manager.page_count() as u32;
        let free_page_count = self.page_manager.free_page_count();

        let tree_height = {
            let btree = self.btree.read();
            btree.height()
        };

        let active_transactions = self.transaction_manager.active_transaction_count();

        let cache_hit_rate = if let Some(ref cache) = self.unified_cache {
            let stats = cache.stats();
            let total = stats.hits.load(std::sync::atomic::Ordering::Relaxed) +
                       stats.misses.load(std::sync::atomic::Ordering::Relaxed);
            if total > 0 {
                Some(stats.hits.load(std::sync::atomic::Ordering::Relaxed) as f64 / total as f64)
            } else {
                None
            }
        } else {
            None
        };

        DatabaseStats {
            page_count,
            free_page_count,
            tree_height,
            active_transactions,
            cache_hit_rate,
            memory_usage_bytes: self.get_memory_usage() as u64,
            disk_usage_bytes: self.get_disk_usage().unwrap_or(0),
            active_connections: 0, // TODO: Track active connections
        }
    }

    pub fn get_metrics(&self) -> crate::features::statistics::MetricsSnapshot {
        self.metrics_collector.get_current_snapshot()
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
            let hits = stats.hits.load(std::sync::atomic::Ordering::Relaxed);
            let misses = stats.misses.load(std::sync::atomic::Ordering::Relaxed);
            let total = hits + misses;
            if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        // TODO: Implement accurate memory usage tracking when methods are available
        // For now, estimate based on configuration
        let mut total = 0;

        // Estimate based on cache size configuration
        total += self._config.cache_size as usize;

        // Estimate based on page count
        total += self.page_manager.page_count() * 4096;

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
            active: self.transaction_manager.active_transaction_count(),
            committed: 0, // TODO: Track committed count
            aborted: 0,   // TODO: Track aborted count
            oldest_active: None, // TODO: Track oldest active transaction
        }
    }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        // TODO: Implement accurate performance metrics when methods are available
        PerformanceMetrics {
            ops_per_second: 0.0,
            avg_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            cache_hit_ratio: self.get_cache_hit_ratio(),
            write_amplification: self.get_write_amplification(),
        }
    }

    fn get_write_amplification(&self) -> f64 {
        if let Some(ref lsm) = self.lsm_tree {
            lsm.get_write_amplification_stats().write_amplification_factor
        } else {
            1.0 // No amplification for B+Tree only
        }
    }

    pub fn cache_stats(&self) -> crate::performance::cache::CacheStats {
        use std::sync::atomic::Ordering;

        if let Some(ref cache) = self.unified_cache {
            let stats = cache.stats();
            let new_stats = crate::performance::cache::CacheStats::new();
            new_stats.hits.store(stats.hits.load(Ordering::Relaxed), Ordering::Relaxed);
            new_stats.misses.store(stats.misses.load(Ordering::Relaxed), Ordering::Relaxed);
            new_stats.evictions.store(stats.evictions.load(Ordering::Relaxed), Ordering::Relaxed);
            new_stats.prefetch_hits.store(stats.prefetch_hits.load(Ordering::Relaxed), Ordering::Relaxed);
            new_stats
        } else {
            Default::default()
        }
    }

    pub fn lsm_stats(&self) -> Option<crate::core::lsm::LSMStats> {
        self.lsm_tree.as_ref().map(|lsm| lsm.stats())
    }

    pub fn get_root_page_id(&self) -> Result<u64, crate::Error> {
        let btree = self.btree.read();
        Ok(btree.root_page_id() as u64)
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