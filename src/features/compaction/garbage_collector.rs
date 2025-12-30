use super::CompactionConfig;
use crate::core::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::error;

#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub version_id: u64,
    pub transaction_id: u64,
    pub created_at: Instant,
    pub size_bytes: u64,
    pub key: Vec<u8>,
    pub is_tombstone: bool,
    pub reference_count: usize,
}

#[derive(Debug, Clone)]
pub struct GCStats {
    pub total_collections: u64,
    pub versions_collected: u64,
    pub bytes_reclaimed: u64,
    pub tombstones_removed: u64,
    pub last_collection: Option<Instant>,
    pub avg_collection_time: Duration,
    pub active_versions: u64,
    pub old_versions: u64,
}

#[derive(Debug)]
pub struct GarbageCollector {
    config: Arc<RwLock<CompactionConfig>>,
    version_store: Arc<RwLock<HashMap<u64, VersionInfo>>>,
    active_transactions: Arc<RwLock<HashSet<u64>>>,
    gc_stats: Arc<RwLock<GCStats>>,
    min_version_watermark: Arc<RwLock<u64>>,
}

impl GarbageCollector {
    pub fn new(config: Arc<RwLock<CompactionConfig>>) -> Result<Self> {
        Ok(Self {
            config,
            version_store: Arc::new(RwLock::new(HashMap::new())),
            active_transactions: Arc::new(RwLock::new(HashSet::new())),
            gc_stats: Arc::new(RwLock::new(GCStats {
                total_collections: 0,
                versions_collected: 0,
                bytes_reclaimed: 0,
                tombstones_removed: 0,
                last_collection: None,
                avg_collection_time: Duration::from_secs(0),
                active_versions: 0,
                old_versions: 0,
            })),
            min_version_watermark: Arc::new(RwLock::new(0)),
        })
    }

    pub async fn collect(&self) -> Result<u64> {
        let start_time = Instant::now();

        // Phase 1: Update watermarks
        self.update_version_watermarks().await?;

        // Phase 2: Collect old versions
        let old_version_bytes = self.collect_old_versions().await?;

        // Phase 3: Collect tombstones
        let tombstone_bytes = self.collect_expired_tombstones().await?;

        // Phase 4: Collect unreferenced data
        let unreferenced_bytes = self.collect_unreferenced_data().await?;

        let total_bytes = old_version_bytes + tombstone_bytes + unreferenced_bytes;
        let duration = start_time.elapsed();

        // Update statistics
        let mut stats = self.gc_stats.write().await;
        stats.total_collections += 1;
        stats.bytes_reclaimed += total_bytes;
        stats.last_collection = Some(start_time);

        // Update average collection time
        let total_time = stats.avg_collection_time.as_secs_f64()
            * (stats.total_collections - 1) as f64
            + duration.as_secs_f64();
        stats.avg_collection_time =
            Duration::from_secs_f64(total_time / stats.total_collections as f64);

        Ok(total_bytes)
    }

    pub async fn collect_version_range(&self, min_version: u64, max_version: u64) -> Result<u64> {
        let version_store = self.version_store.read().await;
        let active_txns = self.active_transactions.read().await;

        let mut bytes_collected = 0u64;
        let mut versions_to_remove = Vec::new();

        for (version_id, version_info) in version_store.iter() {
            if *version_id >= min_version && *version_id <= max_version {
                // Check if version is safe to collect
                if !active_txns.contains(&version_info.transaction_id) {
                    versions_to_remove.push(*version_id);
                    bytes_collected += version_info.size_bytes;
                }
            }
        }

        drop(version_store);
        drop(active_txns);

        let versions_collected_count = versions_to_remove.len() as u64;

        // Remove collected versions
        {
            let mut version_store = self.version_store.write().await;
            for version_id in versions_to_remove {
                version_store.remove(&version_id);
            }
        }

        // Update stats
        let mut stats = self.gc_stats.write().await;
        stats.versions_collected += versions_collected_count;

        Ok(bytes_collected)
    }

    pub async fn register_version(&self, version_info: VersionInfo) -> Result<()> {
        let mut version_store = self.version_store.write().await;
        version_store.insert(version_info.version_id, version_info);

        // Update active version count
        let mut stats = self.gc_stats.write().await;
        stats.active_versions = version_store.len() as u64;

        Ok(())
    }

    pub async fn register_transaction(&self, transaction_id: u64) -> Result<()> {
        let mut active_txns = self.active_transactions.write().await;
        active_txns.insert(transaction_id);
        Ok(())
    }

    pub async fn unregister_transaction(&self, transaction_id: u64) -> Result<()> {
        let mut active_txns = self.active_transactions.write().await;
        active_txns.remove(&transaction_id);
        Ok(())
    }

    pub async fn get_stats(&self) -> GCStats {
        let mut stats = self.gc_stats.read().await.clone();

        // Update current counts
        let version_store = self.version_store.read().await;
        let config = self.config.read().await;
        let retention_cutoff = Instant::now() - config.gc_retention_period;
        drop(config);

        let (active_count, old_count) =
            version_store
                .values()
                .fold((0, 0), |(active, old), version| {
                    if version.created_at > retention_cutoff {
                        (active + 1, old)
                    } else {
                        (active, old + 1)
                    }
                });

        stats.active_versions = active_count;
        stats.old_versions = old_count;

        stats
    }

    pub async fn estimate_collectible_bytes(&self) -> Result<u64> {
        let version_store = self.version_store.read().await;
        let active_txns = self.active_transactions.read().await;
        let config = self.config.read().await;
        let retention_cutoff = Instant::now() - config.gc_retention_period;
        drop(config);

        let collectible_bytes = version_store
            .values()
            .filter(|version| {
                // Can collect if:
                // 1. Version is old enough
                // 2. Associated transaction is not active
                version.created_at <= retention_cutoff
                    && !active_txns.contains(&version.transaction_id)
            })
            .map(|version| version.size_bytes)
            .sum();

        Ok(collectible_bytes)
    }

    pub async fn force_collect_transaction(&self, transaction_id: u64) -> Result<u64> {
        let version_store = self.version_store.read().await;
        let mut bytes_collected = 0u64;
        let mut versions_to_remove = Vec::new();

        for (version_id, version_info) in version_store.iter() {
            if version_info.transaction_id == transaction_id {
                versions_to_remove.push(*version_id);
                bytes_collected += version_info.size_bytes;
            }
        }

        drop(version_store);

        // Remove versions
        {
            let mut version_store = self.version_store.write().await;
            for version_id in versions_to_remove {
                version_store.remove(&version_id);
            }
        }

        // Also remove from active transactions
        {
            let mut active_txns = self.active_transactions.write().await;
            active_txns.remove(&transaction_id);
        }

        Ok(bytes_collected)
    }

    async fn update_version_watermarks(&self) -> Result<()> {
        let active_txns = self.active_transactions.read().await;

        if active_txns.is_empty() {
            // No active transactions - can set watermark to current time
            let version_store = self.version_store.read().await;
            if let Some(max_version) = version_store.keys().max() {
                let mut watermark = self.min_version_watermark.write().await;
                *watermark = *max_version;
            }
        } else {
            // Find minimum version among active transactions
            let version_store = self.version_store.read().await;
            let min_active_version = active_txns
                .iter()
                .filter_map(|txn_id| {
                    version_store
                        .values()
                        .filter(|v| v.transaction_id == *txn_id)
                        .map(|v| v.version_id)
                        .min()
                })
                .min();

            if let Some(min_version) = min_active_version {
                let mut watermark = self.min_version_watermark.write().await;
                *watermark = min_version;
            }
        }

        Ok(())
    }

    async fn collect_old_versions(&self) -> Result<u64> {
        let config = self.config.read().await;
        let retention_period = config.gc_retention_period;
        drop(config);

        let cutoff_time = Instant::now() - retention_period;
        let watermark = *self.min_version_watermark.read().await;

        let version_store = self.version_store.read().await;
        let active_txns = self.active_transactions.read().await;

        let mut bytes_collected = 0u64;
        let mut versions_to_remove = Vec::new();

        for (version_id, version_info) in version_store.iter() {
            // Collect if version is old AND below watermark AND transaction not active
            if version_info.created_at <= cutoff_time
                && *version_id < watermark
                && !active_txns.contains(&version_info.transaction_id)
            {
                versions_to_remove.push(*version_id);
                bytes_collected += version_info.size_bytes;
            }
        }

        drop(version_store);
        drop(active_txns);

        // Remove old versions
        {
            let mut version_store = self.version_store.write().await;
            let mut stats = self.gc_stats.write().await;

            for version_id in versions_to_remove {
                version_store.remove(&version_id);
                stats.versions_collected += 1;
            }
        }

        Ok(bytes_collected)
    }

    async fn collect_expired_tombstones(&self) -> Result<u64> {
        let config = self.config.read().await;
        let retention_period = config.gc_retention_period;
        drop(config);

        let cutoff_time = Instant::now() - retention_period;
        let version_store = self.version_store.read().await;

        let mut bytes_collected = 0u64;
        let mut tombstones_to_remove = Vec::new();

        for (version_id, version_info) in version_store.iter() {
            if version_info.is_tombstone && version_info.created_at <= cutoff_time {
                tombstones_to_remove.push(*version_id);
                bytes_collected += version_info.size_bytes;
            }
        }

        drop(version_store);

        // Remove expired tombstones
        {
            let mut version_store = self.version_store.write().await;
            let mut stats = self.gc_stats.write().await;

            for version_id in tombstones_to_remove {
                version_store.remove(&version_id);
                stats.tombstones_removed += 1;
            }
        }

        Ok(bytes_collected)
    }

    async fn collect_unreferenced_data(&self) -> Result<u64> {
        let version_store = self.version_store.read().await;

        let mut bytes_collected = 0u64;
        let mut unreferenced_versions = Vec::new();

        for (version_id, version_info) in version_store.iter() {
            if version_info.reference_count == 0 {
                unreferenced_versions.push(*version_id);
                bytes_collected += version_info.size_bytes;
            }
        }

        drop(version_store);

        // Remove unreferenced versions
        {
            let mut version_store = self.version_store.write().await;
            for version_id in unreferenced_versions {
                version_store.remove(&version_id);
            }
        }

        Ok(bytes_collected)
    }

    pub async fn get_version_distribution(&self) -> Result<HashMap<String, usize>> {
        let version_store = self.version_store.read().await;
        let config = self.config.read().await;
        let retention_cutoff = Instant::now() - config.gc_retention_period;
        drop(config);

        let mut distribution = HashMap::new();
        distribution.insert("active".to_string(), 0);
        distribution.insert("old_collectible".to_string(), 0);
        distribution.insert("old_referenced".to_string(), 0);
        distribution.insert("tombstones".to_string(), 0);
        distribution.insert("unreferenced".to_string(), 0);

        let active_txns = self.active_transactions.read().await;

        for version_info in version_store.values() {
            let category = if version_info.is_tombstone {
                "tombstones"
            } else if version_info.reference_count == 0 {
                "unreferenced"
            } else if version_info.created_at <= retention_cutoff {
                if active_txns.contains(&version_info.transaction_id) {
                    "old_referenced"
                } else {
                    "old_collectible"
                }
            } else {
                "active"
            };

            *distribution.get_mut(category).unwrap() += 1;
        }

        Ok(distribution)
    }

    pub async fn set_retention_period(&self, retention: Duration) -> Result<()> {
        let mut config = self.config.write().await;
        config.gc_retention_period = retention;
        Ok(())
    }

    pub async fn start_background_collection(&self) -> Result<()> {
        let config = self.config.read().await;
        let enabled = config.enable_background_gc;
        let interval = config.compaction_interval;
        drop(config);

        if !enabled {
            return Ok(());
        }

        let gc = Arc::new(self.clone());
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                if let Err(e) = gc.collect().await {
                    error!("Background GC error: {}", e);
                }
            }
        });

        Ok(())
    }

    pub async fn get_oldest_version(&self) -> Option<VersionInfo> {
        let version_store = self.version_store.read().await;
        version_store.values().min_by_key(|v| v.created_at).cloned()
    }

    pub async fn get_version_count(&self) -> usize {
        let version_store = self.version_store.read().await;
        version_store.len()
    }
}

impl Clone for GarbageCollector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            version_store: self.version_store.clone(),
            active_transactions: self.active_transactions.clone(),
            gc_stats: self.gc_stats.clone(),
            min_version_watermark: self.min_version_watermark.clone(),
        }
    }
}
