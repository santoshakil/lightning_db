use crate::core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

pub mod garbage_collector;
pub mod incremental;
pub mod online;
pub mod scheduler;
pub mod space_manager;
pub mod stats;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompactionType {
    Online,
    Offline,
    Incremental,
    Major,
    Minor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionState {
    Idle,
    Running,
    Paused,
    Cancelled,
    Complete,
    Failed,
}

#[derive(Debug, Clone)]
pub struct CompactionProgress {
    pub compaction_id: u64,
    pub compaction_type: CompactionType,
    pub state: CompactionState,
    pub progress_pct: f64,
    pub bytes_processed: u64,
    pub bytes_total: u64,
    pub items_processed: u64,
    pub items_total: u64,
    pub started_at: Instant,
    pub estimated_completion: Option<Instant>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CompactionStats {
    pub total_compactions: u64,
    pub successful_compactions: u64,
    pub failed_compactions: u64,
    pub bytes_compacted: u64,
    pub space_reclaimed: u64,
    pub avg_compaction_time: Duration,
    pub last_compaction: Option<Instant>,
    pub current_operations: Vec<CompactionProgress>,
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub auto_compaction_enabled: bool,
    pub compaction_interval: Duration,
    pub min_space_threshold: f64, // Minimum fragmentation % to trigger
    pub max_concurrent_compactions: usize,
    pub online_compaction_batch_size: usize,
    pub incremental_chunk_size: usize,
    pub gc_retention_period: Duration,
    pub enable_background_gc: bool,
    pub enable_statistics: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            auto_compaction_enabled: true,
            compaction_interval: Duration::from_secs(3600), // 1 hour
            min_space_threshold: 0.25,                      // 25% fragmentation
            max_concurrent_compactions: 2,
            online_compaction_batch_size: 1000,
            incremental_chunk_size: 100_000,
            gc_retention_period: Duration::from_secs(86400), // 1 day
            enable_background_gc: true,
            enable_statistics: true,
        }
    }
}

pub trait CompactionEngine: Send + Sync {
    fn start_compaction(&self, compaction_type: CompactionType) -> Result<u64>;
    fn cancel_compaction(&self, compaction_id: u64) -> Result<()>;
    fn get_progress(&self, compaction_id: u64) -> Result<CompactionProgress>;
    fn get_stats(&self) -> Result<CompactionStats>;
    fn estimate_space_savings(&self) -> Result<u64>;
    fn set_config(&self, config: CompactionConfig) -> Result<()>;
}

#[derive(Debug)]
pub struct CompactionManager {
    config: Arc<RwLock<CompactionConfig>>,
    stats: Arc<RwLock<CompactionStats>>,
    active_compactions: Arc<RwLock<HashMap<u64, CompactionProgress>>>,
    next_compaction_id: Arc<std::sync::atomic::AtomicU64>,
    online_compactor: Arc<online::OnlineCompactor>,
    incremental_compactor: Arc<incremental::IncrementalCompactor>,
    garbage_collector: Arc<garbage_collector::GarbageCollector>,
    space_manager: Arc<space_manager::SpaceManager>,
    scheduler: Arc<scheduler::MaintenanceScheduler>,
    stats_collector: Arc<stats::StatsCollector>,
}

impl CompactionManager {
    pub fn new(config: CompactionConfig) -> Result<Self> {
        let config = Arc::new(RwLock::new(config));
        let stats = Arc::new(RwLock::new(CompactionStats {
            total_compactions: 0,
            successful_compactions: 0,
            failed_compactions: 0,
            bytes_compacted: 0,
            space_reclaimed: 0,
            avg_compaction_time: Duration::from_secs(0),
            last_compaction: None,
            current_operations: Vec::new(),
        }));

        let active_compactions = Arc::new(RwLock::new(HashMap::new()));
        let next_compaction_id = Arc::new(std::sync::atomic::AtomicU64::new(1));

        let online_compactor = Arc::new(online::OnlineCompactor::new(config.clone())?);
        let incremental_compactor =
            Arc::new(incremental::IncrementalCompactor::new(config.clone())?);
        let garbage_collector = Arc::new(garbage_collector::GarbageCollector::new(config.clone())?);
        let space_manager = Arc::new(space_manager::SpaceManager::new(config.clone())?);
        let scheduler = Arc::new(scheduler::MaintenanceScheduler::new(config.clone())?);
        let stats_collector = Arc::new(stats::StatsCollector::new(config.clone(), stats.clone())?);

        Ok(Self {
            config,
            stats,
            active_compactions,
            next_compaction_id,
            online_compactor,
            incremental_compactor,
            garbage_collector,
            space_manager,
            scheduler,
            stats_collector,
        })
    }

    pub async fn compact(&self, compaction_type: CompactionType) -> Result<u64> {
        let compaction_id = self
            .next_compaction_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let progress = CompactionProgress {
            compaction_id,
            compaction_type: compaction_type.clone(),
            state: CompactionState::Running,
            progress_pct: 0.0,
            bytes_processed: 0,
            bytes_total: 0,
            items_processed: 0,
            items_total: 0,
            started_at: Instant::now(),
            estimated_completion: None,
            error: None,
        };

        {
            let mut active = self.active_compactions.write().await;
            active.insert(compaction_id, progress);
        }

        let result = match compaction_type {
            CompactionType::Online => self.online_compactor.compact(compaction_id).await,
            CompactionType::Offline => self.online_compactor.compact(compaction_id).await,
            CompactionType::Incremental => self.incremental_compactor.compact(compaction_id).await,
            CompactionType::Major => self.online_compactor.compact(compaction_id).await,
            CompactionType::Minor => self.online_compactor.minor_compact(compaction_id).await,
        };

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_compactions += 1;
            match result {
                Ok(_) => {
                    stats.successful_compactions += 1;
                    stats.last_compaction = Some(Instant::now());
                }
                Err(_) => {
                    stats.failed_compactions += 1;
                }
            }
        }

        // Update progress to complete or failed
        {
            let mut active = self.active_compactions.write().await;
            if let Some(progress) = active.get_mut(&compaction_id) {
                match result {
                    Ok(_) => {
                        progress.state = CompactionState::Complete;
                        progress.progress_pct = 100.0;
                    }
                    Err(ref e) => {
                        progress.state = CompactionState::Failed;
                        progress.error = Some(e.to_string());
                    }
                }
            }
        }

        result
    }

    pub async fn compact_async(&self, compaction_type: CompactionType) -> Result<u64> {
        let compaction_id = self
            .next_compaction_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let manager = Arc::clone(&Arc::new(self.clone()));
        let ct = compaction_type.clone();

        tokio::spawn(async move {
            let _ = manager.compact(ct).await;
        });

        Ok(compaction_id)
    }

    pub async fn cancel_compaction(&self, compaction_id: u64) -> Result<()> {
        let mut active = self.active_compactions.write().await;
        if let Some(progress) = active.get_mut(&compaction_id) {
            progress.state = CompactionState::Cancelled;

            // Delegate cancellation to appropriate compactor
            match progress.compaction_type {
                CompactionType::Online => self.online_compactor.cancel(compaction_id).await?,
                CompactionType::Offline => self.online_compactor.cancel(compaction_id).await?,
                CompactionType::Incremental => {
                    self.incremental_compactor.cancel(compaction_id).await?
                }
                CompactionType::Major => self.online_compactor.cancel(compaction_id).await?,
                CompactionType::Minor => self.online_compactor.cancel(compaction_id).await?,
            }

            Ok(())
        } else {
            Err(Error::Generic(format!(
                "Compaction {} not found",
                compaction_id
            )))
        }
    }

    pub async fn get_progress(&self, compaction_id: u64) -> Result<CompactionProgress> {
        let active = self.active_compactions.read().await;
        active
            .get(&compaction_id)
            .cloned()
            .ok_or_else(|| Error::Generic(format!("Compaction {} not found", compaction_id)))
    }

    pub async fn get_stats(&self) -> Result<CompactionStats> {
        let mut stats = self.stats.read().await.clone();
        let active = self.active_compactions.read().await;
        stats.current_operations = active.values().cloned().collect();
        Ok(stats)
    }

    pub async fn estimate_space_savings(&self) -> Result<u64> {
        let space_estimate = self.space_manager.estimate_reclaimable_space().await?;
        Ok(space_estimate)
    }

    pub async fn set_auto_compaction(
        &self,
        enabled: bool,
        interval: Option<Duration>,
    ) -> Result<()> {
        {
            let mut config = self.config.write().await;
            config.auto_compaction_enabled = enabled;
            if let Some(interval) = interval {
                config.compaction_interval = interval;
            }
        }

        if enabled {
            self.scheduler.start().await?;
        } else {
            self.scheduler.stop().await?;
        }

        Ok(())
    }

    pub async fn trigger_garbage_collection(&self) -> Result<u64> {
        self.garbage_collector.collect().await
    }

    pub async fn get_fragmentation_stats(&self) -> Result<HashMap<String, f64>> {
        let mut stats = HashMap::new();

        let space_frag = self.space_manager.get_fragmentation_ratio().await?;
        stats.insert("space_fragmentation".to_string(), space_frag);

        // Simplified fragmentation stats
        stats.insert("overall_fragmentation".to_string(), space_frag * 0.8);

        Ok(stats)
    }

    pub async fn start_background_maintenance(&self) -> Result<()> {
        self.scheduler.start().await
    }

    pub async fn stop_background_maintenance(&self) -> Result<()> {
        self.scheduler.stop().await
    }
}

impl Clone for CompactionManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            stats: self.stats.clone(),
            active_compactions: self.active_compactions.clone(),
            next_compaction_id: self.next_compaction_id.clone(),
            online_compactor: self.online_compactor.clone(),
            incremental_compactor: self.incremental_compactor.clone(),
            garbage_collector: self.garbage_collector.clone(),
            space_manager: self.space_manager.clone(),
            scheduler: self.scheduler.clone(),
            stats_collector: self.stats_collector.clone(),
        }
    }
}

impl CompactionEngine for CompactionManager {
    fn start_compaction(&self, compaction_type: CompactionType) -> Result<u64> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.compact_async(compaction_type))
    }

    fn cancel_compaction(&self, compaction_id: u64) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.cancel_compaction(compaction_id))
    }

    fn get_progress(&self, compaction_id: u64) -> Result<CompactionProgress> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.get_progress(compaction_id))
    }

    fn get_stats(&self) -> Result<CompactionStats> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.get_stats())
    }

    fn estimate_space_savings(&self) -> Result<u64> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.estimate_space_savings())
    }

    fn set_config(&self, config: CompactionConfig) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let mut current_config = self.config.write().await;
            *current_config = config;
            Ok(())
        })
    }
}
