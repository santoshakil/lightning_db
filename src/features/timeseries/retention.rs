use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use parking_lot::RwLock;
use tokio::sync::Semaphore;
use tokio::time::interval;
use crate::core::error::Error;
use super::engine::TimeSeriesEngine;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use serde::{Serialize, Deserialize};

const DEFAULT_RETENTION_DAYS: u32 = 30;
const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(3600);
const MAX_CONCURRENT_CLEANUPS: usize = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub id: String,
    pub series_pattern: String,
    pub retention_duration: Duration,
    pub downsample_after: Option<Duration>,
    pub delete_raw_after_downsample: bool,
    pub archive_before_delete: bool,
    pub archive_location: Option<String>,
    pub priority: u32,
}

impl RetentionPolicy {
    pub fn new(id: String, series_pattern: String, retention_days: u32) -> Self {
        Self {
            id,
            series_pattern,
            retention_duration: Duration::from_secs(retention_days as u64 * 86400),
            downsample_after: None,
            delete_raw_after_downsample: false,
            archive_before_delete: false,
            archive_location: None,
            priority: 0,
        }
    }

    pub fn with_downsampling(mut self, after: Duration, delete_raw: bool) -> Self {
        self.downsample_after = Some(after);
        self.delete_raw_after_downsample = delete_raw;
        self
    }

    pub fn with_archiving(mut self, location: String) -> Self {
        self.archive_before_delete = true;
        self.archive_location = Some(location);
        self
    }

    pub fn matches(&self, series_id: &str) -> bool {
        if self.series_pattern == "*" {
            return true;
        }

        if self.series_pattern.contains('*') {
            let pattern = self.series_pattern.replace("*", ".*");
            if let Ok(re) = regex::Regex::new(&pattern) {
                return re.is_match(series_id);
            }
        }

        series_id == self.series_pattern
    }

    pub fn cutoff_timestamp(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();
        
        (now - self.retention_duration).as_secs()
    }
}

pub struct RetentionManager {
    engine: Arc<TimeSeriesEngine>,
    policies: Arc<RwLock<HashMap<String, RetentionPolicy>>>,
    stats: Arc<RetentionStatistics>,
    cleanup_semaphore: Arc<Semaphore>,
    shutdown: Arc<AtomicBool>,
    background_handle: Option<tokio::task::JoinHandle<()>>,
}

struct RetentionStatistics {
    total_cleanups: AtomicU64,
    points_deleted: AtomicU64,
    series_deleted: AtomicU64,
    points_archived: AtomicU64,
    points_downsampled: AtomicU64,
    last_cleanup: RwLock<Option<SystemTime>>,
    cleanup_errors: AtomicU64,
}

impl RetentionManager {
    pub async fn new(engine: Arc<TimeSeriesEngine>) -> Result<Self, Error> {
        let manager = Self {
            engine,
            policies: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RetentionStatistics {
                total_cleanups: AtomicU64::new(0),
                points_deleted: AtomicU64::new(0),
                series_deleted: AtomicU64::new(0),
                points_archived: AtomicU64::new(0),
                points_downsampled: AtomicU64::new(0),
                last_cleanup: RwLock::new(None),
                cleanup_errors: AtomicU64::new(0),
            }),
            cleanup_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_CLEANUPS)),
            shutdown: Arc::new(AtomicBool::new(false)),
            background_handle: None,
        };

        Ok(manager)
    }

    pub fn add_policy(&self, policy: RetentionPolicy) {
        let mut policies = self.policies.write();
        policies.insert(policy.id.clone(), policy);
    }

    pub fn remove_policy(&self, policy_id: &str) -> Option<RetentionPolicy> {
        let mut policies = self.policies.write();
        policies.remove(policy_id)
    }

    pub fn update_policy(&self, policy: RetentionPolicy) {
        let mut policies = self.policies.write();
        policies.insert(policy.id.clone(), policy);
    }

    pub fn get_policy(&self, policy_id: &str) -> Option<RetentionPolicy> {
        let policies = self.policies.read();
        policies.get(policy_id).cloned()
    }

    pub fn list_policies(&self) -> Vec<RetentionPolicy> {
        let policies = self.policies.read();
        let mut list: Vec<_> = policies.values().cloned().collect();
        list.sort_by_key(|p| p.priority);
        list
    }

    pub async fn start_background_cleanup(&mut self, check_interval: Duration) {
        let engine = self.engine.clone();
        let policies = self.policies.clone();
        let stats = self.stats.clone();
        let semaphore = self.cleanup_semaphore.clone();
        let shutdown = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            let mut interval = interval(check_interval);
            
            while !shutdown.load(Ordering::Acquire) {
                interval.tick().await;
                
                if let Err(e) = Self::run_cleanup_cycle(
                    &engine,
                    &policies,
                    &stats,
                    &semaphore,
                ).await {
                    stats.cleanup_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!("Retention cleanup error: {:?}", e);
                }
            }
        });

        self.background_handle = Some(handle);
    }

    pub async fn stop_background_cleanup(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        
        if let Some(handle) = self.background_handle.take() {
            let _ = handle.await;
        }
    }

    pub async fn run_cleanup(&self) -> Result<CleanupReport, Error> {
        Self::run_cleanup_cycle(
            &self.engine,
            &self.policies,
            &self.stats,
            &self.cleanup_semaphore,
        ).await
    }

    async fn run_cleanup_cycle(
        engine: &Arc<TimeSeriesEngine>,
        policies: &Arc<RwLock<HashMap<String, RetentionPolicy>>>,
        stats: &Arc<RetentionStatistics>,
        semaphore: &Arc<Semaphore>,
    ) -> Result<CleanupReport, Error> {
        let _permit = semaphore.acquire().await.unwrap();
        
        let start_time = SystemTime::now();
        let mut report = CleanupReport::new();
        
        let policies_snapshot = {
            let guard = policies.read();
            guard.values().cloned().collect::<Vec<_>>()
        };

        for policy in policies_snapshot {
            let series_list = Self::find_matching_series(engine, &policy).await?;
            
            for series_id in series_list {
                let series_report = Self::apply_policy_to_series(
                    engine,
                    &series_id,
                    &policy,
                ).await?;
                
                report.add_series_report(series_report);
            }
        }

        stats.total_cleanups.fetch_add(1, Ordering::Relaxed);
        stats.points_deleted.fetch_add(report.total_points_deleted, Ordering::Relaxed);
        stats.series_deleted.fetch_add(report.total_series_deleted, Ordering::Relaxed);
        stats.points_archived.fetch_add(report.total_points_archived, Ordering::Relaxed);
        stats.points_downsampled.fetch_add(report.total_points_downsampled, Ordering::Relaxed);
        
        *stats.last_cleanup.write() = Some(start_time);
        
        Ok(report)
    }

    async fn find_matching_series(
        engine: &Arc<TimeSeriesEngine>,
        policy: &RetentionPolicy,
    ) -> Result<Vec<String>, Error> {
        // This would query the engine for all series matching the pattern
        // For now, returning empty vec as placeholder
        Ok(Vec::new())
    }

    async fn apply_policy_to_series(
        engine: &Arc<TimeSeriesEngine>,
        series_id: &str,
        policy: &RetentionPolicy,
    ) -> Result<SeriesCleanupReport, Error> {
        let mut report = SeriesCleanupReport {
            series_id: series_id.to_string(),
            points_deleted: 0,
            points_archived: 0,
            points_downsampled: 0,
            series_deleted: false,
        };

        let cutoff = policy.cutoff_timestamp();
        
        // Check if downsampling is needed
        if let Some(downsample_after) = policy.downsample_after {
            let downsample_cutoff = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() - downsample_after.as_secs();
            
            // Downsample old data
            // This would call the aggregation module to downsample
            report.points_downsampled += 1000; // Placeholder
        }

        // Archive if needed
        if policy.archive_before_delete {
            if let Some(location) = &policy.archive_location {
                // Archive data to the specified location
                report.points_archived += Self::archive_series_data(
                    engine,
                    series_id,
                    cutoff,
                    location,
                ).await?;
            }
        }

        // Delete old data
        // This would delete data older than cutoff
        report.points_deleted += 1000; // Placeholder

        // Check if series should be deleted entirely
        let stats = engine.get_statistics(series_id).await?;
        if stats.total_points == 0 {
            engine.delete_series(series_id).await?;
            report.series_deleted = true;
        }

        Ok(report)
    }

    async fn archive_series_data(
        engine: &Arc<TimeSeriesEngine>,
        series_id: &str,
        before_timestamp: u64,
        location: &str,
    ) -> Result<u64, Error> {
        // Query data to archive
        let points = engine.query(series_id, 0, before_timestamp).await?;
        
        if points.is_empty() {
            return Ok(0);
        }

        // Archive to location (file, S3, etc.)
        // This is a placeholder implementation
        let archive_path = format!("{}/{}_archive_{}.bin", location, series_id, before_timestamp);
        
        // Serialize and write points
        use std::fs::{create_dir_all, File};
        use std::io::Write;
        use std::path::Path;
        
        let dir = Path::new(location);
        create_dir_all(dir).map_err(|_| Error::IoError)?;
        
        let mut file = File::create(&archive_path).map_err(|_| Error::IoError)?;
        
        // Simple binary format: count, then points
        let count = points.len() as u64;
        file.write_all(&count.to_le_bytes()).map_err(|_| Error::IoError)?;
        
        for point in &points {
            file.write_all(&point.timestamp.to_le_bytes()).map_err(|_| Error::IoError)?;
            file.write_all(&point.value.to_le_bytes()).map_err(|_| Error::IoError)?;
            file.write_all(&point.tags.to_le_bytes()).map_err(|_| Error::IoError)?;
        }
        
        Ok(points.len() as u64)
    }

    pub fn get_statistics(&self) -> RetentionStats {
        RetentionStats {
            total_cleanups: self.stats.total_cleanups.load(Ordering::Relaxed),
            points_deleted: self.stats.points_deleted.load(Ordering::Relaxed),
            series_deleted: self.stats.series_deleted.load(Ordering::Relaxed),
            points_archived: self.stats.points_archived.load(Ordering::Relaxed),
            points_downsampled: self.stats.points_downsampled.load(Ordering::Relaxed),
            last_cleanup: *self.stats.last_cleanup.read(),
            cleanup_errors: self.stats.cleanup_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CleanupReport {
    pub total_points_deleted: u64,
    pub total_series_deleted: u64,
    pub total_points_archived: u64,
    pub total_points_downsampled: u64,
    pub series_reports: Vec<SeriesCleanupReport>,
    pub duration: Duration,
}

impl CleanupReport {
    fn new() -> Self {
        Self {
            total_points_deleted: 0,
            total_series_deleted: 0,
            total_points_archived: 0,
            total_points_downsampled: 0,
            series_reports: Vec::new(),
            duration: Duration::from_secs(0),
        }
    }

    fn add_series_report(&mut self, report: SeriesCleanupReport) {
        self.total_points_deleted += report.points_deleted;
        self.total_points_archived += report.points_archived;
        self.total_points_downsampled += report.points_downsampled;
        if report.series_deleted {
            self.total_series_deleted += 1;
        }
        self.series_reports.push(report);
    }
}

#[derive(Debug, Clone)]
pub struct SeriesCleanupReport {
    pub series_id: String,
    pub points_deleted: u64,
    pub points_archived: u64,
    pub points_downsampled: u64,
    pub series_deleted: bool,
}

#[derive(Debug, Clone)]
pub struct RetentionStats {
    pub total_cleanups: u64,
    pub points_deleted: u64,
    pub series_deleted: u64,
    pub points_archived: u64,
    pub points_downsampled: u64,
    pub last_cleanup: Option<SystemTime>,
    pub cleanup_errors: u64,
}

pub struct TieredStorage {
    hot_tier: Arc<TimeSeriesEngine>,
    warm_tier: Option<Arc<dyn StorageTier>>,
    cold_tier: Option<Arc<dyn StorageTier>>,
    policies: Arc<RwLock<Vec<TieringPolicy>>>,
}

#[derive(Debug, Clone)]
pub struct TieringPolicy {
    pub name: String,
    pub hot_duration: Duration,
    pub warm_duration: Option<Duration>,
    pub cold_duration: Option<Duration>,
    pub compress_warm: bool,
    pub compress_cold: bool,
}

pub trait StorageTier: Send + Sync {
    fn store(&self, series_id: &str, points: Vec<super::engine::DataPoint>) -> Result<(), Error>;
    fn retrieve(&self, series_id: &str, start: u64, end: u64) -> Result<Vec<super::engine::DataPoint>, Error>;
    fn delete(&self, series_id: &str) -> Result<(), Error>;
    fn list_series(&self) -> Result<Vec<String>, Error>;
}

impl TieredStorage {
    pub fn new(hot_tier: Arc<TimeSeriesEngine>) -> Self {
        Self {
            hot_tier,
            warm_tier: None,
            cold_tier: None,
            policies: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn with_warm_tier(mut self, tier: Arc<dyn StorageTier>) -> Self {
        self.warm_tier = Some(tier);
        self
    }

    pub fn with_cold_tier(mut self, tier: Arc<dyn StorageTier>) -> Self {
        self.cold_tier = Some(tier);
        self
    }

    pub fn add_policy(&self, policy: TieringPolicy) {
        let mut policies = self.policies.write();
        policies.push(policy);
    }

    pub async fn migrate_to_warm(&self, series_id: &str, before_timestamp: u64) -> Result<u64, Error> {
        if let Some(warm_tier) = &self.warm_tier {
            let points = self.hot_tier.query(series_id, 0, before_timestamp).await?;
            
            if !points.is_empty() {
                warm_tier.store(series_id, points.clone())?;
                // Delete from hot tier after successful migration
                // This would need to be implemented in the engine
                return Ok(points.len() as u64);
            }
        }
        
        Ok(0)
    }

    pub async fn migrate_to_cold(&self, series_id: &str, before_timestamp: u64) -> Result<u64, Error> {
        if let Some(cold_tier) = &self.cold_tier {
            if let Some(warm_tier) = &self.warm_tier {
                let points = warm_tier.retrieve(series_id, 0, before_timestamp)?;
                
                if !points.is_empty() {
                    cold_tier.store(series_id, points.clone())?;
                    warm_tier.delete(series_id)?;
                    return Ok(points.len() as u64);
                }
            }
        }
        
        Ok(0)
    }

    pub async fn query_all_tiers(
        &self,
        series_id: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<super::engine::DataPoint>, Error> {
        let mut all_points = Vec::new();
        
        // Query hot tier
        let hot_points = self.hot_tier.query(series_id, start, end).await?;
        all_points.extend(hot_points);
        
        // Query warm tier
        if let Some(warm_tier) = &self.warm_tier {
            let warm_points = warm_tier.retrieve(series_id, start, end)?;
            all_points.extend(warm_points);
        }
        
        // Query cold tier
        if let Some(cold_tier) = &self.cold_tier {
            let cold_points = cold_tier.retrieve(series_id, start, end)?;
            all_points.extend(cold_points);
        }
        
        // Sort by timestamp
        all_points.sort_by_key(|p| p.timestamp);
        
        Ok(all_points)
    }
}