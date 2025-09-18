//! Tiered Storage Management
//!
//! Implements intelligent data placement across multiple storage tiers (hot, warm, cold)
//! based on access patterns, age, and data characteristics.

use super::SSTableReader;
use crate::{Error, Result};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Storage tier types
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StorageTier {
    /// Hot tier - frequently accessed data (SSD/NVMe)
    Hot = 0,
    /// Warm tier - moderately accessed data (SSD)
    Warm = 1,
    /// Cold tier - rarely accessed data (HDD/Cloud)
    Cold = 2,
    /// Archive tier - archived data (Tape/Glacier)
    Archive = 3,
}

impl StorageTier {
    /// Get tier name
    pub fn name(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
            StorageTier::Archive => "archive",
        }
    }

    /// Get next tier (for demotion)
    pub fn next_tier(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => Some(StorageTier::Warm),
            StorageTier::Warm => Some(StorageTier::Cold),
            StorageTier::Cold => Some(StorageTier::Archive),
            StorageTier::Archive => None,
        }
    }

    /// Get previous tier (for promotion)
    pub fn previous_tier(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => None,
            StorageTier::Warm => Some(StorageTier::Hot),
            StorageTier::Cold => Some(StorageTier::Warm),
            StorageTier::Archive => Some(StorageTier::Cold),
        }
    }
}

/// Access pattern tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPattern {
    /// Total number of reads
    pub read_count: u64,
    /// Total number of writes
    pub write_count: u64,
    /// Last access time
    pub last_access: u64,
    /// First access time
    pub first_access: u64,
    /// Access frequency (reads per hour)
    pub read_frequency: f64,
    /// Write frequency (writes per hour)
    pub write_frequency: f64,
    /// Sequential access ratio (0.0 to 1.0)
    pub sequential_ratio: f64,
    /// Average access latency (microseconds)
    pub avg_latency: f64,
}

impl Default for AccessPattern {
    fn default() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            read_count: 0,
            write_count: 0,
            last_access: now,
            first_access: now,
            read_frequency: 0.0,
            write_frequency: 0.0,
            sequential_ratio: 0.0,
            avg_latency: 0.0,
        }
    }
}

impl AccessPattern {
    /// Record a read access
    pub fn record_read(&mut self, latency_us: f64) {
        self.read_count += 1;
        self.last_access = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Update running average of latency
        self.avg_latency =
            (self.avg_latency * (self.read_count - 1) as f64 + latency_us) / self.read_count as f64;

        self.update_frequencies();
    }

    /// Record a write access
    pub fn record_write(&mut self, latency_us: f64) {
        self.write_count += 1;
        self.last_access = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Update running average of latency
        let total_ops = self.read_count + self.write_count;
        self.avg_latency =
            (self.avg_latency * (total_ops - 1) as f64 + latency_us) / total_ops as f64;

        self.update_frequencies();
    }

    /// Update access frequencies
    fn update_frequencies(&mut self) {
        let time_diff_hours = (self.last_access - self.first_access) as f64 / (1000.0 * 3600.0);
        if time_diff_hours > 0.0 {
            self.read_frequency = self.read_count as f64 / time_diff_hours;
            self.write_frequency = self.write_count as f64 / time_diff_hours;
        }
    }

    /// Calculate hotness score (0.0 to 100.0)
    pub fn hotness_score(&self) -> f64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let age_hours = (now - self.last_access) as f64 / (1000.0 * 3600.0);
        let recency_score = 100.0 * (-age_hours / 24.0).exp(); // Decay over 24 hours

        let frequency_score = (self.read_frequency + self.write_frequency * 2.0).min(100.0);

        // Combine recency and frequency with weights
        recency_score * 0.6 + frequency_score * 0.4
    }
}

/// Tier configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfig {
    /// Tier type
    pub tier: StorageTier,
    /// Storage path
    pub path: PathBuf,
    /// Maximum size in bytes
    pub max_size: u64,
    /// Current size in bytes
    pub current_size: u64,
    /// Minimum hotness score for this tier
    pub min_hotness: f64,
    /// Maximum hotness score for this tier
    pub max_hotness: f64,
    /// Cost per GB per month
    pub cost_per_gb: f64,
    /// Read latency (microseconds)
    pub read_latency: f64,
    /// Write latency (microseconds)
    pub write_latency: f64,
    /// Throughput (MB/s)
    pub throughput: f64,
}

impl TierConfig {
    /// Create hot tier configuration
    pub fn hot_tier(path: PathBuf, max_size: u64) -> Self {
        Self {
            tier: StorageTier::Hot,
            path,
            max_size,
            current_size: 0,
            min_hotness: 75.0,
            max_hotness: 100.0,
            cost_per_gb: 0.25,    // $0.25/GB/month for NVMe
            read_latency: 50.0,   // 50 microseconds
            write_latency: 100.0, // 100 microseconds
            throughput: 3000.0,   // 3 GB/s
        }
    }

    /// Create warm tier configuration
    pub fn warm_tier(path: PathBuf, max_size: u64) -> Self {
        Self {
            tier: StorageTier::Warm,
            path,
            max_size,
            current_size: 0,
            min_hotness: 25.0,
            max_hotness: 75.0,
            cost_per_gb: 0.10,    // $0.10/GB/month for SSD
            read_latency: 200.0,  // 200 microseconds
            write_latency: 500.0, // 500 microseconds
            throughput: 1000.0,   // 1 GB/s
        }
    }

    /// Create cold tier configuration
    pub fn cold_tier(path: PathBuf, max_size: u64) -> Self {
        Self {
            tier: StorageTier::Cold,
            path,
            max_size,
            current_size: 0,
            min_hotness: 0.0,
            max_hotness: 25.0,
            cost_per_gb: 0.023,     // $0.023/GB/month for HDD
            read_latency: 5000.0,   // 5 milliseconds
            write_latency: 10000.0, // 10 milliseconds
            throughput: 200.0,      // 200 MB/s
        }
    }

    /// Check if tier has space
    pub fn has_space(&self, size: u64) -> bool {
        self.current_size + size <= self.max_size
    }

    /// Get utilization percentage
    pub fn utilization(&self) -> f64 {
        if self.max_size == 0 {
            0.0
        } else {
            (self.current_size as f64 / self.max_size as f64) * 100.0
        }
    }
}

/// SSTable with tier information
#[derive(Debug, Clone)]
pub struct TieredSSTable {
    pub sstable: Arc<SSTableReader>,
    pub tier: StorageTier,
    pub access_pattern: AccessPattern,
    pub placement_score: f64,
    pub last_evaluated: u64,
}

impl TieredSSTable {
    /// Create a new tiered SSTable
    pub fn new(sstable: Arc<SSTableReader>, tier: StorageTier) -> Self {
        Self {
            sstable,
            tier,
            access_pattern: AccessPattern::default(),
            placement_score: 0.0,
            last_evaluated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    /// Update placement score based on access pattern and tier characteristics
    pub fn update_placement_score(&mut self, tier_config: &TierConfig) {
        let hotness = self.access_pattern.hotness_score();

        // Score based on how well the hotness matches the tier
        let tier_match_score =
            if hotness >= tier_config.min_hotness && hotness <= tier_config.max_hotness {
                100.0
            } else if hotness < tier_config.min_hotness {
                // Too cold for this tier
                100.0 - (tier_config.min_hotness - hotness)
            } else {
                // Too hot for this tier
                100.0 - (hotness - tier_config.max_hotness)
            };

        // Factor in cost efficiency
        let cost_efficiency = if self.access_pattern.read_frequency > 0.0 {
            1.0 / (tier_config.cost_per_gb * tier_config.read_latency)
        } else {
            1.0 / tier_config.cost_per_gb
        };

        self.placement_score = tier_match_score * 0.8 + cost_efficiency.min(20.0) * 0.2;
        self.last_evaluated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
    }

    /// Check if SSTable should be moved to a different tier
    pub fn should_migrate(&self, target_tier: StorageTier) -> bool {
        let hotness = self.access_pattern.hotness_score();

        match (self.tier, target_tier) {
            (StorageTier::Hot, StorageTier::Warm) => hotness < 50.0,
            (StorageTier::Warm, StorageTier::Hot) => hotness > 80.0,
            (StorageTier::Warm, StorageTier::Cold) => hotness < 10.0,
            (StorageTier::Cold, StorageTier::Warm) => hotness > 30.0,
            _ => false,
        }
    }
}

/// Migration job
#[derive(Debug, Clone)]
pub struct MigrationJob {
    pub id: u64,
    pub sstable: Arc<SSTableReader>,
    pub from_tier: StorageTier,
    pub to_tier: StorageTier,
    pub priority: u8, // 0 = highest, 255 = lowest
    pub created_at: u64,
    pub estimated_time: Duration,
}

impl MigrationJob {
    /// Create a new migration job
    pub fn new(
        id: u64,
        sstable: Arc<SSTableReader>,
        from_tier: StorageTier,
        to_tier: StorageTier,
    ) -> Self {
        let file_size = sstable.metadata().file_size;
        let estimated_time = Duration::from_secs(file_size / (100 * 1024 * 1024)); // Assume 100 MB/s

        Self {
            id,
            sstable,
            from_tier,
            to_tier,
            priority: 128, // Default priority
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            estimated_time,
        }
    }

    /// Execute the migration
    pub fn execute(&self, tiered_manager: &TieredStorageManager) -> Result<()> {
        // Copy SSTable to new tier
        let source_path = &self.sstable.metadata().file_path;
        let target_dir = tiered_manager.get_tier_path(&self.to_tier)?;
        let file_name = source_path.file_name()
            .ok_or_else(|| Error::InvalidArgument(
                format!("Invalid file path: {:?}", source_path)
            ))?;
        let target_path = target_dir.join(file_name);

        std::fs::copy(source_path, &target_path)?;

        // Update metadata
        tiered_manager.update_sstable_tier(Arc::clone(&self.sstable), self.to_tier, target_path)?;

        // Remove from old tier
        std::fs::remove_file(source_path)?;

        Ok(())
    }
}

/// Tiered storage manager
pub struct TieredStorageManager {
    /// Tier configurations
    tiers: Arc<RwLock<HashMap<StorageTier, TierConfig>>>,
    /// SSTables by tier
    sstables_by_tier: Arc<RwLock<HashMap<StorageTier, Vec<TieredSSTable>>>>,
    /// Access pattern tracking
    access_patterns: Arc<RwLock<HashMap<String, AccessPattern>>>, // filename -> pattern
    /// Migration queue
    migration_queue: Arc<Mutex<VecDeque<MigrationJob>>>,
    /// Background workers
    workers: Vec<thread::JoinHandle<()>>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Next job ID
    next_job_id: Arc<AtomicU64>,
    /// Statistics
    stats: Arc<RwLock<TierStats>>,
    /// Evaluation interval
    evaluation_interval: Duration,
}

impl TieredStorageManager {
    /// Create a new tiered storage manager
    pub fn new(evaluation_interval: Duration) -> Self {
        Self {
            tiers: Arc::new(RwLock::new(HashMap::new())),
            sstables_by_tier: Arc::new(RwLock::new(HashMap::new())),
            access_patterns: Arc::new(RwLock::new(HashMap::new())),
            migration_queue: Arc::new(Mutex::new(VecDeque::new())),
            workers: Vec::new(),
            running: Arc::new(AtomicBool::new(false)),
            next_job_id: Arc::new(AtomicU64::new(1)),
            stats: Arc::new(RwLock::new(TierStats::default())),
            evaluation_interval,
        }
    }

    /// Add a storage tier
    pub fn add_tier(&self, config: TierConfig) -> Result<()> {
        std::fs::create_dir_all(&config.path)?;

        let mut tiers = self.tiers.write();
        let mut sstables = self.sstables_by_tier.write();

        let tier = config.tier;
        tiers.insert(tier, config);
        sstables.insert(tier, Vec::new());

        Ok(())
    }

    /// Get tier path
    pub fn get_tier_path(&self, tier: &StorageTier) -> Result<PathBuf> {
        let tiers = self.tiers.read();
        tiers
            .get(tier)
            .map(|config| config.path.clone())
            .ok_or_else(|| Error::InvalidOperation {
                reason: format!("Tier {:?} not configured", tier),
            })
    }

    /// Add an SSTable to a tier
    pub fn add_sstable(&self, sstable: Arc<SSTableReader>, tier: StorageTier) -> Result<()> {
        let mut sstables = self.sstables_by_tier.write();
        let mut tiers = self.tiers.write();

        // Update tier size
        if let Some(tier_config) = tiers.get_mut(&tier) {
            tier_config.current_size += sstable.metadata().file_size;
        }

        // Add to tier
        if let Some(tier_sstables) = sstables.get_mut(&tier) {
            tier_sstables.push(TieredSSTable::new(sstable, tier));
        }

        Ok(())
    }

    /// Record access to an SSTable
    pub fn record_access(&self, file_path: &str, is_read: bool, latency_us: f64) {
        let mut patterns = self.access_patterns.write();
        let pattern = patterns.entry(file_path.to_string()).or_default();

        if is_read {
            pattern.record_read(latency_us);
        } else {
            pattern.record_write(latency_us);
        }
    }

    /// Update SSTable tier after migration
    pub fn update_sstable_tier(
        &self,
        sstable: Arc<SSTableReader>,
        new_tier: StorageTier,
        _new_path: PathBuf,
    ) -> Result<()> {
        // Remove from old tier
        let mut sstables = self.sstables_by_tier.write();
        for tier_sstables in sstables.values_mut() {
            tier_sstables.retain(|ts| !Arc::ptr_eq(&ts.sstable, &sstable));
        }

        // Add to new tier with updated path
        // Note: In a real implementation, we'd need to update the SSTableReader's metadata
        if let Some(tier_sstables) = sstables.get_mut(&new_tier) {
            tier_sstables.push(TieredSSTable::new(sstable, new_tier));
        }

        Ok(())
    }

    /// Start background workers
    pub fn start(&mut self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);

        // Start evaluation worker
        let tiers = Arc::clone(&self.tiers);
        let sstables = Arc::clone(&self.sstables_by_tier);
        let patterns = Arc::clone(&self.access_patterns);
        let migration_queue = Arc::clone(&self.migration_queue);
        let running = Arc::clone(&self.running);
        let next_job_id = Arc::clone(&self.next_job_id);
        let stats = Arc::clone(&self.stats);
        let interval = self.evaluation_interval;

        let evaluation_worker = thread::Builder::new()
            .name("tier-evaluator".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    if Self::evaluate_and_schedule_migrations(
                        &tiers,
                        &sstables,
                        &patterns,
                        &migration_queue,
                        &next_job_id,
                        &stats,
                    )
                    .is_err()
                    {
                        // Log error but continue
                    }

                    thread::sleep(interval);
                }
            })?;

        self.workers.push(evaluation_worker);

        // Start migration worker
        let migration_queue = Arc::clone(&self.migration_queue);
        let running = Arc::clone(&self.running);

        let migration_worker = thread::Builder::new()
            .name("tier-migrator".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    let job = {
                        let mut queue = migration_queue.lock();
                        queue.pop_front()
                    };

                    if let Some(_job) = job {
                        // Execute migration job
                        // Note: Simplified for now
                        thread::sleep(Duration::from_millis(100));
                    } else {
                        thread::sleep(Duration::from_millis(1000));
                    }
                }
            })?;

        self.workers.push(migration_worker);

        Ok(())
    }

    /// Stop background workers
    pub fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);

        for worker in self.workers.drain(..) {
            worker
                .join()
                .map_err(|_| Error::Generic("Failed to join tiered storage worker thread - thread panicked or was terminated".to_string()))?;
        }

        Ok(())
    }

    /// Evaluate SSTables and schedule migrations
    fn evaluate_and_schedule_migrations(
        tiers: &Arc<RwLock<HashMap<StorageTier, TierConfig>>>,
        sstables: &Arc<RwLock<HashMap<StorageTier, Vec<TieredSSTable>>>>,
        patterns: &Arc<RwLock<HashMap<String, AccessPattern>>>,
        migration_queue: &Arc<Mutex<VecDeque<MigrationJob>>>,
        next_job_id: &Arc<AtomicU64>,
        _stats: &Arc<RwLock<TierStats>>,
    ) -> Result<()> {
        let tiers_read = tiers.read();
        let mut sstables_write = sstables.write();
        let patterns_read = patterns.read();

        // Update access patterns for all SSTables
        for tier_sstables in sstables_write.values_mut() {
            for tiered_sstable in tier_sstables.iter_mut() {
                if let Some(file_name_os) = tiered_sstable
                    .sstable
                    .metadata()
                    .file_path
                    .file_name()
                {
                    let file_name = file_name_os.to_string_lossy();
                    if let Some(pattern) = patterns_read.get(file_name.as_ref()) {
                        tiered_sstable.access_pattern = pattern.clone();
                    }
                }

                // Update placement score
                if let Some(tier_config) = tiers_read.get(&tiered_sstable.tier) {
                    tiered_sstable.update_placement_score(tier_config);
                }
            }
        }

        // Find migration candidates
        let mut migrations = Vec::new();

        for (current_tier, tier_sstables) in sstables_write.iter() {
            for tiered_sstable in tier_sstables {
                let hotness = tiered_sstable.access_pattern.hotness_score();

                // Check if should promote to hotter tier
                if let Some(hotter_tier) = current_tier.previous_tier() {
                    if let Some(hotter_config) = tiers_read.get(&hotter_tier) {
                        if hotness >= hotter_config.min_hotness
                            && hotter_config.has_space(tiered_sstable.sstable.metadata().file_size)
                        {
                            let job_id = next_job_id.fetch_add(1, Ordering::SeqCst);
                            migrations.push(MigrationJob::new(
                                job_id,
                                Arc::clone(&tiered_sstable.sstable),
                                *current_tier,
                                hotter_tier,
                            ));
                        }
                    }
                }

                // Check if should demote to colder tier
                if let Some(colder_tier) = current_tier.next_tier() {
                    if let Some(colder_config) = tiers_read.get(&colder_tier) {
                        if hotness <= colder_config.max_hotness
                            && colder_config.has_space(tiered_sstable.sstable.metadata().file_size)
                        {
                            let job_id = next_job_id.fetch_add(1, Ordering::SeqCst);
                            migrations.push(MigrationJob::new(
                                job_id,
                                Arc::clone(&tiered_sstable.sstable),
                                *current_tier,
                                colder_tier,
                            ));
                        }
                    }
                }
            }
        }

        // Queue migrations
        if !migrations.is_empty() {
            let mut queue = migration_queue.lock();
            for migration in migrations {
                queue.push_back(migration);
            }
        }

        Ok(())
    }

    /// Get tier statistics
    pub fn get_tier_stats(&self) -> HashMap<StorageTier, TierStats> {
        let mut result = HashMap::new();
        let tiers = self.tiers.read();
        let sstables = self.sstables_by_tier.read();

        for (tier, config) in tiers.iter() {
            let tier_sstables = sstables.get(tier).map(|s| s.len()).unwrap_or(0);
            let total_accesses = sstables
                .get(tier)
                .map(|sstables| {
                    sstables
                        .iter()
                        .map(|ts| ts.access_pattern.read_count + ts.access_pattern.write_count)
                        .sum()
                })
                .unwrap_or(0);

            result.insert(
                *tier,
                TierStats {
                    num_sstables: tier_sstables as u64,
                    total_size: config.current_size,
                    max_size: config.max_size,
                    utilization: config.utilization(),
                    total_accesses,
                    avg_hotness: sstables
                        .get(tier)
                        .map(|sstables| {
                            if sstables.is_empty() {
                                0.0
                            } else {
                                sstables
                                    .iter()
                                    .map(|ts| ts.access_pattern.hotness_score())
                                    .sum::<f64>()
                                    / sstables.len() as f64
                            }
                        })
                        .unwrap_or(0.0),
                    migrations_in: 0,
                    migrations_out: 0,
                },
            );
        }

        result
    }

    /// Force evaluation of all SSTables
    pub fn force_evaluation(&self) -> Result<()> {
        Self::evaluate_and_schedule_migrations(
            &self.tiers,
            &self.sstables_by_tier,
            &self.access_patterns,
            &self.migration_queue,
            &self.next_job_id,
            &self.stats,
        )
    }
}

/// Tier statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TierStats {
    pub num_sstables: u64,
    pub total_size: u64,
    pub max_size: u64,
    pub utilization: f64,
    pub total_accesses: u64,
    pub avg_hotness: f64,
    pub migrations_in: u64,
    pub migrations_out: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_storage_tier() {
        assert_eq!(StorageTier::Hot.name(), "hot");
        assert_eq!(StorageTier::Hot.next_tier(), Some(StorageTier::Warm));
        assert_eq!(StorageTier::Warm.previous_tier(), Some(StorageTier::Hot));
    }

    #[test]
    fn test_access_pattern() {
        let mut pattern = AccessPattern::default();

        pattern.record_read(100.0);
        assert_eq!(pattern.read_count, 1);
        assert_eq!(pattern.avg_latency, 100.0);

        pattern.record_write(200.0);
        assert_eq!(pattern.write_count, 1);
        assert_eq!(pattern.avg_latency, 150.0); // (100 + 200) / 2

        let score = pattern.hotness_score();
        assert!(score > 0.0 && score <= 100.0);
    }

    #[test]
    fn test_tier_config() {
        let dir = TempDir::new().unwrap();
        let config = TierConfig::hot_tier(dir.path().to_path_buf(), 1024 * 1024 * 1024);

        assert_eq!(config.tier, StorageTier::Hot);
        assert!(config.has_space(1024 * 1024));
        assert!(!config.has_space(2 * 1024 * 1024 * 1024));
        assert_eq!(config.utilization(), 0.0);
    }

    #[test]
    fn test_tiered_storage_manager() {
        let manager = TieredStorageManager::new(Duration::from_secs(60));

        let dir = TempDir::new().unwrap();
        let config = TierConfig::hot_tier(dir.path().to_path_buf(), 1024 * 1024 * 1024);

        assert!(manager.add_tier(config).is_ok());

        let stats = manager.get_tier_stats();
        assert!(stats.contains_key(&StorageTier::Hot));
    }
}
