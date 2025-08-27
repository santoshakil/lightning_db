use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::{HashMap, BTreeMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{RwLock, Semaphore, mpsc};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Error, Debug)]
pub enum MigrationError {
    #[error("Migration version conflict: {0}")]
    VersionConflict(String),
    #[error("Schema validation failed: {0}")]
    SchemaValidationFailed(String),
    #[error("Data transformation error: {0}")]
    TransformationError(String),
    #[error("Rollback failed: {0}")]
    RollbackFailed(String),
    #[error("Checkpoint creation failed: {0}")]
    CheckpointFailed(String),
    #[error("Dependency not met: {0}")]
    DependencyNotMet(String),
    #[error("Migration already in progress")]
    AlreadyInProgress,
    #[error("Migration timeout exceeded")]
    Timeout,
    #[error("Data integrity check failed: {0}")]
    IntegrityCheckFailed(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type MigrationResult<T> = Result<T, MigrationError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub build: Option<String>,
}

impl MigrationVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            build: None,
        }
    }
    
    pub fn from_string(s: &str) -> MigrationResult<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() < 3 {
            return Err(MigrationError::VersionConflict(
                format!("Invalid version format: {}", s)
            ));
        }
        
        Ok(Self {
            major: parts[0].parse().map_err(|_| 
                MigrationError::VersionConflict("Invalid major version".to_string()))?,
            minor: parts[1].parse().map_err(|_| 
                MigrationError::VersionConflict("Invalid minor version".to_string()))?,
            patch: parts[2].parse().map_err(|_| 
                MigrationError::VersionConflict("Invalid patch version".to_string()))?,
            build: parts.get(3).map(|s| s.to_string()),
        })
    }
    
    pub fn to_string(&self) -> String {
        if let Some(build) = &self.build {
            format!("{}.{}.{}.{}", self.major, self.minor, self.patch, build)
        } else {
            format!("{}.{}.{}", self.major, self.minor, self.patch)
        }
    }
    
    pub fn is_compatible(&self, other: &MigrationVersion) -> bool {
        self.major == other.major && self.minor >= other.minor
    }
}

impl PartialOrd for MigrationVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MigrationVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.major.cmp(&other.major) {
            std::cmp::Ordering::Equal => {
                match self.minor.cmp(&other.minor) {
                    std::cmp::Ordering::Equal => self.patch.cmp(&other.patch),
                    ord => ord,
                }
            }
            ord => ord,
        }
    }
}

impl PartialEq for MigrationVersion {
    fn eq(&self, other: &Self) -> bool {
        self.major == other.major && 
        self.minor == other.minor && 
        self.patch == other.patch
    }
}

impl Eq for MigrationVersion {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationType {
    Schema,
    Data,
    Index,
    Full,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationMetadata {
    pub id: String,
    pub name: String,
    pub description: String,
    pub version: MigrationVersion,
    pub migration_type: MigrationType,
    pub dependencies: Vec<String>,
    pub author: String,
    pub created_at: DateTime<Utc>,
    pub estimated_duration: Duration,
    pub reversible: bool,
    pub checksum: String,
}

#[async_trait]
pub trait Migration: Send + Sync {
    fn metadata(&self) -> &MigrationMetadata;
    
    async fn validate(&self, context: &MigrationContext) -> MigrationResult<()>;
    
    async fn execute(&self, context: &mut MigrationContext) -> MigrationResult<()>;
    
    async fn rollback(&self, context: &mut MigrationContext) -> MigrationResult<()>;
    
    async fn verify(&self, context: &MigrationContext) -> MigrationResult<()>;
    
    fn is_destructive(&self) -> bool {
        false
    }
    
    fn supports_dry_run(&self) -> bool {
        true
    }
}

pub struct MigrationContext {
    pub source_path: PathBuf,
    pub target_path: PathBuf,
    pub temp_path: PathBuf,
    pub batch_size: usize,
    pub parallel_workers: usize,
    pub progress: Arc<MigrationProgress>,
    pub checkpoints: Arc<RwLock<Vec<MigrationCheckpoint>>>,
    pub transformers: Arc<RwLock<HashMap<String, Box<dyn DataTransformer>>>>,
    pub validators: Arc<RwLock<Vec<Box<dyn DataValidator>>>>,
    pub options: MigrationOptions,
    pub metrics: Arc<MigrationMetrics>,
}

impl MigrationContext {
    pub async fn create_checkpoint(&self, name: &str) -> MigrationResult<MigrationCheckpoint> {
        let checkpoint = MigrationCheckpoint {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            timestamp: Utc::now(),
            progress_snapshot: self.progress.snapshot(),
            data_snapshot: self.create_data_snapshot().await?,
        };
        
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.push(checkpoint.clone());
        
        Ok(checkpoint)
    }
    
    async fn create_data_snapshot(&self) -> MigrationResult<DataSnapshot> {
        Ok(DataSnapshot {
            path: self.source_path.clone(),
            size_bytes: 0,
            checksum: String::new(),
            metadata: HashMap::new(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationCheckpoint {
    pub id: String,
    pub name: String,
    pub timestamp: DateTime<Utc>,
    pub progress_snapshot: ProgressSnapshot,
    pub data_snapshot: DataSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSnapshot {
    pub path: PathBuf,
    pub size_bytes: u64,
    pub checksum: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressSnapshot {
    pub total_items: u64,
    pub processed_items: u64,
    pub failed_items: u64,
    pub skipped_items: u64,
    pub percentage: f64,
}

pub struct MigrationProgress {
    total_items: AtomicU64,
    processed_items: AtomicU64,
    failed_items: AtomicU64,
    skipped_items: AtomicU64,
    start_time: Instant,
    is_running: AtomicBool,
}

impl MigrationProgress {
    pub fn new(total_items: u64) -> Self {
        Self {
            total_items: AtomicU64::new(total_items),
            processed_items: AtomicU64::new(0),
            failed_items: AtomicU64::new(0),
            skipped_items: AtomicU64::new(0),
            start_time: Instant::now(),
            is_running: AtomicBool::new(true),
        }
    }
    
    pub fn increment_processed(&self) {
        self.processed_items.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_failed(&self) {
        self.failed_items.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_skipped(&self) {
        self.skipped_items.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn get_percentage(&self) -> f64 {
        let total = self.total_items.load(Ordering::Relaxed) as f64;
        let processed = self.processed_items.load(Ordering::Relaxed) as f64;
        
        if total > 0.0 {
            (processed / total) * 100.0
        } else {
            0.0
        }
    }
    
    pub fn get_eta(&self) -> Option<Duration> {
        let processed = self.processed_items.load(Ordering::Relaxed);
        let total = self.total_items.load(Ordering::Relaxed);
        
        if processed == 0 || processed >= total {
            return None;
        }
        
        let elapsed = self.start_time.elapsed();
        let rate = processed as f64 / elapsed.as_secs_f64();
        let remaining = (total - processed) as f64;
        
        Some(Duration::from_secs_f64(remaining / rate))
    }
    
    pub fn snapshot(&self) -> ProgressSnapshot {
        ProgressSnapshot {
            total_items: self.total_items.load(Ordering::Relaxed),
            processed_items: self.processed_items.load(Ordering::Relaxed),
            failed_items: self.failed_items.load(Ordering::Relaxed),
            skipped_items: self.skipped_items.load(Ordering::Relaxed),
            percentage: self.get_percentage(),
        }
    }
    
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationOptions {
    pub dry_run: bool,
    pub force: bool,
    pub verify_data: bool,
    pub create_backup: bool,
    pub compression: bool,
    pub encryption: bool,
    pub preserve_timestamps: bool,
    pub continue_on_error: bool,
    pub max_retries: usize,
    pub timeout: Option<Duration>,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            force: false,
            verify_data: true,
            create_backup: true,
            compression: false,
            encryption: false,
            preserve_timestamps: true,
            continue_on_error: false,
            max_retries: 3,
            timeout: None,
        }
    }
}

#[async_trait]
pub trait DataTransformer: Send + Sync {
    async fn transform(&self, data: Vec<u8>) -> MigrationResult<Vec<u8>>;
    
    fn name(&self) -> &str;
    
    fn description(&self) -> &str;
}

#[async_trait]
pub trait DataValidator: Send + Sync {
    async fn validate(&self, data: &[u8]) -> MigrationResult<()>;
    
    fn name(&self) -> &str;
}

pub struct MigrationMetrics {
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub transformations_applied: AtomicU64,
    pub validations_performed: AtomicU64,
    pub errors_encountered: AtomicU64,
    pub warnings_generated: AtomicU64,
    pub start_time: Instant,
}

impl MigrationMetrics {
    pub fn new() -> Self {
        Self {
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            transformations_applied: AtomicU64::new(0),
            validations_performed: AtomicU64::new(0),
            errors_encountered: AtomicU64::new(0),
            warnings_generated: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
    
    pub fn throughput_mbps(&self) -> f64 {
        let bytes = self.bytes_written.load(Ordering::Relaxed) as f64;
        let elapsed = self.start_time.elapsed().as_secs_f64();
        
        if elapsed > 0.0 {
            (bytes / 1_048_576.0) / elapsed
        } else {
            0.0
        }
    }
    
    pub fn summary(&self) -> MigrationSummary {
        MigrationSummary {
            duration: self.start_time.elapsed(),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            transformations_applied: self.transformations_applied.load(Ordering::Relaxed),
            validations_performed: self.validations_performed.load(Ordering::Relaxed),
            errors_encountered: self.errors_encountered.load(Ordering::Relaxed),
            warnings_generated: self.warnings_generated.load(Ordering::Relaxed),
            throughput_mbps: self.throughput_mbps(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationSummary {
    pub duration: Duration,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub transformations_applied: u64,
    pub validations_performed: u64,
    pub errors_encountered: u64,
    pub warnings_generated: u64,
    pub throughput_mbps: f64,
}

pub struct MigrationEngine {
    migrations: Arc<RwLock<BTreeMap<MigrationVersion, Box<dyn Migration>>>>,
    history: Arc<RwLock<Vec<MigrationHistoryEntry>>>,
    current_version: Arc<RwLock<Option<MigrationVersion>>>,
    execution_semaphore: Arc<Semaphore>,
    is_running: Arc<AtomicBool>,
}

impl MigrationEngine {
    pub fn new() -> Self {
        Self {
            migrations: Arc::new(RwLock::new(BTreeMap::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            current_version: Arc::new(RwLock::new(None)),
            execution_semaphore: Arc::new(Semaphore::new(1)),
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }
    
    pub async fn register_migration(&self, migration: Box<dyn Migration>) -> MigrationResult<()> {
        let metadata = migration.metadata();
        let version = metadata.version.clone();
        
        let mut migrations = self.migrations.write().await;
        if migrations.contains_key(&version) {
            return Err(MigrationError::VersionConflict(
                format!("Migration version {} already registered", version.to_string())
            ));
        }
        
        migrations.insert(version, migration);
        Ok(())
    }
    
    pub async fn execute_migration(
        &self,
        target_version: MigrationVersion,
        context: &mut MigrationContext,
    ) -> MigrationResult<MigrationSummary> {
        let _permit = self.execution_semaphore.acquire().await
            .map_err(|_| MigrationError::AlreadyInProgress)?;
        
        if self.is_running.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::SeqCst
        ).is_err() {
            return Err(MigrationError::AlreadyInProgress);
        }
        
        let result = self.execute_migration_internal(target_version, context).await;
        
        self.is_running.store(false, Ordering::SeqCst);
        
        result
    }
    
    async fn execute_migration_internal(
        &self,
        target_version: MigrationVersion,
        context: &mut MigrationContext,
    ) -> MigrationResult<MigrationSummary> {
        let migrations = self.migrations.read().await;
        let current_version = self.current_version.read().await.clone();
        
        let migration_path = self.determine_migration_path(
            current_version.as_ref(),
            &target_version,
            &migrations
        )?;
        
        for version in migration_path {
            let migration = migrations.get(&version)
                .ok_or_else(|| MigrationError::VersionConflict(
                    format!("Migration {} not found", version.to_string())
                ))?;
            
            self.execute_single_migration(migration.as_ref(), context).await?;
            
            let mut current = self.current_version.write().await;
            *current = Some(version.clone());
            
            self.record_history(migration.metadata(), true).await;
        }
        
        Ok(context.metrics.summary())
    }
    
    async fn execute_single_migration(
        &self,
        migration: &dyn Migration,
        context: &mut MigrationContext,
    ) -> MigrationResult<()> {
        let metadata = migration.metadata();
        
        println!("Executing migration: {} ({})", metadata.name, metadata.version.to_string());
        
        migration.validate(context).await?;
        
        if context.options.dry_run && migration.supports_dry_run() {
            println!("Dry run completed successfully");
            return Ok(());
        }
        
        if context.options.create_backup {
            context.create_checkpoint("pre_migration").await?;
        }
        
        migration.execute(context).await?;
        
        if context.options.verify_data {
            migration.verify(context).await?;
        }
        
        Ok(())
    }
    
    fn determine_migration_path(
        &self,
        current: Option<&MigrationVersion>,
        target: &MigrationVersion,
        migrations: &BTreeMap<MigrationVersion, Box<dyn Migration>>,
    ) -> MigrationResult<Vec<MigrationVersion>> {
        let mut path = Vec::new();
        
        let start_version = current.cloned().unwrap_or_else(|| 
            MigrationVersion::new(0, 0, 0)
        );
        
        for (version, _) in migrations.iter() {
            if version > &start_version && version <= target {
                path.push(version.clone());
            }
        }
        
        if path.is_empty() && &start_version != target {
            return Err(MigrationError::VersionConflict(
                "No migration path found".to_string()
            ));
        }
        
        Ok(path)
    }
    
    async fn record_history(&self, metadata: &MigrationMetadata, success: bool) {
        let entry = MigrationHistoryEntry {
            id: uuid::Uuid::new_v4().to_string(),
            migration_id: metadata.id.clone(),
            version: metadata.version.clone(),
            executed_at: Utc::now(),
            success,
            duration: metadata.estimated_duration,
            error_message: None,
        };
        
        let mut history = self.history.write().await;
        history.push(entry);
    }
    
    pub async fn rollback_to_version(
        &self,
        target_version: MigrationVersion,
        context: &mut MigrationContext,
    ) -> MigrationResult<()> {
        let current_version = self.current_version.read().await.clone()
            .ok_or_else(|| MigrationError::VersionConflict("No current version".to_string()))?;
        
        if target_version >= current_version {
            return Err(MigrationError::RollbackFailed(
                "Cannot rollback to a future version".to_string()
            ));
        }
        
        let migrations = self.migrations.read().await;
        let rollback_path = self.determine_rollback_path(
            &current_version,
            &target_version,
            &migrations
        )?;
        
        for version in rollback_path {
            let migration = migrations.get(&version)
                .ok_or_else(|| MigrationError::VersionConflict(
                    format!("Migration {} not found", version.to_string())
                ))?;
            
            if !migration.metadata().reversible {
                return Err(MigrationError::RollbackFailed(
                    format!("Migration {} is not reversible", version.to_string())
                ));
            }
            
            migration.rollback(context).await?;
            
            self.record_history(migration.metadata(), false).await;
        }
        
        let mut current = self.current_version.write().await;
        *current = Some(target_version);
        
        Ok(())
    }
    
    fn determine_rollback_path(
        &self,
        current: &MigrationVersion,
        target: &MigrationVersion,
        migrations: &BTreeMap<MigrationVersion, Box<dyn Migration>>,
    ) -> MigrationResult<Vec<MigrationVersion>> {
        let mut path = Vec::new();
        
        for (version, _) in migrations.iter().rev() {
            if version <= current && version > target {
                path.push(version.clone());
            }
        }
        
        Ok(path)
    }
    
    pub async fn get_pending_migrations(&self) -> Vec<MigrationMetadata> {
        let migrations = self.migrations.read().await;
        let current_version = self.current_version.read().await.clone();
        
        let start_version = current_version.unwrap_or_else(|| 
            MigrationVersion::new(0, 0, 0)
        );
        
        migrations.iter()
            .filter(|(version, _)| **version > start_version)
            .map(|(_, migration)| migration.metadata().clone())
            .collect()
    }
    
    pub async fn get_migration_history(&self) -> Vec<MigrationHistoryEntry> {
        self.history.read().await.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationHistoryEntry {
    pub id: String,
    pub migration_id: String,
    pub version: MigrationVersion,
    pub executed_at: DateTime<Utc>,
    pub success: bool,
    pub duration: Duration,
    pub error_message: Option<String>,
}