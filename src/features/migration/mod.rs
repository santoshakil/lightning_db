use crate::core::error::DatabaseResult;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub mod batch_processor;
pub mod cli;
pub mod engine;
pub mod history;
pub mod implementations;
pub mod progress;
pub mod rollback;
pub mod runner;
pub mod schema;
pub mod templates;
pub mod transform;
pub mod validator;

pub use cli::*;
pub use history::*;
pub use progress::*;
pub use rollback::*;
pub use runner::*;
pub use schema::*;
pub use templates::*;
pub use transform::*;
pub use validator::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MigrationVersion(pub u64);

impl MigrationVersion {
    pub const INITIAL: Self = Self(0);

    pub fn new(version: u64) -> Self {
        Self(version)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn prev(&self) -> Option<Self> {
        if self.0 > 0 {
            Some(Self(self.0 - 1))
        } else {
            None
        }
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for MigrationVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationType {
    Schema,
    Data,
    Index,
    Maintenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationMode {
    Online,
    Offline,
    ZeroDowntime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStatus {
    Pending,
    Running,
    Completed,
    Failed,
    RolledBack,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationMetadata {
    pub version: MigrationVersion,
    pub name: String,
    pub description: String,
    pub migration_type: MigrationType,
    pub mode: MigrationMode,
    pub author: String,
    pub created_at: SystemTime,
    pub dependencies: Vec<MigrationVersion>,
    pub reversible: bool,
    pub estimated_duration: Option<Duration>,
    pub checksum: String,
}

#[derive(Debug, Clone)]
pub struct Migration {
    pub metadata: MigrationMetadata,
    pub up_script: String,
    pub down_script: Option<String>,
    pub validation_script: Option<String>,
}

pub trait MigrationStep: Send + Sync {
    fn execute(&self, ctx: &mut MigrationContext) -> DatabaseResult<()>;
    fn rollback(&self, ctx: &mut MigrationContext) -> DatabaseResult<()>;
    fn validate(&self, ctx: &MigrationContext) -> DatabaseResult<bool>;
    fn estimate_duration(&self) -> Option<Duration>;
}

#[derive(Debug)]
pub struct MigrationContext {
    pub database_path: String,
    pub current_version: MigrationVersion,
    pub target_version: MigrationVersion,
    pub dry_run: bool,
    pub force: bool,
    pub batch_size: usize,
    pub timeout: Option<Duration>,
    pub progress: Arc<RwLock<MigrationProgress>>,
    pub temp_data: HashMap<String, serde_json::Value>,
}

impl MigrationContext {
    pub fn new(
        database_path: String,
        current_version: MigrationVersion,
        target_version: MigrationVersion,
    ) -> Self {
        Self {
            database_path,
            current_version,
            target_version,
            dry_run: false,
            force: false,
            batch_size: 1000,
            timeout: Some(Duration::from_secs(300)),
            progress: Arc::new(RwLock::new(MigrationProgress::new())),
            temp_data: HashMap::new(),
        }
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn set_temp_data(&mut self, key: String, value: serde_json::Value) {
        self.temp_data.insert(key, value);
    }

    pub fn get_temp_data(&self, key: &str) -> Option<&serde_json::Value> {
        self.temp_data.get(key)
    }
}

#[derive(Debug, Default)]
pub struct MigrationManager {
    migrations: HashMap<MigrationVersion, Migration>,
    schema_manager: SchemaManager,
    history_manager: HistoryManager,
    validator: MigrationValidator,
    runner: MigrationRunner,
}

impl MigrationManager {
    pub fn new() -> Self {
        Self {
            migrations: HashMap::new(),
            schema_manager: SchemaManager::new(),
            history_manager: HistoryManager::new(),
            validator: MigrationValidator::new(),
            runner: MigrationRunner::new(),
        }
    }

    pub fn load_migrations_from_dir<P: AsRef<Path>>(&mut self, dir: P) -> DatabaseResult<()> {
        self.runner.load_migrations_from_dir(dir)
    }

    pub fn register_migration(&mut self, migration: Migration) -> DatabaseResult<()> {
        let version = migration.metadata.version;
        if self.migrations.contains_key(&version) {
            return Err(crate::core::error::DatabaseError::Migration(format!(
                "Migration version {} already exists",
                version
            )));
        }

        self.migrations.insert(version, migration);
        Ok(())
    }

    pub fn get_current_version(&self, db_path: &str) -> DatabaseResult<MigrationVersion> {
        self.schema_manager.get_current_version(db_path)
    }

    pub fn get_pending_migrations(
        &self,
        current_version: MigrationVersion,
        target_version: Option<MigrationVersion>,
    ) -> Vec<&Migration> {
        let target = target_version.unwrap_or_else(|| {
            self.migrations
                .keys()
                .max()
                .copied()
                .unwrap_or(MigrationVersion::INITIAL)
        });

        let mut pending: Vec<_> = self
            .migrations
            .iter()
            .filter(|(v, _)| **v > current_version && **v <= target)
            .map(|(_, m)| m)
            .collect();

        pending.sort_by_key(|m| m.metadata.version);
        pending
    }

    pub fn migrate_up(
        &mut self,
        ctx: &mut MigrationContext,
    ) -> DatabaseResult<Vec<MigrationVersion>> {
        self.runner.migrate_up(ctx, &self.migrations)
    }

    pub fn migrate_down(
        &mut self,
        ctx: &mut MigrationContext,
        steps: usize,
    ) -> DatabaseResult<Vec<MigrationVersion>> {
        self.runner.migrate_down(ctx, &self.migrations, steps)
    }

    pub fn migrate_to_version(
        &mut self,
        ctx: &mut MigrationContext,
        target_version: MigrationVersion,
    ) -> DatabaseResult<Vec<MigrationVersion>> {
        self.runner
            .migrate_to_version(ctx, &self.migrations, target_version)
    }

    pub fn validate_migration(
        &self,
        migration: &Migration,
        ctx: &MigrationContext,
    ) -> DatabaseResult<ValidationResult> {
        self.validator.validate_migration(migration, ctx)
    }

    pub fn get_migration_status(
        &self,
        db_path: &str,
    ) -> DatabaseResult<HashMap<MigrationVersion, MigrationStatus>> {
        self.history_manager.get_migration_statuses(db_path)
    }

    pub fn get_migration_history(
        &self,
        db_path: &str,
    ) -> DatabaseResult<Vec<MigrationHistoryEntry>> {
        self.history_manager.get_history(db_path)
    }

    pub fn rollback_migration(
        &mut self,
        ctx: &mut MigrationContext,
        version: MigrationVersion,
    ) -> DatabaseResult<()> {
        self.runner
            .rollback_migration(ctx, &self.migrations, version)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    pub migration_dir: String,
    pub backup_before_migration: bool,
    pub validate_before_migration: bool,
    pub auto_rollback_on_failure: bool,
    pub batch_size: usize,
    pub timeout_seconds: u64,
    pub max_concurrent_migrations: usize,
    pub enable_progress_tracking: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            migration_dir: "./migrations".to_string(),
            backup_before_migration: true,
            validate_before_migration: true,
            auto_rollback_on_failure: true,
            batch_size: 1000,
            timeout_seconds: 300,
            max_concurrent_migrations: 1,
            enable_progress_tracking: true,
        }
    }
}

pub fn calculate_checksum(content: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    content.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

pub fn timestamp_to_version(timestamp: SystemTime) -> MigrationVersion {
    MigrationVersion(
        timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}
