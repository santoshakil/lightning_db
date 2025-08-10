//! Schema Migration Framework for Lightning DB
//!
//! This module provides a comprehensive schema migration system that allows
//! database schemas to evolve over time while maintaining data integrity.
//!
//! Features:
//! - Versioned migrations with up/down support
//! - Atomic execution with automatic rollback
//! - Schema validation and compatibility checking
//! - Migration history tracking
//! - Type-safe schema definitions
//! - Testing framework for migrations

use crate::{Database, Error, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{error, info, warn};

pub mod executor;
pub mod history;
pub mod introspection;
pub mod migration;
pub mod testing;
pub mod validator;
pub mod versioning;

pub use executor::{ExecutionResult, MigrationExecutor};
pub use history::{MigrationHistory, MigrationRecord};
pub use introspection::{SchemaDiff, SchemaIntrospector};
pub use migration::{Migration, MigrationDirection, MigrationStep};
pub use validator::{SchemaValidator, ValidationResult};
pub use versioning::{SchemaVersion, VersionManager};

/// Schema migration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Enable automatic migrations on database open
    pub auto_migrate: bool,

    /// Allow destructive migrations (data loss)
    pub allow_destructive: bool,

    /// Migration timeout in seconds
    pub timeout_seconds: u64,

    /// Validate schema after each migration
    pub validate_after_migration: bool,

    /// Keep backup before migration
    pub backup_before_migration: bool,

    /// Migration batch size for large operations
    pub batch_size: usize,

    /// Retry failed migrations
    pub retry_on_failure: bool,

    /// Maximum retry attempts
    pub max_retries: u32,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            auto_migrate: false,
            allow_destructive: false,
            timeout_seconds: 300, // 5 minutes
            validate_after_migration: true,
            backup_before_migration: true,
            batch_size: 1000,
            retry_on_failure: false,
            max_retries: 3,
        }
    }
}

/// Schema definition for Lightning DB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema version
    pub version: SchemaVersion,

    /// Table definitions
    pub tables: BTreeMap<String, TableDefinition>,

    /// Index definitions
    pub indexes: BTreeMap<String, IndexDefinition>,

    /// Constraint definitions
    pub constraints: BTreeMap<String, ConstraintDefinition>,

    /// Custom metadata
    pub metadata: BTreeMap<String, String>,
}

/// Table definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    /// Table name
    pub name: String,

    /// Column definitions
    pub columns: Vec<ColumnDefinition>,

    /// Primary key columns
    pub primary_key: Vec<String>,

    /// Table options
    pub options: TableOptions,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,

    /// Data type
    pub data_type: DataType,

    /// Nullable
    pub nullable: bool,

    /// Default value
    pub default: Option<Value>,

    /// Column constraints
    pub constraints: Vec<ColumnConstraint>,
}

/// Data types supported by Lightning DB
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode)]
pub enum DataType {
    /// Variable-length byte array
    Bytes,

    /// UTF-8 string
    String,

    /// Signed integers
    Int8,
    Int16,
    Int32,
    Int64,

    /// Unsigned integers
    UInt8,
    UInt16,
    UInt32,
    UInt64,

    /// Floating point
    Float32,
    Float64,

    /// Boolean
    Boolean,

    /// Timestamp (nanoseconds since epoch)
    Timestamp,

    /// JSON data
    Json,

    /// Custom type with validation
    Custom(String),
}

/// Value types for defaults and constraints
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Bytes(Vec<u8>),
    String(String),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Boolean(bool),
    Timestamp(i64),
    Json(serde_json::Value),
}

/// Column constraints
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum ColumnConstraint {
    NotNull,
    Unique,
    Check(String), // Expression
    References { table: String, column: String },
}

/// Table options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableOptions {
    /// Enable compression
    pub compression: bool,

    /// Enable encryption
    pub encryption: bool,

    /// Custom storage engine
    pub storage_engine: Option<String>,

    /// Partitioning strategy
    pub partitioning: Option<PartitioningStrategy>,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            compression: true,
            encryption: false,
            storage_engine: None,
            partitioning: None,
        }
    }
}

/// Partitioning strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitioningStrategy {
    Range {
        column: String,
        ranges: Vec<(Value, Value)>,
    },
    Hash {
        column: String,
        buckets: u32,
    },
    List {
        column: String,
        values: Vec<Vec<Value>>,
    },
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Index name
    pub name: String,

    /// Table name
    pub table: String,

    /// Indexed columns
    pub columns: Vec<IndexColumn>,

    /// Index type
    pub index_type: IndexType,

    /// Index options
    pub options: IndexOptions,
}

/// Indexed column with sort order
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexColumn {
    pub name: String,
    pub order: SortOrder,
}

/// Sort order for indexes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

/// Index types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexType {
    BTree,
    Hash,
    FullText,
    Spatial,
}

/// Index options
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct IndexOptions {
    /// Unique index
    pub unique: bool,

    /// Partial index predicate
    pub predicate: Option<String>,

    /// Include columns (covering index)
    pub include: Vec<String>,
}

impl Default for IndexOptions {
    fn default() -> Self {
        Self {
            unique: false,
            predicate: None,
            include: Vec::new(),
        }
    }
}

/// Constraint definition
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ConstraintDefinition {
    /// Constraint name
    pub name: String,

    /// Table name
    pub table: String,

    /// Constraint type
    pub constraint_type: ConstraintType,
}

/// Constraint types
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum ConstraintType {
    PrimaryKey {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        references_table: String,
        references_columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
    },
    Check {
        expression: String,
    },
}

/// Schema migration manager
pub struct MigrationManager {
    config: MigrationConfig,
    version_manager: Arc<VersionManager>,
    executor: Arc<MigrationExecutor>,
    validator: Arc<SchemaValidator>,
    history: Arc<MigrationHistory>,
    introspector: Arc<SchemaIntrospector>,
    migrations: Arc<RwLock<BTreeMap<SchemaVersion, Box<dyn Migration>>>>,
}

impl MigrationManager {
    /// Create a new migration manager
    pub fn new(database: Arc<Database>, config: MigrationConfig) -> Result<Self> {
        let version_manager = Arc::new(VersionManager::new(database.clone())?);
        let executor = Arc::new(MigrationExecutor::new(database.clone(), config.clone())?);
        let validator = Arc::new(SchemaValidator::new(database.clone())?);
        let history = Arc::new(MigrationHistory::new(database.clone())?);
        let introspector = Arc::new(SchemaIntrospector::new(database.clone())?);

        Ok(Self {
            config,
            version_manager,
            executor,
            validator,
            history,
            introspector,
            migrations: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    /// Register a migration
    pub fn register_migration(&self, migration: Box<dyn Migration>) -> Result<()> {
        let version = migration.version();
        let mut migrations = self.migrations.write();

        if migrations.contains_key(&version) {
            return Err(Error::Config(format!(
                "Migration version {} already registered",
                version
            )));
        }

        migrations.insert(version.clone(), migration);
        info!("Registered migration version {}", version);

        Ok(())
    }

    /// Get current schema version
    pub fn current_version(&self) -> Result<SchemaVersion> {
        self.version_manager.current_version()
    }

    /// Get target version (latest available migration)
    pub fn target_version(&self) -> Result<SchemaVersion> {
        let migrations = self.migrations.read();
        migrations
            .keys()
            .last()
            .cloned()
            .ok_or_else(|| Error::Config("No migrations registered".to_string()))
    }

    /// Check if migrations are needed
    pub fn needs_migration(&self) -> Result<bool> {
        let current = self.current_version()?;
        let target = self.target_version()?;
        Ok(current < target)
    }

    /// Run pending migrations
    pub fn migrate(&self) -> Result<Vec<ExecutionResult>> {
        let current = self.current_version()?;
        let target = self.target_version()?;

        if current >= target {
            info!("Database is up to date at version {}", current);
            return Ok(Vec::new());
        }

        info!("Migrating from version {} to {}", current, target);

        // Get migrations to apply
        let pending: Vec<_> = {
            let migrations = self.migrations.read();
            migrations
                .range((
                    std::ops::Bound::Excluded(current.clone()),
                    std::ops::Bound::Included(target),
                ))
                .map(|(v, _)| v.clone())
                .collect()
        };

        // Execute migrations
        let mut results = Vec::new();

        for version in pending {
            let migrations = self.migrations.read();
            let migration = migrations
                .get(&version)
                .ok_or_else(|| Error::Generic(format!("Migration {} not found", version)))?;

            info!(
                "Applying migration {}: {}",
                version,
                migration.description()
            );

            // Validate pre-conditions
            if let Err(e) = self.validator.validate_preconditions(migration.as_ref()) {
                error!("Pre-condition validation failed: {}", e);
                return Err(e);
            }

            // Execute migration
            match self
                .executor
                .execute(migration.as_ref(), MigrationDirection::Up)
            {
                Ok(result) => {
                    // Update version
                    self.version_manager.set_version(&version)?;

                    // Record in history
                    self.history.record_migration(&version, &result)?;

                    // Validate post-conditions
                    if self.config.validate_after_migration {
                        if let Err(e) = self.validator.validate_postconditions(migration.as_ref()) {
                            error!("Post-condition validation failed: {}", e);
                            // Attempt rollback
                            self.rollback_to(&current)?;
                            return Err(e);
                        }
                    }

                    results.push(result);
                }
                Err(e) => {
                    error!("Migration failed: {}", e);

                    // Attempt rollback
                    if self.config.retry_on_failure {
                        warn!("Attempting rollback to version {}", current);
                        self.rollback_to(&current)?;
                    }

                    return Err(e);
                }
            }
        }

        info!("Migration completed successfully");
        Ok(results)
    }

    /// Rollback to a specific version
    pub fn rollback_to(&self, target: &SchemaVersion) -> Result<Vec<ExecutionResult>> {
        let current = self.current_version()?;

        if current <= *target {
            info!("Already at or below target version {}", target);
            return Ok(Vec::new());
        }

        info!("Rolling back from version {} to {}", current, target);

        // Get migrations to rollback
        let to_rollback: Vec<_> = {
            let migrations = self.migrations.read();
            migrations
                .range((
                    std::ops::Bound::Excluded(target.clone()),
                    std::ops::Bound::Included(current),
                ))
                .rev()
                .map(|(v, _)| v.clone())
                .collect()
        };

        // Execute rollbacks
        let mut results = Vec::new();

        for version in to_rollback {
            let migrations = self.migrations.read();
            let migration = migrations
                .get(&version)
                .ok_or_else(|| Error::Generic(format!("Migration {} not found", version)))?;

            info!(
                "Rolling back migration {}: {}",
                version,
                migration.description()
            );

            match self
                .executor
                .execute(migration.as_ref(), MigrationDirection::Down)
            {
                Ok(result) => {
                    // Update version to previous
                    let previous = self.version_manager.get_previous_version(&version)?;
                    self.version_manager.set_version(&previous)?;

                    // Record rollback in history
                    self.history.record_rollback(&version, &result)?;

                    results.push(result);
                }
                Err(e) => {
                    error!("Rollback failed: {}", e);
                    return Err(e);
                }
            }
        }

        info!("Rollback completed successfully");
        Ok(results)
    }

    /// Get migration history
    pub fn get_history(&self, limit: Option<usize>) -> Result<Vec<MigrationRecord>> {
        self.history.get_history(limit)
    }

    /// Validate current schema
    pub fn validate_schema(&self) -> Result<ValidationResult> {
        let current_schema = self.introspector.get_current_schema()?;
        self.validator.validate_schema(&current_schema)
    }

    /// Generate schema diff
    pub fn schema_diff(&self, from: &Schema, to: &Schema) -> Result<SchemaDiff> {
        self.introspector.diff_schemas(from, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_definition() {
        let schema = Schema {
            version: SchemaVersion::new(1, 0),
            tables: BTreeMap::new(),
            indexes: BTreeMap::new(),
            constraints: BTreeMap::new(),
            metadata: BTreeMap::new(),
        };

        assert_eq!(schema.version.major(), 1);
        assert_eq!(schema.version.minor(), 0);
    }

    #[test]
    fn test_data_types() {
        let dt = DataType::String;
        assert_eq!(dt, DataType::String);

        let dt = DataType::Custom("MyType".to_string());
        match dt {
            DataType::Custom(name) => assert_eq!(name, "MyType"),
            _ => panic!("Expected Custom type"),
        }
    }
}
