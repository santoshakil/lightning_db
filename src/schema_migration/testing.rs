//! Testing framework for schema migrations

use crate::{Database, LightningDbConfig, Error, Result};
use super::{Migration, MigrationManager, Schema, SchemaVersion, MigrationConfig};
use std::sync::Arc;
use tempfile::TempDir;
use serde::{Serialize, Deserialize};
use tracing::{info, warn};

/// Migration test harness
pub struct MigrationTestHarness {
    /// Temporary directory for test database
    temp_dir: TempDir,
    
    /// Test database instance
    database: Arc<Database>,
    
    /// Migration manager
    migration_manager: MigrationManager,
    
    /// Test configuration
    config: TestConfig,
}

/// Test configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConfig {
    /// Enable verbose logging
    pub verbose: bool,
    
    /// Run destructive tests
    pub test_destructive: bool,
    
    /// Test rollback scenarios
    pub test_rollback: bool,
    
    /// Test concurrent migrations
    pub test_concurrent: bool,
    
    /// Test large datasets
    pub test_large_data: bool,
    
    /// Data size for large tests
    pub large_data_rows: usize,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            verbose: false,
            test_destructive: true,
            test_rollback: true,
            test_concurrent: false,
            test_large_data: false,
            large_data_rows: 100_000,
        }
    }
}

/// Test result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    /// Test name
    pub name: String,
    
    /// Success status
    pub passed: bool,
    
    /// Error message if failed
    pub error: Option<String>,
    
    /// Duration in milliseconds
    pub duration_ms: u64,
    
    /// Additional metrics
    pub metrics: TestMetrics,
}

/// Test metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestMetrics {
    /// Data integrity checks passed
    pub integrity_checks: usize,
    
    /// Performance benchmarks
    pub performance_ms: Vec<u64>,
    
    /// Memory usage in bytes
    pub memory_usage: u64,
    
    /// Disk usage in bytes
    pub disk_usage: u64,
}

impl MigrationTestHarness {
    /// Create a new test harness
    pub fn new(config: TestConfig) -> Result<Self> {
        let temp_dir = TempDir::new()
            .map_err(|e| Error::Io(e.to_string()))?;
            
        let db_config = LightningDbConfig {
            page_size: 4096,
            cache_size: 10 * 1024 * 1024, // 10MB for testing
            ..Default::default()
        };
        
        let database = Arc::new(Database::create(temp_dir.path(), db_config)?);
        
        let migration_config = MigrationConfig {
            auto_migrate: false,
            allow_destructive: config.test_destructive,
            timeout_seconds: 60,
            validate_after_migration: true,
            backup_before_migration: false, // Disable for tests
            batch_size: 1000,
            retry_on_failure: false,
            max_retries: 0,
        };
        
        let migration_manager = MigrationManager::new(database.clone(), migration_config)?;
        
        Ok(Self {
            temp_dir,
            database,
            migration_manager,
            config,
        })
    }
    
    /// Test a single migration
    pub fn test_migration(&mut self, migration: Box<dyn Migration>) -> Result<TestResult> {
        let start = std::time::Instant::now();
        let test_name = format!("Migration to {}", migration.version());
        
        info!("Testing migration: {}", test_name);
        
        let mut metrics = TestMetrics {
            integrity_checks: 0,
            performance_ms: Vec::new(),
            memory_usage: 0,
            disk_usage: 0,
        };
        
        // Register migration
        self.migration_manager.register_migration(migration)?;
        
        // Test forward migration
        let forward_result = self.test_forward_migration(&mut metrics)?;
        if !forward_result {
            return Ok(TestResult {
                name: test_name,
                passed: false,
                error: Some("Forward migration failed".to_string()),
                duration_ms: start.elapsed().as_millis() as u64,
                metrics,
            });
        }
        
        // Test rollback if configured
        if self.config.test_rollback {
            let rollback_result = self.test_rollback_migration(&mut metrics)?;
            if !rollback_result {
                return Ok(TestResult {
                    name: test_name,
                    passed: false,
                    error: Some("Rollback migration failed".to_string()),
                    duration_ms: start.elapsed().as_millis() as u64,
                    metrics,
                });
            }
        }
        
        // Test with data if configured
        if self.config.test_large_data {
            let data_result = self.test_with_data(&mut metrics)?;
            if !data_result {
                return Ok(TestResult {
                    name: test_name,
                    passed: false,
                    error: Some("Migration with data failed".to_string()),
                    duration_ms: start.elapsed().as_millis() as u64,
                    metrics,
                });
            }
        }
        
        Ok(TestResult {
            name: test_name,
            passed: true,
            error: None,
            duration_ms: start.elapsed().as_millis() as u64,
            metrics,
        })
    }
    
    /// Test multiple migrations in sequence
    pub fn test_migration_sequence(
        &mut self,
        migrations: Vec<Box<dyn Migration>>,
    ) -> Result<Vec<TestResult>> {
        let mut results = Vec::new();
        
        for migration in migrations {
            let result = self.test_migration(migration)?;
            let passed = result.passed;
            results.push(result);
            
            if !passed {
                break; // Stop on first failure
            }
        }
        
        Ok(results)
    }
    
    /// Test forward migration
    fn test_forward_migration(&mut self, metrics: &mut TestMetrics) -> Result<bool> {
        let start = std::time::Instant::now();
        
        // Run migration
        let results = self.migration_manager.migrate()?;
        
        metrics.performance_ms.push(start.elapsed().as_millis() as u64);
        
        // Verify migration succeeded
        for result in &results {
            if !result.success {
                warn!("Migration {} failed: {:?}", result.version, result.error);
                return Ok(false);
            }
        }
        
        // Run integrity checks
        let integrity_ok = self.verify_data_integrity()?;
        metrics.integrity_checks += 1;
        
        Ok(integrity_ok)
    }
    
    /// Test rollback migration
    fn test_rollback_migration(&mut self, metrics: &mut TestMetrics) -> Result<bool> {
        let start = std::time::Instant::now();
        
        // Get current version
        let current_version = self.migration_manager.current_version()?;
        
        // Rollback to previous version
        let previous_version = if current_version.patch() > 0 {
            SchemaVersion::with_patch(
                current_version.major(),
                current_version.minor(),
                current_version.patch() - 1,
            )
        } else if current_version.minor() > 0 {
            SchemaVersion::new(current_version.major(), current_version.minor() - 1)
        } else if current_version.major() > 0 {
            SchemaVersion::new(current_version.major() - 1, 0)
        } else {
            return Ok(true); // Can't rollback from 0.0.0
        };
        
        let results = self.migration_manager.rollback_to(&previous_version)?;
        
        metrics.performance_ms.push(start.elapsed().as_millis() as u64);
        
        // Verify rollback succeeded
        for result in &results {
            if !result.success {
                warn!("Rollback {} failed: {:?}", result.version, result.error);
                return Ok(false);
            }
        }
        
        // Run integrity checks
        let integrity_ok = self.verify_data_integrity()?;
        metrics.integrity_checks += 1;
        
        // Re-apply migration
        let results = self.migration_manager.migrate()?;
        for result in &results {
            if !result.success {
                return Ok(false);
            }
        }
        
        Ok(integrity_ok)
    }
    
    /// Test migration with data
    fn test_with_data(&mut self, metrics: &mut TestMetrics) -> Result<bool> {
        info!("Testing migration with {} rows of data", self.config.large_data_rows);
        
        // Insert test data
        let start = std::time::Instant::now();
        for i in 0..self.config.large_data_rows {
            let key = format!("test_key_{:08}", i);
            let value = format!("test_value_{}", i);
            self.database.put(key.as_bytes(), value.as_bytes())?;
            
            if i % 10000 == 0 && self.config.verbose {
                info!("Inserted {} rows", i);
            }
        }
        
        let insert_time = start.elapsed().as_millis() as u64;
        metrics.performance_ms.push(insert_time);
        
        // Verify data integrity
        let integrity_ok = self.verify_data_integrity()?;
        metrics.integrity_checks += 1;
        
        // Get resource usage
        metrics.memory_usage = self.get_memory_usage();
        metrics.disk_usage = self.get_disk_usage()?;
        
        Ok(integrity_ok)
    }
    
    /// Verify data integrity
    fn verify_data_integrity(&self) -> Result<bool> {
        // Check schema is valid
        let validation_result = self.migration_manager.validate_schema()?;
        if !validation_result.is_valid {
            warn!("Schema validation failed: {:?}", validation_result.errors);
            return Ok(false);
        }
        
        // Check database is consistent
        // This would include checking indexes, constraints, etc.
        
        Ok(true)
    }
    
    /// Get current memory usage
    fn get_memory_usage(&self) -> u64 {
        // In a real implementation, this would query actual memory usage
        // For now, return a placeholder
        50 * 1024 * 1024 // 50MB
    }
    
    /// Get disk usage
    fn get_disk_usage(&self) -> Result<u64> {
        let mut total_size = 0u64;
        
        for entry in std::fs::read_dir(self.temp_dir.path())? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                total_size += metadata.len();
            }
        }
        
        Ok(total_size)
    }
    
    /// Create a snapshot for comparison
    pub fn create_snapshot(&self) -> Result<DatabaseSnapshot> {
        let mut data = Vec::new();
        
        // Capture all key-value pairs
        let scan_result = self.database.scan(None, None)?;
        for item in scan_result {
            let (key, value) = item?;
            data.push((key, value));
        }
        
        Ok(DatabaseSnapshot {
            version: self.migration_manager.current_version()?,
            data,
            timestamp: std::time::SystemTime::now(),
        })
    }
    
    /// Compare two snapshots
    pub fn compare_snapshots(
        &self,
        before: &DatabaseSnapshot,
        after: &DatabaseSnapshot,
    ) -> SnapshotDiff {
        let before_map: std::collections::HashMap<_, _> = before.data.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let after_map: std::collections::HashMap<_, _> = after.data.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
            
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut modified = Vec::new();
        
        // Find added and modified
        for (key, value) in &after_map {
            match before_map.get(key) {
                None => added.push(key.clone()),
                Some(old_value) if old_value != value => modified.push(key.clone()),
                _ => {}
            }
        }
        
        // Find removed
        for key in before_map.keys() {
            if !after_map.contains_key(key) {
                removed.push(key.clone());
            }
        }
        
        SnapshotDiff {
            version_before: before.version.clone(),
            version_after: after.version.clone(),
            added,
            removed,
            modified,
        }
    }
}

/// Database snapshot for testing
#[derive(Debug, Clone)]
pub struct DatabaseSnapshot {
    pub version: SchemaVersion,
    pub data: Vec<(Vec<u8>, Vec<u8>)>,
    pub timestamp: std::time::SystemTime,
}

/// Snapshot difference
#[derive(Debug, Clone)]
pub struct SnapshotDiff {
    pub version_before: SchemaVersion,
    pub version_after: SchemaVersion,
    pub added: Vec<Vec<u8>>,
    pub removed: Vec<Vec<u8>>,
    pub modified: Vec<Vec<u8>>,
}

/// Migration test builder
pub struct TestMigrationBuilder {
    version: SchemaVersion,
    description: String,
    test_data: Vec<(Vec<u8>, Vec<u8>)>,
}

impl TestMigrationBuilder {
    /// Create a new test migration builder
    pub fn new(version: SchemaVersion, description: impl Into<String>) -> Self {
        Self {
            version,
            description: description.into(),
            test_data: Vec::new(),
        }
    }
    
    /// Add test data
    pub fn with_test_data(mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        self.test_data.push((key.as_ref().to_vec(), value.as_ref().to_vec()));
        self
    }
    
    /// Build a test migration
    pub fn build(self) -> TestMigration {
        TestMigration {
            version: self.version,
            description: self.description,
            test_data: self.test_data,
        }
    }
}

/// Simple test migration
pub struct TestMigration {
    version: SchemaVersion,
    description: String,
    test_data: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Migration for TestMigration {
    fn version(&self) -> SchemaVersion {
        self.version.clone()
    }
    
    fn description(&self) -> &str {
        &self.description
    }
    
    fn steps(&self) -> Vec<super::MigrationStep> {
        Vec::new() // No schema changes for test
    }
    
    fn target_schema(&self) -> Schema {
        Schema {
            version: self.version.clone(),
            tables: Default::default(),
            indexes: Default::default(),
            constraints: Default::default(),
            metadata: Default::default(),
        }
    }
    
    fn migrate_up(&self, database: &Database) -> Result<()> {
        // Insert test data
        for (key, value) in &self.test_data {
            database.put(key, value)?;
        }
        Ok(())
    }
    
    fn migrate_down(&self, database: &Database) -> Result<()> {
        // Remove test data
        for (key, _) in &self.test_data {
            database.delete(key)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_harness_creation() {
        let config = TestConfig::default();
        let harness = MigrationTestHarness::new(config);
        assert!(harness.is_ok());
    }
    
    #[test]
    fn test_simple_migration() {
        let config = TestConfig::default();
        let mut harness = MigrationTestHarness::new(config).unwrap();
        
        let migration = TestMigrationBuilder::new(
            SchemaVersion::new(1, 0),
            "Test migration"
        )
        .with_test_data(b"key1", b"value1")
        .with_test_data(b"key2", b"value2")
        .build();
        
        let result = harness.test_migration(Box::new(migration)).unwrap();
        assert!(result.passed);
        assert!(result.error.is_none());
    }
}