//! Comprehensive Migration Examples for Lightning DB
//!
//! This module provides complete, real-world migration scenarios that demonstrate
//! how to properly manage schema evolution in production Lightning DB deployments.
//!
//! Each example includes:
//! - Complete migration code
//! - Data validation and testing
//! - Rollback procedures
//! - Performance considerations
//! - Production best practices

pub mod e01_initial_schema;
pub mod e02_add_user_profiles;
pub mod e03_add_indexes_optimization;
pub mod e04_data_type_evolution;
pub mod e05_table_restructuring;
pub mod e06_complex_data_migration;
pub mod e07_partitioning_strategy;
pub mod e08_zero_downtime_changes;
pub mod e09_performance_tuning;
pub mod e10_production_rollback;

use lightning_db::{
    Database, LightningDbConfig, Result,
    schema_migration::{
        MigrationManager, MigrationConfig, SchemaVersion,
        migration::{Migration, MigrationBuilder, SimpleMigration},
    },
};
use std::sync::Arc;

/// Common utilities for migration examples
pub struct MigrationExampleRunner {
    pub database: Arc<Database>,
    pub manager: MigrationManager,
}

impl MigrationExampleRunner {
    /// Create a new migration example runner
    pub fn new(db_path: &str) -> Result<Self> {
        let config = LightningDbConfig {
            cache_size: 100 * 1024 * 1024, // 100MB cache
            compression_enabled: true,
            ..Default::default()
        };

        let database = Arc::new(Database::create(db_path, config)?);
        
        let migration_config = MigrationConfig {
            auto_migrate: false,
            allow_destructive: false,
            timeout_seconds: 300,
            validate_after_migration: true,
            backup_before_migration: true,
            batch_size: 1000,
            retry_on_failure: true,
            max_retries: 3,
        };

        let manager = MigrationManager::new(database.clone(), migration_config)?;

        Ok(Self { database, manager })
    }

    /// Run a migration and validate results
    pub fn run_migration_with_validation<M: Migration + 'static>(
        &self,
        migration: M,
        validation_fn: Box<dyn Fn(&Database) -> Result<()>>,
    ) -> Result<()> {
        println!("🚀 Running migration: {}", migration.description());
        println!("   Version: {}", migration.version());
        println!("   Destructive: {}", migration.is_destructive());

        // Register and run migration
        self.manager.register_migration(Box::new(migration))?;
        let results = self.manager.migrate()?;

        for result in &results {
            println!("✅ Applied: {} in {}ms", result.version, result.duration_ms);
        }

        // Run validation
        println!("🔍 Validating migration results...");
        validation_fn(&self.database)?;
        println!("✅ Validation passed!");

        // Validate schema integrity
        let validation_result = self.manager.validate_schema()?;
        if !validation_result.is_valid {
            println!("❌ Schema validation failed:");
            for error in &validation_result.errors {
                println!("   - {}", error.message);
            }
            return Err(lightning_db::Error::Validation(
                "Schema validation failed after migration".to_string()
            ));
        }

        println!("✅ Schema validation passed!");
        Ok(())
    }

    /// Demonstrate rollback procedure
    pub fn demonstrate_rollback(&self, target_version: SchemaVersion) -> Result<()> {
        println!("⏪ Demonstrating rollback to version {}", target_version);
        
        let current = self.manager.current_version()?;
        println!("   Current version: {}", current);

        if current <= target_version {
            println!("   Already at target version or below");
            return Ok(());
        }

        let results = self.manager.rollback_to(&target_version)?;
        for result in &results {
            println!("✅ Rolled back: {} in {}ms", result.version, result.duration_ms);
        }

        println!("✅ Rollback completed successfully!");
        Ok(())
    }

    /// Show migration history
    pub fn show_history(&self, limit: Option<usize>) -> Result<()> {
        let history = self.manager.get_history(limit)?;
        
        println!("📚 Migration History:");
        println!("=====================");
        
        if history.is_empty() {
            println!("No migration history found.");
            return Ok(());
        }

        for (i, record) in history.iter().enumerate() {
            println!("{}. Version {} - {} ({})", 
                i + 1, 
                record.version, 
                record.description,
                if record.success { "✅" } else { "❌" }
            );
            println!("   Direction: {:?}", record.direction);
            println!("   Duration: {}ms", record.duration_ms);
            if let Some(error) = &record.error {
                println!("   Error: {}", error);
            }
            println!();
        }

        Ok(())
    }

    /// Insert sample data for testing
    pub fn insert_sample_data(&self, table_prefix: &str, count: usize) -> Result<()> {
        println!("📝 Inserting {} sample records...", count);
        
        for i in 0..count {
            let key = format!("{}_{:06}", table_prefix, i);
            let value = format!("{{\"id\": {}, \"name\": \"Sample {}\", \"created_at\": {}}}", 
                i, i, chrono::Utc::now().timestamp());
            self.database.put(key.as_bytes(), value.as_bytes())?;
        }
        
        println!("✅ Inserted {} records", count);
        Ok(())
    }

    /// Verify data integrity after migration
    pub fn verify_data_integrity(&self, table_prefix: &str, expected_count: usize) -> Result<()> {
        println!("🔍 Verifying data integrity for table '{}'...", table_prefix);
        
        let scan_result = self.database.scan(
            Some(format!("{}_", table_prefix).as_bytes().to_vec()),
            Some(format!("{}~", table_prefix).as_bytes().to_vec()),
        )?;

        let mut count = 0;
        for item in scan_result {
            let (_key, value) = item?;
            
            // Verify JSON structure
            let _: serde_json::Value = serde_json::from_slice(&value)
                .map_err(|e| lightning_db::Error::Parse(format!("Invalid JSON: {}", e)))?;
            
            count += 1;
        }

        if count != expected_count {
            return Err(lightning_db::Error::Validation(
                format!("Expected {} records, found {}", expected_count, count)
            ));
        }

        println!("✅ Data integrity verified: {} records", count);
        Ok(())
    }

    /// Performance benchmark after migration
    pub fn benchmark_performance(&self, operations: usize) -> Result<()> {
        use std::time::Instant;
        
        println!("⚡ Running performance benchmark ({} operations)...", operations);
        
        // Write performance
        let start = Instant::now();
        for i in 0..operations {
            let key = format!("benchmark_{:06}", i);
            let value = format!("test_value_{}", i);
            self.database.put(key.as_bytes(), value.as_bytes())?;
        }
        let write_duration = start.elapsed();
        let write_ops_per_sec = operations as f64 / write_duration.as_secs_f64();

        // Read performance
        let start = Instant::now();
        for i in 0..operations {
            let key = format!("benchmark_{:06}", i);
            self.database.get(key.as_bytes())?;
        }
        let read_duration = start.elapsed();
        let read_ops_per_sec = operations as f64 / read_duration.as_secs_f64();

        // Cleanup
        for i in 0..operations {
            let key = format!("benchmark_{:06}", i);
            self.database.delete(key.as_bytes())?;
        }

        println!("📊 Performance Results:");
        println!("   Write: {:.0} ops/sec ({:.2} μs/op)", 
                write_ops_per_sec, write_duration.as_micros() as f64 / operations as f64);
        println!("   Read:  {:.0} ops/sec ({:.2} μs/op)", 
                read_ops_per_sec, read_duration.as_micros() as f64 / operations as f64);

        Ok(())
    }
}

/// Common validation utilities
pub mod validation {
    use super::*;
    use serde_json::Value;

    /// Validate that a table definition exists
    pub fn validate_table_exists(db: &Database, table_name: &str) -> Result<()> {
        let key = format!("__table_def_{}", table_name);
        match db.get(key.as_bytes())? {
            Some(_) => {
                println!("✅ Table '{}' exists", table_name);
                Ok(())
            }
            None => Err(lightning_db::Error::Validation(
                format!("Table '{}' does not exist", table_name)
            )),
        }
    }

    /// Validate that an index exists
    pub fn validate_index_exists(db: &Database, index_name: &str) -> Result<()> {
        let key = format!("__index_def_{}", index_name);
        match db.get(key.as_bytes())? {
            Some(_) => {
                println!("✅ Index '{}' exists", index_name);
                Ok(())
            }
            None => Err(lightning_db::Error::Validation(
                format!("Index '{}' does not exist", index_name)
            )),
        }
    }

    /// Validate JSON data structure
    pub fn validate_json_structure(
        data: &[u8], 
        required_fields: &[&str]
    ) -> Result<()> {
        let json: Value = serde_json::from_slice(data)
            .map_err(|e| lightning_db::Error::Parse(format!("Invalid JSON: {}", e)))?;

        let obj = json.as_object()
            .ok_or_else(|| lightning_db::Error::Validation("Expected JSON object".to_string()))?;

        for field in required_fields {
            if !obj.contains_key(*field) {
                return Err(lightning_db::Error::Validation(
                    format!("Missing required field: {}", field)
                ));
            }
        }

        Ok(())
    }

    /// Count records with a prefix
    pub fn count_records_with_prefix(db: &Database, prefix: &str) -> Result<usize> {
        let scan_result = db.scan(
            Some(prefix.as_bytes().to_vec()),
            Some(format!("{}~", prefix).as_bytes().to_vec()),
        )?;

        let mut count = 0;
        for item in scan_result {
            let _ = item?;
            count += 1;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_migration_runner_creation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap());
        assert!(runner.is_ok());
    }

    #[test]
    fn test_sample_data_operations() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        
        // Insert sample data
        runner.insert_sample_data("test", 100).unwrap();
        
        // Verify data integrity
        runner.verify_data_integrity("test", 100).unwrap();
    }

    #[test]
    fn test_validation_utilities() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        
        // Test record counting
        runner.insert_sample_data("count_test", 50).unwrap();
        let count = validation::count_records_with_prefix(&runner.database, "count_test_").unwrap();
        assert_eq!(count, 50);
    }
}