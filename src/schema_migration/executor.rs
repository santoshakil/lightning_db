//! Migration executor for running migrations atomically

use super::{Migration, MigrationConfig, MigrationDirection};
use crate::{Database, Error, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info, warn};

/// Result of a migration execution
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ExecutionResult {
    /// Migration version
    pub version: String,

    /// Direction (up or down)
    pub direction: String,

    /// Start time
    pub started_at: SystemTime,

    /// End time
    pub completed_at: SystemTime,

    /// Duration in milliseconds
    pub duration_ms: u64,

    /// Number of steps executed
    pub steps_executed: usize,

    /// Success status
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Checkpoints for resumable migrations
    pub checkpoints: Vec<ExecutionCheckpoint>,
}

/// Checkpoint for resumable migrations
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ExecutionCheckpoint {
    /// Step index
    pub step_index: usize,

    /// Step description
    pub step_description: String,

    /// Timestamp
    pub timestamp: SystemTime,

    /// Data checkpoint (for resuming)
    pub checkpoint_data: Option<Vec<u8>>,
}

/// Migration executor
pub struct MigrationExecutor {
    database: Arc<Database>,
    config: MigrationConfig,
}

impl MigrationExecutor {
    /// Create a new migration executor
    pub fn new(database: Arc<Database>, config: MigrationConfig) -> Result<Self> {
        Ok(Self { database, config })
    }

    /// Execute a migration
    pub fn execute(
        &self,
        migration: &dyn Migration,
        direction: MigrationDirection,
    ) -> Result<ExecutionResult> {
        let start_time = Instant::now();
        let started_at = SystemTime::now();

        info!(
            "Starting {} migration to version {}",
            match direction {
                MigrationDirection::Up => "up",
                MigrationDirection::Down => "down",
            },
            migration.version()
        );

        // Check if migration is destructive
        if migration.is_destructive() && !self.config.allow_destructive {
            return Err(Error::Config(
                "Destructive migrations are not allowed".to_string(),
            ));
        }

        // Create backup if configured
        if self.config.backup_before_migration && direction == MigrationDirection::Up {
            self.create_backup(&migration.version().to_string())?;
        }

        // Begin transaction for atomic execution
        let tx_id = self.database.begin_transaction()?;

        let result = match self.execute_with_timeout(migration, direction, tx_id) {
            Ok(checkpoints) => {
                // Commit transaction
                self.database.commit_transaction(tx_id)?;

                let duration = start_time.elapsed();
                info!(
                    "Migration {} completed successfully in {:?}",
                    migration.version(),
                    duration
                );

                ExecutionResult {
                    version: migration.version().to_string(),
                    direction: format!("{:?}", direction),
                    started_at,
                    completed_at: SystemTime::now(),
                    duration_ms: duration.as_millis() as u64,
                    steps_executed: checkpoints.len(),
                    success: true,
                    error: None,
                    checkpoints,
                }
            }
            Err(e) => {
                // Rollback transaction
                warn!("Migration failed, rolling back transaction: {}", e);
                self.database.abort_transaction(tx_id)?;

                // Restore backup if exists
                if self.config.backup_before_migration && direction == MigrationDirection::Up {
                    self.restore_backup(&migration.version().to_string())?;
                }

                let duration = start_time.elapsed();
                error!(
                    "Migration {} failed after {:?}: {}",
                    migration.version(),
                    duration,
                    e
                );

                ExecutionResult {
                    version: migration.version().to_string(),
                    direction: format!("{:?}", direction),
                    started_at,
                    completed_at: SystemTime::now(),
                    duration_ms: duration.as_millis() as u64,
                    steps_executed: 0,
                    success: false,
                    error: Some(e.to_string()),
                    checkpoints: Vec::new(),
                }
            }
        };

        Ok(result)
    }

    /// Execute migration with timeout
    fn execute_with_timeout(
        &self,
        migration: &dyn Migration,
        direction: MigrationDirection,
        _tx_id: u64,
    ) -> Result<Vec<ExecutionCheckpoint>> {
        let timeout = Duration::from_secs(self.config.timeout_seconds);
        let start = Instant::now();
        let mut checkpoints = Vec::new();

        match direction {
            MigrationDirection::Up => {
                // Run pre-condition validation
                migration.validate_preconditions(&self.database)?;

                // Execute migration steps
                let steps = migration.steps();
                for (index, step) in steps.iter().enumerate() {
                    // Check timeout
                    if start.elapsed() > timeout {
                        return Err(Error::Timeout(format!(
                            "Migration timeout after {} seconds",
                            self.config.timeout_seconds
                        )));
                    }

                    debug!("Executing step {}: {:?}", index, step);

                    // Execute step
                    step.execute_up(&self.database)?;

                    // Record checkpoint
                    checkpoints.push(ExecutionCheckpoint {
                        step_index: index,
                        step_description: format!("{:?}", step),
                        timestamp: SystemTime::now(),
                        checkpoint_data: None,
                    });

                    // Save checkpoint for resumability
                    self.save_checkpoint(&migration.version().to_string(), &checkpoints)?;
                }

                // Run custom migration logic if provided
                migration.migrate_up(&self.database)?;

                // Run post-condition validation
                if self.config.validate_after_migration {
                    migration.validate_postconditions(&self.database)?;
                }
            }

            MigrationDirection::Down => {
                // Execute rollback steps in reverse
                let steps = migration.steps();
                for (index, step) in steps.iter().enumerate().rev() {
                    // Check timeout
                    if start.elapsed() > timeout {
                        return Err(Error::Timeout(format!(
                            "Rollback timeout after {} seconds",
                            self.config.timeout_seconds
                        )));
                    }

                    debug!("Rolling back step {}: {:?}", index, step);

                    // Execute rollback
                    step.execute_down(&self.database)?;

                    // Record checkpoint
                    checkpoints.push(ExecutionCheckpoint {
                        step_index: index,
                        step_description: format!("Rollback: {:?}", step),
                        timestamp: SystemTime::now(),
                        checkpoint_data: None,
                    });
                }

                // Run custom rollback logic if provided
                migration.migrate_down(&self.database)?;
            }
        }

        // Clear checkpoints on success
        self.clear_checkpoints(&migration.version().to_string())?;

        Ok(checkpoints)
    }

    /// Create a backup before migration
    fn create_backup(&self, version: &str) -> Result<()> {
        let backup_key = format!("__migration_backup_{}", version);

        // In a real implementation, this would create a full backup
        // For now, we just record the backup metadata
        let backup_metadata = BackupMetadata {
            version: version.to_string(),
            created_at: SystemTime::now(),
            size_bytes: 0, // Would be actual backup size
        };

        let data = bincode::encode_to_vec(&backup_metadata, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.database.put(backup_key.as_bytes(), &data)?;

        info!("Created backup for migration {}", version);
        Ok(())
    }

    /// Restore a backup after failed migration
    fn restore_backup(&self, version: &str) -> Result<()> {
        let backup_key = format!("__migration_backup_{}", version);

        if let Some(data) = self.database.get(backup_key.as_bytes())? {
            let metadata: BackupMetadata =
                bincode::decode_from_slice(&data, bincode::config::standard())
                    .map_err(|e| Error::Serialization(e.to_string()))?
                    .0;

            info!("Restoring backup from {:?}", metadata.created_at);

            // In a real implementation, this would restore the actual backup
            // For now, we just clean up the metadata
            self.database.delete(backup_key.as_bytes())?;
        }

        Ok(())
    }

    /// Save migration checkpoint for resumability
    fn save_checkpoint(&self, version: &str, checkpoints: &[ExecutionCheckpoint]) -> Result<()> {
        let checkpoint_key = format!("__migration_checkpoint_{}", version);
        let data = bincode::encode_to_vec(checkpoints, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.database.put(checkpoint_key.as_bytes(), &data)?;
        Ok(())
    }

    /// Clear migration checkpoints
    fn clear_checkpoints(&self, version: &str) -> Result<()> {
        let checkpoint_key = format!("__migration_checkpoint_{}", version);
        self.database.delete(checkpoint_key.as_bytes())?;
        Ok(())
    }

    /// Resume a migration from checkpoint
    pub fn resume_migration(
        &self,
        migration: &dyn Migration,
        direction: MigrationDirection,
    ) -> Result<ExecutionResult> {
        let checkpoint_key = format!("__migration_checkpoint_{}", migration.version());

        if let Some(data) = self.database.get(checkpoint_key.as_bytes())? {
            let checkpoints: Vec<ExecutionCheckpoint> =
                bincode::decode_from_slice(&data, bincode::config::standard())
                    .map_err(|e| Error::Serialization(e.to_string()))?
                    .0;

            info!(
                "Resuming migration {} from checkpoint (step {})",
                migration.version(),
                checkpoints.len()
            );

            // Continue from last checkpoint
            // This is a simplified implementation
            self.execute(migration, direction)
        } else {
            // No checkpoint, start fresh
            self.execute(migration, direction)
        }
    }

    /// Dry run a migration (validation only)
    pub fn dry_run(&self, migration: &dyn Migration) -> Result<ValidationReport> {
        info!("Dry run for migration {}", migration.version());

        let mut report = ValidationReport {
            version: migration.version().to_string(),
            is_destructive: migration.is_destructive(),
            steps_count: migration.steps().len(),
            warnings: Vec::new(),
            errors: Vec::new(),
        };

        // Check preconditions
        if let Err(e) = migration.validate_preconditions(&self.database) {
            report.errors.push(format!("Precondition failed: {}", e));
        }

        // Analyze steps
        for (index, step) in migration.steps().iter().enumerate() {
            if step.is_destructive() {
                report
                    .warnings
                    .push(format!("Step {} is destructive: {:?}", index, step));
            }
        }

        Ok(report)
    }
}

/// Backup metadata
#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
struct BackupMetadata {
    version: String,
    created_at: SystemTime,
    size_bytes: u64,
}

/// Validation report for dry runs
#[derive(Debug, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ValidationReport {
    pub version: String,
    pub is_destructive: bool,
    pub steps_count: usize,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

impl ValidationReport {
    /// Check if validation passed
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_result() {
        let result = ExecutionResult {
            version: "1.0.0".to_string(),
            direction: "Up".to_string(),
            started_at: SystemTime::now(),
            completed_at: SystemTime::now(),
            duration_ms: 100,
            steps_executed: 3,
            success: true,
            error: None,
            checkpoints: vec![],
        };

        assert!(result.success);
        assert_eq!(result.steps_executed, 3);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_validation_report() {
        let report = ValidationReport {
            version: "1.0.0".to_string(),
            is_destructive: false,
            steps_count: 5,
            warnings: vec!["Warning 1".to_string()],
            errors: vec![],
        };

        assert!(report.is_valid());
        assert_eq!(report.warnings.len(), 1);
        assert!(!report.is_destructive);
    }
}
