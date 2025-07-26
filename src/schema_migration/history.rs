//! Migration history tracking and management

use crate::{Database, Error, Result};
use super::{SchemaVersion, ExecutionResult};
use std::sync::Arc;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use tracing::info;

/// Migration history record
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct MigrationRecord {
    /// Unique ID for this migration execution
    pub id: String,
    
    /// Migration version
    pub version: SchemaVersion,
    
    /// Migration description
    pub description: String,
    
    /// Direction (up or down)
    pub direction: MigrationDirection,
    
    /// Execution timestamp
    pub executed_at: SystemTime,
    
    /// Duration in milliseconds
    pub duration_ms: u64,
    
    /// Success status
    pub success: bool,
    
    /// Error message if failed
    pub error: Option<String>,
    
    /// Checksum of migration content
    pub checksum: String,
    
    /// User who executed the migration
    pub executed_by: Option<String>,
    
    /// Additional metadata
    pub metadata: BTreeMap<String, String>,
}

/// Migration direction for history
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, bincode::Encode, bincode::Decode)]
pub enum MigrationDirection {
    Up,
    Down,
}

/// Migration history manager
pub struct MigrationHistory {
    database: Arc<Database>,
    history_prefix: Vec<u8>,
}

impl MigrationHistory {
    /// History key prefix
    const HISTORY_PREFIX: &'static [u8] = b"__migration_history_";
    
    /// Create a new migration history manager
    pub fn new(database: Arc<Database>) -> Result<Self> {
        Ok(Self {
            database,
            history_prefix: Self::HISTORY_PREFIX.to_vec(),
        })
    }
    
    /// Record a migration execution
    pub fn record_migration(
        &self,
        version: &SchemaVersion,
        result: &ExecutionResult,
    ) -> Result<()> {
        let record = MigrationRecord {
            id: generate_migration_id(),
            version: version.clone(),
            description: format!("Migration to version {}", version),
            direction: MigrationDirection::Up,
            executed_at: result.started_at,
            duration_ms: result.duration_ms,
            success: result.success,
            error: result.error.clone(),
            checksum: calculate_checksum(version),
            executed_by: get_current_user(),
            metadata: BTreeMap::new(),
        };
        
        self.save_record(&record)?;
        
        info!(
            "Recorded migration {} to version {} (success: {})",
            record.id, version, result.success
        );
        
        Ok(())
    }
    
    /// Record a rollback execution
    pub fn record_rollback(
        &self,
        version: &SchemaVersion,
        result: &ExecutionResult,
    ) -> Result<()> {
        let record = MigrationRecord {
            id: generate_migration_id(),
            version: version.clone(),
            description: format!("Rollback from version {}", version),
            direction: MigrationDirection::Down,
            executed_at: result.started_at,
            duration_ms: result.duration_ms,
            success: result.success,
            error: result.error.clone(),
            checksum: calculate_checksum(version),
            executed_by: get_current_user(),
            metadata: BTreeMap::new(),
        };
        
        self.save_record(&record)?;
        
        info!(
            "Recorded rollback {} from version {} (success: {})",
            record.id, version, result.success
        );
        
        Ok(())
    }
    
    /// Get migration history
    pub fn get_history(&self, limit: Option<usize>) -> Result<Vec<MigrationRecord>> {
        let mut records = Vec::new();
        
        // Scan for all history records
        let prefix = self.history_prefix.clone();
        let scan_result = self.database.scan(
            Some(prefix.clone()),
            Some(next_prefix(&prefix)),
        )?;
        
        // Collect and deserialize records
        for item in scan_result {
            let (_key, value) = item?;
            let record: MigrationRecord = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| Error::Serialization(e.to_string()))?.0;
            records.push(record);
        }
        
        // Sort by execution time (newest first)
        records.sort_by(|a, b| b.executed_at.cmp(&a.executed_at));
        
        // Apply limit if specified
        if let Some(limit) = limit {
            records.truncate(limit);
        }
        
        Ok(records)
    }
    
    /// Get history for a specific version
    pub fn get_version_history(&self, version: &SchemaVersion) -> Result<Vec<MigrationRecord>> {
        let history = self.get_history(None)?;
        Ok(history.into_iter()
            .filter(|r| r.version == *version)
            .collect())
    }
    
    /// Get the last successful migration
    pub fn get_last_successful_migration(&self) -> Result<Option<MigrationRecord>> {
        let history = self.get_history(None)?;
        Ok(history.into_iter()
            .find(|r| r.success && r.direction == MigrationDirection::Up))
    }
    
    /// Check if a version has been applied
    pub fn is_version_applied(&self, version: &SchemaVersion) -> Result<bool> {
        let history = self.get_version_history(version)?;
        
        // Check if there's at least one successful up migration
        // and no subsequent successful down migration
        let mut last_success: Option<&MigrationRecord> = None;
        
        for record in &history {
            if record.success {
                last_success = Some(record);
            }
        }
        
        Ok(last_success.map(|r| r.direction == MigrationDirection::Up).unwrap_or(false))
    }
    
    /// Get failed migrations
    pub fn get_failed_migrations(&self) -> Result<Vec<MigrationRecord>> {
        let history = self.get_history(None)?;
        Ok(history.into_iter()
            .filter(|r| !r.success)
            .collect())
    }
    
    /// Clean up old history records
    pub fn cleanup_old_records(&self, keep_days: u32) -> Result<usize> {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - (keep_days as u64 * 86400);
            
        let history = self.get_history(None)?;
        let mut deleted = 0;
        
        for record in history {
            let record_time = record.executed_at
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
                
            if record_time < cutoff {
                self.delete_record(&record.id)?;
                deleted += 1;
            }
        }
        
        if deleted > 0 {
            info!("Cleaned up {} old migration history records", deleted);
        }
        
        Ok(deleted)
    }
    
    /// Save a history record
    fn save_record(&self, record: &MigrationRecord) -> Result<()> {
        let key = self.make_key(&record.id);
        let data = bincode::encode_to_vec(record, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.database.put(&key, &data)?;
        Ok(())
    }
    
    /// Delete a history record
    fn delete_record(&self, id: &str) -> Result<()> {
        let key = self.make_key(id);
        self.database.delete(&key)?;
        Ok(())
    }
    
    /// Make a history key
    fn make_key(&self, id: &str) -> Vec<u8> {
        let mut key = self.history_prefix.clone();
        key.extend_from_slice(id.as_bytes());
        key
    }
    
    /// Generate migration statistics
    pub fn get_statistics(&self) -> Result<MigrationStatistics> {
        let history = self.get_history(None)?;
        
        let total_migrations = history.len();
        let successful_migrations = history.iter().filter(|r| r.success).count();
        let failed_migrations = total_migrations - successful_migrations;
        
        let total_duration_ms: u64 = history.iter().map(|r| r.duration_ms).sum();
        let avg_duration_ms = if total_migrations > 0 {
            total_duration_ms / total_migrations as u64
        } else {
            0
        };
        
        let last_migration = history.first().cloned();
        
        let versions_applied: Vec<SchemaVersion> = history.iter()
            .filter(|r| r.success && r.direction == MigrationDirection::Up)
            .map(|r| r.version.clone())
            .collect();
        
        Ok(MigrationStatistics {
            total_migrations,
            successful_migrations,
            failed_migrations,
            avg_duration_ms,
            total_duration_ms,
            last_migration,
            versions_applied,
        })
    }
}

/// Migration statistics
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct MigrationStatistics {
    /// Total number of migrations executed
    pub total_migrations: usize,
    
    /// Number of successful migrations
    pub successful_migrations: usize,
    
    /// Number of failed migrations
    pub failed_migrations: usize,
    
    /// Average duration in milliseconds
    pub avg_duration_ms: u64,
    
    /// Total duration in milliseconds
    pub total_duration_ms: u64,
    
    /// Last migration record
    pub last_migration: Option<MigrationRecord>,
    
    /// List of versions successfully applied
    pub versions_applied: Vec<SchemaVersion>,
}

/// Generate a unique migration ID
fn generate_migration_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    
    let random: u32 = rand::random();
    format!("{:016x}_{:08x}", timestamp, random)
}

/// Calculate checksum for a version
fn calculate_checksum(version: &SchemaVersion) -> String {
    // In a real implementation, this would calculate a checksum
    // of the actual migration content
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    version.to_string().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Get current user (for audit trail)
fn get_current_user() -> Option<String> {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
}

/// Get the next prefix for range scans
fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut next = prefix.to_vec();
    for i in (0..next.len()).rev() {
        if next[i] < 255 {
            next[i] += 1;
            return next;
        }
        next[i] = 0;
    }
    next.push(0);
    next
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_migration_record() {
        let record = MigrationRecord {
            id: generate_migration_id(),
            version: SchemaVersion::new(1, 0),
            description: "Initial migration".to_string(),
            direction: MigrationDirection::Up,
            executed_at: SystemTime::now(),
            duration_ms: 100,
            success: true,
            error: None,
            checksum: "abcd1234".to_string(),
            executed_by: Some("test_user".to_string()),
            metadata: BTreeMap::new(),
        };
        
        assert!(record.success);
        assert_eq!(record.direction, MigrationDirection::Up);
        assert!(record.error.is_none());
    }
    
    #[test]
    fn test_migration_id_generation() {
        let id1 = generate_migration_id();
        let id2 = generate_migration_id();
        
        assert_ne!(id1, id2);
        assert!(id1.contains('_'));
        assert!(id2.contains('_'));
    }
    
    #[test]
    fn test_checksum_generation() {
        let v1 = SchemaVersion::new(1, 0);
        let v2 = SchemaVersion::new(1, 0);
        let v3 = SchemaVersion::new(2, 0);
        
        let checksum1 = calculate_checksum(&v1);
        let checksum2 = calculate_checksum(&v2);
        let checksum3 = calculate_checksum(&v3);
        
        assert_eq!(checksum1, checksum2);
        assert_ne!(checksum1, checksum3);
    }
}