//! Schema versioning system for tracking database schema versions

use crate::{Database, Error, Result};
use serde::{Serialize, Deserialize};
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Schema version representation
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash, bincode::Encode, bincode::Decode)]
pub struct SchemaVersion {
    /// Major version (breaking changes)
    major: u32,
    
    /// Minor version (backward compatible changes)
    minor: u32,
    
    /// Patch version (bug fixes)
    patch: u32,
    
    /// Build metadata (optional)
    build: Option<String>,
    
    /// Timestamp when version was created
    timestamp: u64,
}

impl SchemaVersion {
    /// Create a new schema version
    pub fn new(major: u32, minor: u32) -> Self {
        Self::with_patch(major, minor, 0)
    }
    
    /// Create a new schema version with patch
    pub fn with_patch(major: u32, minor: u32, patch: u32) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
            
        Self {
            major,
            minor,
            patch,
            build: None,
            timestamp,
        }
    }
    
    /// Create a new schema version with build metadata
    pub fn with_build(major: u32, minor: u32, patch: u32, build: String) -> Self {
        let mut version = Self::with_patch(major, minor, patch);
        version.build = Some(build);
        version
    }
    
    /// Parse version from string (e.g., "1.2.3" or "1.2.3+build123")
    pub fn parse(version_str: &str) -> Result<Self> {
        let parts: Vec<&str> = version_str.split('+').collect();
        let version_parts: Vec<&str> = parts[0].split('.').collect();
        
        if version_parts.len() < 2 || version_parts.len() > 3 {
            return Err(Error::Parse(format!("Invalid version format: {}", version_str)));
        }
        
        let major = version_parts[0].parse::<u32>()
            .map_err(|_| Error::Parse("Invalid major version".to_string()))?;
        let minor = version_parts[1].parse::<u32>()
            .map_err(|_| Error::Parse("Invalid minor version".to_string()))?;
        let patch = if version_parts.len() > 2 {
            version_parts[2].parse::<u32>()
                .map_err(|_| Error::Parse("Invalid patch version".to_string()))?
        } else {
            0
        };
        
        let mut version = Self::with_patch(major, minor, patch);
        
        if parts.len() > 1 {
            version.build = Some(parts[1].to_string());
        }
        
        Ok(version)
    }
    
    /// Get major version
    pub fn major(&self) -> u32 {
        self.major
    }
    
    /// Get minor version
    pub fn minor(&self) -> u32 {
        self.minor
    }
    
    /// Get patch version
    pub fn patch(&self) -> u32 {
        self.patch
    }
    
    /// Get build metadata
    pub fn build(&self) -> Option<&str> {
        self.build.as_deref()
    }
    
    /// Get timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
    
    /// Check if this version is compatible with another version
    pub fn is_compatible_with(&self, other: &SchemaVersion) -> bool {
        // Same major version = compatible
        self.major == other.major
    }
    
    /// Get the next major version
    pub fn next_major(&self) -> Self {
        Self::new(self.major + 1, 0)
    }
    
    /// Get the next minor version
    pub fn next_minor(&self) -> Self {
        Self::new(self.major, self.minor + 1)
    }
    
    /// Get the next patch version
    pub fn next_patch(&self) -> Self {
        Self::with_patch(self.major, self.minor, self.patch + 1)
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if let Some(build) = &self.build {
            write!(f, "+{}", build)?;
        }
        Ok(())
    }
}

impl PartialOrd for SchemaVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SchemaVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.major.cmp(&other.major) {
            Ordering::Equal => match self.minor.cmp(&other.minor) {
                Ordering::Equal => self.patch.cmp(&other.patch),
                other => other,
            },
            other => other,
        }
    }
}

/// Version manager for tracking current schema version
pub struct VersionManager {
    database: Arc<Database>,
    version_key: Vec<u8>,
}

impl VersionManager {
    /// Version metadata key in database
    const VERSION_KEY: &'static [u8] = b"__schema_version__";
    
    /// Create a new version manager
    pub fn new(database: Arc<Database>) -> Result<Self> {
        Ok(Self {
            database,
            version_key: Self::VERSION_KEY.to_vec(),
        })
    }
    
    /// Get current schema version
    pub fn current_version(&self) -> Result<SchemaVersion> {
        match self.database.get(&self.version_key)? {
            Some(data) => {
                let version: SchemaVersion = bincode::decode_from_slice(&data, bincode::config::standard())
                    .map_err(|e| Error::Serialization(e.to_string()))?.0;
                Ok(version)
            }
            None => {
                // No version set, assume initial version
                Ok(SchemaVersion::new(0, 0))
            }
        }
    }
    
    /// Set current schema version
    pub fn set_version(&self, version: &SchemaVersion) -> Result<()> {
        let data = bincode::encode_to_vec(version, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        self.database.put(&self.version_key, &data)?;
        Ok(())
    }
    
    /// Get version history
    pub fn get_version_history(&self) -> Result<Vec<SchemaVersion>> {
        // For now, we only track current version
        // In a more advanced implementation, we would maintain a full history
        let current = self.current_version()?;
        Ok(vec![current])
    }
    
    /// Get previous version (for rollback)
    pub fn get_previous_version(&self, current: &SchemaVersion) -> Result<SchemaVersion> {
        // Simple logic: decrement patch, then minor, then major
        if current.patch > 0 {
            Ok(SchemaVersion::with_patch(current.major, current.minor, current.patch - 1))
        } else if current.minor > 0 {
            Ok(SchemaVersion::new(current.major, current.minor - 1))
        } else if current.major > 0 {
            Ok(SchemaVersion::new(current.major - 1, 0))
        } else {
            Err(Error::InvalidOperation { reason: "Cannot get previous version of 0.0.0".to_string() })
        }
    }
    
    /// Check if a version exists in history
    pub fn version_exists(&self, version: &SchemaVersion) -> Result<bool> {
        let current = self.current_version()?;
        Ok(*version <= current)
    }
    
    /// Lock version for migration (prevents concurrent migrations)
    pub fn lock_for_migration(&self) -> Result<MigrationLock> {
        let lock_key = b"__migration_lock__";
        let lock_data = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_le_bytes();
        
        // Try to acquire lock
        match self.database.get(lock_key)? {
            Some(_) => Err(Error::Locked("Migration already in progress".to_string())),
            None => {
                self.database.put(lock_key, &lock_data)?;
                Ok(MigrationLock::new(self.database.clone()))
            }
        }
    }
}

/// Migration lock to prevent concurrent migrations
pub struct MigrationLock {
    database: Arc<Database>,
}

impl MigrationLock {
    fn new(database: Arc<Database>) -> Self {
        Self { database }
    }
}

impl Drop for MigrationLock {
    fn drop(&mut self) {
        // Release lock on drop
        let lock_key = b"__migration_lock__";
        let _ = self.database.delete(lock_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_version_creation() {
        let v1 = SchemaVersion::new(1, 0);
        assert_eq!(v1.major(), 1);
        assert_eq!(v1.minor(), 0);
        assert_eq!(v1.patch(), 0);
        assert!(v1.build().is_none());
        
        let v2 = SchemaVersion::with_patch(1, 2, 3);
        assert_eq!(v2.major(), 1);
        assert_eq!(v2.minor(), 2);
        assert_eq!(v2.patch(), 3);
        
        let v3 = SchemaVersion::with_build(2, 0, 0, "alpha".to_string());
        assert_eq!(v3.build(), Some("alpha"));
    }
    
    #[test]
    fn test_version_parsing() {
        let v1 = SchemaVersion::parse("1.0").unwrap();
        assert_eq!(v1.major(), 1);
        assert_eq!(v1.minor(), 0);
        assert_eq!(v1.patch(), 0);
        
        let v2 = SchemaVersion::parse("2.3.4").unwrap();
        assert_eq!(v2.major(), 2);
        assert_eq!(v2.minor(), 3);
        assert_eq!(v2.patch(), 4);
        
        let v3 = SchemaVersion::parse("1.0.0+build123").unwrap();
        assert_eq!(v3.major(), 1);
        assert_eq!(v3.minor(), 0);
        assert_eq!(v3.patch(), 0);
        assert_eq!(v3.build(), Some("build123"));
        
        assert!(SchemaVersion::parse("invalid").is_err());
        assert!(SchemaVersion::parse("1").is_err());
        assert!(SchemaVersion::parse("1.2.3.4").is_err());
    }
    
    #[test]
    fn test_version_comparison() {
        let v1 = SchemaVersion::new(1, 0);
        let v2 = SchemaVersion::new(1, 1);
        let v3 = SchemaVersion::new(2, 0);
        
        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v1 < v3);
        
        assert_eq!(v1, SchemaVersion::new(1, 0));
        assert_ne!(v1, v2);
    }
    
    #[test]
    fn test_version_compatibility() {
        let v1 = SchemaVersion::new(1, 0);
        let v2 = SchemaVersion::new(1, 5);
        let v3 = SchemaVersion::new(2, 0);
        
        assert!(v1.is_compatible_with(&v2));
        assert!(!v1.is_compatible_with(&v3));
        assert!(!v2.is_compatible_with(&v3));
    }
    
    #[test]
    fn test_version_display() {
        let v1 = SchemaVersion::new(1, 0);
        assert_eq!(v1.to_string(), "1.0.0");
        
        let v2 = SchemaVersion::with_patch(2, 3, 4);
        assert_eq!(v2.to_string(), "2.3.4");
        
        let v3 = SchemaVersion::with_build(1, 0, 0, "alpha".to_string());
        assert_eq!(v3.to_string(), "1.0.0+alpha");
    }
    
    #[test]
    fn test_next_versions() {
        let v = SchemaVersion::new(1, 2);
        
        let next_major = v.next_major();
        assert_eq!(next_major.to_string(), "2.0.0");
        
        let next_minor = v.next_minor();
        assert_eq!(next_minor.to_string(), "1.3.0");
        
        let next_patch = v.next_patch();
        assert_eq!(next_patch.to_string(), "1.2.1");
    }
}