use crate::error::{Error, Result};
use crate::{Database, LightningDbConfig};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Backup configuration
#[derive(Debug, Clone)]
pub struct BackupConfig {
    /// Include WAL in backup
    pub include_wal: bool,
    /// Compress backup
    pub compress: bool,
    /// Verify backup after creation
    pub verify: bool,
    /// Maximum file size for incremental backup
    pub max_incremental_size: u64,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            include_wal: true,
            compress: true,
            verify: true,
            max_incremental_size: 100 * 1024 * 1024, // 100MB
        }
    }
}

/// Backup metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupMetadata {
    pub version: u32,
    pub timestamp: u64,
    pub backup_type: BackupType,
    pub source_path: String,
    pub includes_wal: bool,
    pub compressed: bool,
    pub file_count: usize,
    pub total_size: u64,
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BackupType {
    Full,
    Incremental,
    Differential,
}

/// Backup manager for database backup operations
pub struct BackupManager {
    config: BackupConfig,
}

impl BackupManager {
    pub fn new(config: BackupConfig) -> Self {
        Self { config }
    }
    
    /// Create a full backup of the database
    pub fn create_backup<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        source_db: P,
        backup_path: Q,
    ) -> Result<BackupMetadata> {
        let source_path = source_db.as_ref();
        let backup_dir = backup_path.as_ref();
        
        // Create backup directory
        fs::create_dir_all(backup_dir).map_err(Error::Io)?;
        
        // Generate backup metadata
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let mut metadata = BackupMetadata {
            version: 1,
            timestamp,
            backup_type: BackupType::Full,
            source_path: source_path.to_string_lossy().to_string(),
            includes_wal: self.config.include_wal,
            compressed: self.config.compress,
            file_count: 0,
            total_size: 0,
            checksum: None,
        };
        
        // List all database files
        let mut files_to_backup = vec![];
        
        // Main database file
        let btree_file = source_path.join("btree.db");
        if btree_file.exists() {
            files_to_backup.push(("btree.db".to_string(), btree_file));
        }
        
        // Legacy database file for compatibility
        let btree_legacy_file = source_path.join("btree.legacy");
        if btree_legacy_file.exists() {
            files_to_backup.push(("btree.legacy".to_string(), btree_legacy_file));
        }
        
        // LSM tree directory
        let lsm_dir = source_path.join("lsm");
        if lsm_dir.exists() {
            self.collect_directory_files(&lsm_dir, "lsm", &mut files_to_backup)?;
        }
        
        // WAL file
        if self.config.include_wal {
            let wal_file = source_path.join("wal.log");
            if wal_file.exists() {
                files_to_backup.push(("wal.log".to_string(), wal_file));
            }
        }
        
        // Copy files to backup directory
        let mut total_size = 0u64;
        let mut hasher = crc32fast::Hasher::new();
        
        for (relative_path, source_file) in &files_to_backup {
            let dest_file = backup_dir.join(relative_path);
            
            // Create parent directories
            if let Some(parent) = dest_file.parent() {
                fs::create_dir_all(parent).map_err(Error::Io)?;
            }
            
            // Copy file
            let size = self.copy_file_with_compression(
                source_file,
                &dest_file,
                &mut hasher,
            )?;
            
            total_size += size;
        }
        
        metadata.file_count = files_to_backup.len();
        metadata.total_size = total_size;
        metadata.checksum = Some(format!("{:x}", hasher.finalize()));
        
        // Save metadata
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Generic(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&metadata_path, metadata_json).map_err(Error::Io)?;
        
        // Verify backup if requested
        if self.config.verify {
            self.verify_backup(backup_dir)?;
        }
        
        Ok(metadata)
    }
    
    /// Create an incremental backup (only changed files)
    pub fn create_incremental_backup<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        source_db: P,
        backup_path: Q,
        last_backup_time: u64,
    ) -> Result<BackupMetadata> {
        let source_path = source_db.as_ref();
        let backup_dir = backup_path.as_ref();
        
        // Create backup directory
        fs::create_dir_all(backup_dir).map_err(Error::Io)?;
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let mut metadata = BackupMetadata {
            version: 1,
            timestamp,
            backup_type: BackupType::Incremental,
            source_path: source_path.to_string_lossy().to_string(),
            includes_wal: self.config.include_wal,
            compressed: self.config.compress,
            file_count: 0,
            total_size: 0,
            checksum: None,
        };
        
        // Find files modified since last backup
        let mut files_to_backup = vec![];
        self.find_modified_files(source_path, last_backup_time, &mut files_to_backup)?;
        
        // Copy only modified files
        let mut total_size = 0u64;
        let mut hasher = crc32fast::Hasher::new();
        
        for (relative_path, source_file) in &files_to_backup {
            let dest_file = backup_dir.join(relative_path);
            
            if let Some(parent) = dest_file.parent() {
                fs::create_dir_all(parent).map_err(Error::Io)?;
            }
            
            let size = self.copy_file_with_compression(
                source_file,
                &dest_file,
                &mut hasher,
            )?;
            
            total_size += size;
        }
        
        metadata.file_count = files_to_backup.len();
        metadata.total_size = total_size;
        metadata.checksum = Some(format!("{:x}", hasher.finalize()));
        
        // Save metadata
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Generic(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&metadata_path, metadata_json).map_err(Error::Io)?;
        
        Ok(metadata)
    }
    
    /// Restore database from backup
    pub fn restore_backup<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        backup_path: P,
        restore_path: Q,
    ) -> Result<()> {
        let backup_dir = backup_path.as_ref();
        let restore_dir = restore_path.as_ref();
        
        // Load metadata
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = fs::read_to_string(&metadata_path).map_err(Error::Io)?;
        let metadata: BackupMetadata = serde_json::from_str(&metadata_json)
            .map_err(|e| Error::Generic(format!("Failed to parse metadata: {}", e)))?;
        
        // Verify backup if requested
        if self.config.verify {
            self.verify_backup(backup_dir)?;
        }
        
        // Create restore directory
        fs::create_dir_all(restore_dir).map_err(Error::Io)?;
        
        // Restore all files
        self.restore_directory(backup_dir, restore_dir, &metadata)?;
        
        Ok(())
    }
    
    /// List available backups
    pub fn list_backups<P: AsRef<Path>>(backup_root: P) -> Result<Vec<BackupMetadata>> {
        let backup_root = backup_root.as_ref();
        let mut backups = vec![];
        
        if !backup_root.exists() {
            return Ok(backups);
        }
        
        for entry in fs::read_dir(backup_root).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();
            
            if path.is_dir() {
                let metadata_path = path.join("backup_metadata.json");
                if metadata_path.exists() {
                    let metadata_json = fs::read_to_string(&metadata_path).map_err(Error::Io)?;
                    if let Ok(metadata) = serde_json::from_str::<BackupMetadata>(&metadata_json) {
                        backups.push(metadata);
                    }
                }
            }
        }
        
        // Sort by timestamp (newest first)
        backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        
        Ok(backups)
    }
    
    fn collect_directory_files(
        &self,
        dir: &Path,
        prefix: &str,
        files: &mut Vec<(String, PathBuf)>,
    ) -> Result<()> {
        for entry in fs::read_dir(dir).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();
            let relative = format!("{}/{}", prefix, entry.file_name().to_string_lossy());
            
            if path.is_file() {
                files.push((relative, path));
            } else if path.is_dir() {
                self.collect_directory_files(&path, &relative, files)?;
            }
        }
        Ok(())
    }
    
    fn copy_file_with_compression(
        &self,
        source: &Path,
        dest: &Path,
        hasher: &mut crc32fast::Hasher,
    ) -> Result<u64> {
        let mut source_file = File::open(source).map_err(Error::Io)?;
        let mut buffer = Vec::new();
        source_file.read_to_end(&mut buffer).map_err(Error::Io)?;
        
        // Update checksum
        hasher.update(&buffer);
        
        if self.config.compress {
            // Compress with zstd
            let compressed = zstd::encode_all(buffer.as_slice(), 3)
                .map_err(|e| Error::Compression(e.to_string()))?;
            
            let dest_path = PathBuf::from(format!("{}.zst", dest.display()));
            let mut dest_file = File::create(&dest_path).map_err(Error::Io)?;
            dest_file.write_all(&compressed).map_err(Error::Io)?;
            
            Ok(compressed.len() as u64)
        } else {
            let mut dest_file = File::create(dest).map_err(Error::Io)?;
            dest_file.write_all(&buffer).map_err(Error::Io)?;
            
            Ok(buffer.len() as u64)
        }
    }
    
    fn find_modified_files(
        &self,
        dir: &Path,
        since_timestamp: u64,
        files: &mut Vec<(String, PathBuf)>,
    ) -> Result<()> {
        let since = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(since_timestamp);
        
        fn check_file(path: &Path, since: SystemTime, base_dir: &Path, files: &mut Vec<(String, PathBuf)>) -> io::Result<()> {
            let metadata = fs::metadata(path)?;
            if let Ok(modified) = metadata.modified() {
                if modified > since {
                    let relative = path.strip_prefix(base_dir)
                        .unwrap_or(path)
                        .to_string_lossy()
                        .to_string();
                    files.push((relative, path.to_path_buf()));
                }
            }
            Ok(())
        }
        
        // Check main files
        let btree_file = dir.join("btree.db");
        if btree_file.exists() {
            check_file(&btree_file, since, dir, files).map_err(Error::Io)?;
        }
        
        // Check LSM directory
        let lsm_dir = dir.join("lsm");
        if lsm_dir.exists() {
            self.find_modified_in_directory(&lsm_dir, since, dir, files)?;
        }
        
        // Check WAL
        if self.config.include_wal {
            let wal_file = dir.join("wal.log");
            if wal_file.exists() {
                check_file(&wal_file, since, dir, files).map_err(Error::Io)?;
            }
        }
        
        Ok(())
    }
    
    fn find_modified_in_directory(
        &self,
        dir: &Path,
        since: SystemTime,
        base_dir: &Path,
        files: &mut Vec<(String, PathBuf)>,
    ) -> Result<()> {
        for entry in fs::read_dir(dir).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();
            
            if path.is_file() {
                let metadata = fs::metadata(&path).map_err(Error::Io)?;
                if let Ok(modified) = metadata.modified() {
                    if modified > since {
                        let relative = path.strip_prefix(base_dir)
                            .unwrap_or(&path)
                            .to_string_lossy()
                            .to_string();
                        files.push((relative, path));
                    }
                }
            } else if path.is_dir() {
                self.find_modified_in_directory(&path, since, base_dir, files)?;
            }
        }
        Ok(())
    }
    
    fn restore_directory(
        &self,
        backup_dir: &Path,
        restore_dir: &Path,
        metadata: &BackupMetadata,
    ) -> Result<()> {
        for entry in fs::read_dir(backup_dir).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();
            let file_name = entry.file_name();
            
            // Skip metadata file
            if file_name == "backup_metadata.json" {
                continue;
            }
            
            let dest_path = restore_dir.join(&file_name);
            
            if path.is_file() {
                // Check if it's compressed
                if metadata.compressed && path.extension().and_then(|s| s.to_str()) == Some("zst") {
                    // Decompress file
                    let compressed_data = fs::read(&path).map_err(Error::Io)?;
                    let decompressed = zstd::decode_all(compressed_data.as_slice())
                        .map_err(|e| Error::Decompression(e.to_string()))?;
                    
                    // Remove .zst extension
                    let actual_dest = dest_path.with_extension("");
                    fs::write(&actual_dest, decompressed).map_err(Error::Io)?;
                } else {
                    // Direct copy
                    fs::copy(&path, &dest_path).map_err(Error::Io)?;
                }
            } else if path.is_dir() {
                // Recursively restore directory
                fs::create_dir_all(&dest_path).map_err(Error::Io)?;
                self.restore_directory(&path, &dest_path, metadata)?;
            }
        }
        
        Ok(())
    }
    
    fn verify_backup(&self, backup_dir: &Path) -> Result<()> {
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = fs::read_to_string(&metadata_path).map_err(Error::Io)?;
        let metadata: BackupMetadata = serde_json::from_str(&metadata_json)
            .map_err(|e| Error::Generic(format!("Failed to parse metadata: {}", e)))?;
        
        // Verify file count
        let mut file_count = 0;
        self.count_files(backup_dir, &mut file_count)?;
        
        // Subtract metadata file
        file_count -= 1;
        
        if file_count != metadata.file_count {
            return Err(Error::Generic(format!(
                "Backup verification failed: expected {} files, found {}",
                metadata.file_count, file_count
            )));
        }
        
        // TODO: Verify checksum
        
        Ok(())
    }
    
    fn count_files(&self, dir: &Path, count: &mut usize) -> Result<()> {
        for entry in fs::read_dir(dir).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();
            
            if path.is_file() {
                *count += 1;
            } else if path.is_dir() {
                self.count_files(&path, count)?;
            }
        }
        Ok(())
    }
}

/// Convenient backup/restore functions for Database
impl Database {
    /// Create a backup of this database
    pub fn backup<P: AsRef<Path>>(&self, _backup_path: P) -> Result<BackupMetadata> {
        // Sync database before backup
        self.sync()?;
        
        let _manager = BackupManager::new(BackupConfig::default());
        
        // Get database path from config
        // In a real implementation, we'd store the path in the Database struct
        // For now, we'll return an error
        Err(Error::Generic("Database path not available for backup".to_string()))
    }
    
    /// Restore database from backup
    pub fn restore<P: AsRef<Path>, Q: AsRef<Path>>(
        backup_path: P,
        restore_path: Q,
        config: LightningDbConfig,
    ) -> Result<Self> {
        let manager = BackupManager::new(BackupConfig::default());
        manager.restore_backup(backup_path, &restore_path)?;
        
        // Open the restored database
        Self::open(restore_path, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::{Database, LightningDbConfig};
    
    #[test]
    fn test_full_backup_restore() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_db");
        let backup_path = dir.path().join("backup");
        let restore_path = dir.path().join("restored_db");
        
        // Create and populate database
        {
            let config = LightningDbConfig {
                use_optimized_transactions: false,
                use_optimized_page_manager: false,
                compression_enabled: false, // Disable compression to force B+Tree storage
                ..Default::default()
            };
            let db = Database::create(&db_path, config).unwrap();
            db.put(b"key1", b"value1").unwrap();
            db.put(b"key2", b"value2").unwrap();
            db.sync().unwrap();
            // Also flush LSM to ensure data is written to disk
            if let Err(_) = db.flush_lsm() {
                // LSM flush may fail if not using LSM, which is fine
            }
            
        }
        
        // Create backup
        let manager = BackupManager::new(BackupConfig::default());
        
        
        let metadata = manager.create_backup(&db_path, &backup_path).unwrap();
        
        assert_eq!(metadata.backup_type, BackupType::Full);
        assert!(metadata.file_count > 0);
        
        // Restore backup
        manager.restore_backup(&backup_path, &restore_path).unwrap();
        
        
        // Verify restored database
        let config = LightningDbConfig {
            use_optimized_transactions: false,
            use_optimized_page_manager: false, // Use same config as creation
            compression_enabled: false, // Disable compression to force B+Tree storage
            ..Default::default()
        };
        let restored_db = Database::open(&restore_path, config).unwrap();
        
        assert_eq!(restored_db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(restored_db.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    }
    
    #[test]
    fn test_incremental_backup() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_db");
        let full_backup_path = dir.path().join("full_backup");
        let incr_backup_path = dir.path().join("incr_backup");
        
        // Create database with initial data
        let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();
        db.put(b"key1", b"value1").unwrap();
        db.sync().unwrap();
        
        // Create full backup
        let manager = BackupManager::new(BackupConfig::default());
        let full_metadata = manager.create_backup(&db_path, &full_backup_path).unwrap();
        
        // Add more data
        std::thread::sleep(std::time::Duration::from_secs(1));
        db.put(b"key2", b"value2").unwrap();
        db.sync().unwrap();
        
        // Create incremental backup
        let incr_metadata = manager.create_incremental_backup(
            &db_path,
            &incr_backup_path,
            full_metadata.timestamp,
        ).unwrap();
        
        assert_eq!(incr_metadata.backup_type, BackupType::Incremental);
        // Incremental backup should have fewer files
        assert!(incr_metadata.file_count <= full_metadata.file_count);
    }
    
    #[test]
    fn test_list_backups() {
        let dir = tempdir().unwrap();
        let backup_root = dir.path().join("backups");
        
        // Create multiple backup directories
        for i in 0..3 {
            let backup_dir = backup_root.join(format!("backup_{}", i));
            fs::create_dir_all(&backup_dir).unwrap();
            
            let metadata = BackupMetadata {
                version: 1,
                timestamp: i as u64,
                backup_type: BackupType::Full,
                source_path: "/test".to_string(),
                includes_wal: true,
                compressed: true,
                file_count: 1,
                total_size: 1000,
                checksum: None,
            };
            
            let metadata_json = serde_json::to_string(&metadata).unwrap();
            fs::write(backup_dir.join("backup_metadata.json"), metadata_json).unwrap();
        }
        
        let backups = BackupManager::list_backups(&backup_root).unwrap();
        assert_eq!(backups.len(), 3);
        
        // Should be sorted by timestamp (newest first)
        assert_eq!(backups[0].timestamp, 2);
        assert_eq!(backups[1].timestamp, 1);
        assert_eq!(backups[2].timestamp, 0);
    }
}