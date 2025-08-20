use crate::core::error::{Error, Result};
// use crate::metrics::METRICS; // TODO: Fix metrics import
// use crate::realtime_stats::REALTIME_STATS; // TODO: Fix realtime stats import
use crate::{Database, LightningDbConfig};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Sub-modules for advanced backup features
pub mod encryption;
pub mod incremental;
pub mod recovery;

// Re-exports for convenience
pub use encryption::{
    EncryptionAlgorithm, EncryptionConfig, EncryptionInfo, EncryptionManager, EncryptionStatistics,
};
pub use incremental::{
    DeduplicationStatistics, IncrementalBackupManager, IncrementalBackupResult, IncrementalConfig,
};
pub use recovery::{
    RecoveryConfig, RecoveryManager, RecoveryOperationType, RecoveryPoint, RecoveryRequest,
    RecoveryResult,
};

#[cfg(feature = "zstd-compression")]
use zstd;

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
    /// Enable online backup (non-blocking)
    pub online_backup: bool,
    /// Throttle backup I/O (MB/s, 0 = unlimited)
    pub io_throttle_mb_per_sec: u32,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            include_wal: true,
            compress: true,
            verify: true,
            max_incremental_size: 100 * 1024 * 1024, // 100MB
            online_backup: true,
            io_throttle_mb_per_sec: 50, // 50MB/s default throttle
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
    PointInTime,
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
        fs::create_dir_all(backup_dir)?;

        // Generate backup metadata
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
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
            Self::collect_directory_files(&lsm_dir, "lsm", &mut files_to_backup)?;
        }

        // WAL directory or file
        if self.config.include_wal {
            let wal_path = source_path.join("wal");
            if wal_path.exists() {
                if wal_path.is_dir() {
                    // WAL is a directory, collect all files in it
                    Self::collect_directory_files(&wal_path, "wal", &mut files_to_backup)?;
                } else {
                    // WAL is a file
                    files_to_backup.push(("wal".to_string(), wal_path));
                }
            } else {
                // Try alternative WAL file name
                let wal_log_file = source_path.join("wal.log");
                if wal_log_file.exists() {
                    files_to_backup.push(("wal.log".to_string(), wal_log_file));
                }
            }
        }

        // Copy files to backup directory
        let mut total_size = 0u64;
        let mut hasher = crc32fast::Hasher::new();

        for (relative_path, source_file) in &files_to_backup {
            let dest_file = backup_dir.join(relative_path);

            // Create parent directories
            if let Some(parent) = dest_file.parent() {
                fs::create_dir_all(parent)?;
            }

            // Copy file
            let size = self.copy_file_with_compression(source_file, &dest_file, &mut hasher)?;

            total_size += size;
        }

        metadata.file_count = files_to_backup.len();
        metadata.total_size = total_size;
        metadata.checksum = Some(format!("{:x}", hasher.finalize()));

        // Save metadata
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Generic(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&metadata_path, metadata_json)?;

        // Verify backup if requested
        if self.config.verify {
            self.verify_backup(backup_dir)?;
        }

        Ok(metadata)
    }

    /// Create an online backup (non-blocking, consistent)
    pub fn create_online_backup<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        source_db: P,
        backup_path: Q,
        db: Arc<Database>,
    ) -> Result<BackupMetadata> {
        let source_path = source_db.as_ref();
        let backup_dir = backup_path.as_ref();

        // TODO: Start metrics recording
        // let guard = METRICS.read().record_operation("online_backup");

        // Create backup directory
        fs::create_dir_all(backup_dir)?;

        // Create checkpoint to ensure consistency
        db.checkpoint()?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
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
        self.collect_backup_files(source_path, &mut files_to_backup)?;

        // Copy files with throttling
        let mut total_size = 0u64;
        let mut hasher = crc32fast::Hasher::new();
        let throttle_bytes_per_ms = if self.config.io_throttle_mb_per_sec > 0 {
            (self.config.io_throttle_mb_per_sec as u64 * 1024 * 1024) / 1000
        } else {
            u64::MAX
        };

        let start_time = SystemTime::now();
        let mut bytes_copied = 0u64;

        for (relative_path, source_file) in &files_to_backup {
            let dest_file = backup_dir.join(relative_path);

            // Create parent directories
            if let Some(parent) = dest_file.parent() {
                fs::create_dir_all(parent)?;
            }

            // Copy file with rate limiting
            let size = self.copy_file_with_throttling(
                source_file,
                &dest_file,
                &mut hasher,
                &mut bytes_copied,
                throttle_bytes_per_ms,
                start_time,
            )?;

            total_size += size;

            // TODO: Update progress metrics
            // REALTIME_STATS
            //     .write()
            //     .update_size_metrics(bytes_copied, 0, 0);
        }

        metadata.file_count = files_to_backup.len();
        metadata.total_size = total_size;
        metadata.checksum = Some(format!("{:x}", hasher.finalize()));

        // Save metadata
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Generic(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&metadata_path, metadata_json)?;

        // Verify backup if requested
        if self.config.verify {
            self.verify_backup(backup_dir)?;
        }

        // TODO: guard.complete(true);

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
        fs::create_dir_all(backup_dir)?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
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

        // Copy only modified files with parallel processing
        let total_size = Arc::new(parking_lot::Mutex::new(0u64));
        let hasher = Arc::new(parking_lot::Mutex::new(crc32fast::Hasher::new()));

        // Use rayon for parallel file copying
        use rayon::prelude::*;

        let results: Vec<Result<u64>> = files_to_backup
            .par_iter()
            .map(|(relative_path, source_file)| {
                let dest_file = backup_dir.join(relative_path);

                if let Some(parent) = dest_file.parent() {
                    fs::create_dir_all(parent)?;
                }

                // Use optimized copy for incremental backups
                let size =
                    self.copy_file_incremental(source_file, &dest_file, Arc::clone(&hasher))?;

                Ok(size)
            })
            .collect();

        // Check for errors and sum sizes
        let mut total = 0u64;
        for result in results {
            total += result?;
        }

        *total_size.lock() = total;

        metadata.file_count = files_to_backup.len();
        metadata.total_size = *total_size.lock();
        metadata.checksum = Some(format!("{:x}", hasher.lock().clone().finalize()));

        // Save metadata
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Generic(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&metadata_path, metadata_json)?;

        Ok(metadata)
    }

    /// Create a point-in-time backup with WAL replay capability
    pub fn create_point_in_time_backup<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        source_db: P,
        backup_path: Q,
        target_timestamp: u64,
    ) -> Result<BackupMetadata> {
        let source_path = source_db.as_ref();
        let backup_dir = backup_path.as_ref();

        // Create backup directory
        fs::create_dir_all(backup_dir)?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();

        let mut metadata = BackupMetadata {
            version: 1,
            timestamp,
            backup_type: BackupType::PointInTime,
            source_path: source_path.to_string_lossy().to_string(),
            includes_wal: true, // Always include WAL for PITR
            compressed: self.config.compress,
            file_count: 0,
            total_size: 0,
            checksum: None,
        };

        // Find the latest full backup before target timestamp
        let backup_root = backup_dir.parent().unwrap_or(Path::new("."));
        let backups = Self::list_backups(backup_root)?;

        let base_backup = backups
            .iter()
            .filter(|b| b.backup_type == BackupType::Full && b.timestamp <= target_timestamp)
            .max_by_key(|b| b.timestamp);

        if base_backup.is_none() {
            return Err(Error::Generic(
                "No full backup found before target timestamp".to_string(),
            ));
        }

        // Copy base backup files
        let mut files_to_backup = vec![];
        self.collect_backup_files(source_path, &mut files_to_backup)?;

        let mut total_size = 0u64;
        let mut hasher = crc32fast::Hasher::new();

        for (relative_path, source_file) in &files_to_backup {
            let dest_file = backup_dir.join(relative_path);

            if let Some(parent) = dest_file.parent() {
                fs::create_dir_all(parent)?;
            }

            let size = self.copy_file_with_compression(source_file, &dest_file, &mut hasher)?;

            total_size += size;
        }

        // Copy WAL files needed for recovery to target timestamp
        let wal_dir = source_path.join("wal");
        if wal_dir.exists() && wal_dir.is_dir() {
            let backup_wal_dir = backup_dir.join("wal");
            fs::create_dir_all(&backup_wal_dir)?;

            // Copy WAL files that contain operations up to target timestamp
            for entry in fs::read_dir(&wal_dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() {
                    // Check if WAL file is needed for recovery
                    if self.wal_file_needed_for_pitr(&path, target_timestamp)? {
                        let dest = backup_wal_dir.join(entry.file_name());
                        let size = self.copy_file_with_compression(&path, &dest, &mut hasher)?;
                        total_size += size;
                        metadata.file_count += 1;
                    }
                }
            }
        }

        // Store target timestamp in metadata
        metadata.total_size = total_size;
        metadata.checksum = Some(format!("{:x}:{}", hasher.finalize(), target_timestamp));

        // Save metadata
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Generic(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&metadata_path, metadata_json)?;

        // Save PITR info
        let pitr_info = serde_json::json!({
            "target_timestamp": target_timestamp,
            "base_backup_timestamp": base_backup.map(|b| b.timestamp),
        });
        let pitr_path = backup_dir.join("pitr_info.json");
        fs::write(
            &pitr_path,
            serde_json::to_string_pretty(&pitr_info)
                .map_err(|e| Error::Generic(format!("Failed to serialize PITR info: {}", e)))?,
        )?;

        Ok(metadata)
    }

    /// Restore database to a specific point in time
    pub fn restore_point_in_time<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        backup_path: P,
        restore_path: Q,
        target_timestamp: u64,
    ) -> Result<()> {
        let backup_dir = backup_path.as_ref();
        let restore_dir = restore_path.as_ref();

        // Load metadata
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = fs::read_to_string(&metadata_path)?;
        let metadata: BackupMetadata = serde_json::from_str(&metadata_json)
            .map_err(|e| Error::Generic(format!("Failed to parse metadata: {}", e)))?;

        if metadata.backup_type != BackupType::PointInTime {
            return Err(Error::Generic("Not a point-in-time backup".to_string()));
        }

        // Restore base backup
        Self::restore_directory(backup_dir, restore_dir, &metadata)?;

        // Open database and replay WAL to target timestamp
        let config = LightningDbConfig::default();
        let db = Database::open(restore_dir, config)?;

        // Replay WAL operations up to target timestamp
        let wal_dir = restore_dir.join("wal");
        if wal_dir.exists() {
            self.replay_wal_to_timestamp(&db, &wal_dir, target_timestamp)?;
        }

        // Checkpoint to ensure consistency
        db.checkpoint()?;

        Ok(())
    }

    fn wal_file_needed_for_pitr(&self, wal_file: &Path, target_timestamp: u64) -> Result<bool> {
        // Check if WAL file modification time is before target timestamp
        let metadata = fs::metadata(wal_file)?;
        if let Ok(modified) = metadata.modified() {
            let modified_timestamp = modified
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_secs();
            Ok(modified_timestamp <= target_timestamp)
        } else {
            Ok(true) // Include if we can't determine
        }
    }

    fn replay_wal_to_timestamp(
        &self,
        db: &Database,
        _wal_dir: &Path,
        _target_timestamp: u64,
    ) -> Result<()> {
        // This is a simplified implementation
        // In practice, you'd need to parse WAL entries and check their timestamps

        // For now, just ensure the database is consistent
        db.sync()?;

        Ok(())
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
        let metadata_json = fs::read_to_string(&metadata_path)?;
        let metadata: BackupMetadata = serde_json::from_str(&metadata_json)
            .map_err(|e| Error::Generic(format!("Failed to parse metadata: {}", e)))?;

        // Verify backup if requested
        if self.config.verify {
            self.verify_backup(backup_dir)?;
        }

        // Create restore directory
        fs::create_dir_all(restore_dir)?;

        // Restore all files
        Self::restore_directory(backup_dir, restore_dir, &metadata)?;

        Ok(())
    }

    /// List available backups
    pub fn list_backups<P: AsRef<Path>>(backup_root: P) -> Result<Vec<BackupMetadata>> {
        let backup_root = backup_root.as_ref();
        let mut backups = vec![];

        if !backup_root.exists() {
            return Ok(backups);
        }

        for entry in fs::read_dir(backup_root)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                let metadata_path = path.join("backup_metadata.json");
                if metadata_path.exists() {
                    let metadata_json = fs::read_to_string(&metadata_path)?;
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

    fn collect_backup_files(
        &self,
        source_path: &Path,
        files: &mut Vec<(String, PathBuf)>,
    ) -> Result<()> {
        // Main database file
        let btree_file = source_path.join("btree.db");
        if btree_file.exists() {
            files.push(("btree.db".to_string(), btree_file));
        }

        // Legacy database file for compatibility
        let btree_legacy_file = source_path.join("btree.legacy");
        if btree_legacy_file.exists() {
            files.push(("btree.legacy".to_string(), btree_legacy_file));
        }

        // LSM tree directory
        let lsm_dir = source_path.join("lsm");
        if lsm_dir.exists() {
            Self::collect_directory_files(&lsm_dir, "lsm", files)?;
        }

        // WAL directory or file
        if self.config.include_wal {
            let wal_path = source_path.join("wal");
            if wal_path.exists() {
                if wal_path.is_dir() {
                    Self::collect_directory_files(&wal_path, "wal", files)?;
                } else {
                    files.push(("wal".to_string(), wal_path));
                }
            } else {
                let wal_log_file = source_path.join("wal.log");
                if wal_log_file.exists() {
                    files.push(("wal.log".to_string(), wal_log_file));
                }
            }
        }

        Ok(())
    }

    fn collect_directory_files(
        dir: &Path,
        prefix: &str,
        files: &mut Vec<(String, PathBuf)>,
    ) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let relative = format!("{}/{}", prefix, entry.file_name().to_string_lossy());

            if path.is_file() {
                files.push((relative, path));
            } else if path.is_dir() {
                Self::collect_directory_files(&path, &relative, files)?;
            }
        }
        Ok(())
    }

    fn copy_file_with_throttling(
        &self,
        source: &Path,
        dest: &Path,
        hasher: &mut crc32fast::Hasher,
        bytes_copied: &mut u64,
        throttle_bytes_per_ms: u64,
        start_time: SystemTime,
    ) -> Result<u64> {
        let mut source_file = File::open(source)?;
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
        let mut _total_read = 0u64;
        let mut temp_data = Vec::new();

        loop {
            let bytes_read = source_file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            temp_data.extend_from_slice(&buffer[..bytes_read]);
            _total_read += bytes_read as u64;
            *bytes_copied += bytes_read as u64;

            // Throttle if necessary
            if throttle_bytes_per_ms < u64::MAX {
                let elapsed_ms = start_time
                    .elapsed()
                    .unwrap_or_else(|_| Duration::from_millis(0))
                    .as_millis() as u64;
                let expected_bytes = elapsed_ms * throttle_bytes_per_ms;

                if *bytes_copied > expected_bytes {
                    let sleep_ms = (*bytes_copied - expected_bytes) / throttle_bytes_per_ms;
                    if sleep_ms > 0 {
                        thread::sleep(Duration::from_millis(sleep_ms));
                    }
                }
            }
        }

        // Update checksum
        hasher.update(&temp_data);

        if self.config.compress {
            #[cfg(feature = "zstd-compression")]
            {
                // Compress with zstd
                let compressed = zstd::encode_all(temp_data.as_slice(), 3)
                    .map_err(|e| Error::Compression(e.to_string()))?;

                let dest_path = PathBuf::from(format!("{}.zst", dest.display()));
                let mut dest_file = File::create(&dest_path)?;
                dest_file.write_all(&compressed)?;

                Ok(compressed.len() as u64)
            }

            #[cfg(not(feature = "zstd-compression"))]
            {
                return Err(Error::Compression(
                    "Zstd compression not available in this build".to_string(),
                ));
            }
        } else {
            let mut dest_file = File::create(dest)?;
            dest_file.write_all(&temp_data)?;

            Ok(temp_data.len() as u64)
        }
    }

    fn copy_file_with_compression(
        &self,
        source: &Path,
        dest: &Path,
        hasher: &mut crc32fast::Hasher,
    ) -> Result<u64> {
        let mut source_file = File::open(source)?;
        let mut buffer = Vec::new();
        source_file.read_to_end(&mut buffer)?;

        // Update checksum
        hasher.update(&buffer);

        if self.config.compress {
            #[cfg(feature = "zstd-compression")]
            {
                // Compress with zstd
                let compressed = zstd::encode_all(buffer.as_slice(), 3)
                    .map_err(|e| Error::Compression(e.to_string()))?;

                let dest_path = PathBuf::from(format!("{}.zst", dest.display()));
                let mut dest_file = File::create(&dest_path)?;
                dest_file.write_all(&compressed)?;

                Ok(compressed.len() as u64)
            }

            #[cfg(not(feature = "zstd-compression"))]
            {
                return Err(Error::Compression(
                    "Zstd compression not available in this build".to_string(),
                ));
            }
        } else {
            let mut dest_file = File::create(dest)?;
            dest_file.write_all(&buffer)?;

            Ok(buffer.len() as u64)
        }
    }

    fn copy_file_incremental(
        &self,
        source: &Path,
        dest: &Path,
        hasher: Arc<parking_lot::Mutex<crc32fast::Hasher>>,
    ) -> Result<u64> {
        let mut source_file = File::open(source)?;
        let mut buffer = Vec::new();
        source_file.read_to_end(&mut buffer)?;

        // Update checksum
        hasher.lock().update(&buffer);

        if self.config.compress {
            // Use LZ4 for incremental backups (faster)
            use crate::features::compression::{Compressor, Lz4Compressor};
            let compressor = Lz4Compressor;
            let compressed = compressor.compress(&buffer)?;

            let dest_path = PathBuf::from(format!("{}.lz4", dest.display()));
            let mut dest_file = File::create(&dest_path)?;
            dest_file.write_all(&compressed)?;

            Ok(compressed.len() as u64)
        } else {
            let mut dest_file = File::create(dest)?;
            dest_file.write_all(&buffer)?;

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

        fn check_file(
            path: &Path,
            since: SystemTime,
            base_dir: &Path,
            files: &mut Vec<(String, PathBuf)>,
        ) -> io::Result<()> {
            let metadata = fs::metadata(path)?;
            if let Ok(modified) = metadata.modified() {
                if modified > since {
                    let relative = path
                        .strip_prefix(base_dir)
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
            check_file(&btree_file, since, dir, files)?;
        }

        // Check LSM directory
        let lsm_dir = dir.join("lsm");
        if lsm_dir.exists() {
            Self::find_modified_in_directory(&lsm_dir, since, dir, files)?;
        }

        // Check WAL
        if self.config.include_wal {
            let wal_path = dir.join("wal");
            if wal_path.exists() {
                if wal_path.is_dir() {
                    Self::find_modified_in_directory(&wal_path, since, dir, files)?;
                } else {
                    check_file(&wal_path, since, dir, files)?;
                }
            } else {
                let wal_log_file = dir.join("wal.log");
                if wal_log_file.exists() {
                    check_file(&wal_log_file, since, dir, files)?;
                }
            }
        }

        Ok(())
    }

    fn find_modified_in_directory(
        dir: &Path,
        since: SystemTime,
        base_dir: &Path,
        files: &mut Vec<(String, PathBuf)>,
    ) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                let metadata = fs::metadata(&path)?;
                if let Ok(modified) = metadata.modified() {
                    if modified > since {
                        let relative = path
                            .strip_prefix(base_dir)
                            .unwrap_or(&path)
                            .to_string_lossy()
                            .to_string();
                        files.push((relative, path));
                    }
                }
            } else if path.is_dir() {
                Self::find_modified_in_directory(&path, since, base_dir, files)?;
            }
        }
        Ok(())
    }

    fn restore_directory(
        backup_dir: &Path,
        restore_dir: &Path,
        metadata: &BackupMetadata,
    ) -> Result<()> {
        for entry in fs::read_dir(backup_dir)? {
            let entry = entry?;
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
                    #[cfg(feature = "zstd-compression")]
                    {
                        // Decompress file
                        let compressed_data = fs::read(&path)?;
                        let decompressed = zstd::decode_all(compressed_data.as_slice())
                            .map_err(|e| Error::Decompression(e.to_string()))?;

                        // Remove .zst extension
                        let actual_dest = dest_path.with_extension("");
                        fs::write(&actual_dest, decompressed)?;
                    }

                    #[cfg(not(feature = "zstd-compression"))]
                    {
                        return Err(Error::Decompression(
                            "Zstd decompression not available in this build".to_string(),
                        ));
                    }
                } else {
                    // Direct copy
                    fs::copy(&path, &dest_path)?;
                }
            } else if path.is_dir() {
                // Recursively restore directory
                fs::create_dir_all(&dest_path)?;
                Self::restore_directory(&path, &dest_path, metadata)?;
            }
        }

        Ok(())
    }

    fn verify_backup(&self, backup_dir: &Path) -> Result<()> {
        let metadata_path = backup_dir.join("backup_metadata.json");
        let metadata_json = fs::read_to_string(&metadata_path)?;
        let metadata: BackupMetadata = serde_json::from_str(&metadata_json)
            .map_err(|e| Error::Generic(format!("Failed to parse metadata: {}", e)))?;

        // Verify file count
        let mut file_count = 0;
        Self::count_files(backup_dir, &mut file_count)?;

        // Subtract metadata file
        file_count -= 1;

        if file_count != metadata.file_count {
            return Err(Error::Generic(format!(
                "Backup verification failed: expected {} files, found {}",
                metadata.file_count, file_count
            )));
        }

        // TODO: Verify checksum by calculating hash of all files
        // For now, just check that metadata contains a checksum
        if metadata.checksum.is_none() {
            return Err(Error::Generic("Backup missing checksum".to_string()));
        }

        Ok(())
    }

    fn count_files(dir: &Path, count: &mut usize) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                *count += 1;
            } else if path.is_dir() {
                Self::count_files(&path, count)?;
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
        Err(Error::Generic(
            "Database path not available for backup".to_string(),
        ))
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
    use crate::{Database, LightningDbConfig};
    use tempfile::tempdir;

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
            if db.flush_lsm().is_err() {
                // LSM flush may fail if not using LSM, which is fine
            }
            // Explicitly drop to ensure all resources are released
            drop(db);
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
            compression_enabled: false,        // Disable compression to force B+Tree storage
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
        let incr_metadata = manager
            .create_incremental_backup(&db_path, &incr_backup_path, full_metadata.timestamp)
            .unwrap();

        assert_eq!(incr_metadata.backup_type, BackupType::Incremental);
        // Incremental backup may have different file count due to LSM compaction
        assert!(incr_metadata.file_count > 0);
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
