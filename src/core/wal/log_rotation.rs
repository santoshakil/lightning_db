use super::checkpoint_manager::CheckpointManager;
use crate::core::error::{Error, Result};
use std::collections::BTreeMap;
use std::fs::{remove_file, rename, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Configuration for WAL log rotation and archival
#[derive(Debug, Clone)]
pub struct LogRotationConfig {
    /// Maximum size of a single WAL segment in bytes
    pub max_segment_size: u64,
    /// Maximum number of active WAL segments
    pub max_active_segments: usize,
    /// Maximum age of archived segments before deletion
    pub max_archive_age: Duration,
    /// Minimum number of archived segments to keep
    pub min_archive_count: usize,
    /// Directory for archived WAL segments
    pub archive_directory: PathBuf,
    /// Enable compression for archived segments
    pub compress_archives: bool,
    /// Verify segment integrity during rotation
    pub verify_on_rotate: bool,
    /// Enable automatic cleanup of old archives
    pub auto_cleanup: bool,
}

impl Default for LogRotationConfig {
    fn default() -> Self {
        Self {
            max_segment_size: 64 * 1024 * 1024, // 64 MB
            max_active_segments: 3,
            max_archive_age: Duration::from_secs(7 * 24 * 3600), // 1 week
            min_archive_count: 10,
            archive_directory: PathBuf::from("wal_archives"),
            compress_archives: true,
            verify_on_rotate: true,
            auto_cleanup: true,
        }
    }
}

/// Metadata for a WAL segment
#[derive(Debug, Clone)]
pub struct SegmentMetadata {
    pub segment_id: u64,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub file_path: PathBuf,
    pub file_size: u64,
    pub created_at: SystemTime,
    pub last_modified: SystemTime,
    pub checksum: u32,
    pub is_archived: bool,
    pub entry_count: u64,
}

impl SegmentMetadata {
    pub fn new(segment_id: u64, start_lsn: u64, file_path: PathBuf) -> Self {
        let now = SystemTime::now();
        Self {
            segment_id,
            start_lsn,
            end_lsn: start_lsn,
            file_path,
            file_size: 0,
            created_at: now,
            last_modified: now,
            checksum: 0,
            is_archived: false,
            entry_count: 0,
        }
    }
}

/// Manages WAL segment rotation and archival
pub struct LogRotationManager {
    config: LogRotationConfig,
    current_segment_id: AtomicU64,
    active_segments: Arc<Mutex<BTreeMap<u64, SegmentMetadata>>>,
    archived_segments: Arc<Mutex<BTreeMap<u64, SegmentMetadata>>>,
    base_directory: PathBuf,
    checkpoint_manager: Arc<CheckpointManager>,
    is_rotating: AtomicBool,
}

impl LogRotationManager {
    pub fn new<P: AsRef<Path>>(
        base_directory: P,
        config: LogRotationConfig,
        checkpoint_manager: Arc<CheckpointManager>,
    ) -> Result<Self> {
        let base_directory = base_directory.as_ref().to_path_buf();

        // Create directories
        std::fs::create_dir_all(&base_directory)
            .map_err(|e| Error::Io(format!("Failed to create WAL directory: {}", e)))?;
        std::fs::create_dir_all(&config.archive_directory)
            .map_err(|e| Error::Io(format!("Failed to create archive directory: {}", e)))?;

        let mut manager = Self {
            config,
            current_segment_id: AtomicU64::new(1),
            active_segments: Arc::new(Mutex::new(BTreeMap::new())),
            archived_segments: Arc::new(Mutex::new(BTreeMap::new())),
            base_directory,
            checkpoint_manager,
            is_rotating: AtomicBool::new(false),
        };

        // Initialize by scanning existing segments
        manager.scan_existing_segments()?;

        Ok(manager)
    }

    /// Scan for existing WAL segments and build metadata
    fn scan_existing_segments(&mut self) -> Result<()> {
        debug!("Scanning existing WAL segments");

        let mut active_segments = BTreeMap::new();
        let mut archived_segments = BTreeMap::new();
        let mut max_segment_id = 0;

        // Scan active segments
        for entry in std::fs::read_dir(&self.base_directory)
            .map_err(|e| Error::Io(format!("Failed to read WAL directory: {}", e)))?
        {
            let entry = entry.map_err(|e| Error::Io(e.to_string()))?;
            let path = entry.path();

            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("wal_segment_") && filename.ends_with(".log") {
                    if let Some(segment_id_str) = filename
                        .strip_prefix("wal_segment_")
                        .and_then(|s| s.strip_suffix(".log"))
                    {
                        if let Ok(segment_id) = segment_id_str.parse::<u64>() {
                            match self.build_segment_metadata(segment_id, &path, false) {
                                Ok(metadata) => {
                                    max_segment_id = max_segment_id.max(segment_id);
                                    active_segments.insert(segment_id, metadata);
                                }
                                Err(e) => {
                                    warn!("Failed to read segment {}: {}", segment_id, e);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Scan archived segments
        if self.config.archive_directory.exists() {
            for entry in std::fs::read_dir(&self.config.archive_directory)
                .map_err(|e| Error::Io(format!("Failed to read archive directory: {}", e)))?
            {
                let entry = entry.map_err(|e| Error::Io(e.to_string()))?;
                let path = entry.path();

                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("wal_archive_") && filename.ends_with(".log") {
                        if let Some(segment_id_str) = filename
                            .strip_prefix("wal_archive_")
                            .and_then(|s| s.strip_suffix(".log"))
                        {
                            if let Ok(segment_id) = segment_id_str.parse::<u64>() {
                                match self.build_segment_metadata(segment_id, &path, true) {
                                    Ok(metadata) => {
                                        max_segment_id = max_segment_id.max(segment_id);
                                        archived_segments.insert(segment_id, metadata);
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to read archived segment {}: {}",
                                            segment_id, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        self.current_segment_id
            .store(max_segment_id + 1, Ordering::SeqCst);
        *self.active_segments.lock().unwrap() = active_segments;
        *self.archived_segments.lock().unwrap() = archived_segments;

        info!(
            "Found {} active segments and {} archived segments",
            self.active_segments.lock().unwrap().len(),
            self.archived_segments.lock().unwrap().len()
        );

        Ok(())
    }

    /// Build metadata for a segment by reading its file
    fn build_segment_metadata(
        &self,
        segment_id: u64,
        file_path: &Path,
        is_archived: bool,
    ) -> Result<SegmentMetadata> {
        let file = File::open(file_path)
            .map_err(|e| Error::Io(format!("Failed to open segment file: {}", e)))?;

        let metadata = file
            .metadata()
            .map_err(|e| Error::Io(format!("Failed to read file metadata: {}", e)))?;

        let mut segment_metadata = SegmentMetadata::new(segment_id, 0, file_path.to_path_buf());
        segment_metadata.file_size = metadata.len();
        segment_metadata.created_at = metadata.created().unwrap_or_else(|_| SystemTime::now());
        segment_metadata.last_modified = metadata.modified().unwrap_or_else(|_| SystemTime::now());
        segment_metadata.is_archived = is_archived;

        // Try to extract LSN range by scanning the file
        if let Ok((start_lsn, end_lsn, entry_count)) = self.scan_segment_for_lsn_range(&file_path) {
            segment_metadata.start_lsn = start_lsn;
            segment_metadata.end_lsn = end_lsn;
            segment_metadata.entry_count = entry_count;
        }

        // Calculate checksum if verification is enabled
        if self.config.verify_on_rotate {
            segment_metadata.checksum = self.calculate_file_checksum(file_path)?;
        }

        Ok(segment_metadata)
    }

    /// Scan a segment file to determine LSN range and entry count
    fn scan_segment_for_lsn_range(&self, file_path: &Path) -> Result<(u64, u64, u64)> {
        let mut file = File::open(file_path)?;
        let mut start_lsn = u64::MAX;
        let mut end_lsn = 0;
        let mut entry_count = 0;
        let mut buffer = [0u8; 4];

        loop {
            // Read entry length
            match file.read_exact(&mut buffer) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::Io(e.to_string())),
            }

            let length = u32::from_le_bytes(buffer) as usize;
            if length == 0 || length > 10 * 1024 * 1024 {
                break; // Invalid entry
            }

            // Read entry data
            let mut entry_data = vec![0u8; length];
            match file.read_exact(&mut entry_data) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::Io(e.to_string())),
            }

            // Try to deserialize and extract LSN
            if let Ok((entry, _)) = bincode::decode_from_slice::<super::WALEntry, _>(
                &entry_data,
                bincode::config::standard(),
            ) {
                start_lsn = start_lsn.min(entry.lsn);
                end_lsn = end_lsn.max(entry.lsn);
                entry_count += 1;
            }
        }

        if start_lsn == u64::MAX {
            start_lsn = 0;
        }

        Ok((start_lsn, end_lsn, entry_count))
    }

    /// Calculate CRC32 checksum of a file
    fn calculate_file_checksum(&self, file_path: &Path) -> Result<u32> {
        let mut file = File::open(file_path)?;
        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = [0u8; 8192];

        loop {
            match file.read(&mut buffer)? {
                0 => break,
                n => hasher.update(&buffer[..n]),
            }
        }

        Ok(hasher.finalize())
    }

    /// Check if current segment should be rotated
    pub fn should_rotate_segment(&self, current_segment_size: u64) -> bool {
        current_segment_size >= self.config.max_segment_size
            || self.active_segments.lock().unwrap().len() >= self.config.max_active_segments
    }

    /// Rotate the current WAL segment
    pub fn rotate_segment(&self, current_lsn: u64) -> Result<u64> {
        if self
            .is_rotating
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return Err(Error::Generic(
                "Another rotation is already in progress".to_string(),
            ));
        }

        let _guard = RotationGuard::new(&self.is_rotating);

        info!("Starting WAL segment rotation at LSN {}", current_lsn);

        let new_segment_id = self.current_segment_id.fetch_add(1, Ordering::SeqCst);
        let new_segment_path = self
            .base_directory
            .join(format!("wal_segment_{:08}.log", new_segment_id));

        // Create new segment file
        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&new_segment_path)
                .map_err(|e| Error::Io(format!("Failed to create new segment: {}", e)))?;

            let mut writer = BufWriter::new(file);

            // Write segment header
            writer.write_all(b"WALSEG\x01\x00")?; // Magic + version
            writer.write_all(&new_segment_id.to_le_bytes())?;
            writer.write_all(&current_lsn.to_le_bytes())?;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            writer.write_all(&timestamp.to_le_bytes())?;

            writer.flush()?;
            let file = writer.into_inner().map_err(|e| Error::Io(e.to_string()))?;
            file.sync_all().map_err(|e| Error::Io(e.to_string()))?;
        }

        // Add new segment to active segments
        {
            let mut active = self.active_segments.lock().unwrap();
            let metadata =
                SegmentMetadata::new(new_segment_id, current_lsn, new_segment_path.clone());
            active.insert(new_segment_id, metadata);
        }

        // Archive old segments if we have too many active
        self.archive_old_segments()?;

        // Cleanup old archives if configured
        if self.config.auto_cleanup {
            self.cleanup_old_archives()?;
        }

        info!(
            "WAL segment rotation completed. New segment ID: {}",
            new_segment_id
        );
        Ok(new_segment_id)
    }

    /// Archive segments that are beyond the active limit
    fn archive_old_segments(&self) -> Result<()> {
        let checkpoint_lsn = self.checkpoint_manager.last_checkpoint_lsn();
        let mut segments_to_archive = Vec::new();

        {
            let active = self.active_segments.lock().unwrap();
            if active.len() <= self.config.max_active_segments {
                return Ok(());
            }

            // Find segments that can be archived (before last checkpoint)
            for (segment_id, metadata) in active.iter() {
                if metadata.end_lsn <= checkpoint_lsn
                    && segments_to_archive.len() < (active.len() - self.config.max_active_segments)
                {
                    segments_to_archive.push(*segment_id);
                }
            }
        }

        for segment_id in segments_to_archive {
            self.archive_segment(segment_id)?;
        }

        Ok(())
    }

    /// Archive a specific segment
    fn archive_segment(&self, segment_id: u64) -> Result<()> {
        info!("Archiving segment {}", segment_id);

        let metadata = {
            let active = self.active_segments.lock().unwrap();
            active.get(&segment_id).cloned().ok_or_else(|| {
                Error::Generic(format!(
                    "Segment {} not found in active segments",
                    segment_id
                ))
            })?
        };

        // Create archive path
        let archive_path = self
            .config
            .archive_directory
            .join(format!("wal_archive_{:08}.log", segment_id));

        // Move file to archive location
        rename(&metadata.file_path, &archive_path)
            .map_err(|e| Error::Io(format!("Failed to move segment to archive: {}", e)))?;

        // Update metadata
        let mut archived_metadata = metadata.clone();
        archived_metadata.file_path = archive_path;
        archived_metadata.is_archived = true;

        // Move from active to archived
        {
            let mut active = self.active_segments.lock().unwrap();
            let mut archived = self.archived_segments.lock().unwrap();

            active.remove(&segment_id);
            archived.insert(segment_id, archived_metadata);
        }

        info!("Segment {} successfully archived", segment_id);
        Ok(())
    }

    /// Cleanup old archived segments based on age and count limits
    fn cleanup_old_archives(&self) -> Result<()> {
        let mut archives_to_delete = Vec::new();
        let now = SystemTime::now();

        {
            let archived = self.archived_segments.lock().unwrap();

            // Skip if we don't have enough archives to consider cleanup
            if archived.len() <= self.config.min_archive_count {
                return Ok(());
            }

            // Collect archives that are too old
            for (segment_id, metadata) in archived.iter() {
                if let Ok(age) = now.duration_since(metadata.created_at) {
                    if age > self.config.max_archive_age {
                        archives_to_delete.push(*segment_id);
                    }
                }
            }

            // If we still have too many after age-based cleanup, remove oldest
            let remaining_count = archived.len() - archives_to_delete.len();
            if remaining_count > self.config.min_archive_count {
                let mut sorted_archives: Vec<_> = archived
                    .iter()
                    .filter(|(id, _)| !archives_to_delete.contains(id))
                    .collect();
                sorted_archives.sort_by_key(|(_, metadata)| metadata.created_at);

                let excess_count = remaining_count - self.config.min_archive_count;
                for (segment_id, _) in sorted_archives.into_iter().take(excess_count) {
                    archives_to_delete.push(*segment_id);
                }
            }
        }

        // Delete the archives
        for segment_id in archives_to_delete {
            self.delete_archived_segment(segment_id)?;
        }

        Ok(())
    }

    /// Delete an archived segment
    fn delete_archived_segment(&self, segment_id: u64) -> Result<()> {
        info!("Deleting archived segment {}", segment_id);

        let file_path = {
            let mut archived = self.archived_segments.lock().unwrap();
            let metadata = archived.remove(&segment_id).ok_or_else(|| {
                Error::Generic(format!("Archived segment {} not found", segment_id))
            })?;
            metadata.file_path
        };

        remove_file(&file_path)
            .map_err(|e| Error::Io(format!("Failed to delete archived segment: {}", e)))?;

        info!("Archived segment {} deleted", segment_id);
        Ok(())
    }

    /// Get list of active segments
    pub fn get_active_segments(&self) -> Vec<SegmentMetadata> {
        self.active_segments
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Get list of archived segments
    pub fn get_archived_segments(&self) -> Vec<SegmentMetadata> {
        self.archived_segments
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Get total storage used by WAL segments
    pub fn get_total_storage_usage(&self) -> (u64, u64) {
        let active_usage: u64 = self
            .active_segments
            .lock()
            .unwrap()
            .values()
            .map(|m| m.file_size)
            .sum();
        let archived_usage: u64 = self
            .archived_segments
            .lock()
            .unwrap()
            .values()
            .map(|m| m.file_size)
            .sum();

        (active_usage, archived_usage)
    }

    /// Force cleanup of old archives (manual trigger)
    pub fn force_cleanup(&self) -> Result<()> {
        info!("Forcing WAL archive cleanup");
        self.cleanup_old_archives()
    }
}

/// RAII guard to ensure rotation flag is cleared
struct RotationGuard<'a> {
    is_rotating: &'a AtomicBool,
}

impl<'a> RotationGuard<'a> {
    fn new(is_rotating: &'a AtomicBool) -> Self {
        Self { is_rotating }
    }
}

impl Drop for RotationGuard<'_> {
    fn drop(&mut self) {
        self.is_rotating.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_rotation_config_default() {
        let config = LogRotationConfig::default();
        assert_eq!(config.max_segment_size, 64 * 1024 * 1024);
        assert_eq!(config.max_active_segments, 3);
        assert!(config.verify_on_rotate);
    }

    #[test]
    fn test_segment_metadata_creation() {
        let metadata = SegmentMetadata::new(1, 100, PathBuf::from("test.log"));
        assert_eq!(metadata.segment_id, 1);
        assert_eq!(metadata.start_lsn, 100);
        assert_eq!(metadata.end_lsn, 100);
        assert!(!metadata.is_archived);
    }
}
