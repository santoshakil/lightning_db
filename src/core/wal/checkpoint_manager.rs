use crate::core::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Checkpoint metadata stored in the checkpoint file
#[derive(Debug, Clone)]
pub struct CheckpointMetadata {
    pub checkpoint_lsn: u64,
    pub creation_timestamp: u64,
    pub data_file_size: u64,
    pub wal_file_size: u64,
    pub active_transactions: Vec<u64>,
    pub committed_lsn: u64,
    pub version: u32,
    pub checksum: u32,
}

impl CheckpointMetadata {
    const VERSION: u32 = 1;
    const MAGIC: u32 = 0x4348_4B50; // "CHKP"

    pub fn new(checkpoint_lsn: u64, committed_lsn: u64, active_transactions: Vec<u64>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            checkpoint_lsn,
            creation_timestamp: timestamp,
            data_file_size: 0,
            wal_file_size: 0,
            active_transactions,
            committed_lsn,
            version: Self::VERSION,
            checksum: 0,
        }
    }

    pub fn calculate_checksum(&mut self) {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.checkpoint_lsn.to_le_bytes());
        hasher.update(&self.creation_timestamp.to_le_bytes());
        hasher.update(&self.data_file_size.to_le_bytes());
        hasher.update(&self.wal_file_size.to_le_bytes());
        hasher.update(&self.committed_lsn.to_le_bytes());
        hasher.update(&self.version.to_le_bytes());

        for tx_id in &self.active_transactions {
            hasher.update(&tx_id.to_le_bytes());
        }

        self.checksum = hasher.finalize();
    }

    pub fn verify_checksum(&self) -> bool {
        let mut test_metadata = self.clone();
        test_metadata.checksum = 0;
        test_metadata.calculate_checksum();
        test_metadata.checksum == self.checksum
    }
}

/// Manages checkpoint creation, validation, and recovery
pub struct CheckpointManager {
    base_path: PathBuf,
    last_checkpoint_lsn: AtomicU64,
    config: CheckpointConfig,
}

#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    pub checkpoint_interval: Duration,
    pub max_checkpoint_age: Duration,
    pub keep_checkpoint_count: usize,
    pub enable_compression: bool,
    pub enable_incremental: bool,
    pub verify_on_create: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
            max_checkpoint_age: Duration::from_secs(3600), // 1 hour
            keep_checkpoint_count: 5,
            enable_compression: true,
            enable_incremental: false,
            verify_on_create: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    pub checkpoint_lsn: u64,
    pub committed_lsn: u64,
    pub creation_timestamp: u64,
    pub file_path: PathBuf,
    pub metadata: CheckpointMetadata,
}

impl CheckpointManager {
    pub fn new<P: AsRef<Path>>(base_path: P, config: CheckpointConfig) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path)
            .map_err(|e| Error::Io(format!("Failed to create checkpoint directory: {}", e)))?;

        // Find the latest checkpoint
        let last_checkpoint_lsn = Self::find_latest_checkpoint_lsn(&base_path)?;

        Ok(Self {
            base_path,
            last_checkpoint_lsn: AtomicU64::new(last_checkpoint_lsn),
            config,
        })
    }

    fn find_latest_checkpoint_lsn(base_path: &Path) -> Result<u64> {
        let mut max_lsn = 0;

        if !base_path.exists() {
            return Ok(0);
        }

        for entry in std::fs::read_dir(base_path)
            .map_err(|e| Error::Io(format!("Failed to read checkpoint directory: {}", e)))?
        {
            let entry = entry.map_err(|e| Error::Io(e.to_string()))?;
            let path = entry.path();

            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("checkpoint_") && filename.ends_with(".chk") {
                    if let Some(lsn_str) = filename
                        .strip_prefix("checkpoint_")
                        .and_then(|s| s.strip_suffix(".chk"))
                    {
                        if let Ok(lsn) = lsn_str.parse::<u64>() {
                            max_lsn = max_lsn.max(lsn);
                        }
                    }
                }
            }
        }

        Ok(max_lsn)
    }

    /// Create a new checkpoint at the specified LSN
    pub fn create_checkpoint<F>(
        &self,
        checkpoint_lsn: u64,
        mut get_state: F,
    ) -> Result<CheckpointInfo>
    where
        F: FnMut() -> Result<(u64, Vec<u64>)>, // Returns (committed_lsn, active_transactions)
    {
        info!("Creating checkpoint at LSN {}", checkpoint_lsn);

        let (committed_lsn, active_transactions) = get_state()?;

        // Create checkpoint metadata
        let mut metadata =
            CheckpointMetadata::new(checkpoint_lsn, committed_lsn, active_transactions);

        // Set file sizes if available
        if let Ok(data_size) = self.get_file_size("data.db") {
            metadata.data_file_size = data_size;
        }
        if let Ok(wal_size) = self.get_file_size("wal.log") {
            metadata.wal_file_size = wal_size;
        }

        metadata.calculate_checksum();

        // Write checkpoint file
        let checkpoint_path = self
            .base_path
            .join(format!("checkpoint_{:016}.chk", checkpoint_lsn));
        let temp_path = checkpoint_path.with_extension("tmp");

        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)
                .map_err(|e| Error::Io(format!("Failed to create checkpoint file: {}", e)))?;

            let mut writer = BufWriter::new(file);

            // Write checkpoint header
            writer.write_all(&CheckpointMetadata::MAGIC.to_le_bytes())?;
            writer.write_all(&metadata.version.to_le_bytes())?;
            writer.write_all(&metadata.checkpoint_lsn.to_le_bytes())?;
            writer.write_all(&metadata.creation_timestamp.to_le_bytes())?;
            writer.write_all(&metadata.committed_lsn.to_le_bytes())?;
            writer.write_all(&metadata.data_file_size.to_le_bytes())?;
            writer.write_all(&metadata.wal_file_size.to_le_bytes())?;

            // Write active transactions
            writer.write_all(&(metadata.active_transactions.len() as u32).to_le_bytes())?;
            for tx_id in &metadata.active_transactions {
                writer.write_all(&tx_id.to_le_bytes())?;
            }

            writer.write_all(&metadata.checksum.to_le_bytes())?;
            writer.flush()?;

            // Sync to disk
            let file = writer.into_inner().map_err(|e| Error::Io(e.to_string()))?;
            file.sync_all().map_err(|e| Error::Io(e.to_string()))?;
        }

        // Atomically rename temp file to final checkpoint
        std::fs::rename(&temp_path, &checkpoint_path)
            .map_err(|e| Error::Io(format!("Failed to rename checkpoint file: {}", e)))?;

        info!(
            "Checkpoint created successfully at LSN {} -> {:?}",
            checkpoint_lsn, checkpoint_path
        );

        // Verify checkpoint if configured
        if self.config.verify_on_create {
            self.verify_checkpoint(&checkpoint_path)?;
        }

        // Update last checkpoint LSN
        self.last_checkpoint_lsn
            .store(checkpoint_lsn, Ordering::SeqCst);

        // Clean up old checkpoints
        self.cleanup_old_checkpoints()?;

        Ok(CheckpointInfo {
            checkpoint_lsn,
            committed_lsn,
            creation_timestamp: metadata.creation_timestamp,
            file_path: checkpoint_path,
            metadata,
        })
    }

    /// Load and verify a checkpoint by LSN
    pub fn load_checkpoint(&self, checkpoint_lsn: u64) -> Result<CheckpointInfo> {
        let checkpoint_path = self
            .base_path
            .join(format!("checkpoint_{:016}.chk", checkpoint_lsn));

        if !checkpoint_path.exists() {
            return Err(Error::WalCorruption {
                offset: checkpoint_lsn,
                reason: format!("Checkpoint not found at path: {}", checkpoint_path.display()),
            });
        }

        let metadata = self.read_checkpoint_metadata(&checkpoint_path)?;

        // Verify checkpoint integrity
        if !metadata.verify_checksum() {
            return Err(Error::WalCorruption {
                offset: checkpoint_lsn,
                reason: "Checksum verification failed".to_string(),
            });
        }

        if metadata.checkpoint_lsn != checkpoint_lsn {
            return Err(Error::WalCorruption {
                offset: checkpoint_lsn,
                reason: format!(
                    "LSN mismatch: expected {}, found {}",
                    checkpoint_lsn, metadata.checkpoint_lsn
                ),
            });
        }

        Ok(CheckpointInfo {
            checkpoint_lsn: metadata.checkpoint_lsn,
            committed_lsn: metadata.committed_lsn,
            creation_timestamp: metadata.creation_timestamp,
            file_path: checkpoint_path,
            metadata,
        })
    }

    /// Find the latest valid checkpoint
    pub fn find_latest_checkpoint(&self) -> Result<Option<CheckpointInfo>> {
        let checkpoints = self.list_checkpoints()?;

        // Sort by LSN in descending order
        let mut sorted_checkpoints = checkpoints;
        sorted_checkpoints.sort_by(|a, b| b.checkpoint_lsn.cmp(&a.checkpoint_lsn));

        // Find the first valid checkpoint
        for info in sorted_checkpoints {
            match self.load_checkpoint(info.checkpoint_lsn) {
                Ok(checkpoint) => return Ok(Some(checkpoint)),
                Err(e) => {
                    warn!(
                        "Failed to load checkpoint at LSN {}: {}",
                        info.checkpoint_lsn, e
                    );
                }
            }
        }

        Ok(None)
    }

    /// List all available checkpoints
    pub fn list_checkpoints(&self) -> Result<Vec<CheckpointInfo>> {
        let mut checkpoints = Vec::new();

        if !self.base_path.exists() {
            return Ok(checkpoints);
        }

        for entry in std::fs::read_dir(&self.base_path)
            .map_err(|e| Error::Io(format!("Failed to read checkpoint directory: {}", e)))?
        {
            let entry = entry.map_err(|e| Error::Io(e.to_string()))?;
            let path = entry.path();

            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("checkpoint_") && filename.ends_with(".chk") {
                    if let Some(lsn_str) = filename
                        .strip_prefix("checkpoint_")
                        .and_then(|s| s.strip_suffix(".chk"))
                    {
                        if let Ok(checkpoint_lsn) = lsn_str.parse::<u64>() {
                            match self.read_checkpoint_metadata(&path) {
                                Ok(metadata) => {
                                    checkpoints.push(CheckpointInfo {
                                        checkpoint_lsn,
                                        committed_lsn: metadata.committed_lsn,
                                        creation_timestamp: metadata.creation_timestamp,
                                        file_path: path,
                                        metadata,
                                    });
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to read checkpoint metadata from {:?}: {}",
                                        path, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(checkpoints)
    }

    fn read_checkpoint_metadata(&self, checkpoint_path: &Path) -> Result<CheckpointMetadata> {
        let file = File::open(checkpoint_path)
            .map_err(|e| Error::Io(format!("Failed to open checkpoint file: {}", e)))?;
        let mut reader = BufReader::new(file);

        // Read header
        let mut magic = [0u8; 4];
        let mut version = [0u8; 4];
        let mut checkpoint_lsn = [0u8; 8];
        let mut creation_timestamp = [0u8; 8];
        let mut committed_lsn = [0u8; 8];
        let mut data_file_size = [0u8; 8];
        let mut wal_file_size = [0u8; 8];

        reader.read_exact(&mut magic)?;
        reader.read_exact(&mut version)?;
        reader.read_exact(&mut checkpoint_lsn)?;
        reader.read_exact(&mut creation_timestamp)?;
        reader.read_exact(&mut committed_lsn)?;
        reader.read_exact(&mut data_file_size)?;
        reader.read_exact(&mut wal_file_size)?;

        if u32::from_le_bytes(magic) != CheckpointMetadata::MAGIC {
            return Err(Error::WalCorruption {
                offset: 0,
                reason: "Invalid checkpoint magic".to_string(),
            });
        }

        let version = u32::from_le_bytes(version);
        if version != CheckpointMetadata::VERSION {
            return Err(Error::WalCorruption {
                offset: 0,
                reason: format!("Unsupported checkpoint version: {}", version),
            });
        }

        // Read active transactions
        let mut tx_count = [0u8; 4];
        reader.read_exact(&mut tx_count)?;
        let tx_count = u32::from_le_bytes(tx_count) as usize;

        let mut active_transactions = Vec::with_capacity(tx_count);
        for _ in 0..tx_count {
            let mut tx_id = [0u8; 8];
            reader.read_exact(&mut tx_id)?;
            active_transactions.push(u64::from_le_bytes(tx_id));
        }

        // Read checksum
        let mut checksum = [0u8; 4];
        reader.read_exact(&mut checksum)?;

        Ok(CheckpointMetadata {
            checkpoint_lsn: u64::from_le_bytes(checkpoint_lsn),
            creation_timestamp: u64::from_le_bytes(creation_timestamp),
            data_file_size: u64::from_le_bytes(data_file_size),
            wal_file_size: u64::from_le_bytes(wal_file_size),
            active_transactions,
            committed_lsn: u64::from_le_bytes(committed_lsn),
            version,
            checksum: u32::from_le_bytes(checksum),
        })
    }

    fn verify_checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        debug!("Verifying checkpoint: {:?}", checkpoint_path);

        let metadata = self.read_checkpoint_metadata(checkpoint_path)?;

        if !metadata.verify_checksum() {
            return Err(Error::WalCorruption {
                offset: metadata.checkpoint_lsn,
                reason: "Checksum verification failed".to_string(),
            });
        }

        // Additional validation checks
        if metadata.checkpoint_lsn == 0 {
            return Err(Error::WalCorruption {
                offset: metadata.checkpoint_lsn,
                reason: "Invalid LSN (zero)".to_string(),
            });
        }

        if metadata.committed_lsn > metadata.checkpoint_lsn {
            return Err(Error::WalCorruption {
                offset: metadata.checkpoint_lsn,
                reason: format!(
                    "Committed LSN {} > checkpoint LSN {}",
                    metadata.committed_lsn, metadata.checkpoint_lsn
                ),
            });
        }

        debug!(
            "Checkpoint verification successful: LSN {}",
            metadata.checkpoint_lsn
        );
        Ok(())
    }

    fn cleanup_old_checkpoints(&self) -> Result<()> {
        let mut checkpoints = self.list_checkpoints()?;

        if checkpoints.len() <= self.config.keep_checkpoint_count {
            return Ok(());
        }

        // Sort by LSN in descending order (newest first)
        checkpoints.sort_by(|a, b| b.checkpoint_lsn.cmp(&a.checkpoint_lsn));

        // Remove old checkpoints beyond the keep count
        for checkpoint in checkpoints
            .into_iter()
            .skip(self.config.keep_checkpoint_count)
        {
            info!(
                "Removing old checkpoint at LSN {}",
                checkpoint.checkpoint_lsn
            );
            if let Err(e) = std::fs::remove_file(&checkpoint.file_path) {
                warn!(
                    "Failed to remove old checkpoint {:?}: {}",
                    checkpoint.file_path, e
                );
            }
        }

        Ok(())
    }

    fn get_file_size(&self, filename: &str) -> Result<u64> {
        let path = self.base_path.join(filename);
        Ok(std::fs::metadata(path)?.len())
    }

    /// Get the LSN of the last checkpoint
    pub fn last_checkpoint_lsn(&self) -> u64 {
        self.last_checkpoint_lsn.load(Ordering::Acquire)
    }

    /// Check if a checkpoint should be created based on configuration
    pub fn should_create_checkpoint(&self, current_lsn: u64) -> bool {
        let last_checkpoint = self.last_checkpoint_lsn();

        // Always create first checkpoint
        if last_checkpoint == 0 {
            return true;
        }

        // Check if enough operations have passed (rough heuristic)
        if current_lsn - last_checkpoint > 1000 {
            return true;
        }

        // Check time-based criteria
        if let Ok(Some(latest_checkpoint)) = self.find_latest_checkpoint() {
            let age = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .saturating_sub(latest_checkpoint.creation_timestamp);

            if Duration::from_secs(age) > self.config.checkpoint_interval {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_checkpoint_creation_and_loading() {
        let dir = tempdir().expect("Failed to create temp dir");
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(dir.path(), config).expect("Failed to create manager");

        // Create a checkpoint
        let checkpoint_info = manager
            .create_checkpoint(100, || Ok((95, vec![1, 2, 3])))
            .expect("Failed to create checkpoint");

        assert_eq!(checkpoint_info.checkpoint_lsn, 100);
        assert_eq!(checkpoint_info.committed_lsn, 95);
        assert_eq!(checkpoint_info.metadata.active_transactions, vec![1, 2, 3]);

        // Load the checkpoint
        let loaded = manager
            .load_checkpoint(100)
            .expect("Failed to load checkpoint");
        assert_eq!(loaded.checkpoint_lsn, 100);
        assert_eq!(loaded.committed_lsn, 95);
        assert_eq!(loaded.metadata.active_transactions, vec![1, 2, 3]);
    }

    #[test]
    fn test_checkpoint_metadata_checksum() {
        let mut metadata = CheckpointMetadata::new(100, 95, vec![1, 2, 3]);
        metadata.calculate_checksum();

        assert!(metadata.verify_checksum());

        // Corrupt the metadata
        metadata.checkpoint_lsn = 101;
        assert!(!metadata.verify_checksum());
    }
}
