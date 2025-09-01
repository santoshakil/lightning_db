//! Incremental Backup with Deduplication
//!
//! Provides efficient incremental backup capabilities with content-based
//! deduplication, delta compression, and intelligent change detection.

use crate::core::error::{Error, Result};
use blake3;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Incremental backup manager with deduplication
pub struct IncrementalBackupManager {
    config: IncrementalConfig,
    chunk_store: ChunkStore,
    file_index: FileIndex,
    dedup_index: DeduplicationIndex,
    delta_engine: DeltaEngine,
}

/// Configuration for incremental backups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalConfig {
    pub chunk_size_bytes: usize,
    pub max_chunk_size_bytes: usize,
    pub min_chunk_size_bytes: usize,
    pub rolling_hash_window: usize,
    pub deduplication_enabled: bool,
    pub delta_compression_enabled: bool,
    pub content_defined_chunking: bool,
    pub compression_algorithm: CompressionAlgorithm,
    pub hash_algorithm: HashAlgorithm,
    pub similarity_threshold: f64,
    pub max_file_size_for_delta: u64,
    pub backup_metadata_compression: bool,
}

impl Default for IncrementalConfig {
    fn default() -> Self {
        Self {
            chunk_size_bytes: 64 * 1024,       // 64KB default chunk size
            max_chunk_size_bytes: 1024 * 1024, // 1MB max chunk size
            min_chunk_size_bytes: 4 * 1024,    // 4KB min chunk size
            rolling_hash_window: 64,
            deduplication_enabled: true,
            delta_compression_enabled: true,
            content_defined_chunking: true,
            compression_algorithm: CompressionAlgorithm::Zstd,
            hash_algorithm: HashAlgorithm::Blake3,
            similarity_threshold: 0.85,
            max_file_size_for_delta: 100 * 1024 * 1024, // 100MB
            backup_metadata_compression: true,
        }
    }
}

/// Supported compression algorithms
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Zstd,
    Lz4,
    Brotli,
}

/// Supported hash algorithms
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum HashAlgorithm {
    Sha256,
    Blake3,
    XxHash,
}

/// Chunk store for managing deduplicated data blocks
struct ChunkStore {
    chunks: HashMap<String, ChunkMetadata>,
    storage_path: PathBuf,
    total_chunks: usize,
    total_size: u64,
    dedup_savings: u64,
}

/// Metadata for a deduplicated chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChunkMetadata {
    pub hash: String,
    pub size: usize,
    pub compressed_size: usize,
    pub reference_count: usize,
    pub first_seen: SystemTime,
    pub last_accessed: SystemTime,
    pub compression_algorithm: CompressionAlgorithm,
    pub storage_location: ChunkLocation,
}

/// Location of chunk in storage
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ChunkLocation {
    File { path: PathBuf, offset: u64 },
    Memory { data: Vec<u8> },
    Remote { url: String, key: String },
}

/// File index for tracking file changes
struct FileIndex {
    files: HashMap<PathBuf, FileEntry>,
    snapshots: BTreeMap<SystemTime, SnapshotEntry>,
}

/// Entry for a tracked file
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub modified_time: SystemTime,
    pub content_hash: String,
    pub chunk_hashes: Vec<String>,
    pub file_type: FileType,
    pub backup_generations: Vec<BackupGeneration>,
}

/// Type of file
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum FileType {
    Regular,
    Directory,
    Symlink,
    Special,
}

/// Backup generation information
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupGeneration {
    pub backup_id: String,
    pub timestamp: SystemTime,
    pub change_type: ChangeType,
    pub size_change: i64,
    pub chunks_added: usize,
    pub chunks_removed: usize,
}

/// Type of change detected
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ChangeType {
    Created,
    Modified,
    Deleted,
    Moved,
    Metadata,
    Content,
}

/// Snapshot entry for point-in-time state
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotEntry {
    pub backup_id: String,
    pub timestamp: SystemTime,
    pub file_count: usize,
    pub total_size: u64,
    pub chunk_count: usize,
    pub dedup_ratio: f64,
    pub changed_files: Vec<PathBuf>,
}

/// Deduplication index for content similarity
struct DeduplicationIndex {
    content_hashes: HashMap<String, Vec<PathBuf>>,
    similarity_index: HashMap<String, SimilarityEntry>,
    chunk_references: HashMap<String, HashSet<PathBuf>>,
}

/// Similarity entry for near-duplicate detection
#[derive(Debug, Clone)]
struct SimilarityEntry {
    pub file_path: PathBuf,
    pub fingerprint: Vec<u64>,
    pub size: u64,
    pub similarity_scores: HashMap<PathBuf, f64>,
}

/// Delta compression engine
struct DeltaEngine {
    config: IncrementalConfig,
}

/// Incremental backup result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalBackupResult {
    pub backup_id: String,
    pub timestamp: SystemTime,
    pub base_backup_id: Option<String>,
    pub files_processed: usize,
    pub files_changed: usize,
    pub files_added: usize,
    pub files_deleted: usize,
    pub total_size: u64,
    pub compressed_size: u64,
    pub dedup_savings: u64,
    pub dedup_ratio: f64,
    pub chunks_created: usize,
    pub chunks_reused: usize,
    pub processing_time: Duration,
    pub change_summary: ChangeSummary,
}

/// Summary of changes in backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeSummary {
    pub created_files: Vec<PathBuf>,
    pub modified_files: Vec<PathBuf>,
    pub deleted_files: Vec<PathBuf>,
    pub moved_files: Vec<(PathBuf, PathBuf)>,
    pub largest_changes: Vec<LargeChange>,
    pub file_type_stats: HashMap<String, FileTypeStats>,
}

/// Statistics for a file type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileTypeStats {
    pub count: usize,
    pub total_size: u64,
    pub average_size: u64,
    pub largest_file: Option<PathBuf>,
}

/// Information about a large change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LargeChange {
    pub file_path: PathBuf,
    pub change_type: ChangeType,
    pub size_before: u64,
    pub size_after: u64,
    pub size_change: i64,
}

impl IncrementalBackupManager {
    /// Create a new incremental backup manager
    pub fn new(config: IncrementalConfig, storage_path: &Path) -> Result<Self> {
        let chunk_store = ChunkStore {
            chunks: HashMap::new(),
            storage_path: storage_path.join("chunks"),
            total_chunks: 0,
            total_size: 0,
            dedup_savings: 0,
        };

        // Create chunk storage directory
        std::fs::create_dir_all(&chunk_store.storage_path)?;

        let file_index = FileIndex {
            files: HashMap::new(),
            snapshots: BTreeMap::new(),
        };

        let dedup_index = DeduplicationIndex {
            content_hashes: HashMap::new(),
            similarity_index: HashMap::new(),
            chunk_references: HashMap::new(),
        };

        let delta_engine = DeltaEngine {
            config: config.clone(),
        };

        Ok(Self {
            config,
            chunk_store,
            file_index,
            dedup_index,
            delta_engine,
        })
    }

    /// Create an incremental backup
    pub fn create_incremental_backup(
        &mut self,
        source_paths: &[&Path],
        backup_id: &str,
        base_backup_id: Option<&str>,
    ) -> Result<IncrementalBackupResult> {
        let start_time = SystemTime::now();
        println!("🔄 Creating incremental backup: {}", backup_id);

        let mut result = IncrementalBackupResult {
            backup_id: backup_id.to_string(),
            timestamp: start_time,
            base_backup_id: base_backup_id.map(|s| s.to_string()),
            files_processed: 0,
            files_changed: 0,
            files_added: 0,
            files_deleted: 0,
            total_size: 0,
            compressed_size: 0,
            dedup_savings: 0,
            dedup_ratio: 0.0,
            chunks_created: 0,
            chunks_reused: 0,
            processing_time: Duration::from_secs(0),
            change_summary: ChangeSummary {
                created_files: Vec::new(),
                modified_files: Vec::new(),
                deleted_files: Vec::new(),
                moved_files: Vec::new(),
                largest_changes: Vec::new(),
                file_type_stats: HashMap::new(),
            },
        };

        // Scan for changes
        let changes = self.detect_changes(source_paths, base_backup_id)?;
        println!("📊 Detected {} changed files", changes.len());

        // Process changed files
        for (file_path, change_type) in changes {
            match change_type {
                ChangeType::Created | ChangeType::Modified | ChangeType::Content => {
                    self.process_changed_file(&file_path, change_type, backup_id, &mut result)?;
                    result.files_changed += 1;

                    if change_type == ChangeType::Created {
                        result.files_added += 1;
                        result.change_summary.created_files.push(file_path.clone());
                    } else {
                        result.change_summary.modified_files.push(file_path.clone());
                    }
                }
                ChangeType::Deleted => {
                    self.process_deleted_file(&file_path, backup_id)?;
                    result.files_deleted += 1;
                    result.change_summary.deleted_files.push(file_path);
                }
                ChangeType::Moved => {
                    // Handle moved files
                    println!("📦 File moved: {:?}", file_path);
                }
                ChangeType::Metadata => {
                    // Handle metadata-only changes
                    println!("🏷️  Metadata changed: {:?}", file_path);
                }
            }

            result.files_processed += 1;
        }

        // Calculate statistics
        result.dedup_ratio = if result.total_size > 0 {
            result.dedup_savings as f64 / result.total_size as f64
        } else {
            0.0
        };

        result.processing_time = start_time.elapsed().unwrap_or_default();

        // Create snapshot entry
        let snapshot = SnapshotEntry {
            backup_id: backup_id.to_string(),
            timestamp: start_time,
            file_count: result.files_processed,
            total_size: result.total_size,
            chunk_count: result.chunks_created + result.chunks_reused,
            dedup_ratio: result.dedup_ratio,
            changed_files: result.change_summary.created_files.clone(),
        };

        self.file_index.snapshots.insert(start_time, snapshot);

        println!("✅ Incremental backup completed");
        println!("   Files processed: {}", result.files_processed);
        println!("   Files changed: {}", result.files_changed);
        println!("   Dedup ratio: {:.1}%", result.dedup_ratio * 100.0);
        println!("   Processing time: {:?}", result.processing_time);

        Ok(result)
    }

    /// Detect changes since last backup
    fn detect_changes(
        &mut self,
        source_paths: &[&Path],
        base_backup_id: Option<&str>,
    ) -> Result<Vec<(PathBuf, ChangeType)>> {
        let mut changes = Vec::new();

        // Clone the snapshot information to avoid borrow issues
        let base_snapshot_opt = if let Some(base_id) = base_backup_id {
            self.find_snapshot_by_backup_id(base_id)?.cloned()
        } else {
            None
        };

        // Then scan for changes (this requires &mut self)
        for source_path in source_paths {
            self.scan_directory_for_changes(
                source_path,
                &base_snapshot_opt.as_ref(),
                &mut changes,
            )?;
        }

        // Sort changes by priority (deletions first, then creates, then modifications)
        changes.sort_by(|a, b| {
            let priority_a = match a.1 {
                ChangeType::Deleted => 0,
                ChangeType::Created => 1,
                ChangeType::Modified => 2,
                ChangeType::Content => 2,
                ChangeType::Metadata => 3,
                ChangeType::Moved => 4,
            };
            let priority_b = match b.1 {
                ChangeType::Deleted => 0,
                ChangeType::Created => 1,
                ChangeType::Modified => 2,
                ChangeType::Content => 2,
                ChangeType::Metadata => 3,
                ChangeType::Moved => 4,
            };
            priority_a.cmp(&priority_b)
        });

        Ok(changes)
    }

    /// Scan directory recursively for changes
    fn scan_directory_for_changes(
        &mut self,
        dir_path: &Path,
        base_snapshot: &Option<&SnapshotEntry>,
        changes: &mut Vec<(PathBuf, ChangeType)>,
    ) -> Result<()> {
        if !dir_path.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = entry.metadata()?;

            if metadata.is_file() {
                let change_type = self.detect_file_change(&path, &metadata, base_snapshot)?;
                if let Some(change_type) = change_type {
                    changes.push((path, change_type));
                }
            } else if metadata.is_dir() {
                self.scan_directory_for_changes(&path, base_snapshot, changes)?;
            }
        }

        Ok(())
    }

    /// Detect changes in a specific file
    fn detect_file_change(
        &mut self,
        file_path: &Path,
        metadata: &std::fs::Metadata,
        _base_snapshot: &Option<&SnapshotEntry>,
    ) -> Result<Option<ChangeType>> {
        if let Some(existing_entry) = self.file_index.files.get(file_path) {
            // File exists in index, check for changes
            let current_modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);

            if current_modified > existing_entry.modified_time {
                // File has been modified, check if content changed
                let current_hash = self.calculate_file_hash(file_path)?;
                if current_hash != existing_entry.content_hash {
                    return Ok(Some(ChangeType::Content));
                } else {
                    return Ok(Some(ChangeType::Metadata));
                }
            }
        } else {
            // New file
            return Ok(Some(ChangeType::Created));
        }

        Ok(None)
    }

    /// Process a changed file
    fn process_changed_file(
        &mut self,
        file_path: &Path,
        change_type: ChangeType,
        backup_id: &str,
        result: &mut IncrementalBackupResult,
    ) -> Result<()> {
        let metadata = std::fs::metadata(file_path)?;
        let file_size = metadata.len();

        println!("📄 Processing file: {:?} ({} bytes)", file_path, file_size);

        // Calculate content hash
        let content_hash = self.calculate_file_hash(file_path)?;

        // Check for deduplication opportunities
        if self.config.deduplication_enabled {
            if let Some(existing_chunks) = self.find_existing_chunks(&content_hash) {
                // File content already exists, just update references
                self.update_chunk_references(file_path, &existing_chunks)?;
                result.chunks_reused += existing_chunks.len();
                result.dedup_savings += file_size;
                return Ok(());
            }
        }

        // Split file into chunks
        let chunks = if self.config.content_defined_chunking {
            self.create_content_defined_chunks(file_path)?
        } else {
            self.create_fixed_size_chunks(file_path)?
        };

        let mut new_chunks = 0;
        let mut reused_chunks = 0;
        let mut compressed_size = 0;

        // Process each chunk
        for chunk in chunks {
            if let Some(existing_chunk) = self.chunk_store.chunks.get(&chunk.hash) {
                // Chunk already exists, increment reference count
                reused_chunks += 1;
                compressed_size += existing_chunk.compressed_size;
                self.increment_chunk_reference(&chunk.hash)?;
            } else {
                // New chunk, store it
                let stored_chunk = self.store_chunk(&chunk)?;
                compressed_size += stored_chunk.compressed_size;
                new_chunks += 1;
            }
        }

        // Update file index
        let chunk_hashes: Vec<String> = self.get_file_chunk_hashes(file_path)?;
        let file_entry = FileEntry {
            path: file_path.to_path_buf(),
            size: file_size,
            modified_time: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            content_hash,
            chunk_hashes,
            file_type: FileType::Regular,
            backup_generations: vec![BackupGeneration {
                backup_id: backup_id.to_string(),
                timestamp: SystemTime::now(),
                change_type,
                size_change: file_size as i64,
                chunks_added: new_chunks,
                chunks_removed: 0,
            }],
        };

        self.file_index
            .files
            .insert(file_path.to_path_buf(), file_entry);

        // Update result statistics
        result.total_size += file_size;
        result.compressed_size += compressed_size as u64;
        result.chunks_created += new_chunks;
        result.chunks_reused += reused_chunks;

        Ok(())
    }

    /// Process a deleted file
    fn process_deleted_file(&mut self, file_path: &Path, _backup_id: &str) -> Result<()> {
        println!("🗑️  Processing deleted file: {:?}", file_path);

        if let Some(file_entry) = self.file_index.files.remove(file_path) {
            // Decrement reference counts for chunks
            for chunk_hash in &file_entry.chunk_hashes {
                self.decrement_chunk_reference(chunk_hash)?;
            }
        }

        Ok(())
    }

    /// Create content-defined chunks using rolling hash
    fn create_content_defined_chunks(&self, file_path: &Path) -> Result<Vec<Chunk>> {
        let mut file = File::open(file_path)?;
        let mut chunks = Vec::new();
        let mut buffer = vec![0u8; self.config.max_chunk_size_bytes];
        let mut chunk_start = 0usize;

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            let chunk_boundaries = self.find_chunk_boundaries(&buffer[..bytes_read])?;

            for &boundary in &chunk_boundaries {
                if boundary > chunk_start {
                    let chunk_data = &buffer[chunk_start..boundary];
                    let chunk = self.create_chunk_from_data(chunk_data)?;
                    chunks.push(chunk);
                    chunk_start = boundary;
                }
            }

            // Handle remaining data
            if chunk_start < bytes_read {
                let chunk_data = &buffer[chunk_start..bytes_read];
                let chunk = self.create_chunk_from_data(chunk_data)?;
                chunks.push(chunk);
            }
        }

        Ok(chunks)
    }

    /// Create fixed-size chunks
    fn create_fixed_size_chunks(&self, file_path: &Path) -> Result<Vec<Chunk>> {
        let mut file = File::open(file_path)?;
        let mut chunks = Vec::new();
        let mut buffer = vec![0u8; self.config.chunk_size_bytes];

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            let chunk_data = &buffer[..bytes_read];
            let chunk = self.create_chunk_from_data(chunk_data)?;
            chunks.push(chunk);
        }

        Ok(chunks)
    }

    /// Find chunk boundaries using rolling hash
    fn find_chunk_boundaries(&self, data: &[u8]) -> Result<Vec<usize>> {
        let mut boundaries = Vec::new();
        let window_size = self.config.rolling_hash_window;

        if data.len() <= window_size {
            return Ok(vec![data.len()]);
        }

        let mut rolling_hash = 0u64;
        let base = 31u64;
        let modulus = 1_000_000_007u64;

        // Initialize rolling hash for first window
        for &b in data.iter().take(window_size) {
            rolling_hash = (rolling_hash * base + b as u64) % modulus;
        }

        // Look for chunk boundaries
        for (i, window) in data.windows(window_size + 1).enumerate() {
            // Update rolling hash using a sliding window
            let old_char = window[0] as u64;
            let new_char = window[window_size] as u64;
            let base_power = self.mod_pow(base, window_size as u64 - 1, modulus);

            rolling_hash = (rolling_hash + modulus - (old_char * base_power) % modulus) % modulus;
            rolling_hash = (rolling_hash * base + new_char) % modulus;

            // Check if this is a chunk boundary (low bits of hash are zero)
            if rolling_hash & 0xFFF == 0 && i + window_size >= self.config.min_chunk_size_bytes {
                boundaries.push(i);
            }
        }

        // Always include the end of data
        if boundaries.is_empty() || boundaries.last() != Some(&data.len()) {
            boundaries.push(data.len());
        }

        Ok(boundaries)
    }

    /// Modular exponentiation helper
    fn mod_pow(&self, base: u64, exp: u64, modulus: u64) -> u64 {
        let mut result = 1;
        let mut base = base % modulus;
        let mut exp = exp;

        while exp > 0 {
            if exp % 2 == 1 {
                result = (result * base) % modulus;
            }
            exp >>= 1;
            base = (base * base) % modulus;
        }

        result
    }

    /// Create a chunk from raw data
    fn create_chunk_from_data(&self, data: &[u8]) -> Result<Chunk> {
        let hash = match self.config.hash_algorithm {
            HashAlgorithm::Sha256 => {
                let mut hasher = Sha256::new();
                hasher.update(data);
                format!("{:x}", hasher.finalize())
            }
            HashAlgorithm::Blake3 => {
                let hash = blake3::hash(data);
                hash.to_hex().to_string()
            }
            HashAlgorithm::XxHash => {
                // For simplicity, fall back to Blake3
                let hash = blake3::hash(data);
                hash.to_hex().to_string()
            }
        };

        Ok(Chunk {
            hash,
            data: data.to_vec(),
            size: data.len(),
        })
    }

    /// Store a chunk in the chunk store
    fn store_chunk(&mut self, chunk: &Chunk) -> Result<ChunkMetadata> {
        let compressed_data = self.compress_chunk_data(&chunk.data)?;
        let chunk_file_path = self.chunk_store.storage_path.join(&chunk.hash);

        std::fs::write(&chunk_file_path, &compressed_data)?;

        let metadata = ChunkMetadata {
            hash: chunk.hash.clone(),
            size: chunk.size,
            compressed_size: compressed_data.len(),
            reference_count: 1,
            first_seen: SystemTime::now(),
            last_accessed: SystemTime::now(),
            compression_algorithm: self.config.compression_algorithm,
            storage_location: ChunkLocation::File {
                path: chunk_file_path,
                offset: 0,
            },
        };

        self.chunk_store
            .chunks
            .insert(chunk.hash.clone(), metadata.clone());
        self.chunk_store.total_chunks += 1;
        self.chunk_store.total_size += chunk.size as u64;

        Ok(metadata)
    }

    /// Compress chunk data
    fn compress_chunk_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression_algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Zstd => {
                #[cfg(feature = "zstd-compression")]
                {
                    zstd::encode_all(data, 3)
                        .map_err(|e| Error::Generic(format!("Zstd compression failed: {}", e)))
                }
                #[cfg(not(feature = "zstd-compression"))]
                {
                    Err(Error::Generic("Zstd compression not available".to_string()))
                }
            }
            CompressionAlgorithm::Lz4 => {
                use crate::features::compression::{Compressor, Lz4Compressor};
                let compressor = Lz4Compressor;
                compressor.compress(data)
            }
            CompressionAlgorithm::Brotli => Err(Error::Generic(
                "Brotli compression not yet implemented".to_string(),
            )),
        }
    }

    /// Calculate file hash
    fn calculate_file_hash(&self, file_path: &Path) -> Result<String> {
        let mut file = File::open(file_path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 8192];

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        Ok(format!("{:x}", hasher.finalize()))
    }

    /// Find existing chunks for a file
    fn find_existing_chunks(&self, content_hash: &str) -> Option<Vec<String>> {
        if let Some(file_paths) = self.dedup_index.content_hashes.get(content_hash) {
            if let Some(first_path) = file_paths.first() {
                if let Some(file_entry) = self.file_index.files.get(first_path) {
                    return Some(file_entry.chunk_hashes.clone());
                }
            }
        }
        None
    }

    /// Update chunk references for a file
    fn update_chunk_references(&mut self, file_path: &Path, chunk_hashes: &[String]) -> Result<()> {
        for chunk_hash in chunk_hashes {
            self.increment_chunk_reference(chunk_hash)?;

            self.dedup_index
                .chunk_references
                .entry(chunk_hash.clone())
                .or_insert_with(HashSet::new)
                .insert(file_path.to_path_buf());
        }
        Ok(())
    }

    /// Increment chunk reference count
    fn increment_chunk_reference(&mut self, chunk_hash: &str) -> Result<()> {
        if let Some(chunk_metadata) = self.chunk_store.chunks.get_mut(chunk_hash) {
            chunk_metadata.reference_count += 1;
            chunk_metadata.last_accessed = SystemTime::now();
        }
        Ok(())
    }

    /// Decrement chunk reference count
    fn decrement_chunk_reference(&mut self, chunk_hash: &str) -> Result<()> {
        if let Some(chunk_metadata) = self.chunk_store.chunks.get_mut(chunk_hash) {
            chunk_metadata.reference_count = chunk_metadata.reference_count.saturating_sub(1);

            // If no more references, mark for cleanup
            if chunk_metadata.reference_count == 0 {
                println!("🧹 Chunk marked for cleanup: {}", chunk_hash);
            }
        }
        Ok(())
    }

    /// Get chunk hashes for a file
    fn get_file_chunk_hashes(&self, file_path: &Path) -> Result<Vec<String>> {
        if let Some(file_entry) = self.file_index.files.get(file_path) {
            Ok(file_entry.chunk_hashes.clone())
        } else {
            Ok(Vec::new())
        }
    }

    /// Find snapshot by backup ID
    fn find_snapshot_by_backup_id(&self, backup_id: &str) -> Result<Option<&SnapshotEntry>> {
        for snapshot in self.file_index.snapshots.values() {
            if snapshot.backup_id == backup_id {
                return Ok(Some(snapshot));
            }
        }
        Ok(None)
    }

    /// Get deduplication statistics
    pub fn get_dedup_statistics(&self) -> DeduplicationStatistics {
        let total_unique_chunks = self.chunk_store.chunks.len();
        let total_chunk_references: usize = self
            .chunk_store
            .chunks
            .values()
            .map(|chunk| chunk.reference_count)
            .sum();

        let avg_chunk_size = if total_unique_chunks > 0 {
            self.chunk_store.total_size / total_unique_chunks as u64
        } else {
            0
        };

        let storage_efficiency = if self.chunk_store.total_size > 0 {
            let total_compressed_size: u64 = self
                .chunk_store
                .chunks
                .values()
                .map(|chunk| chunk.compressed_size as u64)
                .sum();
            1.0 - (total_compressed_size as f64 / self.chunk_store.total_size as f64)
        } else {
            0.0
        };

        DeduplicationStatistics {
            total_unique_chunks,
            total_chunk_references,
            total_storage_size: self.chunk_store.total_size,
            dedup_savings: self.chunk_store.dedup_savings,
            avg_chunk_size,
            storage_efficiency,
            compression_ratio: storage_efficiency,
        }
    }
}

/// Chunk data structure
#[derive(Debug, Clone)]
struct Chunk {
    hash: String,
    data: Vec<u8>,
    size: usize,
}

/// Deduplication statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicationStatistics {
    pub total_unique_chunks: usize,
    pub total_chunk_references: usize,
    pub total_storage_size: u64,
    pub dedup_savings: u64,
    pub avg_chunk_size: u64,
    pub storage_efficiency: f64,
    pub compression_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_manager() -> (IncrementalBackupManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = IncrementalConfig::default();
        let manager = IncrementalBackupManager::new(config, temp_dir.path()).unwrap();
        (manager, temp_dir)
    }

    #[test]
    fn test_incremental_manager_creation() {
        let (manager, _temp_dir) = create_test_manager();
        assert!(manager.config.deduplication_enabled);
        assert_eq!(manager.config.chunk_size_bytes, 64 * 1024);
    }

    #[test]
    fn test_chunk_creation() {
        let (manager, _temp_dir) = create_test_manager();
        let test_data = b"This is test data for chunking";

        let chunk = manager.create_chunk_from_data(test_data).unwrap();
        assert_eq!(chunk.size, test_data.len());
        assert!(!chunk.hash.is_empty());
    }

    #[test]
    fn test_content_defined_chunking() {
        let (manager, temp_dir) = create_test_manager();

        // Create a test file
        let test_file = temp_dir.path().join("test.txt");
        let test_data = b"This is a test file with some content that will be chunked using content-defined chunking algorithm.";
        std::fs::write(&test_file, test_data).unwrap();

        let chunks = manager.create_content_defined_chunks(&test_file).unwrap();
        assert!(!chunks.is_empty());

        // Verify total size matches
        let total_chunk_size: usize = chunks.iter().map(|c| c.size).sum();
        assert_eq!(total_chunk_size, test_data.len());
    }

    #[test]
    fn test_file_hash_calculation() {
        let (manager, temp_dir) = create_test_manager();

        // Create a test file
        let test_file = temp_dir.path().join("test.txt");
        let test_data = b"Test content for hash calculation";
        std::fs::write(&test_file, test_data).unwrap();

        let hash1 = manager.calculate_file_hash(&test_file).unwrap();
        let hash2 = manager.calculate_file_hash(&test_file).unwrap();

        // Same file should produce same hash
        assert_eq!(hash1, hash2);
        assert!(!hash1.is_empty());
    }

    #[test]
    fn test_dedup_statistics() {
        let (manager, _temp_dir) = create_test_manager();
        let stats = manager.get_dedup_statistics();

        assert_eq!(stats.total_unique_chunks, 0);
        assert_eq!(stats.total_storage_size, 0);
        assert_eq!(stats.dedup_savings, 0);
    }
}
