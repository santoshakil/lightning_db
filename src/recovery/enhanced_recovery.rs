//! Enhanced recovery mechanisms for Lightning DB
//! 
//! This module implements critical recovery improvements:
//! - Double-write buffer for torn page prevention
//! - Metadata redundancy with backup headers
//! - Improved WAL recovery with proper truncation
//! - Recovery progress tracking

use crate::error::{Error, Result};
use crate::storage::{Page, PageId};
use crate::wal::{WalEntry, LogSequenceNumber};
use crate::Header;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use crc32fast::Hasher;

/// Double-write buffer to prevent torn pages
pub struct DoubleWriteBuffer {
    buffer_file: PathBuf,
    buffer: Mutex<File>,
    page_size: usize,
    batch_size: usize,
    active: AtomicBool,
}

impl DoubleWriteBuffer {
    pub fn new(db_path: &Path, page_size: usize, batch_size: usize) -> Result<Self> {
        let buffer_file = db_path.join("double_write.buffer");
        
        let buffer = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&buffer_file)
            .map_err(|e| Error::Io(e))?;
            
        Ok(Self {
            buffer_file,
            buffer: Mutex::new(buffer),
            page_size,
            batch_size,
            active: AtomicBool::new(true),
        })
    }
    
    /// Write pages to double-write buffer first, then to their final location
    pub fn write_pages(&self, pages: &[(PageId, &Page)], write_fn: impl Fn(PageId, &Page) -> Result<()>) -> Result<()> {
        if !self.active.load(Ordering::Acquire) {
            // Double-write disabled, write directly
            for (page_id, page) in pages {
                write_fn(*page_id, page)?;
            }
            return Ok(());
        }
        
        // Phase 1: Write to double-write buffer with checksums
        {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.seek(SeekFrom::Start(0))?;
            
            // Write header with page count and checksum
            let header = DoubleWriteHeader {
                page_count: pages.len() as u32,
                checksum: 0, // Will be calculated
            };
            
            let mut hasher = Hasher::new();
            
            // Write pages data
            for (page_id, page) in pages {
                let page_data = page.as_bytes();
                buffer.write_all(&page_id.to_le_bytes())?;
                buffer.write_all(page_data)?;
                hasher.update(&page_id.to_le_bytes());
                hasher.update(page_data);
            }
            
            // Write header with checksum
            let checksum = hasher.finalize();
            buffer.seek(SeekFrom::Start(0))?;
            buffer.write_all(&header.page_count.to_le_bytes())?;
            buffer.write_all(&checksum.to_le_bytes())?;
            
            // Sync to ensure durability
            buffer.sync_all()?;
        }
        
        // Phase 2: Write pages to their final locations
        for (page_id, page) in pages {
            write_fn(*page_id, page)?;
        }
        
        // Phase 3: Mark double-write buffer as complete
        {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.seek(SeekFrom::Start(0))?;
            buffer.write_all(&0u32.to_le_bytes())?; // Clear page count
            buffer.sync_all()?;
        }
        
        Ok(())
    }
    
    /// Recover any incomplete writes from double-write buffer
    pub fn recover(&self, write_fn: impl Fn(PageId, Vec<u8>) -> Result<()>) -> Result<u32> {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.seek(SeekFrom::Start(0))?;
        
        // Read header
        let mut header_bytes = [0u8; 8];
        buffer.read_exact(&mut header_bytes)?;
        
        let page_count = u32::from_le_bytes([header_bytes[0], header_bytes[1], header_bytes[2], header_bytes[3]]);
        let expected_checksum = u32::from_le_bytes([header_bytes[4], header_bytes[5], header_bytes[6], header_bytes[7]]);
        
        if page_count == 0 {
            // No incomplete writes
            return Ok(0);
        }
        
        // Read and verify pages
        let mut hasher = Hasher::new();
        let mut recovered_pages = Vec::new();
        
        for _ in 0..page_count {
            let mut page_id_bytes = [0u8; 4];
            buffer.read_exact(&mut page_id_bytes)?;
            let page_id = PageId::from_le_bytes(page_id_bytes);
            
            let mut page_data = vec![0u8; self.page_size];
            buffer.read_exact(&mut page_data)?;
            
            hasher.update(&page_id_bytes);
            hasher.update(&page_data);
            
            recovered_pages.push((page_id, page_data));
        }
        
        // Verify checksum
        if hasher.finalize() != expected_checksum {
            return Err(Error::Corruption("Double-write buffer checksum mismatch".into()));
        }
        
        // Recover pages
        let mut recovered = 0;
        for (page_id, page_data) in recovered_pages {
            write_fn(page_id, page_data)?;
            recovered += 1;
        }
        
        // Clear buffer after successful recovery
        buffer.seek(SeekFrom::Start(0))?;
        buffer.write_all(&0u32.to_le_bytes())?;
        buffer.sync_all()?;
        
        Ok(recovered)
    }
}

#[repr(C)]
struct DoubleWriteHeader {
    page_count: u32,
    checksum: u32,
}

/// Redundant metadata storage with multiple header copies
pub struct RedundantMetadata {
    primary_path: PathBuf,
    backup_paths: Vec<PathBuf>,
    header_size: usize,
}

impl RedundantMetadata {
    pub fn new(db_path: &Path) -> Self {
        let primary_path = db_path.join("header");
        let backup_paths = vec![
            db_path.join("header.backup1"),
            db_path.join("header.backup2"),
            db_path.join("header.backup3"),
        ];
        
        Self {
            primary_path,
            backup_paths,
            header_size: std::mem::size_of::<Header>(),
        }
    }
    
    /// Write header to all locations
    pub fn write_header(&self, header: &Header) -> Result<()> {
        let header_bytes = header.to_bytes();
        
        // Write to primary first
        self.write_header_to_file(&self.primary_path, &header_bytes)?;
        
        // Then write to all backups
        for backup_path in &self.backup_paths {
            if let Err(e) = self.write_header_to_file(backup_path, &header_bytes) {
                eprintln!("Warning: Failed to write backup header to {:?}: {}", backup_path, e);
                // Continue with other backups
            }
        }
        
        Ok(())
    }
    
    /// Read header, trying all locations
    pub fn read_header(&self) -> Result<Header> {
        // Try primary first
        if let Ok(header) = self.read_header_from_file(&self.primary_path) {
            if self.verify_header(&header) {
                return Ok(header);
            }
        }
        
        // Try backups
        for backup_path in &self.backup_paths {
            if let Ok(header) = self.read_header_from_file(backup_path) {
                if self.verify_header(&header) {
                    // Restore primary from backup
                    let header_bytes = header.to_bytes();
                    let _ = self.write_header_to_file(&self.primary_path, &header_bytes);
                    return Ok(header);
                }
            }
        }
        
        Err(Error::Corruption("All metadata copies are corrupted".into()))
    }
    
    fn write_header_to_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
            
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    }
    
    fn read_header_from_file(&self, path: &Path) -> Result<Header> {
        let mut file = File::open(path)?;
        let mut data = vec![0u8; self.header_size];
        file.read_exact(&mut data)?;
        Header::from_bytes(&data)
    }
    
    fn verify_header(&self, header: &Header) -> bool {
        header.magic == Header::MAGIC && header.verify_checksum()
    }
}

/// Recovery progress tracker
pub struct RecoveryProgress {
    total_steps: AtomicU64,
    completed_steps: AtomicU64,
    current_phase: Mutex<String>,
    start_time: std::time::Instant,
}

impl RecoveryProgress {
    pub fn new() -> Self {
        Self {
            total_steps: AtomicU64::new(0),
            completed_steps: AtomicU64::new(0),
            current_phase: Mutex::new("Initializing".to_string()),
            start_time: std::time::Instant::now(),
        }
    }
    
    pub fn set_total_steps(&self, total: u64) {
        self.total_steps.store(total, Ordering::Relaxed);
    }
    
    pub fn set_phase(&self, phase: &str) {
        *self.current_phase.lock().unwrap() = phase.to_string();
    }
    
    pub fn increment_progress(&self) {
        self.completed_steps.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn get_progress(&self) -> (u64, u64, String, std::time::Duration) {
        let completed = self.completed_steps.load(Ordering::Relaxed);
        let total = self.total_steps.load(Ordering::Relaxed);
        let phase = self.current_phase.lock().unwrap().clone();
        let elapsed = self.start_time.elapsed();
        
        (completed, total, phase, elapsed)
    }
    
    pub fn estimate_remaining(&self) -> Option<std::time::Duration> {
        let completed = self.completed_steps.load(Ordering::Relaxed);
        let total = self.total_steps.load(Ordering::Relaxed);
        
        if completed == 0 || total == 0 {
            return None;
        }
        
        let elapsed = self.start_time.elapsed();
        let rate = completed as f64 / elapsed.as_secs_f64();
        let remaining = (total - completed) as f64 / rate;
        
        Some(std::time::Duration::from_secs_f64(remaining))
    }
}

/// Enhanced WAL recovery with proper truncation
pub struct EnhancedWalRecovery {
    wal_dir: PathBuf,
    checkpoint_lsn: AtomicU64,
    progress: Arc<RecoveryProgress>,
}

impl EnhancedWalRecovery {
    pub fn new(wal_dir: PathBuf, progress: Arc<RecoveryProgress>) -> Self {
        Self {
            wal_dir,
            checkpoint_lsn: AtomicU64::new(0),
            progress,
        }
    }
    
    /// Recover from WAL with progress tracking
    pub fn recover<F>(&self, mut apply_fn: F) -> Result<RecoveryStats>
    where
        F: FnMut(WalEntry) -> Result<()>,
    {
        self.progress.set_phase("Scanning WAL files");
        
        // Find all WAL segments
        let segments = self.find_wal_segments()?;
        let total_segments = segments.len();
        
        self.progress.set_total_steps(total_segments as u64);
        
        let mut stats = RecoveryStats::default();
        let checkpoint_lsn = self.checkpoint_lsn.load(Ordering::Acquire);
        
        for (idx, segment_path) in segments.iter().enumerate() {
            self.progress.set_phase(&format!("Processing segment {}/{}", idx + 1, total_segments));
            
            match self.recover_segment(segment_path, checkpoint_lsn, &mut apply_fn) {
                Ok(segment_stats) => {
                    stats.merge(segment_stats);
                }
                Err(e) => {
                    eprintln!("Warning: Failed to recover segment {:?}: {}", segment_path, e);
                    stats.corrupted_segments += 1;
                }
            }
            
            self.progress.increment_progress();
        }
        
        self.progress.set_phase("Recovery complete");
        
        Ok(stats)
    }
    
    /// Find all WAL segment files
    fn find_wal_segments(&self) -> Result<Vec<PathBuf>> {
        let mut segments = Vec::new();
        
        for entry in fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().map(|e| e == "wal").unwrap_or(false) {
                segments.push(path);
            }
        }
        
        // Sort by segment number
        segments.sort();
        
        Ok(segments)
    }
    
    /// Recover a single WAL segment
    fn recover_segment<F>(
        &self,
        path: &Path,
        checkpoint_lsn: LogSequenceNumber,
        apply_fn: &mut F,
    ) -> Result<RecoveryStats>
    where
        F: FnMut(WalEntry) -> Result<()>,
    {
        let mut stats = RecoveryStats::default();
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();
        
        let mut position = 0u64;
        
        while position < file_size {
            match WalEntry::read_from(&mut file) {
                Ok(entry) => {
                    if entry.lsn > checkpoint_lsn {
                        apply_fn(entry)?;
                        stats.applied_entries += 1;
                    } else {
                        stats.skipped_entries += 1;
                    }
                    
                    position = file.seek(SeekFrom::Current(0))?;
                }
                Err(e) => {
                    // Try to find next valid entry
                    if self.scan_for_next_entry(&mut file, &mut position, file_size)? {
                        stats.recovered_after_corruption += 1;
                    } else {
                        break;
                    }
                }
            }
        }
        
        Ok(stats)
    }
    
    /// Scan for next valid WAL entry after corruption
    fn scan_for_next_entry(
        &self,
        file: &mut File,
        position: &mut u64,
        file_size: u64,
    ) -> Result<bool> {
        const SCAN_WINDOW: u64 = 4096;
        
        while *position < file_size {
            let mut buffer = vec![0u8; SCAN_WINDOW.min(file_size - *position) as usize];
            file.read_exact(&mut buffer)?;
            
            // Look for WAL entry magic
            for i in 0..buffer.len().saturating_sub(8) {
                if &buffer[i..i + 4] == b"WALE" {
                    // Found potential entry, seek back
                    *position += i as u64;
                    file.seek(SeekFrom::Start(*position))?;
                    return Ok(true);
                }
            }
            
            *position += buffer.len() as u64;
        }
        
        Ok(false)
    }
    
    /// Truncate WAL after successful checkpoint
    pub fn truncate_after_checkpoint(&self, checkpoint_lsn: LogSequenceNumber) -> Result<()> {
        let segments = self.find_wal_segments()?;
        
        for segment_path in segments {
            if self.can_truncate_segment(&segment_path, checkpoint_lsn)? {
                fs::remove_file(&segment_path)?;
            }
        }
        
        self.checkpoint_lsn.store(checkpoint_lsn, Ordering::Release);
        
        Ok(())
    }
    
    /// Check if a segment can be safely truncated
    fn can_truncate_segment(&self, path: &Path, checkpoint_lsn: LogSequenceNumber) -> Result<bool> {
        let mut file = File::open(path)?;
        
        // Read last entry LSN
        let file_size = file.metadata()?.len();
        if file_size < 16 {
            return Ok(true); // Empty or corrupt
        }
        
        // Simple heuristic: read last valid entry
        // In production, maintain segment metadata
        file.seek(SeekFrom::End(-16))?;
        
        let mut lsn_bytes = [0u8; 8];
        file.read_exact(&mut lsn_bytes)?;
        let last_lsn = u64::from_le_bytes(lsn_bytes);
        
        Ok(last_lsn <= checkpoint_lsn)
    }
}

#[derive(Default, Debug)]
pub struct RecoveryStats {
    pub applied_entries: u64,
    pub skipped_entries: u64,
    pub corrupted_segments: u64,
    pub recovered_after_corruption: u64,
}

impl RecoveryStats {
    fn merge(&mut self, other: RecoveryStats) {
        self.applied_entries += other.applied_entries;
        self.skipped_entries += other.skipped_entries;
        self.corrupted_segments += other.corrupted_segments;
        self.recovered_after_corruption += other.recovered_after_corruption;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_double_write_buffer() {
        let temp_dir = TempDir::new().unwrap();
        let dwb = DoubleWriteBuffer::new(temp_dir.path(), 4096, 32).unwrap();
        
        // Test data
        let page1 = Page::new(1);
        let page2 = Page::new(2);
        let pages = vec![(1, &page1), (2, &page2)];
        
        let written_pages = Arc::new(Mutex::new(HashMap::new()));
        let written_pages_clone = written_pages.clone();
        
        // Write pages
        dwb.write_pages(&pages, |id, page| {
            written_pages_clone.lock().unwrap().insert(id, page.clone());
            Ok(())
        }).unwrap();
        
        // Verify pages were written
        assert_eq!(written_pages.lock().unwrap().len(), 2);
        
        // Simulate crash and recovery
        drop(written_pages);
        let recovered = Arc::new(Mutex::new(HashMap::new()));
        let recovered_clone = recovered.clone();
        
        let count = dwb.recover(|id, data| {
            recovered_clone.lock().unwrap().insert(id, data);
            Ok(())
        }).unwrap();
        
        // Should be no pages to recover (write completed)
        assert_eq!(count, 0);
    }
    
    #[test]
    fn test_redundant_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let metadata = RedundantMetadata::new(temp_dir.path());
        
        let header = Header {
            magic: Header::MAGIC,
            version: 1,
            page_size: 4096,
            checksum: 0,
        };
        
        // Write header
        metadata.write_header(&header).unwrap();
        
        // Read back
        let read_header = metadata.read_header().unwrap();
        assert_eq!(read_header.version, 1);
        assert_eq!(read_header.page_size, 4096);
        
        // Corrupt primary
        fs::write(&metadata.primary_path, b"corrupted").unwrap();
        
        // Should still read from backup
        let read_header = metadata.read_header().unwrap();
        assert_eq!(read_header.version, 1);
    }
}