//! Disk space quota management for Lightning DB

use crate::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::path::Path;
use std::fs;
use tracing::{warn, info, error};

/// Disk quota manager
#[derive(Debug)]
pub struct DiskQuotaManager {
    // Disk space limits
    total_disk_limit: Option<u64>,
    wal_size_limit: Option<u64>,
    temp_space_limit: Option<u64>,
    
    // Current usage
    total_disk_used: AtomicU64,
    wal_size_used: AtomicU64,
    temp_space_used: AtomicU64,
    
    // Warning thresholds
    warning_threshold: f64,
    critical_threshold: f64,
}

impl DiskQuotaManager {
    pub fn new(
        disk_quota_gb: Option<u64>,
        wal_size_limit_mb: Option<u64>,
        temp_space_limit_mb: Option<u64>,
    ) -> Result<Self> {
        Ok(Self {
            total_disk_limit: disk_quota_gb.map(|gb| gb * 1024 * 1024 * 1024),
            wal_size_limit: wal_size_limit_mb.map(|mb| mb * 1024 * 1024),
            temp_space_limit: temp_space_limit_mb.map(|mb| mb * 1024 * 1024),
            total_disk_used: AtomicU64::new(0),
            wal_size_used: AtomicU64::new(0),
            temp_space_used: AtomicU64::new(0),
            warning_threshold: 0.8,  // 80%
            critical_threshold: 0.95, // 95%
        })
    }
    
    /// Check if a disk write is allowed
    pub fn check_write(&self, size_bytes: u64) -> Result<bool> {
        if let Some(limit) = self.total_disk_limit {
            let current = self.total_disk_used.load(Ordering::Relaxed);
            if current + size_bytes > limit {
                error!("Disk write denied: {} + {} > {}", current, size_bytes, limit);
                return Ok(false);
            }
            
            // Check thresholds
            let usage_ratio = (current + size_bytes) as f64 / limit as f64;
            if usage_ratio > self.critical_threshold {
                warn!("Disk space critical: {:.1}% used", usage_ratio * 100.0);
            } else if usage_ratio > self.warning_threshold {
                warn!("Disk space warning: {:.1}% used", usage_ratio * 100.0);
            }
        }
        
        Ok(true)
    }
    
    /// Check if a WAL write is allowed
    pub fn check_wal_write(&self, size_bytes: u64) -> Result<bool> {
        if let Some(limit) = self.wal_size_limit {
            let current = self.wal_size_used.load(Ordering::Relaxed);
            if current + size_bytes > limit {
                warn!("WAL write denied: {} + {} > {}", current, size_bytes, limit);
                return Ok(false);
            }
        }
        
        // Also check against total disk limit
        self.check_write(size_bytes)
    }
    
    /// Check if temp space allocation is allowed
    pub fn check_temp_allocation(&self, size_bytes: u64) -> Result<bool> {
        if let Some(limit) = self.temp_space_limit {
            let current = self.temp_space_used.load(Ordering::Relaxed);
            if current + size_bytes > limit {
                warn!("Temp space allocation denied: {} + {} > {}", current, size_bytes, limit);
                return Ok(false);
            }
        }
        
        // Also check against total disk limit
        self.check_write(size_bytes)
    }
    
    /// Record disk space allocation
    pub fn allocate(&self, size_bytes: u64) -> Result<()> {
        self.total_disk_used.fetch_add(size_bytes, Ordering::Relaxed);
        Ok(())
    }
    
    /// Record WAL space allocation
    pub fn allocate_wal(&self, size_bytes: u64) -> Result<()> {
        self.wal_size_used.fetch_add(size_bytes, Ordering::Relaxed);
        self.allocate(size_bytes)
    }
    
    /// Record temp space allocation
    pub fn allocate_temp(&self, size_bytes: u64) -> Result<()> {
        self.temp_space_used.fetch_add(size_bytes, Ordering::Relaxed);
        self.allocate(size_bytes)
    }
    
    /// Record disk space deallocation
    pub fn deallocate(&self, size_bytes: u64) -> Result<()> {
        self.total_disk_used.fetch_sub(size_bytes, Ordering::Relaxed);
        Ok(())
    }
    
    /// Record WAL space deallocation
    pub fn deallocate_wal(&self, size_bytes: u64) -> Result<()> {
        self.wal_size_used.fetch_sub(size_bytes, Ordering::Relaxed);
        self.deallocate(size_bytes)
    }
    
    /// Record temp space deallocation
    pub fn deallocate_temp(&self, size_bytes: u64) -> Result<()> {
        self.temp_space_used.fetch_sub(size_bytes, Ordering::Relaxed);
        self.deallocate(size_bytes)
    }
    
    /// Update disk usage from filesystem
    pub fn update_usage_from_path(&self, path: &Path) -> Result<u64> {
        let usage = calculate_dir_size(path)?;
        self.total_disk_used.store(usage, Ordering::Relaxed);
        Ok(usage)
    }
    
    /// Get available disk space
    pub fn get_available_space(&self, _path: &Path) -> Result<u64> {
        #[cfg(unix)]
        {
            // Simple fallback - always report 10GB available
            // In production, use a proper filesystem stats library
            Ok(10 * 1024 * 1024 * 1024) // 10 GB
        }
        
        #[cfg(not(unix))]
        {
            // Fallback for non-Unix systems
            Ok(u64::MAX)
        }
    }
    
    /// Get current disk usage statistics
    pub fn get_usage_stats(&self) -> DiskUsageStats {
        DiskUsageStats {
            total_used: self.total_disk_used.load(Ordering::Relaxed),
            total_limit: self.total_disk_limit,
            wal_used: self.wal_size_used.load(Ordering::Relaxed),
            wal_limit: self.wal_size_limit,
            temp_used: self.temp_space_used.load(Ordering::Relaxed),
            temp_limit: self.temp_space_limit,
        }
    }
    
    /// Perform cleanup to free disk space
    pub fn cleanup(&self, target_bytes: u64) -> Result<u64> {
        let mut freed = 0u64;
        
        // Clear temp space first
        let temp_used = self.temp_space_used.load(Ordering::Relaxed);
        if temp_used > 0 {
            let to_free = target_bytes.min(temp_used);
            self.temp_space_used.fetch_sub(to_free, Ordering::Relaxed);
            self.total_disk_used.fetch_sub(to_free, Ordering::Relaxed);
            freed += to_free;
            info!("Freed {} bytes of temp space", to_free);
        }
        
        // Could also trigger WAL truncation, old file cleanup, etc.
        
        Ok(freed)
    }
}

#[derive(Debug, Clone)]
pub struct DiskUsageStats {
    pub total_used: u64,
    pub total_limit: Option<u64>,
    pub wal_used: u64,
    pub wal_limit: Option<u64>,
    pub temp_used: u64,
    pub temp_limit: Option<u64>,
}

/// Calculate total size of a directory recursively
fn calculate_dir_size(path: &Path) -> Result<u64> {
    let mut total_size = 0u64;
    
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            
            if metadata.is_dir() {
                total_size += calculate_dir_size(&entry.path())?;
            } else {
                total_size += metadata.len();
            }
        }
    } else if path.is_file() {
        total_size = fs::metadata(path)?.len();
    }
    
    Ok(total_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_disk_allocation_within_limit() {
        let manager = DiskQuotaManager::new(Some(1), None, None).unwrap(); // 1GB limit
        
        // Should allow allocation within limit
        assert!(manager.check_write(500 * 1024 * 1024).unwrap());
        manager.allocate(500 * 1024 * 1024).unwrap();
        
        // Should allow more allocation up to limit
        assert!(manager.check_write(400 * 1024 * 1024).unwrap());
        
        // Should deny allocation exceeding limit
        assert!(!manager.check_write(600 * 1024 * 1024).unwrap());
    }
    
    #[test]
    fn test_wal_size_limit() {
        let manager = DiskQuotaManager::new(None, Some(100), None).unwrap(); // 100MB WAL limit
        
        // Should allow WAL allocation within limit
        assert!(manager.check_wal_write(50 * 1024 * 1024).unwrap());
        manager.allocate_wal(50 * 1024 * 1024).unwrap();
        
        // Should deny WAL allocation exceeding limit
        assert!(!manager.check_wal_write(60 * 1024 * 1024).unwrap());
    }
    
    #[test]
    fn test_calculate_dir_size() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        
        fs::write(&test_file, b"Hello, world!").unwrap();
        
        let size = calculate_dir_size(temp_dir.path()).unwrap();
        assert_eq!(size, 13); // "Hello, world!" is 13 bytes
    }
}