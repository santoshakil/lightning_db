use crate::core::error::{Error, Result};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Configuration for memory-mapped file optimizations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MmapConfig {
    pub enable_huge_pages: bool,
    pub enable_prefault: bool,
    pub enable_async_msync: bool,
    pub max_mapped_regions: usize,
    pub region_size: usize,
    pub flush_interval: Duration,
    pub populate_on_map: bool,
    pub lock_on_fault: bool,
    pub use_direct_io: bool,
    pub huge_page_size: usize,
    pub enable_transparent_huge_pages: bool,
    pub numa_aware_allocation: bool,
}

impl Default for MmapConfig {
    fn default() -> Self {
        Self {
            enable_huge_pages: true,
            enable_prefault: true,
            enable_async_msync: true,
            max_mapped_regions: 16,
            region_size: 256 * 1024 * 1024, // 256MB regions
            flush_interval: Duration::from_secs(5),
            populate_on_map: false,
            lock_on_fault: false,
            use_direct_io: false,
            huge_page_size: 2 * 1024 * 1024, // 2MB huge pages
            enable_transparent_huge_pages: true,
            numa_aware_allocation: false,
        }
    }
}

/// Optimized memory-mapped file manager with multiple regions
#[derive(Debug)]
pub struct OptimizedMmapManager {
    config: MmapConfig,
    regions: Arc<RwLock<HashMap<u64, MmapRegion>>>,
    _file_path: PathBuf,
    file: Arc<Mutex<File>>,
    file_size: Arc<AtomicU64>,

    // Statistics
    page_faults: Arc<AtomicU64>,
    region_creates: Arc<AtomicU64>,
    region_evictions: Arc<AtomicU64>,
    bytes_synced: Arc<AtomicU64>,

    // Background sync thread
    sync_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    shutdown: Arc<AtomicUsize>,
}

struct MmapRegion {
    _region_id: u64,
    offset: u64,
    size: usize,
    mmap: MmapMut,
    last_access: Instant,
    dirty: bool,
    access_count: u64,
}

impl std::fmt::Debug for MmapRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapRegion")
            .field("_region_id", &self._region_id)
            .field("offset", &self.offset)
            .field("size", &self.size)
            .field("mmap", &"<MmapMut>")
            .field("last_access", &self.last_access)
            .field("dirty", &self.dirty)
            .field("access_count", &self.access_count)
            .finish()
    }
}

impl OptimizedMmapManager {
    pub fn create<P: AsRef<Path>>(path: P, initial_size: u64, config: MmapConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true).create(true);

        #[cfg(target_os = "linux")]
        if config.use_direct_io {
            use std::os::unix::fs::OpenOptionsExt;
            open_options.custom_flags(libc::O_DIRECT);
        }

        let file = open_options.open(&path)?;
        file.set_len(initial_size)?;

        let manager = Self {
            config,
            regions: Arc::new(RwLock::new(HashMap::new())),
            _file_path: path,
            file: Arc::new(Mutex::new(file)),
            file_size: Arc::new(AtomicU64::new(initial_size)),
            page_faults: Arc::new(AtomicU64::new(0)),
            region_creates: Arc::new(AtomicU64::new(0)),
            region_evictions: Arc::new(AtomicU64::new(0)),
            bytes_synced: Arc::new(AtomicU64::new(0)),
            sync_thread: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicUsize::new(0)),
        };

        manager.start_background_sync();
        Ok(manager)
    }

    pub fn open<P: AsRef<Path>>(path: P, config: MmapConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true);

        #[cfg(target_os = "linux")]
        if config.use_direct_io {
            use std::os::unix::fs::OpenOptionsExt;
            open_options.custom_flags(libc::O_DIRECT);
        }

        let file = open_options.open(&path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();

        let manager = Self {
            config,
            regions: Arc::new(RwLock::new(HashMap::new())),
            _file_path: path,
            file: Arc::new(Mutex::new(file)),
            file_size: Arc::new(AtomicU64::new(file_size)),
            page_faults: Arc::new(AtomicU64::new(0)),
            region_creates: Arc::new(AtomicU64::new(0)),
            region_evictions: Arc::new(AtomicU64::new(0)),
            bytes_synced: Arc::new(AtomicU64::new(0)),
            sync_thread: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicUsize::new(0)),
        };

        manager.start_background_sync();
        Ok(manager)
    }

    /// Get or create a memory-mapped region for the given offset
    fn get_or_create_region(&self, offset: u64) -> Result<u64> {
        let region_id = offset / self.config.region_size as u64;
        let region_offset = region_id * self.config.region_size as u64;

        // Check if region already exists
        {
            let mut regions = self.regions.write();
            if let Some(region) = regions.get_mut(&region_id) {
                region.last_access = Instant::now();
                region.access_count += 1;
                return Ok(region_id);
            }
        }

        // Create new region
        self.create_region(region_id, region_offset)?;
        Ok(region_id)
    }

    fn create_region(&self, region_id: u64, offset: u64) -> Result<()> {
        let mut regions = self.regions.write();

        // Check again with write lock
        if regions.contains_key(&region_id) {
            return Ok(());
        }

        // Evict LRU region if at capacity
        if regions.len() >= self.config.max_mapped_regions {
            self.evict_lru_region(&mut regions)?;
        }

        // Determine region size
        let file_size = self.file_size.load(Ordering::Acquire);
        let remaining = file_size.saturating_sub(offset);
        let region_size = self.config.region_size.min(remaining as usize);

        if region_size == 0 {
            return Err(Error::InvalidPageId);
        }

        // Create memory mapping with huge pages support
        let file = self.file.lock();
        let mut mmap_options = MmapOptions::new();

        // Configure for huge pages if enabled
        if self.config.enable_huge_pages {
            // Align region to huge page boundary
            let huge_page_size = self.config.huge_page_size;
            let aligned_offset = (offset / huge_page_size as u64) * huge_page_size as u64;
            let aligned_size =
                (region_size + huge_page_size - 1).div_ceil(huge_page_size) * huge_page_size;

            mmap_options.offset(aligned_offset).len(aligned_size);
        }

        // SAFETY: Creating memory mapping from file descriptor
        // Invariants:
        // 1. file is a valid, open file descriptor (locked above)
        // 2. offset and region_size are validated to be within file bounds
        // 3. mmap_options configured with valid parameters
        // 4. File remains open for lifetime of mapping
        // Guarantees:
        // - Memory mapping is valid for file's lifetime
        // - OS manages page fault handling and synchronization
        // - map_mut() ensures exclusive access to mapped region
        let mmap = unsafe {
            mmap_options
                .offset(offset)
                .len(region_size)
                .map_mut(&*file)?
        };

        // Apply huge pages hint on Linux
        #[cfg(target_os = "linux")]
        if self.config.enable_huge_pages || self.config.enable_transparent_huge_pages {
            self.advise_huge_pages(&mut mmap);
        }

        // Apply memory advice for performance after mapping
        // Note: memmap2 advise() method may require mutable reference

        // Prefault pages if configured
        if self.config.enable_prefault {
            self.prefault_region(&mmap);
        }

        let region = MmapRegion {
            _region_id: region_id,
            offset,
            size: region_size,
            mmap,
            last_access: Instant::now(),
            dirty: false,
            access_count: 0,
        };

        regions.insert(region_id, region);
        self.region_creates.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn evict_lru_region(&self, regions: &mut HashMap<u64, MmapRegion>) -> Result<()> {
        // Find LRU region
        let lru_id = regions
            .iter()
            .min_by_key(|(_, r)| r.last_access)
            .map(|(id, _)| *id);

        if let Some(region_id) = lru_id {
            if let Some(region) = regions.remove(&region_id) {
                // Sync if dirty
                if region.dirty {
                    region.mmap.flush()?;
                    self.bytes_synced
                        .fetch_add(region.size as u64, Ordering::Relaxed);
                }

                self.region_evictions.fetch_add(1, Ordering::Relaxed);
                debug!(
                    "Evicted mmap region {} (offset: {}, size: {})",
                    region_id, region.offset, region.size
                );
            }
        }

        Ok(())
    }

    fn prefault_region(&self, mmap: &MmapMut) {
        let start = Instant::now();
        let page_size = if self.config.enable_huge_pages {
            self.config.huge_page_size
        } else {
            4096
        };

        // Touch pages to pre-fault them into memory
        for i in (0..mmap.len()).step_by(page_size) {
            // SAFETY: Pre-faulting pages by touching memory
            // Invariants:
            // 1. ptr points within valid mmap bounds (i < mmap.len())
            // 2. mmap is exclusively owned by this thread
            // 3. Page-aligned access for optimal faulting
            // 4. Volatile operations prevent compiler optimization
            // Guarantees:
            // - Forces OS to allocate physical pages
            // - Read-modify-write preserves existing data
            // - No data races as mmap is exclusively owned
            unsafe {
                let ptr = mmap.as_ptr().add(i) as *mut u8;
                // Read and write to ensure page is fully faulted
                let val = std::ptr::read_volatile(ptr);
                std::ptr::write_volatile(ptr, val);
            }
        }

        let duration = start.elapsed();
        debug!(
            "Prefaulted {} bytes with {}-byte pages in {:?}",
            mmap.len(),
            page_size,
            duration
        );

        self.page_faults
            .fetch_add(mmap.len() as u64 / page_size as u64, Ordering::Relaxed);
    }

    #[cfg(target_os = "linux")]
    fn advise_huge_pages(&self, mmap: &mut MmapMut) {
        use std::os::raw::c_void;

        // Use madvise to suggest huge pages
        const MADV_HUGEPAGE: i32 = 14;
        const MADV_SEQUENTIAL: i32 = 2;
        const MADV_WILLNEED: i32 = 3;

        // SAFETY: Providing memory usage hints to kernel via madvise
        // Invariants:
        // 1. mmap points to valid memory-mapped region
        // 2. Length covers entire mapped region
        // 3. MADV_* constants are valid for target platform
        // 4. Calls are advisory only - failures are non-critical
        // Guarantees:
        // - Kernel may optimize memory management based on hints
        // - No memory corruption as these are hints only
        unsafe {
            // Advise kernel to use huge pages
            libc::madvise(mmap.as_mut_ptr() as *mut c_void, mmap.len(), MADV_HUGEPAGE);

            // Advise sequential access pattern
            libc::madvise(
                mmap.as_mut_ptr() as *mut c_void,
                mmap.len(),
                MADV_SEQUENTIAL,
            );

            // Advise that we'll need this memory soon
            if self.config.populate_on_map {
                libc::madvise(mmap.as_mut_ptr() as *mut c_void, mmap.len(), MADV_WILLNEED);
            }
        }

        debug!(
            "Applied huge page and access pattern hints to {}-byte region",
            mmap.len()
        );
    }

    #[cfg(not(target_os = "linux"))]
    fn advise_huge_pages(&self, _mmap: &mut MmapMut) {
        // No-op on non-Linux platforms
    }

    /// Read data from the given offset
    pub fn read(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let mut remaining = buf.len();
        let mut buf_offset = 0;
        let mut file_offset = offset;

        while remaining > 0 {
            let region_id = self.get_or_create_region(file_offset)?;

            let regions = self.regions.read();
            let region = regions.get(&region_id).ok_or(Error::InvalidPageId)?;

            let region_offset = file_offset - region.offset;
            let available = region.size.saturating_sub(region_offset as usize);
            let to_read = remaining.min(available);

            if to_read == 0 {
                return Err(Error::InvalidPageId);
            }

            buf[buf_offset..buf_offset + to_read].copy_from_slice(
                &region.mmap[region_offset as usize..region_offset as usize + to_read],
            );

            remaining -= to_read;
            buf_offset += to_read;
            file_offset += to_read as u64;
        }

        Ok(())
    }

    /// Write data to the given offset
    pub fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mut remaining = data.len();
        let mut data_offset = 0;
        let mut file_offset = offset;

        while remaining > 0 {
            let region_id = self.get_or_create_region(file_offset)?;

            let mut regions = self.regions.write();
            let region = regions.get_mut(&region_id).ok_or(Error::InvalidPageId)?;

            let region_offset = file_offset - region.offset;
            let available = region.size.saturating_sub(region_offset as usize);
            let to_write = remaining.min(available);

            if to_write == 0 {
                return Err(Error::InvalidPageId);
            }

            region.mmap[region_offset as usize..region_offset as usize + to_write]
                .copy_from_slice(&data[data_offset..data_offset + to_write]);

            region.dirty = true;

            remaining -= to_write;
            data_offset += to_write;
            file_offset += to_write as u64;
        }

        Ok(())
    }

    /// Sync all dirty regions to disk
    pub fn sync(&self) -> Result<()> {
        let mut regions = self.regions.write();
        let mut total_synced = 0u64;

        for region in regions.values_mut() {
            if region.dirty {
                if self.config.enable_async_msync {
                    region.mmap.flush_async()?;
                } else {
                    region.mmap.flush()?;
                }
                region.dirty = false; // Mark as clean after sync
                total_synced += region.size as u64;
            }
        }

        self.bytes_synced.fetch_add(total_synced, Ordering::Relaxed);

        // Sync file metadata
        let file = self.file.lock();
        file.sync_all()?;

        Ok(())
    }

    /// Grow the file and remap regions if needed
    pub fn grow(&self, new_size: u64) -> Result<()> {
        let current_size = self.file_size.load(Ordering::Acquire);
        if new_size <= current_size {
            return Ok(());
        }

        let file = self.file.lock();
        file.set_len(new_size)?;
        self.file_size.store(new_size, Ordering::Release);

        debug!("Grew mmap file from {} to {} bytes", current_size, new_size);
        Ok(())
    }

    fn start_background_sync(&self) {
        let regions = Arc::clone(&self.regions);
        let shutdown = Arc::clone(&self.shutdown);
        let flush_interval = self.config.flush_interval;
        let bytes_synced = Arc::clone(&self.bytes_synced);
        let enable_async_msync = self.config.enable_async_msync;

        let handle = thread::spawn(move || {
            debug!("Started background mmap sync thread");

            while shutdown.load(Ordering::Relaxed) == 0 {
                thread::sleep(flush_interval);

                // Collect dirty regions to flush without holding the lock
                let dirty_regions: Vec<(u64, usize)> = {
                    let regions_guard = regions.read();
                    regions_guard
                        .iter()
                        .filter(|(_, r)| r.dirty)
                        .map(|(id, r)| (*id, r.size))
                        .collect()
                };

                let mut synced = 0u64;

                // Flush each dirty region individually, taking write lock only when needed
                for (region_id, size) in dirty_regions {
                    // Take write lock only for this specific region
                    let flush_result = {
                        let regions_guard = regions.read();
                        if let Some(region) = regions_guard.get(&region_id) {
                            if enable_async_msync {
                                region.mmap.flush_async()
                            } else {
                                region.mmap.flush()
                            }
                        } else {
                            continue;
                        }
                    };

                    // Update dirty flag if flush succeeded
                    if let Ok(()) = flush_result {
                        let mut regions_guard = regions.write();
                        if let Some(region) = regions_guard.get_mut(&region_id) {
                            region.dirty = false;
                            synced += size as u64;
                        }
                    } else if let Err(e) = flush_result {
                        warn!("Failed to sync mmap region {}: {}", region_id, e);
                    }
                }

                if synced > 0 {
                    bytes_synced.fetch_add(synced, Ordering::Relaxed);
                    debug!("Background sync flushed {} bytes", synced);
                }
            }

            debug!("Background mmap sync thread stopped");
        });

        *self.sync_thread.lock() = Some(handle);
    }

    pub fn get_statistics(&self) -> MmapStatistics {
        let regions = self.regions.read();

        MmapStatistics {
            total_regions: regions.len(),
            total_mapped_bytes: regions.values().map(|r| r.size as u64).sum(),
            page_faults: self.page_faults.load(Ordering::Relaxed),
            region_creates: self.region_creates.load(Ordering::Relaxed),
            region_evictions: self.region_evictions.load(Ordering::Relaxed),
            bytes_synced: self.bytes_synced.load(Ordering::Relaxed),
            file_size: self.file_size.load(Ordering::Relaxed),
        }
    }
}

impl Drop for OptimizedMmapManager {
    fn drop(&mut self) {
        self.shutdown.store(1, Ordering::Relaxed);

        if let Some(handle) = self.sync_thread.lock().take() {
            let _ = handle.join();
        }

        // Final sync
        let _ = self.sync();
    }
}

// MmapRegion does not implement Clone since MmapMut cannot be safely cloned

#[derive(Debug, Clone)]
pub struct MmapStatistics {
    pub total_regions: usize,
    pub total_mapped_bytes: u64,
    pub page_faults: u64,
    pub region_creates: u64,
    pub region_evictions: u64,
    pub bytes_synced: u64,
    pub file_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_mmap_manager_basic() {
        let dir = tempdir().expect("Failed to create temp directory for mmap test");
        let path = dir.path().join("test.mmap");

        let config = MmapConfig {
            region_size: 1024,
            max_mapped_regions: 2,
            ..Default::default()
        };

        let manager = OptimizedMmapManager::create(&path, 4096, config)
            .expect("Failed to create mmap manager");

        // Test write
        let data = b"Hello, mmap!";
        manager
            .write(0, data)
            .expect("Failed to write data to mmap");

        // Test read
        let mut buf = vec![0u8; data.len()];
        manager
            .read(0, &mut buf)
            .expect("Failed to read data from mmap");
        assert_eq!(&buf, data);

        // Test statistics
        let stats = manager.get_statistics();
        assert!(stats.total_regions > 0);
        assert!(stats.region_creates > 0);
    }

    #[test]
    fn test_region_eviction() {
        let dir = tempdir().expect("Failed to create temp directory for eviction test");
        let path = dir.path().join("test_evict.mmap");

        let config = MmapConfig {
            region_size: 1024,
            max_mapped_regions: 2,
            ..Default::default()
        };

        let manager = OptimizedMmapManager::create(&path, 4096, config)
            .expect("Failed to create mmap manager for eviction test");

        // Create 3 regions to trigger eviction
        manager
            .write(0, b"region1")
            .expect("Failed to write region1");
        manager
            .write(1024, b"region2")
            .expect("Failed to write region2");
        manager
            .write(2048, b"region3")
            .expect("Failed to write region3");

        let stats = manager.get_statistics();
        assert_eq!(stats.total_regions, 2);
        assert!(stats.region_evictions > 0);
    }
}
