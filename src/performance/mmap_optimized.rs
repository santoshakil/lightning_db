use crate::core::error::{Error, Result};
use memmap2::{MmapMut, MmapOptions, Advice};
use parking_lot::{RwLock, Mutex};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn, error};

const DEFAULT_MMAP_SIZE: usize = 1 << 30;
const DEFAULT_CHUNK_SIZE: usize = 1 << 20;
const HUGE_PAGE_SIZE: usize = 2 << 20;
const PAGE_SIZE: usize = 4096;
const MAX_PREFETCH_DISTANCE: usize = 16;

#[derive(Debug, Clone)]
pub struct MmapConfig {
    pub initial_size: usize,
    pub max_size: usize,
    pub growth_factor: f64,
    pub use_huge_pages: bool,
    pub use_direct_io: bool,
    pub prefetch_enabled: bool,
    pub prefetch_distance: usize,
    pub numa_node: Option<u32>,
    pub lock_memory: bool,
    pub populate_on_map: bool,
    pub async_prefetch: bool,
}

impl Default for MmapConfig {
    fn default() -> Self {
        Self {
            initial_size: DEFAULT_MMAP_SIZE,
            max_size: usize::MAX,
            growth_factor: 1.5,
            use_huge_pages: false,
            use_direct_io: false,
            prefetch_enabled: true,
            prefetch_distance: 8,
            numa_node: None,
            lock_memory: false,
            populate_on_map: false,
            async_prefetch: true,
        }
    }
}

pub struct OptimizedMmap {
    mmap: Arc<RwLock<MmapMut>>,
    file: Arc<Mutex<File>>,
    path: PathBuf,
    config: MmapConfig,
    current_size: AtomicUsize,
    access_pattern: Arc<AccessPattern>,
    prefetch_manager: Arc<PrefetchManager>,
    statistics: Arc<MmapStatistics>,
}

#[derive(Debug)]
struct AccessPattern {
    recent_accesses: RwLock<Vec<AccessInfo>>,
    pattern_type: AtomicAccessPattern,
    stride: AtomicUsize,
}

#[derive(Debug, Clone)]
struct AccessInfo {
    offset: usize,
    size: usize,
    timestamp: Instant,
}

#[derive(Debug)]
enum AtomicAccessPattern {
    Sequential,
    Random,
    Strided,
    Unknown,
}

impl AtomicAccessPattern {
    fn load(&self) -> AccessPatternType {
        match self {
            Self::Sequential => AccessPatternType::Sequential,
            Self::Random => AccessPatternType::Random,
            Self::Strided => AccessPatternType::Strided,
            Self::Unknown => AccessPatternType::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AccessPatternType {
    Sequential,
    Random,
    Strided,
    Unknown,
}

struct PrefetchManager {
    enabled: AtomicBool,
    distance: AtomicUsize,
    last_prefetch: AtomicU64,
    prefetch_queue: crossbeam::queue::ArrayQueue<PrefetchRequest>,
    worker_handle: Option<std::thread::JoinHandle<()>>,
}

#[derive(Debug)]
struct PrefetchRequest {
    offset: usize,
    size: usize,
    priority: u8,
}

#[derive(Debug, Default)]
struct MmapStatistics {
    total_reads: AtomicU64,
    total_writes: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    page_faults: AtomicU64,
    prefetch_hits: AtomicU64,
    prefetch_misses: AtomicU64,
    resize_count: AtomicU64,
}

impl OptimizedMmap {
    pub fn new<P: AsRef<Path>>(path: P, config: MmapConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        let file_size = file.metadata()?.len() as usize;
        let mmap_size = file_size.max(config.initial_size);
        
        if file_size < mmap_size {
            file.set_len(mmap_size as u64)?;
        }
        
        let mmap = unsafe {
            let mut options = MmapOptions::new();
            
            if config.populate_on_map {
                options.populate();
            }
            
            if cfg!(target_os = "linux") {
                if config.use_huge_pages {
                    Self::enable_huge_pages(&mut options);
                }
                
                if let Some(node) = config.numa_node {
                    Self::bind_to_numa_node(&mut options, node);
                }
            }
            
            options.len(mmap_size).map_mut(&file)?
        };
        
        if config.lock_memory {
            mmap.lock()?;
        }
        
        let access_pattern = Arc::new(AccessPattern {
            recent_accesses: RwLock::new(Vec::with_capacity(100)),
            pattern_type: AtomicAccessPattern::Unknown,
            stride: AtomicUsize::new(0),
        });
        
        let prefetch_manager = Arc::new(PrefetchManager::new(
            config.prefetch_enabled,
            config.prefetch_distance,
            config.async_prefetch,
        ));
        
        Ok(Self {
            mmap: Arc::new(RwLock::new(mmap)),
            file: Arc::new(Mutex::new(file)),
            path,
            config,
            current_size: AtomicUsize::new(mmap_size),
            access_pattern,
            prefetch_manager,
            statistics: Arc::new(MmapStatistics::default()),
        })
    }
    
    #[cfg(target_os = "linux")]
    fn enable_huge_pages(options: &mut MmapOptions) {
        use libc::{MAP_HUGETLB, MAP_HUGE_2MB};
        unsafe {
            let flags = MAP_HUGETLB | MAP_HUGE_2MB;
            options.map_raw(flags);
        }
    }
    
    #[cfg(not(target_os = "linux"))]
    fn enable_huge_pages(_options: &mut MmapOptions) {
        warn!("Huge pages not supported on this platform");
    }
    
    #[cfg(target_os = "linux")]
    fn bind_to_numa_node(options: &mut MmapOptions, node: u32) {
        use libc::{mbind, MPOL_BIND};
        unsafe {
            let nodemask = 1u64 << node;
            mbind(
                std::ptr::null_mut(),
                0,
                MPOL_BIND,
                &nodemask as *const _ as *const libc::c_ulong,
                64,
                0,
            );
        }
    }
    
    #[cfg(not(target_os = "linux"))]
    fn bind_to_numa_node(_options: &mut MmapOptions, _node: u32) {
        warn!("NUMA binding not supported on this platform");
    }
    
    pub fn read(&self, offset: usize, size: usize) -> Result<Vec<u8>> {
        self.record_access(offset, size);
        
        if self.config.prefetch_enabled {
            self.prefetch_ahead(offset, size);
        }
        
        let mmap = self.mmap.read();
        
        if offset + size > mmap.len() {
            return Err(Error::Generic(format!(
                "Read out of bounds: offset {} + size {} > mmap size {}",
                offset, size, mmap.len()
            )));
        }
        
        let data = mmap[offset..offset + size].to_vec();
        
        self.statistics.total_reads.fetch_add(1, Ordering::Relaxed);
        
        Ok(data)
    }
    
    pub fn write(&self, offset: usize, data: &[u8]) -> Result<()> {
        self.record_access(offset, data.len());
        
        let mut mmap = self.mmap.write();
        
        if offset + data.len() > mmap.len() {
            drop(mmap);
            self.resize(offset + data.len())?;
            mmap = self.mmap.write();
        }
        
        mmap[offset..offset + data.len()].copy_from_slice(data);
        
        if self.config.use_direct_io {
            mmap.flush_range(offset, data.len())?;
        }
        
        self.statistics.total_writes.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    pub fn read_atomic(&self, offset: usize) -> Result<u64> {
        if offset + 8 > self.current_size.load(Ordering::Acquire) {
            return Err(Error::Generic("Atomic read out of bounds".to_string()));
        }
        
        let mmap = self.mmap.read();
        let ptr = &mmap[offset] as *const u8 as *const AtomicU64;
        
        unsafe {
            Ok((*ptr).load(Ordering::Acquire))
        }
    }
    
    pub fn write_atomic(&self, offset: usize, value: u64) -> Result<()> {
        if offset + 8 > self.current_size.load(Ordering::Acquire) {
            self.resize(offset + 8)?;
        }
        
        let mmap = self.mmap.read();
        let ptr = &mmap[offset] as *const u8 as *const AtomicU64;
        
        unsafe {
            (*ptr).store(value, Ordering::Release);
        }
        
        Ok(())
    }
    
    pub fn compare_and_swap(&self, offset: usize, expected: u64, new: u64) -> Result<u64> {
        if offset + 8 > self.current_size.load(Ordering::Acquire) {
            return Err(Error::Generic("CAS out of bounds".to_string()));
        }
        
        let mmap = self.mmap.read();
        let ptr = &mmap[offset] as *const u8 as *const AtomicU64;
        
        unsafe {
            Ok((*ptr).compare_exchange(
                expected,
                new,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ).unwrap_or_else(|v| v))
        }
    }
    
    fn record_access(&self, offset: usize, size: usize) {
        let info = AccessInfo {
            offset,
            size,
            timestamp: Instant::now(),
        };
        
        let mut accesses = self.access_pattern.recent_accesses.write();
        accesses.push(info);
        
        if accesses.len() > 100 {
            accesses.drain(0..50);
        }
        
        drop(accesses);
        
        self.detect_access_pattern();
    }
    
    fn detect_access_pattern(&self) {
        let accesses = self.access_pattern.recent_accesses.read();
        
        if accesses.len() < 10 {
            return;
        }
        
        let mut sequential_count = 0;
        let mut stride_sum = 0i64;
        
        for window in accesses.windows(2) {
            let stride = window[1].offset as i64 - window[0].offset as i64;
            
            if stride == window[0].size as i64 {
                sequential_count += 1;
            }
            
            stride_sum += stride;
        }
        
        let avg_stride = stride_sum / (accesses.len() - 1) as i64;
        
        if sequential_count > accesses.len() * 3 / 4 {
            self.access_pattern.pattern_type = AtomicAccessPattern::Sequential;
            self.access_pattern.stride.store(avg_stride.abs() as usize, Ordering::Relaxed);
        } else if avg_stride.abs() < 1000 && avg_stride != 0 {
            self.access_pattern.pattern_type = AtomicAccessPattern::Strided;
            self.access_pattern.stride.store(avg_stride.abs() as usize, Ordering::Relaxed);
        } else {
            self.access_pattern.pattern_type = AtomicAccessPattern::Random;
        }
    }
    
    fn prefetch_ahead(&self, offset: usize, size: usize) {
        let pattern = self.access_pattern.pattern_type.load();
        
        match pattern {
            AccessPatternType::Sequential => {
                let prefetch_offset = offset + size;
                let prefetch_size = size * self.config.prefetch_distance;
                self.issue_prefetch(prefetch_offset, prefetch_size, 1);
            }
            AccessPatternType::Strided => {
                let stride = self.access_pattern.stride.load(Ordering::Relaxed);
                for i in 1..=self.config.prefetch_distance {
                    let prefetch_offset = offset + stride * i;
                    self.issue_prefetch(prefetch_offset, size, 2);
                }
            }
            _ => {}
        }
    }
    
    fn issue_prefetch(&self, offset: usize, size: usize, priority: u8) {
        if !self.config.async_prefetch {
            self.prefetch_sync(offset, size);
            return;
        }
        
        let request = PrefetchRequest {
            offset,
            size,
            priority,
        };
        
        if let Err(_) = self.prefetch_manager.prefetch_queue.push(request) {
            debug!("Prefetch queue full, dropping request");
        }
    }
    
    fn prefetch_sync(&self, offset: usize, size: usize) {
        let mmap = self.mmap.read();
        
        if offset + size <= mmap.len() {
            let _ = mmap.advise_range(Advice::WillNeed, offset, size);
            self.statistics.prefetch_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.statistics.prefetch_misses.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    fn resize(&self, new_size: usize) -> Result<()> {
        let mut current_size = self.current_size.load(Ordering::Acquire);
        
        if new_size <= current_size {
            return Ok(());
        }
        
        let mut target_size = current_size;
        while target_size < new_size {
            target_size = (target_size as f64 * self.config.growth_factor) as usize;
        }
        
        target_size = target_size.min(self.config.max_size);
        
        if self.config.use_huge_pages {
            target_size = (target_size + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE * HUGE_PAGE_SIZE;
        } else {
            target_size = (target_size + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;
        }
        
        {
            let mut file = self.file.lock();
            file.set_len(target_size as u64)?;
            file.sync_all()?;
        }
        
        let new_mmap = unsafe {
            let file = self.file.lock();
            MmapOptions::new()
                .len(target_size)
                .map_mut(&*file)?
        };
        
        if self.config.lock_memory {
            new_mmap.lock()?;
        }
        
        *self.mmap.write() = new_mmap;
        self.current_size.store(target_size, Ordering::Release);
        self.statistics.resize_count.fetch_add(1, Ordering::Relaxed);
        
        info!("Resized mmap from {} to {} bytes", current_size, target_size);
        
        Ok(())
    }
    
    pub fn flush(&self) -> Result<()> {
        self.mmap.read().flush()?;
        Ok(())
    }
    
    pub fn flush_range(&self, offset: usize, size: usize) -> Result<()> {
        self.mmap.read().flush_range(offset, size)?;
        Ok(())
    }
    
    pub fn advise(&self, advice: Advice) -> Result<()> {
        self.mmap.read().advise(advice)?;
        Ok(())
    }
    
    pub fn advise_range(&self, advice: Advice, offset: usize, size: usize) -> Result<()> {
        self.mmap.read().advise_range(advice, offset, size)?;
        Ok(())
    }
    
    pub fn size(&self) -> usize {
        self.current_size.load(Ordering::Acquire)
    }
    
    pub fn get_statistics(&self) -> MmapStats {
        MmapStats {
            total_reads: self.statistics.total_reads.load(Ordering::Relaxed),
            total_writes: self.statistics.total_writes.load(Ordering::Relaxed),
            cache_hits: self.statistics.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.statistics.cache_misses.load(Ordering::Relaxed),
            page_faults: self.statistics.page_faults.load(Ordering::Relaxed),
            prefetch_hits: self.statistics.prefetch_hits.load(Ordering::Relaxed),
            prefetch_misses: self.statistics.prefetch_misses.load(Ordering::Relaxed),
            resize_count: self.statistics.resize_count.load(Ordering::Relaxed),
            current_size: self.current_size.load(Ordering::Relaxed),
        }
    }
}

impl PrefetchManager {
    fn new(enabled: bool, distance: usize, async_mode: bool) -> Self {
        let queue = crossbeam::queue::ArrayQueue::new(1024);
        
        let worker_handle = if async_mode && enabled {
            let queue_clone = queue.clone();
            Some(std::thread::spawn(move || {
                Self::prefetch_worker(queue_clone);
            }))
        } else {
            None
        };
        
        Self {
            enabled: AtomicBool::new(enabled),
            distance: AtomicUsize::new(distance),
            last_prefetch: AtomicU64::new(0),
            prefetch_queue: queue,
            worker_handle,
        }
    }
    
    fn prefetch_worker(queue: crossbeam::queue::ArrayQueue<PrefetchRequest>) {
        loop {
            if let Some(request) = queue.pop() {
                debug!(
                    "Processing prefetch request: offset={}, size={}, priority={}",
                    request.offset, request.size, request.priority
                );
            } else {
                std::thread::sleep(Duration::from_micros(100));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MmapStats {
    pub total_reads: u64,
    pub total_writes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub page_faults: u64,
    pub prefetch_hits: u64,
    pub prefetch_misses: u64,
    pub resize_count: u64,
    pub current_size: usize,
}

pub struct MmapPool {
    mmaps: HashMap<PathBuf, Arc<OptimizedMmap>>,
    config: MmapConfig,
}

impl MmapPool {
    pub fn new(config: MmapConfig) -> Self {
        Self {
            mmaps: HashMap::new(),
            config,
        }
    }
    
    pub fn get_or_create<P: AsRef<Path>>(&mut self, path: P) -> Result<Arc<OptimizedMmap>> {
        let path = path.as_ref().to_path_buf();
        
        if let Some(mmap) = self.mmaps.get(&path) {
            return Ok(mmap.clone());
        }
        
        let mmap = Arc::new(OptimizedMmap::new(&path, self.config.clone())?);
        self.mmaps.insert(path, mmap.clone());
        
        Ok(mmap)
    }
    
    pub fn remove<P: AsRef<Path>>(&mut self, path: P) -> Option<Arc<OptimizedMmap>> {
        self.mmaps.remove(path.as_ref())
    }
    
    pub fn clear(&mut self) {
        self.mmaps.clear();
    }
    
    pub fn flush_all(&self) -> Result<()> {
        for mmap in self.mmaps.values() {
            mmap.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_basic_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.mmap");
        
        let mmap = OptimizedMmap::new(&path, MmapConfig::default()).unwrap();
        
        let data = b"Hello, World!";
        mmap.write(0, data).unwrap();
        
        let read_data = mmap.read(0, data.len()).unwrap();
        assert_eq!(read_data, data);
    }
    
    #[test]
    fn test_atomic_operations() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_atomic.mmap");
        
        let mmap = OptimizedMmap::new(&path, MmapConfig::default()).unwrap();
        
        mmap.write_atomic(0, 42).unwrap();
        let value = mmap.read_atomic(0).unwrap();
        assert_eq!(value, 42);
        
        let old = mmap.compare_and_swap(0, 42, 100).unwrap();
        assert_eq!(old, 42);
        
        let value = mmap.read_atomic(0).unwrap();
        assert_eq!(value, 100);
    }
    
    #[test]
    fn test_resize() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_resize.mmap");
        
        let mut config = MmapConfig::default();
        config.initial_size = 1024;
        
        let mmap = OptimizedMmap::new(&path, config).unwrap();
        
        let initial_size = mmap.size();
        assert!(initial_size >= 1024);
        
        let large_data = vec![0u8; 2048];
        mmap.write(0, &large_data).unwrap();
        
        let new_size = mmap.size();
        assert!(new_size > initial_size);
    }
    
    #[test]
    fn test_access_pattern_detection() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_pattern.mmap");
        
        let mmap = OptimizedMmap::new(&path, MmapConfig::default()).unwrap();
        
        for i in 0..20 {
            let offset = i * 100;
            mmap.read(offset, 100).ok();
        }
        
        let pattern = mmap.access_pattern.pattern_type.load();
        assert_eq!(pattern, AccessPatternType::Sequential);
    }
    
    #[test]
    fn test_mmap_pool() {
        let dir = tempdir().unwrap();
        let mut pool = MmapPool::new(MmapConfig::default());
        
        let path1 = dir.path().join("file1.mmap");
        let path2 = dir.path().join("file2.mmap");
        
        let mmap1 = pool.get_or_create(&path1).unwrap();
        let mmap2 = pool.get_or_create(&path2).unwrap();
        
        assert!(!Arc::ptr_eq(&mmap1, &mmap2));
        
        let mmap1_again = pool.get_or_create(&path1).unwrap();
        assert!(Arc::ptr_eq(&mmap1, &mmap1_again));
        
        pool.flush_all().unwrap();
    }
}