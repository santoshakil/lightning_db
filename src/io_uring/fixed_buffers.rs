//! Fixed Buffer Management for io_uring
//!
//! This module manages pre-registered buffers that can be used with io_uring's
//! IORING_OP_READ_FIXED and IORING_OP_WRITE_FIXED operations for true zero-copy I/O.

use super::*;
use super::zero_copy_buffer::{AlignedBuffer, BufferAlignment};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::collections::{HashMap, BTreeMap};
use std::time::Instant;
use std::io::Result;

/// Fixed buffer slot for io_uring
pub struct FixedBufferSlot {
    pub index: u32,
    pub buffer: AlignedBuffer,
    pub size: usize,
    pub in_use: AtomicBool,
    pub last_used: AtomicU64, // Timestamp in microseconds
    pub use_count: AtomicU64,
}

impl FixedBufferSlot {
    pub fn new(index: u32, buffer: AlignedBuffer) -> Self {
        let size = buffer.len();
        Self {
            index,
            buffer,
            size,
            in_use: AtomicBool::new(false),
            last_used: AtomicU64::new(0),
            use_count: AtomicU64::new(0),
        }
    }
    
    /// Try to acquire this slot
    pub fn try_acquire(&self) -> bool {
        self.in_use.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }
    
    /// Release this slot
    pub fn release(&self) {
        self.in_use.store(false, Ordering::Release);
        self.last_used.store(
            Instant::now().elapsed().as_micros() as u64,
            Ordering::Relaxed
        );
    }
    
    /// Record usage
    pub fn record_use(&self) {
        self.use_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Fixed buffer region within a larger buffer
#[derive(Debug, Clone)]
pub struct BufferRegion {
    pub slot_index: u32,
    pub offset: usize,
    pub len: usize,
}

/// Fixed buffer manager for io_uring
pub struct FixedBufferManager {
    slots: Vec<Arc<FixedBufferSlot>>,
    size_index: RwLock<BTreeMap<usize, Vec<u32>>>, // Size -> slot indices
    free_slots: Mutex<Vec<u32>>,
    total_memory: AtomicU64,
    stats: FixedBufferStats,
    config: FixedBufferConfig,
}

#[derive(Debug, Clone)]
pub struct FixedBufferConfig {
    pub num_slots: usize,
    pub slot_sizes: Vec<usize>,
    pub max_memory: usize,
    pub alignment: BufferAlignment,
    pub enable_compaction: bool,
    pub compaction_threshold: f64, // Fragmentation threshold
}

impl Default for FixedBufferConfig {
    fn default() -> Self {
        Self {
            num_slots: 1024,
            slot_sizes: vec![
                4 * 1024,       // 4KB
                16 * 1024,      // 16KB
                64 * 1024,      // 64KB
                256 * 1024,     // 256KB
                1024 * 1024,    // 1MB
                4 * 1024 * 1024, // 4MB
            ],
            max_memory: 1024 * 1024 * 1024, // 1GB
            alignment: BufferAlignment::Page,
            enable_compaction: true,
            compaction_threshold: 0.5,
        }
    }
}

#[derive(Debug, Default)]
pub struct FixedBufferStats {
    pub allocations: AtomicU64,
    pub deallocations: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub compactions: AtomicU64,
    pub fragmentation_ratio: AtomicU64, // Stored as percentage * 100
}

impl FixedBufferManager {
    /// Create a new fixed buffer manager
    pub fn new(config: FixedBufferConfig) -> Result<Self> {
        let mut slots = Vec::with_capacity(config.num_slots);
        let mut size_index = BTreeMap::new();
        let mut free_slots = Vec::with_capacity(config.num_slots);
        let mut total_memory = 0;
        
        // Allocate slots with various sizes
        let mut slot_index = 0u32;
        for &size in &config.slot_sizes {
            let count = config.num_slots / config.slot_sizes.len();
            
            for _ in 0..count {
                if total_memory + size > config.max_memory {
                    break;
                }
                
                let buffer = AlignedBuffer::new(size, config.alignment)?;
                let slot = Arc::new(FixedBufferSlot::new(slot_index, buffer));
                
                slots.push(slot);
                size_index.entry(size).or_insert_with(Vec::new).push(slot_index);
                free_slots.push(slot_index);
                
                total_memory += size;
                slot_index += 1;
                
                if slot_index >= config.num_slots as u32 {
                    break;
                }
            }
            
            if slot_index >= config.num_slots as u32 {
                break;
            }
        }
        
        Ok(Self {
            slots,
            size_index: RwLock::new(size_index),
            free_slots: Mutex::new(free_slots),
            total_memory: AtomicU64::new(total_memory as u64),
            stats: FixedBufferStats::default(),
            config,
        })
    }
    
    /// Get buffer pointers for io_uring registration
    pub fn get_buffer_pointers(&self) -> Vec<(*mut u8, usize)> {
        self.slots.iter()
            .map(|slot| (slot.buffer.as_ptr() as *mut u8, slot.size))
            .collect()
    }
    
    /// Allocate a fixed buffer of at least the specified size
    pub fn allocate(&self, size: usize) -> Option<BufferRegion> {
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        
        // Find the smallest buffer that fits
        let size_index = self.size_index.read().unwrap();
        
        for (&slot_size, indices) in size_index.range(size..) {
            for &index in indices {
                if let Some(slot) = self.slots.get(index as usize) {
                    if slot.try_acquire() {
                        slot.record_use();
                        self.stats.hits.fetch_add(1, Ordering::Relaxed);
                        
                        return Some(BufferRegion {
                            slot_index: index,
                            offset: 0,
                            len: size.min(slot_size),
                        });
                    }
                }
            }
        }
        
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }
    
    /// Allocate multiple buffers atomically
    pub fn allocate_batch(&self, sizes: &[usize]) -> Option<Vec<BufferRegion>> {
        let mut allocated = Vec::with_capacity(sizes.len());
        
        // Try to allocate all buffers
        for &size in sizes {
            match self.allocate(size) {
                Some(region) => allocated.push(region),
                None => {
                    // Rollback on failure
                    for region in allocated {
                        self.free(region);
                    }
                    return None;
                }
            }
        }
        
        Some(allocated)
    }
    
    /// Free a buffer region
    pub fn free(&self, region: BufferRegion) {
        if let Some(slot) = self.slots.get(region.slot_index as usize) {
            slot.release();
            self.stats.deallocations.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    /// Get a buffer slot by index
    pub fn get_slot(&self, index: u32) -> Option<&Arc<FixedBufferSlot>> {
        self.slots.get(index as usize)
    }
    
    /// Calculate fragmentation ratio
    pub fn calculate_fragmentation(&self) -> f64 {
        let mut total_slots = 0;
        let mut free_slots = 0;
        
        for slot in &self.slots {
            total_slots += 1;
            if !slot.in_use.load(Ordering::Relaxed) {
                free_slots += 1;
            }
        }
        
        if total_slots == 0 {
            return 0.0;
        }
        
        // Calculate how scattered the free slots are
        let mut last_was_free = false;
        let mut fragments = 0;
        
        for slot in &self.slots {
            let is_free = !slot.in_use.load(Ordering::Relaxed);
            if is_free && !last_was_free {
                fragments += 1;
            }
            last_was_free = is_free;
        }
        
        let fragmentation = if free_slots > 0 {
            fragments as f64 / free_slots as f64
        } else {
            0.0
        };
        
        self.stats.fragmentation_ratio.store(
            (fragmentation * 10000.0) as u64,
            Ordering::Relaxed
        );
        
        fragmentation
    }
    
    /// Compact buffers to reduce fragmentation
    pub fn compact(&mut self) -> Result<()> {
        if !self.config.enable_compaction {
            return Ok(());
        }
        
        let fragmentation = self.calculate_fragmentation();
        if fragmentation < self.config.compaction_threshold {
            return Ok(()); // No need to compact
        }
        
        self.stats.compactions.fetch_add(1, Ordering::Relaxed);
        
        // TODO: Implement actual compaction
        // This would involve:
        // 1. Identifying contiguous free regions
        // 2. Moving in-use buffers to consolidate free space
        // 3. Updating the io_uring registration
        
        Ok(())
    }
    
    /// Get usage statistics
    pub fn stats(&self) -> &FixedBufferStats {
        &self.stats
    }
    
    /// Get total memory usage
    pub fn total_memory(&self) -> u64 {
        self.total_memory.load(Ordering::Relaxed)
    }
    
    /// Get slot utilization
    pub fn utilization(&self) -> f64 {
        let mut in_use = 0;
        let total = self.slots.len();
        
        for slot in &self.slots {
            if slot.in_use.load(Ordering::Relaxed) {
                in_use += 1;
            }
        }
        
        if total > 0 {
            in_use as f64 / total as f64
        } else {
            0.0
        }
    }
}

/// Guard for automatic buffer release
pub struct FixedBufferGuard {
    manager: Arc<FixedBufferManager>,
    region: BufferRegion,
}

impl FixedBufferGuard {
    pub fn new(manager: Arc<FixedBufferManager>, region: BufferRegion) -> Self {
        Self { manager, region }
    }
    
    pub fn region(&self) -> &BufferRegion {
        &self.region
    }
    
    pub fn slot(&self) -> Option<&Arc<FixedBufferSlot>> {
        self.manager.get_slot(self.region.slot_index)
    }
}

impl Drop for FixedBufferGuard {
    fn drop(&mut self) {
        self.manager.free(self.region.clone());
    }
}

/// Fixed buffer pool with automatic management
pub struct FixedBufferPool {
    manager: Arc<FixedBufferManager>,
    local_cache: Mutex<HashMap<usize, Vec<BufferRegion>>>,
    cache_size: usize,
}

impl FixedBufferPool {
    pub fn new(manager: Arc<FixedBufferManager>, cache_size: usize) -> Self {
        Self {
            manager,
            local_cache: Mutex::new(HashMap::new()),
            cache_size,
        }
    }
    
    /// Get a buffer from the pool
    pub fn get(&self, size: usize) -> Option<FixedBufferGuard> {
        // Check local cache first
        {
            let mut cache = self.local_cache.lock().unwrap();
            if let Some(regions) = cache.get_mut(&size) {
                if let Some(region) = regions.pop() {
                    return Some(FixedBufferGuard::new(
                        Arc::clone(&self.manager),
                        region,
                    ));
                }
            }
        }
        
        // Allocate from manager
        self.manager.allocate(size)
            .map(|region| FixedBufferGuard::new(Arc::clone(&self.manager), region))
    }
    
    /// Return a buffer to the pool
    pub fn put(&self, guard: FixedBufferGuard) {
        let size = guard.region.len;
        let region = guard.region.clone();
        
        // Drop guard to release the buffer
        drop(guard);
        
        // Add to local cache if not full
        let mut cache = self.local_cache.lock().unwrap();
        let regions = cache.entry(size).or_insert_with(Vec::new);
        
        if regions.len() < self.cache_size {
            regions.push(region);
        } else {
            // Cache full, let it be freed
            self.manager.free(region);
        }
    }
    
    /// Clear the local cache
    pub fn clear_cache(&self) {
        let mut cache = self.local_cache.lock().unwrap();
        for (_, regions) in cache.drain() {
            for region in regions {
                self.manager.free(region);
            }
        }
    }
}

/// Buffer registration helper for io_uring
pub struct BufferRegistration {
    io: Box<dyn ZeroCopyIo>,
    manager: Arc<FixedBufferManager>,
    registered: AtomicBool,
}

impl BufferRegistration {
    pub fn new(mut io: Box<dyn ZeroCopyIo>, manager: Arc<FixedBufferManager>) -> Result<Self> {
        let pointers = manager.get_buffer_pointers();
        let buffers: Vec<&[u8]> = pointers.iter()
            .map(|&(ptr, len)| unsafe { std::slice::from_raw_parts(ptr as *const u8, len) })
            .collect();
        
        io.register_buffers(&buffers)?;
        
        Ok(Self {
            io,
            manager,
            registered: AtomicBool::new(true),
        })
    }
    
    /// Get the buffer manager
    pub fn manager(&self) -> &Arc<FixedBufferManager> {
        &self.manager
    }
    
    /// Get the I/O interface
    pub fn io(&mut self) -> &mut dyn ZeroCopyIo {
        &mut *self.io
    }
    
    /// Unregister buffers
    pub fn unregister(&mut self) -> Result<()> {
        if self.registered.compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            self.io.unregister_buffers()?;
        }
        Ok(())
    }
}

impl Drop for BufferRegistration {
    fn drop(&mut self) {
        let _ = self.unregister();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_fixed_buffer_slot() {
        let buffer = AlignedBuffer::new(4096, BufferAlignment::Page).unwrap();
        let slot = FixedBufferSlot::new(0, buffer);
        
        assert!(slot.try_acquire());
        assert!(!slot.try_acquire()); // Already acquired
        
        slot.release();
        assert!(slot.try_acquire()); // Can acquire again
    }
    
    #[test]
    fn test_fixed_buffer_manager() {
        let config = FixedBufferConfig {
            num_slots: 10,
            slot_sizes: vec![1024, 2048, 4096],
            max_memory: 100 * 1024,
            ..Default::default()
        };
        
        let manager = FixedBufferManager::new(config).unwrap();
        
        // Test allocation
        let region1 = manager.allocate(1024).unwrap();
        assert_eq!(region1.len, 1024);
        
        let region2 = manager.allocate(3000).unwrap();
        assert_eq!(region2.len, 3000);
        
        // Test free
        manager.free(region1);
        
        // Should be able to allocate again
        let region3 = manager.allocate(1024).unwrap();
        assert_eq!(region3.len, 1024);
        
        manager.free(region2);
        manager.free(region3);
    }
    
    #[test]
    fn test_batch_allocation() {
        let manager = FixedBufferManager::new(FixedBufferConfig::default()).unwrap();
        
        let sizes = vec![4096, 8192, 16384];
        let regions = manager.allocate_batch(&sizes).unwrap();
        
        assert_eq!(regions.len(), 3);
        assert_eq!(regions[0].len, 4096);
        assert_eq!(regions[1].len, 8192);
        assert_eq!(regions[2].len, 16384);
        
        for region in regions {
            manager.free(region);
        }
    }
    
    #[test]
    fn test_fixed_buffer_pool() {
        let manager = Arc::new(FixedBufferManager::new(FixedBufferConfig::default()).unwrap());
        let pool = FixedBufferPool::new(manager, 5);
        
        // Get buffer
        let guard1 = pool.get(4096).unwrap();
        assert_eq!(guard1.region().len, 4096);
        
        // Return to pool
        pool.put(guard1);
        
        // Should get cached buffer
        let guard2 = pool.get(4096).unwrap();
        assert_eq!(guard2.region().len, 4096);
        
        pool.clear_cache();
    }
    
    #[test]
    fn test_utilization_tracking() {
        let config = FixedBufferConfig {
            num_slots: 10,
            slot_sizes: vec![1024],
            ..Default::default()
        };
        
        let manager = FixedBufferManager::new(config).unwrap();
        
        assert_eq!(manager.utilization(), 0.0);
        
        let regions: Vec<_> = (0..5).map(|_| manager.allocate(1024).unwrap()).collect();
        assert_eq!(manager.utilization(), 0.5);
        
        for region in regions {
            manager.free(region);
        }
        
        assert_eq!(manager.utilization(), 0.0);
    }
}