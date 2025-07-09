use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;

/// Cache-friendly memory layout optimizations for high-performance database operations
pub struct MemoryLayoutOps;

impl MemoryLayoutOps {
    /// Optimal cache line size for most modern CPUs
    pub const CACHE_LINE_SIZE: usize = 64;
    
    /// Optimal page size for memory-mapped operations
    pub const OPTIMAL_PAGE_SIZE: usize = 4096;
    
    /// SIMD alignment requirement
    pub const SIMD_ALIGNMENT: usize = 32;
    
    /// Calculate optimal padding to align to cache line boundaries
    pub fn cache_line_padding(size: usize) -> usize {
        let remainder = size % Self::CACHE_LINE_SIZE;
        if remainder == 0 {
            0
        } else {
            Self::CACHE_LINE_SIZE - remainder
        }
    }
    
    /// Calculate optimal alignment for SIMD operations
    pub fn simd_alignment_padding(size: usize) -> usize {
        let remainder = size % Self::SIMD_ALIGNMENT;
        if remainder == 0 {
            0
        } else {
            Self::SIMD_ALIGNMENT - remainder
        }
    }
}

/// Cache-aligned allocator for critical data structures
pub struct CacheAlignedAllocator;

impl CacheAlignedAllocator {
    /// Allocate memory aligned to cache line boundaries
    pub fn allocate(size: usize) -> Result<NonNull<u8>, std::alloc::AllocError> {
        let layout = Layout::from_size_align(size, MemoryLayoutOps::CACHE_LINE_SIZE)
            .map_err(|_| std::alloc::AllocError)?;
        
        let ptr = unsafe { alloc(layout) };
        NonNull::new(ptr).ok_or(std::alloc::AllocError)
    }
    
    /// Deallocate cache-aligned memory
    pub unsafe fn deallocate(ptr: NonNull<u8>, size: usize) {
        let layout = Layout::from_size_align_unchecked(size, MemoryLayoutOps::CACHE_LINE_SIZE);
        dealloc(ptr.as_ptr(), layout);
    }
}

/// Memory-efficient B-tree node layout optimized for cache performance
#[repr(C, align(64))] // Cache line aligned
pub struct OptimizedBTreeNode {
    /// Node header with metadata
    pub header: NodeHeader,
    
    /// Keys stored in a compact layout
    pub keys: [u8; 3968], // Fits in cache with header
    
    /// Key offsets for variable-length keys
    pub key_offsets: [u16; 256], // Max 256 keys per node
    
    /// Values for leaf nodes or child pointers for internal nodes
    pub values: [u64; 256],
    
    /// Padding to ensure next node starts on cache boundary
    _padding: [u8; 0],
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct NodeHeader {
    /// Node type: 0 = leaf, 1 = internal
    pub node_type: u8,
    
    /// Number of keys in this node
    pub key_count: u16,
    
    /// Flag indicating if node is dirty
    pub is_dirty: bool,
    
    /// Last modification timestamp
    pub last_modified: u64,
    
    /// Node ID for identification
    pub node_id: u32,
    
    /// Parent node ID (0 for root)
    pub parent_id: u32,
    
    /// Reserved for future use
    _reserved: [u8; 44],
}

impl OptimizedBTreeNode {
    /// Create a new optimized B-tree node
    pub fn new(node_type: u8, node_id: u32) -> Self {
        Self {
            header: NodeHeader {
                node_type,
                key_count: 0,
                is_dirty: true,
                last_modified: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                node_id,
                parent_id: 0,
                _reserved: [0; 44],
            },
            keys: [0; 3968],
            key_offsets: [0; 256],
            values: [0; 256],
            _padding: [],
        }
    }
    
    /// Insert a key-value pair using cache-friendly operations
    pub fn insert_key_value(&mut self, key: &[u8], value: u64) -> Result<(), &'static str> {
        if self.header.key_count >= 256 {
            return Err("Node is full");
        }
        
        let key_len = key.len();
        if key_len > 255 {
            return Err("Key too long");
        }
        
        // Find insertion point using binary search
        let insertion_point = self.find_insertion_point(key);
        
        // Calculate key storage offset
        let mut key_offset = 0;
        if self.header.key_count > 0 {
            let last_key_idx = (self.header.key_count - 1) as usize;
            let last_key_offset = self.key_offsets[last_key_idx] as usize;
            let last_key_len = if last_key_idx == 0 {
                last_key_offset
            } else {
                last_key_offset - self.key_offsets[last_key_idx - 1] as usize
            };
            key_offset = last_key_offset + last_key_len;
        }
        
        if key_offset + key_len > self.keys.len() {
            return Err("Not enough space for key");
        }
        
        // Shift existing keys and values if necessary
        if insertion_point < self.header.key_count as usize {
            // Shift offsets
            for i in (insertion_point..self.header.key_count as usize).rev() {
                self.key_offsets[i + 1] = self.key_offsets[i];
                self.values[i + 1] = self.values[i];
            }
        }
        
        // Store the key
        self.keys[key_offset..key_offset + key_len].copy_from_slice(key);
        self.key_offsets[insertion_point] = (key_offset + key_len) as u16;
        self.values[insertion_point] = value;
        
        self.header.key_count += 1;
        self.header.is_dirty = true;
        self.header.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Ok(())
    }
    
    /// Find the insertion point for a key using optimized binary search
    fn find_insertion_point(&self, key: &[u8]) -> usize {
        if self.header.key_count == 0 {
            return 0;
        }
        
        let mut left = 0;
        let mut right = self.header.key_count as usize;
        
        while left < right {
            let mid = left + (right - left) / 2;
            
            match self.compare_key_at_index(mid, key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return mid,
            }
        }
        
        left
    }
    
    /// Compare a key at given index with the provided key
    fn compare_key_at_index(&self, index: usize, key: &[u8]) -> std::cmp::Ordering {
        if index >= self.header.key_count as usize {
            return std::cmp::Ordering::Greater;
        }
        
        let stored_key = self.get_key_at_index(index);
        stored_key.cmp(key)
    }
    
    /// Get key at specified index
    fn get_key_at_index(&self, index: usize) -> &[u8] {
        if index >= self.header.key_count as usize {
            return &[];
        }
        
        let key_end = self.key_offsets[index] as usize;
        let key_start = if index == 0 {
            0
        } else {
            self.key_offsets[index - 1] as usize
        };
        
        &self.keys[key_start..key_end]
    }
    
    /// Get value at specified index
    pub fn get_value_at_index(&self, index: usize) -> Option<u64> {
        if index < self.header.key_count as usize {
            Some(self.values[index])
        } else {
            None
        }
    }
    
    /// Check if node is full
    pub fn is_full(&self) -> bool {
        self.header.key_count >= 256
    }
    
    /// Check if node is leaf
    pub fn is_leaf(&self) -> bool {
        self.header.node_type == 0
    }
    
    /// Get total memory usage of this node
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Pool allocator for frequently allocated objects
pub struct ObjectPool<T> {
    free_objects: Vec<Box<T>>,
    allocator: fn() -> T,
    max_pool_size: usize,
}

impl<T> ObjectPool<T> {
    /// Create a new object pool
    pub fn new(allocator: fn() -> T, max_pool_size: usize) -> Self {
        Self {
            free_objects: Vec::with_capacity(max_pool_size),
            allocator,
            max_pool_size,
        }
    }
    
    /// Get an object from the pool or allocate a new one
    pub fn get(&mut self) -> Box<T> {
        self.free_objects.pop().unwrap_or_else(|| Box::new((self.allocator)()))
    }
    
    /// Return an object to the pool
    pub fn return_object(&mut self, mut obj: Box<T>) {
        if self.free_objects.len() < self.max_pool_size {
            // Reset object to default state if needed
            // This would be customized per object type
            self.free_objects.push(obj);
        }
        // If pool is full, object is dropped
    }
    
    /// Get current pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            free_objects: self.free_objects.len(),
            max_pool_size: self.max_pool_size,
            utilization: 1.0 - (self.free_objects.len() as f64 / self.max_pool_size as f64),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub free_objects: usize,
    pub max_pool_size: usize,
    pub utilization: f64,
}

/// Memory-mapped buffer with optimized layout
pub struct MappedBuffer {
    ptr: NonNull<u8>,
    size: usize,
    layout: Layout,
}

impl MappedBuffer {
    /// Create a new memory-mapped buffer aligned for optimal performance
    pub fn new(size: usize) -> Result<Self, std::alloc::AllocError> {
        // Round size up to page boundary
        let aligned_size = (size + MemoryLayoutOps::OPTIMAL_PAGE_SIZE - 1) 
            & !(MemoryLayoutOps::OPTIMAL_PAGE_SIZE - 1);
        
        let layout = Layout::from_size_align(aligned_size, MemoryLayoutOps::OPTIMAL_PAGE_SIZE)
            .map_err(|_| std::alloc::AllocError)?;
        
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr).ok_or(std::alloc::AllocError)?;
        
        Ok(Self {
            ptr,
            size: aligned_size,
            layout,
        })
    }
    
    /// Get a slice to the buffer
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }
    
    /// Get a mutable slice to the buffer
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }
    
    /// Prefetch data into CPU cache
    #[cfg(target_arch = "x86_64")]
    pub fn prefetch(&self, offset: usize, len: usize) {
        if offset + len <= self.size {
            unsafe {
                let ptr = self.ptr.as_ptr().add(offset);
                let end = ptr.add(len);
                let mut current = ptr;
                
                while current < end {
                    std::arch::x86_64::_mm_prefetch(
                        current as *const i8,
                        std::arch::x86_64::_MM_HINT_T0,
                    );
                    current = current.add(MemoryLayoutOps::CACHE_LINE_SIZE);
                }
            }
        }
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    pub fn prefetch(&self, _offset: usize, _len: usize) {
        // No-op on non-x86_64 platforms
    }
}

impl Drop for MappedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

unsafe impl Send for MappedBuffer {}
unsafe impl Sync for MappedBuffer {}

/// Compact data structure for storing database records with minimal overhead
#[repr(C, packed)]
pub struct CompactRecord {
    /// Record header
    pub header: RecordHeader,
    
    /// Variable-length data follows header
    data: [u8; 0], // Zero-sized array for variable data
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct RecordHeader {
    /// Record size in bytes (including header)
    pub size: u32,
    
    /// Key length
    pub key_len: u16,
    
    /// Value length
    pub value_len: u32,
    
    /// Record flags (compressed, deleted, etc.)
    pub flags: u8,
    
    /// Checksum for integrity verification
    pub checksum: u32,
}

impl CompactRecord {
    /// Calculate the total size needed for a record
    pub fn calculate_size(key_len: usize, value_len: usize) -> usize {
        std::mem::size_of::<RecordHeader>() + key_len + value_len
    }
    
    /// Create a new compact record in the provided buffer
    pub unsafe fn create_in_buffer(
        buffer: &mut [u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<&mut CompactRecord, &'static str> {
        let total_size = Self::calculate_size(key.len(), value.len());
        
        if buffer.len() < total_size {
            return Err("Buffer too small");
        }
        
        let record_ptr = buffer.as_mut_ptr() as *mut CompactRecord;
        let record = &mut *record_ptr;
        
        // Initialize header
        record.header.size = total_size as u32;
        record.header.key_len = key.len() as u16;
        record.header.value_len = value.len() as u32;
        record.header.flags = 0;
        
        // Copy key and value data
        let data_ptr = buffer.as_mut_ptr().add(std::mem::size_of::<RecordHeader>());
        std::ptr::copy_nonoverlapping(key.as_ptr(), data_ptr, key.len());
        std::ptr::copy_nonoverlapping(
            value.as_ptr(),
            data_ptr.add(key.len()),
            value.len(),
        );
        
        // Calculate and store checksum
        let data_slice = std::slice::from_raw_parts(data_ptr, key.len() + value.len());
        record.header.checksum = crate::optimizations::simd::safe::crc32(data_slice);
        
        Ok(record)
    }
    
    /// Get the key from this record
    pub fn key(&self) -> &[u8] {
        unsafe {
            let data_ptr = (self as *const CompactRecord as *const u8)
                .add(std::mem::size_of::<RecordHeader>());
            std::slice::from_raw_parts(data_ptr, self.header.key_len as usize)
        }
    }
    
    /// Get the value from this record
    pub fn value(&self) -> &[u8] {
        unsafe {
            let data_ptr = (self as *const CompactRecord as *const u8)
                .add(std::mem::size_of::<RecordHeader>())
                .add(self.header.key_len as usize);
            std::slice::from_raw_parts(data_ptr, self.header.value_len as usize)
        }
    }
    
    /// Verify record integrity using checksum
    pub fn verify_integrity(&self) -> bool {
        unsafe {
            let data_ptr = (self as *const CompactRecord as *const u8)
                .add(std::mem::size_of::<RecordHeader>());
            let data_len = self.header.key_len as usize + self.header.value_len as usize;
            let data_slice = std::slice::from_raw_parts(data_ptr, data_len);
            
            let calculated_checksum = crate::optimizations::simd::safe::crc32(data_slice);
            calculated_checksum == self.header.checksum
        }
    }
    
    /// Check if record is marked as deleted
    pub fn is_deleted(&self) -> bool {
        self.header.flags & 0x01 != 0
    }
    
    /// Mark record as deleted
    pub fn mark_deleted(&mut self) {
        self.header.flags |= 0x01;
    }
    
    /// Check if record is compressed
    pub fn is_compressed(&self) -> bool {
        self.header.flags & 0x02 != 0
    }
    
    /// Mark record as compressed
    pub fn mark_compressed(&mut self) {
        self.header.flags |= 0x02;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cache_line_alignment() {
        assert_eq!(MemoryLayoutOps::cache_line_padding(64), 0);
        assert_eq!(MemoryLayoutOps::cache_line_padding(65), 63);
        assert_eq!(MemoryLayoutOps::cache_line_padding(128), 0);
    }
    
    #[test]
    fn test_simd_alignment() {
        assert_eq!(MemoryLayoutOps::simd_alignment_padding(32), 0);
        assert_eq!(MemoryLayoutOps::simd_alignment_padding(33), 31);
        assert_eq!(MemoryLayoutOps::simd_alignment_padding(64), 0);
    }
    
    #[test]
    fn test_optimized_btree_node() {
        let mut node = OptimizedBTreeNode::new(0, 1);
        
        assert_eq!(node.header.node_id, 1);
        assert_eq!(node.header.key_count, 0);
        assert!(node.is_leaf());
        
        // Test key insertion
        assert!(node.insert_key_value(b"key1", 100).is_ok());
        assert_eq!(node.header.key_count, 1);
        
        assert!(node.insert_key_value(b"key2", 200).is_ok());
        assert_eq!(node.header.key_count, 2);
        
        // Test key retrieval
        assert_eq!(node.get_key_at_index(0), b"key1");
        assert_eq!(node.get_key_at_index(1), b"key2");
        
        assert_eq!(node.get_value_at_index(0), Some(100));
        assert_eq!(node.get_value_at_index(1), Some(200));
    }
    
    #[test]
    fn test_object_pool() {
        let mut pool = ObjectPool::new(|| String::new(), 10);
        
        let obj1 = pool.get();
        let obj2 = pool.get();
        
        assert_eq!(pool.stats().free_objects, 0);
        
        pool.return_object(obj1);
        assert_eq!(pool.stats().free_objects, 1);
        
        pool.return_object(obj2);
        assert_eq!(pool.stats().free_objects, 2);
        
        let obj3 = pool.get();
        assert_eq!(pool.stats().free_objects, 1);
    }
    
    #[test]
    fn test_mapped_buffer() {
        let mut buffer = MappedBuffer::new(8192).unwrap();
        
        assert!(buffer.size >= 8192);
        assert_eq!(buffer.size % MemoryLayoutOps::OPTIMAL_PAGE_SIZE, 0);
        
        // Test buffer access
        let slice = buffer.as_mut_slice();
        slice[0] = 42;
        slice[100] = 24;
        
        let read_slice = buffer.as_slice();
        assert_eq!(read_slice[0], 42);
        assert_eq!(read_slice[100], 24);
        
        // Test prefetching (should not panic)
        buffer.prefetch(0, 1024);
    }
    
    #[test]
    fn test_compact_record() {
        let key = b"test_key";
        let value = b"test_value_data";
        let record_size = CompactRecord::calculate_size(key.len(), value.len());
        
        let mut buffer = vec![0u8; record_size];
        
        unsafe {
            let record = CompactRecord::create_in_buffer(&mut buffer, key, value).unwrap();
            
            assert_eq!(record.header.size, record_size as u32);
            assert_eq!(record.header.key_len, key.len() as u16);
            assert_eq!(record.header.value_len, value.len() as u32);
            
            assert_eq!(record.key(), key);
            assert_eq!(record.value(), value);
            
            assert!(record.verify_integrity());
            assert!(!record.is_deleted());
            assert!(!record.is_compressed());
        }
    }
    
    #[test]
    fn test_cache_aligned_allocator() {
        let ptr = CacheAlignedAllocator::allocate(1024).unwrap();
        
        // Check alignment
        assert_eq!(ptr.as_ptr() as usize % MemoryLayoutOps::CACHE_LINE_SIZE, 0);
        
        unsafe {
            CacheAlignedAllocator::deallocate(ptr, 1024);
        }
    }
}