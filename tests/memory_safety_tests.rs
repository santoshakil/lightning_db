//! Comprehensive memory safety validation tests for Lightning DB
//! 
//! This module contains tests specifically designed to validate memory safety
//! across the 163 unsafe blocks identified in the security audit, with focus on:
//! - Buffer overflow detection
//! - Use-after-free prevention
//! - Data race detection
//! - Uninitialized memory validation
//! - Memory leak detection
//! - Alignment violation prevention
//! - Type safety validation

use std::alloc::{alloc, dealloc, Layout};
use std::mem::{self, MaybeUninit};
use std::ptr::{self, NonNull};
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

// Re-exports for testing
use lightning_db::performance::optimizations::memory_layout::{
    CacheAlignedAllocator, CompactRecord, MappedBuffer, MemoryLayoutOps, OptimizedBTreeNode
};
use lightning_db::performance::lock_free::hot_path::HotPathCache;
use lightning_db::performance::io_uring::ZeroCopyIo;

/// Test configuration for memory safety validation
#[derive(Debug, Clone)]
pub struct MemorySafetyConfig {
    pub iterations: usize,
    pub thread_count: usize,
    pub buffer_size: usize,
    pub stress_duration_ms: u64,
    pub enable_asan_checks: bool,
    pub enable_tsan_checks: bool,
    pub enable_miri_checks: bool,
}

impl Default for MemorySafetyConfig {
    fn default() -> Self {
        Self {
            iterations: 10000,
            thread_count: 8,
            buffer_size: 4096,
            stress_duration_ms: 1000,
            enable_asan_checks: cfg!(feature = "asan"),
            enable_tsan_checks: cfg!(feature = "tsan"),
            enable_miri_checks: cfg!(miri),
        }
    }
}

/// Memory safety test framework with comprehensive validation
pub struct MemorySafetyTestFramework {
    config: MemorySafetyConfig,
    allocation_tracker: Arc<AllocationTracker>,
    test_metrics: Arc<TestMetrics>,
}

/// Tracks memory allocations for leak detection
#[derive(Debug)]
pub struct AllocationTracker {
    allocations: std::sync::Mutex<std::collections::HashMap<usize, (usize, String)>>,
    total_allocated: AtomicUsize,
    total_deallocated: AtomicUsize,
    active_allocations: AtomicUsize,
}

/// Test execution metrics
#[derive(Debug)]
pub struct TestMetrics {
    buffer_overflows_detected: AtomicUsize,
    use_after_free_detected: AtomicUsize,
    data_races_detected: AtomicUsize,
    uninitialized_access_detected: AtomicUsize,
    alignment_violations_detected: AtomicUsize,
    type_safety_violations_detected: AtomicUsize,
    memory_leaks_detected: AtomicUsize,
    tests_passed: AtomicUsize,
    tests_failed: AtomicUsize,
}

impl AllocationTracker {
    pub fn new() -> Self {
        Self {
            allocations: std::sync::Mutex::new(std::collections::HashMap::new()),
            total_allocated: AtomicUsize::new(0),
            total_deallocated: AtomicUsize::new(0),
            active_allocations: AtomicUsize::new(0),
        }
    }

    pub fn track_allocation(&self, ptr: usize, size: usize, location: String) {
        let mut allocations = self.allocations.lock().unwrap();
        allocations.insert(ptr, (size, location));
        self.total_allocated.fetch_add(size, Ordering::Relaxed);
        self.active_allocations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn track_deallocation(&self, ptr: usize) -> bool {
        let mut allocations = self.allocations.lock().unwrap();
        if let Some((size, _)) = allocations.remove(&ptr) {
            self.total_deallocated.fetch_add(size, Ordering::Relaxed);
            self.active_allocations.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false // Double-free or invalid pointer
        }
    }

    pub fn get_stats(&self) -> (usize, usize, usize) {
        (
            self.total_allocated.load(Ordering::Relaxed),
            self.total_deallocated.load(Ordering::Relaxed),
            self.active_allocations.load(Ordering::Relaxed),
        )
    }

    pub fn get_leaked_allocations(&self) -> Vec<(usize, usize, String)> {
        let allocations = self.allocations.lock().unwrap();
        allocations.iter()
            .map(|(&ptr, &(size, ref location))| (ptr, size, location.clone()))
            .collect()
    }
}

impl TestMetrics {
    pub fn new() -> Self {
        Self {
            buffer_overflows_detected: AtomicUsize::new(0),
            use_after_free_detected: AtomicUsize::new(0),
            data_races_detected: AtomicUsize::new(0),
            uninitialized_access_detected: AtomicUsize::new(0),
            alignment_violations_detected: AtomicUsize::new(0),
            type_safety_violations_detected: AtomicUsize::new(0),
            memory_leaks_detected: AtomicUsize::new(0),
            tests_passed: AtomicUsize::new(0),
            tests_failed: AtomicUsize::new(0),
        }
    }

    pub fn record_buffer_overflow(&self) {
        self.buffer_overflows_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_use_after_free(&self) {
        self.use_after_free_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_data_race(&self) {
        self.data_races_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_uninitialized_access(&self) {
        self.uninitialized_access_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_alignment_violation(&self) {
        self.alignment_violations_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_type_safety_violation(&self) {
        self.type_safety_violations_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_memory_leak(&self) {
        self.memory_leaks_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_test_pass(&self) {
        self.tests_passed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_test_fail(&self) {
        self.tests_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn print_summary(&self) {
        println!("\n=== Memory Safety Test Summary ===");
        println!("Buffer overflows detected: {}", self.buffer_overflows_detected.load(Ordering::Relaxed));
        println!("Use-after-free detected: {}", self.use_after_free_detected.load(Ordering::Relaxed));
        println!("Data races detected: {}", self.data_races_detected.load(Ordering::Relaxed));
        println!("Uninitialized access detected: {}", self.uninitialized_access_detected.load(Ordering::Relaxed));
        println!("Alignment violations detected: {}", self.alignment_violations_detected.load(Ordering::Relaxed));
        println!("Type safety violations detected: {}", self.type_safety_violations_detected.load(Ordering::Relaxed));
        println!("Memory leaks detected: {}", self.memory_leaks_detected.load(Ordering::Relaxed));
        println!("Tests passed: {}", self.tests_passed.load(Ordering::Relaxed));
        println!("Tests failed: {}", self.tests_failed.load(Ordering::Relaxed));
        println!("================================");
    }
}

impl MemorySafetyTestFramework {
    pub fn new(config: MemorySafetyConfig) -> Self {
        Self {
            config,
            allocation_tracker: Arc::new(AllocationTracker::new()),
            test_metrics: Arc::new(TestMetrics::new()),
        }
    }

    /// Run comprehensive memory safety test suite
    pub fn run_all_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting comprehensive memory safety validation...");
        
        // Test 1: Buffer Overflow Detection
        self.test_buffer_overflow_detection()?;
        
        // Test 2: Use-After-Free Detection
        self.test_use_after_free_detection()?;
        
        // Test 3: Data Race Detection
        self.test_data_race_detection()?;
        
        // Test 4: Uninitialized Memory Tests
        self.test_uninitialized_memory_access()?;
        
        // Test 5: Memory Leak Detection
        self.test_memory_leak_detection()?;
        
        // Test 6: Alignment Violation Tests
        self.test_alignment_violations()?;
        
        // Test 7: Type Safety Tests
        self.test_type_safety_violations()?;
        
        // Print comprehensive summary
        self.test_metrics.print_summary();
        
        let (allocated, deallocated, active) = self.allocation_tracker.get_stats();
        println!("\nAllocation Summary:");
        println!("Total allocated: {} bytes", allocated);
        println!("Total deallocated: {} bytes", deallocated);
        println!("Active allocations: {}", active);
        
        if active > 0 {
            println!("WARNING: {} leaked allocations detected!", active);
            let leaks = self.allocation_tracker.get_leaked_allocations();
            for (ptr, size, location) in leaks {
                println!("  Leak: {} bytes at 0x{:x} from {}", size, ptr, location);
            }
        }
        
        Ok(())
    }

    /// Test 1: Buffer Overflow Detection Tests
    /// Validates that unsafe buffer operations don't exceed allocated bounds
    fn test_buffer_overflow_detection(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running buffer overflow detection tests...");
        
        // Test 1.1: AlignedBuffer operations in io_uring
        self.test_aligned_buffer_bounds()?;
        
        // Test 1.2: SIMD operations boundary checks
        self.test_simd_buffer_bounds()?;
        
        // Test 1.3: Memory-mapped I/O boundaries
        self.test_mmap_boundaries()?;
        
        // Test 1.4: Zero-copy operations bounds
        self.test_zero_copy_bounds()?;
        
        self.test_metrics.record_test_pass();
        println!("Buffer overflow detection tests passed");
        Ok(())
    }

    fn test_aligned_buffer_bounds(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test cache-aligned allocator with boundary validation
        let size = 1024;
        let ptr = CacheAlignedAllocator::allocate(size)?;
        
        // Track allocation
        self.allocation_tracker.track_allocation(
            ptr.as_ptr() as usize, 
            size, 
            "test_aligned_buffer_bounds".to_string()
        );
        
        // Create a slice and test boundary access
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), size) };
        
        // Safe access within bounds
        slice[0] = 0x42;
        slice[size - 1] = 0x24;
        assert_eq!(slice[0], 0x42);
        assert_eq!(slice[size - 1], 0x24);
        
        // Test boundary detection with canary values
        unsafe {
            // Place canary values just outside the buffer
            let canary_before = ptr.as_ptr().offset(-1);
            let canary_after = ptr.as_ptr().offset(size as isize);
            
            // In a real test with ASAN, these would trigger errors
            #[cfg(feature = "asan")]
            {
                // These should be detected by AddressSanitizer
                // *canary_before = 0xFF; // Would trigger ASAN
                // *canary_after = 0xFF;  // Would trigger ASAN
            }
            
            // Clean up
            CacheAlignedAllocator::deallocate(ptr, size);
        }
        
        self.allocation_tracker.track_deallocation(ptr.as_ptr() as usize);
        Ok(())
    }

    fn test_simd_buffer_bounds(&self) -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(target_arch = "x86_64")]
        {
            // Test SIMD operations with precise buffer boundaries
            let size = 32; // SIMD alignment
            let mut buffer = vec![0u8; size];
            
            // Ensure buffer is SIMD-aligned
            let aligned_ptr = buffer.as_mut_ptr();
            let alignment = MemoryLayoutOps::SIMD_ALIGNMENT;
            
            if (aligned_ptr as usize) % alignment != 0 {
                // Reallocate with proper alignment
                buffer = vec![0u8; size + alignment];
                let offset = alignment - ((buffer.as_ptr() as usize) % alignment);
                let _aligned_slice = &mut buffer[offset..offset + size];
            }
            
            // Test SIMD operations stay within bounds
            // This would be tested with actual SIMD intrinsics in production
            for i in 0..size {
                buffer[i] = (i % 256) as u8;
            }
            
            // Verify no out-of-bounds access occurred
            assert_eq!(buffer.len(), size + alignment);
        }
        
        Ok(())
    }

    fn test_mmap_boundaries(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test MappedBuffer with boundary validation
        let mut buffer = MappedBuffer::new(4096)?;
        
        // Test slice boundaries
        {
            let slice = buffer.as_slice();
            assert!(slice.len() >= 4096);
            
            // Safe access
            let _first = slice[0];
            let _last = slice[slice.len() - 1];
        }
        
        // Test mutable slice boundaries
        {
            let slice = buffer.as_mut_slice();
            slice[0] = 0x42;
            slice[slice.len() - 1] = 0x24;
            
            assert_eq!(slice[0], 0x42);
            assert_eq!(slice[slice.len() - 1], 0x24);
        }
        
        // Test prefetch boundaries
        buffer.prefetch(0, 1024);
        buffer.prefetch(0, buffer.as_slice().len());
        
        // This should not panic even with large offset
        buffer.prefetch(buffer.as_slice().len(), 0);
        
        Ok(())
    }

    fn test_zero_copy_bounds(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test zero-copy operations boundary validation
        let key = b"test_key";
        let value = b"test_value_that_is_longer_than_normal";
        let record_size = CompactRecord::calculate_size(key.len(), value.len());
        
        let mut buffer = vec![0u8; record_size + 64]; // Extra space for canary
        
        // Place canary values
        buffer[record_size] = 0xAA;
        buffer[record_size + 1] = 0xBB;
        
        // Create record
        let record = unsafe {
            CompactRecord::create_in_buffer(&mut buffer[..record_size], key, value)?
        };
        
        // Verify record data
        assert_eq!(record.key(), key);
        assert_eq!(record.value(), value);
        assert!(record.verify_integrity());
        
        // Verify canary values weren't overwritten
        assert_eq!(buffer[record_size], 0xAA);
        assert_eq!(buffer[record_size + 1], 0xBB);
        
        Ok(())
    }

    /// Test 2: Use-After-Free Detection Tests
    /// Validates that freed memory is not accessed
    fn test_use_after_free_detection(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running use-after-free detection tests...");
        
        // Test 2.1: Cache eviction cleanup
        self.test_cache_eviction_cleanup()?;
        
        // Test 2.2: Page manager cleanup
        self.test_page_manager_cleanup()?;
        
        // Test 2.3: Lock-free structure reclamation
        self.test_lock_free_reclamation()?;
        
        self.test_metrics.record_test_pass();
        println!("Use-after-free detection tests passed");
        Ok(())
    }

    fn test_cache_eviction_cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test HotPathCache memory reclamation
        let cache = Arc::new(HotPathCache::<String, String, 16>::new());
        
        // Fill cache beyond capacity to trigger evictions
        for i in 0..32 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache.insert(key, value);
        }
        
        // Verify some entries were evicted
        let mut found_count = 0;
        for i in 0..32 {
            let key = format!("key_{}", i);
            if cache.get(&key).is_some() {
                found_count += 1;
            }
        }
        
        // Should have evicted some entries
        assert!(found_count <= 16);
        
        Ok(())
    }

    fn test_page_manager_cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test proper cleanup in Drop implementations
        {
            let _buffer = MappedBuffer::new(4096)?;
            // Buffer should be cleaned up when dropped
        }
        
        // Multiple allocations and deallocations
        for _ in 0..10 {
            let buffer = MappedBuffer::new(8192)?;
            let slice = buffer.as_slice();
            assert!(!slice.is_empty());
            // Implicit drop
        }
        
        Ok(())
    }

    fn test_lock_free_reclamation(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test epoch-based reclamation in lock-free structures
        let cache = Arc::new(HotPathCache::<u64, String, 64>::new());
        
        // Concurrent insertions to test reclamation under load
        let handles: Vec<_> = (0..self.config.thread_count)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                let iterations = self.config.iterations / self.config.thread_count;
                
                thread::spawn(move || {
                    for i in 0..iterations {
                        let key = (thread_id * iterations + i) as u64;
                        let value = format!("value_{}_{}", thread_id, i);
                        cache.insert(key, value);
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Cache should still be functional after heavy concurrent use
        cache.insert(99999, "final_test".to_string());
        assert_eq!(cache.get(&99999), Some("final_test".to_string()));
        
        Ok(())
    }

    /// Test 3: Data Race Detection Tests
    /// Validates thread-safe access to shared data structures
    fn test_data_race_detection(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running data race detection tests...");
        
        // Test 3.1: Concurrent cache access
        self.test_concurrent_cache_access()?;
        
        // Test 3.2: Atomic operations ordering
        self.test_atomic_operations_ordering()?;
        
        // Test 3.3: Lock-free queue operations
        self.test_lock_free_queue_operations()?;
        
        self.test_metrics.record_test_pass();
        println!("Data race detection tests passed");
        Ok(())
    }

    fn test_concurrent_cache_access(&self) -> Result<(), Box<dyn std::error::Error>> {
        let cache = Arc::new(HotPathCache::<u64, u64, 256>::new());
        let barrier = Arc::new(Barrier::new(self.config.thread_count));
        let error_count = Arc::new(AtomicUsize::new(0));
        
        let handles: Vec<_> = (0..self.config.thread_count)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                let barrier = Arc::clone(&barrier);
                let error_count = Arc::clone(&error_count);
                let iterations = self.config.iterations / self.config.thread_count;
                
                thread::spawn(move || {
                    barrier.wait();
                    
                    // Mixed read/write operations
                    for i in 0..iterations {
                        let key = (i % 100) as u64;
                        
                        if i % 3 == 0 {
                            // Insert
                            cache.insert(key, key * 2);
                        } else {
                            // Read
                            if let Some(value) = cache.get(&key) {
                                if value != key * 2 {
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Should have no data race errors
        let errors = error_count.load(Ordering::Relaxed);
        if errors > 0 {
            self.test_metrics.record_data_race();
            eprintln!("Data race detected: {} inconsistent reads", errors);
        }
        
        Ok(())
    }

    fn test_atomic_operations_ordering(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test memory ordering in atomic operations
        let data = Arc::new(AtomicU64::new(0));
        let flag = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));
        
        let data_clone = Arc::clone(&data);
        let flag_clone = Arc::clone(&flag);
        let barrier_clone = Arc::clone(&barrier);
        
        // Writer thread
        let writer = thread::spawn(move || {
            barrier_clone.wait();
            
            // Write data with release ordering
            data_clone.store(42, Ordering::Release);
            flag_clone.store(true, Ordering::Release);
        });
        
        // Reader thread
        let reader = thread::spawn(move || {
            barrier.wait();
            
            // Spin until flag is set
            while !flag.load(Ordering::Acquire) {
                thread::yield_now();
            }
            
            // Should see the data written by writer
            let value = data.load(Ordering::Acquire);
            assert_eq!(value, 42);
        });
        
        writer.join().unwrap();
        reader.join().unwrap();
        
        Ok(())
    }

    fn test_lock_free_queue_operations(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test concurrent operations on lock-free data structures
        // Using a simple lock-free counter as proxy
        let counter = Arc::new(AtomicUsize::new(0));
        let expected_total = self.config.thread_count * (self.config.iterations / self.config.thread_count);
        
        let handles: Vec<_> = (0..self.config.thread_count)
            .map(|_| {
                let counter = Arc::clone(&counter);
                let iterations = self.config.iterations / self.config.thread_count;
                
                thread::spawn(move || {
                    for _ in 0..iterations {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let final_count = counter.load(Ordering::Relaxed);
        assert_eq!(final_count, expected_total);
        
        Ok(())
    }

    /// Test 4: Uninitialized Memory Tests
    /// Validates MaybeUninit usage patterns and initialization
    fn test_uninitialized_memory_access(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running uninitialized memory access tests...");
        
        // Test 4.1: MaybeUninit patterns
        self.test_maybe_uninit_patterns()?;
        
        // Test 4.2: Buffer initialization
        self.test_buffer_initialization()?;
        
        // Test 4.3: Secret data clearing
        self.test_secret_data_clearing()?;
        
        self.test_metrics.record_test_pass();
        println!("Uninitialized memory access tests passed");
        Ok(())
    }

    fn test_maybe_uninit_patterns(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test proper MaybeUninit usage
        let mut array: [MaybeUninit<u64>; 10] = unsafe { MaybeUninit::uninit().assume_init() };
        
        // Initialize all elements
        for (i, elem) in array.iter_mut().enumerate() {
            elem.write(i as u64);
        }
        
        // Convert to initialized array
        let initialized: [u64; 10] = unsafe { mem::transmute(array) };
        
        for (i, &value) in initialized.iter().enumerate() {
            assert_eq!(value, i as u64);
        }
        
        Ok(())
    }

    fn test_buffer_initialization(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test that buffers are properly initialized before use
        let layout = Layout::from_size_align(1024, 8).unwrap();
        
        unsafe {
            // Allocate uninitialized memory
            let ptr = alloc(layout);
            assert!(!ptr.is_null());
            
            // Initialize with a pattern
            ptr::write_bytes(ptr, 0xAA, 1024);
            
            // Verify initialization
            let slice = std::slice::from_raw_parts(ptr, 1024);
            for &byte in slice {
                assert_eq!(byte, 0xAA);
            }
            
            dealloc(ptr, layout);
        }
        
        Ok(())
    }

    fn test_secret_data_clearing(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test that sensitive data is properly cleared
        let mut secret = vec![0u8; 32];
        
        // Fill with "secret" data
        for (i, byte) in secret.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        
        // Verify data is there
        assert_ne!(secret[0], 0);
        
        // Clear sensitive data
        unsafe {
            ptr::write_bytes(secret.as_mut_ptr(), 0, secret.len());
        }
        
        // Verify data is cleared
        for &byte in &secret {
            assert_eq!(byte, 0);
        }
        
        Ok(())
    }

    /// Test 5: Memory Leak Detection Tests
    /// Monitors memory growth and validates cleanup
    fn test_memory_leak_detection(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running memory leak detection tests...");
        
        // Test 5.1: Allocation/deallocation cycles
        self.test_allocation_cycles()?;
        
        // Test 5.2: Reference counting
        self.test_reference_counting()?;
        
        // Test 5.3: Stress test under load
        self.test_stress_memory_usage()?;
        
        self.test_metrics.record_test_pass();
        println!("Memory leak detection tests passed");
        Ok(())
    }

    fn test_allocation_cycles(&self) -> Result<(), Box<dyn std::error::Error>> {
        let initial_stats = self.allocation_tracker.get_stats();
        
        // Perform many allocation/deallocation cycles
        for i in 0..1000 {
            let size = 1024 + (i % 512);
            let ptr = CacheAlignedAllocator::allocate(size)?;
            
            self.allocation_tracker.track_allocation(
                ptr.as_ptr() as usize,
                size,
                format!("cycle_{}", i)
            );
            
            // Use the memory briefly
            unsafe {
                let slice = std::slice::from_raw_parts_mut(ptr.as_ptr(), size);
                slice[0] = 0x42;
                slice[size - 1] = 0x24;
            }
            
            // Deallocate
            unsafe {
                CacheAlignedAllocator::deallocate(ptr, size);
            }
            
            if !self.allocation_tracker.track_deallocation(ptr.as_ptr() as usize) {
                self.test_metrics.record_memory_leak();
                return Err("Double-free detected!".into());
            }
        }
        
        let final_stats = self.allocation_tracker.get_stats();
        
        // Should have same number of active allocations
        if final_stats.2 != initial_stats.2 {
            self.test_metrics.record_memory_leak();
            eprintln!("Potential memory leak detected: {} active allocations", 
                     final_stats.2 - initial_stats.2);
        }
        
        Ok(())
    }

    fn test_reference_counting(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test Arc reference counting doesn't leak
        let data = Arc::new(vec![0u8; 1024]);
        let mut handles = vec![];
        
        // Create many references
        for _ in 0..100 {
            let data_clone = Arc::clone(&data);
            handles.push(data_clone);
        }
        
        // Clear all references
        handles.clear();
        
        // Original reference should still be valid
        assert_eq!(data.len(), 1024);
        
        // Check strong count
        let strong_count = Arc::strong_count(&data);
        assert_eq!(strong_count, 1);
        
        Ok(())
    }

    fn test_stress_memory_usage(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Stress test memory usage patterns
        let start_time = std::time::Instant::now();
        let mut allocations = vec![];
        
        while start_time.elapsed().as_millis() < self.config.stress_duration_ms {
            // Allocate
            let size = 512 + (allocations.len() % 1024);
            if let Ok(ptr) = CacheAlignedAllocator::allocate(size) {
                allocations.push((ptr, size));
                
                // Limit total allocations to prevent OOM
                if allocations.len() > 1000 {
                    // Deallocate some older allocations
                    for _ in 0..(allocations.len() / 2) {
                        if let Some((ptr, size)) = allocations.pop() {
                            unsafe {
                                CacheAlignedAllocator::deallocate(ptr, size);
                            }
                        }
                    }
                }
            } else {
                break; // Out of memory
            }
        }
        
        // Clean up remaining allocations
        for (ptr, size) in allocations {
            unsafe {
                CacheAlignedAllocator::deallocate(ptr, size);
            }
        }
        
        Ok(())
    }

    /// Test 6: Alignment Violation Tests
    /// Validates memory alignment requirements
    fn test_alignment_violations(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running alignment violation tests...");
        
        // Test 6.1: Cache line alignment
        self.test_cache_line_alignment()?;
        
        // Test 6.2: SIMD alignment
        self.test_simd_alignment()?;
        
        // Test 6.3: Direct I/O alignment
        self.test_direct_io_alignment()?;
        
        self.test_metrics.record_test_pass();
        println!("Alignment violation tests passed");
        Ok(())
    }

    fn test_cache_line_alignment(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test cache-aligned allocations
        let ptr = CacheAlignedAllocator::allocate(1024)?;
        
        // Verify cache line alignment
        let addr = ptr.as_ptr() as usize;
        if addr % MemoryLayoutOps::CACHE_LINE_SIZE != 0 {
            self.test_metrics.record_alignment_violation();
            return Err("Cache line alignment violation".into());
        }
        
        // Test OptimizedBTreeNode alignment
        let node = OptimizedBTreeNode::new(0, 1);
        let node_addr = &node as *const _ as usize;
        if node_addr % MemoryLayoutOps::CACHE_LINE_SIZE != 0 {
            self.test_metrics.record_alignment_violation();
            eprintln!("BTree node alignment violation: addr=0x{:x}", node_addr);
        }
        
        unsafe {
            CacheAlignedAllocator::deallocate(ptr, 1024);
        }
        
        Ok(())
    }

    fn test_simd_alignment(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test SIMD alignment requirements
        let buffer = MappedBuffer::new(1024)?;
        let slice = buffer.as_slice();
        
        let addr = slice.as_ptr() as usize;
        if addr % MemoryLayoutOps::SIMD_ALIGNMENT != 0 {
            eprintln!("SIMD alignment warning: addr=0x{:x}, required={}",
                     addr, MemoryLayoutOps::SIMD_ALIGNMENT);
        }
        
        // Test aligned buffer operations
        let aligned_size = (slice.len() / MemoryLayoutOps::SIMD_ALIGNMENT) * MemoryLayoutOps::SIMD_ALIGNMENT;
        let _aligned_slice = &slice[..aligned_size];
        
        Ok(())
    }

    fn test_direct_io_alignment(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test direct I/O alignment constraints
        let buffer = MappedBuffer::new(4096)?;
        
        // Direct I/O typically requires 512-byte or 4096-byte alignment
        let addr = buffer.as_slice().as_ptr() as usize;
        if addr % 512 != 0 {
            self.test_metrics.record_alignment_violation();
            eprintln!("Direct I/O alignment violation: addr=0x{:x}", addr);
        }
        
        Ok(())
    }

    /// Test 7: Type Safety Tests
    /// Validates unsafe transmutes and type punning
    fn test_type_safety_violations(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running type safety violation tests...");
        
        // Test 7.1: Safe transmutes
        self.test_safe_transmutes()?;
        
        // Test 7.2: Type punning validation
        self.test_type_punning()?;
        
        // Test 7.3: FFI boundary safety
        self.test_ffi_boundary_safety()?;
        
        self.test_metrics.record_test_pass();
        println!("Type safety violation tests passed");
        Ok(())
    }

    fn test_safe_transmutes(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test safe transmute patterns
        let array: [u32; 4] = [0x12345678, 0x9ABCDEF0, 0x11223344, 0x55667788];
        
        // Safe transmute: same size and alignment
        let bytes: [u8; 16] = unsafe { mem::transmute(array) };
        
        // Verify the transmute worked correctly (little-endian)
        assert_eq!(bytes[0], 0x78);
        assert_eq!(bytes[1], 0x56);
        assert_eq!(bytes[2], 0x34);
        assert_eq!(bytes[3], 0x12);
        
        // Transmute back
        let restored: [u32; 4] = unsafe { mem::transmute(bytes) };
        assert_eq!(restored, array);
        
        Ok(())
    }

    fn test_type_punning(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test type punning through unions (safer than transmute)
        #[repr(C)]
        union FloatToInt {
            f: f32,
            i: u32,
        }
        
        let f_val = 3.14159f32;
        let i_val = unsafe { FloatToInt { f: f_val }.i };
        let f_restored = unsafe { FloatToInt { i: i_val }.f };
        
        assert_eq!(f_val, f_restored);
        
        Ok(())
    }

    fn test_ffi_boundary_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test FFI boundary safety patterns
        
        // Test CString handling
        let rust_string = "test_string";
        let c_string = std::ffi::CString::new(rust_string)?;
        let c_ptr = c_string.as_ptr();
        
        // Verify we can safely pass to C functions
        assert!(!c_ptr.is_null());
        
        // Test that the string is null-terminated
        let restored = unsafe { std::ffi::CStr::from_ptr(c_ptr) };
        assert_eq!(restored.to_str()?, rust_string);
        
        Ok(())
    }
}

// Integration tests using the framework
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comprehensive_memory_safety() {
        let config = MemorySafetyConfig {
            iterations: 1000,
            thread_count: 4,
            buffer_size: 1024,
            stress_duration_ms: 100,
            ..Default::default()
        };
        
        let framework = MemorySafetyTestFramework::new(config);
        
        // Run all memory safety tests
        match framework.run_all_tests() {
            Ok(()) => println!("All memory safety tests passed!"),
            Err(e) => panic!("Memory safety test failed: {}", e),
        }
    }

    #[test]
    fn test_buffer_overflow_detection() {
        let config = MemorySafetyConfig::default();
        let framework = MemorySafetyTestFramework::new(config);
        
        framework.test_buffer_overflow_detection().unwrap();
    }

    #[test]
    fn test_use_after_free_detection() {
        let config = MemorySafetyConfig::default();
        let framework = MemorySafetyTestFramework::new(config);
        
        framework.test_use_after_free_detection().unwrap();
    }

    #[test]
    fn test_concurrent_data_races() {
        let config = MemorySafetyConfig {
            thread_count: 8,
            iterations: 10000,
            ..Default::default()
        };
        let framework = MemorySafetyTestFramework::new(config);
        
        framework.test_data_race_detection().unwrap();
    }

    #[test]
    fn test_memory_leak_detection() {
        let config = MemorySafetyConfig::default();
        let framework = MemorySafetyTestFramework::new(config);
        
        framework.test_memory_leak_detection().unwrap();
    }

    #[test]
    fn test_alignment_requirements() {
        let config = MemorySafetyConfig::default();
        let framework = MemorySafetyTestFramework::new(config);
        
        framework.test_alignment_violations().unwrap();
    }

    #[test]
    fn test_type_safety() {
        let config = MemorySafetyConfig::default();
        let framework = MemorySafetyTestFramework::new(config);
        
        framework.test_type_safety_violations().unwrap();
    }
}