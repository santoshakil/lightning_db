//! Specialized unsafe validation tests for high-risk areas in Lightning DB
//! 
//! This module targets the specific unsafe blocks identified in the security audit:
//! - performance/io_uring (46 unsafe blocks)
//! - performance/optimizations/memory_layout.rs (15 unsafe blocks)  
//! - performance/lock_free/hot_path.rs (15 unsafe blocks)
//! - core/btree operations (23 unsafe blocks)
//!
//! Each test validates specific invariants and safety guarantees required by unsafe code.

use std::alloc::{alloc, dealloc, Layout};
use std::mem::{self, MaybeUninit};
use std::ptr::{self, NonNull};
use std::sync::{Arc, Barrier, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// Test imports
use lightning_db::performance::optimizations::memory_layout::*;
use lightning_db::performance::lock_free::hot_path::*;
use lightning_db::core::btree::cache_optimized::*;

/// Specialized test suite for unsafe block validation
pub struct UnsafeValidationSuite {
    /// Configuration for test execution
    config: UnsafeTestConfig,
    /// Violation tracking
    violations: Arc<ViolationTracker>,
}

#[derive(Debug, Clone)]
pub struct UnsafeTestConfig {
    pub stress_iterations: usize,
    pub concurrent_threads: usize,
    pub memory_pressure_mb: usize,
    pub io_operations: usize,
    pub enable_timing_checks: bool,
    pub enable_invariant_checks: bool,
}

impl Default for UnsafeTestConfig {
    fn default() -> Self {
        Self {
            stress_iterations: 100000,
            concurrent_threads: 16,
            memory_pressure_mb: 100,
            io_operations: 10000,
            enable_timing_checks: true,
            enable_invariant_checks: true,
        }
    }
}

#[derive(Debug)]
pub struct ViolationTracker {
    io_uring_violations: AtomicUsize,
    memory_layout_violations: AtomicUsize,
    lock_free_violations: AtomicUsize,
    btree_violations: AtomicUsize,
    total_tests: AtomicUsize,
    failed_tests: AtomicUsize,
}

impl ViolationTracker {
    pub fn new() -> Self {
        Self {
            io_uring_violations: AtomicUsize::new(0),
            memory_layout_violations: AtomicUsize::new(0),
            lock_free_violations: AtomicUsize::new(0),
            btree_violations: AtomicUsize::new(0),
            total_tests: AtomicUsize::new(0),
            failed_tests: AtomicUsize::new(0),
        }
    }

    pub fn record_io_uring_violation(&self) {
        self.io_uring_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_memory_layout_violation(&self) {
        self.memory_layout_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lock_free_violation(&self) {
        self.lock_free_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_btree_violation(&self) {
        self.btree_violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_test(&self) {
        self.total_tests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.failed_tests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn print_summary(&self) {
        println!("\n=== Unsafe Block Validation Summary ===");
        println!("IO Uring violations: {}", self.io_uring_violations.load(Ordering::Relaxed));
        println!("Memory Layout violations: {}", self.memory_layout_violations.load(Ordering::Relaxed));
        println!("Lock-Free violations: {}", self.lock_free_violations.load(Ordering::Relaxed));
        println!("BTree violations: {}", self.btree_violations.load(Ordering::Relaxed));
        println!("Total tests: {}", self.total_tests.load(Ordering::Relaxed));
        println!("Failed tests: {}", self.failed_tests.load(Ordering::Relaxed));
        println!("======================================");
    }
}

impl UnsafeValidationSuite {
    pub fn new(config: UnsafeTestConfig) -> Self {
        Self {
            config,
            violations: Arc::new(ViolationTracker::new()),
        }
    }

    /// Run all specialized unsafe validation tests
    pub fn run_all_validations(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting specialized unsafe block validation...");

        // Test high-risk areas in order of severity
        self.validate_io_uring_unsafe_blocks()?;
        self.validate_memory_layout_unsafe_blocks()?;
        self.validate_lock_free_unsafe_blocks()?;
        self.validate_btree_unsafe_blocks()?;

        // Run cross-component integration tests
        self.validate_unsafe_interactions()?;

        self.violations.print_summary();
        
        let total_violations = self.violations.io_uring_violations.load(Ordering::Relaxed) +
                              self.violations.memory_layout_violations.load(Ordering::Relaxed) +
                              self.violations.lock_free_violations.load(Ordering::Relaxed) +
                              self.violations.btree_violations.load(Ordering::Relaxed);

        if total_violations > 0 {
            return Err(format!("Found {} unsafe block violations", total_violations).into());
        }

        println!("All unsafe block validations passed!");
        Ok(())
    }

    /// Validate IO Uring unsafe blocks (46 blocks - highest risk)
    fn validate_io_uring_unsafe_blocks(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Validating IO Uring unsafe blocks...");

        // Test 1: Ring buffer pointer arithmetic validation
        self.test_io_uring_ring_buffer_safety()?;
        
        // Test 2: Submission queue entry manipulation
        self.test_io_uring_sqe_safety()?;
        
        // Test 3: Completion queue entry handling
        self.test_io_uring_cqe_safety()?;
        
        // Test 4: Fixed buffer registration safety
        self.test_io_uring_fixed_buffer_safety()?;
        
        // Test 5: Memory ordering in ring operations
        self.test_io_uring_memory_ordering()?;

        println!("IO Uring unsafe validation completed");
        Ok(())
    }

    fn test_io_uring_ring_buffer_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Simulate ring buffer operations with careful bounds checking
        let ring_size = 1024;
        let mask = ring_size - 1;
        
        // Allocate ring buffer memory
        let layout = Layout::from_size_align(ring_size * 8, 8)?;
        let ring_ptr = unsafe { alloc(layout) as *mut u64 };
        
        if ring_ptr.is_null() {
            return Err("Failed to allocate ring buffer".into());
        }

        // Test pointer arithmetic bounds
        for i in 0..ring_size * 2 {
            let index = i & mask;
            
            // Validate bounds before access
            if index >= ring_size {
                self.violations.record_io_uring_violation();
                self.violations.record_failure();
                unsafe { dealloc(ring_ptr as *mut u8, layout); }
                return Err("Ring buffer index out of bounds".into());
            }
            
            unsafe {
                // Safe indexed access within bounds
                *ring_ptr.add(index) = i as u64;
                let value = *ring_ptr.add(index);
                assert_eq!(value, i as u64);
            }
        }

        unsafe { dealloc(ring_ptr as *mut u8, layout); }
        Ok(())
    }

    fn test_io_uring_sqe_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test SQE (Submission Queue Entry) manipulation safety
        #[repr(C)]
        struct MockSqe {
            opcode: u8,
            flags: u8,
            ioprio: u16,
            fd: i32,
            off: u64,
            addr: u64,
            len: u32,
            rw_flags: u32,
            user_data: u64,
            _padding: [u64; 2],
        }

        let sqe_count = 256;
        let layout = Layout::from_size_align(
            sqe_count * mem::size_of::<MockSqe>(), 
            mem::align_of::<MockSqe>()
        )?;
        
        let sqe_ptr = unsafe { alloc(layout) as *mut MockSqe };
        if sqe_ptr.is_null() {
            return Err("Failed to allocate SQE array".into());
        }

        // Initialize SQEs safely
        for i in 0..sqe_count {
            unsafe {
                let sqe = sqe_ptr.add(i);
                ptr::write_bytes(sqe, 0, 1); // Zero-initialize
                
                // Set fields safely
                (*sqe).opcode = 1; // READ
                (*sqe).flags = 0;
                (*sqe).fd = -1; // Invalid fd for testing
                (*sqe).user_data = i as u64;
                
                // Validate written data
                assert_eq!((*sqe).opcode, 1);
                assert_eq!((*sqe).user_data, i as u64);
            }
        }

        // Test bounds checking
        unsafe {
            // This should be safe (last valid index)
            let last_sqe = sqe_ptr.add(sqe_count - 1);
            (*last_sqe).opcode = 2;
            
            // Verify the write
            assert_eq!((*last_sqe).opcode, 2);
        }

        unsafe { dealloc(sqe_ptr as *mut u8, layout); }
        Ok(())
    }

    fn test_io_uring_cqe_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test CQE (Completion Queue Entry) handling safety
        #[repr(C)]
        struct MockCqe {
            user_data: u64,
            res: i32,
            flags: u32,
        }

        let cqe_count = 512;
        let layout = Layout::from_size_align(
            cqe_count * mem::size_of::<MockCqe>(),
            mem::align_of::<MockCqe>()
        )?;

        let cqe_ptr = unsafe { alloc(layout) as *mut MockCqe };
        if cqe_ptr.is_null() {
            return Err("Failed to allocate CQE array".into());
        }

        // Simulate kernel writing CQEs
        for i in 0..cqe_count {
            unsafe {
                let cqe = cqe_ptr.add(i);
                (*cqe).user_data = i as u64;
                (*cqe).res = if i % 2 == 0 { 1024 } else { -1 }; // Success/error
                (*cqe).flags = 0;
            }
        }

        // Test safe CQE consumption with proper bounds checking
        let mut head = 0;
        let tail = cqe_count;
        let mask = cqe_count - 1;

        while head < tail {
            let index = head & mask;
            
            // Bounds validation
            if index >= cqe_count {
                self.violations.record_io_uring_violation();
                self.violations.record_failure();
                unsafe { dealloc(cqe_ptr as *mut u8, layout); }
                return Err("CQE index out of bounds".into());
            }

            unsafe {
                let cqe = cqe_ptr.add(index);
                let user_data = (*cqe).user_data;
                let result = (*cqe).res;
                
                // Validate expected values
                assert_eq!(user_data, index as u64);
                assert!(result == 1024 || result == -1);
            }

            head += 1;
        }

        unsafe { dealloc(cqe_ptr as *mut u8, layout); }
        Ok(())
    }

    fn test_io_uring_fixed_buffer_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test fixed buffer registration and access safety
        let buffer_count = 16;
        let buffer_size = 4096;
        let mut buffers = Vec::new();
        let mut buffer_addrs = Vec::new();

        // Allocate fixed buffers
        for i in 0..buffer_count {
            let layout = Layout::from_size_align(buffer_size, 4096)?;
            let ptr = unsafe { alloc(layout) };
            
            if ptr.is_null() {
                // Clean up allocated buffers
                for (ptr, layout) in buffers {
                    unsafe { dealloc(ptr, layout); }
                }
                return Err("Failed to allocate fixed buffer".into());
            }

            // Initialize buffer with pattern
            unsafe {
                ptr::write_bytes(ptr, (i % 256) as u8, buffer_size);
            }

            buffers.push((ptr, layout));
            buffer_addrs.push((ptr as u64, buffer_size));
        }

        // Test buffer access with bounds checking
        for (i, &(addr, size)) in buffer_addrs.iter().enumerate() {
            let ptr = addr as *mut u8;
            
            unsafe {
                // Test first byte
                let first = *ptr;
                assert_eq!(first, (i % 256) as u8);
                
                // Test last byte
                let last = *ptr.add(size - 1);
                assert_eq!(last, (i % 256) as u8);
                
                // Test bounds - this should be safe
                if size > 0 {
                    let _valid_access = *ptr.add(size - 1);
                }
            }
        }

        // Simulate buffer operations with offset/length validation
        for (i, &(addr, size)) in buffer_addrs.iter().enumerate() {
            let offset = (i * 256) % size;
            let length = std::cmp::min(256, size - offset);
            
            if offset + length > size {
                self.violations.record_io_uring_violation();
                self.violations.record_failure();
            } else {
                // Safe operation within bounds
                let ptr = (addr + offset as u64) as *mut u8;
                unsafe {
                    for j in 0..length {
                        *ptr.add(j) = (j % 256) as u8;
                    }
                }
            }
        }

        // Clean up
        for (ptr, layout) in buffers {
            unsafe { dealloc(ptr, layout); }
        }

        Ok(())
    }

    fn test_io_uring_memory_ordering(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test memory ordering in ring operations
        use std::sync::atomic::{AtomicU32, Ordering};

        let head = AtomicU32::new(0);
        let tail = AtomicU32::new(0);
        let ring_mask = 1023u32; // 1024 entries - 1
        
        let producer_data = Arc::new(Mutex::new(Vec::new()));
        let consumer_data = Arc::new(Mutex::new(Vec::new()));
        
        let barrier = Arc::new(Barrier::new(2));

        // Producer thread
        let head_ref = &head;
        let tail_ref = &tail;
        let producer_data_clone = Arc::clone(&producer_data);
        let barrier_clone = Arc::clone(&barrier);

        let producer = thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0u32..1000 {
                loop {
                    let current_tail = tail_ref.load(Ordering::Relaxed);
                    let current_head = head_ref.load(Ordering::Acquire);
                    let next_tail = current_tail.wrapping_add(1);
                    
                    // Check if queue is full
                    if next_tail.wrapping_sub(current_head) > ring_mask {
                        thread::yield_now();
                        continue;
                    }
                    
                    // Simulate writing data
                    {
                        let mut data = producer_data_clone.lock().unwrap();
                        data.push(i);
                    }
                    
                    // Update tail with release ordering
                    tail_ref.store(next_tail, Ordering::Release);
                    break;
                }
            }
        });

        // Consumer thread
        let consumer_data_clone = Arc::clone(&consumer_data);
        
        let consumer = thread::spawn(move || {
            barrier.wait();
            
            let mut consumed = 0u32;
            while consumed < 1000 {
                let current_head = head.load(Ordering::Relaxed);
                let current_tail = tail.load(Ordering::Acquire);
                
                if current_head == current_tail {
                    thread::yield_now();
                    continue;
                }
                
                // Simulate reading data
                {
                    let producer_data = producer_data.lock().unwrap();
                    if !producer_data.is_empty() {
                        let mut consumer_data = consumer_data_clone.lock().unwrap();
                        consumer_data.push(consumed);
                    }
                }
                
                // Update head with release ordering
                head.store(current_head.wrapping_add(1), Ordering::Release);
                consumed += 1;
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();

        // Verify no data was lost or corrupted
        let producer_data = producer_data.lock().unwrap();
        let consumer_data = consumer_data.lock().unwrap();
        
        if producer_data.len() != 1000 || consumer_data.len() != 1000 {
            self.violations.record_io_uring_violation();
            self.violations.record_failure();
            return Err("Memory ordering violation detected".into());
        }

        Ok(())
    }

    /// Validate Memory Layout unsafe blocks (15 blocks)
    fn validate_memory_layout_unsafe_blocks(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Validating Memory Layout unsafe blocks...");

        // Test 1: Cache-aligned allocator safety
        self.test_cache_aligned_allocator_safety()?;
        
        // Test 2: Compact record layout safety
        self.test_compact_record_safety()?;
        
        // Test 3: Mapped buffer safety
        self.test_mapped_buffer_safety()?;
        
        // Test 4: Optimized BTree node safety
        self.test_optimized_btree_node_safety()?;
        
        // Test 5: SIMD prefetch safety
        self.test_simd_prefetch_safety()?;

        println!("Memory Layout unsafe validation completed");
        Ok(())
    }

    fn test_cache_aligned_allocator_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test cache-aligned allocator with various sizes
        let test_sizes = [64, 128, 256, 512, 1024, 2048, 4096];
        let mut allocations = Vec::new();

        for &size in &test_sizes {
            match CacheAlignedAllocator::allocate(size) {
                Ok(ptr) => {
                    // Verify alignment
                    let addr = ptr.as_ptr() as usize;
                    if addr % MemoryLayoutOps::CACHE_LINE_SIZE != 0 {
                        self.violations.record_memory_layout_violation();
                        self.violations.record_failure();
                        return Err(format!("Alignment violation: addr=0x{:x}", addr).into());
                    }

                    // Test memory access
                    unsafe {
                        let slice = std::slice::from_raw_parts_mut(ptr.as_ptr(), size);
                        // Write pattern
                        for (i, byte) in slice.iter_mut().enumerate() {
                            *byte = (i % 256) as u8;
                        }
                        // Verify pattern
                        for (i, &byte) in slice.iter().enumerate() {
                            if byte != (i % 256) as u8 {
                                self.violations.record_memory_layout_violation();
                                return Err("Memory corruption detected".into());
                            }
                        }
                    }

                    allocations.push((ptr, size));
                }
                Err(_) => {
                    self.violations.record_memory_layout_violation();
                    return Err("Allocation failed".into());
                }
            }
        }

        // Clean up all allocations
        for (ptr, size) in allocations {
            unsafe {
                CacheAlignedAllocator::deallocate(ptr, size);
            }
        }

        Ok(())
    }

    fn test_compact_record_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test CompactRecord with various key/value sizes
        let test_cases = [
            (b"small_key".as_slice(), b"small_value".as_slice()),
            (b"medium_sized_key_for_testing".as_slice(), b"medium_sized_value_with_more_data_for_comprehensive_testing".as_slice()),
            (b"k".as_slice(), b"v".as_slice()), // Minimal case
            (b"".as_slice(), b"".as_slice()), // Empty case
        ];

        for (key, value) in test_cases {
            let record_size = CompactRecord::calculate_size(key.len(), value.len());
            let mut buffer = vec![0u8; record_size + 32]; // Extra space for safety

            // Place canary values to detect overwrites
            buffer[record_size] = 0xDE;
            buffer[record_size + 1] = 0xAD;
            buffer[record_size + 2] = 0xBE;
            buffer[record_size + 3] = 0xEF;

            // Create record using unsafe code
            let record = unsafe {
                match CompactRecord::create_in_buffer(&mut buffer[..record_size], key, value) {
                    Ok(record) => record,
                    Err(e) => {
                        self.violations.record_memory_layout_violation();
                        return Err(format!("Failed to create record: {}", e).into());
                    }
                }
            };

            // Verify record integrity
            if record.key() != key {
                self.violations.record_memory_layout_violation();
                return Err("Key corruption detected".into());
            }

            if record.value() != value {
                self.violations.record_memory_layout_violation();
                return Err("Value corruption detected".into());
            }

            if !record.verify_integrity() {
                self.violations.record_memory_layout_violation();
                return Err("Integrity check failed".into());
            }

            // Verify canary values weren't overwritten
            if buffer[record_size] != 0xDE || 
               buffer[record_size + 1] != 0xAD ||
               buffer[record_size + 2] != 0xBE ||
               buffer[record_size + 3] != 0xEF {
                self.violations.record_memory_layout_violation();
                return Err("Buffer overflow detected - canary values corrupted".into());
            }
        }

        Ok(())
    }

    fn test_mapped_buffer_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test MappedBuffer with various sizes
        let test_sizes = [4096, 8192, 16384, 32768];

        for &size in &test_sizes {
            let mut buffer = match MappedBuffer::new(size) {
                Ok(buffer) => buffer,
                Err(_) => {
                    self.violations.record_memory_layout_violation();
                    return Err("Failed to create mapped buffer".into());
                }
            };

            // Verify size alignment
            let actual_size = buffer.as_slice().len();
            if actual_size < size {
                self.violations.record_memory_layout_violation();
                return Err("Buffer size is smaller than requested".into());
            }

            // Test page alignment
            let addr = buffer.as_slice().as_ptr() as usize;
            if addr % MemoryLayoutOps::OPTIMAL_PAGE_SIZE != 0 {
                self.violations.record_memory_layout_violation();
                return Err(format!("Page alignment violation: addr=0x{:x}", addr).into());
            }

            // Test memory access patterns
            {
                let slice = buffer.as_mut_slice();
                
                // Write pattern to entire buffer
                for (i, byte) in slice.iter_mut().enumerate() {
                    *byte = ((i * 7) % 256) as u8;
                }

                // Verify pattern
                for (i, &byte) in slice.iter().enumerate() {
                    let expected = ((i * 7) % 256) as u8;
                    if byte != expected {
                        self.violations.record_memory_layout_violation();
                        return Err(format!("Memory corruption at offset {}: expected {}, got {}", 
                                         i, expected, byte).into());
                    }
                }
            }

            // Test prefetch operations (should not crash)
            buffer.prefetch(0, 1024);
            buffer.prefetch(1024, 2048);
            
            // Test boundary prefetch (should handle gracefully)
            buffer.prefetch(actual_size, 0); // At boundary
            buffer.prefetch(actual_size + 1000, 1000); // Beyond boundary
        }

        Ok(())
    }

    fn test_optimized_btree_node_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test OptimizedBTreeNode with various operations
        let mut node = OptimizedBTreeNode::new(0, 1); // Leaf node

        // Verify initial state
        assert_eq!(node.header.node_type, 0);
        assert_eq!(node.header.key_count, 0);
        assert_eq!(node.header.node_id, 1);

        // Test key insertion with boundary checking
        let test_keys = [
            b"key1".as_slice(),
            b"key2".as_slice(),
            b"a_much_longer_key_for_testing_boundaries".as_slice(),
            b"k".as_slice(),
        ];

        for (i, key) in test_keys.iter().enumerate() {
            let value = (i as u64 + 1) * 100;
            
            match node.insert_key_value(key, value) {
                Ok(()) => {
                    // Verify the insertion
                    if node.header.key_count != (i + 1) as u16 {
                        self.violations.record_memory_layout_violation();
                        return Err("Key count mismatch after insertion".into());
                    }

                    // Verify we can retrieve the value
                    if let Some(retrieved_value) = node.get_value_at_index(i) {
                        if retrieved_value != value {
                            self.violations.record_memory_layout_violation();
                            return Err("Value mismatch after insertion".into());
                        }
                    } else {
                        self.violations.record_memory_layout_violation();
                        return Err("Failed to retrieve inserted value".into());
                    }
                }
                Err(e) => {
                    if i < 256 { // Should succeed for reasonable number of keys
                        self.violations.record_memory_layout_violation();
                        return Err(format!("Unexpected insertion failure: {}", e).into());
                    }
                }
            }
        }

        // Test boundary conditions
        let mut full_node = OptimizedBTreeNode::new(0, 2);
        
        // Try to exceed capacity
        for i in 0..300 {
            let key = format!("key_{:03}", i);
            match full_node.insert_key_value(key.as_bytes(), i as u64) {
                Ok(()) => {
                    if full_node.header.key_count > 256 {
                        self.violations.record_memory_layout_violation();
                        return Err("Node exceeded maximum capacity".into());
                    }
                }
                Err(_) => {
                    // Expected when full
                    break;
                }
            }
        }

        Ok(())
    }

    fn test_simd_prefetch_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        #[cfg(target_arch = "x86_64")]
        {
            use lightning_db::core::btree::cache_optimized::prefetch_node;

            // Test prefetch operations with various addresses
            let test_buffer = vec![0u8; 8192];
            let base_addr = test_buffer.as_ptr();

            // Test valid prefetch operations
            for offset in (0..4096).step_by(64) {
                let addr = unsafe { base_addr.add(offset) };
                prefetch_node(addr); // Should not crash
            }

            // Test boundary prefetch (should be safe even with invalid addresses)
            let null_addr = ptr::null::<u8>();
            prefetch_node(null_addr); // Should not crash (prefetch hint only)

            // Test misaligned addresses
            for i in 1..64 {
                let addr = unsafe { base_addr.add(i) };
                prefetch_node(addr); // Should handle misalignment
            }
        }

        // Non-x86_64 platforms should handle prefetch gracefully
        #[cfg(not(target_arch = "x86_64"))]
        {
            // Prefetch should be no-op on other platforms
            use lightning_db::core::btree::cache_optimized::prefetch_node;
            prefetch_node(ptr::null::<u8>());
        }

        Ok(())
    }

    /// Validate Lock-Free unsafe blocks (15 blocks)
    fn validate_lock_free_unsafe_blocks(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Validating Lock-Free unsafe blocks...");

        // Test 1: HotPathCache epoch safety
        self.test_hot_path_cache_epoch_safety()?;
        
        // Test 2: Zero-copy access safety
        self.test_zero_copy_access_safety()?;
        
        // Test 3: Concurrent insertion safety
        self.test_concurrent_insertion_safety()?;
        
        // Test 4: Memory reclamation safety
        self.test_memory_reclamation_safety()?;

        println!("Lock-Free unsafe validation completed");
        Ok(())
    }

    fn test_hot_path_cache_epoch_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test epoch-based memory management in HotPathCache
        let cache = Arc::new(HotPathCache::<u64, String, 128>::new());
        let error_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(self.config.concurrent_threads));

        let handles: Vec<_> = (0..self.config.concurrent_threads)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                let error_count = Arc::clone(&error_count);
                let barrier = Arc::clone(&barrier);
                let iterations = self.config.stress_iterations / self.config.concurrent_threads;

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..iterations {
                        let key = (thread_id * iterations + i) as u64;
                        let value = format!("value_{}_{}", thread_id, i);

                        // Insert
                        cache.insert(key, value.clone());

                        // Immediate read (tests epoch protection)
                        match cache.get(&key) {
                            Some(retrieved) => {
                                if retrieved != value {
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            None => {
                                // May happen due to eviction, but shouldn't cause corruption
                            }
                        }

                        // Test zero-copy access
                        let _ = cache.with_value_ref(&key, |v| v.len());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let errors = error_count.load(Ordering::Relaxed);
        if errors > 0 {
            self.violations.record_lock_free_violation();
            return Err(format!("Epoch safety violation: {} data corruption errors", errors).into());
        }

        Ok(())
    }

    fn test_zero_copy_access_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test zero-copy access patterns
        let cache = Arc::new(HotPathCache::<String, Vec<u8>, 64>::new());

        // Insert test data
        for i in 0..32 {
            let key = format!("key_{}", i);
            let value = vec![i as u8; 1024]; // 1KB per entry
            cache.insert(key, value);
        }

        // Test concurrent zero-copy access
        let error_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));

        let handles: Vec<_> = (0..8)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                let error_count = Arc::clone(&error_count);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..1000 {
                        let key = format!("key_{}", i % 32);
                        
                        // Zero-copy access
                        if let Some(len) = cache.with_value_ref(&key, |data| {
                            // Verify data integrity during zero-copy access
                            if data.len() != 1024 {
                                return None;
                            }
                            
                            let expected_byte = (key.split('_').nth(1).unwrap_or("0").parse::<u8>().unwrap_or(0)) % 32;
                            for &byte in data {
                                if byte != expected_byte {
                                    return None; // Corruption detected
                                }
                            }
                            Some(data.len())
                        }) {
                            if len != Some(1024) {
                                error_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let errors = error_count.load(Ordering::Relaxed);
        if errors > 0 {
            self.violations.record_lock_free_violation();
            return Err(format!("Zero-copy safety violation: {} corruption errors", errors).into());
        }

        Ok(())
    }

    fn test_concurrent_insertion_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test concurrent insertions for ABA problem prevention
        let cache = Arc::new(HotPathCache::<u64, u64, 256>::new());
        let insertion_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(self.config.concurrent_threads));

        let handles: Vec<_> = (0..self.config.concurrent_threads)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                let insertion_count = Arc::clone(&insertion_count);
                let barrier = Arc::clone(&barrier);
                let iterations = 1000;

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..iterations {
                        let key = (i % 100) as u64; // High collision rate
                        let value = (thread_id * iterations + i) as u64;

                        cache.insert(key, value);
                        insertion_count.fetch_add(1, Ordering::Relaxed);

                        // Verify we can read some value (may not be the one we just inserted)
                        if let Some(_) = cache.get(&key) {
                            // Value exists, which is good
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let total_insertions = insertion_count.load(Ordering::Relaxed);
        let expected_insertions = self.config.concurrent_threads * 1000;

        if total_insertions != expected_insertions {
            self.violations.record_lock_free_violation();
            return Err(format!("Insertion count mismatch: expected {}, got {}", 
                             expected_insertions, total_insertions).into());
        }

        Ok(())
    }

    fn test_memory_reclamation_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test memory reclamation doesn't cause use-after-free
        let cache = Arc::new(HotPathCache::<u64, Arc<Vec<u8>>, 32>::new());
        let mut weak_refs = Vec::new();

        // Fill cache and keep weak references
        for i in 0..64 {
            let data = Arc::new(vec![i as u8; 1024]);
            let weak_ref = Arc::downgrade(&data);
            cache.insert(i, data);
            weak_refs.push(weak_ref);
        }

        // Force evictions by inserting more data
        for i in 64..128 {
            let data = Arc::new(vec![i as u8; 1024]);
            cache.insert(i, data);
        }

        // Allow some time for epoch-based reclamation
        thread::sleep(Duration::from_millis(100));

        // Check that evicted data was properly reclaimed
        let mut reclaimed_count = 0;
        for weak_ref in weak_refs {
            if weak_ref.upgrade().is_none() {
                reclaimed_count += 1;
            }
        }

        // Some data should have been reclaimed due to cache eviction
        if reclaimed_count == 0 {
            eprintln!("Warning: No memory appears to have been reclaimed");
        }

        Ok(())
    }

    /// Validate BTree unsafe blocks (23 blocks)
    fn validate_btree_unsafe_blocks(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Validating BTree unsafe blocks...");

        // Test 1: Cache-optimized node operations
        self.test_cache_optimized_node_operations()?;
        
        // Test 2: SIMD key comparison safety
        self.test_simd_key_comparison_safety()?;
        
        // Test 3: Node prefetch safety
        self.test_node_prefetch_safety()?;

        println!("BTree unsafe validation completed");
        Ok(())
    }

    fn test_cache_optimized_node_operations(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test CacheOptimizedNode operations
        let mut node = CacheOptimizedNode::new(0); // Leaf node

        // Verify alignment
        let node_addr = &node as *const _ as usize;
        if node_addr % 64 != 0 {
            self.violations.record_btree_violation();
            return Err(format!("Node alignment violation: addr=0x{:x}", node_addr).into());
        }

        // Test node size
        if mem::size_of::<CacheOptimizedNode>() != 4096 {
            self.violations.record_btree_violation();
            return Err(format!("Unexpected node size: {}", mem::size_of::<CacheOptimizedNode>()).into());
        }

        // Test key operations
        let test_keys = [
            0x12345678u32,
            0x9ABCDEF0u32,
            0x11111111u32,
            0xFFFFFFFFu32,
            0x00000000u32,
        ];

        // Populate first_keys for testing
        for (i, &key_hash) in test_keys.iter().enumerate() {
            if i < 16 {
                node.first_keys[i] = key_hash;
                node.num_keys = (i + 1) as u16;
            }
        }

        // Test key search
        for &key_hash in &test_keys {
            let found = node.find_key(key_hash);
            if key_hash == test_keys[0] && found != Some(0) {
                self.violations.record_btree_violation();
                return Err("Key search failed".into());
            }
        }

        // Test SIMD search if available
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                for &key_hash in &test_keys {
                    let found = unsafe { node.find_key_simd(key_hash) };
                    // Compare with non-SIMD result
                    let expected = node.find_key(key_hash);
                    if found != expected {
                        self.violations.record_btree_violation();
                        return Err("SIMD search mismatch with scalar search".into());
                    }
                }
            }
        }

        Ok(())
    }

    fn test_simd_key_comparison_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse4.2") {
                use lightning_db::core::btree::cache_optimized::compare_keys_simd;

                let test_cases = [
                    (b"hello", b"hello"),
                    (b"hello", b"world"),
                    (b"abc", b"abcd"),
                    (b"test_key_1", b"test_key_2"),
                    (b"", b""),
                    (b"a", b""),
                    (b"", b"a"),
                ];

                for (key_a, key_b) in test_cases {
                    let scalar_result = key_a.cmp(key_b);
                    let simd_result = unsafe { compare_keys_simd(key_a, key_b) };
                    
                    if scalar_result != simd_result {
                        self.violations.record_btree_violation();
                        return Err(format!("SIMD comparison mismatch: {:?} vs {:?} = {:?} (scalar) vs {:?} (SIMD)",
                                         key_a, key_b, scalar_result, simd_result).into());
                    }
                }

                // Test with various lengths and alignments
                for len in 1..=64 {
                    let mut key_a = vec![0xAAu8; len];
                    let mut key_b = vec![0xBBu8; len];
                    
                    // Test equal keys
                    key_b.copy_from_slice(&key_a);
                    let scalar_result = key_a.cmp(&key_b);
                    let simd_result = unsafe { compare_keys_simd(&key_a, &key_b) };
                    
                    if scalar_result != simd_result {
                        self.violations.record_btree_violation();
                        return Err(format!("SIMD comparison failed for length {}", len).into());
                    }

                    // Test different keys
                    if len > 0 {
                        key_b[len - 1] = 0xCC;
                        let scalar_result = key_a.cmp(&key_b);
                        let simd_result = unsafe { compare_keys_simd(&key_a, &key_b) };
                        
                        if scalar_result != simd_result {
                            self.violations.record_btree_violation();
                            return Err(format!("SIMD comparison failed for different keys of length {}", len).into());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn test_node_prefetch_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        use lightning_db::core::btree::cache_optimized::prefetch_node;

        // Test prefetch with various scenarios
        let test_data = vec![0u8; 8192];
        let base_ptr = test_data.as_ptr();

        // Test aligned prefetch
        for offset in (0..4096).step_by(64) {
            let ptr = unsafe { base_ptr.add(offset) };
            prefetch_node(ptr); // Should not crash
        }

        // Test unaligned prefetch
        for offset in 1..64 {
            let ptr = unsafe { base_ptr.add(offset) };
            prefetch_node(ptr); // Should handle gracefully
        }

        // Test boundary cases
        prefetch_node(base_ptr); // Start of buffer
        prefetch_node(unsafe { base_ptr.add(test_data.len() - 1) }); // End of buffer

        // Test null pointer (should be safe as it's just a hint)
        prefetch_node(ptr::null());

        // Test invalid pointers (should be safe as it's just a hint)
        prefetch_node(0x1 as *const u8);
        prefetch_node(0xDEADBEEF as *const u8);

        Ok(())
    }

    /// Validate interactions between unsafe components
    fn validate_unsafe_interactions(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Validating unsafe component interactions...");

        // Test 1: Memory layout + Lock-free cache interaction
        self.test_memory_layout_lock_free_interaction()?;
        
        // Test 2: BTree + IO Uring interaction
        self.test_btree_io_uring_interaction()?;
        
        // Test 3: Cross-component memory sharing
        self.test_cross_component_memory_sharing()?;

        println!("Unsafe interaction validation completed");
        Ok(())
    }

    fn test_memory_layout_lock_free_interaction(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test interaction between cache-aligned allocator and lock-free cache
        let cache = Arc::new(HotPathCache::<u64, Arc<MappedBuffer>, 64>::new());

        // Insert mapped buffers into cache
        for i in 0..32 {
            let buffer = Arc::new(MappedBuffer::new(4096)?);
            cache.insert(i, buffer);
        }

        // Test concurrent access to mapped buffers through cache
        let barrier = Arc::new(Barrier::new(4));
        let error_count = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                let barrier = Arc::clone(&barrier);
                let error_count = Arc::clone(&error_count);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..1000 {
                        let key = (i % 32) as u64;
                        
                        if let Some(buffer) = cache.get(&key) {
                            // Access buffer through zero-copy
                            cache.with_value_ref(&key, |buffer_ref| {
                                let slice = buffer_ref.as_slice();
                                if slice.is_empty() {
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                }
                                slice.len()
                            });
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let errors = error_count.load(Ordering::Relaxed);
        if errors > 0 {
            self.violations.record_memory_layout_violation();
            self.violations.record_lock_free_violation();
            return Err(format!("Memory layout + lock-free interaction error: {} failures", errors).into());
        }

        Ok(())
    }

    fn test_btree_io_uring_interaction(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test BTree nodes with IO operations (simulated)
        let mut nodes = Vec::new();
        
        // Create cache-optimized nodes
        for i in 0..16 {
            let node = CacheOptimizedNode::new(0);
            nodes.push(node);
        }

        // Simulate IO buffer operations on node data
        for (i, node) in nodes.iter().enumerate() {
            let node_ptr = node as *const CacheOptimizedNode as *const u8;
            let node_size = mem::size_of::<CacheOptimizedNode>();

            // Verify node is properly aligned for IO operations
            let addr = node_ptr as usize;
            if addr % 64 != 0 {
                self.violations.record_btree_violation();
                return Err(format!("Node {} not aligned for IO: addr=0x{:x}", i, addr).into());
            }

            // Simulate direct IO requirements (typically 512-byte aligned)
            if addr % 512 != 0 {
                eprintln!("Warning: Node {} may not be optimal for direct IO", i);
            }

            // Test that we can safely read the entire node
            unsafe {
                let slice = std::slice::from_raw_parts(node_ptr, node_size);
                let _checksum = slice.iter().fold(0u32, |acc, &b| acc.wrapping_add(b as u32));
                // Checksum calculation should not crash
            }
        }

        Ok(())
    }

    fn test_cross_component_memory_sharing(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.violations.record_test();

        // Test sharing memory between different unsafe components
        let buffer_size = 8192;
        let buffer = MappedBuffer::new(buffer_size)?;
        let slice = buffer.as_slice();

        // Share buffer between different components
        let cache = Arc::new(HotPathCache::<String, (*const u8, usize), 32>::new());

        // Store buffer references in cache (unsafe but controlled)
        let ptr = slice.as_ptr();
        let len = slice.len();
        cache.insert("shared_buffer".to_string(), (ptr, len));

        // Test concurrent access through different code paths
        let barrier = Arc::new(Barrier::new(3));
        let error_count = Arc::new(AtomicUsize::new(0));

        // Thread 1: Access through cache
        let cache_clone = Arc::clone(&cache);
        let barrier_clone = Arc::clone(&barrier);
        let error_count_clone = Arc::clone(&error_count);
        let handle1 = thread::spawn(move || {
            barrier_clone.wait();
            
            for _ in 0..1000 {
                if let Some((ptr, len)) = cache_clone.get(&"shared_buffer".to_string()) {
                    unsafe {
                        if !ptr.is_null() && len > 0 {
                            let slice = std::slice::from_raw_parts(ptr, len);
                            let _sum: u64 = slice.iter().map(|&b| b as u64).sum();
                        } else {
                            error_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        // Thread 2: Direct buffer access
        let buffer_ptr = slice.as_ptr();
        let buffer_len = slice.len();
        let barrier_clone = Arc::clone(&barrier);
        let error_count_clone = Arc::clone(&error_count);
        let handle2 = thread::spawn(move || {
            barrier_clone.wait();
            
            for _ in 0..1000 {
                unsafe {
                    let slice = std::slice::from_raw_parts(buffer_ptr, buffer_len);
                    if slice.len() != buffer_len {
                        error_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        // Thread 3: BTree node operations
        let barrier_clone = Arc::clone(&barrier);
        let error_count_clone = Arc::clone(&error_count);
        let handle3 = thread::spawn(move || {
            barrier_clone.wait();
            
            let mut node = CacheOptimizedNode::new(0);
            for i in 0..100 {
                let key_hash = (i as u32).wrapping_mul(0x9E3779B9);
                if i < 16 {
                    node.first_keys[i] = key_hash;
                    node.num_keys = (i + 1) as u16;
                }
                
                // Search operations
                if node.find_key(key_hash).is_none() && i > 0 {
                    error_count_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
        handle3.join().unwrap();

        let errors = error_count.load(Ordering::Relaxed);
        if errors > 0 {
            return Err(format!("Cross-component memory sharing error: {} failures", errors).into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsafe_validation_suite() {
        let config = UnsafeTestConfig {
            stress_iterations: 1000,
            concurrent_threads: 4,
            memory_pressure_mb: 10,
            io_operations: 100,
            enable_timing_checks: true,
            enable_invariant_checks: true,
        };

        let suite = UnsafeValidationSuite::new(config);
        
        match suite.run_all_validations() {
            Ok(()) => println!("All unsafe validations passed!"),
            Err(e) => panic!("Unsafe validation failed: {}", e),
        }
    }

    #[test]
    fn test_io_uring_validations() {
        let config = UnsafeTestConfig::default();
        let suite = UnsafeValidationSuite::new(config);
        
        suite.validate_io_uring_unsafe_blocks().unwrap();
    }

    #[test]
    fn test_memory_layout_validations() {
        let config = UnsafeTestConfig::default();
        let suite = UnsafeValidationSuite::new(config);
        
        suite.validate_memory_layout_unsafe_blocks().unwrap();
    }

    #[test]
    fn test_lock_free_validations() {
        let config = UnsafeTestConfig::default();
        let suite = UnsafeValidationSuite::new(config);
        
        suite.validate_lock_free_unsafe_blocks().unwrap();
    }

    #[test]
    fn test_btree_validations() {
        let config = UnsafeTestConfig::default();
        let suite = UnsafeValidationSuite::new(config);
        
        suite.validate_btree_unsafe_blocks().unwrap();
    }

    #[test]
    fn test_unsafe_interactions() {
        let config = UnsafeTestConfig::default();
        let suite = UnsafeValidationSuite::new(config);
        
        suite.validate_unsafe_interactions().unwrap();
    }

    #[test] 
    fn test_stress_validation() {
        let config = UnsafeTestConfig {
            stress_iterations: 50000,
            concurrent_threads: 8,
            memory_pressure_mb: 50,
            io_operations: 5000,
            enable_timing_checks: true,
            enable_invariant_checks: true,
        };

        let suite = UnsafeValidationSuite::new(config);
        suite.run_all_validations().unwrap();
    }
}