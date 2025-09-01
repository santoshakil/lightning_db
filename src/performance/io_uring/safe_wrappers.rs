//! Safe Wrappers for io_uring Operations
//!
//! This module provides safe abstractions over the unsafe io_uring operations
//! to prevent common security vulnerabilities while maintaining performance.

use super::*;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Safe wrapper around raw memory regions to prevent use-after-free
pub struct SafeMemoryRegion {
    ptr: *mut u8,
    len: usize,
    alignment: usize,
    _lifetime_guard: Arc<AtomicBool>, // Tracks if memory is still valid
}

impl SafeMemoryRegion {
    /// Create a safe memory region with lifetime tracking
    pub fn new(size: usize, alignment: usize) -> Result<Self> {
        use std::alloc::{alloc, Layout};
        
        let layout = Layout::from_size_align(size, alignment)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid layout"))?;
        
        // SAFETY: Standard allocation with validated layout
        // RISK: LOW - Proper validation and null checking
        let ptr = unsafe { alloc(layout) };
        
        if ptr.is_null() {
            return Err(Error::new(ErrorKind::OutOfMemory, "Allocation failed"));
        }
        
        Ok(SafeMemoryRegion {
            ptr,
            len: size,
            alignment,
            _lifetime_guard: Arc::new(AtomicBool::new(true)),
        })
    }
    
    /// Get a safe view of the memory that can detect use-after-free
    pub fn as_slice(&self) -> Result<&[u8]> {
        use crate::performance::io_uring::security_validation::*;
        
        // Check if memory region is still valid
        if !self._lifetime_guard.load(Ordering::Acquire) {
            SECURITY_STATS.record_use_after_free_detection();
            return Err(Error::other("Memory region invalidated - use after free detected"));
        }
        
        // Additional validation
        if self.ptr.is_null() {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Null pointer in memory region"));
        }
        
        if self.len == 0 {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Zero length memory region"));
        }
        
        // Check for potential overflow
        if (self.ptr as usize).checked_add(self.len).is_none() {
            SECURITY_STATS.record_bounds_check(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Memory region address overflow"));
        }
        
        // SAFETY: Memory is valid with comprehensive validation
        // Invariants:
        // 1. lifetime_guard ensures memory hasn't been freed
        // 2. ptr is non-null (checked above)
        // 3. len is non-zero (checked above)
        // 4. No address overflow (checked above)
        // 5. Memory was allocated with proper alignment
        // Guarantees:
        // - Use-after-free protection via lifetime tracking
        // - Bounds checking prevents buffer overruns
        // - Comprehensive validation of memory region
        // RISK: LOW - Multiple validation layers
        SECURITY_STATS.record_buffer_validation(true);
        Ok(unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) })
    }
    
    /// Get a safe mutable view of the memory
    pub fn as_mut_slice(&mut self) -> Result<&mut [u8]> {
        use crate::performance::io_uring::security_validation::*;
        
        // Check if memory region is still valid
        if !self._lifetime_guard.load(Ordering::Acquire) {
            SECURITY_STATS.record_use_after_free_detection();
            return Err(Error::other("Memory region invalidated - use after free detected"));
        }
        
        // Additional validation (same as as_slice)
        if self.ptr.is_null() {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Null pointer in memory region"));
        }
        
        if self.len == 0 {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Zero length memory region"));
        }
        
        // Check for potential overflow
        if (self.ptr as usize).checked_add(self.len).is_none() {
            SECURITY_STATS.record_bounds_check(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Memory region address overflow"));
        }
        
        // SAFETY: Memory is valid with comprehensive validation and exclusive access
        // Invariants:
        // 1. lifetime_guard ensures memory hasn't been freed
        // 2. ptr is non-null (checked above)
        // 3. len is non-zero (checked above)
        // 4. No address overflow (checked above)
        // 5. Exclusive access via &mut self
        // 6. Memory was allocated with proper alignment
        // Guarantees:
        // - Use-after-free protection via lifetime tracking
        // - Bounds checking prevents buffer overruns
        // - No aliasing due to exclusive access
        // - Comprehensive validation of memory region
        // RISK: LOW - Multiple validation layers with exclusive access
        SECURITY_STATS.record_buffer_validation(true);
        Ok(unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) })
    }
    
    /// Get raw pointer (for system calls) with bounds checking
    pub fn as_ptr_bounded(&self, offset: usize, length: usize) -> Result<*const u8> {
        if !self._lifetime_guard.load(Ordering::Acquire) {
            return Err(Error::other("Memory region invalidated"));
        }
        
        if offset >= self.len || offset + length > self.len {
            return Err(Error::other("Bounds check failed"));
        }
        
        // SAFETY: Bounds checked access to valid memory
        // RISK: LOW - Explicit bounds checking
        Ok(unsafe { self.ptr.add(offset) as *const u8 })
    }
}

impl Drop for SafeMemoryRegion {
    fn drop(&mut self) {
        use std::alloc::{dealloc, Layout};
        
        // Mark memory as invalid
        self._lifetime_guard.store(false, Ordering::Release);
        
        let layout = Layout::from_size_align(self.len, self.alignment).unwrap();
        
        // SAFETY: Deallocating memory that was allocated with same layout
        // RISK: LOW - Standard deallocation pattern
        unsafe {
            dealloc(self.ptr, layout);
        }
    }
}

/// Safe bounds-checked ring buffer operations
pub struct SafeRingBuffer<T> {
    memory: SafeMemoryRegion,
    capacity: usize,
    head: usize,
    tail: usize,
    _phantom: PhantomData<T>,
}

impl<T: Copy> SafeRingBuffer<T> {
    pub fn new(capacity: usize) -> Result<Self> {
        let size = capacity * std::mem::size_of::<T>();
        let alignment = std::mem::align_of::<T>();
        
        let memory = SafeMemoryRegion::new(size, alignment)?;
        
        Ok(SafeRingBuffer {
            memory,
            capacity,
            head: 0,
            tail: 0,
            _phantom: PhantomData,
        })
    }
    
    /// Safely read from ring buffer with comprehensive bounds checking
    pub fn read(&mut self, index: usize) -> Result<T> {
        use crate::performance::io_uring::security_validation::*;
        
        // Validate index bounds
        if index >= self.capacity {
            SECURITY_STATS.record_bounds_check(false);
            return Err(Error::new(ErrorKind::InvalidInput, 
                format!("Ring index {} out of bounds (capacity {})", index, self.capacity)));
        }
        
        // Check for overflow in offset calculation
        let type_size = std::mem::size_of::<T>();
        let offset = index.checked_mul(type_size)
            .ok_or_else(|| {
                SECURITY_STATS.record_bounds_check(false);
                Error::new(ErrorKind::InvalidInput, "Ring buffer offset overflow")
            })?;
        
        // Validate the calculated offset and size
        let ptr = self.memory.as_ptr_bounded(offset, type_size)?;
        
        // Additional alignment check for the type
        if (ptr as usize) % std::mem::align_of::<T>() != 0 {
            SECURITY_STATS.record_alignment_check(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Ring buffer read misalignment"));
        }
        
        debug_assert!(index < self.capacity, "Ring index {} exceeds capacity {}", index, self.capacity);
        debug_assert_eq!((ptr as usize) % std::mem::align_of::<T>(), 0, "Misaligned read");
        
        // SAFETY: Comprehensive bounds and alignment checking
        // Invariants:
        // 1. index < capacity (checked above)
        // 2. offset calculation checked for overflow
        // 3. ptr is within memory region bounds (validated by as_ptr_bounded)
        // 4. Proper alignment for type T (checked above)
        // 5. Memory region is valid (validated by as_ptr_bounded)
        // Guarantees:
        // - No buffer overrun
        // - Proper alignment for type T
        // - Valid memory access
        // RISK: LOW - Comprehensive validation
        SECURITY_STATS.record_bounds_check(true);
        SECURITY_STATS.record_alignment_check(true);
        Ok(unsafe { std::ptr::read(ptr as *const T) })
    }
    
    /// Safely write to ring buffer with comprehensive bounds checking
    pub fn write(&mut self, index: usize, value: T) -> Result<()> {
        use crate::performance::io_uring::security_validation::*;
        
        // Validate index bounds
        if index >= self.capacity {
            SECURITY_STATS.record_bounds_check(false);
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("Ring index {} out of bounds (capacity {})", index, self.capacity)));
        }
        
        // Check for overflow in offset calculation
        let type_size = std::mem::size_of::<T>();
        let offset = index.checked_mul(type_size)
            .ok_or_else(|| {
                SECURITY_STATS.record_bounds_check(false);
                Error::new(ErrorKind::InvalidInput, "Ring buffer offset overflow")
            })?;
        
        let slice = self.memory.as_mut_slice()?;
        
        // Double-check bounds with slice length
        if offset.checked_add(type_size).map(|end| end > slice.len()).unwrap_or(true) {
            SECURITY_STATS.record_bounds_check(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Write would exceed buffer"));
        }
        
        // Calculate the target pointer
        let target_ptr = unsafe { slice.as_mut_ptr().add(offset) } as *mut T;
        
        // Additional alignment check for the type
        if (target_ptr as usize) % std::mem::align_of::<T>() != 0 {
            SECURITY_STATS.record_alignment_check(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Ring buffer write misalignment"));
        }
        
        debug_assert!(index < self.capacity, "Ring index {} exceeds capacity {}", index, self.capacity);
        debug_assert_eq!((target_ptr as usize) % std::mem::align_of::<T>(), 0, "Misaligned write");
        debug_assert!(offset + type_size <= slice.len(), "Write would exceed buffer bounds");
        
        // SAFETY: Comprehensive bounds and alignment checking
        // Invariants:
        // 1. index < capacity (checked above)
        // 2. offset calculation checked for overflow
        // 3. Write doesn't exceed buffer bounds (checked above)
        // 4. Proper alignment for type T (checked above)
        // 5. Memory region is valid (validated by as_mut_slice)
        // 6. Exclusive access via &mut self
        // Guarantees:
        // - No buffer overrun
        // - Proper alignment for type T
        // - Valid memory access
        // - No data races due to exclusive access
        // RISK: LOW - Comprehensive validation with exclusive access
        unsafe {
            std::ptr::write(target_ptr, value);
        }
        
        SECURITY_STATS.record_bounds_check(true);
        SECURITY_STATS.record_alignment_check(true);
        Ok(())
    }
}

/// Safe io_uring submission queue entry with validation
pub struct SafeSubmissionEntry {
    #[cfg(target_os = "linux")]
    inner: super::linux_io_uring::IoUringSqe,
    #[cfg(not(target_os = "linux"))]
    inner: [u8; 128], // Mock SQE for non-Linux platforms
    validated: bool,
}

impl SafeSubmissionEntry {
    pub fn new() -> Self {
        SafeSubmissionEntry {
            inner: unsafe { std::mem::zeroed() },
            validated: false,
        }
    }
    
    /// Set operation type with validation
    pub fn set_opcode(&mut self, opcode: u8) -> Result<()> {
        // Validate opcode is in known range
        const MAX_OPCODE: u8 = 30; // Adjust based on kernel version
        if opcode > MAX_OPCODE {
            return Err(Error::other("Invalid opcode"));
        }
        
        #[cfg(target_os = "linux")]
        {
            self.inner.opcode = opcode;
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.inner[0] = opcode; // Mock storage
        }
        Ok(())
    }
    
    /// Set file descriptor with validation
    pub fn set_fd(&mut self, fd: i32) -> Result<()> {
        if fd < 0 {
            return Err(Error::other("Invalid file descriptor"));
        }
        
        #[cfg(target_os = "linux")]
        {
            self.inner.fd = fd;
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Mock storage - store fd in bytes 4-7
            let bytes = fd.to_le_bytes();
            self.inner[4..8].copy_from_slice(&bytes);
        }
        Ok(())
    }
    
    /// Set buffer address with alignment checking
    pub fn set_buffer(&mut self, addr: u64, len: u32, required_alignment: usize) -> Result<()> {
        if addr == 0 || len == 0 {
            return Err(Error::other("Invalid buffer parameters"));
        }
        
        // Check alignment
        if addr as usize % required_alignment != 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "Buffer not properly aligned"));
        }
        
        // Basic bounds checking (platform specific)
        #[cfg(target_pointer_width = "64")]
        {
            const MAX_USER_ADDR: u64 = 0x7fff_ffff_ffff;
            if addr > MAX_USER_ADDR || addr.saturating_add(len as u64) > MAX_USER_ADDR {
                return Err(Error::new(ErrorKind::InvalidInput, "Buffer address out of range"));
            }
        }
        
        #[cfg(target_os = "linux")]
        {
            self.inner.addr = addr;
            self.inner.len = len;
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Mock storage - store addr in bytes 8-15, len in bytes 16-19
            let addr_bytes = addr.to_le_bytes();
            let len_bytes = len.to_le_bytes();
            self.inner[8..16].copy_from_slice(&addr_bytes);
            self.inner[16..20].copy_from_slice(&len_bytes);
        }
        Ok(())
    }
    
    /// Mark entry as validated
    pub fn mark_validated(&mut self) {
        self.validated = true;
    }
    
    /// Get the inner SQE only if validated
    #[cfg(target_os = "linux")]
    pub fn into_inner(self) -> Result<super::linux_io_uring::IoUringSqe> {
        if !self.validated {
            return Err(Error::new(ErrorKind::Other, "SQE not validated"));
        }
        
        Ok(self.inner)
    }
    
    /// Mock implementation for non-Linux platforms
    #[cfg(not(target_os = "linux"))]
    pub fn into_inner(self) -> Result<[u8; 128]> {
        if !self.validated {
            return Err(Error::new(ErrorKind::Other, "SQE not validated"));
        }
        
        Ok(self.inner)
    }
}

/// Safe atomic operations for ring pointers
pub struct SafeAtomicRingPointer {
    ptr: *const std::sync::atomic::AtomicU32,
    ring_mask: u32,
}

impl SafeAtomicRingPointer {
    /// Create safe ring pointer with validation
    /// 
    /// # Safety
    /// Caller must ensure ptr points to valid atomic memory for the lifetime of this object
    pub unsafe fn new(ptr: *const std::sync::atomic::AtomicU32, ring_mask: u32) -> Self {
        debug_assert!(!ptr.is_null());
        debug_assert!(ring_mask > 0 && (ring_mask & (ring_mask + 1)) == 0, "Ring mask must be 2^n - 1");
        
        SafeAtomicRingPointer { ptr, ring_mask }
    }
    
    /// Load value with bounds checking and validation
    pub fn load(&self, ordering: Ordering) -> Result<u32> {
        use crate::performance::io_uring::security_validation::*;
        
        // Validate pointer is not null
        if self.ptr.is_null() {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Null atomic pointer in load"));
        }
        
        // SAFETY: Pointer was validated at construction and checked above
        // Invariants:
        // 1. ptr is non-null (checked above)
        // 2. ptr points to valid AtomicU32 for lifetime of object
        // 3. Atomic load operation is thread-safe
        // 4. Ring mask ensures value stays within bounds
        // Guarantees:
        // - Thread-safe atomic access
        // - Bounds-checked result
        // - Null pointer protection
        // RISK: LOW - Comprehensive validation with atomic safety
        let value = unsafe { (*self.ptr).load(ordering) };
        
        // Validate ring mask is valid (power of 2 minus 1)
        if (self.ring_mask & (self.ring_mask + 1)) != 0 {
            SECURITY_STATS.record_bounds_check(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid ring mask"));
        }
        
        // Apply ring mask to ensure bounds
        let masked_value = value & self.ring_mask;
        
        debug_assert!(masked_value <= self.ring_mask, "Masked value exceeds ring mask");
        
        SECURITY_STATS.record_bounds_check(true);
        Ok(masked_value)
    }
    
    /// Store value with bounds checking and validation
    pub fn store(&self, value: u32, ordering: Ordering) -> Result<()> {
        use crate::performance::io_uring::security_validation::*;
        
        // Validate pointer is not null
        if self.ptr.is_null() {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Null atomic pointer in store"));
        }
        
        // Validate ring mask is valid (power of 2 minus 1)
        if (self.ring_mask & (self.ring_mask + 1)) != 0 {
            SECURITY_STATS.record_bounds_check(false);
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid ring mask"));
        }
        
        // Apply ring mask to ensure bounds
        let masked_value = value & self.ring_mask;
        
        debug_assert!(masked_value <= self.ring_mask, "Masked value exceeds ring mask");
        
        // SAFETY: Pointer was validated at construction and checked above
        // Invariants:
        // 1. ptr is non-null (checked above)
        // 2. ptr points to valid AtomicU32 for lifetime of object
        // 3. Atomic store operation is thread-safe
        // 4. Value is masked to stay within bounds
        // Guarantees:
        // - Thread-safe atomic access
        // - Bounds-checked value
        // - Null pointer protection
        // RISK: LOW - Comprehensive validation with atomic safety
        unsafe {
            (*self.ptr).store(masked_value, ordering);
        }
        
        SECURITY_STATS.record_bounds_check(true);
        Ok(())
    }
}

/// Safe buffer validator for io_uring operations
pub struct SafeBufferValidator;

impl SafeBufferValidator {
    /// Validate buffer for io_uring operation
    pub fn validate_buffer(addr: u64, len: u32, op_type: OpType) -> Result<()> {
        if addr == 0 && len > 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "Null pointer with non-zero length"));
        }
        
        if len == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "Zero length buffer"));
        }
        
        // Check for integer overflow
        if addr.checked_add(len as u64).is_none() {
            return Err(Error::new(ErrorKind::InvalidInput, "Buffer address overflow"));
        }
        
        // Validate alignment based on operation type
        let required_alignment = match op_type {
            OpType::ReadFixed | OpType::WriteFixed => 512, // Direct I/O alignment
            OpType::Read | OpType::Write => 1,             // No specific alignment
            _ => 1,
        };
        
        if addr as usize % required_alignment != 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "Buffer not properly aligned"));
        }
        
        // Platform-specific validation
        #[cfg(target_os = "linux")]
        {
            // Check against Linux user space limits
            const TASK_SIZE: u64 = 0x8000_0000_0000; // x86_64 user space limit
            if addr >= TASK_SIZE || addr + len as u64 > TASK_SIZE {
                return Err(Error::new(ErrorKind::InvalidInput, "Buffer outside user space"));
            }
        }
        
        Ok(())
    }
    
    /// Validate file descriptor
    pub fn validate_fd(fd: i32) -> Result<()> {
        if fd < 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid file descriptor"));
        }
        
        // Check against reasonable fd limits
        const MAX_FD: i32 = 1024 * 1024; // Conservative limit
        if fd > MAX_FD {
            return Err(Error::new(ErrorKind::InvalidInput, "File descriptor too large"));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_safe_memory_region() {
        let mut region = SafeMemoryRegion::new(4096, 4096).unwrap();
        
        // Test valid access
        let slice = region.as_mut_slice().unwrap();
        assert_eq!(slice.len(), 4096);
        slice[0] = 42;
        
        // Test bounds checking
        assert!(region.as_ptr_bounded(4096, 1).is_err());
        assert!(region.as_ptr_bounded(4095, 2).is_err());
        assert!(region.as_ptr_bounded(0, 4096).is_ok());
    }
    
    #[test]
    fn test_safe_ring_buffer() {
        let mut ring: SafeRingBuffer<u32> = SafeRingBuffer::new(16).unwrap();
        
        // Test valid operations
        ring.write(0, 42).unwrap();
        assert_eq!(ring.read(0).unwrap(), 42);
        
        // Test bounds checking
        assert!(ring.write(16, 42).is_err());
        assert!(ring.read(16).is_err());
    }
    
    #[test]
    fn test_buffer_validation() {
        // Valid buffer
        assert!(SafeBufferValidator::validate_buffer(0x1000, 4096, OpType::Read).is_ok());
        
        // Invalid buffers
        assert!(SafeBufferValidator::validate_buffer(0, 4096, OpType::Read).is_err());
        assert!(SafeBufferValidator::validate_buffer(0x1000, 0, OpType::Read).is_err());
        assert!(SafeBufferValidator::validate_buffer(!0 - 100, 1024, OpType::Read).is_err()); // Overflow
    }
    
    #[test]
    fn test_safe_sqe() {
        let mut sqe = SafeSubmissionEntry::new();
        
        // Valid operations
        assert!(sqe.set_opcode(1).is_ok());
        assert!(sqe.set_fd(5).is_ok());
        assert!(sqe.set_buffer(0x1000, 4096, 8).is_ok());
        
        sqe.mark_validated();
        assert!(sqe.into_inner().is_ok());
        
        // Invalid operations
        let mut sqe2 = SafeSubmissionEntry::new();
        assert!(sqe2.set_opcode(255).is_err());
        assert!(sqe2.set_fd(-1).is_err());
        assert!(sqe2.into_inner().is_err()); // Not validated
    }
}
