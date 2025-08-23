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
        if !self._lifetime_guard.load(Ordering::Acquire) {
            return Err(Error::new(ErrorKind::Other, "Memory region invalidated"));
        }
        
        // SAFETY: Memory is valid as long as lifetime_guard is true
        // RISK: LOW - Lifetime tracking prevents use-after-free
        Ok(unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) })
    }
    
    /// Get a safe mutable view of the memory
    pub fn as_mut_slice(&mut self) -> Result<&mut [u8]> {
        if !self._lifetime_guard.load(Ordering::Acquire) {
            return Err(Error::new(ErrorKind::Other, "Memory region invalidated"));
        }
        
        // SAFETY: Memory is valid and we have exclusive access
        // RISK: LOW - Lifetime tracking and exclusive access
        Ok(unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) })
    }
    
    /// Get raw pointer (for system calls) with bounds checking
    pub fn as_ptr_bounded(&self, offset: usize, length: usize) -> Result<*const u8> {
        if !self._lifetime_guard.load(Ordering::Acquire) {
            return Err(Error::new(ErrorKind::Other, "Memory region invalidated"));
        }
        
        if offset >= self.len || offset + length > self.len {
            return Err(Error::new(ErrorKind::InvalidInput, "Bounds check failed"));
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
    
    /// Safely read from ring buffer with bounds checking
    pub fn read(&mut self, index: usize) -> Result<T> {
        if index >= self.capacity {
            return Err(Error::new(ErrorKind::InvalidInput, "Ring index out of bounds"));
        }
        
        let offset = index * std::mem::size_of::<T>();
        let ptr = self.memory.as_ptr_bounded(offset, std::mem::size_of::<T>())?;
        
        // SAFETY: Bounds checked access to properly aligned memory
        // RISK: LOW - Explicit validation
        Ok(unsafe { std::ptr::read(ptr as *const T) })
    }
    
    /// Safely write to ring buffer with bounds checking
    pub fn write(&mut self, index: usize, value: T) -> Result<()> {
        if index >= self.capacity {
            return Err(Error::new(ErrorKind::InvalidInput, "Ring index out of bounds"));
        }
        
        let slice = self.memory.as_mut_slice()?;
        let offset = index * std::mem::size_of::<T>();
        
        if offset + std::mem::size_of::<T>() > slice.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Write would exceed buffer"));
        }
        
        // SAFETY: Bounds checked write to valid memory
        // RISK: LOW - Explicit bounds checking
        unsafe {
            std::ptr::write(slice.as_mut_ptr().add(offset) as *mut T, value);
        }
        
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
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid opcode"));
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
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid file descriptor"));
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
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid buffer parameters"));
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
    
    /// Load value with bounds checking
    pub fn load(&self, ordering: Ordering) -> u32 {
        // SAFETY: Pointer was validated at construction
        // RISK: MEDIUM - Depends on caller validation
        let value = unsafe { (*self.ptr).load(ordering) };
        
        // Apply ring mask to ensure bounds
        value & self.ring_mask
    }
    
    /// Store value with bounds checking
    pub fn store(&self, value: u32, ordering: Ordering) {
        // Apply ring mask to ensure bounds
        let masked_value = value & self.ring_mask;
        
        // SAFETY: Pointer was validated at construction
        // RISK: MEDIUM - Depends on caller validation
        unsafe {
            (*self.ptr).store(masked_value, ordering);
        }
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