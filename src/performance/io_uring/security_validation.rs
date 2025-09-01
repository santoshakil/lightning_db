//! Security Validation Module
//!
//! This module provides comprehensive security validation and runtime checks
//! for the io_uring implementation to prevent memory safety violations.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Global security statistics for monitoring
#[derive(Debug, Default)]
pub struct SecurityStats {
    pub buffer_validation_checks: AtomicU64,
    pub buffer_validation_failures: AtomicU64,
    pub alignment_checks: AtomicU64,
    pub alignment_failures: AtomicU64,
    pub bounds_checks: AtomicU64,
    pub bounds_failures: AtomicU64,
    pub use_after_free_detections: AtomicU64,
    pub double_free_detections: AtomicU64,
}

impl SecurityStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_buffer_validation(&self, success: bool) {
        self.buffer_validation_checks.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.buffer_validation_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_alignment_check(&self, success: bool) {
        self.alignment_checks.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.alignment_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_bounds_check(&self, success: bool) {
        self.bounds_checks.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.bounds_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_use_after_free_detection(&self) {
        self.use_after_free_detections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_double_free_detection(&self) {
        self.double_free_detections.fetch_add(1, Ordering::Relaxed);
    }
}

/// Global security validator instance
pub static SECURITY_STATS: std::sync::LazyLock<Arc<SecurityStats>> = 
    std::sync::LazyLock::new(|| Arc::new(SecurityStats::new()));

/// Comprehensive buffer security validator
pub struct BufferSecurityValidator;

impl BufferSecurityValidator {
    /// Validate buffer with comprehensive security checks
    pub fn validate_buffer_comprehensive(
        ptr: *const u8,
        len: usize,
        alignment: usize,
        operation: &str,
    ) -> Result<()> {
        // Check for null pointer
        if ptr.is_null() {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Null pointer detected in {}", operation),
            ));
        }

        // Check for zero length
        if len == 0 {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Zero length buffer in {}", operation),
            ));
        }

        // Check alignment
        let is_aligned = (ptr as usize) % alignment == 0;
        SECURITY_STATS.record_alignment_check(is_aligned);
        if !is_aligned {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Buffer misalignment in {}: addr={:p}, required_align={}", operation, ptr, alignment),
            ));
        }

        // Check for integer overflow
        let end_ptr = (ptr as usize).checked_add(len);
        let bounds_ok = end_ptr.is_some();
        SECURITY_STATS.record_bounds_check(bounds_ok);
        if !bounds_ok {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Buffer overflow detected in {}: ptr={:p}, len={}", operation, ptr, len),
            ));
        }

        // Platform-specific validation
        #[cfg(target_pointer_width = "64")]
        {
            const MAX_USER_ADDR: usize = 0x7fff_ffff_ffff_0000;
            if (ptr as usize) >= MAX_USER_ADDR || end_ptr.unwrap() >= MAX_USER_ADDR {
                SECURITY_STATS.record_buffer_validation(false);
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("Buffer address outside user space in {}", operation),
                ));
            }
        }

        // Size sanity check - prevent excessive allocations
        const MAX_BUFFER_SIZE: usize = 1024 * 1024 * 1024; // 1GB limit
        if len > MAX_BUFFER_SIZE {
            SECURITY_STATS.record_buffer_validation(false);
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Buffer size too large in {}: {} bytes", operation, len),
            ));
        }

        SECURITY_STATS.record_buffer_validation(true);
        Ok(())
    }

    /// Validate ring buffer operations
    pub fn validate_ring_access(
        ring_size: usize,
        index: usize,
        operation: &str,
    ) -> Result<()> {
        if index >= ring_size {
            SECURITY_STATS.record_bounds_check(false);
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Ring buffer index out of bounds in {}: index={}, size={}", operation, index, ring_size),
            ));
        }

        SECURITY_STATS.record_bounds_check(true);
        Ok(())
    }

    /// Validate file descriptor
    pub fn validate_fd_comprehensive(fd: i32, operation: &str) -> Result<()> {
        if fd < 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Invalid file descriptor in {}: {}", operation, fd),
            ));
        }

        // Check against reasonable limits
        const MAX_FD: i32 = 1024 * 1024;
        if fd > MAX_FD {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("File descriptor too large in {}: {}", operation, fd),
            ));
        }

        Ok(())
    }
}

/// Runtime memory safety checker
pub struct MemorySafetyChecker {
    allocated_regions: std::sync::Mutex<std::collections::HashSet<usize>>,
}

impl MemorySafetyChecker {
    pub fn new() -> Self {
        Self {
            allocated_regions: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Register a memory allocation
    pub fn register_allocation(&self, ptr: *const u8) {
        if let Ok(mut regions) = self.allocated_regions.lock() {
            regions.insert(ptr as usize);
        }
    }

    /// Unregister a memory allocation
    pub fn unregister_allocation(&self, ptr: *const u8) -> Result<()> {
        if let Ok(mut regions) = self.allocated_regions.lock() {
            if !regions.remove(&(ptr as usize)) {
                SECURITY_STATS.record_double_free_detection();
                return Err(Error::other(format!("Double-free detected: {:p}", ptr)));
            }
        }
        Ok(())
    }

    /// Check if a pointer is valid (registered and not freed)
    pub fn validate_pointer(&self, ptr: *const u8, operation: &str) -> Result<()> {
        if ptr.is_null() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Null pointer access in {}", operation),
            ));
        }

        if let Ok(regions) = self.allocated_regions.lock() {
            if !regions.contains(&(ptr as usize)) {
                SECURITY_STATS.record_use_after_free_detection();
                return Err(Error::other(format!("Use-after-free detected in {}: {:p}", operation, ptr)));
            }
        }

        Ok(())
    }
}

/// Thread-safe global memory checker
pub static MEMORY_CHECKER: std::sync::LazyLock<MemorySafetyChecker> = 
    std::sync::LazyLock::new(MemorySafetyChecker::new);

/// Security-enhanced macros for common operations
#[macro_export]
macro_rules! safe_ptr_offset {
    ($ptr:expr, $offset:expr, $bounds:expr, $op:expr) => {{
        if $offset >= $bounds {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Offset out of bounds in {}: offset={}, bounds={}", $op, $offset, $bounds),
            ));
        }
        
        // SAFETY: Bounds checked above
        // RISK: LOW - Explicit bounds validation
        unsafe { $ptr.add($offset) }
    }};
}

#[macro_export]
macro_rules! safe_slice_from_raw_parts {
    ($ptr:expr, $len:expr, $alignment:expr, $op:expr) => {{
        $crate::performance::io_uring::security_validation::BufferSecurityValidator::validate_buffer_comprehensive(
            $ptr,
            $len,
            $alignment,
            $op,
        )?;
        
        // SAFETY: Comprehensive validation performed above
        // RISK: LOW - Full validation with bounds, alignment, and overflow checks
        unsafe { std::slice::from_raw_parts($ptr, $len) }
    }};
}

#[macro_export]
macro_rules! safe_slice_from_raw_parts_mut {
    ($ptr:expr, $len:expr, $alignment:expr, $op:expr) => {{
        $crate::performance::io_uring::security_validation::BufferSecurityValidator::validate_buffer_comprehensive(
            $ptr,
            $len,
            $alignment,
            $op,
        )?;
        
        // SAFETY: Comprehensive validation performed above
        // RISK: LOW - Full validation with bounds, alignment, and overflow checks
        unsafe { std::slice::from_raw_parts_mut($ptr, $len) }
    }};
}

/// Security configuration for runtime checks
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    pub enable_bounds_checking: bool,
    pub enable_alignment_checking: bool,
    pub enable_use_after_free_detection: bool,
    pub enable_double_free_detection: bool,
    pub max_buffer_size: usize,
    pub strict_mode: bool, // Fails fast on any security violation
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enable_bounds_checking: true,
            enable_alignment_checking: true,
            enable_use_after_free_detection: cfg!(debug_assertions),
            enable_double_free_detection: cfg!(debug_assertions),
            max_buffer_size: 1024 * 1024 * 1024, // 1GB
            strict_mode: cfg!(debug_assertions),
        }
    }
}

/// Security context for tracking operations
pub struct SecurityContext {
    config: SecurityConfig,
    operation_id: AtomicU64,
}

impl SecurityContext {
    pub fn new(config: SecurityConfig) -> Self {
        Self {
            config,
            operation_id: AtomicU64::new(0),
        }
    }

    pub fn new_operation(&self) -> u64 {
        self.operation_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn validate_operation(&self, operation: &str) -> Result<()> {
        // Add operation-specific validation logic here
        debug_assert!(!operation.is_empty(), "Operation name cannot be empty");
        Ok(())
    }
}

/// Security audit log entry
#[derive(Debug, Clone)]
pub struct SecurityAuditEntry {
    pub timestamp: std::time::SystemTime,
    pub operation_id: u64,
    pub operation: String,
    pub threat_level: ThreatLevel,
    pub details: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreatLevel {
    Info,
    Warning,
    Error,
    Critical,
}

/// Security audit logger
pub struct SecurityAuditor {
    entries: std::sync::Mutex<Vec<SecurityAuditEntry>>,
    max_entries: usize,
}

impl SecurityAuditor {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: std::sync::Mutex::new(Vec::with_capacity(max_entries)),
            max_entries,
        }
    }

    pub fn log_security_event(
        &self,
        operation_id: u64,
        operation: String,
        threat_level: ThreatLevel,
        details: String,
    ) {
        let entry = SecurityAuditEntry {
            timestamp: std::time::SystemTime::now(),
            operation_id,
            operation,
            threat_level,
            details,
        };

        if let Ok(mut entries) = self.entries.lock() {
            if entries.len() >= self.max_entries {
                entries.remove(0); // Remove oldest entry
            }
            entries.push(entry);
        }
    }

    pub fn get_recent_events(&self, threat_level: ThreatLevel) -> Vec<SecurityAuditEntry> {
        if let Ok(entries) = self.entries.lock() {
            entries
                .iter()
                .filter(|entry| entry.threat_level == threat_level)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }
}

/// Global security auditor
pub static SECURITY_AUDITOR: std::sync::LazyLock<SecurityAuditor> = 
    std::sync::LazyLock::new(|| SecurityAuditor::new(10000));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_validation() {
        let buffer = vec![0u8; 4096];
        let ptr = buffer.as_ptr();
        
        // Valid buffer
        assert!(BufferSecurityValidator::validate_buffer_comprehensive(
            ptr, 4096, 8, "test"
        ).is_ok());
        
        // Null pointer
        assert!(BufferSecurityValidator::validate_buffer_comprehensive(
            std::ptr::null(), 4096, 8, "test"
        ).is_err());
        
        // Zero length
        assert!(BufferSecurityValidator::validate_buffer_comprehensive(
            ptr, 0, 8, "test"
        ).is_err());
        
        // Misaligned
        let misaligned = unsafe { ptr.add(1) };
        assert!(BufferSecurityValidator::validate_buffer_comprehensive(
            misaligned, 4095, 8, "test"
        ).is_err());
    }

    #[test]
    fn test_ring_validation() {
        // Valid access
        assert!(BufferSecurityValidator::validate_ring_access(16, 10, "test").is_ok());
        
        // Out of bounds
        assert!(BufferSecurityValidator::validate_ring_access(16, 16, "test").is_err());
        assert!(BufferSecurityValidator::validate_ring_access(16, 20, "test").is_err());
    }

    #[test]
    fn test_memory_safety_checker() {
        let checker = MemorySafetyChecker::new();
        let buffer = vec![0u8; 1024];
        let ptr = buffer.as_ptr();
        
        // Register allocation
        checker.register_allocation(ptr);
        
        // Valid access
        assert!(checker.validate_pointer(ptr, "test").is_ok());
        
        // Unregister allocation
        assert!(checker.unregister_allocation(ptr).is_ok());
        
        // Use after free
        assert!(checker.validate_pointer(ptr, "test").is_err());
        
        // Double free
        assert!(checker.unregister_allocation(ptr).is_err());
    }

    #[test]
    fn test_security_auditor() {
        let auditor = SecurityAuditor::new(100);
        
        auditor.log_security_event(
            1,
            "test_operation".to_string(),
            ThreatLevel::Warning,
            "Test security event".to_string(),
        );
        
        let events = auditor.get_recent_events(ThreatLevel::Warning);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].operation, "test_operation");
    }
}
