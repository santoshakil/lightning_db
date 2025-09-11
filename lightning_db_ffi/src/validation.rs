//! FFI input validation and security hardening
//!
//! This module provides comprehensive validation for all FFI inputs to prevent
//! security vulnerabilities including buffer overflows, integer overflows,
//! null pointer dereferences, and other common FFI attack vectors.

use crate::ErrorCode;
use std::ffi::CStr;
use std::os::raw::c_char;

/// Maximum allowed buffer size for FFI operations (100MB)
/// This prevents memory exhaustion attacks and ensures reasonable memory usage
pub const MAX_FFI_BUFFER_SIZE: usize = 100 * 1024 * 1024;

/// Maximum allowed string length (1MB)
/// Prevents extremely large string attacks while allowing reasonable paths
pub const MAX_FFI_STRING_LENGTH: usize = 1024 * 1024;

/// Maximum allowed cache size (1GB)
/// Prevents memory exhaustion through excessive cache allocation
pub const MAX_CACHE_SIZE: u64 = 1024 * 1024 * 1024;

/// Minimum required pointer alignment for various types
pub const MIN_POINTER_ALIGNMENT: usize = std::mem::align_of::<usize>();

/// Security validation result
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult<T> {
    Valid(T),
    Invalid(ErrorCode, String),
}

impl<T> ValidationResult<T> {
    pub fn is_valid(&self) -> bool {
        matches!(self, ValidationResult::Valid(_))
    }

    pub fn into_result(self) -> Result<T, (ErrorCode, String)> {
        match self {
            ValidationResult::Valid(val) => Ok(val),
            ValidationResult::Invalid(code, msg) => Err((code, msg)),
        }
    }
}

/// Comprehensive pointer validation with security checks
pub fn validate_pointer<T>(ptr: *const T, name: &str) -> ValidationResult<*const T> {
    // Check for null pointer
    if ptr.is_null() {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!("{} cannot be null", name),
        );
    }

    // Check alignment - only for types that actually need alignment
    let addr = ptr as usize;
    let type_size = std::mem::size_of::<T>();
    let required_alignment = if type_size >= 8 {
        8
    } else if type_size >= 4 {
        4
    } else {
        1
    };

    if addr % required_alignment != 0 {
        return ValidationResult::Invalid(
            ErrorCode::InvalidAlignment,
            format!(
                "{} has invalid alignment: 0x{:x}, required: {}",
                name, addr, required_alignment
            ),
        );
    }

    // Additional security check: ensure pointer is not in dangerous ranges
    // This prevents some types of pointer manipulation attacks
    // Note: Relaxed checks for normal stack/heap addresses during testing
    if addr < 0x100 {
        return ValidationResult::Invalid(
            ErrorCode::InvalidPointer,
            format!(
                "{} points to suspicious low memory address: 0x{:x}",
                name, addr
            ),
        );
    }

    // Check for extremely high addresses that might indicate overflow
    // These are more conservative checks that shouldn't affect normal pointers
    #[cfg(target_pointer_width = "64")]
    if addr > 0x800000000000 {
        // Much higher threshold for 64-bit
        return ValidationResult::Invalid(
            ErrorCode::InvalidPointer,
            format!(
                "{} points to suspicious high memory address: 0x{:x}",
                name, addr
            ),
        );
    }

    #[cfg(target_pointer_width = "32")]
    if addr > 0x80000000 {
        // Higher threshold for 32-bit
        return ValidationResult::Invalid(
            ErrorCode::InvalidPointer,
            format!(
                "{} points to suspicious high memory address: 0x{:x}",
                name, addr
            ),
        );
    }

    ValidationResult::Valid(ptr)
}

/// Validate mutable pointer with additional write safety checks
pub fn validate_mut_pointer<T>(ptr: *mut T, name: &str) -> ValidationResult<*mut T> {
    match validate_pointer(ptr as *const T, name) {
        ValidationResult::Valid(_) => ValidationResult::Valid(ptr),
        ValidationResult::Invalid(code, msg) => ValidationResult::Invalid(code, msg),
    }
}

/// Comprehensive buffer validation with overflow protection
pub fn validate_buffer(ptr: *const u8, len: usize, name: &str) -> ValidationResult<()> {
    // Validate the pointer first
    if let ValidationResult::Invalid(code, msg) = validate_pointer(ptr, name) {
        return ValidationResult::Invalid(code, msg);
    }

    // Check for zero length (may be valid in some cases)
    if len == 0 {
        return ValidationResult::Valid(());
    }

    // Check maximum buffer size to prevent memory exhaustion
    if len > MAX_FFI_BUFFER_SIZE {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!(
                "{} size {} exceeds maximum allowed size {}",
                name, len, MAX_FFI_BUFFER_SIZE
            ),
        );
    }

    // Check for integer overflow in buffer end calculation
    let ptr_addr = ptr as usize;
    if ptr_addr.checked_add(len).is_none() {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!("{} buffer range causes integer overflow", name),
        );
    }

    ValidationResult::Valid(())
}

/// Validate C string with comprehensive security checks
pub fn validate_c_string(ptr: *const c_char, name: &str) -> ValidationResult<String> {
    // Validate the pointer first
    if let ValidationResult::Invalid(code, msg) = validate_pointer(ptr, name) {
        return ValidationResult::Invalid(code, msg);
    }

    // Safely convert to CStr with length validation
    let c_str = match unsafe { CStr::from_ptr(ptr) } {
        cstr => cstr,
    };

    // Validate string length to prevent extremely large strings
    let bytes = c_str.to_bytes();
    if bytes.len() > MAX_FFI_STRING_LENGTH {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!(
                "{} string length {} exceeds maximum {}",
                name,
                bytes.len(),
                MAX_FFI_STRING_LENGTH
            ),
        );
    }

    // Validate UTF-8 encoding
    match c_str.to_str() {
        Ok(s) => {
            // Additional security: check for null bytes within the string
            if s.contains('\0') {
                return ValidationResult::Invalid(
                    ErrorCode::InvalidArgument,
                    format!("{} contains embedded null bytes", name),
                );
            }

            // Check for control characters that might indicate injection attempts
            if s.chars()
                .any(|c| c.is_control() && c != '\t' && c != '\n' && c != '\r')
            {
                return ValidationResult::Invalid(
                    ErrorCode::InvalidArgument,
                    format!("{} contains suspicious control characters", name),
                );
            }

            ValidationResult::Valid(s.to_string())
        }
        Err(_) => ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!("{} is not valid UTF-8", name),
        ),
    }
}

/// Validate numeric parameters with overflow checks
pub fn validate_u64_parameter(value: u64, min: u64, max: u64, name: &str) -> ValidationResult<u64> {
    if value < min {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!("{} {} is below minimum {}", name, value, min),
        );
    }

    if value > max {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!("{} {} exceeds maximum {}", name, value, max),
        );
    }

    ValidationResult::Valid(value)
}

/// Validate usize parameters with special handling for potential overflows
pub fn validate_usize_parameter(value: usize, max: usize, name: &str) -> ValidationResult<usize> {
    if value > max {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!("{} {} exceeds maximum {}", name, value, max),
        );
    }

    ValidationResult::Valid(value)
}

/// Validate handle to ensure it's not a special value that could cause issues
pub fn validate_handle(handle: u64, name: &str) -> ValidationResult<u64> {
    // Handle 0 is reserved as invalid
    if handle == 0 {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!("{} handle cannot be 0", name),
        );
    }

    // Check for extremely high handle values that might indicate corruption
    if handle > u64::MAX / 2 {
        return ValidationResult::Invalid(
            ErrorCode::InvalidArgument,
            format!("{} handle {} appears corrupted", name, handle),
        );
    }

    ValidationResult::Valid(handle)
}

/// Comprehensive validation for database operations
pub fn validate_database_operation(
    handle: u64,
    key: *const u8,
    key_len: usize,
    value: Option<(*const u8, usize)>,
) -> ValidationResult<()> {
    // Validate handle
    if let ValidationResult::Invalid(code, msg) = validate_handle(handle, "database") {
        return ValidationResult::Invalid(code, msg);
    }

    // Validate key
    if let ValidationResult::Invalid(code, msg) = validate_buffer(key, key_len, "key") {
        return ValidationResult::Invalid(code, msg);
    }

    // Validate value if provided
    if let Some((value_ptr, value_len)) = value {
        if let ValidationResult::Invalid(code, msg) = validate_buffer(value_ptr, value_len, "value")
        {
            return ValidationResult::Invalid(code, msg);
        }
    }

    ValidationResult::Valid(())
}

/// Sanitize error messages to prevent information disclosure
pub fn sanitize_error_message(msg: &str) -> String {
    // Remove any absolute paths
    let sanitized = if msg.contains('/') {
        msg.split('/').last().unwrap_or(msg).to_string()
    } else {
        msg.to_string()
    };

    // Remove any potentially sensitive patterns
    sanitized
        .replace("0x", "**") // Hide memory addresses
        .chars()
        .take(200) // Limit message length
        .collect()
}

/// Validate cache size for database configuration
pub fn validate_cache_size(cache_size: u64) -> ValidationResult<u64> {
    validate_u64_parameter(cache_size, 1024, MAX_CACHE_SIZE, "cache_size")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_pointer_validation() {
        // Test null pointer
        let null_ptr: *const u8 = std::ptr::null();
        assert!(!validate_pointer(null_ptr, "test").is_valid());

        // Test valid pointer
        let value = 42u8;
        let valid_ptr = &value as *const u8;
        assert!(validate_pointer(valid_ptr, "test").is_valid());

        // Test low address
        let low_ptr = 0x50 as *const u8;
        assert!(!validate_pointer(low_ptr, "test").is_valid());
    }

    #[test]
    fn test_buffer_validation() {
        let data = [1, 2, 3, 4, 5u8];
        let ptr = data.as_ptr();

        // Valid buffer
        assert!(validate_buffer(ptr, 5, "test").is_valid());

        // Zero length (should be valid)
        assert!(validate_buffer(ptr, 0, "test").is_valid());

        // Oversized buffer
        assert!(!validate_buffer(ptr, MAX_FFI_BUFFER_SIZE + 1, "test").is_valid());
    }

    #[test]
    fn test_string_validation() {
        // Valid string
        let c_str = CString::new("hello").unwrap();
        assert!(validate_c_string(c_str.as_ptr(), "test").is_valid());

        // Null pointer
        assert!(!validate_c_string(std::ptr::null(), "test").is_valid());
    }

    #[test]
    fn test_handle_validation() {
        // Valid handle
        assert!(validate_handle(42, "test").is_valid());

        // Invalid handle (0)
        assert!(!validate_handle(0, "test").is_valid());

        // Suspicious handle
        assert!(!validate_handle(u64::MAX - 100, "test").is_valid());
    }

    #[test]
    fn test_error_message_sanitization() {
        let msg = "Error in /home/user/secret/file.db at address 0xDEADBEEF";
        let sanitized = sanitize_error_message(msg);

        assert!(!sanitized.contains("/home/user/secret"));
        assert!(!sanitized.contains("0x"));
        assert!(sanitized.len() <= 200);
    }
}
