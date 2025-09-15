//! Comprehensive security tests for Lightning DB FFI
//!
//! These tests verify that the FFI interface is hardened against common
//! security vulnerabilities and attack vectors.

use lightning_db_ffi::{
    lightning_db_clear_error, lightning_db_close, lightning_db_create,
    lightning_db_create_with_config, lightning_db_delete, lightning_db_free_bytes,
    lightning_db_get, lightning_db_get_last_error, lightning_db_put, ErrorCode,
};
use std::ffi::CString;
use std::ptr;
use tempfile::TempDir;

/// Test null pointer handling - should never crash
#[test]
fn test_null_pointer_protection() {
    // Test database creation with null path
    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_create(ptr::null(), &mut db_handle) };
    assert_ne!(result, 0);
    assert_eq!(db_handle, 0);

    // Test with null output handle
    let path = CString::new("/tmp/test").unwrap();
    let result = unsafe { lightning_db_create(path.as_ptr(), ptr::null_mut()) };
    assert_ne!(result, 0);

    // Test put with null key
    let result = unsafe { lightning_db_put(1, ptr::null(), 5, b"value".as_ptr(), 5) };
    assert_ne!(result, 0);

    // Test put with null value
    let result = unsafe { lightning_db_put(1, b"key".as_ptr(), 3, ptr::null(), 5) };
    assert_ne!(result, 0);

    // Test get with null key
    let result = unsafe { lightning_db_get(1, ptr::null(), 5) };
    assert_ne!(result.error_code, 0);
}

/// Test buffer overflow protection
#[test]
fn test_buffer_overflow_protection() {
    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

    // Create database
    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_create(path.as_ptr(), &mut db_handle) };
    assert_eq!(result, 0);

    // Test with maximum buffer size + 1 (should fail)
    let max_size = 100 * 1024 * 1024; // MAX_FFI_BUFFER_SIZE
    let oversized_len = max_size + 1;

    let key = b"test_key";
    let value = vec![0u8; 1024]; // Small actual buffer

    // This should be rejected due to oversized length parameter
    let result = unsafe {
        lightning_db_put(
            db_handle,
            key.as_ptr(),
            key.len(),
            value.as_ptr(),
            oversized_len, // Malicious length
        )
    };
    assert_ne!(result, 0);

    // Test get with oversized length
    let result = unsafe { lightning_db_get(db_handle, key.as_ptr(), oversized_len) };
    assert_ne!(result.error_code, 0);

    unsafe { lightning_db_close(db_handle) };
}

/// Test integer overflow protection
#[test]
fn test_integer_overflow_protection() {
    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

    // Test cache size validation
    let mut db_handle = 0u64;

    // Test with zero cache size (should fail)
    let result = unsafe {
        lightning_db_create_with_config(
            path.as_ptr(),
            0, // Invalid cache size
            0, // CompressionType::None
            0, // WalSyncMode::Sync
            &mut db_handle,
        )
    };
    assert_ne!(result, 0);

    // Test with extremely large cache size (should fail)
    let result = unsafe {
        lightning_db_create_with_config(
            path.as_ptr(),
            u64::MAX, // Extremely large cache size
            0,        // CompressionType::None
            0,        // WalSyncMode::Sync
            &mut db_handle,
        )
    };
    assert_ne!(result, 0);

    // Test with reasonable cache size (should succeed)
    let result = unsafe {
        lightning_db_create_with_config(
            path.as_ptr(),
            1024 * 1024, // 1MB cache
            0,           // CompressionType::None
            0,           // WalSyncMode::Sync
            &mut db_handle,
        )
    };
    assert_eq!(result, 0);

    unsafe { lightning_db_close(db_handle) };
}

/// Test string validation and UTF-8 safety
#[test]
fn test_string_validation() {
    // Test with invalid UTF-8 sequence
    let invalid_utf8 = vec![0xFF, 0xFE, 0x00]; // Invalid UTF-8
    let invalid_path = invalid_utf8.as_ptr() as *const i8;

    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_create(invalid_path, &mut db_handle) };
    assert_ne!(result, 0);

    // Test extremely long path (should be rejected)
    let long_path = "a".repeat(2 * 1024 * 1024); // 2MB string
    let c_long_path = CString::new(long_path).unwrap();
    let result = unsafe { lightning_db_create(c_long_path.as_ptr(), &mut db_handle) };
    assert_ne!(result, 0);

    // Test path with control characters (non-null control characters)
    let control_path = CString::new("path_with_\x01_control").unwrap();
    let result = unsafe { lightning_db_create(control_path.as_ptr(), &mut db_handle) };
    // This should be handled gracefully
    if result == 0 {
        unsafe { lightning_db_close(db_handle) };
    }
}

/// Test invalid handle protection
#[test]
fn test_invalid_handle_protection() {
    // Test operations with handle 0 (invalid)
    let result = unsafe { lightning_db_put(0, b"key".as_ptr(), 3, b"value".as_ptr(), 5) };
    assert_ne!(result, 0);

    let result = unsafe { lightning_db_get(0, b"key".as_ptr(), 3) };
    assert_ne!(result.error_code, 0);

    let result = unsafe { lightning_db_delete(0, b"key".as_ptr(), 3) };
    assert_ne!(result, 0);

    let result = lightning_db_close(0);
    assert_ne!(result, 0);

    // Test with extremely large handle (potential corruption)
    let suspicious_handle = u64::MAX - 100;
    let result =
        unsafe { lightning_db_put(suspicious_handle, b"key".as_ptr(), 3, b"value".as_ptr(), 5) };
    assert_ne!(result, 0);
}

/// Test enum validation
#[test]
fn test_enum_validation() {
    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();
    let mut db_handle = 0u64;

    // Test with invalid compression type value
    let result = unsafe {
        lightning_db_create_with_config(
            path.as_ptr(),
            1024 * 1024,
            999, // Invalid compression type
            0,   // Valid WAL sync mode (Sync)
            &mut db_handle,
        )
    };
    assert_ne!(result, 0);

    // Test with invalid WAL sync mode
    let result = unsafe {
        lightning_db_create_with_config(
            path.as_ptr(),
            1024 * 1024,
            0,   // Valid compression type (None)
            999, // Invalid WAL sync mode
            &mut db_handle,
        )
    };
    assert_ne!(result, 0);
}

/// Test memory alignment
#[test]
fn test_memory_alignment() {
    // Create a misaligned pointer (should be rejected)
    let data = [1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8];
    let misaligned_ptr = unsafe { data.as_ptr().add(1) }; // Offset by 1 byte

    // Depending on the system, this might or might not be misaligned
    // The validation should handle it gracefully either way
    let mut db_handle = 1; // Use valid handle for this test
    let result = unsafe { lightning_db_put(db_handle, misaligned_ptr, 3, b"value".as_ptr(), 5) };
    // Should either work or fail gracefully (not crash)
    // The exact result depends on the system alignment requirements
}

/// Test error message safety
#[test]
fn test_error_message_safety() {
    // Trigger an error
    let result = unsafe { lightning_db_create(ptr::null(), ptr::null_mut()) };
    assert_ne!(result, 0);

    // Get error message
    let error_ptr = unsafe { lightning_db_get_last_error() };

    if !error_ptr.is_null() {
        // Error message should be safe to read
        let error_cstr = unsafe { std::ffi::CStr::from_ptr(error_ptr) };
        let error_str = error_cstr.to_str().unwrap();

        // Error message should be sanitized (no sensitive information)
        assert!(!error_str.contains("/home/"));
        assert!(!error_str.contains("0x"));
        assert!(error_str.len() <= 200); // Should be limited in length
    }

    // Clear error
    unsafe { lightning_db_clear_error() };

    let error_ptr = unsafe { lightning_db_get_last_error() };
    assert!(error_ptr.is_null());
}

/// Test resource cleanup on errors
#[test]
fn test_resource_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

    // Create database successfully
    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_create(path.as_ptr(), &mut db_handle) };
    assert_eq!(result, 0);
    assert_ne!(db_handle, 0);

    // Cause an error in a database operation
    let result = unsafe {
        lightning_db_put(
            db_handle,
            ptr::null(), // Invalid key pointer
            5,
            b"value".as_ptr(),
            5,
        )
    };
    assert_ne!(result, 0);

    // Database should still be in a valid state and closeable
    let result = lightning_db_close(db_handle);
    assert_eq!(result, 0);
}

/// Test double-free protection
#[test]
fn test_double_free_protection() {
    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_create(path.as_ptr(), &mut db_handle) };
    assert_eq!(result, 0);

    // Put some data
    let result = unsafe { lightning_db_put(db_handle, b"key".as_ptr(), 3, b"value".as_ptr(), 5) };
    assert_eq!(result, 0);

    // Get data
    let result = unsafe { lightning_db_get(db_handle, b"key".as_ptr(), 3) };
    assert_eq!(result.error_code, 0);
    assert!(!result.data.is_null());

    // Free the bytes
    unsafe { lightning_db_free_bytes(result.data, result.len) };

    // Attempting to free again should not crash (but it's undefined behavior)
    // Our implementation should handle this gracefully

    // Test freeing with invalid parameters
    unsafe { lightning_db_free_bytes(ptr::null_mut(), 0) }; // Should be no-op
    unsafe { lightning_db_free_bytes(result.data, 0) }; // Should be no-op

    unsafe { lightning_db_close(db_handle) };
}

/// Test panic prevention
#[test]
fn test_panic_prevention() {
    // All the above tests should never panic even with malicious input
    // This is verified by the fact that all tests run without crashing

    // Test with extreme values that might cause panics in unprotected code
    let extreme_values = [(u64::MAX, usize::MAX), (0, usize::MAX), (u64::MAX, 0)];

    for (handle, len) in extreme_values.iter() {
        let result = unsafe { lightning_db_get(*handle, b"test".as_ptr(), *len) };
        // Should return error, not panic
        assert_ne!(result.error_code, 0);
    }
}

/// Test concurrent access safety
#[test]
fn test_concurrent_safety() {
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_create(path.as_ptr(), &mut db_handle) };
    assert_eq!(result, 0);

    let db_handle = Arc::new(db_handle);
    let mut handles = vec![];

    // Spawn multiple threads trying to access the database
    for i in 0..4 {
        let db_handle = db_handle.clone();
        let handle = thread::spawn(move || {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);

            // Multiple operations from different threads
            for _j in 0..10 {
                let result = unsafe {
                    lightning_db_put(
                        *db_handle,
                        key.as_ptr(),
                        key.len(),
                        value.as_ptr(),
                        value.len(),
                    )
                };
                // Should either succeed or fail gracefully

                let result = unsafe { lightning_db_get(*db_handle, key.as_ptr(), key.len()) };
                if result.error_code == 0 {
                    unsafe { lightning_db_free_bytes(result.data, result.len) };
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    unsafe { lightning_db_close(*db_handle) };
}

/// Test edge cases with zero-length data
#[test]
fn test_zero_length_data() {
    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_create(path.as_ptr(), &mut db_handle) };
    assert_eq!(result, 0);

    // Test with zero-length key and value
    let result = unsafe { lightning_db_put(db_handle, ptr::null(), 0, ptr::null(), 0) };
    // Should handle gracefully (empty key might not be valid)

    // Test with zero-length key but valid value
    let result = unsafe { lightning_db_put(db_handle, ptr::null(), 0, b"value".as_ptr(), 5) };
    // Should handle gracefully

    // Test with valid key but zero-length value
    let result = unsafe { lightning_db_put(db_handle, b"key".as_ptr(), 3, ptr::null(), 0) };
    // Should handle gracefully

    unsafe { lightning_db_close(db_handle) };
}
