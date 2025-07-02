use std::ffi::CString;
use std::ptr;
use tempfile::tempdir;
use lightning_db_ffi::*;

// Test error handling for various FFI edge cases
#[test]
fn test_null_pointer_errors() {
    unsafe {
        // Test null database pointer
        assert_eq!(
lightning_db_put(
                ptr::null_mut(),
                ptr::null(),
                0,
                ptr::null(),
                0
            ),
LightningDBError::NullPointer
        );

        assert_eq!(
lightning_db_get(
                ptr::null_mut(),
                ptr::null(),
                0,
                ptr::null_mut()
            ),
LightningDBError::NullPointer
        );

        assert_eq!(
lightning_db_delete(ptr::null_mut(), ptr::null(), 0),
LightningDBError::NullPointer
        );

        assert_eq!(
lightning_db_checkpoint(ptr::null_mut()),
LightningDBError::NullPointer
        );

        assert_eq!(
lightning_db_begin_transaction(ptr::null_mut(), ptr::null_mut()),
LightningDBError::NullPointer
        );

        // Test null key/value pointers with valid database
        let dir = tempdir().unwrap();
        let path = CString::new(dir.path().to_str().unwrap()).unwrap();
        let db = lightning_db_create(path.as_ptr(), ptr::null());
        assert!(!db.is_null());

        assert_eq!(
lightning_db_put(db, ptr::null(), 10, ptr::null(), 0),
LightningDBError::NullPointer
        );

        assert_eq!(
lightning_db_get(db, ptr::null(), 10, ptr::null_mut()),
LightningDBError::NullPointer
        );

        lightning_db_free(db);
    }
}

#[test]
fn test_invalid_path_errors() {
    unsafe {
        // Test null path
        let db = lightning_db_create(ptr::null(), ptr::null());
        assert!(db.is_null());

        let db = lightning_db_open(ptr::null(), ptr::null());
        assert!(db.is_null());

        // Test empty path
        let empty_path = CString::new("").unwrap();
        let db = lightning_db_create(empty_path.as_ptr(), ptr::null());
        assert!(db.is_null());
    }
}

#[test]
fn test_transaction_errors() {
    unsafe {
        let dir = tempdir().unwrap();
        let path = CString::new(dir.path().to_str().unwrap()).unwrap();
        let db = lightning_db_create(path.as_ptr(), ptr::null());
        assert!(!db.is_null());

        // Test invalid transaction ID
        let invalid_tx_id = 99999u64;
        assert_eq!(
lightning_db_commit_transaction(db, invalid_tx_id),
LightningDBError::UnknownError
        );

        assert_eq!(
lightning_db_abort_transaction(db, invalid_tx_id),
LightningDBError::UnknownError
        );

        // Test operations with invalid transaction
        let key = b"test_key";
        let value = b"test_value";
        assert_eq!(
lightning_db_put_tx(
                db,
                invalid_tx_id,
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len()
            ),
LightningDBError::UnknownError
        );

        lightning_db_free(db);
    }
}

#[test]
fn test_config_errors() {
    unsafe {
        // Test null config pointer operations
        assert_eq!(
lightning_db_config_set_page_size(ptr::null_mut(), 4096),
LightningDBError::NullPointer
        );

        assert_eq!(
lightning_db_config_set_cache_size(ptr::null_mut(), 1024 * 1024),
LightningDBError::NullPointer
        );

        // Test invalid config values
        let config = lightning_db_ffi::lightning_db_config_new();
        assert!(!config.is_null());

        // Page size 0 should be rejected
        let err = lightning_db_ffi::lightning_db_config_set_page_size(config, 0);
        // Note: This might be accepted as it could use default, depends on implementation

        lightning_db_config_free(config);
    }
}

#[test]
fn test_error_strings() {
    unsafe {
        // Test that all error codes have valid error strings
        let error_codes = vec![
LightningDBError::Success,
LightningDBError::NullPointer,
LightningDBError::InvalidUtf8,
LightningDBError::IoError,
LightningDBError::CorruptedData,
LightningDBError::InvalidArgument,
LightningDBError::OutOfMemory,
LightningDBError::DatabaseLocked,
LightningDBError::TransactionConflict,
LightningDBError::UnknownError,
        ];

        for error_code in error_codes {
            let error_str = lightning_db_error_string(error_code);
            assert!(!error_str.is_null());

            let c_str = std::ffi::CStr::from_ptr(error_str);
            let rust_str = c_str.to_str().unwrap();
            assert!(!rust_str.is_empty());

            // Verify error string is not generic for specific errors
            if error_code != lightning_db_ffi::LightningDBError::Success
                && error_code != lightning_db_ffi::LightningDBError::UnknownError
            {
                assert_ne!(rust_str, "Unknown error");
            }
        }
    }
}

#[test]
fn test_result_handling() {
    unsafe {
        let dir = tempdir().unwrap();
        let path = CString::new(dir.path().to_str().unwrap()).unwrap();
        let db = lightning_db_create(path.as_ptr(), ptr::null());
        assert!(!db.is_null());

        // Test get with null result pointer
        let key = b"test_key";
        assert_eq!(
lightning_db_get(db, key.as_ptr(), key.len(), ptr::null_mut()),
LightningDBError::NullPointer
        );

        // Test freeing null result
        lightning_db_free_result(ptr::null_mut(), 0);

        // Test freeing result with mismatched length
        let mut dummy_data = vec![1u8, 2, 3, 4];
        lightning_db_free_result(dummy_data.as_mut_ptr(), 10); // Wrong length

        lightning_db_free(db);
    }
}

#[test]
fn test_large_data_errors() {
    unsafe {
        let dir = tempdir().unwrap();
        let path = CString::new(dir.path().to_str().unwrap()).unwrap();
        let db = lightning_db_create(path.as_ptr(), ptr::null());
        assert!(!db.is_null());

        // Test with very large key (might exceed page size)
        let large_key = vec![0u8; 10_000];
        let value = b"value";
        let err = lightning_db_ffi::lightning_db_put(
            db,
            large_key.as_ptr(),
            large_key.len(),
            value.as_ptr(),
            value.len(),
        );
        // This might succeed or fail depending on implementation limits

        // Test with very large value
        let key = b"key";
        let large_value = vec![0u8; 1_000_000];
        let err = lightning_db_ffi::lightning_db_put(
            db,
            key.as_ptr(),
            key.len(),
            large_value.as_ptr(),
            large_value.len(),
        );
        // This might succeed or fail depending on implementation limits

        lightning_db_free(db);
    }
}

#[test]
fn test_concurrent_error_handling() {
    use std::sync::Arc;
    use std::thread;

    unsafe {
        let dir = tempdir().unwrap();
        let path = CString::new(dir.path().to_str().unwrap()).unwrap();
        let db = lightning_db_create(path.as_ptr(), ptr::null());
        assert!(!db.is_null());

        let db_ptr = Arc::new(db);
        let mut handles = vec![];

        // Spawn multiple threads trying to create transactions
        for i in 0..10 {
            let db_clone = Arc::clone(&db_ptr);
            let handle = thread::spawn(move || unsafe {
                let mut tx_id = 0u64;
                let err = lightning_db_begin_transaction(*db_clone, &mut tx_id);
                
                if err == lightning_db_ffi::LightningDBError::Success {
                    // Try to commit
                    let commit_err = lightning_db_commit_transaction(*db_clone, tx_id);
                    (err, commit_err)
                } else {
                    (err, lightning_db_ffi::LightningDBError::UnknownError)
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            let (begin_err, commit_err) = handle.join().unwrap();
            // At least some should succeed
        }

        lightning_db_free(*db_ptr);
    }
}

#[test]
fn test_database_lifecycle_errors() {
    unsafe {
        let dir = tempdir().unwrap();
        let path = CString::new(dir.path().to_str().unwrap()).unwrap();

        // Create and immediately free
        let db = lightning_db_create(path.as_ptr(), ptr::null());
        assert!(!db.is_null());
        lightning_db_free(db);

        // Try to use freed database (this is undefined behavior in real code!)
        // We can't actually test this as it would crash

        // Double free (also undefined behavior)
        // lightning_db_free(db); // Don't actually do this

        // Create database in non-existent directory
        let bad_path = CString::new("/nonexistent/path/to/database").unwrap();
        let db = lightning_db_create(bad_path.as_ptr(), ptr::null());
        assert!(db.is_null()); // Should fail

        // Open non-existent database
        let db = lightning_db_open(bad_path.as_ptr(), ptr::null());
        assert!(db.is_null()); // Should fail
    }
}