use lightning_db_ffi::{
    lightning_db_clear_error, lightning_db_close, lightning_db_create, lightning_db_delete,
    lightning_db_free_bytes, lightning_db_get, lightning_db_get_last_error, lightning_db_open,
    lightning_db_put, ErrorCode,
};

use lightning_db_ffi::{
    lightning_db_begin_transaction, lightning_db_commit_transaction, lightning_db_get_tx,
    lightning_db_put_tx,
};
use std::ffi::CString;
use tempfile::TempDir;

#[test]
fn test_basic_database_operations() {
    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

    // Create database
    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_create(path.as_ptr(), &mut db_handle) };
    assert_eq!(result, 0);
    assert_ne!(db_handle, 0);

    // Put a key-value pair
    let key = b"test_key";
    let value = b"test_value";
    let result = unsafe { lightning_db_put(
        db_handle,
        key.as_ptr(),
        key.len(),
        value.as_ptr(),
        value.len(),
    ) };
    assert_eq!(result, 0);

    // Get the value back
    let result = unsafe { lightning_db_get(db_handle, key.as_ptr(), key.len()) };
    assert_eq!(result.error_code, 0);
    assert!(!result.data.is_null());
    assert_eq!(result.len, value.len());

    // Verify the value matches
    let retrieved_value = unsafe { std::slice::from_raw_parts(result.data, result.len) };
    assert_eq!(retrieved_value, value);

    // Free the result
    unsafe { lightning_db_free_bytes(result.data, result.len) };

    // Delete the key
    let result = unsafe { lightning_db_delete(db_handle, key.as_ptr(), key.len()) };
    assert_eq!(result, 0);

    // Verify it's deleted
    let result = unsafe { lightning_db_get(db_handle, key.as_ptr(), key.len()) };
    assert_eq!(result.error_code, ErrorCode::KeyNotFound as i32);

    // Close the database
    let result = unsafe { lightning_db_close(db_handle) };
    assert_eq!(result, 0);
}

#[test]
fn test_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let path = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

    // Create database
    let mut db_handle = 0u64;
    unsafe { lightning_db_create(path.as_ptr(), &mut db_handle) };

    // Begin transaction
    let mut tx_handle = 0u64;
    let result = unsafe { lightning_db_begin_transaction(db_handle, &mut tx_handle) };
    assert_eq!(result, 0);
    assert_ne!(tx_handle, 0);

    // Put within transaction
    let key = b"tx_key";
    let value = b"tx_value";
    let result = unsafe { lightning_db_put_tx(
        tx_handle,
        key.as_ptr(),
        key.len(),
        value.as_ptr(),
        value.len(),
    ) };
    assert_eq!(result, 0);

    // Get within transaction
    let result = unsafe { lightning_db_get_tx(tx_handle, key.as_ptr(), key.len()) };
    assert_eq!(result.error_code, 0);

    // Verify the value
    let retrieved_value = unsafe { std::slice::from_raw_parts(result.data, result.len) };
    assert_eq!(retrieved_value, value);
    unsafe { lightning_db_free_bytes(result.data, result.len) };

    // Commit transaction
    let result = unsafe { lightning_db_commit_transaction(tx_handle) };
    assert_eq!(result, 0);

    // Verify the value persisted
    let result = unsafe { lightning_db_get(db_handle, key.as_ptr(), key.len()) };
    assert_eq!(result.error_code, 0);
    unsafe { lightning_db_free_bytes(result.data, result.len) };

    // Close database
    unsafe { lightning_db_close(db_handle) };
}

#[test]
fn test_error_handling() {
    // Try to open non-existent database
    let path = CString::new("/non/existent/path").unwrap();
    let mut db_handle = 0u64;
    let result = unsafe { lightning_db_open(path.as_ptr(), &mut db_handle) };
    assert_ne!(result, 0);

    // Check error message
    let error_msg = unsafe { lightning_db_get_last_error() };
    assert!(!error_msg.is_null());

    // Clear error
    unsafe { lightning_db_clear_error() };
    let error_msg = unsafe { lightning_db_get_last_error() };
    assert!(error_msg.is_null());
}
