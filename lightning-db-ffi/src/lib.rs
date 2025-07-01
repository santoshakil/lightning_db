//! C FFI bindings for Lightning DB
//! 
//! This module provides a C-compatible API for Lightning DB.
//! 
//! # Safety
//! 
//! All functions in this module are designed to be safe to call from C.
//! They handle null pointers gracefully and use proper error codes.

use libc::{c_char, size_t};
use lightning_db::{Database, LightningDbConfig};
use std::ffi::{CStr, CString};
use std::ptr;
use std::slice;

/// Opaque type for database handle
pub struct LightningDB {
    inner: Database,
}

/// Opaque type for database configuration
pub struct LightningDBConfig {
    inner: LightningDbConfig,
}

/// Error codes for FFI
#[repr(C)]
#[derive(Debug, PartialEq)]
pub enum LightningDBError {
    Success = 0,
    NullPointer = -1,
    InvalidUtf8 = -2,
    IoError = -3,
    CorruptedData = -4,
    InvalidArgument = -5,
    OutOfMemory = -6,
    DatabaseLocked = -7,
    TransactionConflict = -8,
    UnknownError = -99,
}


/// Result type for data operations
#[repr(C)]
pub struct LightningDBResult {
    pub data: *mut u8,
    pub len: size_t,
}

/// Create a new configuration with default values
///
/// # Safety
///
/// The returned pointer must be freed with `lightning_db_config_free`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_config_new() -> *mut LightningDBConfig {
    Box::into_raw(Box::new(LightningDBConfig {
        inner: LightningDbConfig::default(),
    }))
}

/// Set the page size for the configuration
///
/// # Safety
///
/// `config` must be a valid pointer returned by `lightning_db_config_new`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_config_set_page_size(
    config: *mut LightningDBConfig,
    page_size: size_t,
) -> LightningDBError {
    if config.is_null() {
        return LightningDBError::NullPointer;
    }
    
    let config = &mut *config;
    config.inner.page_size = page_size as u64;
    LightningDBError::Success
}

/// Set the cache size for the configuration
///
/// # Safety
///
/// `config` must be a valid pointer returned by `lightning_db_config_new`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_config_set_cache_size(
    config: *mut LightningDBConfig,
    cache_size: size_t,
) -> LightningDBError {
    if config.is_null() {
        return LightningDBError::NullPointer;
    }
    
    let config = &mut *config;
    config.inner.cache_size = cache_size as u64;
    LightningDBError::Success
}

/// Free a configuration object
///
/// # Safety
///
/// `config` must be a valid pointer returned by `lightning_db_config_new`
/// After calling this function, the pointer becomes invalid
#[no_mangle]
pub unsafe extern "C" fn lightning_db_config_free(config: *mut LightningDBConfig) {
    if !config.is_null() {
        let _ = Box::from_raw(config);
    }
}

/// Open a database at the specified path
///
/// # Safety
///
/// - `path` must be a valid null-terminated C string
/// - `config` must be a valid pointer or NULL (for default config)
/// - The returned pointer must be freed with `lightning_db_free`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_open(
    path: *const c_char,
    config: *const LightningDBConfig,
) -> *mut LightningDB {
    if path.is_null() {
        return ptr::null_mut();
    }

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let config = if config.is_null() {
        LightningDbConfig::default()
    } else {
        (*config).inner.clone()
    };

    match Database::open(path_str, config) {
        Ok(db) => Box::into_raw(Box::new(LightningDB { inner: db })),
        Err(_) => ptr::null_mut(),
    }
}

/// Create a new database at the specified path
///
/// # Safety
///
/// - `path` must be a valid null-terminated C string
/// - `config` must be a valid pointer or NULL (for default config)
/// - The returned pointer must be freed with `lightning_db_free`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_create(
    path: *const c_char,
    config: *const LightningDBConfig,
) -> *mut LightningDB {
    if path.is_null() {
        return ptr::null_mut();
    }

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let config = if config.is_null() {
        LightningDbConfig::default()
    } else {
        (*config).inner.clone()
    };

    match Database::create(path_str, config) {
        Ok(db) => Box::into_raw(Box::new(LightningDB { inner: db })),
        Err(_) => ptr::null_mut(),
    }
}

/// Put a key-value pair into the database
///
/// # Safety
///
/// - `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
/// - `key` and `value` must be valid pointers to arrays of at least `key_len` and `value_len` bytes
#[no_mangle]
pub unsafe extern "C" fn lightning_db_put(
    db: *mut LightningDB,
    key: *const u8,
    key_len: size_t,
    value: *const u8,
    value_len: size_t,
) -> LightningDBError {
    if db.is_null() || key.is_null() || value.is_null() {
        return LightningDBError::NullPointer;
    }

    let db = &mut *db;
    let key_slice = slice::from_raw_parts(key, key_len);
    let value_slice = slice::from_raw_parts(value, value_len);

    match db.inner.put(key_slice, value_slice) {
        Ok(_) => LightningDBError::Success,
        Err(_) => LightningDBError::UnknownError,
    }
}

/// Get a value from the database
///
/// # Safety
///
/// - `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
/// - `key` must be a valid pointer to an array of at least `key_len` bytes
/// - `result` must be a valid pointer to store the result
/// - The returned data pointer in result must be freed with `lightning_db_free_result`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_get(
    db: *mut LightningDB,
    key: *const u8,
    key_len: size_t,
    result: *mut LightningDBResult,
) -> LightningDBError {
    if db.is_null() || key.is_null() || result.is_null() {
        return LightningDBError::NullPointer;
    }

    let db = &mut *db;
    let key_slice = slice::from_raw_parts(key, key_len);

    match db.inner.get(key_slice) {
        Ok(Some(value)) => {
            let value_len = value.len();
            let value_ptr = if value_len > 0 {
                let mut value_copy = Vec::with_capacity(value_len);
                value_copy.extend_from_slice(&value);
                let ptr = value_copy.as_mut_ptr();
                std::mem::forget(value_copy);
                ptr
            } else {
                ptr::null_mut()
            };

            (*result).data = value_ptr;
            (*result).len = value_len;
            LightningDBError::Success
        }
        Ok(None) => {
            (*result).data = ptr::null_mut();
            (*result).len = 0;
            LightningDBError::Success
        }
        Err(_) => LightningDBError::UnknownError,
    }
}

/// Delete a key from the database
///
/// # Safety
///
/// - `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
/// - `key` must be a valid pointer to an array of at least `key_len` bytes
#[no_mangle]
pub unsafe extern "C" fn lightning_db_delete(
    db: *mut LightningDB,
    key: *const u8,
    key_len: size_t,
) -> LightningDBError {
    if db.is_null() || key.is_null() {
        return LightningDBError::NullPointer;
    }

    let db = &mut *db;
    let key_slice = slice::from_raw_parts(key, key_len);

    match db.inner.delete(key_slice) {
        Ok(_) => LightningDBError::Success,
        Err(_) => LightningDBError::UnknownError,
    }
}

/// Free a result returned by get operations
///
/// # Safety
///
/// - `data` must be a valid pointer returned in a `LightningDBResult`
/// - After calling this function, the pointer becomes invalid
#[no_mangle]
pub unsafe extern "C" fn lightning_db_free_result(data: *mut u8, len: size_t) {
    if !data.is_null() && len > 0 {
        let _ = Vec::from_raw_parts(data, len, len);
    }
}

/// Checkpoint the database (flush to disk)
///
/// # Safety
///
/// `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_checkpoint(db: *mut LightningDB) -> LightningDBError {
    if db.is_null() {
        return LightningDBError::NullPointer;
    }

    let db = &mut *db;
    match db.inner.checkpoint() {
        Ok(_) => LightningDBError::Success,
        Err(_) => LightningDBError::UnknownError,
    }
}

/// Close and free a database
///
/// # Safety
///
/// - `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
/// - After calling this function, the pointer becomes invalid
#[no_mangle]
pub unsafe extern "C" fn lightning_db_free(db: *mut LightningDB) {
    if !db.is_null() {
        let db = Box::from_raw(db);
        // Ensure clean shutdown
        let _ = db.inner.shutdown();
    }
}

/// Get the last error message as a C string
///
/// # Safety
///
/// The returned string is statically allocated and should not be freed
#[no_mangle]
pub unsafe extern "C" fn lightning_db_error_string(error: LightningDBError) -> *const c_char {
    let msg = match error {
        LightningDBError::Success => "Success",
        LightningDBError::NullPointer => "Null pointer",
        LightningDBError::InvalidUtf8 => "Invalid UTF-8",
        LightningDBError::IoError => "I/O error",
        LightningDBError::CorruptedData => "Corrupted data",
        LightningDBError::InvalidArgument => "Invalid argument",
        LightningDBError::OutOfMemory => "Out of memory",
        LightningDBError::DatabaseLocked => "Database locked",
        LightningDBError::TransactionConflict => "Transaction conflict",
        LightningDBError::UnknownError => "Unknown error",
    };

    CString::new(msg).unwrap().into_raw() as *const c_char
}

// Transaction support

/// Begin a new transaction
///
/// # Safety
///
/// `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_begin_transaction(
    db: *mut LightningDB,
    tx_id: *mut u64,
) -> LightningDBError {
    if db.is_null() || tx_id.is_null() {
        return LightningDBError::NullPointer;
    }

    let db = &mut *db;
    match db.inner.begin_transaction() {
        Ok(id) => {
            *tx_id = id;
            LightningDBError::Success
        }
        Err(_) => LightningDBError::UnknownError,
    }
}

/// Put a key-value pair within a transaction
///
/// # Safety
///
/// Same as `lightning_db_put` but operates within the specified transaction
#[no_mangle]
pub unsafe extern "C" fn lightning_db_put_tx(
    db: *mut LightningDB,
    tx_id: u64,
    key: *const u8,
    key_len: size_t,
    value: *const u8,
    value_len: size_t,
) -> LightningDBError {
    if db.is_null() || key.is_null() || value.is_null() {
        return LightningDBError::NullPointer;
    }

    let db = &mut *db;
    let key_slice = slice::from_raw_parts(key, key_len);
    let value_slice = slice::from_raw_parts(value, value_len);

    match db.inner.put_tx(tx_id, key_slice, value_slice) {
        Ok(_) => LightningDBError::Success,
        Err(_) => LightningDBError::UnknownError,
    }
}

/// Commit a transaction
///
/// # Safety
///
/// `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_commit_transaction(
    db: *mut LightningDB,
    tx_id: u64,
) -> LightningDBError {
    if db.is_null() {
        return LightningDBError::NullPointer;
    }

    let db = &mut *db;
    match db.inner.commit_transaction(tx_id) {
        Ok(_) => LightningDBError::Success,
        Err(_) => LightningDBError::UnknownError,
    }
}

/// Abort a transaction
///
/// # Safety
///
/// `db` must be a valid pointer returned by `lightning_db_open` or `lightning_db_create`
#[no_mangle]
pub unsafe extern "C" fn lightning_db_abort_transaction(
    db: *mut LightningDB,
    tx_id: u64,
) -> LightningDBError {
    if db.is_null() {
        return LightningDBError::NullPointer;
    }

    let db = &mut *db;
    match db.inner.abort_transaction(tx_id) {
        Ok(_) => LightningDBError::Success,
        Err(_) => LightningDBError::UnknownError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use tempfile::tempdir;

    #[test]
    fn test_basic_operations() {
        unsafe {
            let dir = tempdir().unwrap();
            let path = CString::new(dir.path().to_str().unwrap()).unwrap();
            
            // Create database
            let db = lightning_db_create(path.as_ptr(), ptr::null());
            assert!(!db.is_null());
            
            // Put
            let key = b"test_key";
            let value = b"test_value";
            let result = lightning_db_put(db, key.as_ptr(), key.len(), value.as_ptr(), value.len());
            assert_eq!(result, LightningDBError::Success);
            
            // Get
            let mut get_result = LightningDBResult {
                data: ptr::null_mut(),
                len: 0,
            };
            let result = lightning_db_get(db, key.as_ptr(), key.len(), &mut get_result);
            assert_eq!(result, LightningDBError::Success);
            assert!(!get_result.data.is_null());
            assert_eq!(get_result.len, value.len());
            
            let retrieved = slice::from_raw_parts(get_result.data, get_result.len);
            assert_eq!(retrieved, value);
            
            lightning_db_free_result(get_result.data, get_result.len);
            
            // Delete
            let result = lightning_db_delete(db, key.as_ptr(), key.len());
            assert_eq!(result, LightningDBError::Success);
            
            // Verify deleted
            let mut get_result = LightningDBResult {
                data: ptr::null_mut(),
                len: 0,
            };
            let result = lightning_db_get(db, key.as_ptr(), key.len(), &mut get_result);
            assert_eq!(result, LightningDBError::Success);
            assert!(get_result.data.is_null());
            assert_eq!(get_result.len, 0);
            
            // Cleanup
            lightning_db_free(db);
        }
    }
}