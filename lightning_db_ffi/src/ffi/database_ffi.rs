//! FFI bindings for Lightning DB database operations

use crate::error::{clear_last_error, set_last_error};
use crate::handle_registry::HandleRegistry;
use crate::utils::{bytes_to_vec, c_str_to_string, ByteResult};
use crate::{ffi_try, CompressionType, ConsistencyLevel, ErrorCode, WalSyncMode};
use lightning_db::{Database, LightningDbConfig};
use std::os::raw::c_char;
use std::path::Path;

lazy_static::lazy_static! {
    pub(crate) static ref DATABASE_REGISTRY: HandleRegistry<Database> = HandleRegistry::new();
}

/// Create a new Lightning DB database
///
/// # Safety
/// - path must be a valid null-terminated UTF-8 string
/// - Returns 0 on success with handle stored in out_handle, error code on failure
#[no_mangle]
pub unsafe extern "C" fn lightning_db_create(path: *const c_char, out_handle: *mut u64) -> i32 {
    if path.is_null() || out_handle.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ErrorCode::InvalidArgument as i32;
    }

    let path_str =
        ffi_try!(unsafe { c_str_to_string(path) }.map_err(|e| { lightning_db::Error::Generic(e) }));

    let db = ffi_try!(Database::create(
        Path::new(&path_str),
        LightningDbConfig::default()
    ));
    let handle = DATABASE_REGISTRY.insert(db);

    unsafe {
        *out_handle = handle;
    }

    clear_last_error();
    0
}

/// Open an existing Lightning DB database
///
/// # Safety
/// - path must be a valid null-terminated UTF-8 string
/// - Returns 0 on success with handle stored in out_handle, error code on failure
#[no_mangle]
pub unsafe extern "C" fn lightning_db_open(path: *const c_char, out_handle: *mut u64) -> i32 {
    if path.is_null() || out_handle.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ErrorCode::InvalidArgument as i32;
    }

    let path_str =
        ffi_try!(unsafe { c_str_to_string(path) }.map_err(|e| { lightning_db::Error::Generic(e) }));

    let db = ffi_try!(Database::open(
        Path::new(&path_str),
        LightningDbConfig::default()
    ));
    let handle = DATABASE_REGISTRY.insert(db);

    unsafe {
        *out_handle = handle;
    }

    clear_last_error();
    0
}

/// Create a database with custom configuration
///
/// # Safety
/// - path must be a valid null-terminated UTF-8 string
/// - Returns 0 on success with handle stored in out_handle, error code on failure
#[no_mangle]
pub unsafe extern "C" fn lightning_db_create_with_config(
    path: *const c_char,
    cache_size: u64,
    compression_type: CompressionType,
    wal_sync_mode: WalSyncMode,
    out_handle: *mut u64,
) -> i32 {
    if path.is_null() || out_handle.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ErrorCode::InvalidArgument as i32;
    }

    let path_str =
        ffi_try!(unsafe { c_str_to_string(path) }.map_err(|e| { lightning_db::Error::Generic(e) }));

    let config = LightningDbConfig {
        cache_size,
        compression_enabled: compression_type != CompressionType::None,
        compression_type: match compression_type {
            CompressionType::None => 0,
            CompressionType::Zstd => 1,
            CompressionType::Lz4 => 2,
            CompressionType::Snappy => 3,
        },
        wal_sync_mode: match wal_sync_mode {
            WalSyncMode::Sync => lightning_db::WalSyncMode::Sync,
            WalSyncMode::Periodic => lightning_db::WalSyncMode::Periodic { interval_ms: 1000 },
            WalSyncMode::Async => lightning_db::WalSyncMode::Async,
        },
        ..Default::default()
    };

    let db = ffi_try!(Database::create(Path::new(&path_str), config));
    let handle = DATABASE_REGISTRY.insert(db);

    unsafe {
        *out_handle = handle;
    }

    clear_last_error();
    0
}

/// Close a database and free its resources
///
/// # Safety
/// - The handle must be valid
/// - The handle must not be used after calling this function
#[no_mangle]
pub extern "C" fn lightning_db_close(handle: u64) -> i32 {
    if let Some(db) = DATABASE_REGISTRY.remove(handle) {
        // Shutdown the database gracefully
        if let Err(e) = db.shutdown() {
            set_last_error(ErrorCode::Unknown, format!("Shutdown failed: {}", e));
            return ErrorCode::Unknown as i32;
        }
        clear_last_error();
        0
    } else {
        set_last_error(
            ErrorCode::DatabaseNotFound,
            "Invalid database handle".to_string(),
        );
        ErrorCode::DatabaseNotFound as i32
    }
}

/// Put a key-value pair into the database
///
/// # Safety
/// - key must be valid for key_len bytes
/// - value must be valid for value_len bytes
#[no_mangle]
pub unsafe extern "C" fn lightning_db_put(
    handle: u64,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    if key.is_null() || value.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ErrorCode::InvalidArgument as i32;
    }

    let db = match DATABASE_REGISTRY.get(handle) {
        Some(db) => db,
        None => {
            set_last_error(
                ErrorCode::DatabaseNotFound,
                "Invalid database handle".to_string(),
            );
            return ErrorCode::DatabaseNotFound as i32;
        }
    };

    let key_data = unsafe { bytes_to_vec(key, key_len) };
    let value_data = unsafe { bytes_to_vec(value, value_len) };

    ffi_try!(db.put(&key_data, &value_data));

    clear_last_error();
    0
}

/// Get a value from the database
///
/// # Safety
/// - key must be valid for key_len bytes
/// - The returned ByteResult must be freed using lightning_db_free_bytes
#[no_mangle]
pub unsafe extern "C" fn lightning_db_get(
    handle: u64,
    key: *const u8,
    key_len: usize,
) -> ByteResult {
    if key.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ByteResult::error(ErrorCode::InvalidArgument as i32);
    }

    let db = match DATABASE_REGISTRY.get(handle) {
        Some(db) => db,
        None => {
            set_last_error(
                ErrorCode::DatabaseNotFound,
                "Invalid database handle".to_string(),
            );
            return ByteResult::error(ErrorCode::DatabaseNotFound as i32);
        }
    };

    let key_data = unsafe { bytes_to_vec(key, key_len) };

    match db.get(&key_data) {
        Ok(Some(value)) => {
            clear_last_error();
            ByteResult::success(value)
        }
        Ok(None) => {
            set_last_error(ErrorCode::KeyNotFound, "Key not found".to_string());
            ByteResult::error(ErrorCode::KeyNotFound as i32)
        }
        Err(e) => {
            let code = crate::error::error_to_code(&e);
            set_last_error(code, format!("{}", e));
            ByteResult::error(code as i32)
        }
    }
}

/// Delete a key from the database
///
/// # Safety
/// - key must be valid for key_len bytes
/// - Returns 0 on success (key deleted), 1 if key not found, negative on error
#[no_mangle]
pub unsafe extern "C" fn lightning_db_delete(handle: u64, key: *const u8, key_len: usize) -> i32 {
    if key.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ErrorCode::InvalidArgument as i32;
    }

    let db = match DATABASE_REGISTRY.get(handle) {
        Some(db) => db,
        None => {
            set_last_error(
                ErrorCode::DatabaseNotFound,
                "Invalid database handle".to_string(),
            );
            return ErrorCode::DatabaseNotFound as i32;
        }
    };

    let key_data = unsafe { bytes_to_vec(key, key_len) };

    match db.delete(&key_data) {
        Ok(true) => {
            clear_last_error();
            0 // Key was deleted
        }
        Ok(false) => {
            clear_last_error();
            1 // Key not found
        }
        Err(e) => {
            let code = crate::error::error_to_code(&e);
            set_last_error(code, format!("{}", e));
            -(code as i32)
        }
    }
}

/// Sync the database to disk
///
/// # Safety
/// - The handle must be valid
#[no_mangle]
pub extern "C" fn lightning_db_sync(handle: u64) -> i32 {
    let db = match DATABASE_REGISTRY.get(handle) {
        Some(db) => db,
        None => {
            set_last_error(
                ErrorCode::DatabaseNotFound,
                "Invalid database handle".to_string(),
            );
            return ErrorCode::DatabaseNotFound as i32;
        }
    };

    ffi_try!(db.sync());

    clear_last_error();
    0
}

/// Checkpoint the database
///
/// # Safety
/// - The handle must be valid
#[no_mangle]
pub extern "C" fn lightning_db_checkpoint(handle: u64) -> i32 {
    let db = match DATABASE_REGISTRY.get(handle) {
        Some(db) => db,
        None => {
            set_last_error(
                ErrorCode::DatabaseNotFound,
                "Invalid database handle".to_string(),
            );
            return ErrorCode::DatabaseNotFound as i32;
        }
    };

    ffi_try!(db.checkpoint());

    clear_last_error();
    0
}

/// Put with consistency level
///
/// # Safety
/// - key must be valid for key_len bytes
/// - value must be valid for value_len bytes
#[no_mangle]
pub unsafe extern "C" fn lightning_db_put_with_consistency(
    handle: u64,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    consistency: ConsistencyLevel,
) -> i32 {
    if key.is_null() || value.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ErrorCode::InvalidArgument as i32;
    }

    let db = match DATABASE_REGISTRY.get(handle) {
        Some(db) => db,
        None => {
            set_last_error(
                ErrorCode::DatabaseNotFound,
                "Invalid database handle".to_string(),
            );
            return ErrorCode::DatabaseNotFound as i32;
        }
    };

    let key_data = unsafe { bytes_to_vec(key, key_len) };
    let value_data = unsafe { bytes_to_vec(value, value_len) };

    let consistency_level = match consistency {
        ConsistencyLevel::Eventual => lightning_db::ConsistencyLevel::Eventual,
        ConsistencyLevel::Strong => lightning_db::ConsistencyLevel::Strong,
    };

    ffi_try!(db.put_with_consistency(&key_data, &value_data, consistency_level));

    clear_last_error();
    0
}

/// Get with consistency level
///
/// # Safety
/// - key must be valid for key_len bytes
/// - The returned ByteResult must be freed using lightning_db_free_bytes
#[no_mangle]
pub unsafe extern "C" fn lightning_db_get_with_consistency(
    handle: u64,
    key: *const u8,
    key_len: usize,
    consistency: ConsistencyLevel,
) -> ByteResult {
    if key.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ByteResult::error(ErrorCode::InvalidArgument as i32);
    }

    let db = match DATABASE_REGISTRY.get(handle) {
        Some(db) => db,
        None => {
            set_last_error(
                ErrorCode::DatabaseNotFound,
                "Invalid database handle".to_string(),
            );
            return ByteResult::error(ErrorCode::DatabaseNotFound as i32);
        }
    };

    let key_data = unsafe { bytes_to_vec(key, key_len) };
    let consistency_level = match consistency {
        ConsistencyLevel::Eventual => lightning_db::ConsistencyLevel::Eventual,
        ConsistencyLevel::Strong => lightning_db::ConsistencyLevel::Strong,
    };

    match db.get_with_consistency(&key_data, consistency_level) {
        Ok(Some(value)) => {
            clear_last_error();
            ByteResult::success(value)
        }
        Ok(None) => {
            set_last_error(ErrorCode::KeyNotFound, "Key not found".to_string());
            ByteResult::error(ErrorCode::KeyNotFound as i32)
        }
        Err(e) => {
            let code = crate::error::error_to_code(&e);
            set_last_error(code, format!("{}", e));
            ByteResult::error(code as i32)
        }
    }
}
