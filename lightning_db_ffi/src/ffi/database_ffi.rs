//! FFI bindings for Lightning DB database operations

use crate::error::{clear_last_error, set_last_error};
use crate::handle_registry::HandleRegistry;
use crate::panic_guard::{ffi_guard_bytes, ffi_guard_int, ResourceGuard};
use crate::utils::{bytes_to_vec, c_str_to_string, ByteResult};
use crate::validation::{validate_cache_size, validate_database_operation, ValidationResult};
use crate::{ffi_try_safe, ConsistencyLevel, ErrorCode};
use lightning_db::{Database, LightningDbConfig};
use std::os::raw::c_char;
use std::path::Path;
use std::sync::Arc;

lazy_static::lazy_static! {
    pub(crate) static ref DATABASE_REGISTRY: HandleRegistry<Database> = HandleRegistry::<Database>::new();
}

/// Create a new Lightning DB database with comprehensive security validation
///
/// # Safety
/// - path must be a valid null-terminated UTF-8 string
/// - out_handle must be a valid pointer to u64
/// - Returns 0 on success with handle stored in out_handle, error code on failure
#[no_mangle]
pub unsafe extern "C" fn lightning_db_create(path: *const c_char, out_handle: *mut u64) -> i32 {
    ffi_guard_int("lightning_db_create", || {
        // Validate output handle pointer
        match crate::validation::validate_mut_pointer(out_handle, "out_handle") {
            ValidationResult::Invalid(code, msg) => {
                set_last_error(code, msg);
                return Err(code);
            }
            ValidationResult::Valid(_) => {}
        }

        // Validate and convert path string
        let path_str = match unsafe { c_str_to_string(path) } {
            Ok(s) => s,
            Err(e) => {
                set_last_error(ErrorCode::InvalidUtf8, format!("Invalid path: {}", e));
                return Err(ErrorCode::InvalidUtf8);
            }
        };

        // Create resource cleanup guard
        let mut cleanup_guard = ResourceGuard::new(|| {
            // Any cleanup needed if creation fails
        });

        // Create database
        let db = ffi_try_safe!(
            Database::create(Path::new(&path_str), LightningDbConfig::default()),
            "database_create"
        );

        let handle = DATABASE_REGISTRY.insert(db);

        // Safely write to output handle
        unsafe {
            *out_handle = handle;
        }

        // Success - disarm cleanup guard
        cleanup_guard.disarm();
        Ok(())
    })
}

/// Open an existing Lightning DB database with comprehensive validation
///
/// # Safety
/// - path must be a valid null-terminated UTF-8 string
/// - out_handle must be a valid pointer to u64
/// - Returns 0 on success with handle stored in out_handle, error code on failure
#[no_mangle]
pub unsafe extern "C" fn lightning_db_open(path: *const c_char, out_handle: *mut u64) -> i32 {
    ffi_guard_int("lightning_db_open", || {
        // Validate output handle pointer
        match crate::validation::validate_mut_pointer(out_handle, "out_handle") {
            ValidationResult::Invalid(code, msg) => {
                set_last_error(code, msg);
                return Err(code);
            }
            ValidationResult::Valid(_) => {}
        }

        // Validate and convert path string
        let path_str = match unsafe { c_str_to_string(path) } {
            Ok(s) => s,
            Err(e) => {
                set_last_error(ErrorCode::InvalidUtf8, format!("Invalid path: {}", e));
                return Err(ErrorCode::InvalidUtf8);
            }
        };

        let db = ffi_try_safe!(
            Database::open(Path::new(&path_str), LightningDbConfig::default()),
            "database_open"
        );

        let handle = DATABASE_REGISTRY.insert(db);

        unsafe {
            *out_handle = handle;
        }

        Ok(())
    })
}

/// Create a database with custom configuration and comprehensive validation
///
/// # Safety
/// - path must be a valid null-terminated UTF-8 string
/// - out_handle must be a valid pointer to u64
/// - All numeric parameters are now validated
/// - Returns 0 on success with handle stored in out_handle, error code on failure
#[no_mangle]
pub unsafe extern "C" fn lightning_db_create_with_config(
    path: *const c_char,
    cache_size: u64,
    compression_type: i32,
    wal_sync_mode: i32,
    out_handle: *mut u64,
) -> i32 {
    ffi_guard_int("lightning_db_create_with_config", || {
        // Validate output handle pointer
        match crate::validation::validate_mut_pointer(out_handle, "out_handle") {
            ValidationResult::Invalid(code, msg) => {
                set_last_error(code, msg);
                return Err(code);
            }
            ValidationResult::Valid(_) => {}
        }

        // Validate cache size
        match validate_cache_size(cache_size) {
            ValidationResult::Invalid(code, msg) => {
                set_last_error(code, msg);
                return Err(code);
            }
            ValidationResult::Valid(_) => {}
        }

        // Validate and convert path string
        let path_str = match unsafe { c_str_to_string(path) } {
            Ok(s) => s,
            Err(e) => {
                set_last_error(ErrorCode::InvalidUtf8, format!("Invalid path: {}", e));
                return Err(ErrorCode::InvalidUtf8);
            }
        };

        // Validate compression type
        if compression_type < 0 || compression_type > 3 {
            set_last_error(
                ErrorCode::InvalidArgument,
                format!("Invalid compression type: {}", compression_type),
            );
            return Err(ErrorCode::InvalidArgument);
        }

        // Validate WAL sync mode
        if wal_sync_mode < 0 || wal_sync_mode > 2 {
            set_last_error(
                ErrorCode::InvalidArgument,
                format!("Invalid WAL sync mode: {}", wal_sync_mode),
            );
            return Err(ErrorCode::InvalidArgument);
        }

        let config = LightningDbConfig {
            cache_size,
            compression_enabled: compression_type != 0,
            compression_type,
            wal_sync_mode: match wal_sync_mode {
                0 => lightning_db::WalSyncMode::Sync,
                1 => lightning_db::WalSyncMode::Periodic { interval_ms: 1000 },
                2 => lightning_db::WalSyncMode::Async,
                _ => unreachable!(), // Already validated above
            },
            ..Default::default()
        };

        let db = ffi_try_safe!(
            Database::create(Path::new(&path_str), config),
            "database_create_with_config"
        );
        let handle = DATABASE_REGISTRY.insert(db);

        unsafe {
            *out_handle = handle;
        }

        Ok(())
    })
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

/// Put a key-value pair into the database with comprehensive validation
///
/// # Safety
/// - key must be valid for key_len bytes
/// - value must be valid for value_len bytes
/// - All parameters are now comprehensively validated
#[no_mangle]
pub unsafe extern "C" fn lightning_db_put(
    handle: u64,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    ffi_guard_int("lightning_db_put", || {
        // Comprehensive validation of all parameters
        match validate_database_operation(handle, key, key_len, Some((value, value_len))) {
            ValidationResult::Invalid(code, msg) => {
                set_last_error(code, msg);
                return Err(code);
            }
            ValidationResult::Valid(_) => {}
        }

        let db: Arc<lightning_db::Database> = match DATABASE_REGISTRY.get(handle) {
            Some(db) => db,
            None => {
                set_last_error(
                    ErrorCode::DatabaseNotFound,
                    "Invalid database handle".to_string(),
                );
                return Err(ErrorCode::DatabaseNotFound);
            }
        };

        // Convert buffers with validation
        let key_data = unsafe { bytes_to_vec(key, key_len) };
        let value_data = unsafe { bytes_to_vec(value, value_len) };

        // Validate that conversion was successful (empty vectors indicate validation failure)
        if key_data.is_empty() && key_len > 0 {
            set_last_error(
                ErrorCode::BufferOverflow,
                "Key buffer validation failed".to_string(),
            );
            return Err(ErrorCode::BufferOverflow);
        }

        if value_data.is_empty() && value_len > 0 {
            set_last_error(
                ErrorCode::BufferOverflow,
                "Value buffer validation failed".to_string(),
            );
            return Err(ErrorCode::BufferOverflow);
        }

        ffi_try_safe!(db.put(&key_data, &value_data), "database_put");
        Ok(())
    })
}

/// Get a value from the database with comprehensive validation
///
/// # Safety
/// - key must be valid for key_len bytes
/// - The returned ByteResult must be freed using lightning_db_free_bytes
/// - All parameters are now comprehensively validated
#[no_mangle]
pub unsafe extern "C" fn lightning_db_get(
    handle: u64,
    key: *const u8,
    key_len: usize,
) -> ByteResult {
    ffi_guard_bytes("lightning_db_get", || {
        // Comprehensive validation of parameters
        match validate_database_operation(handle, key, key_len, None) {
            ValidationResult::Invalid(code, msg) => {
                set_last_error(code, msg);
                return Err(code);
            }
            ValidationResult::Valid(_) => {}
        }

        let db: Arc<lightning_db::Database> = match DATABASE_REGISTRY.get(handle) {
            Some(db) => db,
            None => {
                set_last_error(
                    ErrorCode::DatabaseNotFound,
                    "Invalid database handle".to_string(),
                );
                return Err(ErrorCode::DatabaseNotFound);
            }
        };

        // Convert key with validation
        let key_data = unsafe { bytes_to_vec(key, key_len) };

        // Validate that conversion was successful
        if key_data.is_empty() && key_len > 0 {
            set_last_error(
                ErrorCode::BufferOverflow,
                "Key buffer validation failed".to_string(),
            );
            return Err(ErrorCode::BufferOverflow);
        }

        match db.get(&key_data) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => {
                set_last_error(ErrorCode::KeyNotFound, "Key not found".to_string());
                Err(ErrorCode::KeyNotFound)
            }
            Err(e) => {
                let code = crate::error::error_to_code(&e);
                let sanitized_msg = crate::validation::sanitize_error_message(&format!("{}", e));
                set_last_error(code, sanitized_msg);
                Err(code)
            }
        }
    })
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

    let db: Arc<lightning_db::Database> = match DATABASE_REGISTRY.get(handle) {
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

/// Sync the database to disk with validation
///
/// # Safety
/// - The handle must be valid
#[no_mangle]
pub extern "C" fn lightning_db_sync(handle: u64) -> i32 {
    ffi_guard_int("lightning_db_sync", || {
        let db: Arc<lightning_db::Database> = match DATABASE_REGISTRY.get(handle) {
            Some(db) => db,
            None => {
                set_last_error(
                    ErrorCode::DatabaseNotFound,
                    "Invalid database handle".to_string(),
                );
                return Err(ErrorCode::DatabaseNotFound);
            }
        };

        ffi_try_safe!(db.sync(), "database_sync");
        Ok(())
    })
}

/// Checkpoint the database with validation
///
/// # Safety
/// - The handle must be valid
#[no_mangle]
pub extern "C" fn lightning_db_checkpoint(handle: u64) -> i32 {
    ffi_guard_int("lightning_db_checkpoint", || {
        let db: Arc<lightning_db::Database> = match DATABASE_REGISTRY.get(handle) {
            Some(db) => db,
            None => {
                set_last_error(
                    ErrorCode::DatabaseNotFound,
                    "Invalid database handle".to_string(),
                );
                return Err(ErrorCode::DatabaseNotFound);
            }
        };

        ffi_try_safe!(db.checkpoint(), "database_checkpoint");
        Ok(())
    })
}

/// Put with consistency level and comprehensive validation
///
/// # Safety
/// - key must be valid for key_len bytes
/// - value must be valid for value_len bytes
/// - All parameters are now comprehensively validated
#[no_mangle]
pub unsafe extern "C" fn lightning_db_put_with_consistency(
    handle: u64,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    consistency: ConsistencyLevel,
) -> i32 {
    ffi_guard_int("lightning_db_put_with_consistency", || {
        // Comprehensive validation of all parameters
        match validate_database_operation(handle, key, key_len, Some((value, value_len))) {
            ValidationResult::Invalid(code, msg) => {
                set_last_error(code, msg);
                return Err(code);
            }
            ValidationResult::Valid(_) => {}
        }

        let db: Arc<lightning_db::Database> = match DATABASE_REGISTRY.get(handle) {
            Some(db) => db,
            None => {
                set_last_error(
                    ErrorCode::DatabaseNotFound,
                    "Invalid database handle".to_string(),
                );
                return Err(ErrorCode::DatabaseNotFound);
            }
        };

        // Convert buffers with validation
        let key_data = unsafe { bytes_to_vec(key, key_len) };
        let value_data = unsafe { bytes_to_vec(value, value_len) };

        // Validate that conversion was successful
        if key_data.is_empty() && key_len > 0 {
            set_last_error(
                ErrorCode::BufferOverflow,
                "Key buffer validation failed".to_string(),
            );
            return Err(ErrorCode::BufferOverflow);
        }

        if value_data.is_empty() && value_len > 0 {
            set_last_error(
                ErrorCode::BufferOverflow,
                "Value buffer validation failed".to_string(),
            );
            return Err(ErrorCode::BufferOverflow);
        }

        let consistency_level = match consistency {
            ConsistencyLevel::Eventual => lightning_db::ConsistencyLevel::Eventual,
            ConsistencyLevel::Strong => lightning_db::ConsistencyLevel::Strong,
        };

        ffi_try_safe!(
            db.put_with_consistency(&key_data, &value_data, consistency_level),
            "database_put_consistency"
        );
        Ok(())
    })
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

    let db: Arc<lightning_db::Database> = match DATABASE_REGISTRY.get(handle) {
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
