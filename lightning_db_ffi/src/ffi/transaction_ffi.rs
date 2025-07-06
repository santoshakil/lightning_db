//! FFI bindings for Lightning DB transaction operations

use crate::error::{clear_last_error, set_last_error};
use crate::handle_registry::HandleRegistry;
use crate::utils::{bytes_to_vec, ByteResult};
use crate::{ffi_try, ErrorCode};

lazy_static::lazy_static! {
    static ref TRANSACTION_REGISTRY: HandleRegistry<(u64, u64)> = HandleRegistry::new();
}

/// Begin a new transaction
/// 
/// # Safety
/// - The database handle must be valid
/// - Returns 0 on success with transaction handle stored in out_handle, error code on failure
#[no_mangle]
pub extern "C" fn lightning_db_begin_transaction(
    db_handle: u64,
    out_handle: *mut u64,
) -> i32 {
    if out_handle.is_null() {
        set_last_error(ErrorCode::InvalidArgument, "Null pointer provided".to_string());
        return ErrorCode::InvalidArgument as i32;
    }
    
    let db = match super::database_ffi::DATABASE_REGISTRY.get(db_handle) {
        Some(db) => db,
        None => {
            set_last_error(ErrorCode::DatabaseNotFound, "Invalid database handle".to_string());
            return ErrorCode::DatabaseNotFound as i32;
        }
    };
    
    let tx_id = ffi_try!(db.begin_transaction());
    let handle = TRANSACTION_REGISTRY.insert((db_handle, tx_id));
    
    unsafe {
        *out_handle = handle;
    }
    
    clear_last_error();
    0
}

/// Commit a transaction
/// 
/// # Safety
/// - The transaction handle must be valid
/// - The handle must not be used after calling this function
#[no_mangle]
pub extern "C" fn lightning_db_commit_transaction(tx_handle: u64) -> i32 {
    let (db_handle, tx_id) = match TRANSACTION_REGISTRY.remove(tx_handle) {
        Some(data) => *data,
        None => {
            set_last_error(ErrorCode::TransactionNotFound, "Invalid transaction handle".to_string());
            return ErrorCode::TransactionNotFound as i32;
        }
    };
    
    let db = match super::database_ffi::DATABASE_REGISTRY.get(db_handle) {
        Some(db) => db,
        None => {
            set_last_error(ErrorCode::DatabaseNotFound, "Database no longer exists".to_string());
            return ErrorCode::DatabaseNotFound as i32;
        }
    };
    
    ffi_try!(db.commit_transaction(tx_id));
    
    clear_last_error();
    0
}

/// Abort a transaction
/// 
/// # Safety
/// - The transaction handle must be valid
/// - The handle must not be used after calling this function
#[no_mangle]
pub extern "C" fn lightning_db_abort_transaction(tx_handle: u64) -> i32 {
    let (db_handle, tx_id) = match TRANSACTION_REGISTRY.remove(tx_handle) {
        Some(data) => *data,
        None => {
            set_last_error(ErrorCode::TransactionNotFound, "Invalid transaction handle".to_string());
            return ErrorCode::TransactionNotFound as i32;
        }
    };
    
    let db = match super::database_ffi::DATABASE_REGISTRY.get(db_handle) {
        Some(db) => db,
        None => {
            set_last_error(ErrorCode::DatabaseNotFound, "Database no longer exists".to_string());
            return ErrorCode::DatabaseNotFound as i32;
        }
    };
    
    ffi_try!(db.abort_transaction(tx_id));
    
    clear_last_error();
    0
}

/// Put a key-value pair within a transaction
/// 
/// # Safety
/// - key must be valid for key_len bytes
/// - value must be valid for value_len bytes
#[no_mangle]
pub extern "C" fn lightning_db_put_tx(
    tx_handle: u64,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    if key.is_null() || value.is_null() {
        set_last_error(ErrorCode::InvalidArgument, "Null pointer provided".to_string());
        return ErrorCode::InvalidArgument as i32;
    }
    
    let (db_handle, tx_id) = match TRANSACTION_REGISTRY.get(tx_handle) {
        Some(data) => *data,
        None => {
            set_last_error(ErrorCode::TransactionNotFound, "Invalid transaction handle".to_string());
            return ErrorCode::TransactionNotFound as i32;
        }
    };
    
    let db = match super::database_ffi::DATABASE_REGISTRY.get(db_handle) {
        Some(db) => db,
        None => {
            set_last_error(ErrorCode::DatabaseNotFound, "Database no longer exists".to_string());
            return ErrorCode::DatabaseNotFound as i32;
        }
    };
    
    let key_data = unsafe { bytes_to_vec(key, key_len) };
    let value_data = unsafe { bytes_to_vec(value, value_len) };
    
    ffi_try!(db.put_tx(tx_id, &key_data, &value_data));
    
    clear_last_error();
    0
}

/// Get a value within a transaction
/// 
/// # Safety
/// - key must be valid for key_len bytes
/// - The returned ByteResult must be freed using lightning_db_free_bytes
#[no_mangle]
pub extern "C" fn lightning_db_get_tx(
    tx_handle: u64,
    key: *const u8,
    key_len: usize,
) -> ByteResult {
    if key.is_null() {
        set_last_error(ErrorCode::InvalidArgument, "Null pointer provided".to_string());
        return ByteResult::error(ErrorCode::InvalidArgument as i32);
    }
    
    let (db_handle, tx_id) = match TRANSACTION_REGISTRY.get(tx_handle) {
        Some(data) => *data,
        None => {
            set_last_error(ErrorCode::TransactionNotFound, "Invalid transaction handle".to_string());
            return ByteResult::error(ErrorCode::TransactionNotFound as i32);
        }
    };
    
    let db = match super::database_ffi::DATABASE_REGISTRY.get(db_handle) {
        Some(db) => db,
        None => {
            set_last_error(ErrorCode::DatabaseNotFound, "Database no longer exists".to_string());
            return ByteResult::error(ErrorCode::DatabaseNotFound as i32);
        }
    };
    
    let key_data = unsafe { bytes_to_vec(key, key_len) };
    
    match db.get_tx(tx_id, &key_data) {
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

/// Delete a key within a transaction
/// 
/// # Safety
/// - key must be valid for key_len bytes
#[no_mangle]
pub extern "C" fn lightning_db_delete_tx(
    tx_handle: u64,
    key: *const u8,
    key_len: usize,
) -> i32 {
    if key.is_null() {
        set_last_error(ErrorCode::InvalidArgument, "Null pointer provided".to_string());
        return ErrorCode::InvalidArgument as i32;
    }
    
    let (db_handle, tx_id) = match TRANSACTION_REGISTRY.get(tx_handle) {
        Some(data) => *data,
        None => {
            set_last_error(ErrorCode::TransactionNotFound, "Invalid transaction handle".to_string());
            return ErrorCode::TransactionNotFound as i32;
        }
    };
    
    let db = match super::database_ffi::DATABASE_REGISTRY.get(db_handle) {
        Some(db) => db,
        None => {
            set_last_error(ErrorCode::DatabaseNotFound, "Database no longer exists".to_string());
            return ErrorCode::DatabaseNotFound as i32;
        }
    };
    
    let key_data = unsafe { bytes_to_vec(key, key_len) };
    
    ffi_try!(db.delete_tx(tx_id, &key_data));
    
    clear_last_error();
    0
}