//! FFI bindings for Lightning DB iterator operations

use crate::error::{clear_last_error, set_last_error};
use crate::handle_registry::MutableHandleRegistry;
use crate::utils::bytes_to_vec;
use crate::{ffi_try, ErrorCode};
use lightning_db::RangeIterator;
use std::ptr;

lazy_static::lazy_static! {
    static ref ITERATOR_REGISTRY: MutableHandleRegistry<RangeIterator> = MutableHandleRegistry::<RangeIterator>::new();
}

/// Key-value pair result for iterator
#[repr(C)]
pub struct KeyValueResult {
    pub key: *mut u8,
    pub key_len: usize,
    pub value: *mut u8,
    pub value_len: usize,
    pub error_code: i32,
}

impl KeyValueResult {
    pub fn success(key: Vec<u8>, value: Vec<u8>) -> Self {
        let (key_ptr, key_len) = crate::utils::vec_to_bytes(key);
        let (value_ptr, value_len) = crate::utils::vec_to_bytes(value);
        Self {
            key: key_ptr,
            key_len,
            value: value_ptr,
            value_len,
            error_code: 0,
        }
    }

    pub fn error(code: i32) -> Self {
        Self {
            key: ptr::null_mut(),
            key_len: 0,
            value: ptr::null_mut(),
            value_len: 0,
            error_code: code,
        }
    }
}

/// Create an iterator for scanning a range of keys
///
/// # Safety
/// - start_key/end_key must be valid for their respective lengths (can be null for unbounded)
/// - Returns 0 on success with iterator handle stored in out_handle, error code on failure
#[no_mangle]
pub unsafe extern "C" fn lightning_db_scan(
    db_handle: u64,
    start_key: *const u8,
    start_key_len: usize,
    end_key: *const u8,
    end_key_len: usize,
    out_handle: *mut u64,
) -> i32 {
    if out_handle.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Null pointer provided".to_string(),
        );
        return ErrorCode::InvalidArgument as i32;
    }

    let db: std::sync::Arc<lightning_db::Database> = match super::database_ffi::DATABASE_REGISTRY.get(db_handle) {
        Some(db) => db,
        None => {
            set_last_error(
                ErrorCode::DatabaseNotFound,
                "Invalid database handle".to_string(),
            );
            return ErrorCode::DatabaseNotFound as i32;
        }
    };

    let start = if start_key.is_null() || start_key_len == 0 {
        None
    } else {
        Some(unsafe { bytes_to_vec(start_key, start_key_len) })
    };

    let end = if end_key.is_null() || end_key_len == 0 {
        None
    } else {
        Some(unsafe { bytes_to_vec(end_key, end_key_len) })
    };

    let iterator = ffi_try!(db.scan(start, end));
    let handle = ITERATOR_REGISTRY.insert(iterator);

    unsafe {
        *out_handle = handle;
    }

    clear_last_error();
    0
}

/// Get the next key-value pair from an iterator
///
/// # Safety
/// - The iterator handle must be valid
/// - The returned KeyValueResult must be freed using lightning_db_free_key_value
#[no_mangle]
pub extern "C" fn lightning_db_iterator_next(iter_handle: u64) -> KeyValueResult {
    match ITERATOR_REGISTRY.with_mut(iter_handle, |iter: &mut RangeIterator| {
        match iter.next() {
            Some(Ok((key, value))) => KeyValueResult::success(key, value),
            Some(Err(e)) => {
                let code = crate::error::error_to_code(&e);
                set_last_error(code, format!("{}", e));
                KeyValueResult::error(code as i32)
            }
            None => {
                // Iterator exhausted
                KeyValueResult::error(0) // 0 indicates end of iteration
            }
        }
    }) {
        Some(result) => {
            if result.error_code == 0 && result.key.is_null() {
                // Iterator exhausted, remove it
                let _: Option<RangeIterator> = ITERATOR_REGISTRY.remove(iter_handle);
            }
            clear_last_error();
            result
        }
        None => {
            set_last_error(
                ErrorCode::InvalidArgument,
                "Invalid iterator handle".to_string(),
            );
            KeyValueResult::error(ErrorCode::InvalidArgument as i32)
        }
    }
}

/// Close an iterator and free its resources
///
/// # Safety
/// - The handle must be valid
/// - The handle must not be used after calling this function
#[no_mangle]
pub extern "C" fn lightning_db_iterator_close(iter_handle: u64) -> i32 {
    let removed: Option<RangeIterator> = ITERATOR_REGISTRY.remove(iter_handle);
    if removed.is_some() {
        clear_last_error();
        0
    } else {
        set_last_error(
            ErrorCode::InvalidArgument,
            "Invalid iterator handle".to_string(),
        );
        ErrorCode::InvalidArgument as i32
    }
}

/// Free a key-value result
///
/// # Safety
/// - The result must have been returned by lightning_db_iterator_next
/// - The result must not be used after calling this function
#[no_mangle]
pub extern "C" fn lightning_db_free_key_value(result: KeyValueResult) {
    if !result.key.is_null() && result.key_len > 0 {
        unsafe {
            crate::utils::lightning_db_free_bytes(result.key, result.key_len);
        }
    }
    if !result.value.is_null() && result.value_len > 0 {
        unsafe {
            crate::utils::lightning_db_free_bytes(result.value, result.value_len);
        }
    }
}
