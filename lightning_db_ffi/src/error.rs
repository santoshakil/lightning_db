use crate::ErrorCode;
use lightning_db::Error as LightningError;
use parking_lot::Mutex;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

lazy_static::lazy_static! {
    // Store as CString to guarantee a stable, null-terminated pointer for FFI
    static ref LAST_ERROR: Mutex<Option<CString>> = Mutex::new(None);
}

/// Convert Lightning DB error to FFI error code
pub fn error_to_code(error: &LightningError) -> ErrorCode {
    match error {
        LightningError::InvalidKeySize { .. } => ErrorCode::InvalidArgument,
        LightningError::InvalidValueSize { .. } => ErrorCode::InvalidArgument,
        LightningError::TransactionNotFound { .. } => ErrorCode::TransactionNotFound,
        LightningError::Transaction(_) => ErrorCode::TransactionConflict,
        LightningError::KeyNotFound => ErrorCode::KeyNotFound,
        LightningError::Io(_) => ErrorCode::IoError,
        LightningError::Corruption(_) => ErrorCode::CorruptedData,
        LightningError::Memory => ErrorCode::OutOfMemory,
        LightningError::InvalidDatabase => ErrorCode::DatabaseNotFound,
        LightningError::DatabaseLocked { .. } => ErrorCode::TransactionConflict,
        LightningError::ChecksumMismatch { .. } => ErrorCode::CorruptedData,
        _ => ErrorCode::Unknown,
    }
}

/// Set the last error message
pub fn set_last_error(code: ErrorCode, message: String) {
    let mut last_error = LAST_ERROR.lock();
    // Ensure no interior NULs break CString contract
    let sanitized = message.replace('\0', "");
    let formatted = format!("{:?}: {}", code, sanitized);
    match CString::new(formatted) {
        Ok(cstr) => *last_error = Some(cstr),
        Err(_) => *last_error = None, // Fallback to None on unexpected error
    }
}

/// Clear the last error
pub fn clear_last_error() {
    let mut last_error = LAST_ERROR.lock();
    *last_error = None;
}

/// Get the last error message
///
/// # Safety
/// - The returned pointer is valid until the next call to any function that sets an error
/// - The caller must not free the returned pointer
#[no_mangle]
pub extern "C" fn lightning_db_get_last_error() -> *const c_char {
    let last_error = LAST_ERROR.lock();
    match last_error.as_ref() {
        Some(err) => err.as_ptr(),
        None => ptr::null(),
    }
}

/// Get the last error message and copy it to a buffer
///
/// # Safety
/// - buffer must be valid for buffer_len bytes
/// - Returns the number of bytes written (excluding null terminator)
#[no_mangle]
pub extern "C" fn lightning_db_get_last_error_buffer(
    buffer: *mut c_char,
    buffer_len: usize,
) -> i32 {
    if buffer.is_null() || buffer_len == 0 {
        return -1;
    }

    let last_error = LAST_ERROR.lock();
    match last_error.as_ref() {
        Some(err) => {
            let bytes = err.as_bytes_with_nul();
            if bytes.len() > buffer_len {
                return -(bytes.len() as i32);
            }
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), buffer as *mut u8, bytes.len());
            }
            (bytes.len() - 1) as i32 // Exclude null terminator from count
        }
        None => 0,
    }
}

/// Clear the last error
#[no_mangle]
pub extern "C" fn lightning_db_clear_error() {
    clear_last_error();
}

// Helper macro for FFI error handling
#[macro_export]
macro_rules! ffi_try {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                $crate::error::set_last_error($crate::error::error_to_code(&e), format!("{}", e));
                return $crate::error::error_to_code(&e) as i32;
            }
        }
    };
    ($expr:expr, $default:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                $crate::error::set_last_error($crate::error::error_to_code(&e), format!("{}", e));
                return $default;
            }
        }
    };
}
