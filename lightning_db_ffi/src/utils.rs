use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::slice;

/// Convert a C string to a Rust String
///
/// # Safety
/// - The pointer must be a valid null-terminated UTF-8 string
pub unsafe fn c_str_to_string(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Err("Null pointer".to_string());
    }

    match CStr::from_ptr(ptr).to_str() {
        Ok(s) => Ok(s.to_string()),
        Err(e) => Err(format!("Invalid UTF-8: {}", e)),
    }
}

/// Convert a Rust string to a C string
///
/// The caller must free the returned pointer using `lightning_db_free_string`
pub fn string_to_c_str(s: &str) -> *mut c_char {
    match CString::new(s) {
        Ok(c_str) => c_str.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Free a string allocated by this library
///
/// # Safety
/// - The pointer must have been allocated by this library
/// - The pointer must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lightning_db_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = CString::from_raw(ptr);
    }
}

/// Convert a byte slice to a Rust Vec
///
/// Maximum allowed buffer size for FFI operations (100MB)
const MAX_FFI_BUFFER_SIZE: usize = 100 * 1024 * 1024;

/// # Safety
/// - The pointer must be valid for `len` bytes
/// - `len` must not exceed MAX_FFI_BUFFER_SIZE
pub unsafe fn bytes_to_vec(ptr: *const u8, len: usize) -> Vec<u8> {
    if ptr.is_null() || len == 0 {
        return Vec::new();
    }
    
    // Prevent potential buffer overflow attacks
    if len > MAX_FFI_BUFFER_SIZE {
        eprintln!("FFI buffer size {} exceeds maximum {}", len, MAX_FFI_BUFFER_SIZE);
        return Vec::new();
    }

    slice::from_raw_parts(ptr, len).to_vec()
}

/// Allocate memory for bytes
///
/// The caller must free the returned pointer using `lightning_db_free_bytes`
pub fn vec_to_bytes(data: Vec<u8>) -> (*mut u8, usize) {
    let len = data.len();
    let mut data = data;
    let ptr = data.as_mut_ptr();
    std::mem::forget(data);
    (ptr, len)
}

/// Free bytes allocated by this library
///
/// # Safety
/// - The pointer must have been allocated by this library with the given length
/// - The pointer must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lightning_db_free_bytes(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        let _ = Vec::from_raw_parts(ptr, len, len);
    }
}

/// Result structure for returning data across FFI boundary
#[repr(C)]
pub struct ByteResult {
    pub data: *mut u8,
    pub len: usize,
    pub error_code: i32,
}

impl ByteResult {
    pub fn success(data: Vec<u8>) -> Self {
        let (ptr, len) = vec_to_bytes(data);
        Self {
            data: ptr,
            len,
            error_code: 0,
        }
    }

    pub fn error(code: i32) -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
            error_code: code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_conversion() {
        let rust_str = "Hello, Lightning DB!";
        let c_str = string_to_c_str(rust_str);

        assert!(!c_str.is_null());

        unsafe {
            let converted = c_str_to_string(c_str).unwrap();
            assert_eq!(converted, rust_str);

            lightning_db_free_string(c_str);
        }
    }

    #[test]
    fn test_bytes_conversion() {
        let data = vec![1, 2, 3, 4, 5];
        let (ptr, len) = vec_to_bytes(data.clone());

        assert!(!ptr.is_null());
        assert_eq!(len, 5);

        unsafe {
            let converted = bytes_to_vec(ptr, len);
            assert_eq!(converted, data);

            lightning_db_free_bytes(ptr, len);
        }
    }
}
