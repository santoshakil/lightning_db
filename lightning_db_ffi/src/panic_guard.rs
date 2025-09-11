//! Panic prevention and boundary protection for FFI
//!
//! This module provides comprehensive panic prevention across FFI boundaries
//! to ensure that Rust panics never propagate to foreign code, which would
//! cause undefined behavior.

use crate::error::{clear_last_error, set_last_error};
use crate::ErrorCode;
use std::panic;
use std::panic::AssertUnwindSafe;

/// Execute a closure with panic protection
///
/// This function catches any panics that occur during execution and converts
/// them into proper error codes, preventing undefined behavior at the FFI boundary.
pub fn ffi_guard<F, R>(operation_name: &str, f: F) -> Result<R, ErrorCode>
where
    F: FnOnce() -> Result<R, ErrorCode> + std::panic::UnwindSafe,
{
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(panic_payload) => {
            // Convert panic to error
            let panic_msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                format!("Panic in {}: {}", operation_name, s)
            } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                format!("Panic in {}: {}", operation_name, s)
            } else {
                format!("Unknown panic in {}", operation_name)
            };

            // Sanitize the panic message to prevent information disclosure
            let sanitized_msg = crate::validation::sanitize_error_message(&panic_msg);

            set_last_error(ErrorCode::PanicPrevented, sanitized_msg);
            Err(ErrorCode::PanicPrevented)
        }
    }
}

/// Execute a closure with panic protection, returning an integer error code
pub fn ffi_guard_int<F>(operation_name: &str, f: F) -> i32
where
    F: FnOnce() -> Result<(), ErrorCode> + std::panic::UnwindSafe,
{
    match ffi_guard(operation_name, f) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(error_code) => error_code as i32,
    }
}

/// Execute a closure with panic protection, returning a handle
pub fn ffi_guard_handle<F>(operation_name: &str, f: F) -> Result<u64, ErrorCode>
where
    F: FnOnce() -> Result<u64, ErrorCode> + std::panic::UnwindSafe,
{
    ffi_guard(operation_name, f)
}

/// Execute a closure with panic protection, returning a ByteResult
pub fn ffi_guard_bytes<F>(operation_name: &str, f: F) -> crate::utils::ByteResult
where
    F: FnOnce() -> Result<Vec<u8>, ErrorCode> + std::panic::UnwindSafe,
{
    match ffi_guard(operation_name, f) {
        Ok(data) => {
            clear_last_error();
            crate::utils::ByteResult::success(data)
        }
        Err(error_code) => crate::utils::ByteResult::error(error_code as i32),
    }
}

/// Enhanced version of ffi_try macro with panic protection
#[macro_export]
macro_rules! ffi_try_safe {
    ($expr:expr, $op_name:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                let error_code = $crate::error::error_to_code(&e);
                let sanitized_msg = $crate::validation::sanitize_error_message(&format!("{}", e));
                $crate::error::set_last_error(error_code, sanitized_msg);
                return Err(error_code);
            }
        }
    };
    ($expr:expr, $op_name:expr, $default:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                let error_code = $crate::error::error_to_code(&e);
                let sanitized_msg = $crate::validation::sanitize_error_message(&format!("{}", e));
                $crate::error::set_last_error(error_code, sanitized_msg);
                return $default;
            }
        }
    };
}

/// Resource cleanup guard that ensures cleanup even on panic
pub struct ResourceGuard<F>
where
    F: FnOnce(),
{
    cleanup: Option<F>,
}

impl<F> ResourceGuard<F>
where
    F: FnOnce(),
{
    pub fn new(cleanup: F) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    /// Disarm the guard (cleanup won't run)
    pub fn disarm(&mut self) {
        self.cleanup.take();
    }
}

impl<F> Drop for ResourceGuard<F>
where
    F: FnOnce(),
{
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            // Even cleanup can panic, so we need to catch that too
            let _ = panic::catch_unwind(AssertUnwindSafe(cleanup));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_panic_guard_success() {
        let result = ffi_guard("test", || Ok(42));
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_panic_guard_error() {
        let result: Result<i32, ErrorCode> = ffi_guard("test", || Err(ErrorCode::InvalidArgument));
        assert_eq!(result, Err(ErrorCode::InvalidArgument));
    }

    #[test]
    fn test_panic_guard_panic() {
        let result = ffi_guard("test", || -> Result<i32, ErrorCode> {
            panic!("Test panic");
        });
        assert_eq!(result, Err(ErrorCode::PanicPrevented));
    }

    #[test]
    fn test_resource_guard() {
        use std::sync::{Arc, Mutex};

        let cleanup_called = Arc::new(Mutex::new(false));
        let cleanup_called_clone = cleanup_called.clone();

        {
            let _guard = ResourceGuard::new(|| {
                *cleanup_called_clone.lock().unwrap() = true;
            });
        } // guard drops here

        assert!(*cleanup_called.lock().unwrap());
    }

    #[test]
    fn test_resource_guard_disarm() {
        use std::sync::{Arc, Mutex};

        let cleanup_called = Arc::new(Mutex::new(false));
        let cleanup_called_clone = cleanup_called.clone();

        {
            let mut guard = ResourceGuard::new(|| {
                *cleanup_called_clone.lock().unwrap() = true;
            });
            guard.disarm();
        } // guard drops here but cleanup is disarmed

        assert!(!*cleanup_called.lock().unwrap());
    }
}
