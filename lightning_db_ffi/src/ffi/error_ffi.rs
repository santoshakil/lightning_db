//! FFI bindings for error handling
//!
//! Re-exports error handling functions from the error module

pub use crate::error::{
    lightning_db_clear_error, lightning_db_get_last_error, lightning_db_get_last_error_buffer,
};
