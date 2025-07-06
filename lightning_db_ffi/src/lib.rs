pub mod error;
pub mod ffi;
pub mod handle_registry;
pub mod utils;

// Re-export FFI functions
pub use ffi::{
    database_ffi::*, error_ffi::*, iterator_ffi::*, transaction_ffi::*,
};

// Re-export utils types
pub use utils::{ByteResult, lightning_db_free_bytes, lightning_db_free_string};

// Define FFI-compatible enums
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Success = 0,
    InvalidArgument = 1,
    DatabaseNotFound = 2,
    DatabaseExists = 3,
    TransactionNotFound = 4,
    TransactionConflict = 5,
    KeyNotFound = 6,
    IoError = 7,
    CorruptedData = 8,
    OutOfMemory = 9,
    Unknown = 999,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyLevel {
    Eventual = 0,
    Strong = 1,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalSyncMode {
    Sync = 0,
    Periodic = 1,
    Async = 2,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None = 0,
    Zstd = 1,
    Lz4 = 2,
    Snappy = 3,
}

#[no_mangle]
pub extern "C" fn lightning_db_init() -> i32 {
    // Initialize any global state if needed
    0
}

// Force cbindgen to include enum types by using them in dummy functions
#[no_mangle]
pub extern "C" fn _force_enum_inclusion(
    _error_code: ErrorCode,
    _consistency_level: ConsistencyLevel,
    _wal_sync_mode: WalSyncMode,
    _compression_type: CompressionType,
) {
    // Never called - just forces cbindgen to include enum definitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() {
        let result = lightning_db_init();
        assert_eq!(result, 0);
    }
}