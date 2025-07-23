use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(String),

    #[error("Invalid database file")]
    InvalidDatabase,

    #[error("Corrupted page")]
    CorruptedPage,

    #[error("Corrupted database: {0}")]
    CorruptedDatabase(String),

    #[error("Invalid page ID")]
    InvalidPageId,

    #[error("Page not found")]
    PageNotFound,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Memory allocation error")]
    Memory,

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Generic error: {0}")]
    Generic(String),

    // New specific error types
    #[error("Database already exists: {path}")]
    DatabaseExists { path: String },

    #[error("Database not found: {path}")]
    DatabaseNotFound { path: String },

    #[error("Lock acquisition failed: {resource}")]
    LockFailed { resource: String },

    #[error("Checksum mismatch - expected: {expected}, actual: {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Buffer overflow: tried to write {requested} bytes, but only {available} available")]
    BufferOverflow { requested: usize, available: usize },

    #[error("Invalid key size: {size} bytes (min: {min}, max: {max})")]
    InvalidKeySize { size: usize, min: usize, max: usize },

    #[error("Invalid value size: {size} bytes (max: {max})")]
    InvalidValueSize { size: usize, max: usize },

    #[error("Transaction {id} not found")]
    TransactionNotFound { id: u64 },

    #[error("Transaction {id} already {state}")]
    TransactionInvalidState { id: u64, state: String },

    #[error("Maximum transaction limit reached: {limit}")]
    TransactionLimitReached { limit: usize },

    #[error("WAL corruption at offset {offset}: {reason}")]
    WalCorruption { offset: u64, reason: String },

    #[error("Index {name} already exists")]
    IndexExists { name: String },

    #[error("Index {name} not found")]
    IndexNotFound { name: String },

    #[error("Query error: {message}")]
    QueryError { message: String },

    #[error("Concurrent modification detected for key")]
    ConcurrentModification,

    #[error("Resource exhausted: {resource}")]
    ResourceExhausted { resource: String },

    #[error("Operation cancelled")]
    Cancelled,

    #[error("Invalid operation: {reason}")]
    InvalidOperation { reason: String },

    #[error("Unsupported feature: {feature}")]
    UnsupportedFeature { feature: String },

    #[error("Page overflow: node data exceeds page size")]
    PageOverflow,

    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    #[error("Operation throttled for {0:?}")]
    Throttled(std::time::Duration),
    
    #[error("Not implemented: {0}")]
    NotImplemented(String),
    
    #[error("WAL not available")]
    WalNotAvailable,
    
    #[error("Corruption unrecoverable: {0}")]
    CorruptionUnrecoverable(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => Error::DatabaseNotFound {
                path: err.to_string(),
            },
            std::io::ErrorKind::AlreadyExists => Error::DatabaseExists {
                path: err.to_string(),
            },
            std::io::ErrorKind::PermissionDenied => {
                Error::Io(format!("Permission denied: {}", err))
            }
            std::io::ErrorKind::OutOfMemory => Error::Memory,
            std::io::ErrorKind::TimedOut => Error::Timeout(err.to_string()),
            _ => Error::Io(err.to_string()),
        }
    }
}

impl Error {
    pub fn error_code(&self) -> i32 {
        match self {
            Error::Io(_) => -1,
            Error::InvalidDatabase => -2,
            Error::CorruptedPage => -3,
            Error::CorruptedDatabase(_) => -41,
            Error::InvalidPageId => -4,
            Error::PageNotFound => -5,
            Error::KeyNotFound => -6,
            Error::Transaction(_) => -7,
            Error::Serialization(_) => -8,
            Error::InvalidFormat(_) => -33,
            Error::Compression(_) => -9,
            Error::Decompression(_) => -10,
            Error::Index(_) => -11,
            Error::Memory => -12,
            Error::Config(_) => -13,
            Error::Storage(_) => -14,
            Error::Timeout(_) => -15,
            Error::Generic(_) => -99,
            Error::DatabaseExists { .. } => -16,
            Error::DatabaseNotFound { .. } => -17,
            Error::LockFailed { .. } => -18,
            Error::ChecksumMismatch { .. } => -19,
            Error::BufferOverflow { .. } => -20,
            Error::InvalidKeySize { .. } => -21,
            Error::InvalidValueSize { .. } => -22,
            Error::TransactionNotFound { .. } => -23,
            Error::TransactionInvalidState { .. } => -24,
            Error::TransactionLimitReached { .. } => -25,
            Error::WalCorruption { .. } => -26,
            Error::IndexExists { .. } => -27,
            Error::IndexNotFound { .. } => -28,
            Error::QueryError { .. } => -29,
            Error::ConcurrentModification => -30,
            Error::ResourceExhausted { .. } => -31,
            Error::Cancelled => -32,
            Error::InvalidOperation { .. } => -33,
            Error::UnsupportedFeature { .. } => -34,
            Error::PageOverflow => -35,
            Error::ResourceLimitExceeded(_) => -36,
            Error::Throttled(_) => -37,
            Error::NotImplemented(_) => -38,
            Error::WalNotAvailable => -39,
            Error::CorruptionUnrecoverable(_) => -40,
        }
    }

    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Error::KeyNotFound
                | Error::TransactionNotFound { .. }
                | Error::IndexNotFound { .. }
                | Error::Timeout(_)
                | Error::ConcurrentModification
                | Error::Cancelled
                | Error::LockFailed { .. }
        )
    }

    /// Check if this error indicates data corruption
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Error::CorruptedPage
                | Error::CorruptedDatabase(_)
                | Error::ChecksumMismatch { .. }
                | Error::WalCorruption { .. }
                | Error::InvalidDatabase
        )
    }

    /// Convert error to FFI-safe error code and message
    pub fn to_ffi(&self) -> (i32, String) {
        (self.error_code(), self.to_string())
    }
}

/// Context trait for adding context to errors
pub trait ErrorContext<T> {
    fn context(self, msg: &str) -> Result<T>;
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String;
}

impl<T> ErrorContext<T> for Result<T> {
    fn context(self, msg: &str) -> Result<T> {
        self.map_err(|e| Error::Generic(format!("{}: {}", msg, e)))
    }

    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| Error::Generic(format!("{}: {}", f(), e)))
    }
}

// Additional error conversions for common external errors
impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Serialization(format!("UTF-8 conversion error: {}", err))
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Error::Generic(format!("Integer conversion error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(Error::KeyNotFound.error_code(), -6);
        assert_eq!(
            Error::ChecksumMismatch {
                expected: 123,
                actual: 456
            }
            .error_code(),
            -19
        );
    }

    #[test]
    fn test_is_recoverable() {
        assert!(Error::KeyNotFound.is_recoverable());
        assert!(Error::Timeout("test".to_string()).is_recoverable());
        assert!(!Error::CorruptedPage.is_recoverable());
    }

    #[test]
    fn test_is_corruption() {
        assert!(Error::CorruptedPage.is_corruption());
        assert!(Error::ChecksumMismatch {
            expected: 1,
            actual: 2
        }
        .is_corruption());
        assert!(!Error::KeyNotFound.is_corruption());
    }
}
