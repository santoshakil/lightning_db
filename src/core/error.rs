use serde::Serialize;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Serialize)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(String),

    #[error("Database corrupted: {0}")]
    Corruption(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Invalid key size: {size} bytes (min: {min}, max: {max})")]
    InvalidKeySize { size: usize, min: usize, max: usize },

    #[error("Invalid value size: {size} bytes (max: {max})")]
    InvalidValueSize { size: usize, max: usize },

    #[error("Key not found")]
    KeyNotFound,

    #[error("Page not found: {0}")]
    PageNotFound(u32),

    #[error("Transaction {id} not found")]
    TransactionNotFound { id: u64 },

    #[error("Index error: {0}")]
    Index(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("Resource exhausted: {resource}")]
    ResourceExhausted { resource: String },

    #[error("Operation timeout: {0}")]
    Timeout(String),

    #[error("Write-write conflict")]
    WriteWriteConflict,

    #[error("Write-read conflict")]
    WriteReadConflict,

    #[error("Page overflow")]
    PageOverflow,

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Lock failed: {resource}")]
    LockFailed { resource: String },

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("WAL not available")]
    WalNotAvailable,

    #[error("Memory allocation error")]
    Memory,

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid operation: {reason}")]
    InvalidOperation { reason: String },

    #[error("Deadlock detected between transactions: {0}")]
    Deadlock(String),

    #[error("Invalid database file")]
    InvalidDatabase,

    #[error("Operation cancelled")]
    Cancelled,

    #[error("Database locked by {lock_holder}: {suggested_action}")]
    DatabaseLocked {
        lock_holder: String,
        suggested_action: String,
    },

    #[error("Checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Lock timeout: {0}")]
    LockTimeout(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: String, got: String },

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Shutdown in progress")]
    ShutdownInProgress,

    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),

    #[error("Backup error: {0}")]
    Backup(String),

    #[error("Quota exceeded: {0}")]
    QuotaExceeded(String),

    #[error("Commit failed: {0}")]
    CommitFailed(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(u64),

    #[error("Transaction {id} in invalid state: {state}")]
    TransactionInvalidState { id: u64, state: String },

    #[error("Unsupported feature: {feature}")]
    UnsupportedFeature { feature: String },

    #[error("Insufficient resources: {resource} - required: {required}, available: {available}")]
    InsufficientResources {
        resource: String,
        required: String,
        available: String,
    },

    #[error("Database not found: {path}")]
    DatabaseNotFound { path: String },

    #[error("Transaction limit reached: {limit}")]
    TransactionLimitReached { limit: usize },

    #[error("WAL corruption at offset {offset}: {reason}")]
    WalCorruption { offset: u64, reason: String },

    #[error("Generic error: {0}")]
    Generic(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => Error::DatabaseNotFound {
                path: err.to_string(),
            },
            std::io::ErrorKind::AlreadyExists => Error::AlreadyExists(err.to_string()),
            std::io::ErrorKind::PermissionDenied => Error::PermissionDenied(err.to_string()),
            std::io::ErrorKind::OutOfMemory => Error::Memory,
            std::io::ErrorKind::TimedOut => Error::Timeout(err.to_string()),
            _ => Error::Io(err.to_string()),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<snap::Error> for Error {
    fn from(err: snap::Error) -> Self {
        Error::Compression(err.to_string())
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        Error::Generic(self.to_string())
    }
}

impl Error {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Error::Timeout(_)
                | Error::LockTimeout(_)
                | Error::LockFailed { .. }
                | Error::ResourceExhausted { .. }
        )
    }

    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Error::Corruption(_)
                | Error::ChecksumMismatch { .. }
                | Error::InvalidDatabase
                | Error::WalCorruption { .. }
        )
    }

    pub fn is_conflict(&self) -> bool {
        matches!(
            self,
            Error::WriteWriteConflict | Error::WriteReadConflict | Error::Deadlock(_)
        )
    }

    pub fn requires_recovery(&self) -> bool {
        self.is_corruption() || matches!(self, Error::RecoveryFailed(_))
    }

    pub fn is_recoverable(&self) -> bool {
        !matches!(self, Error::Corruption(_) | Error::InvalidDatabase)
    }
}

// Compatibility type alias
pub type DatabaseError = Error;
pub type DatabaseResult<T> = Result<T>;