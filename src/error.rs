use serde::Serialize;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone, Serialize)]
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

    #[error("WAL sequence gap: expected {expected}, found {found} at LSN {lsn}")]
    WalSequenceGap { expected: u64, found: u64, lsn: u64 },

    #[error("WAL entry checksum mismatch at offset {offset}: expected {expected:08x}, found {found:08x}")]
    WalChecksumMismatch { offset: u64, expected: u32, found: u32 },

    #[error("WAL partial entry at offset {offset}: expected {expected_size} bytes, found {actual_size} bytes")]
    WalPartialEntry { offset: u64, expected_size: usize, actual_size: usize },

    #[error("WAL timestamp inconsistency at LSN {lsn}: timestamp {timestamp} is invalid (context: {context})")]
    WalTimestampError { lsn: u64, timestamp: u64, context: String },

    #[error("WAL binary format corruption at offset {offset}: {details}")]
    WalBinaryCorruption { offset: u64, details: String },

    #[error("WAL transaction consistency violation: {violation_type} for tx_id {tx_id}")]
    WalTransactionCorruption { tx_id: u64, violation_type: String },

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

    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("Quota exceeded: {0}")]
    QuotaExceeded(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Resource locked: {0}")]
    Locked(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    #[error("Processing error: {0}")]
    ProcessingError(String),

    #[error("Thread panic: {0}")]
    ThreadPanic(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Conversion error: {0}")]
    ConversionError(String),

    #[error("Schema validation failed: {0}")]
    SchemaValidationFailed(String),

    #[error("IO error: {0}")]
    IoError(String),

    // Raft-specific errors
    #[error("Not leader - redirect to node {0:?}")]
    NotLeader(Option<u64>),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Node not found: {0}")]
    NotFound(String),

    #[error("Election timeout")]
    ElectionTimeout,

    #[error("Log inconsistent: expected {expected}, got {actual}")]
    LogInconsistent { expected: u64, actual: u64 },

    #[error("Snapshot error: {0}")]
    Snapshot(String),

    #[error("Cluster membership error: {0}")]
    Membership(String),

    #[error("Request timeout")]
    RequestTimeout,

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Internal error: {0}")]
    Internal(String),

    // Distributed transaction errors
    #[error("Transaction failed: {0}")]
    TransactionFailed(String),

    #[error("Deadlock detected: {0}")]
    Deadlock(String),

    // === CRASH RECOVERY ERROR HIERARCHY ===
    
    /// Critical errors that prevent any recovery
    #[error("Recovery impossible: {reason}")]
    RecoveryImpossible {
        reason: String,
        suggested_action: String,
    },

    /// WAL corruption that prevents recovery
    #[error("WAL corrupted beyond repair: {details}")]
    WalCorrupted {
        details: String,
        suggested_action: String,
    },

    /// Insufficient resources for recovery
    #[error("Insufficient resources for recovery: {resource} - required: {required}, available: {available}")]
    InsufficientResources {
        resource: String,
        required: String,
        available: String,
    },

    /// Database is locked by another process
    #[error("Database locked by {lock_holder}: {suggested_action}")]
    DatabaseLocked {
        lock_holder: String,
        suggested_action: String,
    },

    /// Partial recovery failure with rollback capability
    #[error("Partial recovery failure in stage '{failed_stage}': completed {completed_stages:?}")]
    PartialRecoveryFailure {
        completed_stages: Vec<String>,
        failed_stage: String,
        cause: Box<Error>,
        rollback_available: bool,
    },

    /// Database state inconsistency detected during recovery
    #[error("Inconsistent database state detected: {description}")]
    InconsistentState {
        description: String,
        diagnostics: String,
        recovery_suggestions: Vec<String>,
    },

    /// Configuration or environment error during recovery
    #[error("Recovery configuration error: {setting} - {issue}")]
    RecoveryConfigurationError {
        setting: String,
        issue: String,
        fix: String,
    },

    /// Permission or access error during recovery
    #[error("Recovery permission error: {path} requires {required_permissions}")]
    RecoveryPermissionError {
        path: String,
        required_permissions: String,
    },

    /// Dependency error during recovery
    #[error("Recovery dependency error: {dependency} - {issue}")]
    RecoveryDependencyError {
        dependency: String,
        issue: String,
    },

    /// Recovery timeout (taking too long)
    #[error("Recovery timeout: stage '{stage}' exceeded {timeout_seconds}s")]
    RecoveryTimeout {
        stage: String,
        timeout_seconds: u64,
        progress: f64, // 0.0 to 1.0
    },

    /// Recovery progress tracking error
    #[error("Recovery progress error: {message}")]
    RecoveryProgress {
        message: String,
    },

    /// Recovery rollback failure
    #[error("Recovery rollback failed for stage '{stage}': {reason}")]
    RecoveryRollbackFailed {
        stage: String,
        reason: String,
        manual_intervention_needed: bool,
    },

    /// Recovery stage dependency failure
    #[error("Recovery stage '{stage}' depends on '{dependency}' which failed")]
    RecoveryStageDependencyFailed {
        stage: String,
        dependency: String,
        dependency_error: Box<Error>,
    },

    /// Recovery verification failure
    #[error("Recovery verification failed: {check_name} - {details}")]
    RecoveryVerificationFailed {
        check_name: String,
        details: String,
        critical: bool,
    },
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
            Error::CorruptedDatabase(_) => -3,
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
            Error::WalSequenceGap { .. } => -66,
            Error::WalChecksumMismatch { .. } => -67,
            Error::WalPartialEntry { .. } => -68,
            Error::WalTimestampError { .. } => -69,
            Error::WalBinaryCorruption { .. } => -70,
            Error::WalTransactionCorruption { .. } => -71,
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
            Error::Encryption(_) => -41,
            Error::QuotaExceeded(_) => -42,
            Error::Parse(_) => -43,
            Error::Validation(_) => -44,
            Error::Locked(_) => -45,
            Error::ParseError(_) => -46,
            Error::ValidationFailed(_) => -47,
            Error::ProcessingError(_) => -48,
            Error::ThreadPanic(_) => -49,
            Error::InvalidInput(_) => -50,
            Error::ConversionError(_) => -51,
            Error::SchemaValidationFailed(_) => -52,
            Error::IoError(_) => -53,
            Error::NotLeader(_) => -54,
            Error::Rpc(_) => -55,
            Error::NotFound(_) => -56,
            Error::ElectionTimeout => -57,
            Error::LogInconsistent { .. } => -58,
            Error::Snapshot(_) => -59,
            Error::Membership(_) => -60,
            Error::RequestTimeout => -61,
            Error::InvalidData(_) => -62,
            Error::Internal(_) => -63,
            Error::TransactionFailed(_) => -64,
            Error::Deadlock(_) => -65,
            
            // Recovery error codes (70-99 range)
            Error::RecoveryImpossible { .. } => -70,
            Error::WalCorrupted { .. } => -71,
            Error::InsufficientResources { .. } => -72,
            Error::DatabaseLocked { .. } => -73,
            Error::PartialRecoveryFailure { .. } => -74,
            Error::InconsistentState { .. } => -75,
            Error::RecoveryConfigurationError { .. } => -76,
            Error::RecoveryPermissionError { .. } => -77,
            Error::RecoveryDependencyError { .. } => -78,
            Error::RecoveryTimeout { .. } => -79,
            Error::RecoveryProgress { .. } => -80,
            Error::RecoveryRollbackFailed { .. } => -81,
            Error::RecoveryStageDependencyFailed { .. } => -82,
            Error::RecoveryVerificationFailed { .. } => -83,
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
                | Error::PartialRecoveryFailure { rollback_available: true, .. }
                | Error::RecoveryTimeout { .. }
                | Error::RecoveryProgress { .. }
        )
    }

    /// Check if this error is a critical recovery failure
    pub fn is_critical_recovery_failure(&self) -> bool {
        matches!(
            self,
            Error::RecoveryImpossible { .. }
                | Error::WalCorrupted { .. }
                | Error::InsufficientResources { .. }
                | Error::DatabaseLocked { .. }
                | Error::InconsistentState { .. }
                | Error::RecoveryRollbackFailed { .. }
                | Error::RecoveryVerificationFailed { critical: true, .. }
        )
    }

    /// Check if this error allows retry with different parameters
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::InsufficientResources { .. }
                | Error::DatabaseLocked { .. }
                | Error::RecoveryTimeout { .. }
                | Error::RecoveryDependencyError { .. }
                | Error::RecoveryPermissionError { .. }
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
                | Error::WalSequenceGap { .. }
                | Error::WalChecksumMismatch { .. }
                | Error::WalPartialEntry { .. }
                | Error::WalTimestampError { .. }
                | Error::WalBinaryCorruption { .. }
                | Error::WalTransactionCorruption { .. }
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

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::ParseError(format!("JSON error: {}", err))
    }
}

#[cfg(feature = "data-import")]
impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
        Error::ParseError(format!("CSV error: {}", err))
    }
}

impl From<regex::Error> for Error {
    fn from(err: regex::Error) -> Self {
        Error::ParseError(format!("Regex error: {}", err))
    }
}

impl From<std::time::SystemTimeError> for Error {
    fn from(err: std::time::SystemTimeError) -> Self {
        Error::Generic(format!("System time error: {}", err))
    }
}

impl From<snap::Error> for Error {
    fn from(err: snap::Error) -> Self {
        Error::Compression(format!("Snappy compression error: {}", err))
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
