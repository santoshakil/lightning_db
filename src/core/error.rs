use serde::Serialize;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone, Serialize)]
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

    #[error("Generic error: {0}")]
    Generic(String),

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

    #[error("Buffer overflow")]
    BufferOverflow,

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Integrity violation: {0}")]
    IntegrityViolation(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Thread panic: {0}")]
    ThreadPanic(String),

    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    #[error("Version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: String, got: String },

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Shutdown in progress")]
    ShutdownInProgress,

    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),

    #[error("Backup error: {0}")]
    BackupError(String),

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Throttled: {0}")]
    Throttled(String),

    #[error("Quota exceeded: {0}")]
    QuotaExceeded(String),

    #[error("Backup failed: {0}")]
    Backup(String),

    #[error("Commit failed: {0}")]
    CommitFailed(String),

    #[error("Compression error: {0}")]
    CompressionError(String),

    #[error("Connection pool exhausted")]
    ConnectionPoolExhausted,

    #[error("Database corrupted: {0}")]
    CorruptedDatabase(String),

    #[error("Corrupted page")]
    CorruptedPage,

    #[error("Corruption unrecoverable: {0}")]
    CorruptionUnrecoverable(String),

    #[error("Custom error: {0}")]
    Custom(String),

    #[error("Deadlock victim")]
    DeadlockVictim,

    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Invalid page ID")]
    InvalidPageId,

    #[error("IO error: {0}")]
    IoError(String),

    #[error("Migration error: {0}")]
    MigrationError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(u64),

    #[error("Timestamp overflow")]
    TimestampOverflow,

    #[error("Transaction retry needed: {0}")]
    TransactionRetry(String),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    #[error("Transaction {id} in invalid state: {state}")]
    TransactionInvalidState { id: u64, state: String },

    #[error("Unsupported feature: {feature}")]
    UnsupportedFeature { feature: String },

    #[error("Recovery timeout: stage {stage} after {timeout_seconds}s")]
    RecoveryTimeout {
        stage: String,
        timeout_seconds: u64,
        progress: f64,
    },

    #[error("Recovery stage {stage} depends on {dependency} which failed")]
    RecoveryStageDependencyFailed {
        stage: String,
        dependency: String,
        dependency_error: Box<Error>,
    },

    #[error("Partial recovery failure in stage {failed_stage}")]
    PartialRecoveryFailure {
        completed_stages: Vec<String>,
        failed_stage: String,
        cause: Box<Error>,
        rollback_available: bool,
    },

    #[error("Recovery rollback failed for stage {stage}: {reason}")]
    RecoveryRollbackFailed {
        stage: String,
        reason: String,
        manual_intervention_needed: bool,
    },

    #[error("Recovery verification failed: {check_name} - {details}")]
    RecoveryVerificationFailed {
        check_name: String,
        details: String,
        critical: bool,
    },

    #[error("Recovery impossible: {reason} - {suggested_action}")]
    RecoveryImpossible {
        reason: String,
        suggested_action: String,
    },

    #[error("Recovery configuration error: {setting} - {issue}")]
    RecoveryConfigurationError {
        setting: String,
        issue: String,
        fix: String,
    },

    #[error("WAL checksum mismatch at offset {offset}")]
    WalChecksumMismatch {
        offset: u64,
        expected: u32,
        found: u32,
    },

    #[error("WAL binary corruption at offset {offset}: {details}")]
    WalBinaryCorruption { offset: u64, details: String },

    #[error("Recovery permission error: {path} requires {required_permissions}")]
    RecoveryPermissionError {
        path: String,
        required_permissions: String,
    },

    #[error("Insufficient resources: {resource} - required: {required}, available: {available}")]
    InsufficientResources {
        resource: String,
        required: String,
        available: String,
    },

    #[error("Database not found: {path}")]
    DatabaseNotFound { path: String },

    #[error("WAL partial entry at offset {offset}: expected {expected_size} bytes, found {actual_size}")]
    WalPartialEntry {
        offset: u64,
        expected_size: usize,
        actual_size: usize,
    },

    #[error("Recovery dependency error: {dependency} - {issue}")]
    RecoveryDependencyError {
        dependency: String,
        issue: String,
    },

    #[error("Transaction limit reached: {limit}")]
    TransactionLimitReached { limit: usize },

    #[error("Checkpoint not found at LSN {checkpoint_lsn}: {path}")]
    CheckpointNotFound {
        checkpoint_lsn: u64,
        path: String,
    },

    #[error("Checkpoint corrupted at LSN {checkpoint_lsn}: {reason}")]
    CheckpointCorrupted {
        checkpoint_lsn: u64,
        reason: String,
    },

    #[error("WAL corruption at offset {offset}: {reason}")]
    WalCorruption { offset: u64, reason: String },
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

impl Error {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Error::Timeout(_)
                | Error::LockTimeout(_)
                | Error::LockFailed { .. }
                | Error::Throttled(_)
                | Error::ResourceExhausted { .. }
        )
    }

    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Error::Corruption(_)
                | Error::ChecksumMismatch { .. }
                | Error::InvalidDatabase
                | Error::IntegrityViolation(_)
                | Error::CorruptedDatabase(_)
                | Error::CorruptedPage
                | Error::CorruptionUnrecoverable(_)
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
        !matches!(
            self,
            Error::CorruptionUnrecoverable(_) | Error::RecoveryImpossible { .. }
        )
    }
}

// Compatibility type alias
pub type DatabaseError = Error;
pub type DatabaseResult<T> = Result<T>;