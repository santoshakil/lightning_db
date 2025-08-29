use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;
pub type DatabaseResult<T> = std::result::Result<T, DatabaseError>;

fn holders_display(holders: &[String]) -> String {
    holders.join(", ")
}

/// Error category for better error handling and recovery strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Critical errors that require immediate shutdown
    Critical,
    /// Transient errors that can be retried
    Transient,
    /// User input errors
    UserInput,
    /// Configuration errors
    Configuration,
    /// Network-related errors
    Network,
    /// Storage/IO errors
    Storage,
    /// Data corruption errors
    Corruption,
    /// Resource exhaustion
    Resource,
    /// Internal logic errors (bugs)
    Internal,
}

/// Error severity levels for logging and monitoring
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorSeverity {
    Debug = 0,
    Info = 1,
    Warning = 2,
    Error = 3,
    Critical = 4,
}

/// Enhanced error context with debugging information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorContext {
    pub operation: String,
    pub component: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub thread_id: Option<String>,
    pub additional_data: std::collections::HashMap<String, String>,
}

impl ErrorContext {
    pub fn new(operation: &str, component: &str) -> Self {
        Self {
            operation: operation.to_string(),
            component: component.to_string(),
            file: None,
            line: None,
            thread_id: None,
            additional_data: std::collections::HashMap::new(),
        }
    }
    
    pub fn with_location(mut self, file: &str, line: u32) -> Self {
        self.file = Some(file.to_string());
        self.line = Some(line);
        self
    }
    
    pub fn with_thread(mut self) -> Self {
        self.thread_id = Some(format!("{:?}", std::thread::current().id()));
        self
    }
    
    pub fn with_data(mut self, key: &str, value: &str) -> Self {
        self.additional_data.insert(key.to_string(), value.to_string());
        self
    }
}

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

    // Transaction isolation errors
    // Note: TransactionNotFound already exists above with different signature

    #[error("Snapshot {0} not found")]
    SnapshotNotFound(u64),

    #[error("Lock timeout: {0}")]
    LockTimeout(String),

    #[error("Lock conflict: {0}")]
    LockConflict(String),

    #[error("Deadlock detected: {0}")]
    Deadlock(String),
    
    #[error("Deadlock detected")]
    DeadlockDetected,
    
    #[error("Write conflict: {0}")]
    WriteConflict(String),
    
    #[error("Serialization conflict: {0}")]
    SerializationConflict(String),
    
    #[error("Compression error: {0}")]
    CompressionError(String),
    
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Transaction retry required: {0}")]
    TransactionRetry(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    // Distributed transaction errors
    #[error("Transaction failed: {0}")]
    TransactionFailed(String),
    
    // Additional errors for time-series and other features
    #[error("Key already exists")]
    KeyAlreadyExists,
    
    #[error("Channel closed")]
    ChannelClosed,
    
    #[error("System shutting down")]
    SystemShuttingDown,
    
    #[error("Buffer full")]
    BufferFull,
    
    #[error("Task join error")]
    TaskJoinError,
    
    #[error("Data corruption detected")]
    Corruption,

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

    // === ENHANCED ERROR HANDLING ===
    
    /// Context-aware error with detailed information
    #[error("{message}")]
    WithContext {
        message: String,
        source: Box<Error>,
        context: ErrorContext,
        backtrace: Option<String>,
    },
    
    /// Resource exhaustion with specific details
    #[error("Resource '{resource}' exhausted: {current}/{limit} - {suggestion}")]
    ResourceExhaustion {
        resource: String,
        current: u64,
        limit: u64,
        suggestion: String,
    },
    
    /// Thread safety violation
    #[error("Thread safety violation in {component}: {details}")]
    ThreadSafetyViolation {
        component: String,
        details: String,
        thread_id: String,
    },
    
    /// Async operation errors
    #[error("Async operation failed in {operation}: {reason}")]
    AsyncOperationFailed {
        operation: String,
        reason: String,
        timeout: Option<std::time::Duration>,
    },
    
    /// Lock contention error with diagnostics
    #[error("Lock contention on '{resource}': waited {wait_time:?}, holders: {}", holders_display(.holders))]
    LockContention {
        resource: String,
        wait_time: std::time::Duration,
        holders: Vec<String>,
    },
    
    /// Invariant violation (internal logic error)
    #[error("Invariant violation in {function}: {invariant} - {details}")]
    InvariantViolation {
        function: String,
        invariant: String,
        details: String,
        backtrace: String,
    },
    
    /// Recoverable error with retry information
    #[error("Recoverable error: {reason} (attempt {attempt}/{max_attempts})")]
    Recoverable {
        reason: String,
        attempt: u32,
        max_attempts: u32,
        retry_after: Option<std::time::Duration>,
        source: Box<Error>,
    },

    /// Migration errors
    #[error("Migration error: {0}")]
    Migration(String),

    /// Backup errors
    #[error("Backup error: {0}")]
    Backup(String),

    /// Invalid state error
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Commit failed error
    #[error("Commit failed: {0}")]
    CommitFailed(String),

    /// Deadlock victim error
    #[error("Transaction selected as deadlock victim")]
    DeadlockVictim,

    /// Connection pool exhausted
    #[error("Connection pool exhausted")]
    ConnectionPoolExhausted,

    /// Prepare phase timeout
    #[error("Prepare phase timeout")]
    PrepareTimeout,

    /// Participant failed in distributed transaction
    #[error("Participant failed: {0}")]
    ParticipantFailed(String),

    /// Connection lost
    #[error("Connection lost")]
    ConnectionLost,

    /// Isolation violation
    #[error("Isolation level violation")]
    IsolationViolation,

    /// Raft consensus error
    #[error("Raft error: {0}")]
    RaftError(String),

    /// Leader election failed
    #[error("Leader election failed")]
    LeaderElectionFailed,

    /// Custom error
    #[error("Custom error: {0}")]
    Custom(String),
}

/// Database-specific error types for migration system
#[derive(Error, Debug, Clone)]
pub enum DatabaseError {
    #[error("IO error: {0}")]
    IoError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Migration error: {0}")]
    MigrationError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
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
    /// Get the error category for handling decisions
    pub fn category(&self) -> ErrorCategory {
        match self {
            Error::Io(_) | Error::IoError(_) => ErrorCategory::Storage,
            Error::InvalidDatabase | Error::CorruptedPage | Error::CorruptedDatabase(_) => ErrorCategory::Corruption,
            Error::KeyNotFound | Error::PageNotFound => ErrorCategory::UserInput,
            Error::Config(_) | Error::InvalidFormat(_) => ErrorCategory::Configuration,
            Error::Memory | Error::ResourceExhausted { .. } | Error::ResourceExhaustion { .. } => ErrorCategory::Resource,
            Error::Timeout(_) | Error::RequestTimeout | Error::RecoveryTimeout { .. } => ErrorCategory::Transient,
            Error::ThreadPanic(_) | Error::InvariantViolation { .. } => ErrorCategory::Internal,
            Error::RecoveryImpossible { .. } | Error::WalCorrupted { .. } => ErrorCategory::Critical,
            Error::LockContention { .. } | Error::AsyncOperationFailed { .. } => ErrorCategory::Transient,
            Error::ThreadSafetyViolation { .. } => ErrorCategory::Internal,
            _ => ErrorCategory::Internal,
        }
    }
    
    /// Get the error severity for logging decisions
    pub fn severity(&self) -> ErrorSeverity {
        match self.category() {
            ErrorCategory::Critical => ErrorSeverity::Critical,
            ErrorCategory::Corruption => ErrorSeverity::Critical,
            ErrorCategory::Internal => ErrorSeverity::Error,
            ErrorCategory::Resource => ErrorSeverity::Error,
            ErrorCategory::Storage => ErrorSeverity::Error,
            ErrorCategory::Network => ErrorSeverity::Warning,
            ErrorCategory::Transient => ErrorSeverity::Warning,
            ErrorCategory::Configuration => ErrorSeverity::Error,
            ErrorCategory::UserInput => ErrorSeverity::Info,
        }
    }
    
    /// Check if this error suggests system shutdown
    pub fn requires_shutdown(&self) -> bool {
        matches!(self.category(), ErrorCategory::Critical) ||
        matches!(self, 
            Error::Memory | 
            Error::InvariantViolation { .. } |
            Error::ThreadSafetyViolation { .. } |
            Error::RecoveryImpossible { .. }
        )
    }
    
    /// Get retry delay for transient errors
    pub fn retry_delay(&self) -> Option<std::time::Duration> {
        match self {
            Error::Recoverable { retry_after, .. } => *retry_after,
            Error::LockContention { wait_time, .. } => {
                Some(std::time::Duration::from_millis(100).min(*wait_time * 2))
            },
            Error::AsyncOperationFailed { timeout, .. } => {
                timeout.map(|t| t / 4) // Quarter of timeout as retry delay
            },
            _ if self.category() == ErrorCategory::Transient => {
                Some(std::time::Duration::from_millis(100))
            },
            _ => None,
        }
    }
    
    /// Add context to an error
    pub fn with_context(self, context: ErrorContext) -> Self {
        let backtrace = std::backtrace::Backtrace::capture();
        Error::WithContext {
            message: self.to_string(),
            source: Box::new(self),
            context,
            backtrace: Some(format!("{}", backtrace)),
        }
    }
    
    /// Create a recoverable error wrapper
    pub fn recoverable(self, attempt: u32, max_attempts: u32) -> Self {
        Error::Recoverable {
            reason: self.to_string(),
            attempt,
            max_attempts,
            retry_after: self.retry_delay(),
            source: Box::new(self),
        }
    }
    
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
            Error::DeadlockDetected => -37,
            Error::WriteConflict(_) => -38,
            Error::SerializationConflict(_) => -39,
            Error::DeadlockVictim => -40,
            Error::InvalidState(_) => -41,
            Error::CommitFailed(_) => -42,
            Error::PrepareTimeout => -43,
            Error::ParticipantFailed(_) => -44,
            Error::ConnectionLost => -45,
            Error::ConnectionPoolExhausted => -46,
            Error::InvalidArgument(_) => -47,
            Error::NotFound(_) => -48,
            Error::ValidationFailed(_) => -49,
            Error::IsolationViolation => -50,
            Error::RaftError(_) => -51,
            Error::LeaderElectionFailed => -52,
            Error::Throttled(_) => -53,
            Error::NotImplemented(_) => -54,
            Error::WalNotAvailable => -55,
            Error::CorruptionUnrecoverable(_) => -56,
            Error::Encryption(_) => -57,
            Error::QuotaExceeded(_) => -58,
            Error::Parse(_) => -59,
            Error::Validation(_) => -60,
            Error::Locked(_) => -61,
            Error::ParseError(_) => -62,
            Error::ProcessingError(_) => -63,
            Error::ThreadPanic(_) => -64,
            Error::InvalidInput(_) => -65,
            Error::ConversionError(_) => -70,
            Error::SchemaValidationFailed(_) => -71,
            Error::IoError(_) => -72,
            Error::NotLeader(_) => -73,
            Error::Rpc(_) => -74,
            Error::ElectionTimeout => -75,
            Error::LogInconsistent { .. } => -76,
            Error::Snapshot(_) => -77,
            Error::Membership(_) => -78,
            Error::RequestTimeout => -79,
            Error::InvalidData(_) => -80,
            Error::Internal(_) => -81,
            Error::TransactionFailed(_) => -82,
            Error::Deadlock(_) => -83,
            
            // Recovery error codes (90-99 range)
            Error::RecoveryImpossible { .. } => -90,
            Error::WalCorrupted { .. } => -91,
            Error::InsufficientResources { .. } => -92,
            Error::DatabaseLocked { .. } => -93,
            Error::PartialRecoveryFailure { .. } => -94,
            Error::InconsistentState { .. } => -95,
            Error::RecoveryConfigurationError { .. } => -96,
            Error::RecoveryPermissionError { .. } => -97,
            Error::RecoveryDependencyError { .. } => -98,
            Error::RecoveryTimeout { .. } => -99,
            Error::RecoveryProgress { .. } => -100,
            Error::RecoveryRollbackFailed { .. } => -101,
            Error::RecoveryStageDependencyFailed { .. } => -102,
            Error::RecoveryVerificationFailed { .. } => -103,
            
            // Enhanced error codes (110+ range)
            Error::WithContext { .. } => -110,
            Error::ResourceExhaustion { .. } => -111,
            Error::ThreadSafetyViolation { .. } => -112,
            Error::AsyncOperationFailed { .. } => -113,
            Error::LockContention { .. } => -114,
            Error::InvariantViolation { .. } => -115,
            Error::Recoverable { .. } => -116,
            Error::SnapshotNotFound(_) => -117,
            Error::LockTimeout(_) => -118,
            Error::LockConflict(_) => -119,
            Error::Serialization(_) => -120,
            Error::Migration(_) => -121,
            Error::Backup(_) => -122,
            Error::TransactionRetry(_) => -123,
            Error::CompressionError(_) => -124,
            Error::ConfigError(_) => -125,
            Error::KeyAlreadyExists => -126,
            Error::ChannelClosed => -127,
            Error::SystemShuttingDown => -128,
            Error::BufferFull => -129,
            Error::TaskJoinError => -130,
            Error::Corruption => -131,
            Error::Custom(_) => -132,
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
                | Error::LockContention { .. }
                | Error::AsyncOperationFailed { .. }
                | Error::Recoverable { .. }
                | Error::ResourceExhaustion { .. }
        ) || self.category() == ErrorCategory::Transient
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

/// Enhanced context trait for adding context to errors
pub trait ErrorContextExt<T> {
    fn context(self, msg: &str) -> Result<T>;
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String;
    fn with_error_context(self, context: crate::core::error::ErrorContext) -> Result<T>;
    fn operation_context(self, operation: &str, component: &str) -> Result<T>;
}

impl<T> ErrorContextExt<T> for Result<T> {
    fn context(self, msg: &str) -> Result<T> {
        self.map_err(|e| e.with_context(
            crate::core::error::ErrorContext::new(msg, "unknown")
        ))
    }

    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| e.with_context(
            crate::core::error::ErrorContext::new(&f(), "unknown")
        ))
    }
    
    fn with_error_context(self, context: crate::core::error::ErrorContext) -> Result<T> {
        self.map_err(|e| e.with_context(context))
    }
    
    fn operation_context(self, operation: &str, component: &str) -> Result<T> {
        self.map_err(|e| e.with_context(
            crate::core::error::ErrorContext::new(operation, component)
                .with_thread()
        ))
    }
}

// Macros for easier error context creation
#[macro_export]
macro_rules! error_context {
    ($operation:expr, $component:expr) => {
        $crate::core::error::ErrorContext::new($operation, $component)
            .with_location(file!(), line!())
            .with_thread()
    };
    ($operation:expr, $component:expr, $($key:expr => $value:expr),+) => {
        {
            let mut ctx = $crate::core::error::ErrorContext::new($operation, $component)
                .with_location(file!(), line!())
                .with_thread();
            $(ctx = ctx.with_data($key, $value);)+
            ctx
        }
    };
}

#[macro_export]
macro_rules! bail_with_context {
    ($err:expr, $operation:expr, $component:expr) => {
        return Err($err.with_context(error_context!($operation, $component)))
    };
    ($err:expr, $operation:expr, $component:expr, $($key:expr => $value:expr),+) => {
        return Err($err.with_context(error_context!($operation, $component, $($key => $value),+)))
    };
}

/// Safe unwrap alternatives
pub trait SafeUnwrap<T> {
    /// Safe unwrap that returns an error instead of panicking
    fn safe_unwrap(self) -> Result<T>;
    /// Safe unwrap with custom error message
    fn safe_unwrap_or_error(self, error: Error) -> Result<T>;
    /// Safe unwrap with context
    fn safe_unwrap_with_context(self, operation: &str, component: &str) -> Result<T>;
}

impl<T> SafeUnwrap<T> for Option<T> {
    fn safe_unwrap(self) -> Result<T> {
        self.ok_or(Error::Generic("None value encountered".to_string()))
    }
    
    fn safe_unwrap_or_error(self, error: Error) -> Result<T> {
        self.ok_or(error)
    }
    
    fn safe_unwrap_with_context(self, operation: &str, component: &str) -> Result<T> {
        self.ok_or(Error::Generic(format!("None value in {} ({})", operation, component)))
    }
}

impl<T, E: Into<Error>> SafeUnwrap<T> for std::result::Result<T, E> {
    fn safe_unwrap(self) -> Result<T> {
        self.map_err(Into::into)
    }
    
    fn safe_unwrap_or_error(self, error: Error) -> Result<T> {
        self.map_err(|_| error)
    }
    
    fn safe_unwrap_with_context(self, operation: &str, component: &str) -> Result<T> {
        self.map_err(|e| e.into().with_context(
            crate::core::error::ErrorContext::new(operation, component)
        ))
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

impl From<std::fmt::Error> for Error {
    fn from(err: std::fmt::Error) -> Self {
        Error::Generic(format!("Format error: {}", err))
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

// Note: std::error::Error is automatically implemented by the Error derive macro

// std::error::Error is automatically implemented by the Error derive macro

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
