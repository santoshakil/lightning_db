//! CLI error handling utilities
//!
//! Provides CLI-specific error types for enterprise-grade error handling
//! with user-friendly messages and appropriate exit codes.

#![allow(dead_code)]

use std::fmt;

/// CLI exit codes for different error categories
pub mod exit_codes {
    /// Success
    pub const SUCCESS: i32 = 0;
    /// General error
    pub const GENERAL_ERROR: i32 = 1;
    /// Invalid command line usage
    pub const USAGE_ERROR: i32 = 2;
    /// Database not found
    pub const NOT_FOUND: i32 = 3;
    /// Database already exists
    pub const ALREADY_EXISTS: i32 = 4;
    /// Validation error
    pub const VALIDATION_ERROR: i32 = 5;
    /// Permission denied
    pub const PERMISSION_DENIED: i32 = 6;
    /// IO error
    pub const IO_ERROR: i32 = 7;
    /// Database operation error
    pub const DATABASE_ERROR: i32 = 8;
    /// Transaction error
    pub const TRANSACTION_ERROR: i32 = 9;
    /// Integrity check failed
    pub const INTEGRITY_FAILED: i32 = 10;
    /// Test failure
    pub const TEST_FAILED: i32 = 11;
    /// Internal error
    pub const INTERNAL_ERROR: i32 = 99;
}

/// CLI-specific error type with user-friendly messages
#[derive(Debug)]
pub enum CliError {
    /// Database not found
    DatabaseNotFound(String),
    /// Database already exists
    DatabaseExists(String),
    /// Validation error
    Validation(String),
    /// IO error
    Io(String),
    /// Permission denied
    PermissionDenied(String),
    /// Database operation error
    Database(String),
    /// Transaction error
    Transaction(String),
    /// Integrity check failed
    IntegrityFailed(String),
    /// Test failure
    TestFailed(String),
    /// Internal error
    Internal(String),
}

impl CliError {
    /// Get the appropriate exit code for this error type
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::DatabaseNotFound(_) => exit_codes::NOT_FOUND,
            CliError::DatabaseExists(_) => exit_codes::ALREADY_EXISTS,
            CliError::Validation(_) => exit_codes::VALIDATION_ERROR,
            CliError::Io(_) => exit_codes::IO_ERROR,
            CliError::PermissionDenied(_) => exit_codes::PERMISSION_DENIED,
            CliError::Database(_) => exit_codes::DATABASE_ERROR,
            CliError::Transaction(_) => exit_codes::TRANSACTION_ERROR,
            CliError::IntegrityFailed(_) => exit_codes::INTEGRITY_FAILED,
            CliError::TestFailed(_) => exit_codes::TEST_FAILED,
            CliError::Internal(_) => exit_codes::INTERNAL_ERROR,
        }
    }

    /// Get a short error category name for logging
    pub fn category(&self) -> &'static str {
        match self {
            CliError::DatabaseNotFound(_) => "NOT_FOUND",
            CliError::DatabaseExists(_) => "ALREADY_EXISTS",
            CliError::Validation(_) => "VALIDATION",
            CliError::Io(_) => "IO",
            CliError::PermissionDenied(_) => "PERMISSION_DENIED",
            CliError::Database(_) => "DATABASE",
            CliError::Transaction(_) => "TRANSACTION",
            CliError::IntegrityFailed(_) => "INTEGRITY",
            CliError::TestFailed(_) => "TEST",
            CliError::Internal(_) => "INTERNAL",
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::DatabaseNotFound(path) => {
                write!(
                    f,
                    "Database not found: {}. Use 'create' to create a new database.",
                    path
                )
            }
            CliError::DatabaseExists(path) => {
                write!(
                    f,
                    "Database already exists: {}. Use 'put', 'get', or 'stats' to access it.",
                    path
                )
            }
            CliError::Validation(msg) => write!(f, "Validation error: {}", msg),
            CliError::Io(msg) => write!(f, "IO error: {}", msg),
            CliError::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
            CliError::Database(msg) => write!(f, "Database error: {}", msg),
            CliError::Transaction(msg) => write!(f, "Transaction error: {}", msg),
            CliError::IntegrityFailed(msg) => write!(f, "Integrity check failed: {}", msg),
            CliError::TestFailed(msg) => write!(f, "Test failed: {}", msg),
            CliError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => CliError::DatabaseNotFound(err.to_string()),
            std::io::ErrorKind::PermissionDenied => CliError::PermissionDenied(err.to_string()),
            std::io::ErrorKind::AlreadyExists => CliError::DatabaseExists(err.to_string()),
            _ => CliError::Io(err.to_string()),
        }
    }
}

impl From<lightning_db::Error> for CliError {
    fn from(err: lightning_db::Error) -> Self {
        // Try to categorize the database error
        let msg = err.to_string();
        if msg.contains("transaction") || msg.contains("Transaction") {
            CliError::Transaction(msg)
        } else if msg.contains("integrity") || msg.contains("Integrity") || msg.contains("corrupt")
        {
            CliError::IntegrityFailed(msg)
        } else if msg.contains("not found") || msg.contains("Not found") {
            CliError::DatabaseNotFound(msg)
        } else {
            CliError::Database(msg)
        }
    }
}

impl From<&str> for CliError {
    fn from(msg: &str) -> Self {
        CliError::Validation(msg.to_string())
    }
}

impl From<String> for CliError {
    fn from(msg: String) -> Self {
        CliError::Validation(msg)
    }
}

/// Convert a result with CliError to a boxed error for consistency
pub fn to_boxed_error<T>(result: Result<T, CliError>) -> Result<T, Box<dyn std::error::Error>> {
    result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

/// Format an error message for display, optionally including debug info
pub fn format_error(error: &dyn std::error::Error, verbose: bool) -> String {
    if verbose {
        format!("[{}] {}", error_source_chain(error), error)
    } else {
        error.to_string()
    }
}

/// Get the full error source chain for debugging
fn error_source_chain(error: &dyn std::error::Error) -> String {
    let mut chain = Vec::new();
    let mut current = error.source();
    while let Some(source) = current {
        chain.push(source.to_string());
        current = source.source();
    }
    if chain.is_empty() {
        "Error".to_string()
    } else {
        chain.join(" -> ")
    }
}

/// Helper macro for creating validation errors
#[macro_export]
macro_rules! validation_error {
    ($($arg:tt)*) => {
        Err(Box::new($crate::cli::utils::error::CliError::Validation(format!($($arg)*))) as Box<dyn std::error::Error>)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exit_codes() {
        assert_eq!(CliError::DatabaseNotFound("test".to_string()).exit_code(), exit_codes::NOT_FOUND);
        assert_eq!(CliError::Validation("test".to_string()).exit_code(), exit_codes::VALIDATION_ERROR);
        assert_eq!(CliError::Database("test".to_string()).exit_code(), exit_codes::DATABASE_ERROR);
    }

    #[test]
    fn test_error_display() {
        let err = CliError::DatabaseNotFound("/path/to/db".to_string());
        let msg = err.to_string();
        assert!(msg.contains("/path/to/db"));
        assert!(msg.contains("create"));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let cli_err: CliError = io_err.into();
        assert!(matches!(cli_err, CliError::DatabaseNotFound(_)));

        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let cli_err: CliError = io_err.into();
        assert!(matches!(cli_err, CliError::PermissionDenied(_)));
    }

    #[test]
    fn test_error_category() {
        assert_eq!(CliError::Validation("test".to_string()).category(), "VALIDATION");
        assert_eq!(CliError::Database("test".to_string()).category(), "DATABASE");
    }
}
