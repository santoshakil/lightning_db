//! Input validation utilities for CLI commands
//!
//! Provides consistent validation across all CLI commands with clear error messages.

#![allow(dead_code)]

use std::path::Path;

/// Result type for CLI operations
pub type CliResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Validation limits for CLI operations
pub mod limits {
    /// Maximum key size (4KB - matches Database internal limit)
    pub const MAX_KEY_SIZE: usize = 4096;
    /// Maximum value size (1MB - matches Database internal limit)
    pub const MAX_VALUE_SIZE: usize = 1024 * 1024;
    /// Maximum cache size in MB (100GB)
    pub const MAX_CACHE_SIZE_MB: u64 = 100 * 1024;
    /// Minimum cache size in MB
    pub const MIN_CACHE_SIZE_MB: u64 = 1;
    /// Maximum scan limit
    pub const MAX_SCAN_LIMIT: usize = 1_000_000;
    /// Maximum benchmark operations
    pub const MAX_BENCH_OPS: usize = 10_000_000;
    /// Maximum benchmark threads
    pub const MAX_BENCH_THREADS: usize = 256;
    /// Maximum benchmark value size (10MB)
    pub const MAX_BENCH_VALUE_SIZE: usize = 10 * 1024 * 1024;
}

/// Validate that a key is not empty and within size limits
pub fn validate_key(key: &str) -> CliResult<()> {
    if key.is_empty() {
        return Err("key cannot be empty".into());
    }
    if key.len() > limits::MAX_KEY_SIZE {
        return Err(format!(
            "key length ({} bytes) exceeds maximum ({} bytes)",
            key.len(),
            limits::MAX_KEY_SIZE
        ).into());
    }
    Ok(())
}

/// Validate that a value is within size limits
pub fn validate_value(value: &str) -> CliResult<()> {
    if value.len() > limits::MAX_VALUE_SIZE {
        return Err(format!(
            "value length ({} bytes) exceeds maximum ({} bytes)",
            value.len(),
            limits::MAX_VALUE_SIZE
        ).into());
    }
    Ok(())
}

/// Validate cache size in MB
pub fn validate_cache_size(size_mb: u64) -> CliResult<u64> {
    if size_mb < limits::MIN_CACHE_SIZE_MB {
        return Err(format!(
            "cache-size must be at least {} MB",
            limits::MIN_CACHE_SIZE_MB
        ).into());
    }
    if size_mb > limits::MAX_CACHE_SIZE_MB {
        return Err(format!(
            "cache-size cannot exceed {} GB",
            limits::MAX_CACHE_SIZE_MB / 1024
        ).into());
    }
    // Use saturating_mul to prevent overflow
    Ok(size_mb.saturating_mul(1024 * 1024))
}

/// Validate scan limit
pub fn validate_scan_limit(limit: usize) -> CliResult<()> {
    if limit == 0 {
        return Err("limit must be at least 1".into());
    }
    if limit > limits::MAX_SCAN_LIMIT {
        return Err(format!(
            "limit cannot exceed {}",
            limits::MAX_SCAN_LIMIT
        ).into());
    }
    Ok(())
}

/// Validate benchmark parameters
pub fn validate_bench_params(ops: usize, threads: usize, value_size: usize) -> CliResult<()> {
    if threads == 0 {
        return Err("threads must be at least 1".into());
    }
    if threads > limits::MAX_BENCH_THREADS {
        return Err(format!(
            "threads cannot exceed {}",
            limits::MAX_BENCH_THREADS
        ).into());
    }
    if ops == 0 {
        return Err("operations must be at least 1".into());
    }
    if ops > limits::MAX_BENCH_OPS {
        return Err(format!(
            "operations cannot exceed {}",
            limits::MAX_BENCH_OPS
        ).into());
    }
    if value_size == 0 {
        return Err("value-size must be at least 1".into());
    }
    if value_size > limits::MAX_BENCH_VALUE_SIZE {
        return Err(format!(
            "value-size cannot exceed {} MB",
            limits::MAX_BENCH_VALUE_SIZE / (1024 * 1024)
        ).into());
    }
    // Ensure ops >= threads to avoid division issues
    if ops < threads {
        return Err(format!(
            "operations ({}) must be >= threads ({})",
            ops, threads
        ).into());
    }
    Ok(())
}

/// Validate checksum sample size for integrity checks
pub fn validate_checksum_sample(sample_size: usize) -> CliResult<()> {
    const MAX_CHECKSUM_SAMPLE: usize = 10_000_000;
    if sample_size > MAX_CHECKSUM_SAMPLE {
        return Err(format!(
            "checksums sample size cannot exceed {} (got {})",
            MAX_CHECKSUM_SAMPLE, sample_size
        ).into());
    }
    Ok(())
}

/// Validate that database path exists (for open operations)
pub fn validate_db_exists(path: &str) -> CliResult<()> {
    let path_obj = Path::new(path);
    if !path_obj.exists() {
        return Err(format!(
            "Database not found: {}. Use 'create' to create a new database.",
            path
        ).into());
    }
    Ok(())
}

/// Validate that database path does not exist (for create operations)
pub fn validate_db_not_exists(path: &str) -> CliResult<()> {
    let path_obj = Path::new(path);
    if path_obj.exists() {
        return Err(format!(
            "Database path already exists: {}. Use commands like 'put', 'get', 'stats' to access it.",
            path
        ).into());
    }
    Ok(())
}

/// Validate that path does not exist (for operations that would overwrite)
pub fn validate_path_not_exists(path: &str, operation: &str) -> CliResult<()> {
    let path_obj = Path::new(path);
    if path_obj.exists() {
        return Err(format!(
            "{} destination already exists: {}. Remove it first or choose a different path.",
            operation, path
        ).into());
    }
    Ok(())
}

/// Validate backup source exists
pub fn validate_backup_exists(path: &str) -> CliResult<()> {
    let path_obj = Path::new(path);
    if !path_obj.exists() {
        return Err(format!(
            "Backup not found: {}. Check the path and try again.",
            path
        ).into());
    }
    Ok(())
}

/// Parse a positive integer from string with a descriptive error
pub fn parse_positive_usize(value: &str, name: &str) -> CliResult<usize> {
    value.parse::<usize>()
        .map_err(|_| format!("{} must be a positive number", name).into())
}

/// Parse a positive u64 from string with a descriptive error
pub fn parse_positive_u64(value: &str, name: &str) -> CliResult<u64> {
    value.parse::<u64>()
        .map_err(|_| format!("{} must be a positive number", name).into())
}

/// Validate that a path is valid and the parent directory exists
pub fn validate_path_parent_exists(path: &str) -> CliResult<()> {
    let path_obj = Path::new(path);

    // Check if the path is not empty
    if path.trim().is_empty() {
        return Err("Path cannot be empty".into());
    }

    // Check if parent directory exists (for non-root paths)
    if let Some(parent) = path_obj.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return Err(format!(
                "Parent directory does not exist: {}",
                parent.display()
            ).into());
        }
    }

    Ok(())
}

/// Validate database path is writable (checks parent directory permissions)
pub fn validate_path_writable(path: &str) -> CliResult<()> {
    let path_obj = Path::new(path);

    // Determine which directory to check for write access
    let dir_to_check = if path_obj.exists() {
        path_obj.to_path_buf()
    } else if let Some(parent) = path_obj.parent() {
        if parent.as_os_str().is_empty() {
            std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf())
        } else {
            parent.to_path_buf()
        }
    } else {
        std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf())
    };

    // Check read access by trying to read directory
    if !dir_to_check.exists() {
        return Err(format!(
            "Directory does not exist: {}",
            dir_to_check.display()
        ).into());
    }

    // Check if it's actually a directory (for the parent case)
    if !path_obj.exists() && !dir_to_check.is_dir() {
        return Err(format!(
            "Path is not a directory: {}",
            dir_to_check.display()
        ).into());
    }

    // Simple write check - try to create and remove a temp file
    let test_file = dir_to_check.join(format!(".lightning_cli_test_{}", std::process::id()));
    match std::fs::File::create(&test_file) {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
            Ok(())
        }
        Err(e) => Err(format!(
            "Path is not writable: {} ({})",
            dir_to_check.display(),
            e
        ).into()),
    }
}

/// Validate index operations parameters
pub fn validate_index_ops(ops: usize) -> CliResult<()> {
    const MAX_INDEX_OPS: usize = 10000;
    if ops == 0 {
        return Err("operations must be at least 1".into());
    }
    if ops > MAX_INDEX_OPS {
        return Err(format!(
            "operations cannot exceed {} for index tests",
            MAX_INDEX_OPS
        ).into());
    }
    Ok(())
}

/// Validate transaction test parameters
pub fn validate_tx_test_ops(ops: usize) -> CliResult<()> {
    const MAX_TX_OPS: usize = 10000;
    if ops == 0 {
        return Err("operations must be at least 1".into());
    }
    if ops > MAX_TX_OPS {
        return Err(format!(
            "operations cannot exceed {} for transaction tests",
            MAX_TX_OPS
        ).into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Key validation tests
    #[test]
    fn test_validate_key_empty() {
        assert!(validate_key("").is_err());
    }

    #[test]
    fn test_validate_key_valid() {
        assert!(validate_key("test_key").is_ok());
    }

    #[test]
    fn test_validate_key_max_size() {
        let long_key = "x".repeat(limits::MAX_KEY_SIZE);
        assert!(validate_key(&long_key).is_ok());
        let too_long = "x".repeat(limits::MAX_KEY_SIZE + 1);
        assert!(validate_key(&too_long).is_err());
    }

    // Value validation tests
    #[test]
    fn test_validate_value_empty() {
        assert!(validate_value("").is_ok()); // Empty values are allowed
    }

    #[test]
    fn test_validate_value_max_size() {
        let long_value = "x".repeat(limits::MAX_VALUE_SIZE);
        assert!(validate_value(&long_value).is_ok());
        let too_long = "x".repeat(limits::MAX_VALUE_SIZE + 1);
        assert!(validate_value(&too_long).is_err());
    }

    // Cache size validation tests
    #[test]
    fn test_validate_cache_size_bounds() {
        assert!(validate_cache_size(0).is_err());
        assert!(validate_cache_size(1).is_ok());
        assert!(validate_cache_size(100 * 1024 + 1).is_err());
    }

    #[test]
    fn test_validate_cache_size_returns_bytes() {
        let result = validate_cache_size(10).unwrap();
        assert_eq!(result, 10 * 1024 * 1024); // 10 MB in bytes
    }

    // Scan limit validation tests
    #[test]
    fn test_validate_scan_limit_bounds() {
        assert!(validate_scan_limit(0).is_err());
        assert!(validate_scan_limit(1).is_ok());
        assert!(validate_scan_limit(limits::MAX_SCAN_LIMIT).is_ok());
        assert!(validate_scan_limit(limits::MAX_SCAN_LIMIT + 1).is_err());
    }

    // Benchmark params validation tests
    #[test]
    fn test_validate_bench_params_zero_threads() {
        assert!(validate_bench_params(1000, 0, 100).is_err());
    }

    #[test]
    fn test_validate_bench_params_zero_ops() {
        assert!(validate_bench_params(0, 1, 100).is_err());
    }

    #[test]
    fn test_validate_bench_params_zero_value_size() {
        assert!(validate_bench_params(1000, 1, 0).is_err());
    }

    #[test]
    fn test_validate_bench_params_ops_less_than_threads() {
        assert!(validate_bench_params(4, 8, 100).is_err());
    }

    #[test]
    fn test_validate_bench_params_valid() {
        assert!(validate_bench_params(1000, 4, 100).is_ok());
        assert!(validate_bench_params(100, 100, 1).is_ok()); // ops == threads is ok
    }

    #[test]
    fn test_validate_bench_params_max_bounds() {
        assert!(validate_bench_params(limits::MAX_BENCH_OPS + 1, 1, 100).is_err());
        assert!(validate_bench_params(1000, limits::MAX_BENCH_THREADS + 1, 100).is_err());
        assert!(validate_bench_params(1000, 1, limits::MAX_BENCH_VALUE_SIZE + 1).is_err());
    }

    // Checksum sample validation tests
    #[test]
    fn test_validate_checksum_sample() {
        assert!(validate_checksum_sample(0).is_ok());
        assert!(validate_checksum_sample(100).is_ok());
        assert!(validate_checksum_sample(10_000_001).is_err());
    }

    // Parse functions tests
    #[test]
    fn test_parse_positive_usize() {
        assert!(parse_positive_usize("123", "test").is_ok());
        assert_eq!(parse_positive_usize("123", "test").unwrap(), 123);
        assert!(parse_positive_usize("-1", "test").is_err());
        assert!(parse_positive_usize("abc", "test").is_err());
        assert!(parse_positive_usize("", "test").is_err());
    }

    #[test]
    fn test_parse_positive_u64() {
        assert!(parse_positive_u64("123", "test").is_ok());
        assert_eq!(parse_positive_u64("123", "test").unwrap(), 123u64);
        assert!(parse_positive_u64("-1", "test").is_err());
        assert!(parse_positive_u64("abc", "test").is_err());
    }

    // Index ops validation tests
    #[test]
    fn test_validate_index_ops() {
        assert!(validate_index_ops(0).is_err());
        assert!(validate_index_ops(1).is_ok());
        assert!(validate_index_ops(10000).is_ok());
        assert!(validate_index_ops(10001).is_err());
    }

    // Transaction test ops validation tests
    #[test]
    fn test_validate_tx_test_ops() {
        assert!(validate_tx_test_ops(0).is_err());
        assert!(validate_tx_test_ops(1).is_ok());
        assert!(validate_tx_test_ops(10000).is_ok());
        assert!(validate_tx_test_ops(10001).is_err());
    }

    // Path validation tests
    #[test]
    fn test_validate_path_parent_exists_empty() {
        assert!(validate_path_parent_exists("").is_err());
        assert!(validate_path_parent_exists("   ").is_err());
    }

    #[test]
    fn test_validate_path_parent_exists_current_dir() {
        assert!(validate_path_parent_exists("./testdb").is_ok());
    }
}
