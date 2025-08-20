use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityConfig {
    /// Enable integrity validation globally
    pub enabled: bool,

    /// Validation level - affects which validations are performed
    pub level: ValidationLevel,

    /// Validate all reads from disk
    pub validate_all_reads: bool,

    /// Validate all writes to disk  
    pub validate_all_writes: bool,

    /// Validate cached data integrity
    pub validate_cached_data: bool,

    /// Enable background validation during idle periods
    pub background_validation: bool,

    /// Checksum algorithm to use
    pub checksum_algorithm: ChecksumAlgorithm,

    /// Maximum validation latency allowed (operations taking longer are skipped)
    pub max_validation_latency: Duration,

    /// Sample rate for non-critical validations (0.0 = never, 1.0 = always)
    pub sampling_rate: f64,

    /// Enable deep structural validation (expensive)
    pub deep_structural_validation: bool,

    /// Enable cross-reference validation
    pub cross_reference_validation: bool,

    /// Enable temporal consistency checks
    pub temporal_consistency_checks: bool,

    /// Maximum errors before aborting validation
    pub max_errors_threshold: usize,

    /// Enable automatic repair of detected issues
    pub auto_repair: bool,

    /// Create backup before attempting repairs
    pub backup_before_repair: bool,

    /// Enable integrity metrics collection
    pub collect_metrics: bool,

    /// Enable integrity alerting
    pub enable_alerts: bool,

    /// Validation context timeout
    pub context_timeout: Duration,

    /// Enable parallel validation where possible
    pub enable_parallel_validation: bool,

    /// Thread pool size for parallel validation
    pub validation_thread_pool_size: usize,
}

impl Default for IntegrityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            level: ValidationLevel::Standard,
            validate_all_reads: false, // Performance impact
            validate_all_writes: true, // Critical for data safety
            validate_cached_data: false,
            background_validation: true,
            checksum_algorithm: ChecksumAlgorithm::CRC32Fast,
            max_validation_latency: Duration::from_millis(10),
            sampling_rate: 0.1, // 10% sampling for reads
            deep_structural_validation: false,
            cross_reference_validation: true,
            temporal_consistency_checks: true,
            max_errors_threshold: 1000,
            auto_repair: false,
            backup_before_repair: true,
            collect_metrics: true,
            enable_alerts: true,
            context_timeout: Duration::from_secs(30),
            enable_parallel_validation: true,
            validation_thread_pool_size: 4,
        }
    }
}

impl IntegrityConfig {
    /// Create config for production environment (balanced performance/safety)
    pub fn production() -> Self {
        Self {
            enabled: true,
            level: ValidationLevel::Standard,
            validate_all_reads: false,
            validate_all_writes: true,
            validate_cached_data: false,
            background_validation: true,
            checksum_algorithm: ChecksumAlgorithm::CRC32Fast,
            max_validation_latency: Duration::from_millis(5),
            sampling_rate: 0.05, // 5% sampling
            deep_structural_validation: false,
            cross_reference_validation: true,
            temporal_consistency_checks: true,
            max_errors_threshold: 100,
            auto_repair: false,
            backup_before_repair: true,
            collect_metrics: true,
            enable_alerts: true,
            context_timeout: Duration::from_secs(10),
            enable_parallel_validation: true,
            validation_thread_pool_size: 2,
        }
    }

    /// Create config for development environment (maximum validation)
    pub fn development() -> Self {
        Self {
            enabled: true,
            level: ValidationLevel::Paranoid,
            validate_all_reads: true,
            validate_all_writes: true,
            validate_cached_data: true,
            background_validation: true,
            checksum_algorithm: ChecksumAlgorithm::XXHash64,
            max_validation_latency: Duration::from_millis(100),
            sampling_rate: 1.0, // 100% validation
            deep_structural_validation: true,
            cross_reference_validation: true,
            temporal_consistency_checks: true,
            max_errors_threshold: 10000,
            auto_repair: false,
            backup_before_repair: true,
            collect_metrics: true,
            enable_alerts: true,
            context_timeout: Duration::from_secs(60),
            enable_parallel_validation: true,
            validation_thread_pool_size: 8,
        }
    }

    /// Create config for performance testing (minimal validation)
    pub fn performance() -> Self {
        Self {
            enabled: true,
            level: ValidationLevel::Minimal,
            validate_all_reads: false,
            validate_all_writes: false,
            validate_cached_data: false,
            background_validation: false,
            checksum_algorithm: ChecksumAlgorithm::CRC32Fast,
            max_validation_latency: Duration::from_millis(1),
            sampling_rate: 0.01, // 1% sampling
            deep_structural_validation: false,
            cross_reference_validation: false,
            temporal_consistency_checks: false,
            max_errors_threshold: 10,
            auto_repair: false,
            backup_before_repair: false,
            collect_metrics: false,
            enable_alerts: false,
            context_timeout: Duration::from_secs(1),
            enable_parallel_validation: false,
            validation_thread_pool_size: 1,
        }
    }

    /// Disable all validation (not recommended for production)
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Should validate this operation based on sampling rate?
    pub fn should_validate_operation(&self, operation_type: OperationType) -> bool {
        if !self.enabled {
            return false;
        }

        match operation_type {
            OperationType::Write => self.validate_all_writes,
            OperationType::Read => {
                if self.validate_all_reads {
                    true
                } else {
                    // Apply sampling rate
                    fastrand::f64() < self.sampling_rate
                }
            }
            OperationType::CacheAccess => self.validate_cached_data,
            OperationType::Background => self.background_validation,
        }
    }

    /// Get validation timeout for operation type
    pub fn get_timeout(&self, operation_type: OperationType) -> Duration {
        match operation_type {
            OperationType::Write => Duration::from_millis(50), // More time for critical writes
            OperationType::Read => self.max_validation_latency,
            OperationType::CacheAccess => Duration::from_millis(1),
            OperationType::Background => self.context_timeout,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationLevel {
    /// Disabled validation
    Disabled,
    /// Minimal validation (only critical errors)
    Minimal,
    /// Standard validation (balance of performance and safety)
    Standard,
    /// Comprehensive validation (thorough but slower)
    Comprehensive,
    /// Paranoid validation (maximum safety, performance impact)
    Paranoid,
}

impl ValidationLevel {
    pub fn includes_checksum_validation(&self) -> bool {
        !matches!(self, ValidationLevel::Disabled)
    }

    pub fn includes_structural_validation(&self) -> bool {
        matches!(
            self,
            ValidationLevel::Standard
                | ValidationLevel::Comprehensive
                | ValidationLevel::Paranoid
        )
    }

    pub fn includes_reference_validation(&self) -> bool {
        matches!(
            self,
            ValidationLevel::Comprehensive | ValidationLevel::Paranoid
        )
    }

    pub fn includes_temporal_validation(&self) -> bool {
        matches!(self, ValidationLevel::Paranoid)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    CRC32Fast,
    CRC32C,
    XXHash32,
    XXHash64,
    SHA256,
}

impl ChecksumAlgorithm {
    pub fn calculate(&self, data: &[u8]) -> u64 {
        match self {
            ChecksumAlgorithm::CRC32Fast => {
                crc32fast::hash(data) as u64
            }
            ChecksumAlgorithm::CRC32C => {
                // Fallback to CRC32Fast if CRC32C not available
                crc32fast::hash(data) as u64
            }
            ChecksumAlgorithm::XXHash32 => {
                xxhash_rust::xxh32::xxh32(data, 0) as u64
            }
            ChecksumAlgorithm::XXHash64 => {
                xxhash_rust::xxh64::xxh64(data, 0)
            }
            ChecksumAlgorithm::SHA256 => {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(data);
                let result = hasher.finalize();
                // Take first 8 bytes of SHA256 hash
                u64::from_be_bytes([
                    result[0], result[1], result[2], result[3],
                    result[4], result[5], result[6], result[7],
                ])
            }
        }
    }

    pub fn is_fast(&self) -> bool {
        matches!(
            self,
            ChecksumAlgorithm::CRC32Fast
                | ChecksumAlgorithm::CRC32C
                | ChecksumAlgorithm::XXHash32
                | ChecksumAlgorithm::XXHash64
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Read,
    Write,
    CacheAccess,
    Background,
}

#[derive(Debug, Clone)]
pub struct ValidationContext {
    pub operation: String,
    pub location: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub thread_id: String,
    pub metadata: std::collections::HashMap<String, String>,
}

impl ValidationContext {
    pub fn new(operation: &str, location: &str) -> Self {
        Self {
            operation: operation.to_string(),
            location: location.to_string(),
            timestamp: chrono::Utc::now(),
            thread_id: format!("{:?}", std::thread::current().id()),
            metadata: std::collections::HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    pub fn add_metadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
    }
}