# Lightning DB - API Reference

## Overview

This document provides comprehensive API reference for Lightning DB's enhanced security and reliability features introduced in version 1.0. All APIs include proper error handling and follow modern Rust patterns.

---

## Core Database API

### Database Initialization

#### `Database::new_with_config(config: LightningDbConfig) -> Result<Database, DatabaseError>`

Creates a new database instance with enhanced configuration options.

```rust
use lightning_db::{Database, LightningDbConfig, SecurityConfig, ReliabilityConfig};

let config = LightningDbConfig {
    // Core settings
    compression_enabled: true,
    cache_size: 1024 * 1024 * 1024, // 1GB
    
    // NEW: Security settings  
    integrity_validation_level: IntegrityLevel::Paranoid,
    enable_audit_logging: true,
    fail_on_corruption: true,
    
    // NEW: Reliability settings
    enable_health_monitoring: true,
    enable_self_healing: true,
    deadlock_prevention_enabled: true,
    
    ..Default::default()
};

let db = Database::new_with_config(config)?;
```

**Error Types**:
- `DatabaseError::ConfigurationError` - Invalid configuration
- `DatabaseError::InsufficientResources` - Not enough memory/disk
- `DatabaseError::SecurityError` - Security validation failed
- `DatabaseError::InitializationFailed` - Database initialization failed

---

## Enhanced Error Handling API

### Error Types Hierarchy

#### `DatabaseError` - Main Error Enum

```rust
pub enum DatabaseError {
    // Core errors
    KeyNotFound { key: Vec<u8>, context: String },
    ValueTooLarge { size: usize, max_size: usize },
    TransactionConflict { key: Vec<u8>, conflicting_tx: u64 },
    
    // NEW: Security errors
    SecurityError(SecurityError),
    DataCorruption { 
        component: String, 
        details: String,
        recovery_possible: bool 
    },
    IntegrityValidationFailed {
        check_name: String,
        expected: String,
        actual: String,
    },
    
    // NEW: Reliability errors  
    DeadlockDetected { 
        resources: Vec<String>,
        suggested_action: String 
    },
    CircuitBreakerOpen { 
        service: String,
        retry_after: Duration 
    },
    AutoRecoveryFailed {
        stage: String,
        attempts: usize,
        last_error: Box<DatabaseError>,
    },
    
    // NEW: Recovery errors (14 new types)
    RecoveryImpossible { 
        reason: String, 
        suggested_action: String 
    },
    WalCorrupted { 
        details: String, 
        suggested_action: String 
    },
    PartialRecoveryFailure {
        completed_stages: Vec<String>,
        failed_stage: String,
        cause: Box<DatabaseError>,
        rollback_available: bool,
    },
    // ... (11 more recovery error types)
}
```

#### Error Classification Methods

```rust
impl DatabaseError {
    /// Returns true if this error indicates a critical system failure
    pub fn is_critical(&self) -> bool;
    
    /// Returns true if the operation can be retried
    pub fn is_retryable(&self) -> bool;
    
    /// Returns true if automatic recovery is possible
    pub fn is_recoverable(&self) -> bool;
    
    /// Returns suggested remediation action
    pub fn suggested_action(&self) -> Option<String>;
    
    /// Returns unique error code for automated handling
    pub fn error_code(&self) -> i32;
}
```

---

## Health Monitoring API

### Health Check System

#### `HealthMonitor::new(checks: Vec<Box<dyn HealthCheck>>) -> HealthMonitor`

Creates a new health monitoring system.

```rust
use lightning_db::{HealthMonitor, DataIntegrityCheck, ResourceCheck, PerformanceCheck};

let monitor = HealthMonitor::new(vec![
    Box::new(DataIntegrityCheck::new()),
    Box::new(ResourceCheck::new()),
    Box::new(PerformanceCheck::new()),
]);

// Start continuous monitoring
let handle = monitor.start_monitoring(Duration::from_secs(30))?;
```

#### `HealthCheck` Trait

```rust
pub trait HealthCheck: Send + Sync {
    /// Name of the health check
    fn name(&self) -> &str;
    
    /// Execute the health check
    fn check(&self) -> HealthStatus;
    
    /// Criticality level of this check
    fn criticality(&self) -> Criticality;
    
    /// Suggested remediation for failures
    fn remediation(&self) -> Option<String>;
}

pub enum HealthStatus {
    Healthy,
    Warning { message: String, remediation: Option<String> },
    Critical { message: String, immediate_action: String },
    Unknown,
}

pub enum Criticality {
    Critical,  // Affects database availability
    High,      // Affects performance
    Medium,    // Potential future issue
    Low,       // Informational
}
```

#### Built-in Health Checks

```rust
// Data integrity monitoring
pub struct DataIntegrityCheck {
    validation_level: IntegrityLevel,
}

impl DataIntegrityCheck {
    pub fn new() -> Self;
    pub fn with_level(level: IntegrityLevel) -> Self;
}

// Resource utilization monitoring  
pub struct ResourceCheck {
    memory_threshold: f64,    // 0.0 - 1.0
    disk_threshold: f64,      // 0.0 - 1.0
}

// Performance monitoring
pub struct PerformanceCheck {
    latency_threshold: Duration,
    throughput_threshold: u64,
}
```

---

## Auto-Recovery API

### Recovery Manager

#### `AutoRecoveryManager::new(config: RecoveryConfig) -> AutoRecoveryManager`

Creates an automatic recovery system.

```rust
use lightning_db::{AutoRecoveryManager, RecoveryConfig, BackoffStrategy};

let recovery_config = RecoveryConfig {
    max_retries: 3,
    backoff_strategy: BackoffStrategy::Exponential {
        base_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(30),
    },
    enable_rollback: true,
    timeout_per_stage: Duration::from_secs(60),
};

let recovery_manager = AutoRecoveryManager::new(recovery_config);
```

#### `execute_with_recovery()` Method

```rust
impl AutoRecoveryManager {
    pub async fn execute_with_recovery<T, E, F, Fut>(
        &self,
        operation: F,
        operation_name: &str,
    ) -> Result<T, E>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: Future<Output = Result<T, E>> + Send,
        E: RetryableError + Send + Sync,
    {
        // Implements automatic retry with exponential backoff
        // Returns original error if max retries exceeded
    }
}
```

### Recovery State Tracking

```rust
pub struct RecoveryState {
    current_stage: Option<RecoveryStage>,
    completed_stages: Vec<RecoveryStage>,
    failed_stages: Vec<RecoveryStageResult>,
    progress_percentage: f64,
    estimated_time_remaining: Option<Duration>,
}

impl RecoveryState {
    pub fn progress(&self) -> f64;
    pub fn current_stage(&self) -> Option<&RecoveryStage>;
    pub fn is_complete(&self) -> bool;
    pub fn has_failed(&self) -> bool;
    pub fn can_rollback(&self) -> bool;
}
```

---

## Circuit Breaker API

### Circuit Breaker Pattern

#### `CircuitBreaker::new(config: CircuitBreakerConfig) -> CircuitBreaker`

Creates a circuit breaker for failure protection.

```rust
use lightning_db::{CircuitBreaker, CircuitBreakerConfig};

let circuit_breaker = CircuitBreaker::new(CircuitBreakerConfig {
    failure_threshold: 5,
    recovery_timeout: Duration::from_secs(30),
    half_open_max_calls: 3,
});

// Use circuit breaker to protect operations
let result = circuit_breaker.call(|| async {
    // Protected operation
    database.write(key, value).await
}).await?;
```

#### Circuit Breaker States

```rust
pub enum CircuitState {
    Closed {
        failure_count: usize,
        last_failure_time: Option<Instant>,
    },
    Open {
        opened_at: Instant,
    },
    HalfOpen {
        success_count: usize,
        failure_count: usize,
    },
}

impl CircuitBreaker {
    pub fn state(&self) -> CircuitState;
    pub fn failure_rate(&self) -> f64;
    pub fn is_open(&self) -> bool;
    pub fn time_until_retry(&self) -> Option<Duration>;
}
```

---

## Security API Enhancements

### Integrity Validation

#### `IntegrityValidator::new(config: ValidationConfig) -> IntegrityValidator`

Creates a data integrity validation system.

```rust
use lightning_db::{IntegrityValidator, ValidationConfig, ChecksumAlgorithm};

let validator = IntegrityValidator::new(ValidationConfig {
    algorithm: ChecksumAlgorithm::Blake3,
    validation_level: ValidationLevel::Comprehensive,
    fail_on_corruption: true,
    enable_repair: true,
});

// Validate specific component
let result = validator.validate_component("btree")?;
match result {
    ValidationResult::Valid => println!("Component is valid"),
    ValidationResult::Corrupted { details, repairable } => {
        if repairable {
            validator.repair_component("btree")?;
        } else {
            return Err(DatabaseError::DataCorruption { 
                component: "btree".to_string(),
                details,
                recovery_possible: false,
            });
        }
    }
}
```

#### Checksum Operations

```rust
pub enum ChecksumAlgorithm {
    Blake3,      // Cryptographically secure, fast
    Xxh64,       // Fast, non-cryptographic
    Crc32,       // Legacy compatibility
}

impl IntegrityValidator {
    pub fn compute_checksum(&self, data: &[u8]) -> Result<Checksum, IntegrityError>;
    pub fn verify_checksum(&self, data: &[u8], expected: &Checksum) -> Result<bool, IntegrityError>;
    pub fn repair_corruption(&self, component: &str) -> Result<RepairResult, IntegrityError>;
}

pub enum RepairResult {
    Repaired { actions_taken: Vec<String> },
    PartiallyRepaired { successful: Vec<String>, failed: Vec<String> },
    RepairFailed { reason: String },
}
```

### Audit Logging

#### `AuditLogger::new(config: AuditConfig) -> AuditLogger`

Creates an audit logging system for security events.

```rust
use lightning_db::{AuditLogger, AuditConfig, AuditEvent, SecurityEventType};

let audit_logger = AuditLogger::new(AuditConfig {
    log_level: AuditLevel::Comprehensive,
    encrypt_logs: true,
    retention_days: 365,
    enable_real_time_alerts: true,
});

// Log security events
audit_logger.log_event(AuditEvent {
    event_type: SecurityEventType::DataAccess,
    resource: "user_data".to_string(),
    operation: "read".to_string(),
    result: OperationResult::Success,
    risk_level: RiskLevel::Low,
    timestamp: SystemTime::now(),
    metadata: HashMap::new(),
});
```

---

## Performance Monitoring API

### Metrics Collection

#### `MetricsCollector::new() -> MetricsCollector`

Creates a performance metrics collection system.

```rust
use lightning_db::{MetricsCollector, PerformanceMetrics};

let metrics = MetricsCollector::new();
metrics.start_collection(Duration::from_secs(10))?;

// Get current metrics
let perf_metrics = metrics.get_performance_metrics();
println!("Read latency P99: {:?}", perf_metrics.read_latency_p99);
println!("Write throughput: {} ops/sec", perf_metrics.write_throughput);
println!("Cache hit rate: {:.2}%", perf_metrics.cache_hit_rate * 100.0);

// Check if metrics meet SLA
if !perf_metrics.meets_sla() {
    println!("Performance SLA violation detected!");
}
```

#### Performance Metrics Structure

```rust
pub struct PerformanceMetrics {
    // Latency metrics
    pub read_latency_p50: Duration,
    pub read_latency_p95: Duration,
    pub read_latency_p99: Duration,
    pub write_latency_p50: Duration,
    pub write_latency_p95: Duration,
    pub write_latency_p99: Duration,
    
    // Throughput metrics
    pub read_throughput: u64,      // ops/sec
    pub write_throughput: u64,     // ops/sec
    pub mixed_workload_throughput: u64,
    
    // Resource metrics
    pub cache_hit_rate: f64,       // 0.0 - 1.0
    pub memory_usage: u64,         // bytes
    pub disk_usage: u64,           // bytes
    pub cpu_usage: f64,            // 0.0 - 1.0
    
    // Reliability metrics
    pub error_rate: f64,           // errors per operation
    pub availability: f64,         // uptime percentage
    pub recovery_time: Duration,   // time to recover from failures
}

impl PerformanceMetrics {
    pub fn meets_sla(&self) -> bool;
    pub fn performance_score(&self) -> f64;  // 0.0 - 100.0
    pub fn identify_bottlenecks(&self) -> Vec<PerformanceBottleneck>;
}
```

---

## Configuration API Extensions

### Enhanced Configuration

#### Security Configuration

```rust
pub struct SecurityConfig {
    pub checksum_algorithm: ChecksumAlgorithm,
    pub validation_level: ValidationLevel,
    pub corruption_action: CorruptionAction,
    pub audit_level: AuditLevel,
    pub enable_encryption: bool,
    pub key_rotation_days: u32,
}

pub enum ValidationLevel {
    Basic,        // Checksum only
    Standard,     // Checksum + structure
    Comprehensive,// Checksum + structure + logical
    Paranoid,     // All validations + continuous monitoring
}

pub enum CorruptionAction {
    Log,          // Log corruption but continue
    Halt,         // Stop database on any corruption
    Repair,       // Attempt automatic repair
    Quarantine,   // Isolate corrupted data
}
```

#### Reliability Configuration

```rust
pub struct ReliabilityConfig {
    pub deadlock_prevention: bool,
    pub circuit_breaker_enabled: bool,
    pub circuit_breaker_failure_threshold: usize,
    pub circuit_breaker_recovery_timeout: Duration,
    pub health_check_interval: Duration,
    pub health_check_timeout: Duration,
    pub auto_remediation_enabled: bool,
    pub self_healing_enabled: bool,
    pub retry_max_attempts: usize,
    pub retry_backoff_strategy: BackoffStrategy,
}

pub enum BackoffStrategy {
    Linear { base_delay: Duration },
    Exponential { base_delay: Duration, max_delay: Duration },
    Fibonacci { base_delay: Duration, max_delay: Duration },
}
```

---

## Migration and Compatibility API

### Version Migration

#### `MigrationManager::new() -> MigrationManager`

Handles database version migrations safely.

```rust
use lightning_db::{MigrationManager, MigrationPlan, DatabaseVersion};

let migration_manager = MigrationManager::new();

// Check if migration is needed
if migration_manager.needs_migration("/path/to/db")? {
    let current_version = migration_manager.detect_version("/path/to/db")?;
    let target_version = DatabaseVersion::Latest;
    
    // Create migration plan
    let plan = migration_manager.create_migration_plan(current_version, target_version)?;
    
    // Execute migration with rollback capability
    match migration_manager.execute_migration(plan).await {
        Ok(result) => println!("Migration successful: {:?}", result),
        Err(e) => {
            println!("Migration failed: {}", e);
            // Automatic rollback is performed
        }
    }
}
```

#### Compatibility Checking

```rust
impl MigrationManager {
    pub fn check_compatibility(&self, db_version: DatabaseVersion, api_version: DatabaseVersion) -> CompatibilityResult;
    pub fn list_required_migrations(&self, from: DatabaseVersion, to: DatabaseVersion) -> Vec<MigrationStep>;
    pub fn estimate_migration_time(&self, plan: &MigrationPlan) -> Duration;
    pub fn validate_migration_safety(&self, plan: &MigrationPlan) -> Result<(), MigrationError>;
}

pub enum CompatibilityResult {
    FullyCompatible,
    BackwardCompatible { warnings: Vec<String> },
    RequiresMigration { steps: Vec<MigrationStep> },
    Incompatible { reason: String },
}
```

---

## Example Usage Patterns

### Production Setup Example

```rust
use lightning_db::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure for production with all reliability features
    let config = LightningDbConfig {
        // Security settings
        integrity_validation_level: IntegrityLevel::Paranoid,
        enable_audit_logging: true,
        fail_on_corruption: true,
        
        // Reliability settings
        enable_health_monitoring: true,
        enable_self_healing: true,
        deadlock_prevention_enabled: true,
        circuit_breaker_enabled: true,
        
        // Performance settings
        cache_size: 8 * 1024 * 1024 * 1024, // 8GB
        compression_enabled: true,
        enable_statistics: true,
        
        ..Default::default()
    };
    
    // Initialize database
    let db = Database::new_with_config(config).await?;
    
    // Start health monitoring
    let health_monitor = HealthMonitor::new(vec![
        Box::new(DataIntegrityCheck::new()),
        Box::new(ResourceCheck::new()),
        Box::new(PerformanceCheck::new()),
    ]);
    let _monitor_handle = health_monitor.start_monitoring(Duration::from_secs(30))?;
    
    // Start metrics collection
    let metrics_collector = MetricsCollector::new();
    metrics_collector.start_collection(Duration::from_secs(10))?;
    
    // Use database with automatic retry and circuit breaker protection
    let auto_recovery = AutoRecoveryManager::new(RecoveryConfig::default());
    let circuit_breaker = CircuitBreaker::new(CircuitBreakerConfig::default());
    
    // Protected database operations
    let result = circuit_breaker.call(|| {
        auto_recovery.execute_with_recovery(
            || async { db.get(b"key").await },
            "database_read"
        )
    }).await?;
    
    println!("Database operation completed successfully");
    
    Ok(())
}
```

---

## Error Handling Best Practices

### Structured Error Handling

```rust
use lightning_db::{DatabaseError, SecurityError, ReliabilityError};

fn handle_database_operation() -> Result<(), ApplicationError> {
    match database.operation() {
        Ok(result) => Ok(result),
        
        // Security errors - halt immediately
        Err(DatabaseError::SecurityError(security_error)) => {
            log::critical!("Security violation detected: {}", security_error);
            alert_security_team(&security_error);
            halt_database_operations();
            Err(ApplicationError::SecurityViolation)
        }
        
        // Data corruption - attempt recovery
        Err(DatabaseError::DataCorruption { component, details, recovery_possible }) => {
            log::error!("Data corruption in {}: {}", component, details);
            
            if recovery_possible {
                match attempt_data_recovery(&component) {
                    Ok(_) => {
                        log::info!("Data recovery successful for {}", component);
                        retry_operation()
                    }
                    Err(e) => {
                        log::error!("Data recovery failed: {}", e);
                        Err(ApplicationError::DataCorruption)
                    }
                }
            } else {
                initiate_emergency_backup();
                Err(ApplicationError::DataCorruption)
            }
        }
        
        // Retryable errors - use exponential backoff
        Err(e) if e.is_retryable() => {
            log::warn!("Retryable error: {}. Attempting retry...", e);
            retry_with_backoff(|| database.operation())
        }
        
        // Non-retryable errors - fail gracefully
        Err(e) => {
            log::error!("Database operation failed: {}", e);
            if let Some(action) = e.suggested_action() {
                log::info!("Suggested action: {}", action);
            }
            Err(ApplicationError::DatabaseError(e))
        }
    }
}
```

---

## API Reference Summary

### New API Features in v1.0

1. **Enhanced Error Handling** (14 new error types)
2. **Health Monitoring System** (continuous monitoring with self-healing)
3. **Auto-Recovery Manager** (automatic retry with rollback)
4. **Circuit Breaker Protection** (cascade failure prevention)
5. **Security API Extensions** (integrity validation, audit logging)
6. **Performance Monitoring** (comprehensive metrics collection)
7. **Migration Support** (safe version upgrades)

### Breaking Changes

- **Error types extended** - New error variants added (backward compatible)
- **Configuration options** - New fields added to config structs (backward compatible)  
- **Health check API** - New trait methods (opt-in implementation)

### Compatibility

- **Rust version**: 1.70+ required
- **API stability**: Stable (follows semantic versioning)
- **Data format**: Backward compatible
- **Migration path**: Automatic migration available

---

**API Reference Version**: 1.0  
**Last Updated**: 2025-01-10  
**Compatibility**: Lightning DB v1.0+