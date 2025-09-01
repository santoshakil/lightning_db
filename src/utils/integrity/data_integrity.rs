use super::{
    error_types::{IntegrityError, IntegrityViolation, ValidationResult, ViolationSeverity},
    validation_config::{IntegrityConfig, OperationType, ValidationContext},
};
use crate::core::error::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Core data integrity validation framework
pub struct DataIntegrityValidator {
    config: IntegrityConfig,
    metrics: Arc<IntegrityMetrics>,
    violation_tracker: Arc<RwLock<ViolationTracker>>,
}

impl DataIntegrityValidator {
    pub fn new(config: IntegrityConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(IntegrityMetrics::new()),
            violation_tracker: Arc::new(RwLock::new(ViolationTracker::new())),
        }
    }

    /// Validate data integrity for critical path operations
    pub fn validate_critical_path<T>(
        &self,
        operation: &str,
        location: &str,
        operation_type: OperationType,
        validation_fn: impl FnOnce(&ValidationContext) -> ValidationResult<T>,
    ) -> Result<T> {
        if !self.config.should_validate_operation(operation_type) {
            // Skip validation based on configuration
            let dummy_context = ValidationContext::new(operation, location);
            return match validation_fn(&dummy_context) {
                ValidationResult::Valid(result) => Ok(result),
                ValidationResult::Invalid(_) => {
                    // In skip mode, we don't fail on validation errors
                    Err(Error::ValidationFailed("Validation skipped".to_string()))
                }
                ValidationResult::Error(err) => Err(Error::ValidationFailed(err.to_string())),
            };
        }

        let start = Instant::now();
        let context = ValidationContext::new(operation, location)
            .with_metadata("operation_type", &format!("{:?}", operation_type));

        let timeout = self.config.get_timeout(operation_type);
        let result = self.execute_with_timeout(validation_fn, &context, timeout);

        let duration = start.elapsed();
        self.record_validation_metrics(operation, operation_type, &result, duration);

        match result {
            ValidationResult::Valid(value) => Ok(value),
            ValidationResult::Invalid(violations) => {
                self.handle_violations(violations, operation, location)?;
                Err(Error::ValidationFailed(format!(
                    "Data integrity validation failed for {} at {}",
                    operation, location
                )))
            }
            ValidationResult::Error(error) => {
                tracing::error!(
                    "Data integrity validation error in {} at {}: {}",
                    operation,
                    location,
                    error
                );
                Err(Error::ValidationFailed(error.to_string()))
            }
        }
    }

    /// Execute validation with timeout
    fn execute_with_timeout<T>(
        &self,
        validation_fn: impl FnOnce(&ValidationContext) -> ValidationResult<T>,
        context: &ValidationContext,
        _timeout: Duration,
    ) -> ValidationResult<T> {
        // For now, execute directly without timeout
        // In a production implementation, you'd use tokio::time::timeout or similar
        validation_fn(context)
    }

    /// Handle validation violations
    fn handle_violations(
        &self,
        violations: Vec<IntegrityViolation>,
        operation: &str,
        location: &str,
    ) -> Result<()> {
        let mut tracker = self.violation_tracker.write().map_err(|_| {
            Error::Internal("Failed to acquire violation tracker lock".to_string())
        })?;

        let critical_violations: Vec<_> = violations
            .iter()
            .filter(|v| v.severity >= ViolationSeverity::Critical)
            .collect();

        tracker.record_violations(&violations);

        // Log violations based on severity
        for violation in &violations {
            match violation.severity {
                ViolationSeverity::Info => tracing::info!("{}", violation),
                ViolationSeverity::Warning => tracing::warn!("{}", violation),
                ViolationSeverity::Error => tracing::error!("{}", violation),
                ViolationSeverity::Critical => tracing::error!("CRITICAL: {}", violation),
                ViolationSeverity::Fatal => tracing::error!("FATAL: {}", violation),
            }
        }

        // Check if we should trigger emergency procedures
        if !critical_violations.is_empty() || tracker.should_trigger_emergency_stop() {
            tracing::error!(
                "Critical data integrity violations detected in {} at {}: {} violations",
                operation,
                location,
                critical_violations.len()
            );

            if self.config.enable_alerts {
                self.send_critical_alert(operation, location, &critical_violations);
            }

            // For critical violations, we should consider emergency shutdown
            if critical_violations
                .iter()
                .any(|v| v.severity == ViolationSeverity::Fatal)
            {
                return Err(Error::CorruptionUnrecoverable(format!(
                    "Fatal data integrity violation in {} at {}",
                    operation, location
                )));
            }
        }

        Ok(())
    }

    /// Send critical alert for severe integrity violations
    fn send_critical_alert(
        &self,
        operation: &str,
        location: &str,
        violations: &[&IntegrityViolation],
    ) {
        tracing::error!(
            "ALERT: Critical data integrity violations detected!\n\
             Operation: {}\n\
             Location: {}\n\
             Violations: {}",
            operation,
            location,
            violations.len()
        );
    }

    /// Record validation metrics
    fn record_validation_metrics<T>(
        &self,
        operation: &str,
        operation_type: OperationType,
        result: &ValidationResult<T>,
        duration: Duration,
    ) {
        self.metrics.record_operation(operation, operation_type, result.is_valid(), duration);

        if self.config.collect_metrics {
            tracing::debug!(
                "Integrity validation: {} ({:?}) - {} in {:?}",
                operation,
                operation_type,
                if result.is_valid() { "PASS" } else { "FAIL" },
                duration
            );
        }
    }

    /// Get current validation metrics
    pub fn get_metrics(&self) -> Arc<IntegrityMetrics> {
        self.metrics.clone()
    }

    /// Get violation summary
    pub fn get_violation_summary(&self) -> Result<ViolationSummary> {
        let tracker = self.violation_tracker.read().map_err(|_| {
            Error::Internal("Failed to acquire violation tracker lock".to_string())
        })?;
        Ok(tracker.get_summary())
    }

    /// Clear violation history
    pub fn clear_violation_history(&self) -> Result<()> {
        let mut tracker = self.violation_tracker.write().map_err(|_| {
            Error::Internal("Failed to acquire violation tracker lock".to_string())
        })?;
        tracker.clear_history();
        Ok(())
    }
}

/// Track validation violations over time
#[derive(Debug)]
struct ViolationTracker {
    violations: Vec<IntegrityViolation>,
    violation_counts: HashMap<String, usize>,
    critical_violation_count: usize,
    last_violation_time: Option<chrono::DateTime<chrono::Utc>>,
    max_violations: usize,
}

impl ViolationTracker {
    fn new() -> Self {
        Self {
            violations: Vec::new(),
            violation_counts: HashMap::new(),
            critical_violation_count: 0,
            last_violation_time: None,
            max_violations: 10000,
        }
    }

    fn record_violations(&mut self, violations: &[IntegrityViolation]) {
        for violation in violations {
            if violation.severity >= ViolationSeverity::Critical {
                self.critical_violation_count += 1;
            }

            let violation_key = format!("{:?}:{}", violation.severity, violation.location);
            *self.violation_counts.entry(violation_key).or_insert(0) += 1;
            
            self.violations.push(violation.clone());
            self.last_violation_time = Some(violation.timestamp);
        }

        // Keep violations list bounded
        if self.violations.len() > self.max_violations {
            self.violations.drain(0..self.violations.len() - self.max_violations);
        }
    }

    fn should_trigger_emergency_stop(&self) -> bool {
        // Trigger emergency stop if too many critical violations
        self.critical_violation_count > 10 ||
        // Or if we're getting too many violations in a short time period
        (self.violations.len() > 100 && 
         self.last_violation_time.is_some_and(|t| {
             chrono::Utc::now().signed_duration_since(t).num_minutes() < 5
         }))
    }

    fn get_summary(&self) -> ViolationSummary {
        let mut severity_counts = HashMap::new();
        for violation in &self.violations {
            *severity_counts.entry(violation.severity).or_insert(0) += 1;
        }

        ViolationSummary {
            total_violations: self.violations.len(),
            critical_violations: self.critical_violation_count,
            severity_breakdown: severity_counts,
            most_recent_violation: self.last_violation_time,
            violation_rate: self.calculate_violation_rate(),
        }
    }

    fn calculate_violation_rate(&self) -> f64 {
        if self.violations.is_empty() {
            return 0.0;
        }

        let now = chrono::Utc::now();
        let hour_ago = now - chrono::Duration::hours(1);
        
        let recent_violations = self.violations
            .iter()
            .filter(|v| v.timestamp > hour_ago)
            .count();

        recent_violations as f64 / 3600.0 // violations per second
    }

    fn clear_history(&mut self) {
        self.violations.clear();
        self.violation_counts.clear();
        self.critical_violation_count = 0;
        self.last_violation_time = None;
    }
}

/// Summary of integrity violations
#[derive(Debug, Clone)]
pub struct ViolationSummary {
    pub total_violations: usize,
    pub critical_violations: usize,
    pub severity_breakdown: HashMap<ViolationSeverity, usize>,
    pub most_recent_violation: Option<chrono::DateTime<chrono::Utc>>,
    pub violation_rate: f64, // violations per second
}

/// Integrity validation metrics
pub struct IntegrityMetrics {
    total_validations: AtomicU64,
    successful_validations: AtomicU64,
    failed_validations: AtomicU64,
    total_validation_time: AtomicU64, // in microseconds
    operations: RwLock<HashMap<String, OperationMetrics>>,
}

#[derive(Debug, Clone)]
struct OperationMetrics {
    count: u64,
    success_count: u64,
    total_duration_micros: u64,
    max_duration_micros: u64,
    min_duration_micros: u64,
}

impl IntegrityMetrics {
    fn new() -> Self {
        Self {
            total_validations: AtomicU64::new(0),
            successful_validations: AtomicU64::new(0),
            failed_validations: AtomicU64::new(0),
            total_validation_time: AtomicU64::new(0),
            operations: RwLock::new(HashMap::new()),
        }
    }

    fn record_operation(
        &self,
        operation: &str,
        operation_type: OperationType,
        success: bool,
        duration: Duration,
    ) {
        self.total_validations.fetch_add(1, Ordering::Relaxed);
        
        if success {
            self.successful_validations.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_validations.fetch_add(1, Ordering::Relaxed);
        }

        let duration_micros = duration.as_micros() as u64;
        self.total_validation_time.fetch_add(duration_micros, Ordering::Relaxed);

        // Update per-operation metrics
        if let Ok(mut operations) = self.operations.write() {
            let key = format!("{}:{:?}", operation, operation_type);
            let metrics = operations.entry(key).or_insert(OperationMetrics {
                count: 0,
                success_count: 0,
                total_duration_micros: 0,
                max_duration_micros: 0,
                min_duration_micros: u64::MAX,
            });

            metrics.count += 1;
            if success {
                metrics.success_count += 1;
            }
            metrics.total_duration_micros += duration_micros;
            metrics.max_duration_micros = metrics.max_duration_micros.max(duration_micros);
            metrics.min_duration_micros = metrics.min_duration_micros.min(duration_micros);
        }
    }

    pub fn get_total_validations(&self) -> u64 {
        self.total_validations.load(Ordering::Relaxed)
    }

    pub fn get_success_rate(&self) -> f64 {
        let total = self.total_validations.load(Ordering::Relaxed);
        if total == 0 {
            return 1.0;
        }
        let successful = self.successful_validations.load(Ordering::Relaxed);
        successful as f64 / total as f64
    }

    pub fn get_average_validation_time(&self) -> Duration {
        let total = self.total_validations.load(Ordering::Relaxed);
        if total == 0 {
            return Duration::ZERO;
        }
        let total_time = self.total_validation_time.load(Ordering::Relaxed);
        Duration::from_micros(total_time / total)
    }
}

/// Report containing integrity validation results
#[derive(Debug, Clone)]
pub struct IntegrityReport {
    pub validation_time: chrono::DateTime<chrono::Utc>,
    pub operations_validated: usize,
    pub violations_found: Vec<IntegrityViolation>,
    pub metrics_summary: String,
    pub recommendations: Vec<String>,
}

impl IntegrityReport {
    pub fn new() -> Self {
        Self {
            validation_time: chrono::Utc::now(),
            operations_validated: 0,
            violations_found: Vec::new(),
            metrics_summary: String::new(),
            recommendations: Vec::new(),
        }
    }

    pub fn add_violation(&mut self, violation: IntegrityViolation) {
        self.violations_found.push(violation);
    }

    pub fn has_critical_violations(&self) -> bool {
        self.violations_found
            .iter()
            .any(|v| v.severity >= ViolationSeverity::Critical)
    }

    pub fn get_violation_count_by_severity(&self, severity: ViolationSeverity) -> usize {
        self.violations_found
            .iter()
            .filter(|v| v.severity == severity)
            .count()
    }
}

/// Helper functions for common validation patterns
pub mod validators {
    use super::*;

    /// Validate checksum match
    pub fn validate_checksum(
        data: &[u8], 
        expected_checksum: u32, 
        location: &str
    ) -> ValidationResult<()> {
        let computed_checksum = crc32fast::hash(data);
        
        if computed_checksum == expected_checksum {
            ValidationResult::Valid(())
        } else {
            let violation = super::super::error_types::create_critical_violation(
                IntegrityError::ChecksumMismatch {
                    expected: expected_checksum,
                    computed: computed_checksum,
                    location: location.to_string(),
                },
                location,
                "Verify data source and consider repair or restore from backup",
            );
            ValidationResult::Invalid(vec![violation])
        }
    }

    /// Validate data size within bounds
    pub fn validate_size_bounds(
        actual_size: usize,
        expected_size: usize,
        tolerance: usize,
        location: &str,
    ) -> ValidationResult<()> {
        if actual_size.abs_diff(expected_size) <= tolerance {
            ValidationResult::Valid(())
        } else {
            let violation = super::super::error_types::create_violation(
                IntegrityError::SizeViolation {
                    expected: expected_size,
                    actual: actual_size,
                    location: location.to_string(),
                },
                ViolationSeverity::Error,
                location,
                None,
            );
            ValidationResult::Invalid(vec![violation])
        }
    }

    /// Validate magic number
    pub fn validate_magic_number(
        data: &[u8],
        expected_magic: u32,
        location: &str,
    ) -> ValidationResult<()> {
        if data.len() < 4 {
            let violation = super::super::error_types::create_critical_violation(
                IntegrityError::SizeViolation {
                    expected: 4,
                    actual: data.len(),
                    location: location.to_string(),
                },
                location,
                "Data structure is corrupted - insufficient data for magic number",
            );
            return ValidationResult::Invalid(vec![violation]);
        }

        let found_magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        
        if found_magic == expected_magic {
            ValidationResult::Valid(())
        } else {
            let violation = super::super::error_types::create_critical_violation(
                IntegrityError::MagicNumberMismatch {
                    expected: expected_magic,
                    found: found_magic,
                    location: location.to_string(),
                },
                location,
                "File or data structure is corrupted - magic number mismatch",
            );
            ValidationResult::Invalid(vec![violation])
        }
    }
}
