use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum IntegrityError {
    #[error("Checksum mismatch: expected {expected:08x}, computed {computed:08x} at {location}")]
    ChecksumMismatch {
        expected: u32,
        computed: u32,
        location: String,
    },

    #[error("Structural violation in {structure}: {violation}")]
    StructuralViolation {
        structure: String,
        violation: String,
    },

    #[error("Reference integrity violation from {from} to {to}: {issue}")]
    ReferenceIntegrity {
        from: String,
        to: String,
        issue: String,
    },

    #[error("Temporal inconsistency in {operation}: {details}")]
    TemporalInconsistency { operation: String, details: String },

    #[error("Size violation at {location}: expected {expected}, actual {actual}")]
    SizeViolation {
        expected: usize,
        actual: usize,
        location: String,
    },

    #[error(
        "Alignment violation: address {address:08x} requires {required_alignment}-byte alignment"
    )]
    AlignmentViolation {
        address: usize,
        required_alignment: usize,
    },

    #[error("Range violation for {field}: value {value} not in range [{min}, {max}]")]
    RangeViolation {
        value: u64,
        min: u64,
        max: u64,
        field: String,
    },

    #[error("Magic number mismatch at {location}: expected {expected:08x}, found {found:08x}")]
    MagicNumberMismatch {
        expected: u32,
        found: u32,
        location: String,
    },

    #[error("Page type violation: page {page_id} has type {found:?}, expected {expected:?}")]
    PageTypeViolation {
        page_id: u32,
        expected: String,
        found: String,
    },

    #[error("Key ordering violation: key at index {index} violates sort order")]
    KeyOrderingViolation { index: usize, details: String },

    #[error("Child pointer validation failed: {details}")]
    ChildPointerViolation { details: String },

    #[error("Page capacity violation: {details}")]
    PageCapacityViolation { details: String },

    #[error("Transaction state violation: {details}")]
    TransactionStateViolation { details: String },

    #[error("Version consistency violation: {details}")]
    VersionConsistencyViolation { details: String },

    #[error("LSM level violation: {details}")]
    LSMLevelViolation { details: String },

    #[error("WAL integrity violation: {details}")]
    WALIntegrityViolation { details: String },

    #[error("Cache coherency violation: {details}")]
    CacheCoherencyViolation { details: String },

    #[error("Corruption detected at {location}: {corruption_type}")]
    CorruptionDetected {
        location: String,
        corruption_type: String,
    },

    #[error("Critical invariant violated: {invariant}")]
    CriticalInvariantViolation { invariant: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityViolation {
    pub error: IntegrityError,
    pub severity: ViolationSeverity,
    pub location: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub context: HashMap<String, String>,
    pub suggested_action: Option<String>,
}

impl std::fmt::Display for IntegrityViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{:?}] {} at {} ({})",
            self.severity,
            self.error,
            self.location,
            self.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ViolationSeverity {
    Info,
    Warning,
    Error,
    Critical,
    Fatal,
}

#[derive(Debug, Clone)]
pub enum ValidationResult<T> {
    Valid(T),
    Invalid(Vec<IntegrityViolation>),
    Error(IntegrityError),
}

impl<T> ValidationResult<T> {
    pub fn is_valid(&self) -> bool {
        matches!(self, ValidationResult::Valid(_))
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self, ValidationResult::Invalid(_))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, ValidationResult::Error(_))
    }

    pub fn map<U, F>(self, f: F) -> ValidationResult<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            ValidationResult::Valid(value) => ValidationResult::Valid(f(value)),
            ValidationResult::Invalid(violations) => ValidationResult::Invalid(violations),
            ValidationResult::Error(error) => ValidationResult::Error(error),
        }
    }

    pub fn and_then<U, F>(self, f: F) -> ValidationResult<U>
    where
        F: FnOnce(T) -> ValidationResult<U>,
    {
        match self {
            ValidationResult::Valid(value) => f(value),
            ValidationResult::Invalid(violations) => ValidationResult::Invalid(violations),
            ValidationResult::Error(error) => ValidationResult::Error(error),
        }
    }

    pub fn unwrap(self) -> T {
        match self {
            ValidationResult::Valid(value) => value,
            ValidationResult::Invalid(violations) => {
                tracing::error!(
                    "Called unwrap on Invalid ValidationResult: {:?}",
                    violations
                );
                std::panic!(
                    "Called unwrap on Invalid ValidationResult with {} violations. Use unwrap_or, expect, or handle errors explicitly.",
                    violations.len()
                )
            }
            ValidationResult::Error(error) => {
                tracing::error!("Called unwrap on Error ValidationResult: {:?}", error);
                std::panic!(
                    "Called unwrap on Error ValidationResult: {}. Use unwrap_or, expect, or handle errors explicitly.",
                    error
                )
            }
        }
    }

    /// Safe alternative to unwrap that returns a Result
    pub fn into_result(self) -> crate::core::error::Result<T>
    where
        T: std::fmt::Debug,
    {
        match self {
            ValidationResult::Valid(value) => Ok(value),
            ValidationResult::Invalid(violations) => {
                Err(crate::core::error::Error::ValidationError(format!(
                    "Validation failed with {} violations: {:?}",
                    violations.len(),
                    violations
                )))
            }
            ValidationResult::Error(error) => Err(crate::core::error::Error::ValidationError(
                format!("Validation error: {}", error),
            )),
        }
    }

    pub fn unwrap_or(self, default: T) -> T {
        match self {
            ValidationResult::Valid(value) => value,
            _ => default,
        }
    }
}

impl<T> From<Result<T, IntegrityError>> for ValidationResult<T> {
    fn from(result: Result<T, IntegrityError>) -> Self {
        match result {
            Ok(value) => ValidationResult::Valid(value),
            Err(error) => ValidationResult::Error(error),
        }
    }
}

pub fn create_violation(
    error: IntegrityError,
    severity: ViolationSeverity,
    location: &str,
    context: Option<HashMap<String, String>>,
) -> IntegrityViolation {
    IntegrityViolation {
        error,
        severity,
        location: location.to_string(),
        timestamp: chrono::Utc::now(),
        context: context.unwrap_or_default(),
        suggested_action: None,
    }
}

pub fn create_critical_violation(
    error: IntegrityError,
    location: &str,
    suggested_action: &str,
) -> IntegrityViolation {
    IntegrityViolation {
        error,
        severity: ViolationSeverity::Critical,
        location: location.to_string(),
        timestamp: chrono::Utc::now(),
        context: HashMap::new(),
        suggested_action: Some(suggested_action.to_string()),
    }
}

pub fn combine_results<T>(results: Vec<ValidationResult<T>>) -> ValidationResult<Vec<T>> {
    let mut values = Vec::new();
    let mut violations = Vec::new();

    for result in results {
        match result {
            ValidationResult::Valid(value) => values.push(value),
            ValidationResult::Invalid(mut v) => violations.append(&mut v),
            ValidationResult::Error(error) => return ValidationResult::Error(error),
        }
    }

    if violations.is_empty() {
        ValidationResult::Valid(values)
    } else {
        ValidationResult::Invalid(violations)
    }
}
