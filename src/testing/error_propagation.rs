//! Simple test module for recovery error propagation
//! 
//! This module contains basic tests that verify error propagation works
//! correctly without depending on the full database functionality.

#[cfg(test)]
mod tests {
    use crate::core::error::Error;
    use crate::core::recovery::{RecoveryStage, RecoveryState};
    use std::time::Duration;

    #[test]
    fn test_recovery_error_types_basic() {
        // Test critical recovery failure detection
        let error = Error::RecoveryImpossible {
            reason: "WAL corrupted beyond repair".to_string(),
            suggested_action: "Restore from backup".to_string(),
        };
        
        assert!(error.is_critical_recovery_failure());
        assert!(!error.is_recoverable());
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_partial_recovery_failure() {
        let partial_error = Error::PartialRecoveryFailure {
            completed_stages: vec!["Initialization".to_string(), "Lock Acquisition".to_string()],
            failed_stage: "WAL Validation".to_string(),
            cause: Box::new(Error::WalCorruption { 
                offset: 1024, 
                reason: "Invalid checksum".to_string() 
            }),
            rollback_available: true,
        };
        
        assert!(!partial_error.is_critical_recovery_failure());
        assert!(partial_error.is_recoverable());
        
        // Test error code assignment
        assert_eq!(partial_error.error_code(), -74);
    }

    #[test]
    fn test_resource_errors() {
        let resource_error = Error::InsufficientResources {
            resource: "memory".to_string(),
            required: "100MB".to_string(),
            available: "50MB".to_string(),
        };
        
        assert!(resource_error.is_retryable());
        assert!(resource_error.is_critical_recovery_failure());
        assert_eq!(resource_error.error_code(), -72);
    }

    #[test]
    fn test_database_locked_error() {
        let lock_error = Error::DatabaseLocked {
            lock_holder: "PID 1234".to_string(),
            suggested_action: "Wait for process to complete or kill PID 1234".to_string(),
        };
        
        assert!(lock_error.is_retryable());
        assert!(lock_error.is_critical_recovery_failure());
        assert_eq!(lock_error.error_code(), -73);
    }

    #[test] 
    fn test_recovery_timeout_error() {
        let timeout_error = Error::RecoveryTimeout {
            stage: "WAL Recovery".to_string(),
            timeout_seconds: 300,
            progress: 0.75,
        };
        
        assert!(timeout_error.is_recoverable());
        assert!(timeout_error.is_retryable());
        assert_eq!(timeout_error.error_code(), -79);
    }

    #[test]
    fn test_recovery_stage_properties() {
        // Test stage dependencies
        let config_stage = RecoveryStage::ConfigValidation;
        let deps = config_stage.dependencies();
        assert_eq!(deps, vec![RecoveryStage::LockAcquisition]);
        
        // Test critical stages
        assert!(RecoveryStage::LockAcquisition.is_critical());
        assert!(RecoveryStage::ConfigValidation.is_critical());
        assert!(!RecoveryStage::ResourceCleanup.is_critical());
        
        // Test rollback support
        assert!(RecoveryStage::TransactionRecovery.supports_rollback());
        assert!(RecoveryStage::IndexReconstruction.supports_rollback());
        assert!(!RecoveryStage::LockAcquisition.supports_rollback());
    }

    #[test]
    fn test_recovery_state_tracking() {
        let state = RecoveryState::new(Duration::from_secs(60));
        
        // Test initial state
        assert!(!state.is_stage_completed(&RecoveryStage::Initialization));
        assert!(!state.is_aborted());
        
        // Test abort functionality
        state.abort();
        assert!(state.is_aborted());
    }

    #[test]
    fn test_error_display_and_formatting() {
        let error = Error::PartialRecoveryFailure {
            completed_stages: vec!["Init".to_string()],
            failed_stage: "WAL".to_string(),
            cause: Box::new(Error::Generic("test cause".to_string())),
            rollback_available: true,
        };
        
        let error_string = error.to_string();
        assert!(error_string.contains("Partial recovery failure"));
        assert!(error_string.contains("WAL"));
    }

    #[test]
    fn test_error_context_propagation() {
        
        
        let base_error: Result<(), Error> = Err(Error::Generic("base error".to_string()));
        let contextualized = base_error.map_err(|e| Error::Generic(format!("During recovery: {}", e)));
        
        assert!(contextualized.is_err());
        if let Err(err) = contextualized {
            let err_string = err.to_string();
            assert!(err_string.contains("During recovery"));
            assert!(err_string.contains("base error"));
        }
    }

    #[test]
    fn test_recovery_verification_error() {
        let verification_error = Error::RecoveryVerificationFailed {
            check_name: "Database Consistency".to_string(),
            details: "Checksum mismatch detected".to_string(),
            critical: true,
        };
        
        assert!(verification_error.is_critical_recovery_failure());
        assert_eq!(verification_error.error_code(), -83);
        
        // Test non-critical verification error
        let non_critical_error = Error::RecoveryVerificationFailed {
            check_name: "Performance Check".to_string(),
            details: "Suboptimal performance detected".to_string(),
            critical: false,
        };
        
        assert!(!non_critical_error.is_critical_recovery_failure());
    }

    #[test]
    fn test_rollback_error() {
        let rollback_error = Error::RecoveryRollbackFailed {
            stage: "Transaction Recovery".to_string(),
            reason: "Cannot undo committed transaction".to_string(),
            manual_intervention_needed: true,
        };
        
        assert!(rollback_error.is_critical_recovery_failure());
        assert_eq!(rollback_error.error_code(), -81);
    }

    #[test]
    fn test_dependency_error() {
        let dependency_error = Error::RecoveryStageDependencyFailed {
            stage: "Index Reconstruction".to_string(),
            dependency: "Transaction Recovery".to_string(),
            dependency_error: Box::new(Error::Generic("Dependency failed".to_string())),
        };
        
        assert_eq!(dependency_error.error_code(), -82);
        
        let error_string = dependency_error.to_string();
        assert!(error_string.contains("Index Reconstruction"));
        assert!(error_string.contains("Transaction Recovery"));
    }

    #[test]
    fn test_all_recovery_error_codes_unique() {
        // Test that all recovery error codes are in the expected range and unique
        let errors = vec![
            Error::RecoveryImpossible { reason: "".to_string(), suggested_action: "".to_string() },
            Error::WalCorrupted { details: "".to_string(), suggested_action: "".to_string() },
            Error::InsufficientResources { resource: "".to_string(), required: "".to_string(), available: "".to_string() },
            Error::DatabaseLocked { lock_holder: "".to_string(), suggested_action: "".to_string() },
            Error::PartialRecoveryFailure { completed_stages: vec![], failed_stage: "".to_string(), cause: Box::new(Error::Generic("".to_string())), rollback_available: false },
            Error::InconsistentState { description: "".to_string(), diagnostics: "".to_string(), recovery_suggestions: vec![] },
            Error::RecoveryConfigurationError { setting: "".to_string(), issue: "".to_string(), fix: "".to_string() },
            Error::RecoveryPermissionError { path: "".to_string(), required_permissions: "".to_string() },
            Error::RecoveryDependencyError { dependency: "".to_string(), issue: "".to_string() },
            Error::RecoveryTimeout { stage: "".to_string(), timeout_seconds: 0, progress: 0.0 },
            Error::RecoveryProgress { message: "".to_string() },
            Error::RecoveryRollbackFailed { stage: "".to_string(), reason: "".to_string(), manual_intervention_needed: false },
            Error::RecoveryStageDependencyFailed { stage: "".to_string(), dependency: "".to_string(), dependency_error: Box::new(Error::Generic("".to_string())) },
            Error::RecoveryVerificationFailed { check_name: "".to_string(), details: "".to_string(), critical: false },
        ];
        
        let mut codes = std::collections::HashSet::new();
        for error in errors {
            let code = error.error_code();
            assert!((-83..=-70).contains(&code), "Recovery error code {} out of expected range -83 to -70", code);
            assert!(codes.insert(code), "Duplicate error code: {}", code);
        }
        
        // Should have 14 unique error codes
        assert_eq!(codes.len(), 14);
    }
}
