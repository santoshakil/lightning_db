use std::time::Duration;
use std::thread;
use crate::{Error, Result};

#[derive(Debug, Clone)]
pub struct RecoveryStrategy {
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub exponential_backoff: bool,
    pub jitter: bool,
}

impl Default for RecoveryStrategy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            exponential_backoff: true,
            jitter: true,
        }
    }
}

pub struct ErrorRecovery;

impl ErrorRecovery {
    pub fn is_transient(error: &Error) -> bool {
        matches!(error,
            Error::LockFailed { .. } |
            Error::LockTimeout(_) |
            Error::Timeout(_) |
            Error::Deadlock(_) |
            Error::ResourceExhausted { .. } |
            Error::InsufficientResources { .. }
        )
    }

    pub fn retry_with_strategy<F, T>(
        strategy: &RecoveryStrategy,
        mut operation: F,
    ) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let mut retries = 0;
        let mut delay = strategy.retry_delay;

        loop {
            match operation() {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if !Self::is_transient(&error) || retries >= strategy.max_retries {
                        return Err(error);
                    }

                    retries += 1;

                    // Apply jitter to avoid thundering herd
                    let actual_delay = if strategy.jitter {
                        let jitter_ms = fastrand::u64(0..delay.as_millis() as u64 / 2);
                        delay + Duration::from_millis(jitter_ms)
                    } else {
                        delay
                    };

                    thread::sleep(actual_delay);

                    // Apply exponential backoff
                    if strategy.exponential_backoff {
                        delay *= 2;
                    }
                }
            }
        }
    }

    pub fn retry<F, T>(operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        Self::retry_with_strategy(&RecoveryStrategy::default(), operation)
    }

    pub fn suggest_recovery_action(error: &Error) -> String {
        match error {
            Error::DatabaseLocked { suggested_action, .. } => suggested_action.clone(),
            Error::LockFailed { .. } | Error::LockTimeout(_) =>
                "Retry the operation after a short delay".to_string(),
            Error::Deadlock(_) =>
                "Abort one transaction and retry".to_string(),
            Error::ResourceExhausted { resource } =>
                format!("Free up {} resources or increase limits", resource),
            Error::InsufficientResources { resource, .. } =>
                format!("Increase {} allocation or reduce usage", resource),
            Error::Memory =>
                "Free up memory or increase allocation".to_string(),
            Error::Corruption(_) | Error::InvalidDatabase =>
                "Run database recovery or restore from backup".to_string(),
            Error::WalCorruption { .. } =>
                "Truncate WAL and replay from last checkpoint".to_string(),
            Error::ChecksumMismatch { .. } =>
                "Verify data integrity and consider recovery".to_string(),
            Error::TransactionLimitReached { .. } =>
                "Wait for active transactions to complete".to_string(),
            _ => "Check error details and retry if appropriate".to_string(),
        }
    }
}