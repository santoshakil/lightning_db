#[cfg(test)]
use crate::core::error::Error;
use crate::core::error::Result;
use std::thread;
use std::time::Duration;
use tracing::warn;

/// Retry policy for operations
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub exponential_base: f64,
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            exponential_base: 2.0,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Execute operation with retry logic
    pub fn execute<F, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let mut attempt = 0;
        let mut delay = self.initial_delay;

        loop {
            attempt += 1;

            match operation() {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if !err.is_recoverable() || attempt >= self.max_attempts {
                        return Err(err);
                    }

                    warn!(
                        "Operation failed (attempt {}/{}): {}, retrying in {:?}",
                        attempt, self.max_attempts, err, delay
                    );

                    thread::sleep(delay);

                    // Calculate next delay with exponential backoff
                    delay = self.calculate_next_delay(delay, attempt);
                }
            }
        }
    }

    /// Execute async operation with retry logic
    pub async fn execute_async<F, T, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0;
        let mut delay = self.initial_delay;

        loop {
            attempt += 1;

            match operation().await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if !err.is_recoverable() || attempt >= self.max_attempts {
                        return Err(err);
                    }

                    warn!(
                        "Async operation failed (attempt {}/{}): {}, retrying in {:?}",
                        attempt, self.max_attempts, err, delay
                    );

                    tokio::time::sleep(delay).await;

                    // Calculate next delay with exponential backoff
                    delay = self.calculate_next_delay(delay, attempt);
                }
            }
        }
    }

    fn calculate_next_delay(&self, current_delay: Duration, attempt: u32) -> Duration {
        let mut next_delay = Duration::from_secs_f64(
            current_delay.as_secs_f64() * self.exponential_base.powf(attempt as f64 - 1.0),
        );

        // Apply jitter if enabled
        #[cfg(feature = "rand")]
        if self.jitter {
            use rand::Rng;
            let mut rng = rand::rng();
            let jitter_factor = rng.random_range(0.5..1.5);
            next_delay = Duration::from_secs_f64(next_delay.as_secs_f64() * jitter_factor);
        }

        // Cap at max delay
        if next_delay > self.max_delay {
            next_delay = self.max_delay;
        }

        next_delay
    }
}

/// Retry specific operations with custom policies
pub struct RetryableOperations;

impl RetryableOperations {
    /// Retry file operations
    pub fn file_operation<F, T>(operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_delay: Duration::from_millis(50),
            ..Default::default()
        };
        policy.execute(operation)
    }

    /// Retry network operations
    pub fn network_operation<F, T>(operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            ..Default::default()
        };
        policy.execute(operation)
    }

    /// Retry lock acquisition
    pub fn lock_operation<F, T>(operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let policy = RetryPolicy {
            max_attempts: 20,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            exponential_base: 1.5,
            ..Default::default()
        };
        policy.execute(operation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn test_retry_success() {
        let counter = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let result = policy.execute(|| {
            let count = counter.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                Err(Error::Timeout("Simulated timeout".to_string()))
            } else {
                Ok(42)
            }
        });

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_retry_exhausted() {
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let result: Result<()> = policy.execute(|| Err(Error::Timeout("Always fails".to_string())));

        assert!(result.is_err());
    }

    #[test]
    fn test_non_recoverable_error() {
        let policy = RetryPolicy::default();

        let result: Result<()> = policy.execute(|| Err(Error::CorruptedPage));

        assert!(result.is_err());
    }
}
