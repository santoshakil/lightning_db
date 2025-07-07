use crate::error::{Error, Result};
use crate::utils::retry::{RetryPolicy, RetryableOperations};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;
use std::time::Duration;

/// Lock acquisition utilities with retry logic
pub struct LockUtils;

impl LockUtils {
    /// Try to acquire a read lock with retry on contention
    pub fn read_with_retry<T>(lock: &RwLock<T>) -> Result<RwLockReadGuard<T>> {
        RetryableOperations::lock_operation(|| {
            lock.try_read().ok_or_else(|| Error::LockFailed {
                resource: "read lock".to_string(),
            })
        })
    }

    /// Try to acquire a write lock with retry on contention
    pub fn write_with_retry<T>(lock: &RwLock<T>) -> Result<RwLockWriteGuard<T>> {
        RetryableOperations::lock_operation(|| {
            lock.try_write().ok_or_else(|| Error::LockFailed {
                resource: "write lock".to_string(),
            })
        })
    }

    /// Try to acquire a read lock on an Arc<RwLock<T>> with retry
    pub fn arc_read_with_retry<T>(lock: &Arc<RwLock<T>>) -> Result<RwLockReadGuard<T>> {
        RetryableOperations::lock_operation(|| {
            lock.try_read().ok_or_else(|| Error::LockFailed {
                resource: "arc read lock".to_string(),
            })
        })
    }

    /// Try to acquire a write lock on an Arc<RwLock<T>> with retry
    pub fn arc_write_with_retry<T>(lock: &Arc<RwLock<T>>) -> Result<RwLockWriteGuard<T>> {
        RetryableOperations::lock_operation(|| {
            lock.try_write().ok_or_else(|| Error::LockFailed {
                resource: "arc write lock".to_string(),
            })
        })
    }

    /// Acquire a read lock with timeout
    pub fn read_with_timeout<T>(lock: &RwLock<T>, timeout: Duration) -> Result<RwLockReadGuard<T>> {
        let policy = RetryPolicy {
            max_attempts: (timeout.as_millis() / 10) as u32, // Try every 10ms
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(10),
            exponential_base: 1.0, // No exponential backoff for lock acquisition
            jitter: true,
        };

        policy.execute(|| {
            lock.try_read()
                .ok_or_else(|| Error::Timeout("Failed to acquire read lock".to_string()))
        })
    }

    /// Acquire a write lock with timeout
    pub fn write_with_timeout<T>(
        lock: &RwLock<T>,
        timeout: Duration,
    ) -> Result<RwLockWriteGuard<T>> {
        let policy = RetryPolicy {
            max_attempts: (timeout.as_millis() / 10) as u32, // Try every 10ms
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(10),
            exponential_base: 1.0, // No exponential backoff for lock acquisition
            jitter: true,
        };

        policy.execute(|| {
            lock.try_write()
                .ok_or_else(|| Error::Timeout("Failed to acquire write lock".to_string()))
        })
    }
}

/// Extension trait for RwLock to add retry methods
pub trait RwLockExt<T> {
    fn read_retry(&self) -> Result<RwLockReadGuard<T>>;
    fn write_retry(&self) -> Result<RwLockWriteGuard<T>>;
    fn read_timeout(&self, timeout: Duration) -> Result<RwLockReadGuard<T>>;
    fn write_timeout(&self, timeout: Duration) -> Result<RwLockWriteGuard<T>>;
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn read_retry(&self) -> Result<RwLockReadGuard<T>> {
        LockUtils::read_with_retry(self)
    }

    fn write_retry(&self) -> Result<RwLockWriteGuard<T>> {
        LockUtils::write_with_retry(self)
    }

    fn read_timeout(&self, timeout: Duration) -> Result<RwLockReadGuard<T>> {
        LockUtils::read_with_timeout(self, timeout)
    }

    fn write_timeout(&self, timeout: Duration) -> Result<RwLockWriteGuard<T>> {
        LockUtils::write_with_timeout(self, timeout)
    }
}

/// Extension trait for Arc<RwLock<T>> to add retry methods
pub trait ArcRwLockExt<T> {
    fn read_retry(&self) -> Result<RwLockReadGuard<T>>;
    fn write_retry(&self) -> Result<RwLockWriteGuard<T>>;
}

impl<T> ArcRwLockExt<T> for Arc<RwLock<T>> {
    fn read_retry(&self) -> Result<RwLockReadGuard<T>> {
        LockUtils::arc_read_with_retry(self)
    }

    fn write_retry(&self) -> Result<RwLockWriteGuard<T>> {
        LockUtils::arc_write_with_retry(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_lock_retry() {
        let lock = Arc::new(RwLock::new(42));
        let lock_clone = lock.clone();

        // Hold write lock in another thread
        let handle = thread::spawn(move || {
            let _guard = lock_clone.write();
            thread::sleep(Duration::from_millis(50));
        });

        // Try to acquire read lock with retry
        thread::sleep(Duration::from_millis(10)); // Let other thread acquire lock first

        // Start trying to get the lock (it will retry while the write lock is held)
        let start = std::time::Instant::now();
        let guard = lock.read_retry();
        let elapsed = start.elapsed();

        // Wait for the write lock holder to finish
        handle.join().unwrap();

        // The read should have succeeded after the write lock was released
        assert!(guard.is_ok(), "Failed to acquire read lock after retries");
        assert_eq!(*guard.unwrap(), 42);

        // Verify that we actually waited and retried (should have taken at least 40ms)
        assert!(
            elapsed.as_millis() >= 40,
            "Lock was acquired too quickly, retry might not be working"
        );
    }

    #[test]
    fn test_lock_timeout() {
        let lock = RwLock::new(42);

        // Should succeed immediately
        let guard = lock.read_timeout(Duration::from_millis(100));
        assert!(guard.is_ok());
        assert_eq!(*guard.unwrap(), 42);
    }
}
