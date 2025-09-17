use crate::core::error::{Error, Result};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Extension trait for `Arc<RwLock<T>>` with timeout support
pub trait TimeoutRwLockExt<T> {
    fn read_timeout(&self, timeout: Duration) -> Result<RwLockReadGuard<'_, T>>;
    fn write_timeout(&self, timeout: Duration) -> Result<RwLockWriteGuard<'_, T>>;
}

impl<T> TimeoutRwLockExt<T> for Arc<RwLock<T>> {
    fn read_timeout(&self, timeout: Duration) -> Result<RwLockReadGuard<'_, T>> {
        let start = Instant::now();
        let mut backoff = Duration::from_micros(1);
        let max_backoff = Duration::from_millis(1);

        loop {
            if let Some(guard) = self.try_read() {
                return Ok(guard);
            }

            if start.elapsed() >= timeout {
                return Err(Error::Timeout(format!(
                    "Failed to acquire read lock within {:?}",
                    timeout
                )));
            }

            thread::sleep(backoff);
            backoff = (backoff * 2).min(max_backoff);
        }
    }

    fn write_timeout(&self, timeout: Duration) -> Result<RwLockWriteGuard<'_, T>> {
        let start = Instant::now();
        let mut backoff = Duration::from_micros(1);
        let max_backoff = Duration::from_millis(1);

        loop {
            if let Some(guard) = self.try_write() {
                return Ok(guard);
            }

            if start.elapsed() >= timeout {
                return Err(Error::Timeout(format!(
                    "Failed to acquire write lock within {:?}",
                    timeout
                )));
            }

            thread::sleep(backoff);
            backoff = (backoff * 2).min(max_backoff);
        }
    }
}

/// Extension trait for `Arc<Mutex<T>>` with timeout support
pub trait TimeoutMutexExt<T> {
    fn lock_timeout(&self, timeout: Duration) -> Result<MutexGuard<'_, T>>;
}

impl<T> TimeoutMutexExt<T> for Arc<Mutex<T>> {
    fn lock_timeout(&self, timeout: Duration) -> Result<MutexGuard<'_, T>> {
        let start = Instant::now();
        let mut backoff = Duration::from_micros(1);
        let max_backoff = Duration::from_millis(1);

        loop {
            if let Some(guard) = self.try_lock() {
                return Ok(guard);
            }

            if start.elapsed() >= timeout {
                return Err(Error::Timeout(format!(
                    "Failed to acquire mutex lock within {:?}",
                    timeout
                )));
            }

            thread::sleep(backoff);
            backoff = (backoff * 2).min(max_backoff);
        }
    }
}

/// Lock ordering utilities to prevent deadlocks
pub struct LockOrdering;

impl LockOrdering {
    /// Acquire two read locks in a consistent order based on memory addresses
    /// This prevents deadlocks when acquiring multiple locks
    pub fn dual_read_timeout<'a, T1, T2>(
        lock1: &'a Arc<RwLock<T1>>,
        lock2: &'a Arc<RwLock<T2>>,
        timeout: Duration,
    ) -> Result<(RwLockReadGuard<'a, T1>, RwLockReadGuard<'a, T2>)> {
        let addr1 = lock1.as_ref() as *const RwLock<T1> as usize;
        let addr2 = lock2.as_ref() as *const RwLock<T2> as usize;

        match addr1.cmp(&addr2) {
            std::cmp::Ordering::Less => {
                let guard1 = lock1.read_timeout(timeout)?;
                let guard2 = lock2.read_timeout(timeout)?;
                Ok((guard1, guard2))
            }
            std::cmp::Ordering::Greater => {
                let guard2 = lock2.read_timeout(timeout)?;
                let guard1 = lock1.read_timeout(timeout)?;
                Ok((guard1, guard2))
            }
            std::cmp::Ordering::Equal => {
                // Same lock - this shouldn't happen in practice, but handle gracefully
                let guard1 = lock1.read_timeout(timeout)?;
                let guard2 = lock2.read_timeout(timeout)?;
                Ok((guard1, guard2))
            }
        }
    }

    /// Acquire two write locks in a consistent order based on memory addresses
    pub fn dual_write_timeout<'a, T1, T2>(
        lock1: &'a Arc<RwLock<T1>>,
        lock2: &'a Arc<RwLock<T2>>,
        timeout: Duration,
    ) -> Result<(RwLockWriteGuard<'a, T1>, RwLockWriteGuard<'a, T2>)> {
        let addr1 = lock1.as_ref() as *const RwLock<T1> as usize;
        let addr2 = lock2.as_ref() as *const RwLock<T2> as usize;

        use std::cmp::Ordering;
        match addr1.cmp(&addr2) {
            Ordering::Less => {
                let guard1 = lock1.write_timeout(timeout)?;
                let guard2 = lock2.write_timeout(timeout)?;
                Ok((guard1, guard2))
            }
            Ordering::Greater => {
                let guard2 = lock2.write_timeout(timeout)?;
                let guard1 = lock1.write_timeout(timeout)?;
                Ok((guard1, guard2))
            }
            Ordering::Equal => {
                // Same lock
                let guard1 = lock1.write_timeout(timeout)?;
                let guard2 = lock2.write_timeout(timeout)?;
                Ok((guard1, guard2))
            }
        }
    }

    /// Acquire multiple read locks in address order to prevent deadlocks
    pub fn multi_read_timeout<T>(
        locks: &[Arc<RwLock<T>>],
        timeout: Duration,
    ) -> Result<Vec<RwLockReadGuard<'_, T>>> {
        // Sort locks by address to ensure consistent ordering
        let mut indexed_locks: Vec<(usize, &Arc<RwLock<T>>)> = locks
            .iter()
            .enumerate()
            .collect();

        indexed_locks.sort_by_key(|(_, lock)| lock.as_ref() as *const RwLock<T> as usize);

        let mut guards = Vec::with_capacity(locks.len());
        let start = Instant::now();

        for (original_index, lock) in indexed_locks {
            if start.elapsed() >= timeout {
                return Err(Error::Timeout(
                    "Timeout acquiring multiple locks".to_string(),
                ));
            }

            let remaining_timeout = timeout.saturating_sub(start.elapsed());
            let guard = lock.read_timeout(remaining_timeout)?;
            guards.push((original_index, guard));
        }

        // Restore original order
        guards.sort_by_key(|(index, _)| *index);
        Ok(guards.into_iter().map(|(_, guard)| guard).collect())
    }
}

/// Hierarchical lock ordering to prevent deadlocks in complex scenarios
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockLevel {
    /// Database-level locks (highest priority)
    Database = 0,
    /// Table-level locks
    Table = 1,
    /// Page-level locks
    Page = 2,
    /// Transaction locks
    Transaction = 3,
    /// Cache locks (lowest priority)
    Cache = 4,
}

/// Hierarchical lock holder that enforces lock ordering
pub struct HierarchicalLocks {
    held_levels: Vec<LockLevel>,
}

impl HierarchicalLocks {
    pub fn new() -> Self {
        Self {
            held_levels: Vec::new(),
        }
    }

    /// Acquire a lock at the specified level
    /// Returns error if this would violate lock ordering
    pub fn acquire_level(&mut self, level: LockLevel) -> Result<()> {
        // Check if we can acquire this level based on hierarchy
        if let Some(&highest_held) = self.held_levels.iter().max() {
            if level < highest_held {
                return Err(Error::Transaction(format!(
                    "Cannot acquire {:?} lock while holding {:?} lock - would violate hierarchy",
                    level, highest_held
                )));
            }
        }

        self.held_levels.push(level);
        Ok(())
    }

    /// Release a lock at the specified level
    pub fn release_level(&mut self, level: LockLevel) {
        self.held_levels.retain(|&l| l != level);
    }

    /// Get the currently held lock levels
    pub fn held_levels(&self) -> &[LockLevel] {
        &self.held_levels
    }
}

impl Default for HierarchicalLocks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeout_rwlock() {
        let lock = Arc::new(RwLock::new(42));

        // Should succeed immediately
        let guard = lock.read_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(*guard, 42);
        drop(guard);

        // Test timeout behavior
        let lock2 = Arc::clone(&lock);
        let _write_guard = lock2.try_write().unwrap();

        // This should timeout
        let result = lock.read_timeout(Duration::from_millis(10));
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_ordering() {
        let lock1 = Arc::new(RwLock::new(1));
        let lock2 = Arc::new(RwLock::new(2));

        let (guard1, guard2) =
            LockOrdering::dual_read_timeout(&lock1, &lock2, Duration::from_millis(100)).unwrap();

        assert_eq!(*guard1, 1);
        assert_eq!(*guard2, 2);
    }

    #[test]
    fn test_hierarchical_locks() {
        let mut locks = HierarchicalLocks::new();

        // Should be able to acquire in ascending order
        assert!(locks.acquire_level(LockLevel::Database).is_ok());
        assert!(locks.acquire_level(LockLevel::Table).is_ok());

        // Should not be able to acquire lower level lock
        assert!(locks.acquire_level(LockLevel::Database).is_err());

        locks.release_level(LockLevel::Table);
        locks.release_level(LockLevel::Database);

        // Should be able to acquire again after release
        assert!(locks.acquire_level(LockLevel::Page).is_ok());
    }
}
