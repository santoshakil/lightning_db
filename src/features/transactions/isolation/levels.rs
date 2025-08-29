use std::fmt;
use serde::{Serialize, Deserialize};

/// SQL standard transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Lowest isolation level - allows dirty reads, non-repeatable reads, and phantom reads
    ReadUncommitted,
    /// Prevents dirty reads, allows non-repeatable reads and phantom reads
    ReadCommitted,
    /// Prevents dirty reads and non-repeatable reads, allows phantom reads
    RepeatableRead,
    /// Highest isolation level - prevents all phenomena
    Serializable,
    /// Snapshot isolation - prevents most phenomena but allows write skew
    Snapshot,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadCommitted
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IsolationLevel::ReadUncommitted => write!(f, "READ UNCOMMITTED"),
            IsolationLevel::ReadCommitted => write!(f, "READ COMMITTED"),
            IsolationLevel::RepeatableRead => write!(f, "REPEATABLE READ"),
            IsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
            IsolationLevel::Snapshot => write!(f, "SNAPSHOT"),
        }
    }
}

impl IsolationLevel {
    /// Check if this isolation level prevents dirty reads
    pub fn prevents_dirty_reads(self) -> bool {
        match self {
            IsolationLevel::ReadUncommitted => false,
            _ => true,
        }
    }

    /// Check if this isolation level prevents non-repeatable reads
    pub fn prevents_non_repeatable_reads(self) -> bool {
        match self {
            IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => false,
            _ => true,
        }
    }

    /// Check if this isolation level prevents phantom reads
    pub fn prevents_phantom_reads(self) -> bool {
        match self {
            IsolationLevel::Serializable => true,
            _ => false,
        }
    }

    /// Check if this isolation level uses snapshot isolation
    pub fn uses_snapshot_isolation(self) -> bool {
        match self {
            IsolationLevel::Snapshot => true,
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => true,
            _ => false,
        }
    }

    /// Check if this isolation level requires predicate locking
    pub fn requires_predicate_locking(self) -> bool {
        match self {
            IsolationLevel::Serializable => true,
            _ => false,
        }
    }

    /// Get required lock types for this isolation level
    pub fn required_lock_types(self) -> LockRequirements {
        match self {
            IsolationLevel::ReadUncommitted => LockRequirements {
                read_locks: false,
                write_locks: true,
                range_locks: false,
                predicate_locks: false,
            },
            IsolationLevel::ReadCommitted => LockRequirements {
                read_locks: true,
                write_locks: true,
                range_locks: false,
                predicate_locks: false,
            },
            IsolationLevel::RepeatableRead => LockRequirements {
                read_locks: true,
                write_locks: true,
                range_locks: false,
                predicate_locks: false,
            },
            IsolationLevel::Serializable => LockRequirements {
                read_locks: true,
                write_locks: true,
                range_locks: true,
                predicate_locks: true,
            },
            IsolationLevel::Snapshot => LockRequirements {
                read_locks: false,
                write_locks: true,
                range_locks: false,
                predicate_locks: false,
            },
        }
    }

    /// Get the lock duration policy for this isolation level
    pub fn lock_duration_policy(self) -> LockDurationPolicy {
        match self {
            IsolationLevel::ReadUncommitted => LockDurationPolicy::None,
            IsolationLevel::ReadCommitted => LockDurationPolicy::ReadUnlockImmediately,
            IsolationLevel::RepeatableRead => LockDurationPolicy::HoldUntilCommit,
            IsolationLevel::Serializable => LockDurationPolicy::HoldUntilCommit,
            IsolationLevel::Snapshot => LockDurationPolicy::WriteLocksOnly,
        }
    }
}

/// Lock requirements for an isolation level
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LockRequirements {
    pub read_locks: bool,
    pub write_locks: bool,
    pub range_locks: bool,
    pub predicate_locks: bool,
}

/// Lock duration policy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockDurationPolicy {
    /// No locks required
    None,
    /// Unlock read locks immediately after read
    ReadUnlockImmediately,
    /// Hold all locks until transaction commit/abort
    HoldUntilCommit,
    /// Only hold write locks until commit
    WriteLocksOnly,
}

/// Transaction isolation configuration
#[derive(Debug, Clone)]
pub struct IsolationConfig {
    pub level: IsolationLevel,
    pub enable_deadlock_detection: bool,
    pub deadlock_timeout: std::time::Duration,
    pub snapshot_cleanup_interval: std::time::Duration,
    pub max_concurrent_transactions: usize,
    pub lock_timeout: std::time::Duration,
}

impl Default for IsolationConfig {
    fn default() -> Self {
        Self {
            level: IsolationLevel::default(),
            enable_deadlock_detection: true,
            deadlock_timeout: std::time::Duration::from_millis(1000),
            snapshot_cleanup_interval: std::time::Duration::from_secs(30),
            max_concurrent_transactions: 1000,
            lock_timeout: std::time::Duration::from_secs(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_properties() {
        // Test dirty read prevention
        assert!(!IsolationLevel::ReadUncommitted.prevents_dirty_reads());
        assert!(IsolationLevel::ReadCommitted.prevents_dirty_reads());
        assert!(IsolationLevel::RepeatableRead.prevents_dirty_reads());
        assert!(IsolationLevel::Serializable.prevents_dirty_reads());

        // Test non-repeatable read prevention
        assert!(!IsolationLevel::ReadUncommitted.prevents_non_repeatable_reads());
        assert!(!IsolationLevel::ReadCommitted.prevents_non_repeatable_reads());
        assert!(IsolationLevel::RepeatableRead.prevents_non_repeatable_reads());
        assert!(IsolationLevel::Serializable.prevents_non_repeatable_reads());

        // Test phantom read prevention
        assert!(!IsolationLevel::ReadUncommitted.prevents_phantom_reads());
        assert!(!IsolationLevel::ReadCommitted.prevents_phantom_reads());
        assert!(!IsolationLevel::RepeatableRead.prevents_phantom_reads());
        assert!(IsolationLevel::Serializable.prevents_phantom_reads());
    }

    #[test]
    fn test_lock_requirements() {
        let req = IsolationLevel::Serializable.required_lock_types();
        assert!(req.read_locks);
        assert!(req.write_locks);
        assert!(req.range_locks);
        assert!(req.predicate_locks);

        let req = IsolationLevel::ReadUncommitted.required_lock_types();
        assert!(!req.read_locks);
        assert!(req.write_locks);
        assert!(!req.range_locks);
        assert!(!req.predicate_locks);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", IsolationLevel::ReadUncommitted), "READ UNCOMMITTED");
        assert_eq!(format!("{}", IsolationLevel::ReadCommitted), "READ COMMITTED");
        assert_eq!(format!("{}", IsolationLevel::RepeatableRead), "REPEATABLE READ");
        assert_eq!(format!("{}", IsolationLevel::Serializable), "SERIALIZABLE");
        assert_eq!(format!("{}", IsolationLevel::Snapshot), "SNAPSHOT");
    }
}