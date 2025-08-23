use crate::core::error::{Error, Result};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use super::locks::{TxId, LockMode, LockManager, LockGranularity};

/// Predicate for range-based locking
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// Exact key match
    Key(Bytes),
    /// Key range [start, end)
    Range { start: Option<Bytes>, end: Option<Bytes> },
    /// Key prefix
    Prefix(Bytes),
    /// Complex condition (serialized as string for simplicity)
    Condition(String),
    /// Gap between two keys (for phantom prevention)
    Gap { before: Option<Bytes>, after: Option<Bytes> },
}

impl Predicate {
    /// Check if this predicate overlaps with another
    pub fn overlaps_with(&self, other: &Predicate) -> bool {
        match (self, other) {
            // Key predicates
            (Predicate::Key(k1), Predicate::Key(k2)) => k1 == k2,
            (Predicate::Key(key), Predicate::Range { start, end }) |
            (Predicate::Range { start, end }, Predicate::Key(key)) => {
                self.key_in_range(key, start.as_ref(), end.as_ref())
            }
            (Predicate::Key(key), Predicate::Prefix(prefix)) |
            (Predicate::Prefix(prefix), Predicate::Key(key)) => {
                key.starts_with(prefix)
            }

            // Range predicates
            (Predicate::Range { start: s1, end: e1 }, Predicate::Range { start: s2, end: e2 }) => {
                self.ranges_overlap(s1.as_ref(), e1.as_ref(), s2.as_ref(), e2.as_ref())
            }
            (Predicate::Range { start, end }, Predicate::Prefix(prefix)) |
            (Predicate::Prefix(prefix), Predicate::Range { start, end }) => {
                // Check if prefix overlaps with range
                self.prefix_overlaps_range(prefix, start.as_ref(), end.as_ref())
            }

            // Prefix predicates
            (Predicate::Prefix(p1), Predicate::Prefix(p2)) => {
                p1.starts_with(p2) || p2.starts_with(p1)
            }

            // Gap predicates
            (Predicate::Gap { before: b1, after: a1 }, Predicate::Gap { before: b2, after: a2 }) => {
                // Gaps overlap if they represent the same or overlapping space
                self.gaps_overlap(b1.as_ref(), a1.as_ref(), b2.as_ref(), a2.as_ref())
            }
            (Predicate::Gap { before, after }, other) | (other, Predicate::Gap { before, after }) => {
                self.predicate_affects_gap(other, before.as_ref(), after.as_ref())
            }

            // Complex conditions - assume they overlap for safety
            (Predicate::Condition(_), _) | (_, Predicate::Condition(_)) => true,
        }
    }

    /// Check if a key satisfies this predicate
    pub fn matches_key(&self, key: &Bytes) -> bool {
        match self {
            Predicate::Key(pred_key) => key == pred_key,
            Predicate::Range { start, end } => {
                self.key_in_range(key, start.as_ref(), end.as_ref())
            }
            Predicate::Prefix(prefix) => key.starts_with(prefix),
            Predicate::Gap { before, after } => {
                // Key is in the gap if it's between before and after
                self.key_in_gap(key, before.as_ref(), after.as_ref())
            }
            Predicate::Condition(_) => true, // Conservative: assume all keys match
        }
    }

    /// Check if insertion/deletion of a key would affect this predicate
    pub fn affected_by_key_change(&self, key: &Bytes, is_insertion: bool) -> bool {
        match self {
            Predicate::Key(pred_key) => key == pred_key,
            Predicate::Range { start, end } => {
                self.key_in_range(key, start.as_ref(), end.as_ref())
            }
            Predicate::Prefix(prefix) => key.starts_with(prefix),
            Predicate::Gap { before, after } => {
                // Gap is affected if key is inserted/deleted within it
                is_insertion && self.key_in_gap(key, before.as_ref(), after.as_ref())
            }
            Predicate::Condition(_) => true, // Conservative
        }
    }

    // Helper methods
    fn key_in_range(&self, key: &Bytes, start: Option<&Bytes>, end: Option<&Bytes>) -> bool {
        match (start, end) {
            (None, None) => true,
            (Some(s), None) => key >= s,
            (None, Some(e)) => key < e,
            (Some(s), Some(e)) => key >= s && key < e,
        }
    }

    fn key_in_gap(&self, key: &Bytes, before: Option<&Bytes>, after: Option<&Bytes>) -> bool {
        match (before, after) {
            (None, None) => false, // Invalid gap
            (Some(b), None) => key > b,
            (None, Some(a)) => key < a,
            (Some(b), Some(a)) => key > b && key < a,
        }
    }

    fn ranges_overlap(
        &self,
        s1: Option<&Bytes>,
        e1: Option<&Bytes>,
        s2: Option<&Bytes>,
        e2: Option<&Bytes>,
    ) -> bool {
        // Two ranges [s1,e1) and [s2,e2) overlap if max(s1,s2) < min(e1,e2)
        let start = match (s1, s2) {
            (None, None) => None,
            (Some(s), None) | (None, Some(s)) => Some(s),
            (Some(s1), Some(s2)) => Some(if s1 > s2 { s1 } else { s2 }),
        };

        let end = match (e1, e2) {
            (None, None) => None,
            (Some(e), None) | (None, Some(e)) => Some(e),
            (Some(e1), Some(e2)) => Some(if e1 < e2 { e1 } else { e2 }),
        };

        match (start, end) {
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => true,
            (Some(s), Some(e)) => s < e,
        }
    }

    fn prefix_overlaps_range(&self, prefix: &Bytes, start: Option<&Bytes>, end: Option<&Bytes>) -> bool {
        // Check if any key with this prefix could be in the range
        match (start, end) {
            (None, None) => true,
            (Some(s), None) => {
                // Check if prefix could produce keys >= s
                prefix >= s || s.starts_with(prefix)
            }
            (None, Some(e)) => {
                // Check if prefix could produce keys < e
                prefix < e
            }
            (Some(s), Some(e)) => {
                // Check if prefix could produce keys in [s,e)
                (prefix >= s && prefix < e) || s.starts_with(prefix) || 
                (prefix < s && self.could_prefix_reach(prefix, s))
            }
        }
    }

    fn could_prefix_reach(&self, prefix: &Bytes, target: &Bytes) -> bool {
        // Check if extending prefix could reach target
        prefix.len() < target.len() && target.starts_with(prefix)
    }

    fn gaps_overlap(
        &self,
        b1: Option<&Bytes>,
        a1: Option<&Bytes>,
        b2: Option<&Bytes>,
        a2: Option<&Bytes>,
    ) -> bool {
        // Two gaps overlap if they represent overlapping key spaces
        // Gap1: (b1, a1), Gap2: (b2, a2)
        match (b1, a1, b2, a2) {
            (Some(b1), Some(a1), Some(b2), Some(a2)) => {
                // Check if the intervals (b1,a1) and (b2,a2) overlap
                b1 < a2 && b2 < a1
            }
            _ => true, // Conservative: assume overlap if unbounded
        }
    }

    fn predicate_affects_gap(&self, predicate: &Predicate, before: Option<&Bytes>, after: Option<&Bytes>) -> bool {
        match predicate {
            Predicate::Key(key) => self.key_in_gap(key, before, after),
            Predicate::Range { start, end } => {
                // Range affects gap if they overlap
                self.ranges_overlap(before, after, start.as_ref(), end.as_ref())
            }
            Predicate::Prefix(prefix) => {
                self.prefix_overlaps_range(prefix, before, after)
            }
            _ => true, // Conservative
        }
    }
}

/// A predicate lock held by a transaction
#[derive(Debug, Clone)]
pub struct PredicateLock {
    pub tx_id: TxId,
    pub predicate: Predicate,
    pub mode: LockMode,
    pub acquired_at: Instant,
}

/// Predicate lock manager for phantom prevention
#[derive(Debug)]
pub struct PredicateLockManager {
    /// Active predicate locks
    predicate_locks: Arc<RwLock<Vec<PredicateLock>>>,
    /// Locks by transaction for cleanup
    tx_predicate_locks: Arc<RwLock<HashMap<TxId, Vec<Predicate>>>>,
    /// Integration with regular lock manager
    lock_manager: Arc<LockManager>,
    /// Statistics
    stats: Arc<RwLock<PredicateLockStats>>,
}

#[derive(Debug, Default, Clone)]
pub struct PredicateLockStats {
    pub predicate_locks_acquired: u64,
    pub predicate_locks_released: u64,
    pub phantom_prevention_blocks: u64,
    pub predicate_conflicts: u64,
    pub active_predicate_locks: usize,
}

impl PredicateLockManager {
    pub fn new(lock_manager: Arc<LockManager>) -> Self {
        Self {
            predicate_locks: Arc::new(RwLock::new(Vec::new())),
            tx_predicate_locks: Arc::new(RwLock::new(HashMap::new())),
            lock_manager,
            stats: Arc::new(RwLock::new(PredicateLockStats::default())),
        }
    }

    /// Acquire a predicate lock
    pub fn acquire_predicate_lock(
        &self,
        tx_id: TxId,
        predicate: Predicate,
        mode: LockMode,
    ) -> Result<()> {
        // Check for conflicts with existing predicate locks
        let conflicts = self.check_predicate_conflicts(tx_id, &predicate, mode)?;
        if !conflicts.is_empty() {
            self.stats.write().predicate_conflicts += 1;
            return Err(Error::LockConflict(format!(
                "Predicate lock conflict: {:?} conflicts with existing locks from transactions {:?}",
                predicate, conflicts
            )));
        }

        // Acquire the lock
        let predicate_lock = PredicateLock {
            tx_id,
            predicate: predicate.clone(),
            mode,
            acquired_at: Instant::now(),
        };

        self.predicate_locks.write().push(predicate_lock);
        
        // Track for this transaction
        self.tx_predicate_locks
            .write()
            .entry(tx_id)
            .or_insert_with(Vec::new)
            .push(predicate.clone());

        self.stats.write().predicate_locks_acquired += 1;
        debug!("Acquired predicate lock: tx={}, predicate={:?}, mode={:?}", 
               tx_id, predicate, mode);

        Ok(())
    }

    /// Check if a key operation would conflict with existing predicate locks
    pub fn check_key_operation_conflicts(
        &self,
        tx_id: TxId,
        key: &Bytes,
        is_insertion: bool,
    ) -> Result<Vec<TxId>> {
        let predicate_locks = self.predicate_locks.read();
        let mut conflicting_txs = Vec::new();

        for lock in predicate_locks.iter() {
            if lock.tx_id != tx_id && lock.predicate.affected_by_key_change(key, is_insertion) {
                conflicting_txs.push(lock.tx_id);
            }
        }

        if !conflicting_txs.is_empty() {
            self.stats.write().phantom_prevention_blocks += 1;
            debug!("Key operation on {:?} conflicts with predicate locks from transactions: {:?}", 
                   key, conflicting_txs);
        }

        Ok(conflicting_txs)
    }

    /// Check if a range operation would conflict with existing predicate locks
    pub fn check_range_operation_conflicts(
        &self,
        tx_id: TxId,
        start_key: Option<&Bytes>,
        end_key: Option<&Bytes>,
    ) -> Result<Vec<TxId>> {
        let range_predicate = Predicate::Range {
            start: start_key.map(|k| k.clone()),
            end: end_key.map(|k| k.clone()),
        };

        self.check_predicate_conflicts(tx_id, &range_predicate, LockMode::Shared)
    }

    /// Acquire locks for a range scan to prevent phantoms
    pub fn acquire_range_scan_locks(
        &self,
        tx_id: TxId,
        start_key: Option<Bytes>,
        end_key: Option<Bytes>,
    ) -> Result<()> {
        // Acquire predicate lock for the entire range
        let range_predicate = Predicate::Range { start: start_key.clone(), end: end_key.clone() };
        self.acquire_predicate_lock(tx_id, range_predicate, LockMode::Shared)?;

        // Also acquire gap locks to prevent insertions
        if let (Some(start), Some(end)) = (&start_key, &end_key) {
            self.acquire_gap_locks(tx_id, start, end)?;
        }

        debug!("Acquired range scan locks: tx={}, range=[{:?}, {:?})", 
               tx_id, start_key, end_key);

        Ok(())
    }

    /// Acquire gap locks between keys to prevent phantom insertions
    pub fn acquire_gap_locks(&self, tx_id: TxId, start: &Bytes, end: &Bytes) -> Result<()> {
        // This is a simplified implementation
        // In a real system, you'd determine gaps based on existing keys
        let gap_predicate = Predicate::Gap {
            before: Some(start.clone()),
            after: Some(end.clone()),
        };

        self.acquire_predicate_lock(tx_id, gap_predicate, LockMode::Shared)
    }

    /// Acquire next-key locks (combination of record + gap)
    pub fn acquire_next_key_lock(&self, tx_id: TxId, key: &Bytes) -> Result<()> {
        // Lock the key itself
        let key_predicate = Predicate::Key(key.clone());
        self.acquire_predicate_lock(tx_id, key_predicate, LockMode::Shared)?;

        // Lock the gap after this key (simplified - would need to find next key)
        let gap_predicate = Predicate::Gap {
            before: Some(key.clone()),
            after: None, // To the next key (not implemented here)
        };
        self.acquire_predicate_lock(tx_id, gap_predicate, LockMode::Shared)?;

        debug!("Acquired next-key lock for key {:?}", key);
        Ok(())
    }

    /// Release all predicate locks for a transaction
    pub fn release_predicate_locks(&self, tx_id: TxId) {
        let mut predicate_locks = self.predicate_locks.write();
        let original_len = predicate_locks.len();
        
        predicate_locks.retain(|lock| lock.tx_id != tx_id);
        let released = original_len - predicate_locks.len();

        self.tx_predicate_locks.write().remove(&tx_id);
        
        if released > 0 {
            self.stats.write().predicate_locks_released += released as u64;
            debug!("Released {} predicate locks for transaction {}", released, tx_id);
        }
    }

    /// Get all predicate locks for a transaction
    pub fn get_predicate_locks_for_transaction(&self, tx_id: TxId) -> Vec<Predicate> {
        self.tx_predicate_locks
            .read()
            .get(&tx_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get current statistics
    pub fn get_stats(&self) -> PredicateLockStats {
        let mut stats = self.stats.write();
        stats.active_predicate_locks = self.predicate_locks.read().len();
        stats.clone()
    }

    /// Get all active predicate locks (for debugging)
    pub fn get_active_predicate_locks(&self) -> Vec<PredicateLock> {
        self.predicate_locks.read().clone()
    }

    /// Check for conflicts with a new predicate lock
    fn check_predicate_conflicts(
        &self,
        tx_id: TxId,
        predicate: &Predicate,
        mode: LockMode,
    ) -> Result<Vec<TxId>> {
        let predicate_locks = self.predicate_locks.read();
        let mut conflicts = Vec::new();

        for lock in predicate_locks.iter() {
            if lock.tx_id != tx_id 
                && lock.predicate.overlaps_with(predicate)
                && !mode.is_compatible_with(lock.mode)
            {
                conflicts.push(lock.tx_id);
            }
        }

        Ok(conflicts)
    }
}

/// Next-key locking implementation for preventing phantoms
#[derive(Debug)]
pub struct NextKeyLocker {
    predicate_manager: Arc<PredicateLockManager>,
    /// Map of keys to their next keys (for efficient next-key locking)
    key_order: Arc<RwLock<BTreeMap<Bytes, Option<Bytes>>>>,
}

impl NextKeyLocker {
    pub fn new(predicate_manager: Arc<PredicateLockManager>) -> Self {
        Self {
            predicate_manager,
            key_order: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Update key order when keys are inserted/deleted
    pub fn update_key_order(&self, key: Bytes, exists: bool) {
        let mut key_order = self.key_order.write();
        
        if exists {
            // Insert the key and update next pointers
            let next_key = key_order.range(key.clone()..).skip(1).next().map(|(k, _)| k.clone());
            key_order.insert(key.clone(), next_key);
            
            // Update the previous key's next pointer
            if let Some((prev_key, _)) = key_order.range(..key.clone()).last() {
                let prev_key = prev_key.clone();
                key_order.insert(prev_key, Some(key.clone()));
            }
        } else {
            // Remove the key and update pointers
            if let Some(next_key) = key_order.remove(&key) {
                // Update previous key to point to our next key
                if let Some((prev_key, _)) = key_order.range(..key.clone()).last() {
                    let prev_key = prev_key.clone();
                    key_order.insert(prev_key, next_key);
                }
            }
        }
    }

    /// Acquire next-key lock for a key
    pub fn acquire_next_key_lock(&self, tx_id: TxId, key: &Bytes, mode: LockMode) -> Result<()> {
        // Lock the key itself
        let key_lock_granularity = LockGranularity::Row(key.clone());
        self.predicate_manager.lock_manager.acquire_lock(tx_id, key_lock_granularity, mode)?;

        // Find and lock the gap to the next key
        let next_key = {
            let key_order = self.key_order.read();
            key_order.range(key.clone()..).skip(1).next().map(|(k, _)| k.clone())
        };

        let gap_predicate = Predicate::Gap {
            before: Some(key.clone()),
            after: next_key,
        };

        self.predicate_manager.acquire_predicate_lock(tx_id, gap_predicate, LockMode::Shared)?;

        debug!("Acquired next-key lock for key {:?}", key);
        Ok(())
    }

    /// Check if an insertion would violate next-key locks
    pub fn check_insertion_allowed(&self, tx_id: TxId, key: &Bytes) -> Result<bool> {
        let conflicts = self.predicate_manager.check_key_operation_conflicts(tx_id, key, true)?;
        Ok(conflicts.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_predicate_overlap() {
        let key_pred = Predicate::Key(Bytes::from("test"));
        let range_pred = Predicate::Range {
            start: Some(Bytes::from("a")),
            end: Some(Bytes::from("z")),
        };

        assert!(key_pred.overlaps_with(&range_pred));
        assert!(range_pred.overlaps_with(&key_pred));

        let non_overlapping_key = Predicate::Key(Bytes::from("1"));
        assert!(!non_overlapping_key.overlaps_with(&range_pred));
    }

    #[test]
    fn test_predicate_key_matching() {
        let range_pred = Predicate::Range {
            start: Some(Bytes::from("b")),
            end: Some(Bytes::from("y")),
        };

        assert!(range_pred.matches_key(&Bytes::from("m")));
        assert!(!range_pred.matches_key(&Bytes::from("a")));
        assert!(!range_pred.matches_key(&Bytes::from("z")));
    }

    #[test]
    fn test_predicate_lock_manager() {
        let lock_manager = Arc::new(LockManager::new(
            Duration::from_secs(1),
            false,
            Duration::from_millis(100),
        ));
        
        let pred_manager = PredicateLockManager::new(lock_manager);

        let range_pred = Predicate::Range {
            start: Some(Bytes::from("a")),
            end: Some(Bytes::from("z")),
        };

        // Acquire predicate lock
        pred_manager
            .acquire_predicate_lock(1, range_pred.clone(), LockMode::Shared)
            .unwrap();

        // Check for conflicts
        let conflicts = pred_manager
            .check_key_operation_conflicts(1, &Bytes::from("m"), true)
            .unwrap();
        assert!(conflicts.is_empty()); // Same transaction

        let conflicts = pred_manager
            .check_key_operation_conflicts(2, &Bytes::from("m"), true)
            .unwrap();
        assert!(!conflicts.is_empty()); // Different transaction should conflict
    }

    #[test]
    fn test_gap_predicate() {
        let gap_pred = Predicate::Gap {
            before: Some(Bytes::from("b")),
            after: Some(Bytes::from("d")),
        };

        assert!(gap_pred.matches_key(&Bytes::from("c")));
        assert!(!gap_pred.matches_key(&Bytes::from("b")));
        assert!(!gap_pred.matches_key(&Bytes::from("d")));
        assert!(!gap_pred.matches_key(&Bytes::from("a")));
        assert!(!gap_pred.matches_key(&Bytes::from("e")));
    }
}