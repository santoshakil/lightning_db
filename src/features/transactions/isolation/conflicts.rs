use crate::core::error::Result;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn, error};

use super::levels::IsolationLevel;
use super::locks::TxId;

/// Type of conflict detected
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictType {
    /// Write-Write conflict: two transactions writing to the same key
    WriteWrite,
    /// Write-Read conflict: reading a key that another transaction wrote
    WriteRead,
    /// Read-Write conflict: another transaction wrote to a key we read
    ReadWrite,
    /// Phantom read: range query affected by concurrent insertions/deletions
    PhantomRead,
    /// Serialization anomaly: cycle in serialization graph
    SerializationAnomaly,
    /// Snapshot isolation write skew
    WriteSkew,
}

/// A detected conflict between transactions
#[derive(Debug, Clone)]
pub struct Conflict {
    pub conflict_type: ConflictType,
    pub tx1: TxId,
    pub tx2: TxId,
    pub key: Option<Bytes>,
    pub range: Option<(Option<Bytes>, Option<Bytes>)>,
    pub timestamp: Instant,
    pub description: String,
}

/// Resolution strategy for conflicts
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConflictResolution {
    /// Abort the transaction that caused the conflict
    AbortConflicting,
    /// Abort the newer transaction
    AbortNewer,
    /// Abort the older transaction
    AbortOlder,
    /// Retry the transaction later
    Retry,
    /// Allow the conflict (for lower isolation levels)
    Allow,
}

/// Serialization graph node for detecting cycles
#[derive(Debug, Clone)]
pub struct SerializationNode {
    pub tx_id: TxId,
    pub depends_on: HashSet<TxId>,
    pub depended_by: HashSet<TxId>,
    pub start_time: Instant,
}

/// Conflict detection and resolution system
#[derive(Debug)]
pub struct ConflictResolver {
    /// Active conflicts
    conflicts: Arc<RwLock<HashMap<u64, Conflict>>>,
    /// Next conflict ID
    next_conflict_id: Arc<AtomicU64>,
    /// Serialization graph for anomaly detection
    serialization_graph: Arc<RwLock<HashMap<TxId, SerializationNode>>>,
    /// Read sets for transactions (for phantom read detection)
    read_sets: Arc<RwLock<HashMap<TxId, HashSet<Bytes>>>>,
    /// Write sets for transactions
    write_sets: Arc<RwLock<HashMap<TxId, HashSet<Bytes>>>>,
    /// Range read sets for phantom detection
    range_reads: Arc<RwLock<HashMap<TxId, Vec<(Option<Bytes>, Option<Bytes>)>>>>,
    /// Conflict statistics
    stats: Arc<RwLock<ConflictStats>>,
}

#[derive(Debug, Default, Clone)]
pub struct ConflictStats {
    pub total_conflicts: u64,
    pub write_write_conflicts: u64,
    pub read_write_conflicts: u64,
    pub phantom_reads: u64,
    pub serialization_anomalies: u64,
    pub conflicts_resolved: u64,
    pub transactions_aborted: u64,
}

impl ConflictResolver {
    pub fn new() -> Self {
        Self {
            conflicts: Arc::new(RwLock::new(HashMap::new())),
            next_conflict_id: Arc::new(AtomicU64::new(1)),
            serialization_graph: Arc::new(RwLock::new(HashMap::new())),
            read_sets: Arc::new(RwLock::new(HashMap::new())),
            write_sets: Arc::new(RwLock::new(HashMap::new())),
            range_reads: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ConflictStats::default())),
        }
    }

    /// Register a transaction for conflict tracking
    pub fn register_transaction(&self, tx_id: TxId) {
        let node = SerializationNode {
            tx_id,
            depends_on: HashSet::new(),
            depended_by: HashSet::new(),
            start_time: Instant::now(),
        };

        self.serialization_graph.write().insert(tx_id, node);
        self.read_sets.write().insert(tx_id, HashSet::new());
        self.write_sets.write().insert(tx_id, HashSet::new());
        self.range_reads.write().insert(tx_id, Vec::new());

        debug!("Registered transaction {} for conflict tracking", tx_id);
    }

    /// Unregister a transaction (on commit/abort)
    pub fn unregister_transaction(&self, tx_id: TxId) {
        self.serialization_graph.write().remove(&tx_id);
        self.read_sets.write().remove(&tx_id);
        self.write_sets.write().remove(&tx_id);
        self.range_reads.write().remove(&tx_id);

        // Remove from serialization graph dependencies
        let mut graph = self.serialization_graph.write();
        for node in graph.values_mut() {
            node.depends_on.remove(&tx_id);
            node.depended_by.remove(&tx_id);
        }

        debug!("Unregistered transaction {} from conflict tracking", tx_id);
    }

    /// Record a read operation
    pub fn record_read(&self, tx_id: TxId, key: Bytes) {
        if let Some(read_set) = self.read_sets.write().get_mut(&tx_id) {
            read_set.insert(key.clone());
        }
    }

    /// Record a write operation
    pub fn record_write(&self, tx_id: TxId, key: Bytes) {
        if let Some(write_set) = self.write_sets.write().get_mut(&tx_id) {
            write_set.insert(key.clone());
        }
    }

    /// Record a range read operation
    pub fn record_range_read(
        &self,
        tx_id: TxId,
        start_key: Option<Bytes>,
        end_key: Option<Bytes>,
    ) {
        if let Some(range_reads) = self.range_reads.write().get_mut(&tx_id) {
            range_reads.push((start_key, end_key));
        }
    }

    /// Check for write-write conflict
    pub fn check_write_write_conflict(
        &self,
        tx_id: TxId,
        key: &Bytes,
        other_writers: &[TxId],
    ) -> Result<Option<Conflict>> {
        for &other_tx in other_writers {
            if other_tx != tx_id {
                let conflict = Conflict {
                    conflict_type: ConflictType::WriteWrite,
                    tx1: tx_id,
                    tx2: other_tx,
                    key: Some(key.clone()),
                    range: None,
                    timestamp: Instant::now(),
                    description: format!(
                        "Write-write conflict: transactions {} and {} both writing to key {:?}",
                        tx_id, other_tx, key
                    ),
                };

                self.add_conflict(conflict.clone());
                self.stats.write().write_write_conflicts += 1;
                
                warn!("Write-write conflict detected: tx {} vs tx {} on key {:?}", 
                      tx_id, other_tx, key);
                
                return Ok(Some(conflict));
            }
        }

        Ok(None)
    }

    /// Check for read-write conflict
    pub fn check_read_write_conflict(
        &self,
        reading_tx: TxId,
        writing_tx: TxId,
        key: &Bytes,
        isolation_level: IsolationLevel,
    ) -> Result<Option<Conflict>> {
        // Only relevant for repeatable read and above
        if !isolation_level.prevents_non_repeatable_reads() {
            return Ok(None);
        }

        let read_sets = self.read_sets.read();
        if let Some(read_set) = read_sets.get(&reading_tx) {
            if read_set.contains(key) {
                let conflict = Conflict {
                    conflict_type: ConflictType::ReadWrite,
                    tx1: reading_tx,
                    tx2: writing_tx,
                    key: Some(key.clone()),
                    range: None,
                    timestamp: Instant::now(),
                    description: format!(
                        "Read-write conflict: transaction {} read key {:?} that transaction {} is writing",
                        reading_tx, key, writing_tx
                    ),
                };

                self.add_conflict(conflict.clone());
                self.add_serialization_dependency(reading_tx, writing_tx);
                self.stats.write().read_write_conflicts += 1;
                
                warn!("Read-write conflict detected: tx {} read key {:?} that tx {} is writing", 
                      reading_tx, key, writing_tx);
                
                return Ok(Some(conflict));
            }
        }

        Ok(None)
    }

    /// Check for phantom reads
    pub fn check_phantom_read(
        &self,
        tx_id: TxId,
        new_key: &Bytes,
        writing_tx: TxId,
        isolation_level: IsolationLevel,
    ) -> Result<Option<Conflict>> {
        // Only check for serializable isolation
        if !isolation_level.prevents_phantom_reads() {
            return Ok(None);
        }

        let range_reads = self.range_reads.read();
        if let Some(ranges) = range_reads.get(&tx_id) {
            for (start_key, end_key) in ranges {
                // Check if new key falls within any range read by this transaction
                let in_range = match (start_key, end_key) {
                    (None, None) => true, // Full table scan
                    (Some(start), None) => new_key >= start,
                    (None, Some(end)) => new_key < end,
                    (Some(start), Some(end)) => new_key >= start && new_key < end,
                };

                if in_range {
                    let conflict = Conflict {
                        conflict_type: ConflictType::PhantomRead,
                        tx1: tx_id,
                        tx2: writing_tx,
                        key: Some(new_key.clone()),
                        range: Some((start_key.clone(), end_key.clone())),
                        timestamp: Instant::now(),
                        description: format!(
                            "Phantom read: transaction {} performed range read that would be affected by transaction {} inserting key {:?}",
                            tx_id, writing_tx, new_key
                        ),
                    };

                    self.add_conflict(conflict.clone());
                    self.stats.write().phantom_reads += 1;
                    
                    warn!("Phantom read detected: tx {} range query affected by tx {} inserting key {:?}", 
                          tx_id, writing_tx, new_key);
                    
                    return Ok(Some(conflict));
                }
            }
        }

        Ok(None)
    }

    /// Check for serialization anomalies (cycles in serialization graph)
    pub fn check_serialization_anomaly(&self) -> Result<Vec<Conflict>> {
        let graph = self.serialization_graph.read();
        let cycles = self.find_cycles(&graph)?;
        
        let mut conflicts = Vec::new();
        for cycle in cycles {
            let conflict = Conflict {
                conflict_type: ConflictType::SerializationAnomaly,
                tx1: cycle[0],
                tx2: cycle[1],
                key: None,
                range: None,
                timestamp: Instant::now(),
                description: format!("Serialization anomaly: cycle detected in transactions {:?}", cycle),
            };

            self.add_conflict(conflict.clone());
            conflicts.push(conflict);
            self.stats.write().serialization_anomalies += 1;
            
            error!("Serialization anomaly detected: cycle in transactions {:?}", cycle);
        }

        Ok(conflicts)
    }

    /// Check for write skew anomaly in snapshot isolation
    pub fn check_write_skew(
        &self,
        tx1: TxId,
        tx2: TxId,
        predicate: &str,
    ) -> Result<Option<Conflict>> {
        // Write skew: T1 reads X, T2 reads Y, T1 writes Y, T2 writes X
        // Both transactions see consistent snapshots but the result is not serializable

        let read_sets = self.read_sets.read();
        let write_sets = self.write_sets.read();

        if let (Some(tx1_reads), Some(tx1_writes), Some(tx2_reads), Some(tx2_writes)) = (
            read_sets.get(&tx1),
            write_sets.get(&tx1),
            read_sets.get(&tx2),
            write_sets.get(&tx2),
        ) {
            // Check if T1 reads what T2 writes and vice versa
            let tx1_reads_tx2_writes = tx2_writes.iter().any(|key| tx1_reads.contains(key));
            let tx2_reads_tx1_writes = tx1_writes.iter().any(|key| tx2_reads.contains(key));

            if tx1_reads_tx2_writes && tx2_reads_tx1_writes {
                let conflict = Conflict {
                    conflict_type: ConflictType::WriteSkew,
                    tx1,
                    tx2,
                    key: None,
                    range: None,
                    timestamp: Instant::now(),
                    description: format!(
                        "Write skew anomaly: transactions {} and {} have overlapping read/write sets (predicate: {})",
                        tx1, tx2, predicate
                    ),
                };

                self.add_conflict(conflict.clone());
                warn!("Write skew anomaly detected between tx {} and tx {}", tx1, tx2);
                
                return Ok(Some(conflict));
            }
        }

        Ok(None)
    }

    /// Resolve a conflict based on isolation level and strategy
    pub fn resolve_conflict(
        &self,
        conflict: &Conflict,
        isolation_level: IsolationLevel,
    ) -> Result<ConflictResolution> {
        let resolution = match (conflict.conflict_type.clone(), isolation_level) {
            // Read uncommitted allows most conflicts
            (ConflictType::WriteWrite, IsolationLevel::ReadUncommitted) => ConflictResolution::Allow,
            (ConflictType::WriteRead, IsolationLevel::ReadUncommitted) => ConflictResolution::Allow,
            (ConflictType::ReadWrite, IsolationLevel::ReadUncommitted) => ConflictResolution::Allow,
            (ConflictType::PhantomRead, IsolationLevel::ReadUncommitted) => ConflictResolution::Allow,
            (ConflictType::WriteSkew, IsolationLevel::ReadUncommitted) => ConflictResolution::Allow,
            
            // Read committed prevents dirty reads but allows others
            (ConflictType::WriteWrite, IsolationLevel::ReadCommitted) => ConflictResolution::AbortNewer,
            (ConflictType::WriteRead, IsolationLevel::ReadCommitted) => ConflictResolution::AbortNewer,
            (ConflictType::ReadWrite, IsolationLevel::ReadCommitted) => ConflictResolution::Allow,
            (ConflictType::PhantomRead, IsolationLevel::ReadCommitted) => ConflictResolution::Allow,
            (ConflictType::WriteSkew, IsolationLevel::ReadCommitted) => ConflictResolution::Allow,
            
            // Repeatable read prevents non-repeatable reads
            (ConflictType::WriteWrite, IsolationLevel::RepeatableRead) => ConflictResolution::AbortNewer,
            (ConflictType::WriteRead, IsolationLevel::RepeatableRead) => ConflictResolution::AbortNewer,
            (ConflictType::ReadWrite, IsolationLevel::RepeatableRead) => ConflictResolution::AbortConflicting,
            (ConflictType::PhantomRead, IsolationLevel::RepeatableRead) => ConflictResolution::Allow,
            (ConflictType::WriteSkew, IsolationLevel::RepeatableRead) => ConflictResolution::Allow,
            
            // Serialization anomalies are generally allowed at lower isolation levels
            (ConflictType::SerializationAnomaly, IsolationLevel::ReadUncommitted) => ConflictResolution::Allow,
            (ConflictType::SerializationAnomaly, IsolationLevel::ReadCommitted) => ConflictResolution::Allow,
            (ConflictType::SerializationAnomaly, IsolationLevel::RepeatableRead) => ConflictResolution::Allow,
            
            // Serializable prevents all anomalies
            (_, IsolationLevel::Serializable) => ConflictResolution::AbortNewer,
            
            // Snapshot isolation
            (ConflictType::WriteWrite, IsolationLevel::Snapshot) => ConflictResolution::AbortConflicting,
            (ConflictType::WriteSkew, IsolationLevel::Snapshot) => ConflictResolution::Allow, // Write skew allowed
            (_, IsolationLevel::Snapshot) => ConflictResolution::Allow,
        };

        self.stats.write().conflicts_resolved += 1;
        debug!("Resolved conflict {:?} with resolution {:?}", conflict.conflict_type, resolution);

        Ok(resolution)
    }

    /// Get all active conflicts
    pub fn get_active_conflicts(&self) -> Vec<Conflict> {
        self.conflicts.read().values().cloned().collect()
    }

    /// Get conflicts involving a specific transaction
    pub fn get_conflicts_for_transaction(&self, tx_id: TxId) -> Vec<Conflict> {
        self.conflicts
            .read()
            .values()
            .filter(|conflict| conflict.tx1 == tx_id || conflict.tx2 == tx_id)
            .cloned()
            .collect()
    }

    /// Clear all conflicts for a transaction (on commit/abort)
    pub fn clear_conflicts_for_transaction(&self, tx_id: TxId) {
        let mut conflicts = self.conflicts.write();
        conflicts.retain(|_, conflict| conflict.tx1 != tx_id && conflict.tx2 != tx_id);
    }

    /// Get current statistics
    pub fn get_stats(&self) -> ConflictStats {
        self.stats.read().clone()
    }

    /// Private helper to add a conflict
    fn add_conflict(&self, conflict: Conflict) {
        let conflict_id = self.next_conflict_id.fetch_add(1, Ordering::Relaxed);
        self.conflicts.write().insert(conflict_id, conflict);
        self.stats.write().total_conflicts += 1;
    }

    /// Add a dependency in the serialization graph
    fn add_serialization_dependency(&self, tx1: TxId, tx2: TxId) {
        let mut graph = self.serialization_graph.write();
        
        if let Some(node1) = graph.get_mut(&tx1) {
            node1.depends_on.insert(tx2);
        }
        
        if let Some(node2) = graph.get_mut(&tx2) {
            node2.depended_by.insert(tx1);
        }
    }

    /// Find cycles in the serialization graph using DFS
    fn find_cycles(&self, graph: &HashMap<TxId, SerializationNode>) -> Result<Vec<Vec<TxId>>> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut cycles = Vec::new();

        for &tx_id in graph.keys() {
            if !visited.contains(&tx_id) {
                if let Some(cycle) = self.dfs_cycle_detection(tx_id, graph, &mut visited, &mut rec_stack) {
                    cycles.push(cycle);
                }
            }
        }

        Ok(cycles)
    }

    /// DFS helper for cycle detection
    fn dfs_cycle_detection(
        &self,
        tx_id: TxId,
        graph: &HashMap<TxId, SerializationNode>,
        visited: &mut HashSet<TxId>,
        rec_stack: &mut HashSet<TxId>,
    ) -> Option<Vec<TxId>> {
        visited.insert(tx_id);
        rec_stack.insert(tx_id);

        if let Some(node) = graph.get(&tx_id) {
            for &dep_tx in &node.depends_on {
                if !visited.contains(&dep_tx) {
                    if let Some(cycle) = self.dfs_cycle_detection(dep_tx, graph, visited, rec_stack) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(&dep_tx) {
                    // Cycle detected
                    return Some(vec![tx_id, dep_tx]);
                }
            }
        }

        rec_stack.remove(&tx_id);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_write_conflict() {
        let resolver = ConflictResolver::new();
        
        resolver.register_transaction(1);
        resolver.register_transaction(2);

        let key = Bytes::from("test_key");
        let other_writers = vec![2];
        
        let conflict = resolver
            .check_write_write_conflict(1, &key, &other_writers)
            .unwrap();

        assert!(conflict.is_some());
        assert_eq!(conflict.unwrap().conflict_type, ConflictType::WriteWrite);
    }

    #[test]
    fn test_phantom_read_detection() {
        let resolver = ConflictResolver::new();
        
        resolver.register_transaction(1);
        resolver.register_transaction(2);

        // Transaction 1 performs a range read
        resolver.record_range_read(1, Some(Bytes::from("a")), Some(Bytes::from("z")));

        // Transaction 2 inserts in the range
        let new_key = Bytes::from("m");
        let conflict = resolver
            .check_phantom_read(1, &new_key, 2, IsolationLevel::Serializable)
            .unwrap();

        assert!(conflict.is_some());
        assert_eq!(conflict.unwrap().conflict_type, ConflictType::PhantomRead);
    }

    #[test]
    fn test_conflict_resolution() {
        let resolver = ConflictResolver::new();
        
        let conflict = Conflict {
            conflict_type: ConflictType::WriteWrite,
            tx1: 1,
            tx2: 2,
            key: Some(Bytes::from("key")),
            range: None,
            timestamp: Instant::now(),
            description: "Test conflict".to_string(),
        };

        let resolution = resolver
            .resolve_conflict(&conflict, IsolationLevel::ReadCommitted)
            .unwrap();

        assert_eq!(resolution, ConflictResolution::AbortNewer);
    }

    #[test]
    fn test_serialization_graph() {
        let resolver = ConflictResolver::new();
        
        resolver.register_transaction(1);
        resolver.register_transaction(2);
        resolver.register_transaction(3);

        // Create dependencies: 1 -> 2 -> 3 -> 1 (cycle)
        resolver.add_serialization_dependency(1, 2);
        resolver.add_serialization_dependency(2, 3);
        resolver.add_serialization_dependency(3, 1);

        let conflicts = resolver.check_serialization_anomaly().unwrap();
        assert!(!conflicts.is_empty());
        assert_eq!(conflicts[0].conflict_type, ConflictType::SerializationAnomaly);
    }
}