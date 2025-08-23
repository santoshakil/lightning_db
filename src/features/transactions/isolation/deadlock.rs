use crate::core::error::Result;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use super::locks::TxId;

/// Deadlock detection and resolution strategy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeadlockResolutionStrategy {
    /// Abort the youngest transaction (lowest priority)
    AbortYoungest,
    /// Abort the transaction with least work done
    AbortLeastWork,
    /// Abort the transaction holding fewest locks
    AbortFewestLocks,
    /// Random selection
    AbortRandom,
}

/// Transaction information for deadlock detection
#[derive(Debug, Clone)]
pub struct TxDeadlockInfo {
    pub tx_id: TxId,
    pub start_time: Instant,
    pub locks_held: usize,
    pub work_done: u64, // Operations performed
    pub priority: i32,  // Higher values = higher priority
}

/// Deadlock detection result
#[derive(Debug, Clone)]
pub struct DeadlockResult {
    pub cycle: Vec<TxId>,
    pub victim: TxId,
    pub reason: String,
}

/// Advanced deadlock detector with various resolution strategies
#[derive(Debug)]
pub struct DeadlockDetector {
    /// Wait-for graph: tx_id -> set of transactions it's waiting for
    wait_graph: Arc<Mutex<HashMap<TxId, HashSet<TxId>>>>,
    /// Transaction information for resolution decisions
    tx_info: Arc<DashMap<TxId, TxDeadlockInfo>>,
    /// Detection configuration
    resolution_strategy: DeadlockResolutionStrategy,
    detection_interval: Duration,
    max_wait_time: Duration,
    /// Statistics
    deadlocks_detected: Arc<AtomicU64>,
    victims_selected: Arc<AtomicU64>,
    last_detection: Arc<Mutex<Instant>>,
}

impl DeadlockDetector {
    pub fn new(
        resolution_strategy: DeadlockResolutionStrategy,
        detection_interval: Duration,
        max_wait_time: Duration,
    ) -> Self {
        Self {
            wait_graph: Arc::new(Mutex::new(HashMap::new())),
            tx_info: Arc::new(DashMap::new()),
            resolution_strategy,
            detection_interval,
            max_wait_time,
            deadlocks_detected: Arc::new(AtomicU64::new(0)),
            victims_selected: Arc::new(AtomicU64::new(0)),
            last_detection: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Register a new transaction
    pub fn register_transaction(&self, tx_id: TxId, priority: i32) {
        let info = TxDeadlockInfo {
            tx_id,
            start_time: Instant::now(),
            locks_held: 0,
            work_done: 0,
            priority,
        };
        self.tx_info.insert(tx_id, info);
        debug!("Registered transaction {} for deadlock detection", tx_id);
    }

    /// Unregister a transaction (on commit/abort)
    pub fn unregister_transaction(&self, tx_id: TxId) {
        self.tx_info.remove(&tx_id);
        
        // Remove from wait graph
        let mut wait_graph = self.wait_graph.lock();
        wait_graph.remove(&tx_id);
        
        // Remove from all wait lists
        for waiters in wait_graph.values_mut() {
            waiters.remove(&tx_id);
        }
        
        debug!("Unregistered transaction {} from deadlock detection", tx_id);
    }

    /// Update transaction info when locks are acquired/released
    pub fn update_lock_count(&self, tx_id: TxId, delta: i32) {
        if let Some(mut info) = self.tx_info.get_mut(&tx_id) {
            if delta > 0 {
                info.locks_held += delta as usize;
            } else {
                info.locks_held = info.locks_held.saturating_sub((-delta) as usize);
            }
        }
    }

    /// Update work done by a transaction
    pub fn update_work_done(&self, tx_id: TxId, work: u64) {
        if let Some(mut info) = self.tx_info.get_mut(&tx_id) {
            info.work_done += work;
        }
    }

    /// Add a wait-for edge (tx1 is waiting for tx2)
    pub fn add_wait_edge(&self, waiting_tx: TxId, blocking_tx: TxId) {
        if waiting_tx == blocking_tx {
            return; // Self-loops not meaningful
        }

        let mut wait_graph = self.wait_graph.lock();
        wait_graph
            .entry(waiting_tx)
            .or_insert_with(HashSet::new)
            .insert(blocking_tx);
        
        debug!("Added wait edge: {} -> {}", waiting_tx, blocking_tx);
    }

    /// Remove a wait-for edge
    pub fn remove_wait_edge(&self, waiting_tx: TxId, blocking_tx: TxId) {
        let mut wait_graph = self.wait_graph.lock();
        if let Some(waiting_for) = wait_graph.get_mut(&waiting_tx) {
            waiting_for.remove(&blocking_tx);
            if waiting_for.is_empty() {
                wait_graph.remove(&waiting_tx);
            }
        }
        debug!("Removed wait edge: {} -> {}", waiting_tx, blocking_tx);
    }

    /// Check if detection should run
    pub fn should_detect(&self) -> bool {
        let last_detection = self.last_detection.lock();
        Instant::now() - *last_detection >= self.detection_interval
    }

    /// Detect deadlocks and return resolution recommendations
    pub fn detect_deadlocks(&self) -> Result<Vec<DeadlockResult>> {
        let mut last_detection = self.last_detection.lock();
        let now = Instant::now();
        
        if now - *last_detection < self.detection_interval {
            return Ok(Vec::new());
        }
        
        *last_detection = now;
        drop(last_detection);

        debug!("Starting deadlock detection");
        
        let wait_graph = self.wait_graph.lock();
        let cycles = self.find_all_cycles(&wait_graph)?;
        drop(wait_graph);
        
        if !cycles.is_empty() {
            self.deadlocks_detected.fetch_add(cycles.len() as u64, Ordering::Relaxed);
            warn!("Detected {} deadlock cycles", cycles.len());
        }

        let mut results = Vec::new();
        for cycle in cycles {
            if let Some(victim) = self.select_victim(&cycle) {
                let result = DeadlockResult {
                    cycle: cycle.clone(),
                    victim,
                    reason: self.get_victim_reason(victim, &cycle),
                };
                results.push(result);
                self.victims_selected.fetch_add(1, Ordering::Relaxed);
                warn!("Selected victim {} for deadlock cycle: {:?}", victim, cycle);
            }
        }

        Ok(results)
    }

    /// Find all cycles in the wait graph using Tarjan's algorithm
    fn find_all_cycles(&self, wait_graph: &HashMap<TxId, HashSet<TxId>>) -> Result<Vec<Vec<TxId>>> {
        let mut index_counter = 0;
        let mut stack = Vec::new();
        let mut indices = HashMap::new();
        let mut lowlinks = HashMap::new();
        let mut on_stack = HashSet::new();
        let mut cycles = Vec::new();

        for &tx_id in wait_graph.keys() {
            if !indices.contains_key(&tx_id) {
                self.tarjan_scc(
                    tx_id,
                    wait_graph,
                    &mut index_counter,
                    &mut stack,
                    &mut indices,
                    &mut lowlinks,
                    &mut on_stack,
                    &mut cycles,
                );
            }
        }

        // Filter out cycles with only one node (self-loops)
        Ok(cycles.into_iter().filter(|cycle| cycle.len() > 1).collect())
    }

    /// Tarjan's strongly connected components algorithm
    fn tarjan_scc(
        &self,
        v: TxId,
        wait_graph: &HashMap<TxId, HashSet<TxId>>,
        index_counter: &mut usize,
        stack: &mut Vec<TxId>,
        indices: &mut HashMap<TxId, usize>,
        lowlinks: &mut HashMap<TxId, usize>,
        on_stack: &mut HashSet<TxId>,
        cycles: &mut Vec<Vec<TxId>>,
    ) {
        indices.insert(v, *index_counter);
        lowlinks.insert(v, *index_counter);
        *index_counter += 1;
        stack.push(v);
        on_stack.insert(v);

        if let Some(neighbors) = wait_graph.get(&v) {
            for &w in neighbors {
                if !indices.contains_key(&w) {
                    self.tarjan_scc(w, wait_graph, index_counter, stack, indices, lowlinks, on_stack, cycles);
                    if let (Some(&v_lowlink), Some(&w_lowlink)) = (lowlinks.get(&v), lowlinks.get(&w)) {
                        lowlinks.insert(v, v_lowlink.min(w_lowlink));
                    }
                } else if on_stack.contains(&w) {
                    if let (Some(&v_lowlink), Some(&w_index)) = (lowlinks.get(&v), indices.get(&w)) {
                        lowlinks.insert(v, v_lowlink.min(w_index));
                    }
                }
            }
        }

        // If v is a root node, pop the stack and create SCC
        if let (Some(&v_lowlink), Some(&v_index)) = (lowlinks.get(&v), indices.get(&v)) {
            if v_lowlink == v_index {
                let mut cycle = Vec::new();
                loop {
                    if let Some(w) = stack.pop() {
                        on_stack.remove(&w);
                        cycle.push(w);
                        if w == v {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if cycle.len() > 1 {
                    cycles.push(cycle);
                }
            }
        }
    }

    /// Select a victim transaction from a deadlock cycle
    fn select_victim(&self, cycle: &[TxId]) -> Option<TxId> {
        if cycle.is_empty() {
            return None;
        }

        match self.resolution_strategy {
            DeadlockResolutionStrategy::AbortYoungest => {
                self.select_youngest_victim(cycle)
            }
            DeadlockResolutionStrategy::AbortLeastWork => {
                self.select_least_work_victim(cycle)
            }
            DeadlockResolutionStrategy::AbortFewestLocks => {
                self.select_fewest_locks_victim(cycle)
            }
            DeadlockResolutionStrategy::AbortRandom => {
                Some(cycle[0]) // Simple: just pick first
            }
        }
    }

    /// Select the youngest (most recent) transaction
    fn select_youngest_victim(&self, cycle: &[TxId]) -> Option<TxId> {
        let mut youngest = None;
        let mut youngest_time = None;

        for &tx_id in cycle {
            if let Some(info) = self.tx_info.get(&tx_id) {
                if youngest_time.is_none() || Some(info.start_time) > youngest_time {
                    youngest = Some(tx_id);
                    youngest_time = Some(info.start_time);
                }
            }
        }

        youngest
    }

    /// Select transaction with least work done
    fn select_least_work_victim(&self, cycle: &[TxId]) -> Option<TxId> {
        let mut victim = None;
        let mut min_work = u64::MAX;

        for &tx_id in cycle {
            if let Some(info) = self.tx_info.get(&tx_id) {
                if info.work_done < min_work {
                    min_work = info.work_done;
                    victim = Some(tx_id);
                }
            }
        }

        victim
    }

    /// Select transaction with fewest locks
    fn select_fewest_locks_victim(&self, cycle: &[TxId]) -> Option<TxId> {
        let mut victim = None;
        let mut min_locks = usize::MAX;

        for &tx_id in cycle {
            if let Some(info) = self.tx_info.get(&tx_id) {
                if info.locks_held < min_locks {
                    min_locks = info.locks_held;
                    victim = Some(tx_id);
                }
            }
        }

        victim
    }

    /// Get explanation for why a transaction was selected as victim
    fn get_victim_reason(&self, victim: TxId, _cycle: &[TxId]) -> String {
        match self.resolution_strategy {
            DeadlockResolutionStrategy::AbortYoungest => {
                if let Some(info) = self.tx_info.get(&victim) {
                    format!("Youngest transaction (started at {:?})", info.start_time)
                } else {
                    "Youngest transaction (no info available)".to_string()
                }
            }
            DeadlockResolutionStrategy::AbortLeastWork => {
                if let Some(info) = self.tx_info.get(&victim) {
                    format!("Least work done ({} operations)", info.work_done)
                } else {
                    "Least work done (no info available)".to_string()
                }
            }
            DeadlockResolutionStrategy::AbortFewestLocks => {
                if let Some(info) = self.tx_info.get(&victim) {
                    format!("Fewest locks held ({} locks)", info.locks_held)
                } else {
                    "Fewest locks held (no info available)".to_string()
                }
            }
            DeadlockResolutionStrategy::AbortRandom => {
                "Random selection".to_string()
            }
        }
    }

    /// Get current statistics
    pub fn get_stats(&self) -> DeadlockStats {
        DeadlockStats {
            active_transactions: self.tx_info.len(),
            deadlocks_detected: self.deadlocks_detected.load(Ordering::Relaxed),
            victims_selected: self.victims_selected.load(Ordering::Relaxed),
            wait_edges: {
                let wait_graph = self.wait_graph.lock();
                wait_graph.values().map(|set| set.len()).sum::<usize>()
            },
        }
    }

    /// Get current wait graph for debugging
    pub fn get_wait_graph(&self) -> HashMap<TxId, HashSet<TxId>> {
        self.wait_graph.lock().clone()
    }

    /// Check for transactions that have been waiting too long
    pub fn check_wait_timeouts(&self) -> Vec<TxId> {
        let mut timed_out = Vec::new();
        let now = Instant::now();

        for entry in self.tx_info.iter() {
            let tx_id = *entry.key();
            let info = entry.value();
            
            if now - info.start_time > self.max_wait_time {
                timed_out.push(tx_id);
            }
        }

        if !timed_out.is_empty() {
            warn!("Detected {} transactions with wait timeout", timed_out.len());
        }

        timed_out
    }
}

/// Deadlock detection statistics
#[derive(Debug, Clone)]
pub struct DeadlockStats {
    pub active_transactions: usize,
    pub deadlocks_detected: u64,
    pub victims_selected: u64,
    pub wait_edges: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deadlock_detection() {
        let detector = DeadlockDetector::new(
            DeadlockResolutionStrategy::AbortYoungest,
            Duration::from_millis(100),
            Duration::from_secs(10),
        );

        // Register transactions
        detector.register_transaction(1, 0);
        detector.register_transaction(2, 0);
        detector.register_transaction(3, 0);

        // Create a cycle: 1 -> 2 -> 3 -> 1
        detector.add_wait_edge(1, 2);
        detector.add_wait_edge(2, 3);
        detector.add_wait_edge(3, 1);

        // Detect deadlocks
        let results = detector.detect_deadlocks().unwrap();
        assert_eq!(results.len(), 1);
        
        let deadlock = &results[0];
        assert_eq!(deadlock.cycle.len(), 3);
        assert!(deadlock.cycle.contains(&1));
        assert!(deadlock.cycle.contains(&2));
        assert!(deadlock.cycle.contains(&3));
    }

    #[test]
    fn test_victim_selection() {
        let detector = DeadlockDetector::new(
            DeadlockResolutionStrategy::AbortLeastWork,
            Duration::from_millis(100),
            Duration::from_secs(10),
        );

        detector.register_transaction(1, 0);
        detector.register_transaction(2, 0);

        // Set different work levels
        detector.update_work_done(1, 100);
        detector.update_work_done(2, 50);

        // Create cycle
        detector.add_wait_edge(1, 2);
        detector.add_wait_edge(2, 1);

        let results = detector.detect_deadlocks().unwrap();
        assert_eq!(results.len(), 1);
        
        // Transaction 2 should be selected (less work done)
        assert_eq!(results[0].victim, 2);
    }

    #[test]
    fn test_no_false_positives() {
        let detector = DeadlockDetector::new(
            DeadlockResolutionStrategy::AbortYoungest,
            Duration::from_millis(100),
            Duration::from_secs(10),
        );

        detector.register_transaction(1, 0);
        detector.register_transaction(2, 0);

        // No cycle: 1 -> 2 (but not 2 -> 1)
        detector.add_wait_edge(1, 2);

        let results = detector.detect_deadlocks().unwrap();
        assert_eq!(results.len(), 0);
    }
}