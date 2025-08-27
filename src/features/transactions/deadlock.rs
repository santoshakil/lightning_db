use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{RwLock, Mutex, mpsc};
use serde::{Serialize, Deserialize};
use crate::error::{Error, Result};
use dashmap::DashMap;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::algo::tarjan_scc;
use petgraph::Direction;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeadlockVictim {
    pub txn_id: super::TransactionId,
    pub priority: i32,
    pub cost: f64,
    pub age: Duration,
    pub locks_held: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitForGraph {
    graph: DiGraph<WaitNode, WaitEdge>,
    node_map: HashMap<super::TransactionId, NodeIndex>,
    last_update: Instant,
    generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WaitNode {
    txn_id: super::TransactionId,
    priority: i32,
    start_time: Instant,
    locks_held: HashSet<String>,
    locks_waiting: HashSet<String>,
    node_type: NodeType,
    cost: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum NodeType {
    Transaction,
    Resource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WaitEdge {
    edge_type: EdgeType,
    resource: String,
    wait_start: Instant,
    timeout: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum EdgeType {
    WaitFor,
    Holds,
}

pub struct DeadlockDetector {
    node_id: String,
    wait_graph: Arc<RwLock<WaitForGraph>>,
    detection_interval: Duration,
    victim_selector: Arc<VictimSelector>,
    distributed_detector: Option<Arc<DistributedDeadlockDetector>>,
    metrics: Arc<DeadlockMetrics>,
    detection_running: Arc<std::sync::atomic::AtomicBool>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: Arc<Mutex<mpsc::Receiver<()>>>,
}

struct VictimSelector {
    strategy: VictimSelectionStrategy,
    weight_priority: f64,
    weight_age: f64,
    weight_cost: f64,
    weight_locks: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VictimSelectionStrategy {
    YoungestTransaction,
    LowestPriority,
    MinimumCost,
    LeastLocks,
    Weighted,
}

struct DistributedDeadlockDetector {
    local_node_id: String,
    peer_nodes: Arc<DashMap<String, PeerNode>>,
    global_wait_graph: Arc<RwLock<GlobalWaitGraph>>,
    probe_sender: mpsc::Sender<DeadlockProbe>,
    probe_receiver: Arc<Mutex<mpsc::Receiver<DeadlockProbe>>>,
}

struct PeerNode {
    node_id: String,
    endpoint: String,
    last_heartbeat: Instant,
    local_graph_version: u64,
}

struct GlobalWaitGraph {
    local_graphs: HashMap<String, WaitForGraph>,
    global_edges: Vec<GlobalEdge>,
    last_merge: Instant,
}

struct GlobalEdge {
    from_node: String,
    from_txn: super::TransactionId,
    to_node: String,
    to_txn: super::TransactionId,
    resource: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeadlockProbe {
    probe_id: u64,
    initiator: super::TransactionId,
    path: Vec<ProbeNode>,
    timestamp: Instant,
    ttl: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProbeNode {
    node_id: String,
    txn_id: super::TransactionId,
    resource: String,
}

struct DeadlockMetrics {
    cycles_detected: Arc<std::sync::atomic::AtomicU64>,
    victims_selected: Arc<std::sync::atomic::AtomicU64>,
    false_positives: Arc<std::sync::atomic::AtomicU64>,
    detection_time_ms: Arc<std::sync::atomic::AtomicU64>,
    graph_nodes: Arc<std::sync::atomic::AtomicU64>,
    graph_edges: Arc<std::sync::atomic::AtomicU64>,
}

impl DeadlockDetector {
    pub fn new(node_id: String, detection_interval: Duration) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        
        Self {
            node_id,
            wait_graph: Arc::new(RwLock::new(WaitForGraph {
                graph: DiGraph::new(),
                node_map: HashMap::new(),
                last_update: Instant::now(),
                generation: 0,
            })),
            detection_interval,
            victim_selector: Arc::new(VictimSelector {
                strategy: VictimSelectionStrategy::Weighted,
                weight_priority: 0.3,
                weight_age: 0.2,
                weight_cost: 0.3,
                weight_locks: 0.2,
            }),
            distributed_detector: None,
            metrics: Arc::new(DeadlockMetrics {
                cycles_detected: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                victims_selected: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                false_positives: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                detection_time_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                graph_nodes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                graph_edges: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
            detection_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_tx,
            shutdown_rx: Arc::new(Mutex::new(shutdown_rx)),
        }
    }

    pub async fn add_wait(
        &self,
        waiter: super::TransactionId,
        holder: super::TransactionId,
        resource: String,
    ) -> Result<()> {
        let mut graph = self.wait_graph.write().await;
        
        let waiter_idx = graph.node_map.get(&waiter).copied().unwrap_or_else(|| {
            let idx = graph.graph.add_node(WaitNode {
                txn_id: waiter,
                priority: 0,
                start_time: Instant::now(),
                locks_held: HashSet::new(),
                locks_waiting: HashSet::new(),
                node_type: NodeType::Transaction,
                cost: 0.0,
            });
            graph.node_map.insert(waiter, idx);
            idx
        });
        
        let holder_idx = graph.node_map.get(&holder).copied().unwrap_or_else(|| {
            let idx = graph.graph.add_node(WaitNode {
                txn_id: holder,
                priority: 0,
                start_time: Instant::now(),
                locks_held: HashSet::new(),
                locks_waiting: HashSet::new(),
                node_type: NodeType::Transaction,
                cost: 0.0,
            });
            graph.node_map.insert(holder, idx);
            idx
        });
        
        graph.graph[waiter_idx].locks_waiting.insert(resource.clone());
        graph.graph[holder_idx].locks_held.insert(resource.clone());
        
        graph.graph.add_edge(
            waiter_idx,
            holder_idx,
            WaitEdge {
                edge_type: EdgeType::WaitFor,
                resource: resource.clone(),
                wait_start: Instant::now(),
                timeout: Some(Duration::from_secs(30)),
            },
        );
        
        graph.last_update = Instant::now();
        graph.generation += 1;
        
        self.metrics.graph_nodes.store(
            graph.graph.node_count() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.metrics.graph_edges.store(
            graph.graph.edge_count() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        
        Ok(())
    }

    pub async fn remove_transaction(&self, txn_id: super::TransactionId) -> Result<()> {
        let mut graph = self.wait_graph.write().await;
        
        if let Some(node_idx) = graph.node_map.remove(&txn_id) {
            graph.graph.remove_node(node_idx);
            
            graph.node_map = graph.node_map
                .iter()
                .filter_map(|(txn, idx)| {
                    if graph.graph.node_weight(*idx).is_some() {
                        Some((*txn, *idx))
                    } else {
                        None
                    }
                })
                .collect();
        }
        
        graph.last_update = Instant::now();
        graph.generation += 1;
        
        Ok(())
    }

    pub async fn detect_deadlocks(&self) -> Result<Vec<DeadlockVictim>> {
        let start = Instant::now();
        self.detection_running.store(true, std::sync::atomic::Ordering::SeqCst);
        
        let graph = self.wait_graph.read().await;
        
        let cycles = self.find_cycles(&graph);
        
        let mut victims = Vec::new();
        
        for cycle in cycles {
            if let Some(victim) = self.select_victim(&graph, &cycle).await {
                victims.push(victim);
                self.metrics.victims_selected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        
        self.metrics.cycles_detected.fetch_add(
            cycles.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        
        let elapsed = start.elapsed().as_millis() as u64;
        self.metrics.detection_time_ms.store(
            elapsed,
            std::sync::atomic::Ordering::Relaxed,
        );
        
        self.detection_running.store(false, std::sync::atomic::Ordering::SeqCst);
        
        Ok(victims)
    }

    fn find_cycles(&self, graph: &WaitForGraph) -> Vec<Vec<super::TransactionId>> {
        let sccs = tarjan_scc(&graph.graph);
        
        let mut cycles = Vec::new();
        
        for scc in sccs {
            if scc.len() > 1 {
                let cycle: Vec<super::TransactionId> = scc
                    .iter()
                    .filter_map(|idx| {
                        graph.graph.node_weight(*idx).map(|node| node.txn_id)
                    })
                    .collect();
                
                if !cycle.is_empty() {
                    cycles.push(cycle);
                }
            } else if scc.len() == 1 {
                let idx = scc[0];
                let has_self_loop = graph.graph
                    .edges_directed(idx, Direction::Outgoing)
                    .any(|edge| edge.target() == idx);
                
                if has_self_loop {
                    if let Some(node) = graph.graph.node_weight(idx) {
                        cycles.push(vec![node.txn_id]);
                    }
                }
            }
        }
        
        cycles
    }

    async fn select_victim(
        &self,
        graph: &WaitForGraph,
        cycle: &[super::TransactionId],
    ) -> Option<DeadlockVictim> {
        let mut candidates = Vec::new();
        
        for txn_id in cycle {
            if let Some(node_idx) = graph.node_map.get(txn_id) {
                if let Some(node) = graph.graph.node_weight(*node_idx) {
                    let age = Instant::now().duration_since(node.start_time);
                    
                    candidates.push(DeadlockVictim {
                        txn_id: *txn_id,
                        priority: node.priority,
                        cost: node.cost,
                        age,
                        locks_held: node.locks_held.len(),
                    });
                }
            }
        }
        
        if candidates.is_empty() {
            return None;
        }
        
        match self.victim_selector.strategy {
            VictimSelectionStrategy::YoungestTransaction => {
                candidates.sort_by_key(|v| v.age);
                candidates.first().copied()
            }
            VictimSelectionStrategy::LowestPriority => {
                candidates.sort_by_key(|v| v.priority);
                candidates.first().copied()
            }
            VictimSelectionStrategy::MinimumCost => {
                candidates.sort_by(|a, b| a.cost.partial_cmp(&b.cost).unwrap());
                candidates.first().copied()
            }
            VictimSelectionStrategy::LeastLocks => {
                candidates.sort_by_key(|v| v.locks_held);
                candidates.first().copied()
            }
            VictimSelectionStrategy::Weighted => {
                let mut best_victim = None;
                let mut best_score = f64::MAX;
                
                for candidate in candidates {
                    let score = self.calculate_victim_score(&candidate);
                    if score < best_score {
                        best_score = score;
                        best_victim = Some(candidate);
                    }
                }
                
                best_victim
            }
        }
    }

    fn calculate_victim_score(&self, victim: &DeadlockVictim) -> f64 {
        let priority_score = (100 - victim.priority) as f64 * self.victim_selector.weight_priority;
        let age_score = victim.age.as_secs_f64() * self.victim_selector.weight_age;
        let cost_score = victim.cost * self.victim_selector.weight_cost;
        let locks_score = victim.locks_held as f64 * self.victim_selector.weight_locks;
        
        priority_score + age_score + cost_score + locks_score
    }

    pub async fn start_detection(&self) {
        let graph = self.wait_graph.clone();
        let interval = self.detection_interval;
        let metrics = self.metrics.clone();
        let victim_selector = self.victim_selector.clone();
        let mut shutdown_rx = self.shutdown_rx.lock().await;
        
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let detector = DeadlockDetector {
                            node_id: String::new(),
                            wait_graph: graph.clone(),
                            detection_interval: interval,
                            victim_selector: victim_selector.clone(),
                            distributed_detector: None,
                            metrics: metrics.clone(),
                            detection_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                            shutdown_tx: mpsc::channel(1).0,
                            shutdown_rx: Arc::new(Mutex::new(mpsc::channel(1).1)),
                        };
                        
                        if let Ok(victims) = detector.detect_deadlocks().await {
                            for victim in victims {
                                detector.handle_victim(victim).await;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    async fn handle_victim(&self, victim: DeadlockVictim) {
        self.remove_transaction(victim.txn_id).await.ok();
    }

    pub async fn enable_distributed_detection(
        &mut self,
        peers: Vec<(String, String)>,
    ) -> Result<()> {
        let (probe_tx, probe_rx) = mpsc::channel(100);
        
        let peer_nodes = Arc::new(DashMap::new());
        for (node_id, endpoint) in peers {
            peer_nodes.insert(
                node_id.clone(),
                PeerNode {
                    node_id,
                    endpoint,
                    last_heartbeat: Instant::now(),
                    local_graph_version: 0,
                },
            );
        }
        
        self.distributed_detector = Some(Arc::new(DistributedDeadlockDetector {
            local_node_id: self.node_id.clone(),
            peer_nodes,
            global_wait_graph: Arc::new(RwLock::new(GlobalWaitGraph {
                local_graphs: HashMap::new(),
                global_edges: Vec::new(),
                last_merge: Instant::now(),
            })),
            probe_sender: probe_tx,
            probe_receiver: Arc::new(Mutex::new(probe_rx)),
        }));
        
        Ok(())
    }

    pub async fn send_probe(&self, initiator: super::TransactionId) -> Result<()> {
        if let Some(dist) = &self.distributed_detector {
            let probe = DeadlockProbe {
                probe_id: rand::random(),
                initiator,
                path: Vec::new(),
                timestamp: Instant::now(),
                ttl: 16,
            };
            
            dist.probe_sender.send(probe).await.map_err(|_| {
                Error::Custom("Failed to send deadlock probe".to_string())
            })?;
        }
        
        Ok(())
    }

    pub async fn merge_global_graphs(&self) -> Result<()> {
        if let Some(dist) = &self.distributed_detector {
            let mut global = dist.global_wait_graph.write().await;
            
            let local = self.wait_graph.read().await;
            global.local_graphs.insert(self.node_id.clone(), local.clone());
            
            global.global_edges.clear();
            
            for (node_id, graph) in &global.local_graphs {
                for edge_ref in graph.graph.edge_references() {
                    let source_node = &graph.graph[edge_ref.source()];
                    let target_node = &graph.graph[edge_ref.target()];
                    
                    global.global_edges.push(GlobalEdge {
                        from_node: node_id.clone(),
                        from_txn: source_node.txn_id,
                        to_node: node_id.clone(),
                        to_txn: target_node.txn_id,
                        resource: edge_ref.weight().resource.clone(),
                    });
                }
            }
            
            global.last_merge = Instant::now();
        }
        
        Ok(())
    }

    pub fn get_metrics(&self) -> DeadlockStatistics {
        DeadlockStatistics {
            cycles_detected: self.metrics.cycles_detected.load(std::sync::atomic::Ordering::Relaxed),
            victims_selected: self.metrics.victims_selected.load(std::sync::atomic::Ordering::Relaxed),
            false_positives: self.metrics.false_positives.load(std::sync::atomic::Ordering::Relaxed),
            avg_detection_time_ms: self.metrics.detection_time_ms.load(std::sync::atomic::Ordering::Relaxed),
            graph_size: GraphSize {
                nodes: self.metrics.graph_nodes.load(std::sync::atomic::Ordering::Relaxed),
                edges: self.metrics.graph_edges.load(std::sync::atomic::Ordering::Relaxed),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadlockStatistics {
    pub cycles_detected: u64,
    pub victims_selected: u64,
    pub false_positives: u64,
    pub avg_detection_time_ms: u64,
    pub graph_size: GraphSize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSize {
    pub nodes: u64,
    pub edges: u64,
}