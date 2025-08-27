use async_trait::async_trait;
use crate::core::error::Error;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::collections::{BinaryHeap, HashSet};
use parking_lot::RwLock;
use rand::Rng;
use std::cmp::Ordering;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    pub dimensions: usize,
    pub max_connections: usize,
    pub ef_construction: usize,
    pub similarity_metric: super::similarity::SimilarityMetric,
    pub seed: u64,
}

pub struct HnswIndex {
    config: Arc<HnswConfig>,
    graph: Arc<RwLock<HnswGraph>>,
    distance_fn: Arc<dyn super::similarity::DistanceFunction>,
    entry_point: Arc<RwLock<Option<u64>>>,
}

struct HnswGraph {
    nodes: std::collections::HashMap<u64, HnswNode>,
    levels: Vec<Vec<u64>>,
    max_level: usize,
}

struct HnswNode {
    id: u64,
    vector: Vec<f32>,
    level: usize,
    connections: Vec<Vec<u64>>,
}

#[derive(Clone)]
struct Neighbor {
    id: u64,
    distance: f32,
}

impl PartialEq for Neighbor {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Neighbor {}

impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.distance.partial_cmp(&self.distance)
    }
}

impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl HnswIndex {
    pub fn new(config: HnswConfig) -> Result<Self, Error> {
        Ok(Self {
            distance_fn: super::similarity::create_distance_function(config.similarity_metric.clone()),
            config: Arc::new(config),
            graph: Arc::new(RwLock::new(HnswGraph {
                nodes: std::collections::HashMap::new(),
                levels: Vec::new(),
                max_level: 0,
            })),
            entry_point: Arc::new(RwLock::new(None)),
        })
    }

    fn select_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let ml = 1.0 / (2.0_f64.ln());
        let level = (-rng.gen::<f64>().ln() * ml).floor() as usize;
        level.min(16)
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry_points: Vec<u64>,
        num_closest: usize,
        level: usize,
        graph: &HnswGraph,
    ) -> Vec<Neighbor> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();
        
        for &point in &entry_points {
            let dist = self.distance_fn.distance(query, &graph.nodes[&point].vector);
            let neighbor = Neighbor { id: point, distance: dist };
            
            candidates.push(neighbor.clone());
            results.push(neighbor.clone());
            visited.insert(point);
        }
        
        while !candidates.is_empty() {
            let current = candidates.pop().unwrap();
            
            if current.distance > results.peek().unwrap().distance {
                break;
            }
            
            let node = &graph.nodes[&current.id];
            let connections = if level < node.connections.len() {
                &node.connections[level]
            } else {
                continue;
            };
            
            for &neighbor_id in connections {
                if !visited.contains(&neighbor_id) {
                    visited.insert(neighbor_id);
                    
                    let neighbor_node = &graph.nodes[&neighbor_id];
                    let dist = self.distance_fn.distance(query, &neighbor_node.vector);
                    
                    if dist < results.peek().unwrap().distance || results.len() < num_closest {
                        let neighbor = Neighbor { id: neighbor_id, distance: dist };
                        candidates.push(neighbor.clone());
                        results.push(neighbor);
                        
                        if results.len() > num_closest {
                            results.pop();
                        }
                    }
                }
            }
        }
        
        let mut final_results: Vec<_> = results.into_sorted_vec();
        final_results.reverse();
        final_results.truncate(num_closest);
        final_results
    }

    fn get_neighbors(&self, level: usize) -> usize {
        if level == 0 {
            self.config.max_connections * 2
        } else {
            self.config.max_connections
        }
    }

    fn prune_connections(
        &self,
        candidates: Vec<Neighbor>,
        m: usize,
    ) -> Vec<u64> {
        let mut pruned = Vec::new();
        let mut candidates = candidates;
        
        candidates.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        
        for candidate in candidates {
            if pruned.len() >= m {
                break;
            }
            
            let mut should_add = true;
            for &existing in &pruned {
                let graph = self.graph.read();
                let dist_to_existing = self.distance_fn.distance(
                    &graph.nodes[&candidate.id].vector,
                    &graph.nodes[&existing].vector,
                );
                
                if dist_to_existing < candidate.distance {
                    should_add = false;
                    break;
                }
            }
            
            if should_add {
                pruned.push(candidate.id);
            }
        }
        
        pruned
    }
}

#[async_trait]
impl super::index::VectorIndex for HnswIndex {
    async fn insert(&self, id: u64, vector: Vec<f32>) -> Result<(), Error> {
        if vector.len() != self.config.dimensions {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                vector.len()
            )));
        }
        
        let level = self.select_level();
        let mut graph = self.graph.write();
        
        let node = HnswNode {
            id,
            vector: vector.clone(),
            level,
            connections: vec![Vec::new(); level + 1],
        };
        
        graph.nodes.insert(id, node);
        
        while graph.levels.len() <= level {
            graph.levels.push(Vec::new());
        }
        
        for l in 0..=level {
            graph.levels[l].push(id);
        }
        
        if level > graph.max_level {
            graph.max_level = level;
        }
        
        drop(graph);
        
        let mut entry_point = self.entry_point.write();
        if entry_point.is_none() {
            *entry_point = Some(id);
            return Ok(());
        }
        
        let entry = entry_point.unwrap();
        drop(entry_point);
        
        let mut nearest = vec![entry];
        
        for lc in (1..=level.min(self.graph.read().max_level)).rev() {
            let graph = self.graph.read();
            nearest = self.search_layer(&vector, nearest.clone(), 1, lc, &graph)
                .into_iter()
                .map(|n| n.id)
                .collect();
        }
        
        for lc in (0..=level.min(self.graph.read().max_level)).rev() {
            let graph = self.graph.read();
            let candidates = self.search_layer(
                &vector,
                nearest.clone(),
                self.config.ef_construction,
                lc,
                &graph,
            );
            drop(graph);
            
            let m = self.get_neighbors(lc);
            let neighbors = self.prune_connections(candidates.clone(), m);
            
            let mut graph = self.graph.write();
            let node = graph.nodes.get_mut(&id).unwrap();
            node.connections[lc] = neighbors.clone();
            
            for &neighbor_id in &neighbors {
                if let Some(neighbor_node) = graph.nodes.get_mut(&neighbor_id) {
                    if lc < neighbor_node.connections.len() {
                        neighbor_node.connections[lc].push(id);
                        
                        let max_conn = self.get_neighbors(lc);
                        if neighbor_node.connections[lc].len() > max_conn {
                            let neighbor_vec = &neighbor_node.vector.clone();
                            let mut candidates_for_pruning = Vec::new();
                            
                            for &conn_id in &neighbor_node.connections[lc] {
                                let conn_vec = &graph.nodes[&conn_id].vector;
                                let dist = self.distance_fn.distance(neighbor_vec, conn_vec);
                                candidates_for_pruning.push(Neighbor { id: conn_id, distance: dist });
                            }
                            
                            drop(graph);
                            let pruned = self.prune_connections(candidates_for_pruning, max_conn);
                            let mut graph = self.graph.write();
                            
                            if let Some(neighbor_node) = graph.nodes.get_mut(&neighbor_id) {
                                neighbor_node.connections[lc] = pruned;
                            }
                        }
                    }
                }
            }
            
            nearest = neighbors;
        }
        
        Ok(())
    }

    async fn insert_batch(&self, vectors: Vec<(u64, Vec<f32>)>) -> Result<(), Error> {
        for (id, vector) in vectors {
            self.insert(id, vector).await?;
        }
        Ok(())
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>, Error> {
        if query.len() != self.config.dimensions {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                query.len()
            )));
        }
        
        let entry_point = *self.entry_point.read();
        if entry_point.is_none() {
            return Ok(Vec::new());
        }
        
        let entry = entry_point.unwrap();
        let mut nearest = vec![entry];
        
        let graph = self.graph.read();
        
        for level in (1..=graph.max_level).rev() {
            nearest = self.search_layer(query, nearest.clone(), 1, level, &graph)
                .into_iter()
                .map(|n| n.id)
                .collect();
        }
        
        let results = self.search_layer(query, nearest, k.max(self.config.ef_construction), 0, &graph);
        
        Ok(results.into_iter()
            .take(k)
            .map(|n| (n.id, n.distance))
            .collect())
    }

    async fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<(u64, f32)>, Error> {
        let results = self.search(query, 100).await?;
        Ok(results.into_iter()
            .filter(|(_, dist)| *dist <= radius)
            .collect())
    }

    async fn delete(&self, id: u64) -> Result<(), Error> {
        let mut graph = self.graph.write();
        
        if let Some(node) = graph.nodes.remove(&id) {
            for level in 0..=node.level {
                if level < graph.levels.len() {
                    graph.levels[level].retain(|&x| x != id);
                }
                
                for (_, other_node) in graph.nodes.iter_mut() {
                    if level < other_node.connections.len() {
                        other_node.connections[level].retain(|&x| x != id);
                    }
                }
            }
            
            let mut entry_point = self.entry_point.write();
            if *entry_point == Some(id) {
                *entry_point = graph.nodes.keys().next().copied();
            }
        }
        
        Ok(())
    }

    async fn rebuild(&self) -> Result<(), Error> {
        let graph = self.graph.read();
        let vectors: Vec<(u64, Vec<f32>)> = graph.nodes.iter()
            .map(|(id, node)| (*id, node.vector.clone()))
            .collect();
        drop(graph);
        
        *self.graph.write() = HnswGraph {
            nodes: std::collections::HashMap::new(),
            levels: Vec::new(),
            max_level: 0,
        };
        *self.entry_point.write() = None;
        
        for (id, vector) in vectors {
            self.insert(id, vector).await?;
        }
        
        Ok(())
    }

    async fn optimize(&self) -> Result<(), Error> {
        self.rebuild().await
    }

    async fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.graph.read().nodes.len()
    }

    fn dimensions(&self) -> usize {
        self.config.dimensions
    }
}