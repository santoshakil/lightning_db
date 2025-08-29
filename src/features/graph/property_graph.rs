use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};
use crate::core::error::{Error, Result};
use dashmap::DashMap;

#[derive(Debug, Clone)]
pub struct PropertyGraph {
    nodes: Arc<DashMap<NodeId, Node>>,
    edges: Arc<DashMap<EdgeId, Edge>>,
    labels: Arc<DashMap<String, HashSet<NodeId>>>,
    edge_types: Arc<DashMap<String, HashSet<EdgeId>>>,
    adjacency: Arc<DashMap<NodeId, AdjacencyList>>,
    properties_index: Arc<DashMap<String, PropertyIndex>>,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeId(pub u64);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct EdgeId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: HashMap<String, Property>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub id: EdgeId,
    pub source: NodeId,
    pub target: NodeId,
    pub edge_type: String,
    pub properties: HashMap<String, Property>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Property {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Date(chrono::DateTime<chrono::Utc>),
    Duration(chrono::Duration),
    Point(Point),
    List(Vec<Property>),
    Map(HashMap<String, Property>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>,
    pub srid: Option<u32>,
}

#[derive(Debug, Clone)]
struct AdjacencyList {
    incoming: HashSet<EdgeId>,
    outgoing: HashSet<EdgeId>,
    undirected: HashSet<EdgeId>,
}

#[derive(Debug, Clone)]
struct PropertyIndex {
    property_name: String,
    index_type: PropertyIndexType,
    values: HashMap<PropertyValue, HashSet<ElementId>>,
}

#[derive(Debug, Clone, Copy)]
enum PropertyIndexType {
    Exact,
    Range,
    FullText,
    Spatial,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum PropertyValue {
    Boolean(bool),
    Integer(i64),
    String(String),
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
enum ElementId {
    Node(NodeId),
    Edge(EdgeId),
}

impl PropertyGraph {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(DashMap::new()),
            edges: Arc::new(DashMap::new()),
            labels: Arc::new(DashMap::new()),
            edge_types: Arc::new(DashMap::new()),
            adjacency: Arc::new(DashMap::new()),
            properties_index: Arc::new(DashMap::new()),
        }
    }

    pub fn add_node(&self, node: Node) -> Result<NodeId> {
        let id = node.id;
        
        for label in &node.labels {
            self.labels
                .entry(label.clone())
                .or_insert_with(HashSet::new)
                .insert(id);
        }
        
        self.adjacency.insert(id, AdjacencyList {
            incoming: HashSet::new(),
            outgoing: HashSet::new(),
            undirected: HashSet::new(),
        });
        
        self.index_properties(ElementId::Node(id), &node.properties);
        
        self.nodes.insert(id, node);
        Ok(id)
    }

    pub fn add_edge(&self, edge: Edge) -> Result<EdgeId> {
        let id = edge.id;
        
        if !self.nodes.contains_key(&edge.source) {
            return Err(Error::Custom(format!("Source node {:?} not found", edge.source)));
        }
        
        if !self.nodes.contains_key(&edge.target) {
            return Err(Error::Custom(format!("Target node {:?} not found", edge.target)));
        }
        
        self.edge_types
            .entry(edge.edge_type.clone())
            .or_insert_with(HashSet::new)
            .insert(id);
        
        if let Some(mut adj) = self.adjacency.get_mut(&edge.source) {
            adj.outgoing.insert(id);
        }
        
        if let Some(mut adj) = self.adjacency.get_mut(&edge.target) {
            adj.incoming.insert(id);
        }
        
        self.index_properties(ElementId::Edge(id), &edge.properties);
        
        self.edges.insert(id, edge);
        Ok(id)
    }

    pub fn get_node(&self, id: NodeId) -> Option<Node> {
        self.nodes.get(&id).map(|n| n.clone())
    }

    pub fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.edges.get(&id).map(|e| e.clone())
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Vec<Node> {
        self.labels
            .get(label)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.get_node(*id))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_edges_by_type(&self, edge_type: &str) -> Vec<Edge> {
        self.edge_types
            .get(edge_type)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.get_edge(*id))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_neighbors(&self, node_id: NodeId, direction: Direction) -> Vec<NodeId> {
        self.adjacency
            .get(&node_id)
            .map(|adj| {
                let edge_ids = match direction {
                    Direction::Incoming => &adj.incoming,
                    Direction::Outgoing => &adj.outgoing,
                    Direction::Both => {
                        let mut all = adj.incoming.clone();
                        all.extend(&adj.outgoing);
                        return self.edges_to_nodes(all.into_iter().collect(), node_id);
                    }
                };
                
                self.edges_to_nodes(edge_ids.iter().cloned().collect(), node_id)
            })
            .unwrap_or_default()
    }

    fn edges_to_nodes(&self, edge_ids: Vec<EdgeId>, from_node: NodeId) -> Vec<NodeId> {
        edge_ids
            .into_iter()
            .filter_map(|edge_id| {
                self.edges.get(&edge_id).map(|edge| {
                    if edge.source == from_node {
                        edge.target
                    } else {
                        edge.source
                    }
                })
            })
            .collect()
    }

    pub fn update_node_property(&self, id: NodeId, key: String, value: Property) -> Result<()> {
        if let Some(mut node) = self.nodes.get_mut(&id) {
            node.properties.insert(key, value);
            Ok(())
        } else {
            Err(Error::Custom(format!("Node {:?} not found", id)))
        }
    }

    pub fn update_edge_property(&self, id: EdgeId, key: String, value: Property) -> Result<()> {
        if let Some(mut edge) = self.edges.get_mut(&id) {
            edge.properties.insert(key, value);
            Ok(())
        } else {
            Err(Error::Custom(format!("Edge {:?} not found", id)))
        }
    }

    pub fn remove_node(&self, id: NodeId) -> Result<()> {
        if let Some((_, node)) = self.nodes.remove(&id) {
            for label in &node.labels {
                if let Some(mut label_nodes) = self.labels.get_mut(label) {
                    label_nodes.remove(&id);
                }
            }
            
            if let Some((_, adj)) = self.adjacency.remove(&id) {
                for edge_id in adj.incoming.iter().chain(adj.outgoing.iter()) {
                    self.edges.remove(edge_id);
                }
            }
            
            Ok(())
        } else {
            Err(Error::Custom(format!("Node {:?} not found", id)))
        }
    }

    pub fn remove_edge(&self, id: EdgeId) -> Result<()> {
        if let Some((_, edge)) = self.edges.remove(&id) {
            if let Some(mut edge_types) = self.edge_types.get_mut(&edge.edge_type) {
                edge_types.remove(&id);
            }
            
            if let Some(mut adj) = self.adjacency.get_mut(&edge.source) {
                adj.outgoing.remove(&id);
            }
            
            if let Some(mut adj) = self.adjacency.get_mut(&edge.target) {
                adj.incoming.remove(&id);
            }
            
            Ok(())
        } else {
            Err(Error::Custom(format!("Edge {:?} not found", id)))
        }
    }

    fn index_properties(&self, element_id: ElementId, properties: &HashMap<String, Property>) {
        for (key, value) in properties {
            let property_value = match value {
                Property::Boolean(b) => Some(PropertyValue::Boolean(*b)),
                Property::Integer(i) => Some(PropertyValue::Integer(*i)),
                Property::String(s) => Some(PropertyValue::String(s.clone())),
                _ => None,
            };
            
            if let Some(pv) = property_value {
                self.properties_index
                    .entry(key.clone())
                    .or_insert_with(|| PropertyIndex {
                        property_name: key.clone(),
                        index_type: PropertyIndexType::Exact,
                        values: HashMap::new(),
                    })
                    .values
                    .entry(pv)
                    .or_insert_with(HashSet::new)
                    .insert(element_id);
            }
        }
    }

    pub fn find_by_property(&self, property_name: &str, value: &Property) -> Vec<ElementId> {
        let property_value = match value {
            Property::Boolean(b) => PropertyValue::Boolean(*b),
            Property::Integer(i) => PropertyValue::Integer(*i),
            Property::String(s) => PropertyValue::String(s.clone()),
            _ => return Vec::new(),
        };
        
        self.properties_index
            .get(property_name)
            .and_then(|index| index.values.get(&property_value))
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_statistics(&self) -> GraphStatistics {
        GraphStatistics {
            node_count: self.nodes.len(),
            edge_count: self.edges.len(),
            label_count: self.labels.len(),
            edge_type_count: self.edge_types.len(),
            property_count: self.properties_index.len(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Incoming,
    Outgoing,
    Both,
}

#[derive(Debug, Clone)]
pub struct GraphStatistics {
    pub node_count: usize,
    pub edge_count: usize,
    pub label_count: usize,
    pub edge_type_count: usize,
    pub property_count: usize,
}

impl Property {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Property::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Property::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Property::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Property::Boolean(b) => Some(*b),
            _ => None,
        }
    }
}