use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use crate::core::error::Result;
use dashmap::DashMap;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct EventStore {
    streams: Arc<DashMap<String, EventStream>>,
    snapshots: Arc<DashMap<String, Snapshot>>,
    projections: Arc<DashMap<String, Projection>>,
}

#[derive(Debug, Clone)]
pub struct EventStream {
    stream_id: String,
    events: Vec<DomainEvent>,
    version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    event_id: String,
    event_type: String,
    aggregate_id: String,
    data: serde_json::Value,
    metadata: EventMetadata,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    user_id: Option<String>,
    correlation_id: String,
    causation_id: String,
}

#[async_trait]
pub trait AggregateRoot: Send + Sync {
    type Command;
    type Event;
    type State;
    
    async fn handle(&self, command: Self::Command, state: &Self::State) -> Result<Vec<Self::Event>>;
    async fn apply(&self, state: &mut Self::State, event: &Self::Event) -> Result<()>;
    async fn project(&self, events: Vec<Self::Event>) -> Result<Self::State>;
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    aggregate_id: String,
    version: u64,
    state: serde_json::Value,
    timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct Projection {
    name: String,
    query: String,
    state: serde_json::Value,
}