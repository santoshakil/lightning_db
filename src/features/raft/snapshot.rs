use serde::{Serialize, Deserialize};
use crate::core::error::Error;
use super::core::{Term, LogIndex};
use super::membership::ClusterConfig;
use bytes::{Bytes, BytesMut, BufMut};
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub cluster_config: ClusterConfig,
}

pub struct SnapshotBuilder {
    metadata: SnapshotMetadata,
    chunks: Vec<Vec<u8>>,
    current_size: usize,
}

impl SnapshotBuilder {
    pub fn new(last_index: LogIndex, last_term: Term, config: ClusterConfig) -> Self {
        Self {
            metadata: SnapshotMetadata {
                last_included_index: last_index,
                last_included_term: last_term,
                cluster_config: config,
            },
            chunks: Vec::new(),
            current_size: 0,
        }
    }

    pub fn add_chunk(&mut self, data: Vec<u8>) {
        self.current_size += data.len();
        self.chunks.push(data);
    }

    pub fn build(self) -> Snapshot {
        let mut data = Vec::with_capacity(self.current_size);
        for chunk in self.chunks {
            data.extend(chunk);
        }
        
        Snapshot {
            metadata: self.metadata,
            data,
        }
    }
}

pub struct SnapshotManager {
    snapshots: Arc<RwLock<std::collections::BTreeMap<LogIndex, Arc<Snapshot>>>>,
    max_snapshots: usize,
}

impl SnapshotManager {
    pub fn new(max_snapshots: usize) -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            max_snapshots,
        }
    }

    pub fn add_snapshot(&self, snapshot: Snapshot) {
        let index = snapshot.metadata.last_included_index;
        let snapshot = Arc::new(snapshot);
        
        let mut snapshots = self.snapshots.write();
        snapshots.insert(index, snapshot);
        
        // Remove old snapshots if we exceed the limit
        while snapshots.len() > self.max_snapshots {
            if let Some(first_key) = snapshots.keys().next().cloned() {
                snapshots.remove(&first_key);
            }
        }
    }

    pub fn get_snapshot(&self, index: LogIndex) -> Option<Arc<Snapshot>> {
        self.snapshots.read().get(&index).cloned()
    }

    pub fn get_latest_snapshot(&self) -> Option<Arc<Snapshot>> {
        self.snapshots.read().values().last().cloned()
    }

    pub fn remove_snapshots_before(&self, index: LogIndex) {
        self.snapshots.write().retain(|&idx, _| idx >= index);
    }

    pub fn list_snapshots(&self) -> Vec<LogIndex> {
        self.snapshots.read().keys().cloned().collect()
    }
}

pub struct ChunkedSnapshot {
    metadata: SnapshotMetadata,
    chunks: Vec<SnapshotChunk>,
    received_chunks: Arc<RwLock<Vec<Option<Vec<u8>>>>>,
    total_size: usize,
}

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub index: usize,
    pub total_chunks: usize,
    pub data: Vec<u8>,
}

impl ChunkedSnapshot {
    pub fn new(snapshot: Snapshot, chunk_size: usize) -> Self {
        let chunks = Self::split_into_chunks(&snapshot.data, chunk_size);
        let total_size = snapshot.data.len();
        
        Self {
            metadata: snapshot.metadata,
            chunks,
            received_chunks: Arc::new(RwLock::new(Vec::new())),
            total_size,
        }
    }

    fn split_into_chunks(data: &[u8], chunk_size: usize) -> Vec<SnapshotChunk> {
        let mut chunks = Vec::new();
        let total_chunks = (data.len() + chunk_size - 1) / chunk_size;
        
        for (index, chunk) in data.chunks(chunk_size).enumerate() {
            chunks.push(SnapshotChunk {
                index,
                total_chunks,
                data: chunk.to_vec(),
            });
        }
        
        chunks
    }

    pub fn get_chunk(&self, index: usize) -> Option<&SnapshotChunk> {
        self.chunks.get(index)
    }

    pub fn add_received_chunk(&self, index: usize, data: Vec<u8>) -> Result<(), Error> {
        let mut received = self.received_chunks.write();
        
        if received.len() <= index {
            received.resize(index + 1, None);
        }
        
        received[index] = Some(data);
        
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        let received = self.received_chunks.read();
        
        if received.len() != self.chunks.len() {
            return false;
        }
        
        received.iter().all(|chunk| chunk.is_some())
    }

    pub fn build_snapshot(&self) -> Result<Snapshot, Error> {
        if !self.is_complete() {
            return Err(Error::InvalidOperation {
                reason: "Snapshot is not complete".to_string(),
            });
        }
        
        let received = self.received_chunks.read();
        let mut data = Vec::with_capacity(self.total_size);
        
        for chunk in received.iter() {
            if let Some(chunk_data) = chunk {
                data.extend(chunk_data);
            }
        }
        
        Ok(Snapshot {
            metadata: self.metadata.clone(),
            data,
        })
    }
}

pub struct SnapshotReader {
    snapshot: Arc<Snapshot>,
    position: usize,
}

impl SnapshotReader {
    pub fn new(snapshot: Arc<Snapshot>) -> Self {
        Self {
            snapshot,
            position: 0,
        }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> usize {
        let remaining = self.snapshot.data.len() - self.position;
        let to_read = buf.len().min(remaining);
        
        buf[..to_read].copy_from_slice(&self.snapshot.data[self.position..self.position + to_read]);
        self.position += to_read;
        
        to_read
    }

    pub fn remaining(&self) -> usize {
        self.snapshot.data.len() - self.position
    }

    pub fn reset(&mut self) {
        self.position = 0;
    }
}

pub struct SnapshotWriter {
    metadata: SnapshotMetadata,
    buffer: BytesMut,
}

impl SnapshotWriter {
    pub fn new(last_index: LogIndex, last_term: Term, config: ClusterConfig) -> Self {
        Self {
            metadata: SnapshotMetadata {
                last_included_index: last_index,
                last_included_term: last_term,
                cluster_config: config,
            },
            buffer: BytesMut::new(),
        }
    }

    pub fn write(&mut self, data: &[u8]) {
        self.buffer.put_slice(data);
    }

    pub fn build(self) -> Snapshot {
        Snapshot {
            metadata: self.metadata,
            data: self.buffer.freeze().to_vec(),
        }
    }
}