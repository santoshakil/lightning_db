use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use crate::core::error::Error;
use super::core::{NodeId, Term, LogIndex};
use super::log::LogEntry;
use super::snapshot::Snapshot;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::net::{TcpStream, TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{Bytes, BytesMut, BufMut};
use std::collections::HashMap;
use parking_lot::RwLock;

const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
    ForwardRequest(ForwardRequest),
    ForwardResponse(ForwardResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardRequest {
    pub client_id: u64,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardResponse {
    pub success: bool,
    pub result: Option<Vec<u8>>,
    pub leader_hint: Option<NodeId>,
}

#[async_trait]
pub trait RaftRpc: Send + Sync {
    async fn request_vote(&self, target: NodeId, request: VoteRequest) -> Result<VoteResponse, Error>;
    async fn append_entries(&self, target: NodeId, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, Error>;
    async fn install_snapshot(&self, target: NodeId, request: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Error>;
    async fn forward_to_leader(&self, leader: NodeId, request: ForwardRequest) -> Result<ForwardResponse, Error>;
}

pub struct TcpRaftRpc {
    node_id: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
    listener: Option<TcpListener>,
}

struct PeerConnection {
    address: String,
    stream: Option<TcpStream>,
    last_heartbeat: std::time::Instant,
}

impl TcpRaftRpc {
    pub async fn new(node_id: NodeId, bind_address: &str) -> Result<Self, Error> {
        let listener = TcpListener::bind(bind_address).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        Ok(Self {
            node_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
            listener: Some(listener),
        })
    }

    pub fn add_peer(&self, node_id: NodeId, address: String) {
        self.peers.write().insert(node_id, PeerConnection {
            address,
            stream: None,
            last_heartbeat: std::time::Instant::now(),
        });
    }

    async fn get_connection(&self, target: NodeId) -> Result<TcpStream, Error> {
        let mut peers = self.peers.write();
        
        if let Some(peer) = peers.get_mut(&target) {
            if peer.stream.is_none() {
                let stream = TcpStream::connect(&peer.address).await
                    .map_err(|e| Error::Rpc(format!("Failed to connect to {}: {}", peer.address, e)))?;
                
                peer.stream = Some(stream.try_clone()
                    .map_err(|e| Error::Rpc(e.to_string()))?);
            }
            
            if let Some(stream) = &peer.stream {
                return Ok(stream.try_clone()
                    .map_err(|e| Error::Rpc(e.to_string()))?);
            }
        }
        
        Err(Error::NotFound(format!("Peer {} not found", target)))
    }

    async fn send_message(&self, target: NodeId, message: RpcMessage) -> Result<RpcMessage, Error> {
        let mut stream = self.get_connection(target).await?;
        
        let data = bincode::encode_to_vec(&message)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        let size = (data.len() as u32).to_le_bytes();
        
        stream.write_all(&size).await
            .map_err(|e| Error::Rpc(e.to_string()))?;
        
        stream.write_all(&data).await
            .map_err(|e| Error::Rpc(e.to_string()))?;
        
        stream.flush().await
            .map_err(|e| Error::Rpc(e.to_string()))?;
        
        // Read response
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await
            .map_err(|e| Error::Rpc(e.to_string()))?;
        
        let size = u32::from_le_bytes(size_buf) as usize;
        
        if size > MAX_MESSAGE_SIZE {
            return Err(Error::Rpc(format!("Message too large: {} bytes", size)));
        }
        
        let mut response_buf = vec![0u8; size];
        stream.read_exact(&mut response_buf).await
            .map_err(|e| Error::Rpc(e.to_string()))?;
        
        bincode::deserialize(&response_buf)
            .map_err(|e| Error::Serialization(e.to_string()))
    }
}

#[async_trait]
impl RaftRpc for TcpRaftRpc {
    async fn request_vote(&self, target: NodeId, request: VoteRequest) -> Result<VoteResponse, Error> {
        let response = self.send_message(target, RpcMessage::VoteRequest(request)).await?;
        
        match response {
            RpcMessage::VoteResponse(resp) => Ok(resp),
            _ => Err(Error::Rpc("Unexpected response type".to_string())),
        }
    }

    async fn append_entries(&self, target: NodeId, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, Error> {
        let response = self.send_message(target, RpcMessage::AppendEntriesRequest(request)).await?;
        
        match response {
            RpcMessage::AppendEntriesResponse(resp) => Ok(resp),
            _ => Err(Error::Rpc("Unexpected response type".to_string())),
        }
    }

    async fn install_snapshot(&self, target: NodeId, request: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Error> {
        let response = self.send_message(target, RpcMessage::InstallSnapshotRequest(request)).await?;
        
        match response {
            RpcMessage::InstallSnapshotResponse(resp) => Ok(resp),
            _ => Err(Error::Rpc("Unexpected response type".to_string())),
        }
    }

    async fn forward_to_leader(&self, leader: NodeId, request: ForwardRequest) -> Result<ForwardResponse, Error> {
        let response = self.send_message(leader, RpcMessage::ForwardRequest(request)).await?;
        
        match response {
            RpcMessage::ForwardResponse(resp) => Ok(resp),
            _ => Err(Error::Rpc("Unexpected response type".to_string())),
        }
    }
}

pub struct GrpcRaftRpc {
    // gRPC-based RPC implementation
    // Would use tonic or similar
}

#[async_trait]
impl RaftRpc for GrpcRaftRpc {
    async fn request_vote(&self, target: NodeId, request: VoteRequest) -> Result<VoteResponse, Error> {
        // gRPC implementation
        Ok(VoteResponse {
            term: request.term,
            vote_granted: false,
        })
    }

    async fn append_entries(&self, target: NodeId, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, Error> {
        // gRPC implementation
        Ok(AppendEntriesResponse {
            term: request.term,
            success: false,
        })
    }

    async fn install_snapshot(&self, target: NodeId, request: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Error> {
        // gRPC implementation
        Ok(InstallSnapshotResponse {
            term: request.term,
        })
    }

    async fn forward_to_leader(&self, leader: NodeId, request: ForwardRequest) -> Result<ForwardResponse, Error> {
        // gRPC implementation
        Ok(ForwardResponse {
            success: false,
            result: None,
            leader_hint: None,
        })
    }
}

pub struct MemoryRaftRpc {
    channels: Arc<RwLock<HashMap<NodeId, mpsc::Sender<RpcMessage>>>>,
}

impl MemoryRaftRpc {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_node(&self, node_id: NodeId, sender: mpsc::Sender<RpcMessage>) {
        self.channels.write().insert(node_id, sender);
    }
}

#[async_trait]
impl RaftRpc for MemoryRaftRpc {
    async fn request_vote(&self, target: NodeId, request: VoteRequest) -> Result<VoteResponse, Error> {
        let channels = self.channels.read();
        
        if let Some(sender) = channels.get(&target) {
            let (tx, rx) = oneshot::channel();
            
            sender.send(RpcMessage::VoteRequest(request)).await
                .map_err(|_| Error::Rpc("Failed to send vote request".to_string()))?;
            
            // In a real implementation, would wait for response
            Ok(VoteResponse {
                term: 0,
                vote_granted: false,
            })
        } else {
            Err(Error::NotFound(format!("Node {} not found", target)))
        }
    }

    async fn append_entries(&self, target: NodeId, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, Error> {
        let channels = self.channels.read();
        
        if let Some(sender) = channels.get(&target) {
            sender.send(RpcMessage::AppendEntriesRequest(request)).await
                .map_err(|_| Error::Rpc("Failed to send append entries".to_string()))?;
            
            Ok(AppendEntriesResponse {
                term: 0,
                success: false,
            })
        } else {
            Err(Error::NotFound(format!("Node {} not found", target)))
        }
    }

    async fn install_snapshot(&self, target: NodeId, request: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Error> {
        let channels = self.channels.read();
        
        if let Some(sender) = channels.get(&target) {
            sender.send(RpcMessage::InstallSnapshotRequest(request)).await
                .map_err(|_| Error::Rpc("Failed to send install snapshot".to_string()))?;
            
            Ok(InstallSnapshotResponse {
                term: 0,
            })
        } else {
            Err(Error::NotFound(format!("Node {} not found", target)))
        }
    }

    async fn forward_to_leader(&self, leader: NodeId, request: ForwardRequest) -> Result<ForwardResponse, Error> {
        let channels = self.channels.read();
        
        if let Some(sender) = channels.get(&leader) {
            sender.send(RpcMessage::ForwardRequest(request)).await
                .map_err(|_| Error::Rpc("Failed to forward to leader".to_string()))?;
            
            Ok(ForwardResponse {
                success: false,
                result: None,
                leader_hint: None,
            })
        } else {
            Err(Error::NotFound(format!("Leader {} not found", leader)))
        }
    }
}