use super::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use bincode::{encode_to_vec, decode_from_slice, config, Encode, Decode};

/// RPC message wrapper with request ID
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct RpcMessage {
    /// Unique request ID
    pub id: u64,
    
    /// The actual message
    pub message: RaftMessage,
}

/// RPC response wrapper
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct RpcResponse {
    /// Request ID this is responding to
    pub id: u64,
    
    /// The response
    pub response: RpcResponseType,
}

/// Types of RPC responses
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum RpcResponseType {
    RequestVote(RequestVoteReply),
    AppendEntries(AppendEntriesReply),
    InstallSnapshot(InstallSnapshotReply),
    Error(String),
}

impl RpcHandler {
    /// Start RPC server
    pub async fn start_server(&self, bind_address: String) -> Result<()> {
        let listener = TcpListener::bind(&bind_address).await
            .map_err(|e| Error::Io(e.to_string()))?;
        
        println!("RPC server listening on {}", bind_address);
        
        loop {
            let (stream, addr) = listener.accept().await
                .map_err(|e| Error::Io(e.to_string()))?;
            
            // Handle connection in a separate task
            let handler = self.clone();
            tokio::spawn(async move {
                if let Err(e) = handler.handle_connection(stream).await {
                    eprintln!("Error handling connection from {}: {}", addr, e);
                }
            });
        }
    }
    
    /// Handle incoming connection
    async fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
        
        loop {
            // Read message length (4 bytes)
            let mut len_bytes = [0u8; 4];
            match stream.read_exact(&mut len_bytes).await {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Connection closed
                    break;
                }
                Err(e) => return Err(Error::Io(e.to_string())),
            }
            
            let message_len = u32::from_be_bytes(len_bytes) as usize;
            
            if message_len > buffer.len() {
                buffer.resize(message_len, 0);
            }
            
            // Read message
            stream.read_exact(&mut buffer[..message_len]).await
                .map_err(|e| Error::Io(e.to_string()))?;
            
            // Deserialize message
            let (rpc_msg, _): (RpcMessage, usize) = decode_from_slice(&buffer[..message_len], config::standard())
                .map_err(|e| Error::Serialization(e.to_string()))?;
            
            // Handle message
            let response = self.handle_rpc_message(rpc_msg.message).await;
            
            // Send response
            let rpc_response = RpcResponse {
                id: rpc_msg.id,
                response,
            };
            
            let response_bytes = encode_to_vec(&rpc_response, config::standard())
                .map_err(|e| Error::Serialization(e.to_string()))?;
            
            // Write response length and data
            stream.write_all(&(response_bytes.len() as u32).to_be_bytes()).await
                .map_err(|e| Error::Io(e.to_string()))?;
            stream.write_all(&response_bytes).await
                .map_err(|e| Error::Io(e.to_string()))?;
            stream.flush().await
                .map_err(|e| Error::Io(e.to_string()))?;
        }
        
        Ok(())
    }
    
    /// Handle RPC message and return response
    async fn handle_rpc_message(&self, message: RaftMessage) -> RpcResponseType {
        // Forward to message channel for processing
        if let Err(e) = self.message_tx.send(message.clone()) {
            return RpcResponseType::Error(format!("Failed to process message: {}", e));
        }
        
        // For some messages, we need to wait for response
        match message {
            RaftMessage::RequestVote(args) => {
                // TODO: Implement proper response handling
                RpcResponseType::RequestVote(RequestVoteReply {
                    term: 0,
                    vote_granted: false,
                    reason: Some("Not implemented".to_string()),
                })
            }
            RaftMessage::AppendEntries(args) => {
                // TODO: Implement proper response handling
                RpcResponseType::AppendEntries(AppendEntriesReply {
                    term: 0,
                    success: false,
                    last_log_index: 0,
                    conflict_term: None,
                    conflict_index: None,
                })
            }
            RaftMessage::InstallSnapshot(args) => {
                // TODO: Implement proper response handling
                RpcResponseType::InstallSnapshot(InstallSnapshotReply {
                    term: 0,
                    bytes_received: 0,
                })
            }
            _ => RpcResponseType::Error("Unsupported message type".to_string()),
        }
    }
    
    /// Connect to a peer
    pub async fn connect_to_peer(&self, peer_id: NodeId, address: String) -> Result<()> {
        let stream = timeout(
            Duration::from_secs(5),
            TcpStream::connect(&address)
        ).await
            .map_err(|_| Error::Timeout("Connection timeout".to_string()))?
            .map_err(|e| Error::Io(e.to_string()))?;
        
        // Create peer connection
        let peer_conn = PeerConnection {
            node_id: peer_id,
            address: address.clone(),
            connected: true,
            last_seen: Instant::now(),
            stream: Some(Arc::new(RwLock::new(stream))),
            request_id: AtomicU64::new(0),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
        };
        
        self.peers.write().unwrap().insert(peer_id, peer_conn);
        
        println!("Connected to peer {} at {}", peer_id, address);
        
        Ok(())
    }
    
    /// Send request to peer
    async fn send_request(&self, peer_id: NodeId, message: RaftMessage) -> Result<RpcResponseType> {
        let peer_conn = self.peers.read().unwrap().get(&peer_id).cloned()
            .ok_or_else(|| Error::NotFound(format!("Peer {} not found", peer_id)))?;
        
        if !peer_conn.connected {
            // Try to reconnect
            self.connect_to_peer(peer_id, peer_conn.address.clone()).await?;
        }
        
        let request_id = peer_conn.request_id.fetch_add(1, Ordering::SeqCst);
        let rpc_msg = RpcMessage { id: request_id, message };
        
        let message_bytes = encode_to_vec(&rpc_msg, config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        // Create response channel
        let (tx, rx) = oneshot::channel();
        peer_conn.pending_requests.write().insert(request_id, tx);
        
        // Send request
        if let Some(stream_arc) = &peer_conn.stream {
            let mut stream = stream_arc.write();
            
            // Write message length and data
            stream.write_all(&(message_bytes.len() as u32).to_be_bytes()).await
                .map_err(|e| Error::Io(e.to_string()))?;
            stream.write_all(&message_bytes).await
                .map_err(|e| Error::Io(e.to_string()))?;
            stream.flush().await
                .map_err(|e| Error::Io(e.to_string()))?;
        }
        
        // Wait for response with timeout
        match timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(Error::Internal("Response channel closed".to_string())),
            Err(_) => {
                peer_conn.pending_requests.write().remove(&request_id);
                Err(Error::Timeout("Request timeout".to_string()))
            }
        }
    }
    
    /// Send RequestVote to peer
    pub async fn send_request_vote(&self, peer_id: NodeId, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        match self.send_request(peer_id, RaftMessage::RequestVote(args)).await? {
            RpcResponseType::RequestVote(reply) => Ok(reply),
            RpcResponseType::Error(e) => Err(Error::Rpc(e)),
            _ => Err(Error::Internal("Unexpected response type".to_string())),
        }
    }
    
    /// Send AppendEntries to peer
    pub async fn send_append_entries(&self, peer_id: NodeId, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        match self.send_request(peer_id, RaftMessage::AppendEntries(args)).await? {
            RpcResponseType::AppendEntries(reply) => Ok(reply),
            RpcResponseType::Error(e) => Err(Error::Rpc(e)),
            _ => Err(Error::Internal("Unexpected response type".to_string())),
        }
    }
    
    /// Send InstallSnapshot to peer
    pub async fn send_install_snapshot(&self, peer_id: NodeId, args: InstallSnapshotArgs) -> Result<InstallSnapshotReply> {
        match self.send_request(peer_id, RaftMessage::InstallSnapshot(args)).await? {
            RpcResponseType::InstallSnapshot(reply) => Ok(reply),
            RpcResponseType::Error(e) => Err(Error::Rpc(e)),
            _ => Err(Error::Internal("Unexpected response type".to_string())),
        }
    }
}

/// Enhanced peer connection with network stream
pub struct PeerConnection {
    pub node_id: NodeId,
    pub address: String,
    pub connected: bool,
    pub last_seen: Instant,
    pub stream: Option<Arc<RwLock<TcpStream>>>,
    pub request_id: AtomicU64,
    pub pending_requests: Arc<RwLock<HashMap<u64, oneshot::Sender<RpcResponseType>>>>,
}

impl Clone for PeerConnection {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            address: self.address.clone(),
            connected: self.connected,
            last_seen: self.last_seen,
            stream: self.stream.clone(),
            request_id: AtomicU64::new(self.request_id.load(Ordering::SeqCst)),
            pending_requests: self.pending_requests.clone(),
        }
    }
}

impl Clone for RpcHandler {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            peers: self.peers.clone(),
            message_tx: self.message_tx.clone(),
        }
    }
}

/// Helper to handle peer response processing
impl PeerConnection {
    /// Process incoming response
    pub fn process_response(&self, response: RpcResponse) {
        if let Some(tx) = self.pending_requests.write().remove(&response.id) {
            let _ = tx.send(response.response);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_rpc_serialization() {
        let args = RequestVoteArgs {
            term: 1,
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 1,
            pre_vote: false,
        };
        
        let rpc_msg = RpcMessage {
            id: 42,
            message: RaftMessage::RequestVote(args),
        };
        
        let bytes = encode_to_vec(&rpc_msg, config::standard()).unwrap();
        let (deserialized, _): (RpcMessage, _) = decode_from_slice(&bytes, config::standard()).unwrap();
        
        match deserialized.message {
            RaftMessage::RequestVote(args) => {
                assert_eq!(args.term, 1);
                assert_eq!(args.candidate_id, 2);
            }
            _ => panic!("Wrong message type"),
        }
    }
}