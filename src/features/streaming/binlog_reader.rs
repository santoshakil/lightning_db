use std::sync::Arc;
use tokio::sync::RwLock;
use bytes::Bytes;
use serde::{Serialize, Deserialize};
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct BinlogReader {
    position: Arc<RwLock<BinlogPosition>>,
    events: Arc<RwLock<Vec<BinlogEvent>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogPosition {
    pub file: String,
    pub position: u64,
    pub server_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogEvent {
    pub timestamp: u64,
    pub event_type: BinlogEventType,
    pub server_id: u64,
    pub log_position: u64,
    pub flags: u16,
    pub data: Bytes,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BinlogEventType {
    Unknown,
    Start,
    Query,
    Stop,
    Rotate,
    Intvar,
    Load,
    Slave,
    CreateFile,
    AppendBlock,
    ExecLoad,
    DeleteFile,
    NewLoad,
    Rand,
    UserVar,
    FormatDescription,
    Xid,
    BeginLoadQuery,
    ExecuteLoadQuery,
    TableMap,
    WriteRows,
    UpdateRows,
    DeleteRows,
    Incident,
    Heartbeat,
    Ignorable,
    RowsQuery,
    GtidLog,
    AnonymousGtidLog,
    PreviousGtidsLog,
}

impl BinlogReader {
    pub async fn read_event(&self) -> Result<Option<BinlogEvent>> {
        Ok(None)
    }
    
    pub async fn seek(&self, position: BinlogPosition) -> Result<()> {
        Ok(())
    }
}