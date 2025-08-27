use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};
use crate::error::Result;
use dashmap::DashMap;

#[derive(Debug, Clone)]
pub struct ChangeStream {
    options: ChangeStreamOptions,
    resume_token: Option<ResumeToken>,
    receiver: Arc<mpsc::Receiver<ChangeEvent>>,
}

#[derive(Debug, Clone)]
pub struct ChangeStreamOptions {
    pub full_document: FullDocument,
    pub resume_after: Option<ResumeToken>,
    pub start_at_operation_time: Option<u64>,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum FullDocument {
    Default,
    UpdateLookup,
    WhenAvailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeToken {
    pub timestamp: u64,
    pub stream_id: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub id: ResumeToken,
    pub operation_type: String,
    pub full_document: Option<serde_json::Value>,
    pub document_key: serde_json::Value,
    pub update_description: Option<UpdateDescription>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateDescription {
    pub updated_fields: serde_json::Value,
    pub removed_fields: Vec<String>,
}

impl ChangeStream {
    pub async fn next(&mut self) -> Result<Option<ChangeEvent>> {
        Ok(None)
    }
}