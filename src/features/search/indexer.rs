use std::sync::Arc;
use std::collections::HashMap;
use crate::error::Result;

pub struct Indexer {
    writer: Arc<IndexWriter>,
}

pub struct Document {
    pub id: String,
    pub fields: Vec<Field>,
}

pub struct Field {
    pub name: String,
    pub value: FieldValue,
    pub indexed: bool,
    pub stored: bool,
}

pub enum FieldValue {
    Text(String),
    Number(f64),
    Boolean(bool),
    Date(u64),
}

pub struct IndexWriter {
    buffer: Vec<Document>,
}

impl IndexWriter {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }
    
    pub async fn add_document(&mut self, doc: Document) -> Result<()> {
        self.buffer.push(doc);
        Ok(())
    }
    
    pub async fn commit(&mut self) -> Result<()> {
        self.buffer.clear();
        Ok(())
    }
}