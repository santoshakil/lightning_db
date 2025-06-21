use crate::error::Result;
use crate::Database;
use std::sync::Arc;

/// Simple batching interface that groups writes into transactions
pub struct SimpleBatcher {
    db: Arc<Database>,
    batch_size: usize,
}

impl SimpleBatcher {
    pub fn new(db: Arc<Database>, batch_size: usize) -> Self {
        Self {
            db,
            batch_size,
        }
    }
    
    /// Write multiple key-value pairs in a single transaction
    pub fn put_batch(&self, writes: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        
        let tx_id = self.db.begin_transaction()?;
        
        for (key, value) in writes {
            self.db.put_tx(tx_id, key, value)?;
        }
        
        self.db.commit_transaction(tx_id)
    }
    
    /// Helper to batch multiple individual puts
    pub fn put_many(&self, writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        
        // Process in chunks of batch_size
        for chunk in writes.chunks(self.batch_size) {
            self.put_batch(chunk)?;
        }
        
        Ok(())
    }
}