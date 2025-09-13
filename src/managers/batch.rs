use std::sync::Arc;
use crate::{Database, Result};
use crate::utils::batching::{WriteBatch, FastAutoBatcher};

type SyncWriteBatcher = FastAutoBatcher;

pub struct BatchManager {
    db: Arc<Database>,
}

impl BatchManager {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
    
    pub fn put(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        self.db.batch_put(pairs)
    }
    
    pub fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        self.db.put_batch(pairs)
    }
    
    pub fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        self.db.write_batch(batch)
    }
    
    pub fn get_batch(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        self.db.get_batch(keys)
    }
    
    pub fn delete_batch(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>> {
        self.db.delete_batch(keys)
    }
    
    pub fn create_sync_batcher(db: Arc<Database>) -> Arc<SyncWriteBatcher> {
        Database::create_with_batcher(db)
    }
    
    pub fn create_auto_batcher(db: Arc<Database>) -> Arc<FastAutoBatcher> {
        Database::create_auto_batcher(db)
    }
    
    pub fn create_fast_auto_batcher(db: Arc<Database>) -> Arc<FastAutoBatcher> {
        Database::create_fast_auto_batcher(db)
    }
    
    pub fn enable_write_batching(db: Arc<Database>) -> Arc<Database> {
        Database::enable_write_batching(db)
    }
}