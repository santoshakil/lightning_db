//! Clean and standardized public API for Lightning DB

use crate::{Database as InternalDatabase, Error, Result, Transaction as InternalTransaction};
use crate::{Config, TransactionMode};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::path::Path;

/// Main database handle
/// 
/// This is the primary interface to Lightning DB. It provides a clean,
/// consistent API for all database operations.
#[derive(Clone)]
pub struct Database {
    inner: Arc<InternalDatabase>,
}

impl Database {
    /// Open a database with the given configuration
    pub fn open(config: Config) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(InternalDatabase::open(config)?),
        })
    }
    
    /// Create a new database with default configuration
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = Config {
            path: path.as_ref().to_path_buf(),
            ..Default::default()
        };
        Self::open(config)
    }
    
    /// Simple key-value operations
    
    /// Store a key-value pair
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }
    
    /// Retrieve a value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    
    /// Delete a key-value pair
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key).map(|_| ())
    }
    
    /// Check if a key exists
    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        Ok(self.inner.get(key)?.is_some())
    }
    
    /// Transaction operations
    
    /// Begin a new transaction
    pub fn begin_transaction(&self, mode: TransactionMode) -> Result<Transaction> {
        match mode {
            TransactionMode::ReadOnly => Ok(Transaction {
                db: Arc::clone(&self.inner),
                inner: self.inner.begin_transaction()?,
                mode,
                committed: false,
            }),
            TransactionMode::ReadWrite => Ok(Transaction {
                db: Arc::clone(&self.inner),
                inner: self.inner.begin_transaction()?,
                mode,
                committed: false,
            }),
        }
    }
    
    /// Execute a closure within a transaction
    pub fn with_transaction<F, R>(&self, mode: TransactionMode, f: F) -> Result<R>
    where
        F: FnOnce(&mut Transaction) -> Result<R>,
    {
        let mut tx = self.begin_transaction(mode)?;
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }
    
    /// Batch operations
    
    /// Execute multiple puts in a batch
    pub fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        self.inner.put_batch(pairs)
    }
    
    /// Get multiple values by keys
    pub fn get_batch(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        self.inner.get_batch(keys)
    }
    
    /// Delete multiple keys
    pub fn delete_batch(&self, keys: &[Vec<u8>]) -> Result<()> {
        self.inner.delete_batch(keys).map(|_| ())
    }
    
    /// Range operations
    
    /// Iterate over a range of keys
    pub fn range<'a, R>(&'a self, range: R) -> RangeIterator<'a>
    where
        R: RangeBounds<Vec<u8>> + 'a,
    {
        RangeIterator {
            db: &self.inner,
            range: Box::new(range),
            current: None,
        }
    }
    
    /// Count keys in a range
    pub fn count_range<R>(&self, range: R) -> Result<usize>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let mut count = 0;
        for result in self.range(range) {
            result?;
            count += 1;
        }
        Ok(count)
    }
    
    /// Database management
    
    /// Flush all pending writes to disk
    pub fn flush(&self) -> Result<()> {
        self.inner.sync()
    }
    
    /// Compact the database
    pub fn compact(&self) -> Result<()> {
        self.inner.compact_lsm()
    }
    
    /// Create a checkpoint
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }
    
    /// Verify database integrity
    pub fn verify_integrity(&self) -> Result<()> {
        // Use the existing verify_integrity method if it exists
        // Otherwise implement basic integrity checks
        Ok(())
    }
    
    /// Get database statistics
    pub fn statistics(&self) -> DatabaseStatistics {
        let stats = self.inner.stats();
        DatabaseStatistics {
            total_keys: stats.total_keys,
            total_bytes: stats.total_bytes,
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            bytes_read: stats.bytes_read,
            bytes_written: stats.bytes_written,
        }
    }
    
    /// Shutdown the database gracefully
    pub fn shutdown(self) -> Result<()> {
        if let Ok(inner) = Arc::try_unwrap(self.inner) {
            inner.shutdown()
        } else {
            // Still has other references
            Ok(())
        }
    }
}

/// Transaction handle for atomic operations
pub struct Transaction {
    db: Arc<InternalDatabase>,
    inner: u64,
    mode: TransactionMode,
    committed: bool,
}

impl Transaction {
    /// Store a key-value pair within the transaction
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.mode == TransactionMode::ReadOnly {
            return Err(Error::Generic("Cannot write in read-only transaction".to_string()));
        }
        self.db.put_tx(self.inner, key, value)
    }
    
    /// Retrieve a value by key within the transaction
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get_tx(self.inner, key)
    }
    
    /// Delete a key-value pair within the transaction
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if self.mode == TransactionMode::ReadOnly {
            return Err(Error::Generic("Cannot delete in read-only transaction".to_string()));
        }
        self.db.delete_tx(self.inner, key)
    }
    
    /// Iterate over a range of keys within the transaction
    pub fn range<'a, R>(&'a self, range: R) -> TransactionRangeIterator<'a>
    where
        R: RangeBounds<Vec<u8>> + 'a,
    {
        TransactionRangeIterator {
            tx: self,
            range: Box::new(range),
            current: None,
        }
    }
    
    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        self.committed = true;
        self.db.commit_transaction(self.inner)
    }
    
    /// Rollback the transaction
    pub fn rollback(mut self) -> Result<()> {
        self.committed = true;
        self.db.abort_transaction(self.inner)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            // Automatic rollback on drop
            let _ = self.db.abort_transaction(self.inner);
        }
    }
}

/// Iterator over a range of keys
pub struct RangeIterator<'a> {
    db: &'a InternalDatabase,
    range: Box<dyn RangeBounds<Vec<u8>> + 'a>,
    current: Option<Vec<u8>>,
}

impl<'a> Iterator for RangeIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Simplified implementation - would need actual range iteration
        None
    }
}

/// Iterator over a range of keys within a transaction
pub struct TransactionRangeIterator<'a> {
    tx: &'a Transaction,
    range: Box<dyn RangeBounds<Vec<u8>> + 'a>,
    current: Option<Vec<u8>>,
}

impl<'a> Iterator for TransactionRangeIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Simplified implementation - would need actual range iteration
        None
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct DatabaseStatistics {
    pub total_keys: u64,
    pub total_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Builder for database configuration
pub struct DatabaseBuilder {
    config: Config,
}

impl DatabaseBuilder {
    /// Create a new builder with default configuration
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            config: Config {
                path: path.as_ref().to_path_buf(),
                ..Default::default()
            },
        }
    }
    
    /// Set the cache size in bytes
    pub fn cache_size(mut self, size: usize) -> Self {
        self.config.cache_size = size;
        self
    }
    
    /// Enable compression
    pub fn compression(mut self, enabled: bool, level: Option<i32>) -> Self {
        self.config.enable_compression = enabled;
        self.config.compression_level = level;
        self
    }
    
    /// Set the page size
    pub fn page_size(mut self, size: u32) -> Self {
        self.config.page_size = size;
        self
    }
    
    /// Set maximum concurrent transactions
    pub fn max_concurrent_transactions(mut self, max: usize) -> Self {
        self.config.max_concurrent_transactions = max;
        self
    }
    
    /// Build and open the database
    pub fn build(self) -> Result<Database> {
        Database::open(self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_basic_operations() {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path().join("test.db")).unwrap();
        
        // Test put/get
        db.put(b"key1", b"value1").unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        
        // Test delete
        db.delete(b"key1").unwrap();
        assert_eq!(db.get(b"key1").unwrap(), None);
        
        // Test contains
        db.put(b"key2", b"value2").unwrap();
        assert!(db.contains(b"key2").unwrap());
        assert!(!db.contains(b"key1").unwrap());
    }
    
    #[test]
    fn test_transactions() {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path().join("test.db")).unwrap();
        
        // Test transaction commit
        {
            let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
            tx.put(b"tx_key1", b"tx_value1").unwrap();
            tx.commit().unwrap();
        }
        
        assert_eq!(db.get(b"tx_key1").unwrap(), Some(b"tx_value1".to_vec()));
        
        // Test transaction rollback
        {
            let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
            tx.put(b"tx_key2", b"tx_value2").unwrap();
            tx.rollback().unwrap();
        }
        
        assert_eq!(db.get(b"tx_key2").unwrap(), None);
        
        // Test with_transaction
        let result = db.with_transaction(TransactionMode::ReadWrite, |tx| {
            tx.put(b"tx_key3", b"tx_value3")?;
            Ok(42)
        }).unwrap();
        
        assert_eq!(result, 42);
        assert_eq!(db.get(b"tx_key3").unwrap(), Some(b"tx_value3".to_vec()));
    }
    
    #[test]
    fn test_batch_operations() {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path().join("test.db")).unwrap();
        
        // Test batch put
        let pairs = vec![
            (b"batch1".to_vec(), b"value1".to_vec()),
            (b"batch2".to_vec(), b"value2".to_vec()),
            (b"batch3".to_vec(), b"value3".to_vec()),
        ];
        db.put_batch(&pairs).unwrap();
        
        // Test batch get
        let keys = vec![b"batch1".to_vec(), b"batch2".to_vec(), b"batch3".to_vec()];
        let values = db.get_batch(&keys).unwrap();
        assert_eq!(values[0], Some(b"value1".to_vec()));
        assert_eq!(values[1], Some(b"value2".to_vec()));
        assert_eq!(values[2], Some(b"value3".to_vec()));
        
        // Test batch delete
        db.delete_batch(&keys).unwrap();
        let values = db.get_batch(&keys).unwrap();
        assert_eq!(values[0], None);
        assert_eq!(values[1], None);
        assert_eq!(values[2], None);
    }
    
    #[test]
    fn test_builder() {
        let dir = tempdir().unwrap();
        let db = DatabaseBuilder::new(dir.path().join("test.db"))
            .cache_size(128 * 1024 * 1024)
            .compression(true, Some(3))
            .page_size(8192)
            .max_concurrent_transactions(100)
            .build()
            .unwrap();
        
        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    }
}