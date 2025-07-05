/// Extension trait for Database to add zero-copy key operations
use crate::{Database, Result, Key, ConsistencyLevel};
use crate::write_batch::WriteBatch;
use crate::logging;

impl Database {
    /// Get value by key using zero-copy key type
    pub fn get_key(&self, key: &Key) -> Result<Option<Vec<u8>>> {
        self.get(key.as_bytes())
    }
    
    /// Get value with consistency level using zero-copy key type
    pub fn get_key_with_consistency(&self, key: &Key, level: ConsistencyLevel) -> Result<Option<Vec<u8>>> {
        self.get_with_consistency(key.as_bytes(), level)
    }
    
    /// Put key-value pair using zero-copy key type
    pub fn put_key(&self, key: &Key, value: &[u8]) -> Result<()> {
        let _timer = logging::OperationTimer::with_key("put_key", key.as_bytes());
        
        // Optimization: For inline keys (no heap allocation), use optimized path
        if key.is_inline() && value.len() <= 1024 {
            return self.put_small_optimized(key.as_bytes(), value);
        }
        
        self.put(key.as_bytes(), value)
    }
    
    /// Put with consistency level using zero-copy key type
    pub fn put_key_with_consistency(&self, key: &Key, value: &[u8], level: ConsistencyLevel) -> Result<()> {
        self.put_with_consistency(key.as_bytes(), value, level)
    }
    
    /// Delete by key using zero-copy key type
    pub fn delete_key(&self, key: &Key) -> Result<bool> {
        self.delete(key.as_bytes())
    }
    
    /// Batch get using zero-copy keys
    pub fn get_batch_keys(&self, keys: &[Key]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        
        // Try to batch cache lookups if possible
        if let Some(ref memory_pool) = self.memory_pool {
            for key in keys {
                if let Ok(Some(cached_value)) = memory_pool.cache_get(key.as_bytes()) {
                    results.push(Some(cached_value));
                } else {
                    // Fall back to regular get for cache misses
                    results.push(self.get(key.as_bytes())?);
                }
            }
        } else {
            // No cache, just do regular gets
            for key in keys {
                results.push(self.get(key.as_bytes())?);
            }
        }
        
        Ok(results)
    }
    
    /// Batch put using zero-copy keys
    pub fn put_batch_keys(&self, pairs: &[(Key, Vec<u8>)]) -> Result<()> {
        // Convert to WriteBatch for atomic operations
        let mut batch = WriteBatch::new();
        
        for (key, value) in pairs {
            let _ = batch.put(key.to_vec(), value.clone());
        }
        
        self.write_batch(&batch)
    }
    
    /// Delete batch using zero-copy keys
    pub fn delete_batch_keys(&self, keys: &[Key]) -> Result<Vec<bool>> {
        let tx_id = self.begin_transaction()?;
        let mut results = Vec::with_capacity(keys.len());
        
        for key in keys {
            let exists = self.get_tx(tx_id, key.as_bytes())?.is_some();
            if exists {
                self.delete_tx(tx_id, key.as_bytes())?;
            }
            results.push(exists);
        }
        
        self.commit_transaction(tx_id)?;
        Ok(results)
    }
    
    /// Optimized range scan using zero-copy keys
    pub fn scan_keys(&self, start_key: Option<&Key>, end_key: Option<&Key>) -> Result<Vec<(Key, Vec<u8>)>> {
        let start = start_key.map(|k| k.to_vec());
        let end = end_key.map(|k| k.to_vec());
        
        let mut iterator = self.scan(start, end)?;
        let mut results = Vec::new();
        
        // Collect all items from iterator
        loop {
            match iterator.next() {
                Some(Ok((k, v))) => results.push((Key::from_vec(k), v)),
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        
        Ok(results)
    }
    
    /// Prefix scan using zero-copy key
    pub fn prefix_scan_key(&self, prefix: &Key) -> Result<Vec<(Key, Vec<u8>)>> {
        let mut iterator = self.scan_prefix(prefix.as_bytes())?;
        let mut results = Vec::new();
        
        // Collect all items from iterator
        loop {
            match iterator.next() {
                Some(Ok((k, v))) => results.push((Key::from_vec(k), v)),
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        
        Ok(results)
    }
    
    /// Transaction operations with zero-copy keys
    pub fn get_tx_key(&self, tx_id: u64, key: &Key) -> Result<Option<Vec<u8>>> {
        self.get_tx(tx_id, key.as_bytes())
    }
    
    pub fn put_tx_key(&self, tx_id: u64, key: &Key, value: &[u8]) -> Result<()> {
        self.put_tx(tx_id, key.as_bytes(), value)
    }
    
    pub fn delete_tx_key(&self, tx_id: u64, key: &Key) -> Result<()> {
        self.delete_tx(tx_id, key.as_bytes())
    }
    
    /// Optimized multi-get that returns keys as zero-copy types
    pub fn multi_get_keys(&self, keys: &[Key]) -> Result<Vec<(Key, Option<Vec<u8>>)>> {
        let mut results = Vec::with_capacity(keys.len());
        
        for key in keys {
            let value = self.get(key.as_bytes())?;
            results.push((key.clone(), value));
        }
        
        Ok(results)
    }
    
    /// Check if key exists (more efficient than get for existence checks)
    pub fn exists_key(&self, key: &Key) -> Result<bool> {
        // Fast path: check cache first
        if let Some(ref memory_pool) = self.memory_pool {
            if memory_pool.cache_get(key.as_bytes()).is_ok() {
                return Ok(true);
            }
        }
        
        // Check storage
        Ok(self.get(key.as_bytes())?.is_some())
    }
    
    /// Atomic compare-and-swap using zero-copy key
    pub fn compare_and_swap_key(
        &self,
        key: &Key,
        expected_value: Option<&[u8]>,
        new_value: Option<&[u8]>,
    ) -> Result<bool> {
        let tx_id = self.begin_transaction()?;
        
        let current_value = self.get_tx(tx_id, key.as_bytes())?;
        
        // Check if current value matches expected
        let matches = match (current_value.as_deref(), expected_value) {
            (None, None) => true,
            (Some(curr), Some(exp)) => curr == exp,
            _ => false,
        };
        
        if matches {
            // Apply the new value
            match new_value {
                Some(val) => self.put_tx(tx_id, key.as_bytes(), val)?,
                None => self.delete_tx(tx_id, key.as_bytes())?,
            }
            self.commit_transaction(tx_id)?;
            Ok(true)
        } else {
            self.abort_transaction(tx_id)?;
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LightningDbConfig;
    use tempfile::TempDir;
    
    #[test]
    fn test_key_operations() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
        
        // Test put and get with Key type
        let key = Key::from("test_key");
        let value = b"test_value";
        
        db.put_key(&key, value)?;
        let retrieved = db.get_key(&key)?;
        assert_eq!(retrieved.as_deref(), Some(value.as_ref()));
        
        // Test delete
        assert!(db.delete_key(&key)?);
        assert!(db.get_key(&key)?.is_none());
        
        Ok(())
    }
    
    #[test]
    fn test_batch_operations() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
        
        // Test batch put
        let pairs = vec![
            (Key::from("key1"), b"value1".to_vec()),
            (Key::from("key2"), b"value2".to_vec()),
            (Key::from("key3"), b"value3".to_vec()),
        ];
        
        db.put_batch_keys(&pairs)?;
        
        // Test batch get
        let keys = vec![Key::from("key1"), Key::from("key2"), Key::from("key3")];
        let values = db.get_batch_keys(&keys)?;
        
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].as_deref(), Some(b"value1".as_ref()));
        assert_eq!(values[1].as_deref(), Some(b"value2".as_ref()));
        assert_eq!(values[2].as_deref(), Some(b"value3".as_ref()));
        
        Ok(())
    }
    
    #[test]
    fn test_inline_key_optimization() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
        
        // Small key that should be inline
        let small_key = Key::from("small");
        assert!(small_key.is_inline());
        
        // Large key that requires heap allocation
        let large_key = Key::from_vec(vec![b'x'; 64]);
        assert!(!large_key.is_inline());
        
        // Both should work correctly
        db.put_key(&small_key, b"small_value")?;
        db.put_key(&large_key, b"large_value")?;
        
        assert_eq!(db.get_key(&small_key)?.as_deref(), Some(b"small_value".as_ref()));
        assert_eq!(db.get_key(&large_key)?.as_deref(), Some(b"large_value".as_ref()));
        
        Ok(())
    }
}