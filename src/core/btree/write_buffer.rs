use super::BPlusTree;
use crate::core::error::Result;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Write buffer for B+Tree to handle burst writes when LSM is disabled
#[derive(Debug)]
pub struct BTreeWriteBuffer {
    buffer: Arc<Mutex<BTreeMap<Bytes, Bytes>>>,
    btree: Arc<RwLock<BPlusTree>>,
    max_buffer_size: usize,
}

impl BTreeWriteBuffer {
    pub fn new(btree: Arc<RwLock<BPlusTree>>, max_buffer_size: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(BTreeMap::new())),
            btree,
            max_buffer_size,
        }
    }

    /// Insert a key-value pair, buffering if needed
    pub fn insert(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let mut buffer = self.buffer.lock();
        buffer.insert(key.into(), value.into());

        // Flush if buffer is too large
        if buffer.len() >= self.max_buffer_size {
            self.flush_locked(&mut buffer)?;
        }

        Ok(())
    }

    /// Get a value, checking buffer first then B+Tree
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check buffer first
        if let Some(value) = self.buffer.lock().get(key) {
            return Ok(Some(value.to_vec()));
        }

        // Fall back to B+Tree
        let btree = self.btree.read();
        btree.get(key)
    }

    /// Delete a key
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        // Remove from buffer
        let mut buffer = self.buffer.lock();
        let was_in_buffer = buffer.remove(key).is_some();
        drop(buffer);

        // Also delete from B+Tree
        let mut btree = self.btree.write();
        let was_in_btree = btree.delete(key)?;

        Ok(was_in_buffer || was_in_btree)
    }

    /// Flush all buffered writes to the B+Tree
    pub fn flush(&self) -> Result<()> {
        let mut buffer = self.buffer.lock();
        self.flush_locked(&mut buffer)
    }

    fn flush_locked(&self, buffer: &mut BTreeMap<Bytes, Bytes>) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        // Extract entries to avoid holding lock during B+Tree operations
        let entries: Vec<(Bytes, Bytes)> = std::mem::take(buffer).into_iter().collect();

        // Insert into B+Tree in batches to reduce lock contention
        let mut btree = self.btree.write();
        for (key, value) in entries {
            btree.insert(key.as_ref(), value.as_ref())?;
        }

        Ok(())
    }

    /// Get the current buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::storage::PageManager;
    use tempfile::tempdir;

    #[test]
    fn test_write_buffer() -> Result<()> {
        let dir = tempdir()?;
        let page_manager = Arc::new(RwLock::new(PageManager::create(
            &dir.path().join("test.db"),
            1024 * 1024,
        )?));

        let btree = Arc::new(RwLock::new(BPlusTree::new(page_manager)?));
        let buffer = BTreeWriteBuffer::new(btree, 100);

        // Test basic operations
        buffer.insert(b"key1".to_vec(), b"value1".to_vec())?;
        assert_eq!(buffer.get(b"key1")?, Some(b"value1".to_vec()));

        // Test buffer size
        assert_eq!(buffer.buffer_size(), 1);

        // Test flush
        buffer.flush()?;
        assert_eq!(buffer.buffer_size(), 0);
        assert_eq!(buffer.get(b"key1")?, Some(b"value1".to_vec()));

        // Test delete
        assert!(buffer.delete(b"key1")?);
        assert_eq!(buffer.get(b"key1")?, None);

        Ok(())
    }

    #[test]
    fn test_auto_flush() -> Result<()> {
        let dir = tempdir()?;
        let page_manager = Arc::new(RwLock::new(PageManager::create(
            &dir.path().join("test.db"),
            1024 * 1024,
        )?));

        let btree = Arc::new(RwLock::new(BPlusTree::new(page_manager)?));
        let buffer = BTreeWriteBuffer::new(btree, 10); // Small buffer for testing

        // Insert enough to trigger auto-flush
        for i in 0..15 {
            buffer.insert(
                format!("key{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )?;
        }

        // Buffer should have been flushed at least once
        assert!(buffer.buffer_size() < 15);

        // All values should still be accessible
        for i in 0..15 {
            assert_eq!(
                buffer.get(format!("key{}", i).as_bytes())?,
                Some(format!("value{}", i).into_bytes())
            );
        }

        Ok(())
    }
}
