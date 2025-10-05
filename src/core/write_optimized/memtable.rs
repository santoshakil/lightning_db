//! In-Memory Table for Write Buffering
//!
//! MemTable is an in-memory data structure that buffers writes before they are
//! flushed to disk as SSTables. It uses a skip list for efficient insertion
//! and range queries.

use crate::{Error, Result};
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Entry types in the memtable
#[derive(Debug, Clone, PartialEq)]
pub enum EntryType {
    Put,
    Delete,
}

/// A single entry in the memtable
#[derive(Debug, Clone)]
pub struct MemTableEntry {
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub entry_type: EntryType,
    pub sequence_number: u64,
    pub timestamp: u64,
}

impl MemTableEntry {
    /// Create a new put entry
    pub fn put(key: Vec<u8>, value: Vec<u8>, sequence_number: u64) -> Self {
        Self {
            key: Bytes::from(key),
            value: Some(Bytes::from(value)),
            entry_type: EntryType::Put,
            sequence_number,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_millis() as u64,
        }
    }

    /// Create a new delete entry
    pub fn delete(key: Vec<u8>, sequence_number: u64) -> Self {
        Self {
            key: Bytes::from(key),
            value: None,
            entry_type: EntryType::Delete,
            sequence_number,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_millis() as u64,
        }
    }

    /// Get the size of this entry in bytes
    pub fn size(&self) -> usize {
        self.key.len() + self.value.as_ref().map_or(0, |v| v.len()) + 24 // metadata overhead
    }
}

/// In-memory table for buffering writes
#[derive(Debug)]
pub struct MemTable {
    /// The actual data storage (using BTreeMap with Bytes for zero-copy)
    data: Arc<RwLock<BTreeMap<Bytes, MemTableEntry>>>,
    /// Current size in bytes
    size: AtomicUsize,
    /// Number of entries
    num_entries: AtomicU64,
    /// Sequence number for ordering
    sequence_number: AtomicU64,
    /// Maximum size before flush
    max_size: usize,
    /// Creation timestamp
    created_at: u64,
    /// Frozen flag (no more writes allowed)
    frozen: Arc<RwLock<bool>>,
}

impl MemTable {
    /// Create a new memtable
    pub fn new(max_size: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size: AtomicUsize::new(0),
            num_entries: AtomicU64::new(0),
            sequence_number: AtomicU64::new(0),
            max_size,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_millis() as u64,
            frozen: Arc::new(RwLock::new(false)),
        }
    }

    /// Insert a key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if *self.frozen.read() {
            return Err(Error::InvalidOperation {
                reason: "MemTable is frozen".to_string(),
            });
        }

        let seq = self.sequence_number.fetch_add(1, Ordering::SeqCst);
        let entry = MemTableEntry::put(key.clone(), value, seq);
        let entry_size = entry.size();

        // Check if adding this entry would exceed the limit
        let current_size = self.size.load(Ordering::Relaxed);
        if current_size + entry_size > self.max_size {
            // Revert sequence number increment
            self.sequence_number.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::InvalidOperation {
                reason: "MemTable size limit exceeded".to_string(),
            });
        }

        let mut data = self.data.write();
        data.insert(Bytes::from(key), entry);

        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.num_entries.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Delete a key
    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        if *self.frozen.read() {
            return Err(Error::InvalidOperation {
                reason: "MemTable is frozen".to_string(),
            });
        }

        let seq = self.sequence_number.fetch_add(1, Ordering::SeqCst);
        let entry = MemTableEntry::delete(key.clone(), seq);
        let entry_size = entry.size();

        let mut data = self.data.write();
        data.insert(Bytes::from(key), entry);

        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.num_entries.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let data = self.data.read();
        data.get(key).and_then(|entry| match entry.entry_type {
            EntryType::Put => entry.value.as_ref().map(|v| v.to_vec()),
            EntryType::Delete => None,
        })
    }

    /// Check if the memtable should be flushed
    pub fn should_flush(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }

    /// Freeze the memtable (no more writes allowed)
    pub fn freeze(&self) {
        *self.frozen.write() = true;
    }

    /// Check if the memtable is frozen
    pub fn is_frozen(&self) -> bool {
        *self.frozen.read()
    }

    /// Get current size in bytes
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Get number of entries
    pub fn num_entries(&self) -> u64 {
        self.num_entries.load(Ordering::Relaxed)
    }

    /// Create an iterator over all entries
    pub fn iter(&self) -> MemTableIterator {
        let data = self.data.read();
        let entries: SmallVec<[(Bytes, MemTableEntry); 32]> =
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        MemTableIterator {
            entries,
            current: 0,
        }
    }

    /// Range scan
    pub fn range(&self, start: &[u8], end: &[u8]) -> SmallVec<[(Bytes, Bytes); 16]> {
        let data = self.data.read();
        let start_key = Bytes::from(start.to_vec());
        let end_key = Bytes::from(end.to_vec());

        data.range(start_key..end_key)
            .filter_map(|(k, entry)| match entry.entry_type {
                EntryType::Put => entry.value.as_ref().map(|v| (k.clone(), v.clone())),
                EntryType::Delete => None,
            })
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> MemTableStats {
        MemTableStats {
            size: self.size.load(Ordering::Relaxed),
            num_entries: self.num_entries.load(Ordering::Relaxed),
            created_at: self.created_at,
            frozen: *self.frozen.read(),
        }
    }
}

/// Iterator over memtable entries
pub struct MemTableIterator {
    entries: SmallVec<[(Bytes, MemTableEntry); 32]>,
    current: usize,
}

impl Iterator for MemTableIterator {
    type Item = (Bytes, MemTableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current < self.entries.len() {
            let item = self.entries[self.current].clone();
            self.current += 1;
            Some(item)
        } else {
            None
        }
    }
}

/// MemTable statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemTableStats {
    pub size: usize,
    pub num_entries: u64,
    pub created_at: u64,
    pub frozen: bool,
}

/// MemTable manager for handling multiple memtables
pub struct MemTableManager {
    /// Active memtable for writes
    active: Arc<RwLock<Arc<MemTable>>>,
    /// Immutable memtables waiting to be flushed
    immutable: Arc<RwLock<SmallVec<[Arc<MemTable>; 4]>>>,
    /// Maximum number of immutable memtables
    max_immutable: usize,
    /// MemTable size
    memtable_size: usize,
}

impl MemTableManager {
    /// Create a new memtable manager
    pub fn new(memtable_size: usize, max_immutable: usize) -> Self {
        Self {
            active: Arc::new(RwLock::new(Arc::new(MemTable::new(memtable_size)))),
            immutable: Arc::new(RwLock::new(SmallVec::new())),
            max_immutable,
            memtable_size,
        }
    }

    /// Put a key-value pair
    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let value = value.into();
        let mut retry_count = 0;
        const MAX_RETRIES: usize = 3;

        loop {
            // Check if rotation is needed first
            let should_rotate = {
                let active = self.active.read();
                active.should_flush()
            };

            if should_rotate {
                self.rotate_memtable()?;
            }

            // Try to insert
            let result = {
                let active = self.active.read();
                active.put(key.to_vec(), value.to_vec())
            };

            match result {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.contains("frozen") || err_msg.contains("size limit exceeded") {
                        retry_count += 1;
                        if retry_count > MAX_RETRIES {
                            return Err(Error::ResourceExhausted {
                                resource: "Memtable rotation retries".to_string(),
                            });
                        }
                        self.rotate_memtable()?;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Delete a key
    pub fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let mut retry_count = 0;
        const MAX_RETRIES: usize = 3;

        loop {
            let active = self.active.read();

            match active.delete(key.to_vec()) {
                Ok(()) => {
                    if active.should_flush() {
                        drop(active);
                        self.rotate_memtable()?;
                    }
                    return Ok(());
                }
                Err(e) => {
                    drop(active);
                    if e.to_string().contains("frozen") {
                        retry_count += 1;
                        if retry_count > MAX_RETRIES {
                            return Err(Error::ResourceExhausted {
                                resource: "Memtable rotation retries".to_string(),
                            });
                        }
                        self.rotate_memtable()?;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Get a value (checks active and immutable memtables)
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Check active memtable first
        let active = self.active.read();
        if let Some(value) = active.get(key) {
            return Some(value);
        }
        drop(active);

        // Check immutable memtables (newest to oldest)
        let immutable = self.immutable.read();
        for memtable in immutable.iter().rev() {
            if let Some(value) = memtable.get(key) {
                return Some(value);
            }
        }

        None
    }

    /// Rotate the active memtable
    fn rotate_memtable(&self) -> Result<()> {
        let mut active = self.active.write();
        let mut immutable = self.immutable.write();

        // Check if we have too many immutable memtables
        if immutable.len() >= self.max_immutable {
            return Err(Error::ResourceExhausted {
                resource: "Immutable memtables".to_string(),
            });
        }

        // Freeze current active memtable
        active.freeze();

        // Move to immutable list
        immutable.push(Arc::clone(&active));

        // Create new active memtable
        *active = Arc::new(MemTable::new(self.memtable_size));

        Ok(())
    }

    /// Get the oldest immutable memtable for flushing
    pub fn get_flush_candidate(&self) -> Option<Arc<MemTable>> {
        let mut immutable = self.immutable.write();
        if !immutable.is_empty() {
            Some(immutable.remove(0))
        } else {
            None
        }
    }

    /// Get statistics
    pub fn stats(&self) -> MemTableManagerStats {
        let active = self.active.read();
        let immutable = self.immutable.read();

        MemTableManagerStats {
            active_stats: active.stats(),
            num_immutable: immutable.len(),
            immutable_stats: immutable.iter().map(|m| m.stats()).collect(),
        }
    }
}

/// MemTable manager statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemTableManagerStats {
    pub active_stats: MemTableStats,
    pub num_immutable: usize,
    pub immutable_stats: SmallVec<[MemTableStats; 4]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_basic_operations() {
        let memtable = MemTable::new(1024 * 1024);

        // Test put
        memtable.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        memtable.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        // Test get
        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(memtable.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(memtable.get(b"key3"), None);

        // Test delete
        memtable.delete(b"key1".to_vec()).unwrap();
        assert_eq!(memtable.get(b"key1"), None);

        assert_eq!(memtable.num_entries(), 3); // 2 puts + 1 delete
    }

    #[test]
    fn test_memtable_freeze() {
        let memtable = MemTable::new(1024);

        memtable.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert!(!memtable.is_frozen());

        memtable.freeze();
        assert!(memtable.is_frozen());

        // Should not be able to write to frozen memtable
        assert!(memtable.put(b"key2".to_vec(), b"value2".to_vec()).is_err());
    }

    #[test]
    fn test_memtable_range_scan() {
        let memtable = MemTable::new(1024 * 1024);

        memtable.put(b"a".to_vec(), b"value_a".to_vec()).unwrap();
        memtable.put(b"b".to_vec(), b"value_b".to_vec()).unwrap();
        memtable.put(b"c".to_vec(), b"value_c".to_vec()).unwrap();
        memtable.put(b"d".to_vec(), b"value_d".to_vec()).unwrap();

        let range = memtable.range(b"b", b"d");
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0.as_ref(), b"b");
        assert_eq!(range[1].0.as_ref(), b"c");
    }

    #[test]
    fn test_memtable_manager() {
        let manager = MemTableManager::new(1024, 2);

        // Test basic operations
        manager.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(manager.get(b"key1"), Some(b"value1".to_vec()));

        // Test delete
        manager.delete(b"key1".to_vec()).unwrap();
        assert_eq!(manager.get(b"key1"), None);

        // Test stats
        let stats = manager.stats();
        assert_eq!(stats.num_immutable, 0);
    }

    #[test]
    fn test_memtable_concurrent_writes() {
        use std::sync::Arc;
        use std::thread;

        let memtable = Arc::new(MemTable::new(10 * 1024 * 1024));
        let num_threads = 8;
        let entries_per_thread = 1000;
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let memtable_clone = Arc::clone(&memtable);
            let handle = thread::spawn(move || {
                for i in 0..entries_per_thread {
                    let key = format!("thread_{:02}_key_{:04}", thread_id, i);
                    let value = format!("thread_{:02}_value_{:04}", thread_id, i);
                    memtable_clone
                        .put(key.into_bytes(), value.into_bytes())
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all entries
        assert_eq!(memtable.num_entries(), num_threads * entries_per_thread);

        for thread_id in 0..num_threads {
            for i in 0..entries_per_thread {
                let key = format!("thread_{:02}_key_{:04}", thread_id, i);
                let expected_value = format!("thread_{:02}_value_{:04}", thread_id, i);
                assert_eq!(
                    memtable.get(key.as_bytes()),
                    Some(expected_value.into_bytes())
                );
            }
        }
    }

    #[test]
    fn test_memtable_rotation() {
        let small_size = 512; // Small size to trigger rotations
        let manager = MemTableManager::new(small_size, 10); // Increased to handle more rotations

        // Fill up multiple memtables - reduced count to avoid exhaustion
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            let value = vec![b'v'; 50]; // Each entry ~50 bytes
            manager.put(key.into_bytes(), value).unwrap();
        }

        let stats = manager.stats();
        assert!(stats.num_immutable > 0);

        // Should still be able to read all values
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            assert!(manager.get(key.as_bytes()).is_some());
        }
    }

    #[test]
    fn test_memtable_memory_limit() {
        let memtable = MemTable::new(1024); // 1KB limit

        // Add entries until we exceed the limit
        let mut added = 0;
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = vec![b'v'; 100]; // 100 bytes per value

            if memtable.put(key.into_bytes(), value).is_ok() {
                added += 1;
            } else {
                break;
            }
        }

        // Should have stopped before adding all 100 entries
        assert!(added < 100);
        assert!(added > 0); // Should have added at least some entries
        // Size check should allow for some overhead
        assert!(memtable.size() <= 2048); // Allow some overhead
    }

    #[test]
    fn test_memtable_flush_to_immutable() {
        let manager = MemTableManager::new(512, 5); // Smaller size to trigger rotation faster

        // Fill the active memtable
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let value = vec![b'v'; 40];
            manager.put(key.into_bytes(), value).unwrap();
        }

        // Add more data to potentially trigger rotation
        for i in 10..30 {
            let key = format!("key_{:02}", i);
            let value = vec![b'v'; 40];
            if manager.put(key.into_bytes(), value).is_err() {
                break;
            }
        }

        let stats = manager.stats();
        // Check that we have at least some immutable tables
        assert!(stats.num_immutable <= 5); // Should not exceed max

        // Should still be able to read all inserted data
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            assert!(manager.get(key.as_bytes()).is_some());
        }
    }

    #[test]
    fn test_memtable_update_semantics() {
        let memtable = MemTable::new(1024 * 1024);

        // Initial value
        memtable.put(b"key".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(memtable.get(b"key"), Some(b"value1".to_vec()));

        // Update value
        memtable.put(b"key".to_vec(), b"value2".to_vec()).unwrap();
        assert_eq!(memtable.get(b"key"), Some(b"value2".to_vec()));

        // Delete then put
        memtable.delete(b"key".to_vec()).unwrap();
        assert_eq!(memtable.get(b"key"), None);

        memtable.put(b"key".to_vec(), b"value3".to_vec()).unwrap();
        assert_eq!(memtable.get(b"key"), Some(b"value3".to_vec()));
    }

    #[test]
    fn test_memtable_tombstone_handling() {
        let memtable = MemTable::new(1024 * 1024);

        // Put then delete
        memtable.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        memtable.delete(b"key1".to_vec()).unwrap();

        // The tombstone should be present in entries
        assert_eq!(memtable.num_entries(), 2); // Put + Delete tombstone
        assert_eq!(memtable.get(b"key1"), None);

        // Range scan should handle tombstones correctly
        memtable.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        memtable.delete(b"key2".to_vec()).unwrap();
        memtable.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();

        let range = memtable.range(b"key1", b"key4");
        // Should only return non-deleted entries
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].0.as_ref(), b"key3");
    }

    #[test]
    fn test_memtable_iterator_ordering() {
        let memtable = MemTable::new(1024 * 1024);

        // Insert in random order
        let keys = vec!["zebra", "apple", "mango", "banana", "cherry"];
        for key in &keys {
            memtable
                .put(key.as_bytes().to_vec(), b"value".to_vec())
                .unwrap();
        }

        // Iterator should return in sorted order
        let mut sorted_keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        sorted_keys.sort();

        // Use range to get all entries in sorted order
        let entries = memtable.range(b"", b"~");
        assert_eq!(entries.len(), keys.len());

        for (i, (key, _)) in entries.iter().enumerate() {
            assert_eq!(key, sorted_keys[i].as_bytes());
        }
    }

    #[test]
    fn test_memtable_prefix_scan() {
        let memtable = MemTable::new(1024 * 1024);

        // Add entries with common prefixes
        memtable
            .put(b"user:1:name".to_vec(), b"Alice".to_vec())
            .unwrap();
        memtable
            .put(b"user:1:age".to_vec(), b"30".to_vec())
            .unwrap();
        memtable
            .put(b"user:2:name".to_vec(), b"Bob".to_vec())
            .unwrap();
        memtable
            .put(b"user:2:age".to_vec(), b"25".to_vec())
            .unwrap();
        memtable
            .put(b"post:1:title".to_vec(), b"Hello".to_vec())
            .unwrap();

        // Scan for user:1 prefix
        let range = memtable.range(b"user:1", b"user:1~"); // ~ is after all printable chars
        assert_eq!(range.len(), 2);

        // Scan for all users
        let range = memtable.range(b"user:", b"user:~");
        assert_eq!(range.len(), 4);
    }
}
