//! In-Memory Table for Write Buffering
//!
//! MemTable is an in-memory data structure that buffers writes before they are
//! flushed to disk as SSTables. It uses a skip list for efficient insertion
//! and range queries.

use crate::{Result, Error};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::cmp::Ordering as CmpOrdering;
use serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Entry types in the memtable
#[derive(Debug, Clone, PartialEq)]
pub enum EntryType {
    Put,
    Delete,
}

/// A single entry in the memtable
#[derive(Debug, Clone)]
pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub entry_type: EntryType,
    pub sequence_number: u64,
    pub timestamp: u64,
}

impl MemTableEntry {
    /// Create a new put entry
    pub fn put(key: Vec<u8>, value: Vec<u8>, sequence_number: u64) -> Self {
        Self {
            key,
            value: Some(value),
            entry_type: EntryType::Put,
            sequence_number,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    /// Create a new delete entry
    pub fn delete(key: Vec<u8>, sequence_number: u64) -> Self {
        Self {
            key,
            value: None,
            entry_type: EntryType::Delete,
            sequence_number,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
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
    /// The actual data storage (using BTreeMap for simplicity, could use skip list)
    data: Arc<RwLock<BTreeMap<Vec<u8>, MemTableEntry>>>,
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
                .unwrap()
                .as_millis() as u64,
            frozen: Arc::new(RwLock::new(false)),
        }
    }

    /// Insert a key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if *self.frozen.read() {
            return Err(Error::InvalidOperation { 
                reason: "MemTable is frozen".to_string() 
            });
        }

        let seq = self.sequence_number.fetch_add(1, Ordering::SeqCst);
        let entry = MemTableEntry::put(key.clone(), value, seq);
        let entry_size = entry.size();

        let mut data = self.data.write();
        data.insert(key, entry);
        
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.num_entries.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Delete a key
    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        if *self.frozen.read() {
            return Err(Error::InvalidOperation { 
                reason: "MemTable is frozen".to_string() 
            });
        }

        let seq = self.sequence_number.fetch_add(1, Ordering::SeqCst);
        let entry = MemTableEntry::delete(key.clone(), seq);
        let entry_size = entry.size();

        let mut data = self.data.write();
        data.insert(key, entry);
        
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.num_entries.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let data = self.data.read();
        data.get(key).and_then(|entry| {
            match entry.entry_type {
                EntryType::Put => entry.value.clone(),
                EntryType::Delete => None,
            }
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
        let entries: Vec<(Vec<u8>, MemTableEntry)> = data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        MemTableIterator {
            entries,
            current: 0,
        }
    }

    /// Range scan
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let data = self.data.read();
        data.range(start.to_vec()..end.to_vec())
            .filter_map(|(k, entry)| {
                match entry.entry_type {
                    EntryType::Put => entry.value.as_ref().map(|v| (k.clone(), v.clone())),
                    EntryType::Delete => None,
                }
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
    entries: Vec<(Vec<u8>, MemTableEntry)>,
    current: usize,
}

impl Iterator for MemTableIterator {
    type Item = (Vec<u8>, MemTableEntry);

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
    immutable: Arc<RwLock<Vec<Arc<MemTable>>>>,
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
            immutable: Arc::new(RwLock::new(Vec::new())),
            max_immutable,
            memtable_size,
        }
    }

    /// Put a key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        loop {
            let active = self.active.read();
            
            // Try to insert into active memtable
            match active.put(key.clone(), value.clone()) {
                Ok(()) => {
                    // Check if we need to rotate
                    if active.should_flush() {
                        drop(active);
                        self.rotate_memtable()?;
                    }
                    return Ok(());
                }
                Err(e) => {
                    drop(active);
                    // If frozen, rotate and retry
                    if e.to_string().contains("frozen") {
                        self.rotate_memtable()?;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Delete a key
    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        loop {
            let active = self.active.read();
            
            match active.delete(key.clone()) {
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
                resource: "Immutable memtables".to_string() 
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
    pub immutable_stats: Vec<MemTableStats>,
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
        assert_eq!(range[0].0, b"b");
        assert_eq!(range[1].0, b"c");
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
}