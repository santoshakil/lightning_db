use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct MemTable {
    map: SkipMap<Vec<u8>, Vec<u8>>,
    size_bytes: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size_bytes: AtomicUsize::new(0),
        }
    }

    fn is_tombstone(value: &[u8]) -> bool {
        value == [0xFF, 0xFF, 0xFF, 0xFF]
    }

    /// Get iterator over all entries
    pub fn entries(&self) -> impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)> + '_ {
        self.map.iter().map(|entry| {
            let key = entry.key().clone();
            let value = if Self::is_tombstone(entry.value()) {
                None
            } else {
                Some(entry.value().clone())
            };
            (key, value)
        })
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let size_delta = key.len() + value.len() + 16; // Overhead estimate

        // Check if key already exists
        if let Some(old_entry) = self.map.get(&key) {
            let old_size = key.len() + old_entry.value().len() + 16;
            self.size_bytes.fetch_sub(old_size, Ordering::Relaxed);
        }

        self.map.insert(key, value);
        self.size_bytes.fetch_add(size_delta, Ordering::Relaxed);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).and_then(|entry| {
            let value = entry.value();
            // Check if this is a tombstone
            if value.len() == 4 && value == &[0xFF, 0xFF, 0xFF, 0xFF] {
                None
            } else {
                Some(value.clone())
            }
        })
    }

    pub fn delete(&self, key: &[u8]) {
        // CRITICAL FIX: Insert tombstone instead of removing entry
        // This maintains delete semantics across SSTable levels
        let tombstone = vec![0xFF, 0xFF, 0xFF, 0xFF];
        self.insert(key.to_vec(), tombstone);
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = MemTableEntry> + '_ {
        self.map.iter().map(|entry| MemTableEntry {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    pub fn range(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = MemTableEntry> + '_ {
        self.map
            .range(start.to_vec()..end.to_vec())
            .map(|entry| MemTableEntry {
                key: entry.key().clone(),
                value: entry.value().clone(),
            })
    }

    pub fn clear(&self) {
        self.map.clear();
        self.size_bytes.store(0, Ordering::Relaxed);
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MemTableEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl MemTableEntry {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn is_tombstone(&self) -> bool {
        self.value == [0xFF, 0xFF, 0xFF, 0xFF]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_basic() {
        let memtable = MemTable::new();

        memtable.insert(b"key1".to_vec(), b"value1".to_vec());
        memtable.insert(b"key2".to_vec(), b"value2".to_vec());

        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(memtable.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(memtable.get(b"key3"), None);

        assert_eq!(memtable.len(), 2);
        assert!(memtable.size_bytes() > 0);
    }

    #[test]
    fn test_memtable_update() {
        let memtable = MemTable::new();

        memtable.insert(b"key".to_vec(), b"value1".to_vec());
        assert_eq!(memtable.get(b"key"), Some(b"value1".to_vec()));

        memtable.insert(b"key".to_vec(), b"value2".to_vec());
        assert_eq!(memtable.get(b"key"), Some(b"value2".to_vec()));

        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn test_memtable_delete() {
        let memtable = MemTable::new();

        memtable.insert(b"key".to_vec(), b"value".to_vec());
        assert_eq!(memtable.get(b"key"), Some(b"value".to_vec()));

        memtable.delete(b"key");
        assert_eq!(memtable.get(b"key"), None);
        // After delete, tombstone is still in memtable (needed for LSM correctness)
        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn test_memtable_iteration() {
        let memtable = MemTable::new();

        memtable.insert(b"b".to_vec(), b"2".to_vec());
        memtable.insert(b"a".to_vec(), b"1".to_vec());
        memtable.insert(b"c".to_vec(), b"3".to_vec());

        let keys: Vec<Vec<u8>> = memtable.iter().map(|e| e.key().to_vec()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
