use crate::btree::BPlusTree;
use crate::error::Result;
use crate::lsm::LSMTree;
use crate::transaction::{Transaction, VersionStore};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tracing::debug;

/// Iterator for range queries over key-value pairs
pub struct RangeIterator {
    // Iterator state
    current_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    direction: ScanDirection,

    // Data sources
    btree_iterator: Option<BTreeIterator>,
    lsm_iterator: Option<LSMIterator>,
    version_store_iterator: Option<VersionStoreIterator>,

    // Merge state for combining multiple sources
    merge_heap: BinaryHeap<IteratorEntry>,
    read_timestamp: u64,

    // Configuration
    include_start: bool,
    include_end: bool,
    limit: Option<usize>,
    count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IteratorEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>, // None indicates tombstone
    source: IteratorSource,
    timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum IteratorSource {
    BTree,
    LSM,
    VersionStore,
}

impl PartialOrd for IteratorEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IteratorEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher timestamp has priority (newer data)
        // For same timestamp, compare keys
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Equal => self.key.cmp(&other.key),
            other_order => other_order,
        }
    }
}

impl RangeIterator {
    pub fn new(
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        direction: ScanDirection,
        read_timestamp: u64,
    ) -> Self {
        Self {
            current_key: start_key.clone(),
            end_key,
            direction,
            btree_iterator: None,
            lsm_iterator: None,
            version_store_iterator: None,
            merge_heap: BinaryHeap::new(),
            read_timestamp,
            include_start: true,
            include_end: false,
            limit: None,
            count: 0,
        }
    }

    pub fn with_bounds(mut self, include_start: bool, include_end: bool) -> Self {
        self.include_start = include_start;
        self.include_end = include_end;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn attach_btree(&mut self, btree: &BPlusTree) -> Result<()> {
        self.btree_iterator = Some(BTreeIterator::new(
            btree,
            self.current_key.clone(),
            self.end_key.clone(),
            self.direction.clone(),
        )?);
        Ok(())
    }

    pub fn attach_lsm(&mut self, lsm: &LSMTree) -> Result<()> {
        self.lsm_iterator = Some(LSMIterator::new(
            lsm,
            self.current_key.clone(),
            self.end_key.clone(),
            self.direction.clone(),
            self.read_timestamp,
        )?);
        Ok(())
    }

    pub fn attach_version_store(&mut self, version_store: &VersionStore) -> Result<()> {
        self.version_store_iterator = Some(VersionStoreIterator::new(
            version_store,
            self.current_key.clone(),
            self.end_key.clone(),
            self.direction.clone(),
            self.read_timestamp,
        )?);
        Ok(())
    }

    fn fill_merge_heap(&mut self) -> Result<()> {
        // Get next entry from each iterator and add to heap
        if let Some(ref mut btree_iter) = self.btree_iterator {
            if let Some(entry) = btree_iter.next()? {
                self.merge_heap.push(entry);
            }
        }

        if let Some(ref mut lsm_iter) = self.lsm_iterator {
            if let Some(entry) = lsm_iter.next()? {
                self.merge_heap.push(entry);
            }
        }

        if let Some(ref mut vs_iter) = self.version_store_iterator {
            if let Some(entry) = vs_iter.next()? {
                self.merge_heap.push(entry);
            }
        }

        Ok(())
    }

    fn is_within_bounds(&self, key: &[u8]) -> bool {
        // Check start bound
        if let Some(ref start_key) = self.current_key {
            match self.direction {
                ScanDirection::Forward => {
                    let cmp = key.cmp(start_key);
                    if !self.include_start && cmp == Ordering::Equal {
                        return false;
                    }
                    if cmp == Ordering::Less {
                        return false;
                    }
                }
                ScanDirection::Backward => {
                    let cmp = key.cmp(start_key);
                    if !self.include_start && cmp == Ordering::Equal {
                        return false;
                    }
                    if cmp == Ordering::Greater {
                        return false;
                    }
                }
            }
        }

        // Check end bound
        if let Some(ref end_key) = self.end_key {
            match self.direction {
                ScanDirection::Forward => {
                    let cmp = key.cmp(end_key);
                    if !self.include_end && cmp == Ordering::Equal {
                        return false;
                    }
                    if cmp == Ordering::Greater {
                        return false;
                    }
                }
                ScanDirection::Backward => {
                    let cmp = key.cmp(end_key);
                    if !self.include_end && cmp == Ordering::Equal {
                        return false;
                    }
                    if cmp == Ordering::Less {
                        return false;
                    }
                }
            }
        }

        true
    }
}

impl Iterator for RangeIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check limit
        if let Some(limit) = self.limit {
            if self.count >= limit {
                return None;
            }
        }

        loop {
            // Fill heap if empty
            if self.merge_heap.is_empty() {
                if let Err(e) = self.fill_merge_heap() {
                    return Some(Err(e));
                }
            }

            // Get the next entry with highest priority (newest timestamp)
            let entry = match self.merge_heap.pop() {
                Some(entry) => entry,
                None => return None, // No more entries
            };

            // Skip entries not within bounds
            if !self.is_within_bounds(&entry.key) {
                continue;
            }

            // Skip tombstones (deleted entries)
            if entry.value.is_none() {
                continue;
            }

            // Deduplicate: skip entries with same key but older timestamp
            let mut skip_duplicates = Vec::new();
            while let Some(next_entry) = self.merge_heap.peek() {
                if next_entry.key == entry.key {
                    skip_duplicates.push(self.merge_heap.pop().unwrap());
                } else {
                    break;
                }
            }

            // Return the value
            self.count += 1;
            return Some(Ok((entry.key, entry.value.unwrap())));
        }
    }
}

/// B+ Tree iterator implementation
struct BTreeIterator {
    // Implementation would depend on B+ tree structure
    // For now, we'll use a simplified approach
    current_position: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl BTreeIterator {
    fn new(
        _btree: &BPlusTree,
        _start_key: Option<Vec<u8>>,
        _end_key: Option<Vec<u8>>,
        _direction: ScanDirection,
    ) -> Result<Self> {
        // This is a simplified implementation
        // In a real implementation, this would traverse the B+ tree structure
        let entries = Vec::new(); // Would be populated from actual B+ tree traversal

        Ok(Self {
            current_position: 0,
            entries,
        })
    }

    fn next(&mut self) -> Result<Option<IteratorEntry>> {
        if self.current_position >= self.entries.len() {
            return Ok(None);
        }

        let (key, value) = &self.entries[self.current_position];
        self.current_position += 1;

        Ok(Some(IteratorEntry {
            key: key.clone(),
            value: Some(value.clone()),
            source: IteratorSource::BTree,
            timestamp: 0, // B+ tree entries don't have timestamps
        }))
    }
}

/// LSM Tree iterator implementation
struct LSMIterator {
    current_level: usize,
    memtable_iterator: Option<MemTableIterator>,
    sstable_iterators: Vec<SSTableIterator>,
    merge_heap: BinaryHeap<IteratorEntry>,
    read_timestamp: u64,
}

impl LSMIterator {
    fn new(
        lsm: &LSMTree,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        direction: ScanDirection,
        read_timestamp: u64,
    ) -> Result<Self> {
        let mut iterator = Self {
            current_level: 0,
            memtable_iterator: None,
            sstable_iterators: Vec::new(),
            merge_heap: BinaryHeap::new(),
            read_timestamp,
        };

        // Initialize memtable iterator
        iterator.memtable_iterator = Some(MemTableIterator::new(
            start_key.clone(),
            end_key.clone(),
            direction.clone(),
        )?);

        // Initialize SSTable iterators for each level
        let stats = lsm.stats();
        for level_stat in &stats.levels {
            // This would iterate through SSTables at each level
            // For now, we'll create placeholder iterators
            iterator.sstable_iterators.push(SSTableIterator::new(
                level_stat.level,
                start_key.clone(),
                end_key.clone(),
                direction.clone(),
            )?);
        }

        Ok(iterator)
    }

    fn next(&mut self) -> Result<Option<IteratorEntry>> {
        // Check memtable first
        if let Some(ref mut memtable_iter) = self.memtable_iterator {
            if let Some((key, value)) = memtable_iter.next() {
                return Ok(Some(IteratorEntry {
                    key,
                    value: Some(value),
                    source: IteratorSource::LSM,
                    timestamp: self.read_timestamp,
                }));
            }
        }

        // Then check SSTables from each level
        while self.current_level < self.sstable_iterators.len() {
            if let Some((key, value)) = self.sstable_iterators[self.current_level].next() {
                return Ok(Some(IteratorEntry {
                    key,
                    value: Some(value),
                    source: IteratorSource::LSM,
                    timestamp: self.read_timestamp,
                }));
            }
            self.current_level += 1;
        }

        // Check merge heap for any remaining entries
        if let Some(entry) = self.merge_heap.pop() {
            return Ok(Some(entry));
        }

        Ok(None)
    }
}

/// Memtable iterator
struct MemTableIterator {
    // Simplified implementation
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    current_position: usize,
}

impl MemTableIterator {
    fn new(
        _start_key: Option<Vec<u8>>,
        _end_key: Option<Vec<u8>>,
        _direction: ScanDirection,
    ) -> Result<Self> {
        Ok(Self {
            entries: Vec::new(),
            current_position: 0,
        })
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.current_position < self.entries.len() {
            let entry = self.entries[self.current_position].clone();
            self.current_position += 1;
            Some(entry)
        } else {
            None
        }
    }
}

/// SSTable iterator
struct SSTableIterator {
    level: usize,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    current_position: usize,
}

impl SSTableIterator {
    fn new(
        level: usize,
        _start_key: Option<Vec<u8>>,
        _end_key: Option<Vec<u8>>,
        _direction: ScanDirection,
    ) -> Result<Self> {
        debug!("Creating SSTable iterator for level {}", level);
        Ok(Self {
            level,
            entries: Vec::new(),
            current_position: 0,
        })
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.current_position < self.entries.len() {
            let entry = self.entries[self.current_position].clone();
            self.current_position += 1;

            // Log level info for debugging iterator progression
            if self.current_position % 100 == 0 {
                debug!(
                    "SSTable iterator at level {} processed {} entries",
                    self.level(),
                    self.current_position
                );
            }

            Some(entry)
        } else {
            None
        }
    }

    pub fn level(&self) -> usize {
        self.level
    }
}

/// Version store iterator
struct VersionStoreIterator {
    entries: Vec<(Vec<u8>, Vec<u8>, u64)>, // key, value, timestamp
    current_position: usize,
    read_timestamp: u64,
}

impl VersionStoreIterator {
    fn new(
        _version_store: &VersionStore,
        _start_key: Option<Vec<u8>>,
        _end_key: Option<Vec<u8>>,
        _direction: ScanDirection,
        read_timestamp: u64,
    ) -> Result<Self> {
        // This would collect all relevant entries from the version store
        // within the specified range and timestamp
        Ok(Self {
            entries: Vec::new(),
            current_position: 0,
            read_timestamp,
        })
    }

    fn next(&mut self) -> Result<Option<IteratorEntry>> {
        while self.current_position < self.entries.len() {
            let (key, value, timestamp) = &self.entries[self.current_position];
            self.current_position += 1;

            // Only return entries that are visible at our read timestamp
            if *timestamp <= self.read_timestamp {
                return Ok(Some(IteratorEntry {
                    key: key.clone(),
                    value: Some(value.clone()),
                    source: IteratorSource::VersionStore,
                    timestamp: *timestamp,
                }));
            }
        }

        Ok(None)
    }
}

/// Transaction-aware iterator that sees uncommitted writes
pub struct TransactionIterator {
    range_iterator: RangeIterator,
    transaction: Arc<RwLock<Transaction>>,
}

impl TransactionIterator {
    pub fn new(range_iterator: RangeIterator, transaction: Arc<RwLock<Transaction>>) -> Self {
        Self {
            range_iterator,
            transaction,
        }
    }
}

impl Iterator for TransactionIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // First check if we have any uncommitted writes that should be visible
        let tx = self.transaction.read();

        // This would need to merge uncommitted writes with the range iterator
        // For now, delegate to the underlying range iterator
        drop(tx);
        self.range_iterator.next()
    }
}

/// Builder for creating iterators with various configurations
pub struct IteratorBuilder {
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    direction: ScanDirection,
    include_start: bool,
    include_end: bool,
    limit: Option<usize>,
    read_timestamp: Option<u64>,
}

impl IteratorBuilder {
    pub fn new() -> Self {
        Self {
            start_key: None,
            end_key: None,
            direction: ScanDirection::Forward,
            include_start: true,
            include_end: false,
            limit: None,
            read_timestamp: None,
        }
    }

    pub fn start_key(mut self, key: Vec<u8>) -> Self {
        self.start_key = Some(key);
        self
    }

    pub fn end_key(mut self, key: Vec<u8>) -> Self {
        self.end_key = Some(key);
        self
    }

    pub fn direction(mut self, direction: ScanDirection) -> Self {
        self.direction = direction;
        self
    }

    pub fn include_start(mut self, include: bool) -> Self {
        self.include_start = include;
        self
    }

    pub fn include_end(mut self, include: bool) -> Self {
        self.include_end = include;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn read_timestamp(mut self, timestamp: u64) -> Self {
        self.read_timestamp = Some(timestamp);
        self
    }

    pub fn build(self) -> RangeIterator {
        let timestamp = self.read_timestamp.unwrap_or(u64::MAX);

        let mut iterator =
            RangeIterator::new(self.start_key, self.end_key, self.direction, timestamp)
                .with_bounds(self.include_start, self.include_end);

        if let Some(limit) = self.limit {
            iterator = iterator.with_limit(limit);
        }

        iterator
    }
}

impl Default for IteratorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iterator_builder() {
        let builder = IteratorBuilder::new()
            .start_key(b"a".to_vec())
            .end_key(b"z".to_vec())
            .direction(ScanDirection::Forward)
            .include_start(true)
            .include_end(false)
            .limit(100);

        let iterator = builder.build();
        assert_eq!(iterator.current_key, Some(b"a".to_vec()));
        assert_eq!(iterator.end_key, Some(b"z".to_vec()));
        assert!(iterator.include_start);
        assert!(!iterator.include_end);
        assert_eq!(iterator.limit, Some(100));
    }

    #[test]
    fn test_iterator_entry_ordering() {
        let entry1 = IteratorEntry {
            key: b"key1".to_vec(),
            value: Some(b"value1".to_vec()),
            source: IteratorSource::BTree,
            timestamp: 100,
        };

        let entry2 = IteratorEntry {
            key: b"key1".to_vec(),
            value: Some(b"value2".to_vec()),
            source: IteratorSource::LSM,
            timestamp: 200,
        };

        // Higher timestamp should have higher priority
        assert!(entry2 > entry1);
    }

    #[test]
    fn test_range_bounds() {
        let iterator = RangeIterator::new(
            Some(b"b".to_vec()),
            Some(b"y".to_vec()),
            ScanDirection::Forward,
            100,
        )
        .with_bounds(false, true);

        assert!(!iterator.is_within_bounds(b"a")); // Before start
        assert!(!iterator.is_within_bounds(b"b")); // At start (excluded)
        assert!(iterator.is_within_bounds(b"c")); // Within range
        assert!(iterator.is_within_bounds(b"y")); // At end (included)
        assert!(!iterator.is_within_bounds(b"z")); // After end
    }
}
