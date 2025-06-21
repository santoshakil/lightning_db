use crate::btree::BPlusTree;
use crate::error::Result;
use crate::lsm::LSMTree;
use crate::transaction::{Transaction, VersionStore};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

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
        // For BinaryHeap (max-heap by default), we need to reverse the comparison
        // to get the smallest key first (for forward iteration)
        match self.key.cmp(&other.key) {
            Ordering::Equal => {
                // Same key: prefer newer timestamp (higher value)
                other.timestamp.cmp(&self.timestamp)
            }
            key_order => key_order.reverse(), // Reverse for min-heap behavior
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
        
        // Immediately populate the heap with first entry
        if let Some(ref mut btree_iter) = self.btree_iterator {
            if let Some(entry) = btree_iter.next()? {
                self.merge_heap.push(entry);
            }
        }
        
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
        
        // Immediately populate the heap with first entry
        if let Some(ref mut lsm_iter) = self.lsm_iterator {
            if let Some(entry) = lsm_iter.next()? {
                self.merge_heap.push(entry);
            }
        }
        
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
        
        // Immediately populate the heap with first entry
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
            
            // Re-fetch entries from the sources that had duplicates
            for dup_entry in skip_duplicates {
                match dup_entry.source {
                    IteratorSource::BTree => {
                        if let Some(ref mut btree_iter) = self.btree_iterator {
                            if let Ok(Some(new_entry)) = btree_iter.next() {
                                self.merge_heap.push(new_entry);
                            }
                        }
                    }
                    IteratorSource::LSM => {
                        if let Some(ref mut lsm_iter) = self.lsm_iterator {
                            if let Ok(Some(new_entry)) = lsm_iter.next() {
                                self.merge_heap.push(new_entry);
                            }
                        }
                    }
                    IteratorSource::VersionStore => {
                        if let Some(ref mut vs_iter) = self.version_store_iterator {
                            if let Ok(Some(new_entry)) = vs_iter.next() {
                                self.merge_heap.push(new_entry);
                            }
                        }
                    }
                }
            }
            
            // CRITICAL: Also refill from the source that provided the main entry
            match entry.source {
                IteratorSource::BTree => {
                    if let Some(ref mut btree_iter) = self.btree_iterator {
                        if let Ok(Some(new_entry)) = btree_iter.next() {
                            self.merge_heap.push(new_entry);
                        }
                    }
                }
                IteratorSource::LSM => {
                    if let Some(ref mut lsm_iter) = self.lsm_iterator {
                        if let Ok(Some(new_entry)) = lsm_iter.next() {
                            self.merge_heap.push(new_entry);
                        }
                    }
                }
                IteratorSource::VersionStore => {
                    if let Some(ref mut vs_iter) = self.version_store_iterator {
                        if let Ok(Some(new_entry)) = vs_iter.next() {
                            self.merge_heap.push(new_entry);
                        }
                    }
                }
            }

            // Return the value
            self.count += 1;
            return Some(Ok((entry.key, entry.value.unwrap())));
        }
    }
}

/// B+ Tree iterator wrapper
struct BTreeIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
}

impl BTreeIterator {
    fn new(
        btree: &BPlusTree,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        direction: ScanDirection,
    ) -> Result<Self> {
        use crate::btree::BTreeLeafIterator;
        
        let forward = match direction {
            ScanDirection::Forward => true,
            ScanDirection::Backward => false,
        };
        
        // Create iterator and collect all entries to avoid lifetime issues
        let iter = BTreeLeafIterator::new(
            btree,
            start_key.clone(),
            end_key.clone(),
            forward,
        )?;
        
        // Collect all entries that match the range
        let mut entries = Vec::new();
        for result in iter {
            match result {
                Ok((key, value)) => entries.push((key, value)),
                Err(e) => return Err(e),
            }
        }
        
        // For backward iteration, reverse the entries
        if !forward {
            entries.reverse();
        }

        Ok(Self { 
            entries,
            position: 0,
        })
    }

    fn next(&mut self) -> Result<Option<IteratorEntry>> {
        if self.position < self.entries.len() {
            let (key, value) = self.entries[self.position].clone();
            self.position += 1;
            Ok(Some(IteratorEntry {
                key,
                value: Some(value),
                source: IteratorSource::BTree,
                timestamp: 0, // B+ tree entries don't have timestamps
            }))
        } else {
            Ok(None)
        }
    }
}

/// LSM Tree iterator implementation
struct LSMIterator {
    lsm_iter: crate::lsm::LSMMemTableIterator,
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
        let forward = match direction {
            ScanDirection::Forward => true,
            ScanDirection::Backward => false,
        };
        
        // Get the memtable and create iterator
        let memtable = lsm.get_memtable();
        let lsm_iter = crate::lsm::LSMMemTableIterator::new(
            &memtable,
            start_key.as_deref(),
            end_key.as_deref(),
            forward,
        );

        Ok(Self {
            lsm_iter,
            read_timestamp,
        })
    }

    fn next(&mut self) -> Result<Option<IteratorEntry>> {
        if let Some((key, value)) = self.lsm_iter.next() {
            Ok(Some(IteratorEntry {
                key,
                value: Some(value),
                source: IteratorSource::LSM,
                timestamp: self.read_timestamp,
            }))
        } else {
            Ok(None)
        }
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

        // Higher timestamp should have higher priority in the heap (comes out first)
        // For a min-heap, this means entry2 should be considered "less than" entry1
        assert!(entry2 < entry1);
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
