use crate::btree::BPlusTree;
use crate::error::Result;
use crate::lsm::LSMTree;
use crate::transaction::VersionStore;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;
use tracing::debug;

/// Fixed iterator implementation with proper ordering and deduplication
pub struct FixedRangeIterator {
    // Iterator state
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    direction: ScanDirection,
    
    // Data sources
    btree_iterator: Option<BTreeIterator>,
    lsm_iterator: Option<LSMIterator>,
    version_store_iterator: Option<VersionStoreIterator>,
    
    // Merge state
    merge_heap: BinaryHeap<MergeEntry>,
    last_key: Option<Vec<u8>>,
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

/// Entry for merge heap with proper ordering
#[derive(Debug, Clone)]
struct MergeEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    timestamp: u64,
    source: IteratorSource,
    // For backward iteration, we need to reverse the order
    direction: ScanDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum IteratorSource {
    BTree,
    LSM,
    VersionStore,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.timestamp == other.timestamp
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.direction {
            ScanDirection::Forward => {
                // For forward scan: smaller keys first, then newer timestamps
                match self.key.cmp(&other.key) {
                    Ordering::Equal => {
                        // Same key: prefer newer timestamp (higher value)
                        other.timestamp.cmp(&self.timestamp)
                    }
                    key_order => key_order.reverse(), // Reverse for min-heap
                }
            }
            ScanDirection::Backward => {
                // For backward scan: larger keys first, then newer timestamps
                match other.key.cmp(&self.key) {
                    Ordering::Equal => {
                        // Same key: prefer newer timestamp (higher value)
                        other.timestamp.cmp(&self.timestamp)
                    }
                    key_order => key_order.reverse(), // Reverse for min-heap
                }
            }
        }
    }
}

impl FixedRangeIterator {
    pub fn new(
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        direction: ScanDirection,
        read_timestamp: u64,
    ) -> Self {
        Self {
            start_key,
            end_key,
            direction,
            btree_iterator: None,
            lsm_iterator: None,
            version_store_iterator: None,
            merge_heap: BinaryHeap::new(),
            last_key: None,
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
            self.start_key.clone(),
            self.end_key.clone(),
            self.direction.clone(),
        )?);
        self.initialize_btree_iterator()?;
        Ok(())
    }

    pub fn attach_lsm(&mut self, lsm: &Arc<LSMTree>) -> Result<()> {
        self.lsm_iterator = Some(LSMIterator::new(
            lsm,
            self.start_key.clone(),
            self.end_key.clone(),
            self.direction.clone(),
            self.read_timestamp,
        )?);
        self.initialize_lsm_iterator()?;
        Ok(())
    }

    pub fn attach_version_store(&mut self, version_store: &Arc<VersionStore>) -> Result<()> {
        self.version_store_iterator = Some(VersionStoreIterator::new(
            version_store,
            self.start_key.clone(),
            self.end_key.clone(),
            self.direction.clone(),
            self.read_timestamp,
        )?);
        self.initialize_version_store_iterator()?;
        Ok(())
    }

    fn initialize_btree_iterator(&mut self) -> Result<()> {
        if let Some(ref mut iter) = self.btree_iterator {
            if let Some((key, value)) = iter.next()? {
                if self.is_key_in_range(&key) {
                    self.merge_heap.push(MergeEntry {
                        key,
                        value: Some(value),
                        timestamp: 0, // BTree entries have no timestamp
                        source: IteratorSource::BTree,
                        direction: self.direction.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn initialize_lsm_iterator(&mut self) -> Result<()> {
        if let Some(ref mut iter) = self.lsm_iterator {
            if let Some((key, value, timestamp)) = iter.next()? {
                if self.is_key_in_range(&key) {
                    self.merge_heap.push(MergeEntry {
                        key,
                        value,
                        timestamp,
                        source: IteratorSource::Lsm,
                        direction: self.direction.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn initialize_version_store_iterator(&mut self) -> Result<()> {
        if let Some(ref mut iter) = self.version_store_iterator {
            if let Some((key, value, timestamp)) = iter.next()? {
                if self.is_key_in_range(&key) {
                    self.merge_heap.push(MergeEntry {
                        key,
                        value,
                        timestamp,
                        source: IteratorSource::VersionStore,
                        direction: self.direction.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn advance_iterator(&mut self, source: &IteratorSource) -> Result<()> {
        match source {
            IteratorSource::BTree => {
                if let Some(ref mut iter) = self.btree_iterator {
                    if let Some((key, value)) = iter.next()? {
                        if self.is_key_in_range(&key) {
                            self.merge_heap.push(MergeEntry {
                                key,
                                value: Some(value),
                                timestamp: 0,
                                source: IteratorSource::BTree,
                                direction: self.direction.clone(),
                            });
                        }
                    }
                }
            }
            IteratorSource::Lsm => {
                if let Some(ref mut iter) = self.lsm_iterator {
                    if let Some((key, value, timestamp)) = iter.next()? {
                        if self.is_key_in_range(&key) {
                            self.merge_heap.push(MergeEntry {
                                key,
                                value,
                                timestamp,
                                source: IteratorSource::Lsm,
                                direction: self.direction.clone(),
                            });
                        }
                    }
                }
            }
            IteratorSource::VersionStore => {
                if let Some(ref mut iter) = self.version_store_iterator {
                    if let Some((key, value, timestamp)) = iter.next()? {
                        if self.is_key_in_range(&key) {
                            self.merge_heap.push(MergeEntry {
                                key,
                                value,
                                timestamp,
                                source: IteratorSource::VersionStore,
                                direction: self.direction.clone(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn is_key_in_range(&self, key: &[u8]) -> bool {
        // Check start bound
        if let Some(ref start) = self.start_key {
            let cmp = key.cmp(start);
            match self.direction {
                ScanDirection::Forward => {
                    if cmp == Ordering::Less || (!self.include_start && cmp == Ordering::Equal) {
                        return false;
                    }
                }
                ScanDirection::Backward => {
                    if cmp == Ordering::Greater || (!self.include_start && cmp == Ordering::Equal) {
                        return false;
                    }
                }
            }
        }

        // Check end bound
        if let Some(ref end) = self.end_key {
            let cmp = key.cmp(end);
            match self.direction {
                ScanDirection::Forward => {
                    if cmp == Ordering::Greater || (!self.include_end && cmp == Ordering::Equal) {
                        return false;
                    }
                }
                ScanDirection::Backward => {
                    if cmp == Ordering::Less || (!self.include_end && cmp == Ordering::Equal) {
                        return false;
                    }
                }
            }
        }

        true
    }

    fn should_skip_duplicate(&self, key: &[u8]) -> bool {
        if let Some(ref last_key) = self.last_key {
            last_key == key
        } else {
            false
        }
    }
}

impl Iterator for FixedRangeIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check limit
        if let Some(limit) = self.limit {
            if self.count >= limit {
                return None;
            }
        }

        loop {
            // Get next entry from heap
            let entry = match self.merge_heap.pop() {
                Some(entry) => entry,
                None => return None,
            };

            // Advance the iterator that provided this entry
            if let Err(e) = self.advance_iterator(&entry.source) {
                return Some(Err(e));
            }

            // Skip if this is a duplicate of the last key we returned
            if self.should_skip_duplicate(&entry.key) {
                continue;
            }

            // Skip tombstones (deleted entries)
            if entry.value.is_none() {
                self.last_key = Some(entry.key);
                continue;
            }

            // We have a valid entry
            self.last_key = Some(entry.key.clone());
            self.count += 1;
            return Some(Ok((entry.key, entry.value.unwrap())));
        }
    }
}

// Placeholder iterator types - these would be implemented separately
struct BTreeIterator;
struct LSMIterator;
struct VersionStoreIterator;

impl BTreeIterator {
    fn new(_btree: &BPlusTree, _start: Option<Vec<u8>>, _end: Option<Vec<u8>>, _dir: ScanDirection) -> Result<Self> {
        Ok(BTreeIterator)
    }
    
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(None) // Placeholder
    }
}

impl LSMIterator {
    fn new(_lsm: &Arc<LSMTree>, _start: Option<Vec<u8>>, _end: Option<Vec<u8>>, _dir: ScanDirection, _ts: u64) -> Result<Self> {
        Ok(LSMIterator)
    }
    
    fn next(&mut self) -> Result<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> {
        Ok(None) // Placeholder
    }
}

impl VersionStoreIterator {
    fn new(_vs: &Arc<VersionStore>, _start: Option<Vec<u8>>, _end: Option<Vec<u8>>, _dir: ScanDirection, _ts: u64) -> Result<Self> {
        Ok(VersionStoreIterator)
    }
    
    fn next(&mut self) -> Result<Option<(Vec<u8>, Option<Vec<u8>>, u64)>> {
        Ok(None) // Placeholder
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_entry_ordering_forward() {
        let entry1 = MergeEntry {
            key: b"a".to_vec(),
            value: Some(b"value1".to_vec()),
            timestamp: 1,
            source: IteratorSource::BTree,
            direction: ScanDirection::Forward,
        };

        let entry2 = MergeEntry {
            key: b"b".to_vec(),
            value: Some(b"value2".to_vec()),
            timestamp: 1,
            source: IteratorSource::BTree,
            direction: ScanDirection::Forward,
        };

        let entry3 = MergeEntry {
            key: b"a".to_vec(),
            value: Some(b"value3".to_vec()),
            timestamp: 2,
            source: IteratorSource::Lsm,
            direction: ScanDirection::Forward,
        };

        // For forward iteration in a min-heap:
        // - entry1 (key="a", ts=1) should come before entry2 (key="b")
        // - entry3 (key="a", ts=2) should come before entry1 (key="a", ts=1) due to higher timestamp
        assert!(entry1 > entry2); // Reversed for min-heap
        assert!(entry3 > entry1); // Higher timestamp wins for same key
    }

    #[test]
    fn test_merge_entry_ordering_backward() {
        let entry1 = MergeEntry {
            key: b"b".to_vec(),
            value: Some(b"value1".to_vec()),
            timestamp: 1,
            source: IteratorSource::BTree,
            direction: ScanDirection::Backward,
        };

        let entry2 = MergeEntry {
            key: b"a".to_vec(),
            value: Some(b"value2".to_vec()),
            timestamp: 1,
            source: IteratorSource::BTree,
            direction: ScanDirection::Backward,
        };

        let entry3 = MergeEntry {
            key: b"b".to_vec(),
            value: Some(b"value3".to_vec()),
            timestamp: 2,
            source: IteratorSource::Lsm,
            direction: ScanDirection::Backward,
        };

        // For backward iteration in a min-heap:
        // - entry1 (key="b") should come before entry2 (key="a")
        // - entry3 (key="b", ts=2) should come before entry1 (key="b", ts=1) due to higher timestamp
        assert!(entry1 > entry2); // Reversed for min-heap
        assert!(entry3 > entry1); // Higher timestamp wins for same key
    }

    #[test]
    fn test_key_range_checking() {
        let mut iter = FixedRangeIterator::new(
            Some(b"b".to_vec()),
            Some(b"d".to_vec()),
            ScanDirection::Forward,
            100,
        );

        assert!(!iter.is_key_in_range(b"a")); // Before start
        assert!(iter.is_key_in_range(b"b"));  // At start
        assert!(iter.is_key_in_range(b"c"));  // In range
        assert!(!iter.is_key_in_range(b"d")); // At end (exclusive by default)
        assert!(!iter.is_key_in_range(b"e")); // After end

        // Test inclusive end
        iter = iter.with_bounds(true, true);
        assert!(iter.is_key_in_range(b"d")); // Now inclusive
    }
}