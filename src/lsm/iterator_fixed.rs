use crate::error::Result;
use crate::lsm::{LSMTree, MemTable};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

/// Iterator entry with source information for merging
#[derive(Clone, Debug)]
struct IteratorEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    source_type: SourceType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SourceType {
    ActiveMemtable,
    ImmutableMemtable(usize),
    SSTable(usize, usize), // (level, sstable_index)
}

impl PartialEq for IteratorEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for IteratorEntry {}

impl PartialOrd for IteratorEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IteratorEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // For min-heap, we wrap in Reverse when pushing
        self.key.cmp(&other.key)
    }
}

/// Full LSM iterator that combines memtable, immutable memtables, and SSTables
pub struct LSMFullIteratorFixed {
    // Range bounds
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    forward: bool,

    // Current iterators
    memtable_entries: Vec<(Vec<u8>, Vec<u8>)>,
    memtable_position: usize,

    immutable_entries: Vec<Vec<(Vec<u8>, Vec<u8>)>>,
    immutable_positions: Vec<usize>,

    sstable_iterators: Vec<Vec<SSTableIterator>>,

    // Merge heap for combining sources
    merge_heap: BinaryHeap<Reverse<IteratorEntry>>,

    // Track if we've initialized
    initialized: bool,
}

struct SSTableIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
}

impl SSTableIterator {
    fn new(
        sstable: Arc<crate::lsm::SSTable>,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Self> {
        // Read all entries from SSTable that match the range
        let all_entries = sstable.iter()?;

        let mut entries = Vec::new();
        for (key, value) in all_entries {
            // Check range bounds
            if let Some(start) = start_key {
                if key.as_slice() < start {
                    continue;
                }
            }
            if let Some(end) = end_key {
                if key.as_slice() >= end {
                    break;
                }
            }
            entries.push((key, value));
        }

        Ok(Self {
            entries,
            position: 0,
        })
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.position < self.entries.len() {
            let entry = self.entries[self.position].clone();
            self.position += 1;
            Some(entry)
        } else {
            None
        }
    }
}

impl LSMFullIteratorFixed {
    pub fn new(
        lsm: &Arc<LSMTree>,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        forward: bool,
    ) -> Result<Self> {
        let mut iter = Self {
            start_key,
            end_key,
            forward,
            memtable_entries: Vec::new(),
            memtable_position: 0,
            immutable_entries: Vec::new(),
            immutable_positions: Vec::new(),
            sstable_iterators: Vec::new(),
            merge_heap: BinaryHeap::new(),
            initialized: false,
        };

        // Initialize all data sources
        iter.initialize(lsm)?;

        Ok(iter)
    }

    fn initialize(&mut self, lsm: &LSMTree) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        // 1. Get entries from active memtable
        let memtable = lsm.get_memtable();
        let memtable_guard = memtable.read();
        self.memtable_entries = self.collect_memtable_entries(&*memtable_guard);
        drop(memtable_guard);

        // 2. Get entries from immutable memtables
        let immutable_memtables = lsm.get_immutable_memtables();
        let immutable_memtables_guard = immutable_memtables.read();
        for immutable in immutable_memtables_guard.iter() {
            let entries = self.collect_memtable_entries(immutable);
            self.immutable_entries.push(entries);
            self.immutable_positions.push(0);
        }
        drop(immutable_memtables_guard);

        // 3. Create iterators for all SSTables
        let levels = lsm.get_levels();
        let levels_guard = levels.read();
        for level in levels_guard.iter() {
            let mut level_iterators = Vec::new();
            for sstable in level.tables().iter() {
                // Check if SSTable might contain keys in our range
                let should_include = match (&self.start_key, &self.end_key) {
                    (Some(start), Some(end)) => {
                        // SSTable range overlaps with query range
                        sstable.max_key() >= start.as_slice() && sstable.min_key() < end.as_slice()
                    }
                    (Some(start), None) => sstable.max_key() >= start.as_slice(),
                    (None, Some(end)) => sstable.min_key() < end.as_slice(),
                    (None, None) => true,
                };

                if should_include {
                    let iter = SSTableIterator::new(
                        sstable.clone(),
                        self.start_key.as_deref(),
                        self.end_key.as_deref(),
                    )?;
                    level_iterators.push(iter);
                }
            }
            self.sstable_iterators.push(level_iterators);
        }
        drop(levels_guard);

        // 4. Prime the merge heap with first entry from each source
        self.prime_merge_heap();

        self.initialized = true;
        Ok(())
    }

    fn collect_memtable_entries(&self, memtable: &MemTable) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries: Vec<_> = memtable
            .entries()
            .filter(|(k, v)| {
                // Filter out tombstones and check range
                v.is_some() && self.is_in_range(k)
            })
            .map(|(k, v)| (k.clone(), v.unwrap().clone()))
            .collect();

        // Sort based on direction
        if self.forward {
            entries.sort_by(|a, b| a.0.cmp(&b.0));
        } else {
            entries.sort_by(|a, b| b.0.cmp(&a.0));
        }

        entries
    }

    fn is_in_range(&self, key: &[u8]) -> bool {
        if let Some(ref start) = self.start_key {
            if key < start.as_slice() {
                return false;
            }
        }
        if let Some(ref end) = self.end_key {
            if key >= end.as_slice() {
                return false;
            }
        }
        true
    }

    fn prime_merge_heap(&mut self) {
        // Add first entry from active memtable
        if self.memtable_position < self.memtable_entries.len() {
            let (key, value) = self.memtable_entries[self.memtable_position].clone();
            self.merge_heap.push(Reverse(IteratorEntry {
                key,
                value,
                source_type: SourceType::ActiveMemtable,
            }));
        }

        // Add first entry from each immutable memtable
        for (idx, entries) in self.immutable_entries.iter().enumerate() {
            if !entries.is_empty() {
                let (key, value) = entries[0].clone();
                self.merge_heap.push(Reverse(IteratorEntry {
                    key,
                    value,
                    source_type: SourceType::ImmutableMemtable(idx),
                }));
            }
        }

        // Add first entry from each SSTable iterator
        for (level_idx, level_iters) in self.sstable_iterators.iter_mut().enumerate() {
            for (sstable_idx, iter) in level_iters.iter_mut().enumerate() {
                if let Some((key, value)) = iter.next() {
                    self.merge_heap.push(Reverse(IteratorEntry {
                        key,
                        value,
                        source_type: SourceType::SSTable(level_idx, sstable_idx),
                    }));
                }
            }
        }
    }

    pub fn next(&mut self) -> Option<(Vec<u8>, Option<Vec<u8>>, u64)> {
        while let Some(Reverse(entry)) = self.merge_heap.pop() {
            // Check if this is a tombstone
            if LSMTree::is_tombstone(&entry.value) {
                // Skip tombstones but continue to next entry
                self.refill_from_source(entry.source_type);
                continue;
            }

            // Refill from the source that provided this entry
            self.refill_from_source(entry.source_type);

            // Skip duplicate keys (keep the first one we see, which is the newest)
            while let Some(Reverse(next_entry)) = self.merge_heap.peek() {
                if next_entry.key == entry.key {
                    let Reverse(dup_entry) = self.merge_heap.pop().unwrap();
                    self.refill_from_source(dup_entry.source_type);
                } else {
                    break;
                }
            }

            return Some((entry.key, Some(entry.value), 0)); // timestamp 0 for now
        }

        None
    }

    fn refill_from_source(&mut self, source_type: SourceType) {
        match source_type {
            SourceType::ActiveMemtable => {
                self.memtable_position += 1;
                if self.memtable_position < self.memtable_entries.len() {
                    let (key, value) = self.memtable_entries[self.memtable_position].clone();
                    self.merge_heap.push(Reverse(IteratorEntry {
                        key,
                        value,
                        source_type: SourceType::ActiveMemtable,
                    }));
                }
            }
            SourceType::ImmutableMemtable(idx) => {
                self.immutable_positions[idx] += 1;
                if self.immutable_positions[idx] < self.immutable_entries[idx].len() {
                    let (key, value) =
                        self.immutable_entries[idx][self.immutable_positions[idx]].clone();
                    self.merge_heap.push(Reverse(IteratorEntry {
                        key,
                        value,
                        source_type: SourceType::ImmutableMemtable(idx),
                    }));
                }
            }
            SourceType::SSTable(level_idx, sstable_idx) => {
                if let Some((key, value)) = self.sstable_iterators[level_idx][sstable_idx].next() {
                    self.merge_heap.push(Reverse(IteratorEntry {
                        key,
                        value,
                        source_type: SourceType::SSTable(level_idx, sstable_idx),
                    }));
                }
            }
        }
    }
}
