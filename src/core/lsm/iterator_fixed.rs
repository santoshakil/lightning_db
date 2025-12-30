use super::{LSMTree, MemTable};
use crate::core::error::Result;
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

impl SourceType {
    /// Returns priority for ordering - lower value means higher priority (newer data)
    fn priority(&self) -> u32 {
        match self {
            SourceType::ActiveMemtable => 0,
            SourceType::ImmutableMemtable(idx) => 1 + *idx as u32,
            SourceType::SSTable(level, _) => 1000 + *level as u32,
        }
    }
}

/// Heap entry wrapper that includes direction for proper comparison
#[derive(Clone, Debug)]
struct HeapEntry {
    entry: IteratorEntry,
    forward: bool,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare keys, reversing for backward iteration
        let key_order = self.entry.key.cmp(&other.entry.key);
        let key_order = if self.forward { key_order } else { key_order.reverse() };

        match key_order {
            Ordering::Equal => {
                // Same key: prefer newer data (lower priority value)
                self.entry.source_type.priority().cmp(&other.entry.source_type.priority())
            }
            ord => ord,
        }
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
    memtable_position: isize, // Signed for bidirectional iteration

    immutable_entries: Vec<Vec<(Vec<u8>, Vec<u8>)>>,
    immutable_positions: Vec<isize>, // Signed for bidirectional iteration

    sstable_iterators: Vec<Vec<SSTableIterator>>,

    // Merge heap for combining sources
    merge_heap: BinaryHeap<Reverse<HeapEntry>>,

    // Track if we've initialized
    initialized: bool,
}

struct SSTableIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    position: isize, // Signed to support backward iteration
    forward: bool,
}

impl SSTableIterator {
    fn new(
        sstable: Arc<crate::core::lsm::SSTable>,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        forward: bool,
    ) -> Result<Self> {
        // Read all entries from SSTable that match the range
        let all_entries = sstable.iter()?;

        let mut entries = Vec::new();
        for (key, value) in all_entries {
            // Check range bounds - direction aware
            // For forward: start_key is lower bound, end_key is upper bound
            // For backward: start_key is upper bound, end_key is lower bound (swapped by scan_reverse)
            if forward {
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
            } else {
                // Backward: start_key is upper bound (exclusive), end_key is lower bound (inclusive)
                // These are swapped by scan_reverse: original [start, end) becomes (end, start] internally
                // But we need to include original start (now end_key) and exclude original end (now start_key)
                if let Some(start) = start_key {
                    // Skip keys >= upper bound (exclusive)
                    if key.as_slice() >= start {
                        continue;
                    }
                }
                if let Some(end) = end_key {
                    // Skip keys < lower bound (lower bound is inclusive)
                    if key.as_slice() < end {
                        continue;
                    }
                }
            }
            entries.push((key, value));
        }

        // Start position depends on direction
        let position = if forward {
            0
        } else {
            entries.len() as isize - 1
        };

        Ok(Self {
            entries,
            position,
            forward,
        })
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.forward {
            if self.position >= 0 && (self.position as usize) < self.entries.len() {
                let entry = self.entries[self.position as usize].clone();
                self.position += 1;
                Some(entry)
            } else {
                None
            }
        } else if self.position >= 0 {
            let entry = self.entries[self.position as usize].clone();
            self.position -= 1;
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
        self.memtable_entries = self.collect_memtable_entries(&memtable_guard);
        drop(memtable_guard);

        // Set starting position based on direction
        self.memtable_position = if self.forward {
            0
        } else {
            self.memtable_entries.len() as isize - 1
        };

        // 2. Get entries from immutable memtables
        let immutable_memtables = lsm.get_immutable_memtables();
        let immutable_memtables_guard = immutable_memtables.read();
        for immutable in immutable_memtables_guard.iter() {
            let entries = self.collect_memtable_entries(immutable);
            let start_pos = if self.forward {
                0
            } else {
                entries.len() as isize - 1
            };
            self.immutable_entries.push(entries);
            self.immutable_positions.push(start_pos);
        }
        drop(immutable_memtables_guard);

        // 3. Create iterators for all SSTables
        // IMPORTANT: Sort SSTables by creation_time (newest first) so tombstones take priority
        let levels = lsm.get_levels();
        let levels_guard = levels.read();
        for level in levels_guard.iter() {
            let mut level_iterators = Vec::new();

            // Get SSTables and sort by creation_time (newest first)
            let mut sstables_sorted: Vec<_> = level.tables().to_vec();
            sstables_sorted.sort_by_key(|s| std::cmp::Reverse(s.creation_time()));

            for sstable in sstables_sorted.iter() {
                // Check if SSTable might contain keys in our range
                // For backward scans, start_key is the upper bound, end_key is the lower bound
                let should_include = if self.forward {
                    match (&self.start_key, &self.end_key) {
                        (Some(start), Some(end)) => {
                            // SSTable range overlaps with query range [start, end)
                            sstable.max_key() >= start.as_slice() && sstable.min_key() < end.as_slice()
                        }
                        (Some(start), None) => sstable.max_key() >= start.as_slice(),
                        (None, Some(end)) => sstable.min_key() < end.as_slice(),
                        (None, None) => true,
                    }
                } else {
                    // Backward: start_key is upper bound (exclusive), end_key is lower bound (inclusive)
                    // Query range is [lower, upper) - same bounds as forward, just iterating in reverse
                    match (&self.start_key, &self.end_key) {
                        (Some(upper), Some(lower)) => {
                            // SSTable range overlaps with query range [lower, upper)
                            sstable.max_key() >= lower.as_slice() && sstable.min_key() < upper.as_slice()
                        }
                        (Some(upper), None) => sstable.min_key() < upper.as_slice(),
                        (None, Some(lower)) => sstable.max_key() >= lower.as_slice(),
                        (None, None) => true,
                    }
                };

                if should_include {
                    let iter = SSTableIterator::new(
                        sstable.clone(),
                        self.start_key.as_deref(),
                        self.end_key.as_deref(),
                        self.forward,
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
            .filter(|(k, _v)| {
                // Check range only, don't filter tombstones
                self.is_in_range(k)
            })
            .map(|(k, v)| {
                // Include tombstones - use the tombstone marker if value is None
                let value = v
                    .clone()
                    .unwrap_or_else(|| LSMTree::TOMBSTONE_MARKER.to_vec());
                (k.clone(), value)
            })
            .collect();

        // Always sort in forward order - direction is handled by HeapEntry::Ord
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        entries
    }

    fn is_in_range(&self, key: &[u8]) -> bool {
        // For forward iteration: key >= start_key AND key < end_key
        // For backward iteration: start_key and end_key are swapped by scan_reverse,
        // so we need: key <= start_key AND key > end_key
        if self.forward {
            // Forward: key must be >= start and < end
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
        } else {
            // Backward: start_key is upper bound (exclusive), end_key is lower bound (inclusive)
            // Original [start, end) becomes: include keys where end_key <= key < start_key
            if let Some(ref start) = self.start_key {
                // Exclude keys >= upper bound (exclusive)
                if key >= start.as_slice() {
                    return false;
                }
            }
            if let Some(ref end) = self.end_key {
                // Exclude keys < lower bound (lower bound is inclusive)
                if key < end.as_slice() {
                    return false;
                }
            }
        }
        true
    }

    fn prime_merge_heap(&mut self) {
        let forward = self.forward;

        // Add first entry from active memtable (position is already set for direction)
        if self.memtable_position >= 0
            && (self.memtable_position as usize) < self.memtable_entries.len()
        {
            let (key, value) = self.memtable_entries[self.memtable_position as usize].clone();
            self.merge_heap.push(Reverse(HeapEntry {
                entry: IteratorEntry {
                    key,
                    value,
                    source_type: SourceType::ActiveMemtable,
                },
                forward,
            }));
        }

        // Add first entry from each immutable memtable (positions already set for direction)
        for (idx, entries) in self.immutable_entries.iter().enumerate() {
            let pos = self.immutable_positions[idx];
            if pos >= 0 && (pos as usize) < entries.len() {
                let (key, value) = entries[pos as usize].clone();
                self.merge_heap.push(Reverse(HeapEntry {
                    entry: IteratorEntry {
                        key,
                        value,
                        source_type: SourceType::ImmutableMemtable(idx),
                    },
                    forward,
                }));
            }
        }

        // Add first entry from each SSTable iterator (already initialized for direction)
        for (level_idx, level_iters) in self.sstable_iterators.iter_mut().enumerate() {
            for (sstable_idx, iter) in level_iters.iter_mut().enumerate() {
                if let Some((key, value)) = iter.next() {
                    self.merge_heap.push(Reverse(HeapEntry {
                        entry: IteratorEntry {
                            key,
                            value,
                            source_type: SourceType::SSTable(level_idx, sstable_idx),
                        },
                        forward,
                    }));
                }
            }
        }
    }

    pub fn advance(&mut self) -> Option<(Vec<u8>, Option<Vec<u8>>, u64)> {
        while let Some(Reverse(heap_entry)) = self.merge_heap.pop() {
            let entry = heap_entry.entry;
            let is_tombstone = LSMTree::is_tombstone(&entry.value);

            // Refill from the source that provided this entry
            self.refill_from_source(entry.source_type.clone());

            // Skip duplicate keys (keep the first one we see, which is the newest)
            // This handles both tombstones and regular values correctly
            while let Some(Reverse(next_heap_entry)) = self.merge_heap.peek() {
                if next_heap_entry.entry.key == entry.key {
                    // Safe: we just peeked so pop will succeed, but use if-let for robustness
                    if let Some(Reverse(dup_heap_entry)) = self.merge_heap.pop() {
                        self.refill_from_source(dup_heap_entry.entry.source_type);
                    }
                } else {
                    break;
                }
            }

            // If the newest entry for this key is a tombstone, skip it
            // Otherwise return the value
            if !is_tombstone {
                return Some((entry.key, Some(entry.value), 0)); // timestamp 0 for now
            }
            // If it was a tombstone, continue to the next key
        }

        None
    }

    fn refill_from_source(&mut self, source_type: SourceType) {
        let forward = self.forward;

        match source_type {
            SourceType::ActiveMemtable => {
                // Move position in the direction of iteration
                if forward {
                    self.memtable_position += 1;
                } else {
                    self.memtable_position -= 1;
                }

                // Check if position is valid
                if self.memtable_position >= 0
                    && (self.memtable_position as usize) < self.memtable_entries.len()
                {
                    let (key, value) =
                        self.memtable_entries[self.memtable_position as usize].clone();
                    self.merge_heap.push(Reverse(HeapEntry {
                        entry: IteratorEntry {
                            key,
                            value,
                            source_type: SourceType::ActiveMemtable,
                        },
                        forward,
                    }));
                }
            }
            SourceType::ImmutableMemtable(idx) => {
                // Move position in the direction of iteration
                if forward {
                    self.immutable_positions[idx] += 1;
                } else {
                    self.immutable_positions[idx] -= 1;
                }

                // Check if position is valid
                let pos = self.immutable_positions[idx];
                if pos >= 0 && (pos as usize) < self.immutable_entries[idx].len() {
                    let (key, value) = self.immutable_entries[idx][pos as usize].clone();
                    self.merge_heap.push(Reverse(HeapEntry {
                        entry: IteratorEntry {
                            key,
                            value,
                            source_type: SourceType::ImmutableMemtable(idx),
                        },
                        forward,
                    }));
                }
            }
            SourceType::SSTable(level_idx, sstable_idx) => {
                // SSTableIterator.next() already handles direction internally
                if let Some((key, value)) = self.sstable_iterators[level_idx][sstable_idx].next() {
                    self.merge_heap.push(Reverse(HeapEntry {
                        entry: IteratorEntry {
                            key,
                            value,
                            source_type: SourceType::SSTable(level_idx, sstable_idx),
                        },
                        forward,
                    }));
                }
            }
        }
    }
}
