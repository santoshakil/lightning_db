use crate::error::Result;
use crate::lsm::{LSMTree, MemTable};
use parking_lot::RwLock;
use std::sync::Arc;

/// Iterator for LSM tree memtable
pub struct LSMMemTableIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    current: usize,
}

impl LSMMemTableIterator {
    pub fn new(
        memtable: &Arc<RwLock<MemTable>>,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        forward: bool,
    ) -> Self {
        let memtable = memtable.read();
        let mut entries: Vec<_> = memtable
            .entries()
            .filter(|(k, v)| {
                // Filter out tombstones (None values)
                v.is_some() &&
                // Check start bound
                start_key.map_or(true, |start| k.as_slice() >= start) &&
                // Check end bound
                end_key.map_or(true, |end| k.as_slice() < end)
            })
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect();

        // Sort entries
        entries.sort_by(|a, b| {
            if forward {
                a.0.cmp(&b.0)
            } else {
                b.0.cmp(&a.0)
            }
        });

        Self {
            entries,
            current: 0,
        }
    }

    pub fn advance(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.current < self.entries.len() {
            let entry = self.entries[self.current].clone();
            self.current += 1;
            Some(entry)
        } else {
            None
        }
    }
}

/// Full LSM iterator that combines memtable and SSTables
pub struct LSMFullIterator {
    memtable_iter: Option<LSMMemTableIterator>,
    // TODO: Add SSTable iterators
}

impl LSMFullIterator {
    pub fn new(
        lsm: &Arc<LSMTree>,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        forward: bool,
    ) -> Result<Self> {
        // Get memtable iterator
        let memtable = lsm.get_memtable();
        let memtable_iter = Some(LSMMemTableIterator::new(
            &memtable,
            start_key.as_deref(),
            end_key.as_deref(),
            forward,
        ));

        Ok(Self { memtable_iter })
    }

    pub fn advance(&mut self) -> Option<(Vec<u8>, Option<Vec<u8>>, u64)> {
        // For now, just return from memtable
        if let Some(ref mut memtable_iter) = self.memtable_iter {
            if let Some((key, value)) = memtable_iter.advance() {
                return Some((key, Some(value), 0)); // timestamp 0 for now
            }
        }

        // TODO: Add SSTable iteration
        None
    }
}
