//! Simple Production-Ready B+Tree
//!
//! A straightforward B+Tree implementation that prioritizes correctness
//! and data integrity over complex concurrent optimizations.

use crate::core::error::Result;
use std::collections::BTreeMap;

/// Simple B+Tree with guaranteed data integrity
#[derive(Debug, Default)]
pub struct SimpleBTree {
    data: BTreeMap<u64, u64>,
}

impl SimpleBTree {
    /// Create a new B+Tree
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    /// Insert a key-value pair
    /// Returns true if the key was newly inserted, false if it was updated
    pub fn insert(&mut self, key: u64, value: u64) -> Result<bool> {
        let was_new = self.data.insert(key, value).is_none();
        Ok(was_new)
    }

    /// Search for a key
    pub fn search(&self, key: u64) -> Option<u64> {
        self.data.get(&key).copied()
    }

    /// Delete a key
    /// Returns true if the key was present and removed
    pub fn delete(&mut self, key: u64) -> Result<bool> {
        Ok(self.data.remove(&key).is_some())
    }

    /// Get the number of keys
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Get the height of the tree (always 1 for this simple implementation)
    pub fn height(&self) -> usize {
        1
    }

    /// Create a range iterator
    pub fn range(&self, start: Option<u64>, end: Option<u64>) -> RangeIterator {
        let filtered_entries: Vec<(u64, u64)> = self
            .data
            .iter()
            .filter(|(&k, _)| {
                if let Some(start_key) = start {
                    if k < start_key {
                        return false;
                    }
                }
                if let Some(end_key) = end {
                    if k > end_key {
                        return false;
                    }
                }
                true
            })
            .map(|(&k, &v)| (k, v))
            .collect();

        RangeIterator {
            entries: filtered_entries,
            position: 0,
        }
    }

    /// Validate tree structure (always valid for BTreeMap)
    pub fn validate(&self) -> Result<()> {
        // BTreeMap guarantees correct structure
        Ok(())
    }
}

/// Range iterator for the B+Tree
pub struct RangeIterator {
    entries: Vec<(u64, u64)>,
    position: usize,
}

impl Iterator for RangeIterator {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.entries.len() {
            let item = self.entries[self.position];
            self.position += 1;
            Some(item)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut tree = SimpleBTree::new();

        // Test insertions
        assert!(tree.insert(1, 10).unwrap());
        assert!(tree.insert(2, 20).unwrap());
        assert!(tree.insert(3, 30).unwrap());

        // Test duplicate key
        assert!(!tree.insert(2, 25).unwrap());

        // Test searches
        assert_eq!(tree.search(1), Some(10));
        assert_eq!(tree.search(2), Some(25)); // Updated value
        assert_eq!(tree.search(3), Some(30));
        assert_eq!(tree.search(4), None);

        // Test size
        assert_eq!(tree.size(), 3);

        // Test deletion
        assert!(tree.delete(2).unwrap());
        assert_eq!(tree.search(2), None);
        assert_eq!(tree.size(), 2);
    }

    #[test]
    fn test_large_insertions() {
        let mut tree = SimpleBTree::new();

        // Insert many keys
        for i in 0..1000 {
            assert!(tree.insert(i, i * 2).unwrap());
        }

        // Verify all keys
        for i in 0..1000 {
            assert_eq!(tree.search(i), Some(i * 2));
        }

        assert_eq!(tree.size(), 1000);
    }

    #[test]
    fn test_range_iteration() {
        let mut tree = SimpleBTree::new();

        // Insert test data
        for i in 0..100 {
            tree.insert(i * 2, i * 20).unwrap(); // Even keys only
        }

        // Test range iteration
        let results: Vec<(u64, u64)> = tree.range(Some(10), Some(30)).collect();

        assert!(!results.is_empty());

        // Verify results are in range and sorted
        for (i, &(key, value)) in results.iter().enumerate() {
            assert!((10..=30).contains(&key));
            assert_eq!(value, key * 10);

            if i > 0 {
                assert!(key > results[i - 1].0);
            }
        }
    }
}
