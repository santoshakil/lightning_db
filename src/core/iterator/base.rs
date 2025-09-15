use crate::core::error::Result;
use std::cmp::Ordering;

/// Base trait for all database iterators
pub trait DatabaseIterator {
    /// The type of key returned by this iterator
    type Key: AsRef<[u8]> + Clone;

    /// The type of value returned by this iterator
    type Value: AsRef<[u8]> + Clone;

    /// Advance the iterator to the next element
    fn next(&mut self) -> Result<Option<(Self::Key, Self::Value)>>;

    /// Check if the iterator is valid (has more elements)
    fn valid(&self) -> bool;

    /// Get the current key without advancing the iterator
    fn current_key(&self) -> Option<&Self::Key>;

    /// Get the current value without advancing the iterator
    fn current_value(&self) -> Option<&Self::Value>;

    /// Seek to the first key greater than or equal to the target
    fn seek(&mut self, target: &[u8]) -> Result<()>;

    /// Seek to the first key in the iterator
    fn seek_to_first(&mut self) -> Result<()>;

    /// Seek to the last key in the iterator
    fn seek_to_last(&mut self) -> Result<()>;

    /// Get iterator statistics
    fn stats(&self) -> IteratorStats {
        IteratorStats::default()
    }
}

/// Extended trait for iterators that support range operations
pub trait RangeIterator: DatabaseIterator {
    /// Set the start bound for the range
    fn set_start_bound(&mut self, key: Option<Vec<u8>>, inclusive: bool);

    /// Set the end bound for the range  
    fn set_end_bound(&mut self, key: Option<Vec<u8>>, inclusive: bool);

    /// Set a limit on the number of items to return
    fn set_limit(&mut self, limit: Option<usize>);

    /// Get the current count of items returned
    fn count(&self) -> usize;
}

/// Trait for iterators that support reverse iteration
pub trait ReversibleIterator: DatabaseIterator {
    /// Move to the previous element
    fn prev(&mut self) -> Result<Option<(Self::Key, Self::Value)>>;

    /// Set the scan direction
    fn set_direction(&mut self, direction: ScanDirection);

    /// Get the current scan direction
    fn direction(&self) -> ScanDirection;
}

/// Trait for iterators that support seeking to specific positions
pub trait SeekableIterator: DatabaseIterator {
    /// Seek to a specific key position
    fn seek_exact(&mut self, key: &[u8]) -> Result<bool>;

    /// Seek to the first key matching a prefix
    fn seek_prefix(&mut self, prefix: &[u8]) -> Result<()>;

    /// Check if the current key has a specific prefix
    fn has_prefix(&self, prefix: &[u8]) -> bool;
}

/// Trait for iterators that can be merged with other iterators
pub trait MergeableIterator: DatabaseIterator {
    /// Get the timestamp for the current entry (for conflict resolution)
    fn current_timestamp(&self) -> Option<u64>;

    /// Get the source identifier for this iterator
    fn source_id(&self) -> IteratorSource;

    /// Check if the current entry is a tombstone (deleted)
    fn is_tombstone(&self) -> bool;
}

/// Scan direction for iterators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

impl Default for ScanDirection {
    fn default() -> Self {
        ScanDirection::Forward
    }
}

/// Iterator source identification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IteratorSource {
    BTree,
    LSM,
    MemTable,
    SSTable(u32), // SSTable with level
    VersionStore,
    WAL,
    Cache,
}

/// Iterator statistics for monitoring and optimization
#[derive(Debug, Clone, Default)]
pub struct IteratorStats {
    pub keys_read: u64,
    pub bytes_read: u64,
    pub seeks_performed: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub io_operations: u64,
}

impl IteratorStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_key_read(&mut self, key_size: usize, value_size: usize) {
        self.keys_read += 1;
        self.bytes_read += (key_size + value_size) as u64;
    }

    pub fn record_seek(&mut self) {
        self.seeks_performed += 1;
    }

    pub fn record_cache_hit(&mut self) {
        self.cache_hits += 1;
    }

    pub fn record_cache_miss(&mut self) {
        self.cache_misses += 1;
        self.io_operations += 1;
    }

    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }
}

/// Configuration for iterator behavior
#[derive(Debug, Clone)]
pub struct IteratorConfig {
    /// Maximum number of keys to prefetch
    pub prefetch_size: usize,

    /// Whether to use caching
    pub enable_cache: bool,

    /// Read timestamp for MVCC
    pub read_timestamp: Option<u64>,

    /// Whether to skip tombstones
    pub skip_tombstones: bool,

    /// Buffer size for I/O operations
    pub buffer_size: usize,
}

impl Default for IteratorConfig {
    fn default() -> Self {
        Self {
            prefetch_size: 100,
            enable_cache: true,
            read_timestamp: None,
            skip_tombstones: true,
            buffer_size: 64 * 1024, // 64KB
        }
    }
}

/// Base implementation for common iterator functionality
pub struct BaseIterator<K, V> {
    config: IteratorConfig,
    stats: IteratorStats,
    current_key: Option<K>,
    current_value: Option<V>,
    direction: ScanDirection,
    start_bound: Option<(Vec<u8>, bool)>, // (key, inclusive)
    end_bound: Option<(Vec<u8>, bool)>,   // (key, inclusive)
    limit: Option<usize>,
    count: usize,
}

impl<K, V> DatabaseIterator for BaseIterator<K, V>
where
    K: AsRef<[u8]> + Clone,
    V: AsRef<[u8]> + Clone,
{
    type Key = K;
    type Value = V;

    fn next(&mut self) -> Result<Option<(Self::Key, Self::Value)>> {
        // Default implementation - should be overridden by concrete iterators
        Ok(None)
    }

    fn valid(&self) -> bool {
        self.current_key.is_some() && !self.is_limit_reached()
    }

    fn current_key(&self) -> Option<&Self::Key> {
        self.current_key.as_ref()
    }

    fn current_value(&self) -> Option<&Self::Value> {
        self.current_value.as_ref()
    }

    fn seek(&mut self, _target: &[u8]) -> Result<()> {
        // Default implementation - should be overridden by concrete iterators
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        // Default implementation - should be overridden by concrete iterators
        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<()> {
        // Default implementation - should be overridden by concrete iterators
        Ok(())
    }

    fn stats(&self) -> IteratorStats {
        self.stats.clone()
    }
}

impl<K, V> BaseIterator<K, V>
where
    K: AsRef<[u8]> + Clone,
    V: AsRef<[u8]> + Clone,
{
    pub fn new(config: IteratorConfig) -> Self {
        Self {
            config,
            stats: IteratorStats::new(),
            current_key: None,
            current_value: None,
            direction: ScanDirection::Forward,
            start_bound: None,
            end_bound: None,
            limit: None,
            count: 0,
        }
    }

    /// Check if a key is within the configured bounds
    pub fn is_key_in_bounds(&self, key: &[u8]) -> bool {
        // Check start bound
        if let Some((start_key, inclusive)) = &self.start_bound {
            let cmp = key.cmp(start_key.as_slice());
            match (cmp, *inclusive) {
                (Ordering::Less, _) => return false,
                (Ordering::Equal, false) => return false,
                _ => {}
            }
        }

        // Check end bound
        if let Some((end_key, inclusive)) = &self.end_bound {
            let cmp = key.cmp(end_key.as_slice());
            match (cmp, *inclusive) {
                (Ordering::Greater, _) => return false,
                (Ordering::Equal, false) => return false,
                _ => {}
            }
        }

        true
    }

    /// Check if the iterator has reached its limit
    pub fn is_limit_reached(&self) -> bool {
        if let Some(limit) = self.limit {
            self.count >= limit
        } else {
            false
        }
    }

    /// Update the current position
    pub fn update_current(&mut self, key: K, value: V) {
        self.current_key = Some(key);
        self.current_value = Some(value);
        self.count += 1;
    }

    /// Clear the current position
    pub fn clear_current(&mut self) {
        self.current_key = None;
        self.current_value = None;
    }

    /// Get configuration
    pub fn config(&self) -> &IteratorConfig {
        &self.config
    }

    /// Get mutable statistics
    pub fn stats_mut(&mut self) -> &mut IteratorStats {
        &mut self.stats
    }

    /// Get the current count
    pub fn current_count(&self) -> usize {
        self.count
    }

    /// Reset the count
    pub fn reset_count(&mut self) {
        self.count = 0;
    }
}

impl<K, V> RangeIterator for BaseIterator<K, V>
where
    K: AsRef<[u8]> + Clone,
    V: AsRef<[u8]> + Clone,
{
    fn set_start_bound(&mut self, key: Option<Vec<u8>>, inclusive: bool) {
        self.start_bound = key.map(|k| (k, inclusive));
    }

    fn set_end_bound(&mut self, key: Option<Vec<u8>>, inclusive: bool) {
        self.end_bound = key.map(|k| (k, inclusive));
    }

    fn set_limit(&mut self, limit: Option<usize>) {
        self.limit = limit;
    }

    fn count(&self) -> usize {
        self.count
    }
}

impl<K, V> ReversibleIterator for BaseIterator<K, V>
where
    K: AsRef<[u8]> + Clone,
    V: AsRef<[u8]> + Clone,
{
    fn prev(&mut self) -> Result<Option<(Self::Key, Self::Value)>> {
        // Default implementation - should be overridden by concrete iterators
        Ok(None)
    }

    fn set_direction(&mut self, direction: ScanDirection) {
        self.direction = direction;
    }

    fn direction(&self) -> ScanDirection {
        self.direction
    }
}

/// Helper functions for iterator implementations
pub mod helpers {
    use super::*;

    /// Compare two keys according to scan direction
    pub fn compare_keys(a: &[u8], b: &[u8], direction: ScanDirection) -> Ordering {
        match direction {
            ScanDirection::Forward => a.cmp(b),
            ScanDirection::Backward => b.cmp(a),
        }
    }

    /// Check if a key matches a prefix
    pub fn has_prefix(key: &[u8], prefix: &[u8]) -> bool {
        key.len() >= prefix.len() && &key[..prefix.len()] == prefix
    }

    /// Find the next key after a given key (for prefix iteration)
    pub fn next_key(key: &[u8]) -> Option<Vec<u8>> {
        let mut next = key.to_vec();
        for i in (0..next.len()).rev() {
            if next[i] < 255 {
                next[i] += 1;
                next.truncate(i + 1);
                return Some(next);
            }
        }
        None // Overflow - no next key
    }

    /// Create a seek target for prefix iteration
    pub fn prefix_seek_target(prefix: &[u8]) -> Vec<u8> {
        prefix.to_vec()
    }

    /// Create an end bound for prefix iteration
    pub fn prefix_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
        next_key(prefix)
    }
}

/// Macro to implement common iterator methods
#[macro_export]
macro_rules! impl_base_iterator {
    ($iterator_type:ty, $key_type:ty, $value_type:ty) => {
        impl DatabaseIterator for $iterator_type {
            type Key = $key_type;
            type Value = $value_type;

            fn valid(&self) -> bool {
                self.current_key.is_some() && !self.is_limit_reached()
            }

            fn current_key(&self) -> Option<&Self::Key> {
                self.current_key.as_ref()
            }

            fn current_value(&self) -> Option<&Self::Value> {
                self.current_value.as_ref()
            }

            fn stats(&self) -> IteratorStats {
                self.stats.clone()
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iterator_bounds() {
        let config = IteratorConfig::default();
        let mut iter = BaseIterator::<Vec<u8>, Vec<u8>>::new(config);

        // Set bounds
        iter.set_start_bound(Some(b"key2".to_vec()), true);
        iter.set_end_bound(Some(b"key8".to_vec()), false);

        // Test keys
        assert!(!iter.is_key_in_bounds(b"key1")); // Before start
        assert!(iter.is_key_in_bounds(b"key2")); // At start (inclusive)
        assert!(iter.is_key_in_bounds(b"key5")); // In range
        assert!(!iter.is_key_in_bounds(b"key8")); // At end (exclusive)
        assert!(!iter.is_key_in_bounds(b"key9")); // After end
    }

    #[test]
    fn test_iterator_limit() {
        let config = IteratorConfig::default();
        let mut iter = BaseIterator::<Vec<u8>, Vec<u8>>::new(config);

        iter.set_limit(Some(3));

        assert!(!iter.is_limit_reached());

        iter.update_current(b"key1".to_vec(), b"value1".to_vec());
        assert!(!iter.is_limit_reached());

        iter.update_current(b"key2".to_vec(), b"value2".to_vec());
        assert!(!iter.is_limit_reached());

        iter.update_current(b"key3".to_vec(), b"value3".to_vec());
        assert!(iter.is_limit_reached());
    }

    #[test]
    fn test_helper_functions() {
        use helpers::*;

        // Test prefix matching
        assert!(has_prefix(b"prefix_key", b"prefix"));
        assert!(!has_prefix(b"other_key", b"prefix"));

        // Test next key generation
        assert_eq!(next_key(b"key"), Some(b"kez".to_vec()));
        assert_eq!(next_key(b"ke\xff"), Some(b"kf".to_vec()));

        // Test key comparison
        assert_eq!(
            compare_keys(b"a", b"b", ScanDirection::Forward),
            Ordering::Less
        );
        assert_eq!(
            compare_keys(b"a", b"b", ScanDirection::Backward),
            Ordering::Greater
        );
    }

    #[test]
    fn test_iterator_stats() {
        let mut stats = IteratorStats::new();

        stats.record_key_read(10, 20);
        stats.record_cache_hit();
        stats.record_cache_miss();

        assert_eq!(stats.keys_read, 1);
        assert_eq!(stats.bytes_read, 30);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.cache_hit_ratio(), 0.5);
    }
}
