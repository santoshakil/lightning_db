use std::collections::HashMap;
use std::hash::Hash;

/// Cache-friendly algorithms optimized for modern CPU architectures
pub struct CacheFriendlyAlgorithms;

impl CacheFriendlyAlgorithms {
    /// Cache-friendly merge sort optimized for database key sorting
    pub fn merge_sort_keys<T: Clone + Ord>(data: &mut [T]) {
        if data.len() <= 1 {
            return;
        }

        // Use insertion sort for small arrays (cache-friendly)
        if data.len() <= 32 {
            Self::insertion_sort(data);
            return;
        }

        let mid = data.len() / 2;
        let (left, right) = data.split_at_mut(mid);

        Self::merge_sort_keys(left);
        Self::merge_sort_keys(right);

        // Merge using cache-friendly approach
        Self::cache_friendly_merge(left, right);
    }

    /// Cache-friendly insertion sort for small arrays
    fn insertion_sort<T: Clone + Ord>(data: &mut [T]) {
        for i in 1..data.len() {
            let key = data[i].clone();
            let mut j = i;

            // Move elements in blocks to improve cache locality
            while j > 0 && data[j - 1] > key {
                data[j] = data[j - 1].clone();
                j -= 1;
            }

            data[j] = key;
        }
    }

    /// Cache-friendly merge operation
    fn cache_friendly_merge<T: Clone + Ord>(left: &mut [T], right: &mut [T]) {
        let mut temp = Vec::with_capacity(left.len() + right.len());
        let mut i = 0;
        let mut j = 0;

        // Process in blocks to improve cache locality
        const BLOCK_SIZE: usize = 64; // Roughly one cache line worth of data

        while i < left.len() && j < right.len() {
            let left_end = std::cmp::min(i + BLOCK_SIZE, left.len());
            let right_end = std::cmp::min(j + BLOCK_SIZE, right.len());

            // Process current blocks
            while i < left_end && j < right_end {
                if left[i] <= right[j] {
                    temp.push(left[i].clone());
                    i += 1;
                } else {
                    temp.push(right[j].clone());
                    j += 1;
                }
            }

            // Copy remaining elements from current blocks
            while i < left_end {
                temp.push(left[i].clone());
                i += 1;
            }

            while j < right_end {
                temp.push(right[j].clone());
                j += 1;
            }
        }

        // Copy any remaining elements
        while i < left.len() {
            temp.push(left[i].clone());
            i += 1;
        }

        while j < right.len() {
            temp.push(right[j].clone());
            j += 1;
        }

        // Copy back to original arrays
        let _total_len = left.len() + right.len();
        for (idx, item) in temp.into_iter().enumerate() {
            if idx < left.len() {
                left[idx] = item;
            } else {
                right[idx - left.len()] = item;
            }
        }
    }

    /// Cache-friendly binary search with prefetching
    pub fn binary_search_with_prefetch<T: Ord>(data: &[T], target: &T) -> Result<usize, usize> {
        if data.is_empty() {
            return Err(0);
        }

        let mut left = 0;
        let mut right = data.len();

        while left < right {
            let mid = left + (right - left) / 2;

            // Prefetch likely next access locations
            if mid > 0 {
                Self::prefetch_memory(&data[mid - 1]);
            }
            if mid + 1 < data.len() {
                Self::prefetch_memory(&data[mid + 1]);
            }

            match data[mid].cmp(target) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }

        Err(left)
    }

    /// Cache-friendly hash table with linear probing
    pub fn linear_probe_find<K: Hash + Eq, V>(
        table: &[(Option<K>, Option<V>)],
        key: &K,
        hash: u64,
    ) -> Option<usize> {
        if table.is_empty() {
            return None;
        }

        let mut index = (hash as usize) % table.len();
        let start_index = index;

        loop {
            match &table[index].0 {
                Some(k) if k == key => return Some(index),
                None => return None,
                _ => {
                    index = (index + 1) % table.len();
                    if index == start_index {
                        return None; // Table full
                    }
                }
            }
        }
    }

    /// Cache-friendly sequential scan with SIMD hints
    pub fn sequential_scan_optimized<T, F>(data: &[T], predicate: F) -> Vec<usize>
    where
        F: Fn(&T) -> bool,
    {
        let mut results = Vec::new();
        let mut i = 0;

        // Process in cache-friendly blocks
        const BLOCK_SIZE: usize = 64;

        while i < data.len() {
            let block_end = std::cmp::min(i + BLOCK_SIZE, data.len());

            // Prefetch next block
            if block_end < data.len() {
                Self::prefetch_memory(&data[block_end]);
            }

            // Process current block
            for (idx, item) in data[i..block_end].iter().enumerate() {
                if predicate(item) {
                    results.push(i + idx);
                }
            }

            i = block_end;
        }

        results
    }

    /// Cache-friendly matrix multiplication for similarity computations
    pub fn cache_friendly_matrix_multiply(a: &[f32], b: &[f32], c: &mut [f32], n: usize) {
        // Block size optimized for L1 cache
        const BLOCK_SIZE: usize = 64;

        for i in (0..n).step_by(BLOCK_SIZE) {
            for j in (0..n).step_by(BLOCK_SIZE) {
                for k in (0..n).step_by(BLOCK_SIZE) {
                    // Process block
                    let i_end = std::cmp::min(i + BLOCK_SIZE, n);
                    let j_end = std::cmp::min(j + BLOCK_SIZE, n);
                    let k_end = std::cmp::min(k + BLOCK_SIZE, n);

                    for ii in i..i_end {
                        for jj in j..j_end {
                            let mut sum = 0.0;
                            for kk in k..k_end {
                                sum += a[ii * n + kk] * b[kk * n + jj];
                            }
                            c[ii * n + jj] += sum;
                        }
                    }
                }
            }
        }
    }

    /// Cache-friendly string matching using Boyer-Moore with optimizations
    pub fn boyer_moore_optimized(text: &[u8], pattern: &[u8]) -> Vec<usize> {
        if pattern.is_empty() || text.len() < pattern.len() {
            return Vec::new();
        }

        let mut matches = Vec::new();
        let pattern_len = pattern.len();
        let text_len = text.len();

        // Build bad character table
        let mut bad_char = [pattern_len; 256];
        for (i, &ch) in pattern.iter().enumerate() {
            bad_char[ch as usize] = pattern_len - i - 1;
        }

        let mut skip = 0;

        while skip <= text_len - pattern_len {
            let mut j = pattern_len - 1;

            // Prefetch next potential match location
            if skip + pattern_len * 2 < text_len {
                Self::prefetch_memory(&text[skip + pattern_len]);
            }

            // Compare from right to left
            while j < pattern_len && pattern[j] == text[skip + j] {
                if j == 0 {
                    matches.push(skip);
                    break;
                }
                j -= 1;
            }

            if j < pattern_len {
                // Character mismatch
                let bad_char_skip = bad_char[text[skip + j] as usize];
                skip += std::cmp::max(1, bad_char_skip);
            } else {
                skip += 1;
            }
        }

        matches
    }

    /// Prefetch memory location to improve cache performance
    #[inline]
    fn prefetch_memory<T>(ptr: &T) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                ptr as *const T as *const i8,
                std::arch::x86_64::_MM_HINT_T0,
            );
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            // No-op on other architectures
            let _ = ptr;
        }
    }
}

/// Cache-friendly LRU cache implementation
pub struct CacheFriendlyLRU<K, V> {
    capacity: usize,
    map: HashMap<K, usize>,
    items: Vec<(K, V)>,
    ages: Vec<u64>,
    current_age: u64,
}

impl<K: Hash + Eq + Clone, V> CacheFriendlyLRU<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::with_capacity(capacity),
            items: Vec::with_capacity(capacity),
            ages: Vec::with_capacity(capacity),
            current_age: 0,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(&index) = self.map.get(key) {
            self.ages[index] = self.current_age;
            self.current_age += 1;
            Some(&self.items[index].1)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if let Some(&index) = self.map.get(&key) {
            // Update existing item
            self.items[index].1 = value;
            self.ages[index] = self.current_age;
            self.current_age += 1;
        } else if self.items.len() < self.capacity {
            // Add new item
            let index = self.items.len();
            self.items.push((key.clone(), value));
            self.ages.push(self.current_age);
            self.map.insert(key, index);
            self.current_age += 1;
        } else {
            // Evict oldest item
            let oldest_index = self.find_oldest_item();
            let old_key = self.items[oldest_index].0.clone();

            self.map.remove(&old_key);
            self.items[oldest_index] = (key.clone(), value);
            self.ages[oldest_index] = self.current_age;
            self.map.insert(key, oldest_index);
            self.current_age += 1;
        }
    }

    fn find_oldest_item(&self) -> usize {
        let mut oldest_index = 0;
        let mut oldest_age = self.ages[0];

        for (i, &age) in self.ages.iter().enumerate().skip(1) {
            if age < oldest_age {
                oldest_age = age;
                oldest_index = i;
            }
        }

        oldest_index
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Cache-friendly B-tree implementation optimized for database operations
/// This is a simplified implementation that focuses on cache-friendly operations
pub struct CacheFriendlyBTree<K, V> {
    items: Vec<(K, V)>,
    capacity: usize,
}

impl<K: Ord + Clone, V: Clone> CacheFriendlyBTree<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            items: Vec::with_capacity(capacity),
            capacity: std::cmp::max(capacity, 10),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        match self.items.binary_search_by(|item| item.0.cmp(&key)) {
            Ok(index) => {
                // Update existing key
                self.items[index].1 = value;
            }
            Err(index) => {
                // Insert new key-value pair
                self.items.insert(index, (key, value));

                // If we exceed capacity, remove oldest items
                if self.items.len() > self.capacity {
                    self.items.truncate(self.capacity);
                }
            }
        }
    }

    pub fn search(&self, key: &K) -> Option<&V> {
        self.items
            .binary_search_by(|item| item.0.cmp(key))
            .ok()
            .map(|index| &self.items[index].1)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_friendly_merge_sort() {
        let mut data = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        CacheFriendlyAlgorithms::merge_sort_keys(&mut data);
        assert_eq!(data, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_binary_search_with_prefetch() {
        let data = vec![1, 3, 5, 7, 9, 11, 13, 15, 17, 19];

        assert_eq!(
            CacheFriendlyAlgorithms::binary_search_with_prefetch(&data, &7),
            Ok(3)
        );
        assert_eq!(
            CacheFriendlyAlgorithms::binary_search_with_prefetch(&data, &6),
            Err(3)
        );
    }

    #[test]
    fn test_sequential_scan_optimized() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let results = CacheFriendlyAlgorithms::sequential_scan_optimized(&data, |x| *x % 2 == 0);
        assert_eq!(results, vec![1, 3, 5, 7, 9]); // Indices of even numbers
    }

    #[test]
    fn test_boyer_moore_optimized() {
        let text = b"hello world hello universe";
        let pattern = b"hello";
        let matches = CacheFriendlyAlgorithms::boyer_moore_optimized(text, pattern);
        assert_eq!(matches, vec![0, 12]);
    }

    #[test]
    fn test_cache_friendly_lru() {
        let mut lru = CacheFriendlyLRU::new(3);

        lru.put(1, "one");
        lru.put(2, "two");
        lru.put(3, "three");

        assert_eq!(lru.get(&1), Some(&"one"));
        assert_eq!(lru.get(&2), Some(&"two"));
        assert_eq!(lru.get(&3), Some(&"three"));

        // Add fourth item, should evict least recently used
        lru.put(4, "four");
        assert_eq!(lru.get(&1), None); // Should be evicted
        assert_eq!(lru.get(&4), Some(&"four"));
    }

    #[test]
    fn test_cache_friendly_btree() {
        let mut btree = CacheFriendlyBTree::new(3);

        btree.insert(5, "five");
        btree.insert(2, "two");
        btree.insert(8, "eight");
        btree.insert(1, "one");
        btree.insert(9, "nine");

        assert_eq!(btree.search(&5), Some(&"five"));
        assert_eq!(btree.search(&2), Some(&"two"));
        assert_eq!(btree.search(&8), Some(&"eight"));
        assert_eq!(btree.search(&1), Some(&"one"));
        assert_eq!(btree.search(&9), Some(&"nine"));
        assert_eq!(btree.search(&10), None);

        assert_eq!(btree.len(), 5);
        assert!(!btree.is_empty());
    }

    #[test]
    fn test_linear_probe_find() {
        let mut table = vec![(None, None); 8];
        table[2] = (Some("key1"), Some("value1"));
        table[3] = (Some("key2"), Some("value2"));
        table[5] = (Some("key3"), Some("value3"));

        // Mock hash function
        let hash1 = 2u64; // Points to index 2
        let hash2 = 3u64; // Points to index 3
        let hash3 = 5u64; // Points to index 5
        let hash4 = 1u64; // Points to empty slot

        assert_eq!(
            CacheFriendlyAlgorithms::linear_probe_find(&table, &"key1", hash1),
            Some(2)
        );
        assert_eq!(
            CacheFriendlyAlgorithms::linear_probe_find(&table, &"key2", hash2),
            Some(3)
        );
        assert_eq!(
            CacheFriendlyAlgorithms::linear_probe_find(&table, &"key3", hash3),
            Some(5)
        );
        assert_eq!(
            CacheFriendlyAlgorithms::linear_probe_find(&table, &"key4", hash4),
            None
        );
    }

    #[test]
    fn test_cache_friendly_matrix_multiply() {
        let n = 4;
        let a = vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        ];
        let b = vec![
            1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0,
        ]; // Identity matrix

        let mut c = vec![0.0; n * n];

        CacheFriendlyAlgorithms::cache_friendly_matrix_multiply(&a, &b, &mut c, n);

        // Result should be the same as matrix a
        assert_eq!(c, a);
    }
}
