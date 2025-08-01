//! Bloom Filter Implementation
//!
//! A space-efficient probabilistic data structure used to test whether an element
//! is a member of a set. Used in SSTables to quickly determine if a key doesn't exist.

use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Bloom filter for efficient membership testing
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: usize,
    num_elements: usize,
}

impl BloomFilter {
    /// Create a new bloom filter
    pub fn new(num_bits: usize, num_hashes: usize) -> Self {
        let byte_size = (num_bits + 7) / 8;
        Self {
            bits: vec![0; byte_size],
            num_bits,
            num_hashes,
            num_elements: 0,
        }
    }

    /// Add an element to the bloom filter
    pub fn add(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let bit_index = self.hash(key, i) % self.num_bits;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;
            self.bits[byte_index] |= 1 << bit_offset;
        }
        self.num_elements += 1;
    }

    /// Check if an element might be in the set
    pub fn may_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let bit_index = self.hash(key, i) % self.num_bits;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;
            if (self.bits[byte_index] & (1 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    /// Calculate hash for a key with a given seed
    fn hash(&self, key: &[u8], seed: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize
    }

    /// Get the false positive rate
    pub fn false_positive_rate(&self) -> f64 {
        let m = self.num_bits as f64;
        let n = self.num_elements as f64;
        let k = self.num_hashes as f64;
        
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Get the number of elements added
    pub fn num_elements(&self) -> usize {
        self.num_elements
    }

    /// Get the size in bytes
    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }

    /// Clear the bloom filter
    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.num_elements = 0;
    }

    /// Merge another bloom filter into this one
    pub fn merge(&mut self, other: &BloomFilter) -> Result<(), &'static str> {
        if self.num_bits != other.num_bits || self.num_hashes != other.num_hashes {
            return Err("Bloom filters must have same parameters to merge");
        }

        for i in 0..self.bits.len() {
            self.bits[i] |= other.bits[i];
        }
        self.num_elements += other.num_elements;

        Ok(())
    }
}

/// Bloom filter builder for creating optimal bloom filters
pub struct BloomFilterBuilder {
    elements: Vec<Vec<u8>>,
    target_elements: usize,
    false_positive_rate: f64,
}

impl BloomFilterBuilder {
    /// Create a new bloom filter builder
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        Self {
            elements: Vec::with_capacity(expected_elements),
            target_elements: expected_elements,
            false_positive_rate,
        }
    }

    /// Add an element to the builder
    pub fn add(&mut self, key: &[u8]) {
        self.elements.push(key.to_vec());
    }

    /// Build the bloom filter with optimal parameters
    pub fn build(self) -> BloomFilter {
        let n = self.elements.len().max(self.target_elements);
        let (num_bits, num_hashes) = Self::optimal_parameters(n, self.false_positive_rate);
        
        let mut filter = BloomFilter::new(num_bits, num_hashes);
        for element in self.elements {
            filter.add(&element);
        }
        
        filter
    }

    /// Calculate optimal bloom filter parameters
    fn optimal_parameters(n: usize, p: f64) -> (usize, usize) {
        // Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let m = (-(n as f64) * p.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        
        // Optimal number of hash functions: k = (m / n) * ln(2)
        let k = ((m as f64 / n as f64) * 2.0_f64.ln()).round() as usize;
        
        (m.max(64), k.max(1)) // Minimum 64 bits and 1 hash
    }
}

/// Counting bloom filter that supports deletion
#[derive(Debug, Clone)]
pub struct CountingBloomFilter {
    counters: Vec<u8>,
    num_counters: usize,
    num_hashes: usize,
    num_elements: usize,
}

impl CountingBloomFilter {
    /// Create a new counting bloom filter
    pub fn new(num_counters: usize, num_hashes: usize) -> Self {
        Self {
            counters: vec![0; num_counters],
            num_counters,
            num_hashes,
            num_elements: 0,
        }
    }

    /// Add an element
    pub fn add(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let index = self.hash(key, i) % self.num_counters;
            if self.counters[index] < 255 {
                self.counters[index] += 1;
            }
        }
        self.num_elements += 1;
    }

    /// Remove an element
    pub fn remove(&mut self, key: &[u8]) {
        let mut all_present = true;
        
        // First check if all bits are set
        for i in 0..self.num_hashes {
            let index = self.hash(key, i) % self.num_counters;
            if self.counters[index] == 0 {
                all_present = false;
                break;
            }
        }

        // Only decrement if all were present
        if all_present {
            for i in 0..self.num_hashes {
                let index = self.hash(key, i) % self.num_counters;
                if self.counters[index] > 0 {
                    self.counters[index] -= 1;
                }
            }
            if self.num_elements > 0 {
                self.num_elements -= 1;
            }
        }
    }

    /// Check if an element might be in the set
    pub fn may_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let index = self.hash(key, i) % self.num_counters;
            if self.counters[index] == 0 {
                return false;
            }
        }
        true
    }

    /// Calculate hash for a key with a given seed
    fn hash(&self, key: &[u8], seed: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize
    }

    /// Convert to a regular bloom filter
    pub fn to_bloom_filter(&self) -> BloomFilter {
        let num_bits = self.num_counters;
        let mut filter = BloomFilter::new(num_bits, self.num_hashes);
        
        // Set bits where counters > 0
        for i in 0..self.num_counters {
            if self.counters[i] > 0 {
                let byte_index = i / 8;
                let bit_offset = i % 8;
                if byte_index < filter.bits.len() {
                    filter.bits[byte_index] |= 1 << bit_offset;
                }
            }
        }
        
        filter.num_elements = self.num_elements;
        filter
    }
}

/// Scalable bloom filter that grows as needed
pub struct ScalableBloomFilter {
    filters: Vec<BloomFilter>,
    false_positive_rate: f64,
    growth_factor: usize,
    current_capacity: usize,
}

impl ScalableBloomFilter {
    /// Create a new scalable bloom filter
    pub fn new(initial_capacity: usize, false_positive_rate: f64) -> Self {
        let (num_bits, num_hashes) = BloomFilterBuilder::optimal_parameters(
            initial_capacity,
            false_positive_rate * 0.5, // Tighter bound for first filter
        );
        
        let mut filters = Vec::new();
        filters.push(BloomFilter::new(num_bits, num_hashes));
        
        Self {
            filters,
            false_positive_rate,
            growth_factor: 2,
            current_capacity: initial_capacity,
        }
    }

    /// Add an element
    pub fn add(&mut self, key: &[u8]) {
        // Check if we need a new filter
        if let Some(last_filter) = self.filters.last() {
            if last_filter.num_elements() >= self.current_capacity {
                self.add_new_filter();
            }
        }

        // Add to the last filter
        if let Some(last_filter) = self.filters.last_mut() {
            last_filter.add(key);
        }
    }

    /// Check if an element might be in the set
    pub fn may_contain(&self, key: &[u8]) -> bool {
        for filter in &self.filters {
            if filter.may_contain(key) {
                return true;
            }
        }
        false
    }

    /// Add a new filter with increased capacity
    fn add_new_filter(&mut self) {
        self.current_capacity *= self.growth_factor;
        
        // Calculate tighter false positive rate for new filter
        let filter_index = self.filters.len();
        let filter_fp_rate = self.false_positive_rate * (0.5_f64).powi(filter_index as i32 + 1);
        
        let (num_bits, num_hashes) = BloomFilterBuilder::optimal_parameters(
            self.current_capacity,
            filter_fp_rate,
        );
        
        self.filters.push(BloomFilter::new(num_bits, num_hashes));
    }

    /// Get total number of elements
    pub fn num_elements(&self) -> usize {
        self.filters.iter().map(|f| f.num_elements()).sum()
    }

    /// Get total size in bytes
    pub fn size_bytes(&self) -> usize {
        self.filters.iter().map(|f| f.size_bytes()).sum()
    }

    /// Get the effective false positive rate
    pub fn effective_false_positive_rate(&self) -> f64 {
        // P(false positive) = 1 - ∏(1 - p_i) for each filter
        let mut combined_rate = 1.0;
        for filter in &self.filters {
            combined_rate *= 1.0 - filter.false_positive_rate();
        }
        1.0 - combined_rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(1000, 3);
        
        // Add some elements
        filter.add(b"hello");
        filter.add(b"world");
        filter.add(b"rust");
        
        // Test membership
        assert!(filter.may_contain(b"hello"));
        assert!(filter.may_contain(b"world"));
        assert!(filter.may_contain(b"rust"));
        
        // These might have false positives but probably won't
        assert!(!filter.may_contain(b"python"));
        assert!(!filter.may_contain(b"java"));
    }

    #[test]
    fn test_bloom_filter_builder() {
        let mut builder = BloomFilterBuilder::new(1000, 0.01);
        
        for i in 0..100 {
            builder.add(&format!("key{}", i).into_bytes());
        }
        
        let filter = builder.build();
        
        // All added elements should be found
        for i in 0..100 {
            assert!(filter.may_contain(&format!("key{}", i).into_bytes()));
        }
        
        // False positive rate should be close to target
        let fp_rate = filter.false_positive_rate();
        assert!(fp_rate < 0.02); // Should be close to 0.01
    }

    #[test]
    fn test_counting_bloom_filter() {
        let mut filter = CountingBloomFilter::new(1000, 3);
        
        // Add and remove elements
        filter.add(b"test");
        assert!(filter.may_contain(b"test"));
        
        filter.remove(b"test");
        // Might still return true due to hash collisions
        
        // Add multiple times
        filter.add(b"multi");
        filter.add(b"multi");
        assert!(filter.may_contain(b"multi"));
        
        filter.remove(b"multi");
        assert!(filter.may_contain(b"multi")); // Still there once
        
        filter.remove(b"multi");
        // Now might be gone (depending on collisions)
    }

    #[test]
    fn test_scalable_bloom_filter() {
        let mut filter = ScalableBloomFilter::new(100, 0.01);
        
        // Add many elements to trigger scaling
        for i in 0..500 {
            filter.add(&format!("key{}", i).into_bytes());
        }
        
        // All elements should be found
        for i in 0..500 {
            assert!(filter.may_contain(&format!("key{}", i).into_bytes()));
        }
        
        // Should have multiple filters
        assert!(filter.filters.len() > 1);
        
        // Total elements
        assert_eq!(filter.num_elements(), 500);
    }

    #[test]
    fn test_bloom_filter_merge() {
        let mut filter1 = BloomFilter::new(1000, 3);
        let mut filter2 = BloomFilter::new(1000, 3);
        
        // Add different elements
        filter1.add(b"a");
        filter1.add(b"b");
        
        filter2.add(b"c");
        filter2.add(b"d");
        
        // Merge
        filter1.merge(&filter2).unwrap();
        
        // Should contain all elements
        assert!(filter1.may_contain(b"a"));
        assert!(filter1.may_contain(b"b"));
        assert!(filter1.may_contain(b"c"));
        assert!(filter1.may_contain(b"d"));
        
        assert_eq!(filter1.num_elements(), 4);
    }
}