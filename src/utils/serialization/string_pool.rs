use bytes::Bytes;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::sync::Arc;

/// String pool for deduplicating common strings
#[derive(Debug, Clone)]
pub struct StringPool {
    pool: Arc<RwLock<StringPoolInner>>,
}

#[derive(Debug)]
struct StringPoolInner {
    strings: FxHashMap<String, u32>,
    reverse: FxHashMap<u32, Bytes>,
    next_id: u32,
    max_size: usize,
    current_size: usize,
    // LRU tracking for better eviction
    access_order: SmallVec<[u32; 64]>,
}

impl StringPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: Arc::new(RwLock::new(StringPoolInner {
                strings: FxHashMap::default(),
                reverse: FxHashMap::default(),
                next_id: 1, // 0 is reserved for "not found"
                max_size,
                current_size: 0,
                access_order: SmallVec::new(),
            })),
        }
    }

    /// Intern a string and return its ID
    pub fn intern(&self, s: &str) -> u32 {
        let mut pool = self.pool.write();

        if let Some(&id) = pool.strings.get(s) {
            // Move to end of access order (LRU)
            if let Some(pos) = pool.access_order.iter().position(|&x| x == id) {
                pool.access_order.remove(pos);
            }
            pool.access_order.push(id);
            return id;
        }

        // Check if we need to evict using LRU
        let string_size = s.len() + std::mem::size_of::<String>() + std::mem::size_of::<u32>() * 2;
        if pool.current_size + string_size > pool.max_size && !pool.strings.is_empty() {
            // LRU eviction: remove least recently used
            if let Some(lru_id) = pool.access_order.first().copied() {
                if let Some(lru_bytes) = pool.reverse.remove(&lru_id) {
                    let lru_string = String::from_utf8_lossy(&lru_bytes);
                    pool.strings.remove(lru_string.as_ref());
                    pool.current_size -= lru_bytes.len()
                        + std::mem::size_of::<String>()
                        + std::mem::size_of::<u32>() * 2;
                    pool.access_order.remove(0);
                }
            }
        }

        let id = pool.next_id;
        pool.next_id = pool.next_id.wrapping_add(1);
        if pool.next_id == 0 {
            pool.next_id = 1; // Skip 0
        }

        let string_bytes = Bytes::from(s.to_string());
        pool.strings.insert(s.to_string(), id);
        pool.reverse.insert(id, string_bytes);
        pool.access_order.push(id);
        pool.current_size += string_size;

        id
    }

    /// Get a string by its ID
    pub fn get(&self, id: u32) -> Option<String> {
        let pool = self.pool.read();
        pool.reverse
            .get(&id)
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
    }

    /// Encode a string to bytes (ID-based)
    pub fn encode(&self, s: &str) -> Vec<u8> {
        let id = self.intern(s);
        id.to_le_bytes().to_vec()
    }

    /// Decode bytes to string
    pub fn decode(&self, bytes: &[u8]) -> Option<String> {
        if bytes.len() != 4 {
            return None;
        }

        let id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        self.get(id)
    }

    /// Get current statistics
    pub fn stats(&self) -> StringPoolStats {
        let pool = self.pool.read();
        StringPoolStats {
            total_strings: pool.strings.len(),
            current_size: pool.current_size,
            max_size: pool.max_size,
            next_id: pool.next_id,
        }
    }

    /// Clear the pool
    pub fn clear(&self) {
        let mut pool = self.pool.write();
        pool.strings.clear();
        pool.reverse.clear();
        pool.access_order.clear();
        pool.next_id = 1;
        pool.current_size = 0;
    }
}

#[derive(Debug, Clone)]
pub struct StringPoolStats {
    pub total_strings: usize,
    pub current_size: usize,
    pub max_size: usize,
    pub next_id: u32,
}

/// String encoder that uses a string pool
#[derive(Debug, Clone)]
pub struct PooledStringEncoder {
    pool: StringPool,
}

impl PooledStringEncoder {
    pub fn new(max_pool_size: usize) -> Self {
        Self {
            pool: StringPool::new(max_pool_size),
        }
    }

    pub fn encode(&self, s: &str) -> Vec<u8> {
        self.pool.encode(s)
    }

    pub fn decode(&self, bytes: &[u8]) -> Option<String> {
        self.pool.decode(bytes)
    }

    pub fn stats(&self) -> StringPoolStats {
        self.pool.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_pool() {
        let pool = StringPool::new(1024 * 1024); // 1MB

        // Test interning
        let id1 = pool.intern("hello");
        let id2 = pool.intern("world");
        let id3 = pool.intern("hello"); // Should return same ID as id1

        assert_eq!(id1, id3);
        assert_ne!(id1, id2);

        // Test retrieval
        assert_eq!(pool.get(id1), Some("hello".to_string()));
        assert_eq!(pool.get(id2), Some("world".to_string()));
        assert_eq!(pool.get(999), None);
    }

    #[test]
    fn test_string_encoding() {
        let pool = StringPool::new(1024);

        let encoded = pool.encode("test string");
        let decoded = pool.decode(&encoded);

        assert_eq!(decoded, Some("test string".to_string()));

        // Test invalid decode
        assert_eq!(pool.decode(&[1, 2, 3]), None); // Wrong size
        assert_eq!(pool.decode(&[0, 0, 0, 0]), None); // ID 0 is reserved
    }

    #[test]
    fn test_pooled_encoder() {
        let encoder = PooledStringEncoder::new(1024);

        let encoded1 = encoder.encode("repeated");
        let encoded2 = encoder.encode("repeated");

        // Same string should produce same encoding
        assert_eq!(encoded1, encoded2);

        let decoded = encoder.decode(&encoded1);
        assert_eq!(decoded, Some("repeated".to_string()));
    }

    #[test]
    fn test_eviction() {
        let pool = StringPool::new(100); // Very small pool

        // Add strings until we exceed the limit
        for i in 0..20 {
            pool.intern(&format!("string_{}", i));
        }

        let stats = pool.stats();
        assert!(stats.current_size <= stats.max_size);
    }
}
