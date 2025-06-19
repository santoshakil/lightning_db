use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// String pool for deduplicating common strings
#[derive(Debug, Clone)]
pub struct StringPool {
    pool: Arc<RwLock<StringPoolInner>>,
}

#[derive(Debug)]
struct StringPoolInner {
    strings: HashMap<String, u32>,
    reverse: HashMap<u32, String>,
    next_id: u32,
    max_size: usize,
    current_size: usize,
}

impl StringPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: Arc::new(RwLock::new(StringPoolInner {
                strings: HashMap::new(),
                reverse: HashMap::new(),
                next_id: 1, // 0 is reserved for "not found"
                max_size,
                current_size: 0,
            })),
        }
    }
    
    /// Intern a string and return its ID
    pub fn intern(&self, s: &str) -> u32 {
        let mut pool = self.pool.write().unwrap();
        
        if let Some(&id) = pool.strings.get(s) {
            return id;
        }
        
        // Check if we need to evict
        let string_size = s.len() + std::mem::size_of::<String>() + std::mem::size_of::<u32>() * 2;
        if pool.current_size + string_size > pool.max_size && !pool.strings.is_empty() {
            // Simple eviction: remove oldest (lowest ID)
            if let Some((&oldest_id, _)) = pool.reverse.iter().min_by_key(|(id, _)| *id) {
                if let Some(oldest_string) = pool.reverse.remove(&oldest_id) {
                    pool.strings.remove(&oldest_string);
                    pool.current_size -= oldest_string.len() + std::mem::size_of::<String>() + std::mem::size_of::<u32>() * 2;
                }
            }
        }
        
        let id = pool.next_id;
        pool.next_id = pool.next_id.wrapping_add(1);
        if pool.next_id == 0 {
            pool.next_id = 1; // Skip 0
        }
        
        pool.strings.insert(s.to_string(), id);
        pool.reverse.insert(id, s.to_string());
        pool.current_size += string_size;
        
        id
    }
    
    /// Get a string by its ID
    pub fn get(&self, id: u32) -> Option<String> {
        self.pool.read().unwrap().reverse.get(&id).cloned()
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
        let pool = self.pool.read().unwrap();
        StringPoolStats {
            total_strings: pool.strings.len(),
            current_size: pool.current_size,
            max_size: pool.max_size,
            next_id: pool.next_id,
        }
    }
    
    /// Clear the pool
    pub fn clear(&self) {
        let mut pool = self.pool.write().unwrap();
        pool.strings.clear();
        pool.reverse.clear();
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