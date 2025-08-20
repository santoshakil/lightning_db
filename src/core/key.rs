use smallvec::SmallVec;
use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

/// Zero-copy key type that avoids heap allocation for small keys
///
/// Keys up to 32 bytes are stored inline on the stack.
/// Larger keys use heap allocation.
pub type SmallKey = SmallVec<[u8; 32]>;

/// Extension trait for SmallKey operations
pub trait SmallKeyExt {
    fn from_slice(slice: &[u8]) -> Self;
    fn as_slice(&self) -> &[u8];
}

impl SmallKeyExt for SmallKey {
    #[inline]
    fn from_slice(slice: &[u8]) -> Self {
        SmallVec::from_slice(slice)
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

/// A key wrapper that provides zero-copy semantics for database operations
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(SmallKey);

impl Key {
    /// Create a new key from a byte slice
    #[inline]
    pub fn new(data: &[u8]) -> Self {
        Key(SmallKey::from_slice(data))
    }

    /// Create a key from owned bytes
    #[inline]
    pub fn from_vec(data: Vec<u8>) -> Self {
        Key(SmallKey::from_vec(data))
    }

    /// Get the key as a byte slice
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get the length of the key
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the key is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Check if this key is stored inline (no heap allocation)
    #[inline]
    pub fn is_inline(&self) -> bool {
        !self.0.spilled()
    }

    /// Convert to owned `Vec<u8>`
    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl Deref for Key {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for Key {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Borrow<[u8]> for Key {
    #[inline]
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Key {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        Key::from_vec(vec)
    }
}

impl From<&[u8]> for Key {
    #[inline]
    fn from(slice: &[u8]) -> Self {
        Key::new(slice)
    }
}

impl From<&str> for Key {
    #[inline]
    fn from(s: &str) -> Self {
        Key::new(s.as_bytes())
    }
}

impl From<String> for Key {
    #[inline]
    fn from(s: String) -> Self {
        Key::from_vec(s.into_bytes())
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({:?})", String::from_utf8_lossy(&self.0))
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(&self.0))
    }
}

/// Zero-copy key comparator for B+Tree operations
pub struct KeyComparator;

impl KeyComparator {
    #[inline]
    pub fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    #[inline]
    pub fn compare_key(a: &Key, b: &Key) -> std::cmp::Ordering {
        a.as_bytes().cmp(b.as_bytes())
    }
}

/// Batch key builder for efficient multi-key operations
pub struct KeyBatch {
    keys: Vec<Key>,
    total_size: usize,
}

impl KeyBatch {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            total_size: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            total_size: 0,
        }
    }

    #[inline]
    pub fn add(&mut self, key: impl Into<Key>) {
        let key = key.into();
        self.total_size += key.len();
        self.keys.push(key);
    }

    #[inline]
    pub fn keys(&self) -> &[Key] {
        &self.keys
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[inline]
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    #[inline]
    pub fn clear(&mut self) {
        self.keys.clear();
        self.total_size = 0;
    }
}

impl Default for KeyBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_key_inline() {
        let small_data = b"hello world";
        let key = Key::new(small_data);

        assert!(key.is_inline());
        assert_eq!(key.as_bytes(), small_data);
        assert_eq!(key.len(), small_data.len());
    }

    #[test]
    fn test_large_key_heap() {
        let large_data = vec![b'x'; 64]; // Larger than inline capacity
        let key = Key::from_vec(large_data.clone());

        assert!(!key.is_inline());
        assert_eq!(key.as_bytes(), &large_data);
        assert_eq!(key.len(), large_data.len());
    }

    #[test]
    fn test_key_conversions() {
        // From string
        let key1 = Key::from("hello");
        assert_eq!(key1.as_bytes(), b"hello");

        // From Vec
        let key2 = Key::from(vec![1, 2, 3]);
        assert_eq!(key2.as_bytes(), &[1, 2, 3]);

        // From slice
        let key3 = Key::from(&[4, 5, 6][..]);
        assert_eq!(key3.as_bytes(), &[4, 5, 6]);
    }

    #[test]
    fn test_key_comparison() {
        let key1 = Key::from("aaa");
        let key2 = Key::from("bbb");
        let key3 = Key::from("aaa");

        assert!(key1 < key2);
        assert_eq!(key1, key3);
        assert!(key2 > key1);
    }

    #[test]
    fn test_key_batch() {
        let mut batch = KeyBatch::new();

        batch.add("key1");
        batch.add(b"key2".as_ref());
        batch.add(vec![1, 2, 3]);

        assert_eq!(batch.len(), 3);
        assert_eq!(batch.total_size(), 4 + 4 + 3);
        assert!(!batch.is_empty());

        batch.clear();
        assert!(batch.is_empty());
        assert_eq!(batch.total_size(), 0);
    }
}
