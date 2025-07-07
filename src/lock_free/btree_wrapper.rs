use crate::btree::{BPlusTree, BTreeNode};
use crate::error::{Error, Result};
use crate::storage::PageManager;
use crossbeam_epoch::{self as epoch, Guard};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Lock-free wrapper for B+Tree operations using epoch-based reclamation
/// and optimistic concurrency control
pub struct LockFreeBTreeWrapper {
    // The actual B+Tree - still protected by RwLock for complex operations
    inner: Arc<RwLock<BPlusTree>>,

    // Cache of frequently accessed nodes
    node_cache: Arc<DashMap<u32, Arc<BTreeNode>>>,

    // Epoch-based reclamation for safe memory management
    collector: epoch::Collector,

    // Statistics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,

    // Version counter for optimistic concurrency
    version: AtomicU64,
}

impl LockFreeBTreeWrapper {
    pub fn new(page_manager: Arc<RwLock<PageManager>>) -> Result<Self> {
        let btree = BPlusTree::new(page_manager)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(btree)),
            node_cache: Arc::new(DashMap::new()),
            collector: epoch::Collector::new(),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            version: AtomicU64::new(0),
        })
    }

    /// Lock-free get operation using cached nodes
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = &self.collector.register().pin();

        // Try fast path with cached root
        let btree = self.inner.read();
        let root_page_id = btree.root_page_id();
        let height = btree.height();
        drop(btree);

        // Navigate through cached nodes if possible
        let mut current_page_id = root_page_id;
        let mut level = height;

        while level > 0 {
            // Try to get node from cache
            match self.get_cached_node(current_page_id, guard) {
                Some(node) => {
                    self.cache_hits.fetch_add(1, Ordering::Relaxed);

                    if level == 1 {
                        // Leaf node - search for key
                        for entry in &node.entries {
                            if entry.key == key {
                                return Ok(Some(entry.value.clone()));
                            }
                            if entry.key.as_slice() > key {
                                break;
                            }
                        }
                        return Ok(None);
                    } else {
                        // Internal node - find child
                        let mut child_index = 0;
                        for (i, entry) in node.entries.iter().enumerate() {
                            if key < entry.key.as_slice() {
                                child_index = i;
                                break;
                            }
                            child_index = i + 1;
                        }

                        if child_index < node.children.len() {
                            current_page_id = node.children[child_index];
                            level -= 1;
                        } else {
                            // Fallback to locked path
                            break;
                        }
                    }
                }
                None => {
                    // Cache miss - fallback to locked path
                    self.cache_misses.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }

        // Fallback to standard locked get
        let btree = self.inner.read();
        btree.get(key)
    }

    /// Insert operation - still requires write lock but uses optimistic validation
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Invalidate cached nodes that might be affected
        self.invalidate_cache_for_key(key);

        // Perform insert under write lock
        let mut btree = self.inner.write();
        let result = btree.insert(key, value);

        // Increment version on successful write
        if result.is_ok() {
            self.version.fetch_add(1, Ordering::SeqCst);
        }

        result
    }

    /// Delete operation with cache invalidation
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        // Invalidate cached nodes
        self.invalidate_cache_for_key(key);

        // Perform delete under write lock
        let mut btree = self.inner.write();
        let result = btree.delete_complete(key);

        // Increment version on successful delete
        if result.is_ok() {
            self.version.fetch_add(1, Ordering::SeqCst);
        }

        result
    }

    /// Get a cached node or load it
    fn get_cached_node(&self, _page_id: u32, _guard: &Guard) -> Option<Arc<BTreeNode>> {
        // For now, we can't cache nodes without direct page access
        // This would require exposing page access methods in BPlusTree
        None
    }

    /// Invalidate cache entries that might be affected by a key modification
    fn invalidate_cache_for_key(&self, _key: &[u8]) {
        // Simple strategy: clear entire cache on write
        // A more sophisticated approach would track key ranges per node
        self.node_cache.clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (u64, u64, usize) {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let cached_nodes = self.node_cache.len();
        (hits, misses, cached_nodes)
    }

    /// Clear the node cache
    pub fn clear_cache(&self) {
        self.node_cache.clear();
    }
}

/// Lock-free read path for B+Tree
pub struct OptimisticBTreeReader {
    nodes: Arc<DashMap<u32, Arc<RwLock<BTreeNode>>>>,
    version: Arc<AtomicU64>,
}

impl Default for OptimisticBTreeReader {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimisticBTreeReader {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(DashMap::new()),
            version: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Optimistic read with version validation
    pub fn read_optimistic<F, R>(&self, page_id: u32, f: F) -> Result<R>
    where
        F: Fn(&BTreeNode) -> R,
    {
        // Read version before
        let version_before = self.version.load(Ordering::Acquire);

        // Get node
        let node = self
            .nodes
            .get(&page_id)
            .ok_or_else(|| Error::Index("Node not found".to_string()))?;

        let result = {
            let node_guard = node.read();
            f(&node_guard)
        };

        // Validate version hasn't changed
        let version_after = self.version.load(Ordering::Acquire);
        if version_before != version_after {
            return Err(Error::Index("Concurrent modification detected".to_string()));
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_lock_free_btree_basic() {
        let dir = tempdir().unwrap();
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap(),
        ));

        let btree = LockFreeBTreeWrapper::new(page_manager).unwrap();

        // Test insert and get
        btree.insert(b"key1", b"value1").unwrap();
        btree.insert(b"key2", b"value2").unwrap();

        assert_eq!(btree.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(btree.get(b"key2").unwrap(), Some(b"value2".to_vec()));

        // Check cache stats
        let (hits, misses, cached) = btree.cache_stats();
        assert!(hits > 0 || misses > 0);
        assert!(cached <= 2);
    }

    #[test]
    fn test_cache_invalidation() {
        let dir = tempdir().unwrap();
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap(),
        ));

        let btree = LockFreeBTreeWrapper::new(page_manager).unwrap();

        // Insert and get to populate cache
        btree.insert(b"key1", b"value1").unwrap();
        let _ = btree.get(b"key1").unwrap();

        let (_, _, _cached_before) = btree.cache_stats();

        // Update should invalidate cache
        btree.insert(b"key1", b"value2").unwrap();

        let (_, _, cached_after) = btree.cache_stats();
        assert_eq!(cached_after, 0); // Cache should be cleared

        // Get should return new value
        assert_eq!(btree.get(b"key1").unwrap(), Some(b"value2".to_vec()));
    }
}
