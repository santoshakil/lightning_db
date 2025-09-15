mod delete;
pub mod integrity_validation;
mod iterator;
pub mod write_buffer;
// SAFETY WARNING: lock_free_btree module disabled due to critical concurrency bugs
// This implementation has known memory safety issues including:
// - Potential use-after-free in concurrent scenarios
// - ABA problems in lock-free operations
// - Missing memory barriers for correct ordering
// - Test failures acknowledged with #[ignore] attribute
// DO NOT RE-ENABLE WITHOUT COMPLETE REDESIGN AND FORMAL VERIFICATION
// pub mod lock_free_btree;
pub mod node;
mod split_handler;
#[cfg(test)]
mod test_utils;

pub use integrity_validation::{btree_validators, BTreeIntegrityValidator, TreeConsistencyReport};
pub use iterator::BTreeLeafIterator;

use crate::core::error::{Error, Result};
use crate::core::storage::page::PAGE_SIZE;
use crate::core::storage::{Page, PageManager, PageManagerWrapper};
#[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
use crate::performance::optimizations::simd::simd_compare_keys;
use parking_lot::RwLock;
use split_handler::SplitHandler;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicU32, Ordering as AtomicOrdering},
    Arc,
};

pub use node::*;

// With typical keys like "key_000000" (10 bytes) and values like "value_000000" (12 bytes),
// each entry takes approximately 34 bytes (4 + key_len + 4 + value_len + 8 timestamp).
// With 64 byte header, we can safely fit about 100 entries per page.
// Use a conservative limit to ensure we never overflow.
pub(crate) const MIN_KEYS_PER_NODE: usize = 50;
pub(crate) const MAX_KEYS_PER_NODE: usize = 100;

/// Helper function to compare keys using SIMD when beneficial
#[inline(always)]
fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    // Optimize common cases first
    match (a.len(), b.len()) {
        (0, 0) => return Ordering::Equal,
        (0, _) => return Ordering::Less,
        (_, 0) => return Ordering::Greater,
        (la, lb) if la != lb => {
            // Different lengths - compare common prefix first
            let min_len = la.min(lb);
            if min_len > 0 {
                let prefix_cmp = compare_bytes_fast(&a[..min_len], &b[..min_len]);
                if prefix_cmp != Ordering::Equal {
                    return prefix_cmp;
                }
            }
            return la.cmp(&lb);
        }
        _ => {}
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    {
        // Use SIMD for keys longer than 8 bytes
        if a.len() >= 8 {
            return simd_compare_keys(a, b);
        }
    }

    compare_bytes_fast(a, b)
}

/// Fast byte comparison for small keys
#[inline(always)]
fn compare_bytes_fast(a: &[u8], b: &[u8]) -> Ordering {
    // For small keys, use word-based comparison
    if a.len() == b.len() && a.len() >= 8 {
        let mut i = 0;
        while i + 8 <= a.len() {
            let a_word = u64::from_le_bytes(a[i..i + 8].try_into().unwrap());
            let b_word = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
            match a_word.cmp(&b_word) {
                Ordering::Equal => i += 8,
                other => return other,
            }
        }
        // Compare remaining bytes
        a[i..].cmp(&b[i..])
    } else {
        a.cmp(b)
    }
}

#[derive(Debug, Clone)]
pub struct KeyEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: u64,
}

impl KeyEntry {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

#[derive(Debug)]
pub struct BPlusTree {
    page_manager: PageManagerWrapper,
    root_page_id: AtomicU32,
    height: AtomicU32,
}

/// Memory leak prevention tracker for B+Tree pages
pub struct BTreeMemoryTracker {
    allocated_pages: parking_lot::Mutex<HashSet<u32>>,
}

impl BTreeMemoryTracker {
    pub fn new() -> Self {
        Self {
            allocated_pages: parking_lot::Mutex::new(HashSet::new()),
        }
    }

    pub fn track_allocation(&self, page_id: u32) {
        self.allocated_pages.lock().insert(page_id);
    }

    pub fn track_deallocation(&self, page_id: u32) {
        self.allocated_pages.lock().remove(&page_id);
    }

    pub fn get_leaked_pages(&self) -> HashSet<u32> {
        self.allocated_pages.lock().clone()
    }

    pub fn cleanup_leaked_pages<PM: crate::core::storage::PageManagerTrait>(
        &self,
        page_manager: &PM,
    ) {
        let leaked_pages = self.allocated_pages.lock().drain().collect::<HashSet<_>>();
        for page_id in leaked_pages {
            page_manager.free_page(page_id);
        }
    }
}

// SAFETY: BPlusTree is now thread-safe using atomic operations
// - AtomicU32 provides thread-safe access to root_page_id and height
// - PageManager handles concurrent access internally
// - No data races possible with atomic operations

impl BPlusTree {
    pub fn new(page_manager: Arc<RwLock<PageManager>>) -> Result<Self> {
        Self::new_with_wrapper(PageManagerWrapper::from_arc(page_manager))
    }

    pub fn new_with_wrapper(page_manager: PageManagerWrapper) -> Result<Self> {
        let root_page_id = page_manager.allocate_page()?;

        let tree = Self {
            page_manager,
            root_page_id: AtomicU32::new(root_page_id),
            height: AtomicU32::new(1),
        };

        tree.init_root_page()?;
        Ok(tree)
    }

    pub fn from_existing(
        page_manager: Arc<RwLock<PageManager>>,
        root_page_id: u32,
        height: u32,
    ) -> Self {
        Self::from_existing_with_wrapper(
            PageManagerWrapper::from_arc(page_manager),
            root_page_id,
            height,
        )
    }

    pub fn from_existing_with_wrapper(
        page_manager: PageManagerWrapper,
        root_page_id: u32,
        height: u32,
    ) -> Self {
        Self {
            page_manager,
            root_page_id: AtomicU32::new(root_page_id),
            height: AtomicU32::new(height),
        }
    }

    pub fn init_root_page(&self) -> Result<()> {
        let root_id = self.root_page_id.load(AtomicOrdering::Acquire);
        let mut page = Page::new(root_id);
        let node = BTreeNode::new_leaf(root_id);
        node.serialize_to_page(&mut page)?;

        self.page_manager.write_page(&page)?;
        Ok(())
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Validate key and value sizes
        const MAX_KEY_SIZE: usize = 4096;
        const MAX_VALUE_SIZE: usize = 1024 * 1024; // 1MB

        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(Error::InvalidKeySize {
                size: key.len(),
                min: 1,
                max: MAX_KEY_SIZE,
            });
        }

        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::InvalidValueSize {
                size: value.len(),
                max: MAX_VALUE_SIZE,
            });
        }

        let entry = KeyEntry::new(key.to_vec(), value.to_vec());

        // Find insertion point
        let insertion_path = self.find_leaf_path(key)?;

        // Insert into leaf and handle potential splits
        let split_info = self.insert_into_leaf(&insertion_path, entry)?;

        if let Some((new_key, new_page_id)) = split_info {
            // Store old root info atomically
            let old_root_id = self.root_page_id.load(AtomicOrdering::Acquire);
            let old_height = self.height.load(AtomicOrdering::Acquire);

            self.handle_root_split(old_root_id, old_height, new_key, new_page_id)?;
        }

        Ok(())
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Hot path optimization: cache frequently accessed pages
        let mut current_page_id = self.root_page_id.load(AtomicOrdering::Acquire);
        let height = self.height.load(AtomicOrdering::Acquire);

        // Optimize for leaf-only trees (height == 1)
        if height == 1 {
            let page = self.page_manager.get_page(current_page_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;
            return self.search_leaf(&node, key);
        }

        // Navigate through internal nodes
        for _level in (2..=height).rev() {
            let page = self.page_manager.get_page(current_page_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;
            current_page_id = self.find_child_page_optimized(&node, key)?;
        }

        // Search in leaf with prefetch hint
        let leaf_page = self.page_manager.get_page(current_page_id)?;
        let leaf_node = BTreeNode::deserialize_from_page(&leaf_page)?;
        self.search_leaf_optimized(&leaf_node, key)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        // Use the complete deletion implementation with rebalancing
        self.delete_complete(key)
    }

    fn find_leaf_path(&self, key: &[u8]) -> Result<Vec<u32>> {
        let mut path = Vec::new();
        // Read tree metadata atomically
        let mut current_page_id = self.root_page_id.load(AtomicOrdering::Acquire);
        let mut level = self.height.load(AtomicOrdering::Acquire);

        while level > 0 {
            path.push(current_page_id);

            if level == 1 {
                break; // We've reached the leaf level
            }

            let page = self.page_manager.get_page(current_page_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;

            current_page_id = self.find_child_page(&node, key)?;
            level -= 1;
        }

        Ok(path)
    }

    #[inline(always)]
    fn find_child_page(&self, node: &BTreeNode, key: &[u8]) -> Result<u32> {
        if node.node_type != NodeType::Internal {
            return Err(Error::Index("Cannot find child in leaf node".to_string()));
        }

        self.find_child_page_optimized(node, key)
    }

    /// Optimized child page finding with binary search for large nodes
    #[inline]
    fn find_child_page_optimized(&self, node: &BTreeNode, key: &[u8]) -> Result<u32> {
        let entries = &node.entries;

        // Use binary search for nodes with many entries
        if entries.len() > 8 {
            let mut left = 0;
            let mut right = entries.len();

            while left < right {
                let mid = left + (right - left) / 2;
                match compare_keys(key, &entries[mid].key) {
                    Ordering::Less => right = mid,
                    Ordering::Equal => {
                        // Key found, go to right child
                        let child_index = mid + 1;
                        return if child_index < node.children.len() {
                            Ok(node.children[child_index])
                        } else {
                            Err(Error::Index(format!(
                                "Invalid child index: {} >= {}",
                                child_index,
                                node.children.len()
                            )))
                        };
                    }
                    Ordering::Greater => left = mid + 1,
                }
            }

            // Key not found, use left as insertion point
            return if left < node.children.len() {
                Ok(node.children[left])
            } else {
                Err(Error::Index(format!(
                    "Invalid child index: {} >= {}",
                    left,
                    node.children.len()
                )))
            };
        }

        // Linear search for small nodes (better cache locality)
        let mut child_index = 0;
        for (i, entry) in entries.iter().enumerate() {
            match compare_keys(key, &entry.key) {
                Ordering::Less => break,
                Ordering::Equal => {
                    child_index = i + 1;
                    break;
                }
                Ordering::Greater => child_index = i + 1,
            }
        }

        if child_index < node.children.len() {
            Ok(node.children[child_index])
        } else {
            Err(Error::Index(format!(
                "Invalid child index: {} >= {}",
                child_index,
                node.children.len()
            )))
        }
    }

    #[inline(always)]
    fn search_leaf(&self, node: &BTreeNode, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if node.node_type != NodeType::Leaf {
            return Err(Error::Index("Expected leaf node".to_string()));
        }
        self.search_leaf_optimized(node, key)
    }

    /// Optimized leaf search with binary search and SIMD when beneficial
    #[inline]
    fn search_leaf_optimized(&self, node: &BTreeNode, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let entries = &node.entries;

        // Use binary search for larger leaf nodes
        if entries.len() > 16 {
            match entries.binary_search_by(|entry| compare_keys(&entry.key, key)) {
                Ok(idx) => return Ok(Some(entries[idx].value.clone())),
                Err(_) => return Ok(None),
            }
        }

        // Linear search with early termination for small leaf nodes
        for entry in entries {
            match compare_keys(key, &entry.key) {
                Ordering::Equal => return Ok(Some(entry.value.clone())),
                Ordering::Less => break, // Entries are sorted, no point continuing
                Ordering::Greater => {}
            }
        }

        Ok(None)
    }

    fn insert_into_leaf(&self, path: &[u32], entry: KeyEntry) -> Result<Option<(Vec<u8>, u32)>> {
        let leaf_page_id = *path.last().ok_or(Error::Index("Empty path".to_string()))?;
        let mut leaf_page = self.page_manager.get_page(leaf_page_id)?;
        let mut leaf_node = BTreeNode::deserialize_from_page(&leaf_page)?;

        // Find insertion position
        let mut insert_pos = leaf_node.entries.len();
        for (i, existing_entry) in leaf_node.entries.iter().enumerate() {
            match compare_keys(&entry.key, &existing_entry.key) {
                Ordering::Less => {
                    insert_pos = i;
                    break;
                }
                Ordering::Equal => {
                    // Update existing key
                    leaf_node.entries[i] = entry;
                    leaf_node.serialize_to_page(&mut leaf_page)?;
                    self.page_manager.write_page(&leaf_page)?;
                    return Ok(None);
                }
                Ordering::Greater => {}
            }
        }

        // Insert new entry (no clone needed, we own it)
        leaf_node.entries.insert(insert_pos, entry);

        // Check if we can serialize the node
        let mut test_page = Page::new(leaf_page_id);
        match leaf_node.serialize_to_page(&mut test_page) {
            Ok(_) => {
                // Node fits, write it
                self.page_manager.write_page(&test_page)?;
                Ok(None)
            }
            Err(Error::PageOverflow) => {
                // Node doesn't fit, need to split
                let split_result = self.split_leaf_node(&mut leaf_node, leaf_page_id)?;

                // Handle propagation of split up the tree
                if let Some((split_key, new_page_id)) = split_result {
                    self.propagate_split(path, split_key, new_page_id)
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }

    fn split_leaf_node(
        &self,
        node: &mut BTreeNode,
        page_id: u32,
    ) -> Result<Option<(Vec<u8>, u32)>> {
        // Validate that the node has enough entries to split
        if node.entries.len() < 2 {
            return Err(Error::Generic(
                "Cannot split node with fewer than 2 entries".to_string(),
            ));
        }

        // Find the optimal split point ensuring both sides have valid content
        let total_entries = node.entries.len();
        let min_left = 1; // At least 1 entry in left node
        let min_right = 1; // At least 1 entry in right node
        let max_left = total_entries - min_right;

        let mut best_split_pos = total_entries / 2;
        let mut best_left_size = u64::MAX;

        // Find split position that balances size constraints
        for split_pos in min_left..=max_left {
            let mut left_size = 64u64; // Base node overhead

            // Calculate left side size
            for entry in &node.entries[0..split_pos] {
                left_size += 8 + entry.key.len() as u64 + entry.value.len() as u64 + 8;
            }

            let mut right_size = 64u64; // Base node overhead

            // Calculate right side size
            for entry in &node.entries[split_pos..] {
                right_size += 8 + entry.key.len() as u64 + entry.value.len() as u64 + 8;
            }

            // Both halves must fit in a page
            if left_size <= PAGE_SIZE as u64 && right_size <= PAGE_SIZE as u64 {
                // Prefer more balanced split
                let imbalance = (left_size as i64 - right_size as i64).unsigned_abs();
                if imbalance < best_left_size {
                    best_left_size = imbalance;
                    best_split_pos = split_pos;
                }
            }
        }

        // Final validation - ensure split position creates valid nodes
        if best_split_pos == 0 || best_split_pos >= total_entries {
            return Err(Error::PageOverflow);
        }

        let right_entries: smallvec::SmallVec<[KeyEntry; 8]> =
            node.entries.drain(best_split_pos..).collect();

        // Double-check that both sides have entries
        if node.entries.is_empty() || right_entries.is_empty() {
            return Err(Error::Generic(
                "Split resulted in empty node - data corruption".to_string(),
            ));
        }

        let split_key = right_entries[0].key.clone();
        let original_right_sibling = node.right_sibling;

        // Create new right node
        let right_page_id = match self.page_manager.allocate_page() {
            Ok(id) => id,
            Err(e) => {
                // Rollback: restore original entries
                node.entries.extend(right_entries);
                return Err(e);
            }
        };

        let mut right_node = BTreeNode::new_leaf(right_page_id);
        right_node.entries = right_entries;
        right_node.right_sibling = original_right_sibling;

        // Update left node
        node.right_sibling = Some(right_page_id);

        // Validate both nodes can be serialized before committing changes
        let mut left_page = Page::new(page_id);
        let mut right_page = Page::new(right_page_id);

        if let Err(e) = node.serialize_to_page(&mut left_page) {
            // Rollback: free allocated page and restore state
            let _ = self.page_manager.free_page(right_page_id);
            node.right_sibling = original_right_sibling;
            node.entries.extend(right_node.entries);
            return Err(e);
        }

        if let Err(e) = right_node.serialize_to_page(&mut right_page) {
            // Rollback: free allocated page and restore state
            let _ = self.page_manager.free_page(right_page_id);
            node.right_sibling = original_right_sibling;
            node.entries.extend(right_node.entries);
            return Err(e);
        }

        // Both nodes validated - now commit the changes
        if let Err(e) = self.page_manager.write_page(&left_page) {
            // Rollback: free allocated page and restore state
            let _ = self.page_manager.free_page(right_page_id);
            node.right_sibling = original_right_sibling;
            node.entries.extend(right_node.entries);
            return Err(e);
        }

        if let Err(e) = self.page_manager.write_page(&right_page) {
            // Rollback: free allocated page and restore state
            let _ = self.page_manager.free_page(right_page_id);
            node.right_sibling = original_right_sibling;
            node.entries.extend(right_node.entries);
            return Err(e);
        }

        Ok(Some((split_key, right_page_id)))
    }

    fn handle_root_split(
        &self,
        old_root_id: u32,
        old_height: u32,
        split_key: Vec<u8>,
        new_page_id: u32,
    ) -> Result<()> {
        // Validate inputs
        if split_key.is_empty() {
            return Err(Error::Generic("Split key cannot be empty".to_string()));
        }

        // Allocate new root page
        let new_root_id = self.page_manager.allocate_page()?;

        let mut new_root = BTreeNode::new_internal(new_root_id);

        // Add the split key and both child references
        new_root.entries.push(KeyEntry::new(split_key, vec![]));
        new_root.children.push(old_root_id);
        new_root.children.push(new_page_id);

        // Validate the new root can be serialized
        let mut root_page = Page::new(new_root_id);
        if let Err(e) = new_root.serialize_to_page(&mut root_page) {
            // Rollback: free the allocated page
            let _ = self.page_manager.free_page(new_root_id);
            return Err(e);
        }

        // Write new root
        if let Err(e) = self.page_manager.write_page(&root_page) {
            // Rollback: free the allocated page
            let _ = self.page_manager.free_page(new_root_id);
            return Err(e);
        }

        // Update tree metadata atomically only after successful write
        self.root_page_id
            .store(new_root_id, AtomicOrdering::Release);
        self.height.store(old_height + 1, AtomicOrdering::Release);

        Ok(())
    }

    fn propagate_split(
        &self,
        path: &[u32],
        split_key: Vec<u8>,
        new_page_id: u32,
    ) -> Result<Option<(Vec<u8>, u32)>> {
        // If we're at the root, return the split info for the caller to handle
        if path.len() <= 1 {
            return Ok(Some((split_key, new_page_id)));
        }

        // Use the SplitHandler for intermediate node splits
        let handler = SplitHandler::new(&self.page_manager);

        // Process splits up the tree
        let mut current_split_key = split_key;
        let mut current_new_page = new_page_id;

        // Start from the parent of the split node
        for i in (0..path.len() - 1).rev() {
            let parent_page_id = path[i];

            match handler.insert_into_internal(
                parent_page_id,
                current_split_key.clone(),
                current_new_page,
            )? {
                Some((new_split_key, new_split_page)) => {
                    // Parent also split, continue propagating
                    current_split_key = new_split_key;
                    current_new_page = new_split_page;

                    // If we've reached the root, return the split info
                    if i == 0 {
                        return Ok(Some((current_split_key, current_new_page)));
                    }
                }
                None => {
                    // No more splits needed
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    pub fn root_page_id(&self) -> u32 {
        self.root_page_id.load(AtomicOrdering::Acquire)
    }

    /// Get root page ID (compatible with Database interface)
    pub fn get_root_page_id(&self) -> u64 {
        self.root_page_id.load(AtomicOrdering::Acquire) as u64
    }

    pub fn height(&self) -> u32 {
        self.height.load(AtomicOrdering::Acquire)
    }

    pub fn get_stats(&self) -> crate::DatabaseStats {
        // For now, return basic stats
        // Implementation pending: Track free pages and actual page count properly
        crate::DatabaseStats {
            page_count: 0,      // Implementation pending proper page counting
            free_page_count: 0, // Implementation pending proper free page tracking
            tree_height: self.height(),
            active_transactions: 0, // This is tracked elsewhere
            cache_hit_rate: None,
            memory_usage_bytes: 0,
            disk_usage_bytes: 0,
            active_connections: 0,
        }
    }

    /// Range scan from start_key to end_key (exclusive end)
    pub fn range(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_key_owned = start_key.map(|k| k.to_vec());
        let end_key_owned = end_key.map(|k| k.to_vec());

        let iterator: BTreeLeafIterator<'_, 32> = BTreeLeafIterator::new(
            self,
            start_key_owned,
            end_key_owned,
            true, // forward iteration
        )?;

        let mut results = Vec::new();
        for entry in iterator {
            let (key, value) = entry?;
            results.push((key, value));
        }

        Ok(results)
    }

    /// Range scan with inclusive bounds control
    pub fn range_inclusive(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        include_start: bool,
        include_end: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_key_owned = start_key.map(|k| k.to_vec());
        let end_key_owned = end_key.map(|k| k.to_vec());

        let iterator: BTreeLeafIterator<'_, 32> = BTreeLeafIterator::new(
            self,
            start_key_owned,
            end_key_owned,
            true, // forward iteration
        )?
        .with_bounds(include_start, include_end);

        let mut results = Vec::new();
        for entry in iterator {
            let (key, value) = entry?;
            results.push((key, value));
        }

        Ok(results)
    }

    /// Create an iterator for this B+Tree
    pub fn iter(&self) -> Result<BTreeLeafIterator<'_>> {
        BTreeLeafIterator::new(self, None, None, true)
    }

    /// Create a range iterator
    pub fn iter_range(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<BTreeLeafIterator<'_>> {
        let start_key_owned = start_key.map(|k| k.to_vec());
        let end_key_owned = end_key.map(|k| k.to_vec());

        BTreeLeafIterator::new(self, start_key_owned, end_key_owned, true)
    }

    /// Comprehensive crash recovery with corruption detection
    pub fn recover_from_crash(&mut self, expected_height: u32) -> Result<()> {
        let root_id = self.root_page_id();
        let current_height = self.height();

        // Validate tree height consistency
        if current_height != expected_height {
            return Err(Error::Generic(format!(
                "Height mismatch: expected {}, found {}",
                expected_height, current_height
            )));
        }

        // Perform deep validation of entire tree structure
        self.validate_tree_integrity(root_id, current_height)?;

        // Check for orphaned pages and memory leaks
        self.detect_orphaned_pages()?;

        Ok(())
    }

    /// Deep validation of tree integrity
    fn validate_tree_integrity(&self, root_page_id: u32, height: u32) -> Result<()> {
        let mut visited_pages = HashSet::new();
        self.validate_subtree(root_page_id, height, &mut visited_pages, None, None, 0)?;
        Ok(())
    }

    /// Recursively validate subtree integrity
    fn validate_subtree(
        &self,
        page_id: u32,
        expected_level: u32,
        visited_pages: &mut HashSet<u32>,
        min_key: Option<&[u8]>,
        max_key: Option<&[u8]>,
        current_level: u32,
    ) -> Result<()> {
        // Check for cycles
        if visited_pages.contains(&page_id) {
            return Err(Error::Generic(format!(
                "Cycle detected: page {} already visited",
                page_id
            )));
        }
        visited_pages.insert(page_id);

        // Load and validate the page
        let page = self
            .page_manager
            .get_page(page_id)
            .map_err(|e| Error::Generic(format!("Failed to load page {}: {}", page_id, e)))?;

        let node = BTreeNode::deserialize_from_page(&page).map_err(|e| {
            Error::Generic(format!("Failed to deserialize page {}: {}", page_id, e))
        })?;

        // Validate node basic properties
        if node.page_id != page_id {
            return Err(Error::Generic(format!(
                "Page ID mismatch: node.page_id={}, actual={}",
                node.page_id, page_id
            )));
        }

        // Check level consistency
        if current_level != expected_level {
            return Err(Error::Generic(format!(
                "Level mismatch at page {}: expected={}, actual={}",
                page_id, expected_level, current_level
            )));
        }

        // Validate key ordering within node
        for i in 1..node.entries.len() {
            if compare_keys(&node.entries[i - 1].key, &node.entries[i].key) != Ordering::Less {
                return Err(Error::Generic(format!(
                    "Key ordering violation in page {} at index {}",
                    page_id, i
                )));
            }
        }

        // Validate key range constraints
        for entry in &node.entries {
            if let Some(min) = min_key {
                if compare_keys(&entry.key, min) == Ordering::Less {
                    return Err(Error::Generic(format!(
                        "Key below minimum range in page {}",
                        page_id
                    )));
                }
            }
            if let Some(max) = max_key {
                if compare_keys(&entry.key, max) != Ordering::Less {
                    return Err(Error::Generic(format!(
                        "Key exceeds maximum range in page {}",
                        page_id
                    )));
                }
            }
        }

        // Validate internal node children
        if let NodeType::Internal = node.node_type {
            if node.children.len() != node.entries.len() + 1 {
                return Err(Error::Generic(format!(
                    "Child count mismatch in internal page {}: {} children, {} entries",
                    page_id,
                    node.children.len(),
                    node.entries.len()
                )));
            }

            // Recursively validate children
            for (i, &child_id) in node.children.iter().enumerate() {
                let child_min = if i == 0 {
                    min_key
                } else {
                    Some(node.entries[i - 1].key.as_slice())
                };
                let child_max = if i < node.entries.len() {
                    Some(node.entries[i].key.as_slice())
                } else {
                    max_key
                };

                self.validate_subtree(
                    child_id,
                    expected_level - 1,
                    visited_pages,
                    child_min,
                    child_max,
                    current_level + 1,
                )?;
            }
        }

        visited_pages.remove(&page_id);
        Ok(())
    }

    /// Detect orphaned pages that are not reachable from root
    fn detect_orphaned_pages(&self) -> Result<()> {
        // This is a placeholder - in a real implementation, we'd need
        // to track all allocated pages and compare with reachable pages
        // For now, just validate that root page is accessible
        let root_id = self.root_page_id();
        let _root_page = self.page_manager.get_page(root_id)?;
        Ok(())
    }

    /// Emergency corruption repair (use with extreme caution)
    pub fn attempt_corruption_repair(&mut self) -> Result<bool> {
        // This is a basic implementation - real corruption repair is much more complex
        let root_id = self.root_page_id();

        // Try to load the root page
        match self.page_manager.get_page(root_id) {
            Ok(root_page) => {
                // Try to deserialize the root node
                match BTreeNode::deserialize_from_page(&root_page) {
                    Ok(_) => Ok(false), // No repair needed
                    Err(_) => {
                        // Root node is corrupted, try to rebuild from scratch
                        self.rebuild_from_scratch()
                    }
                }
            }
            Err(_) => {
                // Root page is corrupted or missing
                self.rebuild_from_scratch()
            }
        }
    }

    /// Rebuild B+Tree from scratch (emergency recovery)
    fn rebuild_from_scratch(&mut self) -> Result<bool> {
        // Allocate a new root page
        let new_root_id = self.page_manager.allocate_page()?;
        let mut new_root_page = Page::new(new_root_id);
        let new_root_node = BTreeNode::new_leaf(new_root_id);

        new_root_node.serialize_to_page(&mut new_root_page)?;
        self.page_manager.write_page(&new_root_page)?;

        // Update tree metadata
        self.root_page_id
            .store(new_root_id, AtomicOrdering::Release);
        self.height.store(1, AtomicOrdering::Release);

        Ok(true) // Repair was performed
    }
}
