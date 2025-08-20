use super::{BPlusTree, BTreeNode, NodeType};
use crate::core::error::{Error, Result};
use std::cmp::Ordering;
use std::mem::MaybeUninit;

/// Branch prediction hints - using stable intrinsics
#[inline(always)]
fn likely(b: bool) -> bool {
    #[cold]
    fn cold() {}
    
    if !b {
        cold();
    }
    b
}

#[inline(always)]
fn unlikely(b: bool) -> bool {
    #[cold]
    fn cold() {}
    
    if b {
        cold();
    }
    b
}

// Optimized constants for iterator performance
const PREFETCH_SIZE: usize = 64;  // Cache line size for prefetching
const STACK_BUFFER_ENTRIES: usize = 32;  // Stack buffer for small result sets

/// Stack-allocated buffer for small key-value pairs to avoid heap allocation
#[repr(align(64))]
struct StackEntryBuffer {
    entries: [MaybeUninit<(Vec<u8>, Vec<u8>)>; STACK_BUFFER_ENTRIES],
    len: usize,
}

impl Default for StackEntryBuffer {
    fn default() -> Self {
        Self {
            entries: unsafe { MaybeUninit::uninit().assume_init() },
            len: 0,
        }
    }
}

/// Optimized key comparison with branch prediction
#[inline(always)]
fn compare_keys_optimized(a: &[u8], b: &[u8]) -> Ordering {
    // Fast path for equal length keys
    a.cmp(b)  // The standard library implementation is already highly optimized
}

/// Iterator for B+Tree that properly traverses leaf nodes - highly optimized
pub struct BTreeLeafIterator<'a, const BATCH_SIZE: usize = 256> {
    btree: &'a BPlusTree,
    current_leaf_id: Option<u32>,
    // Use stack buffer for small result sets, heap buffer for large ones
    current_leaf_entries: Vec<(Vec<u8>, Vec<u8>)>,
    stack_buffer: StackEntryBuffer,
    use_stack_buffer: bool,
    current_position: usize,
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    forward: bool,
    include_start: bool,
    include_end: bool,
    initialized: bool,
    // Prefetching state for better cache performance
    next_leaf_id: Option<u32>,
    prefetch_enabled: bool,
}

impl<'a, const BATCH_SIZE: usize> BTreeLeafIterator<'a, BATCH_SIZE> {
    #[inline]
    pub fn new(
        btree: &'a BPlusTree,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        forward: bool,
    ) -> Result<Self> {
        Ok(Self {
            btree,
            current_leaf_id: None,
            current_leaf_entries: Vec::with_capacity(BATCH_SIZE),
            stack_buffer: StackEntryBuffer::default(),
            use_stack_buffer: false, // Will be determined based on data size
            current_position: 0,
            start_key,
            end_key,
            forward,
            include_start: true,
            include_end: false,
            initialized: false,
            next_leaf_id: None,
            prefetch_enabled: true,
        })
    }
    
    /// Create iterator with optimized settings
    #[inline]
    pub fn new_optimized(
        btree: &'a BPlusTree,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        forward: bool,
        enable_prefetch: bool,
    ) -> Result<Self> {
        let mut iter = Self::new(btree, start_key, end_key, forward)?;
        iter.prefetch_enabled = enable_prefetch;
        Ok(iter)
    }

    pub fn with_bounds(mut self, include_start: bool, include_end: bool) -> Self {
        self.include_start = include_start;
        self.include_end = include_end;
        self
    }

    /// Initialize iterator by finding the first/last leaf - optimized
    #[inline]
    fn initialize(&mut self) -> Result<()> {
        if !self.initialized {
            if self.forward {
                // Forward iteration: find leftmost leaf containing start_key or greater
                let leaf_id = self.find_start_leaf_optimized()?;
                self.current_leaf_id = Some(leaf_id);
                
                // Prefetch next leaf if enabled
                if self.prefetch_enabled {
                    self.prefetch_next_leaf(leaf_id)?;
                }
            } else {
                // Backward iteration: find rightmost leaf containing end_key or less
                self.current_leaf_id = Some(self.find_end_leaf()?);
            }

            self.load_current_leaf_optimized()?;
            self.position_at_start_optimized()?;
            self.initialized = true;
        }
        Ok(())
    }

    /// Find the leftmost leaf that might contain keys >= start_key - optimized traversal
    #[inline]
    fn find_start_leaf_optimized(&self) -> Result<u32> {
        let start_key = self.start_key.as_deref();

        let mut current_page_id = self.btree.root_page_id();
        let mut level = self.btree.height();

        // If height is 1, root is the leaf
        if level == 1 {
            return Ok(current_page_id);
        }

        // Traverse down to leaf level
        while level > 1 {
            let page = self.btree.page_manager.get_page(current_page_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;

            // Find the child to descend into
            current_page_id = if let Some(key) = start_key {
                self.find_child_for_key(&node, key)?
            } else {
                // No start key, go to leftmost child
                *node
                    .children
                    .first()
                    .ok_or_else(|| Error::Index("No children in internal node".to_string()))?
            };

            level -= 1;
        }

        Ok(current_page_id)
    }

    /// Find the rightmost leaf that might contain keys <= end_key
    fn find_end_leaf(&self) -> Result<u32> {
        let end_key = self.end_key.as_deref();

        let mut current_page_id = self.btree.root_page_id();
        let mut level = self.btree.height();

        // If height is 1, root is the leaf
        if level == 1 {
            return Ok(current_page_id);
        }

        // Traverse down to leaf level
        while level > 1 {
            let page = self.btree.page_manager.get_page(current_page_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;

            // Find the child to descend into
            current_page_id = if let Some(key) = end_key {
                self.find_child_for_key(&node, key)?
            } else {
                // No end key, go to rightmost child
                *node
                    .children
                    .last()
                    .ok_or_else(|| Error::Index("No children in internal node".to_string()))?
            };

            level -= 1;
        }

        Ok(current_page_id)
    }

    /// Find the appropriate child for a given key
    fn find_child_for_key(&self, node: &BTreeNode, key: &[u8]) -> Result<u32> {
        if node.node_type != NodeType::Internal {
            return Err(Error::Index("Expected internal node".to_string()));
        }

        let mut child_index = 0;
        for (i, entry) in node.entries.iter().enumerate() {
            match key.cmp(&entry.key) {
                Ordering::Less => break,
                Ordering::Equal | Ordering::Greater => child_index = i + 1,
            }
        }

        node.children
            .get(child_index)
            .copied()
            .ok_or_else(|| Error::Index("Child index out of bounds".to_string()))
    }

    /// Load entries from current leaf - highly optimized
    #[inline]
    fn load_current_leaf_optimized(&mut self) -> Result<()> {
        if let Some(leaf_id) = self.current_leaf_id {
            let page = self.btree.page_manager.get_page(leaf_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;

            if unlikely(node.node_type != NodeType::Leaf) {
                return Err(Error::Index("Expected leaf node".to_string()));
            }

            // Decide whether to use stack buffer or heap buffer
            self.use_stack_buffer = node.entries.len() <= STACK_BUFFER_ENTRIES;
            
            if self.use_stack_buffer {
                // Use stack buffer for small leaf nodes
                self.stack_buffer.len = 0;
                for (i, entry) in node.entries.iter().enumerate().take(STACK_BUFFER_ENTRIES) {
                    unsafe {
                        self.stack_buffer.entries[i].write((entry.key.clone(), entry.value.clone()));
                    }
                    self.stack_buffer.len += 1;
                }
                self.current_leaf_entries.clear();
            } else {
                // Use heap buffer for large leaf nodes with vectorized copying
                self.current_leaf_entries.clear();
                self.current_leaf_entries.reserve_exact(node.entries.len());
                
                // Unroll loop for better performance
                let mut i = 0;
                while i + 3 < node.entries.len() {
                    // Process 4 entries at once
                    self.current_leaf_entries.push((node.entries[i].key.clone(), node.entries[i].value.clone()));
                    self.current_leaf_entries.push((node.entries[i+1].key.clone(), node.entries[i+1].value.clone()));
                    self.current_leaf_entries.push((node.entries[i+2].key.clone(), node.entries[i+2].value.clone()));
                    self.current_leaf_entries.push((node.entries[i+3].key.clone(), node.entries[i+3].value.clone()));
                    i += 4;
                }
                
                // Handle remaining entries
                for entry in &node.entries[i..] {
                    self.current_leaf_entries.push((entry.key.clone(), entry.value.clone()));
                }
            }

            // If we're iterating backward, reverse the entries
            if !self.forward && !self.use_stack_buffer {
                self.current_leaf_entries.reverse();
            }
        }

        Ok(())
    }
    
    /// Prefetch next leaf page for better cache performance
    #[inline]
    fn prefetch_next_leaf(&mut self, current_leaf_id: u32) -> Result<()> {
        if self.prefetch_enabled {
            let page = self.btree.page_manager.get_page(current_leaf_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;
            self.next_leaf_id = node.right_sibling;
            
            // Prefetch the next page if it exists
            if let Some(next_id) = self.next_leaf_id {
                // This would ideally use a prefetch instruction
                let _ = self.btree.page_manager.get_page(next_id);
            }
        }
        Ok(())
    }

    /// Position iterator at the start key within current leaf - optimized binary search
    #[inline]
    fn position_at_start_optimized(&mut self) -> Result<()> {
        if self.forward {
            if let Some(ref start_key) = self.start_key {
                // Use binary search for better performance on large leaf nodes
                if self.use_stack_buffer {
                    // Linear search for stack buffer (small)
                    self.current_position = 0;
                    while self.current_position < self.stack_buffer.len {
                        unsafe {
                            let entry = self.stack_buffer.entries[self.current_position].assume_init_ref();
                            let cmp = compare_keys_optimized(&entry.0, start_key);
                            match cmp {
                                Ordering::Less => self.current_position += 1,
                                Ordering::Equal => {
                                    if !self.include_start {
                                        self.current_position += 1;
                                    }
                                    break;
                                }
                                Ordering::Greater => break,
                            }
                        }
                    }
                } else {
                    // Binary search for heap buffer (large)
                    match self.current_leaf_entries.binary_search_by(|entry| entry.0.cmp(start_key)) {
                        Ok(pos) => {
                            self.current_position = if self.include_start { pos } else { pos + 1 };
                        }
                        Err(pos) => {
                            self.current_position = pos;
                        }
                    }
                }
            } else {
                self.current_position = 0;
            }
        } else {
            // For backward iteration, start at the end with optimized search
            if let Some(ref end_key) = self.end_key {
                if self.use_stack_buffer {
                    // Linear search for stack buffer
                    self.current_position = self.stack_buffer.len;
                    while self.current_position > 0 {
                        unsafe {
                            let entry = self.stack_buffer.entries[self.current_position - 1].assume_init_ref();
                            let cmp = compare_keys_optimized(&entry.0, end_key);
                            match cmp {
                                Ordering::Greater => self.current_position -= 1,
                                Ordering::Equal => {
                                    if !self.include_end {
                                        self.current_position -= 1;
                                    }
                                    break;
                                }
                                Ordering::Less => break,
                            }
                        }
                    }
                } else {
                    // Binary search for heap buffer
                    match self.current_leaf_entries.binary_search_by(|entry| entry.0.cmp(end_key)) {
                        Ok(pos) => {
                            self.current_position = if self.include_end { pos + 1 } else { pos };
                        }
                        Err(pos) => {
                            self.current_position = pos;
                        }
                    }
                }
            } else {
                self.current_position = if self.use_stack_buffer {
                    self.stack_buffer.len
                } else {
                    self.current_leaf_entries.len()
                };
            }
        }

        Ok(())
    }

    /// Move to next/previous leaf - optimized with prefetching
    #[inline]
    fn move_to_next_leaf_optimized(&mut self) -> Result<bool> {
        if let Some(current_id) = self.current_leaf_id {
            if self.forward {
                // Use prefetched next leaf ID if available
                let next_id = if self.prefetch_enabled && self.next_leaf_id.is_some() {
                    self.next_leaf_id.take()
                } else {
                    let page = self.btree.page_manager.get_page(current_id)?;
                    let node = BTreeNode::deserialize_from_page(&page)?;
                    node.right_sibling
                };

                if let Some(next_id) = next_id {
                    self.current_leaf_id = Some(next_id);
                    self.load_current_leaf_optimized()?;
                    self.current_position = 0;
                    
                    // Prefetch next leaf for future use
                    if self.prefetch_enabled {
                        let _ = self.prefetch_next_leaf(next_id);
                    }
                    
                    return Ok(true);
                }
            } else {
                // For backward iteration, we'd need left_sibling pointer
                // Since B+Tree typically only has right_sibling, we'd need to
                // implement a different approach or add left_sibling
                return Ok(false);
            }
        }

        Ok(false)
    }

    /// Check if key is within bounds - optimized with branch hints
    #[inline(always)]
    fn is_key_in_bounds(&self, key: &[u8]) -> bool {
        // Check against end bound for forward iteration
        if self.forward {
            if let Some(ref end_key) = self.end_key {
                let cmp = compare_keys_optimized(key, end_key);
                if unlikely(cmp == Ordering::Greater || (!self.include_end && cmp == Ordering::Equal)) {
                    return false;
                }
            }
        } else {
            // Check against start bound for backward iteration
            if let Some(ref start_key) = self.start_key {
                let cmp = compare_keys_optimized(key, start_key);
                if unlikely(cmp == Ordering::Less || (!self.include_start && cmp == Ordering::Equal)) {
                    return false;
                }
            }
        }

        true
    }
}

impl<const BATCH_SIZE: usize> Iterator for BTreeLeafIterator<'_, BATCH_SIZE> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // Initialize on first call
        if unlikely(!self.initialized) {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            }
        }

        loop {
            if self.forward {
                // Forward iteration - optimized for both stack and heap buffers
                let (_has_entry, entry) = if self.use_stack_buffer {
                    if self.current_position < self.stack_buffer.len {
                        unsafe {
                            let entry = self.stack_buffer.entries[self.current_position].assume_init_ref();
                            (true, Some((entry.0.clone(), entry.1.clone())))
                        }
                    } else {
                        (false, None)
                    }
                } else {
                    if self.current_position < self.current_leaf_entries.len() {
                        let entry = &self.current_leaf_entries[self.current_position];
                        (true, Some((entry.0.clone(), entry.1.clone())))
                    } else {
                        (false, None)
                    }
                };

                if let Some((key, value)) = entry {
                    // Check bounds with optimized comparison
                    if likely(self.is_key_in_bounds(&key)) {
                        self.current_position += 1;
                        return Some(Ok((key, value)));
                    } else {
                        return None;
                    }
                } else {
                    // Try to move to next leaf
                    match self.move_to_next_leaf_optimized() {
                        Ok(true) => continue,
                        Ok(false) => return None,
                        Err(e) => return Some(Err(e)),
                    }
                }
            } else {
                // Backward iteration - optimized
                if self.current_position > 0 {
                    self.current_position -= 1;
                    let (key, value) = if self.use_stack_buffer {
                        unsafe {
                            let entry = self.stack_buffer.entries[self.current_position].assume_init_ref();
                            (entry.0.clone(), entry.1.clone())
                        }
                    } else {
                        let entry = &self.current_leaf_entries[self.current_position];
                        (entry.0.clone(), entry.1.clone())
                    };

                    // Check bounds
                    if likely(self.is_key_in_bounds(&key)) {
                        return Some(Ok((key, value)));
                    } else {
                        return None;
                    }
                } else {
                    // For now, we don't support moving to previous leaf
                    // This would require maintaining left_sibling pointers
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::btree::BPlusTree;
    use crate::core::storage::page::PageManager;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_btree_iterator_forward() {
        let dir = tempdir().unwrap();
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap(),
        ));
        let mut btree = BPlusTree::new(page_manager).unwrap();

        // Insert test data
        let test_data = vec![
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4"),
            ("key5", "value5"),
        ];

        for (key, value) in &test_data {
            btree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Test forward iteration
        let iter = BTreeLeafIterator::<256>::new(&btree, None, None, true).unwrap();
        let results: Vec<_> = iter.collect();

        assert_eq!(results.len(), test_data.len());
        for (i, result) in results.iter().enumerate() {
            let (key, value) = result.as_ref().unwrap();
            assert_eq!(key, test_data[i].0.as_bytes());
            assert_eq!(value, test_data[i].1.as_bytes());
        }
    }

    #[test]
    fn test_btree_iterator_with_bounds() {
        let dir = tempdir().unwrap();
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap(),
        ));
        let mut btree = BPlusTree::new(page_manager).unwrap();

        // Insert test data
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = format!("value{:02}", i);
            btree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Test with start and end bounds
        let iter = BTreeLeafIterator::<256>::new(
            &btree,
            Some(b"key03".to_vec()),
            Some(b"key07".to_vec()),
            true,
        )
        .unwrap();

        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 4); // key03, key04, key05, key06

        // Verify the keys
        let expected_keys = ["key03", "key04", "key05", "key06"];
        for (i, result) in results.iter().enumerate() {
            let (key, _) = result.as_ref().unwrap();
            assert_eq!(key, expected_keys[i].as_bytes());
        }
    }
}
