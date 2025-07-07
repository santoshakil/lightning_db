use crate::btree::{BPlusTree, BTreeNode, NodeType};
use crate::error::{Error, Result};
use std::cmp::Ordering;

/// Iterator for B+Tree that properly traverses leaf nodes
pub struct BTreeLeafIterator<'a> {
    btree: &'a BPlusTree,
    current_leaf_id: Option<u32>,
    current_leaf_entries: Vec<(Vec<u8>, Vec<u8>)>,
    current_position: usize,
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    forward: bool,
    include_start: bool,
    include_end: bool,
    initialized: bool,
}

impl<'a> BTreeLeafIterator<'a> {
    pub fn new(
        btree: &'a BPlusTree,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        forward: bool,
    ) -> Result<Self> {
        Ok(Self {
            btree,
            current_leaf_id: None,
            current_leaf_entries: Vec::new(),
            current_position: 0,
            start_key,
            end_key,
            forward,
            include_start: true,
            include_end: false,
            initialized: false,
        })
    }

    pub fn with_bounds(mut self, include_start: bool, include_end: bool) -> Self {
        self.include_start = include_start;
        self.include_end = include_end;
        self
    }

    /// Initialize iterator by finding the first/last leaf
    fn initialize(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        if self.forward {
            // Forward iteration: find leftmost leaf containing start_key or greater
            self.current_leaf_id = Some(self.find_start_leaf()?);
        } else {
            // Backward iteration: find rightmost leaf containing end_key or less
            self.current_leaf_id = Some(self.find_end_leaf()?);
        }

        self.load_current_leaf()?;
        self.position_at_start()?;
        self.initialized = true;
        Ok(())
    }

    /// Find the leftmost leaf that might contain keys >= start_key
    fn find_start_leaf(&self) -> Result<u32> {
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

    /// Load entries from current leaf
    fn load_current_leaf(&mut self) -> Result<()> {
        self.current_leaf_entries.clear();

        if let Some(leaf_id) = self.current_leaf_id {
            let page = self.btree.page_manager.get_page(leaf_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;

            if node.node_type != NodeType::Leaf {
                return Err(Error::Index("Expected leaf node".to_string()));
            }

            // Extract key-value pairs
            for entry in &node.entries {
                self.current_leaf_entries
                    .push((entry.key.clone(), entry.value.clone()));
            }

            // Sort entries if needed (should already be sorted)
            if self.forward {
                self.current_leaf_entries.sort_by(|a, b| a.0.cmp(&b.0));
            } else {
                self.current_leaf_entries.sort_by(|a, b| b.0.cmp(&a.0));
            }
        }

        Ok(())
    }

    /// Position iterator at the start key within current leaf
    fn position_at_start(&mut self) -> Result<()> {
        if self.forward {
            if let Some(ref start_key) = self.start_key {
                // Find first key >= start_key
                self.current_position = 0;
                while self.current_position < self.current_leaf_entries.len() {
                    let cmp = self.current_leaf_entries[self.current_position]
                        .0
                        .cmp(start_key);
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
            } else {
                self.current_position = 0;
            }
        } else {
            // For backward iteration, start at the end
            if let Some(ref end_key) = self.end_key {
                // Find last key <= end_key
                self.current_position = self.current_leaf_entries.len();
                while self.current_position > 0 {
                    let cmp = self.current_leaf_entries[self.current_position - 1]
                        .0
                        .cmp(end_key);
                    match cmp {
                        Ordering::Greater => self.current_position -= 1,
                        Ordering::Equal => {
                            if self.include_end {
                                break;
                            } else {
                                self.current_position -= 1;
                            }
                        }
                        Ordering::Less => {
                            break;
                        }
                    }
                }
            } else {
                self.current_position = self.current_leaf_entries.len();
            }
        }

        Ok(())
    }

    /// Move to next/previous leaf
    fn move_to_next_leaf(&mut self) -> Result<bool> {
        if let Some(current_id) = self.current_leaf_id {
            let page = self.btree.page_manager.get_page(current_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;

            if self.forward {
                if let Some(next_id) = node.right_sibling {
                    self.current_leaf_id = Some(next_id);
                    self.load_current_leaf()?;
                    self.current_position = 0;
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

    /// Check if key is within bounds
    fn is_key_in_bounds(&self, key: &[u8]) -> bool {
        // Check against end bound for forward iteration
        if self.forward {
            if let Some(ref end_key) = self.end_key {
                let cmp = key.cmp(end_key);
                if cmp == Ordering::Greater || (!self.include_end && cmp == Ordering::Equal) {
                    return false;
                }
            }
        } else {
            // Check against start bound for backward iteration
            if let Some(ref start_key) = self.start_key {
                let cmp = key.cmp(start_key);
                if cmp == Ordering::Less || (!self.include_start && cmp == Ordering::Equal) {
                    return false;
                }
            }
        }

        true
    }
}

impl Iterator for BTreeLeafIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Initialize on first call
        if !self.initialized {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            }
        }

        loop {
            if self.forward {
                // Forward iteration
                if self.current_position < self.current_leaf_entries.len() {
                    let (key, value) = &self.current_leaf_entries[self.current_position];

                    // Check bounds
                    if !self.is_key_in_bounds(key) {
                        return None;
                    }

                    self.current_position += 1;
                    return Some(Ok((key.clone(), value.clone())));
                } else {
                    // Try to move to next leaf
                    match self.move_to_next_leaf() {
                        Ok(true) => continue,
                        Ok(false) => return None,
                        Err(e) => return Some(Err(e)),
                    }
                }
            } else {
                // Backward iteration
                if self.current_position > 0 {
                    self.current_position -= 1;
                    let (key, value) = &self.current_leaf_entries[self.current_position];

                    // Check bounds
                    if !self.is_key_in_bounds(key) {
                        return None;
                    }

                    return Some(Ok((key.clone(), value.clone())));
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
    use crate::btree::BPlusTree;
    use crate::storage::page::PageManager;
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
        let iter = BTreeLeafIterator::new(&btree, None, None, true).unwrap();
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
        let iter = BTreeLeafIterator::new(
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
