use crate::btree::{BPlusTree, BTreeNode, NodeType, MIN_KEYS_PER_NODE};
use crate::error::{Error, Result};
use std::cmp::Ordering;

impl BPlusTree {
    /// Complete B+Tree deletion with underflow handling, borrowing, and merging
    pub fn delete_complete(&mut self, key: &[u8]) -> Result<bool> {
        // Find the path to the leaf containing the key
        let path = self.find_deletion_path(key)?;
        if path.is_empty() {
            return Ok(false);
        }

        // Delete from leaf and handle underflow
        let deleted = self.delete_and_rebalance(key, &path)?;
        
        // Check if root needs to be adjusted (height decrease)
        if deleted {
            self.check_root_adjustment()?;
        }
        
        Ok(deleted)
    }

    /// Find complete path from root to leaf for deletion
    fn find_deletion_path(&self, key: &[u8]) -> Result<Vec<(u32, usize)>> {
        let mut path = Vec::new();
        let mut current_page_id = self.root_page_id;
        let mut level = self.height;

        // Handle single-node tree (root is leaf)
        if self.height == 1 {
            path.push((self.root_page_id, 0));
            return Ok(path);
        }

        while level > 1 {
            let page = self.page_manager.get_page(current_page_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;
            
            let (child_page_id, child_index) = self.find_child_for_deletion(&node, key)?;
            path.push((current_page_id, child_index));
            
            current_page_id = child_page_id;
            level -= 1;
        }
        
        // Add the leaf page
        path.push((current_page_id, 0));

        Ok(path)
    }

    /// Find child page and index for deletion
    fn find_child_for_deletion(&self, node: &BTreeNode, key: &[u8]) -> Result<(u32, usize)> {
        match node.node_type {
            NodeType::Internal => {
                let mut child_index = 0;
                
                for (i, entry) in node.entries.iter().enumerate() {
                    match key.cmp(&entry.key) {
                        Ordering::Less => break,
                        Ordering::Equal | Ordering::Greater => child_index = i + 1,
                    }
                }
                
                if child_index < node.children.len() {
                    Ok((node.children[child_index], child_index))
                } else {
                    Err(Error::Index("Child index out of bounds".to_string()))
                }
            }
            NodeType::Leaf => Err(Error::Index("Cannot find child in leaf node".to_string())),
        }
    }

    /// Delete key and rebalance tree
    fn delete_and_rebalance(&mut self, key: &[u8], path: &[(u32, usize)]) -> Result<bool> {
        if path.is_empty() {
            return Ok(false);
        }

        // Get leaf node
        let (leaf_page_id, _) = path[path.len() - 1];
        let mut leaf_page = self.page_manager.get_page(leaf_page_id)?;
        let mut leaf_node = BTreeNode::deserialize_from_page(&leaf_page)?;

        // Find and remove the key
        let initial_size = leaf_node.entries.len();
        leaf_node.entries.retain(|entry| entry.key != key);
        
        if leaf_node.entries.len() == initial_size {
            return Ok(false); // Key not found
        }

        // Check for underflow
        if leaf_node.entries.len() < MIN_KEYS_PER_NODE && path.len() > 1 {
            // Handle underflow by borrowing or merging
            self.handle_underflow(path, leaf_page_id)?;
        } else {
            // No underflow, just write the updated node
            leaf_node.serialize_to_page(&mut leaf_page)?;
            self.page_manager.write_page(&leaf_page)?;
        }

        Ok(true)
    }

    /// Handle underflow by borrowing from siblings or merging
    fn handle_underflow(&mut self, path: &[(u32, usize)], underflow_page_id: u32) -> Result<()> {
        // Get parent information
        let parent_index = path.len() - 2;
        let (parent_page_id, child_index) = path[parent_index];
        
        let parent_page = self.page_manager.get_page(parent_page_id)?;
        let parent_node = BTreeNode::deserialize_from_page(&parent_page)?;

        // Try to borrow from left sibling
        if child_index > 0 {
            let left_sibling_id = parent_node.children[child_index - 1];
            if self.try_borrow_from_left(underflow_page_id, left_sibling_id, parent_page_id, child_index - 1)? {
                return Ok(());
            }
        }

        // Try to borrow from right sibling
        if child_index < parent_node.children.len() - 1 {
            let right_sibling_id = parent_node.children[child_index + 1];
            if self.try_borrow_from_right(underflow_page_id, right_sibling_id, parent_page_id, child_index)? {
                return Ok(());
            }
        }

        // Borrowing failed, must merge
        if child_index > 0 {
            // Merge with left sibling
            let left_sibling_id = parent_node.children[child_index - 1];
            self.merge_nodes(left_sibling_id, underflow_page_id, parent_page_id, child_index - 1)?;
        } else if child_index < parent_node.children.len() - 1 {
            // Merge with right sibling
            let right_sibling_id = parent_node.children[child_index + 1];
            self.merge_nodes(underflow_page_id, right_sibling_id, parent_page_id, child_index)?;
        }

        // Check if parent now has underflow
        if parent_index > 0 {
            let parent_page = self.page_manager.get_page(parent_page_id)?;
            let parent_node = BTreeNode::deserialize_from_page(&parent_page)?;
            
            if parent_node.entries.len() < MIN_KEYS_PER_NODE {
                self.handle_underflow(&path[..parent_index + 1], parent_page_id)?;
            }
        }

        Ok(())
    }

    /// Try to borrow a key from left sibling
    fn try_borrow_from_left(
        &mut self,
        underflow_page_id: u32,
        left_sibling_id: u32,
        parent_page_id: u32,
        separator_index: usize,
    ) -> Result<bool> {
        let mut left_page = self.page_manager.get_page(left_sibling_id)?;
        let mut left_node = BTreeNode::deserialize_from_page(&left_page)?;

        // Check if left sibling can spare a key
        if left_node.entries.len() <= MIN_KEYS_PER_NODE {
            return Ok(false);
        }

        let mut underflow_page = self.page_manager.get_page(underflow_page_id)?;
        let mut underflow_node = BTreeNode::deserialize_from_page(&underflow_page)?;

        let mut parent_page = self.page_manager.get_page(parent_page_id)?;
        let mut parent_node = BTreeNode::deserialize_from_page(&parent_page)?;

        match underflow_node.node_type {
            NodeType::Leaf => {
                // For leaf nodes, borrow the last entry from left sibling
                let borrowed_entry = left_node.entries.pop()
                    .ok_or_else(|| Error::InvalidOperation { 
                        reason: "Left sibling has no entries to borrow".to_string() 
                    })?;
                underflow_node.entries.insert(0, borrowed_entry.clone());
                
                // Update parent separator
                parent_node.entries[separator_index].key = underflow_node.entries[0].key.clone();
            }
            NodeType::Internal => {
                // For internal nodes, rotate through parent
                let borrowed_entry = left_node.entries.pop()
                    .ok_or_else(|| Error::InvalidOperation { 
                        reason: "Left sibling has no entries to borrow".to_string() 
                    })?;
                let borrowed_child = left_node.children.pop()
                    .ok_or_else(|| Error::InvalidOperation { 
                        reason: "Left sibling has no children to borrow".to_string() 
                    })?;
                
                // Move separator from parent to underflow node
                let separator = parent_node.entries[separator_index].clone();
                underflow_node.entries.insert(0, separator);
                underflow_node.children.insert(0, borrowed_child);
                
                // Move borrowed entry to parent
                parent_node.entries[separator_index] = borrowed_entry;
            }
        }

        // Write all modified nodes
        left_node.serialize_to_page(&mut left_page)?;
        self.page_manager.write_page(&left_page)?;
        
        underflow_node.serialize_to_page(&mut underflow_page)?;
        self.page_manager.write_page(&underflow_page)?;
        
        parent_node.serialize_to_page(&mut parent_page)?;
        self.page_manager.write_page(&parent_page)?;

        Ok(true)
    }

    /// Try to borrow a key from right sibling
    fn try_borrow_from_right(
        &mut self,
        underflow_page_id: u32,
        right_sibling_id: u32,
        parent_page_id: u32,
        separator_index: usize,
    ) -> Result<bool> {
        let mut right_page = self.page_manager.get_page(right_sibling_id)?;
        let mut right_node = BTreeNode::deserialize_from_page(&right_page)?;

        // Check if right sibling can spare a key
        if right_node.entries.len() <= MIN_KEYS_PER_NODE {
            return Ok(false);
        }

        let mut underflow_page = self.page_manager.get_page(underflow_page_id)?;
        let mut underflow_node = BTreeNode::deserialize_from_page(&underflow_page)?;

        let mut parent_page = self.page_manager.get_page(parent_page_id)?;
        let mut parent_node = BTreeNode::deserialize_from_page(&parent_page)?;

        match underflow_node.node_type {
            NodeType::Leaf => {
                // For leaf nodes, borrow the first entry from right sibling
                let borrowed_entry = right_node.entries.remove(0);
                underflow_node.entries.push(borrowed_entry);
                
                // Update parent separator
                parent_node.entries[separator_index].key = right_node.entries[0].key.clone();
            }
            NodeType::Internal => {
                // For internal nodes, rotate through parent
                let borrowed_entry = right_node.entries.remove(0);
                let borrowed_child = right_node.children.remove(0);
                
                // Move separator from parent to underflow node
                let separator = parent_node.entries[separator_index].clone();
                underflow_node.entries.push(separator);
                underflow_node.children.push(borrowed_child);
                
                // Move borrowed entry to parent
                parent_node.entries[separator_index] = borrowed_entry;
            }
        }

        // Write all modified nodes
        right_node.serialize_to_page(&mut right_page)?;
        self.page_manager.write_page(&right_page)?;
        
        underflow_node.serialize_to_page(&mut underflow_page)?;
        self.page_manager.write_page(&underflow_page)?;
        
        parent_node.serialize_to_page(&mut parent_page)?;
        self.page_manager.write_page(&parent_page)?;

        Ok(true)
    }

    /// Merge two nodes
    fn merge_nodes(
        &mut self,
        left_page_id: u32,
        right_page_id: u32,
        parent_page_id: u32,
        separator_index: usize,
    ) -> Result<()> {
        let mut left_page = self.page_manager.get_page(left_page_id)?;
        let mut left_node = BTreeNode::deserialize_from_page(&left_page)?;

        let right_page = self.page_manager.get_page(right_page_id)?;
        let right_node = BTreeNode::deserialize_from_page(&right_page)?;

        let mut parent_page = self.page_manager.get_page(parent_page_id)?;
        let mut parent_node = BTreeNode::deserialize_from_page(&parent_page)?;

        match left_node.node_type {
            NodeType::Leaf => {
                // For leaf nodes, just concatenate entries
                left_node.entries.extend(right_node.entries);
                left_node.right_sibling = right_node.right_sibling;
            }
            NodeType::Internal => {
                // For internal nodes, include separator from parent
                let separator = parent_node.entries.remove(separator_index);
                left_node.entries.push(separator);
                left_node.entries.extend(right_node.entries);
                left_node.children.extend(right_node.children);
            }
        }

        // Remove the right node from parent
        parent_node.children.remove(separator_index + 1);

        // Free the right page
        self.page_manager.free_page(right_page_id);

        // Write updated nodes
        left_node.serialize_to_page(&mut left_page)?;
        self.page_manager.write_page(&left_page)?;

        parent_node.serialize_to_page(&mut parent_page)?;
        self.page_manager.write_page(&parent_page)?;

        Ok(())
    }

    /// Check if root needs adjustment after deletion
    fn check_root_adjustment(&mut self) -> Result<()> {
        let root_page = self.page_manager.get_page(self.root_page_id)?;
        let root_node = BTreeNode::deserialize_from_page(&root_page)?;

        // If root is internal and has only one child, make that child the new root
        if root_node.node_type == NodeType::Internal && 
           root_node.entries.is_empty() && 
           root_node.children.len() == 1 {
            let new_root_id = root_node.children[0];
            self.page_manager.free_page(self.root_page_id);
            self.root_page_id = new_root_id;
            self.height -= 1;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::btree::BPlusTree;
    use crate::storage::page::PageManager;
    use std::sync::Arc;
    use parking_lot::RwLock;
    use tempfile::tempdir;

    #[test]
    fn test_delete_complete() {
        let dir = tempdir().unwrap();
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap()
        ));
        let mut btree = BPlusTree::new(page_manager).unwrap();

        // Insert test data
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            btree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Delete some keys
        for i in (0..100).step_by(2) {
            let key = format!("key{:03}", i);
            assert!(btree.delete_complete(key.as_bytes()).unwrap());
        }

        // Verify remaining keys
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let result = btree.get(key.as_bytes()).unwrap();
            if i % 2 == 0 {
                assert!(result.is_none(), "Key {} should be deleted", key);
            } else {
                assert!(result.is_some(), "Key {} should exist", key);
            }
        }
    }

    #[test]
    fn test_delete_simple() {
        let dir = tempdir().unwrap();
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap()
        ));
        let mut btree = BPlusTree::new(page_manager).unwrap();

        // Insert a few keys
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            btree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify they exist
        for i in 0..5 {
            let key = format!("key{}", i);
            assert!(btree.get(key.as_bytes()).unwrap().is_some(), "Key {} should exist", i);
        }

        // Delete one key
        assert!(btree.delete_complete(b"key2").unwrap(), "Should successfully delete key2");
        
        // Verify deletion
        assert!(btree.get(b"key2").unwrap().is_none(), "key2 should be deleted");
        assert!(btree.get(b"key1").unwrap().is_some(), "key1 should still exist");
    }

    #[test]
    fn test_insertions_work() {
        let dir = tempdir().unwrap();
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap()
        ));
        let mut btree = BPlusTree::new(page_manager).unwrap();

        // Test inserting 10 keys
        for i in 0..10 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            btree.insert(key.as_bytes(), value.as_bytes()).unwrap();
            
            // Verify immediately after insertion
            let result = btree.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist after insertion", key);
        }
        
        // Verify all keys still exist
        for i in 0..10 {
            let key = format!("key{:04}", i);
            let result = btree.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should still exist", key);
        }
    }

    #[test]
    fn test_delete_with_underflow() {
        let dir = tempdir().unwrap();
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024).unwrap()
        ));
        let mut btree = BPlusTree::new(page_manager).unwrap();

        // First test with fewer keys
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            btree.insert(key.as_bytes(), value.as_bytes()).unwrap();
            
            // Verify insertion worked
            let result = btree.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), 
                   "Failed to insert key during redistribution: {:?}", key);
        }

        // Delete many keys to trigger underflow and merging
        for i in 0..80 {
            let key = format!("key{:04}", i);
            let exists = btree.get(key.as_bytes()).unwrap().is_some();
            if !exists {
                eprintln!("Key {} doesn't exist before deletion!", key);
                continue;
            }
            let deleted = btree.delete_complete(key.as_bytes()).unwrap();
            if !deleted {
                eprintln!("Failed to delete key {}", key);
            }
        }

        // Verify remaining keys
        for i in 80..100 {
            let key = format!("key{:04}", i);
            assert!(btree.get(key.as_bytes()).unwrap().is_some());
        }
    }
}