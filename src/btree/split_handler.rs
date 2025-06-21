use crate::btree::{BTreeNode, KeyEntry, NodeType, MAX_KEYS_PER_NODE};
use crate::error::{Error, Result};
use crate::storage::{Page, PageManagerWrapper};

/// Handles complex B+Tree split operations including intermediate nodes
pub struct SplitHandler<'a> {
    page_manager: &'a PageManagerWrapper,
}

impl<'a> SplitHandler<'a> {
    pub fn new(page_manager: &'a PageManagerWrapper) -> Self {
        Self { page_manager }
    }
    
    /// Handle a split that may propagate up the tree
    #[allow(dead_code)]
    pub fn handle_split(
        &self,
        node: &mut BTreeNode,
        node_page_id: u32,
        path: &[u32],
        new_key: Vec<u8>,
        new_child_id: u32,
    ) -> Result<Option<(Vec<u8>, u32)>> {
        match node.node_type {
            NodeType::Leaf => {
                // Leaf splits are already handled
                Err(Error::Generic("Leaf splits should be handled separately".to_string()))
            }
            NodeType::Internal => {
                self.split_internal_node(node, node_page_id, path, new_key, new_child_id)
            }
        }
    }
    
    /// Split an internal node
    fn split_internal_node(
        &self,
        node: &mut BTreeNode,
        node_page_id: u32,
        _path: &[u32],
        new_key: Vec<u8>,
        new_child_id: u32,
    ) -> Result<Option<(Vec<u8>, u32)>> {
        // Find position to insert new key
        let insert_pos = node.entries
            .iter()
            .position(|e| new_key < e.key)
            .unwrap_or(node.entries.len());
        
        // Create new entry
        let new_entry = KeyEntry::new(new_key.clone(), vec![]); // Internal nodes don't store values
        
        // Insert new entry and child
        node.entries.insert(insert_pos, new_entry);
        node.children.insert(insert_pos + 1, new_child_id);
        
        // Check if split is needed
        if node.entries.len() <= MAX_KEYS_PER_NODE {
            // No split needed, just write the updated node
            let mut page = Page::new(node_page_id);
            node.serialize_to_page(&mut page)?;
            self.page_manager.write_page(&page)?;
            return Ok(None);
        }
        
        // Split is needed
        let mid = node.entries.len() / 2;
        let split_key = node.entries[mid].key.clone();
        
        // Create new right node
        let right_page_id = self.page_manager.allocate_page()?;
        let mut right_node = BTreeNode::new_internal(right_page_id);
        
        // Move entries and children to right node
        right_node.entries = node.entries.split_off(mid + 1);
        right_node.children = node.children.split_off(mid + 1);
        
        // Remove the middle key (it goes up to parent)
        node.entries.pop();
        
        // Update parent pointers
        self.update_parent_pointers(&right_node)?;
        
        // Write both nodes
        let mut left_page = Page::new(node_page_id);
        node.serialize_to_page(&mut left_page)?;
        self.page_manager.write_page(&left_page)?;
        
        let mut right_page = Page::new(right_page_id);
        right_node.serialize_to_page(&mut right_page)?;
        self.page_manager.write_page(&right_page)?;
        
        // Return split key and new node ID to propagate up
        Ok(Some((split_key, right_page_id)))
    }
    
    /// Update parent pointers for all children of a node
    fn update_parent_pointers(&self, node: &BTreeNode) -> Result<()> {
        for &child_id in &node.children {
            let child_page = self.page_manager.get_page(child_id)?;
            let mut child_node = BTreeNode::deserialize_from_page(&child_page)?;
            child_node.parent = Some(node.page_id);
            
            let mut updated_page = Page::new(child_id);
            child_node.serialize_to_page(&mut updated_page)?;
            self.page_manager.write_page(&updated_page)?;
        }
        Ok(())
    }
    
    /// Insert a key and child into an internal node, handling splits if necessary
    pub fn insert_into_internal(
        &self,
        parent_page_id: u32,
        new_key: Vec<u8>,
        new_child_id: u32,
    ) -> Result<Option<(Vec<u8>, u32)>> {
        let parent_page = self.page_manager.get_page(parent_page_id)?;
        let mut parent_node = BTreeNode::deserialize_from_page(&parent_page)?;
        
        // Find position to insert new key
        let insert_pos = parent_node.entries
            .iter()
            .position(|e| new_key < e.key)
            .unwrap_or(parent_node.entries.len());
        
        // Create new entry
        let new_entry = KeyEntry::new(new_key.clone(), vec![]); // Internal nodes don't store values
        
        // Insert new entry and child
        parent_node.entries.insert(insert_pos, new_entry);
        parent_node.children.insert(insert_pos + 1, new_child_id);
        
        // Check if split is needed
        if parent_node.entries.len() <= MAX_KEYS_PER_NODE {
            // No split needed, just write the updated node
            let mut page = Page::new(parent_page_id);
            parent_node.serialize_to_page(&mut page)?;
            self.page_manager.write_page(&page)?;
            return Ok(None);
        }
        
        // Split is needed - use existing split logic
        self.split_internal_node(&mut parent_node, parent_page_id, &[], new_key, new_child_id)
    }
    
    /// Handle split propagation up the tree
    #[allow(dead_code)]
    pub fn propagate_split(
        &self,
        path: &[u32],
        split_key: Vec<u8>,
        new_node_id: u32,
    ) -> Result<Option<(Vec<u8>, u32)>> {
        if path.len() < 2 {
            // We've reached the root, need to create new root
            return Ok(Some((split_key, new_node_id)));
        }
        
        // Get parent node
        let parent_id = path[path.len() - 2];
        let parent_page = self.page_manager.get_page(parent_id)?;
        let mut parent_node = BTreeNode::deserialize_from_page(&parent_page)?;
        
        // Insert split key and new child into parent
        let parent_path = &path[..path.len() - 1];
        self.handle_split(
            &mut parent_node,
            parent_id,
            parent_path,
            split_key,
            new_node_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{PageManager, PageManagerWrapper};
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::tempdir;
    
    #[test]
    fn test_internal_node_split() -> Result<()> {
        let dir = tempdir()?;
        let page_manager = Arc::new(RwLock::new(PageManager::create(
            &dir.path().join("test.db"),
            1024 * 1024,
        )?));
        let wrapper = PageManagerWrapper::standard(page_manager);
        
        let handler = SplitHandler::new(&wrapper);
        
        // Create a full internal node
        let mut node = BTreeNode::new_internal(1);
        for i in 0..MAX_KEYS_PER_NODE + 1 {
            node.entries.push(KeyEntry::new(
                format!("key{:03}", i).into_bytes(),
                vec![],
            ));
            node.children.push(100 + i as u32);
        }
        node.children.push(200); // One more child than entries
        
        // Trigger split
        let result = handler.handle_split(
            &mut node,
            1,
            &[0, 1],
            b"key_new".to_vec(),
            300,
        )?;
        
        assert!(result.is_some());
        let (split_key, new_node_id) = result.unwrap();
        assert!(!split_key.is_empty());
        assert!(new_node_id > 1);
        
        Ok(())
    }
}