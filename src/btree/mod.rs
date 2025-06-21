pub mod node;
mod delete;
mod iterator;
mod split_handler;

pub use iterator::BTreeLeafIterator;

use crate::error::{Error, Result};
use crate::storage::{Page, PageManager, PageManagerWrapper};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::sync::Arc;
use split_handler::SplitHandler;

pub use node::*;

// With typical keys like "key_000000" (10 bytes) and values like "value_000000" (12 bytes),
// each entry takes approximately 34 bytes (4 + key_len + 4 + value_len + 8 timestamp).
// With 64 byte header, we can safely fit about 100 entries per page.
// Use a conservative limit to ensure we never overflow.
pub(crate) const MIN_KEYS_PER_NODE: usize = 50;
pub(crate) const MAX_KEYS_PER_NODE: usize = 100;

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

pub struct BPlusTree {
    page_manager: PageManagerWrapper,
    root_page_id: u32,
    height: u32,
}

impl BPlusTree {
    pub fn new(page_manager: Arc<RwLock<PageManager>>) -> Result<Self> {
        Self::new_with_wrapper(PageManagerWrapper::standard(page_manager))
    }
    
    pub fn new_with_wrapper(page_manager: PageManagerWrapper) -> Result<Self> {
        let root_page_id = page_manager.allocate_page()?;

        let tree = Self {
            page_manager,
            root_page_id,
            height: 1,
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
            PageManagerWrapper::standard(page_manager),
            root_page_id,
            height
        )
    }
    
    pub fn from_existing_with_wrapper(
        page_manager: PageManagerWrapper,
        root_page_id: u32,
        height: u32,
    ) -> Self {
        Self {
            page_manager,
            root_page_id,
            height,
        }
    }

    pub fn init_root_page(&self) -> Result<()> {
        let mut page = Page::new(self.root_page_id);
        let node = BTreeNode::new_leaf(self.root_page_id);
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
            // Temporarily store old root info
            let old_root_id = self.root_page_id;
            let old_height = self.height;

            self.handle_root_split_unlocked(old_root_id, old_height, new_key, new_page_id)?;
        }

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Navigate to leaf
        let mut current_page_id = self.root_page_id;
        let mut level = self.height;

        while level > 1 {
            let page = self.page_manager.get_page(current_page_id)?;
            let node = BTreeNode::deserialize_from_page(&page)?;

            current_page_id = self.find_child_page(&node, key)?;
            level -= 1;
        }

        // Search in leaf
        let leaf_page = self.page_manager.get_page(current_page_id)?;
        let leaf_node = BTreeNode::deserialize_from_page(&leaf_page)?;

        self.search_leaf(&leaf_node, key)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        // Use the complete deletion implementation with rebalancing
        self.delete_complete(key)
    }

    fn find_leaf_path(
        &self,
        key: &[u8],
    ) -> Result<Vec<u32>> {
        let mut path = Vec::new();
        let mut current_page_id = self.root_page_id;
        let mut level = self.height;

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

    fn find_child_page(&self, node: &BTreeNode, key: &[u8]) -> Result<u32> {
        match node.node_type {
            NodeType::Internal => {
                let mut child_index = 0;

                for (i, entry) in node.entries.iter().enumerate() {
                    match key.cmp(&entry.key) {
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
                    Err(Error::Index("Invalid child index".to_string()))
                }
            }
            NodeType::Leaf => Err(Error::Index("Cannot find child in leaf node".to_string())),
        }
    }

    fn search_leaf(&self, node: &BTreeNode, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if node.node_type != NodeType::Leaf {
            return Err(Error::Index("Expected leaf node".to_string()));
        }

        for entry in &node.entries {
            match key.cmp(&entry.key) {
                Ordering::Equal => return Ok(Some(entry.value.clone())),
                Ordering::Less => break,
                Ordering::Greater => continue,
            }
        }

        Ok(None)
    }

    fn insert_into_leaf(
        &self,
        path: &[u32],
        entry: KeyEntry,
    ) -> Result<Option<(Vec<u8>, u32)>> {
        let leaf_page_id = *path.last().ok_or(Error::Index("Empty path".to_string()))?;
        let mut leaf_page = self.page_manager.get_page(leaf_page_id)?;
        let mut leaf_node = BTreeNode::deserialize_from_page(&leaf_page)?;

        // Find insertion position
        let mut insert_pos = leaf_node.entries.len();
        for (i, existing_entry) in leaf_node.entries.iter().enumerate() {
            match entry.key.cmp(&existing_entry.key) {
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
                Ordering::Greater => continue,
            }
        }

        // Insert new entry
        leaf_node.entries.insert(insert_pos, entry);

        // Try to serialize the node to check if it fits
        match leaf_node.serialize_to_page(&mut leaf_page) {
            Ok(_) => {
                // Check if we should split based on entry count
                if leaf_node.entries.len() > MAX_KEYS_PER_NODE {
                    let split_result = self.split_leaf_node(&mut leaf_node, leaf_page_id)?;
                    
                    // Handle propagation of split up the tree
                    if let Some((split_key, new_page_id)) = split_result {
                        self.propagate_split(path, split_key, new_page_id)
                    } else {
                        Ok(None)
                    }
                } else {
                    self.page_manager.write_page(&leaf_page)?;
                    Ok(None)
                }
            }
            Err(Error::PageOverflow) => {
                // Page overflow - must split
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
        let mid = node.entries.len() / 2;
        let right_entries = node.entries.split_off(mid);
        let split_key = right_entries[0].key.clone();

        // Create new right node
        let right_page_id = self.page_manager.allocate_page()?;
        let mut right_node = BTreeNode::new_leaf(right_page_id);
        right_node.entries = right_entries;
        right_node.right_sibling = node.right_sibling;

        // Update left node
        node.right_sibling = Some(right_page_id);

        // Write both nodes
        let mut left_page = Page::new(page_id);
        node.serialize_to_page(&mut left_page)?;
        self.page_manager.write_page(&left_page)?;

        let mut right_page = Page::new(right_page_id);
        right_node.serialize_to_page(&mut right_page)?;
        self.page_manager.write_page(&right_page)?;

        Ok(Some((split_key, right_page_id)))
    }

    fn handle_root_split_unlocked(
        &mut self,
        old_root_id: u32,
        old_height: u32,
        split_key: Vec<u8>,
        new_page_id: u32,
    ) -> Result<()> {
        let new_root_id = self.page_manager.allocate_page()?;
        let mut new_root = BTreeNode::new_internal(new_root_id);

        // Add the split key and both child references
        new_root.entries.push(KeyEntry::new(split_key, vec![]));
        new_root.children.push(old_root_id);
        new_root.children.push(new_page_id);

        // Write new root
        let mut root_page = Page::new(new_root_id);
        new_root.serialize_to_page(&mut root_page)?;
        self.page_manager.write_page(&root_page)?;

        // Update tree metadata
        self.root_page_id = new_root_id;
        self.height = old_height + 1;

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
            
            match handler.insert_into_internal(parent_page_id, current_split_key.clone(), current_new_page)? {
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
        self.root_page_id
    }

    pub fn height(&self) -> u32 {
        self.height
    }

    /// Range scan from start_key to end_key (exclusive end)
    pub fn range(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_key_owned = start_key.map(|k| k.to_vec());
        let end_key_owned = end_key.map(|k| k.to_vec());
        
        let iterator = BTreeLeafIterator::new(
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
    pub fn range_inclusive(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>, include_start: bool, include_end: bool) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start_key_owned = start_key.map(|k| k.to_vec());
        let end_key_owned = end_key.map(|k| k.to_vec());
        
        let iterator = BTreeLeafIterator::new(
            self,
            start_key_owned,
            end_key_owned,
            true, // forward iteration
        )?.with_bounds(include_start, include_end);
        
        let mut results = Vec::new();
        for entry in iterator {
            let (key, value) = entry?;
            results.push((key, value));
        }
        
        Ok(results)
    }

    /// Create an iterator for this B+Tree
    pub fn iter(&self) -> Result<BTreeLeafIterator> {
        BTreeLeafIterator::new(self, None, None, true)
    }

    /// Create a range iterator
    pub fn iter_range(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>) -> Result<BTreeLeafIterator> {
        let start_key_owned = start_key.map(|k| k.to_vec());
        let end_key_owned = end_key.map(|k| k.to_vec());
        
        BTreeLeafIterator::new(self, start_key_owned, end_key_owned, true)
    }
}
