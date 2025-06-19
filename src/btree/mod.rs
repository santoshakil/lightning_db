pub mod node;

use crate::error::{Error, Result};
use crate::storage::{Page, PageManager, PageManagerWrapper, PAGE_SIZE};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::sync::Arc;

pub use node::*;

const MIN_KEYS_PER_NODE: usize = (PAGE_SIZE - 64) / 32; // Conservative estimate
const MAX_KEYS_PER_NODE: usize = MIN_KEYS_PER_NODE * 2;

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

    fn init_root_page(&self) -> Result<()> {
        let mut page = Page::new(self.root_page_id);
        let node = BTreeNode::new_leaf(self.root_page_id);
        node.serialize_to_page(&mut page)?;

        self.page_manager.write_page(&page)?;
        Ok(())
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
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
        let deletion_path = self.find_leaf_path(key)?;
        self.delete_from_leaf(&deletion_path, key)
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

        // Check if split is needed
        if leaf_node.entries.len() > MAX_KEYS_PER_NODE {
            self.split_leaf_node(&mut leaf_node, leaf_page_id)
        } else {
            leaf_node.serialize_to_page(&mut leaf_page)?;
            self.page_manager.write_page(&leaf_page)?;
            Ok(None)
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

    fn delete_from_leaf(
        &self,
        path: &[u32],
        key: &[u8],
    ) -> Result<bool> {
        let leaf_page_id = *path.last().ok_or(Error::Index("Empty path".to_string()))?;
        let mut leaf_page = self.page_manager.get_page(leaf_page_id)?;
        let mut leaf_node = BTreeNode::deserialize_from_page(&leaf_page)?;

        // Find and remove the key
        let mut found = false;
        leaf_node.entries.retain(|entry| {
            if entry.key == key {
                found = true;
                false
            } else {
                true
            }
        });

        if found {
            leaf_node.serialize_to_page(&mut leaf_page)?;
            self.page_manager.write_page(&leaf_page)?;
        }

        Ok(found)
    }

    pub fn root_page_id(&self) -> u32 {
        self.root_page_id
    }

    pub fn height(&self) -> u32 {
        self.height
    }
}
