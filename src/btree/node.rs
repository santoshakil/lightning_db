use crate::btree::KeyEntry;
use crate::error::{Error, Result};
#[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
use crate::simd::key_compare::simd_compare_keys;
use crate::storage::{Page, PAGE_SIZE};
use bytes::{Buf, BufMut, BytesMut};
use std::io::{Cursor, Read};

#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    Leaf,
    Internal,
}

#[derive(Debug, Clone)]
pub struct BTreeNode {
    pub page_id: u32,
    pub node_type: NodeType,
    pub entries: Vec<KeyEntry>,
    pub children: Vec<u32>,         // Only used for internal nodes
    pub right_sibling: Option<u32>, // Only used for leaf nodes
    pub parent: Option<u32>,
}

impl BTreeNode {
    pub fn new_leaf(page_id: u32) -> Self {
        Self {
            page_id,
            node_type: NodeType::Leaf,
            entries: Vec::new(),
            children: Vec::new(),
            right_sibling: None,
            parent: None,
        }
    }

    pub fn new_internal(page_id: u32) -> Self {
        Self {
            page_id,
            node_type: NodeType::Internal,
            entries: Vec::new(),
            children: Vec::new(),
            right_sibling: None,
            parent: None,
        }
    }

    pub fn serialize_to_page(&self, page: &mut Page) -> Result<()> {
        // Validate invariant before serializing
        if self.node_type == NodeType::Internal && self.children.len() != self.entries.len() + 1 {
            // Allow root to have 0 entries and 1 child (will be fixed by check_root_adjustment)
            if !(self.entries.is_empty() && self.children.len() == 1) {
                return Err(Error::Generic(format!(
                    "Invalid internal node {}: {} entries but {} children", 
                    self.page_id, self.entries.len(), self.children.len()
                )));
            }
        }
        
        let mut buffer = BytesMut::with_capacity(PAGE_SIZE);

        // Write page header
        buffer.put_u32_le(crate::storage::MAGIC); // magic
        buffer.put_u32_le(1); // version
        buffer.put_u32_le(match self.node_type {
            NodeType::Leaf => 0,
            NodeType::Internal => 1,
        }); // page_type
        buffer.put_u32_le(0); // checksum (will be calculated by PageManager)
        buffer.put_u32_le(self.entries.len() as u32); // num_entries
        buffer.put_u32_le(0); // free_space (calculated later)
        buffer.put_u64_le(self.right_sibling.unwrap_or(0) as u64); // right_sibling

        // Write parent page ID
        buffer.put_u32_le(self.parent.unwrap_or(0));

        // Write children count for internal nodes
        if self.node_type == NodeType::Internal {
            buffer.put_u32_le(self.children.len() as u32);
        } else {
            buffer.put_u32_le(0);
        }

        // Pad to 64 bytes header
        while buffer.len() < 64 {
            buffer.put_u8(0);
        }

        // Write entries
        for entry in &self.entries {
            buffer.put_u32_le(entry.key.len() as u32);
            buffer.put(&entry.key[..]);
            buffer.put_u32_le(entry.value.len() as u32);
            buffer.put(&entry.value[..]);
            buffer.put_u64_le(entry.timestamp);
        }

        // Write children for internal nodes
        if self.node_type == NodeType::Internal {
            for &child_id in &self.children {
                buffer.put_u32_le(child_id);
            }
        }

        // Check if data fits in page
        if buffer.len() > PAGE_SIZE {
            return Err(Error::PageOverflow);
        }

        // Pad to page size
        while buffer.len() < PAGE_SIZE {
            buffer.put_u8(0);
        }

        // Copy to page
        let page_data = page.get_mut_data();
        page_data.copy_from_slice(&buffer[..PAGE_SIZE]);

        Ok(())
    }

    pub fn deserialize_from_page(page: &Page) -> Result<Self> {
        let data = page.get_data();

        // Check if page is empty (all zeros) - this can happen with newly allocated pages
        if data.iter().all(|&b| b == 0) {
            // Return an empty leaf node for empty pages
            return Ok(Self::new_leaf(page.id));
        }

        let mut cursor = Cursor::new(data);

        // Read page header
        let magic = cursor.get_u32_le();
        if magic != crate::storage::MAGIC {
            return Err(Error::CorruptedPage);
        }

        let _version = cursor.get_u32_le();
        let page_type = cursor.get_u32_le();
        let _checksum = cursor.get_u32_le();
        let num_entries = cursor.get_u32_le();
        let _free_space = cursor.get_u32_le();
        let right_sibling = cursor.get_u64_le();

        let parent = cursor.get_u32_le();
        let children_count = cursor.get_u32_le();

        // Skip padding to reach entry data
        cursor.set_position(64);

        let node_type = match page_type {
            0 => NodeType::Leaf,
            1 => NodeType::Internal,
            _ => return Err(Error::CorruptedPage),
        };

        let mut entries = Vec::new();

        // Read entries
        for _ in 0..num_entries {
            // Check if we have enough data for key length
            if cursor.remaining() < 4 {
                // Page is corrupted or incomplete - treat as empty node
                return Ok(Self::new_leaf(page.id));
            }
            let key_len = cursor.get_u32_le() as usize;
            let mut key = vec![0u8; key_len];
            cursor
                .read_exact(&mut key)
                .map_err(|_| Error::CorruptedPage)?;

            let value_len = cursor.get_u32_le() as usize;
            let mut value = vec![0u8; value_len];
            cursor
                .read_exact(&mut value)
                .map_err(|_| Error::CorruptedPage)?;

            let timestamp = cursor.get_u64_le();

            entries.push(KeyEntry {
                key,
                value,
                timestamp,
            });
        }

        let mut children = Vec::new();

        // Read children for internal nodes
        if node_type == NodeType::Internal {
            for _ in 0..children_count {
                children.push(cursor.get_u32_le());
            }
        }

        // Validate internal node invariant
        if node_type == NodeType::Internal {
            if children.len() != entries.len() + 1 {
                // Don't try to fix - just return an error
                return Err(Error::Generic(format!(
                    "Corrupted internal node {}: {} entries but {} children",
                    page.id, entries.len(), children.len()
                )));
            }
        }
        
        Ok(Self {
            page_id: page.id,
            node_type,
            entries,
            children,
            right_sibling: if right_sibling == 0 {
                None
            } else {
                Some(right_sibling as u32)
            },
            parent: if parent == 0 { None } else { Some(parent) },
        })
    }

    pub fn is_full(&self) -> bool {
        // Use accurate size calculation
        let estimated_size = self.get_size_estimate();
        // Leave 256 bytes buffer for new entry (typical max key+value size)
        estimated_size > PAGE_SIZE - 256
    }

    pub fn is_underflow(&self) -> bool {
        // Minimum occupancy should be at least 25% of page
        let min_entries = (PAGE_SIZE - 64) / (32 * 4);
        self.entries.len() < min_entries
    }

    pub fn find_key_position(&self, key: &[u8]) -> (bool, usize) {
        // Use SIMD for larger nodes when available
        #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
        {
            if self.entries.len() > 8 {
                // For larger nodes, try SIMD binary search
                return self.find_key_position_simd(key);
            }
        }

        // Fallback to regular comparison
        for (i, entry) in self.entries.iter().enumerate() {
            match key.cmp(&entry.key) {
                std::cmp::Ordering::Equal => return (true, i),
                std::cmp::Ordering::Less => return (false, i),
                std::cmp::Ordering::Greater => continue,
            }
        }
        (false, self.entries.len())
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    fn find_key_position_simd(&self, key: &[u8]) -> (bool, usize) {
        // Binary search using SIMD comparisons
        let mut left = 0;
        let mut right = self.entries.len();

        while left < right {
            let mid = left + (right - left) / 2;
            match simd_compare_keys(key, &self.entries[mid].key) {
                std::cmp::Ordering::Equal => return (true, mid),
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Greater => left = mid + 1,
            }
        }

        (false, left)
    }

    pub fn get_size_estimate(&self) -> usize {
        let entries_size: usize = self
            .entries
            .iter()
            .map(|e| 8 + e.key.len() + e.value.len() + 8) // lengths + data + timestamp
            .sum();
        let children_size = self.children.len() * 4;
        64 + entries_size + children_size
    }
}
