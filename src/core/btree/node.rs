use super::{KeyEntry, MAX_KEYS_PER_NODE};
use crate::core::error::{Error, Result};
use crate::core::storage::{Page, PAGE_SIZE};
#[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
use crate::performance::optimizations::simd::safe::{
    compare_keys as simd_compare_keys, search_prefix,
};
use bytes::{Buf, BufMut, BytesMut};
use smallvec::SmallVec;
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
    pub entries: SmallVec<[KeyEntry; 8]>, // Most nodes have few entries
    pub children: SmallVec<[u32; 9]>,     // Internal nodes have entries + 1 children
    pub right_sibling: Option<u32>,       // Only used for leaf nodes
    pub parent: Option<u32>,
}

impl BTreeNode {
    pub fn new_leaf(page_id: u32) -> Self {
        Self {
            page_id,
            node_type: NodeType::Leaf,
            entries: SmallVec::new(),
            children: SmallVec::new(),
            right_sibling: None,
            parent: None,
        }
    }

    pub fn new_internal(page_id: u32) -> Self {
        Self {
            page_id,
            node_type: NodeType::Internal,
            entries: SmallVec::new(),
            children: SmallVec::new(),
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
                    self.page_id,
                    self.entries.len(),
                    self.children.len()
                )));
            }
        }

        let mut buffer = BytesMut::with_capacity(PAGE_SIZE);

        // Write page header
        buffer.put_u32_le(crate::core::storage::MAGIC); // magic
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
        if magic != crate::core::storage::MAGIC {
            return Err(Error::Corruption(String::from("Corrupted page")));
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
            _ => return Err(Error::Corruption(String::from("Corrupted page"))),
        };

        let mut entries = SmallVec::new();

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
                .map_err(|_| Error::Corruption(String::from("Corrupted page")))?;

            let value_len = cursor.get_u32_le() as usize;
            let mut value = vec![0u8; value_len];
            cursor
                .read_exact(&mut value)
                .map_err(|_| Error::Corruption(String::from("Corrupted page")))?;

            let timestamp = cursor.get_u64_le();

            entries.push(KeyEntry {
                key,
                value,
                timestamp,
            });
        }

        let mut children = SmallVec::new();

        // Read children for internal nodes
        if node_type == NodeType::Internal {
            for _ in 0..children_count {
                children.push(cursor.get_u32_le());
            }
        }

        // Validate internal node invariant
        if node_type == NodeType::Internal && children.len() != entries.len() + 1 {
            // Don't try to fix - just return an error
            return Err(Error::Generic(format!(
                "Corrupted internal node {}: {} entries but {} children",
                page.id,
                entries.len(),
                children.len()
            )));
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

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        // Quick check for common cases
        if self.entries.len() >= MAX_KEYS_PER_NODE {
            return true;
        }
        
        // More precise size check only when needed
        let estimated_size = self.get_size_estimate();
        estimated_size > PAGE_SIZE - 256
    }

    pub fn is_underflow(&self) -> bool {
        // Minimum occupancy should be at least 25% of page
        let min_entries = (PAGE_SIZE - 64) / (32 * 4);
        self.entries.len() < min_entries
    }

    #[inline]
    pub fn find_key_position(&self, key: &[u8]) -> (bool, usize) {
        let len = self.entries.len();
        
        // For very small nodes, linear search is faster
        if len <= 4 {
            for (i, entry) in self.entries.iter().enumerate() {
                match key.cmp(&entry.key) {
                    std::cmp::Ordering::Equal => return (true, i),
                    std::cmp::Ordering::Less => return (false, i),
                    std::cmp::Ordering::Greater => {}
                }
            }
            return (false, len);
        }
        
        // Use SIMD for larger nodes when available
        #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
        {
            if len > 8 {
                return self.find_key_position_simd(key);
            }
        }

        // Binary search for medium-sized nodes
        match self.entries.binary_search_by(|entry| entry.key.as_slice().cmp(key)) {
            Ok(pos) => (true, pos),
            Err(pos) => (false, pos),
        }
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

    /// SIMD-optimized prefix search for range queries
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    pub fn find_prefix_range_simd(&self, prefix: &[u8]) -> (usize, usize) {
        let mut start_idx = None;
        let mut end_idx = self.entries.len();

        // Find first key with matching prefix
        for (i, entry) in self.entries.iter().enumerate() {
            if let Some(pos) = search_prefix(&entry.key, prefix) {
                if pos == 0 {
                    if start_idx.is_none() {
                        start_idx = Some(i);
                    }
                } else if start_idx.is_some() {
                    end_idx = i;
                    break;
                }
            }
        }

        (start_idx.unwrap_or(self.entries.len()), end_idx)
    }

    /// Vectorized key comparison for bulk operations
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    pub fn bulk_compare_keys_simd(&self, keys: &[&[u8]]) -> Vec<Option<usize>> {
        keys.iter()
            .map(|&key| {
                let (found, pos) = self.find_key_position_simd(key);
                if found {
                    Some(pos)
                } else {
                    None
                }
            })
            .collect()
    }

    /// SIMD-optimized validation that keys are sorted
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    pub fn validate_sorted_simd(&self) -> bool {
        if self.entries.len() <= 1 {
            return true;
        }

        for i in 1..self.entries.len() {
            if simd_compare_keys(&self.entries[i - 1].key, &self.entries[i].key)
                != std::cmp::Ordering::Less
            {
                return false;
            }
        }
        true
    }

    #[inline]
    pub fn get_size_estimate(&self) -> usize {
        // Cache-friendly size calculation
        let mut entries_size = 0;
        for entry in &self.entries {
            entries_size += 16 + entry.key.len() + entry.value.len(); // 4+4+8 + key + value
        }
        64 + entries_size + (self.children.len() << 2) // Use bit shift for *4
    }
}
