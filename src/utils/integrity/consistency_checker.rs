//! B+Tree Consistency Checker
//!
//! Validates the structural consistency of the B+Tree including:
//! - Key ordering
//! - Parent-child relationships
//! - Tree depth consistency
//! - Node occupancy rules

use super::{ConsistencyError, ConsistencyErrorType, ErrorSeverity};
use crate::core::btree::{node::BTreeNode, MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE};
use crate::core::storage::{PageId, PageManager, PageManagerAsync};
use crate::{Database, Result};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// B+Tree consistency checker
pub struct ConsistencyChecker {
    database: Arc<Database>,
    page_manager: Arc<RwLock<PageManager>>,
    keys_validated: Arc<RwLock<u64>>,
    visited_pages: Arc<RwLock<HashSet<PageId>>>,
    parent_map: Arc<RwLock<HashMap<PageId, PageId>>>,
    depth_map: Arc<RwLock<HashMap<PageId, usize>>>,
}

impl ConsistencyChecker {
    /// Create new consistency checker
    pub fn new(database: Arc<Database>) -> Self {
        let page_manager = database.get_page_manager().inner_arc();

        Self {
            database,
            page_manager,
            keys_validated: Arc::new(RwLock::new(0)),
            visited_pages: Arc::new(RwLock::new(HashSet::new())),
            parent_map: Arc::new(RwLock::new(HashMap::new())),
            depth_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check B+Tree consistency
    pub async fn check_btree_consistency(&self) -> Result<Vec<ConsistencyError>> {
        let mut errors = Vec::new();

        // Get root page
        let root_page_id = self.database.get_root_page_id()?;
        if root_page_id == 0 {
            return Ok(errors); // Empty tree
        }

        // Phase 1: Traverse tree and build metadata
        self.traverse_tree(root_page_id as u32, None, 0, &mut errors)
            .await?;

        // Phase 2: Validate tree properties
        self.validate_tree_properties(&mut errors).await?;

        // Phase 3: Check for orphaned pages
        self.check_orphaned_pages(&mut errors).await?;

        // Phase 4: Validate key uniqueness
        self.validate_key_uniqueness(&mut errors).await?;

        Ok(errors)
    }

    /// Traverse tree and collect metadata
    async fn traverse_tree(
        &self,
        page_id: PageId,
        parent_id: Option<PageId>,
        depth: usize,
        errors: &mut Vec<ConsistencyError>,
    ) -> Result<()> {
        // Check for circular reference
        if !self.visited_pages.write().insert(page_id) {
            errors.push(ConsistencyError {
                error_type: ConsistencyErrorType::CircularReference,
                description: format!("Circular reference detected at page {}", page_id),
                affected_pages: vec![page_id as u64],
                severity: ErrorSeverity::Critical,
            });
            return Ok(());
        }

        // Record parent relationship
        if let Some(parent) = parent_id {
            self.parent_map.write().insert(page_id, parent);
        }

        // Record depth
        self.depth_map.write().insert(page_id, depth);

        // Load page
        let page = match self.page_manager.load_page(page_id as u64).await {
            Ok(page) => page,
            Err(_) => {
                errors.push(ConsistencyError {
                    error_type: ConsistencyErrorType::MissingChild,
                    description: format!("Failed to load page {}", page_id),
                    affected_pages: vec![page_id as u64],
                    severity: ErrorSeverity::Critical,
                });
                return Ok(());
            }
        };

        // Parse as B+Tree node
        let node = match BTreeNode::deserialize_from_page(&page) {
            Ok(node) => node,
            Err(_) => {
                errors.push(ConsistencyError {
                    error_type: ConsistencyErrorType::InvalidParentPointer,
                    description: format!("Failed to parse page {} as B+Tree node", page_id),
                    affected_pages: vec![page_id as u64],
                    severity: ErrorSeverity::Critical,
                });
                return Ok(());
            }
        };

        // Validate node properties
        self.validate_node(&node, page_id, parent_id, errors)?;

        // Count keys
        *self.keys_validated.write() += node.entries.len() as u64;

        // Recurse to children if internal node
        if node.node_type == crate::core::btree::node::NodeType::Internal {
            for &child_id in node.children.iter() {
                if child_id != 0 {
                    Box::pin(self.traverse_tree(child_id, Some(page_id), depth + 1, errors))
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Validate individual node properties
    fn validate_node(
        &self,
        node: &BTreeNode,
        page_id: PageId,
        parent_id: Option<PageId>,
        errors: &mut Vec<ConsistencyError>,
    ) -> Result<()> {
        // Check key count bounds (except for root)
        if parent_id.is_some() && node.entries.len() < MIN_KEYS_PER_NODE {
            errors.push(ConsistencyError {
                error_type: ConsistencyErrorType::InvalidKeyOrder,
                description: format!(
                    "Node {} has {} keys, below minimum of {}",
                    page_id,
                    node.entries.len(),
                    MIN_KEYS_PER_NODE
                ),
                affected_pages: vec![page_id as u64],
                severity: ErrorSeverity::Error,
            });
        }

        if node.entries.len() > MAX_KEYS_PER_NODE {
            errors.push(ConsistencyError {
                error_type: ConsistencyErrorType::InvalidKeyOrder,
                description: format!(
                    "Node {} has {} keys, above maximum of {}",
                    page_id,
                    node.entries.len(),
                    MAX_KEYS_PER_NODE
                ),
                affected_pages: vec![page_id as u64],
                severity: ErrorSeverity::Critical,
            });
        }

        // Check key ordering
        for i in 1..node.entries.len() {
            if node.entries[i - 1].key >= node.entries[i].key {
                errors.push(ConsistencyError {
                    error_type: ConsistencyErrorType::InvalidKeyOrder,
                    description: format!(
                        "Keys not in ascending order at node {} (index {})",
                        page_id, i
                    ),
                    affected_pages: vec![page_id as u64],
                    severity: ErrorSeverity::Critical,
                });
            }
        }

        // Check children count for internal nodes
        if node.node_type == crate::core::btree::node::NodeType::Internal {
            let expected_children = node.entries.len() + 1;
            let actual_children = node.children.iter().filter(|&&c| c != 0).count();

            if actual_children != expected_children {
                errors.push(ConsistencyError {
                    error_type: ConsistencyErrorType::MissingChild,
                    description: format!(
                        "Node {} has {} keys but {} children (expected {})",
                        page_id,
                        node.entries.len(),
                        actual_children,
                        expected_children
                    ),
                    affected_pages: vec![page_id as u64],
                    severity: ErrorSeverity::Critical,
                });
            }
        }

        Ok(())
    }

    /// Validate tree-wide properties
    async fn validate_tree_properties(&self, errors: &mut Vec<ConsistencyError>) -> Result<()> {
        let depth_map = self.depth_map.read();

        // Find all leaf nodes
        let leaf_depths: HashSet<usize> = depth_map.values().copied().collect();

        // All leaves should be at the same depth
        if leaf_depths.len() > 1 {
            // Safe: we just checked len() > 1 so min/max will succeed
            if let (Some(&min_depth), Some(&max_depth)) =
                (leaf_depths.iter().min(), leaf_depths.iter().max())
            {
                errors.push(ConsistencyError {
                    error_type: ConsistencyErrorType::BTreeDepthMismatch,
                    description: format!(
                        "B+Tree leaves at different depths: {} to {}",
                        min_depth, max_depth
                    ),
                    affected_pages: vec![],
                    severity: ErrorSeverity::Critical,
                });
            }
        }

        Ok(())
    }

    /// Check for orphaned pages
    async fn check_orphaned_pages(&self, errors: &mut Vec<ConsistencyError>) -> Result<()> {
        // Get all allocated pages
        let all_pages = self.page_manager.get_all_allocated_pages().await?;
        let visited = self.visited_pages.read();

        for page_id in all_pages {
            if !visited.contains(&(page_id as u32)) && page_id != 0 {
                errors.push(ConsistencyError {
                    error_type: ConsistencyErrorType::OrphanedPage,
                    description: format!(
                        "Page {} is allocated but not reachable from root",
                        page_id
                    ),
                    affected_pages: vec![page_id],
                    severity: ErrorSeverity::Error,
                });
            }
        }

        Ok(())
    }

    /// Validate key uniqueness across the tree
    async fn validate_key_uniqueness(&self, errors: &mut Vec<ConsistencyError>) -> Result<()> {
        let mut all_keys = HashMap::new();

        // Collect all keys from leaf nodes without holding the read lock across await
        let page_ids: Vec<u32> = self.visited_pages.read().iter().copied().collect();
        for page_id in page_ids {
            let page = self.page_manager.load_page(page_id as u64).await?;
            let node = BTreeNode::deserialize_from_page(&page)?;

            if node.node_type == crate::core::btree::node::NodeType::Leaf {
                for entry in &node.entries {
                    if let Some(&existing_page) = all_keys.get(&entry.key) {
                        errors.push(ConsistencyError {
                            error_type: ConsistencyErrorType::DuplicateKeys,
                            description: format!(
                                "Duplicate key found in pages {} and {}",
                                existing_page, page_id
                            ),
                            affected_pages: vec![existing_page, page_id as u64],
                            severity: ErrorSeverity::Critical,
                        });
                    } else {
                        all_keys.insert(entry.key.clone(), page_id as u64);
                    }
                }
            }
        }

        Ok(())
    }

    /// Get number of keys validated
    pub fn get_keys_validated(&self) -> u64 {
        *self.keys_validated.read()
    }

    /// Validate a specific subtree
    pub async fn validate_subtree(&self, root_page_id: PageId) -> Result<Vec<ConsistencyError>> {
        let mut errors = Vec::new();

        // Clear previous state
        self.visited_pages.write().clear();
        self.parent_map.write().clear();
        self.depth_map.write().clear();
        *self.keys_validated.write() = 0;

        // Traverse from specified root
        self.traverse_tree(root_page_id, None, 0, &mut errors)
            .await?;

        Ok(errors)
    }

    /// Quick consistency check for a single page
    pub async fn quick_check_page(&self, page_id: PageId) -> Result<bool> {
        let page = self.page_manager.load_page(page_id as u64).await?;
        let node = BTreeNode::deserialize_from_page(&page)?;

        // Basic checks
        if node.entries.len() > MAX_KEYS_PER_NODE {
            return Ok(false);
        }

        // Check key ordering
        for i in 1..node.entries.len() {
            if node.entries[i - 1].key >= node.entries[i].key {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_consistency_checker() {
        // Create test database
        let dir = tempdir().unwrap();
        let _db_path = dir.path().join("test_consistency");

        // Initialize database and insert test data
        // ...

        // Run consistency check
        // ...
    }
}
