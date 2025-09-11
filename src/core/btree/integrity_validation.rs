use super::{BTreeNode, NodeType, MAX_KEYS_PER_NODE, MIN_KEYS_PER_NODE};
use crate::core::error::Result;
use crate::core::storage::{Page, PageManagerTrait};
use crate::utils::integrity::{
    data_integrity::DataIntegrityValidator,
    error_types::{IntegrityError, ValidationResult, ViolationSeverity},
    validation_config::{IntegrityConfig, OperationType, ValidationContext},
};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

/// B+Tree integrity validator
pub struct BTreeIntegrityValidator {
    validator: Arc<DataIntegrityValidator>,
}

impl BTreeIntegrityValidator {
    pub fn new(config: IntegrityConfig) -> Self {
        Self {
            validator: Arc::new(DataIntegrityValidator::new(config)),
        }
    }

    /// Validate B+Tree node integrity
    pub fn validate_node(&self, node: &BTreeNode, page_id: u32) -> Result<()> {
        self.validator.validate_critical_path(
            "btree_node_validation",
            &format!("node_{}", page_id),
            OperationType::Read,
            |context| self.validate_node_structure(node, page_id, context),
        )
    }

    /// Validate B+Tree node before write
    pub fn validate_node_before_write(&self, node: &BTreeNode, page_id: u32) -> Result<()> {
        self.validator.validate_critical_path(
            "btree_node_pre_write",
            &format!("node_{}", page_id),
            OperationType::Write,
            |context| {
                // More strict validation before write
                let structure_result = self.validate_node_structure(node, page_id, context);
                if !structure_result.is_valid() {
                    return structure_result;
                }

                // Additional pre-write checks
                self.validate_node_write_safety(node, page_id, context)
            },
        )
    }

    /// Validate entire B+Tree consistency
    pub fn validate_tree_consistency<P: PageManagerTrait>(
        &self,
        root_page_id: u32,
        height: u32,
        page_manager: &P,
    ) -> Result<TreeConsistencyReport> {
        let mut report = TreeConsistencyReport::new();
        let mut visited_pages = HashSet::new();

        // Validate tree structure recursively
        self.validate_tree_recursive(
            root_page_id,
            height,
            None,
            None,
            page_manager,
            &mut visited_pages,
            &mut report,
            0,
        )?;

        // Check for orphaned pages (pages not reachable from root)
        // This would require additional page management metadata

        Ok(report)
    }

    /// Validate node structure and invariants
    fn validate_node_structure(
        &self,
        node: &BTreeNode,
        page_id: u32,
        _context: &ValidationContext,
    ) -> ValidationResult<()> {
        let mut violations = Vec::new();
        let location = format!("btree_node_{}", page_id);

        // Validate basic node properties
        if node.page_id != page_id {
            violations.push(
                crate::utils::integrity::error_types::create_critical_violation(
                    IntegrityError::StructuralViolation {
                        structure: "btree_node".to_string(),
                        violation: format!(
                            "Node page_id {} doesn't match expected {}",
                            node.page_id, page_id
                        ),
                    },
                    &location,
                    "Node page ID mismatch indicates corruption",
                ),
            );
        }

        // Validate entry count
        match node.node_type {
            NodeType::Leaf => {
                if node.entries.len() > MAX_KEYS_PER_NODE {
                    violations.push(crate::utils::integrity::error_types::create_violation(
                        IntegrityError::PageCapacityViolation {
                            details: format!(
                                "Leaf node has {} entries, max allowed is {}",
                                node.entries.len(),
                                MAX_KEYS_PER_NODE
                            ),
                        },
                        ViolationSeverity::Error,
                        &location,
                        None,
                    ));
                }

                // For leaf nodes, children array should be empty
                if !node.children.is_empty() {
                    violations.push(
                        crate::utils::integrity::error_types::create_critical_violation(
                            IntegrityError::StructuralViolation {
                                structure: "btree_leaf".to_string(),
                                violation: "Leaf node has child pointers".to_string(),
                            },
                            &location,
                            "Leaf nodes should not have children",
                        ),
                    );
                }
            }
            NodeType::Internal => {
                if node.entries.len() > MAX_KEYS_PER_NODE {
                    violations.push(crate::utils::integrity::error_types::create_violation(
                        IntegrityError::PageCapacityViolation {
                            details: format!(
                                "Internal node has {} entries, max allowed is {}",
                                node.entries.len(),
                                MAX_KEYS_PER_NODE
                            ),
                        },
                        ViolationSeverity::Error,
                        &location,
                        None,
                    ));
                }

                // For internal nodes, children count should be entries + 1
                let expected_children = node.entries.len() + 1;
                if node.children.len() != expected_children {
                    violations.push(crate::utils::integrity::error_types::create_critical_violation(
                        IntegrityError::ChildPointerViolation {
                            details: format!(
                                "Internal node has {} children but {} entries (expected {} children)",
                                node.children.len(),
                                node.entries.len(),
                                expected_children
                            ),
                        },
                        &location,
                        "Child pointer count must match B+Tree invariants",
                    ));
                }

                // Validate child page IDs
                for (i, &child_id) in node.children.iter().enumerate() {
                    if child_id == 0 {
                        violations.push(crate::utils::integrity::error_types::create_violation(
                            IntegrityError::ReferenceIntegrity {
                                from: format!("node_{}", page_id),
                                to: format!("child_{}", i),
                                issue: "Invalid child page ID (0)".to_string(),
                            },
                            ViolationSeverity::Error,
                            &location,
                            None,
                        ));
                    }
                }
            }
        }

        // Validate key ordering
        for i in 1..node.entries.len() {
            let prev_key = &node.entries[i - 1].key;
            let curr_key = &node.entries[i].key;

            match prev_key.cmp(curr_key) {
                Ordering::Greater => {
                    violations.push(
                        crate::utils::integrity::error_types::create_critical_violation(
                            IntegrityError::KeyOrderingViolation {
                                index: i,
                                details: format!(
                                    "Key at index {} is greater than key at index {}",
                                    i - 1,
                                    i
                                ),
                            },
                            &location,
                            "Key ordering violation breaks B+Tree invariants",
                        ),
                    );
                }
                Ordering::Equal => {
                    // Duplicate keys might be allowed depending on implementation
                    violations.push(crate::utils::integrity::error_types::create_violation(
                        IntegrityError::KeyOrderingViolation {
                            index: i,
                            details: format!("Duplicate key found at indices {} and {}", i - 1, i),
                        },
                        ViolationSeverity::Warning,
                        &location,
                        None,
                    ));
                }
                Ordering::Less => {} // Correct ordering
            }
        }

        // Validate key and value sizes
        for (i, entry) in node.entries.iter().enumerate() {
            if entry.key.is_empty() {
                violations.push(crate::utils::integrity::error_types::create_violation(
                    IntegrityError::StructuralViolation {
                        structure: "btree_entry".to_string(),
                        violation: format!("Empty key at index {}", i),
                    },
                    ViolationSeverity::Error,
                    &location,
                    None,
                ));
            }

            if entry.key.len() > 4096 {
                violations.push(crate::utils::integrity::error_types::create_violation(
                    IntegrityError::SizeViolation {
                        expected: 4096,
                        actual: entry.key.len(),
                        location: format!("{}:key[{}]", location, i),
                    },
                    ViolationSeverity::Warning,
                    &location,
                    None,
                ));
            }

            if entry.value.len() > 1024 * 1024 {
                violations.push(crate::utils::integrity::error_types::create_violation(
                    IntegrityError::SizeViolation {
                        expected: 1024 * 1024,
                        actual: entry.value.len(),
                        location: format!("{}:value[{}]", location, i),
                    },
                    ViolationSeverity::Warning,
                    &location,
                    None,
                ));
            }

            // Validate timestamp
            if entry.timestamp == 0 {
                violations.push(crate::utils::integrity::error_types::create_violation(
                    IntegrityError::TemporalInconsistency {
                        operation: "entry_timestamp_validation".to_string(),
                        details: format!("Entry at index {} has zero timestamp", i),
                    },
                    ViolationSeverity::Warning,
                    &location,
                    None,
                ));
            }
        }

        if violations.is_empty() {
            ValidationResult::Valid(())
        } else {
            ValidationResult::Invalid(violations)
        }
    }

    /// Validate node is safe to write
    fn validate_node_write_safety(
        &self,
        node: &BTreeNode,
        page_id: u32,
        _context: &ValidationContext,
    ) -> ValidationResult<()> {
        let location = format!("btree_node_{}_write_safety", page_id);
        let mut violations = Vec::new();

        // Check if node can be serialized to a page
        let mut test_page = Page::new(page_id);
        if node.serialize_to_page(&mut test_page).is_err() {
            violations.push(
                crate::utils::integrity::error_types::create_critical_violation(
                    IntegrityError::PageCapacityViolation {
                        details: "Node data exceeds page capacity".to_string(),
                    },
                    &location,
                    "Node too large to serialize - requires split",
                ),
            );
        }

        // Validate minimum key requirements for non-root nodes
        if node.entries.len() < MIN_KEYS_PER_NODE && !node.children.is_empty() {
            // This might be acceptable for root nodes
            violations.push(crate::utils::integrity::error_types::create_violation(
                IntegrityError::StructuralViolation {
                    structure: "btree_node".to_string(),
                    violation: format!(
                        "Node has only {} entries, minimum is {}",
                        node.entries.len(),
                        MIN_KEYS_PER_NODE
                    ),
                },
                ViolationSeverity::Warning,
                &location,
                None,
            ));
        }

        if violations.is_empty() {
            ValidationResult::Valid(())
        } else {
            ValidationResult::Invalid(violations)
        }
    }

    /// Recursively validate tree structure
    fn validate_tree_recursive<P: PageManagerTrait>(
        &self,
        page_id: u32,
        expected_level: u32,
        min_key: Option<&[u8]>,
        max_key: Option<&[u8]>,
        page_manager: &P,
        visited_pages: &mut HashSet<u32>,
        report: &mut TreeConsistencyReport,
        actual_level: u32,
    ) -> Result<()> {
        // Check for cycles
        if visited_pages.contains(&page_id) {
            report.add_error(TreeConsistencyError::CyclicReference {
                page_id,
                path: visited_pages.iter().cloned().collect(),
            });
            return Ok(()); // Continue validation despite cycle
        }

        visited_pages.insert(page_id);
        report.visited_pages += 1;

        // Load and validate the page
        let page = page_manager.get_page(page_id)?;
        let node = BTreeNode::deserialize_from_page(&page)?;

        // Validate this node
        if let Err(e) = self.validate_node(&node, page_id) {
            report.add_error(TreeConsistencyError::NodeValidationFailed {
                page_id,
                error: e.to_string(),
            });
        }

        // Check level consistency
        if expected_level != actual_level {
            report.add_error(TreeConsistencyError::LevelMismatch {
                page_id,
                expected_level,
                actual_level,
            });
        }

        // Validate key ranges
        for entry in &node.entries {
            if let Some(min) = min_key {
                if entry.key.as_slice() < min {
                    report.add_error(TreeConsistencyError::KeyRangeViolation {
                        page_id,
                        key: entry.key.clone(),
                        violation: "Key below minimum range".to_string(),
                    });
                }
            }
            if let Some(max) = max_key {
                if entry.key.as_slice() >= max {
                    report.add_error(TreeConsistencyError::KeyRangeViolation {
                        page_id,
                        key: entry.key.clone(),
                        violation: "Key exceeds maximum range".to_string(),
                    });
                }
            }
        }

        // Recursively validate children
        if let NodeType::Internal = node.node_type {
            for (i, &child_page_id) in node.children.iter().enumerate() {
                let child_min_key = if i == 0 {
                    min_key
                } else {
                    Some(node.entries[i - 1].key.as_slice())
                };

                let child_max_key = if i < node.entries.len() {
                    Some(node.entries[i].key.as_slice())
                } else {
                    max_key
                };

                self.validate_tree_recursive(
                    child_page_id,
                    expected_level - 1,
                    child_min_key,
                    child_max_key,
                    page_manager,
                    visited_pages,
                    report,
                    actual_level + 1,
                )?;
            }
        }

        visited_pages.remove(&page_id);
        Ok(())
    }
}

/// Tree consistency validation report
#[derive(Debug, Clone)]
pub struct TreeConsistencyReport {
    pub visited_pages: usize,
    pub errors: Vec<TreeConsistencyError>,
    pub warnings: Vec<TreeConsistencyWarning>,
}

impl TreeConsistencyReport {
    fn new() -> Self {
        Self {
            visited_pages: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn add_error(&mut self, error: TreeConsistencyError) {
        self.errors.push(error);
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn is_healthy(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone)]
pub enum TreeConsistencyError {
    NodeValidationFailed {
        page_id: u32,
        error: String,
    },
    CyclicReference {
        page_id: u32,
        path: Vec<u32>,
    },
    LevelMismatch {
        page_id: u32,
        expected_level: u32,
        actual_level: u32,
    },
    KeyRangeViolation {
        page_id: u32,
        key: Vec<u8>,
        violation: String,
    },
    OrphanedPage {
        page_id: u32,
    },
    InvalidChildPointer {
        parent_page_id: u32,
        child_page_id: u32,
        error: String,
    },
}

#[derive(Debug, Clone)]
pub enum TreeConsistencyWarning {
    LowUtilization {
        page_id: u32,
        utilization_percent: f64,
    },
    DeepTree {
        height: u32,
        recommended_max: u32,
    },
}

/// Utility functions for B+Tree validation
pub mod btree_validators {
    use super::*;

    /// Quick validation for frequently accessed operations
    pub fn quick_validate_node(node: &BTreeNode, page_id: u32) -> ValidationResult<()> {
        let location = format!("btree_node_{}_quick", page_id);

        // Just check basic invariants
        if node.page_id != page_id {
            let violation = crate::utils::integrity::error_types::create_critical_violation(
                IntegrityError::StructuralViolation {
                    structure: "btree_node".to_string(),
                    violation: "Page ID mismatch".to_string(),
                },
                &location,
                "Node corruption detected",
            );
            return ValidationResult::Invalid(vec![violation]);
        }

        // Check entry count bounds
        if node.entries.len() > MAX_KEYS_PER_NODE {
            let violation = crate::utils::integrity::error_types::create_violation(
                IntegrityError::PageCapacityViolation {
                    details: format!("Too many entries: {}", node.entries.len()),
                },
                ViolationSeverity::Error,
                &location,
                None,
            );
            return ValidationResult::Invalid(vec![violation]);
        }

        ValidationResult::Valid(())
    }

    /// Validate key insertion is safe
    pub fn validate_key_insertion(
        node: &BTreeNode,
        key: &[u8],
        value: &[u8],
    ) -> ValidationResult<()> {
        let location = "btree_insert_validation".to_string();
        let mut violations = Vec::new();

        // Check if insertion would exceed capacity
        let _estimated_size = 8 + key.len() + value.len() + 8; // overhead + key + value + timestamp
        if node.entries.len() >= MAX_KEYS_PER_NODE {
            violations.push(crate::utils::integrity::error_types::create_violation(
                IntegrityError::PageCapacityViolation {
                    details: "Node at maximum capacity for insertion".to_string(),
                },
                ViolationSeverity::Error,
                &location,
                None,
            ));
        }

        // Validate key and value sizes
        if key.is_empty() {
            violations.push(crate::utils::integrity::error_types::create_violation(
                IntegrityError::StructuralViolation {
                    structure: "insert_key".to_string(),
                    violation: "Empty key not allowed".to_string(),
                },
                ViolationSeverity::Error,
                &location,
                None,
            ));
        }

        if key.len() > 4096 {
            violations.push(crate::utils::integrity::error_types::create_violation(
                IntegrityError::SizeViolation {
                    expected: 4096,
                    actual: key.len(),
                    location: "insert_key".to_string(),
                },
                ViolationSeverity::Warning,
                &location,
                None,
            ));
        }

        if violations.is_empty() {
            ValidationResult::Valid(())
        } else {
            ValidationResult::Invalid(violations)
        }
    }

    /// Validate node can be split safely
    pub fn validate_node_split_safety(node: &BTreeNode) -> ValidationResult<()> {
        let location = "btree_split_validation";

        if node.entries.len() < 2 {
            let violation = crate::utils::integrity::error_types::create_critical_violation(
                IntegrityError::StructuralViolation {
                    structure: "btree_split".to_string(),
                    violation: "Cannot split node with less than 2 entries".to_string(),
                },
                location,
                "Node split requires at least 2 entries",
            );
            return ValidationResult::Invalid(vec![violation]);
        }

        ValidationResult::Valid(())
    }
}

/// Integration with existing B+Tree operations
pub trait ValidatedBTreeOperations {
    /// Insert with validation
    fn insert_validated(
        &mut self,
        key: &[u8],
        value: &[u8],
        config: &IntegrityConfig,
    ) -> Result<()>;

    /// Delete with validation
    fn delete_validated(&mut self, key: &[u8], config: &IntegrityConfig) -> Result<bool>;

    /// Get with validation
    fn get_validated(&self, key: &[u8], config: &IntegrityConfig) -> Result<Option<Vec<u8>>>;
}

// Implementation would be added to BPlusTree struct in the existing module
