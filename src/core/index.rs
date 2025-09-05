use super::btree::BPlusTree;
use crate::core::error::{Error, Result};
use super::storage::PageManagerWrapper;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Index metadata and configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Composite,
}

/// Index key that can be single or composite
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum IndexKey {
    Single(Vec<u8>),
    Composite(Vec<Vec<u8>>),
}

impl IndexKey {
    pub fn single(key: Vec<u8>) -> Self {
        IndexKey::Single(key)
    }

    pub fn composite(keys: Vec<Vec<u8>>) -> Self {
        IndexKey::Composite(keys)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            IndexKey::Single(key) => key.clone(),
            IndexKey::Composite(keys) => {
                let mut result = Vec::new();
                for key in keys {
                    result.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    result.extend_from_slice(key);
                }
                result
            }
        }
    }

    pub fn from_bytes(data: &[u8], is_composite: bool) -> Result<Self> {
        if !is_composite {
            return Ok(IndexKey::Single(data.to_vec()));
        }

        let mut keys = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            if offset + 4 > data.len() {
                return Err(Error::Generic("Invalid composite key format".to_string()));
            }

            let len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + len > data.len() {
                return Err(Error::Generic("Invalid composite key length".to_string()));
            }

            keys.push(data[offset..offset + len].to_vec());
            offset += len;
        }

        Ok(IndexKey::Composite(keys))
    }
}

/// Secondary index implementation
#[derive(Debug)]
pub struct SecondaryIndex {
    config: IndexConfig,
    btree: Arc<RwLock<BPlusTree>>,
}

impl SecondaryIndex {
    pub fn create(
        config: IndexConfig,
        page_manager: PageManagerWrapper,
        root_page_id: u32,
    ) -> Result<Self> {
        // Use the provided root page instead of allocating a new one
        let btree = BPlusTree::from_existing_with_wrapper(page_manager.clone(), root_page_id, 1);

        // Initialize the root page
        btree.init_root_page()?;

        Ok(Self {
            config,
            btree: Arc::new(RwLock::new(btree)),
        })
    }

    pub fn open(config: IndexConfig, page_manager: PageManagerWrapper, root_page_id: u32) -> Self {
        let btree = BPlusTree::from_existing_with_wrapper(page_manager.clone(), root_page_id, 1);

        Self {
            config,
            btree: Arc::new(RwLock::new(btree)),
        }
    }

    /// Insert an entry into the index
    pub fn insert(&self, index_key: &IndexKey, primary_key: &[u8]) -> Result<()> {
        let key_bytes = index_key.to_bytes();

        if self.config.unique {
            // Check if key already exists
            let btree = self.btree.read();
            if btree.get(&key_bytes)?.is_some() {
                return Err(Error::Generic("Unique constraint violation".to_string()));
            }
        }

        let mut btree = self.btree.write();
        btree.insert(&key_bytes, primary_key)
    }

    /// Get primary key(s) for an index key
    pub fn get(&self, index_key: &IndexKey) -> Result<Option<Vec<u8>>> {
        let key_bytes = index_key.to_bytes();
        let btree = self.btree.read();
        btree.get(&key_bytes)
    }

    /// Delete an entry from the index
    pub fn delete(&self, index_key: &IndexKey) -> Result<()> {
        let key_bytes = index_key.to_bytes();
        let mut btree = self.btree.write();
        btree.delete(&key_bytes)?;
        Ok(())
    }

    /// Range query on the index
    pub fn range(
        &self,
        start_key: &IndexKey,
        end_key: &IndexKey,
    ) -> Result<Vec<(IndexKey, Vec<u8>)>> {
        let start_bytes = start_key.to_bytes();
        let end_bytes = end_key.to_bytes();

        let btree = self.btree.read();
        let range_results = btree.range(Some(&start_bytes), Some(&end_bytes))?;

        let mut results = Vec::new();
        let is_composite = matches!(self.config.index_type, IndexType::Composite);

        for (key_bytes, primary_key) in range_results {
            let index_key = IndexKey::from_bytes(&key_bytes, is_composite)?;
            results.push((index_key, primary_key));
        }

        Ok(results)
    }

    pub fn config(&self) -> &IndexConfig {
        &self.config
    }
}

/// Index manager that handles multiple secondary indexes
#[derive(Debug)]
pub struct IndexManager {
    indexes: Arc<RwLock<HashMap<String, Arc<SecondaryIndex>>>>,
    page_manager: PageManagerWrapper,
}

impl IndexManager {
    pub fn new(page_manager: PageManagerWrapper) -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
            page_manager,
        }
    }

    /// Create a new secondary index
    pub fn create_index(&self, config: IndexConfig) -> Result<()> {
        let index_name = config.name.clone();

        // Allocate a root page for the index
        let root_page_id = self.page_manager.allocate_page()?;

        let index = Arc::new(SecondaryIndex::create(
            config,
            self.page_manager.clone(),
            root_page_id,
        )?);

        let mut indexes = self.indexes.write();
        if indexes.contains_key(&index_name) {
            return Err(Error::Generic("Index already exists".to_string()));
        }

        indexes.insert(index_name, index);
        Ok(())
    }

    /// Drop an existing index
    pub fn drop_index(&self, index_name: &str) -> Result<()> {
        let mut indexes = self.indexes.write();
        if indexes.remove(index_name).is_none() {
            return Err(Error::Generic("Index not found".to_string()));
        }
        Ok(())
    }

    /// Get an index by name
    pub fn get_index(&self, index_name: &str) -> Option<Arc<SecondaryIndex>> {
        let indexes = self.indexes.read();
        indexes.get(index_name).cloned()
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.read();
        indexes.keys().cloned().collect()
    }

    /// Insert into all relevant indexes
    pub fn insert_into_indexes(
        &self,
        primary_key: &[u8],
        record: &dyn IndexableRecord,
    ) -> Result<()> {
        let indexes = self.indexes.read();

        for (_, index) in indexes.iter() {
            if let Some(index_key) = record.extract_index_key(&index.config) {
                index.insert(&index_key, primary_key)?;
            }
        }

        Ok(())
    }

    /// Delete from all relevant indexes
    pub fn delete_from_indexes(
        &self,
        _primary_key: &[u8],
        record: &dyn IndexableRecord,
    ) -> Result<()> {
        let indexes = self.indexes.read();

        for (_, index) in indexes.iter() {
            if let Some(index_key) = record.extract_index_key(&index.config) {
                index.delete(&index_key)?;
            }
        }

        Ok(())
    }

    /// Update indexes when a record changes
    pub fn update_indexes(
        &self,
        primary_key: &[u8],
        old_record: &dyn IndexableRecord,
        new_record: &dyn IndexableRecord,
    ) -> Result<()> {
        // Delete old index entries
        self.delete_from_indexes(primary_key, old_record)?;

        // Insert new index entries
        self.insert_into_indexes(primary_key, new_record)?;

        Ok(())
    }
}

/// Trait for records that can be indexed
pub trait IndexableRecord {
    /// Extract the index key for a given index configuration
    fn extract_index_key(&self, config: &IndexConfig) -> Option<IndexKey>;

    /// Get field value by name
    fn get_field(&self, field_name: &str) -> Option<Vec<u8>>;
}

/// Simple record implementation for testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleRecord {
    fields: HashMap<String, Vec<u8>>,
}

impl Default for SimpleRecord {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleRecord {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn set_field(&mut self, name: String, value: Vec<u8>) {
        self.fields.insert(name, value);
    }

    pub fn get_field_value(&self, name: &str) -> Option<&Vec<u8>> {
        self.fields.get(name)
    }
}

impl IndexableRecord for SimpleRecord {
    fn extract_index_key(&self, config: &IndexConfig) -> Option<IndexKey> {
        if config.columns.len() == 1 {
            // Single column index
            let field_name = &config.columns[0];
            self.fields
                .get(field_name)
                .map(|value| IndexKey::Single(value.clone()))
        } else {
            // Composite index
            let mut keys = Vec::new();
            for column in &config.columns {
                if let Some(value) = self.fields.get(column) {
                    keys.push(value.clone());
                } else {
                    return None; // Missing column for composite index
                }
            }
            Some(IndexKey::Composite(keys))
        }
    }

    fn get_field(&self, field_name: &str) -> Option<Vec<u8>> {
        self.fields.get(field_name).cloned()
    }
}

/// Query builder for index-based queries
pub struct IndexQuery {
    index_name: String,
    key: Option<IndexKey>,
    range: Option<(IndexKey, IndexKey)>,
    limit: Option<usize>,
}

/// Join operation between two indexes
#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

/// Join query builder for multi-index operations
pub struct JoinQuery {
    left_index: String,
    right_index: String,
    join_type: JoinType,
    left_condition: Option<IndexKey>,
    right_condition: Option<IndexKey>,
    left_range: Option<(IndexKey, IndexKey)>,
    right_range: Option<(IndexKey, IndexKey)>,
    limit: Option<usize>,
}

/// Result of a join operation
#[derive(Debug, Clone)]
pub struct JoinResult {
    pub left_key: Option<Vec<u8>>,
    pub right_key: Option<Vec<u8>>,
    pub left_data: Option<Vec<u8>>,
    pub right_data: Option<Vec<u8>>,
}

impl IndexQuery {
    pub fn new(index_name: String) -> Self {
        Self {
            index_name,
            key: None,
            range: None,
            limit: None,
        }
    }

    pub fn key(mut self, key: IndexKey) -> Self {
        self.key = Some(key);
        self
    }

    pub fn range(mut self, start: IndexKey, end: IndexKey) -> Self {
        self.range = Some((start, end));
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the query
    pub fn execute(&self, index_manager: &IndexManager) -> Result<Vec<Vec<u8>>> {
        let index = index_manager
            .get_index(&self.index_name)
            .ok_or_else(|| Error::Generic("Index not found".to_string()))?;

        let mut results = Vec::new();

        if let Some(ref key) = self.key {
            // Single key lookup
            if let Some(primary_key) = index.get(key)? {
                results.push(primary_key);
            }
        } else if let Some((ref start, ref end)) = self.range {
            // Range query
            let range_results = index.range(start, end)?;
            for (_, primary_key) in range_results {
                results.push(primary_key);

                if let Some(limit) = self.limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }
}

impl JoinQuery {
    pub fn new(left_index: String, right_index: String, join_type: JoinType) -> Self {
        Self {
            left_index,
            right_index,
            join_type,
            left_condition: None,
            right_condition: None,
            left_range: None,
            right_range: None,
            limit: None,
        }
    }

    pub fn left_key(mut self, key: IndexKey) -> Self {
        self.left_condition = Some(key);
        self
    }

    pub fn right_key(mut self, key: IndexKey) -> Self {
        self.right_condition = Some(key);
        self
    }

    pub fn left_range(mut self, start: IndexKey, end: IndexKey) -> Self {
        self.left_range = Some((start, end));
        self
    }

    pub fn right_range(mut self, start: IndexKey, end: IndexKey) -> Self {
        self.right_range = Some((start, end));
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the join query
    pub fn execute(&self, index_manager: &IndexManager) -> Result<Vec<JoinResult>> {
        let left_index = index_manager
            .get_index(&self.left_index)
            .ok_or_else(|| Error::Generic("Left index not found".to_string()))?;
        let right_index = index_manager
            .get_index(&self.right_index)
            .ok_or_else(|| Error::Generic("Right index not found".to_string()))?;

        // Get left side results
        let left_results = self.get_left_results(&left_index)?;
        let right_results = self.get_right_results(&right_index)?;

        // Perform join based on type
        let join_results = match self.join_type {
            JoinType::Inner => self.inner_join(left_results, right_results),
            JoinType::LeftOuter => self.left_outer_join(left_results, right_results),
            JoinType::RightOuter => self.right_outer_join(left_results, right_results),
            JoinType::FullOuter => self.full_outer_join(left_results, right_results),
        };

        // Apply limit if specified
        let mut results = join_results;
        if let Some(limit) = self.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    fn get_left_results(&self, index: &SecondaryIndex) -> Result<Vec<(IndexKey, Vec<u8>)>> {
        if let Some(ref key) = self.left_condition {
            // Single key lookup
            if let Some(primary_key) = index.get(key)? {
                Ok(vec![(key.clone(), primary_key)])
            } else {
                Ok(vec![])
            }
        } else if let Some((ref start, ref end)) = self.left_range {
            // Range query
            index.range(start, end)
        } else {
            // Full scan (not recommended for large datasets)
            // For now, return empty - should implement full index scan
            Ok(vec![])
        }
    }

    fn get_right_results(&self, index: &SecondaryIndex) -> Result<Vec<(IndexKey, Vec<u8>)>> {
        if let Some(ref key) = self.right_condition {
            // Single key lookup
            if let Some(primary_key) = index.get(key)? {
                Ok(vec![(key.clone(), primary_key)])
            } else {
                Ok(vec![])
            }
        } else if let Some((ref start, ref end)) = self.right_range {
            // Range query
            index.range(start, end)
        } else {
            // Full scan (not recommended for large datasets)
            Ok(vec![])
        }
    }

    fn inner_join(
        &self,
        left: Vec<(IndexKey, Vec<u8>)>,
        right: Vec<(IndexKey, Vec<u8>)>,
    ) -> Vec<JoinResult> {
        let mut results = Vec::new();

        // Simple nested loop join - could be optimized with hash join for larger datasets
        for (left_key, left_data) in &left {
            for (right_key, right_data) in &right {
                // For now, join on exact key match
                // In a real implementation, you'd specify join conditions
                if left_key == right_key {
                    results.push(JoinResult {
                        left_key: Some(left_data.clone()),
                        right_key: Some(right_data.clone()),
                        left_data: Some(left_data.clone()),
                        right_data: Some(right_data.clone()),
                    });
                }
            }
        }

        results
    }

    fn left_outer_join(
        &self,
        left: Vec<(IndexKey, Vec<u8>)>,
        right: Vec<(IndexKey, Vec<u8>)>,
    ) -> Vec<JoinResult> {
        let mut results = Vec::new();

        for (left_key, left_data) in &left {
            let mut found_match = false;

            for (right_key, right_data) in &right {
                if left_key == right_key {
                    results.push(JoinResult {
                        left_key: Some(left_data.clone()),
                        right_key: Some(right_data.clone()),
                        left_data: Some(left_data.clone()),
                        right_data: Some(right_data.clone()),
                    });
                    found_match = true;
                }
            }

            if !found_match {
                results.push(JoinResult {
                    left_key: Some(left_data.clone()),
                    right_key: None,
                    left_data: Some(left_data.clone()),
                    right_data: None,
                });
            }
        }

        results
    }

    fn right_outer_join(
        &self,
        left: Vec<(IndexKey, Vec<u8>)>,
        right: Vec<(IndexKey, Vec<u8>)>,
    ) -> Vec<JoinResult> {
        let mut results = Vec::new();

        for (right_key, right_data) in &right {
            let mut found_match = false;

            for (left_key, left_data) in &left {
                if left_key == right_key {
                    results.push(JoinResult {
                        left_key: Some(left_data.clone()),
                        right_key: Some(right_data.clone()),
                        left_data: Some(left_data.clone()),
                        right_data: Some(right_data.clone()),
                    });
                    found_match = true;
                }
            }

            if !found_match {
                results.push(JoinResult {
                    left_key: None,
                    right_key: Some(right_data.clone()),
                    left_data: None,
                    right_data: Some(right_data.clone()),
                });
            }
        }

        results
    }

    fn full_outer_join(
        &self,
        left: Vec<(IndexKey, Vec<u8>)>,
        right: Vec<(IndexKey, Vec<u8>)>,
    ) -> Vec<JoinResult> {
        let mut results = Vec::new();
        let mut matched_right_indices = std::collections::HashSet::new();

        // Process left side matches
        for (left_key, left_data) in &left {
            let mut found_match = false;

            for (right_idx, (right_key, right_data)) in right.iter().enumerate() {
                if left_key == right_key {
                    results.push(JoinResult {
                        left_key: Some(left_data.clone()),
                        right_key: Some(right_data.clone()),
                        left_data: Some(left_data.clone()),
                        right_data: Some(right_data.clone()),
                    });
                    matched_right_indices.insert(right_idx);
                    found_match = true;
                }
            }

            if !found_match {
                results.push(JoinResult {
                    left_key: Some(left_data.clone()),
                    right_key: None,
                    left_data: Some(left_data.clone()),
                    right_data: None,
                });
            }
        }

        // Add unmatched right side entries
        for (right_idx, (_, right_data)) in right.iter().enumerate() {
            if !matched_right_indices.contains(&right_idx) {
                results.push(JoinResult {
                    left_key: None,
                    right_key: Some(right_data.clone()),
                    left_data: None,
                    right_data: Some(right_data.clone()),
                });
            }
        }

        results
    }
}

/// Multi-index query coordinator for complex queries
pub struct MultiIndexQuery {
    conditions: Vec<(String, IndexQuery)>,
    join_operations: Vec<JoinQuery>,
    limit: Option<usize>,
}

impl Default for MultiIndexQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiIndexQuery {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            join_operations: Vec::new(),
            limit: None,
        }
    }

    pub fn add_condition(mut self, alias: String, query: IndexQuery) -> Self {
        self.conditions.push((alias, query));
        self
    }

    pub fn add_join(mut self, join: JoinQuery) -> Self {
        self.join_operations.push(join);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the multi-index query
    pub fn execute(
        &self,
        index_manager: &IndexManager,
    ) -> Result<Vec<std::collections::HashMap<String, Vec<u8>>>> {
        // For now, implement a simple execution strategy
        // In a production system, this would involve query planning and optimization

        let mut results = Vec::new();

        // Execute individual conditions first
        for (alias, query) in &self.conditions {
            let condition_results = query.execute(index_manager)?;

            // For now, just collect results with alias
            for primary_key in condition_results {
                let mut row = std::collections::HashMap::new();
                row.insert(alias.clone(), primary_key);
                results.push(row);
            }
        }

        // Apply joins (simplified implementation)
        for join in &self.join_operations {
            let _join_results = join.execute(index_manager)?;
            // Join result integration requires query result merger implementation
        }

        // Apply limit
        if let Some(limit) = self.limit {
            results.truncate(limit);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_key_serialization() {
        let single_key = IndexKey::single(b"test".to_vec());
        let bytes = single_key.to_bytes();
        let deserialized = IndexKey::from_bytes(&bytes, false).unwrap();
        assert_eq!(single_key, deserialized);

        let composite_key = IndexKey::composite(vec![b"field1".to_vec(), b"field2".to_vec()]);
        let bytes = composite_key.to_bytes();
        let deserialized = IndexKey::from_bytes(&bytes, true).unwrap();
        assert_eq!(composite_key, deserialized);
    }

    #[test]
    fn test_simple_record() {
        let mut record = SimpleRecord::new();
        record.set_field("name".to_string(), b"Alice".to_vec());
        record.set_field("age".to_string(), b"30".to_vec());

        let config = IndexConfig {
            name: "name_idx".to_string(),
            columns: vec!["name".to_string()],
            unique: true,
            index_type: IndexType::BTree,
        };

        let index_key = record.extract_index_key(&config).unwrap();
        assert_eq!(index_key, IndexKey::single(b"Alice".to_vec()));
    }
}
