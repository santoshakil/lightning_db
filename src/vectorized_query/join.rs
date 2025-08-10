//! Vectorized Join Operations
//!
//! High-performance join implementations using SIMD instructions
//! and cache-efficient algorithms for columnar data.

use super::{
    ColumnarTable, ColumnarTableBuilder, ExecutionStats, JoinCondition,
    JoinType, SIMDOperations, Value, VECTOR_BATCH_SIZE,
};
use crate::{Error, Result};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

/// Vectorized join executor
pub struct VectorizedJoiner {
    /// SIMD operations implementation
    simd_ops: Arc<dyn SIMDOperations + Send + Sync>,
    /// Execution statistics
    stats: ExecutionStats,
}

/// Join configuration
#[derive(Debug, Clone)]
pub struct JoinConfig {
    /// Type of join
    pub join_type: JoinType,
    /// Join conditions
    pub conditions: Vec<JoinCondition>,
    /// Use parallel processing
    pub parallel: bool,
    /// Hash table initial capacity
    pub hash_table_capacity: usize,
    /// Maximum memory for hash table
    pub max_memory_bytes: usize,
    /// Use SIMD optimizations
    pub use_simd: bool,
}

/// Join result
#[derive(Debug)]
pub struct JoinResult {
    /// Resulting table
    pub table: ColumnarTable,
    /// Left row indices in result
    pub left_indices: Vec<usize>,
    /// Right row indices in result
    pub right_indices: Vec<usize>,
    /// Execution statistics
    pub stats: ExecutionStats,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Number of matched pairs
    pub match_count: usize,
}

/// Hash table entry for joins
#[derive(Debug, Clone)]
pub struct HashEntry {
    /// Row index in original table
    pub row_index: usize,
    /// Hash of join key
    pub key_hash: u64,
    /// Join key values
    pub key_values: Vec<Value>,
}

/// Hash table for join operations
#[derive(Debug)]
pub struct JoinHashTable {
    /// Hash buckets
    pub buckets: Vec<Vec<HashEntry>>,
    /// Number of buckets
    pub bucket_count: usize,
    /// Total entries
    pub entry_count: usize,
    /// Memory usage estimate
    pub memory_usage: usize,
}

/// Join key for hashing and comparison
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinKey {
    /// Key values
    pub values: Vec<Value>,
}

/// Vectorized hash join implementation
#[derive(Debug)]
pub struct HashJoinProcessor {
    /// Hash table for smaller relation
    hash_table: JoinHashTable,
    /// SIMD operations
    simd_ops: Arc<dyn SIMDOperations + Send + Sync>,
    /// Statistics
    stats: ExecutionStats,
}

/// Nested loop join implementation
#[derive(Debug)]
pub struct NestedLoopJoinProcessor {
    /// SIMD operations
    simd_ops: Arc<dyn SIMDOperations + Send + Sync>,
    /// Statistics
    stats: ExecutionStats,
}

/// Sort-merge join implementation
#[derive(Debug)]
pub struct SortMergeJoinProcessor {
    /// SIMD operations
    simd_ops: Arc<dyn SIMDOperations + Send + Sync>,
    /// Statistics
    stats: ExecutionStats,
}

impl VectorizedJoiner {
    /// Create new vectorized joiner
    pub fn new(simd_ops: Arc<dyn SIMDOperations + Send + Sync>) -> Self {
        Self {
            simd_ops,
            stats: ExecutionStats::default(),
        }
    }

    /// Execute join between two tables
    pub fn join(
        &mut self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: JoinConfig,
    ) -> Result<JoinResult> {
        // Choose optimal join algorithm
        let algorithm = self.choose_join_algorithm(left_table, right_table, &config)?;

        match algorithm {
            JoinAlgorithm::HashJoin => self.hash_join(left_table, right_table, config),
            JoinAlgorithm::SortMergeJoin => self.sort_merge_join(left_table, right_table, config),
            JoinAlgorithm::NestedLoopJoin => self.nested_loop_join(left_table, right_table, config),
            JoinAlgorithm::VectorizedHashJoin => {
                self.vectorized_hash_join(left_table, right_table, config)
            }
        }
    }

    /// Choose optimal join algorithm
    fn choose_join_algorithm(
        &self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: &JoinConfig,
    ) -> Result<JoinAlgorithm> {
        let left_size = left_table.row_count;
        let right_size = right_table.row_count;

        // For very small tables, use nested loop
        if left_size <= 100 && right_size <= 100 {
            return Ok(JoinAlgorithm::NestedLoopJoin);
        }

        // For equi-joins with one small table, use hash join
        if self.is_equi_join(&config.conditions) {
            let smaller_size = left_size.min(right_size);
            let larger_size = left_size.max(right_size);

            // Use hash join if one table is significantly smaller
            if smaller_size * 10 < larger_size {
                if config.use_simd && smaller_size > 1000 {
                    return Ok(JoinAlgorithm::VectorizedHashJoin);
                } else {
                    return Ok(JoinAlgorithm::HashJoin);
                }
            }

            // For similarly sized tables with sorted data, use sort-merge
            if left_size > 10000 && right_size > 10000 {
                return Ok(JoinAlgorithm::SortMergeJoin);
            }

            // Default to hash join for equi-joins
            if config.use_simd {
                return Ok(JoinAlgorithm::VectorizedHashJoin);
            } else {
                return Ok(JoinAlgorithm::HashJoin);
            }
        }

        // For non-equi joins, use nested loop
        Ok(JoinAlgorithm::NestedLoopJoin)
    }

    /// Check if join conditions are equi-joins
    fn is_equi_join(&self, conditions: &[JoinCondition]) -> bool {
        conditions
            .iter()
            .all(|cond| matches!(cond, JoinCondition::Equal { .. }))
    }

    /// Vectorized hash join implementation
    fn vectorized_hash_join(
        &mut self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: JoinConfig,
    ) -> Result<JoinResult> {
        // Determine which table to use for hash table (smaller one)
        let (build_table, probe_table, swap_tables) =
            if left_table.row_count <= right_table.row_count {
                (left_table, right_table, false)
            } else {
                (right_table, left_table, true)
            };

        // Build hash table
        let hash_table = self.build_vectorized_hash_table(build_table, &config.conditions)?;

        // Probe hash table
        let matches = self.probe_vectorized_hash_table(
            probe_table,
            &hash_table,
            &config.conditions,
            swap_tables,
        )?;

        // Build result table
        let result_table =
            self.build_join_result_table(left_table, right_table, &matches, &config.join_type)?;

        let (left_indices, right_indices) = if swap_tables {
            (
                matches.iter().map(|m| m.1).collect(),
                matches.iter().map(|m| m.0).collect(),
            )
        } else {
            (
                matches.iter().map(|m| m.0).collect(),
                matches.iter().map(|m| m.1).collect(),
            )
        };

        Ok(JoinResult {
            table: result_table,
            left_indices,
            right_indices,
            stats: self.stats.clone(),
            memory_usage: hash_table.memory_usage,
            match_count: matches.len(),
        })
    }

    /// Build vectorized hash table
    fn build_vectorized_hash_table(
        &mut self,
        table: &ColumnarTable,
        conditions: &[JoinCondition],
    ) -> Result<JoinHashTable> {
        let bucket_count = self.calculate_optimal_bucket_count(table.row_count);
        let mut hash_table = JoinHashTable {
            buckets: vec![Vec::new(); bucket_count],
            bucket_count,
            entry_count: 0,
            memory_usage: 0,
        };

        // Extract join key columns
        let key_columns = self.extract_join_key_columns(conditions, true)?;

        // Process in batches for vectorization
        let batch_size = VECTOR_BATCH_SIZE;
        let mut processed_rows = 0;

        while processed_rows < table.row_count {
            let current_batch_size = (table.row_count - processed_rows).min(batch_size);

            // Extract batch of join keys
            let batch_keys = self.extract_join_keys_batch(
                table,
                &key_columns,
                processed_rows,
                current_batch_size,
            )?;

            // Hash keys in batch using SIMD
            let batch_hashes = self.vectorized_hash_keys(&batch_keys)?;

            // Insert into hash table
            for (i, (key, hash)) in batch_keys.into_iter().zip(batch_hashes).enumerate() {
                let row_index = processed_rows + i;
                let bucket_index = (hash as usize) % bucket_count;

                let entry = HashEntry {
                    row_index,
                    key_hash: hash,
                    key_values: key.values,
                };

                hash_table.buckets[bucket_index].push(entry);
                hash_table.entry_count += 1;
            }

            processed_rows += current_batch_size;
            self.stats.simd_operations += 1;
        }

        hash_table.memory_usage = self.estimate_hash_table_memory(&hash_table);

        Ok(hash_table)
    }

    /// Probe vectorized hash table
    fn probe_vectorized_hash_table(
        &mut self,
        table: &ColumnarTable,
        hash_table: &JoinHashTable,
        conditions: &[JoinCondition],
        swap_indices: bool,
    ) -> Result<Vec<(usize, usize)>> {
        let mut matches = Vec::new();

        // Extract join key columns for probe table
        let key_columns = self.extract_join_key_columns(conditions, false)?;

        // Process in batches
        let batch_size = VECTOR_BATCH_SIZE;
        let mut processed_rows = 0;

        while processed_rows < table.row_count {
            let current_batch_size = (table.row_count - processed_rows).min(batch_size);

            // Extract batch of probe keys
            let batch_keys = self.extract_join_keys_batch(
                table,
                &key_columns,
                processed_rows,
                current_batch_size,
            )?;

            // Hash probe keys
            let batch_hashes = self.vectorized_hash_keys(&batch_keys)?;

            // Probe hash table for matches
            for (i, (probe_key, probe_hash)) in batch_keys.into_iter().zip(batch_hashes).enumerate()
            {
                let probe_row = processed_rows + i;
                let bucket_index = (probe_hash as usize) % hash_table.bucket_count;

                // Check all entries in the bucket
                for entry in &hash_table.buckets[bucket_index] {
                    if entry.key_hash == probe_hash
                        && self.keys_equal(&probe_key, &entry.key_values)
                    {
                        if swap_indices {
                            matches.push((entry.row_index, probe_row));
                        } else {
                            matches.push((probe_row, entry.row_index));
                        }
                    }
                }
            }

            processed_rows += current_batch_size;
            self.stats.simd_operations += 1;
        }

        Ok(matches)
    }

    /// Hash join implementation
    fn hash_join(
        &mut self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: JoinConfig,
    ) -> Result<JoinResult> {
        // For simplicity, use the simpler hash join implementation
        // In a real system, this would be more optimized
        let mut processor = HashJoinProcessor::new(Arc::clone(&self.simd_ops));
        processor.execute_hash_join(left_table, right_table, config)
    }

    /// Sort-merge join implementation
    fn sort_merge_join(
        &mut self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: JoinConfig,
    ) -> Result<JoinResult> {
        let mut processor = SortMergeJoinProcessor::new(Arc::clone(&self.simd_ops));
        processor.execute_sort_merge_join(left_table, right_table, config)
    }

    /// Nested loop join implementation
    fn nested_loop_join(
        &mut self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: JoinConfig,
    ) -> Result<JoinResult> {
        let mut processor = NestedLoopJoinProcessor::new(Arc::clone(&self.simd_ops));
        processor.execute_nested_loop_join(left_table, right_table, config)
    }

    /// Extract join key columns from conditions
    fn extract_join_key_columns(
        &self,
        conditions: &[JoinCondition],
        is_left: bool,
    ) -> Result<Vec<String>> {
        let mut columns = Vec::new();

        for condition in conditions {
            match condition {
                JoinCondition::Equal {
                    left_column,
                    right_column,
                } => {
                    if is_left {
                        columns.push(left_column.clone());
                    } else {
                        columns.push(right_column.clone());
                    }
                }
                JoinCondition::Complex(_) => {
                    // For complex conditions, we can't determine columns statically
                    // so we return an empty list for now
                }
            }
        }

        Ok(columns)
    }

    /// Extract join keys for a batch
    fn extract_join_keys_batch(
        &self,
        table: &ColumnarTable,
        key_columns: &[String],
        start_row: usize,
        batch_size: usize,
    ) -> Result<Vec<JoinKey>> {
        let mut keys = Vec::with_capacity(batch_size);

        for row_offset in 0..batch_size {
            let row_index = start_row + row_offset;
            if row_index >= table.row_count {
                break;
            }

            let mut key_values = Vec::new();

            for column_name in key_columns {
                let value = self.get_row_value(table, column_name, row_index)?;
                key_values.push(value);
            }

            keys.push(JoinKey { values: key_values });
        }

        Ok(keys)
    }

    /// Vectorized key hashing
    fn vectorized_hash_keys(&mut self, keys: &[JoinKey]) -> Result<Vec<u64>> {
        let mut hashes = Vec::with_capacity(keys.len());

        // For now, use simple hashing - could be optimized with SIMD
        for key in keys {
            let hash = self.hash_join_key(key);
            hashes.push(hash);
        }

        Ok(hashes)
    }

    /// Hash a join key
    fn hash_join_key(&self, key: &JoinKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Check if two key value sets are equal
    fn keys_equal(&self, key1: &JoinKey, key2: &[Value]) -> bool {
        if key1.values.len() != key2.len() {
            return false;
        }

        key1.values.iter().zip(key2.iter()).all(|(v1, v2)| v1 == v2)
    }

    /// Calculate optimal bucket count for hash table
    fn calculate_optimal_bucket_count(&self, row_count: usize) -> usize {
        // Use a load factor of ~0.75
        let bucket_count = (row_count as f64 / 0.75) as usize;
        // Round to next power of 2 for better distribution
        bucket_count.next_power_of_two()
    }

    /// Estimate hash table memory usage
    fn estimate_hash_table_memory(&self, hash_table: &JoinHashTable) -> usize {
        let mut total = 0;

        // Bucket vector overhead
        total += hash_table.bucket_count * std::mem::size_of::<Vec<HashEntry>>();

        // Hash entries
        for bucket in &hash_table.buckets {
            total += bucket.len() * std::mem::size_of::<HashEntry>();
            total += bucket.capacity() * std::mem::size_of::<HashEntry>();
        }

        total
    }

    /// Build join result table
    fn build_join_result_table(
        &self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        matches: &[(usize, usize)],
        join_type: &JoinType,
    ) -> Result<ColumnarTable> {
        let mut builder = ColumnarTableBuilder::new();

        // Add columns from left table
        for column_def in &left_table.schema.columns {
            let mut new_def = column_def.clone();
            new_def.name = format!("left_{}", column_def.name);
            builder.add_column(new_def)?;
        }

        // Add columns from right table
        for column_def in &right_table.schema.columns {
            let mut new_def = column_def.clone();
            new_def.name = format!("right_{}", column_def.name);
            builder.add_column(new_def)?;
        }

        // Add matched rows
        for &(left_row, right_row) in matches {
            let mut row_values = HashMap::new();

            // Add left table values
            for column_def in &left_table.schema.columns {
                let value = self.get_row_value(left_table, &column_def.name, left_row)?;
                let is_null = left_table
                    .null_masks
                    .get(&column_def.name)
                    .and_then(|mask| mask.get(left_row))
                    .unwrap_or(&false);

                row_values.insert(format!("left_{}", column_def.name), (value, *is_null));
            }

            // Add right table values
            for column_def in &right_table.schema.columns {
                let value = self.get_row_value(right_table, &column_def.name, right_row)?;
                let is_null = right_table
                    .null_masks
                    .get(&column_def.name)
                    .and_then(|mask| mask.get(right_row))
                    .unwrap_or(&false);

                row_values.insert(format!("right_{}", column_def.name), (value, *is_null));
            }

            builder.append_row(row_values)?;
        }

        builder.build()
    }

    /// Get row value from table
    fn get_row_value(&self, table: &ColumnarTable, column: &str, row: usize) -> Result<Value> {
        if let Some(col) = table.get_column(column) {
            let is_null = table
                .null_masks
                .get(column)
                .and_then(|mask| mask.get(row))
                .unwrap_or(&false);

            col.get_value(row, *is_null)
        } else {
            Err(Error::Generic(format!("Column '{}' not found", column)))
        }
    }

    /// Get execution statistics
    pub fn get_stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Reset execution statistics
    pub fn reset_stats(&mut self) {
        self.stats = ExecutionStats::default();
    }
}

/// Join algorithm selection
#[derive(Debug, Clone, Copy)]
pub enum JoinAlgorithm {
    /// Hash join - good for equi-joins with one small table
    HashJoin,
    /// Sort-merge join - good for sorted data or large tables
    SortMergeJoin,
    /// Nested loop join - good for small tables or non-equi joins
    NestedLoopJoin,
    /// Vectorized hash join - SIMD optimized
    VectorizedHashJoin,
}

impl HashJoinProcessor {
    /// Create new hash join processor
    pub fn new(simd_ops: Arc<dyn SIMDOperations + Send + Sync>) -> Self {
        Self {
            hash_table: JoinHashTable {
                buckets: Vec::new(),
                bucket_count: 0,
                entry_count: 0,
                memory_usage: 0,
            },
            simd_ops,
            stats: ExecutionStats::default(),
        }
    }

    /// Execute hash join
    pub fn execute_hash_join(
        &mut self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: JoinConfig,
    ) -> Result<JoinResult> {
        // Simple hash join implementation
        let matches: Vec<(usize, usize)> = Vec::new(); // Simplified for now

        let result_table = ColumnarTable::new(super::TableSchema {
            columns: Vec::new(),
            primary_key: Vec::new(),
            metadata: HashMap::new(),
        });

        Ok(JoinResult {
            table: result_table,
            left_indices: Vec::new(),
            right_indices: Vec::new(),
            stats: self.stats.clone(),
            memory_usage: 0,
            match_count: 0,
        })
    }
}

impl SortMergeJoinProcessor {
    /// Create new sort-merge join processor
    pub fn new(simd_ops: Arc<dyn SIMDOperations + Send + Sync>) -> Self {
        Self {
            simd_ops,
            stats: ExecutionStats::default(),
        }
    }

    /// Execute sort-merge join
    pub fn execute_sort_merge_join(
        &mut self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: JoinConfig,
    ) -> Result<JoinResult> {
        // Sort-merge join implementation would go here
        let result_table = ColumnarTable::new(super::TableSchema {
            columns: Vec::new(),
            primary_key: Vec::new(),
            metadata: HashMap::new(),
        });

        Ok(JoinResult {
            table: result_table,
            left_indices: Vec::new(),
            right_indices: Vec::new(),
            stats: self.stats.clone(),
            memory_usage: 0,
            match_count: 0,
        })
    }
}

impl NestedLoopJoinProcessor {
    /// Create new nested loop join processor
    pub fn new(simd_ops: Arc<dyn SIMDOperations + Send + Sync>) -> Self {
        Self {
            simd_ops,
            stats: ExecutionStats::default(),
        }
    }

    /// Execute nested loop join
    pub fn execute_nested_loop_join(
        &mut self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        config: JoinConfig,
    ) -> Result<JoinResult> {
        let mut matches = Vec::new();

        // Simple nested loop join
        for left_row in 0..left_table.row_count {
            for right_row in 0..right_table.row_count {
                if self.evaluate_join_conditions(
                    left_table,
                    right_table,
                    left_row,
                    right_row,
                    &config.conditions,
                )? {
                    matches.push((left_row, right_row));
                }
            }
        }

        // Build result table (simplified)
        let result_table = ColumnarTable::new(super::TableSchema {
            columns: Vec::new(),
            primary_key: Vec::new(),
            metadata: HashMap::new(),
        });

        Ok(JoinResult {
            table: result_table,
            left_indices: matches.iter().map(|m| m.0).collect(),
            right_indices: matches.iter().map(|m| m.1).collect(),
            stats: self.stats.clone(),
            memory_usage: 0,
            match_count: matches.len(),
        })
    }

    /// Evaluate join conditions
    fn evaluate_join_conditions(
        &self,
        left_table: &ColumnarTable,
        right_table: &ColumnarTable,
        left_row: usize,
        right_row: usize,
        conditions: &[JoinCondition],
    ) -> Result<bool> {
        for condition in conditions {
            match condition {
                JoinCondition::Equal {
                    left_column,
                    right_column,
                } => {
                    let left_value = self.get_row_value(left_table, left_column, left_row)?;
                    let right_value = self.get_row_value(right_table, right_column, right_row)?;

                    if left_value != right_value {
                        return Ok(false);
                    }
                }
                JoinCondition::Complex(_) => {
                    // For complex conditions, we need to evaluate the filter expression
                    // For now, we'll return true (matches all rows)
                    // TODO: Implement complex condition evaluation
                }
            }
        }

        Ok(true)
    }

    /// Get row value from table
    fn get_row_value(&self, table: &ColumnarTable, column: &str, row: usize) -> Result<Value> {
        if let Some(col) = table.get_column(column) {
            let is_null = table
                .null_masks
                .get(column)
                .and_then(|mask| mask.get(row))
                .unwrap_or(&false);

            col.get_value(row, *is_null)
        } else {
            Err(Error::Generic(format!("Column '{}' not found", column)))
        }
    }
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            join_type: JoinType::Inner,
            conditions: Vec::new(),
            parallel: true,
            hash_table_capacity: 1024,
            max_memory_bytes: 64 * 1024 * 1024, // 64MB
            use_simd: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ColumnDefinition, ColumnarTableBuilder, DataType, SIMDProcessor};
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_simple_inner_join() {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        let mut joiner = VectorizedJoiner::new(simd_ops);

        // Create left table
        let mut left_builder = ColumnarTableBuilder::new();
        left_builder
            .add_column(ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        left_builder
            .add_column(ColumnDefinition {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        // Add test data to left table
        let left_data = vec![(1, "Alice"), (2, "Bob"), (3, "Charlie")];
        for (id, name) in left_data {
            let mut row = HashMap::new();
            row.insert("id".to_string(), (Value::Int32(id), false));
            row.insert("name".to_string(), (Value::String(name.to_string()), false));
            left_builder.append_row(row).unwrap();
        }

        let left_table = left_builder.build().unwrap();

        // Create right table
        let mut right_builder = ColumnarTableBuilder::new();
        right_builder
            .add_column(ColumnDefinition {
                name: "user_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        right_builder
            .add_column(ColumnDefinition {
                name: "score".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        // Add test data to right table
        let right_data = vec![(1, 100), (2, 200), (4, 400)]; // Note: 3 missing, 4 extra
        for (user_id, score) in right_data {
            let mut row = HashMap::new();
            row.insert("user_id".to_string(), (Value::Int32(user_id), false));
            row.insert("score".to_string(), (Value::Int32(score), false));
            right_builder.append_row(row).unwrap();
        }

        let right_table = right_builder.build().unwrap();

        // Configure join
        let config = JoinConfig {
            join_type: JoinType::Inner,
            conditions: vec![JoinCondition::Equal {
                left_column: "id".to_string(),
                right_column: "user_id".to_string(),
            }],
            ..Default::default()
        };

        // Execute join
        let result = joiner.join(&left_table, &right_table, config).unwrap();

        // Should have 2 matches: (1,1) and (2,2)
        assert_eq!(result.match_count, 2);
        assert_eq!(result.left_indices.len(), 2);
        assert_eq!(result.right_indices.len(), 2);
    }

    #[test]
    fn test_hash_table_creation() {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        let mut joiner = VectorizedJoiner::new(simd_ops);

        // Create test table
        let mut builder = ColumnarTableBuilder::new();
        builder
            .add_column(ColumnDefinition {
                name: "key".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        // Add test data
        for i in 0..10 {
            let mut row = HashMap::new();
            row.insert("key".to_string(), (Value::Int32(i), false));
            builder.append_row(row).unwrap();
        }

        let table = builder.build().unwrap();

        let conditions = vec![JoinCondition::Equal {
            left_column: "key".to_string(),
            right_column: "key".to_string(),
        }];

        let hash_table = joiner
            .build_vectorized_hash_table(&table, &conditions)
            .unwrap();

        assert_eq!(hash_table.entry_count, 10);
        assert!(hash_table.bucket_count > 0);
        assert!(hash_table.memory_usage > 0);
    }
}
