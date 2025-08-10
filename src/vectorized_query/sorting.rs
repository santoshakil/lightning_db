//! Vectorized Sorting Operations
//!
//! High-performance sorting implementations using SIMD instructions
//! and cache-efficient algorithms for columnar data.

use super::{
    ColumnarTable, DataType, ExecutionStats, SIMDOperations, SortColumn, SortDirection, Value,
    VECTOR_BATCH_SIZE,
};
use crate::{Error, Result};
use std::cmp::Ordering;
use std::sync::Arc;

/// Vectorized sorting executor
pub struct VectorizedSorter {
    /// SIMD operations implementation
    simd_ops: Arc<dyn SIMDOperations + Send + Sync>,
    /// Execution statistics
    stats: ExecutionStats,
}

/// Sort key for multi-column sorting
#[derive(Debug, Clone)]
pub struct SortKey {
    /// Values for each sort column in order
    pub values: Vec<Value>,
    /// Original row index
    pub row_index: usize,
}

/// Sorting configuration
#[derive(Debug, Clone)]
pub struct SortConfig {
    /// Columns to sort by
    pub sort_columns: Vec<SortColumn>,
    /// Maximum rows to sort in memory
    pub max_rows_in_memory: usize,
    /// Use parallel sorting
    pub parallel: bool,
    /// Use stable sort (preserves relative order of equal elements)
    pub stable: bool,
    /// SIMD optimization level
    pub simd_level: SIMDLevel,
}

/// SIMD optimization level for sorting
#[derive(Debug, Clone, Copy)]
pub enum SIMDLevel {
    /// No SIMD optimizations
    None,
    /// Basic SIMD for comparisons
    Basic,
    /// Advanced SIMD with vectorized operations
    Advanced,
    /// Maximum SIMD optimization
    Maximum,
}

/// Sorting algorithm selection
#[derive(Debug, Clone, Copy)]
pub enum SortAlgorithm {
    /// Quick sort - O(n log n) average, good for general cases
    QuickSort,
    /// Merge sort - O(n log n) guaranteed, stable
    MergeSort,
    /// Heap sort - O(n log n) guaranteed, in-place
    HeapSort,
    /// Radix sort - O(kn) for integer keys, very fast
    RadixSort,
    /// Counting sort - O(n+k) for small ranges
    CountingSort,
    /// Vectorized merge sort - SIMD optimized
    VectorizedMergeSort,
}

/// Sorting result
#[derive(Debug)]
pub struct SortResult {
    /// Sorted row indices
    pub row_indices: Vec<usize>,
    /// Execution statistics
    pub stats: ExecutionStats,
    /// Algorithm used
    pub algorithm_used: SortAlgorithm,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Whether spilling occurred
    pub spilled: bool,
}

/// Vectorized comparison result
#[derive(Debug)]
pub struct ComparisonResult {
    /// Comparison results (-1, 0, 1)
    pub results: Vec<i8>,
    /// Number of comparisons performed
    pub comparison_count: usize,
}

impl VectorizedSorter {
    /// Create new vectorized sorter
    pub fn new(simd_ops: Arc<dyn SIMDOperations + Send + Sync>) -> Self {
        Self {
            simd_ops,
            stats: ExecutionStats::default(),
        }
    }

    /// Sort table rows based on configuration
    pub fn sort(
        &mut self,
        table: &ColumnarTable,
        config: SortConfig,
        row_mask: Option<&[bool]>,
    ) -> Result<SortResult> {
        let active_rows = self.get_active_rows(table, row_mask);

        if active_rows.len() <= 1 {
            let memory_usage = self.estimate_memory_usage(&active_rows);
            return Ok(SortResult {
                row_indices: active_rows,
                stats: self.stats.clone(),
                algorithm_used: SortAlgorithm::QuickSort,
                memory_usage,
                spilled: false,
            });
        }

        // Choose optimal sorting algorithm
        let algorithm = self.choose_algorithm(&config, &active_rows, table)?;
        let memory_usage = self.estimate_memory_usage(&active_rows);

        // Perform sorting
        let sorted_indices = match algorithm {
            SortAlgorithm::VectorizedMergeSort => {
                self.vectorized_merge_sort(table, &config, active_rows)?
            }
            SortAlgorithm::RadixSort => self.radix_sort(table, &config, active_rows)?,
            SortAlgorithm::QuickSort => self.quick_sort(table, &config, active_rows)?,
            SortAlgorithm::MergeSort => self.merge_sort(table, &config, active_rows)?,
            _ => {
                // Fallback to quick sort
                self.quick_sort(table, &config, active_rows)?
            }
        };

        Ok(SortResult {
            row_indices: sorted_indices,
            stats: self.stats.clone(),
            algorithm_used: algorithm,
            memory_usage,
            spilled: false, // TODO: Implement spilling
        })
    }

    /// Get active rows based on row mask
    fn get_active_rows(&self, table: &ColumnarTable, row_mask: Option<&[bool]>) -> Vec<usize> {
        if let Some(mask) = row_mask {
            (0..table.row_count)
                .filter(|&i| i < mask.len() && mask[i])
                .collect()
        } else {
            (0..table.row_count).collect()
        }
    }

    /// Choose optimal sorting algorithm based on data characteristics
    fn choose_algorithm(
        &self,
        config: &SortConfig,
        rows: &[usize],
        table: &ColumnarTable,
    ) -> Result<SortAlgorithm> {
        // For single column integer sorts with small range, use radix sort
        if config.sort_columns.len() == 1 {
            let col_name = &config.sort_columns[0].column;
            if let Some(column) = table.get_column(col_name) {
                match column.definition.data_type {
                    DataType::Int32 | DataType::Int64 => {
                        if rows.len() > 1000 && self.is_suitable_for_radix(column, rows)? {
                            return Ok(SortAlgorithm::RadixSort);
                        }
                    }
                    _ => {}
                }
            }
        }

        // For small datasets, use insertion sort (implemented as quick sort)
        if rows.len() < 32 {
            return Ok(SortAlgorithm::QuickSort);
        }

        // For stable sorts, use merge sort
        if config.stable {
            return Ok(SortAlgorithm::MergeSort);
        }

        // For SIMD optimization, use vectorized merge sort
        if matches!(config.simd_level, SIMDLevel::Advanced | SIMDLevel::Maximum) {
            return Ok(SortAlgorithm::VectorizedMergeSort);
        }

        // Default to quick sort
        Ok(SortAlgorithm::QuickSort)
    }

    /// Check if column is suitable for radix sort
    fn is_suitable_for_radix(&self, column: &super::Column, rows: &[usize]) -> Result<bool> {
        // For radix sort to be efficient, the range should be reasonable
        // This is a simplified check - in practice, we'd analyze the actual data distribution
        match column.definition.data_type {
            DataType::Int32 => {
                // Suitable if we have enough data points
                Ok(rows.len() > 1000)
            }
            DataType::Int64 => {
                // Less suitable for 64-bit integers due to memory usage
                Ok(rows.len() > 10000)
            }
            _ => Ok(false),
        }
    }

    /// Vectorized merge sort with SIMD optimizations
    fn vectorized_merge_sort(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        rows: Vec<usize>,
    ) -> Result<Vec<usize>> {
        if rows.len() <= 1 {
            return Ok(rows);
        }

        // For small arrays, use insertion sort
        if rows.len() < 32 {
            return self.insertion_sort(table, config, rows);
        }

        // Divide
        let mid = rows.len() / 2;
        let mut left = rows[..mid].to_vec();
        let mut right = rows[mid..].to_vec();

        // Conquer
        left = self.vectorized_merge_sort(table, config, left)?;
        right = self.vectorized_merge_sort(table, config, right)?;

        // Merge with vectorized comparisons
        self.vectorized_merge(table, config, left, right)
    }

    /// Vectorized merge operation
    fn vectorized_merge(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        left: Vec<usize>,
        right: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let mut result = Vec::with_capacity(left.len() + right.len());
        let mut left_idx = 0;
        let mut right_idx = 0;

        // Use vectorized batch comparisons when possible
        while left_idx < left.len() && right_idx < right.len() {
            // Determine batch size for vectorized comparison
            let batch_size = VECTOR_BATCH_SIZE
                .min(left.len() - left_idx)
                .min(right.len() - right_idx);

            if batch_size >= 4 && config.sort_columns.len() == 1 {
                // Use vectorized comparison for single-column sorts
                let comparison_result = self.vectorized_batch_compare(
                    table,
                    &config.sort_columns[0],
                    &left[left_idx..left_idx + batch_size],
                    &right[right_idx..right_idx + batch_size],
                )?;

                // Process comparison results
                for &cmp in &comparison_result.results {
                    if cmp <= 0 {
                        result.push(left[left_idx]);
                        left_idx += 1;
                    } else {
                        result.push(right[right_idx]);
                        right_idx += 1;
                    }

                    if left_idx >= left.len() || right_idx >= right.len() {
                        break;
                    }
                }

                self.stats.simd_operations += 1;
            } else {
                // Fall back to scalar comparison
                let cmp = self.compare_rows(table, config, left[left_idx], right[right_idx])?;
                if cmp <= 0 {
                    result.push(left[left_idx]);
                    left_idx += 1;
                } else {
                    result.push(right[right_idx]);
                    right_idx += 1;
                }
            }
        }

        // Add remaining elements
        result.extend_from_slice(&left[left_idx..]);
        result.extend_from_slice(&right[right_idx..]);

        Ok(result)
    }

    /// Vectorized batch comparison
    fn vectorized_batch_compare(
        &mut self,
        table: &ColumnarTable,
        sort_column: &SortColumn,
        left_rows: &[usize],
        right_rows: &[usize],
    ) -> Result<ComparisonResult> {
        let column = table
            .get_column(&sort_column.column)
            .ok_or_else(|| Error::Generic(format!("Column '{}' not found", sort_column.column)))?;

        let mut results = Vec::with_capacity(left_rows.len().min(right_rows.len()));

        match column.definition.data_type {
            DataType::Int32 => {
                let left_values =
                    self.extract_int32_values(table, &sort_column.column, left_rows)?;
                let right_values =
                    self.extract_int32_values(table, &sort_column.column, right_rows)?;

                // Use SIMD comparison
                let comparison_mask = self.simd_ops.compare_vectors(
                    &left_values,
                    &right_values,
                    DataType::Int32,
                    super::ComparisonOperator::LessThanOrEqual,
                )?;

                for (i, &is_less_equal) in comparison_mask.iter().enumerate() {
                    if i >= left_rows.len() || i >= right_rows.len() {
                        break;
                    }

                    let result = if is_less_equal {
                        if sort_column.direction == SortDirection::Ascending {
                            -1
                        } else {
                            1
                        }
                    } else {
                        if sort_column.direction == SortDirection::Ascending {
                            1
                        } else {
                            -1
                        }
                    };

                    results.push(result);
                }
            }

            DataType::Float32 => {
                let left_values =
                    self.extract_float32_values(table, &sort_column.column, left_rows)?;
                let right_values =
                    self.extract_float32_values(table, &sort_column.column, right_rows)?;

                let comparison_mask = self.simd_ops.compare_vectors(
                    &left_values,
                    &right_values,
                    DataType::Float32,
                    super::ComparisonOperator::LessThanOrEqual,
                )?;

                for (i, &is_less_equal) in comparison_mask.iter().enumerate() {
                    if i >= left_rows.len() || i >= right_rows.len() {
                        break;
                    }

                    let result = if is_less_equal {
                        if sort_column.direction == SortDirection::Ascending {
                            -1
                        } else {
                            1
                        }
                    } else {
                        if sort_column.direction == SortDirection::Ascending {
                            1
                        } else {
                            -1
                        }
                    };

                    results.push(result);
                }
            }

            _ => {
                // Fall back to scalar comparison for unsupported types
                for i in 0..left_rows.len().min(right_rows.len()) {
                    let left_value =
                        self.get_row_value(table, &sort_column.column, left_rows[i])?;
                    let right_value =
                        self.get_row_value(table, &sort_column.column, right_rows[i])?;

                    let cmp = self.compare_values(&left_value, &right_value, sort_column.direction);
                    results.push(cmp as i8);
                }
            }
        }

        let comparison_count = results.len();
        Ok(ComparisonResult {
            results,
            comparison_count,
        })
    }

    /// Extract int32 values for vectorized operations
    fn extract_int32_values(
        &self,
        table: &ColumnarTable,
        column: &str,
        rows: &[usize],
    ) -> Result<Vec<u8>> {
        let mut values = Vec::new();

        for &row in rows {
            let value = self.get_row_value(table, column, row)?;
            match value {
                Value::Int32(v) => values.extend_from_slice(&v.to_le_bytes()),
                _ => return Err(Error::Generic("Expected Int32 value".to_string())),
            }
        }

        Ok(values)
    }

    /// Extract float32 values for vectorized operations
    fn extract_float32_values(
        &self,
        table: &ColumnarTable,
        column: &str,
        rows: &[usize],
    ) -> Result<Vec<u8>> {
        let mut values = Vec::new();

        for &row in rows {
            let value = self.get_row_value(table, column, row)?;
            match value {
                Value::Float32(v) => values.extend_from_slice(&v.to_le_bytes()),
                _ => return Err(Error::Generic("Expected Float32 value".to_string())),
            }
        }

        Ok(values)
    }

    /// Radix sort for integer columns
    fn radix_sort(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        rows: Vec<usize>,
    ) -> Result<Vec<usize>> {
        if config.sort_columns.len() != 1 {
            return Err(Error::Generic(
                "Radix sort only supports single column".to_string(),
            ));
        }

        let sort_column = &config.sort_columns[0];
        let column = table
            .get_column(&sort_column.column)
            .ok_or_else(|| Error::Generic(format!("Column '{}' not found", sort_column.column)))?;

        match column.definition.data_type {
            DataType::Int32 => self.radix_sort_i32(table, sort_column, rows),
            DataType::Int64 => self.radix_sort_i64(table, sort_column, rows),
            _ => Err(Error::Generic(
                "Radix sort only supports integer types".to_string(),
            )),
        }
    }

    /// Radix sort for 32-bit integers
    fn radix_sort_i32(
        &mut self,
        table: &ColumnarTable,
        sort_column: &SortColumn,
        rows: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let mut current = rows;
        let mut temp = vec![0; current.len()];

        // Sort by each byte (4 passes for 32-bit integers)
        for byte_idx in 0..4 {
            let mut counts = [0usize; 256];

            // Count occurrences of each byte value
            for &row in &current {
                let value = self.get_row_value(table, &sort_column.column, row)?;
                if let Value::Int32(v) = value {
                    let byte_val = ((v as u32) >> (byte_idx * 8)) & 0xFF;
                    counts[byte_val as usize] += 1;
                }
            }

            // Calculate cumulative counts
            for i in 1..256 {
                counts[i] += counts[i - 1];
            }

            // Distribute elements based on current byte
            for &row in current.iter().rev() {
                let value = self.get_row_value(table, &sort_column.column, row)?;
                if let Value::Int32(v) = value {
                    let byte_val = ((v as u32) >> (byte_idx * 8)) & 0xFF;
                    counts[byte_val as usize] -= 1;
                    temp[counts[byte_val as usize]] = row;
                }
            }

            // Swap buffers
            std::mem::swap(&mut current, &mut temp);
        }

        // Reverse if descending
        if sort_column.direction == SortDirection::Descending {
            current.reverse();
        }

        self.stats.simd_operations += 4; // 4 passes

        Ok(current)
    }

    /// Radix sort for 64-bit integers
    fn radix_sort_i64(
        &mut self,
        table: &ColumnarTable,
        sort_column: &SortColumn,
        rows: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let mut current = rows;
        let mut temp = vec![0; current.len()];

        // Sort by each byte (8 passes for 64-bit integers)
        for byte_idx in 0..8 {
            let mut counts = [0usize; 256];

            // Count occurrences of each byte value
            for &row in &current {
                let value = self.get_row_value(table, &sort_column.column, row)?;
                if let Value::Int64(v) = value {
                    let byte_val = ((v as u64) >> (byte_idx * 8)) & 0xFF;
                    counts[byte_val as usize] += 1;
                }
            }

            // Calculate cumulative counts
            for i in 1..256 {
                counts[i] += counts[i - 1];
            }

            // Distribute elements based on current byte
            for &row in current.iter().rev() {
                let value = self.get_row_value(table, &sort_column.column, row)?;
                if let Value::Int64(v) = value {
                    let byte_val = ((v as u64) >> (byte_idx * 8)) & 0xFF;
                    counts[byte_val as usize] -= 1;
                    temp[counts[byte_val as usize]] = row;
                }
            }

            // Swap buffers
            std::mem::swap(&mut current, &mut temp);
        }

        // Reverse if descending
        if sort_column.direction == SortDirection::Descending {
            current.reverse();
        }

        self.stats.simd_operations += 8; // 8 passes

        Ok(current)
    }

    /// Quick sort implementation
    fn quick_sort(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        mut rows: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let len = rows.len();
        if len <= 1 {
            return Ok(rows);
        }

        self.quick_sort_recursive(table, config, &mut rows, 0, len - 1)?;
        Ok(rows)
    }

    /// Recursive quick sort
    fn quick_sort_recursive(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        rows: &mut [usize],
        low: usize,
        high: usize,
    ) -> Result<()> {
        if low < high {
            let pivot = self.partition(table, config, rows, low, high)?;

            if pivot > 0 {
                self.quick_sort_recursive(table, config, rows, low, pivot - 1)?;
            }
            if pivot < high {
                self.quick_sort_recursive(table, config, rows, pivot + 1, high)?;
            }
        }

        Ok(())
    }

    /// Partition function for quick sort
    fn partition(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        rows: &mut [usize],
        low: usize,
        high: usize,
    ) -> Result<usize> {
        let pivot_row = rows[high];
        let mut i = low;

        for j in low..high {
            let cmp = self.compare_rows(table, config, rows[j], pivot_row)?;
            if cmp <= 0 {
                rows.swap(i, j);
                i += 1;
            }
        }

        rows.swap(i, high);
        Ok(i)
    }

    /// Merge sort implementation
    fn merge_sort(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        rows: Vec<usize>,
    ) -> Result<Vec<usize>> {
        if rows.len() <= 1 {
            return Ok(rows);
        }

        // For small arrays, use insertion sort
        if rows.len() < 32 {
            return self.insertion_sort(table, config, rows);
        }

        // Divide
        let mid = rows.len() / 2;
        let left = rows[..mid].to_vec();
        let right = rows[mid..].to_vec();

        // Conquer
        let left_sorted = self.merge_sort(table, config, left)?;
        let right_sorted = self.merge_sort(table, config, right)?;

        // Merge
        self.merge(table, config, left_sorted, right_sorted)
    }

    /// Merge operation for merge sort
    fn merge(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        left: Vec<usize>,
        right: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let mut result = Vec::with_capacity(left.len() + right.len());
        let mut left_idx = 0;
        let mut right_idx = 0;

        while left_idx < left.len() && right_idx < right.len() {
            let cmp = self.compare_rows(table, config, left[left_idx], right[right_idx])?;
            if cmp <= 0 {
                result.push(left[left_idx]);
                left_idx += 1;
            } else {
                result.push(right[right_idx]);
                right_idx += 1;
            }
        }

        // Add remaining elements
        result.extend_from_slice(&left[left_idx..]);
        result.extend_from_slice(&right[right_idx..]);

        Ok(result)
    }

    /// Insertion sort for small arrays
    fn insertion_sort(
        &mut self,
        table: &ColumnarTable,
        config: &SortConfig,
        mut rows: Vec<usize>,
    ) -> Result<Vec<usize>> {
        for i in 1..rows.len() {
            let key = rows[i];
            let mut j = i;

            while j > 0 {
                let cmp = self.compare_rows(table, config, rows[j - 1], key)?;
                if cmp <= 0 {
                    break;
                }
                rows[j] = rows[j - 1];
                j -= 1;
            }

            rows[j] = key;
        }

        Ok(rows)
    }

    /// Compare two rows based on sort configuration
    fn compare_rows(
        &self,
        table: &ColumnarTable,
        config: &SortConfig,
        row1: usize,
        row2: usize,
    ) -> Result<i32> {
        for sort_column in &config.sort_columns {
            let value1 = self.get_row_value(table, &sort_column.column, row1)?;
            let value2 = self.get_row_value(table, &sort_column.column, row2)?;

            let cmp = self.compare_values(&value1, &value2, sort_column.direction);
            if cmp != 0 {
                return Ok(cmp);
            }
        }

        Ok(0)
    }

    /// Compare two values with direction
    fn compare_values(&self, value1: &Value, value2: &Value, direction: SortDirection) -> i32 {
        let cmp = match (value1, value2) {
            (Value::Null, Value::Null) => 0,
            (Value::Null, _) => -1, // NULLs sort first
            (_, Value::Null) => 1,

            (Value::Int32(a), Value::Int32(b)) => a.cmp(b) as i32,
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b) as i32,
            (Value::Float32(a), Value::Float32(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal) as i32
            }
            (Value::Float64(a), Value::Float64(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal) as i32
            }
            (Value::String(a), Value::String(b)) => a.cmp(b) as i32,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b) as i32,
            (Value::Date(a), Value::Date(b)) => a.cmp(b) as i32,
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b) as i32,

            _ => 0, // Different types compare as equal
        };

        match direction {
            SortDirection::Ascending => cmp,
            SortDirection::Descending => -cmp,
        }
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

    /// Estimate memory usage
    fn estimate_memory_usage(&self, rows: &[usize]) -> usize {
        rows.len() * std::mem::size_of::<usize>() * 2 // Rough estimate for temporary arrays
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

impl Default for SortConfig {
    fn default() -> Self {
        Self {
            sort_columns: Vec::new(),
            max_rows_in_memory: 1_000_000,
            parallel: true,
            stable: false,
            simd_level: SIMDLevel::Advanced,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ColumnDefinition, ColumnarTableBuilder, SIMDProcessor};
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_simple_sort() {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        let mut sorter = VectorizedSorter::new(simd_ops);

        // Create test table
        let mut builder = ColumnarTableBuilder::new();
        builder
            .add_column(ColumnDefinition {
                name: "values".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        // Add test data in reverse order
        let data = vec![5, 3, 8, 1, 9, 2];
        for value in data {
            let mut row = HashMap::new();
            row.insert("values".to_string(), (Value::Int32(value), false));
            builder.append_row(row).unwrap();
        }

        let table = builder.build().unwrap();

        let config = SortConfig {
            sort_columns: vec![SortColumn {
                column: "values".to_string(),
                direction: SortDirection::Ascending,
                nulls_first: false,
            }],
            ..Default::default()
        };

        let result = sorter.sort(&table, config, None).unwrap();

        // Verify sorted order: should be [3, 5, 0, 1, 4, 2] (indices of original positions)
        // Values at these indices: [1, 2, 3, 5, 8, 9]
        assert_eq!(result.row_indices.len(), 6);

        // Check that values are in ascending order
        for i in 1..result.row_indices.len() {
            let prev_idx = result.row_indices[i - 1];
            let curr_idx = result.row_indices[i];

            let prev_val = sorter.get_row_value(&table, "values", prev_idx).unwrap();
            let curr_val = sorter.get_row_value(&table, "values", curr_idx).unwrap();

            if let (Value::Int32(prev), Value::Int32(curr)) = (prev_val, curr_val) {
                assert!(
                    prev <= curr,
                    "Values not in ascending order: {} > {}",
                    prev,
                    curr
                );
            }
        }
    }

    #[test]
    fn test_multi_column_sort() {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        let mut sorter = VectorizedSorter::new(simd_ops);

        // Create test table
        let mut builder = ColumnarTableBuilder::new();
        builder
            .add_column(ColumnDefinition {
                name: "group".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        builder
            .add_column(ColumnDefinition {
                name: "value".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        // Add test data: (group, value)
        let data = vec![(2, 10), (1, 20), (2, 5), (1, 15)];
        for (group, value) in data {
            let mut row = HashMap::new();
            row.insert("group".to_string(), (Value::Int32(group), false));
            row.insert("value".to_string(), (Value::Int32(value), false));
            builder.append_row(row).unwrap();
        }

        let table = builder.build().unwrap();

        let config = SortConfig {
            sort_columns: vec![
                SortColumn {
                    column: "group".to_string(),
                    direction: SortDirection::Ascending,
                    nulls_first: false,
                },
                SortColumn {
                    column: "value".to_string(),
                    direction: SortDirection::Ascending,
                    nulls_first: false,
                },
            ],
            ..Default::default()
        };

        let result = sorter.sort(&table, config, None).unwrap();

        // Expected order: (1,15), (1,20), (2,5), (2,10)
        assert_eq!(result.row_indices.len(), 4);

        // Verify order
        let expected = vec![(1, 15), (1, 20), (2, 5), (2, 10)];
        for (i, &(exp_group, exp_value)) in expected.iter().enumerate() {
            let row_idx = result.row_indices[i];
            let group_val = sorter.get_row_value(&table, "group", row_idx).unwrap();
            let value_val = sorter.get_row_value(&table, "value", row_idx).unwrap();

            if let (Value::Int32(group), Value::Int32(value)) = (group_val, value_val) {
                assert_eq!(group, exp_group);
                assert_eq!(value, exp_value);
            }
        }
    }

    #[test]
    fn test_descending_sort() {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        let mut sorter = VectorizedSorter::new(simd_ops);

        // Create test table
        let mut builder = ColumnarTableBuilder::new();
        builder
            .add_column(ColumnDefinition {
                name: "values".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        // Add test data
        let data = vec![1, 2, 3, 4, 5];
        for value in data {
            let mut row = HashMap::new();
            row.insert("values".to_string(), (Value::Int32(value), false));
            builder.append_row(row).unwrap();
        }

        let table = builder.build().unwrap();

        let config = SortConfig {
            sort_columns: vec![SortColumn {
                column: "values".to_string(),
                direction: SortDirection::Descending,
                nulls_first: false,
            }],
            ..Default::default()
        };

        let result = sorter.sort(&table, config, None).unwrap();

        // Verify descending order
        for i in 1..result.row_indices.len() {
            let prev_idx = result.row_indices[i - 1];
            let curr_idx = result.row_indices[i];

            let prev_val = sorter.get_row_value(&table, "values", prev_idx).unwrap();
            let curr_val = sorter.get_row_value(&table, "values", curr_idx).unwrap();

            if let (Value::Int32(prev), Value::Int32(curr)) = (prev_val, curr_val) {
                assert!(
                    prev >= curr,
                    "Values not in descending order: {} < {}",
                    prev,
                    curr
                );
            }
        }
    }
}
