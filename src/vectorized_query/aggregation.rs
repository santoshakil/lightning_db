//! Vectorized Aggregation Operations
//!
//! High-performance aggregation implementations using SIMD instructions
//! for operations like SUM, COUNT, MIN, MAX, AVG with grouping support.

use crate::{Result, Error};
use super::{
    DataType, Value, AggregateFunction, SIMDOperations, ColumnarTable, ColumnBatch,
    ExecutionStats, VECTOR_BATCH_SIZE
};
use std::collections::HashMap;
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Vectorized aggregation executor
pub struct VectorizedAggregator {
    /// SIMD operations implementation
    simd_ops: Arc<dyn SIMDOperations + Send + Sync>,
    /// Execution statistics
    stats: ExecutionStats,
}

/// Aggregation state for a single group
#[derive(Debug, Clone)]
pub struct AggregationState {
    /// Group key hash
    pub group_hash: u64,
    /// Group key values
    pub group_values: HashMap<String, Value>,
    /// Aggregate results
    pub aggregates: HashMap<String, AggregateValue>,
    /// Number of rows in this group
    pub row_count: u64,
}

/// Single aggregate value with state
#[derive(Debug, Clone)]
pub struct AggregateValue {
    /// Current aggregate value
    pub value: Value,
    /// Additional state for complex aggregates
    pub state: AggregateState,
}

/// State for different aggregate functions
#[derive(Debug, Clone)]
pub enum AggregateState {
    /// COUNT: just count
    Count(u64),
    /// SUM: running sum
    Sum(f64),
    /// MIN: current minimum
    Min(f64),
    /// MAX: current maximum  
    Max(f64),
    /// AVG: sum and count
    Avg { sum: f64, count: u64 },
    /// STDDEV: sum, sum_of_squares, count
    StdDev { sum: f64, sum_of_squares: f64, count: u64 },
    /// Custom: arbitrary state
    Custom(HashMap<String, Value>),
}

/// Group key for aggregation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    /// Column values that define the group
    pub values: Vec<Value>,
}

/// Aggregation configuration
#[derive(Debug, Clone)]
pub struct AggregationConfig {
    /// Functions to compute
    pub functions: Vec<AggregateFunction>,
    /// Columns to group by
    pub group_by_columns: Vec<String>,
    /// Maximum number of groups before spilling
    pub max_groups_in_memory: usize,
    /// Use parallel aggregation
    pub parallel: bool,
    /// Hash table initial capacity
    pub initial_capacity: usize,
}

/// Vectorized aggregation result
#[derive(Debug)]
pub struct AggregationResult {
    /// Grouped results
    pub groups: HashMap<GroupKey, AggregationState>,
    /// Execution statistics
    pub stats: ExecutionStats,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Number of unique groups
    pub group_count: usize,
}

impl VectorizedAggregator {
    /// Create new vectorized aggregator
    pub fn new(simd_ops: Arc<dyn SIMDOperations + Send + Sync>) -> Self {
        Self {
            simd_ops,
            stats: ExecutionStats::default(),
        }
    }
    
    /// Execute aggregation over table
    pub fn aggregate(
        &mut self,
        table: &ColumnarTable,
        config: AggregationConfig,
        row_mask: Option<&[bool]>,
    ) -> Result<AggregationResult> {
        if config.group_by_columns.is_empty() {
            self.aggregate_simple(table, &config.functions, row_mask)
        } else {
            self.aggregate_grouped(table, &config, row_mask)
        }
    }
    
    /// Simple aggregation without grouping
    fn aggregate_simple(
        &mut self,
        table: &ColumnarTable,
        functions: &[AggregateFunction],
        row_mask: Option<&[bool]>,
    ) -> Result<AggregationResult> {
        let mut group_state = AggregationState {
            group_hash: 0,
            group_values: HashMap::new(),
            aggregates: HashMap::new(),
            row_count: 0,
        };
        
        // Initialize aggregate states
        for func in functions {
            let agg_name = self.function_name(func);
            let initial_state = self.create_initial_state(func)?;
            group_state.aggregates.insert(agg_name, initial_state);
        }
        
        // Process data in batches
        let batch_size = VECTOR_BATCH_SIZE;
        let total_rows = table.row_count;
        let mut processed_rows = 0;
        
        while processed_rows < total_rows {
            let current_batch_size = (total_rows - processed_rows).min(batch_size);
            
            // Process each aggregate function
            for func in functions {
                let agg_name = self.function_name(func);
                let aggregate_state = group_state.aggregates.get_mut(&agg_name)
                    .ok_or_else(|| Error::Generic("Aggregate state not found".to_string()))?;
                
                self.process_batch_simple(
                    func,
                    aggregate_state,
                    table,
                    processed_rows,
                    current_batch_size,
                    row_mask,
                )?;
            }
            
            processed_rows += current_batch_size;
            self.stats.simd_operations += 1;
        }
        
        // Finalize aggregates
        self.finalize_aggregates(&mut group_state)?;
        
        let mut groups = HashMap::new();
        groups.insert(GroupKey { values: vec![] }, group_state);
        
        let memory_usage = self.estimate_memory_usage(&groups);
        
        Ok(AggregationResult {
            groups,
            stats: self.stats.clone(),
            memory_usage,
            group_count: 1,
        })
    }
    
    /// Grouped aggregation
    fn aggregate_grouped(
        &mut self,
        table: &ColumnarTable,
        config: &AggregationConfig,
        row_mask: Option<&[bool]>,
    ) -> Result<AggregationResult> {
        let mut groups: HashMap<GroupKey, AggregationState> = HashMap::with_capacity(config.initial_capacity);
        
        // Process data in batches
        let batch_size = VECTOR_BATCH_SIZE;
        let total_rows = table.row_count;
        let mut processed_rows = 0;
        
        while processed_rows < total_rows {
            let current_batch_size = (total_rows - processed_rows).min(batch_size);
            
            // Get group key batches
            let group_keys = self.extract_group_keys(
                table,
                &config.group_by_columns,
                processed_rows,
                current_batch_size,
                row_mask,
            )?;
            
            // Process each row in the batch
            for (row_idx, group_key) in group_keys.into_iter().enumerate() {
                let global_row = processed_rows + row_idx;
                
                // Skip if row is filtered out
                if let Some(mask) = row_mask {
                    if global_row >= mask.len() || !mask[global_row] {
                        continue;
                    }
                }
                
                // Get or create group state
                let group_state = groups.entry(group_key.clone()).or_insert_with(|| {
                    let mut state = AggregationState {
                        group_hash: self.hash_group_key(&group_key),
                        group_values: HashMap::new(),
                        aggregates: HashMap::new(),
                        row_count: 0,
                    };
                    
                    // Initialize group values
                    for (i, col_name) in config.group_by_columns.iter().enumerate() {
                        if i < group_key.values.len() {
                            state.group_values.insert(col_name.clone(), group_key.values[i].clone());
                        }
                    }
                    
                    // Initialize aggregate states
                    for func in &config.functions {
                        let agg_name = self.function_name(func);
                        if let Ok(initial_state) = self.create_initial_state(func) {
                            state.aggregates.insert(agg_name, initial_state);
                        }
                    }
                    
                    state
                });
                
                // Update aggregates for this row
                for func in &config.functions {
                    let agg_name = self.function_name(func);
                    if let Some(aggregate_state) = group_state.aggregates.get_mut(&agg_name) {
                        self.update_aggregate_single_row(func, aggregate_state, table, global_row)?;
                    }
                }
                
                group_state.row_count += 1;
            }
            
            processed_rows += current_batch_size;
            self.stats.simd_operations += 1;
            
            // Check memory usage and spill if necessary
            if groups.len() > config.max_groups_in_memory {
                // TODO: Implement spilling to disk
                return Err(Error::Generic("Too many groups, spilling not implemented yet".to_string()));
            }
        }
        
        // Finalize all group aggregates
        for group_state in groups.values_mut() {
            self.finalize_aggregates(group_state)?;
        }
        
        Ok(AggregationResult {
            memory_usage: self.estimate_memory_usage(&groups),
            group_count: groups.len(),
            groups,
            stats: self.stats.clone(),
        })
    }
    
    /// Process a batch for simple aggregation
    fn process_batch_simple(
        &mut self,
        function: &AggregateFunction,
        aggregate_state: &mut AggregateValue,
        table: &ColumnarTable,
        start_row: usize,
        batch_size: usize,
        row_mask: Option<&[bool]>,
    ) -> Result<()> {
        match function {
            AggregateFunction::Count => {
                let mut count = 0u64;
                for i in 0..batch_size {
                    let global_row = start_row + i;
                    if let Some(mask) = row_mask {
                        if global_row < mask.len() && mask[global_row] {
                            count += 1;
                        }
                    } else {
                        count += 1;
                    }
                }
                
                if let AggregateState::Count(ref mut current_count) = aggregate_state.state {
                    *current_count += count;
                    aggregate_state.value = Value::Int64(*current_count as i64);
                }
            }
            
            AggregateFunction::Sum(column) => {
                let sum = self.compute_vectorized_sum(table, column, start_row, batch_size, row_mask)?;
                
                if let AggregateState::Sum(ref mut current_sum) = aggregate_state.state {
                    *current_sum += sum;
                    aggregate_state.value = Value::Float64(*current_sum);
                }
            }
            
            AggregateFunction::Min(column) => {
                let min = self.compute_vectorized_min(table, column, start_row, batch_size, row_mask)?;
                
                if let AggregateState::Min(ref mut current_min) = aggregate_state.state {
                    *current_min = current_min.min(min);
                    aggregate_state.value = Value::Float64(*current_min);
                }
            }
            
            AggregateFunction::Max(column) => {
                let max = self.compute_vectorized_max(table, column, start_row, batch_size, row_mask)?;
                
                if let AggregateState::Max(ref mut current_max) = aggregate_state.state {
                    *current_max = current_max.max(max);
                    aggregate_state.value = Value::Float64(*current_max);
                }
            }
            
            AggregateFunction::Avg(column) => {
                let sum = self.compute_vectorized_sum(table, column, start_row, batch_size, row_mask)?;
                let count = self.count_non_null_rows(table, column, start_row, batch_size, row_mask)?;
                
                if let AggregateState::Avg { sum: ref mut current_sum, count: ref mut current_count } = aggregate_state.state {
                    *current_sum += sum;
                    *current_count += count;
                    
                    if *current_count > 0 {
                        aggregate_state.value = Value::Float64(*current_sum / *current_count as f64);
                    }
                }
            }
            
            _ => return Err(Error::Generic("Aggregate function not implemented".to_string())),
        }
        
        Ok(())
    }
    
    /// Compute vectorized sum using SIMD
    fn compute_vectorized_sum(
        &mut self,
        table: &ColumnarTable,
        column: &str,
        start_row: usize,
        batch_size: usize,
        row_mask: Option<&[bool]>,
    ) -> Result<f64> {
        let batch = table.get_column_batch(column, start_row, batch_size)?;
        
        if batch.data.is_empty() {
            return Ok(0.0);
        }
        
        // Filter data based on row mask
        let filtered_data = self.filter_batch_data(&batch, row_mask, start_row)?;
        
        if filtered_data.is_empty() {
            return Ok(0.0);
        }
        
        // Use SIMD sum operation
        let sum = self.simd_ops.sum_vector(&filtered_data, batch.data_type)?;
        Ok(sum)
    }
    
    /// Compute vectorized min using SIMD
    fn compute_vectorized_min(
        &mut self,
        table: &ColumnarTable,
        column: &str,
        start_row: usize,
        batch_size: usize,
        row_mask: Option<&[bool]>,
    ) -> Result<f64> {
        let batch = table.get_column_batch(column, start_row, batch_size)?;
        
        if batch.data.is_empty() {
            return Ok(f64::INFINITY);
        }
        
        let filtered_data = self.filter_batch_data(&batch, row_mask, start_row)?;
        
        if filtered_data.is_empty() {
            return Ok(f64::INFINITY);
        }
        
        let min = self.simd_ops.min_vector(&filtered_data, batch.data_type)?;
        Ok(min)
    }
    
    /// Compute vectorized max using SIMD
    fn compute_vectorized_max(
        &mut self,
        table: &ColumnarTable,
        column: &str,
        start_row: usize,
        batch_size: usize,
        row_mask: Option<&[bool]>,
    ) -> Result<f64> {
        let batch = table.get_column_batch(column, start_row, batch_size)?;
        
        if batch.data.is_empty() {
            return Ok(f64::NEG_INFINITY);
        }
        
        let filtered_data = self.filter_batch_data(&batch, row_mask, start_row)?;
        
        if filtered_data.is_empty() {
            return Ok(f64::NEG_INFINITY);
        }
        
        let max = self.simd_ops.max_vector(&filtered_data, batch.data_type)?;
        Ok(max)
    }
    
    /// Count non-null rows in batch
    fn count_non_null_rows(
        &self,
        table: &ColumnarTable,
        column: &str,
        start_row: usize,
        batch_size: usize,
        row_mask: Option<&[bool]>,
    ) -> Result<u64> {
        let batch = table.get_column_batch(column, start_row, batch_size)?;
        let mut count = 0u64;
        
        for i in 0..batch.size {
            let global_row = start_row + i;
            let is_null = batch.null_mask.get(i).unwrap_or(&false);
            let include_row = if let Some(mask) = row_mask {
                global_row < mask.len() && mask[global_row]
            } else {
                true
            };
            
            if include_row && !is_null {
                count += 1;
            }
        }
        
        Ok(count)
    }
    
    /// Filter batch data based on row mask
    fn filter_batch_data(
        &self,
        batch: &ColumnBatch,
        row_mask: Option<&[bool]>,
        start_row: usize,
    ) -> Result<Vec<u8>> {
        let element_size = batch.data_type.size_bytes();
        let mut filtered_data = Vec::new();
        
        for i in 0..batch.size {
            let global_row = start_row + i;
            let is_null = batch.null_mask.get(i).unwrap_or(&false);
            let include_row = if let Some(mask) = row_mask {
                global_row < mask.len() && mask[global_row]
            } else {
                true
            };
            
            if include_row && !is_null {
                let start_byte = i * element_size;
                let end_byte = start_byte + element_size;
                if end_byte <= batch.data.len() {
                    filtered_data.extend_from_slice(&batch.data[start_byte..end_byte]);
                }
            }
        }
        
        Ok(filtered_data)
    }
    
    /// Extract group keys for a batch
    fn extract_group_keys(
        &self,
        table: &ColumnarTable,
        group_columns: &[String],
        start_row: usize,
        batch_size: usize,
        row_mask: Option<&[bool]>,
    ) -> Result<Vec<GroupKey>> {
        let mut group_keys = Vec::with_capacity(batch_size);
        
        // Get batches for all group columns
        let mut column_batches = HashMap::new();
        for col_name in group_columns {
            let batch = table.get_column_batch(col_name, start_row, batch_size)?;
            column_batches.insert(col_name.clone(), batch);
        }
        
        // Extract values for each row
        for row_idx in 0..batch_size {
            let global_row = start_row + row_idx;
            
            // Skip if row is filtered out
            if let Some(mask) = row_mask {
                if global_row >= mask.len() || !mask[global_row] {
                    group_keys.push(GroupKey { values: vec![] }); // Placeholder
                    continue;
                }
            }
            
            let mut group_values = Vec::new();
            
            for col_name in group_columns {
                if let Some(batch) = column_batches.get(col_name) {
                    let is_null = batch.null_mask.get(row_idx).unwrap_or(&false);
                    
                    if *is_null {
                        group_values.push(Value::Null);
                    } else {
                        let value = self.extract_value_from_batch(batch, row_idx)?;
                        group_values.push(value);
                    }
                }
            }
            
            group_keys.push(GroupKey { values: group_values });
        }
        
        Ok(group_keys)
    }
    
    /// Extract single value from batch at specific index
    fn extract_value_from_batch(&self, batch: &ColumnBatch, index: usize) -> Result<Value> {
        let element_size = batch.data_type.size_bytes();
        let start_byte = index * element_size;
        let end_byte = start_byte + element_size;
        
        if end_byte > batch.data.len() {
            return Err(Error::Generic("Index out of bounds in batch".to_string()));
        }
        
        let bytes = &batch.data[start_byte..end_byte];
        
        match batch.data_type {
            DataType::Int32 => {
                let value = i32::from_le_bytes(bytes.try_into().unwrap());
                Ok(Value::Int32(value))
            }
            DataType::Int64 => {
                let value = i64::from_le_bytes(bytes.try_into().unwrap());
                Ok(Value::Int64(value))
            }
            DataType::Float32 => {
                let value = f32::from_le_bytes(bytes.try_into().unwrap());
                Ok(Value::Float32(value))
            }
            DataType::Float64 => {
                let value = f64::from_le_bytes(bytes.try_into().unwrap());
                Ok(Value::Float64(value))
            }
            DataType::Bool => {
                Ok(Value::Bool(bytes[0] != 0))
            }
            DataType::String => {
                // For string columns, we'd need access to the string dictionary
                // For now, return the ID as an integer
                let id = u32::from_le_bytes(bytes.try_into().unwrap());
                Ok(Value::Int32(id as i32))
            }
            DataType::Date => {
                let value = i32::from_le_bytes(bytes.try_into().unwrap());
                Ok(Value::Date(value))
            }
            DataType::Timestamp => {
                let value = i64::from_le_bytes(bytes.try_into().unwrap());
                Ok(Value::Timestamp(value))
            }
        }
    }
    
    /// Update aggregate for a single row
    fn update_aggregate_single_row(
        &self,
        function: &AggregateFunction,
        aggregate_state: &mut AggregateValue,
        table: &ColumnarTable,
        row: usize,
    ) -> Result<()> {
        match function {
            AggregateFunction::Count => {
                if let AggregateState::Count(ref mut count) = aggregate_state.state {
                    *count += 1;
                    aggregate_state.value = Value::Int64(*count as i64);
                }
            }
            
            AggregateFunction::Sum(column) => {
                let value = self.get_row_value_as_float(table, column, row)?;
                if let AggregateState::Sum(ref mut sum) = aggregate_state.state {
                    *sum += value;
                    aggregate_state.value = Value::Float64(*sum);
                }
            }
            
            AggregateFunction::Min(column) => {
                let value = self.get_row_value_as_float(table, column, row)?;
                if let AggregateState::Min(ref mut min) = aggregate_state.state {
                    *min = min.min(value);
                    aggregate_state.value = Value::Float64(*min);
                }
            }
            
            AggregateFunction::Max(column) => {
                let value = self.get_row_value_as_float(table, column, row)?;
                if let AggregateState::Max(ref mut max) = aggregate_state.state {
                    *max = max.max(value);
                    aggregate_state.value = Value::Float64(*max);
                }
            }
            
            AggregateFunction::Avg(column) => {
                let value = self.get_row_value_as_float(table, column, row)?;
                if let AggregateState::Avg { ref mut sum, ref mut count } = aggregate_state.state {
                    *sum += value;
                    *count += 1;
                    aggregate_state.value = Value::Float64(*sum / *count as f64);
                }
            }
            
            _ => return Err(Error::Generic("Aggregate function not implemented".to_string())),
        }
        
        Ok(())
    }
    
    /// Get row value as float for numeric operations
    fn get_row_value_as_float(&self, table: &ColumnarTable, column: &str, row: usize) -> Result<f64> {
        if let Some(null_mask) = table.null_masks.get(column) {
            if row < null_mask.len() && null_mask[row] {
                return Ok(0.0); // NULL values contribute 0 to numeric aggregates
            }
        }
        
        if let Some(col) = table.get_column(column) {
            let is_null = table.null_masks.get(column)
                .and_then(|mask| mask.get(row))
                .unwrap_or(&false);
            
            let value = col.get_value(row, *is_null)?;
            
            match value {
                Value::Int32(v) => Ok(v as f64),
                Value::Int64(v) => Ok(v as f64),
                Value::Float32(v) => Ok(v as f64),
                Value::Float64(v) => Ok(v),
                Value::Null => Ok(0.0),
                _ => Err(Error::Generic("Cannot convert value to float".to_string())),
            }
        } else {
            Err(Error::Generic(format!("Column '{}' not found", column)))
        }
    }
    
    /// Create initial state for aggregate function
    fn create_initial_state(&self, function: &AggregateFunction) -> Result<AggregateValue> {
        let (value, state) = match function {
            AggregateFunction::Count => (Value::Int64(0), AggregateState::Count(0)),
            AggregateFunction::Sum(_) => (Value::Float64(0.0), AggregateState::Sum(0.0)),
            AggregateFunction::Min(_) => (Value::Float64(f64::INFINITY), AggregateState::Min(f64::INFINITY)),
            AggregateFunction::Max(_) => (Value::Float64(f64::NEG_INFINITY), AggregateState::Max(f64::NEG_INFINITY)),
            AggregateFunction::Avg(_) => (Value::Float64(0.0), AggregateState::Avg { sum: 0.0, count: 0 }),
            _ => return Err(Error::Generic("Aggregate function not supported".to_string())),
        };
        
        Ok(AggregateValue { value, state })
    }
    
    /// Get function name for result column
    fn function_name(&self, function: &AggregateFunction) -> String {
        match function {
            AggregateFunction::Count => "count".to_string(),
            AggregateFunction::Sum(col) => format!("sum_{}", col),
            AggregateFunction::Min(col) => format!("min_{}", col),
            AggregateFunction::Max(col) => format!("max_{}", col),
            AggregateFunction::Avg(col) => format!("avg_{}", col),
            _ => "unknown".to_string(),
        }
    }
    
    /// Finalize aggregates (compute final values)
    fn finalize_aggregates(&self, group_state: &mut AggregationState) -> Result<()> {
        for (_, aggregate) in group_state.aggregates.iter_mut() {
            match &mut aggregate.state {
                AggregateState::Avg { sum, count } => {
                    if *count > 0 {
                        aggregate.value = Value::Float64(*sum / *count as f64);
                    } else {
                        aggregate.value = Value::Null;
                    }
                }
                _ => {} // Other aggregates are already finalized
            }
        }
        Ok(())
    }
    
    /// Hash group key for efficient lookup
    fn hash_group_key(&self, group_key: &GroupKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        group_key.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Estimate memory usage of aggregation results
    fn estimate_memory_usage(&self, groups: &HashMap<GroupKey, AggregationState>) -> usize {
        let mut total = 0;
        
        for (key, state) in groups {
            // Estimate group key size
            total += key.values.len() * 16; // Rough estimate per value
            
            // Estimate state size
            total += state.group_values.len() * 16;
            total += state.aggregates.len() * 32;
            total += 64; // Base overhead
        }
        
        total
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

impl Default for AggregationConfig {
    fn default() -> Self {
        Self {
            functions: Vec::new(),
            group_by_columns: Vec::new(),
            max_groups_in_memory: 1_000_000,
            parallel: true,
            initial_capacity: 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{ColumnarTableBuilder, ColumnDefinition, SIMDProcessor};
    use std::collections::HashMap;
    
    #[test]
    fn test_simple_count() {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        let mut aggregator = VectorizedAggregator::new(simd_ops);
        
        // Create test table
        let mut builder = ColumnarTableBuilder::new();
        builder.add_column(ColumnDefinition {
            name: "values".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default_value: None,
            metadata: HashMap::new(),
        }).unwrap();
        
        // Add test data
        for i in 0..100 {
            let mut row = HashMap::new();
            row.insert("values".to_string(), (Value::Int32(i), false));
            builder.append_row(row).unwrap();
        }
        
        let table = builder.build().unwrap();
        
        let config = AggregationConfig {
            functions: vec![AggregateFunction::Count],
            group_by_columns: vec![],
            ..Default::default()
        };
        
        let result = aggregator.aggregate(&table, config, None).unwrap();
        assert_eq!(result.group_count, 1);
        
        let group_state = result.groups.values().next().unwrap();
        let count_value = group_state.aggregates.get("count").unwrap();
        assert_eq!(count_value.value, Value::Int64(100));
    }
    
    #[test]
    fn test_simple_sum() {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        let mut aggregator = VectorizedAggregator::new(simd_ops);
        
        // Create test table
        let mut builder = ColumnarTableBuilder::new();
        builder.add_column(ColumnDefinition {
            name: "values".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default_value: None,
            metadata: HashMap::new(),
        }).unwrap();
        
        // Add test data: 1 + 2 + 3 + 4 + 5 = 15
        for i in 1..=5 {
            let mut row = HashMap::new();
            row.insert("values".to_string(), (Value::Int32(i), false));
            builder.append_row(row).unwrap();
        }
        
        let table = builder.build().unwrap();
        
        let config = AggregationConfig {
            functions: vec![AggregateFunction::Sum("values".to_string())],
            group_by_columns: vec![],
            ..Default::default()
        };
        
        let result = aggregator.aggregate(&table, config, None).unwrap();
        let group_state = result.groups.values().next().unwrap();
        let sum_value = group_state.aggregates.get("sum_values").unwrap();
        assert_eq!(sum_value.value, Value::Float64(15.0));
    }
    
    #[test]
    fn test_grouped_aggregation() {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        let mut aggregator = VectorizedAggregator::new(simd_ops);
        
        // Create test table with groups
        let mut builder = ColumnarTableBuilder::new();
        builder.add_column(ColumnDefinition {
            name: "group_col".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default_value: None,
            metadata: HashMap::new(),
        }).unwrap();
        
        builder.add_column(ColumnDefinition {
            name: "values".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default_value: None,
            metadata: HashMap::new(),
        }).unwrap();
        
        // Add test data: group 1 has values [1,2], group 2 has values [3,4]
        let data = vec![(1, 1), (1, 2), (2, 3), (2, 4)];
        for (group, value) in data {
            let mut row = HashMap::new();
            row.insert("group_col".to_string(), (Value::Int32(group), false));
            row.insert("values".to_string(), (Value::Int32(value), false));
            builder.append_row(row).unwrap();
        }
        
        let table = builder.build().unwrap();
        
        let config = AggregationConfig {
            functions: vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("values".to_string()),
            ],
            group_by_columns: vec!["group_col".to_string()],
            ..Default::default()
        };
        
        let result = aggregator.aggregate(&table, config, None).unwrap();
        assert_eq!(result.group_count, 2);
        
        // Verify results for each group
        for (group_key, group_state) in &result.groups {
            if !group_key.values.is_empty() {
                match group_key.values[0] {
                    Value::Int32(1) => {
                        // Group 1: count=2, sum=3
                        assert_eq!(group_state.aggregates.get("count").unwrap().value, Value::Int64(2));
                        assert_eq!(group_state.aggregates.get("sum_values").unwrap().value, Value::Float64(3.0));
                    }
                    Value::Int32(2) => {
                        // Group 2: count=2, sum=7
                        assert_eq!(group_state.aggregates.get("count").unwrap().value, Value::Int64(2));
                        assert_eq!(group_state.aggregates.get("sum_values").unwrap().value, Value::Float64(7.0));
                    }
                    _ => panic!("Unexpected group value"),
                }
            }
        }
    }
}