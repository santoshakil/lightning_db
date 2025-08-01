//! Vectorized Query Executor
//!
//! This module provides the main query execution engine that orchestrates
//! vectorized operations using SIMD instructions and columnar storage.

use crate::{Result, Error};
use super::{
    QueryOperation, FilterExpression, ExecutionPlan, VectorizedContext, ExecutionStats,
    ColumnarTable, ColumnBatch, SIMDProcessor, SIMDOperations, DataType, ComparisonOperator,
    Value, AggregateFunction, SortColumn, JoinType, JoinCondition, VECTOR_BATCH_SIZE
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Vectorized query executor
pub struct VectorizedExecutor {
    /// SIMD processor for low-level operations
    simd_processor: Arc<dyn SIMDOperations + Send + Sync>,
    /// Execution context
    context: VectorizedContext,
    /// Execution statistics
    stats: ExecutionStats,
}

/// Query execution result
#[derive(Debug)]
pub struct QueryResult {
    /// Result table
    pub table: ColumnarTable,
    /// Execution statistics
    pub stats: ExecutionStats,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Number of rows processed
    pub rows_processed: u64,
}

/// Intermediate execution state
#[derive(Debug)]
struct ExecutionState {
    /// Current working table
    table: ColumnarTable,
    /// Active row mask (true = include row)
    row_mask: Vec<bool>,
    /// Current row count
    row_count: usize,
}

impl VectorizedExecutor {
    /// Create new vectorized executor
    pub fn new() -> Self {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        
        Self {
            simd_processor: simd_ops,
            context: VectorizedContext::default(),
            stats: ExecutionStats::default(),
        }
    }
    
    /// Create executor with custom context
    pub fn with_context(context: VectorizedContext) -> Self {
        let processor = SIMDProcessor::new();
        let simd_ops = processor.get_implementation();
        
        Self {
            simd_processor: simd_ops,
            context,
            stats: ExecutionStats::default(),
        }
    }
    
    /// Execute query plan
    pub fn execute(&mut self, plan: ExecutionPlan, input_table: ColumnarTable) -> Result<QueryResult> {
        let start_time = Instant::now();
        self.stats = ExecutionStats::default();
        
        let mut state = ExecutionState {
            table: input_table,
            row_mask: vec![true; 0], // Will be initialized based on table size
            row_count: 0,
        };
        
        // Initialize row mask
        state.row_count = state.table.row_count;
        state.row_mask = vec![true; state.row_count];
        
        // Execute the query plan
        let result_state = self.execute_operation(&plan.root, state)?;
        
        let execution_time = start_time.elapsed();
        self.stats.execution_time_us = execution_time.as_micros() as u64;
        self.stats.rows_processed = result_state.row_count as u64;
        
        // Calculate rows per second
        if self.stats.execution_time_us > 0 {
            self.stats.rows_per_second = (self.stats.rows_processed as f64 * 1_000_000.0) / self.stats.execution_time_us as f64;
        }
        
        Ok(QueryResult {
            table: result_state.table,
            stats: self.stats.clone(),
            execution_time_us: self.stats.execution_time_us,
            rows_processed: self.stats.rows_processed,
        })
    }
    
    /// Execute a single operation
    fn execute_operation(&mut self, operation: &QueryOperation, mut state: ExecutionState) -> Result<ExecutionState> {
        match operation {
            QueryOperation::Scan { column, filter } => {
                if let Some(filter_expr) = filter {
                    state = self.execute_filter(filter_expr, state)?;
                }
                Ok(state)
            }
            
            QueryOperation::Filter { expression } => {
                self.execute_filter(expression, state)
            }
            
            QueryOperation::Aggregate { functions, group_by } => {
                self.execute_aggregate(functions, group_by, state)
            }
            
            QueryOperation::Sort { columns } => {
                self.execute_sort(columns, state)
            }
            
            QueryOperation::Project { columns } => {
                self.execute_project(columns, state)
            }
            
            QueryOperation::Limit { count, offset } => {
                self.execute_limit(*count, *offset, state)
            }
            
            QueryOperation::Join { left, right, join_type, condition } => {
                // For simplicity, this is a basic implementation
                // In a real system, this would be much more complex
                Err(Error::Generic("Join operations not yet implemented".to_string()))
            }
        }
    }
    
    /// Execute filter operation
    fn execute_filter(&mut self, expression: &FilterExpression, mut state: ExecutionState) -> Result<ExecutionState> {
        let filter_mask = self.evaluate_filter_expression(expression, &state.table)?;
        
        // Combine with existing row mask
        for (i, &include) in filter_mask.iter().enumerate() {
            if i < state.row_mask.len() {
                state.row_mask[i] = state.row_mask[i] && include;
            }
        }
        
        // Update row count
        state.row_count = state.row_mask.iter().filter(|&&x| x).count();
        
        self.stats.simd_operations += (filter_mask.len() / VECTOR_BATCH_SIZE) as u64;
        
        Ok(state)
    }
    
    /// Evaluate filter expression
    fn evaluate_filter_expression(&mut self, expression: &FilterExpression, table: &ColumnarTable) -> Result<Vec<bool>> {
        match expression {
            FilterExpression::Column { name, operator, value } => {
                self.evaluate_column_filter(name, *operator, value, table)
            }
            
            FilterExpression::And(left, right) => {
                let left_mask = self.evaluate_filter_expression(left, table)?;
                let right_mask = self.evaluate_filter_expression(right, table)?;
                Ok(self.simd_processor.and_bitmask(&left_mask, &right_mask))
            }
            
            FilterExpression::Or(left, right) => {
                let left_mask = self.evaluate_filter_expression(left, table)?;
                let right_mask = self.evaluate_filter_expression(right, table)?;
                Ok(self.simd_processor.or_bitmask(&left_mask, &right_mask))
            }
            
            FilterExpression::Not(expr) => {
                let mask = self.evaluate_filter_expression(expr, table)?;
                Ok(self.simd_processor.not_bitmask(&mask))
            }
            
            FilterExpression::In { column, values } => {
                self.evaluate_in_filter(column, values, table)
            }
            
            FilterExpression::Between { column, min, max } => {
                self.evaluate_between_filter(column, min, max, table)
            }
            
            FilterExpression::IsNull(column) => {
                self.evaluate_null_filter(column, true, table)
            }
            
            FilterExpression::IsNotNull(column) => {
                self.evaluate_null_filter(column, false, table)
            }
        }
    }
    
    /// Evaluate column comparison filter
    fn evaluate_column_filter(&mut self, column: &str, operator: ComparisonOperator, value: &Value, table: &ColumnarTable) -> Result<Vec<bool>> {
        let mut result_mask = Vec::new();
        let total_rows = table.row_count;
        let batch_size = VECTOR_BATCH_SIZE;
        
        let mut processed_rows = 0;
        while processed_rows < total_rows {
            let current_batch_size = (total_rows - processed_rows).min(batch_size);
            let batch = table.get_column_batch(column, processed_rows, current_batch_size)?;
            
            if batch.data.is_empty() {
                break;
            }
            
            // Convert value to bytes for comparison
            let scalar_bytes = self.value_to_bytes(value, batch.data_type)?;
            
            // Perform SIMD comparison
            let batch_result = self.simd_processor.compare_scalar(
                &batch.data,
                &scalar_bytes,
                batch.data_type,
                operator,
            )?;
            
            result_mask.extend(batch_result);
            processed_rows += current_batch_size;
            
            self.stats.simd_operations += 1;
        }
        
        Ok(result_mask)
    }
    
    /// Evaluate IN filter
    fn evaluate_in_filter(&mut self, column: &str, values: &[Value], table: &ColumnarTable) -> Result<Vec<bool>> {
        let mut result_mask = vec![false; table.row_count];
        
        // For each value in the IN clause, perform comparison and OR the results
        for value in values {
            let value_mask = self.evaluate_column_filter(column, ComparisonOperator::Equal, value, table)?;
            result_mask = self.simd_processor.or_bitmask(&result_mask, &value_mask);
        }
        
        Ok(result_mask)
    }
    
    /// Evaluate BETWEEN filter
    fn evaluate_between_filter(&mut self, column: &str, min: &Value, max: &Value, table: &ColumnarTable) -> Result<Vec<bool>> {
        let min_mask = self.evaluate_column_filter(column, ComparisonOperator::GreaterThanOrEqual, min, table)?;
        let max_mask = self.evaluate_column_filter(column, ComparisonOperator::LessThanOrEqual, max, table)?;
        Ok(self.simd_processor.and_bitmask(&min_mask, &max_mask))
    }
    
    /// Evaluate NULL filter
    fn evaluate_null_filter(&mut self, column: &str, is_null: bool, table: &ColumnarTable) -> Result<Vec<bool>> {
        if let Some(null_mask) = table.null_masks.get(column) {
            if is_null {
                Ok(null_mask.clone())
            } else {
                Ok(self.simd_processor.not_bitmask(null_mask))
            }
        } else {
            // Column has no null mask, so all values are non-null
            Ok(vec![!is_null; table.row_count])
        }
    }
    
    /// Execute aggregation operation
    fn execute_aggregate(&mut self, functions: &[AggregateFunction], group_by: &[String], mut state: ExecutionState) -> Result<ExecutionState> {
        if group_by.is_empty() {
            // Simple aggregation without grouping
            self.execute_simple_aggregate(functions, &mut state)
        } else {
            // Grouped aggregation
            self.execute_grouped_aggregate(functions, group_by, &mut state)
        }
    }
    
    /// Execute simple aggregation (no grouping)
    fn execute_simple_aggregate(&mut self, functions: &[AggregateFunction], state: &mut ExecutionState) -> Result<ExecutionState> {
        let mut results = HashMap::new();
        
        for func in functions {
            let result = match func {
                AggregateFunction::Count => {
                    Value::Int64(state.row_mask.iter().filter(|&&x| x).count() as i64)
                }
                
                AggregateFunction::Sum(column) => {
                    let sum = self.execute_vectorized_sum(column, &state.table, &state.row_mask)?;
                    match state.table.get_column(column).unwrap().definition.data_type {
                        DataType::Int32 => Value::Int64(sum as i64),
                        DataType::Int64 => Value::Int64(sum as i64),
                        DataType::Float32 => Value::Float32(sum as f32),
                        DataType::Float64 => Value::Float64(sum),
                        _ => return Err(Error::Generic("Invalid data type for SUM".to_string())),
                    }
                }
                
                AggregateFunction::Avg(column) => {
                    let sum = self.execute_vectorized_sum(column, &state.table, &state.row_mask)?;
                    let count = state.row_mask.iter().filter(|&&x| x).count() as f64;
                    if count > 0.0 {
                        Value::Float64(sum / count)
                    } else {
                        Value::Null
                    }
                }
                
                AggregateFunction::Min(column) => {
                    let min = self.execute_vectorized_min(column, &state.table, &state.row_mask)?;
                    match state.table.get_column(column).unwrap().definition.data_type {
                        DataType::Int32 => Value::Int32(min as i32),
                        DataType::Int64 => Value::Int64(min as i64),
                        DataType::Float32 => Value::Float32(min as f32),
                        DataType::Float64 => Value::Float64(min),
                        _ => return Err(Error::Generic("Invalid data type for MIN".to_string())),
                    }
                }
                
                AggregateFunction::Max(column) => {
                    let max = self.execute_vectorized_max(column, &state.table, &state.row_mask)?;
                    match state.table.get_column(column).unwrap().definition.data_type {
                        DataType::Int32 => Value::Int32(max as i32),
                        DataType::Int64 => Value::Int64(max as i64),
                        DataType::Float32 => Value::Float32(max as f32),
                        DataType::Float64 => Value::Float64(max),
                        _ => return Err(Error::Generic("Invalid data type for MAX".to_string())),
                    }
                }
                
                _ => return Err(Error::Generic("Aggregate function not yet implemented".to_string())),
            };
            
            let column_name = match func {
                AggregateFunction::Count => "count".to_string(),
                AggregateFunction::Sum(col) => format!("sum_{}", col),
                AggregateFunction::Avg(col) => format!("avg_{}", col),
                AggregateFunction::Min(col) => format!("min_{}", col),
                AggregateFunction::Max(col) => format!("max_{}", col),
                _ => "aggregate".to_string(),
            };
            
            results.insert(column_name, (result, false));
        }
        
        // Create new table with aggregation results
        let mut builder = super::ColumnarTableBuilder::new();
        
        for (name, (value, _)) in &results {
            let col_def = super::ColumnDefinition {
                name: name.clone(),
                data_type: value.data_type(),
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            };
            builder.add_column(col_def)?;
        }
        
        builder.append_row(results)?;
        let result_table = builder.build()?;
        
        Ok(ExecutionState {
            table: result_table,
            row_mask: vec![true; 1],
            row_count: 1,
        })
    }
    
    /// Execute grouped aggregation
    fn execute_grouped_aggregate(&mut self, _functions: &[AggregateFunction], _group_by: &[String], state: &mut ExecutionState) -> Result<ExecutionState> {
        // Grouped aggregation is complex and would require:
        // 1. Grouping/partitioning the data
        // 2. Applying aggregates to each group
        // 3. Combining results
        // For now, return an error
        Err(Error::Generic("Grouped aggregation not yet implemented".to_string()))
    }
    
    /// Execute vectorized sum
    fn execute_vectorized_sum(&mut self, column: &str, table: &ColumnarTable, row_mask: &[bool]) -> Result<f64> {
        let mut total_sum = 0.0;
        let batch_size = VECTOR_BATCH_SIZE;
        let total_rows = table.row_count;
        
        let mut processed_rows = 0;
        while processed_rows < total_rows {
            let current_batch_size = (total_rows - processed_rows).min(batch_size);
            let batch = table.get_column_batch(column, processed_rows, current_batch_size)?;
            
            if batch.data.is_empty() {
                break;
            }
            
            // Filter data based on row mask
            let filtered_data = self.filter_batch_data(&batch.data, &batch.null_mask, row_mask, processed_rows, batch.data_type)?;
            
            if !filtered_data.is_empty() {
                let batch_sum = self.simd_processor.sum_vector(&filtered_data, batch.data_type)?;
                total_sum += batch_sum;
                self.stats.simd_operations += 1;
            }
            
            processed_rows += current_batch_size;
        }
        
        Ok(total_sum)
    }
    
    /// Execute vectorized min
    fn execute_vectorized_min(&mut self, column: &str, table: &ColumnarTable, row_mask: &[bool]) -> Result<f64> {
        let mut global_min = f64::INFINITY;
        let batch_size = VECTOR_BATCH_SIZE;
        let total_rows = table.row_count;
        
        let mut processed_rows = 0;
        while processed_rows < total_rows {
            let current_batch_size = (total_rows - processed_rows).min(batch_size);
            let batch = table.get_column_batch(column, processed_rows, current_batch_size)?;
            
            if batch.data.is_empty() {
                break;
            }
            
            let filtered_data = self.filter_batch_data(&batch.data, &batch.null_mask, row_mask, processed_rows, batch.data_type)?;
            
            if !filtered_data.is_empty() {
                let batch_min = self.simd_processor.min_vector(&filtered_data, batch.data_type)?;
                global_min = global_min.min(batch_min);
                self.stats.simd_operations += 1;
            }
            
            processed_rows += current_batch_size;
        }
        
        Ok(global_min)
    }
    
    /// Execute vectorized max
    fn execute_vectorized_max(&mut self, column: &str, table: &ColumnarTable, row_mask: &[bool]) -> Result<f64> {
        let mut global_max = f64::NEG_INFINITY;
        let batch_size = VECTOR_BATCH_SIZE;
        let total_rows = table.row_count;
        
        let mut processed_rows = 0;
        while processed_rows < total_rows {
            let current_batch_size = (total_rows - processed_rows).min(batch_size);
            let batch = table.get_column_batch(column, processed_rows, current_batch_size)?;
            
            if batch.data.is_empty() {
                break;
            }
            
            let filtered_data = self.filter_batch_data(&batch.data, &batch.null_mask, row_mask, processed_rows, batch.data_type)?;
            
            if !filtered_data.is_empty() {
                let batch_max = self.simd_processor.max_vector(&filtered_data, batch.data_type)?;
                global_max = global_max.max(batch_max);
                self.stats.simd_operations += 1;
            }
            
            processed_rows += current_batch_size;
        }
        
        Ok(global_max)
    }
    
    /// Filter batch data based on row mask
    fn filter_batch_data(&self, data: &[u8], null_mask: &[bool], row_mask: &[bool], start_row: usize, data_type: DataType) -> Result<Vec<u8>> {
        let element_size = data_type.size_bytes();
        let num_elements = data.len() / element_size;
        let mut filtered_data = Vec::new();
        
        for i in 0..num_elements {
            let global_row = start_row + i;
            if global_row < row_mask.len() && row_mask[global_row] && !null_mask.get(i).unwrap_or(&false) {
                let start_byte = i * element_size;
                let end_byte = start_byte + element_size;
                if end_byte <= data.len() {
                    filtered_data.extend_from_slice(&data[start_byte..end_byte]);
                }
            }
        }
        
        Ok(filtered_data)
    }
    
    /// Execute sort operation
    fn execute_sort(&mut self, _columns: &[SortColumn], state: ExecutionState) -> Result<ExecutionState> {
        // Sorting is complex for vectorized execution and would require:
        // 1. Vectorized comparison functions
        // 2. Efficient sorting algorithms adapted for SIMD
        // 3. Multi-column sorting logic
        // For now, return an error
        Err(Error::Generic("Sort operations not yet implemented".to_string()))
    }
    
    /// Execute projection operation
    fn execute_project(&mut self, columns: &[String], mut state: ExecutionState) -> Result<ExecutionState> {
        // Create new table with only the projected columns
        let mut new_schema = state.table.schema.clone();
        new_schema.columns = new_schema.columns
            .into_iter()
            .filter(|col| columns.contains(&col.name))
            .collect();
        
        let mut new_columns = HashMap::new();
        let mut new_null_masks = HashMap::new();
        
        for column_name in columns {
            if let Some(column) = state.table.columns.get(column_name) {
                new_columns.insert(column_name.clone(), Arc::clone(column));
                
                if let Some(null_mask) = state.table.null_masks.get(column_name) {
                    new_null_masks.insert(column_name.clone(), null_mask.clone());
                }
            }
        }
        
        state.table.schema = new_schema;
        state.table.columns = new_columns;
        state.table.null_masks = new_null_masks;
        
        Ok(state)
    }
    
    /// Execute limit operation
    fn execute_limit(&mut self, count: usize, offset: usize, mut state: ExecutionState) -> Result<ExecutionState> {
        let total_rows = state.row_count;
        let start_row = offset.min(total_rows);
        let end_row = (start_row + count).min(total_rows);
        
        // Apply limit to row mask
        for i in 0..state.row_mask.len() {
            if i < start_row || i >= end_row {
                state.row_mask[i] = false;
            }
        }
        
        state.row_count = state.row_mask.iter().filter(|&&x| x).count();
        
        Ok(state)
    }
    
    /// Convert value to bytes for SIMD operations
    fn value_to_bytes(&self, value: &Value, data_type: DataType) -> Result<Vec<u8>> {
        match (value, data_type) {
            (Value::Int32(v), DataType::Int32) => Ok(v.to_le_bytes().to_vec()),
            (Value::Int64(v), DataType::Int64) => Ok(v.to_le_bytes().to_vec()),
            (Value::Float32(v), DataType::Float32) => Ok(v.to_le_bytes().to_vec()),
            (Value::Float64(v), DataType::Float64) => Ok(v.to_le_bytes().to_vec()),
            (Value::Bool(v), DataType::Bool) => Ok(vec![if *v { 1 } else { 0 }]),
            (Value::Date(v), DataType::Date) => Ok(v.to_le_bytes().to_vec()),
            (Value::Timestamp(v), DataType::Timestamp) => Ok(v.to_le_bytes().to_vec()),
            _ => Err(Error::Generic("Value type doesn't match expected data type".to_string())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{ColumnarTableBuilder, ColumnDefinition};
    
    #[test]
    fn test_executor_creation() {
        let executor = VectorizedExecutor::new();
        assert_eq!(executor.stats.rows_processed, 0);
        assert_eq!(executor.stats.simd_operations, 0);
    }
    
    #[test]
    fn test_simple_filter() {
        let mut executor = VectorizedExecutor::new();
        
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
        
        // Create simple scan operation
        let operation = QueryOperation::Scan {
            column: "values".to_string(),
            filter: Some(FilterExpression::Column {
                name: "values".to_string(),
                operator: ComparisonOperator::GreaterThan,
                value: Value::Int32(50),
            }),
        };
        
        let plan = ExecutionPlan::new(operation);
        let result = executor.execute(plan, table).unwrap();
        
        assert!(result.stats.simd_operations > 0);
        assert!(result.stats.rows_processed > 0);
        assert!(result.execution_time_us > 0);
    }
    
    #[test]
    fn test_simple_aggregation() {
        let mut executor = VectorizedExecutor::new();
        
        // Create test table
        let mut builder = ColumnarTableBuilder::new();
        builder.add_column(ColumnDefinition {
            name: "values".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default_value: None,
            metadata: HashMap::new(),
        }).unwrap();
        
        // Add test data: values 1, 2, 3, 4, 5
        for i in 1..=5 {
            let mut row = HashMap::new();
            row.insert("values".to_string(), (Value::Int32(i), false));
            builder.append_row(row).unwrap();
        }
        
        let table = builder.build().unwrap();
        
        // Create aggregation operation
        let operation = QueryOperation::Aggregate {
            functions: vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("values".to_string()),
                AggregateFunction::Avg("values".to_string()),
            ],
            group_by: vec![],
        };
        
        let plan = ExecutionPlan::new(operation);
        let result = executor.execute(plan, table).unwrap();
        
        assert_eq!(result.table.row_count, 1);
        assert!(result.stats.simd_operations > 0);
    }
    
    #[test]
    fn test_projection() {
        let mut executor = VectorizedExecutor::new();
        
        // Create test table with multiple columns
        let mut builder = ColumnarTableBuilder::new();
        builder.add_column(ColumnDefinition {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            default_value: None,
            metadata: HashMap::new(),
        }).unwrap();
        
        builder.add_column(ColumnDefinition {
            name: "value".to_string(),
            data_type: DataType::Float32,
            nullable: false,
            default_value: None,
            metadata: HashMap::new(),
        }).unwrap();
        
        // Add test data
        for i in 0..10 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), (Value::Int32(i), false));
            row.insert("value".to_string(), (Value::Float32(i as f32 * 1.5), false));
            builder.append_row(row).unwrap();
        }
        
        let table = builder.build().unwrap();
        assert_eq!(table.columns.len(), 2);
        
        // Project only the "value" column
        let operation = QueryOperation::Project {
            columns: vec!["value".to_string()],
        };
        
        let plan = ExecutionPlan::new(operation);
        let result = executor.execute(plan, table).unwrap();
        
        assert_eq!(result.table.columns.len(), 1);
        assert!(result.table.columns.contains_key("value"));
        assert!(!result.table.columns.contains_key("id"));
    }
}