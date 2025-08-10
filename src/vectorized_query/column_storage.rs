//! Columnar Storage for Vectorized Query Execution
//!
//! This module provides columnar data structures optimized for SIMD operations
//! and vectorized query processing. Data is stored in contiguous memory layouts
//! that maximize cache efficiency and enable efficient vectorization.

use super::{DataType, Value};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Columnar table for vectorized operations
#[derive(Debug, Clone)]
pub struct ColumnarTable {
    /// Table schema
    pub schema: TableSchema,
    /// Column data storage
    pub columns: HashMap<String, Arc<Column>>,
    /// Number of rows
    pub row_count: usize,
    /// Null masks for each column
    pub null_masks: HashMap<String, Vec<bool>>,
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Column definitions
    pub columns: Vec<ColumnDefinition>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Table metadata
    pub metadata: HashMap<String, String>,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Whether column allows nulls
    pub nullable: bool,
    /// Default value
    pub default_value: Option<Value>,
    /// Column metadata
    pub metadata: HashMap<String, String>,
}

/// Column data storage
#[derive(Debug)]
pub struct Column {
    /// Column definition
    pub definition: ColumnDefinition,
    /// Raw data bytes (aligned for SIMD)
    pub data: Vec<u8>,
    /// String dictionary for string columns
    pub string_dict: Option<StringDictionary>,
    /// Column statistics
    pub stats: ColumnStatistics,
}

/// String dictionary for efficient string storage
#[derive(Debug, Clone)]
pub struct StringDictionary {
    /// String to ID mapping
    pub string_to_id: HashMap<String, u32>,
    /// ID to string mapping
    pub id_to_string: Vec<String>,
    /// Next available ID
    pub next_id: u32,
}

/// Column statistics for query optimization
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    /// Number of rows
    pub row_count: u64,
    /// Number of null values
    pub null_count: u64,
    /// Minimum value
    pub min_value: Option<Value>,
    /// Maximum value
    pub max_value: Option<Value>,
    /// Number of distinct values
    pub distinct_count: Option<u64>,
    /// Data size in bytes
    pub data_size_bytes: u64,
    /// Whether column is sorted
    pub is_sorted: bool,
    /// Histogram for numeric types
    pub histogram: Option<Histogram>,
}

/// Histogram for numeric data distribution
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Bucket boundaries
    pub boundaries: Vec<f64>,
    /// Count in each bucket
    pub counts: Vec<u64>,
    /// Total sample count
    pub total_count: u64,
}

/// Batch of column data for vectorized processing
#[derive(Debug)]
pub struct ColumnBatch {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Raw data
    pub data: Vec<u8>,
    /// Null mask (true = null, false = not null)
    pub null_mask: Vec<bool>,
    /// Batch size
    pub size: usize,
    /// Starting row index
    pub start_row: usize,
}

/// Vectorized table builder
#[derive(Debug)]
pub struct ColumnarTableBuilder {
    /// Schema being built
    schema: TableSchema,
    /// Column builders
    column_builders: HashMap<String, ColumnBuilder>,
    /// Current row count
    row_count: usize,
}

/// Column builder for constructing columns efficiently
#[derive(Debug)]
pub struct ColumnBuilder {
    /// Column definition
    definition: ColumnDefinition,
    /// Data buffer
    data: Vec<u8>,
    /// Null mask
    null_mask: Vec<bool>,
    /// String dictionary (for string columns)
    string_dict: Option<StringDictionary>,
    /// Current statistics
    stats: ColumnStatistics,
}

impl ColumnarTable {
    /// Create a new empty table with schema
    pub fn new(schema: TableSchema) -> Self {
        let mut columns = HashMap::new();
        let mut null_masks = HashMap::new();

        for col_def in &schema.columns {
            let column = Arc::new(Column::new(col_def.clone()));
            columns.insert(col_def.name.clone(), column);
            null_masks.insert(col_def.name.clone(), Vec::new());
        }

        Self {
            schema,
            columns,
            row_count: 0,
            null_masks,
        }
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&Arc<Column>> {
        self.columns.get(name)
    }

    /// Get column batch for vectorized processing
    pub fn get_column_batch(
        &self,
        name: &str,
        start_row: usize,
        batch_size: usize,
    ) -> Result<ColumnBatch> {
        let column = self
            .get_column(name)
            .ok_or_else(|| Error::Generic(format!("Column '{}' not found", name)))?;

        let end_row = (start_row + batch_size).min(self.row_count);
        let actual_size = end_row - start_row;

        if start_row >= self.row_count {
            return Ok(ColumnBatch {
                name: name.to_string(),
                data_type: column.definition.data_type,
                data: Vec::new(),
                null_mask: Vec::new(),
                size: 0,
                start_row,
            });
        }

        let element_size = column.definition.data_type.size_bytes();
        let data_start = start_row * element_size;
        let data_end = end_row * element_size;

        let data = if data_end <= column.data.len() {
            column.data[data_start..data_end].to_vec()
        } else {
            Vec::new()
        };

        let null_mask = if let Some(mask) = self.null_masks.get(name) {
            if end_row <= mask.len() {
                mask[start_row..end_row].to_vec()
            } else {
                Vec::new()
            }
        } else {
            vec![false; actual_size]
        };

        Ok(ColumnBatch {
            name: name.to_string(),
            data_type: column.definition.data_type,
            data,
            null_mask,
            size: actual_size,
            start_row,
        })
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<String> {
        self.schema.columns.iter().map(|c| c.name.clone()).collect()
    }

    /// Add a new column to the table
    pub fn add_column(
        &mut self,
        definition: ColumnDefinition,
        data: Vec<u8>,
        null_mask: Vec<bool>,
    ) -> Result<()> {
        if data.len() / definition.data_type.size_bytes() != self.row_count {
            return Err(Error::Generic(
                "Column data size doesn't match table row count".to_string(),
            ));
        }

        let mut column = Column::new(definition.clone());
        column.data = data;
        column.update_statistics(&null_mask);

        self.columns
            .insert(definition.name.clone(), Arc::new(column));
        self.null_masks.insert(definition.name.clone(), null_mask);
        self.schema.columns.push(definition);

        Ok(())
    }

    /// Estimate memory usage
    pub fn memory_usage(&self) -> usize {
        let mut total = 0;
        for column in self.columns.values() {
            total += column.data.len();
            total += column.data.capacity();
        }
        for mask in self.null_masks.values() {
            total += mask.len();
            total += mask.capacity();
        }
        total
    }

    /// Create iterator over batches
    pub fn batch_iterator(&self, batch_size: usize) -> BatchIterator {
        BatchIterator::new(self, batch_size)
    }

    /// Get table statistics
    pub fn get_statistics(&self) -> TableStatistics {
        let mut column_stats = HashMap::new();
        let mut total_size = 0;

        for (name, column) in &self.columns {
            column_stats.insert(name.clone(), column.stats.clone());
            total_size += column.stats.data_size_bytes;
        }

        TableStatistics {
            row_count: self.row_count as u64,
            column_count: self.columns.len() as u64,
            total_size_bytes: total_size,
            column_statistics: column_stats,
        }
    }
}

impl Column {
    /// Create new empty column
    pub fn new(definition: ColumnDefinition) -> Self {
        let is_string = matches!(definition.data_type, DataType::String);
        Self {
            definition,
            data: Vec::new(),
            string_dict: if is_string {
                Some(StringDictionary::new())
            } else {
                None
            },
            stats: ColumnStatistics::default(),
        }
    }

    /// Append value to column
    pub fn append_value(&mut self, value: &Value, is_null: bool) -> Result<()> {
        if is_null {
            self.append_null()?;
        } else {
            match (&self.definition.data_type, value) {
                (DataType::Int32, Value::Int32(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Int64, Value::Int64(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Float32, Value::Float32(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Float64, Value::Float64(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Bool, Value::Bool(v)) => {
                    self.data.push(if *v { 1 } else { 0 });
                }
                (DataType::String, Value::String(s)) => {
                    if let Some(ref mut dict) = self.string_dict {
                        let id = dict.get_or_insert_id(s.clone());
                        self.data.extend_from_slice(&id.to_le_bytes());
                    } else {
                        return Err(Error::Generic(
                            "String dictionary not initialized".to_string(),
                        ));
                    }
                }
                (DataType::Date, Value::Date(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Timestamp, Value::Timestamp(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                _ => {
                    return Err(Error::Generic(
                        "Value type doesn't match column type".to_string(),
                    ))
                }
            }
        }

        self.stats.row_count += 1;
        if is_null {
            self.stats.null_count += 1;
        }

        Ok(())
    }

    fn append_null(&mut self) -> Result<()> {
        let size = self.definition.data_type.size_bytes();
        self.data.extend(vec![0; size]);
        Ok(())
    }

    /// Update column statistics
    pub fn update_statistics(&mut self, null_mask: &[bool]) {
        self.stats.row_count = null_mask.len() as u64;
        self.stats.null_count = null_mask.iter().filter(|&&is_null| is_null).count() as u64;
        self.stats.data_size_bytes = self.data.len() as u64;

        // Calculate min/max for numeric types
        if !self.data.is_empty() && self.stats.null_count < self.stats.row_count {
            match self.definition.data_type {
                DataType::Int32 => {
                    let values = bytemuck::cast_slice::<u8, i32>(&self.data);
                    let non_null_values: Vec<i32> = values
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !null_mask.get(*i).unwrap_or(&false))
                        .map(|(_, v)| *v)
                        .collect();

                    if !non_null_values.is_empty() {
                        let min = *non_null_values.iter().min().unwrap();
                        let max = *non_null_values.iter().max().unwrap();
                        self.stats.min_value = Some(Value::Int32(min));
                        self.stats.max_value = Some(Value::Int32(max));
                    }
                }
                DataType::Float32 => {
                    let values = bytemuck::cast_slice::<u8, f32>(&self.data);
                    let non_null_values: Vec<f32> = values
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !null_mask.get(*i).unwrap_or(&false))
                        .map(|(_, v)| *v)
                        .collect();

                    if !non_null_values.is_empty() {
                        let min = non_null_values.iter().fold(f32::INFINITY, |a, &b| a.min(b));
                        let max = non_null_values
                            .iter()
                            .fold(f32::NEG_INFINITY, |a, &b| a.max(b));
                        self.stats.min_value = Some(Value::Float32(min));
                        self.stats.max_value = Some(Value::Float32(max));
                    }
                }
                _ => {} // Other types not implemented yet
            }
        }
    }

    /// Get value at specific row
    pub fn get_value(&self, row: usize, is_null: bool) -> Result<Value> {
        if is_null {
            return Ok(Value::Null);
        }

        let element_size = self.definition.data_type.size_bytes();
        let start = row * element_size;
        let end = start + element_size;

        if end > self.data.len() {
            return Err(Error::Generic("Row index out of bounds".to_string()));
        }

        let bytes = &self.data[start..end];

        match self.definition.data_type {
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
            DataType::Bool => Ok(Value::Bool(bytes[0] != 0)),
            DataType::String => {
                if let Some(ref dict) = self.string_dict {
                    let id = u32::from_le_bytes(bytes.try_into().unwrap());
                    if let Some(string) = dict.id_to_string.get(id as usize) {
                        Ok(Value::String(string.clone()))
                    } else {
                        Err(Error::Generic("Invalid string dictionary ID".to_string()))
                    }
                } else {
                    Err(Error::Generic(
                        "String dictionary not available".to_string(),
                    ))
                }
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
}

impl StringDictionary {
    /// Create new string dictionary
    pub fn new() -> Self {
        Self {
            string_to_id: HashMap::new(),
            id_to_string: Vec::new(),
            next_id: 0,
        }
    }

    /// Get or insert string and return its ID
    pub fn get_or_insert_id(&mut self, string: String) -> u32 {
        if let Some(&id) = self.string_to_id.get(&string) {
            id
        } else {
            let id = self.next_id;
            self.string_to_id.insert(string.clone(), id);
            self.id_to_string.push(string);
            self.next_id += 1;
            id
        }
    }

    /// Get string by ID
    pub fn get_string(&self, id: u32) -> Option<&String> {
        self.id_to_string.get(id as usize)
    }

    /// Get ID by string
    pub fn get_id(&self, string: &str) -> Option<u32> {
        self.string_to_id.get(string).copied()
    }
}

impl ColumnarTableBuilder {
    /// Create new table builder
    pub fn new() -> Self {
        Self {
            schema: TableSchema {
                columns: Vec::new(),
                primary_key: Vec::new(),
                metadata: HashMap::new(),
            },
            column_builders: HashMap::new(),
            row_count: 0,
        }
    }

    /// Add column definition
    pub fn add_column(&mut self, definition: ColumnDefinition) -> Result<()> {
        if self.column_builders.contains_key(&definition.name) {
            return Err(Error::Generic(format!(
                "Column '{}' already exists",
                definition.name
            )));
        }

        let builder = ColumnBuilder::new(definition.clone());
        self.column_builders
            .insert(definition.name.clone(), builder);
        self.schema.columns.push(definition);
        Ok(())
    }

    /// Append row to table
    pub fn append_row(&mut self, values: HashMap<String, (Value, bool)>) -> Result<()> {
        for (name, builder) in &mut self.column_builders {
            if let Some((value, is_null)) = values.get(name) {
                builder.append_value(value, *is_null)?;
            } else if builder.definition.nullable {
                builder.append_null()?;
            } else {
                return Err(Error::Generic(format!(
                    "Non-nullable column '{}' missing value",
                    name
                )));
            }
        }

        self.row_count += 1;
        Ok(())
    }

    /// Build the final table
    pub fn build(self) -> Result<ColumnarTable> {
        let mut columns = HashMap::new();
        let mut null_masks = HashMap::new();

        for (name, builder) in self.column_builders {
            let (column, null_mask) = builder.build();
            columns.insert(name.clone(), Arc::new(column));
            null_masks.insert(name, null_mask);
        }

        Ok(ColumnarTable {
            schema: self.schema,
            columns,
            row_count: self.row_count,
            null_masks,
        })
    }
}

impl ColumnBuilder {
    /// Create new column builder
    pub fn new(definition: ColumnDefinition) -> Self {
        Self {
            definition: definition.clone(),
            data: Vec::new(),
            null_mask: Vec::new(),
            string_dict: if matches!(definition.data_type, DataType::String) {
                Some(StringDictionary::new())
            } else {
                None
            },
            stats: ColumnStatistics::default(),
        }
    }

    /// Append value to column
    pub fn append_value(&mut self, value: &Value, is_null: bool) -> Result<()> {
        self.null_mask.push(is_null);

        if is_null {
            self.append_null()?;
            self.stats.null_count += 1;
        } else {
            match (&self.definition.data_type, value) {
                (DataType::Int32, Value::Int32(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Int64, Value::Int64(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Float32, Value::Float32(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Float64, Value::Float64(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Bool, Value::Bool(v)) => {
                    self.data.push(if *v { 1 } else { 0 });
                }
                (DataType::String, Value::String(s)) => {
                    if let Some(ref mut dict) = self.string_dict {
                        let id = dict.get_or_insert_id(s.clone());
                        self.data.extend_from_slice(&id.to_le_bytes());
                    } else {
                        return Err(Error::Generic(
                            "String dictionary not initialized".to_string(),
                        ));
                    }
                }
                (DataType::Date, Value::Date(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                (DataType::Timestamp, Value::Timestamp(v)) => {
                    self.data.extend_from_slice(&v.to_le_bytes());
                }
                _ => {
                    return Err(Error::Generic(
                        "Value type doesn't match column type".to_string(),
                    ))
                }
            }
        }

        self.stats.row_count += 1;
        Ok(())
    }

    pub fn append_null(&mut self) -> Result<()> {
        let size = self.definition.data_type.size_bytes();
        self.data.extend(vec![0; size]);
        Ok(())
    }

    /// Build final column
    pub fn build(mut self) -> (Column, Vec<bool>) {
        self.update_statistics();

        let mut column = Column::new(self.definition);
        column.data = self.data;
        column.string_dict = self.string_dict;
        column.stats = self.stats;

        (column, self.null_mask)
    }

    fn update_statistics(&mut self) {
        self.stats.data_size_bytes = self.data.len() as u64;
        // Additional statistics calculation can be added here
    }
}

/// Table statistics
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Number of rows
    pub row_count: u64,
    /// Number of columns
    pub column_count: u64,
    /// Total size in bytes
    pub total_size_bytes: u64,
    /// Per-column statistics
    pub column_statistics: HashMap<String, ColumnStatistics>,
}

/// Batch iterator for vectorized processing
pub struct BatchIterator<'a> {
    table: &'a ColumnarTable,
    batch_size: usize,
    current_row: usize,
}

impl<'a> BatchIterator<'a> {
    pub fn new(table: &'a ColumnarTable, batch_size: usize) -> Self {
        Self {
            table,
            batch_size,
            current_row: 0,
        }
    }
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = HashMap<String, ColumnBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row >= self.table.row_count {
            return None;
        }

        let mut batches = HashMap::new();

        for column_name in self.table.column_names() {
            if let Ok(batch) =
                self.table
                    .get_column_batch(&column_name, self.current_row, self.batch_size)
            {
                if batch.size > 0 {
                    batches.insert(column_name, batch);
                }
            }
        }

        if batches.is_empty() {
            return None;
        }

        self.current_row += self.batch_size;
        Some(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_creation() {
        let definition = ColumnDefinition {
            name: "test_col".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            default_value: None,
            metadata: HashMap::new(),
        };

        let column = Column::new(definition);
        assert_eq!(column.definition.name, "test_col");
        assert_eq!(column.definition.data_type, DataType::Int32);
    }

    #[test]
    fn test_string_dictionary() {
        let mut dict = StringDictionary::new();

        let id1 = dict.get_or_insert_id("hello".to_string());
        let id2 = dict.get_or_insert_id("world".to_string());
        let id3 = dict.get_or_insert_id("hello".to_string()); // Should return same ID

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id1, id3);

        assert_eq!(dict.get_string(id1), Some(&"hello".to_string()));
        assert_eq!(dict.get_string(id2), Some(&"world".to_string()));
    }

    #[test]
    fn test_table_builder() {
        let mut builder = ColumnarTableBuilder::new();

        // Add columns
        builder
            .add_column(ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        builder
            .add_column(ColumnDefinition {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: true,
                default_value: None,
                metadata: HashMap::new(),
            })
            .unwrap();

        // Add rows
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), (Value::Int32(1), false));
        row1.insert(
            "name".to_string(),
            (Value::String("Alice".to_string()), false),
        );
        builder.append_row(row1).unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), (Value::Int32(2), false));
        row2.insert("name".to_string(), (Value::Null, true));
        builder.append_row(row2).unwrap();

        let table = builder.build().unwrap();
        assert_eq!(table.row_count, 2);
        assert_eq!(table.columns.len(), 2);
    }

    #[test]
    fn test_column_batch() {
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
        for i in 0..100 {
            let mut row = HashMap::new();
            row.insert("values".to_string(), (Value::Int32(i), false));
            builder.append_row(row).unwrap();
        }

        let table = builder.build().unwrap();

        // Get batch
        let batch = table.get_column_batch("values", 10, 20).unwrap();
        assert_eq!(batch.size, 20);
        assert_eq!(batch.start_row, 10);
        assert_eq!(batch.data.len(), 20 * 4); // 20 i32s = 80 bytes
    }

    #[test]
    fn test_batch_iterator() {
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
        for i in 0..50 {
            let mut row = HashMap::new();
            row.insert("values".to_string(), (Value::Int32(i), false));
            builder.append_row(row).unwrap();
        }

        let table = builder.build().unwrap();
        let mut iterator = table.batch_iterator(20);

        let first_batch = iterator.next().unwrap();
        assert!(first_batch.contains_key("values"));
        assert_eq!(first_batch["values"].size, 20);

        let second_batch = iterator.next().unwrap();
        assert_eq!(second_batch["values"].size, 20);

        let third_batch = iterator.next().unwrap();
        assert_eq!(third_batch["values"].size, 10); // Remaining rows

        assert!(iterator.next().is_none());
    }
}
