//! Parquet Import/Export Handler
//!
//! Provides comprehensive Parquet format support for analytics workloads.
//! Supports reading from and writing to Apache Parquet files with
//! schema inference, compression, and optimized columnar storage.

use super::{
    DataFormat, FieldType, ImportExportConfig, OperationStats, ProcessingError, SchemaDefinition,
};
use crate::{Database, Result};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use base64::prelude::*;
use chrono::{DateTime, Utc};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
    },
};

/// Import Parquet data with full schema support and optimization
pub fn import_parquet(
    database: &Database,
    file_path: &Path,
    format: &DataFormat,
    schema: Option<&SchemaDefinition>,
    table_prefix: &str,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    let file = File::open(file_path)
        .map_err(|e| crate::Error::Io(format!("Failed to open Parquet file: {}", e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        crate::Error::InvalidInput(format!("Failed to create Parquet reader: {}", e))
    })?;

    let mut arrow_reader = builder
        .with_batch_size(config.batch_size)
        .build()
        .map_err(|e| crate::Error::InvalidInput(format!("Failed to build Arrow reader: {}", e)))?;

    let parquet_schema = arrow_reader.schema();
    let mut record_number = 0u64;
    let mut total_bytes = 0u64;

    // Validate schema if provided
    if let Some(expected_schema) = schema {
        validate_parquet_schema(&parquet_schema, expected_schema, stats)?;
    }

    // Process batches
    while let Some(batch_result) = arrow_reader.next() {
        let batch = batch_result
            .map_err(|e| crate::Error::InvalidInput(format!("Failed to read batch: {}", e)))?;

        total_bytes += estimate_batch_size(&batch);

        // Convert Arrow batch to Lightning DB records
        let records =
            convert_arrow_batch_to_records(&batch, table_prefix, &mut record_number, stats)?;

        // Insert records in batches
        for record in records {
            match database.put(&record.key, &record.value) {
                Ok(_) => stats.records_success += 1,
                Err(e) => {
                    stats.records_failed += 1;
                    if !config.skip_errors {
                        return Err(e);
                    }
                    stats.errors.push(ProcessingError {
                        record_number,
                        error_type: "DatabaseInsertError".to_string(),
                        message: e.to_string(),
                        data: Some(format!("key: {:?}", record.key)),
                    });
                }
            }
        }

        stats.records_processed += batch.num_rows() as u64;
        stats.bytes_processed = total_bytes;
    }

    stats.end_time = Some(Utc::now());
    Ok(())
}

/// Export to Parquet format with compression and optimization
pub fn export_parquet(
    database: &Database,
    table_prefix: &str,
    file_path: &Path,
    format: &DataFormat,
    schema: Option<&SchemaDefinition>,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    let file = File::create(file_path)
        .map_err(|e| crate::Error::Io(format!("Failed to create Parquet file: {}", e)))?;

    // Determine compression and writer properties from format
    let (compression, row_group_size) = match format {
        DataFormat::Parquet {
            compression,
            row_group_size,
        } => {
            let comp = match compression.as_deref() {
                Some("snappy") => Compression::SNAPPY,
                Some("gzip") => Compression::GZIP(Default::default()),
                Some("lz4") => Compression::LZ4,
                Some("zstd") => Compression::ZSTD(Default::default()),
                Some("brotli") => Compression::BROTLI(Default::default()),
                _ => Compression::SNAPPY, // Default
            };
            (comp, row_group_size.unwrap_or(config.batch_size))
        }
        _ => (Compression::SNAPPY, config.batch_size),
    };

    let properties = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(row_group_size)
        .build();

    // Create Arrow schema from Lightning DB data or provided schema
    let arrow_schema = if let Some(lightning_schema) = schema {
        create_arrow_schema_from_definition(lightning_schema)?
    } else {
        // Infer schema by scanning some records
        infer_arrow_schema_from_database(database, table_prefix)?
    };

    let arrow_schema_arc = Arc::new(arrow_schema.clone());
    let mut arrow_writer = ArrowWriter::try_new(file, arrow_schema_arc.clone(), Some(properties))
        .map_err(|e| {
        crate::Error::InvalidInput(format!("Failed to create Arrow writer: {}", e))
    })?;

    // Collect data from Lightning DB
    let mut records = Vec::new();
    let prefix_bytes = table_prefix.as_bytes();

    // Use range scan to get all records with the table prefix
    let iter = database
        .range(Some(prefix_bytes), None)
        .map_err(|e| crate::Error::Generic(format!("Failed to scan database: {}", e)))?;

    for (key, value) in iter {
        if key.starts_with(prefix_bytes) {
            records.push(ParquetRecord { key, value });
            stats.records_processed += 1;

            // Process in batches to manage memory
            if records.len() >= config.batch_size {
                let batch = create_arrow_batch_from_records(&records, &arrow_schema, stats)?;
                arrow_writer.write(&batch).map_err(|e| {
                    crate::Error::InvalidInput(format!("Failed to write batch: {}", e))
                })?;
                stats.records_success += records.len() as u64;
                records.clear();
            }
        }
    }

    // Write remaining records
    if !records.is_empty() {
        let batch = create_arrow_batch_from_records(&records, &arrow_schema, stats)?;
        arrow_writer.write(&batch).map_err(|e| {
            crate::Error::InvalidInput(format!("Failed to write final batch: {}", e))
        })?;
        stats.records_success += records.len() as u64;
    }

    arrow_writer.close().map_err(|e| {
        crate::Error::InvalidInput(format!("Failed to close Parquet writer: {}", e))
    })?;

    stats.end_time = Some(Utc::now());
    Ok(())
}

/// Parquet utilities for analysis and introspection
pub struct ParquetUtils;

impl ParquetUtils {
    /// Analyze Parquet file structure and metadata
    pub fn analyze_parquet_file<P: AsRef<Path>>(file_path: P) -> Result<ParquetFileInfo> {
        let file = File::open(file_path.as_ref())
            .map_err(|e| crate::Error::Io(format!("Failed to open Parquet file: {}", e)))?;

        let file_reader = SerializedFileReader::new(file)
            .map_err(|e| crate::Error::InvalidInput(format!("Invalid Parquet file: {}", e)))?;

        let metadata = file_reader.metadata();
        let schema = metadata.file_metadata().schema();

        let mut total_rows = 0u64;
        for i in 0..metadata.num_row_groups() {
            total_rows += metadata.row_group(i).num_rows() as u64;
        }

        let file_size = std::fs::metadata(file_path.as_ref())
            .map_err(|e| crate::Error::Io(format!("Failed to get file size: {}", e)))?
            .len();

        let compression = if metadata.num_row_groups() > 0 {
            metadata
                .row_group(0)
                .columns()
                .iter()
                .map(|col| format!("{:?}", col.compression()))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "Unknown".to_string()
        };

        let schema_info = format!(
            "Fields: {}, Root schema: {}",
            schema.get_fields().len(),
            schema.name()
        );

        Ok(ParquetFileInfo {
            row_count: total_rows,
            column_count: schema.get_fields().len(),
            file_size_bytes: file_size,
            compression,
            schema_info,
        })
    }

    /// Get detailed column statistics
    pub fn get_column_statistics<P: AsRef<Path>>(file_path: P) -> Result<Vec<ColumnStatistics>> {
        let file = File::open(file_path.as_ref())
            .map_err(|e| crate::Error::Io(format!("Failed to open Parquet file: {}", e)))?;

        let file_reader = SerializedFileReader::new(file)
            .map_err(|e| crate::Error::InvalidInput(format!("Invalid Parquet file: {}", e)))?;

        let metadata = file_reader.metadata();
        let mut column_stats = Vec::new();

        // Get statistics from row groups instead of schema details
        for rg_idx in 0..metadata.num_row_groups() {
            let row_group = metadata.row_group(rg_idx);

            for col_idx in 0..row_group.num_columns() {
                let column_chunk = row_group.column(col_idx);

                // Only process first row group for schema info
                if rg_idx == 0 {
                    let column_path = column_chunk.column_path();
                    let column_name = column_path.parts().join(".");

                    let mut total_null_count = 0u64;
                    let mut total_value_count = 0u64;

                    // Aggregate across all row groups for this column
                    for rg in 0..metadata.num_row_groups() {
                        let rg_meta = metadata.row_group(rg);
                        let col_chunk = rg_meta.column(col_idx);

                        if let Some(stats) = col_chunk.statistics() {
                            total_null_count += stats.null_count_opt().unwrap_or(0);
                            total_value_count += col_chunk.num_values() as u64;
                        }
                    }

                    column_stats.push(ColumnStatistics {
                        name: column_name,
                        data_type: format!("{:?}", column_chunk.column_type()),
                        null_count: total_null_count,
                        value_count: total_value_count,
                        compression_ratio: 0.0, // Would need uncompressed size calculation
                    });
                }
            }
            break; // Only process first row group for schema
        }

        Ok(column_stats)
    }
}

/// Parquet file information
#[derive(Debug, Clone)]
pub struct ParquetFileInfo {
    pub row_count: u64,
    pub column_count: usize,
    pub file_size_bytes: u64,
    pub compression: String,
    pub schema_info: String,
}

/// Column statistics for Parquet files
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub name: String,
    pub data_type: String,
    pub null_count: u64,
    pub value_count: u64,
    pub compression_ratio: f64,
}

/// Internal record structure for Parquet processing
#[derive(Debug, Clone)]
struct ParquetRecord {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Lightning DB record for import
#[derive(Debug, Clone)]
struct LightningRecord {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Helper functions for Parquet processing

/// Validate Parquet schema against expected Lightning DB schema
fn validate_parquet_schema(
    parquet_schema: &Schema,
    expected_schema: &SchemaDefinition,
    stats: &mut OperationStats,
) -> Result<()> {
    let parquet_fields: HashMap<String, &Field> = parquet_schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.as_ref()))
        .collect();

    for field_def in &expected_schema.fields {
        if let Some(parquet_field) = parquet_fields.get(&field_def.name) {
            // Validate field type compatibility
            if !is_compatible_type(parquet_field.data_type(), &field_def.field_type) {
                stats.errors.push(ProcessingError {
                    record_number: 0,
                    error_type: "SchemaValidationError".to_string(),
                    message: format!(
                        "Incompatible type for field '{}': expected {:?}, found {:?}",
                        field_def.name,
                        field_def.field_type,
                        parquet_field.data_type()
                    ),
                    data: None,
                });
            }
        } else if !field_def.nullable {
            return Err(crate::Error::InvalidInput(format!(
                "Required field '{}' not found in Parquet schema",
                field_def.name
            )));
        }
    }

    Ok(())
}

/// Check if Arrow data type is compatible with Lightning DB field type
fn is_compatible_type(arrow_type: &DataType, lightning_type: &FieldType) -> bool {
    match (arrow_type, lightning_type) {
        (DataType::Utf8 | DataType::LargeUtf8, FieldType::String { .. }) => true,
        (DataType::Int32 | DataType::Int64, FieldType::Integer { .. }) => true,
        (DataType::Float32 | DataType::Float64, FieldType::Float { .. }) => true,
        (DataType::Boolean, FieldType::Boolean) => true,
        (
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64,
            FieldType::DateTime { .. },
        ) => true,
        (DataType::Binary | DataType::LargeBinary, FieldType::Bytes) => true,
        _ => false,
    }
}

/// Estimate memory size of an Arrow RecordBatch
fn estimate_batch_size(batch: &RecordBatch) -> u64 {
    let mut size = 0u64;
    for column in batch.columns() {
        size += column.get_array_memory_size() as u64;
    }
    size
}

/// Convert Arrow RecordBatch to Lightning DB records
fn convert_arrow_batch_to_records(
    batch: &RecordBatch,
    table_prefix: &str,
    record_number: &mut u64,
    stats: &mut OperationStats,
) -> Result<Vec<LightningRecord>> {
    let mut records = Vec::new();
    let num_rows = batch.num_rows();

    for row_idx in 0..num_rows {
        *record_number += 1;

        // Create a key using table prefix and row number
        let key = format!("{}_{:010}", table_prefix, *record_number).into_bytes();

        // Serialize row data as JSON for the value
        let mut row_data = serde_json::Map::new();

        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let field_name = field.name().clone();

            let value = match extract_value_from_array(column, row_idx) {
                Ok(v) => v,
                Err(e) => {
                    stats.errors.push(ProcessingError {
                        record_number: *record_number,
                        error_type: "DataExtractionError".to_string(),
                        message: format!(
                            "Failed to extract value for field '{}': {}",
                            field_name, e
                        ),
                        data: None,
                    });
                    continue;
                }
            };

            row_data.insert(field_name, value);
        }

        let value = serde_json::to_vec(&row_data).map_err(|e| {
            crate::Error::InvalidInput(format!("Failed to serialize row data: {}", e))
        })?;

        records.push(LightningRecord { key, value });
    }

    Ok(records)
}

/// Extract JSON value from Arrow array at specific index
fn extract_value_from_array(array: &dyn Array, index: usize) -> Result<serde_json::Value> {
    if array.is_null(index) {
        return Ok(serde_json::Value::Null);
    }

    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(serde_json::Value::String(
                string_array.value(index).to_string(),
            ))
        }
        DataType::LargeUtf8 => {
            let string_array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(serde_json::Value::String(
                string_array.value(index).to_string(),
            ))
        }
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(serde_json::Value::Number(serde_json::Number::from(
                int_array.value(index),
            )))
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(serde_json::Value::Number(serde_json::Number::from(
                int_array.value(index),
            )))
        }
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let val = float_array.value(index) as f64;
            Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(val).unwrap_or_else(|| serde_json::Number::from(0)),
            ))
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = float_array.value(index);
            Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(val).unwrap_or_else(|| serde_json::Number::from(0)),
            ))
        }
        DataType::Boolean => {
            let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(serde_json::Value::Bool(bool_array.value(index)))
        }
        DataType::Timestamp(_, _) => {
            let timestamp_array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let timestamp = timestamp_array.value(index);
            let dt = DateTime::from_timestamp_nanos(timestamp);
            Ok(serde_json::Value::String(dt.to_rfc3339()))
        }
        DataType::Binary => {
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = binary_array.value(index);
            Ok(serde_json::Value::String(
                base64::prelude::BASE64_STANDARD.encode(bytes),
            ))
        }
        _ => Err(crate::Error::InvalidInput(format!(
            "Unsupported Arrow data type: {:?}",
            array.data_type()
        ))),
    }
}

/// Create Arrow schema from Lightning DB schema definition
fn create_arrow_schema_from_definition(schema_def: &SchemaDefinition) -> Result<Schema> {
    let mut fields = Vec::new();

    for field_def in &schema_def.fields {
        let arrow_type = match &field_def.field_type {
            FieldType::String { .. } => DataType::Utf8,
            FieldType::Integer { .. } => DataType::Int64,
            FieldType::Float { .. } => DataType::Float64,
            FieldType::Boolean => DataType::Boolean,
            FieldType::DateTime { .. } => DataType::Timestamp(TimeUnit::Nanosecond, None),
            FieldType::Json => DataType::Utf8, // Store JSON as string
            FieldType::Bytes => DataType::Binary,
        };

        fields.push(Field::new(&field_def.name, arrow_type, field_def.nullable));
    }

    Ok(Schema::new(fields))
}

/// Infer Arrow schema by examining database records
fn infer_arrow_schema_from_database(database: &Database, table_prefix: &str) -> Result<Schema> {
    // Sample a few records to infer schema
    let prefix_bytes = table_prefix.as_bytes();
    let mut sample_records: Vec<Vec<u8>> = Vec::new();

    let iter = database
        .range(Some(prefix_bytes), None)
        .map_err(|e| crate::Error::Generic(format!("Failed to scan database: {}", e)))?;

    let mut count = 0;
    for (key, value) in iter {
        if key.starts_with(prefix_bytes) && count < 10 {
            sample_records.push(value.to_vec());
            count += 1;
        } else if count >= 10 {
            break;
        }
    }

    if sample_records.is_empty() {
        return Err(crate::Error::InvalidInput(
            "No records found to infer schema".to_string(),
        ));
    }

    // Parse first record to determine field structure
    let first_record: serde_json::Value =
        serde_json::from_slice(&sample_records[0]).map_err(|e| {
            crate::Error::InvalidInput(format!("Failed to parse record as JSON: {}", e))
        })?;

    let mut fields = Vec::new();

    if let serde_json::Value::Object(obj) = first_record {
        for (key, value) in obj {
            let arrow_type = match value {
                serde_json::Value::String(_) => DataType::Utf8,
                serde_json::Value::Number(n) => {
                    if n.is_i64() {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    }
                }
                serde_json::Value::Bool(_) => DataType::Boolean,
                serde_json::Value::Null => DataType::Utf8, // Default to string for null
                _ => DataType::Utf8,                       // Complex types as strings
            };

            fields.push(Field::new(&key, arrow_type, true)); // Allow nulls by default
        }
    }

    if fields.is_empty() {
        return Err(crate::Error::InvalidInput(
            "Could not infer any fields from sample records".to_string(),
        ));
    }

    Ok(Schema::new(fields))
}

/// Create Arrow RecordBatch from Lightning DB records
fn create_arrow_batch_from_records(
    records: &[ParquetRecord],
    schema: &Schema,
    stats: &mut OperationStats,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields() {
        let array = create_array_for_field(field, records, stats)?;
        columns.push(array);
    }

    RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| crate::Error::InvalidInput(format!("Failed to create RecordBatch: {}", e)))
}

/// Create Arrow array for a specific field from records
fn create_array_for_field(
    field: &Field,
    records: &[ParquetRecord],
    _stats: &mut OperationStats,
) -> Result<ArrayRef> {
    let field_name = field.name();

    match field.data_type() {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for record in records {
                if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&record.value) {
                    if let Some(value) = json_value.get(field_name) {
                        if let Some(s) = value.as_str() {
                            builder.append_value(s);
                        } else {
                            builder.append_value(&value.to_string());
                        }
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for record in records {
                if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&record.value) {
                    if let Some(value) = json_value.get(field_name) {
                        if let Some(i) = value.as_i64() {
                            builder.append_value(i);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for record in records {
                if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&record.value) {
                    if let Some(value) = json_value.get(field_name) {
                        if let Some(f) = value.as_f64() {
                            builder.append_value(f);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for record in records {
                if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&record.value) {
                    if let Some(value) = json_value.get(field_name) {
                        if let Some(b) = value.as_bool() {
                            builder.append_value(b);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            // Default to string representation for unsupported types
            let mut builder = StringBuilder::new();
            for record in records {
                if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&record.value) {
                    if let Some(value) = json_value.get(field_name) {
                        builder.append_value(&value.to_string());
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::{tempdir, NamedTempFile};

    #[test]
    fn test_parquet_file_info() {
        // This test would require creating an actual Parquet file
        // For now, just test the structure
        let info = ParquetFileInfo {
            row_count: 1000,
            column_count: 5,
            file_size_bytes: 4096,
            compression: "SNAPPY".to_string(),
            schema_info: "Fields: 5, Root schema: test_schema".to_string(),
        };

        assert_eq!(info.row_count, 1000);
        assert_eq!(info.column_count, 5);
    }

    #[test]
    fn test_column_statistics() {
        let stats = ColumnStatistics {
            name: "test_column".to_string(),
            data_type: "INT64".to_string(),
            null_count: 10,
            value_count: 1000,
            compression_ratio: 0.75,
        };

        assert_eq!(stats.name, "test_column");
        assert_eq!(stats.null_count, 10);
    }

    #[test]
    fn test_type_compatibility() {
        assert!(is_compatible_type(
            &DataType::Utf8,
            &FieldType::String { max_length: None }
        ));
        assert!(is_compatible_type(
            &DataType::Int64,
            &FieldType::Integer {
                min: None,
                max: None
            }
        ));
        assert!(is_compatible_type(&DataType::Boolean, &FieldType::Boolean));
        assert!(!is_compatible_type(&DataType::Utf8, &FieldType::Boolean));
    }
}
