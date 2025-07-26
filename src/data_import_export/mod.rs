//! Data Import/Export Utilities
//!
//! This module provides production-grade data import and export capabilities
//! with support for multiple formats, schema validation, and streaming processing.

pub mod csv_handler;
pub mod json_handler;
pub mod parquet_handler;
pub mod streaming;
pub mod transformation;
pub mod validation;

use crate::{Database, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use chrono::{DateTime, Utc};
// Enable serde feature for chrono
#[allow(unused_imports)]
use chrono::serde::ts_milliseconds;

/// Import/Export operation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStats {
    pub records_processed: u64,
    pub records_success: u64,
    pub records_failed: u64,
    pub bytes_processed: u64,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub start_time: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_milliseconds_option")]
    pub end_time: Option<DateTime<Utc>>,
    pub errors: Vec<ProcessingError>,
}

/// Processing error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingError {
    pub record_number: u64,
    pub error_type: String,
    pub message: String,
    pub data: Option<String>,
}

/// Import/Export configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportExportConfig {
    pub batch_size: usize,
    pub max_memory_mb: usize,
    pub parallel_workers: usize,
    pub validate_schema: bool,
    pub skip_errors: bool,
    pub progress_callback: Option<String>,
    pub compression: Option<CompressionType>,
    pub encoding: Option<String>,
}

/// Supported compression types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
    Lz4,
}

/// Schema definition for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub name: String,
    pub version: String,
    pub fields: Vec<FieldDefinition>,
    pub constraints: Vec<ConstraintDefinition>,
}

/// Field definition in schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub default_value: Option<serde_json::Value>,
    pub validation_rules: Vec<ValidationRule>,
}

/// Field types supported in schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    String { max_length: Option<usize> },
    Integer { min: Option<i64>, max: Option<i64> },
    Float { min: Option<f64>, max: Option<f64> },
    Boolean,
    DateTime { format: Option<String> },
    Json,
    Bytes,
}

/// Validation rules for fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationRule {
    Required,
    MinLength(usize),
    MaxLength(usize),
    Pattern(String),
    Range { min: f64, max: f64 },
    OneOf(Vec<String>),
    Custom { name: String, expression: String },
}

/// Schema constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintDefinition {
    UniqueKey { fields: Vec<String> },
    ForeignKey { fields: Vec<String>, reference_table: String, reference_fields: Vec<String> },
    Check { expression: String },
}

/// Data format for import/export
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataFormat {
    Csv {
        delimiter: char,
        quote_char: char,
        escape_char: Option<char>,
        headers: bool,
    },
    Json {
        pretty: bool,
        array_format: bool,
    },
    Parquet {
        compression: Option<String>,
        row_group_size: Option<usize>,
    },
    Custom {
        format_name: String,
        options: BTreeMap<String, String>,
    },
}

/// Import/Export operation result
#[derive(Debug)]
pub struct OperationResult {
    pub stats: OperationStats,
    pub output_path: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

/// Main import/export manager
pub struct ImportExportManager {
    database: Database,
    config: ImportExportConfig,
}

impl Default for ImportExportConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            max_memory_mb: 512,
            parallel_workers: 4,
            validate_schema: true,
            skip_errors: false,
            progress_callback: None,
            compression: None,
            encoding: Some("utf-8".to_string()),
        }
    }
}

impl ImportExportManager {
    /// Create new import/export manager
    pub fn new(database: Database, config: ImportExportConfig) -> Self {
        Self { database, config }
    }
    
    /// Import data from file
    pub fn import_from_file<P: AsRef<Path>>(
        &self,
        file_path: P,
        format: DataFormat,
        schema: Option<SchemaDefinition>,
        table_prefix: &str,
    ) -> Result<OperationResult> {
        let mut stats = OperationStats {
            records_processed: 0,
            records_success: 0,
            records_failed: 0,
            bytes_processed: 0,
            start_time: Utc::now(),
            end_time: None,
            errors: Vec::new(),
        };
        
        match format {
            DataFormat::Csv { .. } => {
                csv_handler::import_csv(
                    &self.database,
                    file_path.as_ref(),
                    &format,
                    schema.as_ref(),
                    table_prefix,
                    &self.config,
                    &mut stats,
                )
            },
            DataFormat::Json { .. } => {
                json_handler::import_json(
                    &self.database,
                    file_path.as_ref(),
                    &format,
                    schema.as_ref(),
                    table_prefix,
                    &self.config,
                    &mut stats,
                )
            },
            DataFormat::Parquet { .. } => {
                parquet_handler::import_parquet(
                    &self.database,
                    file_path.as_ref(),
                    &format,
                    schema.as_ref(),
                    table_prefix,
                    &self.config,
                    &mut stats,
                )
            },
            DataFormat::Custom { .. } => {
                return Err(crate::Error::InvalidInput(
                    "Custom formats not yet implemented".to_string()
                ));
            },
        }?;
        
        stats.end_time = Some(Utc::now());
        
        Ok(OperationResult {
            stats,
            output_path: None,
            metadata: BTreeMap::new(),
        })
    }
    
    /// Export data to file
    pub fn export_to_file<P: AsRef<Path>>(
        &self,
        table_prefix: &str,
        file_path: P,
        format: DataFormat,
        schema: Option<SchemaDefinition>,
    ) -> Result<OperationResult> {
        let mut stats = OperationStats {
            records_processed: 0,
            records_success: 0,
            records_failed: 0,
            bytes_processed: 0,
            start_time: Utc::now(),
            end_time: None,
            errors: Vec::new(),
        };
        
        match format {
            DataFormat::Csv { .. } => {
                csv_handler::export_csv(
                    &self.database,
                    table_prefix,
                    file_path.as_ref(),
                    &format,
                    schema.as_ref(),
                    &self.config,
                    &mut stats,
                )
            },
            DataFormat::Json { .. } => {
                json_handler::export_json(
                    &self.database,
                    table_prefix,
                    file_path.as_ref(),
                    &format,
                    schema.as_ref(),
                    &self.config,
                    &mut stats,
                )
            },
            DataFormat::Parquet { .. } => {
                parquet_handler::export_parquet(
                    &self.database,
                    table_prefix,
                    file_path.as_ref(),
                    &format,
                    schema.as_ref(),
                    &self.config,
                    &mut stats,
                )
            },
            DataFormat::Custom { .. } => {
                return Err(crate::Error::InvalidInput(
                    "Custom formats not yet implemented".to_string()
                ));
            },
        }?;
        
        stats.end_time = Some(Utc::now());
        
        Ok(OperationResult {
            stats,
            output_path: Some(file_path.as_ref().to_string_lossy().to_string()),
            metadata: BTreeMap::new(),
        })
    }
    
    /// Stream import data with callback
    pub fn stream_import<F>(
        &self,
        format: DataFormat,
        schema: Option<SchemaDefinition>,
        table_prefix: &str,
        data_callback: F,
    ) -> Result<OperationResult>
    where
        F: Fn() -> Option<Vec<u8>>,
    {
        streaming::stream_import(
            &self.database,
            format,
            schema,
            table_prefix,
            &self.config,
            data_callback,
        )
    }
    
    /// Stream export data with callback
    pub fn stream_export<F>(
        &self,
        table_prefix: &str,
        format: DataFormat,
        schema: Option<SchemaDefinition>,
        output_callback: F,
    ) -> Result<OperationResult>
    where
        F: Fn(Vec<u8>) -> Result<()>,
    {
        streaming::stream_export(
            &self.database,
            table_prefix,
            format,
            schema,
            &self.config,
            output_callback,
        )
    }
    
    /// Validate data against schema
    pub fn validate_data(
        &self,
        data: &serde_json::Value,
        schema: &SchemaDefinition,
    ) -> Result<Vec<ValidationError>> {
        validation::validate_record(data, schema)
    }
    
    /// Get operation statistics
    pub fn get_operation_stats(&self, operation_id: &str) -> Result<Option<OperationStats>> {
        let key = format!("_import_export_stats_{}", operation_id);
        match self.database.get(key.as_bytes()) {
            Ok(data) => {
                let stats: OperationStats = serde_json::from_slice(&data.as_ref().unwrap())?;
                Ok(Some(stats))
            },
            Err(_) => Ok(None),
        }
    }
}

/// Validation error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub field: String,
    pub rule: String,
    pub message: String,
    pub value: Option<serde_json::Value>,
}

impl OperationStats {
    pub fn new() -> Self {
        Self {
            records_processed: 0,
            records_success: 0,
            records_failed: 0,
            bytes_processed: 0,
            start_time: Utc::now(),
            end_time: None,
            errors: Vec::new(),
        }
    }
    
    pub fn add_error(&mut self, error: ProcessingError) {
        self.errors.push(error);
        self.records_failed += 1;
    }
    
    pub fn add_success(&mut self, bytes: usize) {
        self.records_success += 1;
        self.bytes_processed += bytes as u64;
    }
    
    pub fn finish(&mut self) {
        self.end_time = Some(Utc::now());
    }
    
    pub fn duration_seconds(&self) -> f64 {
        let end = self.end_time.unwrap_or_else(Utc::now);
        end.signed_duration_since(self.start_time).num_milliseconds() as f64 / 1000.0
    }
    
    pub fn success_rate(&self) -> f64 {
        if self.records_processed == 0 {
            0.0
        } else {
            self.records_success as f64 / self.records_processed as f64
        }
    }
    
    pub fn throughput_records_per_second(&self) -> f64 {
        let duration = self.duration_seconds();
        if duration > 0.0 {
            self.records_processed as f64 / duration
        } else {
            0.0
        }
    }
    
    pub fn throughput_mb_per_second(&self) -> f64 {
        let duration = self.duration_seconds();
        if duration > 0.0 {
            (self.bytes_processed as f64 / 1_048_576.0) / duration
        } else {
            0.0
        }
    }
}

impl std::fmt::Display for OperationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Records: {}/{} ({:.1}%), Duration: {:.2}s, Throughput: {:.0} rec/s, {:.2} MB/s",
               self.records_success,
               self.records_processed,
               self.success_rate() * 100.0,
               self.duration_seconds(),
               self.throughput_records_per_second(),
               self.throughput_mb_per_second())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_stats() {
        let mut stats = OperationStats::new();
        
        // Add some operations
        stats.records_processed = 100;
        stats.add_success(1024);
        stats.add_success(2048);
        
        assert_eq!(stats.records_success, 2);
        assert_eq!(stats.bytes_processed, 3072);
        assert_eq!(stats.success_rate(), 0.02); // 2/100
    }
    
    #[test]
    fn test_import_export_config_default() {
        let config = ImportExportConfig::default();
        
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.max_memory_mb, 512);
        assert_eq!(config.parallel_workers, 4);
        assert!(config.validate_schema);
        assert!(!config.skip_errors);
    }
    
    #[test]
    fn test_data_format_csv() {
        let format = DataFormat::Csv {
            delimiter: ',',
            quote_char: '"',
            escape_char: None,
            headers: true,
        };
        
        match format {
            DataFormat::Csv { delimiter, .. } => assert_eq!(delimiter, ','),
            _ => panic!("Expected CSV format"),
        }
    }
    
    #[test]
    fn test_schema_definition() {
        let schema = SchemaDefinition {
            name: "test_schema".to_string(),
            version: "1.0".to_string(),
            fields: vec![
                FieldDefinition {
                    name: "id".to_string(),
                    field_type: FieldType::Integer { min: Some(0), max: None },
                    nullable: false,
                    default_value: None,
                    validation_rules: vec![ValidationRule::Required],
                },
                FieldDefinition {
                    name: "name".to_string(),
                    field_type: FieldType::String { max_length: Some(255) },
                    nullable: false,
                    default_value: None,
                    validation_rules: vec![ValidationRule::Required, ValidationRule::MaxLength(255)],
                },
            ],
            constraints: vec![],
        };
        
        assert_eq!(schema.name, "test_schema");
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
    }
}