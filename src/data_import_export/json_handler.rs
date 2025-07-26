//! JSON Import/Export Handler
//!
//! Provides comprehensive JSON processing with support for nested data structures,
//! array handling, schema validation, and streaming processing.

use crate::{Database, Result};
use super::{
    DataFormat, SchemaDefinition, ImportExportConfig, OperationStats, ProcessingError,
    validation::{self, ValidationError},
};
use serde_json::{Value, Map};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, BufRead, Write};
use std::path::Path;

/// JSON-specific configuration
#[derive(Debug, Clone)]
pub struct JsonConfig {
    pub pretty_print: bool,
    pub array_format: bool,  // true: [{},{}], false: {}\n{}\n (JSONL)
    pub max_nesting_depth: usize,
    pub validate_json: bool,
    pub flatten_arrays: bool,
    pub preserve_order: bool,
}

impl Default for JsonConfig {
    fn default() -> Self {
        Self {
            pretty_print: false,
            array_format: true,
            max_nesting_depth: 32,
            validate_json: true,
            flatten_arrays: false,
            preserve_order: true,
        }
    }
}

/// JSON array processing strategy
#[derive(Debug, Clone)]
pub enum ArrayProcessingStrategy {
    /// Store each array element as separate records
    Flatten { prefix: String },
    /// Store entire array as single record
    Preserve,
    /// Extract nested objects from arrays
    ExtractObjects { key_field: String },
}

/// Import JSON data into the database
pub fn import_json(
    database: &Database,
    file_path: &Path,
    format: &DataFormat,
    schema: Option<&SchemaDefinition>,
    table_prefix: &str,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    println!("📜 Importing JSON from: {:?}", file_path);
    
    // Extract JSON configuration
    let json_config = match format {
        DataFormat::Json { pretty, array_format } => {
            JsonConfig {
                pretty_print: *pretty,
                array_format: *array_format,
                ..JsonConfig::default()
            }
        },
        _ => return Err(crate::Error::InvalidInput("Expected JSON format".to_string())),
    };
    
    // Read and parse JSON file
    let file = File::open(file_path)?;
    let buf_reader = BufReader::new(file);
    
    if json_config.array_format {
        // Parse as JSON array or single object
        import_json_array(database, buf_reader, &json_config, schema, table_prefix, config, stats)
    } else {
        // Parse as JSONL (newline-delimited JSON)
        import_jsonl(database, buf_reader, &json_config, schema, table_prefix, config, stats)
    }
}

/// Import JSON array or single object
fn import_json_array<R: std::io::Read>(
    database: &Database,
    mut reader: R,
    json_config: &JsonConfig,
    schema: Option<&SchemaDefinition>,
    table_prefix: &str,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    // Read entire file into memory (for now - could be optimized for streaming)
    let mut contents = String::new();
    reader.read_to_string(&mut contents)?;
    
    let json_value: Value = serde_json::from_str(&contents)
        .map_err(|e| crate::Error::ParseError(format!("Invalid JSON: {}", e)))?;
    
    match json_value {
        Value::Array(array) => {
            println!("📋 Processing JSON array with {} elements", array.len());
            
            let mut batch = Vec::new();
            let mut record_number = 1;
            
            for item in array {
                stats.records_processed += 1;
                
                match process_json_record(&item, schema, record_number, json_config) {
                    Ok(processed_records) => {
                        batch.extend(processed_records);
                        
                        // Process batch when full
                        if batch.len() >= config.batch_size {
                            process_json_batch(database, &batch, table_prefix, stats)?;
                            batch.clear();
                        }
                    },
                    Err(validation_errors) => {
                        handle_validation_errors(validation_errors, record_number, config, stats)?;
                    },
                }
                
                record_number += 1;
                report_progress(stats);
            }
            
            // Process remaining batch
            if !batch.is_empty() {
                process_json_batch(database, &batch, table_prefix, stats)?;
            }
        },
        Value::Object(_) => {
            println!("📋 Processing single JSON object");
            
            match process_json_record(&json_value, schema, 1, json_config) {
                Ok(processed_records) => {
                    stats.records_processed += processed_records.len() as u64;
                    process_json_batch(database, &processed_records, table_prefix, stats)?;
                },
                Err(validation_errors) => {
                    handle_validation_errors(validation_errors, 1, config, stats)?;
                },
            }
        },
        _ => {
            return Err(crate::Error::InvalidInput(
                "JSON must be an object or array".to_string()
            ));
        },
    }
    
    println!("✅ JSON import completed: {}", stats);
    Ok(())
}

/// Import JSONL (newline-delimited JSON)
fn import_jsonl<R: std::io::Read>(
    database: &Database,
    reader: R,
    json_config: &JsonConfig,
    schema: Option<&SchemaDefinition>,
    table_prefix: &str,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    println!("📋 Processing JSONL format");
    
    let buf_reader = BufReader::new(reader);
    let mut batch = Vec::new();
    let mut record_number = 1;
    
    for line in buf_reader.lines() {
        match line {
            Ok(line_content) => {
                if line_content.trim().is_empty() {
                    continue;
                }
                
                stats.records_processed += 1;
                
                match serde_json::from_str::<Value>(&line_content) {
                    Ok(json_value) => {
                        match process_json_record(&json_value, schema, record_number, json_config) {
                            Ok(processed_records) => {
                                batch.extend(processed_records);
                                
                                // Process batch when full
                                if batch.len() >= config.batch_size {
                                    process_json_batch(database, &batch, table_prefix, stats)?;
                                    batch.clear();
                                }
                            },
                            Err(validation_errors) => {
                                handle_validation_errors(validation_errors, record_number, config, stats)?;
                            },
                        }
                    },
                    Err(e) => {
                        let error = ProcessingError {
                            record_number,
                            error_type: "json_parse".to_string(),
                            message: format!("Invalid JSON on line {}: {}", record_number, e),
                            data: Some(line_content),
                        };
                        
                        if config.skip_errors {
                            stats.add_error(error);
                        } else {
                            return Err(crate::Error::ParseError(e.to_string()));
                        }
                    },
                }
            },
            Err(e) => {
                if config.skip_errors {
                    stats.add_error(ProcessingError {
                        record_number,
                        error_type: "read_error".to_string(),
                        message: e.to_string(),
                        data: None,
                    });
                } else {
                    return Err(crate::Error::IoError(e.to_string()));
                }
            },
        }
        
        record_number += 1;
        report_progress(stats);
    }
    
    // Process remaining batch
    if !batch.is_empty() {
        process_json_batch(database, &batch, table_prefix, stats)?;
    }
    
    println!("✅ JSONL import completed: {}", stats);
    Ok(())
}

/// Export data to JSON format
pub fn export_json(
    database: &Database,
    table_prefix: &str,
    file_path: &Path,
    format: &DataFormat,
    schema: Option<&SchemaDefinition>,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    println!("📤 Exporting to JSON: {:?}", file_path);
    
    // Extract JSON configuration
    let json_config = match format {
        DataFormat::Json { pretty, array_format } => {
            JsonConfig {
                pretty_print: *pretty,
                array_format: *array_format,
                ..JsonConfig::default()
            }
        },
        _ => return Err(crate::Error::InvalidInput("Expected JSON format".to_string())),
    };
    
    // Create output file
    let file = File::create(file_path)?;
    let mut buf_writer = BufWriter::new(file);
    
    // Scan database for records
    let scan = database.scan(
        Some(format!("{}_", table_prefix).as_bytes().to_vec()),
        Some(format!("{}~", table_prefix).as_bytes().to_vec()),
    )?;
    
    if json_config.array_format {
        export_as_json_array(database, scan, &mut buf_writer, &json_config, schema, config, stats)
    } else {
        export_as_jsonl(database, scan, &mut buf_writer, &json_config, schema, config, stats)
    }
}

/// Export as JSON array
fn export_as_json_array<W: Write>(
    _database: &Database,
    scan: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
    writer: &mut W,
    json_config: &JsonConfig,
    schema: Option<&SchemaDefinition>,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    // Start JSON array
    write!(writer, "[")?;
    
    let mut first_record = true;
    
    for item in scan {
        let (key, value) = item?;
        stats.records_processed += 1;
        
        match serde_json::from_slice::<Value>(&value) {
            Ok(json_value) => {
                // Apply schema transformations if needed
                let processed_value = if let Some(schema_def) = schema {
                    apply_schema_transformations(&json_value, schema_def)?
                } else {
                    json_value
                };
                
                // Write record separator
                if !first_record {
                    write!(writer, ",")?;
                }
                
                if json_config.pretty_print {
                    write!(writer, "\n  ")?;
                }
                
                // Write JSON record
                if json_config.pretty_print {
                    serde_json::to_writer_pretty(&mut *writer, &processed_value)?;
                } else {
                    serde_json::to_writer(&mut *writer, &processed_value)?;
                }
                
                stats.add_success(value.len());
                first_record = false;
            },
            Err(e) => {
                if config.skip_errors {
                    stats.add_error(ProcessingError {
                        record_number: stats.records_processed,
                        error_type: "json_parse".to_string(),
                        message: e.to_string(),
                        data: Some(String::from_utf8_lossy(&key).to_string()),
                    });
                } else {
                    return Err(crate::Error::ParseError(e.to_string()));
                }
            },
        }
        
        report_progress(stats);
    }
    
    // Close JSON array
    if json_config.pretty_print {
        write!(writer, "\n]")?;
    } else {
        write!(writer, "]")?;
    }
    
    writer.flush()?;
    println!("✅ JSON array export completed: {}", stats);
    Ok(())
}

/// Export as JSONL (newline-delimited JSON)
fn export_as_jsonl<W: Write>(
    _database: &Database,
    scan: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
    writer: &mut W,
    json_config: &JsonConfig,
    schema: Option<&SchemaDefinition>,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    for item in scan {
        let (key, value) = item?;
        stats.records_processed += 1;
        
        match serde_json::from_slice::<Value>(&value) {
            Ok(json_value) => {
                // Apply schema transformations if needed
                let processed_value = if let Some(schema_def) = schema {
                    apply_schema_transformations(&json_value, schema_def)?
                } else {
                    json_value
                };
                
                // Write JSON record
                if json_config.pretty_print {
                    serde_json::to_writer_pretty(&mut *writer, &processed_value)?;
                } else {
                    serde_json::to_writer(&mut *writer, &processed_value)?;
                }
                
                writeln!(writer)?; // Add newline for JSONL format
                
                stats.add_success(value.len());
            },
            Err(e) => {
                if config.skip_errors {
                    stats.add_error(ProcessingError {
                        record_number: stats.records_processed,
                        error_type: "json_parse".to_string(),
                        message: e.to_string(),
                        data: Some(String::from_utf8_lossy(&key).to_string()),
                    });
                } else {
                    return Err(crate::Error::ParseError(e.to_string()));
                }
            },
        }
        
        report_progress(stats);
    }
    
    writer.flush()?;
    println!("✅ JSONL export completed: {}", stats);
    Ok(())
}

/// Process a single JSON record
fn process_json_record(
    json_value: &Value,
    schema: Option<&SchemaDefinition>,
    record_number: u64,
    json_config: &JsonConfig,
) -> std::result::Result<Vec<(u64, Value)>, Vec<ValidationError>> {
    let mut validation_errors = Vec::new();
    
    // Validate against schema if provided
    if let Some(schema_def) = schema {
        match validation::validate_record(json_value, schema_def) {
            Ok(errors) => {
                if !errors.is_empty() {
                    validation_errors.extend(errors);
                }
            },
            Err(e) => {
                validation_errors.push(ValidationError {
                    field: "_record".to_string(),
                    rule: "schema_validation".to_string(),
                    message: e.to_string(),
                    value: Some(json_value.clone()),
                });
            },
        }
    }
    
    if !validation_errors.is_empty() {
        return Err(validation_errors);
    }
    
    // Process nested structures and arrays
    let processed_records = if json_config.flatten_arrays {
        flatten_json_arrays(json_value, record_number).map_err(|e| vec![ValidationError {
            field: "_flatten".to_string(),
            rule: "array_processing".to_string(),
            message: e.to_string(),
            value: Some(json_value.clone()),
        }])?
    } else {
        vec![(record_number, json_value.clone())]
    };
    
    Ok(processed_records)
}

/// Flatten JSON arrays into separate records
fn flatten_json_arrays(json_value: &Value, base_record_number: u64) -> Result<Vec<(u64, Value)>> {
    let mut records = Vec::new();
    
    match json_value {
        Value::Array(array) => {
            for (i, item) in array.iter().enumerate() {
                let record_id = base_record_number * 1000 + i as u64;
                let flattened = flatten_json_arrays(item, record_id)?;
                records.extend(flattened);
            }
        },
        Value::Object(obj) => {
            let mut flattened_obj = Map::new();
            let mut _has_arrays = false;
            
            for (key, value) in obj {
                match value {
                    Value::Array(array) => {
                        _has_arrays = true;
                        // Store array info in main record
                        flattened_obj.insert(
                            format!("{}_count", key),
                            Value::Number(serde_json::Number::from(array.len())),
                        );
                        
                        // Create separate records for array elements
                        for (i, item) in array.iter().enumerate() {
                            let array_record_id = base_record_number * 1000 + i as u64;
                            let mut array_record = Map::new();
                            array_record.insert("_parent_id".to_string(), Value::Number(serde_json::Number::from(base_record_number)));
                            array_record.insert("_array_field".to_string(), Value::String(key.clone()));
                            array_record.insert("_array_index".to_string(), Value::Number(serde_json::Number::from(i)));
                            array_record.insert("value".to_string(), item.clone());
                            
                            records.push((array_record_id, Value::Object(array_record)));
                        }
                    },
                    _ => {
                        flattened_obj.insert(key.clone(), value.clone());
                    },
                }
            }
            
            records.push((base_record_number, Value::Object(flattened_obj)));
        },
        _ => {
            records.push((base_record_number, json_value.clone()));
        },
    }
    
    Ok(records)
}

/// Apply schema transformations to JSON value
fn apply_schema_transformations(json_value: &Value, _schema: &SchemaDefinition) -> Result<Value> {
    // For now, just validate and return the original value
    // In the future, this could include:
    // - Type conversions
    // - Field renaming
    // - Default value application
    // - Computed field generation
    
    Ok(json_value.clone())
}

/// Process a batch of JSON records
fn process_json_batch(
    database: &Database,
    batch: &[(u64, Value)],
    table_prefix: &str,
    stats: &mut OperationStats,
) -> Result<()> {
    for (record_number, json_record) in batch {
        let key = format!("{}_record_{:08}", table_prefix, record_number);
        let value = serde_json::to_vec(json_record)?;
        
        database.put(key.as_bytes(), &value)?;
        stats.add_success(value.len());
    }
    
    Ok(())
}

/// Handle validation errors
fn handle_validation_errors(
    validation_errors: Vec<ValidationError>,
    record_number: u64,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    if config.skip_errors {
        for error in validation_errors {
            stats.add_error(ProcessingError {
                record_number,
                error_type: "validation".to_string(),
                message: error.message,
                data: error.value.map(|v| v.to_string()),
            });
        }
        Ok(())
    } else {
        Err(crate::Error::ValidationFailed(
            format!("Record {} validation failed", record_number)
        ))
    }
}

/// Report progress periodically
fn report_progress(stats: &OperationStats) {
    if stats.records_processed % 10000 == 0 && stats.records_processed > 0 {
        println!("  📊 Processed {} records ({} success, {} errors)", 
                stats.records_processed, stats.records_success, stats.records_failed);
    }
}

/// JSON utilities for advanced processing
pub struct JsonUtils;

impl JsonUtils {
    /// Detect JSON format (array vs JSONL)
    pub fn detect_format<P: AsRef<Path>>(file_path: P) -> Result<bool> {
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);
        let mut first_line = String::new();
        
        buf_reader.read_line(&mut first_line)?;
        let trimmed = first_line.trim();
        
        // Check if it starts with '[' (JSON array) or '{' (JSONL)
        Ok(trimmed.starts_with('['))
    }
    
    /// Validate JSON structure and schema compatibility
    pub fn validate_json_structure<P: AsRef<Path>>(
        file_path: P,
        schema: Option<&SchemaDefinition>,
    ) -> Result<JsonStructureInfo> {
        let file = File::open(file_path)?;
        let buf_reader = BufReader::new(file);
        
        let mut info = JsonStructureInfo {
            is_array: false,
            total_records: 0,
            max_nesting_depth: 0,
            field_types: BTreeMap::new(),
            schema_violations: Vec::new(),
            sample_records: Vec::new(),
        };
        
        // Try to parse as JSON
        let mut contents = String::new();
        use std::io::Read;
        let mut reader = buf_reader;
        reader.read_to_string(&mut contents)?;
        
        match serde_json::from_str::<Value>(&contents) {
            Ok(json_value) => {
                match &json_value {
                    Value::Array(array) => {
                        info.is_array = true;
                        info.total_records = array.len();
                        
                        // Analyze first few records
                        for (i, item) in array.iter().take(10).enumerate() {
                            analyze_json_value(item, &mut info, 0);
                            if i < 3 {
                                info.sample_records.push(item.clone());
                            }
                        }
                    },
                    Value::Object(_) => {
                        info.total_records = 1;
                        analyze_json_value(&json_value, &mut info, 0);
                        info.sample_records.push(json_value);
                    },
                    _ => {
                        return Err(crate::Error::InvalidInput(
                            "JSON must be object or array".to_string()
                        ));
                    },
                }
            },
            Err(_) => {
                // Try parsing as JSONL
                let lines: Vec<&str> = contents.lines().collect();
                info.total_records = lines.len();
                
                for (i, line) in lines.iter().take(10).enumerate() {
                    if let Ok(json_value) = serde_json::from_str::<Value>(line) {
                        analyze_json_value(&json_value, &mut info, 0);
                        if i < 3 {
                            info.sample_records.push(json_value);
                        }
                    }
                }
            },
        }
        
        // Validate against schema if provided
        if let Some(schema_def) = schema {
            for record in &info.sample_records {
                if let Ok(errors) = validation::validate_record(record, schema_def) {
                    info.schema_violations.extend(errors);
                }
            }
        }
        
        Ok(info)
    }
    
    /// Convert JSON to flattened key-value pairs
    pub fn flatten_json(json_value: &Value, prefix: &str) -> BTreeMap<String, Value> {
        let mut flattened = BTreeMap::new();
        flatten_json_recursive(json_value, prefix, &mut flattened);
        flattened
    }
    
    /// Unflatten key-value pairs back to JSON
    pub fn unflatten_json(flattened: &BTreeMap<String, Value>) -> Value {
        let mut result = Map::new();
        
        for (key, value) in flattened {
            let parts: Vec<&str> = key.split('.').collect();
            insert_nested_value(&mut result, &parts, value.clone());
        }
        
        Value::Object(result)
    }
}

/// Analyze JSON value structure
fn analyze_json_value(value: &Value, info: &mut JsonStructureInfo, depth: usize) {
    info.max_nesting_depth = info.max_nesting_depth.max(depth);
    
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let type_name = get_json_type_name(val);
                *info.field_types.entry(key.clone()).or_insert_with(BTreeMap::new)
                    .entry(type_name).or_insert(0) += 1;
                
                analyze_json_value(val, info, depth + 1);
            }
        },
        Value::Array(array) => {
            for item in array {
                analyze_json_value(item, info, depth + 1);
            }
        },
        _ => {}
    }
}

/// Get JSON type name for analysis
fn get_json_type_name(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(n) => {
            if n.is_i64() {
                "integer".to_string()
            } else {
                "float".to_string()
            }
        },
        Value::String(_) => "string".to_string(),
        Value::Array(_) => "array".to_string(),
        Value::Object(_) => "object".to_string(),
    }
}

/// Flatten JSON recursively
fn flatten_json_recursive(value: &Value, prefix: &str, result: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let new_key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_json_recursive(val, &new_key, result);
            }
        },
        Value::Array(array) => {
            for (i, item) in array.iter().enumerate() {
                let new_key = format!("{}[{}]", prefix, i);
                flatten_json_recursive(item, &new_key, result);
            }
        },
        _ => {
            result.insert(prefix.to_string(), value.clone());
        },
    }
}

/// Insert nested value into JSON object
fn insert_nested_value(obj: &mut Map<String, Value>, parts: &[&str], value: Value) {
    if parts.is_empty() {
        return;
    }
    
    if parts.len() == 1 {
        obj.insert(parts[0].to_string(), value);
        return;
    }
    
    let key = parts[0];
    let remaining = &parts[1..];
    
    let nested = obj.entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    
    if let Value::Object(nested_obj) = nested {
        insert_nested_value(nested_obj, remaining, value);
    }
}

/// JSON structure analysis information
#[derive(Debug, Clone)]
pub struct JsonStructureInfo {
    pub is_array: bool,
    pub total_records: usize,
    pub max_nesting_depth: usize,
    pub field_types: BTreeMap<String, BTreeMap<String, usize>>,
    pub schema_violations: Vec<ValidationError>,
    pub sample_records: Vec<Value>,
}

impl JsonStructureInfo {
    pub fn is_valid(&self) -> bool {
        self.total_records > 0 && self.max_nesting_depth < 50
    }
    
    pub fn complexity_score(&self) -> f64 {
        let type_diversity = self.field_types.values()
            .map(|types| types.len())
            .sum::<usize>() as f64;
        
        let nesting_penalty = (self.max_nesting_depth as f64).powf(1.5);
        let record_bonus = (self.total_records as f64).log10().max(1.0);
        
        (type_diversity + nesting_penalty) / record_bonus
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_json_config_default() {
        let config = JsonConfig::default();
        assert!(!config.pretty_print);
        assert!(config.array_format);
        assert_eq!(config.max_nesting_depth, 32);
        assert!(config.validate_json);
    }
    
    #[test]
    fn test_json_format_detection() -> Result<()> {
        // Test JSON array format
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "[{{\"name\": \"John\"}}, {{\"name\": \"Jane\"}}]")?;
        
        let is_array = JsonUtils::detect_format(temp_file.path())?;
        assert!(is_array);
        
        // Test JSONL format
        let mut temp_file2 = NamedTempFile::new()?;
        writeln!(temp_file2, "{{\"name\": \"John\"}}")?;
        writeln!(temp_file2, "{{\"name\": \"Jane\"}}")?;
        
        let is_array2 = JsonUtils::detect_format(temp_file2.path())?;
        assert!(!is_array2);
        
        Ok(())
    }
    
    #[test]
    fn test_flatten_json() {
        let json = serde_json::json!({
            "name": "John",
            "address": {
                "street": "123 Main St",
                "city": "NYC"
            },
            "scores": [85, 92, 78]
        });
        
        let flattened = JsonUtils::flatten_json(&json, "");
        
        assert_eq!(flattened.get("name"), Some(&Value::String("John".to_string())));
        assert_eq!(flattened.get("address.street"), Some(&Value::String("123 Main St".to_string())));
        assert_eq!(flattened.get("scores[0]"), Some(&Value::Number(serde_json::Number::from(85))));
    }
    
    #[test]
    fn test_get_json_type_name() {
        assert_eq!(get_json_type_name(&Value::Null), "null");
        assert_eq!(get_json_type_name(&Value::Bool(true)), "boolean");
        assert_eq!(get_json_type_name(&Value::Number(serde_json::Number::from(42))), "integer");
        assert_eq!(get_json_type_name(&Value::String("test".to_string())), "string");
        assert_eq!(get_json_type_name(&serde_json::json!([])), "array");
        assert_eq!(get_json_type_name(&serde_json::json!({})), "object");
    }
    
    #[test]
    fn test_flatten_json_arrays() -> Result<()> {
        let json = serde_json::json!({
            "users": [
                {"name": "John", "age": 30},
                {"name": "Jane", "age": 25}
            ],
            "total": 2
        });
        
        let flattened = flatten_json_arrays(&json, 1)?;
        
        // Should create main record plus array element records
        assert!(flattened.len() >= 3);
        
        Ok(())
    }
}