//! CSV Import/Export Handler
//!
//! Provides comprehensive CSV processing capabilities with schema validation,
//! error handling, and performance optimization.

use super::{
    validation::{self, ValidationError},
    DataFormat, ImportExportConfig, OperationStats, ProcessingError, SchemaDefinition,
};
use crate::{Database, Result};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// CSV-specific configuration
#[derive(Debug, Clone)]
pub struct CsvConfig {
    pub delimiter: u8,
    pub quote_char: u8,
    pub escape_char: Option<u8>,
    pub has_headers: bool,
    pub flexible: bool, // Allow records with different lengths
    pub trim_whitespace: bool,
    pub skip_empty_lines: bool,
    pub comment_char: Option<u8>,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote_char: b'"',
            escape_char: None,
            has_headers: true,
            flexible: false,
            trim_whitespace: true,
            skip_empty_lines: true,
            comment_char: Some(b'#'),
        }
    }
}

/// Import CSV data into the database
pub fn import_csv(
    database: &Database,
    file_path: &Path,
    format: &DataFormat,
    schema: Option<&SchemaDefinition>,
    table_prefix: &str,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    println!("📄 Importing CSV from: {:?}", file_path);

    // Extract CSV configuration
    let csv_config = match format {
        DataFormat::Csv {
            delimiter,
            quote_char,
            escape_char,
            headers,
        } => CsvConfig {
            delimiter: *delimiter as u8,
            quote_char: *quote_char as u8,
            escape_char: escape_char.map(|c| c as u8),
            has_headers: *headers,
            ..CsvConfig::default()
        },
        _ => {
            return Err(crate::Error::InvalidInput(
                "Expected CSV format".to_string(),
            ))
        }
    };

    // Open and configure CSV reader
    let file = File::open(file_path)?;
    let buf_reader = BufReader::new(file);

    let mut reader = ReaderBuilder::new()
        .delimiter(csv_config.delimiter)
        .quote(csv_config.quote_char)
        .has_headers(csv_config.has_headers)
        .flexible(csv_config.flexible)
        .trim(csv::Trim::All)
        .comment(csv_config.comment_char)
        .from_reader(buf_reader);

    // Get headers
    let headers = if csv_config.has_headers {
        reader.headers()?.clone()
    } else {
        // Generate column names if no headers
        let first_record = reader
            .records()
            .next()
            .ok_or_else(|| crate::Error::InvalidInput("Empty CSV file".to_string()))??;

        let mut headers = StringRecord::new();
        for i in 0..first_record.len() {
            headers.push_field(&format!("column_{}", i));
        }
        headers
    };

    println!("📋 CSV headers: {:?}", headers.iter().collect::<Vec<_>>());

    // Validate schema compatibility
    if let Some(schema_def) = schema {
        validate_csv_schema(&headers, schema_def)?;
    }

    // Process records in batches
    let mut batch = Vec::new();
    let mut record_number = if csv_config.has_headers { 2 } else { 1 }; // Line numbers start at 1

    for result in reader.records() {
        stats.records_processed += 1;

        match result {
            Ok(record) => {
                match process_csv_record(&record, &headers, schema, record_number) {
                    Ok(json_record) => {
                        batch.push((record_number, json_record));

                        // Process batch when full
                        if batch.len() >= config.batch_size {
                            process_batch(database, &batch, table_prefix, stats)?;
                            batch.clear();
                        }
                    }
                    Err(validation_errors) => {
                        if config.skip_errors {
                            for error in validation_errors {
                                stats.add_error(ProcessingError {
                                    record_number,
                                    error_type: "validation".to_string(),
                                    message: error.message,
                                    data: Some(record.iter().collect::<Vec<_>>().join(",")),
                                });
                            }
                        } else {
                            return Err(crate::Error::ValidationFailed(format!(
                                "Record {} validation failed",
                                record_number
                            )));
                        }
                    }
                }
            }
            Err(e) => {
                let error = ProcessingError {
                    record_number,
                    error_type: "parse".to_string(),
                    message: e.to_string(),
                    data: None,
                };

                if config.skip_errors {
                    stats.add_error(error);
                } else {
                    return Err(crate::Error::ParseError(e.to_string()));
                }
            }
        }

        record_number += 1;

        // Progress reporting
        if stats.records_processed % 10000 == 0 {
            println!(
                "  📊 Processed {} records ({} success, {} errors)",
                stats.records_processed, stats.records_success, stats.records_failed
            );
        }
    }

    // Process remaining batch
    if !batch.is_empty() {
        process_batch(database, &batch, table_prefix, stats)?;
    }

    println!("✅ CSV import completed: {}", stats);
    Ok(())
}

/// Export data to CSV format
pub fn export_csv(
    database: &Database,
    table_prefix: &str,
    file_path: &Path,
    format: &DataFormat,
    schema: Option<&SchemaDefinition>,
    config: &ImportExportConfig,
    stats: &mut OperationStats,
) -> Result<()> {
    println!("📤 Exporting to CSV: {:?}", file_path);

    // Extract CSV configuration
    let csv_config = match format {
        DataFormat::Csv {
            delimiter,
            quote_char,
            escape_char,
            headers,
        } => CsvConfig {
            delimiter: *delimiter as u8,
            quote_char: *quote_char as u8,
            escape_char: escape_char.map(|c| c as u8),
            has_headers: *headers,
            ..CsvConfig::default()
        },
        _ => {
            return Err(crate::Error::InvalidInput(
                "Expected CSV format".to_string(),
            ))
        }
    };

    // Create CSV writer
    let file = File::create(file_path)?;
    let buf_writer = BufWriter::new(file);

    let mut writer = WriterBuilder::new()
        .delimiter(csv_config.delimiter)
        .quote(csv_config.quote_char)
        .from_writer(buf_writer);

    // Scan database for records with the given prefix
    let scan = database.scan(
        Some(format!("{}_", table_prefix).as_bytes().to_vec()),
        Some(format!("{}~", table_prefix).as_bytes().to_vec()),
    )?;

    let mut headers_written = false;
    let mut field_order: Vec<String> = Vec::new();

    // If schema is provided, use it to determine field order
    if let Some(schema_def) = schema {
        field_order = schema_def.fields.iter().map(|f| f.name.clone()).collect();

        if csv_config.has_headers {
            writer.write_record(&field_order)?;
            headers_written = true;
        }
    }

    // Process records
    for item in scan {
        let (key, value) = item?;
        stats.records_processed += 1;

        match serde_json::from_slice::<Value>(&value) {
            Ok(json_value) => {
                // Convert JSON to CSV record
                match json_to_csv_record(
                    &json_value,
                    &mut field_order,
                    csv_config.has_headers && !headers_written,
                ) {
                    Ok((csv_record, headers)) => {
                        // Write headers if this is the first record and we don't have schema
                        if csv_config.has_headers && !headers_written {
                            if let Some(header_record) = headers {
                                writer.write_record(&header_record)?;
                                headers_written = true;
                            }
                        }

                        writer.write_record(&csv_record)?;
                        stats.add_success(value.len());
                    }
                    Err(e) => {
                        if config.skip_errors {
                            stats.add_error(ProcessingError {
                                record_number: stats.records_processed,
                                error_type: "conversion".to_string(),
                                message: e,
                                data: Some(String::from_utf8_lossy(&key).to_string()),
                            });
                        } else {
                            return Err(crate::Error::ConversionError(e));
                        }
                    }
                }
            }
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
            }
        }

        // Progress reporting
        if stats.records_processed % 10000 == 0 {
            println!(
                "  📊 Exported {} records ({} success, {} errors)",
                stats.records_processed, stats.records_success, stats.records_failed
            );
        }
    }

    writer.flush()?;
    println!("✅ CSV export completed: {}", stats);
    Ok(())
}

/// Validate CSV schema compatibility
fn validate_csv_schema(headers: &StringRecord, schema: &SchemaDefinition) -> Result<()> {
    let csv_headers: Vec<&str> = headers.iter().collect();
    let schema_fields: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();

    // Check for missing required fields
    for field in &schema.fields {
        if !field.nullable && !csv_headers.contains(&field.name.as_str()) {
            return Err(crate::Error::SchemaValidationFailed(format!(
                "Required field '{}' not found in CSV headers",
                field.name
            )));
        }
    }

    // Check for unexpected fields (warning only)
    for header in &csv_headers {
        if !schema_fields.contains(header) {
            println!("⚠️  Warning: CSV header '{}' not defined in schema", header);
        }
    }

    Ok(())
}

/// Process a single CSV record
fn process_csv_record(
    record: &StringRecord,
    headers: &StringRecord,
    schema: Option<&SchemaDefinition>,
    _record_number: u64,
) -> std::result::Result<Value, Vec<ValidationError>> {
    let mut json_object = Map::new();
    let mut validation_errors = Vec::new();

    // Convert CSV record to JSON object
    for (i, field_value) in record.iter().enumerate() {
        let default_name = format!("column_{}", i);
        let field_name = headers.get(i).unwrap_or(&default_name);

        // Convert string value to appropriate JSON type
        let json_value = convert_csv_value_to_json(field_value, field_name, schema);
        json_object.insert(field_name.to_string(), json_value);
    }

    let json_record = Value::Object(json_object);

    // Validate against schema if provided
    if let Some(schema_def) = schema {
        match validation::validate_record(&json_record, schema_def) {
            Ok(errors) => {
                if !errors.is_empty() {
                    validation_errors.extend(errors);
                }
            }
            Err(e) => {
                validation_errors.push(ValidationError {
                    field: "_record".to_string(),
                    rule: "schema_validation".to_string(),
                    message: e.to_string(),
                    value: Some(json_record.clone()),
                });
            }
        }
    }

    if validation_errors.is_empty() {
        Ok(json_record)
    } else {
        Err(validation_errors)
    }
}

/// Convert CSV string value to appropriate JSON type
fn convert_csv_value_to_json(
    value: &str,
    field_name: &str,
    schema: Option<&SchemaDefinition>,
) -> Value {
    let trimmed = value.trim();

    // Handle empty values
    if trimmed.is_empty() {
        return Value::Null;
    }

    // Use schema type information if available
    if let Some(schema_def) = schema {
        if let Some(field_def) = schema_def.fields.iter().find(|f| f.name == field_name) {
            return convert_with_schema_type(trimmed, field_def);
        }
    }

    // Attempt automatic type detection
    auto_detect_type(trimmed)
}

/// Convert value using schema type information
fn convert_with_schema_type(value: &str, field_def: &super::FieldDefinition) -> Value {
    use super::FieldType;

    match &field_def.field_type {
        FieldType::String { .. } => Value::String(value.to_string()),
        FieldType::Integer { .. } => value
            .parse::<i64>()
            .map(|i| Value::Number(Number::from(i)))
            .unwrap_or_else(|_| Value::String(value.to_string())),
        FieldType::Float { .. } => value
            .parse::<f64>()
            .map(|f| Value::Number(Number::from_f64(f).unwrap_or(Number::from(0))))
            .unwrap_or_else(|_| Value::String(value.to_string())),
        FieldType::Boolean => match value.to_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => Value::Bool(true),
            "false" | "f" | "no" | "n" | "0" => Value::Bool(false),
            _ => Value::String(value.to_string()),
        },
        FieldType::DateTime { format } => {
            if let Some(_fmt) = format {
                // Try to parse with provided format
                // For now, just store as string
                Value::String(value.to_string())
            } else {
                // Try common datetime formats
                Value::String(value.to_string())
            }
        }
        FieldType::Json => {
            serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()))
        }
        FieldType::Bytes => {
            // Assume base64 encoding
            Value::String(value.to_string())
        }
    }
}

/// Automatically detect value type
fn auto_detect_type(value: &str) -> Value {
    // Try integer
    if let Ok(i) = value.parse::<i64>() {
        return Value::Number(Number::from(i));
    }

    // Try float
    if let Ok(f) = value.parse::<f64>() {
        if let Some(num) = Number::from_f64(f) {
            return Value::Number(num);
        }
    }

    // Try boolean
    match value.to_lowercase().as_str() {
        "true" | "false" => {
            return Value::Bool(value.to_lowercase() == "true");
        }
        _ => {}
    }

    // Try JSON
    if value.starts_with('{') || value.starts_with('[') {
        if let Ok(json) = serde_json::from_str(value) {
            return json;
        }
    }

    // Default to string
    Value::String(value.to_string())
}

/// Process a batch of records
fn process_batch(
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

/// Convert JSON record to CSV format
fn json_to_csv_record(
    json_value: &Value,
    field_order: &mut Vec<String>,
    generate_headers: bool,
) -> std::result::Result<(Vec<String>, Option<Vec<String>>), String> {
    let obj = json_value
        .as_object()
        .ok_or_else(|| "Expected JSON object".to_string())?;

    let mut headers = None;

    // Generate field order if not provided
    if field_order.is_empty() {
        *field_order = obj.keys().cloned().collect();
        field_order.sort(); // Ensure consistent order

        if generate_headers {
            headers = Some(field_order.clone());
        }
    }

    // Convert values to CSV format
    let mut csv_record = Vec::new();
    for field_name in field_order {
        let value = obj.get(field_name).unwrap_or(&Value::Null);

        let csv_value = json_value_to_csv_string(value);
        csv_record.push(csv_value);
    }

    Ok((csv_record, headers))
}

/// Convert JSON value to CSV string representation
fn json_value_to_csv_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => {
            // Serialize complex types as JSON strings
            serde_json::to_string(value).unwrap_or_else(|_| String::new())
        }
    }
}

/// CSV import/export utilities
pub struct CsvUtils;

impl CsvUtils {
    /// Detect CSV delimiter by analyzing file content
    pub fn detect_delimiter<P: AsRef<Path>>(file_path: P) -> Result<char> {
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);
        let mut sample = String::new();

        // Read first few lines for analysis
        use std::io::BufRead;
        for _ in 0..10 {
            let mut line = String::new();
            match buf_reader.read_line(&mut line) {
                Ok(0) => break, // EOF
                Ok(_) => sample.push_str(&line),
                Err(_) => break,
            }
        }

        // Count potential delimiters
        let delimiters = [',', ';', '\t', '|'];
        let mut counts = BTreeMap::new();

        for &delimiter in &delimiters {
            let count = sample.chars().filter(|&c| c == delimiter).count();
            counts.insert(delimiter, count);
        }

        // Return delimiter with highest count
        counts
            .iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&delimiter, _)| delimiter)
            .ok_or_else(|| crate::Error::InvalidInput("Could not detect CSV delimiter".to_string()))
    }

    /// Validate CSV file structure
    pub fn validate_csv_structure<P: AsRef<Path>>(
        file_path: P,
        config: &CsvConfig,
    ) -> Result<CsvStructureInfo> {
        let file = File::open(file_path)?;
        let buf_reader = BufReader::new(file);

        let mut reader = ReaderBuilder::new()
            .delimiter(config.delimiter)
            .quote(config.quote_char)
            .has_headers(config.has_headers)
            .flexible(config.flexible)
            .from_reader(buf_reader);

        let mut info = CsvStructureInfo {
            total_records: 0,
            column_count: 0,
            has_headers: config.has_headers,
            headers: Vec::new(),
            sample_records: Vec::new(),
            inconsistent_records: 0,
        };

        // Get headers
        if config.has_headers {
            if let Ok(headers) = reader.headers() {
                info.headers = headers.iter().map(|h| h.to_string()).collect();
                info.column_count = headers.len();
            }
        }

        // Analyze records
        for (i, result) in reader.records().enumerate() {
            match result {
                Ok(record) => {
                    info.total_records += 1;

                    if info.column_count == 0 {
                        info.column_count = record.len();
                    } else if record.len() != info.column_count {
                        info.inconsistent_records += 1;
                    }

                    // Collect sample records
                    if i < 5 {
                        info.sample_records
                            .push(record.iter().map(|s| s.to_string()).collect());
                    }
                }
                Err(_) => {
                    info.inconsistent_records += 1;
                }
            }

            // Limit analysis for large files
            if i >= 10000 {
                break;
            }
        }

        Ok(info)
    }
}

/// CSV structure information
#[derive(Debug, Clone)]
pub struct CsvStructureInfo {
    pub total_records: usize,
    pub column_count: usize,
    pub has_headers: bool,
    pub headers: Vec<String>,
    pub sample_records: Vec<Vec<String>>,
    pub inconsistent_records: usize,
}

impl CsvStructureInfo {
    pub fn is_valid(&self) -> bool {
        self.total_records > 0
            && self.column_count > 0
            && (self.inconsistent_records as f64 / self.total_records as f64) < 0.1
    }

    pub fn consistency_ratio(&self) -> f64 {
        if self.total_records == 0 {
            0.0
        } else {
            1.0 - (self.inconsistent_records as f64 / self.total_records as f64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_csv_config_default() {
        let config = CsvConfig::default();
        assert_eq!(config.delimiter, b',');
        assert_eq!(config.quote_char, b'"');
        assert!(config.has_headers);
        assert!(config.trim_whitespace);
    }

    #[test]
    fn test_auto_detect_type() {
        assert_eq!(auto_detect_type("123"), Value::Number(Number::from(123)));
        assert_eq!(
            auto_detect_type("123.45"),
            Value::Number(Number::from_f64(123.45).unwrap())
        );
        assert_eq!(auto_detect_type("true"), Value::Bool(true));
        assert_eq!(auto_detect_type("false"), Value::Bool(false));
        assert_eq!(
            auto_detect_type("hello"),
            Value::String("hello".to_string())
        );
    }

    #[test]
    fn test_json_value_to_csv_string() {
        assert_eq!(json_value_to_csv_string(&Value::Null), "");
        assert_eq!(json_value_to_csv_string(&Value::Bool(true)), "true");
        assert_eq!(
            json_value_to_csv_string(&Value::Number(Number::from(42))),
            "42"
        );
        assert_eq!(
            json_value_to_csv_string(&Value::String("test".to_string())),
            "test"
        );
    }

    #[test]
    fn test_csv_structure_validation() -> Result<()> {
        // Create temporary CSV file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "John,30,NYC")?;
        writeln!(temp_file, "Jane,25,LA")?;

        let config = CsvConfig::default();
        let info = CsvUtils::validate_csv_structure(temp_file.path(), &config)?;

        assert_eq!(info.total_records, 2);
        assert_eq!(info.column_count, 3);
        assert!(info.has_headers);
        assert_eq!(info.headers, vec!["name", "age", "city"]);
        assert!(info.is_valid());

        Ok(())
    }

    #[test]
    fn test_delimiter_detection() -> Result<()> {
        // Create CSV with semicolon delimiter
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name;age;city")?;
        writeln!(temp_file, "John;30;NYC")?;
        writeln!(temp_file, "Jane;25;LA")?;

        let delimiter = CsvUtils::detect_delimiter(temp_file.path())?;
        assert_eq!(delimiter, ';');

        Ok(())
    }
}
