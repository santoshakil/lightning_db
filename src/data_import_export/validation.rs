//! Data Validation Module
//!
//! Provides comprehensive validation capabilities for imported data including
//! schema validation, data type checking, constraint validation, and custom rules.

use crate::Result;
use super::{
    SchemaDefinition, FieldType, ValidationRule, ConstraintDefinition,
};

// Re-export ValidationError for use by other modules
pub use super::ValidationError;
use serde_json::{Value, Map};
use std::collections::BTreeMap;
use regex::Regex;
use chrono::{DateTime, Utc, NaiveDateTime};

/// Validate a record against a schema definition
pub fn validate_record(
    record: &Value,
    schema: &SchemaDefinition,
) -> Result<Vec<ValidationError>> {
    let mut errors = Vec::new();
    
    let obj = match record.as_object() {
        Some(obj) => obj,
        None => {
            errors.push(ValidationError {
                field: "_root".to_string(),
                rule: "type_check".to_string(),
                message: "Expected JSON object".to_string(),
                value: Some(record.clone()),
            });
            return Ok(errors);
        },
    };
    
    // Validate each field in the schema
    for field_def in &schema.fields {
        let field_value = obj.get(&field_def.name);
        
        // Check for missing required fields
        if field_value.is_none() {
            if !field_def.nullable {
                errors.push(ValidationError {
                    field: field_def.name.clone(),
                    rule: "required".to_string(),
                    message: format!("Field '{}' is required but missing", field_def.name),
                    value: None,
                });
            }
            continue;
        }
        
        let value = field_value.unwrap();
        
        // Check for null values
        if value.is_null() {
            if !field_def.nullable {
                errors.push(ValidationError {
                    field: field_def.name.clone(),
                    rule: "nullable".to_string(),
                    message: format!("Field '{}' cannot be null", field_def.name),
                    value: Some(value.clone()),
                });
            }
            continue;
        }
        
        // Validate field type
        let type_errors = validate_field_type(value, &field_def.field_type, &field_def.name);
        errors.extend(type_errors);
        
        // Validate field rules
        for rule in &field_def.validation_rules {
            let rule_errors = validate_field_rule(value, rule, &field_def.name);
            errors.extend(rule_errors);
        }
    }
    
    // Check for unexpected fields
    for (field_name, _) in obj {
        if !schema.fields.iter().any(|f| f.name == *field_name) {
            // This is a warning, not an error
            // Could be configurable in the future
        }
    }
    
    // Validate schema-level constraints
    for constraint in &schema.constraints {
        let constraint_errors = validate_constraint(obj, constraint);
        errors.extend(constraint_errors);
    }
    
    Ok(errors)
}

/// Validate field type
fn validate_field_type(
    value: &Value,
    field_type: &FieldType,
    field_name: &str,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    
    match field_type {
        FieldType::String { max_length } => {
            if let Some(s) = value.as_str() {
                if let Some(max_len) = max_length {
                    if s.len() > *max_len {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            rule: "max_length".to_string(),
                            message: format!("String length {} exceeds maximum {}", s.len(), max_len),
                            value: Some(value.clone()),
                        });
                    }
                }
            } else {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: "type_check".to_string(),
                    message: "Expected string value".to_string(),
                    value: Some(value.clone()),
                });
            }
        },
        FieldType::Integer { min, max } => {
            if let Some(num) = value.as_i64() {
                if let Some(min_val) = min {
                    if num < *min_val {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            rule: "min_value".to_string(),
                            message: format!("Value {} is below minimum {}", num, min_val),
                            value: Some(value.clone()),
                        });
                    }
                }
                if let Some(max_val) = max {
                    if num > *max_val {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            rule: "max_value".to_string(),
                            message: format!("Value {} exceeds maximum {}", num, max_val),
                            value: Some(value.clone()),
                        });
                    }
                }
            } else {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: "type_check".to_string(),
                    message: "Expected integer value".to_string(),
                    value: Some(value.clone()),
                });
            }
        },
        FieldType::Float { min, max } => {
            if let Some(num) = value.as_f64() {
                if let Some(min_val) = min {
                    if num < *min_val {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            rule: "min_value".to_string(),
                            message: format!("Value {} is below minimum {}", num, min_val),
                            value: Some(value.clone()),
                        });
                    }
                }
                if let Some(max_val) = max {
                    if num > *max_val {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            rule: "max_value".to_string(),
                            message: format!("Value {} exceeds maximum {}", num, max_val),
                            value: Some(value.clone()),
                        });
                    }
                }
            } else {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: "type_check".to_string(),
                    message: "Expected numeric value".to_string(),
                    value: Some(value.clone()),
                });
            }
        },
        FieldType::Boolean => {
            if !value.is_boolean() {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: "type_check".to_string(),
                    message: "Expected boolean value".to_string(),
                    value: Some(value.clone()),
                });
            }
        },
        FieldType::DateTime { format } => {
            if let Some(s) = value.as_str() {
                if let Some(fmt) = format {
                    // Try to parse with specified format
                    if let Err(_) = NaiveDateTime::parse_from_str(s, fmt) {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            rule: "datetime_format".to_string(),
                            message: format!("Invalid datetime format, expected: {}", fmt),
                            value: Some(value.clone()),
                        });
                    }
                } else {
                    // Try common formats
                    if !is_valid_datetime(s) {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            rule: "datetime_format".to_string(),
                            message: "Invalid datetime format".to_string(),
                            value: Some(value.clone()),
                        });
                    }
                }
            } else {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: "type_check".to_string(),
                    message: "Expected datetime string".to_string(),
                    value: Some(value.clone()),
                });
            }
        },
        FieldType::Json => {
            // Any JSON value is valid
        },
        FieldType::Bytes => {
            if let Some(s) = value.as_str() {
                // Try to decode as base64
                use base64::Engine;
                let engine = base64::engine::general_purpose::STANDARD;
                if let Err(_) = engine.decode(s) {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        rule: "base64_format".to_string(),
                        message: "Expected valid base64 encoded string".to_string(),
                        value: Some(value.clone()),
                    });
                }
            } else {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: "type_check".to_string(),
                    message: "Expected base64 encoded string".to_string(),
                    value: Some(value.clone()),
                });
            }
        },
    }
    
    errors
}

/// Validate field rule
fn validate_field_rule(
    value: &Value,
    rule: &ValidationRule,
    field_name: &str,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    
    match rule {
        ValidationRule::Required => {
            // This is handled at the field level
        },
        ValidationRule::MinLength(min_len) => {
            if let Some(s) = value.as_str() {
                if s.len() < *min_len {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        rule: "min_length".to_string(),
                        message: format!("String length {} is below minimum {}", s.len(), min_len),
                        value: Some(value.clone()),
                    });
                }
            }
        },
        ValidationRule::MaxLength(max_len) => {
            if let Some(s) = value.as_str() {
                if s.len() > *max_len {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        rule: "max_length".to_string(),
                        message: format!("String length {} exceeds maximum {}", s.len(), max_len),
                        value: Some(value.clone()),
                    });
                }
            }
        },
        ValidationRule::Pattern(pattern) => {
            if let Some(s) = value.as_str() {
                match Regex::new(pattern) {
                    Ok(regex) => {
                        if !regex.is_match(s) {
                            errors.push(ValidationError {
                                field: field_name.to_string(),
                                rule: "pattern".to_string(),
                                message: format!("Value '{}' does not match pattern '{}'" , s, pattern),
                                value: Some(value.clone()),
                            });
                        }
                    },
                    Err(_) => {
                        errors.push(ValidationError {
                            field: field_name.to_string(),
                            rule: "pattern".to_string(),
                            message: format!("Invalid regex pattern: {}", pattern),
                            value: Some(value.clone()),
                        });
                    },
                }
            }
        },
        ValidationRule::Range { min, max } => {
            let num_value = if let Some(i) = value.as_i64() {
                i as f64
            } else if let Some(f) = value.as_f64() {
                f
            } else {
                return Vec::new(); // Not a number, skip this rule
            };
            
            if num_value < *min || num_value > *max {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: "range".to_string(),
                    message: format!("Value {} is outside range [{}, {}]", num_value, min, max),
                    value: Some(value.clone()),
                });
            }
        },
        ValidationRule::OneOf(allowed_values) => {
            let string_value = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => {
                    errors.push(ValidationError {
                        field: field_name.to_string(),
                        rule: "one_of".to_string(),
                        message: "Cannot validate complex types against allowed values".to_string(),
                        value: Some(value.clone()),
                    });
                    return errors;
                },
            };
            
            if !allowed_values.contains(&string_value) {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: "one_of".to_string(),
                    message: format!("Value '{}' is not in allowed list: {:?}", string_value, allowed_values),
                    value: Some(value.clone()),
                });
            }
        },
        ValidationRule::Custom { name, expression } => {
            // For now, just validate that the expression is not empty
            // In a full implementation, this would evaluate the custom expression
            if expression.trim().is_empty() {
                errors.push(ValidationError {
                    field: field_name.to_string(),
                    rule: name.clone(),
                    message: "Custom validation expression is empty".to_string(),
                    value: Some(value.clone()),
                });
            }
        },
    }
    
    errors
}

/// Validate schema constraint
fn validate_constraint(
    _record: &Map<String, Value>,
    constraint: &ConstraintDefinition,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    
    match constraint {
        ConstraintDefinition::UniqueKey { fields: _ } => {
            // Note: Uniqueness cannot be validated at the single record level
            // This would need to be validated at the batch or database level
        },
        ConstraintDefinition::ForeignKey { fields: _, reference_table: _, reference_fields: _ } => {
            // Note: Foreign key validation requires access to the referenced table
            // This would need to be implemented at the database level
        },
        ConstraintDefinition::Check { expression } => {
            // For now, just check if expression is present
            // In a full implementation, this would evaluate the check expression
            if expression.trim().is_empty() {
                errors.push(ValidationError {
                    field: "_constraint".to_string(),
                    rule: "check".to_string(),
                    message: "Check constraint expression is empty".to_string(),
                    value: None,
                });
            }
        },
    }
    
    errors
}

/// Check if string is a valid datetime
fn is_valid_datetime(s: &str) -> bool {
    // Try common datetime formats
    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ];
    
    for format in &formats {
        if NaiveDateTime::parse_from_str(s, format).is_ok() {
            return true;
        }
        
        // Also try with just date formats
        if let Ok(_date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            return true;
        }
        if let Ok(_date) = chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y") {
            return true;
        }
        if let Ok(_date) = chrono::NaiveDate::parse_from_str(s, "%d/%m/%Y") {
            return true;
        }
    }
    
    // Try parsing as RFC3339 (ISO 8601)
    if s.parse::<DateTime<Utc>>().is_ok() {
        return true;
    }
    
    false
}

/// Batch validation for performance
pub struct BatchValidator {
    schema: SchemaDefinition,
    errors: Vec<(usize, Vec<ValidationError>)>,
}

impl BatchValidator {
    pub fn new(schema: SchemaDefinition) -> Self {
        Self {
            schema,
            errors: Vec::new(),
        }
    }
    
    /// Validate a batch of records
    pub fn validate_batch(&mut self, records: &[Value]) -> Result<()> {
        self.errors.clear();
        
        for (index, record) in records.iter().enumerate() {
            match validate_record(record, &self.schema) {
                Ok(record_errors) => {
                    if !record_errors.is_empty() {
                        self.errors.push((index, record_errors));
                    }
                },
                Err(e) => {
                    return Err(e);
                },
            }
        }
        
        Ok(())
    }
    
    /// Get validation errors
    pub fn get_errors(&self) -> &[(usize, Vec<ValidationError>)] {
        &self.errors
    }
    
    /// Check if validation passed
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
    
    /// Get error count
    pub fn error_count(&self) -> usize {
        self.errors.iter().map(|(_, errors)| errors.len()).sum()
    }
    
    /// Get detailed validation report
    pub fn get_validation_report(&self) -> ValidationReport {
        let mut field_error_counts = BTreeMap::new();
        let mut rule_error_counts = BTreeMap::new();
        let mut total_errors = 0;
        
        for (_, errors) in &self.errors {
            for error in errors {
                *field_error_counts.entry(error.field.clone()).or_insert(0) += 1;
                *rule_error_counts.entry(error.rule.clone()).or_insert(0) += 1;
                total_errors += 1;
            }
        }
        
        ValidationReport {
            total_records: self.errors.len(),
            total_errors,
            field_error_counts,
            rule_error_counts,
            errors: self.errors.clone(),
        }
    }
}

/// Validation report for batch operations
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub total_records: usize,
    pub total_errors: usize,
    pub field_error_counts: BTreeMap<String, usize>,
    pub rule_error_counts: BTreeMap<String, usize>,
    pub errors: Vec<(usize, Vec<ValidationError>)>,
}

impl ValidationReport {
    pub fn print_summary(&self) {
        println!("\n📊 Validation Report:");
        println!("=====================");
        println!("Records validated: {}", self.total_records);
        println!("Total errors: {}", self.total_errors);
        
        if !self.field_error_counts.is_empty() {
            println!("\n🏷️  Errors by field:");
            for (field, count) in &self.field_error_counts {
                println!("  {}: {}", field, count);
            }
        }
        
        if !self.rule_error_counts.is_empty() {
            println!("\n📜 Errors by rule:");
            for (rule, count) in &self.rule_error_counts {
                println!("  {}: {}", rule, count);
            }
        }
    }
}

/// Custom validation functions
pub struct CustomValidators;

impl CustomValidators {
    /// Validate email format
    pub fn validate_email(value: &Value) -> std::result::Result<(), ValidationError> {
        if let Some(s) = value.as_str() {
            let email_regex = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
                .map_err(|e| ValidationError {
                    field: "email".to_string(),
                    rule: "email_format".to_string(),
                    message: format!("Regex error: {}", e),
                    value: Some(value.clone()),
                })?;
            
            if !email_regex.is_match(s) {
                return Err(ValidationError {
                    field: "email".to_string(),
                    rule: "email_format".to_string(),
                    message: "Invalid email format".to_string(),
                    value: Some(value.clone()),
                });
            }
        } else {
            return Err(ValidationError {
                field: "email".to_string(),
                rule: "email_format".to_string(),
                message: "Expected string value for email".to_string(),
                value: Some(value.clone()),
            });
        }
        
        Ok(())
    }
    
    /// Validate URL format
    pub fn validate_url(value: &Value) -> std::result::Result<(), ValidationError> {
        if let Some(s) = value.as_str() {
            let url_regex = Regex::new(r"^https?://[^\s/$.?#].[^\s]*$")
                .map_err(|e| ValidationError {
                    field: "url".to_string(),
                    rule: "url_format".to_string(),
                    message: format!("Regex error: {}", e),
                    value: Some(value.clone()),
                })?;
            
            if !url_regex.is_match(s) {
                return Err(ValidationError {
                    field: "url".to_string(),
                    rule: "url_format".to_string(),
                    message: "Invalid URL format".to_string(),
                    value: Some(value.clone()),
                });
            }
        } else {
            return Err(ValidationError {
                field: "url".to_string(),
                rule: "url_format".to_string(),
                message: "Expected string value for URL".to_string(),
                value: Some(value.clone()),
            });
        }
        
        Ok(())
    }
    
    /// Validate phone number format
    pub fn validate_phone(value: &Value) -> std::result::Result<(), ValidationError> {
        if let Some(s) = value.as_str() {
            // Simple phone validation - digits, spaces, dashes, parentheses, plus
            let _phone_regex = Regex::new(r"^[\+]?[1-9]?[0-9]{7,15}$")
                .map_err(|e| ValidationError {
                    field: "phone".to_string(),
                    rule: "phone_format".to_string(),
                    message: format!("Regex error: {}", e),
                    value: Some(value.clone()),
                })?;
            
            // Remove formatting characters
            let cleaned = s.chars()
                .filter(|c| c.is_ascii_digit() || *c == '+')
                .collect::<String>();
            
            if cleaned.len() < 7 || cleaned.len() > 15 {
                return Err(ValidationError {
                    field: "phone".to_string(),
                    rule: "phone_format".to_string(),
                    message: "Phone number must be 7-15 digits".to_string(),
                    value: Some(value.clone()),
                });
            }
        } else {
            return Err(ValidationError {
                field: "phone".to_string(),
                rule: "phone_format".to_string(),
                message: "Expected string value for phone".to_string(),
                value: Some(value.clone()),
            });
        }
        
        Ok(())
    }
    
    /// Validate credit card number (Luhn algorithm)
    pub fn validate_credit_card(value: &Value) -> std::result::Result<(), ValidationError> {
        if let Some(s) = value.as_str() {
            let cleaned = s.chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>();
            
            if cleaned.len() < 13 || cleaned.len() > 19 {
                return Err(ValidationError {
                    field: "credit_card".to_string(),
                    rule: "credit_card_format".to_string(),
                    message: "Credit card number must be 13-19 digits".to_string(),
                    value: Some(value.clone()),
                });
            }
            
            // Luhn algorithm validation
            if !is_valid_luhn(&cleaned) {
                return Err(ValidationError {
                    field: "credit_card".to_string(),
                    rule: "credit_card_format".to_string(),
                    message: "Invalid credit card number (Luhn check failed)".to_string(),
                    value: Some(value.clone()),
                });
            }
        } else {
            return Err(ValidationError {
                field: "credit_card".to_string(),
                rule: "credit_card_format".to_string(),
                message: "Expected string value for credit card".to_string(),
                value: Some(value.clone()),
            });
        }
        
        Ok(())
    }
}

/// Validate using Luhn algorithm
fn is_valid_luhn(number: &str) -> bool {
    let mut sum = 0;
    let mut alternate = false;
    
    for ch in number.chars().rev() {
        let mut digit = ch.to_digit(10).unwrap() as usize;
        
        if alternate {
            digit *= 2;
            if digit > 9 {
                digit -= 9;
            }
        }
        
        sum += digit;
        alternate = !alternate;
    }
    
    sum % 10 == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::FieldDefinition;
    use serde_json::json;

    #[test]
    fn test_validate_string_field() {
        let field_def = FieldDefinition {
            name: "name".to_string(),
            field_type: FieldType::String { max_length: Some(10) },
            nullable: false,
            default_value: None,
            validation_rules: vec![ValidationRule::MinLength(2)],
        };
        
        // Valid string
        let value = json!("John");
        assert!(validate_field_type(&value, &field_def.field_type, "name").is_empty());
        
        // Too long
        let value = json!("This is too long");
        assert!(!validate_field_type(&value, &field_def.field_type, "name").is_empty());
        
        // Wrong type
        let value = json!(123);
        assert!(!validate_field_type(&value, &field_def.field_type, "name").is_empty());
    }
    
    #[test]
    fn test_validate_integer_field() {
        let field_type = FieldType::Integer { min: Some(0), max: Some(100) };
        
        // Valid integer
        let value = json!(50);
        assert!(validate_field_type(&value, &field_type, "age").is_empty());
        
        // Below minimum
        let value = json!(-5);
        assert!(!validate_field_type(&value, &field_type, "age").is_empty());
        
        // Above maximum
        let value = json!(150);
        assert!(!validate_field_type(&value, &field_type, "age").is_empty());
    }
    
    #[test]
    fn test_validate_email() {
        let valid_email = json!("user@example.com");
        assert!(CustomValidators::validate_email(&valid_email).is_ok());
        
        let invalid_email = json!("invalid-email");
        assert!(CustomValidators::validate_email(&invalid_email).is_err());
        
        let not_string = json!(123);
        assert!(CustomValidators::validate_email(&not_string).is_err());
    }
    
    #[test]
    fn test_validate_phone() {
        let valid_phone = json!("+1234567890");
        assert!(CustomValidators::validate_phone(&valid_phone).is_ok());
        
        let too_short = json!("123");
        assert!(CustomValidators::validate_phone(&too_short).is_err());
        
        let too_long = json!("12345678901234567890");
        assert!(CustomValidators::validate_phone(&too_long).is_err());
    }
    
    #[test]
    fn test_luhn_algorithm() {
        // Valid credit card numbers
        assert!(is_valid_luhn("4532015112830366")); // Visa
        assert!(is_valid_luhn("5555555555554444")); // MasterCard
        
        // Invalid numbers
        assert!(!is_valid_luhn("1234567890123456"));
        assert!(!is_valid_luhn("0000000000000000"));
    }
    
    #[test]
    fn test_datetime_validation() {
        assert!(is_valid_datetime("2023-12-25 10:30:00"));
        assert!(is_valid_datetime("2023-12-25T10:30:00Z"));
        assert!(is_valid_datetime("2023-12-25"));
        assert!(is_valid_datetime("12/25/2023"));
        
        assert!(!is_valid_datetime("invalid-date"));
        assert!(!is_valid_datetime("2023-13-01")); // Invalid month
    }
    
    #[test]
    fn test_batch_validator() {
        let schema = SchemaDefinition {
            name: "test_schema".to_string(),
            version: "1.0".to_string(),
            fields: vec![
                FieldDefinition {
                    name: "name".to_string(),
                    field_type: FieldType::String { max_length: Some(50) },
                    nullable: false,
                    default_value: None,
                    validation_rules: vec![ValidationRule::Required],
                },
                FieldDefinition {
                    name: "age".to_string(),
                    field_type: FieldType::Integer { min: Some(0), max: Some(120) },
                    nullable: false,
                    default_value: None,
                    validation_rules: vec![ValidationRule::Range { min: 0.0, max: 120.0 }],
                },
            ],
            constraints: vec![],
        };
        
        let mut validator = BatchValidator::new(schema);
        
        let records = vec![
            json!({"name": "John", "age": 30}),
            json!({"name": "Jane", "age": 25}),
            json!({"name": "", "age": -5}), // Invalid: empty name, negative age
        ];
        
        validator.validate_batch(&records).unwrap();
        
        assert!(!validator.is_valid());
        assert!(validator.error_count() > 0);
        
        let report = validator.get_validation_report();
        report.print_summary();
    }
}