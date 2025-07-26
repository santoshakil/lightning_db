//! Data Transformation Pipeline
//!
//! Provides data transformation capabilities during import/export operations
//! including field mapping, data cleaning, and custom transformations.

use crate::Result;
use serde_json::{Value, Map};
use std::collections::BTreeMap;

/// Transformation pipeline for data processing
#[derive(Debug, Clone)]
pub struct TransformationPipeline {
    pub steps: Vec<TransformationStep>,
}

/// Individual transformation step
#[derive(Debug, Clone)]
pub enum TransformationStep {
    /// Map field names
    FieldMapping {
        mappings: BTreeMap<String, String>,
    },
    /// Apply value transformations
    ValueTransform {
        field: String,
        transform: ValueTransformation,
    },
    /// Filter records based on condition
    Filter {
        condition: FilterCondition,
    },
    /// Add computed fields
    ComputedField {
        field_name: String,
        expression: String,
    },
    /// Data cleaning operations
    DataCleaning {
        operations: Vec<CleaningOperation>,
    },
}

/// Value transformation types
#[derive(Debug, Clone)]
pub enum ValueTransformation {
    /// Convert to uppercase
    ToUpperCase,
    /// Convert to lowercase
    ToLowerCase,
    /// Trim whitespace
    Trim,
    /// Apply regular expression
    RegexReplace { pattern: String, replacement: String },
    /// Convert data type
    TypeConversion { target_type: String },
    /// Apply custom function
    Custom { function_name: String },
}

/// Filter condition
#[derive(Debug, Clone)]
pub enum FilterCondition {
    /// Field equals value
    FieldEquals { field: String, value: Value },
    /// Field contains value
    FieldContains { field: String, value: String },
    /// Field matches pattern
    FieldMatches { field: String, pattern: String },
    /// Custom condition
    Custom { expression: String },
}

/// Data cleaning operations
#[derive(Debug, Clone)]
pub enum CleaningOperation {
    /// Remove duplicate records
    RemoveDuplicates { key_fields: Vec<String> },
    /// Handle null values
    HandleNulls { strategy: NullHandlingStrategy },
    /// Validate data integrity
    ValidateIntegrity { rules: Vec<String> },
    /// Normalize formats
    NormalizeFormats { field_types: BTreeMap<String, String> },
}

/// Null handling strategies
#[derive(Debug, Clone)]
pub enum NullHandlingStrategy {
    /// Remove records with null values
    RemoveRecord,
    /// Replace with default value
    ReplaceWithDefault { default_value: Value },
    /// Replace with computed value
    ReplaceWithComputed { expression: String },
    /// Skip null fields
    Skip,
}

impl TransformationPipeline {
    /// Create new transformation pipeline
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
        }
    }
    
    /// Add transformation step
    pub fn add_step(mut self, step: TransformationStep) -> Self {
        self.steps.push(step);
        self
    }
    
    /// Apply transformation pipeline to a record
    pub fn transform_record(&self, record: &Value) -> Result<Option<Value>> {
        let mut current_record = record.clone();
        
        for step in &self.steps {
            match self.apply_step(&current_record, step)? {
                Some(transformed) => current_record = transformed,
                None => return Ok(None), // Record filtered out
            }
        }
        
        Ok(Some(current_record))
    }
    
    /// Apply a single transformation step
    fn apply_step(&self, record: &Value, step: &TransformationStep) -> Result<Option<Value>> {
        match step {
            TransformationStep::FieldMapping { mappings } => {
                self.apply_field_mapping(record, mappings)
            },
            TransformationStep::ValueTransform { field, transform } => {
                self.apply_value_transform(record, field, transform)
            },
            TransformationStep::Filter { condition } => {
                if self.evaluate_filter_condition(record, condition)? {
                    Ok(Some(record.clone()))
                } else {
                    Ok(None)
                }
            },
            TransformationStep::ComputedField { field_name, expression } => {
                self.add_computed_field(record, field_name, expression)
            },
            TransformationStep::DataCleaning { operations } => {
                self.apply_data_cleaning(record, operations)
            },
        }
    }
    
    /// Apply field mapping
    fn apply_field_mapping(
        &self,
        record: &Value,
        mappings: &BTreeMap<String, String>,
    ) -> Result<Option<Value>> {
        if let Value::Object(obj) = record {
            let mut new_obj = Map::new();
            
            for (old_field, new_field) in mappings {
                if let Some(value) = obj.get(old_field) {
                    new_obj.insert(new_field.clone(), value.clone());
                }
            }
            
            // Keep unmapped fields
            for (field, value) in obj {
                if !mappings.contains_key(field) {
                    new_obj.insert(field.clone(), value.clone());
                }
            }
            
            Ok(Some(Value::Object(new_obj)))
        } else {
            Ok(Some(record.clone()))
        }
    }
    
    /// Apply value transformation
    fn apply_value_transform(
        &self,
        record: &Value,
        field: &str,
        transform: &ValueTransformation,
    ) -> Result<Option<Value>> {
        if let Value::Object(obj) = record {
            let mut new_obj = obj.clone();
            
            if let Some(value) = obj.get(field) {
                let transformed_value = self.transform_value(value, transform)?;
                new_obj.insert(field.to_string(), transformed_value);
            }
            
            Ok(Some(Value::Object(new_obj)))
        } else {
            Ok(Some(record.clone()))
        }
    }
    
    /// Transform a single value
    fn transform_value(&self, value: &Value, transform: &ValueTransformation) -> Result<Value> {
        match transform {
            ValueTransformation::ToUpperCase => {
                if let Some(s) = value.as_str() {
                    Ok(Value::String(s.to_uppercase()))
                } else {
                    Ok(value.clone())
                }
            },
            ValueTransformation::ToLowerCase => {
                if let Some(s) = value.as_str() {
                    Ok(Value::String(s.to_lowercase()))
                } else {
                    Ok(value.clone())
                }
            },
            ValueTransformation::Trim => {
                if let Some(s) = value.as_str() {
                    Ok(Value::String(s.trim().to_string()))
                } else {
                    Ok(value.clone())
                }
            },
            ValueTransformation::RegexReplace { pattern, replacement } => {
                if let Some(s) = value.as_str() {
                    match regex::Regex::new(pattern) {
                        Ok(re) => {
                            let result = re.replace_all(s, replacement);
                            Ok(Value::String(result.to_string()))
                        },
                        Err(_) => Ok(value.clone()),
                    }
                } else {
                    Ok(value.clone())
                }
            },
            ValueTransformation::TypeConversion { target_type } => {
                self.convert_type(value, target_type)
            },
            ValueTransformation::Custom { function_name } => {
                // Placeholder for custom transformation functions
                self.apply_custom_transformation(value, function_name)
            },
        }
    }
    
    /// Convert value type
    fn convert_type(&self, value: &Value, target_type: &str) -> Result<Value> {
        match target_type.to_lowercase().as_str() {
            "string" => Ok(Value::String(value.to_string())),
            "integer" | "int" => {
                if let Some(s) = value.as_str() {
                    match s.parse::<i64>() {
                        Ok(i) => Ok(Value::Number(serde_json::Number::from(i))),
                        Err(_) => Ok(value.clone()),
                    }
                } else if let Some(f) = value.as_f64() {
                    Ok(Value::Number(serde_json::Number::from(f as i64)))
                } else {
                    Ok(value.clone())
                }
            },
            "float" | "number" => {
                if let Some(s) = value.as_str() {
                    match s.parse::<f64>() {
                        Ok(f) => Ok(Value::Number(serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)))),
                        Err(_) => Ok(value.clone()),
                    }
                } else {
                    Ok(value.clone())
                }
            },
            "boolean" | "bool" => {
                if let Some(s) = value.as_str() {
                    match s.to_lowercase().as_str() {
                        "true" | "1" | "yes" => Ok(Value::Bool(true)),
                        "false" | "0" | "no" => Ok(Value::Bool(false)),
                        _ => Ok(value.clone()),
                    }
                } else {
                    Ok(value.clone())
                }
            },
            _ => Ok(value.clone()),
        }
    }
    
    /// Apply custom transformation
    fn apply_custom_transformation(&self, value: &Value, _function_name: &str) -> Result<Value> {
        // Placeholder for custom transformation functions
        // In a real implementation, this would look up and execute custom functions
        Ok(value.clone())
    }
    
    /// Evaluate filter condition
    fn evaluate_filter_condition(&self, record: &Value, condition: &FilterCondition) -> Result<bool> {
        match condition {
            FilterCondition::FieldEquals { field, value } => {
                if let Value::Object(obj) = record {
                    Ok(obj.get(field) == Some(value))
                } else {
                    Ok(false)
                }
            },
            FilterCondition::FieldContains { field, value } => {
                if let Value::Object(obj) = record {
                    if let Some(field_value) = obj.get(field) {
                        if let Some(s) = field_value.as_str() {
                            Ok(s.contains(value))
                        } else {
                            Ok(false)
                        }
                    } else {
                        Ok(false)
                    }
                } else {
                    Ok(false)
                }
            },
            FilterCondition::FieldMatches { field, pattern } => {
                if let Value::Object(obj) = record {
                    if let Some(field_value) = obj.get(field) {
                        if let Some(s) = field_value.as_str() {
                            match regex::Regex::new(pattern) {
                                Ok(re) => Ok(re.is_match(s)),
                                Err(_) => Ok(false),
                            }
                        } else {
                            Ok(false)
                        }
                    } else {
                        Ok(false)
                    }
                } else {
                    Ok(false)
                }
            },
            FilterCondition::Custom { expression: _ } => {
                // Placeholder for custom filter expressions
                Ok(true)
            },
        }
    }
    
    /// Add computed field
    fn add_computed_field(
        &self,
        record: &Value,
        field_name: &str,
        _expression: &str,
    ) -> Result<Option<Value>> {
        if let Value::Object(obj) = record {
            let mut new_obj = obj.clone();
            
            // Placeholder for computed field logic
            // In a real implementation, this would evaluate the expression
            let computed_value = Value::String("computed_value".to_string());
            new_obj.insert(field_name.to_string(), computed_value);
            
            Ok(Some(Value::Object(new_obj)))
        } else {
            Ok(Some(record.clone()))
        }
    }
    
    /// Apply data cleaning operations
    fn apply_data_cleaning(
        &self,
        record: &Value,
        _operations: &[CleaningOperation],
    ) -> Result<Option<Value>> {
        // Placeholder for data cleaning logic
        // In a real implementation, this would apply various cleaning operations
        Ok(Some(record.clone()))
    }
}

/// Transformation builder for fluent API
pub struct TransformationBuilder {
    pipeline: TransformationPipeline,
}

impl TransformationBuilder {
    pub fn new() -> Self {
        Self {
            pipeline: TransformationPipeline::new(),
        }
    }
    
    pub fn map_fields(mut self, mappings: BTreeMap<String, String>) -> Self {
        self.pipeline = self.pipeline.add_step(TransformationStep::FieldMapping { mappings });
        self
    }
    
    pub fn transform_field(mut self, field: String, transform: ValueTransformation) -> Self {
        self.pipeline = self.pipeline.add_step(TransformationStep::ValueTransform { field, transform });
        self
    }
    
    pub fn filter_records(mut self, condition: FilterCondition) -> Self {
        self.pipeline = self.pipeline.add_step(TransformationStep::Filter { condition });
        self
    }
    
    pub fn add_computed_field(mut self, field_name: String, expression: String) -> Self {
        self.pipeline = self.pipeline.add_step(TransformationStep::ComputedField { field_name, expression });
        self
    }
    
    pub fn clean_data(mut self, operations: Vec<CleaningOperation>) -> Self {
        self.pipeline = self.pipeline.add_step(TransformationStep::DataCleaning { operations });
        self
    }
    
    pub fn build(self) -> TransformationPipeline {
        self.pipeline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_field_mapping() {
        let mut mappings = BTreeMap::new();
        mappings.insert("old_name".to_string(), "new_name".to_string());
        
        let pipeline = TransformationPipeline::new()
            .add_step(TransformationStep::FieldMapping { mappings });
        
        let record = json!({
            "old_name": "value",
            "other_field": "other_value"
        });
        
        let result = pipeline.transform_record(&record).unwrap().unwrap();
        
        assert_eq!(result["new_name"], "value");
        assert_eq!(result["other_field"], "other_value");
        assert!(result.get("old_name").is_none());
    }
    
    #[test]
    fn test_value_transformation() {
        let pipeline = TransformationPipeline::new()
            .add_step(TransformationStep::ValueTransform {
                field: "name".to_string(),
                transform: ValueTransformation::ToUpperCase,
            });
        
        let record = json!({
            "name": "john doe",
            "age": 30
        });
        
        let result = pipeline.transform_record(&record).unwrap().unwrap();
        
        assert_eq!(result["name"], "JOHN DOE");
        assert_eq!(result["age"], 30);
    }
    
    #[test]
    fn test_filter_condition() {
        let pipeline = TransformationPipeline::new()
            .add_step(TransformationStep::Filter {
                condition: FilterCondition::FieldEquals {
                    field: "status".to_string(),
                    value: json!("active"),
                },
            });
        
        let active_record = json!({
            "name": "John",
            "status": "active"
        });
        
        let inactive_record = json!({
            "name": "Jane",
            "status": "inactive"
        });
        
        let active_result = pipeline.transform_record(&active_record).unwrap();
        let inactive_result = pipeline.transform_record(&inactive_record).unwrap();
        
        assert!(active_result.is_some());
        assert!(inactive_result.is_none());
    }
    
    #[test]
    fn test_transformation_builder() {
        let mut mappings = BTreeMap::new();
        mappings.insert("first_name".to_string(), "name".to_string());
        
        let pipeline = TransformationBuilder::new()
            .map_fields(mappings)
            .transform_field("name".to_string(), ValueTransformation::ToUpperCase)
            .filter_records(FilterCondition::FieldContains {
                field: "name".to_string(),
                value: "JOHN".to_string(),
            })
            .build();
        
        let record = json!({
            "first_name": "john doe",
            "age": 30
        });
        
        let result = pipeline.transform_record(&record).unwrap();
        assert!(result.is_some());
    }
}