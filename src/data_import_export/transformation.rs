//! Data Transformation Pipeline
//!
//! Provides data transformation capabilities during import/export operations
//! including field mapping, data cleaning, and custom transformations.

use crate::Result;
use serde_json::{Map, Value};
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
    FieldMapping { mappings: BTreeMap<String, String> },
    /// Apply value transformations
    ValueTransform {
        field: String,
        transform: ValueTransformation,
    },
    /// Filter records based on condition
    Filter { condition: FilterCondition },
    /// Add computed fields
    ComputedField {
        field_name: String,
        expression: String,
    },
    /// Data cleaning operations
    DataCleaning { operations: Vec<CleaningOperation> },
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
    RegexReplace {
        pattern: String,
        replacement: String,
    },
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
    NormalizeFormats {
        field_types: BTreeMap<String, String>,
    },
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
        Self { steps: Vec::new() }
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
            }
            TransformationStep::ValueTransform { field, transform } => {
                self.apply_value_transform(record, field, transform)
            }
            TransformationStep::Filter { condition } => {
                if self.evaluate_filter_condition(record, condition)? {
                    Ok(Some(record.clone()))
                } else {
                    Ok(None)
                }
            }
            TransformationStep::ComputedField {
                field_name,
                expression,
            } => self.add_computed_field(record, field_name, expression),
            TransformationStep::DataCleaning { operations } => {
                self.apply_data_cleaning(record, operations)
            }
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
            }
            ValueTransformation::ToLowerCase => {
                if let Some(s) = value.as_str() {
                    Ok(Value::String(s.to_lowercase()))
                } else {
                    Ok(value.clone())
                }
            }
            ValueTransformation::Trim => {
                if let Some(s) = value.as_str() {
                    Ok(Value::String(s.trim().to_string()))
                } else {
                    Ok(value.clone())
                }
            }
            ValueTransformation::RegexReplace {
                pattern,
                replacement,
            } => {
                if let Some(s) = value.as_str() {
                    match regex::Regex::new(pattern) {
                        Ok(re) => {
                            let result = re.replace_all(s, replacement);
                            Ok(Value::String(result.to_string()))
                        }
                        Err(_) => Ok(value.clone()),
                    }
                } else {
                    Ok(value.clone())
                }
            }
            ValueTransformation::TypeConversion { target_type } => {
                self.convert_type(value, target_type)
            }
            ValueTransformation::Custom { function_name } => {
                // Placeholder for custom transformation functions
                self.apply_custom_transformation(value, function_name)
            }
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
            }
            "float" | "number" => {
                if let Some(s) = value.as_str() {
                    match s.parse::<f64>() {
                        Ok(f) => Ok(Value::Number(
                            serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)),
                        )),
                        Err(_) => Ok(value.clone()),
                    }
                } else {
                    Ok(value.clone())
                }
            }
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
            }
            _ => Ok(value.clone()),
        }
    }

    /// Apply custom transformation
    fn apply_custom_transformation(&self, value: &Value, function_name: &str) -> Result<Value> {
        match function_name {
            "capitalize" => {
                if let Some(s) = value.as_str() {
                    let mut chars: Vec<char> = s.chars().collect();
                    if let Some(first_char) = chars.get_mut(0) {
                        *first_char = first_char.to_uppercase().next().unwrap_or(*first_char);
                    }
                    Ok(Value::String(chars.into_iter().collect()))
                } else {
                    Ok(value.clone())
                }
            }
            "reverse" => {
                if let Some(s) = value.as_str() {
                    Ok(Value::String(s.chars().rev().collect()))
                } else {
                    Ok(value.clone())
                }
            }
            "hash" => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};

                let mut hasher = DefaultHasher::new();
                value.to_string().hash(&mut hasher);
                let hash_value = hasher.finish();
                Ok(Value::String(format!("{:x}", hash_value)))
            }
            "length" => {
                if let Some(s) = value.as_str() {
                    Ok(Value::Number(serde_json::Number::from(s.len())))
                } else {
                    Ok(Value::Number(serde_json::Number::from(0)))
                }
            }
            "abs" => {
                if let Some(n) = value.as_f64() {
                    Ok(Value::Number(
                        serde_json::Number::from_f64(n.abs())
                            .unwrap_or(serde_json::Number::from(0)),
                    ))
                } else {
                    Ok(value.clone())
                }
            }
            "round" => {
                if let Some(n) = value.as_f64() {
                    Ok(Value::Number(serde_json::Number::from(n.round() as i64)))
                } else {
                    Ok(value.clone())
                }
            }
            "floor" => {
                if let Some(n) = value.as_f64() {
                    Ok(Value::Number(serde_json::Number::from(n.floor() as i64)))
                } else {
                    Ok(value.clone())
                }
            }
            "ceil" => {
                if let Some(n) = value.as_f64() {
                    Ok(Value::Number(serde_json::Number::from(n.ceil() as i64)))
                } else {
                    Ok(value.clone())
                }
            }
            "strip_whitespace" => {
                if let Some(s) = value.as_str() {
                    Ok(Value::String(
                        s.chars().filter(|c| !c.is_whitespace()).collect(),
                    ))
                } else {
                    Ok(value.clone())
                }
            }
            "extract_domain" => {
                if let Some(s) = value.as_str() {
                    if let Some(at_pos) = s.rfind('@') {
                        let domain = &s[at_pos + 1..];
                        Ok(Value::String(domain.to_string()))
                    } else {
                        Ok(value.clone())
                    }
                } else {
                    Ok(value.clone())
                }
            }
            "mask_email" => {
                if let Some(s) = value.as_str() {
                    if let Some(at_pos) = s.find('@') {
                        let (username, domain) = s.split_at(at_pos);
                        if username.len() > 2 {
                            let masked_username = format!(
                                "{}{}{}",
                                &username[..1],
                                "*".repeat(username.len() - 2),
                                &username[username.len() - 1..]
                            );
                            Ok(Value::String(format!("{}{}", masked_username, domain)))
                        } else {
                            Ok(Value::String(format!("**{}", domain)))
                        }
                    } else {
                        Ok(value.clone())
                    }
                } else {
                    Ok(value.clone())
                }
            }
            _ => {
                // Unknown function, return original value with error marker
                Ok(Value::String(format!(
                    "CUSTOM_ERROR: Unknown function '{}'",
                    function_name
                )))
            }
        }
    }

    /// Evaluate filter condition
    fn evaluate_filter_condition(
        &self,
        record: &Value,
        condition: &FilterCondition,
    ) -> Result<bool> {
        match condition {
            FilterCondition::FieldEquals { field, value } => {
                if let Value::Object(obj) = record {
                    Ok(obj.get(field) == Some(value))
                } else {
                    Ok(false)
                }
            }
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
            }
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
            }
            FilterCondition::Custom { expression: _ } => {
                // Placeholder for custom filter expressions
                Ok(true)
            }
        }
    }

    /// Add computed field
    fn add_computed_field(
        &self,
        record: &Value,
        field_name: &str,
        expression: &str,
    ) -> Result<Option<Value>> {
        if let Value::Object(obj) = record {
            let mut new_obj = obj.clone();

            // Evaluate expression to compute field value
            let computed_value = self.evaluate_expression(expression, obj)?;
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
        operations: &[CleaningOperation],
    ) -> Result<Option<Value>> {
        let mut current_record = record.clone();

        for operation in operations {
            match operation {
                CleaningOperation::HandleNulls { strategy } => {
                    current_record = self.handle_null_values(&current_record, strategy)?;
                }
                CleaningOperation::RemoveDuplicates { .. } => {
                    // Note: This operation would typically be applied at the dataset level,
                    // not per-record. For now, we just pass through.
                    continue;
                }
                CleaningOperation::ValidateIntegrity { rules } => {
                    if !self.validate_record_integrity(&current_record, rules)? {
                        return Ok(None); // Filter out invalid records
                    }
                }
                CleaningOperation::NormalizeFormats { field_types } => {
                    current_record = self.normalize_field_formats(&current_record, field_types)?;
                }
            }
        }

        Ok(Some(current_record))
    }

    /// Evaluate expression for computed fields
    fn evaluate_expression(&self, expression: &str, record: &Map<String, Value>) -> Result<Value> {
        let expr = expression.trim();

        // Handle concat function: concat(field1, 'literal', field2)
        if expr.starts_with("concat(") && expr.ends_with(")") {
            return self.evaluate_concat_expression(expr, record);
        }

        // Handle conditional function: if(condition, true_value, false_value)
        if expr.starts_with("if(") && expr.ends_with(")") {
            return self.evaluate_if_expression(expr, record);
        }

        // Handle simple field reference
        if let Some(value) = record.get(expr) {
            return Ok(value.clone());
        }

        // Handle string literal
        if expr.starts_with('"') && expr.ends_with('"') {
            return Ok(Value::String(expr[1..expr.len() - 1].to_string()));
        }
        if expr.starts_with('\'') && expr.ends_with('\'') {
            return Ok(Value::String(expr[1..expr.len() - 1].to_string()));
        }

        // Handle numeric literal
        if let Ok(i) = expr.parse::<i64>() {
            return Ok(Value::Number(serde_json::Number::from(i)));
        }
        if let Ok(f) = expr.parse::<f64>() {
            return Ok(Value::Number(
                serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)),
            ));
        }

        // Default fallback
        Ok(Value::String(format!("EXPR_ERROR: {}", expr)))
    }

    /// Evaluate concat expression: concat(arg1, arg2, ...)
    fn evaluate_concat_expression(&self, expr: &str, record: &Map<String, Value>) -> Result<Value> {
        let inner = &expr[7..expr.len() - 1]; // Remove "concat(" and ")"
        let args = self.parse_function_arguments(inner)?;

        let mut result = String::new();
        for arg in args {
            let value = self.evaluate_expression(&arg, record)?;
            match value {
                Value::String(s) => result.push_str(&s),
                Value::Number(n) => result.push_str(&n.to_string()),
                Value::Bool(b) => result.push_str(&b.to_string()),
                _ => result.push_str(&value.to_string()),
            }
        }

        Ok(Value::String(result))
    }

    /// Evaluate if expression: if(condition, true_value, false_value)
    fn evaluate_if_expression(&self, expr: &str, record: &Map<String, Value>) -> Result<Value> {
        let inner = &expr[3..expr.len() - 1]; // Remove "if(" and ")"
        let args = self.parse_function_arguments(inner)?;

        if args.len() != 3 {
            return Ok(Value::String("IF_ERROR: Expected 3 arguments".to_string()));
        }

        let condition_result = self.evaluate_condition(&args[0], record)?;

        if condition_result {
            self.evaluate_expression(&args[1], record)
        } else {
            self.evaluate_expression(&args[2], record)
        }
    }

    /// Parse function arguments, handling nested parentheses and quoted strings
    fn parse_function_arguments(&self, args_str: &str) -> Result<Vec<String>> {
        let mut args = Vec::new();
        let mut current_arg = String::new();
        let mut paren_depth = 0;
        let mut in_quote = false;
        let mut quote_char = '"';

        for ch in args_str.chars() {
            match ch {
                '"' | '\'' if !in_quote => {
                    in_quote = true;
                    quote_char = ch;
                    current_arg.push(ch);
                }
                c if in_quote && c == quote_char => {
                    in_quote = false;
                    current_arg.push(ch);
                }
                '(' if !in_quote => {
                    paren_depth += 1;
                    current_arg.push(ch);
                }
                ')' if !in_quote => {
                    paren_depth -= 1;
                    current_arg.push(ch);
                }
                ',' if !in_quote && paren_depth == 0 => {
                    args.push(current_arg.trim().to_string());
                    current_arg.clear();
                }
                _ => {
                    current_arg.push(ch);
                }
            }
        }

        if !current_arg.trim().is_empty() {
            args.push(current_arg.trim().to_string());
        }

        Ok(args)
    }

    /// Evaluate a condition (supports >, <, >=, <=, ==, !=)
    fn evaluate_condition(&self, condition: &str, record: &Map<String, Value>) -> Result<bool> {
        let condition = condition.trim();

        // Parse comparison operators
        let operators = [">=", "<=", "==", "!=", ">", "<"];

        for op in &operators {
            if let Some(pos) = condition.find(op) {
                let left = condition[..pos].trim();
                let right = condition[pos + op.len()..].trim();

                let left_val = self.evaluate_expression(left, record)?;
                let right_val = self.evaluate_expression(right, record)?;

                return self.compare_values(&left_val, &right_val, op);
            }
        }

        // If no operator found, evaluate as expression and check truthiness
        let value = self.evaluate_expression(condition, record)?;
        Ok(self.is_truthy(&value))
    }

    /// Compare two values using the given operator
    fn compare_values(&self, left: &Value, right: &Value, op: &str) -> Result<bool> {
        match (left, right) {
            (Value::Number(l), Value::Number(r)) => {
                let l_f = l.as_f64().unwrap_or(0.0);
                let r_f = r.as_f64().unwrap_or(0.0);

                Ok(match op {
                    ">" => l_f > r_f,
                    "<" => l_f < r_f,
                    ">=" => l_f >= r_f,
                    "<=" => l_f <= r_f,
                    "==" => (l_f - r_f).abs() < f64::EPSILON,
                    "!=" => (l_f - r_f).abs() >= f64::EPSILON,
                    _ => false,
                })
            }
            (Value::String(l), Value::String(r)) => Ok(match op {
                ">" => l > r,
                "<" => l < r,
                ">=" => l >= r,
                "<=" => l <= r,
                "==" => l == r,
                "!=" => l != r,
                _ => false,
            }),
            (Value::Bool(l), Value::Bool(r)) => Ok(match op {
                "==" => l == r,
                "!=" => l != r,
                _ => false,
            }),
            _ => {
                // Try to convert to strings for comparison
                let l_str = left.to_string();
                let r_str = right.to_string();
                Ok(match op {
                    "==" => l_str == r_str,
                    "!=" => l_str != r_str,
                    _ => false,
                })
            }
        }
    }

    /// Check if a value is considered "truthy"
    fn is_truthy(&self, value: &Value) -> bool {
        match value {
            Value::Bool(b) => *b,
            Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
            Value::String(s) => !s.is_empty() && s != "false" && s != "0",
            Value::Null => false,
            _ => true,
        }
    }

    /// Handle null values in record according to strategy
    fn handle_null_values(&self, record: &Value, strategy: &NullHandlingStrategy) -> Result<Value> {
        if let Value::Object(obj) = record {
            let mut new_obj = obj.clone();

            match strategy {
                NullHandlingStrategy::RemoveRecord => {
                    // Check if any field is null
                    for (_, value) in obj {
                        if value.is_null() {
                            return Ok(Value::Null); // Signal to filter this record
                        }
                    }
                }
                NullHandlingStrategy::ReplaceWithDefault { default_value } => {
                    for (key, value) in new_obj.iter_mut() {
                        if value.is_null() {
                            *value = default_value.clone();
                        }
                    }
                }
                NullHandlingStrategy::ReplaceWithComputed { expression } => {
                    for (key, value) in new_obj.iter_mut() {
                        if value.is_null() {
                            // Create a temporary record for expression evaluation
                            let temp_record = obj.clone();
                            *value = self.evaluate_expression(expression, &temp_record)?;
                        }
                    }
                }
                NullHandlingStrategy::Skip => {
                    // Remove null fields
                    new_obj.retain(|_, value| !value.is_null());
                }
            }

            Ok(Value::Object(new_obj))
        } else {
            Ok(record.clone())
        }
    }

    /// Validate record integrity according to rules
    fn validate_record_integrity(&self, record: &Value, rules: &[String]) -> Result<bool> {
        if let Value::Object(obj) = record {
            for rule in rules {
                if !self.evaluate_validation_rule(rule, obj)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Evaluate a validation rule
    fn evaluate_validation_rule(&self, rule: &str, record: &Map<String, Value>) -> Result<bool> {
        // Simple validation rules like "field_name != null", "amount > 0", etc.
        self.evaluate_condition(rule, record)
    }

    /// Normalize field formats according to type specifications
    fn normalize_field_formats(
        &self,
        record: &Value,
        field_types: &BTreeMap<String, String>,
    ) -> Result<Value> {
        if let Value::Object(obj) = record {
            let mut new_obj = obj.clone();

            for (field_name, target_type) in field_types {
                if let Some(value) = new_obj.get_mut(field_name) {
                    *value = self.convert_type(value, target_type)?;
                }
            }

            Ok(Value::Object(new_obj))
        } else {
            Ok(record.clone())
        }
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
        self.pipeline = self
            .pipeline
            .add_step(TransformationStep::FieldMapping { mappings });
        self
    }

    pub fn transform_field(mut self, field: String, transform: ValueTransformation) -> Self {
        self.pipeline = self
            .pipeline
            .add_step(TransformationStep::ValueTransform { field, transform });
        self
    }

    pub fn filter_records(mut self, condition: FilterCondition) -> Self {
        self.pipeline = self
            .pipeline
            .add_step(TransformationStep::Filter { condition });
        self
    }

    pub fn add_computed_field(mut self, field_name: String, expression: String) -> Self {
        self.pipeline = self.pipeline.add_step(TransformationStep::ComputedField {
            field_name,
            expression,
        });
        self
    }

    pub fn clean_data(mut self, operations: Vec<CleaningOperation>) -> Self {
        self.pipeline = self
            .pipeline
            .add_step(TransformationStep::DataCleaning { operations });
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

        let pipeline =
            TransformationPipeline::new().add_step(TransformationStep::FieldMapping { mappings });

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
        let pipeline = TransformationPipeline::new().add_step(TransformationStep::ValueTransform {
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
        let pipeline = TransformationPipeline::new().add_step(TransformationStep::Filter {
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
