//! Schema validation and compatibility checking

use super::{ColumnDefinition, DataType, Migration, Schema, TableDefinition};
use crate::{Database, Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::info;

/// Schema validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Overall validation status
    pub is_valid: bool,

    /// Validation errors
    pub errors: Vec<ValidationError>,

    /// Validation warnings
    pub warnings: Vec<ValidationWarning>,

    /// Schema statistics
    pub statistics: SchemaStatistics,
}

/// Validation error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Error type
    pub error_type: ValidationErrorType,

    /// Error message
    pub message: String,

    /// Context (table, column, etc.)
    pub context: Option<String>,
}

/// Validation error types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationErrorType {
    /// Missing required element
    MissingElement,

    /// Duplicate element
    DuplicateElement,

    /// Invalid data type
    InvalidDataType,

    /// Invalid constraint
    InvalidConstraint,

    /// Circular dependency
    CircularDependency,

    /// Incompatible change
    IncompatibleChange,

    /// Data integrity violation
    DataIntegrityViolation,
}

/// Validation warning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWarning {
    /// Warning type
    pub warning_type: ValidationWarningType,

    /// Warning message
    pub message: String,

    /// Context
    pub context: Option<String>,
}

/// Validation warning types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationWarningType {
    /// Performance impact
    PerformanceImpact,

    /// Deprecated feature
    DeprecatedFeature,

    /// Missing index
    MissingIndex,

    /// Unused element
    UnusedElement,

    /// Large table
    LargeTable,
}

/// Schema statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaStatistics {
    /// Number of tables
    pub table_count: usize,

    /// Number of columns
    pub column_count: usize,

    /// Number of indexes
    pub index_count: usize,

    /// Number of constraints
    pub constraint_count: usize,

    /// Estimated size
    pub estimated_size_bytes: u64,
}

/// Schema validator
pub struct SchemaValidator {
    database: Arc<Database>,
}

impl SchemaValidator {
    /// Create a new schema validator
    pub fn new(database: Arc<Database>) -> Result<Self> {
        Ok(Self { database })
    }

    /// Validate a schema
    pub fn validate_schema(&self, schema: &Schema) -> Result<ValidationResult> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Validate tables
        self.validate_tables(&schema.tables, &mut errors, &mut warnings)?;

        // Validate indexes
        self.validate_indexes(&schema.tables, &schema.indexes, &mut errors, &mut warnings)?;

        // Validate constraints
        self.validate_constraints(
            &schema.tables,
            &schema.constraints,
            &mut errors,
            &mut warnings,
        )?;

        // Check for circular dependencies
        self.check_circular_dependencies(&schema.tables, &mut errors)?;

        // Generate statistics
        let statistics = self.calculate_statistics(schema);

        // Performance warnings
        self.check_performance_issues(schema, &mut warnings);

        let is_valid = errors.is_empty();

        Ok(ValidationResult {
            is_valid,
            errors,
            warnings,
            statistics,
        })
    }

    /// Validate migration preconditions
    pub fn validate_preconditions(&self, migration: &dyn Migration) -> Result<()> {
        info!(
            "Validating preconditions for migration {}",
            migration.version()
        );

        // Let the migration validate its own preconditions
        migration.validate_preconditions(&self.database)?;

        // Additional validation
        for step in migration.steps() {
            match &step {
                super::MigrationStep::DropTable { name, .. } => {
                    // Check if table exists
                    let table_key = format!("__table_def_{}", name);
                    if self.database.get(table_key.as_bytes())?.is_none() {
                        return Err(Error::Validation(format!("Table {} does not exist", name)));
                    }
                }
                super::MigrationStep::DropColumn { table, column, .. } => {
                    // Check if column exists
                    let table_key = format!("__table_def_{}", table);
                    if let Some(data) = self.database.get(table_key.as_bytes())? {
                        let table_def: TableDefinition = serde_json::from_slice(&data)
                            .map_err(|e| Error::Serialization(e.to_string()))?;

                        if !table_def.columns.iter().any(|c| c.name == *column) {
                            return Err(Error::Validation(format!(
                                "Column {} does not exist in table {}",
                                column, table
                            )));
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Validate migration postconditions
    pub fn validate_postconditions(&self, migration: &dyn Migration) -> Result<()> {
        info!(
            "Validating postconditions for migration {}",
            migration.version()
        );

        // Let the migration validate its own postconditions
        migration.validate_postconditions(&self.database)?;

        // Validate the target schema
        let target_schema = migration.target_schema();
        let validation_result = self.validate_schema(&target_schema)?;

        if !validation_result.is_valid {
            let errors: Vec<String> = validation_result
                .errors
                .iter()
                .map(|e| e.message.clone())
                .collect();
            return Err(Error::Validation(format!(
                "Schema validation failed: {}",
                errors.join(", ")
            )));
        }

        Ok(())
    }

    /// Validate tables
    fn validate_tables(
        &self,
        tables: &std::collections::BTreeMap<String, TableDefinition>,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) -> Result<()> {
        let mut table_names = HashSet::new();

        for (name, table) in tables {
            // Check for duplicate table names
            if !table_names.insert(name.clone()) {
                errors.push(ValidationError {
                    error_type: ValidationErrorType::DuplicateElement,
                    message: format!("Duplicate table name: {}", name),
                    context: Some(name.clone()),
                });
            }

            // Validate table definition
            self.validate_table_definition(table, errors, warnings)?;
        }

        Ok(())
    }

    /// Validate a single table definition
    fn validate_table_definition(
        &self,
        table: &TableDefinition,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) -> Result<()> {
        let mut column_names = HashSet::new();

        // Validate columns
        for column in &table.columns {
            // Check for duplicate column names
            if !column_names.insert(column.name.clone()) {
                errors.push(ValidationError {
                    error_type: ValidationErrorType::DuplicateElement,
                    message: format!("Duplicate column name: {}", column.name),
                    context: Some(table.name.clone()),
                });
            }

            // Validate column definition
            self.validate_column_definition(column, &table.name, errors)?;
        }

        // Validate primary key
        for pk_column in &table.primary_key {
            if !column_names.contains(pk_column) {
                errors.push(ValidationError {
                    error_type: ValidationErrorType::MissingElement,
                    message: format!("Primary key column {} not found in table", pk_column),
                    context: Some(table.name.clone()),
                });
            }
        }

        // Warn if no primary key
        if table.primary_key.is_empty() {
            warnings.push(ValidationWarning {
                warning_type: ValidationWarningType::MissingIndex,
                message: "Table has no primary key".to_string(),
                context: Some(table.name.clone()),
            });
        }

        Ok(())
    }

    /// Validate column definition
    fn validate_column_definition(
        &self,
        column: &ColumnDefinition,
        table_name: &str,
        errors: &mut Vec<ValidationError>,
    ) -> Result<()> {
        // Validate data type
        match &column.data_type {
            DataType::Custom(type_name) => {
                // Check if custom type is registered
                let type_key = format!("__custom_type_{}", type_name);
                if self.database.get(type_key.as_bytes())?.is_none() {
                    errors.push(ValidationError {
                        error_type: ValidationErrorType::InvalidDataType,
                        message: format!("Unknown custom type: {}", type_name),
                        context: Some(format!("{}.{}", table_name, column.name)),
                    });
                }
            }
            _ => {} // Built-in types are always valid
        }

        // Validate constraints
        for constraint in &column.constraints {
            match constraint {
                super::ColumnConstraint::References {
                    table,
                    column: _ref_column,
                } => {
                    // Check if referenced table exists
                    let ref_table_key = format!("__table_def_{}", table);
                    if self.database.get(ref_table_key.as_bytes())?.is_none() {
                        errors.push(ValidationError {
                            error_type: ValidationErrorType::InvalidConstraint,
                            message: format!("Referenced table {} does not exist", table),
                            context: Some(format!("{}.{}", table_name, column.name)),
                        });
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Validate indexes
    fn validate_indexes(
        &self,
        tables: &std::collections::BTreeMap<String, TableDefinition>,
        indexes: &std::collections::BTreeMap<String, super::IndexDefinition>,
        errors: &mut Vec<ValidationError>,
        _warnings: &mut Vec<ValidationWarning>,
    ) -> Result<()> {
        for (name, index) in indexes {
            // Check if table exists
            if !tables.contains_key(&index.table) {
                errors.push(ValidationError {
                    error_type: ValidationErrorType::MissingElement,
                    message: format!("Table {} for index {} not found", index.table, name),
                    context: Some(name.clone()),
                });
                continue;
            }

            let table = &tables[&index.table];
            let column_names: HashSet<_> = table.columns.iter().map(|c| &c.name).collect();

            // Check if all indexed columns exist
            for index_col in &index.columns {
                if !column_names.contains(&index_col.name) {
                    errors.push(ValidationError {
                        error_type: ValidationErrorType::MissingElement,
                        message: format!("Column {} for index {} not found", index_col.name, name),
                        context: Some(name.clone()),
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate constraints
    fn validate_constraints(
        &self,
        tables: &std::collections::BTreeMap<String, TableDefinition>,
        constraints: &std::collections::BTreeMap<String, super::ConstraintDefinition>,
        errors: &mut Vec<ValidationError>,
        _warnings: &mut Vec<ValidationWarning>,
    ) -> Result<()> {
        for (name, constraint) in constraints {
            // Check if table exists
            if !tables.contains_key(&constraint.table) {
                errors.push(ValidationError {
                    error_type: ValidationErrorType::MissingElement,
                    message: format!(
                        "Table {} for constraint {} not found",
                        constraint.table, name
                    ),
                    context: Some(name.clone()),
                });
            }
        }

        Ok(())
    }

    /// Check for circular dependencies
    fn check_circular_dependencies(
        &self,
        tables: &std::collections::BTreeMap<String, TableDefinition>,
        errors: &mut Vec<ValidationError>,
    ) -> Result<()> {
        // Build dependency graph
        let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

        for (table_name, table) in tables {
            let mut table_deps = HashSet::new();

            for column in &table.columns {
                for constraint in &column.constraints {
                    if let super::ColumnConstraint::References {
                        table: ref_table, ..
                    } = constraint
                    {
                        table_deps.insert(ref_table.clone());
                    }
                }
            }

            dependencies.insert(table_name.clone(), table_deps);
        }

        // Check for cycles using DFS
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();

        for table in tables.keys() {
            if !visited.contains(table) {
                if self.has_cycle(table, &dependencies, &mut visited, &mut stack) {
                    errors.push(ValidationError {
                        error_type: ValidationErrorType::CircularDependency,
                        message: "Circular dependency detected in foreign key references"
                            .to_string(),
                        context: Some(table.clone()),
                    });
                }
            }
        }

        Ok(())
    }

    /// DFS helper for cycle detection
    fn has_cycle(
        &self,
        node: &str,
        graph: &HashMap<String, HashSet<String>>,
        visited: &mut HashSet<String>,
        stack: &mut HashSet<String>,
    ) -> bool {
        visited.insert(node.to_string());
        stack.insert(node.to_string());

        if let Some(neighbors) = graph.get(node) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    if self.has_cycle(neighbor, graph, visited, stack) {
                        return true;
                    }
                } else if stack.contains(neighbor) {
                    return true;
                }
            }
        }

        stack.remove(node);
        false
    }

    /// Calculate schema statistics
    fn calculate_statistics(&self, schema: &Schema) -> SchemaStatistics {
        let mut column_count = 0;
        let mut estimated_size = 0u64;

        for table in schema.tables.values() {
            column_count += table.columns.len();

            // Estimate table size (very rough estimate)
            estimated_size += table.columns.len() as u64 * 1024 * 1024; // 1MB per column
        }

        SchemaStatistics {
            table_count: schema.tables.len(),
            column_count,
            index_count: schema.indexes.len(),
            constraint_count: schema.constraints.len(),
            estimated_size_bytes: estimated_size,
        }
    }

    /// Check for performance issues
    fn check_performance_issues(&self, schema: &Schema, warnings: &mut Vec<ValidationWarning>) {
        // Check for large tables without indexes
        for (table_name, table) in &schema.tables {
            let table_indexes: Vec<_> = schema
                .indexes
                .values()
                .filter(|idx| idx.table == *table_name)
                .collect();

            if table_indexes.is_empty() && table.columns.len() > 10 {
                warnings.push(ValidationWarning {
                    warning_type: ValidationWarningType::MissingIndex,
                    message: "Large table without indexes may have performance issues".to_string(),
                    context: Some(table_name.clone()),
                });
            }
        }

        // Check for too many indexes
        for (table_name, _) in &schema.tables {
            let index_count = schema
                .indexes
                .values()
                .filter(|idx| idx.table == *table_name)
                .count();

            if index_count > 10 {
                warnings.push(ValidationWarning {
                    warning_type: ValidationWarningType::PerformanceImpact,
                    message: format!(
                        "Table has {} indexes which may impact write performance",
                        index_count
                    ),
                    context: Some(table_name.clone()),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result() {
        let result = ValidationResult {
            is_valid: true,
            errors: vec![],
            warnings: vec![ValidationWarning {
                warning_type: ValidationWarningType::MissingIndex,
                message: "No index on foreign key".to_string(),
                context: Some("users.org_id".to_string()),
            }],
            statistics: SchemaStatistics {
                table_count: 5,
                column_count: 20,
                index_count: 8,
                constraint_count: 10,
                estimated_size_bytes: 1024 * 1024 * 100, // 100MB
            },
        };

        assert!(result.is_valid);
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.statistics.table_count, 5);
    }
}
