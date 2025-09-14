use super::{Migration, MigrationMetadata, MigrationMode, MigrationType, MigrationVersion};
use crate::core::error::{DatabaseError, DatabaseResult};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::SystemTime};

pub struct MigrationTemplateManager {
    templates: HashMap<String, MigrationTemplate>,
}

impl Default for MigrationTemplateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationTemplateManager {
    pub fn new() -> Self {
        let mut manager = Self {
            templates: HashMap::new(),
        };
        manager.load_builtin_templates();
        manager
    }

    fn load_builtin_templates(&mut self) {
        self.templates.insert(
            "create_table".to_string(),
            MigrationTemplate::create_table_template(),
        );

        self.templates.insert(
            "add_column".to_string(),
            MigrationTemplate::add_column_template(),
        );

        self.templates.insert(
            "drop_column".to_string(),
            MigrationTemplate::drop_column_template(),
        );

        self.templates.insert(
            "create_index".to_string(),
            MigrationTemplate::create_index_template(),
        );

        self.templates.insert(
            "data_migration".to_string(),
            MigrationTemplate::data_migration_template(),
        );

        self.templates
            .insert("custom".to_string(), MigrationTemplate::custom_template());
    }

    pub fn generate_migration(
        &self,
        template_name: &str,
        version: MigrationVersion,
        name: &str,
        params: &HashMap<String, String>,
    ) -> DatabaseResult<Migration> {
        let template = self.templates.get(template_name).ok_or_else(|| {
            DatabaseError::Migration(format!("Template '{}' not found", template_name))
        })?;

        template.generate_migration(version, name, params)
    }

    pub fn list_templates(&self) -> Vec<&str> {
        self.templates.keys().map(|s| s.as_str()).collect()
    }

    pub fn add_template(&mut self, name: String, template: MigrationTemplate) {
        self.templates.insert(name, template);
    }

    pub fn remove_template(&mut self, name: &str) -> Option<MigrationTemplate> {
        self.templates.remove(name)
    }

    pub fn get_template(&self, name: &str) -> Option<&MigrationTemplate> {
        self.templates.get(name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationTemplate {
    pub name: String,
    pub description: String,
    pub migration_type: MigrationType,
    pub mode: MigrationMode,
    pub up_template: String,
    pub down_template: Option<String>,
    pub required_params: Vec<TemplateParameter>,
    pub optional_params: Vec<TemplateParameter>,
    pub reversible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateParameter {
    pub name: String,
    pub description: String,
    pub param_type: ParameterType,
    pub default_value: Option<String>,
    pub validation_regex: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterType {
    String,
    Integer,
    Boolean,
    TableName,
    ColumnName,
    DataType,
    SqlExpression,
}

impl MigrationTemplate {
    pub fn generate_migration(
        &self,
        version: MigrationVersion,
        name: &str,
        params: &HashMap<String, String>,
    ) -> DatabaseResult<Migration> {
        self.validate_parameters(params)?;

        let up_script = self.render_template(&self.up_template, params)?;
        let down_script = if let Some(ref down_template) = self.down_template {
            Some(self.render_template(down_template, params)?)
        } else {
            None
        };

        let metadata = MigrationMetadata {
            version,
            name: name.to_string(),
            description: format!("Generated from template: {}", self.name),
            migration_type: self.migration_type.clone(),
            mode: self.mode.clone(),
            author: "template_generator".to_string(),
            created_at: SystemTime::now(),
            dependencies: Vec::new(),
            reversible: self.reversible && down_script.is_some(),
            estimated_duration: None,
            checksum: super::calculate_checksum(&up_script),
        };

        Ok(Migration {
            metadata,
            up_script,
            down_script,
            validation_script: None,
        })
    }

    fn validate_parameters(&self, params: &HashMap<String, String>) -> DatabaseResult<()> {
        for required_param in &self.required_params {
            if !params.contains_key(&required_param.name) {
                return Err(DatabaseError::Migration(format!(
                    "Required parameter '{}' is missing",
                    required_param.name
                )));
            }

            if let Some(value) = params.get(&required_param.name) {
                self.validate_parameter_value(&required_param, value)?;
            }
        }

        for (param_name, value) in params {
            let param_def = self
                .required_params
                .iter()
                .chain(self.optional_params.iter())
                .find(|p| &p.name == param_name);

            if let Some(param_def) = param_def {
                self.validate_parameter_value(param_def, value)?;
            }
        }

        Ok(())
    }

    fn validate_parameter_value(
        &self,
        param_def: &TemplateParameter,
        value: &str,
    ) -> DatabaseResult<()> {
        match param_def.param_type {
            ParameterType::String => {
                if value.is_empty() {
                    return Err(DatabaseError::Migration(format!(
                        "Parameter '{}' cannot be empty",
                        param_def.name
                    )));
                }
            }
            ParameterType::Integer => {
                if value.parse::<i64>().is_err() {
                    return Err(DatabaseError::Migration(format!(
                        "Parameter '{}' must be a valid integer",
                        param_def.name
                    )));
                }
            }
            ParameterType::Boolean => {
                if !matches!(
                    value.to_lowercase().as_str(),
                    "true" | "false" | "1" | "0" | "yes" | "no"
                ) {
                    return Err(DatabaseError::Migration(format!(
                        "Parameter '{}' must be a valid boolean",
                        param_def.name
                    )));
                }
            }
            ParameterType::TableName | ParameterType::ColumnName => {
                if !value.chars().all(|c| c.is_alphanumeric() || c == '_') || value.is_empty() {
                    return Err(DatabaseError::Migration(format!(
                        "Parameter '{}' must be a valid identifier",
                        param_def.name
                    )));
                }
            }
            ParameterType::DataType => {
                let valid_types = [
                    "INTEGER", "TEXT", "BLOB", "REAL", "NUMERIC", "VARCHAR", "CHAR",
                ];
                let normalized_type = value.to_uppercase();
                let is_valid = valid_types.iter().any(|&t| normalized_type.starts_with(t));

                if !is_valid {
                    return Err(DatabaseError::Migration(format!(
                        "Parameter '{}' must be a valid data type",
                        param_def.name
                    )));
                }
            }
            ParameterType::SqlExpression => {
                if value.trim().is_empty() {
                    return Err(DatabaseError::Migration(format!(
                        "Parameter '{}' cannot be empty SQL expression",
                        param_def.name
                    )));
                }
            }
        }

        if let Some(ref regex_pattern) = param_def.validation_regex {
            let regex = regex::Regex::new(regex_pattern).map_err(|e| {
                DatabaseError::Migration(format!(
                    "Invalid validation regex for parameter '{}': {}",
                    param_def.name, e
                ))
            })?;

            if !regex.is_match(value) {
                return Err(DatabaseError::Migration(format!(
                    "Parameter '{}' does not match required pattern",
                    param_def.name
                )));
            }
        }

        Ok(())
    }

    fn render_template(
        &self,
        template: &str,
        params: &HashMap<String, String>,
    ) -> DatabaseResult<String> {
        let mut result = template.to_string();

        for (key, value) in params {
            let placeholder = format!("{{{{{}}}}}", key);
            result = result.replace(&placeholder, value);
        }

        for optional_param in &self.optional_params {
            if let Some(ref default_value) = optional_param.default_value {
                if !params.contains_key(&optional_param.name) {
                    let placeholder = format!("{{{{{}}}}}", optional_param.name);
                    result = result.replace(&placeholder, default_value);
                }
            }
        }

        if result.contains("{{") && result.contains("}}") {
            let remaining_placeholders: Vec<&str> = result
                .split("{{")
                .skip(1)
                .filter_map(|s| s.split("}}").next())
                .collect();

            if !remaining_placeholders.is_empty() {
                return Err(DatabaseError::Migration(format!(
                    "Unresolved template placeholders: {:?}",
                    remaining_placeholders
                )));
            }
        }

        Ok(result)
    }

    pub fn create_table_template() -> Self {
        Self {
            name: "create_table".to_string(),
            description: "Create a new table with columns".to_string(),
            migration_type: MigrationType::Schema,
            mode: MigrationMode::Offline,
            up_template: r#"-- +migrate Up
CREATE TABLE {{table_name}} (
    id INTEGER PRIMARY KEY{{auto_increment}},
    {{columns}}
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"#
            .to_string(),
            down_template: Some(
                r#"-- +migrate Down
DROP TABLE IF EXISTS {{table_name}};"#
                    .to_string(),
            ),
            required_params: vec![
                TemplateParameter {
                    name: "table_name".to_string(),
                    description: "Name of the table to create".to_string(),
                    param_type: ParameterType::TableName,
                    default_value: None,
                    validation_regex: Some(r"^[a-zA-Z_][a-zA-Z0-9_]*$".to_string()),
                },
                TemplateParameter {
                    name: "columns".to_string(),
                    description: "Column definitions (name TYPE, ...)".to_string(),
                    param_type: ParameterType::SqlExpression,
                    default_value: None,
                    validation_regex: None,
                },
            ],
            optional_params: vec![TemplateParameter {
                name: "auto_increment".to_string(),
                description: "AUTO_INCREMENT for primary key".to_string(),
                param_type: ParameterType::String,
                default_value: Some("".to_string()),
                validation_regex: None,
            }],
            reversible: true,
        }
    }

    pub fn add_column_template() -> Self {
        Self {
            name: "add_column".to_string(),
            description: "Add a new column to an existing table".to_string(),
            migration_type: MigrationType::Schema,
            mode: MigrationMode::Online,
            up_template: r#"-- +migrate Up
ALTER TABLE {{table_name}} ADD COLUMN {{column_name}} {{data_type}}{{not_null}}{{default_value}};"#
                .to_string(),
            down_template: Some(
                r#"-- +migrate Down
ALTER TABLE {{table_name}} DROP COLUMN {{column_name}};"#
                    .to_string(),
            ),
            required_params: vec![
                TemplateParameter {
                    name: "table_name".to_string(),
                    description: "Name of the table".to_string(),
                    param_type: ParameterType::TableName,
                    default_value: None,
                    validation_regex: Some(r"^[a-zA-Z_][a-zA-Z0-9_]*$".to_string()),
                },
                TemplateParameter {
                    name: "column_name".to_string(),
                    description: "Name of the new column".to_string(),
                    param_type: ParameterType::ColumnName,
                    default_value: None,
                    validation_regex: Some(r"^[a-zA-Z_][a-zA-Z0-9_]*$".to_string()),
                },
                TemplateParameter {
                    name: "data_type".to_string(),
                    description: "Data type of the new column".to_string(),
                    param_type: ParameterType::DataType,
                    default_value: None,
                    validation_regex: None,
                },
            ],
            optional_params: vec![
                TemplateParameter {
                    name: "not_null".to_string(),
                    description: "NOT NULL constraint".to_string(),
                    param_type: ParameterType::String,
                    default_value: Some("".to_string()),
                    validation_regex: None,
                },
                TemplateParameter {
                    name: "default_value".to_string(),
                    description: "Default value for the column".to_string(),
                    param_type: ParameterType::String,
                    default_value: Some("".to_string()),
                    validation_regex: None,
                },
            ],
            reversible: true,
        }
    }

    pub fn drop_column_template() -> Self {
        Self {
            name: "drop_column".to_string(),
            description: "Remove a column from a table".to_string(),
            migration_type: MigrationType::Schema,
            mode: MigrationMode::Offline,
            up_template: r#"-- +migrate Up
ALTER TABLE {{table_name}} DROP COLUMN {{column_name}};"#
                .to_string(),
            down_template: None,
            required_params: vec![
                TemplateParameter {
                    name: "table_name".to_string(),
                    description: "Name of the table".to_string(),
                    param_type: ParameterType::TableName,
                    default_value: None,
                    validation_regex: Some(r"^[a-zA-Z_][a-zA-Z0-9_]*$".to_string()),
                },
                TemplateParameter {
                    name: "column_name".to_string(),
                    description: "Name of the column to drop".to_string(),
                    param_type: ParameterType::ColumnName,
                    default_value: None,
                    validation_regex: Some(r"^[a-zA-Z_][a-zA-Z0-9_]*$".to_string()),
                },
            ],
            optional_params: vec![],
            reversible: false,
        }
    }

    pub fn create_index_template() -> Self {
        Self {
            name: "create_index".to_string(),
            description: "Create an index on one or more columns".to_string(),
            migration_type: MigrationType::Index,
            mode: MigrationMode::Online,
            up_template: r#"-- +migrate Up
CREATE{{unique}} INDEX {{index_name}} ON {{table_name}} ({{columns}});"#
                .to_string(),
            down_template: Some(
                r#"-- +migrate Down
DROP INDEX IF EXISTS {{index_name}};"#
                    .to_string(),
            ),
            required_params: vec![
                TemplateParameter {
                    name: "table_name".to_string(),
                    description: "Name of the table".to_string(),
                    param_type: ParameterType::TableName,
                    default_value: None,
                    validation_regex: Some(r"^[a-zA-Z_][a-zA-Z0-9_]*$".to_string()),
                },
                TemplateParameter {
                    name: "index_name".to_string(),
                    description: "Name of the index".to_string(),
                    param_type: ParameterType::String,
                    default_value: None,
                    validation_regex: Some(r"^[a-zA-Z_][a-zA-Z0-9_]*$".to_string()),
                },
                TemplateParameter {
                    name: "columns".to_string(),
                    description: "Comma-separated list of columns".to_string(),
                    param_type: ParameterType::SqlExpression,
                    default_value: None,
                    validation_regex: None,
                },
            ],
            optional_params: vec![TemplateParameter {
                name: "unique".to_string(),
                description: "UNIQUE keyword for unique index".to_string(),
                param_type: ParameterType::String,
                default_value: Some("".to_string()),
                validation_regex: None,
            }],
            reversible: true,
        }
    }

    pub fn data_migration_template() -> Self {
        Self {
            name: "data_migration".to_string(),
            description: "Migrate data between tables or transform existing data".to_string(),
            migration_type: MigrationType::Data,
            mode: MigrationMode::Offline,
            up_template: r#"-- +migrate Up
{{migration_sql}}"#
                .to_string(),
            down_template: Some(
                r#"-- +migrate Down
{{rollback_sql}}"#
                    .to_string(),
            ),
            required_params: vec![TemplateParameter {
                name: "migration_sql".to_string(),
                description: "SQL statements for data migration".to_string(),
                param_type: ParameterType::SqlExpression,
                default_value: None,
                validation_regex: None,
            }],
            optional_params: vec![TemplateParameter {
                name: "rollback_sql".to_string(),
                description: "SQL statements to rollback the migration".to_string(),
                param_type: ParameterType::SqlExpression,
                default_value: Some("-- No rollback available".to_string()),
                validation_regex: None,
            }],
            reversible: true,
        }
    }

    pub fn custom_template() -> Self {
        Self {
            name: "custom".to_string(),
            description: "Custom migration with user-defined SQL".to_string(),
            migration_type: MigrationType::Schema,
            mode: MigrationMode::Offline,
            up_template: r#"-- +migrate Up
{{up_sql}}"#
                .to_string(),
            down_template: Some(
                r#"-- +migrate Down
{{down_sql}}"#
                    .to_string(),
            ),
            required_params: vec![TemplateParameter {
                name: "up_sql".to_string(),
                description: "SQL statements for the migration".to_string(),
                param_type: ParameterType::SqlExpression,
                default_value: None,
                validation_regex: None,
            }],
            optional_params: vec![TemplateParameter {
                name: "down_sql".to_string(),
                description: "SQL statements to rollback the migration".to_string(),
                param_type: ParameterType::SqlExpression,
                default_value: Some("-- No rollback available".to_string()),
                validation_regex: None,
            }],
            reversible: true,
        }
    }
}

pub struct MigrationGenerator;

impl MigrationGenerator {
    pub fn generate_timestamp_version() -> MigrationVersion {
        super::timestamp_to_version(SystemTime::now())
    }

    pub fn generate_sequential_version(current_version: MigrationVersion) -> MigrationVersion {
        current_version.next()
    }

    pub fn generate_migration_filename(version: MigrationVersion, name: &str) -> String {
        format!("{}_{}.sql", version, name.to_lowercase().replace(' ', "_"))
    }

    pub fn save_migration_to_file(
        migration: &Migration,
        directory: &str,
        filename: &str,
    ) -> DatabaseResult<String> {
        std::fs::create_dir_all(directory).map_err(|e| DatabaseError::Io(e.to_string()))?;

        let file_path = format!("{}/{}", directory, filename);

        let mut content = String::new();
        content.push_str(&format!("-- Migration: {}\n", migration.metadata.name));
        content.push_str(&format!("-- Version: {}\n", migration.metadata.version));
        content.push_str(&format!(
            "-- Type: {:?}\n",
            migration.metadata.migration_type
        ));
        content.push_str(&format!("-- Mode: {:?}\n", migration.metadata.mode));
        content.push_str(&format!("-- Author: {}\n", migration.metadata.author));
        content.push_str(&format!(
            "-- Created: {:?}\n",
            migration.metadata.created_at
        ));

        if !migration.metadata.dependencies.is_empty() {
            content.push_str(&format!(
                "-- Dependencies: {:?}\n",
                migration.metadata.dependencies
            ));
        }

        content.push_str(&format!(
            "-- Reversible: {}\n",
            migration.metadata.reversible
        ));
        content.push_str(&format!("-- Checksum: {}\n", migration.metadata.checksum));
        content.push('\n');

        content.push_str(&migration.up_script);

        if let Some(ref down_script) = migration.down_script {
            content.push_str("\n\n");
            content.push_str(down_script);
        }

        std::fs::write(&file_path, content).map_err(|e| DatabaseError::Io(e.to_string()))?;

        Ok(file_path)
    }
}
