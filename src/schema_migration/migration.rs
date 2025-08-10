//! Migration definition and interface

use super::{Schema, SchemaVersion};
use crate::{Database, Result};
use serde::{Deserialize, Serialize};

/// Migration direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationDirection {
    /// Apply migration (upgrade)
    Up,
    /// Revert migration (downgrade)
    Down,
}

/// Migration trait that all migrations must implement
pub trait Migration: Send + Sync {
    /// Get the version this migration upgrades to
    fn version(&self) -> SchemaVersion;

    /// Get a description of what this migration does
    fn description(&self) -> &str;

    /// Check if this migration is destructive (data loss possible)
    fn is_destructive(&self) -> bool {
        false
    }

    /// Get the list of migration steps
    fn steps(&self) -> Vec<MigrationStep>;

    /// Pre-migration validation
    fn validate_preconditions(&self, _database: &Database) -> Result<()> {
        Ok(())
    }

    /// Post-migration validation
    fn validate_postconditions(&self, _database: &Database) -> Result<()> {
        Ok(())
    }

    /// Get the schema after this migration
    fn target_schema(&self) -> Schema;

    /// Custom migration logic (if steps are not sufficient)
    fn migrate_up(&self, database: &Database) -> Result<()> {
        // Default implementation uses steps
        for step in self.steps() {
            step.execute_up(database)?;
        }
        Ok(())
    }

    /// Custom rollback logic (if steps are not sufficient)
    fn migrate_down(&self, database: &Database) -> Result<()> {
        // Default implementation uses steps in reverse
        for step in self.steps().into_iter().rev() {
            step.execute_down(database)?;
        }
        Ok(())
    }
}

/// A single step in a migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationStep {
    /// Create a new table
    CreateTable {
        name: String,
        definition: super::TableDefinition,
    },

    /// Drop a table
    DropTable { name: String, if_exists: bool },

    /// Rename a table
    RenameTable { old_name: String, new_name: String },

    /// Add a column to a table
    AddColumn {
        table: String,
        column: super::ColumnDefinition,
    },

    /// Drop a column from a table
    DropColumn {
        table: String,
        column: String,
        if_exists: bool,
    },

    /// Rename a column
    RenameColumn {
        table: String,
        old_name: String,
        new_name: String,
    },

    /// Modify a column definition
    ModifyColumn {
        table: String,
        column: String,
        new_definition: super::ColumnDefinition,
    },

    /// Create an index
    CreateIndex { definition: super::IndexDefinition },

    /// Drop an index
    DropIndex { name: String, if_exists: bool },

    /// Add a constraint
    AddConstraint {
        definition: super::ConstraintDefinition,
    },

    /// Drop a constraint
    DropConstraint {
        table: String,
        name: String,
        if_exists: bool,
    },

    /// Execute custom code
    Custom {
        up: String,   // Code to execute on up migration
        down: String, // Code to execute on down migration
    },

    /// Execute a data migration
    DataMigration {
        description: String,
        up_query: String,
        down_query: String,
    },
}

impl MigrationStep {
    /// Execute the step in the up direction
    pub fn execute_up(&self, database: &Database) -> Result<()> {
        match self {
            MigrationStep::CreateTable { name, definition } => {
                // Store table definition in metadata
                let table_key = format!("__table_def_{}", name);
                let table_data = serde_json::to_vec(definition)
                    .map_err(|e| crate::Error::Serialization(e.to_string()))?;
                database.put(table_key.as_bytes(), &table_data)?;
            }

            MigrationStep::DropTable { name, if_exists } => {
                let table_key = format!("__table_def_{}", name);
                if database.get(table_key.as_bytes())?.is_some() || !if_exists {
                    database.delete(table_key.as_bytes())?;
                }
            }

            MigrationStep::RenameTable { old_name, new_name } => {
                let old_key = format!("__table_def_{}", old_name);
                let new_key = format!("__table_def_{}", new_name);

                if let Some(data) = database.get(old_key.as_bytes())? {
                    database.put(new_key.as_bytes(), &data)?;
                    database.delete(old_key.as_bytes())?;
                }
            }

            MigrationStep::CreateIndex { definition } => {
                let index_key = format!("__index_def_{}", definition.name);
                let index_data = serde_json::to_vec(definition)
                    .map_err(|e| crate::Error::Serialization(e.to_string()))?;
                database.put(index_key.as_bytes(), &index_data)?;
            }

            MigrationStep::DropIndex { name, if_exists } => {
                let index_key = format!("__index_def_{}", name);
                if database.get(index_key.as_bytes())?.is_some() || !if_exists {
                    database.delete(index_key.as_bytes())?;
                }
            }

            MigrationStep::Custom { up, .. } => {
                // In a real implementation, this would execute the custom code
                // For now, we just log it
                tracing::info!("Executing custom migration: {}", up);
            }

            _ => {
                // Other steps would be implemented similarly
                tracing::warn!("Migration step not fully implemented: {:?}", self);
            }
        }

        Ok(())
    }

    /// Execute the step in the down direction
    pub fn execute_down(&self, database: &Database) -> Result<()> {
        match self {
            MigrationStep::CreateTable { name, .. } => {
                // Reverse of create is drop
                let table_key = format!("__table_def_{}", name);
                database.delete(table_key.as_bytes())?;
            }

            MigrationStep::DropTable {
                name: _,
                if_exists: _,
            } => {
                // For rollback, we need the original definition
                // This would be stored in migration history
                tracing::warn!("Cannot rollback table drop without original definition");
            }

            MigrationStep::RenameTable { old_name, new_name } => {
                // Reverse the rename
                let old_key = format!("__table_def_{}", old_name);
                let new_key = format!("__table_def_{}", new_name);

                if let Some(data) = database.get(new_key.as_bytes())? {
                    database.put(old_key.as_bytes(), &data)?;
                    database.delete(new_key.as_bytes())?;
                }
            }

            MigrationStep::CreateIndex { definition } => {
                // Reverse of create is drop
                let index_key = format!("__index_def_{}", definition.name);
                database.delete(index_key.as_bytes())?;
            }

            MigrationStep::DropIndex {
                name: _,
                if_exists: _,
            } => {
                // For rollback, we need the original definition
                tracing::warn!("Cannot rollback index drop without original definition");
            }

            MigrationStep::Custom { down, .. } => {
                // Execute the down custom code
                tracing::info!("Executing custom rollback: {}", down);
            }

            _ => {
                tracing::warn!("Rollback not fully implemented for step: {:?}", self);
            }
        }

        Ok(())
    }

    /// Check if this step is destructive
    pub fn is_destructive(&self) -> bool {
        matches!(
            self,
            MigrationStep::DropTable { .. }
                | MigrationStep::DropColumn { .. }
                | MigrationStep::DropIndex { .. }
                | MigrationStep::DropConstraint { .. }
        )
    }
}

/// Builder for creating migrations
pub struct MigrationBuilder {
    version: SchemaVersion,
    description: String,
    steps: Vec<MigrationStep>,
    target_schema: Option<Schema>,
}

impl MigrationBuilder {
    /// Create a new migration builder
    pub fn new(version: SchemaVersion, description: impl Into<String>) -> Self {
        Self {
            version,
            description: description.into(),
            steps: Vec::new(),
            target_schema: None,
        }
    }

    /// Add a migration step
    pub fn add_step(mut self, step: MigrationStep) -> Self {
        self.steps.push(step);
        self
    }

    /// Create a table
    pub fn create_table(self, name: impl Into<String>, definition: super::TableDefinition) -> Self {
        self.add_step(MigrationStep::CreateTable {
            name: name.into(),
            definition,
        })
    }

    /// Drop a table
    pub fn drop_table(self, name: impl Into<String>, if_exists: bool) -> Self {
        self.add_step(MigrationStep::DropTable {
            name: name.into(),
            if_exists,
        })
    }

    /// Rename a table
    pub fn rename_table(self, old_name: impl Into<String>, new_name: impl Into<String>) -> Self {
        self.add_step(MigrationStep::RenameTable {
            old_name: old_name.into(),
            new_name: new_name.into(),
        })
    }

    /// Add a column
    pub fn add_column(self, table: impl Into<String>, column: super::ColumnDefinition) -> Self {
        self.add_step(MigrationStep::AddColumn {
            table: table.into(),
            column,
        })
    }

    /// Drop a column
    pub fn drop_column(
        self,
        table: impl Into<String>,
        column: impl Into<String>,
        if_exists: bool,
    ) -> Self {
        self.add_step(MigrationStep::DropColumn {
            table: table.into(),
            column: column.into(),
            if_exists,
        })
    }

    /// Create an index
    pub fn create_index(self, definition: super::IndexDefinition) -> Self {
        self.add_step(MigrationStep::CreateIndex { definition })
    }

    /// Drop an index
    pub fn drop_index(self, name: impl Into<String>, if_exists: bool) -> Self {
        self.add_step(MigrationStep::DropIndex {
            name: name.into(),
            if_exists,
        })
    }

    /// Add custom migration code
    pub fn custom(self, up: impl Into<String>, down: impl Into<String>) -> Self {
        self.add_step(MigrationStep::Custom {
            up: up.into(),
            down: down.into(),
        })
    }

    /// Set the target schema
    pub fn target_schema(mut self, schema: Schema) -> Self {
        self.target_schema = Some(schema);
        self
    }

    /// Build the migration
    pub fn build(self) -> Result<SimpleMigration> {
        let target_schema = self
            .target_schema
            .ok_or_else(|| crate::Error::Config("Target schema not set".to_string()))?;

        Ok(SimpleMigration {
            version: self.version,
            description: self.description,
            steps: self.steps,
            target_schema,
        })
    }
}

/// Simple migration implementation
pub struct SimpleMigration {
    version: SchemaVersion,
    description: String,
    steps: Vec<MigrationStep>,
    target_schema: Schema,
}

impl Migration for SimpleMigration {
    fn version(&self) -> SchemaVersion {
        self.version.clone()
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn steps(&self) -> Vec<MigrationStep> {
        self.steps.clone()
    }

    fn target_schema(&self) -> Schema {
        self.target_schema.clone()
    }

    fn is_destructive(&self) -> bool {
        self.steps.iter().any(|step| step.is_destructive())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_migration::{ColumnDefinition, DataType, TableDefinition};

    #[test]
    fn test_migration_builder() {
        let version = SchemaVersion::new(1, 0);
        let table_def = TableDefinition {
            name: "users".to_string(),
            columns: vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            }],
            primary_key: vec!["id".to_string()],
            options: Default::default(),
        };

        let target_schema = Schema {
            version: version.clone(),
            tables: Default::default(),
            indexes: Default::default(),
            constraints: Default::default(),
            metadata: Default::default(),
        };

        let migration = MigrationBuilder::new(version, "Create users table")
            .create_table("users", table_def)
            .target_schema(target_schema)
            .build()
            .unwrap();

        assert_eq!(migration.description(), "Create users table");
        assert_eq!(migration.steps().len(), 1);
        assert!(!migration.is_destructive());
    }

    #[test]
    fn test_destructive_steps() {
        let drop_table = MigrationStep::DropTable {
            name: "users".to_string(),
            if_exists: true,
        };
        assert!(drop_table.is_destructive());

        let create_table = MigrationStep::CreateTable {
            name: "users".to_string(),
            definition: TableDefinition {
                name: "users".to_string(),
                columns: vec![],
                primary_key: vec![],
                options: Default::default(),
            },
        };
        assert!(!create_table.is_destructive());
    }
}
